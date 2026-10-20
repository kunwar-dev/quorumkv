// Package server wires a WAL-backed Raft node, the replicated KV store, the
// HTTP Raft transport and the client-facing HTTP API into a single process.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/kunwar-dev/quorumkv/internal/metrics"
	"github.com/kunwar-dev/quorumkv/kv"
	"github.com/kunwar-dev/quorumkv/raft"
	"github.com/kunwar-dev/quorumkv/transport"
	"github.com/kunwar-dev/quorumkv/wal"
)

// MaxValueSize bounds the size of a single value.
const MaxValueSize = 1 << 20

// Request headers used for exactly-once semantics.
const (
	HeaderClientID = "X-Client-Id"
	HeaderSeq      = "X-Request-Seq"
	HeaderLeader   = "X-Raft-Leader"
)

// Config configures a Server.
type Config struct {
	// ID is this node's ID; it must be a key of Peers.
	ID string
	// Peers maps every node ID (including this one) to its host:port.
	Peers map[string]string
	// DataDir holds this node's WAL and snapshot.
	DataDir           string
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	SnapshotThreshold uint64
	// RequestTimeout bounds how long a client request waits to commit.
	RequestTimeout time.Duration
	// NoSync disables fsync. Unsafe; for benchmarks only.
	NoSync bool
	Logger *slog.Logger
}

// Server is a single quorumkv node.
type Server struct {
	cfg     Config
	logger  *slog.Logger
	storage *wal.Storage
	store   *kv.Store
	http    *http.Server
	reg     *metrics.Registry
	reqs    *metrics.CounterVec
	latency *metrics.HistogramVec
}

// New opens the node's storage, restores its state and prepares the HTTP
// handlers. Call Serve to start accepting traffic.
func New(cfg Config) (*Server, error) {
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("server: peers must include this node %q", cfg.ID)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 10_000
	}

	storage, err := wal.Open(cfg.DataDir, wal.Options{NoSync: cfg.NoSync, Logger: cfg.Logger})
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	store, err := kv.Open(raft.Config{
		ID:                cfg.ID,
		Peers:             ids,
		Storage:           storage,
		Transport:         transport.NewHTTP(cfg.Peers),
		ElectionTimeout:   cfg.ElectionTimeout,
		HeartbeatInterval: cfg.HeartbeatInterval,
		Logger:            cfg.Logger,
	}, kv.Options{SnapshotThreshold: cfg.SnapshotThreshold, Logger: cfg.Logger})
	if err != nil {
		storage.Close()
		return nil, err
	}

	s := &Server{cfg: cfg, logger: cfg.Logger.With("node", cfg.ID), storage: storage, store: store, reg: metrics.NewRegistry()}
	s.registerMetrics()
	s.http = &http.Server{Handler: s.routes(), ReadHeaderTimeout: 5 * time.Second}
	return s, nil
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/kv/{key...}", s.handleKV(kv.OpGet))
	mux.HandleFunc("PUT /v1/kv/{key...}", s.handleKV(kv.OpPut))
	mux.HandleFunc("DELETE /v1/kv/{key...}", s.handleKV(kv.OpDelete))
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("GET /metrics", s.reg.Handler())
	mux.Handle(transport.Prefix, transport.Handler(s.store.Node()))
	return mux
}

// Serve accepts connections on ln until Shutdown. It returns
// http.ErrServerClosed after a graceful shutdown.
func (s *Server) Serve(ln net.Listener) error {
	s.logger.Info("serving", "addr", ln.Addr().String(), "peers", len(s.cfg.Peers))
	return s.http.Serve(ln)
}

// Shutdown stops accepting requests, waits for in-flight ones, then stops
// the Raft node and closes storage.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.http.Shutdown(ctx)
	s.store.Close()
	if cerr := s.storage.Close(); err == nil {
		err = cerr
	}
	return err
}

// Handler returns the server's HTTP handler (useful for tests).
func (s *Server) Handler() http.Handler { return s.http.Handler }

func (s *Server) handleKV(op kv.Op) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		code := s.serveKV(w, r, op)
		s.reqs.Inc(op.String(), strconv.Itoa(code))
		s.latency.Observe(time.Since(start).Seconds(), op.String())
	}
}

func (s *Server) serveKV(w http.ResponseWriter, r *http.Request, op kv.Op) int {
	key := r.PathValue("key")
	if key == "" {
		return writeError(w, http.StatusBadRequest, "key is required")
	}
	cmd := kv.Command{Op: op, Key: key, ClientID: r.Header.Get(HeaderClientID)}
	if cmd.ClientID != "" {
		seq, err := strconv.ParseUint(r.Header.Get(HeaderSeq), 10, 64)
		if err != nil || seq == 0 {
			return writeError(w, http.StatusBadRequest, HeaderSeq+" must be a positive integer when "+HeaderClientID+" is set")
		}
		cmd.Seq = seq
	}
	if op == kv.OpPut {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxValueSize))
		if err != nil {
			return writeError(w, http.StatusRequestEntityTooLarge, "value exceeds 1 MiB")
		}
		cmd.Value = body
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	res, err := s.store.Execute(ctx, cmd)

	var notLeader *kv.NotLeaderError
	switch {
	case err == nil:
	case errors.As(err, &notLeader):
		if addr, ok := s.cfg.Peers[notLeader.Leader]; ok && notLeader.Leader != "" {
			w.Header().Set(HeaderLeader, notLeader.Leader)
			w.Header().Set("Location", "http://"+addr+r.URL.RequestURI())
			w.WriteHeader(http.StatusTemporaryRedirect)
			return http.StatusTemporaryRedirect
		}
		w.Header().Set("Retry-After", "1")
		return writeError(w, http.StatusServiceUnavailable, "no leader elected; retry")
	case errors.Is(err, kv.ErrLeadershipLost), errors.Is(err, context.DeadlineExceeded):
		w.Header().Set("Retry-After", "1")
		return writeError(w, http.StatusServiceUnavailable, err.Error())
	default:
		s.logger.Error("request failed", "op", op, "err", err)
		return writeError(w, http.StatusInternalServerError, err.Error())
	}

	switch {
	case op == kv.OpGet && res.Found:
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		w.Write(res.Value)
		return http.StatusOK
	case op == kv.OpPut || res.Found:
		w.WriteHeader(http.StatusNoContent)
		return http.StatusNoContent
	default:
		return writeError(w, http.StatusNotFound, "key not found")
	}
}

// StatusResponse is returned by GET /v1/status.
type StatusResponse struct {
	ID            string `json:"id"`
	Role          string `json:"role"`
	Term          uint64 `json:"term"`
	Leader        string `json:"leader"`
	LeaderAddr    string `json:"leader_addr,omitempty"`
	CommitIndex   uint64 `json:"commit_index"`
	AppliedIndex  uint64 `json:"applied_index"`
	LastLogIndex  uint64 `json:"last_log_index"`
	SnapshotIndex uint64 `json:"snapshot_index"`
	Keys          int    `json:"keys"`
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	st, stats := s.store.Node().Status(), s.store.Stats()
	resp := StatusResponse{
		ID: st.ID, Role: st.Role.String(), Term: st.Term, Leader: st.Leader, LeaderAddr: s.cfg.Peers[st.Leader],
		CommitIndex: st.CommitIndex, AppliedIndex: stats.AppliedIndex, LastLogIndex: st.LastLogIndex,
		SnapshotIndex: st.SnapshotIndex, Keys: stats.Keys,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) registerMetrics() {
	s.reqs = s.reg.NewCounterVec("quorumkv_requests_total", "Client requests by operation and HTTP status.", "op", "code")
	s.latency = s.reg.NewHistogramVec("quorumkv_request_duration_seconds", "Client request latency, including replication.", metrics.DefaultLatencyBuckets, "op")
	node := s.store.Node()
	gauge := func(name, help string, f func(raft.Status) float64) {
		s.reg.NewGaugeFunc(name, help, func() float64 { return f(node.Status()) })
	}
	gauge("quorumkv_raft_term", "Current Raft term.", func(st raft.Status) float64 { return float64(st.Term) })
	gauge("quorumkv_raft_is_leader", "1 if this node is the leader.", func(st raft.Status) float64 {
		if st.Role == raft.Leader {
			return 1
		}
		return 0
	})
	gauge("quorumkv_raft_commit_index", "Highest committed log index.", func(st raft.Status) float64 { return float64(st.CommitIndex) })
	gauge("quorumkv_raft_last_log_index", "Index of the last log entry.", func(st raft.Status) float64 { return float64(st.LastLogIndex) })
	gauge("quorumkv_raft_snapshot_index", "Index covered by the latest snapshot.", func(st raft.Status) float64 { return float64(st.SnapshotIndex) })
	s.reg.NewGaugeFunc("quorumkv_kv_keys", "Number of keys in the store.", func() float64 { return float64(s.store.Stats().Keys) })
}

func writeError(w http.ResponseWriter, code int, msg string) int {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
	return code
}
