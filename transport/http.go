// Package transport carries Raft RPCs between nodes over HTTP.
//
// Each RPC is a POST of a gob-encoded request to /raft/v1/<method>. HTTP/1.1
// keep-alive connections are pooled per peer, and every call honours its
// context deadline so a slow or partitioned peer cannot stall the leader.
package transport

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/kunwar-dev/quorumkv/raft"
)

const (
	pathRequestVote     = "/raft/v1/request-vote"
	pathAppendEntries   = "/raft/v1/append-entries"
	pathInstallSnapshot = "/raft/v1/install-snapshot"
	// Prefix is the URL prefix under which Handler must be mounted.
	Prefix = "/raft/"
)

// HTTP is a raft.Transport over HTTP.
type HTTP struct {
	peers  map[string]string // node ID -> host:port
	client *http.Client
}

var _ raft.Transport = (*HTTP)(nil)

// NewHTTP returns a transport that resolves node IDs using peers.
func NewHTTP(peers map[string]string) *HTTP {
	return &HTTP{
		peers: peers,
		client: &http.Client{Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConnsPerHost: 8,
			IdleConnTimeout:     90 * time.Second,
		}},
	}
}

// RequestVote implements raft.Transport.
func (t *HTTP) RequestVote(ctx context.Context, peer string, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	resp := new(raft.RequestVoteResponse)
	return resp, t.call(ctx, peer, pathRequestVote, req, resp)
}

// AppendEntries implements raft.Transport.
func (t *HTTP) AppendEntries(ctx context.Context, peer string, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	resp := new(raft.AppendEntriesResponse)
	return resp, t.call(ctx, peer, pathAppendEntries, req, resp)
}

// InstallSnapshot implements raft.Transport.
func (t *HTTP) InstallSnapshot(ctx context.Context, peer string, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	resp := new(raft.InstallSnapshotResponse)
	return resp, t.call(ctx, peer, pathInstallSnapshot, req, resp)
}

func (t *HTTP) call(ctx context.Context, peer, path string, req, resp any) error {
	addr, ok := t.peers[peer]
	if !ok {
		return fmt.Errorf("transport: unknown peer %q", peer)
	}
	var body bytes.Buffer
	if err := gob.NewEncoder(&body).Encode(req); err != nil {
		return fmt.Errorf("transport: encode: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, &body)
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/x-gob")
	httpResp, err := t.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(httpResp.Body, 512))
		return fmt.Errorf("transport: %s %s: %s: %s", peer, path, httpResp.Status, bytes.TrimSpace(msg))
	}
	if err := gob.NewDecoder(httpResp.Body).Decode(resp); err != nil {
		return fmt.Errorf("transport: decode: %w", err)
	}
	return nil
}

// Handler serves incoming Raft RPCs for h. Mount it at Prefix.
func Handler(h raft.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+pathRequestVote, serve(h.HandleRequestVote))
	mux.HandleFunc("POST "+pathAppendEntries, serve(h.HandleAppendEntries))
	mux.HandleFunc("POST "+pathInstallSnapshot, serve(h.HandleInstallSnapshot))
	return mux
}

func serve[Req, Resp any](handle func(*Req) *Resp) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req := new(Req)
		if err := gob.NewDecoder(r.Body).Decode(req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/x-gob")
		if err := gob.NewEncoder(w).Encode(handle(req)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
