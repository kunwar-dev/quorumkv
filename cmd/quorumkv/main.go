// Command quorumkv runs a single node of a quorumkv cluster.
//
//	quorumkv -id n1 -peers n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kunwar-dev/quorumkv/server"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "quorumkv:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		id                = flag.String("id", "", "node ID (required)")
		peers             = flag.String("peers", "", "comma-separated id=host:port for every node, including this one (required)")
		listen            = flag.String("listen", "", "listen address (default: this node's address from -peers)")
		dataDir           = flag.String("data-dir", "", "data directory (default: ./data/<id>)")
		electionTimeout   = flag.Duration("election-timeout", 300*time.Millisecond, "base election timeout")
		heartbeat         = flag.Duration("heartbeat", 50*time.Millisecond, "leader heartbeat interval")
		snapshotThreshold = flag.Uint64("snapshot-threshold", 10_000, "entries between snapshots")
		logLevel          = flag.String("log-level", "info", "debug, info, warn or error")
		logFormat         = flag.String("log-format", "text", "text or json")
		noSync            = flag.Bool("unsafe-no-fsync", false, "disable fsync (benchmarks only; loses data on crash)")
		showVersion       = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()
	if *showVersion {
		fmt.Println("quorumkv", version)
		return nil
	}

	peerMap, err := parsePeers(*peers)
	if err != nil {
		return err
	}
	if *id == "" {
		return errors.New("-id is required")
	}
	addr := *listen
	if addr == "" {
		addr = peerMap[*id]
	}
	if *dataDir == "" {
		*dataDir = filepath.Join("data", *id)
	}
	logger, err := newLogger(*logLevel, *logFormat)
	if err != nil {
		return err
	}

	srv, err := server.New(server.Config{
		ID:                *id,
		Peers:             peerMap,
		DataDir:           *dataDir,
		ElectionTimeout:   *electionTimeout,
		HeartbeatInterval: *heartbeat,
		SnapshotThreshold: *snapshotThreshold,
		NoSync:            *noSync,
		Logger:            logger,
	})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	logger.Info("starting quorumkv", "version", version, "id", *id, "data_dir", *dataDir)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		logger.Info("shutting down")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func parsePeers(s string) (map[string]string, error) {
	if s == "" {
		return nil, errors.New("-peers is required")
	}
	m := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("invalid peer %q: want id=host:port", part)
		}
		if _, dup := m[id]; dup {
			return nil, fmt.Errorf("duplicate peer id %q", id)
		}
		m[id] = addr
	}
	return m, nil
}

func newLogger(level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid -log-level %q", level)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	switch format {
	case "text":
		return slog.New(slog.NewTextHandler(os.Stderr, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), nil
	default:
		return nil, fmt.Errorf("invalid -log-format %q", format)
	}
}
