package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kunwar-dev/quorumkv/client"
	"github.com/kunwar-dev/quorumkv/server"
)

type node struct {
	id   string
	addr string
	srv  *server.Server
}

type harness struct {
	t     *testing.T
	root  string
	peers map[string]string
	nodes map[string]*node
}

func newHarness(t *testing.T, n int) *harness {
	t.Helper()
	h := &harness{t: t, root: t.TempDir(), peers: map[string]string{}, nodes: map[string]*node{}}
	listeners := map[string]net.Listener{}
	for i := 1; i <= n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("n%d", i)
		h.peers[id] = ln.Addr().String()
		listeners[id] = ln
	}
	for id, ln := range listeners {
		h.start(id, ln)
	}
	t.Cleanup(func() {
		for id := range h.nodes {
			h.stop(id)
		}
	})
	return h
}

func (h *harness) start(id string, ln net.Listener) {
	h.t.Helper()
	srv, err := server.New(server.Config{
		ID:                id,
		Peers:             h.peers,
		DataDir:           filepath.Join(h.root, id),
		ElectionTimeout:   150 * time.Millisecond,
		HeartbeatInterval: 30 * time.Millisecond,
		SnapshotThreshold: 50,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		h.t.Fatal(err)
	}
	go srv.Serve(ln)
	h.nodes[id] = &node{id: id, addr: h.peers[id], srv: srv}
}

func (h *harness) stop(id string) {
	n, ok := h.nodes[id]
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n.srv.Shutdown(ctx)
	delete(h.nodes, id)
}

func (h *harness) restart(id string) {
	h.t.Helper()
	var ln net.Listener
	var err error
	for range 50 { // the port may take a moment to be released
		if ln, err = net.Listen("tcp", h.peers[id]); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		h.t.Fatal(err)
	}
	h.start(id, ln)
}

func (h *harness) status(addr string) (server.StatusResponse, error) {
	var st server.StatusResponse
	resp, err := http.Get("http://" + addr + "/v1/status")
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	return st, json.NewDecoder(resp.Body).Decode(&st)
}

func (h *harness) leader() string {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for id, n := range h.nodes {
			if st, err := h.status(n.addr); err == nil && st.Role == "leader" {
				return id
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatal("no leader")
	return ""
}

func (h *harness) endpoints() []string {
	var eps []string
	for _, a := range h.peers {
		eps = append(eps, a)
	}
	return eps
}

func TestEndToEnd(t *testing.T) {
	h := newHarness(t, 3)
	c, err := client.New(h.endpoints())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := range 120 {
		if err := c.Put(ctx, fmt.Sprintf("user/%d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if v, err := c.Get(ctx, "user/7"); err != nil || string(v) != "v7" {
		t.Fatalf("get user/7 = %q, %v", v, err)
	}
	if err := c.Delete(ctx, "user/7"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := c.Get(ctx, "user/7"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("get deleted key: %v", err)
	}

	// Kill the leader: the cluster must elect a new one and keep serving.
	old := h.leader()
	h.stop(old)
	if err := c.Put(ctx, "after-failover", []byte("ok")); err != nil {
		t.Fatalf("put after leader failure: %v", err)
	}
	if v, err := c.Get(ctx, "user/42"); err != nil || string(v) != "v42" {
		t.Fatalf("data lost after failover: %q, %v", v, err)
	}

	// Restart the old leader from disk; it must catch up.
	h.restart(old)
	deadline := time.Now().Add(10 * time.Second)
	for {
		st, err := h.status(h.peers[old])
		leaderSt, lerr := h.status(h.peers[h.leader()])
		if err == nil && lerr == nil && st.AppliedIndex >= leaderSt.CommitIndex && st.Keys == leaderSt.Keys {
			if st.SnapshotIndex == 0 {
				t.Fatal("expected restarted node to have a snapshot (threshold 50)")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted node did not catch up: %+v vs leader %+v", st, leaderSt)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestFollowerRedirectsToLeader(t *testing.T) {
	h := newHarness(t, 3)
	leader := h.leader()
	var follower string
	for id := range h.peers {
		if id != leader {
			follower = id
			break
		}
	}
	hc := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, _ := http.NewRequest(http.MethodPut, "http://"+h.peers[follower]+"/v1/kv/k", strings.NewReader("v"))
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want 307", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); !strings.Contains(loc, h.peers[leader]) {
		t.Fatalf("Location %q does not point at leader %s", loc, h.peers[leader])
	}
}

func TestMetricsEndpoint(t *testing.T) {
	h := newHarness(t, 1)
	c, _ := client.New(h.endpoints())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Put(ctx, "m", []byte("1")); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get("http://" + h.peers["n1"] + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, want := range []string{`quorumkv_requests_total{op="put",code="204"} 1`, "quorumkv_raft_is_leader 1", "quorumkv_kv_keys 1"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}
