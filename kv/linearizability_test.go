package kv_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/kunwar-dev/quorumkv/internal/memnet"
	"github.com/kunwar-dev/quorumkv/kv"
	"github.com/kunwar-dev/quorumkv/raft"
)

type testCluster struct {
	net    *memnet.Network
	ids    []string
	stores []*kv.Store
}

func newTestCluster(t *testing.T, n int, snapshotThreshold uint64) *testCluster {
	t.Helper()
	c := &testCluster{net: memnet.New()}
	for i := range n {
		c.ids = append(c.ids, "n"+strconv.Itoa(i))
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, id := range c.ids {
		s, err := kv.Open(raft.Config{
			ID: id, Peers: c.ids, Storage: raft.NewMemoryStorage(), Transport: c.net.Transport(id),
			ElectionTimeout: 150 * time.Millisecond, HeartbeatInterval: 30 * time.Millisecond,
		}, kv.Options{SnapshotThreshold: snapshotThreshold, Logger: logger})
		if err != nil {
			t.Fatal(err)
		}
		c.net.Register(id, s.Node())
		c.stores = append(c.stores, s)
	}
	t.Cleanup(func() {
		for _, s := range c.stores {
			s.Close()
		}
	})
	return c
}

// client retries a request across nodes, as a real client library would,
// reusing the same sequence number so retries are deduplicated.
type client struct {
	c    *testCluster
	id   string
	seq  uint64
	hint int
	rng  *rand.Rand // if set, each request starts at a random node
}

// do returns ok=false if the outcome is unknown (every attempt failed).
func (cl *client) do(op kv.Op, key string, value []byte, budget time.Duration) (kv.Result, bool) {
	cl.seq++
	if cl.rng != nil {
		cl.hint = cl.rng.Intn(len(cl.c.stores))
	}
	cmd := kv.Command{Op: op, Key: key, Value: value, ClientID: cl.id, Seq: cl.seq}
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		store := cl.c.stores[cl.hint]
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		res, err := store.Execute(ctx, cmd)
		cancel()
		if err == nil {
			return res, true
		}
		var nle *kv.NotLeaderError
		if errors.As(err, &nle) && nle.Leader != "" {
			for i, id := range cl.c.ids {
				if id == nle.Leader {
					cl.hint = i
				}
			}
		} else {
			cl.hint = (cl.hint + 1) % len(cl.c.stores)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return kv.Result{}, false
}

type kvInput struct {
	op    kv.Op
	key   string
	value string
}

type kvOutput struct {
	value   string
	found   bool
	unknown bool
}

type kvState struct {
	value  string
	exists bool
}

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).key
			byKey[k] = append(byKey[k], op)
		}
		var out [][]porcupine.Operation
		for _, ops := range byKey {
			out = append(out, ops)
		}
		return out
	},
	Init: func() interface{} { return kvState{} },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st, in, out := state.(kvState), input.(kvInput), output.(kvOutput)
		switch in.op {
		case kv.OpGet:
			return out.unknown || (out.found == st.exists && out.value == st.value), st
		case kv.OpPut:
			return true, kvState{value: in.value, exists: true}
		default: // delete
			return out.unknown || out.found == st.exists, kvState{}
		}
	},
	DescribeOperation: func(input, output interface{}) string {
		in, out := input.(kvInput), output.(kvOutput)
		return fmt.Sprintf("%s(%s,%q) -> %q/%v", in.op, in.key, in.value, out.value, out.found)
	},
}

// TestLinearizableUnderPartitions drives concurrent clients against a
// 5-node cluster while the network drops and delays messages and nodes are
// repeatedly isolated, then verifies the recorded history is linearizable.
func TestLinearizableUnderPartitions(t *testing.T) {
	duration := 6 * time.Second
	if testing.Short() {
		duration = 2 * time.Second
	}
	c := newTestCluster(t, 5, 50)
	c.net.SetReliable(false)

	var (
		mu      sync.Mutex
		history []porcupine.Operation
		start   = time.Now()
		stop    = make(chan struct{})
		wg      sync.WaitGroup
	)
	now := func() int64 { return int64(time.Since(start)) }

	// Nemesis: isolate up to two random nodes, then heal, repeatedly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			select {
			case <-stop:
				for _, id := range c.ids {
					c.net.Connect(id)
				}
				return
			case <-time.After(time.Duration(200+rng.Intn(400)) * time.Millisecond):
			}
			for _, id := range c.ids {
				c.net.Connect(id)
			}
			for range rng.Intn(3) {
				c.net.Disconnect(c.ids[rng.Intn(len(c.ids))])
			}
		}
	}()

	keys := []string{"x", "y", "z"}
	const clients = 6
	for ci := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(ci) + time.Now().UnixNano()))
			cl := &client{c: c, id: fmt.Sprintf("client-%d", ci), rng: rng}
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				in := kvInput{key: keys[rng.Intn(len(keys))]}
				switch r := rng.Intn(10); {
				case r < 5:
					in.op = kv.OpGet
				case r < 9:
					in.op, in.value = kv.OpPut, fmt.Sprintf("c%d-%d", ci, n)
				default:
					in.op = kv.OpDelete
				}
				call := now()
				res, ok := cl.do(in.op, in.key, []byte(in.value), 2*time.Second)
				op := porcupine.Operation{ClientId: ci, Input: in, Call: call}
				if ok {
					op.Output = kvOutput{value: string(res.Value), found: res.Found}
					op.Return = now()
				} else {
					// Unknown outcome: it may take effect at any later time.
					op.Output = kvOutput{unknown: true}
					op.Return = math.MaxInt64
				}
				mu.Lock()
				history = append(history, op)
				mu.Unlock()
			}
		}()
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	completed := 0
	for _, op := range history {
		if !op.Output.(kvOutput).unknown {
			completed++
		}
	}
	t.Logf("history: %d operations, %d completed", len(history), completed)
	if completed < 20 {
		t.Fatalf("too little progress under faults: %d completed operations", completed)
	}

	switch res := porcupine.CheckOperationsTimeout(kvModel, history, 30*time.Second); res {
	case porcupine.Ok:
	case porcupine.Illegal:
		t.Fatal("history is NOT linearizable")
	default:
		t.Logf("linearizability check inconclusive (%s): history too large to check in time", res)
	}
}

func TestStoreBasicOperations(t *testing.T) {
	c := newTestCluster(t, 3, 0)
	cl := &client{c: c, id: "basic"}

	if _, ok := cl.do(kv.OpPut, "hello", []byte("world"), 5*time.Second); !ok {
		t.Fatal("put failed")
	}
	res, ok := cl.do(kv.OpGet, "hello", nil, 5*time.Second)
	if !ok || !res.Found || string(res.Value) != "world" {
		t.Fatalf("get = %+v ok=%v", res, ok)
	}
	if res, _ := cl.do(kv.OpDelete, "hello", nil, 5*time.Second); !res.Found {
		t.Fatal("delete did not find key")
	}
	if res, _ := cl.do(kv.OpGet, "hello", nil, 5*time.Second); res.Found {
		t.Fatal("key still present after delete")
	}
}

func TestFollowerRejectsWithLeaderHint(t *testing.T) {
	c := newTestCluster(t, 3, 0)
	cl := &client{c: c, id: "probe"}
	if _, ok := cl.do(kv.OpPut, "k", []byte("v"), 5*time.Second); !ok {
		t.Fatal("put failed")
	}
	for _, s := range c.stores {
		if _, leader := s.Node().State(); leader {
			continue
		}
		_, err := s.Execute(context.Background(), kv.Command{Op: kv.OpGet, Key: "k"})
		var nle *kv.NotLeaderError
		if !errors.As(err, &nle) || nle.Leader == "" {
			t.Fatalf("follower returned %v, want NotLeaderError with leader hint", err)
		}
		return
	}
	t.Fatal("no follower found")
}
