package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	mrand "math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kunwar-dev/quorumkv/client"
)

// runBench drives a closed-loop workload: c concurrent clients each issue
// requests back-to-back until n requests have completed in total.
func runBench(endpoints []string, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	n := fs.Int("n", 20_000, "total requests")
	conc := fs.Int("c", 32, "concurrent clients")
	size := fs.Int("size", 128, "value size in bytes")
	keys := fs.Int("keys", 10_000, "key space")
	writes := fs.Float64("writes", 0.5, "fraction of requests that are writes (0..1)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *conc <= 0 || *n <= 0 {
		return errors.New("bench: -n and -c must be positive")
	}

	value := make([]byte, *size)
	rand.Read(value)

	// Preload so reads hit existing keys.
	loader, err := client.New(endpoints)
	if err != nil {
		return err
	}
	preload := min(*keys, 1000)
	for i := range preload {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := loader.Put(ctx, keyName(i), value)
		cancel()
		if err != nil {
			return fmt.Errorf("preload: %w", err)
		}
	}

	var (
		remaining atomic.Int64
		failures  atomic.Int64
		mu        sync.Mutex
		latencies = make([]time.Duration, 0, *n)
		wg        sync.WaitGroup
	)
	remaining.Store(int64(*n))
	start := time.Now()
	for w := range *conc {
		c, err := client.New(rotate(endpoints, w))
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := mrand.New(mrand.NewSource(int64(w) + time.Now().UnixNano()))
			local := make([]time.Duration, 0, *n / *conc + 1)
			for remaining.Add(-1) >= 0 {
				key := keyName(rng.Intn(preload))
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				t0 := time.Now()
				var err error
				if rng.Float64() < *writes {
					err = c.Put(ctx, key, value)
				} else {
					_, err = c.Get(ctx, key)
				}
				cancel()
				if err != nil && !errors.Is(err, client.ErrNotFound) {
					failures.Add(1)
					continue
				}
				local = append(local, time.Since(t0))
			}
			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	slices.Sort(latencies)
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[min(len(latencies)-1, int(float64(len(latencies))*p))]
	}
	fmt.Printf("requests:    %d ok, %d failed\n", len(latencies), failures.Load())
	fmt.Printf("concurrency: %d clients, %.0f%% writes, %d-byte values\n", *conc, *writes*100, *size)
	fmt.Printf("duration:    %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput:  %.0f req/s\n", float64(len(latencies))/elapsed.Seconds())
	fmt.Printf("latency:     p50 %s  p95 %s  p99 %s  max %s\n",
		pct(0.50).Round(10*time.Microsecond), pct(0.95).Round(10*time.Microsecond),
		pct(0.99).Round(10*time.Microsecond), pct(1).Round(10*time.Microsecond))
	return nil
}

func keyName(i int) string { return fmt.Sprintf("bench/%06d", i) }

func rotate(s []string, k int) []string {
	k %= len(s)
	return append(append([]string(nil), s[k:]...), s[:k]...)
}
