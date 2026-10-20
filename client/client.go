// Package client is a Go client for quorumkv.
//
// The client tracks the current leader, follows leader redirects, fails over
// across endpoints, and retries transient failures. Each logical request
// carries the client's ID and a sequence number that stays the same across
// retries, so the cluster executes it exactly once even if a response is lost.
//
// A Client issues one request at a time; use one Client per goroutine.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// ErrNotFound is returned when a key does not exist.
var ErrNotFound = errors.New("quorumkv: key not found")

// Client talks to a quorumkv cluster.
type Client struct {
	hc *http.Client
	id string

	mu        sync.Mutex
	endpoints []string
	current   int
	seq       uint64
}

// Option customises a Client.
type Option func(*Client)

// WithHTTPClient overrides the underlying HTTP client.
func WithHTTPClient(hc *http.Client) Option { return func(c *Client) { c.hc = hc } }

// New returns a client for the cluster reachable at endpoints (host:port).
func New(endpoints []string, opts ...Option) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("quorumkv: at least one endpoint is required")
	}
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, err
	}
	c := &Client{
		hc:        &http.Client{Timeout: 10 * time.Second},
		id:        hex.EncodeToString(raw[:]),
		endpoints: append([]string(nil), endpoints...),
	}
	for _, o := range opts {
		o(c)
	}
	// Redirects are handled explicitly so the leader hint can be remembered.
	hc := *c.hc
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.hc = &hc
	return c, nil
}

// Get returns the value stored at key, or ErrNotFound.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	return c.do(ctx, http.MethodGet, key, nil)
}

// Put stores value at key.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	_, err := c.do(ctx, http.MethodPut, key, value)
	return err
}

// Delete removes key. It returns ErrNotFound if the key did not exist.
func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.do(ctx, http.MethodDelete, key, nil)
	return err
}

func (c *Client) do(ctx context.Context, method, key string, value []byte) ([]byte, error) {
	if key == "" {
		return nil, errors.New("quorumkv: empty key")
	}
	c.mu.Lock()
	c.seq++
	seq := c.seq
	c.mu.Unlock()

	path := (&url.URL{Path: "/v1/kv/" + key}).EscapedPath()
	backoff := 20 * time.Millisecond
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return nil, fmt.Errorf("%w (last error: %v)", err, lastErr)
			}
			return nil, err
		}
		endpoint := c.endpoint()
		req, err := http.NewRequestWithContext(ctx, method, "http://"+endpoint+path, bytes.NewReader(value))
		if err != nil {
			return nil, err
		}
		req.Header.Set("X-Client-Id", c.id)
		req.Header.Set("X-Request-Seq", fmt.Sprint(seq))

		resp, err := c.hc.Do(req)
		if err != nil {
			lastErr = err
			c.rotate(endpoint)
			sleep(ctx, &backoff)
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		resp.Body.Close()

		switch {
		case resp.StatusCode == http.StatusTemporaryRedirect:
			if loc, err := url.Parse(resp.Header.Get("Location")); err == nil && loc.Host != "" {
				c.setLeader(loc.Host)
				continue
			}
			lastErr = errors.New("redirect without location")
		case resp.StatusCode == http.StatusServiceUnavailable:
			lastErr = fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(body))
			sleep(ctx, &backoff)
			continue
		case resp.StatusCode == http.StatusNotFound:
			return nil, ErrNotFound
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			if readErr != nil {
				lastErr = readErr
				continue
			}
			return body, nil
		default:
			return nil, fmt.Errorf("quorumkv: %s: %s", resp.Status, bytes.TrimSpace(body))
		}
		c.rotate(endpoint)
		sleep(ctx, &backoff)
	}
}

func (c *Client) endpoint() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.endpoints[c.current]
}

// rotate moves to the next endpoint if `failed` is still the current one.
func (c *Client) rotate(failed string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.endpoints[c.current] == failed {
		c.current = (c.current + 1) % len(c.endpoints)
	}
}

func (c *Client) setLeader(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, e := range c.endpoints {
		if e == addr {
			c.current = i
			return
		}
	}
	c.endpoints = append(c.endpoints, addr)
	c.current = len(c.endpoints) - 1
}

func sleep(ctx context.Context, backoff *time.Duration) {
	t := time.NewTimer(*backoff)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
	*backoff = min(*backoff*2, time.Second)
}
