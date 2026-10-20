// Package metrics is a minimal, dependency-free implementation of the
// Prometheus text exposition format: labelled counters, labelled histograms
// and gauges computed on scrape.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
)

type collector interface {
	write(w io.Writer)
}

// Registry holds metrics and renders them for scraping.
type Registry struct {
	mu         sync.Mutex
	collectors []collector
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) register(c collector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.collectors = append(r.collectors, c)
}

// Write renders every metric in registration order.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	cs := slices.Clone(r.collectors)
	r.mu.Unlock()
	for _, c := range cs {
		c.write(w)
	}
}

// Handler serves the registry at a /metrics endpoint.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.Write(w)
	})
}

// CounterVec is a set of monotonically increasing counters keyed by labels.
type CounterVec struct {
	name, help string
	labels     []string
	mu         sync.Mutex
	values     map[string]uint64
}

// NewCounterVec registers a counter family.
func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{name: name, help: help, labels: labels, values: map[string]uint64{}}
	r.register(c)
	return c
}

// Inc increments the counter for the given label values.
func (c *CounterVec) Inc(labelValues ...string) {
	key := labelKey(c.labels, labelValues)
	c.mu.Lock()
	c.values[key]++
	c.mu.Unlock()
}

func (c *CounterVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", c.name, c.help, c.name)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range sortedKeys(c.values) {
		fmt.Fprintf(w, "%s%s %d\n", c.name, k, c.values[k])
	}
}

// HistogramVec is a set of histograms keyed by labels.
type HistogramVec struct {
	name, help string
	labels     []string
	buckets    []float64
	mu         sync.Mutex
	values     map[string]*histogram
}

type histogram struct {
	counts []uint64
	sum    float64
	count  uint64
}

// DefaultLatencyBuckets covers 0.5ms to 5s, suitable for request latency in seconds.
var DefaultLatencyBuckets = []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}

// NewHistogramVec registers a histogram family with the given upper bounds.
func (r *Registry) NewHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	h := &HistogramVec{name: name, help: help, labels: labels, buckets: buckets, values: map[string]*histogram{}}
	r.register(h)
	return h
}

// Observe records v for the given label values.
func (h *HistogramVec) Observe(v float64, labelValues ...string) {
	key := labelKey(h.labels, labelValues)
	h.mu.Lock()
	defer h.mu.Unlock()
	hist, ok := h.values[key]
	if !ok {
		hist = &histogram{counts: make([]uint64, len(h.buckets))}
		h.values[key] = hist
	}
	for i, ub := range h.buckets {
		if v <= ub {
			hist.counts[i]++
		}
	}
	hist.sum += v
	hist.count++
}

func (h *HistogramVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range sortedKeys(h.values) {
		hist := h.values[k]
		for i, ub := range h.buckets {
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, withLabel(k, "le", formatFloat(ub)), hist.counts[i])
		}
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, withLabel(k, "le", "+Inf"), hist.count)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.name, k, formatFloat(hist.sum))
		fmt.Fprintf(w, "%s_count%s %d\n", h.name, k, hist.count)
	}
}

// GaugeFunc reports a value computed at scrape time.
type GaugeFunc struct {
	name, help string
	fn         func() float64
}

// NewGaugeFunc registers a gauge whose value is read from fn on each scrape.
func (r *Registry) NewGaugeFunc(name, help string, fn func() float64) {
	r.register(&GaugeFunc{name: name, help: help, fn: fn})
}

func (g *GaugeFunc) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %s\n", g.name, g.help, g.name, g.name, formatFloat(g.fn()))
}

func labelKey(names, values []string) string {
	if len(names) != len(values) {
		panic(fmt.Sprintf("metrics: expected %d label values, got %d", len(names), len(values)))
	}
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n)
		b.WriteString(`="`)
		b.WriteString(escape(values[i]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func withLabel(key, name, value string) string {
	l := name + `="` + value + `"`
	if key == "" {
		return "{" + l + "}"
	}
	return key[:len(key)-1] + "," + l + "}"
}

func escape(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(s)
}

func formatFloat(f float64) string {
	if math.IsInf(f, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
