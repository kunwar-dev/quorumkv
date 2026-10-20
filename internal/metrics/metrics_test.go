package metrics

import (
	"strings"
	"testing"
)

func TestExpositionFormat(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("qkv_requests_total", "Requests.", "op", "code")
	h := r.NewHistogramVec("qkv_latency_seconds", "Latency.", []float64{0.01, 0.1}, "op")
	r.NewGaugeFunc("qkv_term", "Term.", func() float64 { return 7 })

	c.Inc("put", "204")
	c.Inc("put", "204")
	c.Inc("get", "404")
	h.Observe(0.005, "get")
	h.Observe(0.05, "get")

	var b strings.Builder
	r.Write(&b)
	out := b.String()
	for _, want := range []string{
		"# TYPE qkv_requests_total counter",
		`qkv_requests_total{op="put",code="204"} 2`,
		`qkv_requests_total{op="get",code="404"} 1`,
		`qkv_latency_seconds_bucket{op="get",le="0.01"} 1`,
		`qkv_latency_seconds_bucket{op="get",le="0.1"} 2`,
		`qkv_latency_seconds_bucket{op="get",le="+Inf"} 2`,
		`qkv_latency_seconds_count{op="get"} 2`,
		"qkv_term 7",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}
