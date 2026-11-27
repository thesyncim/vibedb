package gatewayruntime

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
)

func TestObservabilityHandlerHealthzReadyzAndMethodEnforcement(t *testing.T) {
	alive := true
	ready := true
	reason := ""
	h := NewObservabilityHandler(ObservabilitySource{
		Alive: func() bool { return alive },
		Ready: func() (bool, string) { return ready, reason },
		Metrics: func() gateway.MetricsSnapshot {
			return gateway.MetricsSnapshot{
				RouteSingle:   12,
				RouteTargeted: 3,
				ShardsFanned:  18,
				ReadLatency: gateway.LatencyHistogramSnapshot{
					Count: 3,
					SumUS: 1750,
					Buckets: [gateway.LatencyBucketCount]uint64{
						1, // <=50us
						0, // <=100us
						1, // <=250us
						0, // <=500us
						0, // <=1000us
						1, // <=2500us
					},
				},
				WriteLatency: gateway.LatencyHistogramSnapshot{
					Count: 1,
					SumUS: 6000000,
					Buckets: [gateway.LatencyBucketCount]uint64{
						0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
						1, // +Inf (>5s)
					},
				},
			}
		},
	})

	// 1. /healthz 200 OK
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"status":"ok"}` {
		t.Fatalf("/healthz = %d %q", rec.Code, rec.Body.String())
	}

	// 2. /readyz 200 OK while ready, then 503 during drain while /healthz stays 200.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"status":"ready"}` {
		t.Fatalf("/readyz = %d %q", rec.Code, rec.Body.String())
	}
	ready = false
	reason = "draining"
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"draining"`) {
		t.Fatalf("/readyz draining = %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz during drain = %d, want 200", rec.Code)
	}

	// 3. /healthz returns 503 once closing.
	alive = false
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/healthz closing = %d, want 503", rec.Code)
	}

	// 4. POST rejected with 405 Method Not Allowed.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("POST /metrics = %d Allow=%q", rec.Code, rec.Header().Get("Allow"))
	}

	// 5. /metrics emits valid cumulative Prometheus exposition format.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Fatalf("Content-Type = %q", ct)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`vibedb_gateway_route_total{kind="single"} 12`,
		`vibedb_gateway_route_total{kind="targeted"} 3`,
		`vibedb_gateway_shards_fanned_total 18`,
		`# TYPE vibedb_gateway_read_duration_seconds histogram`,
		`vibedb_gateway_read_duration_seconds_bucket{le="0.00005"} 1`,
		`vibedb_gateway_read_duration_seconds_bucket{le="0.0001"} 1`,
		`vibedb_gateway_read_duration_seconds_bucket{le="0.00025"} 2`,
		`vibedb_gateway_read_duration_seconds_bucket{le="0.0025"} 3`,
		`vibedb_gateway_read_duration_seconds_bucket{le="+Inf"} 3`,
		`vibedb_gateway_read_duration_seconds_sum 0.00175`,
		`vibedb_gateway_read_duration_seconds_count 3`,
		`vibedb_gateway_write_duration_seconds_bucket{le="5"} 0`,
		`vibedb_gateway_write_duration_seconds_bucket{le="+Inf"} 1`,
		`vibedb_gateway_write_duration_seconds_sum 6`,
		`vibedb_gateway_write_duration_seconds_count 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics missing %q\nfull body:\n%s", want, body)
		}
	}

	// 6. Non-loopback plaintext observability address is rejected.
	if _, _, _, err := startGatewayObservabilityHTTP(t.Context(), "0.0.0.0:9090", nil, h, func(string, ...any) {}); err == nil {
		t.Fatal("expected non-loopback plaintext observability HTTP to be rejected")
	}
}
