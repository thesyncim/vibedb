package gatewayruntime

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/thesyncim/vibedb/gateway"
)

const (
	observabilityReadHeaderTimeout = 5 * time.Second
	observabilityReadTimeout       = 10 * time.Second
	observabilityWriteTimeout      = 10 * time.Second
	observabilityIdleTimeout       = 30 * time.Second
	observabilityMaxHeaderBytes    = 8 << 10
	observabilityShutdownTimeout   = 5 * time.Second
)

// ObservabilitySource provides live gateway state to the HTTP observability
// handler (/healthz, /readyz, /metrics). All callbacks must be safe for
// concurrent invocation and non-blocking.
type ObservabilitySource struct {
	Alive      func() bool
	Ready      func() (bool, string)
	Metrics    func() gateway.MetricsSnapshot
	Dist       func() *gateway.DistributedMetrics
	Controller func() *gatewayControllerMetrics
}

// NewObservabilityHandler builds a bounded, stdlib-only HTTP handler exposing:
//   - GET/HEAD /healthz : process liveness (200 while alive, 503 once closing)
//   - GET/HEAD /readyz  : serving readiness (200 while serving & not draining, 503 otherwise)
//   - GET/HEAD /metrics : Prometheus text exposition format (v0.0.4) with
//     gateway counters, cumulative read/write duration histograms, and optional
//     distributed RF3 and controller aggregates.
func NewObservabilityHandler(src ObservabilitySource) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if !allowObservabilityMethod(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if src.Alive != nil && !src.Alive() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("{\"status\":\"closing\"}\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{\"status\":\"ok\"}\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !allowObservabilityMethod(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		ready := true
		reason := ""
		if src.Ready != nil {
			ready, reason = src.Ready()
		}
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
			if reason == "" {
				reason = "not_ready"
			}
			_, _ = fmt.Fprintf(w, "{\"status\":\"not_ready\",\"reason\":%q}\n", reason)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{\"status\":\"ready\"}\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if !allowObservabilityMethod(w, r) {
			return
		}
		var buf bytes.Buffer
		buf.Grow(4096)
		if src.Metrics != nil {
			writePrometheusGatewayMetrics(&buf, src.Metrics())
		}
		if src.Dist != nil {
			if dist := src.Dist(); dist != nil {
				if agg, err := dist.Aggregate(); err == nil {
					writePrometheusDistributedMetrics(&buf, agg)
				}
			}
		}
		if src.Controller != nil {
			if ctrl := src.Controller(); ctrl != nil {
				writePrometheusControllerMetrics(&buf, ctrl.Snapshot())
			}
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write(buf.Bytes())
		}
	})
	return mux
}

func allowObservabilityMethod(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return true
	}
	w.Header().Set("Allow", "GET, HEAD")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusMethodNotAllowed)
	return false
}

func writePrometheusGatewayMetrics(buf *bytes.Buffer, m gateway.MetricsSnapshot) {
	buf.WriteString("# HELP vibedb_gateway_route_total Gateway operations routed by route classification.\n")
	buf.WriteString("# TYPE vibedb_gateway_route_total counter\n")
	fmt.Fprintf(buf, "vibedb_gateway_route_total{kind=\"single\"} %d\n", m.RouteSingle)
	fmt.Fprintf(buf, "vibedb_gateway_route_total{kind=\"targeted\"} %d\n", m.RouteTargeted)
	fmt.Fprintf(buf, "vibedb_gateway_route_total{kind=\"scatter\"} %d\n", m.RouteScatter)
	fmt.Fprintf(buf, "vibedb_gateway_route_total{kind=\"empty\"} %d\n", m.RouteEmpty)

	buf.WriteString("# HELP vibedb_gateway_shards_fanned_total Cumulative shards selected across routed operations.\n")
	buf.WriteString("# TYPE vibedb_gateway_shards_fanned_total counter\n")
	fmt.Fprintf(buf, "vibedb_gateway_shards_fanned_total %d\n", m.ShardsFanned)

	buf.WriteString("# HELP vibedb_gateway_rows_returned_total Rows returned by completed gateway operations.\n")
	buf.WriteString("# TYPE vibedb_gateway_rows_returned_total counter\n")
	fmt.Fprintf(buf, "vibedb_gateway_rows_returned_total %d\n", m.RowsReturned)

	buf.WriteString("# HELP vibedb_gateway_bytes_returned_total Result bytes returned by completed gateway operations.\n")
	buf.WriteString("# TYPE vibedb_gateway_bytes_returned_total counter\n")
	fmt.Fprintf(buf, "vibedb_gateway_bytes_returned_total %d\n", m.BytesReturned)

	buf.WriteString("# HELP vibedb_gateway_retries_total Stale-generation gateway retries.\n")
	buf.WriteString("# TYPE vibedb_gateway_retries_total counter\n")
	fmt.Fprintf(buf, "vibedb_gateway_retries_total %d\n", m.Retries)

	buf.WriteString("# HELP vibedb_gateway_scatter_reason_total Gateway scatter operations by reason.\n")
	buf.WriteString("# TYPE vibedb_gateway_scatter_reason_total counter\n")
	fmt.Fprintf(buf, "vibedb_gateway_scatter_reason_total{reason=\"all_shards\"} %d\n", m.ScatterAllShards)
	fmt.Fprintf(buf, "vibedb_gateway_scatter_reason_total{reason=\"unknown_route\"} %d\n", m.ScatterUnknownRoute)

	writePrometheusLatencyHistogram(
		buf,
		"vibedb_gateway_read_duration_seconds",
		"End-to-end latency of completed gateway read operations in seconds.",
		m.ReadLatency,
	)
	writePrometheusLatencyHistogram(
		buf,
		"vibedb_gateway_write_duration_seconds",
		"End-to-end latency of completed gateway write operations in seconds.",
		m.WriteLatency,
	)
}

func writePrometheusLatencyHistogram(
	buf *bytes.Buffer,
	name string,
	help string,
	h gateway.LatencyHistogramSnapshot,
) {
	fmt.Fprintf(buf, "# HELP %s %s\n", name, help)
	fmt.Fprintf(buf, "# TYPE %s histogram\n", name)
	var cumulative uint64
	for i, boundUS := range gateway.LatencyBoundsUS {
		cumulative += h.Buckets[i]
		le := strconv.FormatFloat(float64(boundUS)/1e6, 'f', -1, 64)
		fmt.Fprintf(buf, "%s_bucket{le=\"%s\"} %d\n", name, le, cumulative)
	}
	cumulative += h.Buckets[gateway.LatencyBucketCount-1]
	fmt.Fprintf(buf, "%s_bucket{le=\"+Inf\"} %d\n", name, cumulative)
	fmt.Fprintf(buf, "%s_sum %s\n", name, strconv.FormatFloat(float64(h.SumUS)/1e6, 'f', -1, 64))
	fmt.Fprintf(buf, "%s_count %d\n", name, h.Count)
}

func writePrometheusDistributedMetrics(buf *bytes.Buffer, agg gateway.DistributedMetricsAggregate) {
	buf.WriteString("# HELP vibedb_rf3_samples Cached RF3 member and node metric samples.\n")
	buf.WriteString("# TYPE vibedb_rf3_samples gauge\n")
	fmt.Fprintf(buf, "vibedb_rf3_samples %d\n", agg.Samples)

	buf.WriteString("# HELP vibedb_rf3_collection_reads_total Successful RF3 shard-control metric reads.\n")
	buf.WriteString("# TYPE vibedb_rf3_collection_reads_total counter\n")
	fmt.Fprintf(buf, "vibedb_rf3_collection_reads_total %d\n", agg.Reads)

	buf.WriteString("# HELP vibedb_rf3_collection_faults_total Failed RF3 shard-control metric reads.\n")
	buf.WriteString("# TYPE vibedb_rf3_collection_faults_total counter\n")
	fmt.Fprintf(buf, "vibedb_rf3_collection_faults_total %d\n", agg.Faults)

	buf.WriteString("# HELP vibedb_rf3_committed_entries_total Sum of committed Raft entries across sampled group members.\n")
	buf.WriteString("# TYPE vibedb_rf3_committed_entries_total counter\n")
	fmt.Fprintf(buf, "vibedb_rf3_committed_entries_total %d\n", agg.Cut.CommittedEntries)

	buf.WriteString("# HELP vibedb_rf3_applied_entries_total Sum of applied Raft entries across sampled group members.\n")
	buf.WriteString("# TYPE vibedb_rf3_applied_entries_total counter\n")
	fmt.Fprintf(buf, "vibedb_rf3_applied_entries_total %d\n", agg.Cut.AppliedEntries)

	buf.WriteString("# HELP vibedb_rf3_raft_faults_total Sum of Raft faults across sampled group members.\n")
	buf.WriteString("# TYPE vibedb_rf3_raft_faults_total counter\n")
	fmt.Fprintf(buf, "vibedb_rf3_raft_faults_total %d\n", agg.Cut.Faults)
}

func writePrometheusControllerMetrics(buf *bytes.Buffer, c gatewayControllerMetricsSnapshot) {
	buf.WriteString("# HELP vibedb_controller_passes_total Total gateway controller reconcile passes.\n")
	buf.WriteString("# TYPE vibedb_controller_passes_total counter\n")
	fmt.Fprintf(buf, "vibedb_controller_passes_total{controller=\"split\"} %d\n", c.SplitPasses)
	fmt.Fprintf(buf, "vibedb_controller_passes_total{controller=\"replica_move\"} %d\n", c.MovePasses)

	buf.WriteString("# HELP vibedb_controller_faults_total Total gateway controller reconcile faults.\n")
	buf.WriteString("# TYPE vibedb_controller_faults_total counter\n")
	fmt.Fprintf(buf, "vibedb_controller_faults_total{controller=\"split\"} %d\n", c.SplitFaults)
	fmt.Fprintf(buf, "vibedb_controller_faults_total{controller=\"replica_move\"} %d\n", c.MoveFaults)
}

func startGatewayObservabilityHTTP(
	ctx context.Context,
	address string,
	tlsConfig *tls.Config,
	handler http.Handler,
	logf func(string, ...any),
) (*http.Server, net.Listener, <-chan struct{}, error) {
	if tlsConfig == nil {
		if err := requireLoopbackListen(address); err != nil {
			return nil, nil, nil, fmt.Errorf("observability HTTP without TLS: %w", err)
		}
	}
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("listen observability HTTP %q: %w", address, err)
	}
	if tlsConfig != nil {
		ln = tls.NewListener(ln, tlsConfig.Clone())
	}
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: observabilityReadHeaderTimeout,
		ReadTimeout:       observabilityReadTimeout,
		WriteTimeout:      observabilityWriteTimeout,
		IdleTimeout:       observabilityIdleTimeout,
		MaxHeaderBytes:    observabilityMaxHeaderBytes,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) && ctx.Err() == nil {
			logf("gateway: observability HTTP stopped: %v", err)
		}
	}()
	logf("gateway: observability HTTP serving /healthz, /readyz, /metrics on %s", ln.Addr())
	return srv, ln, done, nil
}

func stopGatewayObservabilityHTTP(srv *http.Server, done <-chan struct{}) error {
	if srv == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), observabilityShutdownTimeout)
	defer cancel()
	err := srv.Shutdown(shutdownCtx)
	if done != nil {
		<-done
	}
	return err
}
