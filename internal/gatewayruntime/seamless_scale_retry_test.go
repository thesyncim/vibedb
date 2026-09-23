//go:build linux

package gatewayruntime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type seamlessScaleTestTimeoutError struct{}

func (seamlessScaleTestTimeoutError) Error() string   { return "i/o timeout" }
func (seamlessScaleTestTimeoutError) Timeout() bool   { return true }
func (seamlessScaleTestTimeoutError) Temporary() bool { return true }

var _ net.Error = seamlessScaleTestTimeoutError{}

func TestSeamlessScaleTimeoutClassification(t *testing.T) {
	start := time.Now()
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "wrapped network timeout", err: fmt.Errorf("native read: %w", seamlessScaleTestTimeoutError{}), want: true},
		{name: "wrapped deadline", err: fmt.Errorf("SQL read: %w", os.ErrDeadlineExceeded), want: true},
		{name: "wrapped context deadline", err: fmt.Errorf("request: %w", context.DeadlineExceeded), want: true},
		{name: "eof", err: fmt.Errorf("native read: %w", io.EOF), want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := seamlessScaleIsTimeout(test.err); got != test.want {
				t.Fatalf("timeout classification=%t want %t", got, test.want)
			}
		})
	}
	workload := &seamlessScaleWorkload{acknowledged: make(map[string]seamlessScaleAck)}
	evidence := workload.phaseEvidence(seamlessScalePhaseDuring, start, start.Add(time.Second), 2, 0,
		[]seamlessScaleSample{
			{Scheduled: start, Started: start, Completed: start.Add(time.Millisecond), Err: fmt.Errorf("read: %w", seamlessScaleTestTimeoutError{})},
			{Scheduled: start, Started: start, Completed: start.Add(2 * time.Millisecond), Err: io.EOF},
		})
	if evidence.Errors != 2 || evidence.Timeouts != 1 {
		t.Fatalf("timeout samples changed strict error accounting: errors=%d timeouts=%d", evidence.Errors, evidence.Timeouts)
	}
}

func TestSeamlessScaleReopensIncompleteSQLSocketWithoutReplayingWrite(t *testing.T) {
	old, oldPeer := net.Pipe()
	defer oldPeer.Close()
	oldQuery := make(chan string, 1)
	go func() {
		defer oldPeer.Close()
		query, err := readSeamlessScaleSimpleQuery(oldPeer)
		if err == nil {
			oldQuery <- query
		}
	}()
	var opened atomic.Uint32
	var peersMu sync.Mutex
	var peers []net.Conn
	queries := make(chan string, 1)
	openSQL := func(context.Context) (net.Conn, error) {
		client, peer := net.Pipe()
		peersMu.Lock()
		peers = append(peers, peer)
		peersMu.Unlock()
		opened.Add(1)
		go func() {
			query, err := readSeamlessScaleSimpleQuery(peer)
			if err != nil {
				return
			}
			queries <- query
			_ = writeSeamlessScaleCommandComplete(peer, "SELECT 1")
		}()
		return client, nil
	}
	connection := &seamlessScaleConnection{sql: old, openSQL: openSQL}
	t.Cleanup(func() {
		if connection.sql != nil {
			_ = connection.sql.Close()
		}
		peersMu.Lock()
		defer peersMu.Unlock()
		for _, peer := range peers {
			_ = peer.Close()
		}
	})
	workload := &seamlessScaleWorkload{connections: []seamlessScaleConnection{*connection}, workerCalls: make([]seamlessScaleWorkerCall, 1)}
	connection = &workload.connections[0]
	sample := workload.doJob(t.Context(), 0, connection, time.Now())
	if sample.Err == nil || sample.Ack != nil || !seamlessScaleIsIncompleteIO(sample.Err) {
		t.Fatalf("ambiguous write sample was accepted: %+v", sample)
	}
	if opened.Load() != 1 || connection.sql == nil || connection.sql == old {
		t.Fatalf("incomplete response did not install a fresh socket: opened=%d current=%p old=%p err=%v",
			opened.Load(), connection.sql, old, sample.Err)
	}
	select {
	case query := <-oldQuery:
		if !strings.HasPrefix(query, "INSERT INTO scale_alpha ") {
			t.Fatalf("unexpected original ambiguous operation: %q", query[:min(len(query), 100)])
		}
	case <-time.After(time.Second):
		t.Fatal("server did not receive original write")
	}
	if _, err := fusedDDLWireQuery(t.Context(), connection.sql, "SELECT 1", false); err != nil {
		t.Fatalf("next request did not succeed on replacement socket: %v", err)
	}
	select {
	case query := <-queries:
		if query != "SELECT 1" {
			t.Fatalf("replacement socket replayed ambiguous write instead of next request: %q", query)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement socket did not receive next request")
	}
}

func TestSeamlessScaleGatewayReadResetsIncompleteStreamForNextJob(t *testing.T) {
	old, oldPeer := net.Pipe()
	defer oldPeer.Close()
	var oldRequests atomic.Uint32
	go func() {
		reader := bufio.NewReader(oldPeer)
		if _, err := reader.ReadString('\n'); err == nil {
			oldRequests.Add(1)
		}
	}()
	var opened atomic.Uint32
	var peersMu sync.Mutex
	var peers []net.Conn
	openGate := func(context.Context) (net.Conn, error) {
		client, peer := net.Pipe()
		peersMu.Lock()
		peers = append(peers, peer)
		peersMu.Unlock()
		opened.Add(1)
		row := seamlessScaleAck{Table: "scale_alpha", ID: "seed-scale_alpha-0001", Value: 7, Marker: "marker"}
		go func() {
			reader := bufio.NewReader(peer)
			if _, err := reader.ReadString('\n'); err == nil {
				_, _ = io.WriteString(peer, seamlessScaleTestGatewayResponse(row))
			}
		}()
		return client, nil
	}
	workload := &seamlessScaleWorkload{}
	workload.connections = append(workload.connections, seamlessScaleConnection{
		gate: old, reader: bufio.NewReaderSize(old, 64<<10), openGate: openGate,
	})
	connection := &workload.connections[0]
	t.Cleanup(func() {
		if connection.gate != nil {
			_ = connection.gate.Close()
		}
		peersMu.Lock()
		defer peersMu.Unlock()
		for _, peer := range peers {
			_ = peer.Close()
		}
	})
	row := seamlessScaleAck{Table: "scale_alpha", ID: "seed-scale_alpha-0001", Value: 7, Marker: "marker"}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	_, retries, firstErr := workload.gatewayRead(ctx, connection, row)
	cancel()
	if !seamlessScaleIsTimeout(firstErr) || retries != 0 {
		t.Fatalf("incomplete first request was retried or hidden: retries=%d err=%v", retries, firstErr)
	}
	if opened.Load() != 1 || connection.gate == old {
		t.Fatalf("first error did not replace native stream: opened=%d gate=%p old=%p", opened.Load(), connection.gate, old)
	}
	if got := oldRequests.Load(); got != 1 {
		t.Fatalf("first read sent %d requests over the old stream, want exactly one", got)
	}
	if _, retries, err := workload.gatewayRead(t.Context(), connection, row); err != nil || retries != 0 {
		t.Fatalf("next native request failed on replacement stream: retries=%d err=%v", retries, err)
	}
	if got := oldRequests.Load(); got != 1 || opened.Load() != 1 {
		t.Fatalf("stream recovery retried an old request: old_requests=%d reopened=%d", got, opened.Load())
	}
}

func readSeamlessScaleSimpleQuery(connection net.Conn) (string, error) {
	var header [5]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return "", err
	}
	if header[0] != 'Q' {
		return "", fmt.Errorf("unexpected PostgreSQL frontend message %q", header[0])
	}
	length := int(binary.BigEndian.Uint32(header[1:])) - 4
	if length < 1 || length > 1<<20 {
		return "", fmt.Errorf("invalid PostgreSQL query length %d", length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(connection, payload); err != nil {
		return "", err
	}
	return string(bytes.TrimSuffix(payload, []byte{0})), nil
}

func writeSeamlessScaleCommandComplete(connection net.Conn, tag string) error {
	payload := append([]byte(tag), 0)
	frame := []byte{'C'}
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)+4))
	frame = append(frame, payload...)
	frame = append(frame, 'Z', 0, 0, 0, 5, 'I')
	_, err := connection.Write(frame)
	return err
}

func seamlessScaleTestGatewayResponse(row seamlessScaleAck) string {
	return fmt.Sprintf(`{"ok":true,"found":[true],"documents":[{"id":%q,"value":%d,"marker":%q}],"observations":[{"applied":1,"topology_recovery_epoch":1,"cluster_id":"%032x","cluster_incarnation":"%032x","shard_incarnation":"%032x","group_id":"%032x","route_id":"%064x"}]}`+"\n",
		row.ID, row.Value, row.Marker, 1, 2, 3, 4, 5)
}

func TestSeamlessScaleInFlightWatchdogCapturesOnceBeforeDeadline(t *testing.T) {
	calls := make([]seamlessScaleWorkerCall, 2)
	calls[1].stage.Store(uint32(seamlessScaleCallNativeRead))
	calls[1].startedNS.Store(time.Now().Add(-3 * time.Second).UnixNano())
	calls[1].sequence.Store(77)
	var active atomic.Bool
	var cycle atomic.Uint32
	cycle.Store(2)
	var captures atomic.Uint32
	captured := make(chan seamlessScaleStall, 1)
	workload := &seamlessScaleWorkload{acknowledged: make(map[string]seamlessScaleAck)}
	samples := []seamlessScaleSample{{Scheduled: time.Now(), Started: time.Now(), Completed: time.Now()}}
	before := workload.phaseEvidence(seamlessScalePhaseDuring, samples[0].Scheduled, samples[0].Completed.Add(time.Second), 1, 0, samples)
	stop := startSeamlessScaleStallWatchdog(t.Context(), &active, &cycle, calls,
		time.Millisecond, 2*time.Second, func(stall seamlessScaleStall) {
			captures.Add(1)
			captured <- stall
		})
	time.Sleep(10 * time.Millisecond)
	if got := captures.Load(); got != 0 {
		t.Fatalf("watchdog captured while the during phase was inactive: %d", got)
	}
	active.Store(true)
	select {
	case stall := <-captured:
		if stall.Worker != 1 || stall.Stage != seamlessScaleCallNativeRead || stall.Sequence != 77 || stall.Cycle != 2 {
			t.Fatalf("unexpected captured call: %+v", stall)
		}
		if stall.Age < 2*time.Second || stall.Age >= 10*time.Second {
			t.Fatalf("capture age=%s is not before the 10s call deadline", stall.Age)
		}
	case <-time.After(time.Second):
		t.Fatal("watchdog did not capture the overdue call")
	}
	active.Store(false)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := captures.Load(); got != 1 {
		t.Fatalf("capture count=%d want 1", got)
	}
	after := workload.phaseEvidence(seamlessScalePhaseDuring, samples[0].Scheduled, samples[0].Completed.Add(time.Second), 1, 0, samples)
	if after.Scheduled != before.Scheduled || after.Completed != before.Completed || len(samples) != int(before.Scheduled) {
		t.Fatalf("watchdog changed the measured sample denominator: before=%d/%d after=%d/%d samples=%d",
			before.Scheduled, before.Completed, after.Scheduled, after.Completed, len(samples))
	}
}

func TestSeamlessScaleSQLRecoveryUsesElapsedDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		calls := 0
		result, retries, err := retrySeamlessScaleSQL(t.Context(), false, func(context.Context) (fusedPGResult, error) {
			calls++
			if time.Since(started) < time.Second {
				return fusedPGResult{code: "40001", message: "transaction aborted"}, nil
			}
			return fusedPGResult{tag: "INSERT 0 1"}, nil
		})
		if err != nil || result.tag != "INSERT 0 1" || retries < 4 || calls != int(retries)+1 {
			t.Fatalf("result=%+v retries=%d calls=%d err=%v", result, retries, calls, err)
		}
	})
}

func TestSeamlessScaleSQLNeverReplaysAmbiguousOrPermanentWrites(t *testing.T) {
	for _, response := range []fusedPGResult{
		{code: "40003", message: "write outcome unknown: no reachable leader"},
		{code: "XX000", message: "no reachable leader"},
		{code: "42501", message: "unauthorized"},
		{code: "23505", message: "duplicate key"},
	} {
		calls := 0
		result, retries, err := retrySeamlessScaleSQL(t.Context(), false, func(context.Context) (fusedPGResult, error) { calls++; return response, nil })
		if err != nil || result.code != response.code || retries != 0 || calls != 1 {
			t.Fatalf("response=%+v retries=%d calls=%d err=%v", response, retries, calls, err)
		}
	}
	for _, readOnly := range []bool{false, true} {
		calls := 0
		_, retries, err := retrySeamlessScaleSQL(t.Context(), readOnly, func(context.Context) (fusedPGResult, error) { calls++; return fusedPGResult{}, io.EOF })
		if !errors.Is(err, io.EOF) || retries != 0 || calls != 1 {
			t.Fatal("transport error retried")
		}
	}
}

func TestSeamlessScaleSQLDoesNotRetryDirectIndexConflict(t *testing.T) {
	response := fusedPGResult{code: "40001", message: "vibedb: transaction conflict: VIBEDB_RF3_DIRECT_ABORT result_code=11"}
	if seamlessScaleSQLRetryable(response, false) || seamlessScaleSQLRetryable(response, true) {
		t.Fatal("direct IndexConflict was classified as a retryable serialization abort")
	}
	if !seamlessScaleSQLRetryable(fusedPGResult{code: "40001", message: "transaction serialization failure"}, false) {
		t.Fatal("ordinary complete serialization abort lost its retry path")
	}
	calls := 0
	result, retries, err := retrySeamlessScaleSQL(t.Context(), false, func(context.Context) (fusedPGResult, error) {
		calls++
		return response, nil
	})
	if err != nil || result.code != "40001" || retries != 0 || calls != 1 {
		t.Fatalf("result=%+v retries=%d calls=%d err=%v", result, retries, calls, err)
	}
}

func TestSeamlessScaleSQLRetryStopsAtDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		_, retries, err := retrySeamlessScaleSQL(t.Context(), true, func(context.Context) (fusedPGResult, error) {
			return fusedPGResult{code: "XX000", message: "no reachable leader"}, nil
		})
		if !errors.Is(err, context.DeadlineExceeded) || retries == 0 || time.Since(started) != seamlessScaleRecoveryBudget {
			t.Fatalf("elapsed=%s retries=%d err=%v", time.Since(started), retries, err)
		}
	})
}

func TestSeamlessScaleFaultWindowsPreserveEverySampleAndSteadyContinuity(t *testing.T) {
	start := time.Unix(100, 0)
	workload := &seamlessScaleWorkload{history: make(map[string][]seamlessScaleWindow)}
	for i := range 5 {
		from := start.Add(time.Duration(i) * 10 * time.Second)
		samples := []seamlessScaleSample{
			{Scheduled: from, Started: from.Add(time.Millisecond), Completed: from.Add(2 * time.Millisecond), Latency: 2 * time.Millisecond, QueueLag: time.Millisecond},
			{Scheduled: from.Add(time.Second), Started: from.Add(time.Second + time.Millisecond), Completed: from.Add(time.Second + 2*time.Millisecond), Latency: 2 * time.Millisecond, QueueLag: time.Millisecond},
		}
		evidence := workload.phaseEvidence(seamlessScalePhaseDuring, from, from.Add(10*time.Second), 2, 0, samples)
		workload.history[seamlessScalePhaseDuring] = append(workload.history[seamlessScalePhaseDuring], seamlessScaleWindow{evidence: evidence, samples: samples})
	}
	workload.MarkFault(start.Add(11 * time.Second))
	steady, recovery, normalWindows, faultWindows, injections := workload.FaultTimingEvidence()
	// Continuity is the largest gap inside any one window (1s here). The 9s
	// between adjacent windows is the harness draining one window before
	// scheduling the next, and the gap across the excluded recovery windows
	// belongs to no steady window; neither may be reported as a pause.
	if normalWindows != 3 || faultWindows != 2 || injections != 1 || steady.Scheduled != 6 || recovery.Scheduled != 4 ||
		steady.DurationNS != uint64(30*time.Second) || recovery.DurationNS != uint64(20*time.Second) ||
		steady.CompletionGapNS != uint64(time.Second) || steady.MaxPauseNS != uint64(time.Second) ||
		recovery.CompletionGapNS != uint64(time.Second) {
		t.Fatalf("lost sample or fabricated gap: steady=%+v recovery=%+v windows=%d/%d injections=%d", steady, recovery, normalWindows, faultWindows, injections)
	}
	// A real stall inside a steady window is still reported in full.
	stalled := workload.history[seamlessScalePhaseDuring][4]
	stalled.samples = append(stalled.samples, seamlessScaleSample{
		Scheduled: time.Unix(141, 0), Started: time.Unix(141, 0), Completed: time.Unix(147, 0),
		Latency: 6 * time.Second,
	})
	stalled.evidence = workload.phaseEvidence(seamlessScalePhaseDuring, time.Unix(140, 0), time.Unix(150, 0), 3, 0, stalled.samples)
	workload.history[seamlessScalePhaseDuring][4] = stalled
	steady, _, _, _, _ = workload.FaultTimingEvidence()
	if steady.MaxPauseNS < uint64(5*time.Second) {
		t.Fatalf("intra-window stall hidden: steady=%+v", steady)
	}
}
