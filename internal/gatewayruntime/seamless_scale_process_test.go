//go:build linux

package gatewayruntime

// This is the shipped-command qualification for an online physical-node
// scale cycle.  It intentionally uses the real vibedb and vibedb-shard
// binaries, the authenticated cluster-control CLI, and the PostgreSQL/native
// clients.  The only test-local code is measurement and exact conservation
// checking; no in-memory placement or fake control endpoint can satisfy the
// gate.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/clustercontrol"
	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibejson"
)

const (
	seamlessScaleProcessEnvironment      = "VIBEDB_SEAMLESS_SCALE_E2E"
	seamlessScaleEvidenceEnvironment     = "VIBEDB_SEAMLESS_SCALE_EVIDENCE"
	seamlessScaleDiagnosticStacksEnv     = "VIBEDB_RF3_DIAGNOSTIC_STACKS"
	seamlessScaleDiagnosticAbortEnv      = "VIBEDB_RF3_DIAGNOSTIC_ABORT_REASON"
	seamlessScaleDiagnosticAbortTableEnv = "VIBEDB_RF3_DIAGNOSTIC_ABORT_TABLE"
	// When set, a failed qualification moves its temporary state directory to
	// this path after all process cleanup has run. It is intentionally opt-in
	// because the directory contains private test credentials and catalogs.
	seamlessScaleFailureEnvironment = "VIBEDB_SEAMLESS_SCALE_FAILURE"
	seamlessScaleWindowDuration     = 10 * time.Second
	seamlessScaleWatchdogInterval   = 100 * time.Millisecond
	seamlessScaleWatchdogThreshold  = 2 * time.Second
	seamlessScaleMinimumSamples     = 10_000
	seamlessScaleOfferedRate        = 1_200
	// A calibration candidate must complete at least 97% of its offered load
	// with a bounded arrival queue to count as sustained. The bounded queue is
	// the overload test; the throughput floor tolerates the final drain, which
	// the measured span includes, on a noisy shared runner.
	seamlessScaleSustainedThroughputPPM = 970_000
	seamlessScaleSustainedQueueLag      = 500 * time.Millisecond
	seamlessScaleWorkloadConnections    = 16
	seamlessScaleOperationWait          = 750 * time.Millisecond
	seamlessScaleRecoveryBudget         = 10 * time.Second
	// The evidence split requires at least this many during windows fully
	// outside every fault shadow. The actor drains to it before stopping so
	// a fast run cannot cover all windows with recovery intervals.
	seamlessScaleMinimumSteadyWindows = 3
	// Worst case the last fault shadow plus three windows must still clear
	// after the cycles finish. The drain gives up well inside the job
	// timeout and lets the evidence validation fail loudly instead.
	seamlessScaleSteadyDrainTimeout = 90 * time.Second
	// cluster dev gives node zero the only autonomous topology controller.
	// Keep the controller and the long-lived survivor gateway running while
	// this qualification retires each physical node in turn.
	seamlessScaleControllerIndex    = 0
	seamlessScaleSurvivorIndex      = 1
	seamlessScaleFirstRetiringIndex = 2
)

var seamlessScaleTables = []string{"scale_alpha", "scale_beta", "scale_gamma"}

// seamlessScaleClusterManifest intentionally decodes only the stable public
// fields needed by the fixture.  New fields in a shipped manifest are
// accepted, while required identity/path fields remain checked below.
type seamlessScaleClusterManifest struct {
	Format              uint16                      `json:"format"`
	Nodes               uint8                       `json:"nodes"`
	Replicas            uint8                       `json:"replicas"`
	PhysicalNodes       uint8                       `json:"physical_nodes"`
	ClientEndpoint      string                      `json:"client_endpoint"`
	ClientCertificate   string                      `json:"client_certificate"`
	ClientKey           string                      `json:"client_key"`
	ClientNode          string                      `json:"client_node"`
	GatewayNode         string                      `json:"gateway_node"`
	Roots               string                      `json:"roots"`
	NodeManifests       []seamlessScalePhysicalNode `json:"node_manifests"`
	CatalogPath         string                      `json:"catalog_path"`
	AuthorizationPolicy string                      `json:"authorization_policy"`
}

type seamlessScalePhysicalNode struct {
	Node                  string   `json:"node"`
	GatewayNode           string   `json:"gateway_node"`
	FrontendListen        string   `json:"frontend_listen"`
	GatewayControl        string   `json:"gateway_control"`
	ServeManifest         string   `json:"serve_manifest"`
	CatalogSessionJournal string   `json:"catalog_session_journal"`
	Groups                []string `json:"groups"`
}

type seamlessScaleTarget struct {
	NodeID       rafttransport.NodeID
	Incarnation  uint64
	Certificate  string
	Key          string
	Manifest     string
	Descriptor   string
	Public       clustercontrol.NodeDescriptor
	PreparedRoot string
}

const seamlessScaleIdentityOID = "1.3.6.1.4.1.32473.1.1"

type seamlessScaleWorkload struct {
	survivorSQL            net.Conn
	survivorGate           net.Conn
	connections            []seamlessScaleConnection
	workerCalls            []seamlessScaleWorkerCall
	duringActive           atomic.Bool
	currentCycle           atomic.Uint32
	firstIOFailureCaptured [4]atomic.Bool
	firstIOFailureWG       sync.WaitGroup
	captureFirstIOFailure  func(uint32, seamlessScaleSample)
	failedWindowSequence   atomic.Uint64
	mu                     sync.Mutex
	historyMu              sync.Mutex
	faultStarts            []time.Time
	history                map[string][]seamlessScaleWindow
	sequence               uint64
	seedRows               []seamlessScaleAck
	seed                   map[string]seamlessScaleAck
	acknowledged           map[string]seamlessScaleAck
	sqlRequests            uint64
	gateRequests           uint64
	reportWindow           func(seamlessScalePhaseEvidence, []seamlessScaleSample)
}

type seamlessScaleConnection struct {
	sql      net.Conn
	gate     net.Conn
	reader   *bufio.Reader
	openSQL  func(context.Context) (net.Conn, error)
	openGate func(context.Context) (net.Conn, error)
}

type seamlessScaleAck struct {
	Table  string
	ID     string
	Value  int
	Marker string
}

type seamlessScaleSample struct {
	Scheduled time.Time
	Started   time.Time
	Completed time.Time
	Worker    int
	Sequence  uint64
	Stage     seamlessScaleCallStage
	Latency   time.Duration
	QueueLag  time.Duration
	Ack       *seamlessScaleAck
	Verified  *seamlessScaleAck
	Retries   uint64
	Err       error
}

type seamlessScaleCallStage uint32

const (
	seamlessScaleCallIdle seamlessScaleCallStage = iota
	seamlessScaleCallSQLWrite
	seamlessScaleCallNativeRead
	seamlessScaleCallSQLRead
)

func (stage seamlessScaleCallStage) String() string {
	switch stage {
	case seamlessScaleCallSQLWrite:
		return "sql_write"
	case seamlessScaleCallNativeRead:
		return "native_read"
	case seamlessScaleCallSQLRead:
		return "sql_read"
	default:
		return "idle"
	}
}

type seamlessScaleWorkerCall struct {
	stage     atomic.Uint32
	startedNS atomic.Int64
	sequence  atomic.Uint64
}

type seamlessScaleStall struct {
	Worker    int                    `json:"worker"`
	Stage     seamlessScaleCallStage `json:"-"`
	StageName string                 `json:"stage"`
	Sequence  uint64                 `json:"sequence"`
	StartedAt time.Time              `json:"started_at"`
	Age       time.Duration          `json:"age_ns"`
	Cycle     uint32                 `json:"cycle"`
}

func findSeamlessScaleStall(calls []seamlessScaleWorkerCall, now time.Time, threshold time.Duration) (seamlessScaleStall, bool) {
	for worker := range calls {
		stage := seamlessScaleCallStage(calls[worker].stage.Load())
		if stage == seamlessScaleCallIdle {
			continue
		}
		started := time.Unix(0, calls[worker].startedNS.Load())
		age := now.Sub(started)
		if age < threshold {
			continue
		}
		return seamlessScaleStall{Worker: worker, Stage: stage, StageName: stage.String(),
			Sequence: calls[worker].sequence.Load(), StartedAt: started, Age: age}, true
	}
	return seamlessScaleStall{}, false
}

func startSeamlessScaleStallWatchdog(
	ctx context.Context,
	active *atomic.Bool,
	cycle *atomic.Uint32,
	calls []seamlessScaleWorkerCall,
	interval, threshold time.Duration,
	capture func(seamlessScaleStall),
) func() error {
	watchCtx, cancel := context.WithCancel(ctx)
	loopDone := make(chan struct{})
	var captures sync.WaitGroup
	var capturedCycle [4]atomic.Bool
	go func() {
		defer close(loopDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case now := <-ticker.C:
				if active == nil || !active.Load() {
					continue
				}
				stall, found := findSeamlessScaleStall(calls, now, threshold)
				if !found || cycle == nil {
					continue
				}
				stall.Cycle = cycle.Load()
				if stall.Cycle == 0 || int(stall.Cycle) >= len(capturedCycle) ||
					!capturedCycle[stall.Cycle].CompareAndSwap(false, true) {
					continue
				}
				captures.Add(1)
				go func(stall seamlessScaleStall) {
					defer captures.Done()
					capture(stall)
				}(stall)
			}
		}
	}()
	return func() error {
		cancel()
		<-loopDone
		done := make(chan struct{})
		go func() { captures.Wait(); close(done) }()
		select {
		case <-done:
			return nil
		case <-time.After(8 * time.Second):
			return errors.New("per-cycle in-flight stall captures did not finish within 8s")
		}
	}
}

type seamlessScaleWatchdogProcessState struct {
	Index            int    `json:"index"`
	PID              int    `json:"pid,omitempty"`
	Alive            bool   `json:"alive"`
	DiagnosticSerial uint64 `json:"diagnostic_serial,omitempty"`
}

func snapshotSeamlessScaleWatchdogProcesses(processes []*seamlessScaleNodeProcess) []seamlessScaleWatchdogProcessState {
	states := make([]seamlessScaleWatchdogProcessState, 0, len(processes))
	seen := make(map[*seamlessScaleNodeProcess]struct{}, len(processes))
	for index, process := range processes {
		if process == nil {
			continue
		}
		if _, ok := seen[process]; ok {
			continue
		}
		seen[process] = struct{}{}
		command, exited, diagnostic := process.runtimeSnapshot()
		state := seamlessScaleWatchdogProcessState{Index: index}
		if command != nil && command.Process != nil {
			state.PID = command.Process.Pid
		}
		if exited != nil {
			select {
			case <-exited:
			default:
				state.Alive = true
			}
		}
		if diagnostic != nil {
			state.DiagnosticSerial, _, _ = latestSeamlessScaleRaftSnapshot(process, "VIBEDB_RF3_DIAGNOSTIC ")
		}
		states = append(states, state)
	}
	return states
}

type seamlessScaleWindow struct {
	evidence seamlessScalePhaseEvidence
	samples  []seamlessScaleSample
}

func newSeamlessScaleWorkload(t *testing.T, ctx context.Context, address string, survivorSQL, survivorGateway net.Conn,
	openGateway func(context.Context) (net.Conn, error),
) *seamlessScaleWorkload {
	t.Helper()
	openSQL := func(openCtx context.Context) (net.Conn, error) {
		return fusedOpenDDLWire(openCtx, address)
	}
	workload := &seamlessScaleWorkload{survivorSQL: survivorSQL, survivorGate: survivorGateway,
		history: make(map[string][]seamlessScaleWindow), seed: make(map[string]seamlessScaleAck),
		acknowledged: make(map[string]seamlessScaleAck)}
	t.Cleanup(workload.Close)
	workload.connections = append(workload.connections, seamlessScaleConnection{
		sql: survivorSQL, gate: survivorGateway, reader: bufio.NewReaderSize(survivorGateway, 64<<10),
		openSQL: openSQL, openGate: openGateway,
	})
	for index := 1; index < seamlessScaleWorkloadConnections; index++ {
		connection, err := openSQL(ctx)
		if err != nil {
			t.Fatalf("open scale workload SQL connection %d: %v", index+1, err)
		}
		gate, err := openGateway(ctx)
		if err != nil {
			_ = connection.Close()
			t.Fatalf("open scale workload native connection %d: %v", index+1, err)
		}
		workload.connections = append(workload.connections, seamlessScaleConnection{
			sql: connection, gate: gate, reader: bufio.NewReaderSize(gate, 64<<10),
			openSQL: openSQL, openGate: openGateway,
		})
	}
	workload.workerCalls = make([]seamlessScaleWorkerCall, len(workload.connections))
	return workload
}

func (workload *seamlessScaleWorkload) Close() {
	if workload == nil {
		return
	}
	for _, connection := range workload.connections {
		if connection.sql != nil {
			_ = connection.sql.Close()
		}
		if connection.gate != nil {
			_ = connection.gate.Close()
		}
	}
}

func (connection *seamlessScaleConnection) openSQLConnection(ctx context.Context) error {
	if connection == nil || connection.openSQL == nil || ctx == nil {
		return errors.New("scale workload SQL reconnect is not configured")
	}
	if connection.sql != nil {
		return nil
	}
	sql, err := connection.openSQL(ctx)
	if err != nil {
		return err
	}
	connection.sql = sql
	return nil
}

func (connection *seamlessScaleConnection) refreshSQLAfterIncompleteResponse(ctx context.Context) error {
	if connection == nil || ctx == nil || connection.openSQL == nil {
		return errors.New("scale workload SQL reconnect is not configured")
	}
	if connection.sql != nil {
		_ = connection.sql.Close()
		connection.sql = nil
	}
	openCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	return connection.openSQLConnection(openCtx)
}

func (connection *seamlessScaleConnection) openGatewayConnection(ctx context.Context) error {
	if connection == nil || connection.openGate == nil || ctx == nil {
		return errors.New("scale workload gateway reconnect is not configured")
	}
	if connection.gate != nil {
		return nil
	}
	gate, err := connection.openGate(ctx)
	if err != nil {
		return err
	}
	connection.gate = gate
	connection.reader = bufio.NewReaderSize(gate, 64<<10)
	return nil
}

func (connection *seamlessScaleConnection) refreshGatewayAfterIncompleteResponse(ctx context.Context) error {
	if connection == nil || ctx == nil || connection.openGate == nil {
		return errors.New("scale workload gateway reconnect is not configured")
	}
	if connection.gate != nil {
		_ = connection.gate.Close()
		connection.gate = nil
		connection.reader = nil
	}
	openCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	return connection.openGatewayConnection(openCtx)
}

func (workload *seamlessScaleWorkload) refreshSQLAfterIncompleteResponse(
	ctx context.Context, connection *seamlessScaleConnection,
) error {
	err := connection.refreshSQLAfterIncompleteResponse(ctx)
	if len(workload.connections) != 0 && connection == &workload.connections[0] && connection.sql != nil {
		workload.survivorSQL = connection.sql
	}
	return err
}

func (workload *seamlessScaleWorkload) refreshGatewayAfterIncompleteResponse(
	ctx context.Context, connection *seamlessScaleConnection,
) error {
	err := connection.refreshGatewayAfterIncompleteResponse(ctx)
	if len(workload.connections) != 0 && connection == &workload.connections[0] && connection.gate != nil {
		workload.mu.Lock()
		workload.survivorGate = connection.gate
		workload.mu.Unlock()
	}
	return err
}

func (workload *seamlessScaleWorkload) Seed(t *testing.T, ctx context.Context) error {
	t.Helper()
	for tableIndex, table := range seamlessScaleTables {
		for rowIndex := 0; rowIndex < 512; rowIndex++ {
			row := seamlessScaleAck{Table: table, ID: fmt.Sprintf("seed-%s-%04d", table, rowIndex),
				Value: 10_000 + tableIndex*10_000 + rowIndex, Marker: seamlessScalePayload(table, rowIndex)}
			query := fmt.Sprintf("INSERT INTO %s (id,value,marker) VALUES ('%s',%d,'%s')", table, row.ID, row.Value, row.Marker)
			result, err := fusedDDLWireQuery(ctx, workload.survivorSQL, query, false)
			if err != nil {
				return fmt.Errorf("seed %s/%s: %w", table, row.ID, err)
			}
			if result.code != "" || result.tag != "INSERT 0 1" {
				return fmt.Errorf("seed %s/%s was not acknowledged: %+v", table, row.ID, result)
			}
			key := seamlessScaleAckKey(row)
			workload.mu.Lock()
			workload.seed[key] = row
			workload.seedRows = append(workload.seedRows, row)
			workload.acknowledged[key] = row
			workload.mu.Unlock()
		}
	}
	return nil
}

// seamlessScalePayload is a 1 KiB deterministic marker. It keeps the seeded
// image (3 tables x 512 rows) well above the migration burst, so every move is
// paced, while bounding growth from ~20% inserts to ~200 KiB/s at 1000/s. An
// 8 KiB marker grew the dataset by gigabytes over a run, making each later
// cycle migrate far more than the first on shared CI hardware.
func seamlessScalePayload(table string, index int) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("vibedb-scale/%s/%d", table, index)))
	return strings.Repeat(hex.EncodeToString(digest[:]), 16)
}

// Window drives fixed open-loop arrivals.  A bounded queue turns scheduler
// overload into an explicit missed sample, which is rejected by the strict
// evidence contract instead of silently shortening the denominator.
func (workload *seamlessScaleWorkload) Window(ctx context.Context, phase string, duration time.Duration, rate int) seamlessScalePhaseEvidence {
	if rate <= 0 || duration <= 0 {
		return seamlessScalePhaseEvidence{Phase: phase}
	}
	during := phase == seamlessScalePhaseDuring
	if during {
		workload.duringActive.Store(true)
		defer workload.duringActive.Store(false)
	}
	start := time.Now()
	deadline := start.Add(duration)
	interval := time.Second / time.Duration(rate)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	// Preserve arrivals through the explicit fault recovery budget. This is
	// bounded timestamp storage (at1200/s,12000 entries), not dropped load.
	jobs := make(chan time.Time, rate*int(seamlessScaleRecoveryBudget/time.Second))
	results := make(chan seamlessScaleSample, rate*2)
	var workers sync.WaitGroup
	for index := range workload.connections {
		workers.Add(1)
		go func(worker int, connection *seamlessScaleConnection) {
			defer workers.Done()
			for scheduled := range jobs {
				results <- workload.doJob(ctx, worker, connection, scheduled)
			}
		}(index, &workload.connections[index])
	}
	var scheduled, missed uint64
	ticker := time.NewTicker(interval)
	samples := make([]seamlessScaleSample, 0, int(rate*int(duration/time.Second))+1)
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		for sample := range results {
			samples = append(samples, sample)
		}
	}()
	for next := start; ; {
		if next.After(deadline) {
			break
		}
		if !next.After(time.Now()) {
			select {
			case jobs <- next:
				scheduled++
			default:
				missed++
			}
			next = next.Add(interval)
			continue
		}
		select {
		case <-ctx.Done():
			missed++
			next = deadline.Add(interval)
		case <-ticker.C:
		}
	}
	ticker.Stop()
	close(jobs)
	workers.Wait()
	close(results)
	<-collectorDone
	end := time.Now()
	if end.Before(deadline) {
		end = deadline
	}
	evidence := workload.phaseEvidence(phase, start, end, scheduled, missed, samples)
	workload.historyMu.Lock()
	workload.history[phase] = append(workload.history[phase], seamlessScaleWindow{evidence: evidence, samples: append([]seamlessScaleSample(nil), samples...)})
	workload.historyMu.Unlock()
	if workload.reportWindow != nil {
		workload.reportWindow(evidence, samples)
	}
	return evidence
}

// WindowSet returns one phase over a sequence of independently measured
// windows. The returned span is the real wall-clock span that contains those
// windows, and all samples remain assigned by scheduled arrival time.
func (workload *seamlessScaleWorkload) WindowSet(ctx context.Context, phase string, count int, duration time.Duration, rate int) seamlessScalePhaseEvidence {
	if count <= 0 {
		return seamlessScalePhaseEvidence{Phase: phase}
	}
	var start, end time.Time
	var scheduled, missed, gap uint64
	var samples []seamlessScaleSample
	for index := 0; index < count; index++ {
		window := workload.Window(ctx, phase, duration, rate)
		gap = max(gap, window.CompletionGapNS)
		if start.IsZero() || window.StartNS < uint64(start.UnixNano()) {
			start = time.Unix(0, int64(window.StartNS))
		}
		windowEnd := time.Unix(0, int64(window.EndNS))
		if windowEnd.After(end) {
			end = windowEnd
		}
		scheduled += window.Scheduled
		missed += window.Missed
		workload.historyMu.Lock()
		entries := workload.history[phase]
		if len(entries) != 0 {
			samples = append(samples, entries[len(entries)-1].samples...)
		}
		workload.historyMu.Unlock()
	}
	if start.IsZero() || end.IsZero() {
		return seamlessScalePhaseEvidence{Phase: phase}
	}
	return withinWindowContinuity(workload.phaseEvidence(phase, start, end, scheduled, missed, samples), gap)
}

// withinWindowContinuity replaces a merged phase's completion gap with the
// largest gap observed inside any one window. Each window stops arrivals at
// its deadline and drains in-flight work before the next window schedules,
// so the gap across a window boundary is roughly one request latency created
// by the harness itself, not a pause of the system under test.
func withinWindowContinuity(evidence seamlessScalePhaseEvidence, gap uint64) seamlessScalePhaseEvidence {
	if gap == 0 && evidence.Completed > 1 {
		gap = 1
	}
	evidence.MaxPauseNS, evidence.CompletionGapNS = gap, gap
	return evidence
}

// WindowUntil keeps the same open-loop actor active across an operation wave.
// The stop signal is observed only between complete windows, so each retained
// sample has a real scheduled-arrival interval and the final window cannot be
// silently truncated at a topology event.
func (workload *seamlessScaleWorkload) WindowUntil(ctx context.Context, phase string, duration time.Duration, rate int, stop <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		default:
		}
		workload.Window(ctx, phase, duration, rate)
	}
}

// calibrateSeamlessScaleRate selects a sustained rate using the complete five
// baseline windows. The chosen candidate's entire measurement is the baseline;
// rejected candidates remain in history and no windows are discarded from it.
//
// A candidate is sustained only when the cluster completes essentially all of
// the offered load without a growing arrival queue. An open-loop actor that
// offers more than capacity accepts every request yet queues them, so every
// later latency and continuity measurement would describe the backlog rather
// than the system (coordinated omission). Candidates descend until one holds.
// Every candidate keeps each phase above seamlessScaleMinimumSamples.
var seamlessScaleCalibrationRates = []int{seamlessScaleOfferedRate, 1_000, 800, 600, 400}

func calibrateSeamlessScaleRate(t *testing.T, workload *seamlessScaleWorkload, ctx context.Context) (int, seamlessScalePhaseEvidence) {
	t.Helper()
	for _, candidate := range seamlessScaleCalibrationRates {
		phase := fmt.Sprintf("calibration-%d", candidate)
		probe := workload.WindowSet(ctx, phase, 5, seamlessScaleWindowDuration, candidate)
		t.Logf("scale sustained calibration rate=%d windows=5 scheduled=%d completed=%d errors=%d missed=%d writes=%d reads=%d p99=%s", candidate, probe.Scheduled, probe.Completed, probe.Errors, probe.Missed, probe.AcknowledgedWrites, probe.VerifiedReads, time.Duration(probe.P99NS))
		if probe.Errors != 0 || probe.Timeouts != 0 {
			// Capacity selection must never conceal an operation/data failure.
			t.Fatalf("scale calibration operation failure: errors=%d timeouts=%d", probe.Errors, probe.Timeouts)
		}
		// Throughput is measured over the whole span including the final
		// drain, so a queue that outgrows the offered rate lowers it.
		sustained := probe.ThroughputMilli*1_000_000 >= uint64(candidate)*1_000*seamlessScaleSustainedThroughputPPM &&
			probe.QueueLagP99NS <= uint64(seamlessScaleSustainedQueueLag)
		t.Logf("scale calibration rate=%d sustained=%t throughput_milli=%d queue_lag_p99=%s max_window_gap=%s",
			candidate, sustained, probe.ThroughputMilli,
			time.Duration(probe.QueueLagP99NS), time.Duration(probe.CompletionGapNS))
		if probe.Scheduled >= uint64(candidate)*uint64(5*seamlessScaleWindowDuration/time.Second) && probe.Started == probe.Scheduled &&
			probe.Completed == probe.Started && probe.Missed == 0 && sustained {
			workload.historyMu.Lock()
			workload.history[seamlessScalePhaseBaseline] = workload.history[phase]
			for index := range workload.history[seamlessScalePhaseBaseline] {
				workload.history[seamlessScalePhaseBaseline][index].evidence.Phase = seamlessScalePhaseBaseline
			}
			delete(workload.history, phase)
			workload.historyMu.Unlock()
			probe.Phase = seamlessScalePhaseBaseline
			return candidate, probe
		}
	}
	t.Fatal("scale workload calibration found no sustained offered rate")
	return 0, seamlessScalePhaseEvidence{}
}

// MarkFault records an injected process failure before issuing the stop. Only
// windows intersecting the following fixed10s recovery interval receive the
// recovery timing budget; normal migration work keeps the original SLOs.
func (workload *seamlessScaleWorkload) MarkFault(start time.Time) {
	workload.historyMu.Lock()
	workload.faultStarts = append(workload.faultStarts, start)
	workload.historyMu.Unlock()
}

func seamlessScaleRecoveryWindow(window seamlessScalePhaseEvidence, starts []time.Time) bool {
	for _, start := range starts {
		if window.StartNS < uint64(start.Add(seamlessScaleRecoveryBudget).UnixNano()) && window.EndNS > uint64(start.UnixNano()) {
			return true
		}
	}
	return false
}

// Timing summaries retain the exact samples and actual duration of disjoint
// steady/recovery windows. Pauses across excluded windows are not data: compute
// continuity within each contiguous run instead of treating exclusion as idle.
func (workload *seamlessScaleWorkload) FaultTimingEvidence() (seamlessScalePhaseEvidence, seamlessScalePhaseEvidence, uint64, uint64, uint64) {
	workload.historyMu.Lock()
	windows := append([]seamlessScaleWindow(nil), workload.history[seamlessScalePhaseDuring]...)
	faults := append([]time.Time(nil), workload.faultStarts...)
	workload.historyMu.Unlock()
	var partitions [2]seamlessScalePhaseEvidence
	var counts [2]uint64
	for class := range 2 {
		var samples []seamlessScaleSample
		var start, end time.Time
		var duration, scheduled, missed, gap uint64
		var previousEnd time.Time
		previousSelected := false
		for _, window := range windows {
			selected := seamlessScaleRecoveryWindow(window.evidence, faults) == (class == 1)
			if !selected {
				previousSelected = false
				continue
			}
			counts[class]++
			wstart, wend := time.Unix(0, int64(window.evidence.StartNS)), time.Unix(0, int64(window.evidence.EndNS))
			if start.IsZero() {
				start = wstart
			}
			end = wend
			duration += uint64(wend.Sub(wstart))
			if previousSelected && wstart.After(previousEnd) {
				duration += uint64(wstart.Sub(previousEnd))
			}
			// Continuity is measured inside each window; see
			// withinWindowContinuity for why boundary gaps are excluded.
			gap = max(gap, window.evidence.CompletionGapNS)
			previousEnd, previousSelected = wend, true
			scheduled += window.evidence.Scheduled
			missed += window.evidence.Missed
			samples = append(samples, window.samples...)
		}
		phase := seamlessScalePhaseSteady
		if class == 1 {
			phase = seamlessScalePhaseRecovery
		}
		if counts[class] == 0 {
			partitions[class].Phase = phase
			continue
		}
		value := workload.phaseEvidence(phase, start, end, scheduled, missed, samples)
		value.DurationNS, value.MaxPauseNS, value.CompletionGapNS = duration, gap, gap
		value.OfferedRateMilli = scheduled * 1_000_000_000_000 / max(uint64(1), duration)
		value.ThroughputMilli = value.Successes * 1_000_000_000_000 / max(uint64(1), duration)
		partitions[class] = value
	}
	return partitions[0], partitions[1], counts[0], counts[1], uint64(len(faults))
}

// drainSteadyDuringCoverage keeps the during actor running until its history
// holds the required steady windows. Fault shadows extend a fixed budget
// past each restart while the during span follows the cycle speed, so a fast
// run can otherwise cover every window with recovery intervals and fail the
// evidence split without any workload failure. Windows starting after the
// last shadow are disjoint from every fault by construction, so the drain
// converges as soon as three of them complete; the timeout only bounds a
// wedged actor and lets validation fail loudly.
func drainSteadyDuringCoverage(t *testing.T, workload *seamlessScaleWorkload, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		_, _, steady, _, _ := workload.FaultTimingEvidence()
		if steady >= seamlessScaleMinimumSteadyWindows {
			t.Logf("scale steady during coverage drained: steady_windows=%d", steady)
			return
		}
		if !time.Now().Before(deadline) {
			t.Logf("scale steady during coverage incomplete after drain: steady_windows=%d", steady)
			return
		}
		time.Sleep(time.Second)
	}
}

func TestSeamlessScaleRecoveryWindowBoundsShadow(t *testing.T) {
	fault := time.Unix(1_790_192_100, 0)
	shadow := seamlessScaleRecoveryBudget
	window := func(start, end time.Time) seamlessScalePhaseEvidence {
		return seamlessScalePhaseEvidence{StartNS: uint64(start.UnixNano()), EndNS: uint64(end.UnixNano())}
	}
	for _, test := range []struct {
		name       string
		start, end time.Time
		recovery   bool
	}{
		{"before shadow", fault.Add(-20 * time.Second), fault.Add(-10 * time.Second), false},
		{"ending at fault start", fault.Add(-10 * time.Second), fault, false},
		{"starting at shadow end", fault.Add(shadow), fault.Add(shadow + seamlessScaleWindowDuration), false},
		{"after shadow", fault.Add(shadow + time.Second), fault.Add(shadow + 11*time.Second), false},
		{"straddling fault start", fault.Add(-time.Second), fault.Add(time.Second), true},
		{"straddling shadow end", fault.Add(shadow - time.Second), fault.Add(shadow + time.Second), true},
		{"inside shadow", fault.Add(time.Second), fault.Add(2 * time.Second), true},
		{"covering shadow", fault.Add(-time.Second), fault.Add(shadow + time.Second), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := seamlessScaleRecoveryWindow(window(test.start, test.end), []time.Time{fault}); got != test.recovery {
				t.Fatalf("recovery=%t want=%t", got, test.recovery)
			}
		})
	}
}

func TestDrainSteadyDuringCoverageWaitsForSteadyWindows(t *testing.T) {
	base := time.Now()
	fault := base.Add(5 * time.Second)
	window := func(start time.Time) seamlessScaleWindow {
		return seamlessScaleWindow{evidence: seamlessScalePhaseEvidence{
			StartNS: uint64(start.UnixNano()),
			EndNS:   uint64(start.Add(seamlessScaleWindowDuration).UnixNano())}}
	}
	workload := &seamlessScaleWorkload{
		history:     map[string][]seamlessScaleWindow{seamlessScalePhaseDuring: {window(base)}},
		faultStarts: []time.Time{fault},
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(50 * time.Millisecond)
		steady := fault.Add(seamlessScaleRecoveryBudget)
		workload.historyMu.Lock()
		for index := range seamlessScaleMinimumSteadyWindows {
			start := steady.Add(time.Duration(index) * seamlessScaleWindowDuration)
			workload.history[seamlessScalePhaseDuring] = append(
				workload.history[seamlessScalePhaseDuring], window(start))
		}
		workload.historyMu.Unlock()
	}()
	drainSteadyDuringCoverage(t, workload, 30*time.Second)
	<-done
	if _, _, steady, _, _ := workload.FaultTimingEvidence(); steady != seamlessScaleMinimumSteadyWindows {
		t.Fatalf("steady_windows=%d want=%d", steady, seamlessScaleMinimumSteadyWindows)
	}
}

func TestDrainSteadyDuringCoverageTimeoutIsBounded(t *testing.T) {
	base := time.Now()
	workload := &seamlessScaleWorkload{
		history: map[string][]seamlessScaleWindow{seamlessScalePhaseDuring: {{
			evidence: seamlessScalePhaseEvidence{
				StartNS: uint64(base.UnixNano()),
				EndNS:   uint64(base.Add(seamlessScaleWindowDuration).UnixNano())}}}},
		faultStarts: []time.Time{base.Add(time.Second)},
	}
	started := time.Now()
	drainSteadyDuringCoverage(t, workload, 20*time.Millisecond)
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("drain took %s", elapsed)
	}
	if _, _, steady, _, _ := workload.FaultTimingEvidence(); steady != 0 {
		t.Fatalf("steady_windows=%d want=0", steady)
	}
}

// HistoryEvidence returns the complete measured span for a phase. It is used
// for the migration phase because its length is determined by the actual
// join/rebalance/decommission wave rather than by a test-local operation
// count. Samples are copied while holding the history lock and then reduced
// outside the lock so the foreground actor never blocks on quantiles.
func (workload *seamlessScaleWorkload) HistoryEvidence(phase string) seamlessScalePhaseEvidence {
	workload.historyMu.Lock()
	entries := append([]seamlessScaleWindow(nil), workload.history[phase]...)
	workload.historyMu.Unlock()
	if len(entries) == 0 {
		return seamlessScalePhaseEvidence{Phase: phase}
	}
	start := time.Unix(0, int64(entries[0].evidence.StartNS))
	end := time.Unix(0, int64(entries[0].evidence.EndNS))
	var scheduled, missed, gap uint64
	var samples []seamlessScaleSample
	for _, entry := range entries {
		gap = max(gap, entry.evidence.CompletionGapNS)
		entryStart := time.Unix(0, int64(entry.evidence.StartNS))
		entryEnd := time.Unix(0, int64(entry.evidence.EndNS))
		if entryStart.Before(start) {
			start = entryStart
		}
		if entryEnd.After(end) {
			end = entryEnd
		}
		scheduled += entry.evidence.Scheduled
		missed += entry.evidence.Missed
		samples = append(samples, entry.samples...)
	}
	return withinWindowContinuity(workload.phaseEvidence(phase, start, end, scheduled, missed, samples), gap)
}

func (workload *seamlessScaleWorkload) doJob(ctx context.Context, worker int, connection *seamlessScaleConnection, scheduled time.Time) seamlessScaleSample {
	sample := seamlessScaleSample{Scheduled: scheduled, Started: time.Now(), Worker: worker}
	workload.mu.Lock()
	sequence := workload.sequence
	workload.sequence++
	workload.mu.Unlock()
	sample.Sequence = sequence
	setWorkerCall := func(stage seamlessScaleCallStage) {
		call := &workload.workerCalls[worker]
		if stage == seamlessScaleCallIdle {
			call.stage.Store(uint32(seamlessScaleCallIdle))
			return
		}
		call.startedNS.Store(time.Now().UnixNano())
		call.sequence.Store(sequence)
		call.stage.Store(uint32(stage))
		sample.Stage = stage
	}
	if sequence%5 == 0 {
		table := seamlessScaleTables[sequence%uint64(len(seamlessScaleTables))]
		rowIndex := int(sequence % 1_000_000)
		row := seamlessScaleAck{Table: table, ID: fmt.Sprintf("live-%s-%012d", table, sequence),
			Value: int(sequence%1_000_000) + 50_000, Marker: seamlessScalePayload(table, rowIndex)}
		query := fmt.Sprintf("INSERT INTO %s (id,value,marker) VALUES ('%s',%d,'%s')", table, row.ID, row.Value, row.Marker)
		if err := connection.openSQLConnection(ctx); err != nil {
			sample.Err = fmt.Errorf("open SQL workload connection: %w", err)
		} else {
			setWorkerCall(seamlessScaleCallSQLWrite)
			result, retries, err := seamlessScaleQueryRetry(ctx, connection.sql, query, false, false)
			setWorkerCall(seamlessScaleCallIdle)
			sample.Retries = retries
			if err != nil {
				if reopenErr := workload.refreshSQLAfterIncompleteResponse(ctx, connection); reopenErr != nil {
					err = errors.Join(err, fmt.Errorf("reopen SQL connection after incomplete response: %w", reopenErr))
				}
			} else if result.code != "" || result.tag != "INSERT 0 1" {
				err = fmt.Errorf("unexpected write acknowledgement: %+v", result)
			}
			if err != nil && result.code == "40001" && os.Getenv(seamlessScaleDiagnosticAbortEnv) == "1" {
				if filter := os.Getenv(seamlessScaleDiagnosticAbortTableEnv); filter == "" || strings.Contains(row.Table, filter) {
					err = errors.Join(err, workload.inspectDirectAbortRow(ctx, connection.sql, row))
				}
			}
			if err != nil {
				queryDigest := sha256.Sum256([]byte(query))
				sample.Err = fmt.Errorf(
					"SQL write table=%s id=%s sequence=%d worker=%d retries=%d query_sha256=%x: %w",
					table, row.ID, sequence, worker, retries, queryDigest, err,
				)
			}
			if err == nil {
				workload.mu.Lock()
				workload.sqlRequests++
				workload.mu.Unlock()
				key := seamlessScaleAckKey(row)
				workload.mu.Lock()
				workload.acknowledged[key] = row
				workload.mu.Unlock()
				sample.Ack = &row
			}
		}
	} else {
		workload.mu.Lock()
		var row seamlessScaleAck
		if len(workload.seedRows) != 0 {
			row = workload.seedRows[sequence%uint64(len(workload.seedRows))]
		}
		workload.mu.Unlock()
		if row.ID == "" {
			sample.Err = errors.New("empty seeded workload")
		} else {
			// Each worker owns persistent SQL and native streams through every
			// topology wave. Do not serialize all foreground readers through one
			// client mutex: that measures a single round trip, not cluster capacity.
			setWorkerCall(seamlessScaleCallNativeRead)
			result, retries, err := workload.gatewayRead(ctx, connection, row)
			setWorkerCall(seamlessScaleCallIdle)
			sample.Retries = retries
			if err == nil {
				workload.mu.Lock()
				workload.gateRequests++
				workload.mu.Unlock()
				sample.Verified = &row
			}
			if err != nil {
				sample.Err = fmt.Errorf("native read: %w", err)
			} else {
				// A gateway read is the foreground read for the sample. Keep one
				// SQL read on the same survivor connection as a second oracle.
				if openErr := connection.openSQLConnection(ctx); openErr != nil {
					err = openErr
				} else {
					setWorkerCall(seamlessScaleCallSQLRead)
					result, retries, err = seamlessScaleQueryRetry(ctx, connection.sql,
						fmt.Sprintf("SELECT id,value,marker FROM %s WHERE id='%s'", row.Table, row.ID), false, true)
					setWorkerCall(seamlessScaleCallIdle)
					if err != nil {
						if reopenErr := workload.refreshSQLAfterIncompleteResponse(ctx, connection); reopenErr != nil {
							err = errors.Join(err, fmt.Errorf("reopen SQL connection after incomplete response: %w", reopenErr))
						}
					}
				}
				sample.Retries += retries
				if err == nil {
					workload.mu.Lock()
					workload.sqlRequests++
					workload.mu.Unlock()
					if !seamlessScaleSQLMatches(result, row) {
						err = fmt.Errorf("unexpected SQL read result: code=%s message=%s rows=%d columns=%v key=%s worker=%d sequence=%d", result.code, result.message, len(result.rows), result.columns, row.ID, worker, sequence)
					}
				}
				if err != nil {
					sample.Err = fmt.Errorf("SQL read: %w", err)
				}
			}
		}
	}
	sample.Completed = time.Now()
	sample.QueueLag = sample.Started.Sub(sample.Scheduled)
	sample.Latency = sample.Completed.Sub(sample.Scheduled)
	if sample.Err != nil && workload.duringActive.Load() && workload.captureFirstIOFailure != nil {
		cycle := workload.currentCycle.Load()
		if cycle != 0 && int(cycle) < len(workload.firstIOFailureCaptured) &&
			seamlessScaleIsIncompleteIO(sample.Err) && workload.firstIOFailureCaptured[cycle].CompareAndSwap(false, true) {
			workload.firstIOFailureWG.Add(1)
			go func() {
				defer workload.firstIOFailureWG.Done()
				workload.captureFirstIOFailure(cycle, sample)
			}()
		}
	}
	return sample
}

func (workload *seamlessScaleWorkload) inspectDirectAbortRow(ctx context.Context, connection net.Conn, row seamlessScaleAck) error {
	diagnosticCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if connection == nil {
		return errors.New("direct-abort row check has no live SQL connection")
	}
	result, err := fusedDDLWireQuery(diagnosticCtx, connection,
		fmt.Sprintf("SELECT id,value,marker FROM %s WHERE id='%s'", row.Table, row.ID), false)
	if err != nil {
		return fmt.Errorf("direct-abort row check query failed: %w", err)
	}
	if result.code != "" {
		return fmt.Errorf("direct-abort row check response code=%s message=%q", result.code, result.message)
	}
	if len(result.rows) == 0 {
		return errors.New("direct-abort row check found no row")
	}
	if len(result.rows) != 1 || len(result.rows[0]) != 3 || len(result.columns) != 3 {
		return fmt.Errorf("direct-abort row check returned rows=%d columns=%d", len(result.rows), len(result.columns))
	}
	id, idErr := fusedPGCellText(result.rows[0][0], result.columns[0])
	marker, markerErr := fusedPGCellText(result.rows[0][2], result.columns[2])
	valueMatches := result.rows[0][1] == strconv.Itoa(row.Value)
	markerMatches := markerErr == nil && marker == row.Marker
	return fmt.Errorf("direct-abort row check present=true id_matches=%t value_matches=%t marker_matches=%t value=%q id_error=%v marker_error=%v",
		idErr == nil && id == row.ID, valueMatches, markerMatches, result.rows[0][1], idErr, markerErr)
}

func seamlessScaleQueryRetry(ctx context.Context, connection net.Conn, query string, extended, readOnly bool) (fusedPGResult, uint64, error) {
	return retrySeamlessScaleSQL(ctx, readOnly, func(attempt context.Context) (fusedPGResult, error) {
		return fusedDDLWireQuery(attempt, connection, query, extended)
	})
}

// Only complete responses proving safe replay may retry. An unknown write or
// a transport failure must never resubmit SQL under a new durable identity.
func retrySeamlessScaleSQL(ctx context.Context, readOnly bool, call func(context.Context) (fusedPGResult, error)) (fusedPGResult, uint64, error) {
	// The recovery deadline is armed only once a retryable response proves a
	// replay is safe; the first attempt runs under the caller's context.
	armed := false
	for attempt := uint64(0); ; attempt++ {
		result, err := call(ctx)
		if err != nil || !seamlessScaleSQLRetryable(result, readOnly) {
			return result, attempt, err
		}
		if !armed {
			recovery, cancel := context.WithTimeout(ctx, seamlessScaleRecoveryBudget)
			defer cancel()
			ctx, armed = recovery, true
		}
		last := fmt.Errorf("transient PostgreSQL response %s: %s", result.code, result.message)
		if err := fusedWaitRetry(ctx, seamlessScaleRetryDelay(attempt)); err != nil {
			return result, attempt + 1, errors.Join(last, err)
		}
	}
}

func seamlessScaleSQLRetryable(result fusedPGResult, readOnly bool) bool {
	if result.code == "40001" {
		if strings.Contains(result.message, "VIBEDB_RF3_DIRECT_ABORT result_code=11") {
			// Mutation-format code 11 is a retained IndexConflict, not a
			// serialization race. Reissuing the INSERT cannot make it valid.
			return false
		}
		return true
	} // Complete transaction abort.
	if !readOnly {
		return false
	}
	switch result.code {
	case "XX000", "55000", "55P03", "57P03":
		return fusedPGResultTransient(result)
	default:
		return false
	}
}

func seamlessScaleRetryDelay(attempt uint64) time.Duration {
	return 20 * time.Millisecond << min(attempt, 4)
}

func (workload *seamlessScaleWorkload) gatewayRead(ctx context.Context, connection *seamlessScaleConnection, row seamlessScaleAck) (result fusedPGResult, retries uint64, resultErr error) {
	startedIO := false
	defer func() {
		if resultErr == nil || !startedIO {
			return
		}
		if reopenErr := workload.refreshGatewayAfterIncompleteResponse(ctx, connection); reopenErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("reopen native gateway connection after incomplete response: %w", reopenErr))
		}
	}()
	request := rf3FixturePointRequest(row.Table, row.ID)
	raw, err := vibejson.Marshal(&request)
	if err != nil {
		return fusedPGResult{}, 0, err
	}
	// A failed refresh leaves the gate unopened while the gateway restarts.
	// Reopen it lazily like the SQL stream: report the dial failure as a
	// sample error instead of dereferencing a nil connection below.
	if err := connection.openGatewayConnection(ctx); err != nil {
		return fusedPGResult{}, 0, err
	}
	// The recovery deadline is armed only once a retryable response proves a
	// replay is safe; the first attempt runs under the caller's context.
	armed := false
	for attempt := uint64(0); ; attempt++ {
		startedIO = true
		if err := connection.gate.SetDeadline(minFusedDeadline(ctx, time.Now().Add(seamlessScaleRecoveryBudget))); err != nil {
			return fusedPGResult{}, attempt, err
		}
		if _, err := connection.gate.Write(append(raw, '\n')); err != nil {
			return fusedPGResult{}, attempt, err
		}
		response, err := connection.reader.ReadSlice('\n')
		if err == nil && seamlessScaleGatewayResponseMatches(response, row) {
			return fusedPGResult{}, attempt, nil
		}
		if err != nil {
			return fusedPGResult{}, attempt, err
		}
		last := fmt.Errorf("native gateway response did not match %s/%s: %.1024s", row.Table, row.ID, response)
		if !durableRF3ExternalRetryableResponse(response) {
			return fusedPGResult{}, attempt, last
		}
		if !armed {
			recovery, cancel := context.WithTimeout(ctx, seamlessScaleRecoveryBudget)
			defer cancel()
			ctx, armed = recovery, true
		}
		if err := fusedWaitRetry(ctx, seamlessScaleRetryDelay(attempt)); err != nil {
			return fusedPGResult{}, attempt + 1, errors.Join(last, err)
		}
	}
}

func seamlessScaleGatewayResponseMatches(raw []byte, row seamlessScaleAck) bool {
	if !rf3FixturePointResponseMatches(raw, row.ID) {
		return false
	}
	document, err := vibejson.Parse(raw)
	if err != nil {
		return false
	}
	documentsNode, present := document.Get("documents")
	documents, valid := documentsNode.Array()
	if !present || !valid || len(documents) != 1 {
		return false
	}
	valueNode, present := documents[0].Get("value")
	value, valid := valueNode.Int64()
	if !present || !valid || value != int64(row.Value) {
		return false
	}
	markerNode, present := documents[0].Get("marker")
	marker, valid := markerNode.Text()
	return present && valid && marker == row.Marker
}

func (workload *seamlessScaleWorkload) phaseEvidence(phase string, start, end time.Time, scheduled, missed uint64, samples []seamlessScaleSample) seamlessScalePhaseEvidence {
	latencies := make([]uint64, 0, len(samples))
	queue := make([]uint64, 0, len(samples))
	var started, completed, successes, errorsCount, timeouts, retries, acknowledged, verified uint64
	completedTimes := make([]time.Time, 0, len(samples))
	for _, sample := range samples {
		if !sample.Started.IsZero() {
			started++
		}
		if !sample.Completed.IsZero() {
			completed++
			completedTimes = append(completedTimes, sample.Completed)
		}
		if sample.Err == nil {
			successes++
		} else {
			errorsCount++
			if seamlessScaleIsTimeout(sample.Err) {
				timeouts++
			}
		}
		retries += sample.Retries
		if sample.Ack != nil {
			acknowledged++
		}
		if sample.Verified != nil {
			verified++
		}
		if sample.Completed.After(sample.Scheduled) {
			latencies = append(latencies, uint64(sample.Latency))
		}
		if sample.Started.After(sample.Scheduled) {
			queue = append(queue, uint64(sample.QueueLag))
		}
	}
	if started > scheduled {
		started = scheduled
	}
	if completed > started {
		completed = started
	}
	sort.Slice(completedTimes, func(i, j int) bool { return completedTimes[i].Before(completedTimes[j]) })
	var completionGap uint64
	for index := 1; index < len(completedTimes); index++ {
		gap := uint64(completedTimes[index].Sub(completedTimes[index-1]))
		if gap > completionGap {
			completionGap = gap
		}
	}
	if completionGap == 0 && len(completedTimes) > 1 {
		completionGap = 1
	}
	digest := workload.ackDigest()
	return seamlessScalePhaseEvidence{Phase: phase, StartNS: uint64(start.UnixNano()), EndNS: uint64(end.UnixNano()),
		DurationNS: uint64(end.Sub(start)), Scheduled: scheduled, Started: started, Completed: completed,
		Requests: scheduled, Successes: successes, Errors: errorsCount, Timeouts: timeouts, Missed: missed,
		Retries: retries, AcknowledgedWrites: acknowledged, VerifiedReads: verified, P50NS: seamlessScaleQuantile(latencies, 0.50),
		P95NS: seamlessScaleQuantile(latencies, 0.95), P99NS: seamlessScaleQuantile(latencies, 0.99), MaxPauseNS: completionGap,
		CompletionGapNS: completionGap, QueueLagP99NS: seamlessScaleQuantile(queue, 0.99),
		OfferedRateMilli:      uint64(scheduled) * 1_000_000_000_000 / maxUint64(1, uint64(end.Sub(start))),
		ThroughputMilli:       uint64(successes) * 1_000_000_000_000 / maxUint64(1, uint64(end.Sub(start))),
		AcknowledgementDigest: digest, VerificationDigest: digest}
}

func seamlessScaleIsTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError) && networkError.Timeout()
}

func seamlessScaleIOErrorClass(err error) string {
	if err == nil {
		return ""
	}
	if seamlessScaleIsTimeout(err) {
		return "timeout"
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return "unexpected_eof"
	}
	if errors.Is(err, io.EOF) {
		return "eof"
	}
	if errors.Is(err, net.ErrClosed) {
		return "closed"
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "native gateway response did not match") ||
		strings.Contains(message, "invalid postgresql") || strings.Contains(message, "truncated postgresql") ||
		strings.Contains(message, "unexpected postgresql") || strings.Contains(message, "protocol") {
		return "protocol"
	}
	var networkError *net.OpError
	if errors.As(err, &networkError) {
		return "network"
	}
	var wrappedNetworkError net.Error
	if errors.As(err, &wrappedNetworkError) {
		return "network"
	}
	return "other"
}

func seamlessScaleIsIncompleteIO(err error) bool {
	class := seamlessScaleIOErrorClass(err)
	return class != "" && class != "other"
}

func (workload *seamlessScaleWorkload) ackDigest() string {
	workload.mu.Lock()
	keys := make([]string, 0, len(workload.acknowledged))
	rows := make(map[string]seamlessScaleAck, len(workload.acknowledged))
	for key, row := range workload.acknowledged {
		keys = append(keys, key)
		rows[key] = row
	}
	workload.mu.Unlock()
	sort.Strings(keys)
	hash := sha256.New()
	for _, key := range keys {
		row := rows[key]
		_, _ = io.WriteString(hash, key+"\x00"+strconv.Itoa(row.Value)+"\x00"+row.Marker+"\n")
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func (workload *seamlessScaleWorkload) VerifyAllAcknowledgements(ctx context.Context) error {
	workload.mu.Lock()
	rows := make([]seamlessScaleAck, 0, len(workload.acknowledged))
	for _, row := range workload.acknowledged {
		rows = append(rows, row)
	}
	workload.mu.Unlock()
	sort.Slice(rows, func(i, j int) bool { return seamlessScaleAckKey(rows[i]) < seamlessScaleAckKey(rows[j]) })
	if len(rows) != 0 {
		if _, _, err := workload.gatewayRead(ctx, &workload.connections[0], rows[0]); err != nil {
			return fmt.Errorf("original survivor native session: %w", err)
		}
	}
	for _, row := range rows {
		result, err := fusedDDLWireQuery(ctx, workload.survivorSQL,
			fmt.Sprintf("SELECT id,value,marker FROM %s WHERE id='%s'", row.Table, row.ID), false)
		if err != nil {
			return err
		}
		if !seamlessScaleSQLMatches(result, row) {
			return fmt.Errorf("acknowledged row mismatch %s/%s: %+v", row.Table, row.ID, result)
		}
	}
	return nil
}

func (workload *seamlessScaleWorkload) VerifyExactPG(ctx context.Context, address string) error {
	connection, err := fusedOpenDDLWire(ctx, address)
	if err != nil {
		return fmt.Errorf("open independent SQL oracle: %w", err)
	}
	defer connection.Close()
	workload.mu.Lock()
	rows := make([]seamlessScaleAck, 0, len(workload.acknowledged))
	for _, row := range workload.acknowledged {
		rows = append(rows, row)
	}
	workload.mu.Unlock()
	for _, row := range rows {
		result, err := fusedDDLWireQuery(ctx, connection,
			fmt.Sprintf("SELECT id,value,marker FROM %s WHERE id='%s'", row.Table, row.ID), false)
		if err != nil {
			return err
		}
		if !seamlessScaleSQLMatches(result, row) {
			return fmt.Errorf("post-stop row mismatch %s/%s: %+v", row.Table, row.ID, result)
		}
	}
	return nil
}

func (workload *seamlessScaleWorkload) Evidence(baseline, during, after seamlessScalePhaseEvidence) seamlessScaleEvidence {
	// The digest is the sorted set of every acknowledged key/version/value at
	// the terminal oracle. Reusing that final digest in each phase makes the
	// evidence a conservation claim across the whole run instead of comparing
	// snapshots that necessarily differ as the open-loop writer appends rows.
	digest := workload.ackDigest()
	baseline.AcknowledgementDigest, baseline.VerificationDigest = digest, digest
	during.AcknowledgementDigest, during.VerificationDigest = digest, digest
	after.AcknowledgementDigest, after.VerificationDigest = digest, digest
	return seamlessScaleEvidence{Result: "pass", Phase: "terminal_post_stop_verified",
		Baseline: baseline, During: during, After: after}
}

func (workload *seamlessScaleWorkload) WindowCount(phase string) uint64 {
	workload.historyMu.Lock()
	defer workload.historyMu.Unlock()
	return uint64(len(workload.history[phase]))
}

func (workload *seamlessScaleWorkload) SurvivorSessionsStable() bool {
	workload.mu.Lock()
	defer workload.mu.Unlock()
	return workload.survivorSQL != nil && workload.survivorGate != nil && len(workload.acknowledged) != 0 &&
		workload.sqlRequests != 0 && workload.gateRequests != 0
}

func seamlessScaleAckKey(row seamlessScaleAck) string { return row.Table + "\x00" + row.ID }

func seamlessScaleBudget(value *clustercontrol.BudgetStatus) seamlessScaleBudgetEvidence {
	if value == nil {
		return seamlessScaleBudgetEvidence{}
	}
	return seamlessScaleBudgetEvidence{ThrottledCalls: value.ThrottledCalls, ThrottledBytes: value.ThrottledBytes,
		PeakActive: uint64(value.PeakActive), MaxActive: uint64(value.MaxActive)}
}

func boolToUint64(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func maxUint32(left, right uint32) uint32 {
	if left > right {
		return left
	}
	return right
}

func countSeamlessScaleServingNodes(response clustercontrol.Response) int {
	count := 0
	for _, node := range response.Nodes {
		switch strings.ToLower(node.Lifecycle) {
		case "retiring", "draining", "decommissioning", "decommissioned", "retired":
			continue
		default:
			count++
		}
	}
	return count
}

func seamlessScaleQuantile(values []uint64, quantile float64) uint64 {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	index := int(float64(len(values)-1) * quantile)
	return values[index]
}

func maxUint64(left, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}

func TestSeamlessScaleInOutProcessQualification(t *testing.T) {
	if os.Getenv(seamlessScaleProcessEnvironment) != "1" {
		t.Skip("set " + seamlessScaleProcessEnvironment + "=1 for the mandatory Linux scale-in/out qualification")
	}
	if runtime.GOOS != "linux" || testing.Short() {
		t.Fatal("seamless scale qualification requires a non-short Linux process runner")
	}
	if _, err := os.Stat("/proc"); err != nil {
		t.Fatalf("required Linux /proc is unavailable: %v", err)
	}
	childDiagnosticTable := ""
	for _, entry := range seamlessScaleDiagnosticEnvironment() {
		if value, ok := strings.CutPrefix(entry, seamlessScaleDiagnosticAbortTableEnv+"="); ok {
			childDiagnosticTable = value
			break
		}
	}
	t.Logf("scale child diagnostic config: abort_reason=1 abort_table=%s", childDiagnosticTable)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	defer cancel()
	root, err := os.MkdirTemp("", "ss-qual-")
	if err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(root, "state")
	t.Cleanup(func() {
		if !t.Failed() {
			_ = os.RemoveAll(root)
			return
		}
		if path := os.Getenv(seamlessScaleFailureEnvironment); path != "" {
			if err := copySeamlessScaleFailureTree(root, path); err != nil {
				t.Logf("preserve failed scale state: %v", err)
			} else {
				_ = os.RemoveAll(root)
				t.Logf("preserved failed scale state: %s", path)
				return
			}
		}
		_ = os.RemoveAll(root)
	})
	bin := t.TempDir()
	vibedbBinary := filepath.Join(bin, "vibedb")
	shardBinary := filepath.Join(bin, "vibedb-shard")
	replicaProcessBuild(t, ctx, vibedbBinary, "./cmd/vibedb")
	replicaProcessBuild(t, ctx, shardBinary, "./cmd/vibedb-shard")

	pgReservation, err := rf3testfixture.ReserveLoopbackAddresses(3)
	if err != nil {
		t.Fatalf("reserve survivor PostgreSQL endpoints: %v", err)
	}
	pgListens := append([]string(nil), pgReservation.Addresses...)
	if err := pgReservation.Close(); err != nil {
		t.Fatalf("release PostgreSQL endpoint reservations: %v", err)
	}
	if err := writeSeamlessScaleSchemas(root); err != nil {
		t.Fatalf("write scale schemas: %v", err)
	}
	caCertificate, caKey, err := writeSeamlessScaleCA(root)
	if err != nil {
		t.Fatalf("write qualification CA: %v", err)
	}

	args := []string{"cluster", "dev", "--replicas", "3", "--physical-nodes", "3", "--root", state,
		"--diagnostics-on-exit", "--shard-binary", shardBinary, "--pg-listens", strings.Join(pgListens, ","),
		"--tls-ca-certificate", caCertificate, "--tls-ca-key", caKey}
	for _, table := range seamlessScaleTables {
		args = append(args, "--table-schema", filepath.Join(root, table+".sql"))
	}
	bootstrap := startSeamlessScaleSupervisor(t, ctx, vibedbBinary, args, "VibeDB development RF3 physical cluster ready:")
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := bootstrap.Stop(stopCtx); err != nil {
			t.Errorf("stop scale supervisor: %v", err)
		}
	})

	cluster, _ := readSeamlessScaleCluster(t, state)
	if cluster.Format != 2 || cluster.Nodes != 3 || cluster.Replicas != 3 || cluster.PhysicalNodes != 3 || len(cluster.NodeManifests) != 3 {
		t.Fatalf("initial cluster topology=%+v", cluster)
	}
	if err := validateSeamlessScaleManifestPaths(cluster); err != nil {
		t.Fatal(err)
	}

	// cluster dev is used only to emit the canonical RF3 inventory. Stop its
	// supervisor before starting the same shipped node manifests directly; the
	// direct process set lets this fixture restart one controller owner without
	// taking survivor frontends or SQL sessions down with it.
	if err := bootstrap.Stop(ctx); err != nil {
		t.Fatalf("stop bootstrap supervisor before isolated process set: %v", err)
	}
	domain, err := reissueSeamlessScaleClusterCredentials(cluster, caCertificate, caKey)
	if err != nil {
		t.Fatalf("reissue initial cluster credentials under fixture CA: %v", err)
	}
	profilePath := writeSeamlessScaleOperatorProfile(t, root, cluster, caCertificate, caKey, domain)
	gatewaySeeds := seamlessScaleInitialGatewaySeeds(t, cluster)
	if len(cluster.NodeManifests) == 0 {
		t.Fatal("initial cluster has no source node manifest")
	}
	// Bind the saved original endpoints before allocating any empty-node
	// addresses. The bootstrap supervisor released those fixed ports above;
	// reserving targets first could otherwise steal an original peer port.
	physical := startSeamlessScalePhysicalCluster(t, ctx, shardBinary, cluster)
	listenerReservation, err := rf3testfixture.ReserveLoopbackAddresses(12)
	if err != nil {
		t.Fatalf("reserve empty-node listeners: %v", err)
	}
	emptyTargets := make([]seamlessScaleTarget, 3)
	initialNodeIDs := seamlessScaleInitialNodeIDs(t, cluster)
	for index := range emptyTargets {
		emptyTargets[index] = writeSeamlessScaleTargetPreparation(t, root, cluster, cluster.NodeManifests[0].ServeManifest,
			caCertificate, caKey, domain, index, listenerReservation.Addresses[index*4:(index+1)*4], initialNodeIDs, gatewaySeeds)
		if code := runSeamlessScaleCommand(ctx, shardBinary, "prepare-node-rf3", "-manifest", filepath.Join(root, fmt.Sprintf("empty-node-%d.prepare-node.vibejson", index+1))); code != 0 {
			t.Fatalf("prepare-node-rf3 target %d returned %d", index+1, code)
		}
		if err := writeSeamlessScaleTargetDescriptor(emptyTargets[index]); err != nil {
			t.Fatalf("write target %d public descriptor: %v", index+1, err)
		}
		if err := assertSeamlessScaleEmptyManifest(emptyTargets[index].Manifest, hex.EncodeToString(emptyTargets[index].NodeID[:])); err != nil {
			t.Fatalf("target %d empty manifest: %v", index+1, err)
		}
	}
	// All target addresses were reserved together while the original listeners
	// were live, so neither a sibling target nor an original owns the same port.
	if err := listenerReservation.Close(); err != nil {
		t.Fatalf("release empty-node listener reservations: %v", err)
	}
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for index, node := range physical.nodes {
			diagnostic := node.diagnostic.String()
			if len(diagnostic) > 32<<10 {
				diagnostic = diagnostic[len(diagnostic)-(32<<10):]
			}
			t.Logf("physical node %d final diagnostics:\n%s", index, diagnostic)
		}
	})

	firstRetiringIndex := seamlessScaleInitialRetiringIndex(len(cluster.NodeManifests))
	if firstRetiringIndex < 0 {
		t.Fatalf("initial topology has no non-controller retiring node: %d", len(cluster.NodeManifests))
	}
	const survivorIndex = seamlessScaleSurvivorIndex
	clusterProfile, err := servicetls.LoadProfile(cluster.ClientCertificate, cluster.ClientKey, cluster.Roots, fusedNodeProcessOID, time.Now)
	if err != nil {
		t.Fatalf("load traffic client profile: %v", err)
	}
	controlIdentities, err := readSeamlessScaleManifestIdentities(cluster.NodeManifests[survivorIndex].ServeManifest)
	if err != nil || len(controlIdentities) == 0 {
		t.Fatalf("load survivor node-control identity: %v", err)
	}
	controlProfile, err := servicetls.LoadProfile(controlIdentities[0].Certificate, controlIdentities[0].Key,
		controlIdentities[0].Roots, controlIdentities[0].IdentityOID, time.Now)
	if err != nil {
		t.Fatalf("load survivor node-control profile: %v", err)
	}

	// The three targets are prepared by the shipped command but remain absent
	// from the catalog until their own cycle. Initial grants below contain only
	// the original physical identities; no future target is pre-authorized.
	for index := range emptyTargets {
		descriptor, descriptorErr := clustercontrol.LoadNodeDescriptor(emptyTargets[index].Descriptor)
		if descriptorErr != nil {
			t.Fatalf("load target %d descriptor: %v", index+1, descriptorErr)
		}
		emptyTargets[index].Public = descriptor
	}
	nodesResponse := runSeamlessScaleCLI(t, ctx, vibedbBinary, "nodes", profilePath)
	if !nodesResponse.OK || len(nodesResponse.Nodes) != 3 || !validEvidenceDigest(nodesResponse.GroupInventoryDigest) {
		t.Fatalf("initial nodes response=%+v", nodesResponse)
	}
	for index, target := range emptyTargets {
		if hasSeamlessScaleNode(nodesResponse, target.NodeID) {
			t.Fatalf("empty target %d was pre-enrolled in the initial operator directory", index+1)
		}
	}
	beforeInventoryDigest := nodesResponse.GroupInventoryDigest

	// Keep one survivor gateway and one survivor SQL session alive through all
	// three physical waves. A session on the first retiring frontend is opened
	// only after that node's isolated restart, so the restart itself cannot
	// destroy the witness that must block safe_to_stop.
	survivorGateway, err := fusedDialGateway(ctx, clusterProfile, mustSeamlessScaleNodeID(t, cluster.NodeManifests[survivorIndex].GatewayNode), cluster.NodeManifests[survivorIndex].FrontendListen)
	if err != nil {
		t.Fatalf("open survivor gateway session: %v", err)
	}
	defer survivorGateway.Close()
	survivorSQL, err := fusedOpenDDLWire(ctx, pgListens[survivorIndex])
	if err != nil {
		t.Fatalf("open survivor SQL session: %v", err)
	}
	defer survivorSQL.Close()

	workload := newSeamlessScaleWorkload(t, ctx, pgListens[survivorIndex], survivorSQL, survivorGateway, func(openCtx context.Context) (net.Conn, error) {
		return fusedDialGateway(openCtx, clusterProfile, mustSeamlessScaleNodeID(t, cluster.NodeManifests[survivorIndex].GatewayNode),
			cluster.NodeManifests[survivorIndex].FrontendListen)
	})
	if err := workload.Seed(t, ctx); err != nil {
		t.Fatalf("seed acknowledged workload rows: %v", err)
	}
	// Resolve the profile before any load: an unknown profile must fail before
	// the qualification spends minutes of runner time.
	profileBounds, err := loadSeamlessScalePerformanceBounds(os.Getenv)
	if err != nil {
		t.Fatalf("performance bounds: %v", err)
	}
	var calibratedRate int
	var baseline seamlessScalePhaseEvidence
	if profileBounds.FixedRate != 0 {
		calibratedRate = profileBounds.FixedRate
		baseline = workload.WindowSet(ctx, seamlessScalePhaseBaseline, 5, seamlessScaleWindowDuration, calibratedRate)
	} else {
		calibratedRate, baseline = calibrateSeamlessScaleRate(t, workload, ctx)
	}
	t.Logf("scale baseline complete: profile=%s rate=%d", profileBounds.Profile, calibratedRate)
	t.Logf("scale baseline complete: rate=%d scheduled=%d completed=%d errors=%d missed=%d p99=%s", calibratedRate,
		baseline.Scheduled, baseline.Completed, baseline.Errors, baseline.Missed, time.Duration(baseline.P99NS))
	// These are already mandatory final evidence gates. An invalid baseline
	// cannot qualify after any topology wave, so preserve the failure now.
	if baseline.Errors != 0 || baseline.Timeouts != 0 || baseline.Missed != 0 || baseline.Completed != baseline.Scheduled {
		t.Fatalf("strict baseline cannot qualify: scheduled=%d completed=%d errors=%d timeouts=%d missed=%d",
			baseline.Scheduled, baseline.Completed, baseline.Errors, baseline.Timeouts, baseline.Missed)
	}
	targetProcesses := make([]*seamlessScaleNodeProcess, len(emptyTargets))
	var targetProcessesMu sync.Mutex
	// A failed foreground window cannot become a qualification by completing
	// more topology cycles. Preserve its measured counters and first errors,
	// then capture the serving processes before cancellation and stop at the
	// actual failing phase instead of hiding it at the end.
	workload.reportWindow = func(window seamlessScalePhaseEvidence, samples []seamlessScaleSample) {
		if window.Errors == 0 && window.Timeouts == 0 && window.Missed == 0 && window.Completed == window.Scheduled {
			return
		}
		captureDone := make(chan struct{})
		go func() {
			workload.firstIOFailureWG.Wait()
			close(captureDone)
		}()
		select {
		case <-captureDone:
		case <-time.After(30 * time.Second):
			t.Errorf("first foreground I/O diagnostic did not finish before teardown")
		}
		failures := make([]string, 0, 3)
		type failureSample struct {
			Scheduled   time.Time `json:"scheduled"`
			Started     time.Time `json:"started"`
			Completed   time.Time `json:"completed"`
			Worker      int       `json:"worker"`
			Sequence    uint64    `json:"sequence"`
			Stage       string    `json:"stage"`
			ErrorClass  string    `json:"error_class"`
			SocketError string    `json:"socket_error"`
		}
		type failureCount struct {
			Stage      string `json:"stage"`
			ErrorClass string `json:"error_class"`
			Count      uint64 `json:"count"`
		}
		failureSamples := make([]failureSample, 0, min(len(samples), 64))
		failureCounts := make(map[string]uint64)
		for _, sample := range samples {
			if sample.Err != nil {
				stage, class := sample.Stage.String(), seamlessScaleIOErrorClass(sample.Err)
				failureCounts[stage+"\x00"+class]++
				if len(failures) < cap(failures) {
					failures = append(failures, fmt.Sprintf("%.2048s", sample.Err.Error()))
				}
				if len(failureSamples) < 64 {
					failureSamples = append(failureSamples, failureSample{
						Scheduled: sample.Scheduled.UTC(), Started: sample.Started.UTC(), Completed: sample.Completed.UTC(),
						Worker: sample.Worker, Sequence: sample.Sequence, Stage: stage, ErrorClass: class,
						SocketError: fmt.Sprintf("%.2048s", sample.Err.Error()),
					})
				}
			}
		}
		countKeys := make([]string, 0, len(failureCounts))
		for key := range failureCounts {
			countKeys = append(countKeys, key)
		}
		sort.Strings(countKeys)
		counts := make([]failureCount, 0, len(countKeys))
		for _, key := range countKeys {
			parts := strings.SplitN(key, "\x00", 2)
			class := ""
			if len(parts) == 2 {
				class = parts[1]
			}
			counts = append(counts, failureCount{Stage: parts[0], ErrorClass: class, Count: failureCounts[key]})
		}
		processes := append([]*seamlessScaleNodeProcess(nil), physical.nodes...)
		targetProcessesMu.Lock()
		processes = append(processes, targetProcesses...)
		targetProcessesMu.Unlock()
		directOutcomes := seamlessScaleProcessDirectOutcomes(processes)
		raftSnapshots, snapshotErr := captureSeamlessScaleRaftDiagnostics(processes)
		if snapshotErr != nil {
			t.Errorf("capture first failed-window Raft snapshots: %v", snapshotErr)
		}
		capturedAt := time.Now().UTC()
		snapshotError := ""
		if snapshotErr != nil {
			snapshotError = snapshotErr.Error()
		}
		if path := os.Getenv(seamlessScaleEvidenceEnvironment); path != "" {
			raw, err := json.Marshal(struct {
				Window           seamlessScalePhaseEvidence `json:"window"`
				Errors           []string                   `json:"errors"`
				FailureSamples   []failureSample            `json:"failure_samples,omitempty"`
				StageErrorCounts []failureCount             `json:"stage_error_counts,omitempty"`
				CapturedAt       time.Time                  `json:"captured_at"`
				ProcessSnapshots []json.RawMessage          `json:"process_snapshots,omitempty"`
				DirectOutcomes   []string                   `json:"direct_outcomes,omitempty"`
				SnapshotError    string                     `json:"snapshot_error,omitempty"`
			}{window, failures, failureSamples, counts, capturedAt, raftSnapshots, directOutcomes, snapshotError})
			if err == nil {
				index := workload.failedWindowSequence.Add(1)
				err = os.WriteFile(fmt.Sprintf("%s.failed-window-%s-%04d.json", path, window.Phase, index), append(raw, '\n'), 0600)
			}
			if err != nil {
				t.Errorf("persist failed workload window: %v", err)
			}
		}
		t.Errorf("scale foreground window failed: phase=%s scheduled=%d completed=%d errors=%d timeouts=%d missed=%d first_errors=%q",
			window.Phase, window.Scheduled, window.Completed, window.Errors, window.Timeouts, window.Missed, failures)
		cancel()
	}
	workload.captureFirstIOFailure = func(cycle uint32, sample seamlessScaleSample) {
		capturedAt := time.Now().UTC()
		processes := append([]*seamlessScaleNodeProcess(nil), physical.nodes...)
		targetProcessesMu.Lock()
		processes = append(processes, targetProcesses...)
		targetProcessesMu.Unlock()
		before := snapshotSeamlessScaleWatchdogProcesses(processes)
		snapshots, snapshotErr := captureSeamlessScaleRaftDiagnostics(processes)
		after := snapshotSeamlessScaleWatchdogProcesses(processes)
		directOutcomes := seamlessScaleProcessDirectOutcomes(processes)
		artifact := struct {
			Cycle           uint32                              `json:"cycle"`
			CapturedAt      time.Time                           `json:"captured_at"`
			Scheduled       time.Time                           `json:"scheduled"`
			Started         time.Time                           `json:"started"`
			Completed       time.Time                           `json:"completed"`
			Worker          int                                 `json:"worker"`
			Sequence        uint64                              `json:"sequence"`
			Stage           string                              `json:"stage"`
			ErrorClass      string                              `json:"error_class"`
			Error           string                              `json:"error"`
			ProcessesBefore []seamlessScaleWatchdogProcessState `json:"processes_before"`
			Snapshots       []json.RawMessage                   `json:"process_snapshots,omitempty"`
			ProcessesAfter  []seamlessScaleWatchdogProcessState `json:"processes_after"`
			DirectOutcomes  []string                            `json:"direct_outcomes,omitempty"`
			SnapshotError   string                              `json:"snapshot_error,omitempty"`
		}{Cycle: cycle, CapturedAt: capturedAt, Scheduled: sample.Scheduled.UTC(), Started: sample.Started.UTC(),
			Completed: sample.Completed.UTC(), Worker: sample.Worker, Sequence: sample.Sequence,
			Stage: sample.Stage.String(), ErrorClass: seamlessScaleIOErrorClass(sample.Err),
			Error: fmt.Sprintf("%.2048s", sample.Err), ProcessesBefore: before, Snapshots: snapshots,
			ProcessesAfter: after, DirectOutcomes: directOutcomes}
		if snapshotErr != nil {
			artifact.SnapshotError = snapshotErr.Error()
		}
		if evidencePath := os.Getenv(seamlessScaleEvidenceEnvironment); evidencePath != "" {
			raw, err := json.Marshal(artifact)
			if err == nil {
				err = os.WriteFile(fmt.Sprintf("%s.cycle-%d-first-foreground-io-failure.json", evidencePath, cycle), append(raw, '\n'), 0o600)
			}
			if err != nil {
				t.Errorf("persist first foreground I/O failure diagnostics: %v", err)
			}
		}
		t.Logf("first foreground I/O failure captured: cycle=%d worker=%d stage=%s sequence=%d class=%s snapshot_error=%q",
			cycle, sample.Worker, sample.Stage, sample.Sequence, seamlessScaleIOErrorClass(sample.Err), artifact.SnapshotError)
	}
	stopWatchdog := startSeamlessScaleStallWatchdog(ctx, &workload.duringActive, &workload.currentCycle,
		workload.workerCalls, seamlessScaleWatchdogInterval, seamlessScaleWatchdogThreshold,
		func(stall seamlessScaleStall) {
			triggeredAt := time.Now().UTC()
			processes := append([]*seamlessScaleNodeProcess(nil), physical.nodes...)
			targetProcessesMu.Lock()
			processes = append(processes, targetProcesses...)
			targetProcessesMu.Unlock()
			before := snapshotSeamlessScaleWatchdogProcesses(processes)
			snapshots, captureErr := captureSeamlessScaleRaftDiagnostics(processes)
			after := snapshotSeamlessScaleWatchdogProcesses(processes)
			errText := ""
			if captureErr != nil {
				errText = captureErr.Error()
			}
			if path := os.Getenv(seamlessScaleEvidenceEnvironment); path != "" {
				raw, err := json.Marshal(struct {
					TriggeredAt     time.Time                           `json:"triggered_at"`
					Stall           seamlessScaleStall                  `json:"stall"`
					ProcessesBefore []seamlessScaleWatchdogProcessState `json:"processes_before"`
					Snapshots       []json.RawMessage                   `json:"process_snapshots,omitempty"`
					ProcessesAfter  []seamlessScaleWatchdogProcessState `json:"processes_after"`
					CaptureError    string                              `json:"capture_error,omitempty"`
				}{triggeredAt, stall, before, snapshots, after, errText})
				if err == nil {
					err = os.WriteFile(fmt.Sprintf("%s.cycle-%d-first-inflight-stall.json", path, stall.Cycle), append(raw, '\n'), 0o600)
				}
				if err != nil {
					t.Errorf("persist first in-flight stall diagnostics: %v", err)
				}
			}
			t.Logf("first in-flight call stall captured: cycle=%d worker=%d stage=%s sequence=%d age=%s capture_error=%q",
				stall.Cycle, stall.Worker, stall.StageName, stall.Sequence, stall.Age, errText)
		})
	t.Cleanup(func() {
		if err := stopWatchdog(); err != nil {
			t.Errorf("stop in-flight stall watchdog: %v", err)
		}
	})

	// The actor starts before the first enrollment and runs until the third
	// retired process has stopped. Every retained during sample is assigned by
	// scheduled arrival and the final complete window, not by a post-hoc
	// completion timestamp.
	duringStop := make(chan struct{})
	duringDone := make(chan seamlessScalePhaseEvidence, 1)
	go func() {
		workload.WindowUntil(ctx, seamlessScalePhaseDuring, seamlessScaleWindowDuration, calibratedRate, duringStop)
		duringDone <- workload.HistoryEvidence(seamlessScalePhaseDuring)
	}()

	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for index, process := range targetProcesses {
			if process == nil || process.diagnostic == nil {
				continue
			}
			diagnostic := process.diagnostic.String()
			if len(diagnostic) > 32<<10 {
				diagnostic = diagnostic[len(diagnostic)-(32<<10):]
			}
			t.Logf("empty target %d final diagnostics:\n%s", index, diagnostic)
		}
	})
	var completedCycles uint64
	var physicalPeak = countSeamlessScaleServingNodes(nodesResponse)
	var controllerRestarted, anyTargetRestarted bool
	var duplicateStable = true
	var postRestartProof bool
	var firstSessionBlocked, firstSessionReleased bool
	var finalSafeResponse clustercontrol.Response
	var finalNodesResponse = nodesResponse
	var aggregateBudget seamlessScaleBudgetEvidence
	var applicationMoved, internalMoved uint32

	for cycle := 0; cycle < len(emptyTargets); cycle++ {
		workload.currentCycle.Store(uint32(cycle + 1))
		target := &emptyTargets[cycle]
		process := startSeamlessScaleEmptyNode(t, ctx, shardBinary, target.Manifest, controlProfile, target.NodeID)
		targetProcessesMu.Lock()
		targetProcesses[cycle] = process
		targetProcessesMu.Unlock()

		currentNodes := runSeamlessScaleCLI(t, ctx, vibedbBinary, "nodes", profilePath)
		if !currentNodes.OK || countSeamlessScaleServingNodes(currentNodes) != 3 || hasSeamlessScaleNode(currentNodes, target.NodeID) {
			t.Fatalf("cycle %d pre-enrollment topology=%+v", cycle+1, currentNodes)
		}
		if cycle == 0 && len(currentNodes.Nodes) != 3 {
			t.Fatalf("cycle %d expected initial three physical nodes: %+v", cycle+1, currentNodes)
		}
		physicalPeak = maxInt(physicalPeak, countSeamlessScaleServingNodes(currentNodes))

		joinRequestID := mustSeamlessScaleRequestID(t)
		join := runSeamlessScaleCLI(t, ctx, vibedbBinary, "join", profilePath,
			"--node-file", target.Descriptor, "--request-id", joinRequestID, "--wait", seamlessScaleOperationWait.String())
		if !join.OK || join.OperationID == "" {
			t.Fatalf("cycle %d join response=%+v", cycle+1, join)
		}
		joinDuplicate := runSeamlessScaleCLI(t, ctx, vibedbBinary, "join", profilePath,
			"--node-file", target.Descriptor, "--request-id", joinRequestID, "--wait", seamlessScaleOperationWait.String())
		if !joinDuplicate.OK || joinDuplicate.OperationID != join.OperationID {
			duplicateStable = false
			t.Fatalf("cycle %d duplicate join changed operation: first=%+v duplicate=%+v", cycle+1, join, joinDuplicate)
		}
		var joinPacingResponse, joinFinal clustercontrol.Response
		joinPacingObserved := false
		if err := waitSeamlessScaleOperation(ctx, vibedbBinary, profilePath, join.OperationID, func(response clustercontrol.Response) bool {
			joinFinal = response
			if seamlessScalePacingObserved(response) {
				joinPacingResponse = response
				joinPacingObserved = true
				return true
			}
			return seamlessScaleTerminalSuccess(response)
		}, func(response clustercontrol.Response) {
			processes := append([]*seamlessScaleNodeProcess(nil), physical.nodes...)
			processes = append(processes, targetProcesses...)
			captures, captureErr := captureSeamlessScaleRaftDiagnostics(processes)
			artifact := struct {
				Cycle      int                            `json:"cycle"`
				Operation  string                         `json:"operation_id"`
				Status     clustercontrol.Response        `json:"status"`
				CapturedAt time.Time                      `json:"captured_at"`
				Details    seamlessScaleFirstStallDetails `json:"first_stall_details"`
				Processes  []json.RawMessage              `json:"process_snapshots,omitempty"`
				Error      string                         `json:"snapshot_error,omitempty"`
			}{Cycle: cycle + 1, Operation: join.OperationID, Status: response,
				CapturedAt: time.Now().UTC(), Details: captureSeamlessScaleFirstStallDetails(response, cluster, target), Processes: captures}
			if captureErr != nil {
				artifact.Error = captureErr.Error()
			}
			if raw, marshalErr := json.MarshalIndent(artifact, "", "  "); marshalErr == nil {
				if evidencePath := os.Getenv(seamlessScaleEvidenceEnvironment); evidencePath != "" {
					marshalErr = os.WriteFile(fmt.Sprintf("%s.cycle-%d-join-pre-pacing-first-stall.json", evidencePath, cycle+1), append(raw, '\n'), 0o600)
				}
				if marshalErr != nil {
					t.Errorf("persist first-stalled pre-pacing join diagnostics: %v", marshalErr)
				}
			} else {
				t.Errorf("marshal first-stalled pre-pacing join diagnostics: %v", marshalErr)
			}
			t.Logf("cycle %d pre-pacing join first-stall diagnostics captured: process_count=%d error=%v",
				cycle+1, len(captures), captureErr)
		}); err != nil {
			t.Fatalf("cycle %d join did not reach pacing or completion: %v", cycle+1, err)
		}
		if !joinPacingObserved {
			t.Fatalf("cycle %d enrollment move never exposed positive pacing in an active move phase: final=%+v", cycle+1, joinFinal)
		}
		applicationMoved = maxUint32(applicationMoved, joinPacingResponse.ApplicationGroupsMoved)
		internalMoved = maxUint32(internalMoved, joinPacingResponse.InternalGroupsMoved)
		aggregateBudget.ThrottledCalls = maxUint64(aggregateBudget.ThrottledCalls, joinPacingResponse.Budget.ThrottledCalls)
		aggregateBudget.ThrottledBytes = maxUint64(aggregateBudget.ThrottledBytes, joinPacingResponse.Budget.ThrottledBytes)
		aggregateBudget.PeakActive = maxUint64(aggregateBudget.PeakActive, uint64(joinPacingResponse.Budget.PeakActive))
		aggregateBudget.MaxActive = maxUint64(aggregateBudget.MaxActive, uint64(joinPacingResponse.Budget.MaxActive))
		physicalPeak = maxInt(physicalPeak, countSeamlessScaleServingNodes(joinPacingResponse))
		t.Logf("scale cycle %d enrollment pacing observed: operation=%s throttled_calls=%d throttled_bytes=%d", cycle+1,
			join.OperationID, joinPacingResponse.Budget.ThrottledCalls, joinPacingResponse.Budget.ThrottledBytes)

		workload.MarkFault(time.Now())
		if err := targetProcesses[cycle].Restart(ctx); err != nil {
			t.Fatalf("cycle %d restart target during enrollment migration: %v", cycle+1, err)
		}
		anyTargetRestarted = true
		if cycle == 0 {
			// Node zero is the controller owner for this direct process set.
			// Restarting this process exercises durable operation recovery while
			// the two survivor frontends and their sessions remain connected.
			workload.MarkFault(time.Now())
			if err := physical.Restart(ctx, seamlessScaleControllerIndex); err != nil {
				t.Fatalf("cycle %d restart controller owner during enrollment migration: %v", cycle+1, err)
			}
			controllerRestarted = true
		}
		postRestartStatus, err := runSeamlessScaleCLIForPoll(ctx, vibedbBinary, profilePath, join.OperationID)
		if err != nil {
			t.Fatalf("cycle %d post-restart durable enrollment status: %v", cycle+1, err)
		}
		if postRestartStatus.Phase == "" || postRestartStatus.Budget == nil ||
			postRestartStatus.State == "failed" || postRestartStatus.OperationID != join.OperationID {
			t.Fatalf("cycle %d post-restart enrollment status is not an operation proof: %+v", cycle+1, postRestartStatus)
		}
		postRestartProof = true
		var stallObservers []func(clustercontrol.Response)
		if cycle == len(emptyTargets)-1 {
			stallObservers = append(stallObservers, func(response clustercontrol.Response) {
				processes := append([]*seamlessScaleNodeProcess(nil), physical.nodes...)
				processes = append(processes, targetProcesses...)
				captures, captureErr := captureSeamlessScaleRaftDiagnostics(processes)
				artifact := struct {
					Cycle      int                            `json:"cycle"`
					Operation  string                         `json:"operation_id"`
					Status     clustercontrol.Response        `json:"status"`
					CapturedAt time.Time                      `json:"captured_at"`
					Details    seamlessScaleFirstStallDetails `json:"first_stall_details"`
					Processes  []json.RawMessage              `json:"process_snapshots,omitempty"`
					Error      string                         `json:"snapshot_error,omitempty"`
				}{Cycle: cycle + 1, Operation: join.OperationID, Status: response,
					CapturedAt: time.Now().UTC(), Details: captureSeamlessScaleFirstStallDetails(response, cluster, target), Processes: captures}
				if captureErr != nil {
					artifact.Error = captureErr.Error()
				}
				if raw, marshalErr := json.MarshalIndent(artifact, "", "  "); marshalErr == nil {
					if evidencePath := os.Getenv(seamlessScaleEvidenceEnvironment); evidencePath != "" {
						marshalErr = os.WriteFile(fmt.Sprintf("%s.cycle-%d-join-first-stall.json", evidencePath, cycle+1), append(raw, '\n'), 0o600)
					}
					if marshalErr != nil {
						t.Errorf("persist first-stalled join diagnostics: %v", marshalErr)
					}
				} else {
					t.Errorf("marshal first-stalled join diagnostics: %v", marshalErr)
				}
				t.Logf("cycle %d first stalled join diagnostics captured: process_count=%d error=%v",
					cycle+1, len(captures), captureErr)
			})
		}
		if err := waitSeamlessScaleOperation(ctx, vibedbBinary, profilePath, join.OperationID, func(response clustercontrol.Response) bool {
			joinFinal = response
			return seamlessScaleTerminalSuccess(response)
		}, stallObservers...); err != nil {
			t.Fatalf("cycle %d join did not complete after restart: %v", cycle+1, err)
		}
		applicationMoved = maxUint32(applicationMoved, joinFinal.ApplicationGroupsMoved)
		internalMoved = maxUint32(internalMoved, joinFinal.InternalGroupsMoved)
		physicalPeak = maxInt(physicalPeak, countSeamlessScaleServingNodes(joinFinal))
		t.Logf("scale cycle %d join complete after restart: operation=%s", cycle+1, join.OperationID)

		rebalanceRequestID := mustSeamlessScaleRequestID(t)
		rebalance := runSeamlessScaleCLI(t, ctx, vibedbBinary, "rebalance", profilePath,
			"--request-id", rebalanceRequestID, "--desired-node-count", "4", "--max-moves", "32",
			// The join above is the required real migration wave. Rebalance may
			// legitimately find no work once the newly enrolled target is balanced.
			"--max-migration-bytes", strconv.FormatUint(64<<20, 10), "--hysteresis-ppm", "1",
			"--wait", seamlessScaleOperationWait.String())
		if !rebalance.OK || rebalance.OperationID == "" {
			t.Fatalf("cycle %d rebalance response=%+v", cycle+1, rebalance)
		}
		rebalanceDuplicate := runSeamlessScaleCLI(t, ctx, vibedbBinary, "rebalance", profilePath,
			"--request-id", rebalanceRequestID, "--desired-node-count", "4", "--max-moves", "32",
			"--max-migration-bytes", strconv.FormatUint(64<<20, 10), "--hysteresis-ppm", "1",
			"--wait", seamlessScaleOperationWait.String())
		if !rebalanceDuplicate.OK || rebalanceDuplicate.OperationID != rebalance.OperationID {
			duplicateStable = false
			t.Fatalf("cycle %d duplicate rebalance changed operation: first=%+v duplicate=%+v", cycle+1, rebalance, rebalanceDuplicate)
		}

		var rebalanceFinal clustercontrol.Response
		if err := waitSeamlessScaleOperation(ctx, vibedbBinary, profilePath, rebalance.OperationID, func(response clustercontrol.Response) bool {
			rebalanceFinal = response
			return seamlessScaleTerminalSuccess(response)
		}); err != nil {
			t.Fatalf("cycle %d rebalance did not complete after restart: %v", cycle+1, err)
		}
		applicationMoved = maxUint32(applicationMoved, rebalanceFinal.ApplicationGroupsMoved)
		internalMoved = maxUint32(internalMoved, rebalanceFinal.InternalGroupsMoved)
		physicalPeak = maxInt(physicalPeak, countSeamlessScaleServingNodes(rebalanceFinal))
		t.Logf("scale cycle %d rebalance complete after restart: operation=%s", cycle+1, rebalance.OperationID)

		retireID := target.NodeID
		if cycle == 0 {
			retireID = initialNodeIDs[firstRetiringIndex]
		} else {
			retireID = emptyTargets[cycle-1].NodeID
		}
		retireIDText := hex.EncodeToString(retireID[:])
		retireIncarnation := seamlessScaleNodeIncarnation(rebalanceFinal, retireIDText)
		if retireIncarnation == 0 {
			latestNodes := runSeamlessScaleCLI(t, ctx, vibedbBinary, "nodes", profilePath)
			retireIncarnation = seamlessScaleNodeIncarnation(latestNodes, retireIDText)
		}
		if retireIncarnation == 0 {
			t.Fatalf("cycle %d has no retiring incarnation for %s", cycle+1, retireIDText)
		}

		var retiringSQL net.Conn
		var blocked bool
		if cycle == 0 {
			// Open only after the isolated controller restart, per the witness
			// contract. This connection must survive long enough to hold the
			// exact frontend-session blocker.
			retiringSQL, err = fusedOpenDDLWire(ctx, pgListens[firstRetiringIndex])
			if err != nil {
				t.Fatalf("cycle %d open retiring SQL witness after restart: %v", cycle+1, err)
			}
			probe, probeErr := fusedDDLWireQuery(ctx, retiringSQL, "SELECT 1", false)
			if probeErr != nil || probe.code != "" {
				t.Fatalf("cycle %d retiring SQL probe: result=%+v err=%v", cycle+1, probe, probeErr)
			}
		}
		retireRequestID := mustSeamlessScaleRequestID(t)
		retire := runSeamlessScaleCLI(t, ctx, vibedbBinary, "decommission", profilePath,
			"--node", retireIDText, "--incarnation", strconv.FormatUint(retireIncarnation, 10),
			"--request-id", retireRequestID, "--wait", seamlessScaleOperationWait.String())
		if !retire.OK || retire.OperationID == "" {
			t.Fatalf("cycle %d decommission response=%+v", cycle+1, retire)
		}
		if cycle == 0 {
			// The decommission proof may spend several minutes draining real
			// replicas. Keep the witness active while that proof runs so an
			// idle frontend connection cannot disappear before the controller
			// observes its authenticated session blocker. This must exercise the
			// real stored-data route, not only the PG protocol's local SELECT 1:
			// an insert and its exact read both travel over the held retiring
			// frontend after the Active -> Draining transition has started.
			var witnessLastProbe time.Time
			var witnessProbeErr error
			witnessRow := seamlessScaleAck{Table: seamlessScaleTables[0],
				ID: "drain-witness-" + retireRequestID[:16], Value: 91_337,
				Marker: seamlessScalePayload("drain-witness", cycle)}
			witnessWriteDone := false
			blocked, err = pollSeamlessScaleStatus(ctx, vibedbBinary, profilePath, retire.OperationID, func(response clustercontrol.Response) bool {
				if witnessProbeErr != nil {
					return true
				}
				if !witnessWriteDone {
					writeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
					result, writeErr := fusedDDLWireQuery(writeCtx, retiringSQL,
						fmt.Sprintf("INSERT INTO %s (id,value,marker) VALUES ('%s',%d,'%s')", witnessRow.Table, witnessRow.ID, witnessRow.Value, witnessRow.Marker), false)
					cancel()
					if writeErr != nil || result.code != "" || result.tag != "INSERT 0 1" {
						witnessProbeErr = fmt.Errorf("retiring SQL witness stored-data write: result=%+v err=%v", result, writeErr)
						return true
					}
					witnessWriteDone = true
				}
				if witnessLastProbe.IsZero() || time.Since(witnessLastProbe) >= 5*time.Second {
					witnessLastProbe = time.Now()
					probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
					probe, probeErr := fusedDDLWireQuery(probeCtx, retiringSQL,
						fmt.Sprintf("SELECT id,value,marker FROM %s WHERE id='%s'", witnessRow.Table, witnessRow.ID), false)
					cancel()
					if probeErr != nil || !seamlessScaleSQLMatches(probe, witnessRow) {
						witnessProbeErr = fmt.Errorf("retiring SQL witness keepalive: result=%+v err=%v", probe, probeErr)
						return true
					}
				}
				return !response.SafeToStop && hasSeamlessScaleSessionBlocker(response, retireIDText, retireIncarnation)
			})
			if witnessProbeErr != nil {
				err = witnessProbeErr
			}
			if err != nil || !blocked {
				t.Fatalf("cycle %d did not expose exact live frontend-session blocker: %v", cycle+1, err)
			}
			firstSessionBlocked = true
			if err := retiringSQL.Close(); err != nil {
				t.Fatalf("cycle %d close retiring SQL witness: %v", cycle+1, err)
			}
			firstSessionReleased = true
		}
		var safeResponse clustercontrol.Response
		var stallCaptureErr error
		safe, err := pollSeamlessScaleStatus(ctx, vibedbBinary, profilePath, retire.OperationID, func(response clustercontrol.Response) bool {
			safeResponse = response
			return response.SafeToStop && len(response.Blockers) == 0 &&
				response.RetiringReferences == 0
		}, func(response clustercontrol.Response) {
			processes := append([]*seamlessScaleNodeProcess(nil), physical.nodes...)
			processes = append(processes, targetProcesses...)
			captures, captureErr := captureSeamlessScaleRaftDiagnostics(processes)
			if captureErr != nil {
				stallCaptureErr = captureErr
				return
			}
			artifact := struct {
				Cycle      int                     `json:"cycle"`
				Operation  string                  `json:"operation_id"`
				Status     clustercontrol.Response `json:"status"`
				CapturedAt time.Time               `json:"captured_at"`
				Processes  []json.RawMessage       `json:"process_snapshots"`
			}{cycle + 1, retire.OperationID, response, time.Now().UTC(), captures}
			raw, marshalErr := json.MarshalIndent(artifact, "", "  ")
			if marshalErr == nil {
				if evidencePath := os.Getenv(seamlessScaleEvidenceEnvironment); evidencePath != "" {
					marshalErr = os.WriteFile(fmt.Sprintf("%s.cycle-%d-first-stall-raft.json", evidencePath, cycle+1), append(raw, '\n'), 0o600)
				}
			}
			if marshalErr != nil {
				stallCaptureErr = marshalErr
				t.Errorf("persist first-stall Raft snapshots: %v", marshalErr)
				return
			}
			t.Logf("cycle %d first stalled drain Raft snapshots captured for %d live processes", cycle+1, len(captures))
		})
		if stallCaptureErr != nil && err == nil {
			err = stallCaptureErr
		}
		if err != nil || !safe {
			t.Fatalf("cycle %d decommission did not report safe_to_stop with zero references: %v response=%+v", cycle+1, err, safeResponse)
		}
		applicationMoved = maxUint32(applicationMoved, safeResponse.ApplicationGroupsMoved)
		internalMoved = maxUint32(internalMoved, safeResponse.InternalGroupsMoved)
		finalSafeResponse = safeResponse
		physicalPeak = maxInt(physicalPeak, countSeamlessScaleServingNodes(safeResponse))

		retireDuplicate := runSeamlessScaleCLI(t, ctx, vibedbBinary, "decommission", profilePath,
			"--node", retireIDText, "--incarnation", strconv.FormatUint(retireIncarnation, 10),
			"--request-id", retireRequestID, "--wait", seamlessScaleOperationWait.String())
		if !retireDuplicate.OK || retireDuplicate.OperationID != retire.OperationID {
			duplicateStable = false
			t.Fatalf("cycle %d duplicate decommission changed operation: first=%+v duplicate=%+v", cycle+1, retire, retireDuplicate)
		}

		if cycle == 0 {
			if err := physical.StopAt(ctx, firstRetiringIndex); err != nil {
				t.Fatalf("cycle %d stop original node after safe_to_stop: %v", cycle+1, err)
			}
		} else {
			if err := targetProcesses[cycle-1].StopContext(ctx); err != nil {
				t.Fatalf("cycle %d stop retired target after safe_to_stop: %v", cycle+1, err)
			}
		}
		finalNodesResponse = runSeamlessScaleCLI(t, ctx, vibedbBinary, "nodes", profilePath)
		if !finalNodesResponse.OK || countSeamlessScaleServingNodes(finalNodesResponse) != 3 ||
			hasSeamlessScaleServingNode(finalNodesResponse, retireID) ||
			!validSeamlessScaleRetiredTombstone(finalNodesResponse, retireID, retireIncarnation) {
			t.Fatalf("cycle %d post-stop topology=%+v", cycle+1, finalNodesResponse)
		}
		physicalPeak = maxInt(physicalPeak, countSeamlessScaleServingNodes(finalNodesResponse))
		completedCycles++
		t.Logf("scale cycle %d decommission complete: node=%s operation=%s", cycle+1, retireIDText, retire.OperationID)
	}

	drainSteadyDuringCoverage(t, workload, seamlessScaleSteadyDrainTimeout)
	close(duringStop)
	during := <-duringDone
	after := workload.WindowSet(ctx, seamlessScalePhaseAfter, 3, seamlessScaleWindowDuration, calibratedRate)
	if err := workload.VerifyAllAcknowledgements(ctx); err != nil {
		t.Fatalf("acknowledged data oracle: %v", err)
	}
	// Every acknowledged row has now been verified on the original held SQL
	// session. End the measured continuity interval and release all worker
	// sessions before the independent fresh-session oracle: the workload uses
	// the listener's entire bounded connection budget.
	survivorSessionsStable := workload.SurvivorSessionsStable()
	workload.Close()
	if err := workload.VerifyExactPG(ctx, pgListens[survivorIndex]); err != nil {
		t.Fatalf("post-stop survivor SQL oracle: %v", err)
	}

	evidence := workload.Evidence(baseline, during, after)
	evidence.PhysicalBefore = uint64(countSeamlessScaleServingNodes(nodesResponse))
	evidence.PhysicalPeak = uint64(physicalPeak)
	evidence.PhysicalAfter = uint64(countSeamlessScaleServingNodes(finalNodesResponse))
	evidence.ApplicationGroupsMoved = uint64(applicationMoved)
	evidence.InternalGroupsMoved = uint64(internalMoved)
	evidence.EmptyTargetAtEnrollment = true
	evidence.ControllerRestarted = controllerRestarted
	evidence.TargetRestarted = anyTargetRestarted
	evidence.DuplicateOperationStable = duplicateStable
	evidence.PostRestartOperationRecovered = postRestartProof
	evidence.SafeToStop = finalSafeResponse.SafeToStop && len(finalSafeResponse.Blockers) == 0 &&
		finalSafeResponse.RetiringReferences == 0
	evidence.NodeStopped = completedCycles == uint64(len(emptyTargets))
	evidence.AcknowledgedDataIntact = true
	evidence.NoSkippedSuccess = !t.Skipped()
	evidence.Cycles = completedCycles
	evidence.BaselineWindows = workload.WindowCount(seamlessScalePhaseBaseline)
	evidence.DuringWindows = workload.WindowCount(seamlessScalePhaseDuring)
	evidence.AfterWindows = workload.WindowCount(seamlessScalePhaseAfter)
	evidence.SurvivorSessionStable = survivorSessionsStable
	evidence.RetiringSessionBlocked = firstSessionBlocked
	evidence.RetiringSessionReleased = firstSessionReleased
	evidence.RetiringReferencesAfter = uint64(finalSafeResponse.RetiringReferences)
	evidence.GroupInventoryBeforeDigest = beforeInventoryDigest
	evidence.GroupInventoryAfterDigest = finalNodesResponse.GroupInventoryDigest
	evidence.Budget = aggregateBudget
	evidence.SteadyDuring, evidence.RecoveryDuring, evidence.SteadyWindows, evidence.RecoveryWindows, evidence.FaultInjections = workload.FaultTimingEvidence()
	// Stop every surviving catalog voter before
	// starting any of them: sequential restarts would hide a bootstrap cycle.
	live := []*seamlessScaleNodeProcess{physical.nodes[0], physical.nodes[survivorIndex], targetProcesses[len(targetProcesses)-1]}
	for _, process := range live {
		if err := process.StopContext(ctx); err != nil {
			t.Fatalf("stop all catalog voters for cold recovery: %v", err)
		}
	}
	for index, process := range live {
		live[index] = launchSeamlessScaleNode(t, shardBinary, process.manifest, process.ready)
	}
	for _, process := range live {
		if err := process.ready(ctx, process.manifest); err != nil {
			t.Fatalf("cold catalog quorum recovery: %v\n%s", err, process.diagnostic.String())
		}
	}
	if err := workload.VerifyExactPG(ctx, pgListens[survivorIndex]); err != nil {
		t.Fatalf("cold cluster acknowledged-data oracle: %v", err)
	}
	coldNodes := runSeamlessScaleCLI(t, ctx, vibedbBinary, "nodes", profilePath)
	if !coldNodes.OK || countSeamlessScaleServingNodes(coldNodes) != 3 || coldNodes.GroupInventoryDigest != finalNodesResponse.GroupInventoryDigest {
		t.Fatalf("cold cluster changed committed placement: %+v", coldNodes)
	}
	t.Log("scale cold catalog quorum restart preserved all acknowledgements and placement")
	bounds, err := loadSeamlessScalePerformanceBounds(os.Getenv)
	if err != nil {
		t.Fatalf("performance bounds: %v", err)
	}
	// Preserve measured aggregates before validating them. Diagnostic output
	// cannot be mistaken for the separately emitted successful qualification.
	t.Logf("scale measured evidence: %+v", evidence)
	if path := os.Getenv(seamlessScaleEvidenceEnvironment); path != "" {
		measured, marshalErr := json.Marshal(evidence)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if err := os.WriteFile(path+".measured.json", append(measured, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := evidence.valid(bounds); err != nil {
		t.Fatalf("strict scale qualification evidence: %v", err)
	}
	if path := os.Getenv(seamlessScaleEvidenceEnvironment); path != "" {
		if err := writeSeamlessScaleEvidence(path, evidence); err != nil {
			t.Fatalf("write scale evidence: %v", err)
		}
	} else {
		t.Logf("scale evidence: %+v", evidence)
	}
}

func copySeamlessScaleFailureTree(source, destination string) error {
	if _, err := os.Stat(destination); err == nil {
		return fmt.Errorf("destination already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unsupported failure artifact %s", path)
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, info.Mode().Perm())
		if err != nil {
			_ = input.Close()
			return err
		}
		_, copyErr := io.Copy(output, input)
		closeOutputErr := output.Close()
		closeInputErr := input.Close()
		return errors.Join(copyErr, closeOutputErr, closeInputErr)
	})
}

func writeSeamlessScaleSchemas(root string) error {
	for _, table := range seamlessScaleTables {
		path := filepath.Join(root, table+".sql")
		raw := []byte(fmt.Sprintf("CREATE TABLE %s (id TEXT PRIMARY KEY, value INTEGER NOT NULL, marker TEXT NOT NULL)", table))
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			return err
		}
	}
	return nil
}

func writeSeamlessScaleCA(root string) (certificatePath, keyPath string, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		return "", "", err
	}
	now := time.Now().UTC()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "seamless-scale qualification CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	der, err := x509.CreateCertificate(cryptorand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return "", "", err
	}
	certificatePath = filepath.Join(root, "qualification-ca.pem")
	keyPath = filepath.Join(root, "qualification-ca-key.pem")
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return "", "", err
	}
	if err := os.WriteFile(certificatePath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		return "", "", err
	}
	return certificatePath, keyPath, nil
}

type seamlessScaleManifestTLS struct {
	Certificate string `json:"certificate"`
	Key         string `json:"key"`
	Roots       string `json:"roots"`
	IdentityOID string `json:"identity_oid"`
}

type seamlessScaleManifestIdentity struct {
	TLS seamlessScaleManifestTLS `json:"tls"`
}

func readSeamlessScaleManifestIdentities(path string) ([]seamlessScaleManifestTLS, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var document struct {
		TLS     seamlessScaleManifestTLS       `json:"tls"`
		Gateway *seamlessScaleManifestIdentity `json:"gateway"`
	}
	if err := json.Unmarshal(raw, &document); err != nil || document.TLS.Certificate == "" || document.TLS.Key == "" {
		return nil, errors.New("scale fixture: manifest has no storage TLS identity")
	}
	identities := []seamlessScaleManifestTLS{document.TLS}
	if document.Gateway != nil && document.Gateway.TLS.Certificate != "" {
		identities = append(identities, document.Gateway.TLS)
	}
	return identities, nil
}

func loadSeamlessScaleCA(certificatePath, keyPath string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	certificatePEM, err := os.ReadFile(certificatePath)
	if err != nil {
		return nil, nil, err
	}
	block, rest := pem.Decode(certificatePEM)
	if block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, nil, errors.New("scale fixture: invalid CA certificate")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil || !certificate.IsCA {
		return nil, nil, errors.Join(errors.New("scale fixture: invalid CA certificate"), err)
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, nil, err
	}
	keyBlock, keyRest := pem.Decode(keyPEM)
	if keyBlock == nil || len(bytes.TrimSpace(keyRest)) != 0 {
		return nil, nil, errors.New("scale fixture: invalid CA key")
	}
	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}
	public, ok := certificate.PublicKey.(*ecdsa.PublicKey)
	if !ok || public.X.Cmp(key.PublicKey.X) != 0 || public.Y.Cmp(key.PublicKey.Y) != 0 {
		return nil, nil, errors.New("scale fixture: CA key does not match certificate")
	}
	return certificate, key, nil
}

func writeSeamlessScaleLeaf(certificatePath, keyPath string, ca *x509.Certificate, caKey *ecdsa.PrivateKey, identity rafttransport.PeerIdentity, serial int64) error {
	// Reissuing the fixture CA must preserve the SPKI already pinned by
	// initial provisioning. New target paths receive newly generated keys.
	var key *ecdsa.PrivateKey
	raw, err := os.ReadFile(keyPath)
	if err == nil {
		block, _ := pem.Decode(raw)
		if block == nil {
			return errors.New("scale fixture: invalid existing private key")
		}
		key, err = x509.ParseECPrivateKey(block.Bytes)
	} else if errors.Is(err, os.ErrNotExist) {
		key, err = ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	}
	if err != nil {
		return err
	}
	extension, err := rafttransport.PeerIdentityExtension(asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}, identity)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "seamless-scale qualification identity"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, ExtraExtensions: []pkix.Extension{extension}}
	der, err := x509.CreateCertificate(cryptorand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	certificatePEM := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw})...)
	if err := os.WriteFile(certificatePath, certificatePEM, 0o600); err != nil {
		return err
	}
	return os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600)
}

func reissueSeamlessScaleClusterCredentials(cluster seamlessScaleClusterManifest, certificatePath, keyPath string) (rafttransport.TrustDomain, error) {
	ca, caKey, err := loadSeamlessScaleCA(certificatePath, keyPath)
	if err != nil {
		return rafttransport.TrustDomain{}, err
	}
	identities := make([]seamlessScaleManifestTLS, 0, len(cluster.NodeManifests)*2+1)
	for _, node := range cluster.NodeManifests {
		items, readErr := readSeamlessScaleManifestIdentities(node.ServeManifest)
		if readErr != nil {
			return rafttransport.TrustDomain{}, readErr
		}
		identities = append(identities, items...)
	}
	identities = append(identities, seamlessScaleManifestTLS{Certificate: cluster.ClientCertificate, Key: cluster.ClientKey, Roots: cluster.Roots, IdentityOID: seamlessScaleIdentityOID})
	seen := make(map[string]struct{}, len(identities))
	var domain rafttransport.TrustDomain
	serial := int64(2)
	for _, item := range identities {
		if item.Certificate == "" || item.Key == "" {
			return rafttransport.TrustDomain{}, errors.New("scale fixture: incomplete cluster TLS identity")
		}
		profile, loadErr := servicetls.LoadProfile(item.Certificate, item.Key, item.Roots, item.IdentityOID, time.Now)
		if loadErr != nil {
			return rafttransport.TrustDomain{}, loadErr
		}
		identity := profile.LocalIdentity()
		if domain == (rafttransport.TrustDomain{}) {
			domain = identity.TrustDomain
		} else if identity.TrustDomain != domain {
			return rafttransport.TrustDomain{}, errors.New("scale fixture: cluster identities use different trust domains")
		}
		key := item.Certificate + "\x00" + item.Key
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if err := writeSeamlessScaleLeaf(item.Certificate, item.Key, ca, caKey, identity, serial); err != nil {
			return rafttransport.TrustDomain{}, err
		}
		serial++
	}
	return domain, nil
}

func mintSeamlessScaleTargetCredential(caCertificatePath, caKeyPath, targetCertificatePath, targetKeyPath string, domain rafttransport.TrustDomain, node rafttransport.NodeID, serial int64) error {
	ca, caKey, err := loadSeamlessScaleCA(caCertificatePath, caKeyPath)
	if err != nil {
		return err
	}
	return writeSeamlessScaleLeaf(targetCertificatePath, targetKeyPath, ca, caKey,
		rafttransport.PeerIdentity{TrustDomain: domain, Node: node}, serial)
}

type seamlessScaleNodeLogInput struct {
	KeyID           string                     `json:"key_id"`
	WrappedKey      string                     `json:"wrapped_key"`
	KeyMaterialPath string                     `json:"key_material_path"`
	Options         raftstore.NodeStoreOptions `json:"options"`
}

// buildSeamlessScaleEmptyPreparation delegates the public empty-node grammar
// to the shared RF3 fixture constructor. It intentionally reads only the
// immutable node-log key geometry from the canonical source manifest; all
// target-owned paths, credentials and listeners are supplied as fresh values.
func buildSeamlessScaleEmptyPreparation(sourceManifest, targetRoot, targetCertificate, targetKey, targetNodeKey, policy, roots string, listeners map[string]string, grantNodes []rafttransport.NodeID, gatewaySeeds []nodecontrol.BootstrapGatewaySeed) ([]byte, error) {
	raw, err := os.ReadFile(sourceManifest)
	if err != nil {
		return nil, err
	}
	var source struct {
		NodeLog              seamlessScaleNodeLogInput          `json:"node_log"`
		CanonicalSourceSeeds []nodecontrol.BootstrapGatewaySeed `json:"canonical_source_seeds"`
	}
	if err := vibejson.Unmarshal(raw, &source); err != nil {
		return nil, err
	}
	if source.NodeLog.KeyID == "" || len(grantNodes) == 0 {
		return nil, errors.New("scale fixture: source node-log key or initial grants are incomplete")
	}
	material, err := os.ReadFile(targetNodeKey)
	if err != nil || len(material) != 32 {
		return nil, errors.New("scale fixture: target node-log key material is not a 32-byte key")
	}
	defer clear(material)
	// A serving manifest may omit wrapped metadata: its existing node log
	// owns that header. This fresh fixture node has its own key and provider
	// metadata, so it neither reads nor copies another node's secret.
	key := raftstore.Key{ID: source.NodeLog.KeyID, Wrapped: []byte("seamless-scale-fixture-key")}
	defer clear(key.Material[:])
	copy(key.Material[:], material)
	// The cycle-3 artifact needs at least 1.25 GB across alpha, beta, and gamma.
	// Keep positive pacing but finish well inside the 10-minute drain bound.
	migrationBudget := migrationbudget.DefaultConfig()
	migrationBudget.NetworkSend = migrationbudget.RateLimit{BytesPerSecond: 8 << 20, BurstBytes: 64 << 10}
	migrationBudget.NetworkReceive = migrationbudget.RateLimit{BytesPerSecond: 8 << 20, BurstBytes: 64 << 10}
	options := rf3testfixture.EmptyNodeOptions{Root: targetRoot, NodeIncarnation: 1, Key: key,
		NodeStore: source.NodeLog.Options, Listeners: rf3testfixture.ProcessListeners{
			Peer: listeners["peer"], Native: listeners["native"], Snapshot: listeners["snapshot"], Control: listeners["control"],
		}, Credential: rf3testfixture.Credential{Certificate: targetCertificate, Key: targetKey}, Roots: roots,
		AuthorizationPolicy: policy, GrantNodes: grantNodes, GatewaySeeds: gatewaySeeds,
		CanonicalSourceSeeds: source.CanonicalSourceSeeds, MigrationBudget: &migrationBudget}
	return rf3testfixture.EmptyNodePreparationManifest(options, targetNodeKey)
}

func seamlessScaleInitialNodeIDs(t *testing.T, cluster seamlessScaleClusterManifest) []rafttransport.NodeID {
	t.Helper()
	ids := make([]rafttransport.NodeID, 0, len(cluster.NodeManifests))
	for index, node := range cluster.NodeManifests {
		id, err := parseSeamlessScaleNodeID(node.Node)
		if err != nil {
			t.Fatalf("initial node %d identity: %v", index, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func parseSeamlessScaleNodeID(value string) (rafttransport.NodeID, error) {
	var node rafttransport.NodeID
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != len(node) {
		return node, errors.New("node ID must be exactly 16 lowercase hex bytes")
	}
	copy(node[:], raw)
	if node == (rafttransport.NodeID{}) {
		return node, errors.New("node ID is zero")
	}
	return node, nil
}

func seamlessScaleInitialGatewaySeeds(t *testing.T, cluster seamlessScaleClusterManifest) []nodecontrol.BootstrapGatewaySeed {
	t.Helper()
	seeds := make([]nodecontrol.BootstrapGatewaySeed, 0, len(cluster.NodeManifests))
	for index, node := range cluster.NodeManifests {
		nodeID, err := parseSeamlessScaleNodeID(node.GatewayNode)
		if err != nil {
			t.Fatalf("gateway seed node %d identity: %v", index, err)
		}
		identities, err := readSeamlessScaleManifestIdentities(node.ServeManifest)
		if err != nil || len(identities) < 2 {
			t.Fatalf("gateway seed node %d TLS identity: %v", index, err)
		}
		certificatePEM, err := os.ReadFile(identities[len(identities)-1].Certificate)
		if err != nil {
			t.Fatalf("gateway seed node %d certificate: %v", index, err)
		}
		block, _ := pem.Decode(certificatePEM)
		if block == nil {
			t.Fatalf("gateway seed node %d certificate PEM is empty", index)
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatalf("gateway seed node %d certificate: %v", index, err)
		}
		spki, err := x509.MarshalPKIXPublicKey(certificate.PublicKey)
		if err != nil {
			t.Fatalf("gateway seed node %d public key: %v", index, err)
		}
		pinBytes := sha256.Sum256(spki)
		var pin replication.Digest
		copy(pin[:], pinBytes[:])
		seed := nodecontrol.BootstrapGatewaySeed{NodeID: nodeID, Incarnation: 1, ControlAddress: node.GatewayControl, SPKIPinDigest: pin}
		if !seed.Valid() {
			t.Fatalf("gateway seed node %d is invalid: %+v", index, seed)
		}
		seeds = append(seeds, seed)
	}
	return seeds
}

func writeSeamlessScaleTargetPreparation(t *testing.T, root string, cluster seamlessScaleClusterManifest, sourceManifest string, caCertificate, caKey string, domain rafttransport.TrustDomain, index int, addresses []string, grantNodes []rafttransport.NodeID, gatewaySeeds []nodecontrol.BootstrapGatewaySeed) seamlessScaleTarget {
	t.Helper()
	if len(addresses) != 4 {
		t.Fatalf("target %d requires four listener addresses, got %d", index, len(addresses))
	}
	var nodeID rafttransport.NodeID
	if _, err := io.ReadFull(cryptorand.Reader, nodeID[:]); err != nil || nodeID == (rafttransport.NodeID{}) {
		t.Fatalf("target %d node identity: %v", index, err)
	}
	targetRoot := filepath.Join(root, fmt.Sprintf("empty-node-%d", index+1))
	targetCertificate := filepath.Join(root, fmt.Sprintf("empty-node-%d-cert.pem", index+1))
	targetKey := filepath.Join(root, fmt.Sprintf("empty-node-%d-key.pem", index+1))
	targetNodeKey := filepath.Join(root, fmt.Sprintf("empty-node-%d-node-key", index+1))
	if err := mintSeamlessScaleTargetCredential(caCertificate, caKey, targetCertificate, targetKey, domain, nodeID, int64(100+index)); err != nil {
		t.Fatalf("target %d credential: %v", index, err)
	}
	var keyMaterial [32]byte
	if _, err := io.ReadFull(cryptorand.Reader, keyMaterial[:]); err != nil {
		t.Fatalf("target %d generate node key: %v", index, err)
	}
	if err := os.WriteFile(targetNodeKey, keyMaterial[:], 0o600); err != nil {
		t.Fatalf("target %d target node key: %v", index, err)
	}
	clear(keyMaterial[:])
	listenerNames := []string{"peer", "native", "snapshot", "control"}
	listenerValues := make(map[string]string, len(listenerNames))
	for i, name := range listenerNames {
		listenerValues[name] = addresses[i]
	}
	input, err := buildSeamlessScaleEmptyPreparation(sourceManifest, targetRoot, targetCertificate, targetKey, targetNodeKey, cluster.AuthorizationPolicy, cluster.Roots, listenerValues, grantNodes, gatewaySeeds)
	if err != nil {
		t.Fatalf("target %d canonical empty preparation: %v", index, err)
	}
	inputPath := filepath.Join(root, fmt.Sprintf("empty-node-%d.prepare-node.vibejson", index+1))
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("target %d preparation input: %v", index, err)
	}
	return seamlessScaleTarget{NodeID: nodeID, Incarnation: 1, Certificate: targetCertificate, Key: targetKey, PreparedRoot: targetRoot, Manifest: filepath.Join(targetRoot, "serve-rf3.vibejson"), Descriptor: filepath.Join(targetRoot, "node-descriptor.vibejson")}
}

// writeSeamlessScaleTargetDescriptor derives the public enrollment document
// from the manifest emitted by the real prepare-node-rf3 command. The test
// never invents a private key or a gateway identity in this descriptor: the
// service-key pin is the certificate's SPKI digest and the four addresses are
// the prepared node's authenticated physical listeners.
func writeSeamlessScaleTargetDescriptor(target seamlessScaleTarget) error {
	raw, err := os.ReadFile(target.Manifest)
	if err != nil {
		return err
	}
	var manifest struct {
		NodeIncarnation uint64 `json:"node_incarnation"`
		Listeners       struct {
			Peer     string `json:"peer"`
			Native   string `json:"native"`
			Snapshot string `json:"snapshot"`
			Control  string `json:"control"`
		} `json:"listeners"`
		TLS struct {
			Certificate string `json:"certificate"`
		} `json:"tls"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return err
	}
	if manifest.NodeIncarnation == 0 || manifest.Listeners.Peer == "" || manifest.Listeners.Native == "" ||
		manifest.Listeners.Control == "" || manifest.TLS.Certificate == "" {
		return errors.New("scale fixture: prepared target lacks physical identity/listeners")
	}
	certificatePEM, err := os.ReadFile(manifest.TLS.Certificate)
	if err != nil {
		return err
	}
	block, rest := pem.Decode(certificatePEM)
	if block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) == 0 {
		return errors.New("scale fixture: target certificate chain is incomplete")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return err
	}
	spki, err := x509.MarshalPKIXPublicKey(certificate.PublicKey)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(spki)
	capacity := [7]uint64{1, 1, 1, 1, 1, 1, 1}
	descriptor := clustercontrol.NodeDescriptor{
		Format: clustercontrol.Format, NodeID: hex.EncodeToString(target.NodeID[:]), Incarnation: manifest.NodeIncarnation,
		ServiceKeyDigest: hex.EncodeToString(digest[:]), FailureDomain: fmt.Sprintf("seamless-scale-%d", target.NodeID[0]),
		Roles: []string{"control", "storage"}, DataEndpoint: manifest.Listeners.Peer, NativeEndpoint: manifest.Listeners.Native,
		ControlEndpoint: manifest.Listeners.Control, DataAddress: manifest.Listeners.Peer, NativeAddress: manifest.Listeners.Native,
		ControlAddress: manifest.Listeners.Control, Capacity: capacity, MigrationCapacity: 1, MaxReceives: 1,
	}
	if !descriptor.Valid() {
		return clustercontrol.ErrInvalidNodeDescriptor
	}
	encoded, err := vibejson.Marshal(&descriptor)
	if err != nil {
		return err
	}
	if err := os.WriteFile(target.Descriptor, encoded, 0o600); err != nil {
		return err
	}
	return nil
}

func readSeamlessScaleCluster(t *testing.T, root string) (seamlessScaleClusterManifest, []byte) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "cluster.vibejson"))
	if err != nil {
		t.Fatalf("read cluster manifest: %v", err)
	}
	var cluster seamlessScaleClusterManifest
	if err := vibejson.Unmarshal(raw, &cluster); err != nil {
		t.Fatalf("decode cluster manifest: %v", err)
	}
	return cluster, raw
}

func startSeamlessScaleSupervisor(t *testing.T, ctx context.Context, binary string, args []string, marker string) *fusedSupervisorProcess {
	t.Helper()
	process, err := startFusedSupervisor(binary, args)
	if err != nil {
		t.Fatalf("start scale supervisor: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := process.Stop(stopCtx); err != nil {
			t.Errorf("stop scale supervisor: %v\n%s", err, process.Diagnostics())
		}
	})
	if err := process.WaitReady(ctx, marker); err != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = process.Stop(stopCtx)
		cancel()
		t.Fatalf("scale supervisor readiness: %v\n%s", err, process.Diagnostics())
	}
	return process
}

func validateSeamlessScaleManifestPaths(cluster seamlessScaleClusterManifest) error {
	for name, path := range map[string]string{
		"client certificate": cluster.ClientCertificate, "client key": cluster.ClientKey,
		"roots": cluster.Roots, "authorization policy": cluster.AuthorizationPolicy,
	} {
		if path == "" || !filepath.IsAbs(path) {
			return fmt.Errorf("cluster manifest %s is not absolute: %q", name, path)
		}
	}
	if cluster.ClientEndpoint == "" || cluster.GatewayNode == "" || len(cluster.NodeManifests) != 3 {
		return errors.New("cluster manifest lacks authenticated operator endpoint")
	}
	for index, node := range cluster.NodeManifests {
		if node.Node == "" || node.GatewayNode == "" || node.FrontendListen == "" || node.ServeManifest == "" || len(node.Groups) == 0 {
			return fmt.Errorf("initial node %d is incomplete", index)
		}
	}
	return nil
}

func writeSeamlessScaleOperatorProfile(t *testing.T, root string, cluster seamlessScaleClusterManifest, caCertificate, caKey string, domain rafttransport.TrustDomain) string {
	t.Helper()
	if len(cluster.NodeManifests) < 2 {
		t.Fatal("operator profile requires a surviving second frontend")
	}
	// The retiring frontend is node zero. All operator polls use the
	// authenticated survivor so draining and stopping node zero cannot strand
	// the operation-status client.
	var operator rafttransport.NodeID
	if _, err := cryptorand.Read(operator[:]); err != nil {
		t.Fatal(err)
	}
	certificate, key := filepath.Join(root, "operator-cert.pem"), filepath.Join(root, "operator-key.pem")
	if err := mintSeamlessScaleTargetCredential(caCertificate, caKey, certificate, key, domain, operator, 90); err != nil {
		t.Fatal(err)
	}
	var policy struct {
		Generation uint64 `json:"generation"`
		Principals []struct {
			Node         string   `json:"node"`
			Capabilities []string `json:"capabilities"`
		} `json:"principals"`
	}
	rawPolicy, err := os.ReadFile(cluster.AuthorizationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(rawPolicy, &policy); err != nil {
		t.Fatal(err)
	}
	principal := struct {
		Node         string   `json:"node"`
		Capabilities []string `json:"capabilities"`
	}{hex.EncodeToString(operator[:]), []string{"membership", "topology"}}
	policy.Principals = append(policy.Principals, principal)
	sort.Slice(policy.Principals, func(i, j int) bool { return policy.Principals[i].Node < policy.Principals[j].Node })
	rawPolicy, err = vibejson.Marshal(&policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cluster.AuthorizationPolicy, rawPolicy, 0o600); err != nil {
		t.Fatal(err)
	}
	survivor := cluster.NodeManifests[1]
	profile := clustercontrol.Profile{Format: clustercontrol.Format, Address: survivor.FrontendListen,
		ServerNode: survivor.GatewayNode, Certificate: certificate, Key: key,
		Roots: cluster.Roots, IdentityOID: fusedNodeProcessOID}
	raw, err := vibejson.Marshal(&profile)
	if err != nil {
		t.Fatalf("marshal operator profile: %v", err)
	}
	path := filepath.Join(root, "operator-profile.vibejson")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write operator profile: %v", err)
	}
	return path
}

func mustSeamlessScaleRequestID(t *testing.T) string {
	t.Helper()
	id, err := clustercontrol.NewRequestID()
	if err != nil {
		t.Fatalf("request id: %v", err)
	}
	return id
}

func mustSeamlessScaleNodeID(t *testing.T, value string) rafttransport.NodeID {
	t.Helper()
	var node rafttransport.NodeID
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != len(node) {
		t.Fatalf("node id %q: %v", value, err)
	}
	copy(node[:], raw)
	return node
}

func hasSeamlessScaleNode(response clustercontrol.Response, nodeID rafttransport.NodeID) bool {
	for _, node := range response.Nodes {
		if node.NodeID == hex.EncodeToString(nodeID[:]) {
			return true
		}
	}
	return false
}

func seamlessScaleInitialRetiringIndex(nodeCount int) int {
	if nodeCount <= seamlessScaleFirstRetiringIndex {
		return -1
	}
	return seamlessScaleFirstRetiringIndex
}

func TestSeamlessScaleInitialRetirementLeavesControllerAndSurvivor(t *testing.T) {
	if got := seamlessScaleInitialRetiringIndex(3); got != 2 {
		t.Fatalf("initial retiring index=%d, want 2", got)
	}
	if got := seamlessScaleInitialRetiringIndex(2); got >= 0 {
		t.Fatalf("two-node topology selected retiring index %d", got)
	}
	if seamlessScaleFirstRetiringIndex == seamlessScaleControllerIndex ||
		seamlessScaleFirstRetiringIndex == seamlessScaleSurvivorIndex {
		t.Fatal("initial retiring node must differ from controller and long-lived survivor")
	}
}

func hasSeamlessScaleServingNode(response clustercontrol.Response, nodeID rafttransport.NodeID) bool {
	want := hex.EncodeToString(nodeID[:])
	for _, node := range response.Nodes {
		if node.NodeID != want {
			continue
		}
		switch strings.ToLower(node.Lifecycle) {
		case "retiring", "draining", "decommissioning", "decommissioned", "retired":
			continue
		default:
			return true
		}
	}
	return false
}

// The nodes command retains a decommissioned record as a durable tombstone.
// A stopped target is therefore absent from the serving set while its
// terminal record, when present, must retain the exact incarnation and proof.
func validSeamlessScaleRetiredTombstone(response clustercontrol.Response, nodeID rafttransport.NodeID, incarnation uint64) bool {
	want := hex.EncodeToString(nodeID[:])
	for _, node := range response.Nodes {
		if node.NodeID != want {
			continue
		}
		if strings.ToLower(node.Lifecycle) != "decommissioned" || node.Incarnation != incarnation || !node.SafeToStop {
			return false
		}
	}
	return true
}

func TestSeamlessScalePostStopTopologyAcceptsTerminalTombstone(t *testing.T) {
	nodeID := mustSeamlessScaleNodeID(t, "11111111111111111111111111111111")
	active := clustercontrol.NodeStatus{NodeID: hex.EncodeToString(nodeID[:]), Incarnation: 7, Lifecycle: "active"}
	valid := clustercontrol.NodeStatus{NodeID: hex.EncodeToString(nodeID[:]), Incarnation: 7, Lifecycle: "decommissioned", SafeToStop: true}
	for _, test := range []struct {
		name    string
		node    clustercontrol.NodeStatus
		valid   bool
		serving bool
	}{
		{name: "active retired node is serving", node: active, valid: false, serving: true},
		{name: "unsafe terminal tombstone is rejected", node: clustercontrol.NodeStatus{NodeID: active.NodeID, Incarnation: 7, Lifecycle: "decommissioned"}, valid: false},
		{name: "wrong incarnation tombstone is rejected", node: clustercontrol.NodeStatus{NodeID: active.NodeID, Incarnation: 8, Lifecycle: "decommissioned", SafeToStop: true}, valid: false},
		{name: "safe terminal tombstone is allowed", node: valid, valid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := clustercontrol.Response{OK: true, Nodes: []clustercontrol.NodeStatus{test.node}}
			if got := validSeamlessScaleRetiredTombstone(response, nodeID, 7); got != test.valid {
				t.Fatalf("valid tombstone=%v, want %v: response=%+v", got, test.valid, response)
			}
			if got := hasSeamlessScaleServingNode(response, nodeID); got != test.serving {
				t.Fatalf("serving=%v, want %v: response=%+v", got, test.serving, response)
			}
		})
	}
	if !validSeamlessScaleRetiredTombstone(clustercontrol.Response{OK: true}, nodeID, 7) {
		t.Fatal("absent terminal tombstone should be allowed for a compacted node response")
	}
}

func hasSeamlessScaleSessionBlocker(response clustercontrol.Response, nodeID string, incarnation uint64) bool {
	for _, blocker := range response.Blockers {
		if blocker.Code == clustercontrol.BlockerGatewaySessions && blocker.NodeID == nodeID && blocker.NodeIncarnation == incarnation &&
			strings.Contains(strings.ToLower(blocker.Detail), "session") {
			return true
		}
	}
	return false
}

func TestHasSeamlessScaleSessionBlockerUsesCanonicalCode(t *testing.T) {
	const nodeID = "11111111111111111111111111111111"
	response := clustercontrol.Response{Blockers: []clustercontrol.Blocker{{
		Code: clustercontrol.BlockerGatewaySessions, Detail: "retiring frontend still has one authenticated session",
		NodeID: nodeID, NodeIncarnation: 7,
	}}}
	if !hasSeamlessScaleSessionBlocker(response, nodeID, 7) {
		t.Fatal("canonical gateway session blocker was not recognized")
	}
	for _, test := range []struct {
		name        string
		code        string
		nodeID      string
		incarnation uint64
		detail      string
	}{
		{name: "legacy singular code", code: "gateway_session", nodeID: nodeID, incarnation: 7, detail: "retiring frontend still has one authenticated session"},
		{name: "foreign node", code: clustercontrol.BlockerGatewaySessions, nodeID: "22222222222222222222222222222222", incarnation: 7, detail: "retiring frontend still has one authenticated session"},
		{name: "foreign incarnation", code: clustercontrol.BlockerGatewaySessions, nodeID: nodeID, incarnation: 8, detail: "retiring frontend still has one authenticated session"},
		{name: "unrelated detail", code: clustercontrol.BlockerGatewaySessions, nodeID: nodeID, incarnation: 7, detail: "no remaining references"},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := clustercontrol.Response{Blockers: []clustercontrol.Blocker{{
				Code: test.code, Detail: test.detail, NodeID: test.nodeID, NodeIncarnation: test.incarnation,
			}}}
			if hasSeamlessScaleSessionBlocker(response, nodeID, 7) {
				t.Fatal("invalid gateway session blocker was accepted")
			}
		})
	}
}

// A refresh that fails while the gateway restarts must leave the worker's
// gate unopened without panicking the next read: the read reports the dial
// failure as a sample error and reopens lazily once the gateway returns.
func TestSeamlessScaleGatewayReadSurvivesFailedReconnect(t *testing.T) {
	dialErr := errors.New("dial gateway: connection refused")
	opens := 0
	pipe, peer := net.Pipe()
	defer pipe.Close()
	defer peer.Close()
	connection := &seamlessScaleConnection{openGate: func(context.Context) (net.Conn, error) {
		opens++
		return nil, dialErr
	}}
	workload := &seamlessScaleWorkload{}
	row := seamlessScaleAck{Table: "scale_a", ID: "seed-scale_a-0000"}
	if _, _, err := workload.gatewayRead(t.Context(), connection, row); !errors.Is(err, dialErr) {
		t.Fatalf("gateway read with refused dial err=%v", err)
	}
	if connection.gate != nil || connection.reader != nil {
		t.Fatal("failed gateway reopen must leave the connection unopened")
	}
	if err := connection.refreshGatewayAfterIncompleteResponse(t.Context()); !errors.Is(err, dialErr) {
		t.Fatalf("gateway refresh with refused dial err=%v", err)
	}
	connection.openGate = func(context.Context) (net.Conn, error) {
		opens++
		return pipe, nil
	}
	if err := connection.openGatewayConnection(t.Context()); err != nil {
		t.Fatalf("gateway reopen after refused dial err=%v", err)
	}
	if connection.gate == nil || connection.reader == nil {
		t.Fatal("successful gateway reopen left the connection unopened")
	}
	before := opens
	if err := connection.openGatewayConnection(t.Context()); err != nil || opens != before {
		t.Fatalf("open gateway connection reopened a live gate err=%v opens=%d", err, opens)
	}
}

func seamlessScaleNodeIncarnation(response clustercontrol.Response, nodeID string) uint64 {
	for _, node := range response.Nodes {
		if node.NodeID == nodeID {
			return node.Incarnation
		}
	}
	return 0
}

func seamlessScaleTerminalSuccess(response clustercontrol.Response) bool {
	return response.OK && response.OperationID != "" &&
		(response.State == "complete" || response.State == "completed" || response.State == "succeeded")
}

func seamlessScalePacingObserved(response clustercontrol.Response) bool {
	if response.Budget == nil || response.Budget.ThrottledCalls == 0 || response.Budget.ThrottledBytes == 0 ||
		seamlessScaleTerminalSuccess(response) {
		return false
	}
	switch strings.ToLower(response.Phase) {
	case "moving", "copying":
		return true
	default:
		return false
	}
}

func TestSeamlessScalePacingObservedRequiresActiveCanonicalPhase(t *testing.T) {
	positive := clustercontrol.Response{OK: true, OperationID: "operation", State: "running", Phase: "moving",
		Budget: &clustercontrol.BudgetStatus{ThrottledCalls: 1, ThrottledBytes: 1}}
	if !seamlessScalePacingObserved(positive) {
		t.Fatal("active moving response with positive budget was rejected")
	}
	for name, response := range map[string]clustercontrol.Response{
		"completed": {OK: true, OperationID: "operation", State: "complete", Phase: "moving",
			Budget: &clustercontrol.BudgetStatus{ThrottledCalls: 1, ThrottledBytes: 1}},
		"noncanonical phase": {OK: true, OperationID: "operation", State: "running", Phase: "move",
			Budget: &clustercontrol.BudgetStatus{ThrottledCalls: 1, ThrottledBytes: 1}},
		"zero budget": {OK: true, OperationID: "operation", State: "running", Phase: "moving"},
	} {
		if seamlessScalePacingObserved(response) {
			t.Fatalf("%s response incorrectly proved active pacing", name)
		}
	}
}

func runSeamlessScaleCommand(ctx context.Context, binary string, args ...string) int {
	command := exec.CommandContext(ctx, binary, args...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			return exit.ExitCode()
		}
		return 1
	}
	return 0
}

func runSeamlessScaleCLI(t *testing.T, ctx context.Context, binary, operation, profile string, extra ...string) clustercontrol.Response {
	t.Helper()
	args := []string{"cluster", operation, "--profile", profile, "--json"}
	args = append(args, extra...)
	command := exec.CommandContext(ctx, binary, args...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	raw := stdout.Bytes()
	if len(raw) != 0 && raw[len(raw)-1] == '\n' {
		raw = raw[:len(raw)-1]
	}
	response, decodeErr := clustercontrol.DecodeResponse(append(raw, '\n'))
	if decodeErr != nil {
		t.Fatalf("cluster %s decode response: run=%v stdout=%q stderr=%q decode=%v", operation, err, stdout.String(), stderr.String(), decodeErr)
	}
	if err != nil && response.OK {
		t.Fatalf("cluster %s failed after an OK response: %v stderr=%s", operation, err, stderr.String())
	}
	return response
}

func pollSeamlessScaleStatus(
	ctx context.Context, binary, profile, operationID string,
	predicate func(clustercontrol.Response) bool,
	stallObservers ...func(clustercontrol.Response),
) (bool, error) {
	deadline := time.NewTimer(10 * time.Minute)
	defer deadline.Stop()
	var prior string
	var lastResponse clustercontrol.Response
	var stallKey string
	var stallSince time.Time
	stallCaptured := false
	observeStall := func(response clustercontrol.Response) {
		if len(stallObservers) == 0 || stallObservers[0] == nil ||
			(response.State != "running" && response.State != "draining") || response.Phase == "" {
			stallKey, stallSince = "", time.Time{}
			return
		}
		evidence := response.Evidence
		blockerCounts := make(map[string]int, len(response.Blockers))
		moveFailures := make([]string, 0, 1)
		for _, blocker := range response.Blockers {
			blockerCounts[blocker.Code]++
			// Keep the first-stall clock tied to the durable move cursor and
			// its last execution error. Other blocker details often include
			// catalog or directory revisions that can churn without advancing
			// the move and must not postpone the diagnostic snapshot.
			if blocker.Code == "move_execution" {
				moveFailures = append(moveFailures, blocker.Detail)
			}
		}
		blockerCodes := make([]string, 0, len(blockerCounts))
		for code := range blockerCounts {
			blockerCodes = append(blockerCodes, code)
		}
		sort.Strings(blockerCodes)
		sort.Strings(moveFailures)
		key := fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d/%t/%v/%v", response.State, response.Phase,
			response.ApplicationGroupsMoved, response.InternalGroupsMoved, response.RetiringReferences,
			seamlessScaleOutstandingMoves(evidence), seamlessScaleServingReplicas(evidence), response.SafeToStop,
			blockerCodes, moveFailures)
		if key != stallKey {
			stallKey, stallSince = key, time.Now()
			return
		}
		if !stallCaptured && time.Since(stallSince) >= 30*time.Second {
			stallCaptured = true
			stallObservers[0](response)
		}
	}
	for {
		response, err := runSeamlessScaleCLIForPoll(ctx, binary, profile, operationID)
		if err != nil {
			// A rejected status reflects the live control plane's momentary
			// view, not a broken poll: a leader-election gap right after a
			// membership change (ActionAddLearner/PromoteVoter/TransferLeader)
			// is expected Raft behavior, typically resolving within one
			// election timeout. Treat it like any other not-yet-converged
			// state and keep polling until the overall deadline; any other
			// error (process failure, malformed response, wrong operation ID)
			// is a genuine poll failure and still fails fast.
			if !errors.Is(err, errSeamlessScaleClusterStatusRejected) {
				return false, err
			}
			state := err.Error()
			if state != prior {
				fmt.Printf("scale operation %s: %s\n", operationID, state)
				prior = state
			}
		} else {
			budget := "nil"
			if response.Budget != nil {
				budget = fmt.Sprintf("calls=%d bytes=%d peak=%d max=%d", response.Budget.ThrottledCalls,
					response.Budget.ThrottledBytes, response.Budget.PeakActive, response.Budget.MaxActive)
			}
			state := fmt.Sprintf("state=%s phase=%s budget={%s} blockers=%+v", response.State, response.Phase, budget, response.Blockers)
			if state != prior {
				fmt.Printf("scale operation %s: %s\n", operationID, state)
				prior = state
			}
			if predicate(response) {
				return true, nil
			}
			lastResponse = response
		}
		observeStall(lastResponse)
		select {
		case <-ctx.Done():
			return false, context.Cause(ctx)
		case <-deadline.C:
			return false, errors.New("operation status polling deadline exceeded")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func seamlessScaleOutstandingMoves(evidence *clustercontrol.SafeToStopEvidence) uint32 {
	if evidence == nil {
		return 0
	}
	return evidence.OutstandingMoves
}

func seamlessScaleServingReplicas(evidence *clustercontrol.SafeToStopEvidence) uint32 {
	if evidence == nil {
		return 0
	}
	return evidence.ServingReplicas
}

func captureSeamlessScaleRaftDiagnostics(
	processes []*seamlessScaleNodeProcess,
) ([]json.RawMessage, error) {
	const diagnosticPrefix = "VIBEDB_RF3_DIAGNOSTIC "
	type target struct {
		process *seamlessScaleNodeProcess
		command *exec.Cmd
		exited  chan struct{}
		serial  uint64
	}
	unique := make(map[*seamlessScaleNodeProcess]struct{}, len(processes))
	targets := make([]target, 0, len(processes))
	for _, process := range processes {
		if process == nil {
			continue
		}
		command, exited, diagnostic := process.runtimeSnapshot()
		if command == nil || command.Process == nil || diagnostic == nil {
			continue
		}
		if _, duplicate := unique[process]; duplicate {
			continue
		}
		unique[process] = struct{}{}
		if exited != nil {
			select {
			case <-exited:
				continue
			default:
			}
		}
		serial, _, _ := latestSeamlessScaleRaftSnapshot(process, diagnosticPrefix)
		targets = append(targets, target{process: process, command: command, exited: exited, serial: serial})
	}
	if len(targets) == 0 {
		return nil, errors.New("first-stall diagnostic has no live RF3 processes")
	}
	type signalResult struct {
		process *seamlessScaleNodeProcess
		err     error
	}
	signals := make(chan signalResult, len(targets))
	for _, current := range targets {
		go func(current target) {
			signals <- signalResult{process: current.process, err: current.command.Process.Signal(syscall.SIGUSR1)}
		}(current)
	}
	for range targets {
		result := <-signals
		if result.err != nil {
			_, exited, _ := result.process.runtimeSnapshot()
			if exited == nil {
				return nil, fmt.Errorf("signal RF3 process for diagnostic: %w", result.err)
			}
			select {
			case <-exited:
			default:
				command, _, _ := result.process.runtimeSnapshot()
				pid := 0
				if command != nil && command.Process != nil {
					pid = command.Process.Pid
				}
				return nil, fmt.Errorf("signal RF3 process %d for diagnostic: %w", pid, result.err)
			}
		}
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	captures := make([]json.RawMessage, len(targets))
	found := make([]bool, len(targets))
	remaining := len(targets)
	for remaining != 0 {
		for index, current := range targets {
			if found[index] {
				continue
			}
			serial, raw, ok := latestSeamlessScaleRaftSnapshot(current.process, diagnosticPrefix)
			if !ok || serial <= current.serial {
				continue
			}
			var record struct {
				Groups     int                `json:"groups"`
				RaftGroups *[]json.RawMessage `json:"raft_groups"`
			}
			if err := json.Unmarshal(raw, &record); err != nil || record.RaftGroups == nil && record.Groups != 0 {
				continue
			}
			captures[index] = raw
			found[index] = true
			remaining--
		}
		if remaining == 0 {
			return captures, nil
		}
		select {
		case <-timer.C:
			return captures, fmt.Errorf("only %d of %d live RF3 processes emitted fresh Raft snapshots", len(targets)-remaining, len(targets))
		case <-ticker.C:
		}
	}
	return captures, nil
}

type seamlessScaleDiagnosticEndpointProbe struct {
	NodeID    string        `json:"node_id"`
	Address   string        `json:"address"`
	StartedAt time.Time     `json:"started_at"`
	Elapsed   time.Duration `json:"elapsed_ns"`
	Connected bool          `json:"connected"`
	Error     string        `json:"error,omitempty"`
}

type seamlessScaleDiagnosticSourceRecord struct {
	NodeID    string `json:"node_id"`
	GroupID   string `json:"group_id"`
	Operation string `json:"operation_id"`
	Revision  uint64 `json:"revision"`
	State     uint8  `json:"state"`
	Bytes     int    `json:"bytes"`
}

type seamlessScaleDiagnosticJournalEntry struct {
	Name         string   `json:"name"`
	Directory    bool     `json:"directory"`
	ChildEntries []string `json:"child_entries,omitempty"`
}

type seamlessScaleDiagnosticSourceJournal struct {
	NodeID  string                                `json:"node_id"`
	Path    string                                `json:"path"`
	Present bool                                  `json:"present"`
	Error   string                                `json:"error,omitempty"`
	Entries []seamlessScaleDiagnosticJournalEntry `json:"entries,omitempty"`
}

type seamlessScaleDiagnosticFileMarker struct {
	Name    string `json:"name"`
	Present bool   `json:"present"`
	IsDir   bool   `json:"is_dir,omitempty"`
	Bytes   int64  `json:"bytes,omitempty"`
}

type seamlessScaleFirstStallDetails struct {
	CapturedAt            time.Time                              `json:"captured_at"`
	MoveExecutionFailures []clustercontrol.Blocker               `json:"move_execution_failures,omitempty"`
	Endpoints             []seamlessScaleDiagnosticEndpointProbe `json:"control_endpoint_probes,omitempty"`
	SourceJournals        []seamlessScaleDiagnosticSourceJournal `json:"source_export_journals,omitempty"`
	SourceJournalRecords  []seamlessScaleDiagnosticSourceRecord  `json:"source_export_journal_records,omitempty"`
	TargetMarkers         []seamlessScaleDiagnosticFileMarker    `json:"target_runtime_markers,omitempty"`
}

func captureSeamlessScaleFirstStallDetails(
	response clustercontrol.Response,
	cluster seamlessScaleClusterManifest,
	target *seamlessScaleTarget,
) seamlessScaleFirstStallDetails {
	details := seamlessScaleFirstStallDetails{CapturedAt: time.Now().UTC()}
	for _, blocker := range response.Blockers {
		if blocker.Code == "move_execution" {
			details.MoveExecutionFailures = append(details.MoveExecutionFailures, blocker)
		}
	}
	type manifestProbe struct {
		nodeID string
		path   string
	}
	manifests := make([]manifestProbe, 0, len(cluster.NodeManifests)+1)
	for _, node := range cluster.NodeManifests {
		manifests = append(manifests, manifestProbe{nodeID: node.Node, path: node.ServeManifest})
	}
	if target != nil {
		manifests = append(manifests, manifestProbe{nodeID: hex.EncodeToString(target.NodeID[:]), path: target.Manifest})
	}
	seenEndpoints := make(map[string]struct{}, len(manifests))
	for _, current := range manifests {
		raw, err := os.ReadFile(current.path)
		if err != nil {
			details.Endpoints = append(details.Endpoints, seamlessScaleDiagnosticEndpointProbe{
				NodeID: current.nodeID, StartedAt: time.Now().UTC(), Error: "read manifest: " + err.Error(),
			})
			continue
		}
		var manifest struct {
			Listeners struct {
				Control string `json:"control"`
			} `json:"listeners"`
			ReplicaControl struct {
				SourceJournalPath string `json:"source_journal_path"`
			} `json:"replica_control"`
		}
		if err = json.Unmarshal(raw, &manifest); err != nil {
			details.Endpoints = append(details.Endpoints, seamlessScaleDiagnosticEndpointProbe{
				NodeID: current.nodeID, StartedAt: time.Now().UTC(), Error: "decode manifest: " + err.Error(),
			})
			continue
		}
		address := manifest.Listeners.Control
		if address != "" {
			if _, duplicate := seenEndpoints[address]; !duplicate {
				seenEndpoints[address] = struct{}{}
				startedAt, started := time.Now().UTC(), time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
				connection, dialErr := (&net.Dialer{}).DialContext(ctx, "tcp", address)
				cancel()
				probe := seamlessScaleDiagnosticEndpointProbe{NodeID: current.nodeID, Address: address,
					StartedAt: startedAt, Elapsed: time.Since(started), Connected: dialErr == nil}
				if dialErr != nil {
					probe.Error = dialErr.Error()
				} else {
					_ = connection.Close()
				}
				details.Endpoints = append(details.Endpoints, probe)
			}
		}
		if manifest.ReplicaControl.SourceJournalPath != "" {
			details.SourceJournals = append(details.SourceJournals,
				inspectSeamlessScaleSourceJournal(current.nodeID, manifest.ReplicaControl.SourceJournalPath))
			details.SourceJournalRecords = append(details.SourceJournalRecords,
				seamlessScaleReadSourceJournalRecords(current.nodeID, manifest.ReplicaControl.SourceJournalPath)...)
		}
	}
	if target != nil {
		root := target.PreparedRoot
		for _, marker := range []struct{ name, path string }{
			{"serve_manifest", target.Manifest}, {"descriptor", target.Descriptor},
			{"adopted_groups_state", filepath.Join(root, "adopted-groups.state")},
			{"node_control_journal", filepath.Join(root, "node-control-journal")},
			{"replica_action_journal", filepath.Join(root, "replica-actions")},
			{"rf3_diagnostics", filepath.Join(root, "rf3-diagnostics.json")},
			{"enrollments", filepath.Join(root, "enrollments")},
		} {
			info, err := os.Stat(marker.path)
			entry := seamlessScaleDiagnosticFileMarker{Name: marker.name, Present: err == nil}
			if err == nil {
				entry.IsDir, entry.Bytes = info.IsDir(), info.Size()
			}
			details.TargetMarkers = append(details.TargetMarkers, entry)
		}
	}
	return details
}

func inspectSeamlessScaleSourceJournal(nodeID, root string) seamlessScaleDiagnosticSourceJournal {
	const maxJournalEntries = 32
	result := seamlessScaleDiagnosticSourceJournal{NodeID: nodeID, Path: root}
	entries, err := os.ReadDir(root)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.Present = true
	for index, entry := range entries {
		if index == maxJournalEntries {
			break
		}
		current := seamlessScaleDiagnosticJournalEntry{Name: entry.Name(), Directory: entry.IsDir()}
		if entry.IsDir() {
			children, childErr := os.ReadDir(filepath.Join(root, entry.Name()))
			if childErr == nil {
				current.ChildEntries = make([]string, 0, len(children))
				for childIndex, child := range children {
					if childIndex == maxJournalEntries {
						break
					}
					current.ChildEntries = append(current.ChildEntries, child.Name())
				}
			}
		}
		result.Entries = append(result.Entries, current)
	}
	return result
}

func seamlessScaleReadSourceJournalRecords(nodeID, root string) []seamlessScaleDiagnosticSourceRecord {
	const maxDiagnosticRecords = 128
	var records []seamlessScaleDiagnosticSourceRecord
	entries, err := os.ReadDir(root)
	if err != nil {
		return records
	}
	for _, entry := range entries {
		if len(records) >= maxDiagnosticRecords {
			break
		}
		path := filepath.Join(root, entry.Name())
		if entry.IsDir() {
			children, childErr := os.ReadDir(path)
			if childErr != nil {
				continue
			}
			for _, child := range children {
				if len(records) >= maxDiagnosticRecords {
					break
				}
				if !child.IsDir() && strings.HasPrefix(child.Name(), "s-") {
					readSeamlessScaleSourceRecord(nodeID, filepath.Join(path, child.Name()), &records)
				}
			}
		} else if strings.HasPrefix(entry.Name(), "s-") {
			readSeamlessScaleSourceRecord(nodeID, path, &records)
		}
	}
	return records
}

func readSeamlessScaleSourceRecord(nodeID, path string, records *[]seamlessScaleDiagnosticSourceRecord) {
	const sourceJournalHeaderBytes = 32
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) < sourceJournalHeaderBytes+snapshottransfer.SourceControlRequestBytes ||
		len(raw) > snapshottransfer.MaxSourceJournalBytes {
		return
	}
	request, err := snapshottransfer.OpenSourceControlRequest(raw[sourceJournalHeaderBytes : sourceJournalHeaderBytes+snapshottransfer.SourceControlRequestBytes])
	if err != nil {
		return
	}
	*records = append(*records, seamlessScaleDiagnosticSourceRecord{
		NodeID: nodeID, GroupID: hex.EncodeToString(request.Group.GroupID[:]),
		Operation: hex.EncodeToString(request.Operation[:]), Revision: binary.BigEndian.Uint64(raw[16:24]),
		State: raw[8], Bytes: len(raw),
	})
}

func latestSeamlessScaleRaftSnapshot(
	process *seamlessScaleNodeProcess, prefix string,
) (uint64, json.RawMessage, bool) {
	if process == nil {
		return 0, nil, false
	}
	_, _, diagnostic := process.runtimeSnapshot()
	if diagnostic == nil {
		return 0, nil, false
	}
	output := diagnostic.String()
	start := strings.LastIndex(output, prefix)
	if start < 0 {
		return 0, nil, false
	}
	start += len(prefix)
	end := strings.IndexByte(output[start:], '\n')
	if end < 0 {
		end = len(output) - start
	}
	raw := json.RawMessage(strings.TrimSpace(output[start : start+end]))
	var record struct {
		Event  string `json:"event"`
		Serial uint64 `json:"serial"`
	}
	if json.Unmarshal(raw, &record) != nil || record.Event != "snapshot" || record.Serial == 0 {
		return 0, nil, false
	}
	return record.Serial, append(json.RawMessage(nil), raw...), true
}

func waitSeamlessScaleOperation(ctx context.Context, binary, profile, operationID string,
	predicate func(clustercontrol.Response) bool, stallObservers ...func(clustercontrol.Response),
) error {
	if _, err := pollSeamlessScaleStatus(ctx, binary, profile, operationID, predicate, stallObservers...); err != nil {
		return fmt.Errorf("operation %s did not reach requested state: %w", operationID, err)
	}
	return nil
}

func runSeamlessScaleCLIForPoll(ctx context.Context, binary, profile, operationID string) (clustercontrol.Response, error) {
	command := exec.CommandContext(ctx, binary, "cluster", "status", "--profile", profile, "--operation", operationID, "--json", "--wait", seamlessScaleOperationWait.String())
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil && stdout.Len() == 0 {
		if seamlessScaleTransientPollFailure(stderr.String()) {
			return clustercontrol.Response{}, fmt.Errorf("%w %s: cluster status failed: %v: %s",
				errSeamlessScaleClusterStatusRejected, operationID, err, stderr.String())
		}
		return clustercontrol.Response{}, fmt.Errorf("cluster status failed: %w: %s", err, stderr.String())
	}
	raw := stdout.Bytes()
	if len(raw) != 0 && raw[len(raw)-1] == '\n' {
		raw = raw[:len(raw)-1]
	}
	response, err := clustercontrol.DecodeResponse(append(raw, '\n'))
	if err != nil {
		return clustercontrol.Response{}, fmt.Errorf("cluster status decode: %w: %s", err, stderr.String())
	}
	if response.OperationID != operationID {
		return clustercontrol.Response{}, fmt.Errorf("cluster status operation mismatch: got %q want %q", response.OperationID, operationID)
	}
	if !response.OK {
		return clustercontrol.Response{}, fmt.Errorf("%w %s: %s", errSeamlessScaleClusterStatusRejected, operationID, response.Error)
	}
	return response, nil
}

// errSeamlessScaleClusterStatusRejected marks a cluster-status poll that
// either completed and decoded but reported OK=false, or never reached a
// listener at all because the CLI's own dial raced a control-plane endpoint
// that has not finished binding yet (an expected transient during startup
// or right after a membership change). pollSeamlessScaleStatus retries both
// like any other not-yet-converged state instead of failing immediately;
// every other runSeamlessScaleCLIForPoll error (a decodable-but-malformed
// response, mismatched operation ID, or a process failure that is not a
// recognized transient connectivity error) is unwrapped and still fails the
// poll right away.
var errSeamlessScaleClusterStatusRejected = errors.New("cluster status rejected operation")

// seamlessScaleTransientPollFailure reports whether stderr from a failed
// cluster-status invocation names a plain connectivity race rather than a
// genuine defect: the CLI process itself never received a decodable
// response because the target listener was not yet accepting connections,
// closed the connection early, or the leader was momentarily unknown. Every
// one of these recurs naturally around a membership change or a process
// still starting; a real bug (bad arguments, a decode/logic error) does not
// produce these specific network-layer strings.
func seamlessScaleTransientPollFailure(stderr string) bool {
	for _, substring := range []string{
		"connection refused", "EOF", "connection reset",
		"i/o timeout", "no reachable leader", "no route to host",
	} {
		if strings.Contains(stderr, substring) {
			return true
		}
	}
	return false
}

func digestSeamlessScaleBytes(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// The remaining helpers are deliberately small process wrappers. They are
// kept independent from the existing fused RF3 fixture so a scale test cannot
// accidentally assert a static three-node layout after a node has moved.
type seamlessScaleNodeProcess struct {
	runtimeMu  sync.RWMutex
	command    *exec.Cmd
	diagnostic *rf3testfixture.ProcessDiagnostic
	exited     chan struct{}
	manifest   string
	ready      func(context.Context, string) error
	instance   uint64
}

func (process *seamlessScaleNodeProcess) runtimeSnapshot() (*exec.Cmd, chan struct{}, *rf3testfixture.ProcessDiagnostic) {
	if process == nil {
		return nil, nil, nil
	}
	process.runtimeMu.RLock()
	command, exited, diagnostic := process.command, process.exited, process.diagnostic
	process.runtimeMu.RUnlock()
	return command, exited, diagnostic
}

func seamlessScaleProcessDirectOutcomes(processes []*seamlessScaleNodeProcess) []string {
	const maxOutcomesPerProcess = 64
	var outcomes []string
	for _, process := range processes {
		if process == nil {
			continue
		}
		command, _, diagnostic := process.runtimeSnapshot()
		if diagnostic == nil {
			continue
		}
		pid := 0
		if command != nil && command.Process != nil {
			pid = command.Process.Pid
		}
		captured := 0
		for _, line := range strings.Split(diagnostic.String(), "\n") {
			if !strings.Contains(line, "VIBEDB_RF3_DIRECT_SQL_OUTCOME") &&
				!strings.Contains(line, "VIBEDB_RF3_DIRECT_ABORT_ATTEMPT") &&
				!strings.Contains(line, "VIBEDB_RF3_TEST_CHILD_ENV") {
				continue
			}
			outcomes = append(outcomes, fmt.Sprintf("pid=%d %s", pid, line))
			captured++
			if captured == maxOutcomesPerProcess {
				break
			}
		}
	}
	return outcomes
}

type seamlessScalePhysicalCluster struct {
	nodes []*seamlessScaleNodeProcess
}

func startSeamlessScalePhysicalCluster(t *testing.T, ctx context.Context, binary string, cluster seamlessScaleClusterManifest) *seamlessScalePhysicalCluster {
	t.Helper()
	physical := &seamlessScalePhysicalCluster{nodes: make([]*seamlessScaleNodeProcess, 0, len(cluster.NodeManifests))}
	for index, node := range cluster.NodeManifests {
		if node.ServeManifest == "" {
			t.Fatalf("physical node %d has no serve manifest", index+1)
		}
		physical.nodes = append(physical.nodes, launchSeamlessScaleNode(t, binary, node.ServeManifest, waitSeamlessScaleManifestGateway))
	}
	// Start every voter before waiting for a gateway: opening the replicated
	// catalog requires a quorum of those same processes.
	for _, process := range physical.nodes {
		if err := process.ready(ctx, process.manifest); err != nil {
			t.Fatalf("physical node readiness: %v\n%s", err, process.diagnostic.String())
		}
	}
	t.Cleanup(func() {
		for index := len(physical.nodes) - 1; index >= 0; index-- {
			physical.nodes[index].Stop(t)
		}
	})
	return physical
}

func (physical *seamlessScalePhysicalCluster) Restart(ctx context.Context, index int) error {
	if physical == nil || index < 0 || index >= len(physical.nodes) {
		return errors.New("physical node index out of range")
	}
	return physical.nodes[index].Restart(ctx)
}

func (physical *seamlessScalePhysicalCluster) StopAt(ctx context.Context, index int) error {
	if physical == nil || index < 0 || index >= len(physical.nodes) {
		return errors.New("physical node index out of range")
	}
	return physical.nodes[index].StopContext(ctx)
}

func startSeamlessScaleEmptyNode(t *testing.T, ctx context.Context, binary, manifest string, clientProfile *rafttransport.PeerTLS, target rafttransport.NodeID) *seamlessScaleNodeProcess {
	t.Helper()
	ready := func(readyCtx context.Context, readyManifest string) error {
		return waitSeamlessScaleManifestControl(readyCtx, readyManifest, clientProfile, target)
	}
	return startSeamlessScaleNodeReady(t, ctx, binary, manifest, ready)
}

func startSeamlessScaleNodeReady(t *testing.T, ctx context.Context, binary, manifest string, ready func(context.Context, string) error) *seamlessScaleNodeProcess {
	t.Helper()
	process := launchSeamlessScaleNode(t, binary, manifest, ready)
	if err := ready(ctx, manifest); err != nil {
		t.Fatalf("node readiness: %v\n%s", err, process.diagnostic.String())
	}
	return process
}

func launchSeamlessScaleNode(t *testing.T, binary, manifest string, ready func(context.Context, string) error) *seamlessScaleNodeProcess {
	t.Helper()
	process := &seamlessScaleNodeProcess{command: exec.Command(binary, "serve-node", "-manifest", manifest, "-reload-prepared-groups"), diagnostic: new(rf3testfixture.ProcessDiagnostic), exited: make(chan struct{}), manifest: manifest, ready: ready}
	process.command.Env = seamlessScaleDiagnosticEnvironment()
	for _, entry := range process.command.Env {
		if table, ok := strings.CutPrefix(entry, seamlessScaleDiagnosticAbortTableEnv+"="); ok {
			_, _ = fmt.Fprintf(process.diagnostic, "VIBEDB_RF3_TEST_CHILD_ENV abort_reason=1 abort_table=%s\n", table)
			break
		}
	}
	process.command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	process.command.WaitDelay = 2 * time.Second
	process.command.Stdout, process.command.Stderr = process.diagnostic, process.diagnostic
	process.markInstance()
	if err := process.command.Start(); err != nil {
		t.Fatalf("start empty target: %v", err)
	}
	go func() { _ = process.command.Wait(); close(process.exited) }()
	t.Cleanup(func() { process.Stop(t) })
	return process
}

func (process *seamlessScaleNodeProcess) Stop(t *testing.T) {
	_ = process.StopContext(context.Background())
}

func (process *seamlessScaleNodeProcess) StopContext(ctx context.Context) error {
	command, exited, _ := process.runtimeSnapshot()
	if command == nil || command.Process == nil {
		return nil
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	select {
	case <-exited:
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		<-exited
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	return nil
}

func (process *seamlessScaleNodeProcess) Restart(ctx context.Context) error {
	oldCommand, oldExited, diagnostic := process.runtimeSnapshot()
	if process == nil || oldCommand == nil {
		return errors.New("nil target process")
	}
	_ = oldCommand.Process.Signal(syscall.SIGTERM)
	select {
	case <-oldExited:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	command := exec.Command(oldCommand.Path, oldCommand.Args[1:]...)
	command.Env = seamlessScaleDiagnosticEnvironment()
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.WaitDelay = 2 * time.Second
	command.Stdout, command.Stderr = diagnostic, diagnostic
	process.markInstance()
	if err := command.Start(); err != nil {
		return err
	}
	exited := make(chan struct{})
	process.runtimeMu.Lock()
	process.command, process.exited = command, exited
	process.runtimeMu.Unlock()
	go func() { _ = command.Wait(); close(exited) }()
	if err := process.ready(ctx, process.manifest); err != nil {
		state := "running"
		if command.ProcessState != nil {
			state = command.ProcessState.String()
		}
		return fmt.Errorf("restart readiness: %w (process=%s diagnostics=%q)", err, state, process.diagnostic.String())
	}
	return nil
}

func seamlessScaleDiagnosticEnvironment() []string {
	abortTable := os.Getenv(seamlessScaleDiagnosticAbortTableEnv)
	if abortTable == "" {
		// Match all three scale distributions during a diagnostic run so a
		// failure on a different table still preserves its request identity.
		abortTable = "scale_"
	}
	prefixes := []string{
		seamlessScaleDiagnosticStacksEnv + "=",
		seamlessScaleDiagnosticAbortEnv + "=",
		seamlessScaleDiagnosticAbortTableEnv + "=",
	}
	environment := os.Environ()
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefixes[0]) && !strings.HasPrefix(entry, prefixes[1]) &&
			!strings.HasPrefix(entry, prefixes[2]) {
			result = append(result, entry)
		}
	}
	return append(result, prefixes[0]+"1", prefixes[1]+"1", prefixes[2]+abortTable)
}

func TestSeamlessScaleDiagnosticEnvironmentScopesDirectOutcomes(t *testing.T) {
	t.Setenv(seamlessScaleDiagnosticAbortTableEnv, "scale_alpha")
	environment := seamlessScaleDiagnosticEnvironment()
	var abortReason, abortTable string
	tableEntries := 0
	for _, entry := range environment {
		if value, ok := strings.CutPrefix(entry, seamlessScaleDiagnosticAbortEnv+"="); ok {
			abortReason = value
		}
		if value, ok := strings.CutPrefix(entry, seamlessScaleDiagnosticAbortTableEnv+"="); ok {
			abortTable = value
			tableEntries++
		}
	}
	if abortReason != "1" || abortTable != "scale_alpha" || tableEntries != 1 {
		t.Fatalf("direct-outcome diagnostic environment reason=%q table=%q", abortReason, abortTable)
	}
	t.Setenv(seamlessScaleDiagnosticAbortTableEnv, "")
	for _, entry := range seamlessScaleDiagnosticEnvironment() {
		if value, ok := strings.CutPrefix(entry, seamlessScaleDiagnosticAbortTableEnv+"="); ok && value != "scale_" {
			t.Fatalf("default direct-outcome table filter=%q, want scale_", value)
		}
	}
}

func TestSeamlessScaleSourceJournalSnapshotRetainsEmptyRootAndEntries(t *testing.T) {
	root := filepath.Join(t.TempDir(), "source-exports")
	operation := filepath.Join(root, "operation-hash")
	if err := os.MkdirAll(operation, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(operation, "journal.lock"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	got := inspectSeamlessScaleSourceJournal("node", root)
	if !got.Present || got.Path != root || len(got.Entries) != 1 || got.Entries[0].Name != "operation-hash" ||
		!got.Entries[0].Directory || len(got.Entries[0].ChildEntries) != 1 || got.Entries[0].ChildEntries[0] != "journal.lock" {
		t.Fatalf("source journal snapshot=%+v", got)
	}
	empty := inspectSeamlessScaleSourceJournal("node", t.TempDir())
	if !empty.Present || len(empty.Entries) != 0 || empty.Error != "" {
		t.Fatalf("empty source journal snapshot=%+v", empty)
	}
}

func (process *seamlessScaleNodeProcess) markInstance() {
	process.runtimeMu.Lock()
	process.instance++
	instance, diagnostic := process.instance, process.diagnostic
	process.runtimeMu.Unlock()
	fmt.Fprintf(diagnostic, "RF3 process instance=%d starting\n", instance)
}

func waitSeamlessScaleManifestGateway(ctx context.Context, manifestPath string) error {
	deadline := time.Now().Add(30 * time.Second)
	var lastAddress string
	var lastReadErr, lastDialErr error
	for time.Now().Before(deadline) {
		address, err := readSeamlessScaleGatewayAddress(manifestPath)
		lastReadErr = err
		if err == nil {
			lastAddress = address
			connection, dialErr := (&net.Dialer{Timeout: 500 * time.Millisecond}).DialContext(ctx, "tcp", address)
			lastDialErr = dialErr
			if dialErr == nil {
				_ = connection.Close()
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(50 * time.Millisecond):
		}
	}
	return fmt.Errorf("empty node gateway did not become reachable at %q (read=%v dial=%v)",
		lastAddress, lastReadErr, lastDialErr)
}

func readSeamlessScaleGatewayAddress(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var document struct {
		Gateway *struct {
			Listen string `json:"listen"`
		} `json:"gateway"`
	}
	if err := json.Unmarshal(raw, &document); err != nil || document.Gateway == nil || document.Gateway.Listen == "" {
		return "", errors.New("manifest has no gateway listener")
	}
	return document.Gateway.Listen, nil
}

func readSeamlessScaleControlAddress(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var document struct {
		Listeners struct {
			Control string `json:"control"`
		} `json:"listeners"`
	}
	if err := json.Unmarshal(raw, &document); err != nil || document.Listeners.Control == "" {
		return "", errors.New("manifest has no node-control listener")
	}
	return document.Listeners.Control, nil
}

func waitSeamlessScaleManifestControl(ctx context.Context, manifestPath string, profile *rafttransport.PeerTLS, target rafttransport.NodeID) error {
	if profile == nil || target == (rafttransport.NodeID{}) {
		return errors.New("scale fixture: node-control readiness requires client profile and target")
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		address, err := readSeamlessScaleControlAddress(manifestPath)
		if err == nil {
			dialCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
			raw, dialErr := (&net.Dialer{Timeout: 500 * time.Millisecond}).DialContext(dialCtx, "tcp", address)
			cancel()
			if dialErr == nil {
				connection, clientErr := profile.Client(ctx, raw, target, rafttransport.TrafficShardControl,
					func() time.Time { return time.Now().Add(2 * time.Second) })
				if clientErr == nil {
					_ = connection.Close()
					return nil
				}
				_ = raw.Close()
			}
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(50 * time.Millisecond):
		}
	}
	return errors.New("empty node-control listener did not become authenticated")
}

func readSeamlessScaleGatewayProfile(path string) (*rafttransport.PeerTLS, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var document struct {
		Gateway *struct {
			TLS struct {
				Certificate string `json:"certificate"`
				Key         string `json:"key"`
				Roots       string `json:"roots"`
				IdentityOID string `json:"identity_oid"`
			} `json:"tls"`
		} `json:"gateway"`
	}
	if err := json.Unmarshal(raw, &document); err != nil || document.Gateway == nil {
		return nil, errors.New("manifest has no gateway TLS identity")
	}
	return servicetls.LoadProfile(document.Gateway.TLS.Certificate, document.Gateway.TLS.Key,
		document.Gateway.TLS.Roots, document.Gateway.TLS.IdentityOID, time.Now)
}

func stopSeamlessScalePhysicalChild(ctx context.Context, supervisor *fusedSupervisorProcess, manifest string) error {
	children, err := fusedDescendants(supervisor.PID())
	if err != nil {
		return err
	}
	for _, child := range children {
		if len(child.Argv) >= 4 && child.Argv[0] != "" && child.Argv[1] == "serve-node" && child.Argv[3] == manifest {
			if err := fusedSignalProcess(child, syscall.SIGTERM, false); err != nil {
				return err
			}
			return waitFusedProcessGone(ctx, child)
		}
	}
	return fmt.Errorf("retiring process manifest %q not found", manifest)
}

func waitFusedProcessGone(ctx context.Context, want fusedLinuxProcess) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		processes, err := fusedReadLinuxProcesses()
		if err != nil {
			return err
		}
		found := false
		for _, process := range processes {
			if process.PID == want.PID && process.StartTime == want.StartTime {
				found = true
				break
			}
		}
		if !found {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

func loadSeamlessScaleDescriptor(path string) (clustercontrol.NodeDescriptor, error) {
	return clustercontrol.LoadNodeDescriptor(path)
}

func assertSeamlessScaleEmptyManifest(path, nodeID string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	var groups []json.RawMessage
	if err := json.Unmarshal(fields["groups"], &groups); err != nil || len(groups) != 0 {
		return errors.New("empty target manifest carries serving groups")
	}
	var seeds []json.RawMessage
	if err := json.Unmarshal(fields["bootstrap_gateway_seeds"], &seeds); err != nil || len(seeds) == 0 {
		return errors.New("empty target manifest lacks trusted gateway seeds")
	}
	var incarnation uint64
	_ = json.Unmarshal(fields["node_incarnation"], &incarnation)
	if incarnation == 0 || nodeID == "" {
		return errors.New("empty target manifest has no durable physical identity")
	}
	return nil
}

// fusedSignalProcess is deliberately reused from the Linux process fixture.
// This wrapper keeps the scale test's child matching readable and makes the
// stop-after-safe_to_stop ordering explicit at the call site.
var _ = bufio.ErrInvalidUnreadByte
var _ = io.EOF
var _ = sort.Strings
var _ = sync.Once{}

func TestSeamlessScalePreparationDoesNotReadSourceKey(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source.json")
	// An existing serving manifest intentionally omits wrapped-key metadata.
	// Its key path is inaccessible: fresh preparation must use only the target key.
	if err := os.WriteFile(source, []byte(`{"node_log":{"key_id":"fixture-key","key_material_path":"/missing/source-key","options":{"MaxGroups":64}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	targetKey := filepath.Join(root, "target-key")
	if err := os.WriteFile(targetKey, bytes.Repeat([]byte{7}, 32), 0600); err != nil {
		t.Fatal(err)
	}
	seed := nodecontrol.BootstrapGatewaySeed{NodeID: rafttransport.NodeID{1}, Incarnation: 1, ControlAddress: "127.0.0.1:9000", SPKIPinDigest: replication.Digest{2}}
	raw, err := buildSeamlessScaleEmptyPreparation(source, filepath.Join(root, "target"), "/cert", "/key", targetKey, "/policy", "/roots",
		map[string]string{"peer": "127.0.0.1:9001", "native": "127.0.0.1:9002", "snapshot": "127.0.0.1:9003", "control": "127.0.0.1:9004"},
		[]rafttransport.NodeID{{1}}, []nodecontrol.BootstrapGatewaySeed{seed})
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		NodeLog seamlessScaleNodeLogInput `json:"node_log"`
	}
	if err := vibejson.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if result.NodeLog.KeyMaterialPath != targetKey || result.NodeLog.WrappedKey == "" {
		t.Fatal("fresh preparation lost its own key provider")
	}
}

func seamlessScaleSQLMatches(result fusedPGResult, row seamlessScaleAck) bool {
	if result.code != "" || len(result.rows) != 1 || len(result.rows[0]) != 3 || len(result.columns) != 3 || !fusedPGNumericOID(result.columns[1]) {
		return false
	}
	id, idErr := fusedPGCellText(result.rows[0][0], result.columns[0])
	marker, markerErr := fusedPGCellText(result.rows[0][2], result.columns[2])
	return idErr == nil && markerErr == nil && id == row.ID && marker == row.Marker && result.rows[0][1] == strconv.Itoa(row.Value)
}

func TestSeamlessScaleSQLOracleUsesDeclaredColumnTypes(t *testing.T) {
	row := seamlessScaleAck{ID: "quoted-id", Value: 7, Marker: "marker"}
	result := fusedPGResult{columns: []uint32{114, 114, 114}, rows: [][]string{{`"quoted-id"`, "7", `"marker"`}}}
	if !seamlessScaleSQLMatches(result, row) {
		t.Fatal("canonical JSON columns rejected")
	}
	result.rows[0][0] = row.ID
	if seamlessScaleSQLMatches(result, row) {
		t.Fatal("invalid unquoted JSON accepted")
	}
	result.columns = []uint32{25, 23, 25}
	result.rows[0][2] = row.Marker
	if !seamlessScaleSQLMatches(result, row) {
		t.Fatal("PostgreSQL text columns rejected")
	}
	result.rows[0][1] = "8"
	if seamlessScaleSQLMatches(result, row) {
		t.Fatal("wrong numeric value accepted")
	}
}
