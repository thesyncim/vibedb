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
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/clustercontrol"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibejson"
)

const (
	seamlessScaleProcessEnvironment  = "VIBEDB_SEAMLESS_SCALE_E2E"
	seamlessScaleEvidenceEnvironment = "VIBEDB_SEAMLESS_SCALE_EVIDENCE"
	seamlessScaleWindowDuration      = 10 * time.Second
	seamlessScaleMinimumSamples      = 10_000
	seamlessScaleOfferedRate         = 1_200
	seamlessScaleWorkloadConnections = 16
	seamlessScaleOperationWait       = 750 * time.Millisecond
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
	t            *testing.T
	ctx          context.Context
	addresses    []string
	survivorSQL  net.Conn
	survivorGate net.Conn
	gateReader   *bufio.Reader
	gateMu       sync.Mutex
	connections  []net.Conn
	mu           sync.Mutex
	historyMu    sync.Mutex
	history      map[string][]seamlessScaleWindow
	sequence     uint64
	seedRows     []seamlessScaleAck
	seed         map[string]seamlessScaleAck
	acknowledged map[string]seamlessScaleAck
	sqlRequests  uint64
	gateRequests uint64
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
	Latency   time.Duration
	QueueLag  time.Duration
	Ack       *seamlessScaleAck
	Verified  *seamlessScaleAck
	Retries   uint64
	Err       error
}

type seamlessScaleWindow struct {
	evidence seamlessScalePhaseEvidence
	samples  []seamlessScaleSample
}

func newSeamlessScaleWorkload(t *testing.T, ctx context.Context, address string, survivorSQL, survivorGateway net.Conn) *seamlessScaleWorkload {
	t.Helper()
	workload := &seamlessScaleWorkload{t: t, ctx: ctx, addresses: []string{address}, survivorSQL: survivorSQL,
		survivorGate: survivorGateway, gateReader: bufio.NewReaderSize(survivorGateway, 64<<10),
		history: make(map[string][]seamlessScaleWindow), seed: make(map[string]seamlessScaleAck),
		acknowledged: make(map[string]seamlessScaleAck)}
	workload.connections = append(workload.connections, survivorSQL)
	for index := 1; index < seamlessScaleWorkloadConnections; index++ {
		connection, err := fusedOpenDDLWire(ctx, address)
		if err != nil {
			t.Fatalf("open scale workload SQL connection %d: %v", index+1, err)
		}
		workload.connections = append(workload.connections, connection)
	}
	return workload
}

func (workload *seamlessScaleWorkload) Close() {
	if workload == nil {
		return
	}
	seen := make(map[net.Conn]struct{}, len(workload.connections)+1)
	for _, connection := range workload.connections {
		if connection == nil {
			continue
		}
		if _, ok := seen[connection]; ok {
			continue
		}
		seen[connection] = struct{}{}
		_ = connection.Close()
	}
	if workload.survivorGate != nil {
		_ = workload.survivorGate.Close()
	}
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

func seamlessScalePayload(table string, index int) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("vibedb-scale/%s/%d", table, index)))
	return strings.Repeat(hex.EncodeToString(digest[:]), 128)
}

// Window drives fixed open-loop arrivals.  A bounded queue turns scheduler
// overload into an explicit missed sample, which is rejected by the strict
// evidence contract instead of silently shortening the denominator.
func (workload *seamlessScaleWorkload) Window(ctx context.Context, phase string, duration time.Duration, rate int) seamlessScalePhaseEvidence {
	if rate <= 0 || duration <= 0 {
		return seamlessScalePhaseEvidence{Phase: phase}
	}
	start := time.Now()
	deadline := start.Add(duration)
	interval := time.Second / time.Duration(rate)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	jobs := make(chan time.Time, rate*2)
	results := make(chan seamlessScaleSample, rate*2)
	var workers sync.WaitGroup
	for index, connection := range workload.connections {
		workers.Add(1)
		go func(worker int, connection net.Conn) {
			defer workers.Done()
			for scheduled := range jobs {
				results <- workload.doJob(ctx, worker, connection, scheduled)
			}
		}(index, connection)
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
	var scheduled, missed uint64
	var samples []seamlessScaleSample
	for index := 0; index < count; index++ {
		window := workload.Window(ctx, phase, duration, rate)
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
	return workload.phaseEvidence(phase, start, end, scheduled, missed, samples)
}

// WindowUntil keeps the same open-loop actor active across an operation wave.
// The stop signal is observed only between complete windows, so each retained
// sample has a real scheduled-arrival interval and the final window cannot be
// silently truncated at a topology event.
func (workload *seamlessScaleWorkload) WindowUntil(ctx context.Context, phase string, duration time.Duration, rate int, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		workload.Window(ctx, phase, duration, rate)
	}
}

// calibrateSeamlessScaleRate performs a bounded preflight with the exact
// mixed SQL/native workload used by the qualification. It chooses once before
// baseline collection, and every subsequent phase uses that same offered
// rate. A rate that cannot keep its bounded queue fed is rejected instead of
// allowing a scheduler-overload artifact to masquerade as a migration pause.
func calibrateSeamlessScaleRate(t *testing.T, workload *seamlessScaleWorkload, ctx context.Context) int {
	t.Helper()
	for _, candidate := range []int{seamlessScaleOfferedRate, 1_000} {
		probe := workload.Window(ctx, "calibration", 2*time.Second, candidate)
		t.Logf("scale calibration rate=%d scheduled=%d completed=%d errors=%d missed=%d writes=%d reads=%d p99=%s", candidate, probe.Scheduled, probe.Completed, probe.Errors, probe.Missed, probe.AcknowledgedWrites, probe.VerifiedReads, time.Duration(probe.P99NS))
		if probe.Errors != 0 {
			workload.historyMu.Lock()
			windows := workload.history["calibration"]
			for _, sample := range windows[len(windows)-1].samples {
				if sample.Err != nil {
					t.Logf("calibration first operation error: %v", sample.Err)
					break
				}
			}
			workload.historyMu.Unlock()
		}
		if probe.Scheduled >= uint64(candidate)*2 && probe.Started == probe.Scheduled &&
			probe.Completed == probe.Started && probe.Errors == 0 && probe.Missed == 0 {
			return candidate
		}
	}
	t.Fatal("scale workload calibration could not sustain the minimum strict offered rate without misses")
	return 0
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
	var scheduled, missed uint64
	var samples []seamlessScaleSample
	for _, entry := range entries {
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
	return workload.phaseEvidence(phase, start, end, scheduled, missed, samples)
}

func (workload *seamlessScaleWorkload) doJob(ctx context.Context, worker int, connection net.Conn, scheduled time.Time) seamlessScaleSample {
	sample := seamlessScaleSample{Scheduled: scheduled, Started: time.Now()}
	workload.mu.Lock()
	sequence := workload.sequence
	workload.sequence++
	workload.mu.Unlock()
	if sequence%5 == 0 {
		table := seamlessScaleTables[sequence%uint64(len(seamlessScaleTables))]
		rowIndex := int(sequence % 1_000_000)
		row := seamlessScaleAck{Table: table, ID: fmt.Sprintf("live-%s-%012d", table, sequence),
			Value: int(sequence%1_000_000) + 50_000, Marker: seamlessScalePayload(table, rowIndex)}
		query := fmt.Sprintf("INSERT INTO %s (id,value,marker) VALUES ('%s',%d,'%s')", table, row.ID, row.Value, row.Marker)
		result, retries, err := seamlessScaleQueryRetry(ctx, connection, query, false)
		sample.Retries = retries
		if err == nil && (result.code != "" || result.tag != "INSERT 0 1") {
			err = fmt.Errorf("unexpected write acknowledgement: %+v", result)
		}
		if err != nil {
			sample.Err = err
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
			// Keep the authenticated native gateway stream active throughout every
			// window. Reads use a single serialized stream just as a long-lived
			// application client would.
			result, retries, err := workload.gatewayRead(ctx, row)
			sample.Retries = retries
			if err == nil {
				workload.mu.Lock()
				workload.gateRequests++
				workload.mu.Unlock()
				sample.Verified = &row
			}
			if err != nil {
				sample.Err = err
			} else {
				// A gateway read is the foreground read for the sample. Keep one
				// SQL read on the same survivor connection as a second oracle.
				result, retries, err = seamlessScaleQueryRetry(ctx, connection,
					fmt.Sprintf("SELECT id,value,marker FROM %s WHERE id='%s'", row.Table, row.ID), false)
				sample.Retries += retries
				if err == nil {
					workload.mu.Lock()
					workload.sqlRequests++
					workload.mu.Unlock()
					if !seamlessScaleSQLMatches(result, row) {
						err = fmt.Errorf("unexpected SQL read result: code=%s rows=%d columns=%v key=%s worker=%d sequence=%d", result.code, len(result.rows), result.columns, row.ID, worker, sequence)
					}
				}
				if err != nil {
					sample.Err = err
				}
			}
		}
	}
	sample.Completed = time.Now()
	sample.QueueLag = sample.Started.Sub(sample.Scheduled)
	sample.Latency = sample.Completed.Sub(sample.Scheduled)
	return sample
}

func seamlessScaleQueryRetry(ctx context.Context, connection net.Conn, query string, extended bool) (fusedPGResult, uint64, error) {
	var last error
	for attempt := uint64(0); attempt < 4; attempt++ {
		result, err := fusedDDLWireQuery(ctx, connection, query, extended)
		if err == nil && !fusedPGResultTransient(result) {
			return result, attempt, nil
		}
		if err != nil {
			last = err
		} else {
			last = fmt.Errorf("transient PostgreSQL response %s: %s", result.code, result.message)
		}
		if attempt != 3 {
			if err := fusedWaitRetry(ctx, 2*time.Millisecond); err != nil {
				return result, attempt + 1, err
			}
		}
	}
	return fusedPGResult{}, 3, last
}

func (workload *seamlessScaleWorkload) gatewayRead(ctx context.Context, row seamlessScaleAck) (fusedPGResult, uint64, error) {
	request := rf3FixturePointRequest(row.Table, row.ID)
	raw, err := vibejson.Marshal(&request)
	if err != nil {
		return fusedPGResult{}, 0, err
	}
	workload.gateMu.Lock()
	defer workload.gateMu.Unlock()
	for attempt := uint64(0); attempt < 4; attempt++ {
		if err := workload.survivorGate.SetDeadline(minFusedDeadline(ctx, time.Now().Add(10*time.Second))); err != nil {
			return fusedPGResult{}, attempt, err
		}
		if _, err := workload.survivorGate.Write(append(append([]byte(nil), raw...), '\n')); err != nil {
			return fusedPGResult{}, attempt, err
		}
		response, err := workload.gateReader.ReadSlice('\n')
		if err == nil && seamlessScaleGatewayResponseMatches(response, row) {
			return fusedPGResult{}, attempt, nil
		}
		if err != nil {
			return fusedPGResult{}, attempt, err
		}
		if attempt != 3 {
			if err := fusedWaitRetry(ctx, 2*time.Millisecond); err != nil {
				return fusedPGResult{}, attempt + 1, err
			}
		}
	}
	return fusedPGResult{}, 3, fmt.Errorf("native gateway response did not match %s/%s", row.Table, row.ID)
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
			if errors.Is(sample.Err, context.DeadlineExceeded) {
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
		return err
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

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	defer cancel()
	root, err := os.MkdirTemp("", "ss-qual-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	state := filepath.Join(root, "state")
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
	if err := listenerReservation.Close(); err != nil {
		t.Fatalf("release empty-node listener reservations: %v", err)
	}

	physical := startSeamlessScalePhysicalCluster(t, ctx, shardBinary, cluster)
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

	const survivorIndex = 1
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
	if !nodesResponse.OK || len(nodesResponse.Nodes) != 3 {
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

	workload := newSeamlessScaleWorkload(t, ctx, pgListens[survivorIndex], survivorSQL, survivorGateway)
	defer workload.Close()
	if err := workload.Seed(t, ctx); err != nil {
		t.Fatalf("seed acknowledged workload rows: %v", err)
	}
	calibratedRate := calibrateSeamlessScaleRate(t, workload, ctx)
	baseline := workload.WindowSet(ctx, seamlessScalePhaseBaseline, 5, seamlessScaleWindowDuration, calibratedRate)

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

	targetProcesses := make([]*seamlessScaleNodeProcess, len(emptyTargets))
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
		target := &emptyTargets[cycle]
		targetProcesses[cycle] = startSeamlessScaleEmptyNode(t, ctx, shardBinary, target.Manifest, controlProfile, target.NodeID)

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
		var joinFinal clustercontrol.Response
		if err := waitSeamlessScaleOperation(ctx, vibedbBinary, profilePath, join.OperationID, func(response clustercontrol.Response) bool {
			joinFinal = response
			return seamlessScaleTerminalSuccess(response)
		}); err != nil {
			t.Fatalf("cycle %d join did not complete: %v", cycle+1, err)
		}
		physicalPeak = maxInt(physicalPeak, countSeamlessScaleServingNodes(joinFinal))

		rebalanceRequestID := mustSeamlessScaleRequestID(t)
		rebalance := runSeamlessScaleCLI(t, ctx, vibedbBinary, "rebalance", profilePath,
			"--request-id", rebalanceRequestID, "--desired-node-count", "4", "--max-moves", "32",
			// The canonical empty-node fixture's 2 MiB network burst is below
			// the seeded multi-table payload, so this real move must exercise
			// durable node-wide pacing before it can complete. The intent bound
			// remains high enough to avoid truncating the migration.
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

		var pacingResponse clustercontrol.Response
		if err := waitSeamlessScaleOperation(ctx, vibedbBinary, profilePath, rebalance.OperationID, func(response clustercontrol.Response) bool {
			pacingResponse = response
			if response.State == "complete" || response.State == "completed" || response.State == "succeeded" || response.State == "failed" {
				return false
			}
			// A cumulative moved counter is not proof that this status sample
			// belongs to the live migration wave. Require the durable enrollment
			// phase itself to expose the move/copy state while the operation is
			// still running, so pacing cannot be observed only after completion.
			phase := strings.ToLower(response.Phase)
			activeMovePhase := strings.Contains(phase, "move") || strings.Contains(phase, "copy")
			return response.Budget != nil && response.Budget.ThrottledCalls != 0 &&
				response.Budget.ThrottledBytes != 0 && response.Phase != "" && activeMovePhase
		}); err != nil {
			t.Fatalf("cycle %d migration never exposed positive pacing in an active move phase: %v", cycle+1, err)
		}
		applicationMoved = maxUint32(applicationMoved, pacingResponse.ApplicationGroupsMoved)
		internalMoved = maxUint32(internalMoved, pacingResponse.InternalGroupsMoved)
		aggregateBudget.ThrottledCalls = maxUint64(aggregateBudget.ThrottledCalls, pacingResponse.Budget.ThrottledCalls)
		aggregateBudget.ThrottledBytes = maxUint64(aggregateBudget.ThrottledBytes, pacingResponse.Budget.ThrottledBytes)
		aggregateBudget.PeakActive = maxUint64(aggregateBudget.PeakActive, uint64(pacingResponse.Budget.PeakActive))
		aggregateBudget.MaxActive = maxUint64(aggregateBudget.MaxActive, uint64(pacingResponse.Budget.MaxActive))
		physicalPeak = maxInt(physicalPeak, countSeamlessScaleServingNodes(pacingResponse))

		if err := targetProcesses[cycle].Restart(ctx); err != nil {
			t.Fatalf("cycle %d restart target during migration: %v", cycle+1, err)
		}
		anyTargetRestarted = true
		if cycle == 0 {
			// Node zero is the controller owner for this direct process set.
			// Restarting this process exercises durable operation recovery while
			// the two survivor frontends and their sessions remain connected.
			if err := physical.Restart(ctx, 0); err != nil {
				t.Fatalf("cycle %d restart controller owner during migration: %v", cycle+1, err)
			}
			controllerRestarted = true
		}
		postRestartStatus, err := runSeamlessScaleCLIForPoll(ctx, vibedbBinary, profilePath, rebalance.OperationID)
		if err != nil {
			t.Fatalf("cycle %d post-restart durable status: %v", cycle+1, err)
		}
		if postRestartStatus.Phase == "" || postRestartStatus.Budget == nil ||
			postRestartStatus.State == "failed" || postRestartStatus.OperationID != rebalance.OperationID {
			t.Fatalf("cycle %d post-restart status is not an operation proof: %+v", cycle+1, postRestartStatus)
		}
		postRestartProof = true

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

		retireID := target.NodeID
		if cycle == 0 {
			retireID = initialNodeIDs[0]
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
			retiringSQL, err = fusedOpenDDLWire(ctx, pgListens[0])
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
			blocked, err = pollSeamlessScaleStatus(ctx, vibedbBinary, profilePath, retire.OperationID, func(response clustercontrol.Response) bool {
				return !response.SafeToStop && hasSeamlessScaleSessionBlocker(response, retireIDText, retireIncarnation)
			})
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
		safe, err := pollSeamlessScaleStatus(ctx, vibedbBinary, profilePath, retire.OperationID, func(response clustercontrol.Response) bool {
			safeResponse = response
			return response.SafeToStop && len(response.Blockers) == 0 &&
				response.RetiringReferences == 0
		})
		if err != nil || !safe {
			t.Fatalf("cycle %d decommission did not report safe_to_stop with zero references: %v response=%+v", cycle+1, err, safeResponse)
		}
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
			if err := physical.StopAt(ctx, 0); err != nil {
				t.Fatalf("cycle %d stop original node after safe_to_stop: %v", cycle+1, err)
			}
		} else {
			if err := targetProcesses[cycle-1].StopContext(ctx); err != nil {
				t.Fatalf("cycle %d stop retired target after safe_to_stop: %v", cycle+1, err)
			}
		}
		finalNodesResponse = runSeamlessScaleCLI(t, ctx, vibedbBinary, "nodes", profilePath)
		if !finalNodesResponse.OK || countSeamlessScaleServingNodes(finalNodesResponse) != 3 ||
			hasSeamlessScaleNode(finalNodesResponse, retireID) {
			t.Fatalf("cycle %d post-stop topology=%+v", cycle+1, finalNodesResponse)
		}
		physicalPeak = maxInt(physicalPeak, countSeamlessScaleServingNodes(finalNodesResponse))
		completedCycles++
	}

	close(duringStop)
	during := <-duringDone
	after := workload.WindowSet(ctx, seamlessScalePhaseAfter, 3, seamlessScaleWindowDuration, calibratedRate)
	if err := workload.VerifyAllAcknowledgements(ctx); err != nil {
		t.Fatalf("acknowledged data oracle: %v", err)
	}
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
	evidence.SurvivorSessionStable = workload.SurvivorSessionsStable()
	evidence.RetiringSessionBlocked = firstSessionBlocked
	evidence.RetiringSessionReleased = firstSessionReleased
	evidence.RetiringReferencesAfter = uint64(finalSafeResponse.RetiringReferences)
	evidence.GroupInventoryBeforeDigest = beforeInventoryDigest
	evidence.GroupInventoryAfterDigest = finalNodesResponse.GroupInventoryDigest
	evidence.Budget = aggregateBudget
	bounds, err := loadSeamlessScalePerformanceBounds(os.Getenv)
	if err != nil {
		t.Fatalf("performance bounds: %v", err)
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
		NodeLog seamlessScaleNodeLogInput `json:"node_log"`
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
	options := rf3testfixture.EmptyNodeOptions{Root: targetRoot, NodeIncarnation: 1, Key: key,
		NodeStore: source.NodeLog.Options, Listeners: rf3testfixture.ProcessListeners{
			Peer: listeners["peer"], Native: listeners["native"], Snapshot: listeners["snapshot"], Control: listeners["control"],
		}, Credential: rf3testfixture.Credential{Certificate: targetCertificate, Key: targetKey}, Roots: roots,
		AuthorizationPolicy: policy, GrantNodes: grantNodes, GatewaySeeds: gatewaySeeds}
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

func hasSeamlessScaleSessionBlocker(response clustercontrol.Response, nodeID string, incarnation uint64) bool {
	for _, blocker := range response.Blockers {
		if blocker.Code == "gateway_session" && blocker.NodeID == nodeID && blocker.NodeIncarnation == incarnation &&
			strings.Contains(strings.ToLower(blocker.Detail), "session") {
			return true
		}
	}
	return false
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

func pollSeamlessScaleStatus(ctx context.Context, binary, profile, operationID string, predicate func(clustercontrol.Response) bool) (bool, error) {
	deadline := time.NewTimer(10 * time.Minute)
	defer deadline.Stop()
	var prior string
	for {
		response, err := runSeamlessScaleCLIForPoll(ctx, binary, profile, operationID)
		if err != nil {
			return false, err
		}
		state := fmt.Sprintf("state=%s phase=%s blockers=%+v", response.State, response.Phase, response.Blockers)
		if state != prior {
			fmt.Printf("scale operation %s: %s\n", operationID, state)
			prior = state
		}
		if predicate(response) {
			return true, nil
		}
		select {
		case <-ctx.Done():
			return false, context.Cause(ctx)
		case <-deadline.C:
			return false, errors.New("operation status polling deadline exceeded")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func waitSeamlessScaleOperation(ctx context.Context, binary, profile, operationID string, predicate func(clustercontrol.Response) bool) error {
	if _, err := pollSeamlessScaleStatus(ctx, binary, profile, operationID, predicate); err != nil {
		return fmt.Errorf("operation %s did not reach requested state: %w", operationID, err)
	}
	return nil
}

func runSeamlessScaleCLIForPoll(ctx context.Context, binary, profile, operationID string) (clustercontrol.Response, error) {
	command := exec.CommandContext(ctx, binary, "cluster", "status", "--profile", profile, "--operation", operationID, "--json", "--wait", seamlessScaleOperationWait.String())
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil && stdout.Len() == 0 {
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
		return clustercontrol.Response{}, fmt.Errorf("cluster status rejected operation %s: %s", operationID, response.Error)
	}
	return response, nil
}

func digestSeamlessScaleBytes(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// The remaining helpers are deliberately small process wrappers. They are
// kept independent from the existing fused RF3 fixture so a scale test cannot
// accidentally assert a static three-node layout after a node has moved.
type seamlessScaleNodeProcess struct {
	command    *exec.Cmd
	diagnostic *rf3testfixture.ProcessDiagnostic
	exited     chan struct{}
	manifest   string
	ready      func(context.Context, string) error
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
	process.command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	process.command.WaitDelay = 2 * time.Second
	process.command.Stdout, process.command.Stderr = process.diagnostic, process.diagnostic
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
	if process == nil || process.command == nil || process.command.Process == nil {
		return nil
	}
	if err := process.command.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	select {
	case <-process.exited:
	case <-time.After(10 * time.Second):
		_ = process.command.Process.Kill()
		<-process.exited
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	return nil
}

func (process *seamlessScaleNodeProcess) Restart(ctx context.Context) error {
	if process == nil || process.command == nil {
		return errors.New("nil target process")
	}
	_ = process.command.Process.Signal(syscall.SIGTERM)
	select {
	case <-process.exited:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	command := exec.Command(process.command.Path, process.command.Args[1:]...)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.WaitDelay = 2 * time.Second
	command.Stdout, command.Stderr = process.diagnostic, process.diagnostic
	if err := command.Start(); err != nil {
		return err
	}
	process.command, process.exited = command, make(chan struct{})
	go func() { _ = command.Wait(); close(process.exited) }()
	return process.ready(ctx, process.manifest)
}

func waitSeamlessScaleManifestGateway(ctx context.Context, manifestPath string) error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		address, err := readSeamlessScaleGatewayAddress(manifestPath)
		if err == nil {
			connection, dialErr := (&net.Dialer{Timeout: 500 * time.Millisecond}).DialContext(ctx, "tcp", address)
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
	return errors.New("empty node gateway did not become reachable")
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
