package rf3bench

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"
)

const (
	ChaosSchemaVersion               = uint16(2)
	MaximumChaosRuns                 = 1024
	QualificationSchemaVersion       = uint16(1)
	RequiredAsymmetricPartitionLoops = uint32(2)
	RequiredWaiterWaves              = uint32(4)
	RequiredWaiterCallsPerWave       = uint32(64)
	WALGrowthBoundBytes              = uint64(256 << 20)
	WaiterRSSGrowthBoundBytes        = uint64(256 << 20)
	QualificationPathEnvironment     = "VIBEDB_RF3_QUALIFICATION_PATH"
)

// Qualification is the exact bounded evidence emitted by the external RF3
// process test. All counters describe observed cuts, not inferred availability
// or latency. RSS growth uses the sampled high-water mark. WAL growth is the
// positive endpoint delta; reclamation never causes unsigned underflow.
type Qualification struct {
	KillBeforeRequestCuts, KillAdmissionResponseCuts, KillAfterApplyResponseCuts                uint32
	AsymmetricPartitionLoops                                                                    uint32
	AsymmetricRejectedConnections                                                               uint64
	WaiterWaves, WaiterCalls, WaiterCompletions, WaiterRefusals, WaiterReuseCompletions         uint32
	WALBaselineBytes, WALFinalBytes, WALGrowthBytes, WALGrowthBoundBytes                        uint64
	WaiterRSSBaselineBytes, WaiterRSSPeakBytes, WaiterRSSGrowthBytes, WaiterRSSGrowthBoundBytes uint64
	LostResponseApplied, AckOutcome                                                             uint64
}

func (q Qualification) Validate() error {
	// Reclamation can reduce allocation. Preserve both measured endpoints;
	// growth is the positive delta, never unsigned underflow.
	expectedWALGrowth := uint64(0)
	if q.WALFinalBytes > q.WALBaselineBytes {
		expectedWALGrowth = q.WALFinalBytes - q.WALBaselineBytes
	}
	expectedRSSGrowth := uint64(0)
	if q.WaiterRSSPeakBytes > q.WaiterRSSBaselineBytes {
		expectedRSSGrowth = q.WaiterRSSPeakBytes - q.WaiterRSSBaselineBytes
	}
	if q.KillBeforeRequestCuts != 1 || q.KillAdmissionResponseCuts != 1 ||
		q.KillAfterApplyResponseCuts != 1 ||
		q.AsymmetricPartitionLoops != RequiredAsymmetricPartitionLoops ||
		q.AsymmetricRejectedConnections == 0 || q.WaiterWaves != RequiredWaiterWaves ||
		q.WaiterCalls != RequiredWaiterWaves*RequiredWaiterCallsPerWave ||
		q.WaiterCompletions == 0 || q.WaiterCompletions+q.WaiterRefusals != q.WaiterCalls ||
		q.WaiterReuseCompletions != q.WaiterWaves || q.WALBaselineBytes == 0 ||
		q.WALFinalBytes == 0 || q.WALGrowthBytes != expectedWALGrowth ||
		q.WALGrowthBoundBytes != WALGrowthBoundBytes || q.WALGrowthBytes > q.WALGrowthBoundBytes ||
		q.WaiterRSSBaselineBytes == 0 || q.WaiterRSSPeakBytes < q.WaiterRSSBaselineBytes ||
		q.WaiterRSSGrowthBoundBytes != WaiterRSSGrowthBoundBytes ||
		q.WaiterRSSGrowthBytes != expectedRSSGrowth || q.WaiterRSSGrowthBytes > q.WaiterRSSGrowthBoundBytes ||
		q.LostResponseApplied == 0 || q.AckOutcome == 0 {
		return ErrInvalidReport
	}
	return nil
}

// ChaosRun is one isolated execution of an external fault harness. ElapsedNS
// covers the test process only; build time is deliberately excluded. The
// output digest authenticates all stdout and stderr bytes without embedding
// unbounded logs in the evidence file.
type ChaosRun struct {
	Ordinal            uint32
	ElapsedNS          uint64
	OutputBytes        uint64
	OutputSHA256       [32]byte
	ExitCode           int32
	TimedOut           bool
	ExactRun           bool
	QualificationExact bool
	Qualification      Qualification
	Passed             bool
}

// ChaosReport records raw qualification outcomes. A report may contain failed
// runs: preserving failed evidence is essential, while the runner separately
// returns a failing exit status.
type ChaosReport struct {
	TimeoutNS uint64
	Metadata  []Metadata
	Runs      []ChaosRun
}

func (report ChaosReport) Validate() error {
	if report.TimeoutNS == 0 || len(report.Runs) == 0 || len(report.Runs) > MaximumChaosRuns ||
		len(report.Metadata) > MaximumMetadata {
		return ErrInvalidReport
	}
	for index := range report.Metadata {
		entry := report.Metadata[index]
		if !validName(entry.Key) || reservedChaosMetadataKey(entry.Key) || !validValue(entry.Value) ||
			(index != 0 && bytes.Compare(report.Metadata[index-1].Key, entry.Key) >= 0) {
			return ErrInvalidReport
		}
	}
	for index, run := range report.Runs {
		if run.Ordinal != uint32(index+1) || run.ElapsedNS == 0 ||
			(run.Passed && (run.TimedOut || !run.ExactRun || !run.QualificationExact ||
				run.ExitCode != 0 || run.Qualification.Validate() != nil)) {
			return ErrInvalidReport
		}
	}
	return nil
}

func reservedChaosMetadataKey(key []byte) bool {
	for _, reserved := range [][]byte{
		[]byte("durability"), []byte("fault_harness"), []byte("format"),
		[]byte("replicas"), []byte("topology"),
	} {
		if bytes.Equal(key, reserved) {
			return true
		}
	}
	return false
}

// WriteChaosTSV emits the canonical, claim-free external fault evidence. It
// intentionally emits no availability or failover-latency statistic because
// the current black-box test exposes only whole-process execution time.
func WriteChaosTSV(destination io.Writer, report ChaosReport) error {
	if destination == nil {
		return ErrInvalidReport
	}
	if err := report.Validate(); err != nil {
		return err
	}
	buffer := make([]byte, 0, 512)
	write := func(fields ...[]byte) error {
		buffer = buffer[:0]
		for index, field := range fields {
			if index != 0 {
				buffer = append(buffer, '\t')
			}
			buffer = append(buffer, field...)
		}
		buffer = append(buffer, '\n')
		for len(buffer) != 0 {
			written, err := destination.Write(buffer)
			if written > 0 {
				buffer = buffer[written:]
			}
			if err != nil {
				return err
			}
			if written == 0 {
				return io.ErrShortWrite
			}
		}
		return nil
	}
	u64 := func(value uint64) []byte { return strconv.AppendUint(nil, value, 10) }
	i64 := func(value int64) []byte { return strconv.AppendInt(nil, value, 10) }
	boolean := func(value bool) []byte { return strconv.AppendBool(nil, value) }
	if err := write([]byte("schema"), []byte("vibedb.rf3.chaos-evidence"),
		u64(uint64(ChaosSchemaVersion))); err != nil {
		return err
	}
	for _, entry := range []Metadata{
		{Key: []byte("durability"), Value: []byte("power-safe")},
		{Key: []byte("fault_harness"), Value: []byte("external-process")},
		{Key: []byte("format"), Value: []byte("canonical-tsv")},
		{Key: []byte("replicas"), Value: []byte("3")},
		{Key: []byte("topology"), Value: []byte("rf3")},
	} {
		if err := write([]byte("meta"), entry.Key, entry.Value); err != nil {
			return err
		}
	}
	for _, entry := range report.Metadata {
		if err := write([]byte("meta"), entry.Key, entry.Value); err != nil {
			return err
		}
	}
	if err := write([]byte("config"), []byte("runs"), u64(uint64(len(report.Runs)))); err != nil {
		return err
	}
	if err := write([]byte("config"), []byte("timeout_ns"), u64(report.TimeoutNS)); err != nil {
		return err
	}
	if err := write([]byte("raw_header"), []byte("ordinal"), []byte("elapsed_ns"),
		[]byte("exit_code"), []byte("timed_out"), []byte("exact_run"),
		[]byte("qualification_exact"), []byte("passed"), []byte("output_bytes"), []byte("output_sha256"),
		[]byte("kill_before_request_cuts"), []byte("kill_admission_response_cuts"), []byte("kill_after_apply_response_cuts"),
		[]byte("asymmetric_partition_loops"), []byte("asymmetric_rejected_connections"),
		[]byte("waiter_waves"), []byte("waiter_calls"), []byte("waiter_completions"), []byte("waiter_refusals"), []byte("waiter_reuse_completions"),
		[]byte("wal_baseline_bytes"), []byte("wal_final_bytes"), []byte("wal_growth_bytes"), []byte("wal_growth_bound_bytes"),
		[]byte("waiter_rss_baseline_bytes"), []byte("waiter_rss_peak_bytes"), []byte("waiter_rss_growth_bytes"), []byte("waiter_rss_growth_bound_bytes"),
		[]byte("lost_response_applied"), []byte("ack_outcome")); err != nil {
		return err
	}
	var passed uint64
	for _, run := range report.Runs {
		if run.Passed {
			passed++
		}
		digest := make([]byte, hex.EncodedLen(len(run.OutputSHA256)))
		hex.Encode(digest, run.OutputSHA256[:])
		q := run.Qualification
		if err := write([]byte("raw"), u64(uint64(run.Ordinal)), u64(run.ElapsedNS),
			i64(int64(run.ExitCode)), boolean(run.TimedOut), boolean(run.ExactRun),
			boolean(run.QualificationExact), boolean(run.Passed), u64(run.OutputBytes), digest,
			u64(uint64(q.KillBeforeRequestCuts)), u64(uint64(q.KillAdmissionResponseCuts)), u64(uint64(q.KillAfterApplyResponseCuts)),
			u64(uint64(q.AsymmetricPartitionLoops)), u64(q.AsymmetricRejectedConnections),
			u64(uint64(q.WaiterWaves)), u64(uint64(q.WaiterCalls)), u64(uint64(q.WaiterCompletions)), u64(uint64(q.WaiterRefusals)), u64(uint64(q.WaiterReuseCompletions)),
			u64(q.WALBaselineBytes), u64(q.WALFinalBytes), u64(q.WALGrowthBytes), u64(q.WALGrowthBoundBytes),
			u64(q.WaiterRSSBaselineBytes), u64(q.WaiterRSSPeakBytes), u64(q.WaiterRSSGrowthBytes), u64(q.WaiterRSSGrowthBoundBytes),
			u64(q.LostResponseApplied), u64(q.AckOutcome)); err != nil {
			return err
		}
	}
	return write([]byte("summary"), []byte("passed"), u64(passed),
		[]byte("failed"), u64(uint64(len(report.Runs))-passed))
}

var qualificationNames = []string{
	"kill_before_request_cuts", "kill_admission_response_cuts", "kill_after_apply_response_cuts",
	"asymmetric_partition_loops", "asymmetric_rejected_connections", "waiter_waves", "waiter_calls",
	"waiter_completions", "waiter_refusals", "waiter_reuse_completions", "wal_baseline_bytes",
	"wal_final_bytes", "wal_growth_bytes", "wal_growth_bound_bytes", "waiter_rss_baseline_bytes",
	"waiter_rss_peak_bytes", "waiter_rss_growth_bytes", "waiter_rss_growth_bound_bytes",
	"lost_response_applied", "ack_outcome",
}

func qualificationValues(q Qualification) []uint64 {
	return []uint64{uint64(q.KillBeforeRequestCuts), uint64(q.KillAdmissionResponseCuts),
		uint64(q.KillAfterApplyResponseCuts), uint64(q.AsymmetricPartitionLoops), q.AsymmetricRejectedConnections,
		uint64(q.WaiterWaves), uint64(q.WaiterCalls), uint64(q.WaiterCompletions), uint64(q.WaiterRefusals),
		uint64(q.WaiterReuseCompletions), q.WALBaselineBytes, q.WALFinalBytes, q.WALGrowthBytes,
		q.WALGrowthBoundBytes, q.WaiterRSSBaselineBytes, q.WaiterRSSPeakBytes, q.WaiterRSSGrowthBytes,
		q.WaiterRSSGrowthBoundBytes, q.LostResponseApplied, q.AckOutcome}
}

// WriteQualificationTSV writes the small child-to-runner evidence artifact.
func WriteQualificationTSV(destination io.Writer, q Qualification) error {
	if destination == nil || q.Validate() != nil {
		return ErrInvalidReport
	}
	var out strings.Builder
	fmtLine := func(fields ...string) { out.WriteString(strings.Join(fields, "\t")); out.WriteByte('\n') }
	fmtLine("schema", "vibedb.rf3.qualification", strconv.FormatUint(uint64(QualificationSchemaVersion), 10))
	fmtLine(append([]string{"qualification_header"}, qualificationNames...)...)
	values := qualificationValues(q)
	row := make([]string, 1, len(values)+1)
	row[0] = "qualification"
	for _, value := range values {
		row = append(row, strconv.FormatUint(value, 10))
	}
	fmtLine(row...)
	encoded := []byte(out.String())
	for len(encoded) != 0 {
		written, err := destination.Write(encoded)
		if written > 0 {
			encoded = encoded[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// ParseQualificationTSV accepts only the exact canonical encoding.
func ParseQualificationTSV(value []byte) (Qualification, error) {
	lines := strings.Split(strings.TrimSuffix(string(value), "\n"), "\n")
	if len(lines) != 3 || lines[0] != "schema\tvibedb.rf3.qualification\t1" ||
		lines[1] != "qualification_header\t"+strings.Join(qualificationNames, "\t") {
		return Qualification{}, ErrInvalidReport
	}
	fields := strings.Split(lines[2], "\t")
	if len(fields) != len(qualificationNames)+1 || fields[0] != "qualification" {
		return Qualification{}, ErrInvalidReport
	}
	values := make([]uint64, len(qualificationNames))
	for i := range values {
		parsed, err := strconv.ParseUint(fields[i+1], 10, 64)
		if err != nil {
			return Qualification{}, errors.Join(ErrInvalidReport, err)
		}
		values[i] = parsed
	}
	q := Qualification{
		KillBeforeRequestCuts: uint32(values[0]), KillAdmissionResponseCuts: uint32(values[1]), KillAfterApplyResponseCuts: uint32(values[2]),
		AsymmetricPartitionLoops: uint32(values[3]), AsymmetricRejectedConnections: values[4], WaiterWaves: uint32(values[5]),
		WaiterCalls: uint32(values[6]), WaiterCompletions: uint32(values[7]), WaiterRefusals: uint32(values[8]), WaiterReuseCompletions: uint32(values[9]),
		WALBaselineBytes: values[10], WALFinalBytes: values[11], WALGrowthBytes: values[12], WALGrowthBoundBytes: values[13],
		WaiterRSSBaselineBytes: values[14], WaiterRSSPeakBytes: values[15], WaiterRSSGrowthBytes: values[16], WaiterRSSGrowthBoundBytes: values[17],
		LostResponseApplied: values[18], AckOutcome: values[19],
	}
	if q.Validate() != nil {
		return Qualification{}, ErrInvalidReport
	}
	var canonical bytes.Buffer
	if WriteQualificationTSV(&canonical, q) != nil || !bytes.Equal(canonical.Bytes(), value) {
		return Qualification{}, ErrInvalidReport
	}
	return q, nil
}
