package rf3bench

import (
	"bytes"
	"encoding/hex"
	"io"
	"strconv"
)

const (
	ChaosSchemaVersion = uint16(1)
	MaximumChaosRuns   = 1024
)

// ChaosRun is one isolated execution of an external fault harness. ElapsedNS
// covers the test process only; build time is deliberately excluded. The
// output digest authenticates all stdout and stderr bytes without embedding
// unbounded logs in the evidence file.
type ChaosRun struct {
	Ordinal      uint32
	ElapsedNS    uint64
	OutputBytes  uint64
	OutputSHA256 [32]byte
	ExitCode     int32
	TimedOut     bool
	ExactRun     bool
	Passed       bool
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
			(run.Passed && (run.TimedOut || !run.ExactRun || run.ExitCode != 0)) {
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
		[]byte("passed"), []byte("output_bytes"), []byte("output_sha256")); err != nil {
		return err
	}
	var passed uint64
	for _, run := range report.Runs {
		if run.Passed {
			passed++
		}
		digest := make([]byte, hex.EncodedLen(len(run.OutputSHA256)))
		hex.Encode(digest, run.OutputSHA256[:])
		if err := write([]byte("raw"), u64(uint64(run.Ordinal)), u64(run.ElapsedNS),
			i64(int64(run.ExitCode)), boolean(run.TimedOut), boolean(run.ExactRun),
			boolean(run.Passed), u64(run.OutputBytes), digest); err != nil {
			return err
		}
	}
	return write([]byte("summary"), []byte("passed"), u64(passed),
		[]byte("failed"), u64(uint64(len(report.Runs))-passed))
}
