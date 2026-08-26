// Package rf3bench defines the stable, claim-free evidence format used by the
// RF3 benchmark and chaos qualification runners. It owns reporting only; it
// grants no cluster, storage, or fault-injection authority.
package rf3bench

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"slices"
	"strconv"
)

const SchemaVersion = uint16(1)

const (
	MaximumClients    = 4096
	MaximumOperations = 10_000_000
	MaximumMetadata   = 128
	MaximumCounters   = 1024
)

var ErrInvalidReport = errors.New("rf3bench: invalid report")

type Workload uint8

const (
	WorkloadRead Workload = iota + 1
	WorkloadWrite
	WorkloadMixed
)

func (workload Workload) bytes() []byte {
	switch workload {
	case WorkloadRead:
		return []byte("read")
	case WorkloadWrite:
		return []byte("write")
	case WorkloadMixed:
		return []byte("mixed")
	default:
		return nil
	}
}

type Operation uint8

const (
	OperationRead Operation = iota + 1
	OperationWrite
)

func (operation Operation) bytes() []byte {
	switch operation {
	case OperationRead:
		return []byte("read")
	case OperationWrite:
		return []byte("write")
	default:
		return nil
	}
}

// Config is the exact measured window. Warmup operations are excluded from
// Samples and ElapsedNS. RF3 and power-safe are fixed format facts rather than
// user-selectable labels.
type Config struct {
	Clients    uint32
	Operations uint64
	Warmup     uint64
	Seed       uint64
	ElapsedNS  uint64
	Workload   Workload
}

// Metadata is one deterministic environment fact. Keys are canonical ASCII
// identifiers and entries must be strictly ordered by Key.
type Metadata struct {
	Key   []byte
	Value []byte
}

// Sample is one completed measured gateway operation. Ordinal is globally
// unique and fixes output order independently of goroutine completion order.
// Commit and Checkpoint are the serving member's coherent post-operation cut.
type Sample struct {
	Ordinal        uint64
	Client         uint32
	ClientSequence uint64
	LatencyNS      uint64
	Applied        uint64
	Commit         uint64
	Checkpoint     uint64
	PayloadBytes   uint32
	Retries        uint32
	Operation      Operation
	Found          bool
}

// Counter records one exact monotonic before/after cut. Scope and Name are
// canonical ASCII identifiers. Examples include raft transport bytes,
// checkpoint-group syncs, and durable relation DeviceBytes/FileEnd.
type Counter struct {
	Scope  []byte
	Name   []byte
	Before uint64
	After  uint64
}

// Report is emitted as canonical TSV. The writer never uses encoding/json and
// never reorders caller data silently: noncanonical input fails closed.
type Report struct {
	Config   Config
	Metadata []Metadata
	Samples  []Sample
	Counters []Counter
}

func (report Report) Validate() error {
	config := report.Config
	if config.Clients == 0 || config.Clients > MaximumClients ||
		config.Operations == 0 || config.Operations > MaximumOperations ||
		config.ElapsedNS == 0 || config.Workload.bytes() == nil ||
		uint64(len(report.Samples)) != config.Operations ||
		len(report.Metadata) > MaximumMetadata || len(report.Counters) > MaximumCounters {
		return ErrInvalidReport
	}
	for index := range report.Metadata {
		entry := report.Metadata[index]
		if !validName(entry.Key) || reservedMetadataKey(entry.Key) || !validValue(entry.Value) ||
			(index != 0 && bytes.Compare(report.Metadata[index-1].Key, entry.Key) >= 0) {
			return ErrInvalidReport
		}
	}
	for index := range report.Samples {
		sample := report.Samples[index]
		if sample.Ordinal != uint64(index+1) || sample.Client >= config.Clients ||
			sample.ClientSequence == 0 || sample.LatencyNS == 0 ||
			sample.Operation.bytes() == nil || sample.Applied == 0 ||
			sample.Commit < sample.Applied || sample.Checkpoint > sample.Applied {
			return ErrInvalidReport
		}
	}
	for index := range report.Counters {
		counter := report.Counters[index]
		if !validName(counter.Scope) || !validName(counter.Name) || counter.After < counter.Before {
			return ErrInvalidReport
		}
		if index != 0 {
			previous := report.Counters[index-1]
			if compared := bytes.Compare(previous.Scope, counter.Scope); compared > 0 ||
				(compared == 0 && bytes.Compare(previous.Name, counter.Name) >= 0) {
				return ErrInvalidReport
			}
		}
	}
	return nil
}

func reservedMetadataKey(key []byte) bool {
	for _, reserved := range [][]byte{
		[]byte("durability"), []byte("format"), []byte("gomaxprocs"),
		[]byte("replicas"), []byte("topology"),
	} {
		if bytes.Equal(key, reserved) {
			return true
		}
	}
	return false
}

func validName(value []byte) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') || character == '_' ||
			character == '-' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func validValue(value []byte) bool {
	if len(value) > 4096 {
		return false
	}
	for _, character := range value {
		if character == '\t' || character == '\n' || character == '\r' || character == 0 {
			return false
		}
	}
	return true
}

func WriteTSV(destination io.Writer, report Report) error {
	if destination == nil {
		return ErrInvalidReport
	}
	if err := report.Validate(); err != nil {
		return err
	}
	buffer := make([]byte, 0, 1024)
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
	boolean := func(value bool) []byte { return strconv.AppendBool(nil, value) }
	if err := write([]byte("schema"), []byte("vibedb.rf3.evidence"), u64(uint64(SchemaVersion))); err != nil {
		return err
	}
	fixed := []Metadata{
		{Key: []byte("durability"), Value: []byte("power-safe")},
		{Key: []byte("format"), Value: []byte("canonical-tsv")},
		{Key: []byte("gomaxprocs"), Value: u64(uint64(runtime.GOMAXPROCS(0)))},
		{Key: []byte("replicas"), Value: []byte("3")},
		{Key: []byte("topology"), Value: []byte("rf3")},
	}
	for _, entry := range fixed {
		if err := write([]byte("meta"), entry.Key, entry.Value); err != nil {
			return err
		}
	}
	for _, entry := range report.Metadata {
		if err := write([]byte("meta"), entry.Key, entry.Value); err != nil {
			return err
		}
	}
	config := report.Config
	for _, entry := range []Metadata{
		{Key: []byte("clients"), Value: u64(uint64(config.Clients))},
		{Key: []byte("elapsed_ns"), Value: u64(config.ElapsedNS)},
		{Key: []byte("operations"), Value: u64(config.Operations)},
		{Key: []byte("seed"), Value: u64(config.Seed)},
		{Key: []byte("warmup"), Value: u64(config.Warmup)},
		{Key: []byte("workload"), Value: config.Workload.bytes()},
	} {
		if err := write([]byte("config"), entry.Key, entry.Value); err != nil {
			return err
		}
	}
	if err := write([]byte("raw_header"), []byte("ordinal"), []byte("client"),
		[]byte("client_sequence"), []byte("operation"), []byte("latency_ns"),
		[]byte("retries"), []byte("applied"), []byte("commit"),
		[]byte("checkpoint"), []byte("found"), []byte("payload_bytes")); err != nil {
		return err
	}
	for _, sample := range report.Samples {
		if err := write([]byte("raw"), u64(sample.Ordinal), u64(uint64(sample.Client)),
			u64(sample.ClientSequence), sample.Operation.bytes(), u64(sample.LatencyNS),
			u64(uint64(sample.Retries)), u64(sample.Applied), u64(sample.Commit),
			u64(sample.Checkpoint), boolean(sample.Found), u64(uint64(sample.PayloadBytes))); err != nil {
			return err
		}
	}
	if err := write([]byte("summary_header"), []byte("operation"), []byte("samples"),
		[]byte("p50_ns"), []byte("p95_ns"), []byte("p99_ns"),
		[]byte("p99.9_ns"), []byte("max_ns")); err != nil {
		return err
	}
	for _, operation := range []Operation{OperationRead, OperationWrite} {
		latencies := make([]uint64, 0, len(report.Samples))
		for _, sample := range report.Samples {
			if sample.Operation == operation {
				latencies = append(latencies, sample.LatencyNS)
			}
		}
		if len(latencies) == 0 {
			continue
		}
		slices.Sort(latencies)
		if err := write([]byte("summary"), operation.bytes(), u64(uint64(len(latencies))),
			u64(nearestRank(latencies, 500)), u64(nearestRank(latencies, 950)),
			u64(nearestRank(latencies, 990)), u64(nearestRank(latencies, 999)),
			u64(latencies[len(latencies)-1])); err != nil {
			return err
		}
	}
	for _, counter := range report.Counters {
		if err := write([]byte("counter"), counter.Scope, counter.Name,
			u64(counter.Before), u64(counter.After), u64(counter.After-counter.Before)); err != nil {
			return err
		}
	}
	return nil
}

// nearestRank uses ceil(q*n)-1 with q expressed in thousandths. It is stable
// for small samples and makes p99.9 an exact named order statistic.
func nearestRank(sorted []uint64, thousandths uint64) uint64 {
	position := (thousandths*uint64(len(sorted)) + 999) / 1000
	if position == 0 {
		position = 1
	}
	return sorted[position-1]
}
