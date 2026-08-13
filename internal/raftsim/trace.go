package raftsim

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	// TraceFormatVersion freezes the canonical byte grammar.
	TraceFormatVersion uint16 = 1
	// SimulatorVersion freezes the event vocabulary and choice algorithm.
	SimulatorVersion uint16 = 1
	// MaxTraceEvents bounds both retained memory and encoded trace size.
	MaxTraceEvents = 1 << 18

	traceHeaderBytes  = 64
	traceEventBytes   = 48
	traceTrailerBytes = 8
)

var (
	traceMagic = [8]byte{'V', 'D', 'B', 'S', 'I', 'M', 0, 0}
	traceCRC   = crc32.MakeTable(crc32.Castagnoli)

	// ErrInvalidTrace reports malformed, noncanonical, corrupt, or unsupported
	// trace bytes.
	ErrInvalidTrace = errors.New("raftsim: invalid trace")
	// ErrTraceFull reports the fixed event bound.
	ErrTraceFull = errors.New("raftsim: trace event limit reached")
)

// TraceHeader identifies the exact scenario and seeded choice stream.
type TraceHeader struct {
	FormatVersion    uint16
	SimulatorVersion uint16
	Seed             uint64
	ScenarioDigest   [32]byte
}

// Trace is an owned, bounded sequence of simulator decisions.
type Trace struct {
	header TraceHeader
	events []Event
}

// NewTrace returns an empty trace for one exact scenario and seed.
func NewTrace(seed uint64, scenarioDigest [32]byte) *Trace {
	return &Trace{header: TraceHeader{
		FormatVersion: TraceFormatVersion, SimulatorVersion: SimulatorVersion,
		Seed: seed, ScenarioDigest: scenarioDigest,
	}}
}

// Header returns the immutable trace identity.
func (t *Trace) Header() TraceHeader {
	if t == nil {
		return TraceHeader{}
	}
	return t.header
}

// Len reports the number of recorded decisions.
func (t *Trace) Len() int {
	if t == nil {
		return 0
	}
	return len(t.events)
}

// Event returns one decision without exposing the retained slice.
func (t *Trace) Event(i int) (Event, bool) {
	if t == nil || i < 0 || i >= len(t.events) {
		return Event{}, false
	}
	return t.events[i], true
}

// Append validates and records e. Step is assigned canonically and any caller
// value is ignored.
func (t *Trace) Append(e Event) error {
	if t == nil || len(t.events) >= MaxTraceEvents {
		return ErrTraceFull
	}
	e.Step = uint32(len(t.events))
	if !e.Valid() {
		return ErrInvalidTrace
	}
	if len(t.events) != 0 && e.Time < t.events[len(t.events)-1].Time {
		return ErrInvalidTrace
	}
	t.events = append(t.events, e)
	return nil
}

// AppendBinary appends the canonical trace encoding to dst.
func (t *Trace) AppendBinary(dst []byte) ([]byte, error) {
	if t == nil || t.header.FormatVersion != TraceFormatVersion ||
		t.header.SimulatorVersion != SimulatorVersion || len(t.events) > MaxTraceEvents {
		return dst, ErrInvalidTrace
	}
	total64 := uint64(traceHeaderBytes) + uint64(len(t.events))*traceEventBytes + traceTrailerBytes
	if total64 > uint64(^uint32(0)) || total64 > uint64(int(^uint(0)>>1)-len(dst)) {
		return dst, ErrTraceFull
	}
	start := len(dst)
	dst = append(dst, make([]byte, int(total64))...)
	b := dst[start:]
	copy(b[0:8], traceMagic[:])
	binary.LittleEndian.PutUint16(b[8:10], t.header.FormatVersion)
	binary.LittleEndian.PutUint16(b[10:12], t.header.SimulatorVersion)
	binary.LittleEndian.PutUint16(b[12:14], traceHeaderBytes)
	binary.LittleEndian.PutUint16(b[14:16], traceEventBytes)
	binary.LittleEndian.PutUint32(b[16:20], uint32(total64))
	binary.LittleEndian.PutUint32(b[20:24], uint32(len(t.events)))
	binary.LittleEndian.PutUint64(b[24:32], t.header.Seed)
	copy(b[32:64], t.header.ScenarioDigest[:])
	for i, e := range t.events {
		if e.Step != uint32(i) || !e.Valid() ||
			(i != 0 && e.Time < t.events[i-1].Time) {
			return dst[:start], ErrInvalidTrace
		}
		off := traceHeaderBytes + i*traceEventBytes
		b[off] = byte(e.Kind)
		// off+1 is flags and off+2:off+4 is reserved; zero from make.
		binary.LittleEndian.PutUint32(b[off+4:off+8], e.Step)
		binary.LittleEndian.PutUint64(b[off+8:off+16], e.Time)
		binary.LittleEndian.PutUint64(b[off+16:off+24], e.Node)
		binary.LittleEndian.PutUint64(b[off+24:off+32], e.Peer)
		binary.LittleEndian.PutUint64(b[off+32:off+40], e.Ref)
		binary.LittleEndian.PutUint64(b[off+40:off+48], e.Value)
	}
	trailer := len(b) - traceTrailerBytes
	sum := crc32.Checksum(b[:trailer], traceCRC)
	binary.LittleEndian.PutUint32(b[trailer:trailer+4], sum)
	binary.LittleEndian.PutUint32(b[trailer+4:], ^sum)
	return dst, nil
}

// OpenTrace verifies and owns one canonical trace encoding.
func OpenTrace(src []byte) (*Trace, error) {
	if len(src) < traceHeaderBytes+traceTrailerBytes || string(src[:8]) != string(traceMagic[:]) {
		return nil, ErrInvalidTrace
	}
	format := binary.LittleEndian.Uint16(src[8:10])
	simulator := binary.LittleEndian.Uint16(src[10:12])
	if format != TraceFormatVersion || simulator != SimulatorVersion ||
		binary.LittleEndian.Uint16(src[12:14]) != traceHeaderBytes ||
		binary.LittleEndian.Uint16(src[14:16]) != traceEventBytes ||
		uint64(binary.LittleEndian.Uint32(src[16:20])) != uint64(len(src)) {
		return nil, ErrInvalidTrace
	}
	count := binary.LittleEndian.Uint32(src[20:24])
	if count > MaxTraceEvents {
		return nil, ErrInvalidTrace
	}
	want := uint64(traceHeaderBytes) + uint64(count)*traceEventBytes + traceTrailerBytes
	if want != uint64(len(src)) {
		return nil, ErrInvalidTrace
	}
	trailer := len(src) - traceTrailerBytes
	sum := binary.LittleEndian.Uint32(src[trailer : trailer+4])
	if binary.LittleEndian.Uint32(src[trailer+4:]) != ^sum ||
		crc32.Checksum(src[:trailer], traceCRC) != sum {
		return nil, ErrInvalidTrace
	}
	t := &Trace{header: TraceHeader{
		FormatVersion: format, SimulatorVersion: simulator,
		Seed: binary.LittleEndian.Uint64(src[24:32]),
	}}
	copy(t.header.ScenarioDigest[:], src[32:64])
	if count != 0 {
		t.events = make([]Event, int(count))
	}
	for i := range t.events {
		off := traceHeaderBytes + i*traceEventBytes
		if src[off+1] != 0 || binary.LittleEndian.Uint16(src[off+2:off+4]) != 0 {
			return nil, ErrInvalidTrace
		}
		e := Event{
			Kind: EventKind(src[off]), Step: binary.LittleEndian.Uint32(src[off+4 : off+8]),
			Time:  binary.LittleEndian.Uint64(src[off+8 : off+16]),
			Node:  binary.LittleEndian.Uint64(src[off+16 : off+24]),
			Peer:  binary.LittleEndian.Uint64(src[off+24 : off+32]),
			Ref:   binary.LittleEndian.Uint64(src[off+32 : off+40]),
			Value: binary.LittleEndian.Uint64(src[off+40 : off+48]),
		}
		if e.Step != uint32(i) || !e.Valid() ||
			(i != 0 && e.Time < t.events[i-1].Time) {
			return nil, ErrInvalidTrace
		}
		t.events[i] = e
	}
	return t, nil
}

// Digest returns the SHA-256 digest of the canonical bytes.
func (t *Trace) Digest() ([32]byte, error) {
	b, err := t.AppendBinary(nil)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(b), nil
}

// Executor applies one recorded decision in a serialized model.
type Executor interface {
	ReplayIdentity() ReplayIdentity
	Execute(Event) error
}

// ReplayIdentity binds an executor to the same simulator grammar and exact
// scenario payloads named by trace Ref values.
type ReplayIdentity struct {
	SimulatorVersion uint16
	ScenarioDigest   [32]byte
}

// Replay applies every trace decision in order and reports the first failing
// step. A successful result equals Len().
func Replay(t *Trace, executor Executor) (int, error) {
	if t == nil || executor == nil {
		return 0, ErrInvalidTrace
	}
	identity := executor.ReplayIdentity()
	if identity.SimulatorVersion != t.header.SimulatorVersion ||
		identity.ScenarioDigest != t.header.ScenarioDigest {
		return 0, ErrInvalidTrace
	}
	for i, event := range t.events {
		if err := executor.Execute(event); err != nil {
			return i, err
		}
	}
	return len(t.events), nil
}
