package raftsim

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"testing"
)

func sampleTrace(t *testing.T) *Trace {
	t.Helper()
	var scenario [32]byte
	for i := range scenario {
		scenario[i] = byte(i)
	}
	trace := NewTrace(0x0102030405060708, scenario)
	events := []Event{
		{Kind: EventCampaign, Time: 0, Node: 1},
		{Kind: EventCaptureReady, Time: 1, Node: 1, Ref: 1},
		{Kind: EventPersistReady, Time: 1, Node: 1, Ref: 7},
		{Kind: EventSendMessage, Time: 2, Node: 1, Ref: 9},
		{Kind: EventDeliverMessage, Time: 3, Node: 2, Peer: 1, Ref: 9},
	}
	for _, event := range events {
		if err := trace.Append(event); err != nil {
			t.Fatal(err)
		}
	}
	return trace
}

func TestTraceRoundTripAndByteExactReplay(t *testing.T) {
	const golden = "56444253494d0000010001004000300038010000050000000807060504030201000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000500000001000000010000000000000001000000000000000000000000000000010000000000000000000000000000000600000002000000010000000000000001000000000000000000000000000000070000000000000000000000000000000700000003000000020000000000000001000000000000000000000000000000090000000000000000000000000000000e0000000400000003000000000000000200000000000000010000000000000009000000000000000000000000000000ed857959127a86a6"
	trace := sampleTrace(t)
	first, err := trace.AppendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(first); got != golden {
		t.Fatalf("trace golden mismatch:\ngot  %s\nwant %s", got, golden)
	}
	opened, err := OpenTrace(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := opened.AppendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("decode/re-encode changed canonical trace")
	}
	if trace.Header() != opened.Header() || trace.Len() != opened.Len() {
		t.Fatal("round trip changed trace identity")
	}
}

func TestTraceRejectsEveryOneBitCorruption(t *testing.T) {
	encoded, err := sampleTrace(t).AppendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range encoded {
		for bit := byte(1); bit != 0; bit <<= 1 {
			mutated := bytes.Clone(encoded)
			mutated[i] ^= bit
			if _, err := OpenTrace(mutated); err == nil {
				t.Fatalf("accepted bit flip at byte %d mask %#x", i, bit)
			}
		}
	}
}

func TestTraceRejectsTruncationTrailingAndInvalidEvent(t *testing.T) {
	encoded, err := sampleTrace(t).AppendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range encoded {
		if _, err := OpenTrace(encoded[:i]); err == nil {
			t.Fatalf("accepted truncation at %d", i)
		}
	}
	if _, err := OpenTrace(append(bytes.Clone(encoded), 0)); err == nil {
		t.Fatal("accepted trailing byte")
	}
	trace := NewTrace(1, [32]byte{})
	if err := trace.Append(Event{Kind: EventCampaign}); !errors.Is(err, ErrInvalidTrace) {
		t.Fatalf("invalid event = %v", err)
	}
	if err := trace.Append(Event{Kind: EventCampaign, Node: 1, Time: 2}); err != nil {
		t.Fatal(err)
	}
	if err := trace.Append(Event{Kind: EventCrash, Node: 1, Time: 1}); !errors.Is(err, ErrInvalidTrace) {
		t.Fatalf("time regression = %v", err)
	}
}

func TestOpenTraceRejectsResealedLogicalTimeRegression(t *testing.T) {
	encoded, err := sampleTrace(t).AppendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	// The final event originally occurs at time 3; move it behind time 2 and
	// recompute the public CRC so semantic validation, not corruption, rejects.
	offset := traceHeaderBytes + 4*traceEventBytes
	binary.LittleEndian.PutUint64(encoded[offset+8:offset+16], 1)
	trailer := len(encoded) - traceTrailerBytes
	sum := crc32.Checksum(encoded[:trailer], traceCRC)
	binary.LittleEndian.PutUint32(encoded[trailer:trailer+4], sum)
	binary.LittleEndian.PutUint32(encoded[trailer+4:], ^sum)
	if _, err := OpenTrace(encoded); !errors.Is(err, ErrInvalidTrace) {
		t.Fatalf("OpenTrace(time regression) error = %v", err)
	}
}

type replayRecorder struct {
	events   []Event
	failAt   uint32
	identity ReplayIdentity
}

func (r *replayRecorder) ReplayIdentity() ReplayIdentity { return r.identity }

func (r *replayRecorder) Execute(event Event) error {
	if event.Step == r.failAt {
		return errors.New("injected replay failure")
	}
	r.events = append(r.events, event)
	return nil
}

func TestReplayReportsFirstFailure(t *testing.T) {
	trace := sampleTrace(t)
	header := trace.Header()
	recorder := &replayRecorder{failAt: 3, identity: ReplayIdentity{
		SimulatorVersion: header.SimulatorVersion,
		ScenarioDigest:   header.ScenarioDigest,
	}}
	step, err := Replay(trace, recorder)
	if err == nil || step != 3 || len(recorder.events) != 3 {
		t.Fatalf("Replay = step %d, events %d, err %v", step, len(recorder.events), err)
	}
}

func TestReplayRejectsDifferentScenario(t *testing.T) {
	trace := sampleTrace(t)
	header := trace.Header()
	header.ScenarioDigest[0] ^= 1
	recorder := &replayRecorder{failAt: ^uint32(0), identity: ReplayIdentity{
		SimulatorVersion: header.SimulatorVersion,
		ScenarioDigest:   header.ScenarioDigest,
	}}
	if step, err := Replay(trace, recorder); !errors.Is(err, ErrInvalidTrace) || step != 0 {
		t.Fatalf("Replay mismatch = step %d, err %v", step, err)
	}
}

func TestTraceMaximumEventCountAndOverflow(t *testing.T) {
	trace := NewTrace(1, [32]byte{})
	trace.events = make([]Event, MaxTraceEvents)
	for i := range trace.events {
		trace.events[i] = Event{
			Step: uint32(i), Kind: EventLeaderTick, Time: uint64(i), Node: 1,
		}
	}
	encoded, err := trace.AppendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	wantBytes := traceHeaderBytes + MaxTraceEvents*traceEventBytes + traceTrailerBytes
	if len(encoded) != wantBytes {
		t.Fatalf("maximum trace bytes = %d, want %d", len(encoded), wantBytes)
	}
	opened, err := OpenTrace(encoded)
	if err != nil || opened.Len() != MaxTraceEvents {
		t.Fatalf("OpenTrace(maximum) = len %d, err %v", opened.Len(), err)
	}
	trace.events = append(trace.events, Event{})
	if _, err := trace.AppendBinary(nil); err == nil {
		t.Fatal("encoded more than MaxTraceEvents")
	}
}

type replayCounter int

var replayBenchmarkIdentity ReplayIdentity

func (*replayCounter) ReplayIdentity() ReplayIdentity { return replayBenchmarkIdentity }

func (r *replayCounter) Execute(Event) error {
	*r++
	return nil
}

func BenchmarkTraceReplay10000Events(b *testing.B) {
	var scenario [32]byte
	trace := NewTrace(7, scenario)
	for i := 0; i < 10_000; i++ {
		if err := trace.Append(Event{Kind: EventLeaderTick, Node: 1}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	header := trace.Header()
	replayBenchmarkIdentity = ReplayIdentity{
		SimulatorVersion: header.SimulatorVersion,
		ScenarioDigest:   header.ScenarioDigest,
	}
	var count replayCounter
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count = 0
		if n, err := Replay(trace, &count); err != nil || n != trace.Len() || int(count) != n {
			b.Fatalf("Replay = %d, %v", n, err)
		}
	}
}
