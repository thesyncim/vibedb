//go:build !race

package replicatedstate

import "testing"

var (
	codecBytesSink  []byte
	codecDigestSink [32]byte
	codecStateSink  State
	codecRecordSink CompletionRecord
)

func TestDigestAndCompletionAppendAllocationBounds(t *testing.T) {
	_, record := codecCompletion(ResultApplied)
	encoded, err := AppendCompletionRecord(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	scratch := make([]byte, 0, len(encoded))

	if allocations := testing.AllocsPerRun(1000, func() {
		codecDigestSink = CompletionKey(record.Tenant, record.ClientID, record.ClientEpoch, record.ClientSequence)
	}); allocations != 0 {
		t.Fatalf("CompletionKey allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		codecDigestSink = CommandDigest(record.Completion)
	}); allocations != 0 {
		t.Fatalf("CommandDigest allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		var appendErr error
		codecBytesSink, appendErr = AppendCompletionRecord(scratch[:0], record)
		if appendErr != nil {
			panic(appendErr)
		}
	}); allocations != 0 {
		t.Fatalf("pre-sized AppendCompletionRecord allocations = %v, want 0", allocations)
	}
}

func BenchmarkCompletionKey(b *testing.B) {
	_, record := codecCompletion(ResultApplied)
	b.ReportAllocs()
	for b.Loop() {
		codecDigestSink = CompletionKey(record.Tenant, record.ClientID, record.ClientEpoch, record.ClientSequence)
	}
}

func BenchmarkCommandDigest(b *testing.B) {
	_, record := codecCompletion(ResultApplied)
	b.ReportAllocs()
	b.SetBytes(int64(len(record.Completion)))
	for b.Loop() {
		codecDigestSink = CommandDigest(record.Completion)
	}
}

func BenchmarkAppendState(b *testing.B) {
	state := codecState()
	encoded, err := AppendState(nil, state)
	if err != nil {
		b.Fatal(err)
	}
	scratch := make([]byte, 0, len(encoded))
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		codecBytesSink, err = AppendState(scratch[:0], state)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenState(b *testing.B) {
	encoded, err := AppendState(nil, codecState())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		codecStateSink, err = OpenState(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendCompletionRecord(b *testing.B) {
	_, record := codecCompletion(ResultApplied)
	encoded, err := AppendCompletionRecord(nil, record)
	if err != nil {
		b.Fatal(err)
	}
	scratch := make([]byte, 0, len(encoded))
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		codecBytesSink, err = AppendCompletionRecord(scratch[:0], record)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenCompletionRecord(b *testing.B) {
	_, record := codecCompletion(ResultApplied)
	encoded, err := AppendCompletionRecord(nil, record)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		codecRecordSink, err = OpenCompletionRecord(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}
