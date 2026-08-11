//go:build !race

package replicatedstate

import "testing"

var (
	codecBytesSink  []byte
	codecDigestSink [32]byte
	codecStateSink  StateV1
	codecRecordSink CompletionRecordV1
)

func TestDigestAndCompletionAppendAllocationBounds(t *testing.T) {
	_, record := codecCompletionV1(ResultApplied)
	encoded, err := AppendCompletionRecordV1(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	scratch := make([]byte, 0, len(encoded))

	if allocations := testing.AllocsPerRun(1000, func() {
		codecDigestSink = CompletionKeyV1(record.Tenant, record.ClientID, record.ClientEpoch, record.ClientSequence)
	}); allocations != 0 {
		t.Fatalf("CompletionKeyV1 allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		codecDigestSink = CommandDigestV1(record.Completion)
	}); allocations != 0 {
		t.Fatalf("CommandDigestV1 allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		var appendErr error
		codecBytesSink, appendErr = AppendCompletionRecordV1(scratch[:0], record)
		if appendErr != nil {
			panic(appendErr)
		}
	}); allocations != 0 {
		t.Fatalf("pre-sized AppendCompletionRecordV1 allocations = %v, want 0", allocations)
	}
}

func BenchmarkCompletionKeyV1(b *testing.B) {
	_, record := codecCompletionV1(ResultApplied)
	b.ReportAllocs()
	for b.Loop() {
		codecDigestSink = CompletionKeyV1(record.Tenant, record.ClientID, record.ClientEpoch, record.ClientSequence)
	}
}

func BenchmarkCommandDigestV1(b *testing.B) {
	_, record := codecCompletionV1(ResultApplied)
	b.ReportAllocs()
	b.SetBytes(int64(len(record.Completion)))
	for b.Loop() {
		codecDigestSink = CommandDigestV1(record.Completion)
	}
}

func BenchmarkAppendStateV1(b *testing.B) {
	state := codecStateV1()
	encoded, err := AppendStateV1(nil, state)
	if err != nil {
		b.Fatal(err)
	}
	scratch := make([]byte, 0, len(encoded))
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		codecBytesSink, err = AppendStateV1(scratch[:0], state)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenStateV1(b *testing.B) {
	encoded, err := AppendStateV1(nil, codecStateV1())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		codecStateSink, err = OpenStateV1(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendCompletionRecordV1(b *testing.B) {
	_, record := codecCompletionV1(ResultApplied)
	encoded, err := AppendCompletionRecordV1(nil, record)
	if err != nil {
		b.Fatal(err)
	}
	scratch := make([]byte, 0, len(encoded))
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		codecBytesSink, err = AppendCompletionRecordV1(scratch[:0], record)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenCompletionRecordV1(b *testing.B) {
	_, record := codecCompletionV1(ResultApplied)
	encoded, err := AppendCompletionRecordV1(nil, record)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		codecRecordSink, err = OpenCompletionRecordV1(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}
