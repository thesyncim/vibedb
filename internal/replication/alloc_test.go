package replication

import "testing"

var (
	allocationBytesSink  []byte
	allocationIntSink    int
	allocationDigestSink Digest
)

func TestV1HotPathsAllocateZero(t *testing.T) {
	command := testCommand()
	encodedCommand := encodeCommand(t, command)
	commandScratch := make([]byte, 0, len(encodedCommand))
	if allocations := testing.AllocsPerRun(1000, func() {
		var err error
		allocationBytesSink, err = AppendCommandV1(commandScratch[:0], command)
		if err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("pre-sized AppendCommandV1 allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		view, err := OpenCommandV1(encodedCommand)
		if err != nil {
			panic(err)
		}
		iterator := view.Mutations()
		for iterator.Next() {
			allocationIntSink += len(iterator.Mutation().Key)
		}
	}); allocations != 0 {
		t.Fatalf("OpenCommandV1 + iteration allocations = %v, want 0", allocations)
	}

	completion := testInlineCompletion()
	encodedCompletion := encodeCompletion(t, completion)
	completionScratch := make([]byte, 0, len(encodedCompletion))
	if allocations := testing.AllocsPerRun(1000, func() {
		allocationDigestSink = CompletionResultDigestV1(
			completion.ResultCode, completion.ResultFormat, completion.InlineResult,
		)
	}); allocations != 0 {
		t.Fatalf("CompletionResultDigestV1 allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		var err error
		allocationBytesSink, err = AppendCompletionV1(completionScratch[:0], completion)
		if err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("pre-sized AppendCompletionV1 allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		view, err := OpenCompletionV1(encodedCompletion)
		if err != nil {
			panic(err)
		}
		allocationIntSink += len(view.InlineResult)
	}); allocations != 0 {
		t.Fatalf("OpenCompletionV1 allocations = %v, want 0", allocations)
	}
}

func BenchmarkAppendCommandV1(b *testing.B) {
	command := testCommand()
	encoded := encodeCommand(b, command)
	scratch := make([]byte, 0, len(encoded))
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		var err error
		allocationBytesSink, err = AppendCommandV1(scratch[:0], command)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenCommandV1(b *testing.B) {
	encoded := encodeCommand(b, testCommand())
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		view, err := OpenCommandV1(encoded)
		if err != nil {
			b.Fatal(err)
		}
		iterator := view.Mutations()
		for iterator.Next() {
			allocationIntSink += len(iterator.Mutation().Key)
		}
	}
}

func BenchmarkAppendCompletionV1(b *testing.B) {
	completion := testInlineCompletion()
	encoded := encodeCompletion(b, completion)
	scratch := make([]byte, 0, len(encoded))
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		var err error
		allocationBytesSink, err = AppendCompletionV1(scratch[:0], completion)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenCompletionV1(b *testing.B) {
	encoded := encodeCompletion(b, testInlineCompletion())
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		view, err := OpenCompletionV1(encoded)
		if err != nil {
			b.Fatal(err)
		}
		allocationIntSink += len(view.InlineResult)
	}
}
