package replication

import "testing"

var (
	allocationBytesSink  []byte
	allocationIntSink    int
	allocationDigestSink Digest
)

func TestHotPathsAllocateZero(t *testing.T) {
	command := testCommand()
	encodedCommand := encodeCommand(t, command)
	commandScratch := make([]byte, 0, len(encodedCommand))
	if allocations := testing.AllocsPerRun(1000, func() {
		var err error
		allocationBytesSink, err = AppendCommand(commandScratch[:0], command)
		if err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("pre-sized AppendCommand allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		view, err := OpenCommand(encodedCommand)
		if err != nil {
			panic(err)
		}
		iterator := view.Mutations()
		for iterator.Next() {
			allocationIntSink += len(iterator.Mutation().Key)
		}
	}); allocations != 0 {
		t.Fatalf("OpenCommand + iteration allocations = %v, want 0", allocations)
	}
	retire := testSessionRetireCommand()
	encodedRetire := encodeCommand(t, retire)
	retireScratch := make([]byte, 0, len(encodedRetire))
	if allocations := testing.AllocsPerRun(1000, func() {
		var err error
		allocationBytesSink, err = AppendCommand(retireScratch[:0], retire)
		if err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("pre-sized session-retire AppendCommand allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		view, err := OpenCommand(encodedRetire)
		if err != nil {
			panic(err)
		}
		allocationIntSink += int(view.Kind()) + view.MutationCount()
	}); allocations != 0 {
		t.Fatalf("session-retire OpenCommand allocations = %v, want 0", allocations)
	}
	open := testSessionOpenCommand()
	encodedOpen := encodeCommand(t, open)
	openScratch := make([]byte, 0, len(encodedOpen))
	if allocations := testing.AllocsPerRun(1000, func() {
		var err error
		allocationBytesSink, err = AppendCommand(openScratch[:0], open)
		if err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("pre-sized session-open AppendCommand allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		view, err := OpenCommand(encodedOpen)
		if err != nil {
			panic(err)
		}
		allocationIntSink += int(view.Kind()) + view.MutationCount()
	}); allocations != 0 {
		t.Fatalf("session-open OpenCommand allocations = %v, want 0", allocations)
	}

	completion := testInlineCompletion()
	completionBytes := testCompletionBytes(completion)
	encodedCompletion := encodeCompletion(t, completion)
	completionScratch := make([]byte, 0, len(encodedCompletion))
	if allocations := testing.AllocsPerRun(1000, func() {
		allocationDigestSink = CompletionResultDigest(
			completion.ResultCode, completion.ResultFormat, completion.InlineResult,
		)
	}); allocations != 0 {
		t.Fatalf("CompletionResultDigest allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		var err error
		allocationBytesSink, err = AppendCompletion(completionScratch[:0], completion)
		if err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("pre-sized AppendCompletion allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		var err error
		allocationBytesSink, err = AppendCompletionBytes(completionScratch[:0], completionBytes)
		if err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("pre-sized AppendCompletionBytes allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		view, err := OpenCompletion(encodedCompletion)
		if err != nil {
			panic(err)
		}
		allocationIntSink += len(view.InlineResult)
	}); allocations != 0 {
		t.Fatalf("OpenCompletion allocations = %v, want 0", allocations)
	}
}

func BenchmarkAppendCommand(b *testing.B) {
	command := testCommand()
	encoded := encodeCommand(b, command)
	scratch := make([]byte, 0, len(encoded))
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		var err error
		allocationBytesSink, err = AppendCommand(scratch[:0], command)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenCommand(b *testing.B) {
	encoded := encodeCommand(b, testCommand())
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		view, err := OpenCommand(encoded)
		if err != nil {
			b.Fatal(err)
		}
		iterator := view.Mutations()
		for iterator.Next() {
			allocationIntSink += len(iterator.Mutation().Key)
		}
	}
}

func BenchmarkAppendCompletion(b *testing.B) {
	completion := testInlineCompletion()
	encoded := encodeCompletion(b, completion)
	scratch := make([]byte, 0, len(encoded))
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		var err error
		allocationBytesSink, err = AppendCompletion(scratch[:0], completion)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendCompletionBytes(b *testing.B) {
	completion := testInlineCompletion()
	completionBytes := testCompletionBytes(completion)
	encoded := encodeCompletion(b, completion)
	scratch := make([]byte, 0, len(encoded))
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		var err error
		allocationBytesSink, err = AppendCompletionBytes(scratch[:0], completionBytes)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenCompletion(b *testing.B) {
	encoded := encodeCompletion(b, testInlineCompletion())
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		view, err := OpenCompletion(encoded)
		if err != nil {
			b.Fatal(err)
		}
		allocationIntSink += len(view.InlineResult)
	}
}
