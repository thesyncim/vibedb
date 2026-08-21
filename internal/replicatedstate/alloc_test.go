//go:build !race

package replicatedstate

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

var (
	codecBytesSink   []byte
	codecDigestSink  [32]byte
	codecStateSink   State
	codecRecordSink  CompletionRecord
	artifactSink     SnapshotArtifactManifest
	artifactByteSink int
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

func BenchmarkWriteSnapshotArtifact(b *testing.B) {
	_, snapshot := snapshotArtifactFixture(b)
	options := SnapshotArtifactOptions{
		PayloadBuffer: make([]byte, 0, MaxSnapshotArtifactChunkBytes),
	}
	var err error
	artifactSink, err = WriteSnapshotArtifact(io.Discard, snapshot, options)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(artifactSink.PayloadBytes))
	b.ResetTimer()
	for b.Loop() {
		artifactSink, err = WriteSnapshotArtifact(io.Discard, snapshot, options)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVerifySnapshotArtifact(b *testing.B) {
	_, snapshot := snapshotArtifactFixture(b)
	var artifact bytes.Buffer
	var err error
	artifactSink, err = WriteSnapshotArtifact(&artifact, snapshot, SnapshotArtifactOptions{})
	if err != nil {
		b.Fatal(err)
	}
	encoded := artifact.Bytes()
	callbacks := SnapshotArtifactCallbacks{
		PayloadBuffer: make([]byte, 0, MaxSnapshotArtifactChunkBytes),
	}
	b.ReportAllocs()
	b.SetBytes(int64(artifactSink.PayloadBytes))
	b.ResetTimer()
	for b.Loop() {
		artifactSink, err = VerifySnapshotArtifact(bytes.NewReader(encoded), callbacks)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSnapshotArtifactRowIteratorAllocationBound(t *testing.T) {
	_, snapshot := snapshotArtifactFixture(t)
	var artifact bytes.Buffer
	if _, err := WriteSnapshotArtifact(&artifact, snapshot, SnapshotArtifactOptions{}); err != nil {
		t.Fatal(err)
	}
	var captured SnapshotArtifactRows
	stop := errors.New("captured")
	_, _ = VerifySnapshotArtifact(bytes.NewReader(artifact.Bytes()), SnapshotArtifactCallbacks{
		Rows: func(_ SnapshotArtifactCheckpoint, rows SnapshotArtifactRows) error {
			captured = rows
			return stop
		},
	})
	if captured.Len() == 0 {
		t.Fatal("did not capture verified rows")
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		iterator := captured.Iterator()
		for {
			key, value, ok := iterator.Next()
			if !ok {
				break
			}
			artifactByteSink += len(key) + len(value)
		}
	}); allocations != 0 {
		t.Fatalf("verified row iteration allocations = %v, want 0", allocations)
	}
}

func BenchmarkSnapshotArtifactRowIterator(b *testing.B) {
	_, snapshot := snapshotArtifactFixture(b)
	var artifact bytes.Buffer
	if _, err := WriteSnapshotArtifact(&artifact, snapshot, SnapshotArtifactOptions{}); err != nil {
		b.Fatal(err)
	}
	var captured SnapshotArtifactRows
	stop := errors.New("captured")
	_, _ = VerifySnapshotArtifact(bytes.NewReader(artifact.Bytes()), SnapshotArtifactCallbacks{
		Rows: func(_ SnapshotArtifactCheckpoint, rows SnapshotArtifactRows) error {
			captured = rows
			return stop
		},
	})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		iterator := captured.Iterator()
		for {
			key, value, ok := iterator.Next()
			if !ok {
				break
			}
			artifactByteSink += len(key) + len(value)
		}
	}
}
