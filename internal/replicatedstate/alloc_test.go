//go:build !race

package replicatedstate

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

var (
	codecBytesSink     []byte
	codecDigestSink    [32]byte
	codecStateSink     State
	codecSessionSink   SessionView
	codecSlotSink      SessionSlotView
	codecAuthoritySink AuthorityBindingView
	codecErrSink       error
	artifactSink       SnapshotArtifactManifest
	artifactByteSink   int
	codecBoolSink      bool
)

func TestGlobalIndexLocatorComparisonAllocatesZero(t *testing.T) {
	left := []byte(`["doc\u002d1",1.00]`)
	right := []byte(`["doc-1",1e0]`)
	if !globalIndexLocatorsEqual(left, right, 2) {
		t.Fatal("semantically equal byte-distinct locator rejected")
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		codecBoolSink = globalIndexLocatorsEqual(left, right, 2)
	}); allocations != 0 {
		t.Fatalf("global locator semantic equality allocations = %v, want 0", allocations)
	}
}

func BenchmarkGlobalIndexLocatorSemanticEquality(b *testing.B) {
	left := []byte(`["doc\u002d1",1.00]`)
	right := []byte(`["doc-1",1e0]`)
	b.ReportAllocs()
	b.SetBytes(int64(len(left) + len(right)))
	for b.Loop() {
		codecBoolSink = globalIndexLocatorsEqual(left, right, 2)
	}
}

func TestSessionCodecAllocationBounds(t *testing.T) {
	record := sessionCodecRecord()
	encodedRecord, err := AppendSessionRecord(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	slot := sessionCodecSlot(t)
	encodedSlot, err := AppendSessionSlot(nil, slot)
	if err != nil {
		t.Fatal(err)
	}
	command := openCodecLogicalCommand(t, codecLogicalCommand())
	binding := testBinding()
	machine := &Machine{
		binding: binding, distribution: []byte(binding.Distribution), shard: []byte(binding.Shard),
	}
	sessionView, err := OpenSessionRecord(encodedRecord)
	if err != nil {
		t.Fatal(err)
	}
	slotView, err := OpenSessionSlot(encodedSlot)
	if err != nil {
		t.Fatal(err)
	}
	encodedCompletion, err := machine.appendSessionCompletion(nil, sessionView, slotView)
	if err != nil {
		t.Fatal(err)
	}
	recordScratch := make([]byte, 0, len(encodedRecord))
	slotScratch := make([]byte, 0, len(encodedSlot))
	completionScratch := make([]byte, 0, len(encodedCompletion))
	authorityRecord, err := AppendAuthorityBinding(
		nil, record.Tenant, record.ClientID, record.AuthorityClass,
	)
	if err != nil {
		t.Fatal(err)
	}
	authorityScratch := make([]byte, 0, len(authorityRecord))

	if allocations := testing.AllocsPerRun(1000, func() {
		codecBytesSink, codecErrSink = AppendAuthorityBinding(
			authorityScratch[:0], record.Tenant, record.ClientID, record.AuthorityClass,
		)
	}); codecErrSink != nil || allocations != 0 {
		t.Fatalf("pre-sized AppendAuthorityBinding allocations = %v, want 0; err=%v",
			allocations, codecErrSink)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		codecAuthoritySink, codecErrSink = OpenAuthorityBinding(authorityRecord)
	}); codecErrSink != nil || allocations != 0 {
		t.Fatalf("OpenAuthorityBinding allocations = %v, want 0; err=%v",
			allocations, codecErrSink)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		codecDigestSink = SessionKey(record.AuthorityClass, record.Tenant, record.ClientID)
	}); allocations != 0 {
		t.Fatalf("SessionKey allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		codecDigestSink = LogicalCommandDigest(command)
	}); allocations != 0 {
		t.Fatalf("LogicalCommandDigest allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		codecBytesSink, codecErrSink = machine.appendSessionCompletion(
			completionScratch[:0], sessionView, slotView,
		)
		if codecErrSink != nil {
			panic(codecErrSink)
		}
	}); allocations != 0 {
		t.Fatalf("pre-sized completion reconstruction allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		codecBytesSink, codecErrSink = AppendSessionRecord(recordScratch[:0], record)
		if codecErrSink != nil {
			panic(codecErrSink)
		}
	}); allocations != 0 {
		t.Fatalf("pre-sized AppendSessionRecord allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		codecSessionSink, codecErrSink = OpenSessionRecord(encodedRecord)
	}); codecErrSink != nil || allocations != 0 {
		t.Fatalf("OpenSessionRecord allocations = %v, want 0; err=%v", allocations, codecErrSink)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		codecBytesSink, codecErrSink = AppendSessionSlot(slotScratch[:0], slot)
		if codecErrSink != nil {
			panic(codecErrSink)
		}
	}); allocations != 0 {
		t.Fatalf("pre-sized AppendSessionSlot allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		codecSlotSink, codecErrSink = OpenSessionSlot(encodedSlot)
	}); codecErrSink != nil || allocations != 0 {
		t.Fatalf("OpenSessionSlot allocations = %v, want 0; err=%v", allocations, codecErrSink)
	}
}

func BenchmarkSessionKey(b *testing.B) {
	record := sessionCodecRecord()
	b.ReportAllocs()
	for b.Loop() {
		codecDigestSink = SessionKey(record.AuthorityClass, record.Tenant, record.ClientID)
	}
}

func BenchmarkLogicalCommandDigest(b *testing.B) {
	command := openCodecLogicalCommand(b, codecLogicalCommand())
	b.ReportAllocs()
	b.SetBytes(int64(len(command.Bytes())))
	for b.Loop() {
		codecDigestSink = LogicalCommandDigest(command)
	}
}

func BenchmarkReconstructSessionCompletion(b *testing.B) {
	recordBytes, err := AppendSessionRecord(nil, sessionCodecRecord())
	if err != nil {
		b.Fatal(err)
	}
	session, err := OpenSessionRecord(recordBytes)
	if err != nil {
		b.Fatal(err)
	}
	slotBytes, err := AppendSessionSlot(nil, sessionCodecSlot(b))
	if err != nil {
		b.Fatal(err)
	}
	slot, err := OpenSessionSlot(slotBytes)
	if err != nil {
		b.Fatal(err)
	}
	binding := testBinding()
	machine := &Machine{
		binding: binding, distribution: []byte(binding.Distribution), shard: []byte(binding.Shard),
	}
	completion, err := machine.appendSessionCompletion(nil, session, slot)
	if err != nil {
		b.Fatal(err)
	}
	scratch := make([]byte, 0, len(completion))
	b.ReportAllocs()
	b.SetBytes(int64(len(completion)))
	for b.Loop() {
		codecBytesSink, codecErrSink = machine.appendSessionCompletion(scratch[:0], session, slot)
		if codecErrSink != nil {
			b.Fatal(codecErrSink)
		}
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

func BenchmarkAppendSessionRecord(b *testing.B) {
	record := sessionCodecRecord()
	encoded, err := AppendSessionRecord(nil, record)
	if err != nil {
		b.Fatal(err)
	}
	scratch := make([]byte, 0, len(encoded))
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		codecBytesSink, err = AppendSessionRecord(scratch[:0], record)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenSessionRecord(b *testing.B) {
	encoded, err := AppendSessionRecord(nil, sessionCodecRecord())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		codecSessionSink, err = OpenSessionRecord(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendSessionSlot(b *testing.B) {
	slot := sessionCodecSlot(b)
	encoded, err := AppendSessionSlot(nil, slot)
	if err != nil {
		b.Fatal(err)
	}
	scratch := make([]byte, 0, len(encoded))
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		codecBytesSink, err = AppendSessionSlot(scratch[:0], slot)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenSessionSlot(b *testing.B) {
	encoded, err := AppendSessionSlot(nil, sessionCodecSlot(b))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		codecSlotSink, err = OpenSessionSlot(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteSnapshotArtifact(b *testing.B) {
	_, snapshot := snapshotArtifactFixture(b)
	options := SnapshotArtifactOptions{
		PayloadBuffer: make([]byte, 0, DefaultSnapshotArtifactChunkBytes),
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

func BenchmarkWriteSnapshotArtifactExceptionalRow(b *testing.B) {
	key := bytes.Repeat([]byte{'k'}, replication.MaxMutationKeyBytes)
	value := bytes.Repeat([]byte{'v'}, replication.MaxMutationValueBytes)
	rowBytes, ok := snapshotArtifactRowBytes(key, value)
	if !ok || rowBytes != MaxSnapshotArtifactChunkBytes {
		b.Fatalf("maximum exceptional row = %d, %t", rowBytes, ok)
	}
	writer := snapshotArtifactWriter{
		w: io.Discard, target: DefaultSnapshotArtifactChunkBytes,
		payload:    make([]byte, 0, DefaultSnapshotArtifactChunkBytes),
		collection: SnapshotArtifactUser,
	}
	b.ReportAllocs()
	b.SetBytes(int64(rowBytes))
	b.ResetTimer()
	for b.Loop() {
		if err := writer.writeExceptionalRow(key, value, rowBytes); err != nil {
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
