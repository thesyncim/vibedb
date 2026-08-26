package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

func TestSourceCaptureDescriptorCodecCanonicalAndBounded(t *testing.T) {
	d := testSourceCaptureDescriptor()
	prefix := []byte("retained-prefix")
	raw, err := AppendSourceCaptureDescriptor(bytes.Clone(prefix), d)
	if err != nil || !bytes.Equal(raw[:len(prefix)], prefix) {
		t.Fatalf("append prefix=%t err=%v", bytes.Equal(raw[:len(prefix)], prefix), err)
	}
	frame := raw[len(prefix):]
	opened, err := OpenSourceCaptureDescriptor(frame)
	if err != nil || opened != d {
		t.Fatalf("open=%+v want=%+v err=%v", opened, d, err)
	}
	reencoded, err := AppendSourceCaptureDescriptor(nil, opened)
	if err != nil || !bytes.Equal(reencoded, frame) {
		t.Fatalf("canonical=%t err=%v", bytes.Equal(reencoded, frame), err)
	}
	for _, mutate := range []func([]byte){
		func(v []byte) { v[8]++ },
		func(v []byte) { v[16] = 1 },
		func(v []byte) { v[352] = 0 },
		func(v []byte) { v[len(v)-1] ^= 1 },
	} {
		corrupt := bytes.Clone(frame)
		mutate(corrupt)
		if _, err := OpenSourceCaptureDescriptor(corrupt); !errors.Is(err, ErrSplitControlRecord) {
			t.Fatalf("corrupt accepted: %v", err)
		}
	}
	invalid := d
	invalid.Head.Applied = invalid.Base.Applied - 1
	if got, err := AppendSourceCaptureDescriptor(prefix, invalid); !errors.Is(err, ErrSplitControlRecord) || !bytes.Equal(got, prefix) {
		t.Fatalf("invalid mutated destination=%t err=%v", !bytes.Equal(got, prefix), err)
	}
}

func TestSourceCaptureDescriptorReflectsPublishedCapture(t *testing.T) {
	partitioner, err := NewPartitioner(testSplitPlan(t, "node-b"), "docs", []string{"/tenant"}, distribution.DefaultVirtualBucketBits)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newSourceCaptureFixture(t, partitioner)
	if _, err = fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	capture, err := NewSourceCapture(partitioner, "split-capture", fixture.capture)
	if err != nil {
		t.Fatal(err)
	}
	if err = fixture.machine.BeginTransitionCapture(capture); err != nil {
		t.Fatal(err)
	}
	descriptor, err := capture.Descriptor()
	if err != nil || descriptor.Head.Applied != capture.Head() || descriptor.Collection != "docs" {
		t.Fatalf("descriptor=%+v err=%v", descriptor, err)
	}
	if err = partitioner.ValidateSourceCaptureDescriptor(descriptor); err != nil {
		t.Fatal(err)
	}
	raw, err := AppendSourceCaptureDescriptor(nil, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenSourceCaptureDescriptor(raw)
	if err != nil || opened != descriptor {
		t.Fatalf("reopen equal=%t err=%v", opened == descriptor, err)
	}
}

func TestChildArtifactSetCodecCanonicalAndBounded(t *testing.T) {
	set := testChildArtifactControlSet()
	raw, err := AppendChildArtifactSet(nil, set)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenChildArtifactSet(raw)
	if err != nil || opened != set {
		t.Fatalf("open equal=%t err=%v", opened == set, err)
	}
	reencoded, err := AppendChildArtifactSet(nil, opened)
	if err != nil || !bytes.Equal(reencoded, raw) {
		t.Fatalf("canonical=%t err=%v", bytes.Equal(reencoded, raw), err)
	}
	for offset := 0; offset < len(raw); offset += max(1, len(raw)/31) {
		corrupt := bytes.Clone(raw)
		corrupt[offset] ^= 0x80
		if _, err := OpenChildArtifactSet(corrupt); !errors.Is(err, ErrSplitControlRecord) {
			t.Fatalf("corruption at %d accepted: %v", offset, err)
		}
	}
	invalid := set
	invalid.Children[0].Descriptor.Range.Start = distribution.KeyspacePoint{0xff}
	if _, err := AppendChildArtifactSet(nil, invalid); !errors.Is(err, ErrSplitControlRecord) {
		t.Fatalf("invalid range err=%v", err)
	}
}

func TestTailCursorCodecCanonicalAndStrict(t *testing.T) {
	cursor := testTailControlCursor()
	raw, err := AppendTailCursor(nil, cursor)
	if err != nil || len(raw) != tailControlBytes {
		t.Fatalf("bytes=%d err=%v", len(raw), err)
	}
	opened, err := OpenTailCursor(raw)
	if err != nil || opened != cursor {
		t.Fatalf("open equal=%t err=%v", opened == cursor, err)
	}
	reencoded, err := AppendTailCursor(nil, opened)
	if err != nil || !bytes.Equal(reencoded, raw) {
		t.Fatalf("canonical=%t err=%v", bytes.Equal(reencoded, raw), err)
	}
	for _, offset := range []int{0, 10, 16, 100, 320, 321, len(raw) - 1} {
		corrupt := bytes.Clone(raw)
		corrupt[offset] ^= 1
		if _, err := OpenTailCursor(corrupt); !errors.Is(err, ErrSplitControlRecord) {
			t.Fatalf("corruption at %d accepted: %v", offset, err)
		}
	}
	binary := cursor
	binary.childBaseDigests[2] = [sha256.Size]byte{}
	if _, err := AppendTailCursor(nil, binary); err != nil {
		t.Fatalf("binary split cursor: %v", err)
	}
}

func TestSplitControlAppendDoesNotAllocateWithCapacity(t *testing.T) {
	if controlRaceEnabled {
		t.Skip("race instrumentation changes allocation accounting")
	}
	descriptor := testSourceCaptureDescriptor()
	artifacts := testChildArtifactControlSet()
	tail := testTailControlCursor()
	captureBuffer := make([]byte, 0, captureControlHeaderBytes+len(descriptor.Collection)+sha256.Size)
	artifactBuffer := make([]byte, 0, MaxArtifactControlRecordBytes)
	tailBuffer := make([]byte, 0, tailControlBytes)
	var captureWorkspace, artifactWorkspace, tailWorkspace SplitControlRecordWorkspace
	_, _ = AppendSourceCaptureDescriptorWithWorkspace(captureBuffer[:0], descriptor, &captureWorkspace)
	_, _ = AppendChildArtifactSetWithWorkspace(artifactBuffer[:0], artifacts, &artifactWorkspace)
	_, _ = AppendTailCursorWithWorkspace(tailBuffer[:0], tail, &tailWorkspace)
	if got := testing.AllocsPerRun(100, func() {
		var err error
		captureBuffer, err = AppendSourceCaptureDescriptorWithWorkspace(captureBuffer[:0], descriptor, &captureWorkspace)
		if err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("capture allocations=%g", got)
	}
	if got := testing.AllocsPerRun(100, func() {
		var err error
		artifactBuffer, err = AppendChildArtifactSetWithWorkspace(artifactBuffer[:0], artifacts, &artifactWorkspace)
		if err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("artifact allocations=%g", got)
	}
	if got := testing.AllocsPerRun(100, func() {
		var err error
		tailBuffer, err = AppendTailCursorWithWorkspace(tailBuffer[:0], tail, &tailWorkspace)
		if err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("tail allocations=%g", got)
	}
}

func FuzzOpenSourceCaptureDescriptorCanonical(f *testing.F) {
	seed, _ := AppendSourceCaptureDescriptor(nil, testSourceCaptureDescriptor())
	f.Add(seed)
	f.Add([]byte(nil))
	f.Add([]byte("not-a-record"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		d, err := OpenSourceCaptureDescriptor(raw)
		if err != nil {
			return
		}
		canonical, err := AppendSourceCaptureDescriptor(nil, d)
		if err != nil || !bytes.Equal(canonical, raw) {
			t.Fatalf("noncanonical success err=%v", err)
		}
	})
}

func FuzzOpenChildArtifactSetCanonical(f *testing.F) {
	seed, _ := AppendChildArtifactSet(nil, testChildArtifactControlSet())
	f.Add(seed)
	f.Add([]byte(nil))
	f.Add([]byte("not-a-record"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		set, err := OpenChildArtifactSet(raw)
		if err != nil {
			return
		}
		canonical, err := AppendChildArtifactSet(nil, set)
		if err != nil || !bytes.Equal(canonical, raw) {
			t.Fatalf("noncanonical success err=%v", err)
		}
	})
}

func FuzzOpenTailCursorCanonical(f *testing.F) {
	seed, _ := AppendTailCursor(nil, testTailControlCursor())
	f.Add(seed)
	f.Add([]byte(nil))
	f.Add([]byte("not-a-record"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		cursor, err := OpenTailCursor(raw)
		if err != nil {
			return
		}
		canonical, err := AppendTailCursor(nil, cursor)
		if err != nil || !bytes.Equal(canonical, raw) {
			t.Fatalf("noncanonical success err=%v", err)
		}
	})
}

func FuzzOpenRetainedPruneCursorCanonical(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("not-a-record"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		cursor, err := OpenRetainedPruneCursor(raw)
		if err != nil {
			return
		}
		canonical, err := AppendRetainedPruneCursor(nil, cursor)
		if err != nil || !bytes.Equal(canonical, raw) {
			t.Fatalf("noncanonical success err=%v", err)
		}
	})
}

func testSourceCaptureDescriptor() SourceCaptureDescriptor {
	return SourceCaptureDescriptor{
		PlanDigest: testControlDigest(1), PlacementDigest: testControlDigest(2), Collection: "docs",
		Base:        ChildArtifactSourceCut{DataChainDigest: testControlDigest(3), BaseDigest: testControlDigest(4), EntryDigest: testControlDigest(5), Applied: 10, Term: 2, RouteGeneration: 7},
		Head:        ChildArtifactSourceCut{DataChainDigest: testControlDigest(6), BaseDigest: testControlDigest(4), EntryDigest: testControlDigest(7), Applied: 12, Term: 3, RouteGeneration: 8},
		Coordinates: TailSourceCoordinates{OwnershipEpoch: 4, RoutingVersion: 5, RouteGeneration: 8},
	}
}

func testTailControlCursor() TailCursor {
	return TailCursor{
		planDigest: testControlDigest(1), placementDigest: testControlDigest(2), dataChainDigest: testControlDigest(3),
		baseDigest: testControlDigest(4), entryDigest: testControlDigest(5),
		childBaseDigests: [3][sha256.Size]byte{testControlDigest(6), testControlDigest(7), testControlDigest(8)},
		applied:          11, term: 2, ownershipEpoch: 3, routingVersion: 4, routeGeneration: 5, sealed: true,
	}
}

func testChildArtifactControlSet() ChildArtifactSet {
	partition := PartitionStats{
		PlanDigest: testControlDigest(1), SourceDigest: testControlDigest(2), SourceBase: testControlDigest(3),
		SourceEntry: testControlDigest(4), SourceApplied: 10, SourceTerm: 2, RouteGeneration: 3,
		Rows: [3]uint64{1, 1}, Bytes: [3]uint64{9, 8},
	}
	cut := ChildArtifactSourceCut{DataChainDigest: partition.SourceDigest, BaseDigest: partition.SourceBase,
		EntryDigest: partition.SourceEntry, Applied: partition.SourceApplied, Term: partition.SourceTerm,
		RouteGeneration: partition.RouteGeneration}
	manifest := ChildArtifactManifest{
		Present: true, Child: 0, PlanDigest: partition.PlanDigest, PlacementDigest: testControlDigest(5), Source: cut,
		TargetRoutingVersion: 2, Descriptor: ChildArtifactDescriptor{
			Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Point: distribution.KeyspacePoint{0x80}}},
			Shard: "child-a", AllocationGeneration: 2, OwnershipEpoch: 3, LeaderCount: 3,
		},
		TargetChunkBytes: MinChildArtifactChunkBytes, Chunks: 1, Rows: 1, RowBytes: 9, PayloadBytes: 17,
		EncodedBytes: 999, HeaderDigest: testControlDigest(6), LastChunkDigest: testControlDigest(7), Digest: testControlDigest(8),
	}
	return ChildArtifactSet{Partition: partition, Children: [3]ChildArtifactManifest{manifest}}
}

func testControlDigest(value byte) [sha256.Size]byte { var d [sha256.Size]byte; d[0] = value; return d }
