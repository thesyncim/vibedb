package replicatedstate

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

func bundleSnapshotArtifactFixture(
	t *testing.T,
) ([]byte, SnapshotArtifactManifest, *ReadSnapshot) {
	t.Helper()
	fixture := newRelationBundleFixture(t, true)
	command := fixture.command(t, 1,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: []byte("doc"),
			Value: []byte(`{"email":"a","n":1}`),
		}}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: []byte{0x91, 0x01, 'a'},
			Value: []byte(`["doc"]`),
		}}},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	snapshot, err := fixture.machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	artifact, manifest := writeSnapshotArtifactFixture(t, snapshot)
	return artifact, manifest, snapshot
}

func TestBundleSnapshotArtifactRoundTripCursorAndSnapshotBase(t *testing.T) {
	artifact, written, snapshot := bundleSnapshotArtifactFixture(t)
	defer snapshot.Close()
	if !written.Bundle || len(written.Relations) != 2 ||
		written.RelationManifestDigest != snapshot.Fence().RelationManifestDigest ||
		written.Relations[0].Rows != 1 || written.Relations[1].Rows != 1 {
		t.Fatalf("bundle manifest = %+v", written)
	}
	seen := [3]uint64{}
	verified, err := VerifySnapshotArtifact(bytes.NewReader(artifact), SnapshotArtifactCallbacks{
		Rows: func(checkpoint SnapshotArtifactCheckpoint, rows SnapshotArtifactRows) error {
			if rows.Relation() != checkpoint.Relation || rows.Collection() != checkpoint.Collection {
				t.Fatalf("row identity = %d/%d checkpoint=%+v",
					rows.Collection(), rows.Relation(), checkpoint)
			}
			if checkpoint.Collection == SnapshotArtifactUser {
				seen[checkpoint.Relation] += checkpoint.Rows
			}
			return nil
		},
	})
	if err != nil || !equalSnapshotArtifactManifest(verified, written) ||
		seen[1] != written.Relations[0].Rows || seen[2] != written.Relations[1].Rows {
		t.Fatalf("verified=%+v seen=%v err=%v", verified, seen, err)
	}
	base, err := BuildSnapshotBase(written, testBootstrap())
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenSnapshotBase(base)
	if err != nil || !equalSnapshotArtifactManifest(opened.Manifest, written) {
		t.Fatalf("open base manifest=%+v err=%v", opened.Manifest, err)
	}

	first := bytes.Index(artifact, snapshotArtifactRelationMagic[:])
	secondRelative := -1
	if first >= 0 {
		secondRelative = bytes.Index(
			artifact[first+snapshotArtifactRelationBytes:], snapshotArtifactRelationMagic[:],
		)
	}
	if first < 0 || secondRelative < 0 {
		t.Fatalf("relation certificates missing: first=%d second=%d", first, secondRelative)
	}
	second := first + snapshotArtifactRelationBytes + secondRelative
	prefixEnd := second + snapshotArtifactRelationBytes
	_, cursor, err := ContinueSnapshotArtifact(
		bytes.NewReader(artifact[:prefixEnd]), nil, SnapshotArtifactCallbacks{},
	)
	if !errors.Is(err, ErrSnapshotArtifact) || cursor == nil ||
		cursor.Offset() != uint64(prefixEnd) ||
		cursor.PrefixManifest().Relations[1].ImageDigest == ([32]byte{}) {
		t.Fatalf("relation boundary cursor offset=%d want=%d err=%v",
			cursor.Offset(), prefixEnd, err)
	}
	raw, err := AppendSnapshotArtifactCursor(nil, cursor)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSnapshotArtifactCursor(raw)
	if err != nil || reopened.Offset() != cursor.Offset() ||
		!equalSnapshotArtifactManifest(reopened.PrefixManifest(), cursor.PrefixManifest()) {
		t.Fatalf("reopened cursor offset=%d err=%v", reopened.Offset(), err)
	}
	resumed, _, err := ContinueSnapshotArtifact(
		bytes.NewReader(artifact[prefixEnd:]), reopened, SnapshotArtifactCallbacks{},
	)
	if err != nil || !equalSnapshotArtifactManifest(resumed, written) {
		t.Fatalf("resumed=%+v err=%v", resumed, err)
	}
}

func TestStreamedBundleSnapshotBaseWithoutCaptureInstalls(t *testing.T) {
	fixture := newRelationBundleFixture(t, false)
	cut, err := fixture.machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	_, manifest := writeSnapshotArtifactFixture(t, cut)
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	base, err := BuildSnapshotBase(manifest, testBootstrap())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.InstallSnapshot(base); err != nil {
		t.Fatalf("install streamed bundle without capture: %v", err)
	}
}

func TestBundleSnapshotArtifactRejectsRelationCertificateCorruption(t *testing.T) {
	artifact, _, snapshot := bundleSnapshotArtifactFixture(t)
	defer snapshot.Close()
	at := bytes.Index(artifact, snapshotArtifactRelationMagic[:])
	if at < 0 {
		t.Fatal("relation certificate missing")
	}
	for _, offset := range []int{19, 24, 32, 64, 96} {
		corrupt := bytes.Clone(artifact)
		corrupt[at+offset] ^= 1
		if _, err := VerifySnapshotArtifact(
			bytes.NewReader(corrupt), SnapshotArtifactCallbacks{},
		); !errors.Is(err, ErrSnapshotArtifact) {
			t.Fatalf("corruption at %d accepted: %v", offset, err)
		}
	}
}

func TestBundleSnapshotArtifactFailedCallbackDoesNotAdvancePriorCursor(t *testing.T) {
	artifact, _, snapshot := bundleSnapshotArtifactFixture(t)
	defer snapshot.Close()
	first := bytes.Index(artifact, snapshotArtifactRelationMagic[:])
	if first < 0 {
		t.Fatal("first relation certificate missing")
	}
	prefixEnd := first + snapshotArtifactRelationBytes
	_, cursor, err := ContinueSnapshotArtifact(
		bytes.NewReader(artifact[:prefixEnd]), nil, SnapshotArtifactCallbacks{},
	)
	if !errors.Is(err, ErrSnapshotArtifact) || cursor == nil || cursor.Offset() != uint64(prefixEnd) {
		t.Fatalf("prefix offset=%d err=%v", cursor.Offset(), err)
	}
	rawBefore, err := AppendSnapshotArtifactCursor(nil, cursor)
	if err != nil {
		t.Fatal(err)
	}
	callbackErr := errors.New("stop before cursor persistence")
	_, next, err := ContinueSnapshotArtifact(
		bytes.NewReader(artifact[prefixEnd:]), cursor, SnapshotArtifactCallbacks{
			Rows: func(checkpoint SnapshotArtifactCheckpoint, _ SnapshotArtifactRows) error {
				if checkpoint.Relation == 2 {
					return callbackErr
				}
				return nil
			},
		},
	)
	if !errors.Is(err, callbackErr) || next == nil || next.Offset() != cursor.Offset() {
		t.Fatalf("failed callback next=%d prior=%d err=%v", next.Offset(), cursor.Offset(), err)
	}
	rawAfter, err := AppendSnapshotArtifactCursor(nil, cursor)
	if err != nil || !bytes.Equal(rawAfter, rawBefore) {
		t.Fatalf("prior cursor mutated after callback failure: equal=%v err=%v",
			bytes.Equal(rawAfter, rawBefore), err)
	}
}

func BenchmarkWriteBundleSnapshotArtifact(b *testing.B) {
	fixture := newRelationBundleFixture(b, true)
	snapshot, err := fixture.machine.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snapshot.Close()
	workspace := make([]byte, MaxSnapshotArtifactChunkBytes)
	options := SnapshotArtifactOptions{
		TargetChunkBytes: MinSnapshotArtifactChunkBytes,
		PayloadBuffer:    workspace,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := WriteSnapshotArtifact(io.Discard, snapshot, options); err != nil {
			b.Fatal(err)
		}
	}
}

func TestWriteBundleSnapshotArtifactCallerWorkspaceBoundsAllocations(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	snapshot, err := fixture.machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	workspace := make([]byte, MaxSnapshotArtifactChunkBytes)
	options := SnapshotArtifactOptions{
		TargetChunkBytes: MinSnapshotArtifactChunkBytes,
		PayloadBuffer:    workspace,
	}
	allocs := testing.AllocsPerRun(20, func() {
		if _, err := WriteSnapshotArtifact(io.Discard, snapshot, options); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 60 {
		t.Fatalf("bundle artifact allocations = %.0f, want <= 60", allocs)
	}
}

func TestBundleSnapshotArtifactMaximumRelationHeaderAndCursorAreBounded(t *testing.T) {
	fixture := newRelationBundleFixture(t, false)
	cut, err := fixture.machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	state := cut.State()
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	stateEnvelope, err := AppendState(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	relations := make([]SnapshotArtifactRelation, replication.MaxRelationsPerBundle)
	for i := range relations {
		relations[i] = SnapshotArtifactRelation{
			Relation: replication.RelationID(i + 1), Kind: RelationJSON,
			Collection: []byte(fmt.Sprintf("relation-%02d", i+1)),
		}
	}
	header, digest, err := makeSnapshotArtifactHeaderForRelations(
		stateEnvelope, string(relations[0].Collection), MinSnapshotArtifactChunkBytes,
		fixture.machine.manifestDigest, relations, true,
	)
	if err != nil || len(header) > maxSnapshotArtifactHeaderBytes {
		t.Fatalf("header bytes=%d err=%v", len(header), err)
	}
	manifest, expectedState, encodedBytes, err := readSnapshotArtifactHeader(bytes.NewReader(header))
	if err != nil || len(manifest.Relations) != replication.MaxRelationsPerBundle ||
		manifest.HeaderDigest != digest {
		t.Fatalf("manifest relations=%d digest=%x err=%v",
			len(manifest.Relations), manifest.HeaderDigest, err)
	}
	cursor := &SnapshotArtifactCursor{
		manifest: manifest, expectedStateDocument: expectedState,
		encodedBytes: encodedBytes, previousDigest: digest,
		currentCollection: SnapshotArtifactSystem, nextRelation: 1,
		captureImageDigest: snapshotArtifactEmptyCaptureImageDigest(),
	}
	raw, err := AppendSnapshotArtifactCursor(nil, cursor)
	if err != nil || len(raw) > maxSnapshotArtifactCursorBytes {
		t.Fatalf("cursor bytes=%d err=%v", len(raw), err)
	}
	opened, err := OpenSnapshotArtifactCursor(raw)
	if err != nil || len(opened.PrefixManifest().Relations) != replication.MaxRelationsPerBundle {
		t.Fatalf("opened max cursor relations=%d err=%v",
			len(opened.PrefixManifest().Relations), err)
	}
}
