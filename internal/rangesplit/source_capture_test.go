package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestSourceCaptureAtomicallyFollowsApplyAndRecovers(t *testing.T) {
	partitioner, err := NewPartitioner(
		testSplitPlan(t, "node-b"), "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newSourceCaptureFixture(t, partitioner)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	fixture.clientEpoch = fixture.openSession(t, 2, []byte("tenant"), sourceCaptureID(20))
	capture, err := NewSourceCapture(partitioner, "split-capture", fixture.capture)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.machine.BeginTransitionCapture(capture); err != nil {
		t.Fatal(err)
	}

	cut, err := fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	options := ChildArtifactOptions{TargetChunkBytes: MinChildArtifactChunkBytes}
	options.Writers[1] = &artifact
	options.PayloadBuffers[1] = make([]byte, 0, MaxChildArtifactChunkBytes)
	var artifactWorkspace ChildArtifactWorkspace
	set, err := partitioner.WriteChildArtifacts(cut, options, &artifactWorkspace)
	closeErr := cut.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("artifact=%v close=%v", err, closeErr)
	}
	cursor, err := partitioner.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}

	left := documentForChild(t, partitioner, 0)
	right := documentForChild(t, partitioner, 1)
	left, err = vibejson.AppendCanonicalize(nil, left)
	if err != nil {
		t.Fatal(err)
	}
	right, err = vibejson.AppendCanonicalize(nil, right)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(
		sourceCaptureMeta(3), fixture.command(2, replication.Mutation{
			Kind: replication.MutationPut, Key: []byte("row"), Value: left,
		}),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(
		sourceCaptureMeta(4), fixture.command(3, replication.Mutation{
			Kind: replication.MutationPut, Key: []byte("row"), Value: right,
		}),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(sourceCaptureMeta(5), nil); err != nil {
		t.Fatal(err)
	}
	if capture.Head() != 5 || fixture.capture.Len() != 4 {
		t.Fatalf("head=%d rows=%d", capture.Head(), fixture.capture.Len())
	}

	var readWorkspace SourceCaptureWorkspace
	entry, ok, err := capture.NextTailEntry(cursor, &readWorkspace)
	if err != nil || !ok || len(entry.Transitions) != 1 ||
		entry.Transitions[0].BeforeWitness.Present ||
		!bytes.Equal(entry.Transitions[0].After, left) {
		t.Fatalf("insert entry=%+v ok=%v err=%v", entry, ok, err)
	}
	cursor = translateCapturedEntry(t, partitioner, cursor, entry)
	entry, ok, err = capture.NextTailEntry(cursor, &readWorkspace)
	if err != nil || !ok || len(entry.Transitions) != 1 ||
		!exactBeforeWitness(partitioner, entry.Transitions[0].BeforeWitness, left) ||
		!bytes.Equal(entry.Transitions[0].After, right) {
		t.Fatalf("move entry=%+v ok=%v err=%v", entry, ok, err)
	}
	cursor = translateCapturedEntry(t, partitioner, cursor, entry)
	entry, ok, err = capture.NextTailEntry(cursor, &readWorkspace)
	if err != nil || !ok || len(entry.Transitions) != 0 {
		t.Fatalf("empty entry=%+v ok=%v err=%v", entry, ok, err)
	}
	cursor = translateCapturedEntry(t, partitioner, cursor, entry)
	if _, ok, err := capture.NextTailEntry(cursor, &readWorkspace); err != nil || ok {
		t.Fatalf("head read ok=%v err=%v", ok, err)
	}

	recovered, err := NewSourceCapture(partitioner, "split-capture", fixture.capture)
	if err != nil {
		t.Fatal(err)
	}
	reopenOptions := fixture.options
	reopenOptions.TransitionCapture = recovered
	reopened, err := replicatedstate.Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		replicatedstate.UserCollection{Name: "docs", Target: fixture.user},
		fixture.log, reopenOptions,
	)
	if err != nil || recovered.Head() != 5 {
		t.Fatalf("reopen=%v head=%d", err, recovered.Head())
	}
	if _, err := reopened.ApplyNormal(
		sourceCaptureMeta(6), fixture.command(4, replication.Mutation{
			Kind: replication.MutationDelete, Key: []byte("row"),
		}),
	); err != nil {
		t.Fatal(err)
	}
	entry, ok, err = recovered.NextTailEntry(cursor, &readWorkspace)
	if err != nil || !ok || len(entry.Transitions) != 1 ||
		!exactBeforeWitness(partitioner, entry.Transitions[0].BeforeWitness, right) ||
		entry.Transitions[0].After != nil {
		t.Fatalf("delete entry=%+v ok=%v err=%v", entry, ok, err)
	}
	cursor = translateCapturedEntry(t, partitioner, cursor, entry)
	if _, err := reopened.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 7, Term: 2, Type: pb.EntryConfChange,
	}, &pb.ConfState{Voters: []uint64{1, 2}}); err != nil {
		t.Fatal(err)
	}
	entry, ok, err = recovered.NextTailEntry(cursor, &readWorkspace)
	if err != nil || !ok || len(entry.Transitions) != 0 {
		t.Fatalf("configuration entry=%+v ok=%v err=%v", entry, ok, err)
	}
	cursor = translateCapturedEntry(t, partitioner, cursor, entry)
	ownership, err := replicatedstate.AppendOwnershipTransition(nil, replicatedstate.OwnershipTransition{
		From: fixture.binding, ExpectedReplicaSetVersion: 7,
		SourceMember: 1, TargetMember: 2,
		ToOwnershipEpoch:  fixture.binding.OwnershipEpoch + 1,
		ToRoutingVersion:  fixture.binding.RoutingVersion + 1,
		ToRouteGeneration: fixture.binding.RouteGeneration + 1,
		ToOwnedRange:      partitioner.children[partitioner.retained].Range,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.ApplyNormal(sourceCaptureMeta(8), ownership); err != nil {
		t.Fatal(err)
	}
	entry, ok, err = recovered.NextTailEntry(cursor, &readWorkspace)
	if err != nil || !ok || len(entry.Transitions) != 0 || !tailEntrySeals(entry) {
		t.Fatalf("seal entry=%+v ok=%v err=%v", entry, ok, err)
	}
	cursor = translateCapturedEntry(t, partitioner, cursor, entry)
	if !cursor.Sealed() || recovered.Head() != 8 {
		t.Fatalf("sealed cursor=%+v head=%d", cursor, recovered.Head())
	}
}

func TestSourceCaptureRecoveryRejectsRecordCorruption(t *testing.T) {
	partitioner, err := NewPartitioner(
		testSplitPlan(t, "node-b"), "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newSourceCaptureFixture(t, partitioner)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	fixture.clientEpoch = fixture.openSession(t, 2, []byte("tenant"), sourceCaptureID(20))
	capture, _ := NewSourceCapture(partitioner, "split-capture", fixture.capture)
	if err := fixture.machine.BeginTransitionCapture(capture); err != nil {
		t.Fatal(err)
	}
	document := documentForChild(t, partitioner, 0)
	document, err = vibejson.AppendCanonicalize(nil, document)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(
		sourceCaptureMeta(3), fixture.command(2, replication.Mutation{
			Kind: replication.MutationPut, Key: []byte("row"), Value: document,
		}),
	); err != nil {
		t.Fatal(err)
	}
	var key [8]byte
	key[7] = 3
	raw, found, err := fixture.capture.AppendRaw(nil, key[:])
	if err != nil || !found {
		t.Fatal(err)
	}
	// Corrupt the embedded semantic digest while preserving the binary framing.
	raw[216] ^= 1
	if err := fixture.capture.Update(func(batch *durable.WriteBatch) error {
		return batch.Put(key[:], raw)
	}); err != nil {
		t.Fatal(err)
	}
	recovered, _ := NewSourceCapture(partitioner, "split-capture", fixture.capture)
	options := fixture.options
	options.TransitionCapture = recovered
	if _, err := replicatedstate.Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		replicatedstate.UserCollection{Name: "docs", Target: fixture.user},
		fixture.log, options,
	); !errors.Is(err, ErrSourceCapture) && !errors.Is(err, replicatedstate.ErrTransitionCapture) {
		t.Fatalf("corrupt recovery error=%v", err)
	}
}

func TestSourceCaptureBinaryFormatBoundsAliasesAndAllocations(t *testing.T) {
	capture, _, _, document := newSourceCaptureEncodedEntry(t)
	var key [8]byte
	key[7] = 3
	raw, found, err := capture.target.Collection.AppendRaw(nil, key[:])
	if err != nil || !found {
		t.Fatalf("read found=%v err=%v", found, err)
	}

	logicalBytes := len("row") + len(document)
	wantPhysical := sourceCaptureEntryFixedBytes + sourceCaptureTransitionHeaderBytes +
		logicalBytes
	bound, err := capture.MaxEncodedBytes(replicatedstate.TransitionCaptureBounds{
		Transitions: 1, KeyBytes: uint64(len("row")), AfterBytes: uint64(len(document)),
	})
	if err != nil || bound != wantPhysical || len(raw) != wantPhysical {
		t.Fatalf("bound=%d physical=%d want=%d err=%v", bound, len(raw), wantPhysical, err)
	}
	if _, err := capture.MaxEncodedBytes(replicatedstate.TransitionCaptureBounds{
		Transitions: replicatedstate.MaxDistinctMutations + 1,
	}); !errors.Is(err, ErrSourceCapture) {
		t.Fatalf("excess transitions error=%v", err)
	}
	if _, err := capture.MaxEncodedBytes(replicatedstate.TransitionCaptureBounds{
		KeyBytes: math.MaxUint64,
	}); !errors.Is(err, ErrSourceCapture) {
		t.Fatalf("overflow error=%v", err)
	}

	var workspace SourceCaptureWorkspace
	record, err := capture.decodeEntry(raw, &workspace)
	if err != nil || len(record.Transitions) != 1 {
		t.Fatalf("decode transitions=%d err=%v", len(record.Transitions), err)
	}
	transition := record.Transitions[0]
	if cap(record.Transitions) != len(record.Transitions) ||
		cap(transition.Key) != len(transition.Key) ||
		cap(transition.After) != len(transition.After) || transition.Before != nil {
		t.Fatalf("unclamped decode transitions=%d/%d key=%d/%d after=%d/%d before=%v",
			len(record.Transitions), cap(record.Transitions), len(transition.Key), cap(transition.Key),
			len(transition.After), cap(transition.After), transition.Before)
	}
	keyOffset := sourceCaptureEntryFixedBytes + sourceCaptureTransitionHeaderBytes
	if &transition.Key[0] != &raw[keyOffset] ||
		&transition.After[0] != &raw[keyOffset+len(transition.Key)] {
		t.Fatal("decoded transition does not borrow the input envelope")
	}
	next := raw[keyOffset+len(transition.Key)]
	grown := append(transition.Key, 0xff)
	if len(grown) != len(transition.Key)+1 || raw[keyOffset+len(transition.Key)] != next {
		t.Fatal("appending a cap-clamped key changed adjacent envelope bytes")
	}
	raw[keyOffset] ^= 0x20
	if transition.Key[0] != raw[keyOffset] {
		t.Fatal("borrowed key did not follow input lifetime")
	}
	raw[keyOffset] ^= 0x20

	if _, err := capture.decodeEntry(raw, &workspace); err != nil {
		t.Fatal(err)
	}
	var allocationErr error
	allocations := testing.AllocsPerRun(1000, func() {
		_, allocationErr = capture.decodeEntry(raw, &workspace)
	})
	if allocationErr != nil || allocations != 0 {
		t.Fatalf("warmed decode allocations=%f err=%v", allocations, allocationErr)
	}
	t.Logf("source capture bytes: logical=%d physical=%d ratio=%.2f",
		logicalBytes, len(raw), float64(len(raw))/float64(logicalBytes))
}

func TestMaximumSourceCaptureRecordBytesExactAndBounded(t *testing.T) {
	got, err := MaximumSourceCaptureRecordBytes(3, 11, 101, 211)
	want := sourceCaptureEntryFixedBytes +
		3*sourceCaptureTransitionHeaderBytes + 211
	if err != nil || got != want {
		t.Fatalf("maximum record bytes = %d,%v; want %d", got, err, want)
	}
	for _, limits := range [][4]int{
		{}, {0, 1, 1, 1}, {1, 0, 1, 1}, {1, 1, 0, 1}, {1, 1, 1, 0},
		{math.MaxInt, 2, 1, 1}, {math.MaxInt, 1, 2, 1},
		{1, 1, 1, math.MaxInt},
	} {
		if _, err := MaximumSourceCaptureRecordBytes(
			limits[0], limits[1], limits[2], limits[3],
		); !errors.Is(err, ErrSourceCapture) {
			t.Fatalf("limits %v error = %v, want ErrSourceCapture", limits, err)
		}
	}
	capped, err := MaximumSourceCaptureRecordBytes(3, 11, 101, replication.MaxCommandBytes+1)
	if want := sourceCaptureEntryFixedBytes + 3*sourceCaptureTransitionHeaderBytes +
		3*(11+101); err != nil || capped != want {
		t.Fatalf("command-capped maximum = %d,%v; want %d", capped, err, want)
	}
	commandCapped, err := MaximumSourceCaptureRecordBytes(
		replicatedstate.MaxDistinctMutations, replication.MaxMutationKeyBytes,
		replication.MaxMutationValueBytes, replication.MaxCommandBytes+1,
	)
	if want := sourceCaptureEntryFixedBytes +
		int(replicatedstate.MaxDistinctMutations)*sourceCaptureTransitionHeaderBytes +
		replication.MaxCommandBytes; err != nil || commandCapped != want {
		t.Fatalf("command ceiling = %d,%v; want %d", commandCapped, err, want)
	}
	for _, bounds := range []replicatedstate.TransitionCaptureBounds{
		{Transitions: 3, KeyBytes: 211},
		{Transitions: 3, KeyBytes: 11, AfterBytes: 200},
		{Transitions: 3, BeforeBytes: 3 * 101, AfterBytes: 211},
	} {
		exact, exactErr := (&SourceCapture{}).MaxEncodedBytes(bounds)
		if exactErr != nil || exact > got {
			t.Fatalf("actual bound %+v = %d,%v exceeds provision %d", bounds, exact, exactErr, got)
		}
	}
}

func TestMaximumSourceCaptureRecordBytesMatchesArtifactHostileBound(t *testing.T) {
	got, err := MaximumSourceCaptureRecordBytes(
		replicatedstate.MaxDistinctMutations,
		replication.MaxMutationKeyBytes,
		replication.MaxMutationValueBytes,
		replication.MaxCommandBytes,
	)
	if err != nil || got != replicatedstate.MaxTransitionCaptureRecordBytes {
		t.Fatalf("hostile capture bound = %d, %v; want %d", got, err,
			replicatedstate.MaxTransitionCaptureRecordBytes)
	}
}

func TestSourceCaptureBinaryPhysicalBytesExcludeBeforePayload(t *testing.T) {
	for _, documentBytes := range []int{256, 1 << 10, 8 << 10} {
		t.Run(fmt.Sprintf("documents_%d", documentBytes), func(t *testing.T) {
			partitioner, err := NewPartitioner(
				testSplitPlan(t, "node-b"), "docs", []string{"/tenant", "/sequence"},
				distribution.DefaultVirtualBucketBits,
			)
			if err != nil {
				t.Fatal(err)
			}
			fixture := newSourceCaptureFixture(t, partitioner)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			fixture.clientEpoch = fixture.openSession(
				t, 2, []byte("tenant"), sourceCaptureID(20),
			)
			capture, err := NewSourceCapture(partitioner, "split-capture", fixture.capture)
			if err != nil {
				t.Fatal(err)
			}
			if err := fixture.machine.BeginTransitionCapture(capture); err != nil {
				t.Fatal(err)
			}
			before := sizedSourceCaptureDocument(documentBytes, 'a')
			after := sizedSourceCaptureDocument(128, 'b')
			before, err = vibejson.AppendCanonicalize(nil, before)
			if err != nil {
				t.Fatal(err)
			}
			after, err = vibejson.AppendCanonicalize(nil, after)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.machine.ApplyNormal(
				sourceCaptureMeta(3), fixture.command(2, replication.Mutation{
					Kind: replication.MutationPut, Key: []byte("row"), Value: before,
				}),
			); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.machine.ApplyNormal(
				sourceCaptureMeta(4), fixture.command(3, replication.Mutation{
					Kind: replication.MutationPut, Key: []byte("row"), Value: after,
				}),
			); err != nil {
				t.Fatal(err)
			}
			var key [8]byte
			key[7] = 4
			raw, found, err := fixture.capture.AppendRaw(nil, key[:])
			if err != nil || !found {
				t.Fatalf("read found=%v err=%v", found, err)
			}
			logical := len("row") + len(after)
			wantPhysical := logical + sourceCaptureEntryFixedBytes +
				sourceCaptureTransitionHeaderBytes
			bound, err := capture.MaxEncodedBytes(replicatedstate.TransitionCaptureBounds{
				Transitions: 1,
				KeyBytes:    uint64(len("row")),
				BeforeBytes: uint64(len(before)),
				AfterBytes:  uint64(len(after)),
			})
			if err != nil || bound != wantPhysical || len(raw) != wantPhysical {
				t.Fatalf("logical=%d physical=%d bound=%d want=%d err=%v",
					logical, len(raw), bound, wantPhysical, err)
			}
			record, err := capture.decodeEntry(raw, &SourceCaptureWorkspace{})
			if err != nil || len(record.Transitions) != 1 ||
				!exactBeforeWitness(partitioner, record.Transitions[0].BeforeWitness, before) ||
				!bytes.Equal(record.Transitions[0].After, after) {
				t.Fatalf("round trip transitions=%d witness=%+v before=%d after=%d err=%v",
					len(record.Transitions), record.Transitions[0].BeforeWitness,
					len(before), len(after), err)
			}
			t.Logf("source capture bytes: before=%d after=%d logical=%d physical=%d ratio=%.3f",
				len(before), len(after), logical, len(raw), float64(len(raw))/float64(logical))
		})
	}
}

func sizedSourceCaptureDocument(size int, fill byte) []byte {
	prefix := []byte(`{"tenant":"acme","sequence":8,"payload":"`)
	suffix := []byte(`"}`)
	document := make([]byte, 0, size)
	document = append(document, prefix...)
	document = append(document, bytes.Repeat([]byte{fill}, size-len(prefix)-len(suffix))...)
	document = append(document, suffix...)
	return document
}

func TestSourceCaptureBinaryMultipleTransitionsAndWorkspaceRelease(t *testing.T) {
	capture, _, readWorkspace, document := newSourceCaptureEncodedEntry(t, "a", "b")
	raw := bytes.Clone(readWorkspace.raw)
	var workspace SourceCaptureWorkspace
	record, err := capture.decodeEntry(raw, &workspace)
	if err != nil || len(record.Transitions) != 2 ||
		!bytes.Equal(record.Transitions[0].Key, []byte("a")) ||
		!bytes.Equal(record.Transitions[1].Key, []byte("b")) {
		t.Fatalf("multi-transition decode=%+v err=%v", record.Transitions, err)
	}
	wantBytes := sourceCaptureEntryFixedBytes + 2*sourceCaptureTransitionHeaderBytes +
		len("a") + len("b") + 2*len(document)
	if len(raw) != wantBytes {
		t.Fatalf("multi-transition bytes=%d want=%d", len(raw), wantBytes)
	}
	for index, transition := range capture.encode.transitions[:cap(capture.encode.transitions)] {
		if transition.Key != nil || transition.Before != nil ||
			transition.BeforeWitness != (TailBeforeWitness{}) || transition.After != nil {
			t.Fatalf("encoder retained transition %d: %+v", index, transition)
		}
	}

	cursor := sourceCaptureEntryFixedBytes
	frameEnds := make([]int, 0, len(record.Transitions))
	keyOffsets := make([]int, 0, len(record.Transitions))
	for range record.Transitions {
		header := raw[cursor : cursor+sourceCaptureTransitionHeaderBytes]
		keyBytes := int(binary.LittleEndian.Uint32(header[4:8]))
		beforeBytes := int(binary.LittleEndian.Uint32(header[8:12]))
		afterBytes := int(binary.LittleEndian.Uint32(header[12:16]))
		cursor += sourceCaptureTransitionHeaderBytes
		keyOffsets = append(keyOffsets, cursor)
		_ = beforeBytes
		cursor += keyBytes + afterBytes
		frameEnds = append(frameEnds, cursor)
	}

	single := bytes.Clone(raw[:frameEnds[0]])
	binary.LittleEndian.PutUint32(single[12:16], uint32(len(single)))
	binary.LittleEndian.PutUint32(single[16:20], 1)
	singleRecord := record
	singleRecord.Transitions = record.Transitions[:1]
	var hashWorkspace SourceCaptureWorkspace
	digest := capture.hashEntry(&singleRecord, &hashWorkspace)
	copy(single[216:248], digest[:])
	singleDecoded, err := capture.decodeEntry(single, &workspace)
	if err != nil || len(singleDecoded.Transitions) != 1 || cap(singleDecoded.Transitions) != 1 {
		t.Fatalf("single decode transitions=%d/%d err=%v",
			len(singleDecoded.Transitions), cap(singleDecoded.Transitions), err)
	}
	for index, transition := range workspace.transitions[:cap(workspace.transitions)] {
		if index == 0 {
			continue
		}
		if transition.Key != nil || transition.Before != nil ||
			transition.BeforeWitness != (TailBeforeWitness{}) || transition.After != nil {
			t.Fatalf("decoder retained stale transition %d: %+v", index, transition)
		}
	}

	outOfOrder := bytes.Clone(raw)
	outOfOrder[keyOffsets[1]] = outOfOrder[keyOffsets[0]]
	badRecord := record
	badRecord.Transitions = append([]TailTransition(nil), record.Transitions...)
	badRecord.Transitions[1].Key = outOfOrder[keyOffsets[1] : keyOffsets[1]+1]
	digest = capture.hashEntry(&badRecord, &hashWorkspace)
	copy(outOfOrder[216:248], digest[:])
	if _, err := capture.decodeEntry(outOfOrder, &SourceCaptureWorkspace{}); !errors.Is(err, ErrSourceCapture) {
		t.Fatalf("out-of-order transitions with valid digest error=%v", err)
	}
}

func TestSourceCaptureLateWitnessFailureReleasesBorrowedMutations(t *testing.T) {
	partitioner, err := NewPartitioner(
		testSplitPlan(t, "node-b"), "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newSourceCaptureFixture(t, partitioner)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	fixture.clientEpoch = fixture.openSession(t, 2, []byte("tenant"), sourceCaptureID(20))
	valid := documentForChild(t, partitioner, 0)
	invalid, canonicalErr := vibejson.AppendCanonicalize(
		nil, []byte(`{"payload":"valid-json-without-placement-columns"}`),
	)
	if canonicalErr != nil {
		t.Fatal(canonicalErr)
	}
	if _, err := fixture.machine.ApplyNormal(
		sourceCaptureMeta(3), fixture.command(2,
			replication.Mutation{Kind: replication.MutationPut, Key: []byte("a"), Value: valid},
			replication.Mutation{Kind: replication.MutationPut, Key: []byte("b"), Value: invalid},
		),
	); err != nil {
		t.Fatal(err)
	}
	after := documentForChild(t, partitioner, 1)
	capture, err := NewSourceCapture(partitioner, "split-capture", fixture.capture)
	if err != nil || fixture.machine.BeginTransitionCapture(capture) != nil {
		t.Fatal(err)
	}
	_, err = fixture.machine.ApplyNormal(
		sourceCaptureMeta(4), fixture.command(3,
			replication.Mutation{Kind: replication.MutationPut, Key: []byte("a"), Value: after},
			replication.Mutation{Kind: replication.MutationPut, Key: []byte("b"), Value: valid},
		),
	)
	if !errors.Is(err, ErrSourceCapture) && !errors.Is(err, replicatedstate.ErrTransitionCapture) {
		t.Fatalf("late placement error=%v", err)
	}
	if len(capture.encode.transitions) != 0 || capture.encode.record.Transitions != nil {
		t.Fatalf("retained active transitions after error: %+v", capture.encode.transitions)
	}
	for index, transition := range capture.encode.transitions[:cap(capture.encode.transitions)] {
		if transition.Key != nil || transition.Before != nil || transition.After != nil ||
			transition.BeforeWitness != (TailBeforeWitness{}) {
			t.Fatalf("retained borrowed transition %d after error: %+v", index, transition)
		}
	}
}

func TestSourceCaptureBinaryRejectsTruncationAndNoncanonicalFrames(t *testing.T) {
	capture, _, _, _ := newSourceCaptureEncodedEntry(t)
	var entryKey [8]byte
	entryKey[7] = 3
	entry, found, err := capture.target.Collection.AppendRaw(nil, entryKey[:])
	if err != nil || !found {
		t.Fatalf("entry read found=%v err=%v", found, err)
	}
	var headerKey [8]byte
	header, found, err := capture.target.Collection.AppendRaw(nil, headerKey[:])
	if err != nil || !found {
		t.Fatalf("header read found=%v err=%v", found, err)
	}
	for end := 0; end < len(entry); end++ {
		if _, err := capture.decodeEntry(entry[:end], &SourceCaptureWorkspace{}); !errors.Is(err, ErrSourceCapture) {
			t.Fatalf("entry prefix %d/%d error=%v", end, len(entry), err)
		}
		if end >= sourceCaptureEntryFixedBytes {
			candidate := bytes.Clone(entry[:end])
			binary.LittleEndian.PutUint32(candidate[12:16], uint32(len(candidate)))
			if _, err := capture.decodeEntry(candidate, &SourceCaptureWorkspace{}); !errors.Is(err, ErrSourceCapture) {
				t.Fatalf("reframed entry prefix %d/%d error=%v", end, len(entry), err)
			}
		}
	}
	for end := 0; end < len(header); end++ {
		if _, _, err := capture.decodeHeader(header[:end], &SourceCaptureWorkspace{}); !errors.Is(err, ErrSourceCapture) {
			t.Fatalf("header prefix %d/%d error=%v", end, len(header), err)
		}
		if end >= sourceCaptureHeaderFixedBytes {
			candidate := bytes.Clone(header[:end])
			binary.LittleEndian.PutUint32(candidate[12:16], uint32(len(candidate)))
			binary.LittleEndian.PutUint32(candidate[16:20], uint32(len(candidate)-sourceCaptureHeaderFixedBytes))
			if _, _, err := capture.decodeHeader(candidate, &SourceCaptureWorkspace{}); !errors.Is(err, ErrSourceCapture) {
				t.Fatalf("reframed header prefix %d/%d error=%v", end, len(header), err)
			}
		}
	}

	entryCases := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"magic", func(raw []byte) []byte { raw[0] ^= 1; return raw }},
		{"format", func(raw []byte) []byte { binary.LittleEndian.PutUint16(raw[8:10], 1); return raw }},
		{"kind", func(raw []byte) []byte { raw[10] = sourceCaptureHeaderKind; return raw }},
		{"envelope_reserved", func(raw []byte) []byte { raw[11] = 1; return raw }},
		{"total", func(raw []byte) []byte { binary.LittleEndian.PutUint32(raw[12:16], uint32(len(raw)-1)); return raw }},
		{"entry_reserved", func(raw []byte) []byte { raw[20] = 1; return raw }},
		{"count", func(raw []byte) []byte {
			binary.LittleEndian.PutUint32(raw[16:20], replicatedstate.MaxDistinctMutations+1)
			return raw
		}},
		{"digest", func(raw []byte) []byte { raw[216] ^= 1; return raw }},
		{"presence_zero", func(raw []byte) []byte { raw[sourceCaptureEntryFixedBytes] = 0; return raw }},
		{"presence_unknown", func(raw []byte) []byte { raw[sourceCaptureEntryFixedBytes] |= 4; return raw }},
		{"transition_reserved", func(raw []byte) []byte { raw[sourceCaptureEntryFixedBytes+1] = 1; return raw }},
		{"key_absent", func(raw []byte) []byte {
			binary.LittleEndian.PutUint32(raw[sourceCaptureEntryFixedBytes+4:], 0)
			return raw
		}},
		{"absent_before_has_bytes", func(raw []byte) []byte {
			binary.LittleEndian.PutUint32(raw[sourceCaptureEntryFixedBytes+8:], 1)
			return raw
		}},
		{"present_after_has_no_bytes", func(raw []byte) []byte {
			binary.LittleEndian.PutUint32(raw[sourceCaptureEntryFixedBytes+12:], 0)
			return raw
		}},
		{"trailing", func(raw []byte) []byte {
			raw = append(raw, 0)
			binary.LittleEndian.PutUint32(raw[12:16], uint32(len(raw)))
			return raw
		}},
	}
	for _, test := range entryCases {
		t.Run("entry_"+test.name, func(t *testing.T) {
			candidate := test.mutate(bytes.Clone(entry))
			if _, err := capture.decodeEntry(candidate, &SourceCaptureWorkspace{}); !errors.Is(err, ErrSourceCapture) {
				t.Fatalf("error=%v", err)
			}
		})
	}
	headerCases := []struct {
		name   string
		mutate func([]byte)
	}{
		{"format", func(raw []byte) { binary.LittleEndian.PutUint16(raw[8:10], 1) }},
		{"reserved", func(raw []byte) { raw[20] = 1 }},
		{"collection_length", func(raw []byte) { binary.LittleEndian.PutUint32(raw[16:20], 0) }},
		{"digest", func(raw []byte) { raw[184] ^= 1 }},
	}
	for _, test := range headerCases {
		t.Run("header_"+test.name, func(t *testing.T) {
			candidate := bytes.Clone(header)
			test.mutate(candidate)
			if _, _, err := capture.decodeHeader(candidate, &SourceCaptureWorkspace{}); !errors.Is(err, ErrSourceCapture) {
				t.Fatalf("error=%v", err)
			}
		})
	}
	var decodeWorkspace SourceCaptureWorkspace
	record, err := capture.decodeEntry(entry, &decodeWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	assertWitnessRejected := func(
		name string,
		decoder *SourceCapture,
		witness TailBeforeWitness,
	) {
		t.Run(name, func(t *testing.T) {
			candidate := bytes.Clone(entry)
			header := candidate[sourceCaptureEntryFixedBytes : sourceCaptureEntryFixedBytes+sourceCaptureTransitionHeaderBytes]
			if witness.Present {
				header[0] |= sourceCaptureBeforePresent
			} else {
				header[0] &^= sourceCaptureBeforePresent
			}
			binary.LittleEndian.PutUint32(header[8:12], witness.DocumentBytes)
			copy(header[16:24], witness.Point[:])
			copy(header[24:56], witness.Digest[:])
			candidateRecord := record
			candidateRecord.Transitions = append([]TailTransition(nil), record.Transitions...)
			candidateRecord.Transitions[0].BeforeWitness = witness
			var hashWorkspace SourceCaptureWorkspace
			digest := decoder.hashEntry(&candidateRecord, &hashWorkspace)
			copy(candidate[216:248], digest[:])
			if _, err := decoder.decodeEntry(candidate, &SourceCaptureWorkspace{}); !errors.Is(err, ErrSourceCapture) {
				t.Fatalf("validly checksummed malformed witness error=%v", err)
			}
		})
	}
	assertWitnessRejected("absent_nonzero_point", capture, TailBeforeWitness{
		Point: distribution.KeyspacePoint{1},
	})
	assertWitnessRejected("absent_nonzero_digest", capture, TailBeforeWitness{
		Digest: [sha256.Size]byte{1},
	})
	assertWitnessRejected("present_zero_length", capture, TailBeforeWitness{
		Present: true, Point: distribution.KeyspacePoint{1}, Digest: [sha256.Size]byte{1},
	})
	assertWitnessRejected("present_zero_digest", capture, TailBeforeWitness{
		Present: true, Point: distribution.KeyspacePoint{1}, DocumentBytes: 1,
	})
	partialPartitioner, partialErr := NewPartitioner(
		testSplitPlanWithNeighbor(t, "node-c"), "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if partialErr != nil {
		t.Fatal(partialErr)
	}
	partialCapture, partialErr := NewSourceCapture(
		partialPartitioner, "split-capture", capture.target.Collection,
	)
	if partialErr != nil {
		t.Fatal(partialErr)
	}
	assertWitnessRejected("present_outside_source", partialCapture, TailBeforeWitness{
		Present: true, Point: distribution.KeyspacePoint{0x80}, DocumentBytes: 1,
		Digest: [sha256.Size]byte{1},
	})
	invalidDocument := bytes.Clone(entry)
	transitionHeader := invalidDocument[sourceCaptureEntryFixedBytes:]
	keyBytes := int(binary.LittleEndian.Uint32(transitionHeader[4:8]))
	beforeBytes := int(binary.LittleEndian.Uint32(transitionHeader[8:12]))
	afterBytes := int(binary.LittleEndian.Uint32(transitionHeader[12:16]))
	afterStart := sourceCaptureEntryFixedBytes + sourceCaptureTransitionHeaderBytes +
		keyBytes
	_ = beforeBytes
	invalidDocument[afterStart] = 'x'
	badRecord := record
	badRecord.Transitions = []TailTransition{{
		Key:           invalidDocument[sourceCaptureEntryFixedBytes+sourceCaptureTransitionHeaderBytes : sourceCaptureEntryFixedBytes+sourceCaptureTransitionHeaderBytes+keyBytes],
		BeforeWitness: record.Transitions[0].BeforeWitness,
		After:         invalidDocument[afterStart : afterStart+afterBytes],
	}}
	var hashWorkspace SourceCaptureWorkspace
	digest := capture.hashEntry(&badRecord, &hashWorkspace)
	copy(invalidDocument[216:248], digest[:])
	if _, err := capture.decodeEntry(invalidDocument, &SourceCaptureWorkspace{}); !errors.Is(err, ErrSourceCapture) {
		t.Fatalf("invalid embedded document with valid digest error=%v", err)
	}
	if _, err := capture.decodeEntry([]byte(`[2,"json-envelope"]`), &SourceCaptureWorkspace{}); !errors.Is(err, ErrSourceCapture) {
		t.Fatalf("JSON envelope error=%v", err)
	}
}

func TestNewSourceCaptureRejectsNonOpaqueCollection(t *testing.T) {
	partitioner, err := NewPartitioner(
		testSplitPlan(t, "node-b"), "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newSourceCaptureFixture(t, partitioner)
	if _, err := NewSourceCapture(partitioner, "capture", fixture.user.Collection); !errors.Is(err, ErrSourceCapture) {
		t.Fatalf("non-opaque collection error=%v", err)
	}
	if _, err := NewSourceCapture(partitioner, "capture", fixture.capture); err != nil {
		t.Fatalf("opaque collection error=%v", err)
	}
}

func BenchmarkSourceCaptureNextTailEntry(b *testing.B) {
	capture, cursor, workspace, document := newSourceCaptureEncodedEntry(b)
	logicalBytes := len("row") + len(document)
	b.ReportAllocs()
	b.SetBytes(int64(logicalBytes))
	b.ResetTimer()
	for range b.N {
		if _, ok, err := capture.NextTailEntry(cursor, workspace); err != nil || !ok {
			b.Fatalf("read ok=%v err=%v", ok, err)
		}
	}
	reportSourceCaptureBytes(b, workspace, logicalBytes)
}

func BenchmarkSourceCaptureLiveRead(b *testing.B) {
	capture, cursor, workspace, document := newSourceCaptureEncodedEntry(b)
	logicalBytes := len("row") + len(document)
	binary.BigEndian.PutUint64(workspace.key[:], cursor.applied+1)
	b.ReportAllocs()
	b.SetBytes(int64(logicalBytes))
	b.ResetTimer()
	for range b.N {
		raw, found, err := capture.target.Collection.AppendRaw(
			workspace.raw[:0], workspace.key[:],
		)
		if err != nil || !found {
			b.Fatalf("read found=%v err=%v", found, err)
		}
		workspace.raw = raw
	}
	reportSourceCaptureBytes(b, workspace, logicalBytes)
}

func BenchmarkSourceCaptureSnapshotRead(b *testing.B) {
	capture, cursor, workspace, document := newSourceCaptureEncodedEntry(b)
	logicalBytes := len("row") + len(document)
	binary.BigEndian.PutUint64(workspace.key[:], cursor.applied+1)
	var snapshot durable.Snapshot
	if err := capture.target.Collection.SnapshotInto(&snapshot); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := snapshot.Close(); err != nil {
			b.Error(err)
		}
	})
	b.ReportAllocs()
	b.SetBytes(int64(logicalBytes))
	b.ResetTimer()
	for range b.N {
		raw, found, err := snapshot.AppendRaw(workspace.raw[:0], workspace.key[:])
		if err != nil || !found {
			b.Fatalf("read found=%v err=%v", found, err)
		}
		workspace.raw = raw
	}
	reportSourceCaptureBytes(b, workspace, logicalBytes)
}

func BenchmarkSourceCaptureDecodeEntry(b *testing.B) {
	capture, cursor, workspace, document := newSourceCaptureEncodedEntry(b)
	logicalBytes := len("row") + len(document)
	binary.BigEndian.PutUint64(workspace.key[:], cursor.applied+1)
	raw, found, err := capture.target.Collection.AppendRaw(nil, workspace.key[:])
	if err != nil || !found {
		b.Fatalf("read found=%v err=%v", found, err)
	}
	if _, err := capture.decodeEntry(raw, workspace); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(logicalBytes))
	b.ResetTimer()
	for range b.N {
		if _, err := capture.decodeEntry(raw, workspace); err != nil {
			b.Fatal(err)
		}
	}
	reportSourceCaptureBytes(b, workspace, logicalBytes)
}

func BenchmarkSourceCaptureAppendHeader(b *testing.B) {
	capture, _, _, _ := newSourceCaptureEncodedEntry(b)
	state := replicatedstate.State{
		Applied:            capture.current.applied,
		LastTerm:           capture.current.term,
		LastEntryDigest:    capture.current.entryDigest,
		DataChainDigest:    capture.current.dataChainDigest,
		SnapshotBaseDigest: capture.base.BaseDigest,
		Binding: replicatedstate.Binding{
			OwnershipEpoch:  capture.current.ownershipEpoch,
			RoutingVersion:  capture.current.routingVersion,
			RouteGeneration: capture.current.routeGeneration,
		},
	}
	encodedBytes := sourceCaptureHeaderFixedBytes + len(capture.partitioner.collection)
	dst := make([]byte, 0, encodedBytes)
	b.Run("cold_workspace", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(encodedBytes))
		for range b.N {
			var workspace SourceCaptureWorkspace
			encoded, _, err := capture.appendHeader(dst[:0], state, &workspace)
			if err != nil || len(encoded) != encodedBytes {
				b.Fatalf("header bytes=%d err=%v", len(encoded), err)
			}
		}
	})
	b.Run("warm_workspace", func(b *testing.B) {
		var workspace SourceCaptureWorkspace
		if _, _, err := capture.appendHeader(dst[:0], state, &workspace); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.SetBytes(int64(encodedBytes))
		b.ResetTimer()
		for range b.N {
			encoded, _, err := capture.appendHeader(dst[:0], state, &workspace)
			if err != nil || len(encoded) != encodedBytes {
				b.Fatalf("header bytes=%d err=%v", len(encoded), err)
			}
		}
	})
}

func newSourceCaptureEncodedEntry(
	b testing.TB,
	keys ...string,
) (*SourceCapture, TailCursor, *SourceCaptureWorkspace, []byte) {
	b.Helper()
	if len(keys) == 0 {
		keys = []string{"row"}
	}
	partitioner, err := NewPartitioner(
		testSplitPlan(b, "node-b"), "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		b.Fatal(err)
	}
	fixture := newSourceCaptureFixture(b, partitioner)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		b.Fatal(err)
	}
	fixture.clientEpoch = fixture.openSession(b, 2, []byte("tenant"), sourceCaptureID(20))
	capture, err := NewSourceCapture(partitioner, "split-capture", fixture.capture)
	if err != nil {
		b.Fatal(err)
	}
	if err := fixture.machine.BeginTransitionCapture(capture); err != nil {
		b.Fatal(err)
	}
	cut, err := fixture.machine.Snapshot("docs")
	if err != nil {
		b.Fatal(err)
	}
	var artifact bytes.Buffer
	options := ChildArtifactOptions{TargetChunkBytes: MinChildArtifactChunkBytes}
	options.Writers[1] = &artifact
	options.PayloadBuffers[1] = make([]byte, 0, MaxChildArtifactChunkBytes)
	var artifactWorkspace ChildArtifactWorkspace
	set, err := partitioner.WriteChildArtifacts(cut, options, &artifactWorkspace)
	if err != nil || cut.Close() != nil {
		b.Fatal(err)
	}
	cursor, err := partitioner.InitialTailCursor(set)
	if err != nil {
		b.Fatal(err)
	}
	document, err := vibejson.AppendCanonicalize(nil, documentForChild(b, partitioner, 1))
	if err != nil {
		b.Fatal(err)
	}
	mutations := make([]replication.Mutation, len(keys))
	for index, key := range keys {
		mutations[index] = replication.Mutation{
			Kind: replication.MutationPut, Key: []byte(key), Value: document,
		}
	}
	if _, err := fixture.machine.ApplyNormal(
		sourceCaptureMeta(3), fixture.command(2, mutations...),
	); err != nil {
		b.Fatal(err)
	}
	var workspace SourceCaptureWorkspace
	if _, ok, err := capture.NextTailEntry(cursor, &workspace); err != nil || !ok {
		b.Fatalf("warm read ok=%v err=%v", ok, err)
	}
	return capture, cursor, &workspace, document
}

func reportSourceCaptureBytes(b *testing.B, workspace *SourceCaptureWorkspace, logical int) {
	b.Helper()
	b.ReportMetric(float64(logical), "logical-B/op")
	b.ReportMetric(float64(len(workspace.raw)), "physical-B/op")
}

type sourceCaptureFixture struct {
	machine            *replicatedstate.Machine
	binding            replicatedstate.Binding
	bootstrap          *pb.Snapshot
	system             replicatedstate.CollectionTarget
	user               replicatedstate.CollectionTarget
	capture            *durable.Collection
	log                *durable.TxnLog
	options            replicatedstate.Options
	clientEpoch        uint64
	retainedPruneEpoch uint64
}

const sourceCaptureRetryWindow = uint16(8)

type sourceCaptureValidator struct{}

func (sourceCaptureValidator) ValidatePut(_, _ []byte) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}

func (sourceCaptureValidator) ValidateDelete(_, _ []byte, _ bool) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}

func newSourceCaptureFixture(t testing.TB, partitioner *Partitioner) sourceCaptureFixture {
	t.Helper()
	dir := t.TempDir()
	create := func(name string, options durable.Options) *durable.Collection {
		file, err := os.OpenFile(
			filepath.Join(dir, name+".vdb"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
		)
		if err != nil {
			t.Fatal(err)
		}
		collection, err := durable.Create(file, options)
		if err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close(); _ = file.Close() })
		return collection
	}
	systemCollection := create("system", durable.Options{OpaqueValues: true})
	userCollection := create("user", durable.Options{
		MaxDocumentBytes: 16 << 10, MaxBatchDocuments: 4, MaxBatchBytes: 32 << 10,
	})
	captureCollection := create("capture", durable.Options{
		OpaqueValues:     true,
		MaxDocumentBytes: 128 << 10, MaxBatchDocuments: 1, MaxBatchBytes: 256 << 10,
	})
	target := func(collection *durable.Collection) replicatedstate.CollectionTarget {
		return replicatedstate.CollectionTarget{
			Collection:       collection,
			Validation:       replicatedstate.ValidationDeterministicMutation,
			ValidationDigest: sha256.Sum256([]byte("range-split-source-capture-test")),
			Validator:        sourceCaptureValidator{},
			Limits: replicatedstate.CollectionLimits{
				MaxKeyBytes:          collection.MaxKeyBytes(),
				MaxDocumentBytes:     collection.MaxDocumentBytes(),
				MaxDistinctMutations: collection.MaxBatchDocuments(),
				MaxBatchBytes:        collection.MaxBatchBytes(),
			},
		}
	}
	system := target(systemCollection)
	system.Validation = replicatedstate.ValidationOpaqueBinary
	system.ValidationDigest = [32]byte{}
	system.Validator = nil
	user := target(userCollection)
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	binding := replicatedstate.Binding{
		ClusterID: sourceCaptureID(1), ClusterIncarnation: sourceCaptureID(2),
		TopologyRecoveryEpoch: 3,
		Distribution:          string(partitioner.source.Distribution), Shard: string(partitioner.source.Shard),
		AllocationGeneration: uint64(partitioner.source.AllocationGeneration),
		ShardIncarnation:     sourceCaptureID(4), GroupID: sourceCaptureID(5),
		ActivePolicyGeneration: 6, ProtectionEpoch: 7,
		OwnershipEpoch: uint64(partitioner.source.OwnershipEpoch), SchemaGeneration: 8,
		RoutingVersion: uint64(partitioner.source.RoutingVersion), RouteGeneration: 19,
		OwnedRange: partitioner.source.Range,
	}
	index, term := uint64(1), uint64(1)
	bootstrap := &pb.Snapshot{
		Data: []byte("source-capture-bootstrap"),
		Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1}},
		},
	}
	maxDocuments, err := replicatedstate.RequiredBundleTransactionDocuments(
		user.Limits.MaxDistinctMutations,
		sourceCaptureRetryWindow,
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	options := replicatedstate.Options{
		TxnLimits: durable.TxnLimits{
			MaxCollections: 3,
			MaxDocuments:   maxDocuments,
			MaxBytes:       64 << 20,
		},
		MaxSessions: 128,
		RetryWindow: sourceCaptureRetryWindow,
	}
	machine, err := replicatedstate.Open(
		binding, bootstrap, system,
		replicatedstate.UserCollection{Name: "docs", Target: user}, log, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	return sourceCaptureFixture{
		machine: machine, binding: binding, bootstrap: bootstrap,
		system: system, user: user, capture: captureCollection, log: log, options: options,
	}
}

func (f sourceCaptureFixture) command(
	sequence uint64,
	mutations ...replication.Mutation,
) []byte {
	fingerprint := sha256.Sum256([]byte{byte(sequence), 0x73})
	encoded, err := replication.AppendCommand(nil, replication.Command{
		ClusterID: f.binding.ClusterID, ClusterIncarnation: f.binding.ClusterIncarnation,
		TopologyRecoveryEpoch: f.binding.TopologyRecoveryEpoch,
		Distribution:          f.binding.Distribution, Shard: f.binding.Shard,
		AllocationGeneration: f.binding.AllocationGeneration,
		ShardIncarnation:     f.binding.ShardIncarnation, GroupID: f.binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: f.binding.ActivePolicyGeneration,
		ProtectionEpoch: f.binding.ProtectionEpoch, OwnershipEpoch: f.binding.OwnershipEpoch,
		SchemaGeneration: f.binding.SchemaGeneration, RoutingVersion: f.binding.RoutingVersion,
		RouteGeneration: f.binding.RouteGeneration, Tenant: []byte("tenant"),
		ClientID: sourceCaptureID(20), ClientEpoch: f.clientEpoch, ClientSequence: sequence,
		Fingerprint: fingerprint,
		Batches:     []replication.RelationMutationBatch{{Relation: 1, Mutations: mutations}},
	})
	if err != nil {
		panic(err)
	}
	return encoded
}

func (f sourceCaptureFixture) openSession(
	t testing.TB,
	index uint64,
	tenant []byte,
	clientID replication.ID128,
) uint64 {
	t.Helper()
	seed := make([]byte, 0, len("rangesplit/test-session-open/")+len(tenant)+len(clientID))
	seed = append(seed, "rangesplit/test-session-open/"...)
	seed = append(seed, tenant...)
	seed = append(seed, clientID[:]...)
	encoded, err := replication.AppendCommand(nil, replication.Command{
		Kind:                   replication.CommandSessionOpen,
		ClusterID:              f.binding.ClusterID,
		ClusterIncarnation:     f.binding.ClusterIncarnation,
		TopologyRecoveryEpoch:  f.binding.TopologyRecoveryEpoch,
		Distribution:           f.binding.Distribution,
		Shard:                  f.binding.Shard,
		AllocationGeneration:   f.binding.AllocationGeneration,
		ShardIncarnation:       f.binding.ShardIncarnation,
		GroupID:                f.binding.GroupID,
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: f.binding.ActivePolicyGeneration,
		ProtectionEpoch:        f.binding.ProtectionEpoch,
		OwnershipEpoch:         f.binding.OwnershipEpoch,
		SchemaGeneration:       f.binding.SchemaGeneration,
		RoutingVersion:         f.binding.RoutingVersion,
		RouteGeneration:        f.binding.RouteGeneration,
		Tenant:                 tenant,
		ClientID:               clientID,
		ClientSequence:         1,
		NextDeadlineUnixNano:   2_000_000_000_000_000_000,
		Fingerprint:            sha256.Sum256(seed),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.machine.AdmitCommand(encoded); err != nil {
		t.Fatalf("admit session open at %d: %v", index, err)
	}
	publication, err := f.machine.ApplyNormal(sourceCaptureMeta(index), encoded)
	if err != nil || publication.Applied != index {
		t.Fatalf("apply session open at %d = %+v, %v", index, publication, err)
	}
	lookup, err := f.machine.LookupCompletion(encoded)
	if err != nil {
		t.Fatalf("lookup session open at %d: %v", index, err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != replicatedstate.ResultSessionOpened ||
		completion.ClientEpoch != index || completion.ClientSequence != 1 ||
		completion.AppliedSequence != index {
		t.Fatalf("session open completion at %d = %+v, %v", index, completion, err)
	}
	return completion.ClientEpoch
}

func sourceCaptureID(seed byte) replication.ID128 {
	var id replication.ID128
	for index := range id {
		id[index] = seed + byte(index)
	}
	return id
}

func sourceCaptureMeta(index uint64) raftmodel.ApplyMeta {
	return raftmodel.ApplyMeta{Index: index, Term: 2, Type: pb.EntryNormal}
}

func translateCapturedEntry(
	t testing.TB,
	partitioner *Partitioner,
	cursor TailCursor,
	entry TailEntry,
) TailCursor {
	t.Helper()
	sinks := []TailSink{consumeTailBatch, consumeTailBatch}
	var workspace TailWorkspace
	next, _, err := partitioner.TranslateTailEntry(cursor, entry, sinks, &workspace)
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func exactBeforeWitness(
	partitioner *Partitioner,
	witness TailBeforeWitness,
	document []byte,
) bool {
	var workspace distribution.DocumentPointWorkspace
	point, err := partitioner.program.Point(document, &workspace)
	return err == nil && witness.Present && witness.Point == point &&
		witness.DocumentBytes == uint32(len(document)) &&
		witness.Digest == sha256.Sum256(document)
}
