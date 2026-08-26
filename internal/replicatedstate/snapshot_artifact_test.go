package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
)

type snapshotArtifactRow struct {
	collection SnapshotArtifactCollection
	key        []byte
	value      []byte
}

func snapshotArtifactFixture(t testing.TB) (machineFixture, *ReadSnapshot) {
	t.Helper()
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	open := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, open)
	for i := uint64(1); i <= 7; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		value := []byte(fmt.Sprintf(`{"sequence":%d,"payload":"%s"}`, i, bytes.Repeat([]byte{'a' + byte(i)}, 1400)))
		command := testCommand(fixture.binding, i, replication.Mutation{
			Kind: replication.MutationPut, Key: key, Value: value,
		})
		if _, err := fixture.machine.ApplyNormal(normalMeta(i+2), command); err != nil {
			t.Fatalf("ApplyNormal(%d): %v", i+2, err)
		}
	}
	snapshot, err := fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	return fixture, snapshot
}

func writeSnapshotArtifactFixture(t testing.TB, snapshot *ReadSnapshot) ([]byte, SnapshotArtifactManifest) {
	t.Helper()
	var output bytes.Buffer
	manifest, err := WriteSnapshotArtifact(&output, snapshot, SnapshotArtifactOptions{
		TargetChunkBytes: MinSnapshotArtifactChunkBytes,
	})
	if err != nil {
		t.Fatalf("WriteSnapshotArtifact: %v", err)
	}
	return output.Bytes(), manifest
}

func TestSnapshotArtifactDeterministicRoundTripAndCheckpoints(t *testing.T) {
	_, snapshot := snapshotArtifactFixture(t)
	first, written := writeSnapshotArtifactFixture(t, snapshot)
	// The artifact authenticates the apply contract; changing conditional
	// mutations or JSON-relation affected-row semantics changes this vector.
	const golden = "cb3587dce54d821f2350729cda5e9206717cede1e3db2b2ef3be472f4b008664"
	if digest := fmt.Sprintf("%x", sha256.Sum256(first)); digest != golden {
		t.Fatalf("artifact golden digest = %s, want %s", digest, golden)
	}
	second, again := writeSnapshotArtifactFixture(t, snapshot)
	if !bytes.Equal(first, second) {
		t.Fatal("same snapshot and options produced different artifact bytes")
	}
	if written.Digest != again.Digest || written.HeaderDigest != again.HeaderDigest ||
		written.ImageDigest != again.ImageDigest {
		t.Fatal("same artifact produced different manifest digests")
	}
	var buffered bytes.Buffer
	reusable := bytes.Repeat([]byte{0xcc}, MaxSnapshotArtifactChunkBytes)
	withBuffer, err := WriteSnapshotArtifact(&buffered, snapshot, SnapshotArtifactOptions{
		TargetChunkBytes: MinSnapshotArtifactChunkBytes,
		PayloadBuffer:    reusable,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, buffered.Bytes()) || withBuffer.Digest != written.Digest {
		t.Fatal("caller workspace changed deterministic artifact")
	}

	var rows []snapshotArtifactRow
	var checkpoints []SnapshotArtifactCheckpoint
	verified, err := VerifySnapshotArtifact(bytes.NewReader(first), SnapshotArtifactCallbacks{
		Row: func(collection SnapshotArtifactCollection, key, value []byte) error {
			rows = append(rows, snapshotArtifactRow{
				collection: collection, key: bytes.Clone(key), value: bytes.Clone(value),
			})
			return nil
		},
		Chunk: func(checkpoint SnapshotArtifactCheckpoint, cursor *SnapshotArtifactCursor) error {
			if cursor.Offset() != checkpoint.EndOffset ||
				cursor.NextSequence() != checkpoint.Sequence+1 ||
				cursor.PreviousDigest() != checkpoint.Digest {
				t.Fatalf("checkpoint cursor = offset %d sequence %d digest %x, checkpoint %+v",
					cursor.Offset(), cursor.NextSequence(), cursor.PreviousDigest(), checkpoint)
			}
			checkpoints = append(checkpoints, checkpoint)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("VerifySnapshotArtifact: %v", err)
	}
	if !equalState(verified.State, snapshot.State()) || !bytes.Equal(verified.UserCollection, []byte("docs")) {
		t.Fatalf("verified identity = %+v %q", verified.State, verified.UserCollection)
	}
	if verified.TargetChunkBytes != MinSnapshotArtifactChunkBytes ||
		verified.Chunks != written.Chunks || verified.SystemRows != written.SystemRows ||
		verified.UserRows != written.UserRows || verified.PayloadBytes != written.PayloadBytes ||
		verified.EncodedBytes != uint64(len(first)) || verified.EncodedBytes != written.EncodedBytes ||
		verified.HeaderDigest != written.HeaderDigest ||
		verified.LastChunkDigest != written.LastChunkDigest ||
		verified.ImageDigest != written.ImageDigest || verified.Digest != written.Digest {
		t.Fatalf("verified manifest = %+v, written = %+v", verified, written)
	}
	if verified.Chunks < 4 || len(checkpoints) != int(verified.Chunks) {
		t.Fatalf("checkpoints = %d, chunks = %d", len(checkpoints), verified.Chunks)
	}
	previousEnd := uint64(0)
	for i, checkpoint := range checkpoints {
		if checkpoint.Sequence != uint64(i) || checkpoint.Rows == 0 || checkpoint.PayloadBytes == 0 ||
			checkpoint.EndOffset <= previousEnd || checkpoint.EndOffset >= verified.EncodedBytes ||
			checkpoint.Digest == ([sha256.Size]byte{}) {
			t.Fatalf("checkpoint[%d] = %+v", i, checkpoint)
		}
		previousEnd = checkpoint.EndOffset
	}
	if checkpoints[len(checkpoints)-1].Digest != verified.LastChunkDigest {
		t.Fatal("last checkpoint digest differs from manifest")
	}
	wantRows := collectSnapshotArtifactSourceRows(t, snapshot)
	if len(rows) != len(wantRows) {
		t.Fatalf("decoded rows = %d, source rows = %d", len(rows), len(wantRows))
	}
	for i := range rows {
		if rows[i].collection != wantRows[i].collection ||
			!bytes.Equal(rows[i].key, wantRows[i].key) ||
			!bytes.Equal(rows[i].value, wantRows[i].value) {
			t.Fatalf("decoded row[%d] = %+v, source = %+v", i, rows[i], wantRows[i])
		}
	}

	systemRows, userRows := uint64(0), uint64(0)
	var lastSystem, lastUser []byte
	for _, row := range rows {
		switch row.collection {
		case SnapshotArtifactSystem:
			if lastSystem != nil && bytes.Compare(lastSystem, row.key) >= 0 {
				t.Fatalf("system rows out of order: %x then %x", lastSystem, row.key)
			}
			lastSystem = row.key
			systemRows++
		case SnapshotArtifactUser:
			if lastUser != nil && bytes.Compare(lastUser, row.key) >= 0 {
				t.Fatalf("user rows out of order: %x then %x", lastUser, row.key)
			}
			lastUser = row.key
			userRows++
		default:
			t.Fatalf("unknown collection %d", row.collection)
		}
	}
	wantSystemRows, systemRowsOK := stateSystemRowCount(verified.State)
	if systemRows != verified.SystemRows || userRows != verified.UserRows ||
		!systemRowsOK || systemRows != wantSystemRows ||
		userRows != 7 {
		t.Fatalf("row totals system=%d user=%d manifest=%+v", systemRows, userRows, verified)
	}
}

func TestSnapshotArtifactImageDigestIsCanonicalAcrossChunking(t *testing.T) {
	_, snapshot := snapshotArtifactFixture(t)
	var compact, wide bytes.Buffer
	compactManifest, err := WriteSnapshotArtifact(&compact, snapshot, SnapshotArtifactOptions{
		TargetChunkBytes: MinSnapshotArtifactChunkBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	wideManifest, err := WriteSnapshotArtifact(&wide, snapshot, SnapshotArtifactOptions{
		TargetChunkBytes: DefaultSnapshotArtifactChunkBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	if compactManifest.Digest == wideManifest.Digest || bytes.Equal(compact.Bytes(), wide.Bytes()) {
		t.Fatal("different chunking unexpectedly produced the same artifact identity")
	}
	if compactManifest.ImageDigest == ([sha256.Size]byte{}) ||
		compactManifest.ImageDigest != wideManifest.ImageDigest {
		t.Fatalf("chunk-dependent image digests: compact=%x wide=%x",
			compactManifest.ImageDigest, wideManifest.ImageDigest)
	}
	audited, err := snapshot.CanonicalImageDigest()
	if err != nil {
		t.Fatal(err)
	}
	if audited != compactManifest.ImageDigest {
		t.Fatalf("artifact image digest = %x, explicit audit = %x",
			compactManifest.ImageDigest, audited)
	}
}

func collectSnapshotArtifactSourceRows(t testing.TB, snapshot *ReadSnapshot) []snapshotArtifactRow {
	t.Helper()
	var rows []snapshotArtifactRow
	appendRow := func(collection SnapshotArtifactCollection) func([]byte, []byte) error {
		return func(key, value []byte) error {
			rows = append(rows, snapshotArtifactRow{
				collection: collection, key: bytes.Clone(key), value: bytes.Clone(value),
			})
			return nil
		}
	}
	if err := snapshot.RangeSystem(appendRow(SnapshotArtifactSystem)); err != nil {
		t.Fatalf("RangeSystem: %v", err)
	}
	user, ok := snapshot.Collection("docs")
	if !ok {
		t.Fatal("missing user snapshot")
	}
	if err := user.RangeRaw(appendRow(SnapshotArtifactUser)); err != nil {
		t.Fatalf("RangeRaw: %v", err)
	}
	return rows
}

func TestSnapshotArtifactEmptyUserImage(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	snapshot, err := fixture.machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	artifact, written := writeSnapshotArtifactFixture(t, snapshot)
	verified, err := VerifySnapshotArtifact(bytes.NewReader(artifact), SnapshotArtifactCallbacks{})
	if err != nil {
		t.Fatal(err)
	}
	if written.SystemRows != 1 || written.UserRows != 0 ||
		verified.SystemRows != 1 || verified.UserRows != 0 || verified.Chunks != 1 {
		t.Fatalf("written=%+v verified=%+v", written, verified)
	}
}

func TestSnapshotArtifactDoesNotFragmentRowAboveTarget(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	open := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, open)
	value := []byte(fmt.Sprintf(`{"payload":"%s"}`, bytes.Repeat([]byte{'x'}, 8<<10)))
	command := testCommand(fixture.binding, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("large"), Value: value,
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	snapshot, err := fixture.machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	artifact, _ := writeSnapshotArtifactFixture(t, snapshot)
	userChunks := 0
	if _, err := VerifySnapshotArtifact(bytes.NewReader(artifact), SnapshotArtifactCallbacks{
		Chunk: func(checkpoint SnapshotArtifactCheckpoint, _ *SnapshotArtifactCursor) error {
			if checkpoint.Collection == SnapshotArtifactUser {
				userChunks++
				if checkpoint.Rows != 1 || checkpoint.PayloadBytes <= MinSnapshotArtifactChunkBytes {
					t.Fatalf("oversize row checkpoint = %+v", checkpoint)
				}
			}
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if userChunks != 1 {
		t.Fatalf("user chunks = %d, want 1", userChunks)
	}
}

func TestSnapshotArtifactExceptionalRowMatchesContiguousFraming(t *testing.T) {
	key := bytes.Repeat([]byte{'k'}, 73)
	value := bytes.Repeat([]byte{'v'}, MinSnapshotArtifactChunkBytes+211)
	rowBytes, ok := snapshotArtifactRowBytes(SnapshotArtifactUser, key, value)
	if !ok || rowBytes <= MinSnapshotArtifactChunkBytes {
		t.Fatalf("exceptional row bytes = %d, %t", rowBytes, ok)
	}
	previous := sha256.Sum256([]byte("prior chunk"))

	var referenceOutput bytes.Buffer
	reference := snapshotArtifactWriter{
		w:          &referenceOutput,
		target:     MinSnapshotArtifactChunkBytes,
		collection: SnapshotArtifactUser,
		chunks:     7, userRows: 11, payloadBytes: 1234, encodedBytes: 5678,
		previousDigest: previous,
	}
	reference.payload = make([]byte, 0, rowBytes)
	reference.payload = binary.LittleEndian.AppendUint32(reference.payload, uint32(len(key)))
	reference.payload = binary.LittleEndian.AppendUint32(reference.payload, uint32(len(value)))
	reference.payload = append(reference.payload, key...)
	reference.payload = append(reference.payload, value...)
	reference.chunkRows = 1
	if err := reference.flush(); err != nil {
		t.Fatal(err)
	}

	directOutput := &snapshotArtifactRecordingWriter{}
	direct := snapshotArtifactWriter{
		w: directOutput, target: MinSnapshotArtifactChunkBytes,
		payload:    make([]byte, 0, MinSnapshotArtifactChunkBytes),
		collection: SnapshotArtifactUser,
		chunks:     7, userRows: 11, payloadBytes: 1234, encodedBytes: 5678,
		previousDigest: previous,
	}
	if err := direct.writeExceptionalRow(key, value, rowBytes); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(referenceOutput.Bytes(), directOutput.Bytes()) {
		t.Fatal("segmented exceptional row changed canonical chunk bytes")
	}
	wantSegments := []int{
		snapshotArtifactChunkHeaderBytes,
		snapshotArtifactRowHeaderBytes,
		len(key),
		len(value),
		sha256.Size,
	}
	if !slices.Equal(directOutput.writes, wantSegments) {
		t.Fatalf("exceptional write segments = %v, want %v", directOutput.writes, wantSegments)
	}
	if direct.chunks != reference.chunks || direct.userRows != reference.userRows ||
		direct.payloadBytes != reference.payloadBytes ||
		direct.encodedBytes != reference.encodedBytes ||
		direct.previousDigest != reference.previousDigest {
		t.Fatalf("segmented writer state = %+v, reference %+v", direct, reference)
	}
}

func TestSnapshotArtifactMaximumRowDoesNotGrowAggregatePayload(t *testing.T) {
	key := bytes.Repeat([]byte{'k'}, replication.MaxMutationKeyBytes)
	value := bytes.Repeat([]byte{'v'}, replication.MaxMutationValueBytes)
	rowBytes, ok := snapshotArtifactRowBytes(SnapshotArtifactUser, key, value)
	if !ok || rowBytes != replication.MaxMutationKeyBytes+
		replication.MaxMutationValueBytes+snapshotArtifactRowHeaderBytes {
		t.Fatalf("maximum row bytes = %d, %t", rowBytes, ok)
	}
	payload := make([]byte, 0, DefaultSnapshotArtifactChunkBytes)
	payloadStart := &payload[:1][0]
	writer := snapshotArtifactWriter{
		w: io.Discard, target: DefaultSnapshotArtifactChunkBytes,
		payload: payload, collection: SnapshotArtifactUser,
	}
	if err := writer.writeExceptionalRow(key, value, rowBytes); err != nil {
		t.Fatal(err)
	}
	if len(writer.payload) != 0 || cap(writer.payload) != DefaultSnapshotArtifactChunkBytes ||
		&writer.payload[:1][0] != payloadStart {
		t.Fatalf("maximum row grew aggregate payload: len/cap %d/%d", len(writer.payload), cap(writer.payload))
	}
	if writer.chunks != 1 || writer.userRows != 1 ||
		writer.payloadBytes != uint64(rowBytes) {
		t.Fatalf("maximum row counters = %+v", writer)
	}
}

func TestSnapshotArtifactCaptureRowExactHostileBound(t *testing.T) {
	key := []byte{'k'}
	value := make([]byte, MaxTransitionCaptureRecordBytes)
	rowBytes, ok := snapshotArtifactRowBytes(SnapshotArtifactCapture, key, value)
	if !ok || rowBytes != snapshotArtifactRowHeaderBytes+len(key)+len(value) {
		t.Fatalf("exact capture row = %d/%v", rowBytes, ok)
	}
	value = append(value, 0)
	if _, ok := snapshotArtifactRowBytes(SnapshotArtifactCapture, key, value); ok {
		t.Fatal("capture row above hostile bound accepted")
	}
}

func TestVerifySnapshotArtifactRecomputesCaptureImageDigest(t *testing.T) {
	_, snapshot := snapshotArtifactFixture(t)
	artifact, _ := writeSnapshotArtifactFixture(t, snapshot)
	footer := artifact[len(artifact)-snapshotArtifactFooterBytes:]
	footer[168] ^= 1
	digest := snapshotArtifactDigest(snapshotArtifactFooterDomain, footer[:208])
	copy(footer[208:240], digest[:])
	if _, err := VerifySnapshotArtifact(bytes.NewReader(artifact), SnapshotArtifactCallbacks{}); !errors.Is(err, ErrSnapshotArtifact) {
		t.Fatalf("self-consistent wrong capture digest error = %v", err)
	}
}

func TestSnapshotArtifactRejectsDualBorrowedRowConsumers(t *testing.T) {
	_, snapshot := snapshotArtifactFixture(t)
	artifact, _ := writeSnapshotArtifactFixture(t, snapshot)
	_, err := VerifySnapshotArtifact(bytes.NewReader(artifact), SnapshotArtifactCallbacks{
		Row:  func(SnapshotArtifactCollection, []byte, []byte) error { return nil },
		Rows: func(SnapshotArtifactCheckpoint, SnapshotArtifactRows) error { return nil },
	})
	if !errors.Is(err, ErrSnapshotArtifact) {
		t.Fatalf("dual row consumers error = %v", err)
	}
}

func TestSnapshotArtifactExceptionalRowHandlesEverySegmentShortWrite(t *testing.T) {
	key := bytes.Repeat([]byte{'k'}, 41)
	value := bytes.Repeat([]byte{'v'}, MinSnapshotArtifactChunkBytes+17)
	rowBytes, ok := snapshotArtifactRowBytes(SnapshotArtifactUser, key, value)
	if !ok {
		t.Fatal("exceptional short-write row is invalid")
	}
	for _, test := range []struct {
		name string
		cut  int
	}{
		{name: "chunk-header", cut: 1},
		{name: "row-header", cut: snapshotArtifactChunkHeaderBytes + 1},
		{name: "key", cut: snapshotArtifactChunkHeaderBytes + snapshotArtifactRowHeaderBytes + 1},
		{name: "value", cut: snapshotArtifactChunkHeaderBytes + snapshotArtifactRowHeaderBytes + len(key) + 1},
		{name: "digest", cut: snapshotArtifactChunkHeaderBytes + rowBytes + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			short := &snapshotArtifactShortWriter{remaining: test.cut}
			writer := snapshotArtifactWriter{
				w: short, target: MinSnapshotArtifactChunkBytes,
				payload:    make([]byte, 0, MinSnapshotArtifactChunkBytes),
				collection: SnapshotArtifactUser,
			}
			if err := writer.writeExceptionalRow(key, value, rowBytes); !errors.Is(err, io.ErrShortWrite) {
				t.Fatalf("short exceptional write = %v", err)
			}
			if writer.chunks != 0 || writer.userRows != 0 ||
				writer.payloadBytes != 0 || writer.encodedBytes != 0 {
				t.Fatalf("short exceptional write committed counters: %+v", writer)
			}
		})
	}
}

func TestSnapshotArtifactResumesAtEveryVerifiedChunk(t *testing.T) {
	_, snapshot := snapshotArtifactFixture(t)
	artifact, written := writeSnapshotArtifactFixture(t, snapshot)
	var checkpoints []SnapshotArtifactCheckpoint
	if _, err := VerifySnapshotArtifact(bytes.NewReader(artifact), SnapshotArtifactCallbacks{
		Chunk: func(checkpoint SnapshotArtifactCheckpoint, _ *SnapshotArtifactCursor) error {
			checkpoints = append(checkpoints, checkpoint)
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	wantRows := collectSnapshotArtifactSourceRows(t, snapshot)
	for _, split := range checkpoints {
		t.Run(fmt.Sprintf("sequence-%d", split.Sequence), func(t *testing.T) {
			var rows []snapshotArtifactRow
			callbacks := SnapshotArtifactCallbacks{
				Row: func(collection SnapshotArtifactCollection, key, value []byte) error {
					rows = append(rows, snapshotArtifactRow{
						collection: collection, key: bytes.Clone(key), value: bytes.Clone(value),
					})
					return nil
				},
			}
			manifest, cursor, err := ContinueSnapshotArtifact(
				bytes.NewReader(artifact[:split.EndOffset]), nil, callbacks,
			)
			if manifest.State.Applied != 0 || manifest.Digest != ([sha256.Size]byte{}) ||
				len(manifest.UserCollection) != 0 || !errors.Is(err, ErrSnapshotArtifact) ||
				cursor == nil || cursor.Offset() != split.EndOffset ||
				cursor.NextSequence() != split.Sequence+1 ||
				cursor.PreviousDigest() != split.Digest {
				t.Fatalf("prefix = manifest %+v cursor offset=%d sequence=%d digest=%x error=%v",
					manifest, cursor.Offset(), cursor.NextSequence(), cursor.PreviousDigest(), err)
			}
			manifest, finalCursor, err := ContinueSnapshotArtifact(
				bytes.NewReader(artifact[split.EndOffset:]), cursor, callbacks,
			)
			if err != nil || manifest.Digest != written.Digest ||
				manifest.EncodedBytes != uint64(len(artifact)) || finalCursor == nil {
				t.Fatalf("resume = manifest %+v cursor=%+v error=%v", manifest, finalCursor, err)
			}
			if len(rows) != len(wantRows) {
				t.Fatalf("resumed rows = %d, want %d", len(rows), len(wantRows))
			}
			for i := range rows {
				if rows[i].collection != wantRows[i].collection ||
					!bytes.Equal(rows[i].key, wantRows[i].key) ||
					!bytes.Equal(rows[i].value, wantRows[i].value) {
					t.Fatalf("resumed row[%d] = %+v, want %+v", i, rows[i], wantRows[i])
				}
			}
		})
	}
}

func TestSnapshotArtifactChunkCallbackOrderingAndReplayCursor(t *testing.T) {
	_, snapshot := snapshotArtifactFixture(t)
	artifact, _ := writeSnapshotArtifactFixture(t, snapshot)
	phase := uint8(0)
	rows := uint64(0)
	chunks := 0
	stop := errors.New("stop after first chunk")
	_, cursor, err := ContinueSnapshotArtifact(bytes.NewReader(artifact), nil, SnapshotArtifactCallbacks{
		BeginChunk: func(checkpoint SnapshotArtifactCheckpoint) error {
			if phase != 0 || rows != 0 || checkpoint.Sequence != 0 {
				t.Fatalf("begin phase=%d rows=%d checkpoint=%+v", phase, rows, checkpoint)
			}
			phase = 1
			return nil
		},
		Row: func(SnapshotArtifactCollection, []byte, []byte) error {
			if phase != 1 {
				t.Fatalf("row phase = %d", phase)
			}
			rows++
			return nil
		},
		Chunk: func(checkpoint SnapshotArtifactCheckpoint, next *SnapshotArtifactCursor) error {
			if phase != 1 || rows != checkpoint.Rows || next.Offset() != checkpoint.EndOffset {
				t.Fatalf("chunk phase=%d rows=%d checkpoint=%+v offset=%d",
					phase, rows, checkpoint, next.Offset())
			}
			chunks++
			phase = 2
			return stop
		},
	})
	if !errors.Is(err, stop) || cursor == nil || cursor.NextSequence() != 0 ||
		cursor.Offset() >= uint64(len(artifact)) || chunks != 1 {
		t.Fatalf("stopped cursor offset=%d sequence=%d chunks=%d error=%v",
			cursor.Offset(), cursor.NextSequence(), chunks, err)
	}
}

func TestSnapshotArtifactRejectsCorruptionTruncationAndTrailingBytes(t *testing.T) {
	_, snapshot := snapshotArtifactFixture(t)
	artifact, _ := writeSnapshotArtifactFixture(t, snapshot)
	headerBytes := int(binary.LittleEndian.Uint32(artifact[12:16]))
	firstPayload := headerBytes + snapshotArtifactChunkHeaderBytes
	cases := []struct {
		name string
		data []byte
	}{
		{"header", corruptSnapshotArtifactByte(artifact, 32)},
		{"header_digest", corruptSnapshotArtifactByte(artifact, headerBytes-1)},
		{"chunk_header", corruptSnapshotArtifactByte(artifact, headerBytes+25)},
		{"chunk_payload", corruptSnapshotArtifactByte(artifact, firstPayload)},
		{"footer", corruptSnapshotArtifactByte(artifact, len(artifact)-snapshotArtifactFooterBytes+32)},
		{"footer_digest", corruptSnapshotArtifactByte(artifact, len(artifact)-1)},
		{"short_header", bytes.Clone(artifact[:20])},
		{"short_chunk", bytes.Clone(artifact[:firstPayload+1])},
		{"short_footer", bytes.Clone(artifact[:len(artifact)-1])},
		{"trailing", append(bytes.Clone(artifact), 1)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := VerifySnapshotArtifact(bytes.NewReader(test.data), SnapshotArtifactCallbacks{}); !errors.Is(err, ErrSnapshotArtifact) {
				t.Fatalf("VerifySnapshotArtifact error = %v", err)
			}
		})
	}
}

func TestSnapshotArtifactHiddenSystemKeyGrammar(t *testing.T) {
	key := func(prefix byte, size int) []byte {
		result := bytes.Repeat([]byte{0x7f}, size)
		result[0] = prefix
		return result
	}
	payload := func(key []byte) []byte {
		result := make([]byte, snapshotArtifactRowHeaderBytes+len(key)+1)
		binary.LittleEndian.PutUint32(result[0:4], uint32(len(key)))
		binary.LittleEndian.PutUint32(result[4:8], 1)
		copy(result[snapshotArtifactRowHeaderBytes:], key)
		result[len(result)-1] = 1
		return result
	}
	consume := func(key []byte) error {
		previousKey := make([]byte, replication.MaxMutationKeyBytes)
		previousKeyBytes := 0
		_, err := consumeSnapshotArtifactRows(
			snapshotArtifactChunk{Collection: SnapshotArtifactSystem, Rows: 1},
			payload(key), nil, previousKey, &previousKeyBytes, nil, true, nil,
		)
		return err
	}

	for _, valid := range [][]byte{
		key(1, sha256.Size+1),
		key(2, sha256.Size+3),
	} {
		if err := consume(valid); err != nil {
			t.Fatalf("valid hidden system key %x rejected: %v", valid, err)
		}
	}
	for _, invalid := range [][]byte{
		key(1, sha256.Size),
		key(1, sha256.Size+2),
		key(1, sha256.Size+3),
		key(2, sha256.Size+1),
		key(2, sha256.Size+2),
		key(2, sha256.Size+4),
		key(3, sha256.Size+1),
		key(3, sha256.Size+3),
	} {
		if err := consume(invalid); !errors.Is(err, ErrSnapshotArtifact) {
			t.Fatalf("invalid hidden system key %x error = %v", invalid, err)
		}
	}
}

func TestSnapshotArtifactValidatesExactTransactionRows(t *testing.T) {
	rowPayload := func(key, value []byte) []byte {
		result := make([]byte, snapshotArtifactRowHeaderBytes+len(key)+len(value))
		binary.LittleEndian.PutUint32(result[0:4], uint32(len(key)))
		binary.LittleEndian.PutUint32(result[4:8], uint32(len(value)))
		copy(result[snapshotArtifactRowHeaderBytes:], key)
		copy(result[snapshotArtifactRowHeaderBytes+len(key):], value)
		return result
	}
	consume := func(key, value []byte, scratch *snapshotArtifactTransactionScratch) error {
		previousKey := make([]byte, replication.MaxMutationKeyBytes)
		previousKeyBytes := 0
		_, err := consumeSnapshotArtifactRows(
			snapshotArtifactChunk{Collection: SnapshotArtifactSystem, Rows: 1},
			rowPayload(key, value), nil, previousKey, &previousKeyBytes, nil, true, scratch,
		)
		return err
	}
	control := transactionCodecControl(t)
	controlValue, err := AppendTransactionControl(nil, control)
	if err != nil {
		t.Fatal(err)
	}
	controlKey, _ := TransactionControlStorageKey(control.Role, control.ID)
	id, coordinator := transactionCodecCoordinatorPayload(t)
	payloadValue, err := AppendTransactionCoordinatorPayload(
		nil, id, distributedtxn.ReplicatedPayloadCoordinator, coordinator,
	)
	if err != nil {
		t.Fatal(err)
	}
	payloadKey, _ := TransactionCoordinatorPayloadStorageKey(id)
	manifestID, segment, _ := transactionCodecManifestSegment(t)
	manifestValue, err := AppendTransactionManifestPage(nil, manifestID, segment)
	if err != nil {
		t.Fatal(err)
	}
	manifestKey, _ := TransactionManifestPageStorageKey(manifestID, segment.Index)
	mutation := transactionCodecMutation()
	command, err := replication.OpenCommand(testCommand(testBinding(), 1, mutation))
	if err != nil {
		t.Fatal(err)
	}
	relations := command.RelationBatches()
	if !relations.Next() || relations.Batch().Relation != 1 {
		t.Fatal("transaction relation fixture did not expose relation 1")
	}
	relationValue, err := AppendTransactionRelationPayload(nil, control.ID, relations.Batch())
	if err != nil {
		t.Fatal(err)
	}
	relationKey, _ := TransactionRelationPayloadStorageKey(control.ID, 1)
	intentValue, err := AppendTransactionIntent(nil, control.ID, 1, []byte("intent-key"))
	if err != nil {
		t.Fatal(err)
	}
	intentKey, _ := TransactionIntentStorageKey(1, []byte("intent-key"))
	scratch := new(snapshotArtifactTransactionScratch)
	for name, row := range map[string]struct{ key, value []byte }{
		"control":  {controlKey[:], controlValue},
		"payload":  {payloadKey[:], payloadValue},
		"manifest": {manifestKey[:], manifestValue},
		"relation": {relationKey[:], relationValue},
		"intent":   {intentKey[:], intentValue},
	} {
		t.Run(name, func(t *testing.T) {
			if err := consume(row.key, row.value, scratch); err != nil {
				t.Fatal(err)
			}
			corrupt := bytes.Clone(row.value)
			corrupt[len(corrupt)-1] ^= 1
			if err := consume(row.key, corrupt, scratch); !errors.Is(err, ErrSnapshotArtifact) {
				t.Fatalf("corrupt row error=%v", err)
			}
			wrongKey := bytes.Clone(row.key)
			wrongKey[len(wrongKey)-1] ^= 1
			if err := consume(wrongKey, row.value, scratch); !errors.Is(err, ErrSnapshotArtifact) {
				t.Fatalf("mismatched key error=%v", err)
			}
		})
	}
	if cap(scratch.participants) != distributedtxn.MaxManifestPageParticipants ||
		cap(scratch.identities) != distributedtxn.MaxManifestPageParticipants*distributedtxn.MaxShardIdentityBytes*2 {
		t.Fatal("manifest scratch was not retained at the fixed max-page bound")
	}
	largeSystemValue := make([]byte, replication.MaxMutationValueBytes+1)
	if _, ok := snapshotArtifactRowBytes(SnapshotArtifactSystem, relationKey[:], largeSystemValue); !ok {
		t.Fatal("system row sizing rejected a valid packed-transaction value class")
	}
	if _, ok := snapshotArtifactRowBytes(SnapshotArtifactUser, relationKey[:], largeSystemValue); ok {
		t.Fatal("user row sizing inherited the wider hidden-transaction bound")
	}
}

func TestSnapshotArtifactBoundsAndCallbackFailures(t *testing.T) {
	_, snapshot := snapshotArtifactFixture(t)
	for _, target := range []int{-1, MinSnapshotArtifactChunkBytes - 1, MaxSnapshotArtifactChunkBytes + 1} {
		if _, err := WriteSnapshotArtifact(io.Discard, snapshot, SnapshotArtifactOptions{TargetChunkBytes: target}); !errors.Is(err, ErrSnapshotArtifactBound) {
			t.Fatalf("target %d error = %v", target, err)
		}
	}
	if _, err := WriteSnapshotArtifact(io.Discard, snapshot, SnapshotArtifactOptions{
		TargetChunkBytes: MinSnapshotArtifactChunkBytes,
		PayloadBuffer:    make([]byte, 0, MinSnapshotArtifactChunkBytes-1),
	}); !errors.Is(err, ErrSnapshotArtifactBound) {
		t.Fatalf("short payload buffer error = %v", err)
	}
	artifact, _ := writeSnapshotArtifactFixture(t, snapshot)
	headerBytes := int(binary.LittleEndian.Uint32(artifact[12:16]))
	oversized := bytes.Clone(artifact)
	binary.LittleEndian.PutUint32(
		oversized[headerBytes+32:headerBytes+36], MaxSnapshotArtifactChunkBytes+1,
	)
	if _, err := VerifySnapshotArtifact(bytes.NewReader(oversized), SnapshotArtifactCallbacks{}); !errors.Is(err, ErrSnapshotArtifact) {
		t.Fatalf("oversized chunk error = %v", err)
	}
	corruptPayload := corruptSnapshotArtifactByte(
		artifact, headerBytes+snapshotArtifactChunkHeaderBytes,
	)
	called := 0
	if _, err := VerifySnapshotArtifact(bytes.NewReader(corruptPayload), SnapshotArtifactCallbacks{
		Row: func(SnapshotArtifactCollection, []byte, []byte) error {
			called++
			return nil
		},
	}); !errors.Is(err, ErrSnapshotArtifact) || called != 0 {
		t.Fatalf("corrupt payload callback count = %d, error = %v", called, err)
	}

	stopRow := errors.New("stop row")
	rows := 0
	if _, err := VerifySnapshotArtifact(bytes.NewReader(artifact), SnapshotArtifactCallbacks{
		Row: func(SnapshotArtifactCollection, []byte, []byte) error {
			rows++
			return stopRow
		},
	}); !errors.Is(err, stopRow) || rows != 1 {
		t.Fatalf("row callback = rows %d, error %v", rows, err)
	}
	stopChunk := errors.New("stop chunk")
	chunks := 0
	if _, err := VerifySnapshotArtifact(bytes.NewReader(artifact), SnapshotArtifactCallbacks{
		Chunk: func(SnapshotArtifactCheckpoint, *SnapshotArtifactCursor) error {
			chunks++
			return stopChunk
		},
	}); !errors.Is(err, stopChunk) || chunks != 1 {
		t.Fatalf("chunk callback = chunks %d, error %v", chunks, err)
	}
}

func TestRequiredSnapshotArtifactPayloadCapacity(t *testing.T) {
	ordinary, err := RequiredSnapshotArtifactPayloadCapacity(0, 32, 4096)
	if err != nil || ordinary != DefaultSnapshotArtifactChunkBytes {
		t.Fatalf("ordinary payload capacity = %d, %v", ordinary, err)
	}
	maximum, err := RequiredSnapshotArtifactPayloadCapacity(
		DefaultSnapshotArtifactChunkBytes,
		replication.MaxMutationKeyBytes,
		replication.MaxMutationValueBytes,
	)
	if err != nil || maximum != DefaultSnapshotArtifactChunkBytes {
		t.Fatalf("maximum payload capacity = %d, %v", maximum, err)
	}
	for _, limits := range [][2]int{
		{0, 1},
		{1, 0},
		{replication.MaxMutationKeyBytes + 1, 1},
		{1, replication.MaxMutationValueBytes + 1},
	} {
		if _, err := RequiredSnapshotArtifactPayloadCapacity(
			0,
			limits[0],
			limits[1],
		); !errors.Is(err, ErrSnapshotArtifactBound) {
			t.Fatalf("row bounds %v error = %v", limits, err)
		}
	}
}

func TestSnapshotArtifactWriterHandlesShortWrites(t *testing.T) {
	_, snapshot := snapshotArtifactFixture(t)
	short := &snapshotArtifactShortWriter{remaining: 97}
	if _, err := WriteSnapshotArtifact(short, snapshot, SnapshotArtifactOptions{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short writer error = %v", err)
	}
}

type snapshotArtifactShortWriter struct {
	remaining int
}

func (w *snapshotArtifactShortWriter) Write(src []byte) (int, error) {
	if w.remaining == 0 {
		return 0, nil
	}
	n := min(len(src), w.remaining)
	w.remaining -= n
	return n, nil
}

type snapshotArtifactRecordingWriter struct {
	bytes.Buffer
	writes []int
}

func (w *snapshotArtifactRecordingWriter) Write(src []byte) (int, error) {
	w.writes = append(w.writes, len(src))
	return w.Buffer.Write(src)
}

func corruptSnapshotArtifactByte(src []byte, offset int) []byte {
	result := bytes.Clone(src)
	result[offset] ^= 0x40
	return result
}

func FuzzVerifySnapshotArtifact(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("not an artifact"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = VerifySnapshotArtifact(bytes.NewReader(data), SnapshotArtifactCallbacks{})
	})
}
