package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"

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
	for i := uint64(1); i <= 7; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		value := []byte(fmt.Sprintf(`{"sequence":%d,"payload":"%s"}`, i, bytes.Repeat([]byte{'a' + byte(i)}, 1400)))
		command := testCommand(fixture.binding, i, replication.Mutation{
			Kind: replication.MutationPut, Key: key, Value: value,
		})
		if _, err := fixture.machine.ApplyNormal(normalMeta(i+1), command); err != nil {
			t.Fatalf("ApplyNormal(%d): %v", i+1, err)
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
	const golden = "b3ed7c4b1656b759b061e1b93b334550bc71a45f479b3a06eea84497f3a3ec0b"
	if digest := fmt.Sprintf("%x", sha256.Sum256(first)); digest != golden {
		t.Fatalf("artifact golden digest = %s, want %s", digest, golden)
	}
	second, again := writeSnapshotArtifactFixture(t, snapshot)
	if !bytes.Equal(first, second) {
		t.Fatal("same snapshot and options produced different artifact bytes")
	}
	if written.Digest != again.Digest || written.HeaderDigest != again.HeaderDigest {
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
		Chunk: func(checkpoint SnapshotArtifactCheckpoint) error {
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
		verified.LastChunkDigest != written.LastChunkDigest || verified.Digest != written.Digest {
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
	if systemRows != verified.SystemRows || userRows != verified.UserRows ||
		systemRows != verified.State.CompletionCount+1 || userRows != 7 {
		t.Fatalf("row totals system=%d user=%d manifest=%+v", systemRows, userRows, verified)
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
	value := []byte(fmt.Sprintf(`{"payload":"%s"}`, bytes.Repeat([]byte{'x'}, 8<<10)))
	command := testCommand(fixture.binding, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("large"), Value: value,
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), command); err != nil {
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
		Chunk: func(checkpoint SnapshotArtifactCheckpoint) error {
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
		Chunk: func(SnapshotArtifactCheckpoint) error {
			chunks++
			return stopChunk
		},
	}); !errors.Is(err, stopChunk) || chunks != 1 {
		t.Fatalf("chunk callback = chunks %d, error %v", chunks, err)
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
