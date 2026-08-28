package replicatedstate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

func TestSnapshotArtifactCaptureBoundProfilesPreserveResumeAndBase(t *testing.T) {
	_, snapshot := snapshotArtifactFixture(t)
	for _, target := range []int{MinSnapshotArtifactChunkBytes, MaxSnapshotArtifactChunkBytes} {
		t.Run(fmt.Sprint(target), func(t *testing.T) {
			var output bytes.Buffer
			written, err := WriteSnapshotArtifact(&output, snapshot, SnapshotArtifactOptions{TargetChunkBytes: target})
			if err != nil {
				t.Fatal(err)
			}
			artifact := output.Bytes()
			limit := uint32(legacySnapshotArtifactChunkBytes)
			if target > legacySnapshotArtifactChunkBytes {
				limit = MaxSnapshotArtifactChunkBytes
			}
			if binary.LittleEndian.Uint32(artifact[28:32]) != limit {
				t.Fatal("wrong authenticated ceiling")
			}
			var cursors [][]byte
			verified, err := VerifySnapshotArtifact(bytes.NewReader(artifact), SnapshotArtifactCallbacks{
				Chunk: func(_ SnapshotArtifactCheckpoint, cursor *SnapshotArtifactCursor) error {
					raw, err := AppendSnapshotArtifactCursor(nil, cursor)
					cursors = append(cursors, raw)
					return err
				},
			})
			if err != nil || !equalSnapshotArtifactManifest(verified, written) || len(cursors) == 0 {
				t.Fatalf("verify err=%v", err)
			}
			for _, raw := range cursors {
				cursor, err := OpenSnapshotArtifactCursor(raw)
				if err != nil {
					t.Fatal(err)
				}
				if got, err := snapshotArtifactCursorChunkLimit(cursor); err != nil || got != limit {
					t.Fatalf("persisted cursor changed ceiling: %d %v", got, err)
				}
				resumed, _, err := ContinueSnapshotArtifact(bytes.NewReader(artifact[cursor.Offset():]), cursor, SnapshotArtifactCallbacks{})
				if err != nil || !equalSnapshotArtifactManifest(resumed, written) {
					t.Fatalf("resume err=%v", err)
				}
			}
			base, err := BuildSnapshotBase(written, testBootstrap())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := OpenSnapshotBase(base); err != nil {
				t.Fatal(err)
			}
			if err := validateExpectedSnapshotArtifact(written); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSnapshotArtifactEnforcesHistoricalCeilingBeforePayloadRead(t *testing.T) {
	_, snapshot := snapshotArtifactFixture(t)
	state, err := AppendState(nil, snapshot.state)
	if err != nil {
		t.Fatal(err)
	}
	header, digest, err := makeSnapshotArtifactHeader(state, snapshot.userName, MinSnapshotArtifactChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	writer := snapshotArtifactWriter{collection: SnapshotArtifactSystem, previousDigest: digest}
	if _, err := writer.prepareChunk(legacySnapshotArtifactChunkBytes+1, 1); err != nil {
		t.Fatal(err)
	}
	// No payload is supplied. Refusal must be the declared bound, not EOF
	// from attempting to read an oversized frame under the newer global limit.
	artifact := append(header, writer.chunkHeader[:]...)
	if _, err := VerifySnapshotArtifact(bytes.NewReader(artifact), SnapshotArtifactCallbacks{}); !errors.Is(err, ErrSnapshotArtifactBound) {
		t.Fatalf("did not enforce legacy ceiling before payload read: %v", err)
	}
	for _, limit := range []uint32{legacySnapshotArtifactChunkBytes - 1, legacySnapshotArtifactChunkBytes + 1, MaxSnapshotArtifactChunkBytes + 1} {
		binary.LittleEndian.PutUint32(artifact[28:32], limit)
		if _, _, _, err := readSnapshotArtifactHeader(bytes.NewReader(artifact)); !errors.Is(err, ErrSnapshotArtifactBound) {
			t.Fatalf("unknown ceiling %d accepted: %v", limit, err)
		}
	}
}
