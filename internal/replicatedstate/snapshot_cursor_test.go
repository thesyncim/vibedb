package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"
)

func snapshotArtifactCursorFixture(
	t testing.TB,
) ([]byte, SnapshotArtifactManifest, *SnapshotArtifactCursor) {
	t.Helper()
	_, snapshot := snapshotArtifactFixture(t)
	artifact, written := writeSnapshotArtifactFixture(t, snapshot)
	var cursor *SnapshotArtifactCursor
	if _, err := VerifySnapshotArtifact(bytes.NewReader(artifact), SnapshotArtifactCallbacks{
		Chunk: func(_ SnapshotArtifactCheckpoint, next *SnapshotArtifactCursor) error {
			cursor = next
			return nil
		},
	}); err != nil {
		t.Fatalf("VerifySnapshotArtifact: %v", err)
	}
	if cursor == nil || cursor.NextSequence() != written.Chunks {
		t.Fatalf("cursor = %+v, chunks = %d", cursor, written.Chunks)
	}
	return artifact, written, cursor
}

func TestSnapshotArtifactCursorRoundTripAndResume(t *testing.T) {
	artifact, written, cursor := snapshotArtifactCursorFixture(t)
	encoded, err := AppendSnapshotArtifactCursor(nil, cursor)
	if err != nil {
		t.Fatal(err)
	}
	wantGolden := [sha256.Size]byte{
		0x15, 0x1d, 0xa2, 0x8e, 0x43, 0x08, 0xe8, 0xfc,
		0x2e, 0x9f, 0xcc, 0x6e, 0xaf, 0xf1, 0xd3, 0xe2,
		0xd8, 0xcd, 0x24, 0xf5, 0x1c, 0x61, 0x05, 0xa3,
		0x1d, 0x2e, 0xd9, 0x1d, 0x36, 0x1e, 0x04, 0xbe,
	}
	if got := sha256.Sum256(encoded); got != wantGolden {
		t.Fatalf("cursor golden digest = %x, want %x", got, wantGolden)
	}
	decoded, err := OpenSnapshotArtifactCursor(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Offset() != cursor.Offset() ||
		decoded.NextSequence() != cursor.NextSequence() ||
		decoded.PreviousDigest() != cursor.PreviousDigest() {
		t.Fatalf("decoded cursor offset=%d sequence=%d digest=%x; want %d %d %x",
			decoded.Offset(), decoded.NextSequence(), decoded.PreviousDigest(),
			cursor.Offset(), cursor.NextSequence(), cursor.PreviousDigest())
	}
	gotPrefix, wantPrefix := decoded.PrefixManifest(), cursor.PrefixManifest()
	if !equalState(gotPrefix.State, wantPrefix.State) ||
		!bytes.Equal(gotPrefix.UserCollection, wantPrefix.UserCollection) ||
		gotPrefix.TargetChunkBytes != wantPrefix.TargetChunkBytes ||
		gotPrefix.Chunks != wantPrefix.Chunks ||
		gotPrefix.SystemRows != wantPrefix.SystemRows ||
		gotPrefix.UserRows != wantPrefix.UserRows ||
		gotPrefix.PayloadBytes != wantPrefix.PayloadBytes ||
		gotPrefix.HeaderDigest != wantPrefix.HeaderDigest ||
		gotPrefix.LastChunkDigest != wantPrefix.LastChunkDigest {
		t.Fatalf("decoded prefix = %+v, want %+v", gotPrefix, wantPrefix)
	}
	manifest, _, err := ContinueSnapshotArtifact(
		bytes.NewReader(artifact[decoded.Offset():]), decoded, SnapshotArtifactCallbacks{},
	)
	if err != nil || manifest.Digest != written.Digest ||
		manifest.EncodedBytes != written.EncodedBytes {
		t.Fatalf("resume manifest = %+v, error = %v", manifest, err)
	}

	prefixed := []byte("prefix")
	appended, err := AppendSnapshotArtifactCursor(bytes.Clone(prefixed), decoded)
	if err != nil || !bytes.Equal(appended[:len(prefixed)], prefixed) {
		t.Fatalf("prefixed append = %x, %v", appended, err)
	}
	second, err := OpenSnapshotArtifactCursor(appended[len(prefixed):])
	if err != nil || second.Offset() != decoded.Offset() {
		t.Fatalf("prefixed open offset = %d, %v", second.Offset(), err)
	}
}

func TestOpenSnapshotArtifactCursorOwnsInput(t *testing.T) {
	_, _, cursor := snapshotArtifactCursorFixture(t)
	encoded, err := AppendSnapshotArtifactCursor(nil, cursor)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Clone(encoded)
	decoded, err := OpenSnapshotArtifactCursor(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for index := range encoded {
		encoded[index] ^= 0xff
	}
	reencoded, err := AppendSnapshotArtifactCursor(nil, decoded)
	if err != nil || !bytes.Equal(reencoded, want) {
		t.Fatalf("cursor retained caller input: encoded=%x want=%x err=%v", reencoded, want, err)
	}
}

func TestSnapshotArtifactCursorStrictCorruptionAndTruncation(t *testing.T) {
	_, _, cursor := snapshotArtifactCursorFixture(t)
	encoded, err := AppendSnapshotArtifactCursor(nil, cursor)
	if err != nil {
		t.Fatal(err)
	}
	for name, offset := range map[string]int{
		"magic": 0, "format": 8, "reserved": 30, "state": snapshotArtifactCursorFixedBytes,
		"name": len(encoded) - sha256.Size - 1, "checksum": len(encoded) - 1,
	} {
		t.Run(name, func(t *testing.T) {
			corrupt := bytes.Clone(encoded)
			corrupt[offset] ^= 0x80
			if _, err := OpenSnapshotArtifactCursor(corrupt); !errors.Is(err, ErrSnapshotArtifact) {
				t.Fatalf("OpenSnapshotArtifactCursor error = %v", err)
			}
		})
	}
	for _, cut := range []int{0, 1, snapshotArtifactCursorFixedBytes - 1, len(encoded) - 1} {
		if _, err := OpenSnapshotArtifactCursor(encoded[:cut]); !errors.Is(err, ErrSnapshotArtifact) {
			t.Fatalf("cut %d error = %v", cut, err)
		}
	}
	resealed := bytes.Clone(encoded)
	resealed[30] = 1
	digest := snapshotArtifactDigest(snapshotArtifactCursorDomain, resealed[:len(resealed)-sha256.Size])
	copy(resealed[len(resealed)-sha256.Size:], digest[:])
	if _, err := OpenSnapshotArtifactCursor(resealed); !errors.Is(err, ErrSnapshotArtifact) {
		t.Fatalf("resealed reserved field error = %v", err)
	}
	resealed = bytes.Clone(encoded)
	binary.LittleEndian.PutUint64(
		resealed[40:48], binary.LittleEndian.Uint64(resealed[40:48])+1,
	)
	digest = snapshotArtifactDigest(snapshotArtifactCursorDomain, resealed[:len(resealed)-sha256.Size])
	copy(resealed[len(resealed)-sha256.Size:], digest[:])
	if _, err := OpenSnapshotArtifactCursor(resealed); !errors.Is(err, ErrSnapshotArtifact) {
		t.Fatalf("resealed impossible offset error = %v", err)
	}
}

func TestSnapshotArtifactCursorRejectsDestinationAlias(t *testing.T) {
	_, _, cursor := snapshotArtifactCursorFixture(t)
	encoded, err := AppendSnapshotArtifactCursor(nil, cursor)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 4, 4+len(encoded))
	copy(dst, "safe")
	writable := dst[:cap(dst)][len(dst):]
	copy(writable, cursor.manifest.UserCollection)
	alias := *cursor
	alias.manifest = cursor.manifest
	alias.manifest.UserCollection = writable[:len(cursor.manifest.UserCollection)]
	before := bytes.Clone(dst)
	got, err := AppendSnapshotArtifactCursor(dst, &alias)
	if !errors.Is(err, ErrCodecAlias) || !bytes.Equal(got, before) {
		t.Fatalf("alias append = %x, %v", got, err)
	}
}

func FuzzOpenSnapshotArtifactCursor(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("cursor"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = OpenSnapshotArtifactCursor(data)
	})
}
