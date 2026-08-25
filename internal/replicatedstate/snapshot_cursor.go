package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/replication"
)

const (
	snapshotArtifactCursorFormat     = uint16(1)
	snapshotArtifactCursorFixedBytes = 440
	maxSnapshotArtifactCursorBytes   = snapshotArtifactCursorFixedBytes +
		MaxStateEnvelopeBytes + replication.MaxCollectionBytes + sha256.Size
)

var (
	snapshotArtifactCursorMagic  = [8]byte{'V', 'D', 'B', 'S', 'C', 'U', 'R', 0}
	snapshotArtifactCursorDomain = []byte(
		"vibedb/replicated-state/snapshot-artifact-cursor\x00",
	)
)

// AppendSnapshotArtifactCursor appends one strict binary resume cursor. On
// error dst is unchanged. The cursor is a verified-prefix capability, so a
// receiver must persist it only after the row effects named by the same chunk
// are durable.
func AppendSnapshotArtifactCursor(
	dst []byte,
	cursor *SnapshotArtifactCursor,
) ([]byte, error) {
	if err := validateSnapshotArtifactCursor(cursor); err != nil {
		return dst, err
	}
	stateEnvelope, err := AppendState(nil, cursor.manifest.State)
	if err != nil {
		return dst, fmt.Errorf("%w: cursor state", ErrSnapshotArtifact)
	}
	total := snapshotArtifactCursorFixedBytes + len(stateEnvelope) +
		len(cursor.manifest.UserCollection) + sha256.Size
	if total > maxSnapshotArtifactCursorBytes || uint64(total) > math.MaxUint32 {
		return dst, fmt.Errorf("%w: cursor bytes", ErrSnapshotArtifactBound)
	}
	region := writableAppendRegion(dst, total)
	if byteSlicesOverlap(region, cursor.manifest.UserCollection) {
		return dst, ErrCodecAlias
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]
	copy(frame[0:8], snapshotArtifactCursorMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], snapshotArtifactCursorFormat)
	binary.LittleEndian.PutUint16(frame[10:12], snapshotArtifactCursorFixedBytes)
	binary.LittleEndian.PutUint32(frame[12:16], uint32(total))
	binary.LittleEndian.PutUint32(frame[16:20], uint32(len(stateEnvelope)))
	binary.LittleEndian.PutUint16(frame[20:22], uint16(len(cursor.manifest.UserCollection)))
	binary.LittleEndian.PutUint16(frame[22:24], cursor.previousKeyBytes)
	binary.LittleEndian.PutUint32(frame[24:28], cursor.manifest.TargetChunkBytes)
	frame[28] = byte(cursor.currentCollection)
	if cursor.stateRowSeen {
		frame[29] = 1
	}
	binary.LittleEndian.PutUint64(frame[32:40], cursor.nextSequence)
	binary.LittleEndian.PutUint64(frame[40:48], cursor.encodedBytes)
	binary.LittleEndian.PutUint64(frame[48:56], cursor.manifest.Chunks)
	binary.LittleEndian.PutUint64(frame[56:64], cursor.manifest.SystemRows)
	binary.LittleEndian.PutUint64(frame[64:72], cursor.manifest.UserRows)
	binary.LittleEndian.PutUint64(frame[72:80], cursor.manifest.PayloadBytes)
	binary.LittleEndian.PutUint64(frame[80:88], cursor.manifest.CaptureRows)
	copy(frame[88:120], cursor.manifest.HeaderDigest[:])
	copy(frame[120:152], cursor.previousDigest[:])
	copy(frame[152:152+int(cursor.previousKeyBytes)], cursor.previousKey[:cursor.previousKeyBytes])
	copy(frame[408:440], cursor.captureImageDigest[:])
	at := snapshotArtifactCursorFixedBytes
	at += copy(frame[at:], stateEnvelope)
	copy(frame[at:], cursor.manifest.UserCollection)
	digest := snapshotArtifactDigest(snapshotArtifactCursorDomain, frame[:len(frame)-sha256.Size])
	copy(frame[len(frame)-sha256.Size:], digest[:])
	return dst, nil
}

// OpenSnapshotArtifactCursor strictly decodes one complete persisted cursor.
// The returned cursor owns its variable-length state and may be retained.
func OpenSnapshotArtifactCursor(src []byte) (*SnapshotArtifactCursor, error) {
	if len(src) < snapshotArtifactCursorFixedBytes+sha256.Size ||
		len(src) > maxSnapshotArtifactCursorBytes {
		return nil, fmt.Errorf("%w: cursor length", ErrSnapshotArtifact)
	}
	if !bytes.Equal(src[0:8], snapshotArtifactCursorMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != snapshotArtifactCursorFormat ||
		binary.LittleEndian.Uint16(src[10:12]) != snapshotArtifactCursorFixedBytes ||
		binary.LittleEndian.Uint32(src[12:16]) != uint32(len(src)) ||
		src[29] > 1 || !allZero(src[30:32]) {
		return nil, fmt.Errorf("%w: cursor header", ErrSnapshotArtifact)
	}
	stateBytes := uint64(binary.LittleEndian.Uint32(src[16:20]))
	nameBytes := uint64(binary.LittleEndian.Uint16(src[20:22]))
	previousKeyBytes := uint64(binary.LittleEndian.Uint16(src[22:24]))
	if stateBytes == 0 || stateBytes > MaxStateEnvelopeBytes ||
		nameBytes == 0 || nameBytes > replication.MaxCollectionBytes ||
		previousKeyBytes > replication.MaxMutationKeyBytes ||
		uint64(snapshotArtifactCursorFixedBytes)+stateBytes+nameBytes+sha256.Size != uint64(len(src)) ||
		!allZero(src[152+int(previousKeyBytes):408]) {
		return nil, fmt.Errorf("%w: cursor bounds", ErrSnapshotArtifactBound)
	}
	wantDigest := snapshotArtifactDigest(snapshotArtifactCursorDomain, src[:len(src)-sha256.Size])
	var storedDigest [sha256.Size]byte
	copy(storedDigest[:], src[len(src)-sha256.Size:])
	if storedDigest != wantDigest {
		return nil, fmt.Errorf("%w: cursor digest", ErrSnapshotArtifact)
	}
	stateEnd := snapshotArtifactCursorFixedBytes + int(stateBytes)
	stateEnvelope := src[snapshotArtifactCursorFixedBytes:stateEnd]
	state, err := OpenState(stateEnvelope)
	if err != nil {
		return nil, fmt.Errorf("%w: cursor state: %v", ErrSnapshotArtifact, err)
	}
	name := bytes.Clone(src[stateEnd : stateEnd+int(nameBytes)])
	cursor := &SnapshotArtifactCursor{
		manifest: SnapshotArtifactManifest{
			State: state, UserCollection: name,
			TargetChunkBytes: binary.LittleEndian.Uint32(src[24:28]),
			Chunks:           binary.LittleEndian.Uint64(src[48:56]),
			SystemRows:       binary.LittleEndian.Uint64(src[56:64]),
			UserRows:         binary.LittleEndian.Uint64(src[64:72]),
			PayloadBytes:     binary.LittleEndian.Uint64(src[72:80]),
			CaptureRows:      binary.LittleEndian.Uint64(src[80:88]),
		},
		expectedStateDocument: bytes.Clone(stateEnvelope),
		nextSequence:          binary.LittleEndian.Uint64(src[32:40]),
		encodedBytes:          binary.LittleEndian.Uint64(src[40:48]),
		previousKeyBytes:      uint16(previousKeyBytes),
		currentCollection:     SnapshotArtifactCollection(src[28]),
		stateRowSeen:          src[29] == 1,
	}
	copy(cursor.manifest.HeaderDigest[:], src[88:120])
	copy(cursor.manifest.LastChunkDigest[:], src[120:152])
	copy(cursor.previousDigest[:], src[120:152])
	copy(cursor.previousKey[:], src[152:152+int(previousKeyBytes)])
	copy(cursor.captureImageDigest[:], src[408:440])
	if err := validateSnapshotArtifactCursor(cursor); err != nil {
		return nil, err
	}
	return cursor, nil
}
