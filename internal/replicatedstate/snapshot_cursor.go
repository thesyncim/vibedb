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
	snapshotArtifactCursorFormat              = uint16(1)
	snapshotArtifactCursorFixedBytes          = 440
	snapshotArtifactCursorRelationHeaderBytes = 40
	snapshotArtifactCursorRelationFixedBytes  = 48
	maxSnapshotArtifactCursorBytes            = snapshotArtifactCursorFixedBytes +
		MaxStateEnvelopeBytes + replication.MaxCollectionBytes + sha256.Size +
		snapshotArtifactCursorRelationHeaderBytes +
		replication.MaxRelationsPerBundle*(snapshotArtifactCursorRelationFixedBytes+replication.MaxIdentityBytes)
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
	relationBytes := 0
	if cursor.manifest.Bundle {
		relationBytes = snapshotArtifactCursorRelationHeaderBytes
		for i := range cursor.manifest.Relations {
			relationBytes += snapshotArtifactCursorRelationFixedBytes +
				len(cursor.manifest.Relations[i].Collection)
		}
	}
	total := snapshotArtifactCursorFixedBytes + len(stateEnvelope) +
		len(cursor.manifest.UserCollection) + relationBytes + sha256.Size
	if total > maxSnapshotArtifactCursorBytes || uint64(total) > math.MaxUint32 {
		return dst, fmt.Errorf("%w: cursor bytes", ErrSnapshotArtifactBound)
	}
	region := writableAppendRegion(dst, total)
	if byteSlicesOverlap(region, cursor.manifest.UserCollection) {
		return dst, ErrCodecAlias
	}
	for i := range cursor.manifest.Relations {
		if byteSlicesOverlap(region, cursor.manifest.Relations[i].Collection) {
			return dst, ErrCodecAlias
		}
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
	binary.LittleEndian.PutUint16(frame[30:32], uint16(cursor.nextRelation))
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
	at += copy(frame[at:], cursor.manifest.UserCollection)
	if cursor.manifest.Bundle {
		copy(frame[at:at+sha256.Size], cursor.manifest.RelationManifestDigest[:])
		binary.LittleEndian.PutUint16(frame[at+32:at+34], uint16(len(cursor.manifest.Relations)))
		binary.LittleEndian.PutUint32(frame[at+36:at+40], uint32(relationBytes))
		at += snapshotArtifactCursorRelationHeaderBytes
		for i := range cursor.manifest.Relations {
			relation := &cursor.manifest.Relations[i]
			binary.LittleEndian.PutUint16(frame[at:at+2], uint16(relation.Relation))
			frame[at+2] = byte(relation.Kind)
			binary.LittleEndian.PutUint16(frame[at+4:at+6], uint16(len(relation.Collection)))
			binary.LittleEndian.PutUint64(frame[at+8:at+16], relation.Rows)
			copy(frame[at+16:at+48], relation.ImageDigest[:])
			at += snapshotArtifactCursorRelationFixedBytes
			at += copy(frame[at:], relation.Collection)
		}
	}
	if at != len(frame)-sha256.Size {
		return dst[:start], fmt.Errorf("%w: cursor relation geometry", ErrSnapshotArtifact)
	}
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
		src[29] > 1 {
		return nil, fmt.Errorf("%w: cursor header", ErrSnapshotArtifact)
	}
	stateBytes := uint64(binary.LittleEndian.Uint32(src[16:20]))
	nameBytes := uint64(binary.LittleEndian.Uint16(src[20:22]))
	previousKeyBytes := uint64(binary.LittleEndian.Uint16(src[22:24]))
	nextRelation := replication.RelationID(binary.LittleEndian.Uint16(src[30:32]))
	minimum := uint64(snapshotArtifactCursorFixedBytes) + stateBytes + nameBytes + sha256.Size
	if stateBytes == 0 || stateBytes > MaxStateEnvelopeBytes ||
		nameBytes == 0 || nameBytes > replication.MaxCollectionBytes ||
		previousKeyBytes > replication.MaxMutationKeyBytes ||
		minimum > uint64(len(src)) ||
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
	relationStart := stateEnd + int(nameBytes)
	relationEnd := len(src) - sha256.Size
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
		nextRelation:          nextRelation,
	}
	copy(cursor.manifest.HeaderDigest[:], src[88:120])
	copy(cursor.manifest.LastChunkDigest[:], src[120:152])
	copy(cursor.previousDigest[:], src[120:152])
	copy(cursor.previousKey[:], src[152:152+int(previousKeyBytes)])
	copy(cursor.captureImageDigest[:], src[408:440])
	if cursor.currentCollection != SnapshotArtifactSystem ||
		cursor.previousKeyBytes != 0 && cursor.previousKey[0] >= routeGateHeadPrefix {
		baseRows, ok := stateSystemRowCount(cursor.manifest.State)
		if !ok || cursor.manifest.SystemRows < baseRows {
			return nil, fmt.Errorf("%w: cursor route-gate rows", ErrSnapshotArtifact)
		}
		cursor.routeGateRows = cursor.manifest.SystemRows - baseRows
	}
	if nextRelation == 0 {
		if relationStart != relationEnd {
			return nil, fmt.Errorf("%w: singleton cursor geometry", ErrSnapshotArtifact)
		}
	} else {
		if relationEnd-relationStart < snapshotArtifactCursorRelationHeaderBytes {
			return nil, fmt.Errorf("%w: cursor relation header", ErrSnapshotArtifact)
		}
		header := src[relationStart : relationStart+snapshotArtifactCursorRelationHeaderBytes]
		count := int(binary.LittleEndian.Uint16(header[32:34]))
		relationBytes := int(binary.LittleEndian.Uint32(header[36:40]))
		if count < 2 || count > replication.MaxRelationsPerBundle ||
			!allZero(header[34:36]) || relationBytes != relationEnd-relationStart ||
			relationBytes < snapshotArtifactCursorRelationHeaderBytes+
				count*(snapshotArtifactCursorRelationFixedBytes+1) ||
			relationBytes > snapshotArtifactCursorRelationHeaderBytes+
				count*(snapshotArtifactCursorRelationFixedBytes+replication.MaxIdentityBytes) {
			return nil, fmt.Errorf("%w: cursor relation bounds", ErrSnapshotArtifactBound)
		}
		cursor.manifest.Bundle = true
		copy(cursor.manifest.RelationManifestDigest[:], header[:sha256.Size])
		cursor.manifest.Relations = make([]SnapshotArtifactRelation, count)
		at := relationStart + snapshotArtifactCursorRelationHeaderBytes
		for i := range cursor.manifest.Relations {
			if at > relationEnd-snapshotArtifactCursorRelationFixedBytes {
				return nil, fmt.Errorf("%w: cursor relation", ErrSnapshotArtifact)
			}
			fixed := src[at : at+snapshotArtifactCursorRelationFixedBytes]
			nameBytes := int(binary.LittleEndian.Uint16(fixed[4:6]))
			if fixed[3] != 0 || binary.LittleEndian.Uint16(fixed[6:8]) != 0 ||
				nameBytes == 0 || nameBytes > replication.MaxIdentityBytes ||
				at+snapshotArtifactCursorRelationFixedBytes > relationEnd-nameBytes {
				return nil, fmt.Errorf("%w: cursor relation identity", ErrSnapshotArtifact)
			}
			relation := &cursor.manifest.Relations[i]
			relation.Relation = replication.RelationID(binary.LittleEndian.Uint16(fixed[0:2]))
			relation.Kind = RelationKind(fixed[2])
			relation.Rows = binary.LittleEndian.Uint64(fixed[8:16])
			copy(relation.ImageDigest[:], fixed[16:48])
			at += snapshotArtifactCursorRelationFixedBytes
			relation.Collection = bytes.Clone(src[at : at+nameBytes])
			at += nameBytes
		}
		if at != relationEnd {
			return nil, fmt.Errorf("%w: cursor trailing relation", ErrSnapshotArtifact)
		}
	}
	if err := validateSnapshotArtifactCursor(cursor); err != nil {
		return nil, err
	}
	return cursor, nil
}
