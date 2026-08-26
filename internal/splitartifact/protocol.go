// Package splitartifact provides the authenticated, bounded data plane used to
// stream immutable range-split child artifacts between shard processes.
package splitartifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/internal/rangesplit"
)

const (
	IdentityBytes = 112
	RequestBytes  = 136
	ResponseBytes = 72

	MinChunkBytes         = 4 << 10
	AbsoluteMaxChunkBytes = 8 << 20
	AbsoluteMaxReconnects = 64
)

var (
	ErrProtocol     = errors.New("splitartifact: invalid protocol")
	ErrUnauthorized = errors.New("splitartifact: unauthorized")
	ErrBound        = errors.New("splitartifact: resource bound exceeded")
	ErrChunk        = errors.New("splitartifact: invalid chunk")
	ErrSource       = errors.New("splitartifact: source failure")
)

var (
	requestMagic  = [8]byte{'V', 'B', 'S', 'P', 'R', 'E', 'Q', 0}
	responseMagic = [8]byte{'V', 'B', 'S', 'P', 'R', 'E', 'S', 0}
)

const (
	responseChunk    byte = 1
	responseComplete byte = 2
)

// Identity binds every byte request to one durable split operation, exact
// plan, child ordinal, and certified immutable artifact.
type Identity struct {
	Operation      [32]byte
	PlanDigest     [sha256.Size]byte
	Child          uint8
	ArtifactDigest [sha256.Size]byte
	ArtifactBytes  uint64
}

func NewIdentity(operation [32]byte, manifest rangesplit.ChildArtifactManifest) (Identity, error) {
	identity := Identity{
		Operation: operation, PlanDigest: manifest.PlanDigest, Child: manifest.Child,
		ArtifactDigest: manifest.Digest, ArtifactBytes: manifest.EncodedBytes,
	}
	if !identity.Valid() || !manifest.Present {
		return Identity{}, ErrProtocol
	}
	return identity, nil
}

func (identity Identity) Valid() bool {
	return identity.Operation != ([32]byte{}) && identity.PlanDigest != ([32]byte{}) &&
		identity.ArtifactDigest != ([32]byte{}) && identity.ArtifactBytes != 0 &&
		identity.ArtifactBytes <= math.MaxInt64
}

type request struct {
	Identity   Identity
	Offset     uint64
	ChunkBytes uint32
}

func appendIdentity(dst []byte, identity Identity) ([]byte, error) {
	if !identity.Valid() || len(dst) > math.MaxInt-IdentityBytes {
		return dst, ErrProtocol
	}
	start := len(dst)
	dst = append(dst, make([]byte, IdentityBytes)...)
	raw := dst[start:]
	copy(raw[0:32], identity.Operation[:])
	copy(raw[32:64], identity.PlanDigest[:])
	raw[64] = identity.Child
	copy(raw[72:104], identity.ArtifactDigest[:])
	binary.BigEndian.PutUint64(raw[104:112], identity.ArtifactBytes)
	return dst, nil
}

func openIdentity(raw []byte) (Identity, error) {
	if len(raw) != IdentityBytes || !allZero(raw[65:72]) {
		return Identity{}, ErrProtocol
	}
	var identity Identity
	copy(identity.Operation[:], raw[0:32])
	copy(identity.PlanDigest[:], raw[32:64])
	identity.Child = raw[64]
	copy(identity.ArtifactDigest[:], raw[72:104])
	identity.ArtifactBytes = binary.BigEndian.Uint64(raw[104:112])
	if !identity.Valid() {
		return Identity{}, ErrProtocol
	}
	return identity, nil
}

func appendRequest(dst []byte, value request) ([]byte, error) {
	if !value.Identity.Valid() || value.Offset > value.Identity.ArtifactBytes ||
		value.ChunkBytes < MinChunkBytes || value.ChunkBytes > AbsoluteMaxChunkBytes ||
		len(dst) > math.MaxInt-RequestBytes {
		return dst, ErrProtocol
	}
	start := len(dst)
	dst = append(dst, make([]byte, RequestBytes)...)
	raw := dst[start:]
	copy(raw[:8], requestMagic[:])
	if _, err := appendIdentity(raw[8:8], value.Identity); err != nil {
		return dst[:start], err
	}
	binary.BigEndian.PutUint64(raw[120:128], value.Offset)
	binary.BigEndian.PutUint32(raw[128:132], value.ChunkBytes)
	return dst, nil
}

func openRequest(raw []byte) (request, error) {
	if len(raw) != RequestBytes || !bytes.Equal(raw[:8], requestMagic[:]) ||
		!allZero(raw[132:136]) {
		return request{}, ErrProtocol
	}
	identity, err := openIdentity(raw[8:120])
	value := request{Identity: identity, Offset: binary.BigEndian.Uint64(raw[120:128]),
		ChunkBytes: binary.BigEndian.Uint32(raw[128:132])}
	if err != nil || value.Offset > identity.ArtifactBytes ||
		value.ChunkBytes < MinChunkBytes || value.ChunkBytes > AbsoluteMaxChunkBytes {
		return request{}, errors.Join(ErrProtocol, err)
	}
	return value, nil
}

func allZero(raw []byte) bool {
	for _, value := range raw {
		if value != 0 {
			return false
		}
	}
	return true
}
