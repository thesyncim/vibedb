// Package snapshottransfer provides the bounded, non-activating transport and
// crash-safe repository for certified replicated-state snapshot artifacts.
package snapshottransfer

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

const (
	DescriptorBytes       = 232
	AbsoluteMaxChunkBytes = 8 << 20
	MinChunkBytes         = 4 << 10
)

var (
	ErrDescriptor     = errors.New("snapshottransfer: invalid descriptor")
	ErrBound          = errors.New("snapshottransfer: resource bound exceeded")
	ErrStaleFence     = errors.New("snapshottransfer: stale snapshot fence")
	ErrChunk          = errors.New("snapshottransfer: invalid chunk")
	ErrRepository     = errors.New("snapshottransfer: repository failure")
	ErrOutcomeUnknown = errors.New("snapshottransfer: durability outcome unknown")
	ErrArtifactBusy   = errors.New("snapshottransfer: artifact has active readers")
)

var descriptorMagic = [8]byte{'V', 'B', 'S', 'N', 'A', 'P', 0, 0}

// Descriptor is the complete immutable identity of one transfer artifact.
// It is fixed-width so an untrusted peer cannot influence allocation while it
// is being authenticated and fenced.
type Descriptor struct {
	Group             raftmember.GroupKey
	SourceMember      uint64
	TargetMember      uint64
	TargetStore       [16]byte
	TargetIncarnation uint64
	SchemaGeneration  uint64
	ReplicaSetVersion uint64
	SnapshotIndex     uint64
	SnapshotTerm      uint64
	Lineage           [sha256.Size]byte
	ArtifactHash      [sha256.Size]byte
	ArtifactBytes     uint64
	ChunkBytes        uint32
}

func (d Descriptor) Valid() bool {
	return d.Group.ClusterID != ([16]byte{}) &&
		d.Group.ClusterIncarnation != ([16]byte{}) &&
		d.Group.TopologyRecoveryEpoch != 0 &&
		d.Group.ShardIncarnation != ([16]byte{}) && d.Group.GroupID != ([16]byte{}) &&
		d.SourceMember != 0 && d.TargetMember != 0 &&
		d.SourceMember != d.TargetMember && d.TargetStore != ([16]byte{}) &&
		d.TargetIncarnation != 0 && d.SchemaGeneration != 0 &&
		d.ReplicaSetVersion != 0 && d.SnapshotIndex != 0 && d.SnapshotTerm != 0 &&
		d.Lineage != ([sha256.Size]byte{}) && d.ArtifactHash != ([sha256.Size]byte{}) &&
		d.ArtifactBytes != 0 && d.ChunkBytes >= MinChunkBytes &&
		d.ChunkBytes <= AbsoluteMaxChunkBytes
}

// AppendDescriptor appends the sole canonical descriptor grammar.
func AppendDescriptor(dst []byte, d Descriptor) ([]byte, error) {
	if !d.Valid() || len(dst) > math.MaxInt-DescriptorBytes {
		return dst, ErrDescriptor
	}
	start := len(dst)
	dst = append(dst, make([]byte, DescriptorBytes)...)
	b := dst[start:]
	copy(b[:8], descriptorMagic[:])
	copy(b[8:24], d.Group.ClusterID[:])
	copy(b[24:40], d.Group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(b[40:48], d.Group.TopologyRecoveryEpoch)
	copy(b[48:64], d.Group.ShardIncarnation[:])
	copy(b[64:80], d.Group.GroupID[:])
	binary.BigEndian.PutUint64(b[80:88], d.SourceMember)
	binary.BigEndian.PutUint64(b[88:96], d.TargetMember)
	copy(b[96:112], d.TargetStore[:])
	binary.BigEndian.PutUint64(b[112:120], d.TargetIncarnation)
	binary.BigEndian.PutUint64(b[120:128], d.SchemaGeneration)
	binary.BigEndian.PutUint64(b[128:136], d.ReplicaSetVersion)
	binary.BigEndian.PutUint64(b[136:144], d.SnapshotIndex)
	binary.BigEndian.PutUint64(b[144:152], d.SnapshotTerm)
	copy(b[152:184], d.Lineage[:])
	copy(b[184:216], d.ArtifactHash[:])
	binary.BigEndian.PutUint64(b[216:224], d.ArtifactBytes)
	binary.BigEndian.PutUint32(b[224:228], d.ChunkBytes)
	// 228:232 is the required zero canonical tail.
	return dst, nil
}

func OpenDescriptor(raw []byte) (Descriptor, error) {
	if len(raw) != DescriptorBytes || !bytes.Equal(raw[:8], descriptorMagic[:]) ||
		binary.BigEndian.Uint32(raw[228:232]) != 0 {
		return Descriptor{}, ErrDescriptor
	}
	var d Descriptor
	copy(d.Group.ClusterID[:], raw[8:24])
	copy(d.Group.ClusterIncarnation[:], raw[24:40])
	d.Group.TopologyRecoveryEpoch = binary.BigEndian.Uint64(raw[40:48])
	copy(d.Group.ShardIncarnation[:], raw[48:64])
	copy(d.Group.GroupID[:], raw[64:80])
	d.SourceMember = binary.BigEndian.Uint64(raw[80:88])
	d.TargetMember = binary.BigEndian.Uint64(raw[88:96])
	copy(d.TargetStore[:], raw[96:112])
	d.TargetIncarnation = binary.BigEndian.Uint64(raw[112:120])
	d.SchemaGeneration = binary.BigEndian.Uint64(raw[120:128])
	d.ReplicaSetVersion = binary.BigEndian.Uint64(raw[128:136])
	d.SnapshotIndex = binary.BigEndian.Uint64(raw[136:144])
	d.SnapshotTerm = binary.BigEndian.Uint64(raw[144:152])
	copy(d.Lineage[:], raw[152:184])
	copy(d.ArtifactHash[:], raw[184:216])
	d.ArtifactBytes = binary.BigEndian.Uint64(raw[216:224])
	d.ChunkBytes = binary.BigEndian.Uint32(raw[224:228])
	if !d.Valid() {
		return Descriptor{}, ErrDescriptor
	}
	return d, nil
}
