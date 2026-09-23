package raftstore

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"slices"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
)

const (
	nodeDescriptorGroup = ^uint64(0) - 1
	nodeDescriptorFixed = 88
	nodeIdentityBytes   = 128
)

// nodeStoreBounds is the complete immutable memory and disk geometry of a
// node log. Every value which sizes a fixed hot-path arena or an engine index
// is authenticated in NODEMETA and must match exactly on reopen.
type nodeStoreBounds struct {
	segmentBytes, maxWaveBytes, maxSegmentEvents, recentWaves uint64
	maxEntriesPerGroup, readerSlots, maxGroups                uint64
}

func validateNodeIdentity(identity NodeIdentity) error {
	if identity.ClusterID == ([16]byte{}) || identity.ClusterIncarnation == ([16]byte{}) || identity.NodeID == ([16]byte{}) {
		return ErrInvalid
	}
	return nil
}

func marshalNodeIdentity(identity NodeIdentity, bounds nodeStoreBounds) ([]byte, error) {
	if validateNodeIdentity(identity) != nil || !validNodeStoreBounds(bounds) {
		return nil, ErrInvalid
	}
	b := make([]byte, nodeIdentityBytes)
	binary.LittleEndian.PutUint16(b[0:2], 1)
	copy(b[8:24], identity.ClusterID[:])
	copy(b[24:40], identity.ClusterIncarnation[:])
	copy(b[40:56], identity.NodeID[:])
	binary.LittleEndian.PutUint64(b[56:64], bounds.segmentBytes)
	binary.LittleEndian.PutUint64(b[64:72], bounds.maxWaveBytes)
	binary.LittleEndian.PutUint64(b[72:80], bounds.maxSegmentEvents)
	binary.LittleEndian.PutUint64(b[80:88], bounds.recentWaves)
	binary.LittleEndian.PutUint64(b[88:96], bounds.maxEntriesPerGroup)
	binary.LittleEndian.PutUint64(b[96:104], bounds.readerSlots)
	binary.LittleEndian.PutUint64(b[104:112], bounds.maxGroups)
	binary.LittleEndian.PutUint32(b[112:116], crc32.Checksum(b[:112], crcTable))
	return b, nil
}

func unmarshalNodeIdentity(b []byte) (NodeIdentity, nodeStoreBounds, error) {
	if len(b) != nodeIdentityBytes || binary.LittleEndian.Uint16(b[0:2]) != 1 || !allZero(b[2:8]) || !allZero(b[116:128]) || binary.LittleEndian.Uint32(b[112:116]) != crc32.Checksum(b[:112], crcTable) {
		return NodeIdentity{}, nodeStoreBounds{}, ErrCorrupt
	}
	var identity NodeIdentity
	copy(identity.ClusterID[:], b[8:24])
	copy(identity.ClusterIncarnation[:], b[24:40])
	copy(identity.NodeID[:], b[40:56])
	bounds := nodeStoreBounds{
		segmentBytes: binary.LittleEndian.Uint64(b[56:64]), maxWaveBytes: binary.LittleEndian.Uint64(b[64:72]),
		maxSegmentEvents: binary.LittleEndian.Uint64(b[72:80]), recentWaves: binary.LittleEndian.Uint64(b[80:88]),
		maxEntriesPerGroup: binary.LittleEndian.Uint64(b[88:96]), readerSlots: binary.LittleEndian.Uint64(b[96:104]),
		maxGroups: binary.LittleEndian.Uint64(b[104:112]),
	}
	if validateNodeIdentity(identity) != nil || !validNodeStoreBounds(bounds) {
		return NodeIdentity{}, nodeStoreBounds{}, ErrCorrupt
	}
	return identity, bounds, nil
}

func validNodeStoreBounds(bounds nodeStoreBounds) bool {
	return bounds.segmentBytes >= 1<<20 && bounds.segmentBytes < 1<<32 &&
		bounds.maxWaveBytes >= 72 && bounds.maxWaveBytes <= seglog.MaximumWaveBytes &&
		bounds.maxSegmentEvents > 0 && bounds.maxSegmentEvents <= AbsoluteMaxEntries &&
		bounds.recentWaves > 0 && bounds.recentWaves <= AbsoluteMaxRecords &&
		bounds.maxEntriesPerGroup > 0 && bounds.maxEntriesPerGroup <= MaxReadyEntries &&
		bounds.readerSlots <= math.MaxUint32 && bounds.maxGroups > 0 && bounds.maxGroups <= math.MaxUint32
}

func validateGroupDescriptor(descriptor GroupDescriptor, allowZeroKey bool) error {
	if (!allowZeroKey && descriptor.LogKey == 0) || descriptor.LogKey >= nodeDescriptorGroup || descriptor.TopologyRecoveryEpoch == 0 ||
		descriptor.AllocationGeneration == 0 || descriptor.MemberID == 0 || descriptor.GroupID == ([16]byte{}) ||
		descriptor.ShardIncarnation == ([16]byte{}) || descriptor.StoreID == ([16]byte{}) ||
		len(descriptor.Distribution) == 0 || len(descriptor.Distribution) > MaxIdentityComponentBytes || !utf8.ValidString(descriptor.Distribution) || bytes.IndexByte([]byte(descriptor.Distribution), 0) >= 0 ||
		len(descriptor.Shard) == 0 || len(descriptor.Shard) > MaxIdentityComponentBytes || !utf8.ValidString(descriptor.Shard) || bytes.IndexByte([]byte(descriptor.Shard), 0) >= 0 {
		return ErrInvalid
	}
	return nil
}

func (d GroupDescriptor) Identity(node NodeIdentity) Identity {
	return Identity{ClusterID: node.ClusterID, ClusterIncarnation: node.ClusterIncarnation, Distribution: d.Distribution, Shard: d.Shard, AllocationGeneration: d.AllocationGeneration, ShardIncarnation: d.ShardIncarnation, GroupID: d.GroupID, MemberID: d.MemberID, StoreID: d.StoreID}
}

func appendGroupDescriptor(dst []byte, d GroupDescriptor) ([]byte, error) {
	if validateGroupDescriptor(d, false) != nil {
		return dst, ErrInvalid
	}
	if nodeDescriptorFixed+len(d.Distribution)+len(d.Shard) > cap(dst)-len(dst) {
		return dst, ErrBounds
	}
	start := len(dst)
	dst = append(dst, make([]byte, nodeDescriptorFixed+len(d.Distribution)+len(d.Shard))...)
	b := dst[start:]
	binary.LittleEndian.PutUint64(b[0:8], d.LogKey)
	binary.LittleEndian.PutUint64(b[8:16], d.TopologyRecoveryEpoch)
	binary.LittleEndian.PutUint64(b[16:24], d.AllocationGeneration)
	binary.LittleEndian.PutUint64(b[24:32], d.MemberID)
	copy(b[32:48], d.GroupID[:])
	copy(b[48:64], d.ShardIncarnation[:])
	copy(b[64:80], d.StoreID[:])
	binary.LittleEndian.PutUint16(b[80:82], uint16(len(d.Distribution)))
	binary.LittleEndian.PutUint16(b[82:84], uint16(len(d.Shard)))
	copy(b[84:], d.Distribution)
	copy(b[84+len(d.Distribution):], d.Shard)
	binary.LittleEndian.PutUint32(b[len(b)-4:], crc32.Checksum(b[:len(b)-4], crcTable))
	return dst, nil
}

func decodeGroupDescriptor(data []byte) (GroupDescriptor, error) {
	if len(data) < nodeDescriptorFixed {
		return GroupDescriptor{}, ErrCorrupt
	}
	dl, sl := int(binary.LittleEndian.Uint16(data[80:82])), int(binary.LittleEndian.Uint16(data[82:84]))
	if len(data) != nodeDescriptorFixed+dl+sl || binary.LittleEndian.Uint32(data[len(data)-4:]) != crc32.Checksum(data[:len(data)-4], crcTable) {
		return GroupDescriptor{}, ErrCorrupt
	}
	d := GroupDescriptor{LogKey: binary.LittleEndian.Uint64(data[0:8]), TopologyRecoveryEpoch: binary.LittleEndian.Uint64(data[8:16]), AllocationGeneration: binary.LittleEndian.Uint64(data[16:24]), MemberID: binary.LittleEndian.Uint64(data[24:32])}
	copy(d.GroupID[:], data[32:48])
	copy(d.ShardIncarnation[:], data[48:64])
	copy(d.StoreID[:], data[64:80])
	d.Distribution, d.Shard = string(data[84:84+dl]), string(data[84+dl:84+dl+sl])
	if validateGroupDescriptor(d, false) != nil {
		return GroupDescriptor{}, ErrCorrupt
	}
	return d, nil
}

func canonicalInitialDescriptors(bootstraps []NodeBootstrap) ([]NodeBootstrap, []GroupDescriptor, []uint32, error) {
	// An empty node log is a valid capacity reservation.  It contains only the
	// descriptor group until the controller commits the first group enrollment;
	// no Raft or SQL authority is implied by the reservation.  Dynamic nodes
	// use this form while they wait for an exact replicated enrollment intent.
	if uint64(len(bootstraps)) > uint64(math.MaxUint32) {
		return nil, nil, nil, ErrInvalid
	}
	boots := slices.Clone(bootstraps)
	slices.SortFunc(boots, func(a, b NodeBootstrap) int { return bytes.Compare(a.Descriptor.GroupID[:], b.Descriptor.GroupID[:]) })
	descriptors := make([]GroupDescriptor, len(boots))
	order := make([]uint32, len(boots))
	for i := range boots {
		d := boots[i].Descriptor
		if d.LogKey != 0 || validateGroupDescriptor(d, true) != nil || i > 0 && bytes.Compare(boots[i-1].Descriptor.GroupID[:], d.GroupID[:]) >= 0 {
			return nil, nil, nil, ErrInvalid
		}
		d.LogKey = uint64(i + 1)
		boots[i].Descriptor, descriptors[i], order[i] = d, d, uint32(i)
	}
	return boots, descriptors, order, nil
}
