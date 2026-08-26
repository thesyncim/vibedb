package membershipgrant

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

var ErrCodec = errors.New("membershipgrant: invalid canonical grant")

const CanonicalGrantBytes = 240

var canonicalGrantMagic = [8]byte{'V', 'B', 'M', 'G', 'R', 'A', 'N', 'T'}

// AppendCanonical appends the sole fixed-width representation of Grant.
func AppendCanonical(dst []byte, grant Grant) ([]byte, error) {
	if !grant.Valid() || len(dst) > math.MaxInt-CanonicalGrantBytes {
		return dst, ErrCodec
	}
	start := len(dst)
	dst = append(dst, make([]byte, CanonicalGrantBytes)...)
	raw := dst[start:]
	copy(raw[:8], canonicalGrantMagic[:])
	offset := 8
	for _, value := range [...][16]byte{
		grant.Group.ClusterID, grant.Group.ClusterIncarnation,
	} {
		copy(raw[offset:offset+16], value[:])
		offset += 16
	}
	binary.LittleEndian.PutUint64(raw[offset:offset+8], grant.Group.TopologyRecoveryEpoch)
	offset += 8
	for _, value := range [...][16]byte{
		grant.Group.ShardIncarnation, grant.Group.GroupID, grant.TransitionID,
	} {
		copy(raw[offset:offset+16], value[:])
		offset += 16
	}
	for _, value := range [...]uint64{
		grant.MetadataEpoch, grant.CatalogGeneration, grant.InitialReplicaSetVersion,
		grant.InitialVoters[0], grant.InitialVoters[1], grant.InitialVoters[2],
	} {
		binary.LittleEndian.PutUint64(raw[offset:offset+8], value)
		offset += 8
	}
	copy(raw[offset:offset+32], grant.InitialRosterDigest[:])
	offset += 32
	copy(raw[offset:offset+32], grant.InitialDescriptorDigest[:])
	offset += 32
	for _, value := range [...]uint64{grant.SourceMember, grant.TargetMember} {
		binary.LittleEndian.PutUint64(raw[offset:offset+8], value)
		offset += 8
	}
	copy(raw[offset:offset+16], grant.TargetNode[:])
	return dst, nil
}

// OpenCanonical accepts exactly one complete canonical Grant and borrows no
// storage from raw.
func OpenCanonical(raw []byte) (Grant, error) {
	if len(raw) != CanonicalGrantBytes || !bytes.Equal(raw[:8], canonicalGrantMagic[:]) {
		return Grant{}, ErrCodec
	}
	var grant Grant
	offset := 8
	for _, destination := range []*[16]byte{
		&grant.Group.ClusterID, &grant.Group.ClusterIncarnation,
	} {
		copy(destination[:], raw[offset:offset+16])
		offset += 16
	}
	grant.Group.TopologyRecoveryEpoch = binary.LittleEndian.Uint64(raw[offset : offset+8])
	offset += 8
	for _, destination := range []*[16]byte{
		&grant.Group.ShardIncarnation, &grant.Group.GroupID, &grant.TransitionID,
	} {
		copy(destination[:], raw[offset:offset+16])
		offset += 16
	}
	values := []*uint64{
		&grant.MetadataEpoch, &grant.CatalogGeneration, &grant.InitialReplicaSetVersion,
		&grant.InitialVoters[0], &grant.InitialVoters[1], &grant.InitialVoters[2],
	}
	for _, destination := range values {
		*destination = binary.LittleEndian.Uint64(raw[offset : offset+8])
		offset += 8
	}
	copy(grant.InitialRosterDigest[:], raw[offset:offset+32])
	offset += 32
	copy(grant.InitialDescriptorDigest[:], raw[offset:offset+32])
	offset += 32
	grant.SourceMember = binary.LittleEndian.Uint64(raw[offset : offset+8])
	offset += 8
	grant.TargetMember = binary.LittleEndian.Uint64(raw[offset : offset+8])
	offset += 8
	copy(grant.TargetNode[:], raw[offset:offset+16])
	if !grant.Valid() {
		return Grant{}, ErrCodec
	}
	return grant, nil
}
