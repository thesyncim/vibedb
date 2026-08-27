// Package clusterbackup defines the catalog-authorized, non-serving boundary
// between per-group certified snapshot artifacts and a future live backup
// controller. It cannot mint replica membership or serving authority.
package clusterbackup

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

const (
	HeaderBytes                 = 96
	GroupCutBytes               = 248
	TrailerBytes                = sha256.Size
	AbsoluteMaxCertificateBytes = 16 << 20
	AbsoluteMaxGroupCuts        = (AbsoluteMaxCertificateBytes - HeaderBytes - TrailerBytes) / GroupCutBytes
)

var (
	ErrCertificate      = errors.New("clusterbackup: invalid certificate")
	ErrCatalogCut       = errors.New("clusterbackup: catalog cut mismatch")
	ErrArtifactEvidence = errors.New("clusterbackup: artifact evidence mismatch")
	certificateMagic    = [8]byte{'V', 'B', 'C', 'L', 'U', 'S', 'B', 'K'}
)

// CatalogCut is the exact replicated catalog authority a controller observed.
// Groups is the complete sorted RF3 inventory, not a caller-selected subset.
type CatalogCut struct {
	Generation       uint64
	Digest           [sha256.Size]byte
	PolicyGeneration uint64
	Groups           []raftmember.GroupKey
}

// GroupCut binds one coherent artifact to its source Raft publication without
// carrying a target member/store identity from the learner-move protocol.
type GroupCut struct {
	Group                  raftmember.GroupKey
	SourceMember           uint64
	SchemaGeneration       uint64
	ReplicaSetVersion      uint64
	SnapshotIndex          uint64
	SnapshotTerm           uint64
	Lineage                [sha256.Size]byte
	RelationManifestDigest [sha256.Size]byte
	ArtifactHash           [sha256.Size]byte
	ArtifactBytes          uint64
	ArtifactManifestDigest [sha256.Size]byte
}

type Certificate struct {
	Operation         [sha256.Size]byte
	CatalogGeneration uint64
	CatalogDigest     [sha256.Size]byte
	PolicyGeneration  uint64
	Groups            []GroupCut
	Digest            [sha256.Size]byte
}

// GroupCutFromVerifiedArtifact converts the manifest returned by the ordinary
// complete artifact verifier into backup evidence. It does not accept a
// transfer Descriptor because that structure is bound to a learner target.
func GroupCutFromVerifiedArtifact(sourceMember uint64,
	manifest replicatedstate.SnapshotArtifactManifest,
	artifactHash [sha256.Size]byte, artifactBytes uint64,
) (GroupCut, error) {
	binding := manifest.State.Binding
	cut := GroupCut{Group: raftmember.GroupKey{ClusterID: [16]byte(binding.ClusterID),
		ClusterIncarnation:    [16]byte(binding.ClusterIncarnation),
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		ShardIncarnation:      [16]byte(binding.ShardIncarnation), GroupID: [16]byte(binding.GroupID)},
		SourceMember: sourceMember, SchemaGeneration: binding.SchemaGeneration,
		ReplicaSetVersion: manifest.State.ReplicaSetVersion,
		SnapshotIndex:     manifest.State.Applied, SnapshotTerm: manifest.State.LastTerm,
		Lineage: manifest.State.LastEntryDigest, RelationManifestDigest: manifest.RelationManifestDigest,
		ArtifactHash: artifactHash, ArtifactBytes: artifactBytes,
		ArtifactManifestDigest: manifest.Digest}
	if !cut.Valid() || manifest.Seeded || manifest.EncodedBytes != artifactBytes {
		return GroupCut{}, ErrArtifactEvidence
	}
	return cut, nil
}

func validGroup(group raftmember.GroupKey) bool {
	return group.ClusterID != ([16]byte{}) && group.ClusterIncarnation != ([16]byte{}) &&
		group.TopologyRecoveryEpoch != 0 && group.ShardIncarnation != ([16]byte{}) &&
		group.GroupID != ([16]byte{})
}

func (cut GroupCut) Valid() bool {
	return validGroup(cut.Group) && cut.SourceMember != 0 && cut.SchemaGeneration != 0 &&
		cut.ReplicaSetVersion != 0 && cut.SnapshotIndex != 0 && cut.SnapshotTerm != 0 &&
		cut.Lineage != ([sha256.Size]byte{}) &&
		cut.ArtifactHash != ([sha256.Size]byte{}) && cut.ArtifactBytes != 0 &&
		cut.ArtifactManifestDigest != ([sha256.Size]byte{})
}

func compareGroup(left, right raftmember.GroupKey) int {
	var a, b [72]byte
	appendGroup(a[:0], left)
	appendGroup(b[:0], right)
	return bytes.Compare(a[:], b[:])
}

// Certify admits only the complete exact group inventory authorized by one
// catalog cut. It owns cuts and returns their canonical certificate digest.
func Certify(operation [sha256.Size]byte, authority CatalogCut, cuts []GroupCut) (Certificate, error) {
	if operation == ([sha256.Size]byte{}) || authority.Generation == 0 ||
		authority.Digest == ([sha256.Size]byte{}) || authority.PolicyGeneration == 0 ||
		len(cuts) == 0 || len(cuts) != len(authority.Groups) || len(cuts) > AbsoluteMaxGroupCuts {
		return Certificate{}, ErrCatalogCut
	}
	owned := slices.Clone(cuts)
	for index := range owned {
		if !owned[index].Valid() || !validGroup(authority.Groups[index]) ||
			(index != 0 && compareGroup(owned[index-1].Group, owned[index].Group) >= 0) ||
			(index != 0 && compareGroup(authority.Groups[index-1], authority.Groups[index]) >= 0) ||
			owned[index].Group != authority.Groups[index] {
			return Certificate{}, ErrCatalogCut
		}
	}
	certificate := Certificate{Operation: operation, CatalogGeneration: authority.Generation,
		CatalogDigest: authority.Digest, PolicyGeneration: authority.PolicyGeneration, Groups: owned}
	raw, err := AppendCertificate(nil, certificate)
	if err != nil {
		return Certificate{}, err
	}
	certificate.Digest = sha256.Sum256(raw[:len(raw)-TrailerBytes])
	return certificate, nil
}

func AppendCertificate(dst []byte, certificate Certificate) ([]byte, error) {
	count := len(certificate.Groups)
	if certificate.Operation == ([sha256.Size]byte{}) || certificate.CatalogGeneration == 0 ||
		certificate.CatalogDigest == ([sha256.Size]byte{}) || certificate.PolicyGeneration == 0 ||
		count == 0 || count > AbsoluteMaxGroupCuts || len(dst) > math.MaxInt-(HeaderBytes+count*GroupCutBytes+TrailerBytes) {
		return dst, ErrCertificate
	}
	start := len(dst)
	dst = append(dst, make([]byte, HeaderBytes+count*GroupCutBytes+TrailerBytes)...)
	raw := dst[start:]
	copy(raw[:8], certificateMagic[:])
	copy(raw[8:40], certificate.Operation[:])
	binary.BigEndian.PutUint64(raw[40:48], certificate.CatalogGeneration)
	copy(raw[48:80], certificate.CatalogDigest[:])
	binary.BigEndian.PutUint64(raw[80:88], certificate.PolicyGeneration)
	binary.BigEndian.PutUint32(raw[88:92], uint32(count))
	for index, cut := range certificate.Groups {
		if !cut.Valid() || index != 0 && compareGroup(certificate.Groups[index-1].Group, cut.Group) >= 0 {
			return dst[:start], ErrCertificate
		}
		appendGroupCut(raw[HeaderBytes+index*GroupCutBytes:], cut)
	}
	digest := sha256.Sum256(raw[:len(raw)-TrailerBytes])
	copy(raw[len(raw)-TrailerBytes:], digest[:])
	return dst, nil
}

func OpenCertificate(raw []byte) (Certificate, error) {
	if len(raw) < HeaderBytes+GroupCutBytes+TrailerBytes {
		return Certificate{}, ErrCertificate
	}
	count := int(binary.BigEndian.Uint32(raw[88:92]))
	if count <= 0 || count > AbsoluteMaxGroupCuts {
		return Certificate{}, ErrCertificate
	}
	return OpenCertificateInto(raw, make([]GroupCut, count))
}

// OpenCertificateInto authenticates before decoding and uses caller-owned
// exact-capacity storage, allowing allocation-free catalog/controller replay.
func OpenCertificateInto(raw []byte, groups []GroupCut) (Certificate, error) {
	if len(raw) < HeaderBytes+GroupCutBytes+TrailerBytes || len(raw) > AbsoluteMaxCertificateBytes ||
		[8]byte(raw[:8]) != certificateMagic || binary.BigEndian.Uint32(raw[92:96]) != 0 {
		return Certificate{}, ErrCertificate
	}
	count := int(binary.BigEndian.Uint32(raw[88:92]))
	if count == 0 || count > AbsoluteMaxGroupCuts || len(groups) != count ||
		len(raw) != HeaderBytes+count*GroupCutBytes+TrailerBytes ||
		sha256.Sum256(raw[:len(raw)-TrailerBytes]) != [sha256.Size]byte(raw[len(raw)-TrailerBytes:]) {
		return Certificate{}, ErrCertificate
	}
	certificate := Certificate{CatalogGeneration: binary.BigEndian.Uint64(raw[40:48]),
		PolicyGeneration: binary.BigEndian.Uint64(raw[80:88]), Groups: groups}
	copy(certificate.Operation[:], raw[8:40])
	copy(certificate.CatalogDigest[:], raw[48:80])
	for index := range certificate.Groups {
		certificate.Groups[index] = openGroupCut(raw[HeaderBytes+index*GroupCutBytes:])
		if !certificate.Groups[index].Valid() || index != 0 && compareGroup(certificate.Groups[index-1].Group, certificate.Groups[index].Group) >= 0 {
			return Certificate{}, ErrCertificate
		}
	}
	copy(certificate.Digest[:], raw[len(raw)-TrailerBytes:])
	if certificate.Operation == ([sha256.Size]byte{}) || certificate.CatalogGeneration == 0 ||
		certificate.CatalogDigest == ([sha256.Size]byte{}) || certificate.PolicyGeneration == 0 {
		return Certificate{}, ErrCertificate
	}
	return certificate, nil
}

func appendGroup(dst []byte, group raftmember.GroupKey) []byte {
	dst = append(dst, group.ClusterID[:]...)
	dst = append(dst, group.ClusterIncarnation[:]...)
	dst = binary.BigEndian.AppendUint64(dst, group.TopologyRecoveryEpoch)
	dst = append(dst, group.ShardIncarnation[:]...)
	return append(dst, group.GroupID[:]...)
}

func appendGroupCut(dst []byte, cut GroupCut) {
	appendGroup(dst[:0], cut.Group)
	binary.BigEndian.PutUint64(dst[72:80], cut.SourceMember)
	binary.BigEndian.PutUint64(dst[80:88], cut.SchemaGeneration)
	binary.BigEndian.PutUint64(dst[88:96], cut.ReplicaSetVersion)
	binary.BigEndian.PutUint64(dst[96:104], cut.SnapshotIndex)
	binary.BigEndian.PutUint64(dst[104:112], cut.SnapshotTerm)
	copy(dst[112:144], cut.Lineage[:])
	copy(dst[144:176], cut.RelationManifestDigest[:])
	copy(dst[176:208], cut.ArtifactHash[:])
	binary.BigEndian.PutUint64(dst[208:216], cut.ArtifactBytes)
	copy(dst[216:248], cut.ArtifactManifestDigest[:])
}

func openGroupCut(src []byte) (cut GroupCut) {
	copy(cut.Group.ClusterID[:], src[:16])
	copy(cut.Group.ClusterIncarnation[:], src[16:32])
	cut.Group.TopologyRecoveryEpoch = binary.BigEndian.Uint64(src[32:40])
	copy(cut.Group.ShardIncarnation[:], src[40:56])
	copy(cut.Group.GroupID[:], src[56:72])
	cut.SourceMember = binary.BigEndian.Uint64(src[72:80])
	cut.SchemaGeneration = binary.BigEndian.Uint64(src[80:88])
	cut.ReplicaSetVersion = binary.BigEndian.Uint64(src[88:96])
	cut.SnapshotIndex = binary.BigEndian.Uint64(src[96:104])
	cut.SnapshotTerm = binary.BigEndian.Uint64(src[104:112])
	copy(cut.Lineage[:], src[112:144])
	copy(cut.RelationManifestDigest[:], src[144:176])
	copy(cut.ArtifactHash[:], src[176:208])
	cut.ArtifactBytes = binary.BigEndian.Uint64(src[208:216])
	copy(cut.ArtifactManifestDigest[:], src[216:248])
	return cut
}
