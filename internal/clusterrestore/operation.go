// Package clusterrestore activates one completely verified backup into a new
// cluster identity. It deliberately cannot reuse source serving authority.
package clusterrestore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var ErrOperation = errors.New("clusterrestore: invalid activation operation")

const (
	operationHeaderBytes  = 64 + clusterbackup.RestoreStagingPermitBytes
	targetGroupBytes      = 72 + 3*(8+16+16)
	operationTrailerBytes = sha256.Size
)

var operationMagic = [8]byte{'V', 'B', 'R', 'S', 'T', 'A', 'C', 'T'}

type ReplicaIdentity struct {
	Member uint64
	Node   rafttransport.NodeID
	Store  [16]byte
}

// TargetGroup is fresh authority for one source cut. It has no ownership,
// routing, grant, policy, or copied source-member fields.
type TargetGroup struct {
	Group    raftmember.GroupKey
	Replicas [3]ReplicaIdentity
}

// Operation is the canonical replicated bootstrap command. Certificate order
// is preserved exactly; Targets[index] is the fresh identity for that cut.
type Operation struct {
	Permit             clusterbackup.RestoreStagingPermit
	Certificate        clusterbackup.Certificate
	CatalogOrdinal     uint32
	PolicyGeneration   uint64
	BuildGrammarDigest [sha256.Size]byte
	Targets            []TargetGroup
	Digest             [sha256.Size]byte
}

func NewOperation(permit clusterbackup.RestoreStagingPermit,
	certificate clusterbackup.Certificate, catalogOrdinal uint32, policyGeneration uint64,
	buildGrammarDigest [sha256.Size]byte, targets []TargetGroup,
) (Operation, error) {
	operation := Operation{Permit: permit, Certificate: certificate,
		CatalogOrdinal: catalogOrdinal, PolicyGeneration: policyGeneration,
		BuildGrammarDigest: buildGrammarDigest, Targets: cloneTargets(targets)}
	raw, err := AppendOperation(nil, operation)
	if err != nil {
		return Operation{}, err
	}
	operation.Digest = sha256.Sum256(raw[:len(raw)-operationTrailerBytes])
	return operation, nil
}

func cloneTargets(targets []TargetGroup) []TargetGroup { return slices.Clone(targets) }

func AppendOperation(dst []byte, operation Operation) ([]byte, error) {
	certificateRaw, err := clusterbackup.AppendCertificate(nil, operation.Certificate)
	count := len(operation.Targets)
	if err != nil || !validOperation(operation, certificateRaw) ||
		count > (math.MaxInt-operationHeaderBytes-operationTrailerBytes-len(certificateRaw))/targetGroupBytes {
		return dst, errors.Join(ErrOperation, err)
	}
	start := len(dst)
	dst = append(dst, make([]byte, operationHeaderBytes+len(certificateRaw)+count*targetGroupBytes+operationTrailerBytes)...)
	raw := dst[start:]
	copy(raw[:8], operationMagic[:])
	binary.BigEndian.PutUint16(raw[8:10], 1)
	binary.BigEndian.PutUint32(raw[12:16], uint32(count))
	binary.BigEndian.PutUint32(raw[16:20], operation.CatalogOrdinal)
	binary.BigEndian.PutUint32(raw[20:24], uint32(len(certificateRaw)))
	binary.BigEndian.PutUint64(raw[24:32], operation.PolicyGeneration)
	copy(raw[32:64], operation.BuildGrammarDigest[:])
	_, _ = clusterbackup.AppendRestoreStagingPermit(raw[:64], operation.Permit)
	copy(raw[operationHeaderBytes:], certificateRaw)
	offset := operationHeaderBytes + len(certificateRaw)
	for _, target := range operation.Targets {
		appendGroupKey(raw[offset:offset+72], target.Group)
		offset += 72
		for _, replica := range target.Replicas {
			binary.BigEndian.PutUint64(raw[offset:offset+8], replica.Member)
			copy(raw[offset+8:offset+24], replica.Node[:])
			copy(raw[offset+24:offset+40], replica.Store[:])
			offset += 40
		}
	}
	digest := sha256.Sum256(raw[:offset])
	copy(raw[offset:], digest[:])
	return dst, nil
}

func OpenOperation(raw []byte) (Operation, error) {
	if len(raw) < operationHeaderBytes+clusterbackup.HeaderBytes+clusterbackup.GroupCutBytes+
		clusterbackup.TrailerBytes+targetGroupBytes+operationTrailerBytes ||
		[8]byte(raw[:8]) != operationMagic || binary.BigEndian.Uint16(raw[8:10]) != 1 ||
		binary.BigEndian.Uint16(raw[10:12]) != 0 {
		return Operation{}, ErrOperation
	}
	count := int(binary.BigEndian.Uint32(raw[12:16]))
	certificateBytes := int(binary.BigEndian.Uint32(raw[20:24]))
	if count <= 0 || count > clusterbackup.AbsoluteMaxGroupCuts ||
		certificateBytes <= 0 || len(raw) != operationHeaderBytes+certificateBytes+
		count*targetGroupBytes+operationTrailerBytes {
		return Operation{}, ErrOperation
	}
	digest := sha256.Sum256(raw[:len(raw)-operationTrailerBytes])
	if digest != [sha256.Size]byte(raw[len(raw)-operationTrailerBytes:]) {
		return Operation{}, ErrOperation
	}
	permit, err := clusterbackup.OpenRestoreStagingPermit(raw[64:operationHeaderBytes])
	certificate, certificateErr := clusterbackup.OpenCertificate(
		raw[operationHeaderBytes : operationHeaderBytes+certificateBytes])
	if err != nil || certificateErr != nil {
		return Operation{}, errors.Join(ErrOperation, err, certificateErr)
	}
	targets := make([]TargetGroup, count)
	offset := operationHeaderBytes + certificateBytes
	for index := range targets {
		targets[index].Group = openGroupKey(raw[offset : offset+72])
		offset += 72
		for replica := range targets[index].Replicas {
			targets[index].Replicas[replica].Member = binary.BigEndian.Uint64(raw[offset : offset+8])
			copy(targets[index].Replicas[replica].Node[:], raw[offset+8:offset+24])
			copy(targets[index].Replicas[replica].Store[:], raw[offset+24:offset+40])
			offset += 40
		}
	}
	operation := Operation{Permit: permit, Certificate: certificate,
		CatalogOrdinal:   binary.BigEndian.Uint32(raw[16:20]),
		PolicyGeneration: binary.BigEndian.Uint64(raw[24:32]), Targets: targets, Digest: digest}
	copy(operation.BuildGrammarDigest[:], raw[32:64])
	canonical, canonicalErr := AppendOperation(nil, operation)
	if canonicalErr != nil || !bytes.Equal(canonical, raw) {
		return Operation{}, errors.Join(ErrOperation, canonicalErr)
	}
	return operation, nil
}

func validOperation(operation Operation, certificateRaw []byte) bool {
	count := len(operation.Targets)
	if count == 0 || count != len(operation.Certificate.Groups) ||
		count != int(operation.Permit.Groups) || operation.CatalogOrdinal >= uint32(count) ||
		operation.Permit.CertificateDigest != operation.Certificate.Digest ||
		operation.Permit.CatalogGeneration != operation.Certificate.CatalogGeneration ||
		operation.Permit.CatalogDigest != operation.Certificate.CatalogDigest ||
		operation.Permit.Restore == ([sha256.Size]byte{}) || operation.PolicyGeneration == 0 ||
		operation.BuildGrammarDigest == ([sha256.Size]byte{}) || len(certificateRaw) == 0 {
		return false
	}
	seenNodes := make(map[rafttransport.NodeID]struct{}, count*3)
	seenStores := make(map[[16]byte]struct{}, count*3)
	seenGroups := make(map[raftmember.GroupKey]struct{}, count)
	for index, target := range operation.Targets {
		source := operation.Certificate.Groups[index]
		if !source.Valid() || target.Group.ClusterID != operation.Permit.TargetClusterID ||
			target.Group.ClusterIncarnation != operation.Permit.TargetClusterIncarnation ||
			target.Group.TopologyRecoveryEpoch == 0 || target.Group.ShardIncarnation == ([16]byte{}) ||
			target.Group.GroupID == ([16]byte{}) || target.Group == source.Group {
			return false
		}
		if _, duplicate := seenGroups[target.Group]; duplicate {
			return false
		}
		seenGroups[target.Group] = struct{}{}
		for ordinal, replica := range target.Replicas {
			if replica.Member != uint64(ordinal+1) || replica.Node == (rafttransport.NodeID{}) ||
				replica.Store == ([16]byte{}) {
				return false
			}
			if _, duplicate := seenNodes[replica.Node]; duplicate {
				return false
			}
			if _, duplicate := seenStores[replica.Store]; duplicate {
				return false
			}
			seenNodes[replica.Node], seenStores[replica.Store] = struct{}{}, struct{}{}
		}
	}
	return true
}

func appendGroupKey(raw []byte, group raftmember.GroupKey) {
	copy(raw[:16], group.ClusterID[:])
	copy(raw[16:32], group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(raw[32:40], group.TopologyRecoveryEpoch)
	copy(raw[40:56], group.ShardIncarnation[:])
	copy(raw[56:72], group.GroupID[:])
}

func openGroupKey(raw []byte) (group raftmember.GroupKey) {
	copy(group.ClusterID[:], raw[:16])
	copy(group.ClusterIncarnation[:], raw[16:32])
	group.TopologyRecoveryEpoch = binary.BigEndian.Uint64(raw[32:40])
	copy(group.ShardIncarnation[:], raw[40:56])
	copy(group.GroupID[:], raw[56:72])
	return
}
