package clusterrestore

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/raftmember"
)

// TargetSpec binds the metadata that the fresh catalog must publish. Network
// endpoints are deliberately outside the operation: authenticated node IDs,
// not mutable DNS addresses, are replicated authority.
type TargetSpec struct {
	CatalogOrdinal      uint32
	PolicyGeneration    uint64
	TopologyEpoch       uint64
	BuildGrammarDigest  [sha256.Size]byte
	TargetPolicyDigest  [sha256.Size]byte
	TargetCatalogDigest [sha256.Size]byte
}

// SealFreshOperation generates every target group, node, and store identity
// from the operating system CSPRNG. The staging permit already binds the fresh
// cluster trust domain established during artifact verification.
func SealFreshOperation(permit clusterbackup.RestoreStagingPermit, certificate clusterbackup.Certificate,
	spec TargetSpec,
) (Operation, error) {
	return sealFreshOperation(rand.Reader, permit, certificate, spec)
}

func sealFreshOperation(entropy io.Reader, permit clusterbackup.RestoreStagingPermit,
	certificate clusterbackup.Certificate,
	spec TargetSpec,
) (Operation, error) {
	targets, err := planFreshTargets(entropy, permit, certificate, spec.TopologyEpoch)
	if err != nil {
		return Operation{}, err
	}
	return NewOperation(permit, certificate, spec.CatalogOrdinal, spec.PolicyGeneration,
		spec.BuildGrammarDigest, spec.TargetPolicyDigest, spec.TargetCatalogDigest, targets)
}

// PlanFreshTargets mints identities before the caller constructs its fresh
// catalog/schema projection. The returned plan grants no serving authority;
// NewOperation must subsequently seal its exact projection and policy digests.
// This avoids a circular dependency between fresh node IDs and catalog bytes.
func PlanFreshTargets(permit clusterbackup.RestoreStagingPermit,
	certificate clusterbackup.Certificate, topologyEpoch uint64,
) ([]TargetGroup, error) {
	return planFreshTargets(rand.Reader, permit, certificate, topologyEpoch)
}

func planFreshTargets(entropy io.Reader, permit clusterbackup.RestoreStagingPermit,
	certificate clusterbackup.Certificate, topologyEpoch uint64,
) ([]TargetGroup, error) {
	if entropy == nil || topologyEpoch == 0 || len(certificate.Groups) == 0 ||
		permit.CertificateDigest != certificate.Digest || permit.CatalogGeneration != certificate.CatalogGeneration ||
		permit.CatalogDigest != certificate.CatalogDigest || permit.Restore == ([sha256.Size]byte{}) {
		return nil, ErrOperation
	}
	if _, err := clusterbackup.AppendCertificate(nil, certificate); err != nil {
		return nil, errors.Join(ErrOperation, err)
	}
	targets := make([]TargetGroup, len(certificate.Groups))
	for ordinal := range targets {
		target := &targets[ordinal]
		target.Group = raftmember.GroupKey{ClusterID: permit.TargetClusterID,
			ClusterIncarnation:    permit.TargetClusterIncarnation,
			TopologyRecoveryEpoch: topologyEpoch}
		if err := readIdentity(entropy, target.Group.ShardIncarnation[:]); err != nil {
			return nil, err
		}
		if err := readIdentity(entropy, target.Group.GroupID[:]); err != nil {
			return nil, err
		}
		for replica := range target.Replicas {
			target.Replicas[replica].Member = uint64(replica + 1)
			target.Replicas[replica].NodeIncarnation = 1
			if err := readIdentity(entropy, target.Replicas[replica].Node[:]); err != nil {
				return nil, err
			}
			if err := readIdentity(entropy, target.Replicas[replica].Store[:]); err != nil {
				return nil, err
			}
		}
	}
	if !validTargetGroups(permit, certificate, targets) {
		return nil, ErrOperation
	}
	return targets, nil
}

func readIdentity(entropy io.Reader, destination []byte) error {
	if _, err := io.ReadFull(entropy, destination); err != nil {
		return errors.Join(ErrOperation, err)
	}
	return nil
}
