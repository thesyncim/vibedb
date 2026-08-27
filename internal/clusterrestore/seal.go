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
	if entropy == nil || spec.TopologyEpoch == 0 || len(certificate.Groups) == 0 {
		return Operation{}, ErrOperation
	}
	targets := make([]TargetGroup, len(certificate.Groups))
	for ordinal := range targets {
		target := &targets[ordinal]
		target.Group = raftmember.GroupKey{ClusterID: permit.TargetClusterID,
			ClusterIncarnation:    permit.TargetClusterIncarnation,
			TopologyRecoveryEpoch: spec.TopologyEpoch}
		if err := readIdentity(entropy, target.Group.ShardIncarnation[:]); err != nil {
			return Operation{}, err
		}
		if err := readIdentity(entropy, target.Group.GroupID[:]); err != nil {
			return Operation{}, err
		}
		for replica := range target.Replicas {
			target.Replicas[replica].Member = uint64(replica + 1)
			target.Replicas[replica].NodeIncarnation = 1
			if err := readIdentity(entropy, target.Replicas[replica].Node[:]); err != nil {
				return Operation{}, err
			}
			if err := readIdentity(entropy, target.Replicas[replica].Store[:]); err != nil {
				return Operation{}, err
			}
		}
	}
	return NewOperation(permit, certificate, spec.CatalogOrdinal, spec.PolicyGeneration,
		spec.BuildGrammarDigest, spec.TargetPolicyDigest, spec.TargetCatalogDigest, targets)
}

func readIdentity(entropy io.Reader, destination []byte) error {
	if _, err := io.ReadFull(entropy, destination); err != nil {
		return errors.Join(ErrOperation, err)
	}
	return nil
}
