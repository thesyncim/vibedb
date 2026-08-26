package rebalance

import (
	"context"

	"github.com/thesyncim/vibedb/gateway"
)

// ReplicatedFailureAuthority adapts the concrete catalog-Raft source to the
// scheduler's failure vocabulary. It performs no probing and owns no clock;
// the only certificates it can expose were already quorum-confirmed across
// gateway.FailureConfirmationRevisions committed health revisions.
type ReplicatedFailureAuthority struct {
	Source gateway.ReplicaFailureCertificateSource
}

func (authority ReplicatedFailureAuthority) VisitFailureCertificates(
	ctx context.Context, catalog *gateway.Snapshot,
	visit func(FailureQuorumCertificate) error,
) error {
	if ctx == nil || catalog == nil || authority.Source == nil || visit == nil {
		return ErrFailureEvidence
	}
	return authority.Source.VisitReplicaFailureCertificates(ctx, catalog,
		func(source gateway.ReplicatedFailureCertificate) error {
			if source.FirstRevision == 0 || source.ConfirmedRevision < source.FirstRevision ||
				source.ConfirmedRevision-source.FirstRevision+1 < MinimumFailureConfirmationEpochs {
				return ErrFailureEvidence
			}
			certificate := FailureQuorumCertificate{
				Distribution: source.Distribution, Shard: source.Shard, Group: source.Group,
				CatalogGeneration: source.CatalogGeneration,
				ReplicaSetVersion: source.ReplicaSetVersion, LeaderTerm: source.LeaderTerm,
				CommitIndex: source.CommitIndex, FirstFailureEpoch: source.FirstRevision,
				ConfirmedEpoch: source.ConfirmedRevision, SuspectMember: source.SuspectMember,
				Confirmations: make([]FailureConfirmation, len(source.Confirmations)),
			}
			for index, confirmation := range source.Confirmations {
				certificate.Confirmations[index] = FailureConfirmation{
					Member: confirmation.Member, FirstFailureEpoch: confirmation.FirstRevision,
					ConfirmedEpoch: confirmation.ConfirmedRevision, LeaderTerm: confirmation.LeaderTerm,
					ReplicaSetVersion: confirmation.ReplicaSetVersion, CommitIndex: confirmation.CommitIndex,
				}
			}
			return visit(certificate)
		})
}
