package rebalance

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
)

type fixedReplicatedFailureSource struct {
	certificate gateway.ReplicatedFailureCertificate
}

func (source fixedReplicatedFailureSource) VisitReplicaFailureCertificates(
	_ context.Context, _ *gateway.Snapshot,
	visit func(gateway.ReplicatedFailureCertificate) error,
) error {
	return visit(source.certificate)
}

func TestReplicatedFailureAuthorityMapsExactCommittedCertificate(t *testing.T) {
	catalog := &gateway.Snapshot{}
	certificate := gateway.ReplicatedFailureCertificate{
		Distribution: "d", Shard: "s", CatalogGeneration: 9,
		ReplicaSetVersion: 7, LeaderTerm: 5, CommitIndex: 44,
		FirstRevision: 10, ConfirmedRevision: 12, SuspectMember: 3,
		Confirmations: []gateway.ReplicatedFailureConfirmation{
			{Member: 1, FirstRevision: 10, ConfirmedRevision: 12, LeaderTerm: 5, ReplicaSetVersion: 7, CommitIndex: 44},
			{Member: 2, FirstRevision: 10, ConfirmedRevision: 12, LeaderTerm: 5, ReplicaSetVersion: 7, CommitIndex: 44},
		},
	}
	authority := ReplicatedFailureAuthority{Source: fixedReplicatedFailureSource{certificate}}
	visited := 0
	err := authority.VisitFailureCertificates(context.Background(), catalog,
		func(result FailureQuorumCertificate) error {
			visited++
			if result.FirstFailureEpoch != 10 || result.ConfirmedEpoch != 12 ||
				result.CommitIndex != 44 || len(result.Confirmations) != 2 {
				t.Fatalf("wrong mapped certificate: %+v", result)
			}
			return nil
		})
	if err != nil || visited != 1 {
		t.Fatalf("visited=%d err=%v", visited, err)
	}
}
