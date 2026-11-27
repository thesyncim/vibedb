package splitcontroller

import (
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func testGlobalIndexRetainedPruneAuthorityWithCertificate(
	t *testing.T,
	plan *Plan,
	cutover rangesplit.CutoverCertificate,
) {
	t.Helper()
	published := plan.targetSnapshotForTest(t)
	cert := testRetainedPruneCertificate(t, plan, published, cutover)
	rawCert, err := gateway.AppendRetainedPruneCertificate(nil, cert)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := gateway.CatalogSnapshotDigest(published)
	if err != nil {
		t.Fatal(err)
	}
	payload := remoteStepPayload{
		Catalog:       plan.next,
		CatalogDigest: digest,
		RetainedPrune: rawCert,
	}
	observed := Observation{Catalog: published, Certificate: &cutover}

	buildPruneCmd := func(relation replication.RelationID) replication.CommandView {
		t.Helper()
		client := RetainedPruneClientID(plan.operation)
		tenant := RetainedPruneTenant(plan.operation)
		hSum := sha256.Sum256(append([]byte("vibedb/split-prune/retry-home\x00"), client[:]...))
		var home replication.RetryHome
		copy(home[:], hSum[:])
		cut := cutover.SourceCut()
		coords := cutover.SourceCoordinates()
		batchDigest := replication.Digest{0x42}
		proof := replication.RetainedPruneProof{
			OperationDigest:   replication.Digest(plan.operation),
			CertificateDigest: replication.Digest(cutover.Digest()),
			BatchDigest:       batchDigest,
			DataChainDigest:   replication.Digest(cut.DataChainDigest),
			EntryDigest:       replication.Digest(cut.EntryDigest),
			BaseDigest:        replication.Digest(cut.BaseDigest),
			CutApplied:        cut.Applied,
			CutTerm:           cut.Term,
			OwnershipEpoch:    coords.OwnershipEpoch,
			RoutingVersion:    coords.RoutingVersion,
			RouteGeneration:   coords.RouteGeneration,
			RetainedRange:     plan.children[plan.retained].Range,
		}
		raw, err := replication.AppendCommand(nil, replication.Command{
			Kind:                   replication.CommandRetainedPrune,
			AuthorityClass:         replication.CommandAuthorityTopology,
			ClusterID:              replication.ID128{1},
			ClusterIncarnation:     replication.ID128{2},
			TopologyRecoveryEpoch:  1,
			Distribution:           "docs",
			Shard:                  "source",
			AllocationGeneration:   1,
			ShardIncarnation:       replication.ID128{3},
			GroupID:                replication.ID128{4},
			ReplicaSetVersion:      1,
			ActivePolicyGeneration: 1,
			ProtectionEpoch:        1,
			OwnershipEpoch:         coords.OwnershipEpoch,
			SchemaGeneration:       1,
			RoutingVersion:         coords.RoutingVersion,
			RouteGeneration:        coords.RouteGeneration,
			Tenant:                 tenant,
			ClientID:               client,
			ClientEpoch:            1,
			ClientSequence:         2,
			AckThrough:             1,
			Fingerprint:            batchDigest,
			RetryHome:              home,
			RetainedPrune:          proof,
			Batches: []replication.RelationMutationBatch{{
				Relation: relation,
				Mutations: []replication.Mutation{{
					Kind:                replication.MutationDeleteDigestEqual,
					Key:                 []byte("idx-key"),
					ExpectedValueLength: 8,
					ExpectedValueDigest: replication.Digest{9},
				}},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		view, err := replication.OpenCommand(raw)
		if err != nil {
			t.Fatal(err)
		}
		return view
	}

	// Base-only plan (no indexRelations): accepts relation 1, rejects relation 2.
	if !validSourceTopologyCommand(plan, observed, payload, ActionPruneRetained, buildPruneCmd(1)) {
		t.Fatal("base-only plan rejected relation 1 retained prune")
	}
	if validSourceTopologyCommand(plan, observed, payload, ActionPruneRetained, buildPruneCmd(2)) {
		t.Fatal("base-only plan accepted unplanned relation 2 retained prune")
	}

	// Plan with one global index relation (Relation 2): accepts 1 and 2, rejects 3.
	indexedPlan := *plan
	indexedPlan.indexRelations = []sqldriver.ReplicatedShardRelationIdentity{
		splitGlobalIndexRelation(true),
	}
	if !validSourceTopologyCommand(&indexedPlan, observed, payload, ActionPruneRetained, buildPruneCmd(1)) {
		t.Fatal("global-index plan rejected base relation 1 retained prune")
	}
	if !validSourceTopologyCommand(&indexedPlan, observed, payload, ActionPruneRetained, buildPruneCmd(2)) {
		t.Fatal("global-index plan rejected planned global index relation 2 retained prune")
	}
	if validSourceTopologyCommand(&indexedPlan, observed, payload, ActionPruneRetained, buildPruneCmd(3)) {
		t.Fatal("global-index plan accepted out-of-range relation 3 retained prune")
	}
}
