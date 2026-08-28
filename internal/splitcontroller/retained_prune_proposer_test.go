package splitcontroller

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestRetainedPrunePendingMatchBindsExactRF3SourceAndDeletes(t *testing.T) {
	operation := OperationID{1, 2, 3}
	clientID := RetainedPruneClientID(operation)
	fence := retainedPruneTestFence()
	keys := [][]byte{[]byte("a"), []byte("middle"), []byte("z")}
	mutations := make([]gateway.NativeMutation, len(keys))
	encodedMutations := make([]replication.Mutation, len(keys))
	for index := range keys {
		digest := replication.Digest{byte(index + 1)}
		mutations[index] = gateway.NativeMutation{
			Relation: 1, Kind: replication.MutationDeleteDigestEqual, Key: keys[index],
			ExpectedValueLength: uint64(index + 10), ExpectedValueDigest: digest,
		}
		encodedMutations[index] = replication.Mutation{
			Kind: replication.MutationDeleteDigestEqual, Key: keys[index],
			ExpectedValueLength: uint64(index + 10), ExpectedValueDigest: digest,
		}
	}
	digest := replication.Digest{20}
	proof := retainedPruneTestProof(digest)
	raw := retainedPruneTestCommand(t, fence, clientID, 1, proof, encodedMutations)
	if !retainedPrunePendingMatches(raw, fence, clientID, 1, proof, mutations) {
		t.Fatal("exact pending prune command rejected")
	}
	stale := fence
	stale.Command.OwnershipEpoch++
	if retainedPrunePendingMatches(raw, stale, clientID, 1, proof, mutations) {
		t.Fatal("stale source fence accepted")
	}
	changed := append([]gateway.NativeMutation(nil), mutations...)
	changed[1].Key = []byte("other")
	if retainedPrunePendingMatches(raw, fence, clientID, 1, proof, changed) {
		t.Fatal("different pending delete set accepted")
	}
	wrongProof := proof
	wrongProof.BatchDigest[0]++
	if retainedPrunePendingMatches(raw, fence, clientID, 1, wrongProof, mutations) {
		t.Fatal("different certified prune digest accepted")
	}
	if RetainedPruneClientID(OperationID{1, 2, 4}) == clientID ||
		bytes.Equal(RetainedPruneTenant(operation), RetainedPruneTenant(OperationID{1, 2, 4})) {
		t.Fatal("split operations shared controller identity")
	}
}

func TestRetainedPrunePendingMatchBindsCanonicalMultiRelationDeletes(t *testing.T) {
	fence := retainedPruneTestFence()
	clientID := RetainedPruneClientID(OperationID{9})
	proof := retainedPruneTestProof(replication.Digest{8})
	mutations := []gateway.NativeMutation{
		{Relation: 1, Kind: replication.MutationDeleteDigestEqual, Key: []byte("base"), ExpectedValueLength: 4, ExpectedValueDigest: replication.Digest{1}},
		{Relation: 2, Kind: replication.MutationDeleteDigestEqual, Key: []byte("index"), ExpectedValueLength: 5, ExpectedValueDigest: replication.Digest{2}},
	}
	raw, err := replication.AppendCommand(nil, replication.Command{
		Kind: replication.CommandRetainedPrune, AuthorityClass: replication.CommandAuthorityTopology,
		ClusterID: replication.ID128(fence.Group.ClusterID), ClusterIncarnation: replication.ID128(fence.Group.ClusterIncarnation),
		TopologyRecoveryEpoch: fence.Group.TopologyRecoveryEpoch, Distribution: "docs", Shard: "source",
		AllocationGeneration: fence.AllocationGeneration, ShardIncarnation: replication.ID128(fence.Group.ShardIncarnation),
		GroupID: replication.ID128(fence.Group.GroupID), ReplicaSetVersion: fence.Command.ReplicaSetVersion,
		ActivePolicyGeneration: fence.Command.ActivePolicyGeneration, ProtectionEpoch: fence.Command.ProtectionEpoch,
		OwnershipEpoch: fence.Command.OwnershipEpoch, SchemaGeneration: fence.Command.SchemaGeneration,
		RoutingVersion: fence.Command.RoutingVersion, RouteGeneration: fence.Command.RouteGeneration,
		Tenant: []byte("split-prune"), ClientID: clientID, ClientEpoch: 19, ClientSequence: 2,
		AckThrough: 1, Fingerprint: proof.BatchDigest, RetryHome: replication.RetryHome{21}, RetainedPrune: proof,
		Batches: []replication.RelationMutationBatch{
			{Relation: 1, Mutations: []replication.Mutation{{Kind: mutations[0].Kind, Key: mutations[0].Key, ExpectedValueLength: mutations[0].ExpectedValueLength, ExpectedValueDigest: mutations[0].ExpectedValueDigest}}},
			{Relation: 2, Mutations: []replication.Mutation{{Kind: mutations[1].Kind, Key: mutations[1].Key, ExpectedValueLength: mutations[1].ExpectedValueLength, ExpectedValueDigest: mutations[1].ExpectedValueDigest}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !retainedPrunePendingMatches(raw, fence, clientID, 1, proof, mutations) {
		t.Fatal("exact multi-relation pending prune rejected")
	}
	mutations[1].ExpectedValueLength++
	if retainedPrunePendingMatches(raw, fence, clientID, 1, proof, mutations) {
		t.Fatal("changed old index identity accepted")
	}
}

func retainedPruneTestFence() raftservice.ServingFence {
	return raftservice.ServingFence{
		Group: raftmember.GroupKey{
			ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
			TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5},
		},
		AllocationGeneration: 6,
		Command: raftservice.CommandFence{
			ReplicaSetVersion: 7, ActivePolicyGeneration: 8, ProtectionEpoch: 9,
			OwnershipEpoch: 10, SchemaGeneration: 11,
			RelationManifestDigest: [32]byte{12}, RoutingVersion: 13, RouteGeneration: 14,
		},
		MemberID: 15, StoreID: [16]byte{16}, NodeIncarnation: 17, Term: 18,
	}
}

func retainedPruneTestCommand(
	t testing.TB,
	fence raftservice.ServingFence,
	clientID replication.ID128,
	relation replication.RelationID,
	proof replication.RetainedPruneProof,
	mutations []replication.Mutation,
) []byte {
	t.Helper()
	raw, err := replication.AppendCommand(nil, replication.Command{
		Kind: replication.CommandRetainedPrune, AuthorityClass: replication.CommandAuthorityTopology,
		ClusterID:             replication.ID128(fence.Group.ClusterID),
		ClusterIncarnation:    replication.ID128(fence.Group.ClusterIncarnation),
		TopologyRecoveryEpoch: fence.Group.TopologyRecoveryEpoch,
		Distribution:          "docs", Shard: "source", AllocationGeneration: fence.AllocationGeneration,
		ShardIncarnation:       replication.ID128(fence.Group.ShardIncarnation),
		GroupID:                replication.ID128(fence.Group.GroupID),
		ReplicaSetVersion:      fence.Command.ReplicaSetVersion,
		ActivePolicyGeneration: fence.Command.ActivePolicyGeneration,
		ProtectionEpoch:        fence.Command.ProtectionEpoch, OwnershipEpoch: fence.Command.OwnershipEpoch,
		SchemaGeneration: fence.Command.SchemaGeneration,
		RoutingVersion:   fence.Command.RoutingVersion, RouteGeneration: fence.Command.RouteGeneration,
		Tenant: []byte("split-prune"), ClientID: clientID, ClientEpoch: 19, ClientSequence: 2,
		AckThrough: 1, Fingerprint: proof.BatchDigest, RetryHome: replication.RetryHome{21},
		RetainedPrune: proof,
		Batches:       []replication.RelationMutationBatch{{Relation: relation, Mutations: mutations}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func retainedPruneTestProof(batch replication.Digest) replication.RetainedPruneProof {
	return replication.RetainedPruneProof{
		OperationDigest: replication.Digest{1}, CertificateDigest: replication.Digest{2},
		BatchDigest: batch, DataChainDigest: replication.Digest{3},
		EntryDigest: replication.Digest{4}, BaseDigest: replication.Digest{5},
		CutApplied: 6, CutTerm: 7, OwnershipEpoch: 10, RoutingVersion: 13,
		RouteGeneration: 14,
		RetainedRange: distribution.KeyRange{End: distribution.KeyspaceEnd{
			Point: distribution.KeyspacePoint{0x80},
		}},
	}
}
