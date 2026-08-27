package gateway

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

func transactionOrchestratorResult(
	role distributedtxn.ReplicatedRole,
	operation distributedtxn.ReplicatedOperation,
	revision uint64,
	affected int64,
) [24]byte {
	var result [24]byte
	result[0], result[1], result[2] = byte(role), byte(operation), 2
	binary.LittleEndian.PutUint64(result[8:16], revision)
	if (operation == distributedtxn.ReplicatedApplyReleaseParticipant ||
		operation == distributedtxn.ReplicatedRetireCoordinator) && affected >= 0 {
		result[2] |= 1
		binary.LittleEndian.PutUint64(result[16:24], uint64(affected))
	}
	return result
}

func appendTransactionOrchestratorCompletion(
	command replication.CommandView,
	resultCode uint32,
	result []byte,
	applied uint64,
) []byte {
	digest := replication.CompletionResultDigest(
		resultCode, replicatedstate.ResultFormatTransaction, result,
	)
	encoded, err := replication.AppendCompletionBytes(nil, replication.CompletionBytes{
		ClusterID: command.ClusterID, ClusterIncarnation: command.ClusterIncarnation,
		TopologyRecoveryEpoch: command.TopologyRecoveryEpoch,
		Distribution:          command.Distribution, Shard: command.Shard,
		AllocationGeneration: command.AllocationGeneration,
		ShardIncarnation:     command.ShardIncarnation, GroupID: command.GroupID,
		ReplicaSetVersion:      command.ReplicaSetVersion,
		ActivePolicyGeneration: command.ActivePolicyGeneration,
		ProtectionEpoch:        command.ProtectionEpoch,
		RoutingVersion:         command.RoutingVersion, RouteGeneration: command.RouteGeneration,
		Tenant: command.Tenant, ClientID: command.ClientID, ClientEpoch: command.ClientEpoch,
		ClientSequence: command.ClientSequence, Fingerprint: command.Fingerprint,
		RetryHome: command.RetryHome, AppliedSequence: applied,
		ResultCode: resultCode, ResultFormat: replicatedstate.ResultFormatTransaction,
		Storage: replication.CompletionInline, ResultLength: uint64(len(result)),
		ResultDigest: digest, InlineResult: result,
	})
	if err != nil {
		panic(err)
	}
	return encoded
}

func TestReplicatedTransactionCommandEncoderIsCanonical(t *testing.T) {
	participant := durableFaultParticipants(t)[0]
	control := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedStagePrepareParticipant,
		ID:        distributedtxn.ID{1}, PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
		ControllerEpoch: 7, ExecutionPinDigest: distributedtxn.Digest{8},
		Participant: distributedtxn.ParticipantStage{
			CoordinatorGroup: distributedtxn.ID(participant.Route.Group.GroupID),
			CoordinatorShardIncarnation: distributedtxn.ID(
				participant.Route.Group.ShardIncarnation,
			),
			CoordinatorAllocation: participant.Route.AllocationGeneration,
			BucketBits:            participant.BucketBits, IntentScopes: participant.IntentScopes,
			ParticipantOrdinal: 0, MutationDigest: transactionMutationDigest(participant.Batches),
		},
	}
	encoder := replicatedTransactionCommandEncoder{tenant: []byte("tenant")}
	first, err := encoder.appendExact(nil, replication.RetryHome{2}, participant.Route, control, participant.Batches)
	if err != nil {
		t.Fatal(err)
	}
	second, err := encoder.appendExact(nil, replication.RetryHome{2}, participant.Route, control, participant.Batches)
	if err != nil || string(first) != string(second) {
		t.Fatalf("canonical command drift: %v", err)
	}
	if _, err = replication.OpenCommand(first); err != nil {
		t.Fatal(err)
	}
}

func replicatedTransactionEncoderFixture(t testing.TB) (ReplicatedRoute, distributedtxn.ReplicatedCommand) {
	t.Helper()
	id := distributedtxn.ID{1}
	payload, err := distributedtxn.AppendCoordinator(nil, distributedtxn.CoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: 1, RecoveryDeadline: 1,
		Participants: []distributedtxn.ParticipantRef{{
			Distribution: []byte("docs"), Shard: []byte("-80"), RoutingVersion: 1,
			AllocationGeneration: 1, OwnershipEpoch: 1,
			AuthorityWitness: distributedtxn.AuthorityWitness{1},
			MutationDigest:   distributedtxn.Digest{1}, State: distributedtxn.ParticipantStaged,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ReplicatedRoute{
			Distribution: distribution.DistributionName("docs"), Shard: distribution.ShardID("-80"),
			Group: raftmember.GroupKey{
				ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
				TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4},
			},
			AllocationGeneration: 1,
			Command: raftservice.CommandFence{
				ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
				OwnershipEpoch: 1, SchemaGeneration: 1, RelationManifestDigest: [32]byte{1},
				RoutingVersion: 1, RouteGeneration: 1,
			},
		}, distributedtxn.ReplicatedCommand{
			Role:      distributedtxn.ReplicatedRoleCoordinator,
			Operation: distributedtxn.ReplicatedStageCoordinator,
			ID:        id, PayloadKind: distributedtxn.ReplicatedPayloadCoordinator, Payload: payload,
			ControllerEpoch: 1, ExecutionPinDigest: distributedtxn.Digest{1},
		}
}

func TestReplicatedTransactionCommandEncoderPreservesPrefixAndBoundsScratch(t *testing.T) {
	route, control := replicatedTransactionEncoderFixture(t)
	encoder := replicatedTransactionCommandEncoder{
		tenant:         []byte("tenant"),
		controlScratch: make([]byte, 0, replicatedTransactionRetainedControlBytes+1),
	}
	prefix := []byte("prefix")
	dst := append([]byte(nil), prefix...)
	dst = dst[:len(dst):len(dst)]
	encoded, err := encoder.appendExact(dst, replication.RetryHome{1}, route, control, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded[:len(prefix)], prefix) {
		t.Fatalf("destination prefix changed: %q", encoded[:len(prefix)])
	}
	if _, err = replication.OpenCommand(encoded[len(prefix):]); err != nil {
		t.Fatal(err)
	}
	if encoder.controlScratch != nil {
		t.Fatalf("oversized scratch retained: capacity=%d", cap(encoder.controlScratch))
	}
}

func BenchmarkReplicatedTransactionCommandEncoderWarm(b *testing.B) {
	route, control := replicatedTransactionEncoderFixture(b)
	encoder := replicatedTransactionCommandEncoder{tenant: []byte("tenant")}
	warm, err := encoder.appendExact(nil, replication.RetryHome{1}, route, control, nil)
	if err != nil {
		b.Fatal(err)
	}
	dst := make([]byte, 0, len(warm))
	b.ReportAllocs()
	b.SetBytes(int64(len(warm)))
	for b.Loop() {
		dst, err = encoder.appendExact(dst[:0], replication.RetryHome{1}, route, control, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func transactionMutationDigest(batches []replication.RelationMutationBatch) distributedtxn.Digest {
	var digester replication.TransactionMutationDigester
	digest, err := digester.Digest(batches)
	if err != nil {
		panic(err)
	}
	return distributedtxn.Digest(digest)
}
