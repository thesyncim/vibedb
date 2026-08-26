package gateway

import (
	"encoding/binary"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
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

func transactionMutationDigest(batches []replication.RelationMutationBatch) distributedtxn.Digest {
	var digester replication.TransactionMutationDigester
	digest, err := digester.Digest(batches)
	if err != nil {
		panic(err)
	}
	return distributedtxn.Digest(digest)
}
