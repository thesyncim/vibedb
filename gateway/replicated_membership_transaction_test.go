package gateway

import (
	"bytes"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Use the real command encoder, wire validation, durable transaction machine,
// and completion reconstruction. Only the Raft transport is replaced by the
// deterministic adapter; the process qualification covers the real owners.
func TestMembershipStableTransactionFinishesAcrossLostResponsesAndReopen(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		name := "singleton"
		if checkpoint {
			name = "checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			route, machine, reopen := newRouteSessionMachineWithCheckpoint(t, checkpoint)
			client := &routeSessionDropClient{base: machine}
			executor, err := NewReplicatedExecutor(client, 1, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			ctx, err := serviceauthz.WithAuthority(t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1})
			if err != nil {
				t.Fatal(err)
			}
			id := distributedtxn.ID{77}
			key, document := []byte("membership-write"), []byte(`{"id":"membership-write","n":1}`)
			batches := []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{{
				Kind: replication.MutationPutAbsentOrEqual, Key: key, Value: document,
			}}}}
			mutationDigest := transactionMutationDigest(batches)
			payload, err := distributedtxn.AppendCoordinator(nil, distributedtxn.CoordinatorRecord{
				ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1, CatalogGeneration: 1, RecoveryDeadline: 3,
				Participants: []distributedtxn.ParticipantRef{{Distribution: []byte(route.Distribution), Shard: []byte(route.Shard),
					RoutingVersion: route.Command.RoutingVersion, AllocationGeneration: route.AllocationGeneration,
					OwnershipEpoch: route.Command.OwnershipEpoch, AuthorityWitness: replicatedTransactionRouteAuthorityWitness(route, true),
					MutationDigest: mutationDigest, State: distributedtxn.ParticipantStaged}},
			})
			if err != nil {
				t.Fatal(err)
			}
			retirement, err := distributedtxn.AppendReplicatedRetirementSummary(nil, distributedtxn.ReplicatedRetirementSummary{AffectedRows: 1, AffectedRowsValid: true})
			if err != nil {
				t.Fatal(err)
			}
			controls := []distributedtxn.ReplicatedCommand{
				{Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedBeginPrepareCoordinator,
					PayloadKind: distributedtxn.ReplicatedPayloadCoordinator, Payload: payload,
					Participant: distributedtxn.ParticipantStage{CoordinatorGroup: distributedtxn.ID(route.Group.GroupID),
						CoordinatorShardIncarnation: distributedtxn.ID(route.Group.ShardIncarnation), CoordinatorAllocation: route.AllocationGeneration,
						BucketBits: 8, IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}}, MutationDigest: mutationDigest}},
				{Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedCommitCoordinator, ExpectedRevision: 1},
				{Role: distributedtxn.ReplicatedRoleParticipant, Operation: distributedtxn.ReplicatedApplyReleaseParticipant, ExpectedRevision: 2},
				{Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedRetireCoordinator, ExpectedRevision: 2,
					PayloadKind: distributedtxn.ReplicatedPayloadRetirement, Payload: retirement},
			}
			var firstCommand []byte
			for index, control := range controls {
				control.ID, control.ControllerEpoch, control.ExecutionPinDigest = id, 7, distributedtxn.Digest{19}
				encoder := replicatedTransactionCommandEncoder{tenant: []byte("tenant"), membershipStable: true}
				var mutations []replication.RelationMutationBatch
				if index == 0 {
					mutations = batches
				}
				exact, err := encoder.appendExact(nil, replication.RetryHome{1}, route, control, mutations)
				if err != nil {
					t.Fatal(err)
				}
				// The next command is already encoded when membership changes.
				applied := machine.state.Applied + 1
				publication, err := machine.machine.ApplyConfiguration(raftmodel.ApplyMeta{Index: applied, Term: machine.state.Fence.Term, Type: pb.EntryConfChange},
					&pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{uint64(4 + index)}})
				if err != nil {
					t.Fatal(err)
				}
				machine.state.Applied, machine.state.Commit = applied, applied
				machine.state.Fence.Command.ReplicaSetVersion = publication.ReplicaSetVersion
				reopen()
				client.drop = replication.CommandTransaction
				if _, err = executor.Propose(ctx, route, exact); err == nil {
					t.Fatalf("operation %d acknowledged lost response", control.Operation)
				}
				if !bytes.Equal(client.dropped, exact) {
					t.Fatalf("operation %d did not reach durable apply", control.Operation)
				}
				reopen()
				result, err := executor.RetryUnknown(ctx, route, exact)
				if err != nil {
					t.Fatalf("operation %d recovery: %v", control.Operation, err)
				}
				completion, err := replication.OpenCompletion(result.Completion)
				if err != nil || completion.ResultCode != replicatedstate.ResultApplied {
					t.Fatalf("operation %d result=%d err=%v", control.Operation, completion.ResultCode, err)
				}
				if index == 0 {
					firstCommand = bytes.Clone(exact)
				}
				if index == 2 || index == 3 {
					value, err := replicatedstate.OpenTransactionCompletionResult(completion.ResultCode, completion.InlineResult)
					if err != nil || !value.AffectedRowsValid || value.AffectedRows != 1 {
						t.Fatalf("operation %d affected rows=%+v err=%v", control.Operation, value, err)
					}
				}
			}
			reopen()
			row, err := machine.machine.PointReadInto(1, key, 1, 4<<20, nil)
			if err != nil || !row.Found || !bytes.Equal(row.Value, document) {
				t.Fatalf("committed document=%+v err=%v", row, err)
			}
			applied := machine.state.Applied
			result, err := executor.RetryUnknown(ctx, route, firstCommand)
			if err != nil {
				t.Fatal(err)
			}
			completion, err := replication.OpenCompletion(result.Completion)
			if err != nil || completion.ResultCode != replicatedstate.ResultApplied ||
				completion.ReplicaSetVersion != route.Command.ReplicaSetVersion || completion.AppliedSequence > applied {
				t.Fatalf("historical retry lost its original authority or reapplied: %+v err=%v", completion, err)
			}
			// Historical transaction completion uses the later durable control
			// revision, not the original reply's applied index. That settlement
			// must itself survive reopen byte-for-byte without re-execution.
			settled := bytes.Clone(result.Completion)
			reopen()
			retried, err := executor.RetryUnknown(ctx, route, firstCommand)
			if err != nil || !bytes.Equal(settled, retried.Completion) {
				t.Fatalf("historical settlement changed after reopen: %v", err)
			}
		})
	}
}
