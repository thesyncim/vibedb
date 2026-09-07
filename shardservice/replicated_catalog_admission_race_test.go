package shardservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// catalogAdmissionRaceOwner models the serialized ordering that exposed the
// CI deadlock: the server's initial probe sees the old serving cut, the
// catalog command is prepared, membership is applied, and only then does the
// owner report the pre-admission stale result. The owner deliberately keeps
// this fixture synchronous so the server counters are the only observation
// under test.
type catalogAdmissionRaceOwner struct {
	*fakeReplicatedOwner
	membershipRequest raftservice.MembershipRequest
	preparedCommand   []byte
	preparedFence     raftservice.ServingFence
	prepared          bool
	membershipApplied bool
}

func (owner *catalogAdmissionRaceOwner) ApplyMembership(
	ctx context.Context,
	request raftservice.MembershipRequest,
) error {
	owner.membershipApplied = true
	// Publish the membership change only after the old command has been
	// captured. Probe therefore returns a new command fence on the refusal
	// refresh, while SubmitOwned still reports the pre-admission stale outcome.
	owner.state.Command.ReplicaSetVersion++
	return owner.fakeReplicatedOwner.ApplyMembership(ctx, request)
}

func (owner *catalogAdmissionRaceOwner) SubmitOwned(
	ctx context.Context,
	fence raftservice.ServingFence,
	command []byte,
) (raftservice.Result, error) {
	owner.prepared = true
	owner.preparedFence = fence
	owner.preparedCommand = bytes.Clone(command)
	if err := owner.ApplyMembership(ctx, owner.membershipRequest); err != nil {
		return raftservice.Result{State: owner.state}, err
	}
	return raftservice.Result{State: owner.state,
			Outcome: raftserve.Outcome{Code: raftserve.OutcomeStaleCommand}},
		replicatedstate.ErrStaleCommand
}

// settledCatalogOwner models the post-admission result after the state-machine
// fix: the stale catalog command has a durable apply witness and therefore
// reaches the server as a valid deterministic refusal. It also retains the
// exact bytes passed to SubmitOwned so a repeated native retry can prove that
// the original command was not rebuilt under the new membership.
type settledCatalogOwner struct {
	*fakeReplicatedOwner
	command  []byte
	complete []byte
	calls    int
}

func (owner *settledCatalogOwner) SubmitOwned(
	_ context.Context,
	_ raftservice.ServingFence,
	command []byte,
) (raftservice.Result, error) {
	owner.calls++
	if owner.command == nil {
		owner.command = bytes.Clone(command)
	} else if !bytes.Equal(owner.command, command) {
		return raftservice.Result{State: owner.state}, replicatedstate.ErrStaleCommand
	}
	return raftservice.Result{State: owner.state,
		Outcome: raftserve.Outcome{Code: raftserve.OutcomeCompletion, AppliedIndex: 15,
			CompletionAppliedSequence: 15, CompletionBytes: len(owner.complete)},
		Completion: owner.complete}, nil
}

func testReplicatedCatalogCompletion(
	t testing.TB,
	command []byte,
	applied uint64,
) []byte {
	t.Helper()
	view, err := replication.OpenCommand(command)
	if err != nil {
		t.Fatalf("OpenCommand: %v", err)
	}
	result, err := replicatedstate.AppendMutationCompletionResult(
		nil, replicatedstate.ResultStaleFence, 0,
	)
	if err != nil {
		t.Fatalf("AppendMutationCompletionResult: %v", err)
	}
	return testReplicatedCompletionEnvelope(t, view, applied,
		replicatedstate.ResultStaleFence, result)
}

func testReplicatedCompletionEnvelope(
	t testing.TB,
	command replication.CommandView,
	applied uint64,
	resultCode uint32,
	result []byte,
) []byte {
	t.Helper()
	encoded, err := replication.AppendCompletion(nil, replication.Completion{
		ClusterID:              command.ClusterID,
		ClusterIncarnation:     command.ClusterIncarnation,
		TopologyRecoveryEpoch:  command.TopologyRecoveryEpoch,
		Distribution:           string(command.Distribution),
		Shard:                  string(command.Shard),
		AllocationGeneration:   command.AllocationGeneration,
		ShardIncarnation:       command.ShardIncarnation,
		GroupID:                command.GroupID,
		ReplicaSetVersion:      command.ReplicaSetVersion,
		ActivePolicyGeneration: command.ActivePolicyGeneration,
		ProtectionEpoch:        command.ProtectionEpoch,
		RoutingVersion:         command.RoutingVersion,
		RouteGeneration:        command.RouteGeneration,
		Tenant:                 command.Tenant,
		ClientID:               command.ClientID,
		ClientEpoch:            command.ClientEpoch,
		ClientSequence:         command.ClientSequence,
		Fingerprint:            command.Fingerprint,
		RetryHome:              command.RetryHome,
		AppliedSequence:        applied,
		ResultCode:             resultCode,
		ResultFormat:           replicatedstate.ResultFormatMutation,
		Storage:                replication.CompletionInline,
		ResultLength:           uint64(len(result)),
		ResultDigest: replication.CompletionResultDigest(
			resultCode, replicatedstate.ResultFormatMutation, result,
		),
		InlineResult: result,
	})
	if err != nil {
		t.Fatalf("AppendCompletion: %v", err)
	}
	return encoded
}

func testReplicatedCatalogCommand(t testing.TB, fence ReplicatedFence) []byte {
	t.Helper()
	command := replication.Command{
		Kind:                   replication.CommandMutationBatch,
		AuthorityClass:         replication.CommandAuthorityTopology,
		ClusterID:              fence.Group.ClusterID,
		ClusterIncarnation:     fence.Group.ClusterIncarnation,
		TopologyRecoveryEpoch:  fence.Group.TopologyRecoveryEpoch,
		Distribution:           "catalog",
		Shard:                  "controlplane",
		AllocationGeneration:   fence.AllocationGeneration,
		ShardIncarnation:       fence.Group.ShardIncarnation,
		GroupID:                fence.Group.GroupID,
		ReplicaSetVersion:      fence.Command.ReplicaSetVersion,
		ActivePolicyGeneration: fence.Command.ActivePolicyGeneration,
		ProtectionEpoch:        fence.Command.ProtectionEpoch,
		OwnershipEpoch:         fence.Command.OwnershipEpoch,
		SchemaGeneration:       fence.Command.SchemaGeneration,
		RoutingVersion:         fence.Command.RoutingVersion,
		RouteGeneration:        fence.Command.RouteGeneration,
		Tenant:                 []byte("tenant"),
		ClientID:               replication.ID128{1},
		ClientEpoch:            2,
		ClientSequence:         1,
		Fingerprint:            sha256.Sum256([]byte("catalog-membership-race")),
		Batches: []replication.RelationMutationBatch{{
			Relation: 1,
			Mutations: []replication.Mutation{{
				Kind: replication.MutationPut,
				Key:  []byte("catalog-key"), Value: []byte(`{"id":1}`),
			}},
		}},
	}
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatalf("AppendCommand: %v", err)
	}
	return encoded
}

func TestReplicatedServerCatalogMembershipRaceRecordsUnappliedStaleOutcome(t *testing.T) {
	fence := testReplicatedFence()
	state := testReplicatedServingState()
	command := testReplicatedCatalogCommand(t, fence)
	membership := raftservice.MembershipRequest{
		Fence: raftservice.ServingFence{
			Group: fence.Group, AllocationGeneration: fence.AllocationGeneration,
			Command: fence.Command, MemberID: fence.MemberID, StoreID: fence.StoreID,
			NodeIncarnation: fence.NodeIncarnation, Term: fence.Term,
		},
		Kind:                      raftservice.MembershipAddLearner,
		TransitionID:              [16]byte{2},
		MetadataEpoch:             3,
		CatalogGeneration:         4,
		ExpectedReplicaSetVersion: fence.Command.ReplicaSetVersion,
		SourceMember:              fence.MemberID,
		TargetMember:              fence.MemberID + 1,
	}
	owner := &catalogAdmissionRaceOwner{
		fakeReplicatedOwner: &fakeReplicatedOwner{state: state},
		membershipRequest:   membership,
	}
	server := testReplicatedServer(owner)
	request := &ReplicatedRequest{
		Operation: ReplicatedPropose, Capability: serviceauthz.CapabilityTopology,
		Fence: fence, Command: command,
	}
	response := server.executeReplicated(context.Background(), request)
	stats := server.Stats()
	if response.Kind != ReplicatedOutcomeUnknown ||
		stats.ProposalInvalidDeterministic != 1 ||
		stats.ProposalInvalidDeterministicCode != raftserve.OutcomeStaleCommand ||
		stats.ProposalInvalidDeterministicApplied != 0 ||
		stats.ProposalInvalidDeterministicReasons&ReplicatedDeterministicInvalidAppliedIndex == 0 {
		t.Fatalf("response=%+v stats=%+v", response, stats)
	}
	if !owner.prepared || !owner.membershipApplied ||
		!bytes.Equal(owner.preparedCommand, command) ||
		owner.preparedFence.Command != fence.Command ||
		owner.membership != membership {
		t.Fatalf("interleaving was not exercised: prepared=%t applied=%t fence=%+v membership=%+v",
			owner.prepared, owner.membershipApplied, owner.preparedFence, owner.membership)
	}
}

func TestReplicatedServerSettledCatalogStaleRefusalKeepsWitnessAndRetryBytes(t *testing.T) {
	fence := testReplicatedFence()
	state := testReplicatedServingState()
	command := testReplicatedCatalogCommand(t, fence)
	owner := &settledCatalogOwner{
		fakeReplicatedOwner: &fakeReplicatedOwner{state: state},
		complete:            testReplicatedCatalogCompletion(t, command, 15),
	}
	server := testReplicatedServer(owner)
	request := &ReplicatedRequest{
		Operation: ReplicatedPropose, Capability: serviceauthz.CapabilityTopology,
		Fence: fence, Command: command,
	}
	first := server.executeReplicated(context.Background(), request)
	second := server.executeReplicated(context.Background(), request)
	if first.Kind != ReplicatedCompletion || second.Kind != ReplicatedCompletion ||
		!validReplicatedResponse(first) || !validReplicatedResponse(second) ||
		first.Outcome.Code != raftserve.OutcomeCompletion ||
		second.Outcome.Code != raftserve.OutcomeCompletion ||
		first.Outcome.AppliedIndex != 15 || second.Outcome.AppliedIndex != 15 ||
		first.Outcome.CompletionAppliedSequence != 15 ||
		second.Outcome.CompletionAppliedSequence != 15 ||
		first.Outcome.CompletionBytes != len(owner.complete) ||
		second.Outcome.CompletionBytes != len(owner.complete) ||
		first.State.Applied != 15 || second.State.Applied != 15 ||
		!bytes.Equal(first.Completion, owner.complete) ||
		!bytes.Equal(second.Completion, owner.complete) ||
		!bytes.Equal(first.Completion, second.Completion) {
		t.Fatalf("first=%+v second=%+v stats=%+v", first, second, server.Stats())
	}
	stats := server.Stats()
	if stats.ProposalInvalidDeterministic != 0 || stats.ProposalUnknownSubmit != 0 ||
		stats.ProposalUnknownAbandoned != 0 || owner.calls != 2 ||
		!bytes.Equal(owner.command, command) {
		t.Fatalf("settled retry lost witness or exact command: owner=%+v stats=%+v", owner, stats)
	}
}
