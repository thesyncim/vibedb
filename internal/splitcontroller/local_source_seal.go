package splitcontroller

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"go.etcd.io/raft/v3"
)

// SourceSealProposer is the shipped serialized owner capability used to put
// the terminal ownership fence through the source Raft group. A nil result is
// admission, not application; the controller settles it by observing the
// durable source state and captured terminal entry before certification.
type SourceSealProposer interface {
	ProposeOwnershipTransition(context.Context, raftservice.ServingFence, []byte) error
}

// ExecuteSealSource admits exactly one immutable range-narrowing fence through
// the currently observed source leader. It derives both voter identities from
// the durable ConfState and refuses joint membership, copied runtime identity,
// stale command fences, or an unsealed child tail. It does not wait for apply,
// synthesize a certificate, activate a child, or publish routing.
func (a *LocalSourceActions) ExecuteSealSource(
	ctx context.Context,
	plan *Plan,
	state replicatedstate.State,
	tail rangesplit.TailCursor,
	serving raftservice.ServingState,
	proposer SourceSealProposer,
) error {
	if a == nil || ctx == nil || plan == nil || proposer == nil ||
		plan.operation != a.store.operation || tail.Sealed() || state.ConfState == nil {
		return ErrInvalidPlan
	}
	a.mu.Lock()
	manifest, err := a.machine.RelationManifestDigest()
	cut, snapshotErr := a.machine.Snapshot()
	a.mu.Unlock()
	if err != nil || snapshotErr != nil {
		return errors.Join(err, snapshotErr)
	}
	cutState := cut.State()
	placementErr := plan.validateGlobalIndexCut(cut)
	closeErr := cut.Close()
	if placementErr != nil || closeErr != nil {
		return errors.Join(placementErr, closeErr)
	}
	if !sameSplitCut(state, cutState) {
		return ErrTopologyConflict
	}
	if !sourceServingStateMatches(state, serving, manifest) {
		return ErrTopologyConflict
	}
	voters := state.ConfState.GetVoters()
	if len(voters) < 2 || len(state.ConfState.GetVotersOutgoing()) != 0 ||
		len(state.ConfState.GetLearnersNext()) != 0 || state.ConfState.GetAutoLeave() {
		return ErrTopologyConflict
	}
	source := serving.Identity.MemberID
	var target uint64
	for _, member := range voters {
		if member != source && (target == 0 || member < target) {
			target = member
		}
	}
	if target == 0 {
		return ErrTopologyConflict
	}
	command, err := plan.AppendSourceSeal(
		make([]byte, 0, replicatedstate.MaxOwnershipTransitionBytes),
		state, tail, source, target,
	)
	if err != nil {
		return err
	}
	if err = proposer.ProposeOwnershipTransition(ctx, serving.Fence(), command); err != nil {
		return err
	}
	return nil
}

func sourceServingStateMatches(
	state replicatedstate.State,
	serving raftservice.ServingState,
	manifest [32]byte,
) bool {
	binding, identity, status, command := state.Binding, serving.Identity, serving.Status, serving.Command
	group := identity.Group
	return manifest != ([32]byte{}) && command.Valid() &&
		group.ClusterID == [16]byte(binding.ClusterID) &&
		group.ClusterIncarnation == [16]byte(binding.ClusterIncarnation) &&
		group.TopologyRecoveryEpoch == binding.TopologyRecoveryEpoch &&
		group.ShardIncarnation == [16]byte(binding.ShardIncarnation) &&
		group.GroupID == [16]byte(binding.GroupID) &&
		identity.Distribution == binding.Distribution && identity.Shard == binding.Shard &&
		identity.AllocationGeneration == binding.AllocationGeneration &&
		identity.MemberID != 0 && identity.StoreID != ([16]byte{}) && identity.NodeIncarnation != 0 &&
		status.MemberID == identity.MemberID && status.LeaderID == identity.MemberID &&
		status.RaftState == raft.StateLeader && status.Term != 0 &&
		status.Applied == state.Applied && status.Applied <= status.Commit &&
		command.ReplicaSetVersion == state.ReplicaSetVersion &&
		command.ActivePolicyGeneration == binding.ActivePolicyGeneration &&
		command.ProtectionEpoch == binding.ProtectionEpoch &&
		command.OwnershipEpoch == binding.OwnershipEpoch &&
		command.SchemaGeneration == binding.SchemaGeneration &&
		command.RelationManifestDigest == manifest &&
		command.RoutingVersion == binding.RoutingVersion &&
		command.RouteGeneration == binding.RouteGeneration
}
