package raftservice

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

// TransferSplitSourceLeadership is an internal admitted-split capability, not
// a public membership API. It preserves the voter set and admits a handoff
// only from the exact current leader fence to another existing voter.
func (owner *Owner) TransferSplitSourceLeadership(ctx context.Context, fence ServingFence, target uint64) error {
	if owner == nil || ctx == nil || target == 0 || target == fence.MemberID {
		return ErrInvalidOwner
	}
	_, err := owner.enqueue(ctx, ownerRequest{kind: requestSplitSourceLeadership,
		group: fence.Group, fence: fence, targetMember: target, reply: make(chan ownerReply, 1),
		transferDelivery: &transferDelivery{}})
	if errors.Is(err, errOwnerTransferCanceled) {
		return context.Cause(ctx)
	}
	return err
}

func (owners *ExecutionOwners) TransferSplitSourceLeadership(ctx context.Context, fence ServingFence, target uint64) error {
	owner, err := owners.owner(fence.Group)
	if err != nil {
		return err
	}
	return owner.TransferSplitSourceLeadership(ctx, fence, target)
}

// TransferSchemaLeadership hands leadership to another voter before replacing
// the local SQL generation. Selecting the voter inside the serialized owner
// binds the choice to the same committed ConfState used by the transfer.
func (owner *Owner) TransferSchemaLeadership(ctx context.Context, fence ServingFence) error {
	if owner == nil || ctx == nil {
		return ErrInvalidOwner
	}
	_, err := owner.enqueue(ctx, ownerRequest{kind: requestSchemaLeadershipTransfer,
		group: fence.Group, fence: fence, reply: make(chan ownerReply, 1),
		transferDelivery: &transferDelivery{}})
	if errors.Is(err, errOwnerTransferCanceled) {
		return context.Cause(ctx)
	}
	return err
}

func (owners *ExecutionOwners) TransferSchemaLeadership(ctx context.Context, fence ServingFence) error {
	owner, err := owners.owner(fence.Group)
	if err != nil {
		return err
	}
	return owner.TransferSchemaLeadership(ctx, fence)
}

func (owner *Owner) schemaLeadershipTarget(fence ServingFence) (uint64, error) {
	member, found := owner.members[fence.Group]
	if !found || !servingFenceMatchesIdentity(fence, member) {
		return 0, ErrServingFence
	}
	publication, err := owner.host.Publication(fence.Group)
	if err != nil || publication.ReplicaSetVersion != fence.Command.ReplicaSetVersion ||
		publication.ConfState == nil || len(publication.ConfState.GetVotersOutgoing()) != 0 {
		return 0, errors.Join(ErrServingFence, err)
	}
	for _, voter := range publication.ConfState.GetVoters() {
		if voter != fence.MemberID {
			return voter, nil
		}
	}
	return 0, ErrServingFence
}

func (owner *Owner) validateLeaderTransferAdmission(fence ServingFence, target uint64) error {
	if target == 0 || target == fence.MemberID {
		return ErrServingFence
	}
	member, found := owner.members[fence.Group]
	if !found || !servingFenceMatchesIdentity(fence, member) {
		return ErrServingFence
	}
	publication, err := owner.host.Publication(fence.Group)
	if err != nil || publication.ReplicaSetVersion != fence.Command.ReplicaSetVersion ||
		publication.ConfState == nil || len(publication.ConfState.GetVotersOutgoing()) != 0 ||
		!containsSorted(publication.ConfState.GetVoters(), target) {
		return errors.Join(ErrServingFence, err)
	}
	status, err := owner.host.Status(fence.Group)
	if err != nil {
		return err
	}
	if status.MemberID != fence.MemberID || status.LeaderID != fence.MemberID || status.Term != fence.Term {
		return &NotLeaderError{Status: status}
	}
	progress, found, err := owner.host.Progress(fence.Group, target)
	if err != nil {
		return err
	}
	if !caughtUp(progress, found, status.Commit, false) {
		return ErrMembershipNotCaughtUp
	}
	return nil
}

func (owner *Owner) prepareLeaderTransferAdmission(
	fence ServingFence, target uint64,
) (raftmember.LeaderTransferGuard, error) {
	if err := owner.validateLeaderTransferAdmission(fence, target); err != nil {
		return raftmember.LeaderTransferGuard{}, err
	}
	return owner.host.PrepareLeaderTransfer(fence.Group, target)
}
