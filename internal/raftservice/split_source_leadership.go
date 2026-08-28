package raftservice

import (
	"context"
	"errors"
)

// TransferSplitSourceLeadership is an internal admitted-split capability, not
// a public membership API. It preserves the voter set and admits a handoff
// only from the exact current leader fence to another existing voter.
func (owner *Owner) TransferSplitSourceLeadership(ctx context.Context, fence ServingFence, target uint64) error {
	if owner == nil || ctx == nil || target == 0 || target == fence.MemberID {
		return ErrInvalidOwner
	}
	_, err := owner.enqueue(ctx, ownerRequest{kind: requestSplitSourceLeadership,
		group: fence.Group, fence: fence, targetMember: target, reply: make(chan ownerReply, 1)})
	return err
}

func (owners *ExecutionOwners) TransferSplitSourceLeadership(ctx context.Context, fence ServingFence, target uint64) error {
	owner, err := owners.owner(fence.Group)
	if err != nil {
		return err
	}
	return owner.TransferSplitSourceLeadership(ctx, fence, target)
}

func (owner *Owner) transferSplitSourceLeadership(fence ServingFence, target uint64) error {
	member, found := owner.members[fence.Group]
	if !found || !servingFenceMatchesIdentity(fence, member) || target == fence.MemberID {
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
	return owner.host.TransferLeader(fence.Group, target)
}
