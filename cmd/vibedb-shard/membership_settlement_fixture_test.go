//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var errRF3MembershipSettlement = errors.New("RF3 fixture membership settlement differs from admitted mutation")

func rf3ExpectedMembership(before replicacontrol.Observation, request shardservice.ReplicatedMembershipRequest) (*pb.ConfState, uint64, error) {
	if before.State.ConfState == nil || before.State.ReplicaSetVersion == 0 ||
		before.State.ReplicaSetVersion != request.ExpectedReplicaSetVersion ||
		request.SourceMember == 0 || request.TargetMember == 0 || request.SourceMember == request.TargetMember ||
		!slices.Contains(before.State.ConfState.GetVoters(), request.SourceMember) ||
		len(before.State.ConfState.GetVotersOutgoing()) != 0 || len(before.State.ConfState.GetLearnersNext()) != 0 || before.State.ConfState.GetAutoLeave() {
		return nil, 0, errRF3MembershipSettlement
	}
	expected := proto.Clone(before.State.ConfState).(*pb.ConfState)
	version := request.ExpectedReplicaSetVersion
	isVoter := slices.Contains(expected.GetVoters(), request.TargetMember)
	isLearner := slices.Contains(expected.GetLearners(), request.TargetMember)
	switch request.Kind {
	case raftservice.MembershipAddLearner:
		if isVoter || isLearner {
			return nil, 0, errRF3MembershipSettlement
		}
		expected.Learners = append(expected.Learners, request.TargetMember)
		slices.Sort(expected.Learners)
	case raftservice.MembershipPromoteVoter:
		if isVoter || !isLearner {
			return nil, 0, errRF3MembershipSettlement
		}
		expected.Learners = slices.DeleteFunc(expected.Learners, func(member uint64) bool { return member == request.TargetMember })
		expected.Voters = append(expected.Voters, request.TargetMember)
		slices.Sort(expected.Voters)
	case raftservice.MembershipRemoveVoter:
		if !isVoter || isLearner {
			return nil, 0, errRF3MembershipSettlement
		}
		expected.Voters = slices.DeleteFunc(expected.Voters, func(member uint64) bool { return member == request.SourceMember })
	case raftservice.MembershipTransferLeader:
		if !isVoter || isLearner || before.Status.LeaderID != request.SourceMember {
			return nil, 0, errRF3MembershipSettlement
		}
		return expected, version, nil
	default:
		return nil, 0, errRF3MembershipSettlement
	}
	// Applied ConfChange results come from Raft's tracker, which explicitly
	// materializes AutoLeave=false for a non-joint configuration. A bootstrap
	// may omit this optional protobuf field; preserve exact proof comparison
	// by predicting the applied spelling, not by ignoring field presence.
	expected.AutoLeave = new(false)
	if version == ^uint64(0) {
		return nil, 0, errRF3MembershipSettlement
	}
	// Configuration versions are applied log indexes, not adjacent counters.
	// The return value is only the pre-admission version to compare against.
	return expected, version, nil
}

// This callback is deliberately observation-only. Admission has finished and
// cannot be repeated by the bounded settlement loop.
type rf3MembershipObservationFunc func(context.Context) (shardservice.ReplicatedMemberState, replicacontrol.Observation, error)

func rf3MembershipNetworkObserver(observer *replicacontrol.Client, address string, node rafttransport.NodeID,
	profile *rafttransport.PeerTLS, authority serviceauthz.Authority, allocation uint64, request replicacontrol.Request,
) rf3MembershipObservationFunc {
	return func(ctx context.Context) (shardservice.ReplicatedMemberState, replicacontrol.Observation, error) {
		state, err := probeRF3CommandMember(ctx, address, node, profile, authority.Node, request.Group, allocation, authority.Generation)
		if err != nil {
			return state, replicacontrol.Observation{}, err
		}
		// ReplicaSetVersion is the configuration's log index, not a contiguous
		// counter. Bind the control observation to the exact native version.
		request.ExpectedReplicaSetVersion = state.Fence.Command.ReplicaSetVersion
		observed, err := observer.Observe(ctx, node, request)
		return state, observed, err
	}
}

func rf3AwaitMembershipSettlement(ctx context.Context, before replicacontrol.Observation, request shardservice.ReplicatedMembershipRequest,
	observe rf3MembershipObservationFunc,
) (shardservice.ReplicatedMemberState, error) {
	expected, version, err := rf3ExpectedMembership(before, request)
	if err != nil || observe == nil {
		return shardservice.ReplicatedMemberState{}, errors.Join(errRF3MembershipSettlement, err)
	}
	for {
		if err := context.Cause(ctx); err != nil {
			return shardservice.ReplicatedMemberState{}, err
		}
		state, observed, observeErr := observe(ctx)
		if observeErr == nil && observed.State.ReplicaSetVersion == state.Fence.Command.ReplicaSetVersion &&
			observed.State.Binding == before.State.Binding && observed.Request.Group == before.Request.Group &&
			observed.Status.MemberID == before.Status.MemberID && observed.State.Applied >= before.State.Applied &&
			proto.Equal(observed.State.ConfState, expected) &&
			state.Fence.Group == before.Request.Group && state.Fence.MemberID == before.Status.MemberID &&
			state.Applied >= observed.State.ReplicaSetVersion {
			if request.Kind == raftservice.MembershipTransferLeader {
				if observed.State.ReplicaSetVersion == version && observed.Status.LeaderID == request.TargetMember && observed.Status.Term > before.Status.Term &&
					state.LeaderID == request.TargetMember && state.Fence.Term >= observed.Status.Term {
					return state, nil
				}
			} else if observed.State.ReplicaSetVersion > version && observed.State.Applied > before.State.Applied {
				return state, nil
			}
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return shardservice.ReplicatedMemberState{}, context.Cause(ctx)
		case <-timer.C:
		}
	}
}
