//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"fmt"
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

type rf3TargetPublicationObservationFunc func(context.Context, replicacontrol.Request) (replicacontrol.Observation, error)

var errRF3TargetPublication = errors.New("RF3 target publication did not settle")

type rf3TargetPublicationError struct {
	cause           error
	attempts        int
	lastObservation replicacontrol.Observation
	lastError       error
}

func (err *rf3TargetPublicationError) Error() string {
	return fmt.Sprintf("%v after %d attempts: last observation=%+v last error=%v",
		errRF3TargetPublication, err.attempts, err.lastObservation, err.lastError)
}

func (err *rf3TargetPublicationError) Unwrap() error { return err.cause }

func rf3ExpectedTargetPromotion(before replicacontrol.Observation, target uint64) *pb.ConfState {
	conf := before.State.ConfState
	if conf == nil || target == 0 || slices.Contains(conf.GetVoters(), target) ||
		!slices.Contains(conf.GetLearners(), target) || len(conf.GetVotersOutgoing()) != 0 ||
		len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
		return nil
	}
	expected := proto.Clone(conf).(*pb.ConfState)
	expected.Learners = slices.DeleteFunc(expected.Learners, func(member uint64) bool { return member == target })
	expected.Voters = append(expected.Voters, target)
	slices.Sort(expected.Voters)
	expected.AutoLeave = new(false)
	return expected
}

func rf3TargetPublicationMatches(
	before replicacontrol.Observation,
	request replicacontrol.Request,
	promoted raftservice.CommandFence,
	observed replicacontrol.Observation,
) bool {
	expectedRequest := request
	expectedRequest.ExpectedReplicaSetVersion = promoted.ReplicaSetVersion
	expectedConf := rf3ExpectedTargetPromotion(before, request.TargetMember)
	zeroDigest := [32]byte{}
	return before.Request == request && request.ExpectedReplicaSetVersion == before.State.ReplicaSetVersion &&
		promoted.ReplicaSetVersion > before.State.ReplicaSetVersion && observed.Request == expectedRequest &&
		observed.Status.MemberID == request.TargetMember &&
		observed.State.ReplicaSetVersion == promoted.ReplicaSetVersion &&
		observed.Publication.ReplicaSetVersion == promoted.ReplicaSetVersion &&
		observed.State.Applied >= promoted.ReplicaSetVersion &&
		observed.Publication.Applied == observed.State.Applied &&
		observed.Status.Applied == observed.State.Applied &&
		observed.Publication.DataChainDigest == observed.State.DataChainDigest &&
		expectedConf != nil && proto.Equal(observed.State.ConfState, expectedConf) &&
		proto.Equal(observed.Publication.ConfState, observed.State.ConfState) &&
		observed.State.Binding == before.State.Binding &&
		before.State.SnapshotBaseDigest != zeroDigest &&
		observed.State.SnapshotBaseDigest == before.State.SnapshotBaseDigest
}

func rf3AwaitTargetPublication(
	ctx context.Context,
	before replicacontrol.Observation,
	request replicacontrol.Request,
	promoted raftservice.CommandFence,
	observe rf3TargetPublicationObservationFunc,
) (replicacontrol.Observation, error) {
	if ctx == nil || observe == nil || request.TargetMember == 0 || promoted.ReplicaSetVersion == 0 {
		return replicacontrol.Observation{}, &rf3TargetPublicationError{
			cause: errRF3TargetPublication,
		}
	}
	if before.State.SnapshotBaseDigest == ([32]byte{}) {
		return replicacontrol.Observation{}, &rf3TargetPublicationError{
			cause:     errRF3TargetPublication,
			lastError: errors.New("target publication has no snapshot-base digest"),
		}
	}
	var (
		attempts        int
		lastObservation replicacontrol.Observation
		lastError       error
	)
	for {
		if cause := context.Cause(ctx); cause != nil {
			return replicacontrol.Observation{}, &rf3TargetPublicationError{
				cause: cause, attempts: attempts, lastObservation: lastObservation, lastError: lastError,
			}
		}
		attempt := request
		attempt.ExpectedReplicaSetVersion = 0
		attempts++
		observed, err := observe(ctx, attempt)
		lastObservation, lastError = observed, err
		if err == nil && rf3TargetPublicationMatches(before, request, promoted, observed) {
			return observed, nil
		}
		if cause := context.Cause(ctx); cause != nil {
			return replicacontrol.Observation{}, &rf3TargetPublicationError{
				cause: cause, attempts: attempts, lastObservation: lastObservation, lastError: lastError,
			}
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			cause := context.Cause(ctx)
			if cause == nil {
				cause = context.Canceled
			}
			return replicacontrol.Observation{}, &rf3TargetPublicationError{
				cause: cause, attempts: attempts, lastObservation: lastObservation, lastError: lastError,
			}
		case <-timer.C:
		}
	}
}

func rf3MembershipNetworkObserver(observer *replicacontrol.Client, address string, node rafttransport.NodeID,
	profile *rafttransport.PeerTLS, authority serviceauthz.Authority, allocation uint64, request replicacontrol.Request,
) rf3MembershipObservationFunc {
	return func(ctx context.Context) (shardservice.ReplicatedMemberState, replicacontrol.Observation, error) {
		state, err := probeRF3CommandMember(ctx, address, node, profile, authority.Node, request.Group, allocation, authority.Generation)
		if err != nil {
			return state, replicacontrol.Observation{}, err
		}
		// The command endpoint can expose the applied configuration before the
		// publication read by replica control catches up. Discover that local
		// publication instead of pinning the read to a transiently newer command
		// fence; rf3AwaitMembershipSettlement still validates its exact version,
		// configuration, binding, member, group, and applied cut below.
		request.ExpectedReplicaSetVersion = 0
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
