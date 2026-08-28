//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/shardservice"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRF3MembershipSettlementWaitsForExactConfiguration(t *testing.T) {
	for _, kind := range []raftservice.MembershipKind{raftservice.MembershipAddLearner, raftservice.MembershipPromoteVoter,
		raftservice.MembershipRemoveVoter, raftservice.MembershipTransferLeader} {
		t.Run(string(rune('0'+kind)), func(t *testing.T) {
			before, request := rf3MembershipSettlementFixture(kind)
			original := proto.Clone(before.State.ConfState)
			expected, version, err := rf3ExpectedMembership(before, request)
			if err != nil {
				t.Fatal(err)
			}
			want := &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}
			switch kind {
			case raftservice.MembershipAddLearner:
				want = &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}
			case raftservice.MembershipRemoveVoter:
				want = &pb.ConfState{Voters: []uint64{2, 3, 4}}
			}
			if kind != raftservice.MembershipTransferLeader {
				want.AutoLeave = new(false)
			}
			if !proto.Equal(expected, want) || !proto.Equal(before.State.ConfState, original) {
				t.Fatal("wrong roster transformation or mutated original cut")
			}
			observed := before
			if kind != raftservice.MembershipTransferLeader {
				version = 12 // configuration index, not before version + 1
			}
			observed.State.ConfState, observed.State.ReplicaSetVersion = expected, version
			observed.State.Applied = 15
			observed.Status.Term++
			observed.Status.LeaderID = request.TargetMember
			state := shardservice.ReplicatedMemberState{Fence: shardservice.ReplicatedFence{
				Group: before.Request.Group, MemberID: before.Status.MemberID, Term: observed.Status.Term,
				Command: raftservice.CommandFence{ReplicaSetVersion: version}},
				Applied: observed.State.Applied, LeaderID: request.TargetMember}
			calls := 0
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, err = rf3AwaitMembershipSettlement(ctx, before, request,
				func(context.Context) (shardservice.ReplicatedMemberState, replicacontrol.Observation, error) {
					calls++
					if calls == 1 {
						return state, before, nil // accepted, but not applied yet
					}
					return state, observed, nil
				})
			if err != nil || calls != 2 {
				t.Fatalf("settlement calls=%d err=%v", calls, err)
			}
			mutations := map[string]func(*replicacontrol.Observation, *shardservice.ReplicatedMemberState){
				"other roster": func(o *replicacontrol.Observation, _ *shardservice.ReplicatedMemberState) {
					o.State.ConfState.Voters[0] = 99
				},
				"mismatched native version": func(o *replicacontrol.Observation, s *shardservice.ReplicatedMemberState) {
					o.State.ReplicaSetVersion++
				},
				"foreign group": func(o *replicacontrol.Observation, _ *shardservice.ReplicatedMemberState) {
					o.Request.Group.GroupID[0]++
				},
				"foreign binding": func(o *replicacontrol.Observation, _ *shardservice.ReplicatedMemberState) {
					o.State.Binding.AllocationGeneration++
				},
				"foreign member": func(o *replicacontrol.Observation, _ *shardservice.ReplicatedMemberState) { o.Status.MemberID++ },
				"stale native":   func(_ *replicacontrol.Observation, s *shardservice.ReplicatedMemberState) { s.Applied = version - 1 },
			}
			if kind == raftservice.MembershipTransferLeader {
				mutations["old transfer term"] = func(o *replicacontrol.Observation, _ *shardservice.ReplicatedMemberState) {
					o.Status.Term = before.Status.Term
				}
				mutations["wrong transfer leader"] = func(o *replicacontrol.Observation, _ *shardservice.ReplicatedMemberState) {
					o.Status.LeaderID = request.SourceMember
				}
			}
			for name, mutate := range mutations {
				t.Run(name, func(t *testing.T) {
					bad, badState := observed, state
					bad.State.ConfState = proto.Clone(expected).(*pb.ConfState)
					mutate(&bad, &badState)
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					_, err := rf3AwaitMembershipSettlement(ctx, before, request,
						func(context.Context) (shardservice.ReplicatedMemberState, replicacontrol.Observation, error) {
							cancel()
							return badState, bad, nil
						})
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("wrong committed cut accepted: %v", err)
					}
				})
			}
		})
	}
}

func rf3MembershipSettlementFixture(kind raftservice.MembershipKind) (replicacontrol.Observation, shardservice.ReplicatedMembershipRequest) {
	conf := &pb.ConfState{Voters: []uint64{1, 2, 3}}
	if kind == raftservice.MembershipPromoteVoter {
		conf.Learners = []uint64{4}
	}
	if kind == raftservice.MembershipRemoveVoter || kind == raftservice.MembershipTransferLeader {
		conf.Voters = append(conf.Voters, 4)
	}
	before := replicacontrol.Observation{Request: replicacontrol.Request{Group: rf3CommandGroup()},
		State:  replicatedstate.State{Applied: 10, ReplicaSetVersion: 3, ConfState: conf},
		Status: raftmember.RuntimeStatus{MemberID: 1, LeaderID: 1, Term: 5}}
	request := shardservice.ReplicatedMembershipRequest{Kind: kind, SourceMember: 1, TargetMember: 4, ExpectedReplicaSetVersion: 3}
	return before, request
}

func TestRF3MembershipSettlementMatchesAppliedRaftConfiguration(t *testing.T) {
	before, request := rf3MembershipSettlementFixture(raftservice.MembershipAddLearner)
	before.State.ConfState = rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}).Snapshot.GetMetadata().GetConfState()
	if before.State.ConfState.AutoLeave != nil {
		t.Fatal("fixture must cover the omitted bootstrap AutoLeave field")
	}
	storage := raft.NewMemoryStorage()
	if err := storage.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
		Index: new(uint64(1)), Term: new(uint64(1)), ConfState: proto.Clone(before.State.ConfState).(*pb.ConfState),
	}}); err != nil {
		t.Fatal(err)
	}
	node, err := raft.NewRawNode(&raft.Config{ID: before.Status.MemberID, ElectionTick: 10, HeartbeatTick: 1,
		Storage: storage, Applied: 1, MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256})
	if err != nil {
		t.Fatal(err)
	}
	applied := node.ApplyConfChange(&pb.ConfChange{
		Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: &request.TargetMember,
	})
	if applied.AutoLeave == nil || applied.GetAutoLeave() {
		t.Fatal("Raft configuration did not materialize the non-joint AutoLeave field")
	}
	raw, err := proto.Marshal(applied)
	if err != nil {
		t.Fatal(err)
	}
	var decoded pb.ConfState
	if err := proto.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	bootstrapSpelling := proto.Clone(before.State.ConfState).(*pb.ConfState)
	bootstrapSpelling.Learners = []uint64{request.TargetMember}
	if proto.Equal(bootstrapSpelling, &decoded) {
		t.Fatal("fixture no longer reproduces the optional-field presence mismatch")
	}
	observed := before
	observed.State.ConfState = &decoded
	observed.State.ReplicaSetVersion, observed.State.Applied = 12, 12
	state := shardservice.ReplicatedMemberState{Fence: shardservice.ReplicatedFence{
		Group: before.Request.Group, MemberID: before.Status.MemberID, Term: before.Status.Term,
		Command: raftservice.CommandFence{ReplicaSetVersion: observed.State.ReplicaSetVersion}},
		Applied: observed.State.Applied, LeaderID: before.Status.LeaderID}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	_, err = rf3AwaitMembershipSettlement(ctx, before, request,
		func(context.Context) (shardservice.ReplicatedMemberState, replicacontrol.Observation, error) {
			calls++
			cancel() // one exact observation must settle, without a timed retry
			return state, observed, nil
		})
	if err != nil || calls != 1 {
		t.Fatalf("applied Raft configuration refused: calls=%d error=%v", calls, err)
	}
	if before.State.ConfState.AutoLeave != nil {
		t.Fatal("settlement changed the original bootstrap proof")
	}
}
