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

func rf3TargetPublicationSettlementFixture() (
	replicacontrol.Observation,
	replicacontrol.Request,
	raftservice.CommandFence,
	replicacontrol.Observation,
) {
	before, membership := rf3MembershipSettlementFixture(raftservice.MembershipPromoteVoter)
	request := replicacontrol.Request{
		Operation: [32]byte{0x41}, Step: [32]byte{0x42},
		Group: before.Request.Group, TargetMember: membership.TargetMember,
		ExpectedReplicaSetVersion: membership.ExpectedReplicaSetVersion,
	}
	before.Request = request
	before.Status.MemberID = request.TargetMember
	before.State.Binding.AllocationGeneration = 7
	before.State.SnapshotBaseDigest[0] = 0x43
	promoted := raftservice.CommandFence{ReplicaSetVersion: 12}
	ready := before
	ready.Request.ExpectedReplicaSetVersion = promoted.ReplicaSetVersion
	ready.State.ConfState = &pb.ConfState{Voters: []uint64{1, 2, 3, 4}, AutoLeave: new(false)}
	ready.State.Applied = 15
	ready.State.ReplicaSetVersion = promoted.ReplicaSetVersion
	ready.Publication.Applied = ready.State.Applied
	ready.Publication.ReplicaSetVersion = ready.State.ReplicaSetVersion
	ready.Publication.ConfState = proto.Clone(ready.State.ConfState).(*pb.ConfState)
	ready.Status.Applied = ready.State.Applied
	return before, request, promoted, ready
}

func TestRF3TargetPublicationConvergesThroughDiscovery(t *testing.T) {
	before, request, promoted, ready := rf3TargetPublicationSettlementFixture()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var calls int
	var attemptedVersions []uint64
	got, err := rf3AwaitTargetPublication(ctx, before, request, promoted,
		func(_ context.Context, attempt replicacontrol.Request) (replicacontrol.Observation, error) {
			calls++
			attemptedVersions = append(attemptedVersions, attempt.ExpectedReplicaSetVersion)
			switch calls {
			case 1:
				return before, replicacontrol.ErrStale
			case 2:
				return before, nil
			default:
				return ready, nil
			}
		})
	if err != nil || calls != 3 {
		t.Fatalf("target publication calls=%d err=%v", calls, err)
	}
	if got.State.ReplicaSetVersion != promoted.ReplicaSetVersion ||
		got.Status.MemberID != request.TargetMember {
		t.Fatalf("target publication=%+v", got)
	}
	for index, version := range attemptedVersions {
		if version != 0 {
			t.Fatalf("attempt %d pinned expected replica-set version=%d", index+1, version)
		}
	}
	if request.ExpectedReplicaSetVersion == 0 {
		t.Fatal("discovery helper mutated caller request")
	}
}

func TestRF3TargetPublicationRejectsWrongCutsAndRetainsDiagnostics(t *testing.T) {
	before, request, promoted, ready := rf3TargetPublicationSettlementFixture()
	mutations := map[string]func(*replicacontrol.Observation){
		"wrong operation": func(observation *replicacontrol.Observation) {
			observation.Request.Operation[0]++
		},
		"wrong step": func(observation *replicacontrol.Observation) {
			observation.Request.Step[0]++
		},
		"wrong group": func(observation *replicacontrol.Observation) {
			observation.Request.Group.GroupID[0]++
		},
		"wrong target": func(observation *replicacontrol.Observation) {
			observation.Request.TargetMember++
		},
		"wrong discovered version": func(observation *replicacontrol.Observation) {
			observation.Request.ExpectedReplicaSetVersion++
		},
		"wrong status member": func(observation *replicacontrol.Observation) {
			observation.Status.MemberID++
		},
		"wrong status applied": func(observation *replicacontrol.Observation) {
			observation.Status.Applied--
		},
		"wrong state version": func(observation *replicacontrol.Observation) {
			observation.State.ReplicaSetVersion++
		},
		"wrong publication version": func(observation *replicacontrol.Observation) {
			observation.Publication.ReplicaSetVersion++
		},
		"state below config": func(observation *replicacontrol.Observation) {
			observation.State.Applied = promoted.ReplicaSetVersion - 1
		},
		"publication below config": func(observation *replicacontrol.Observation) {
			observation.Publication.Applied = promoted.ReplicaSetVersion - 1
		},
		"publication applied differs from state": func(observation *replicacontrol.Observation) {
			observation.Publication.Applied--
		},
		"publication data chain differs from state": func(observation *replicacontrol.Observation) {
			observation.Publication.DataChainDigest[0]++
		},
		"publication configuration differs from state": func(observation *replicacontrol.Observation) {
			observation.Publication.ConfState.Voters[0]++
		},
		"binding changed": func(observation *replicacontrol.Observation) {
			observation.State.Binding.AllocationGeneration++
		},
		"snapshot digest changed": func(observation *replicacontrol.Observation) {
			observation.State.SnapshotBaseDigest[0]++
		},
		"target remains learner": func(observation *replicacontrol.Observation) {
			observation.State.ConfState = &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}
			observation.Publication.ConfState = proto.Clone(observation.State.ConfState).(*pb.ConfState)
		},
		"target is not a voter": func(observation *replicacontrol.Observation) {
			observation.State.ConfState = &pb.ConfState{Voters: []uint64{1, 2, 3}}
			observation.Publication.ConfState = proto.Clone(observation.State.ConfState).(*pb.ConfState)
		},
		"promotion adds another voter": func(observation *replicacontrol.Observation) {
			observation.State.ConfState.Voters = append(observation.State.ConfState.Voters, 5)
			observation.Publication.ConfState = proto.Clone(observation.State.ConfState).(*pb.ConfState)
		},
		"promotion removes an existing voter": func(observation *replicacontrol.Observation) {
			observation.State.ConfState.Voters = observation.State.ConfState.Voters[1:]
			observation.Publication.ConfState = proto.Clone(observation.State.ConfState).(*pb.ConfState)
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			bad := ready
			bad.State.ConfState = proto.Clone(ready.State.ConfState).(*pb.ConfState)
			bad.Publication.ConfState = proto.Clone(ready.Publication.ConfState).(*pb.ConfState)
			mutate(&bad)
			if rf3TargetPublicationMatches(before, request, promoted, bad) {
				t.Fatal("wrong target publication cut was accepted")
			}
		})
	}

	bad := ready
	bad.Request.Group.GroupID[0]++
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	calls := 0
	_, err := rf3AwaitTargetPublication(ctx, before, request, promoted,
		func(_ context.Context, attempt replicacontrol.Request) (replicacontrol.Observation, error) {
			calls++
			if attempt.ExpectedReplicaSetVersion != 0 {
				t.Fatalf("attempt pinned expected replica-set version=%d", attempt.ExpectedReplicaSetVersion)
			}
			return bad, errors.New("transient observation failure")
		})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wrong-cut timeout error=%v", err)
	}
	var diagnostics *rf3TargetPublicationError
	if !errors.As(err, &diagnostics) {
		t.Fatalf("timeout error omitted diagnostics: %v", err)
	}
	if diagnostics.attempts != calls || diagnostics.attempts == 0 ||
		diagnostics.lastObservation.Request.Group != bad.Request.Group ||
		diagnostics.lastError == nil || diagnostics.lastError.Error() != "transient observation failure" {
		t.Fatalf("timeout diagnostics=%+v calls=%d", diagnostics, calls)
	}
}
