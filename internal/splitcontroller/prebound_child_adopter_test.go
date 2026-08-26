package splitcontroller

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	pb "go.etcd.io/raft/v3/raftpb"
)

type recordingExecutionGroupRegistrar struct {
	calls  int
	roster []rafttransport.Member
	group  raftservice.ExecutionGroup
}

func (registrar *recordingExecutionGroupRegistrar) RegisterExecutionGroup(
	roster []rafttransport.Member,
	group raftservice.ExecutionGroup,
) error {
	registrar.calls++
	registrar.roster = roster
	registrar.group = group
	return nil
}

func TestPreboundChildRuntimeAdopterFreezesRF3Authority(t *testing.T) {
	plan, _, _, _ := testPlanWithChildLeaders(t, rf3ChildLeaders())
	target, ok := plan.Target(1)
	if !ok {
		t.Fatal("missing child target")
	}
	roster := testChildExecutionRoster(target)
	registrar := new(recordingExecutionGroupRegistrar)
	adopter, err := NewPreboundChildRuntimeAdopter(
		registrar, plan.OperationID(), 1, target, roster,
	)
	if err != nil {
		t.Fatal(err)
	}
	if adopter.command.ReplicaSetVersion != roster[0].ReplicaSetVersion ||
		adopter.command.ActivePolicyGeneration != target.Authority.ActivePolicyGeneration ||
		adopter.command.ProtectionEpoch != target.Authority.ProtectionEpoch ||
		adopter.command.OwnershipEpoch != target.Authority.OwnershipEpoch ||
		adopter.command.SchemaGeneration != target.Authority.SchemaGeneration ||
		adopter.command.RelationManifestDigest != target.SQL.RelationManifestDigest ||
		adopter.command.RoutingVersion != target.Authority.RoutingVersion ||
		adopter.command.RouteGeneration != target.Authority.RouteGeneration {
		t.Fatalf("derived command fence = %+v", adopter.command)
	}
	for index := 1; index < len(adopter.roster); index++ {
		if adopter.roster[index-1].MemberID >= adopter.roster[index].MemberID {
			t.Fatalf("roster is not canonical: %+v", adopter.roster)
		}
	}
	roster[0].MemberID++
	if adopter.roster[0].MemberID == roster[0].MemberID {
		t.Fatal("adopter retained caller roster storage")
	}
	if err = adopter.AdoptSplitChild(
		context.Background(), OperationID{0xff}, 1, PreparedChildRuntime{},
	); !errors.Is(err, ErrRuntimeStore) || registrar.calls != 0 {
		t.Fatalf("wrong operation err=%v calls=%d", err, registrar.calls)
	}
}

func TestPreboundChildRuntimeAdopterRejectsMixedOrIncompleteRoster(t *testing.T) {
	plan, _, _, _ := testPlanWithChildLeaders(t, rf3ChildLeaders())
	target, _ := plan.Target(1)
	valid := testChildExecutionRoster(target)
	tests := []struct {
		name string
		edit func([]rafttransport.Member) []rafttransport.Member
	}{
		{name: "not RF3", edit: func(roster []rafttransport.Member) []rafttransport.Member {
			return roster[:2]
		}},
		{name: "mixed group", edit: func(roster []rafttransport.Member) []rafttransport.Member {
			roster[2].Group.GroupID[0] ^= 0xff
			return roster
		}},
		{name: "mixed version", edit: func(roster []rafttransport.Member) []rafttransport.Member {
			roster[2].ReplicaSetVersion++
			return roster
		}},
		{name: "learner", edit: func(roster []rafttransport.Member) []rafttransport.Member {
			roster[2].Role = rafttransport.MemberLearner
			return roster
		}},
		{name: "duplicate node", edit: func(roster []rafttransport.Member) []rafttransport.Member {
			roster[2].Node = roster[1].Node
			return roster
		}},
		{name: "missing local member", edit: func(roster []rafttransport.Member) []rafttransport.Member {
			for index := range roster {
				if roster[index].MemberID == target.WAL.MemberID {
					roster[index].MemberID += 100
				}
			}
			return roster
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			roster := append([]rafttransport.Member(nil), valid...)
			if adopter, err := NewPreboundChildRuntimeAdopter(
				new(recordingExecutionGroupRegistrar), plan.OperationID(), 1, target,
				test.edit(roster),
			); adopter != nil || !errors.Is(err, ErrRuntimeStore) {
				t.Fatalf("adopter=%v err=%v", adopter, err)
			}
		})
	}
}

func TestChildPublicationMustMatchPreboundRF3Roster(t *testing.T) {
	plan, _, _, _ := testPlanWithChildLeaders(t, rf3ChildLeaders())
	target, _ := plan.Target(1)
	roster := testChildExecutionRoster(target)
	conf := &pb.ConfState{Voters: []uint64{
		roster[1].MemberID, roster[2].MemberID, roster[0].MemberID,
	}}
	if !childPublicationMatchesRoster(1, conf, roster) {
		t.Fatal("exact RF3 publication rejected")
	}
	conf.Learners = []uint64{99}
	if childPublicationMatchesRoster(1, conf, roster) {
		t.Fatal("publication with learner accepted")
	}
	conf.Learners = nil
	conf.Voters = conf.Voters[:2]
	if childPublicationMatchesRoster(1, conf, roster) {
		t.Fatal("RF2 publication accepted")
	}
	conf.Voters = []uint64{roster[0].MemberID, roster[1].MemberID, 99}
	if childPublicationMatchesRoster(1, conf, roster) {
		t.Fatal("foreign voter accepted")
	}
}

func rf3ChildLeaders() []distribution.EndpointID {
	return []distribution.EndpointID{"node-b", "node-c", "node-d"}
}

func testChildExecutionRoster(target ChildTarget) []rafttransport.Member {
	group := raftmember.GroupKey{
		ClusterID: target.WAL.ClusterID, ClusterIncarnation: target.WAL.ClusterIncarnation,
		TopologyRecoveryEpoch: target.TopologyRecoveryEpoch,
		ShardIncarnation:      target.WAL.ShardIncarnation, GroupID: target.WAL.GroupID,
	}
	result := make([]rafttransport.Member, len(target.Replicas))
	for index, replica := range target.Replicas {
		result[index] = rafttransport.Member{Group: group, ReplicaSetVersion: 1,
			MemberID: replica.Member, Node: replica.Node, Role: rafttransport.MemberVoter}
	}
	if len(result) == 3 {
		result[0], result[2] = result[2], result[0]
	}
	return result
}
