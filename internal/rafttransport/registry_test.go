package rafttransport

import (
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
)

func TestStaticRegistryRF1RF2RF3(t *testing.T) {
	for _, test := range []struct {
		name     string
		replicas int
	}{{"RF1", 1}, {"RF2", 2}, {"RF3", 3}} {
		t.Run(test.name, func(t *testing.T) {
			replicas := test.replicas
			group := testGroup(byte(replicas))
			members := make([]Member, replicas)
			for i := range members {
				members[i] = Member{
					Group: group, ReplicaSetVersion: 1,
					MemberID: uint64(i + 11), Node: testNode(byte(i + 1)), Role: MemberVoter,
				}
			}
			registry, err := NewStaticRegistry(testNode(1), members, Limits{MaxGroups: 1, MaxMembers: replicas})
			if err != nil {
				t.Fatalf("NewStaticRegistry: %v", err)
			}
			if got := registry.LocalNode(); got != testNode(1) {
				t.Fatalf("LocalNode = %x, want %x", got, testNode(1))
			}
			if got, err := registry.LocalMember(group); err != nil || got != 11 {
				t.Fatalf("LocalMember = %d, %v; want 11, nil", got, err)
			}
			for _, member := range members {
				if got, err := registry.Node(group, member.MemberID); err != nil || got != member.Node {
					t.Fatalf("Node(%d) = %x, %v; want %x, nil", member.MemberID, got, err, member.Node)
				}
				if got, err := registry.Member(group, member.Node); err != nil || got != member.MemberID {
					t.Fatalf("Member(%x) = %d, %v; want %d, nil", member.Node, got, err, member.MemberID)
				}
			}
		})
	}
}

func TestStaticRegistryAcceptsUnsortedMultipleGroups(t *testing.T) {
	group1 := testGroup(1)
	group2 := testGroupInDomain(group1, 2)
	members := []Member{
		{Group: group2, ReplicaSetVersion: 1, MemberID: 22, Node: testNode(2), Role: MemberVoter},
		{Group: group1, ReplicaSetVersion: 1, MemberID: 12, Node: testNode(2), Role: MemberVoter},
		{Group: group2, ReplicaSetVersion: 1, MemberID: 21, Node: testNode(1), Role: MemberVoter},
		{Group: group1, ReplicaSetVersion: 1, MemberID: 11, Node: testNode(1), Role: MemberVoter},
	}
	registry, err := NewStaticRegistry(testNode(1), members, Limits{MaxGroups: 2, MaxMembers: 4})
	if err != nil {
		t.Fatalf("NewStaticRegistry: %v", err)
	}
	if got, err := registry.LocalMember(group1); err != nil || got != 11 {
		t.Fatalf("group 1 local = %d, %v; want 11, nil", got, err)
	}
	if got, err := registry.LocalMember(group2); err != nil || got != 21 {
		t.Fatalf("group 2 local = %d, %v; want 21, nil", got, err)
	}
}

func TestStaticRegistryRequiresOneNonemptyTrustDomain(t *testing.T) {
	limits := Limits{MaxGroups: 2, MaxMembers: 2}
	if _, err := NewStaticRegistry(testNode(1), nil, limits); !errors.Is(err, ErrInvalidGroup) {
		t.Fatalf("empty registry error = %v, want ErrInvalidGroup", err)
	}
	group := testGroup(7)
	member := Member{
		Group: group, ReplicaSetVersion: 1, MemberID: 11,
		Node: testNode(1), Role: MemberVoter,
	}
	registry, err := NewStaticRegistry(testNode(1), []Member{member}, limits)
	if err != nil {
		t.Fatal(err)
	}
	want := TrustDomain{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
	}
	if got := registry.TrustDomain(); got != want {
		t.Fatalf("TrustDomain = %+v, want %+v", got, want)
	}

	for _, coordinate := range []string{"cluster ID", "cluster incarnation"} {
		t.Run(coordinate, func(t *testing.T) {
			other := testGroupInDomain(group, 8)
			if coordinate == "cluster ID" {
				other.ClusterID[0]++
			} else {
				other.ClusterIncarnation[0]++
			}
			_, err := NewStaticRegistry(testNode(1), []Member{
				member,
				{
					Group: other, ReplicaSetVersion: 1, MemberID: 21,
					Node: testNode(1), Role: MemberVoter,
				},
			}, limits)
			if !errors.Is(err, ErrInvalidGroup) {
				t.Fatalf("mixed domain error = %v, want ErrInvalidGroup", err)
			}
		})
	}
}

func TestStaticRegistryValidatesLimitsBeforeMembers(t *testing.T) {
	valid := Limits{MaxGroups: 1, MaxMembers: 1}
	tests := []struct {
		name   string
		limits Limits
	}{
		{name: "zero groups", limits: Limits{MaxMembers: 1}},
		{name: "negative groups", limits: Limits{MaxGroups: -1, MaxMembers: 1}},
		{name: "absolute group bound", limits: Limits{MaxGroups: AbsoluteMaxGroups + 1, MaxMembers: 1}},
		{name: "zero members", limits: Limits{MaxGroups: 1}},
		{name: "negative members", limits: Limits{MaxGroups: 1, MaxMembers: -1}},
		{name: "group product", limits: Limits{MaxGroups: 1, MaxMembers: raftmodel.MaxConfStateMembers + 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewStaticRegistry(NodeID{}, []Member{{}}, test.limits)
			if !errors.Is(err, ErrInvalidLimits) {
				t.Fatalf("error = %v, want ErrInvalidLimits", err)
			}
		})
	}

	tooMany := make([]Member, 2)
	if _, err := NewStaticRegistry(NodeID{}, tooMany, valid); !errors.Is(err, ErrRegistryBound) {
		t.Fatalf("oversize error = %v, want ErrRegistryBound before invalid local node", err)
	}
}

func TestStaticRegistryAcceptsAbsoluteBounds(t *testing.T) {
	if AbsoluteMaxGroups != multiraft.AbsoluteMaxGroups {
		t.Fatalf("transport groups %d differ from Host groups %d", AbsoluteMaxGroups, multiraft.AbsoluteMaxGroups)
	}
	member := Member{Group: testGroup(1), ReplicaSetVersion: 1, MemberID: 1, Node: testNode(1), Role: MemberVoter}
	limits := Limits{
		MaxGroups:  AbsoluteMaxGroups,
		MaxMembers: AbsoluteMaxGroups * raftmodel.MaxConfStateMembers,
	}
	if _, err := NewStaticRegistry(testNode(1), []Member{member}, limits); err != nil {
		t.Fatalf("NewStaticRegistry at absolute bounds: %v", err)
	}

	limits.MaxMembers++
	if _, err := NewStaticRegistry(testNode(1), []Member{member}, limits); !errors.Is(err, ErrInvalidLimits) {
		t.Fatalf("MaxMembers above absolute bound error = %v, want ErrInvalidLimits", err)
	}
}

func TestStaticRegistryConstructsAndDirectlyLooksUpAbsoluteGroupCount(t *testing.T) {
	members := make([]Member, AbsoluteMaxGroups)
	domain := testGroup(1)
	for index := range members {
		group := testGroupInDomain(domain, index+1)
		members[index] = Member{
			Group: group, ReplicaSetVersion: 1, MemberID: uint64(index + 1),
			Node: testNode(1), Role: MemberVoter,
		}
	}
	registry, err := NewStaticRegistry(testNode(1), members, Limits{
		MaxGroups: AbsoluteMaxGroups, MaxMembers: AbsoluteMaxGroups,
	})
	if err != nil {
		t.Fatalf("NewStaticRegistry: %v", err)
	}
	for _, index := range []int{0, AbsoluteMaxGroups / 2, AbsoluteMaxGroups - 1} {
		if got, err := registry.LocalMember(members[index].Group); err != nil || got != members[index].MemberID {
			t.Fatalf("lookup %d = %d, %v", index, got, err)
		}
	}
}

func TestStaticRegistryEnrollmentDigestIsOrderIndependentAndExcludesDynamicAuthority(t *testing.T) {
	group := testGroup(1)
	members := []Member{
		{Group: group, ReplicaSetVersion: 7, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 7, MemberID: 2, Node: testNode(2), Role: MemberLearner},
	}
	open := func(input []Member) [32]byte {
		t.Helper()
		registry, err := NewStaticRegistry(testNode(1), input, Limits{MaxGroups: 1, MaxMembers: 2})
		if err != nil {
			t.Fatalf("NewStaticRegistry: %v", err)
		}
		digest, ok := registry.rosterDigest(group)
		if !ok {
			t.Fatal("roster digest missing")
		}
		return digest
	}
	want := open(members)
	reversed := slices.Clone(members)
	slices.Reverse(reversed)
	if got := open(reversed); got != want {
		t.Fatalf("order changed roster digest: %x != %x", got, want)
	}
	for _, mutate := range []func([]Member){
		func(rows []Member) {
			rows[0].ReplicaSetVersion++
			rows[1].ReplicaSetVersion++
		},
		func(rows []Member) { rows[1].Role = MemberVoter },
	} {
		changed := slices.Clone(members)
		mutate(changed)
		if got := open(changed); got != want {
			t.Fatalf("dynamic authority changed stable enrollment digest: %x", got)
		}
	}
	changed := slices.Clone(members)
	changed[1].Node = testNode(3)
	if got := open(changed); got == want {
		t.Fatalf("stable node mutation did not change enrollment digest: %x", got)
	}
}

func TestStaticRegistryValidatesIdentityCoordinates(t *testing.T) {
	valid := Member{Group: testGroup(1), ReplicaSetVersion: 1, MemberID: 1, Node: testNode(1), Role: MemberVoter}
	tests := []struct {
		name   string
		mutate func(*Member)
		want   error
	}{
		{name: "node", mutate: func(member *Member) { member.Node = NodeID{} }, want: ErrInvalidNode},
		{name: "member", mutate: func(member *Member) { member.MemberID = 0 }, want: ErrInvalidMember},
		{name: "local target", mutate: func(member *Member) { member.MemberID = math.MaxUint64 }, want: ErrInvalidMember},
		{name: "local thread", mutate: func(member *Member) { member.MemberID = math.MaxUint64 - 1 }, want: ErrInvalidMember},
		{name: "role", mutate: func(member *Member) { member.Role = 0 }, want: ErrInvalidRole},
		{name: "replica set", mutate: func(member *Member) { member.ReplicaSetVersion = 0 }, want: ErrReplicaSet},
		{name: "cluster", mutate: func(member *Member) { member.Group.ClusterID = [16]byte{} }, want: ErrInvalidGroup},
		{name: "cluster incarnation", mutate: func(member *Member) { member.Group.ClusterIncarnation = [16]byte{} }, want: ErrInvalidGroup},
		{name: "recovery epoch", mutate: func(member *Member) { member.Group.TopologyRecoveryEpoch = 0 }, want: ErrInvalidGroup},
		{name: "shard incarnation", mutate: func(member *Member) { member.Group.ShardIncarnation = [16]byte{} }, want: ErrInvalidGroup},
		{name: "group", mutate: func(member *Member) { member.Group.GroupID = [16]byte{} }, want: ErrInvalidGroup},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			member := valid
			test.mutate(&member)
			_, err := NewStaticRegistry(testNode(1), []Member{member}, Limits{MaxGroups: 1, MaxMembers: 1})
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}

	if _, err := NewStaticRegistry(NodeID{}, []Member{valid}, Limits{MaxGroups: 1, MaxMembers: 1}); !errors.Is(err, ErrInvalidNode) {
		t.Fatalf("zero local error = %v, want ErrInvalidNode", err)
	}
}

func TestStaticRegistryRejectsDuplicateMemberAndNode(t *testing.T) {
	group := testGroup(1)
	tests := []struct {
		name    string
		members []Member
		want    error
	}{
		{
			name: "member",
			members: []Member{
				{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: testNode(1), Role: MemberVoter},
				{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: testNode(2), Role: MemberVoter},
			},
			want: ErrDuplicateMember,
		},
		{
			name: "node",
			members: []Member{
				{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: testNode(1), Role: MemberVoter},
				{Group: group, ReplicaSetVersion: 1, MemberID: 2, Node: testNode(1), Role: MemberVoter},
			},
			want: ErrDuplicateNode,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewStaticRegistry(testNode(1), test.members, Limits{MaxGroups: 1, MaxMembers: 2})
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestStaticRegistryRejectsMixedReplicaSetVersions(t *testing.T) {
	group := testGroup(1)
	_, err := NewStaticRegistry(testNode(1), []Member{
		{Group: group, ReplicaSetVersion: 7, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 8, MemberID: 2, Node: testNode(2), Role: MemberVoter},
	}, Limits{MaxGroups: 1, MaxMembers: 2})
	if !errors.Is(err, ErrReplicaSet) {
		t.Fatalf("mixed version error = %v, want ErrReplicaSet", err)
	}
}

func TestStaticRegistryRequiresLocalMemberInEveryGroup(t *testing.T) {
	group1 := testGroup(1)
	members := []Member{
		{Group: group1, ReplicaSetVersion: 1, MemberID: 11, Node: testNode(1), Role: MemberVoter},
		{Group: testGroupInDomain(group1, 2), ReplicaSetVersion: 1, MemberID: 21, Node: testNode(2), Role: MemberVoter},
	}
	_, err := NewStaticRegistry(testNode(1), members, Limits{MaxGroups: 2, MaxMembers: 2})
	if !errors.Is(err, ErrLocalMember) {
		t.Fatalf("error = %v, want ErrLocalMember", err)
	}
}

func TestStaticRegistryRequiresVoterButAllowsLocalLearner(t *testing.T) {
	group := testGroup(1)
	_, err := NewStaticRegistry(testNode(1), []Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: testNode(1), Role: MemberLearner},
	}, Limits{MaxGroups: 1, MaxMembers: 1})
	if !errors.Is(err, ErrInvalidRole) {
		t.Fatalf("all-learner error = %v, want ErrInvalidRole", err)
	}
	if _, err := NewStaticRegistry(testNode(1), []Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: testNode(1), Role: MemberLearner},
		{Group: group, ReplicaSetVersion: 1, MemberID: 2, Node: testNode(2), Role: MemberVoter},
	}, Limits{MaxGroups: 1, MaxMembers: 2}); err != nil {
		t.Fatalf("local learner with remote voter: %v", err)
	}
}

func TestStaticRegistryEnforcesInputBounds(t *testing.T) {
	group1 := testGroup(1)
	group2 := testGroupInDomain(group1, 2)
	_, err := NewStaticRegistry(testNode(1), []Member{
		{Group: group1, ReplicaSetVersion: 1, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group2, ReplicaSetVersion: 1, MemberID: 1, Node: testNode(1), Role: MemberVoter},
	}, Limits{MaxGroups: 1, MaxMembers: 2})
	if !errors.Is(err, ErrRegistryBound) {
		t.Fatalf("group bound error = %v, want ErrRegistryBound", err)
	}

	members := make([]Member, raftmodel.MaxConfStateMembers+1)
	for i := range members {
		members[i] = Member{Group: group1, ReplicaSetVersion: 1, MemberID: uint64(i + 1), Node: testNode16(i + 1), Role: MemberVoter}
	}
	_, err = NewStaticRegistry(testNode16(1), members, Limits{MaxGroups: 2, MaxMembers: len(members)})
	if !errors.Is(err, ErrRegistryBound) {
		t.Fatalf("per-group bound error = %v, want ErrRegistryBound", err)
	}
}

func TestStaticRegistryDefensivelyCopiesMembers(t *testing.T) {
	group := testGroup(1)
	members := []Member{{Group: group, ReplicaSetVersion: 1, MemberID: 7, Node: testNode(1), Role: MemberVoter}}
	registry, err := NewStaticRegistry(testNode(1), members, Limits{MaxGroups: 1, MaxMembers: 1})
	if err != nil {
		t.Fatalf("NewStaticRegistry: %v", err)
	}
	members[0] = Member{}
	if got, err := registry.Node(group, 7); err != nil || got != testNode(1) {
		t.Fatalf("Node after input mutation = %x, %v; want %x, nil", got, err, testNode(1))
	}
}

func TestStaticRegistryLookupErrors(t *testing.T) {
	group := testGroup(1)
	registry, err := NewStaticRegistry(testNode(1), []Member{{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: testNode(1), Role: MemberVoter}}, Limits{MaxGroups: 1, MaxMembers: 1})
	if err != nil {
		t.Fatalf("NewStaticRegistry: %v", err)
	}
	if _, err := registry.LocalMember(testGroup(2)); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("LocalMember error = %v, want ErrGroupNotFound", err)
	}
	if _, err := registry.Node(group, 2); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("Node error = %v, want ErrMemberNotFound", err)
	}
	if _, err := registry.Member(group, testNode(2)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("Member error = %v, want ErrNodeNotFound", err)
	}
}

func TestStaticRegistryLookupsAllocateNothing(t *testing.T) {
	group := testGroup(1)
	node := testNode(1)
	registry, err := NewStaticRegistry(node, []Member{{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: node, Role: MemberVoter}}, Limits{MaxGroups: 1, MaxMembers: 1})
	if err != nil {
		t.Fatalf("NewStaticRegistry: %v", err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_, _ = registry.LocalMember(group)
		_, _ = registry.Node(group, 1)
		_, _ = registry.Member(group, node)
	}); got != 0 {
		t.Fatalf("lookup allocations = %v, want 0", got)
	}
}

func testGroup(seed byte) raftmember.GroupKey {
	return raftmember.GroupKey{
		ClusterID:             [16]byte{seed, 1},
		ClusterIncarnation:    [16]byte{seed, 2},
		TopologyRecoveryEpoch: uint64(seed),
		ShardIncarnation:      [16]byte{seed, 3},
		GroupID:               [16]byte{seed, 4},
	}
}

func testNode(seed byte) NodeID { return NodeID{seed} }

func testPeerIdentity(registry *StaticRegistry, node NodeID) PeerIdentity {
	return PeerIdentity{TrustDomain: registry.TrustDomain(), Node: node}
}

func testNode16(seed int) NodeID {
	return NodeID{byte(seed), byte(seed >> 8)}
}

func testGroup16(seed int) raftmember.GroupKey {
	group := testGroup(byte(seed))
	group.ClusterID[2] = byte(seed >> 8)
	group.ClusterIncarnation[2] = byte(seed >> 8)
	group.ShardIncarnation[2] = byte(seed >> 8)
	group.GroupID[2] = byte(seed >> 8)
	group.TopologyRecoveryEpoch = uint64(seed)
	return group
}

func testGroupInDomain(domain raftmember.GroupKey, seed int) raftmember.GroupKey {
	group := testGroup16(seed)
	group.ClusterID = domain.ClusterID
	group.ClusterIncarnation = domain.ClusterIncarnation
	return group
}
