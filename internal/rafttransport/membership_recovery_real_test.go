package rafttransport

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Exercise the actual Raft term/log/configuration boundary after reconstructing
// transport solely from each node's durable membership. No prior generation or
// process-local handoff cache survives restart.
func TestRealRaftMembershipRecoveryAfterLostAppendOrCommit(t *testing.T) {
	for stage := range 3 {
		for _, fault := range []string{"append", "commit"} {
			for _, restart := range []string{"leader", "follower", "all"} {
				t.Run(fmt.Sprintf("stage%d/%s/%s", stage, fault, restart), func(t *testing.T) {
					c := newMembershipRecoveryCluster(t)
					c.elect(t, 2)
					for prior := 0; prior < stage; prior++ {
						c.change(t, prior)
					}
					last, _ := c.nodes[2].store.LastIndex()
					transition := last + 1
					c.drop = func(m *pb.Message) bool {
						if m.GetTo() != 4 {
							return false
						}
						if fault == "commit" {
							return m.GetCommit() >= transition
						}
						for _, entry := range m.Entries {
							if entry.GetIndex() >= transition {
								return true
							}
						}
						return false
					}
					c.change(t, stage)
					lagging := c.nodes[4]
					if c.dropped == 0 || lagging.version >= transition {
						t.Fatalf("fault not exercised: dropped=%d version=%d transition=%d", c.dropped, lagging.version, transition)
					}
					last, _ = lagging.store.LastIndex()
					if fault == "commit" && last < transition {
						t.Fatal("lost-commit fixture did not persist the configuration append")
					}
					c.drop = nil
					for _, id := range []uint64{1, 2, 3, 4} {
						if restart == "all" || restart == "leader" && id == 2 || restart == "follower" && id == 4 {
							c.reopen(t, id)
						}
					}
					c.elect(t, 2)
					for attempts := 0; c.nodes[4].version < transition && attempts < 100; attempts++ {
						c.tick(t)
					}
					if c.nodes[4].version != transition {
						t.Fatalf("configuration catch-up stuck at%d, want%d", c.nodes[4].version, transition)
					}
					c.proveWrite(t)
				})
			}
		}
	}
}

// The follower missed promotion itself, not just its commit. It still calls
// node3 a learner when node3 becomes leader using the other three voters.
func TestRealRaftPromotedLeaderCatchesUpFollowerMissingPromotion(t *testing.T) {
	for _, missesAddition := range []bool{false, true} {
		t.Run(fmt.Sprintf("misses-addition=%v", missesAddition), func(t *testing.T) {
			c := newMembershipRecoveryCluster(t)
			c.elect(t, 2)
			if missesAddition {
				c.drop = func(m *pb.Message) bool { return m.GetTo() == 4 || m.GetFrom() == 4 }
			}
			c.change(t, 0)
			c.drop = func(m *pb.Message) bool { return m.GetTo() == 4 || m.GetFrom() == 4 }
			c.change(t, 1)
			if !missesAddition && c.nodes[4].conf.Learners[0] != 3 {
				t.Fatal("follower already learned promotion")
			}
			c.nodes[2].raw.TransferLeader(3)
			c.settle(t)
			if c.nodes[3].raw.BasicStatus().RaftState != raft.StateLeader {
				t.Fatal("promoted target was not elected")
			}
			c.drop = nil
			for attempt := 0; c.nodes[4].version < c.nodes[3].version && attempt < 100; attempt++ {
				c.tick(t)
			}
			if c.nodes[4].version != c.nodes[3].version {
				t.Fatal("new leader could not deliver missing promotion")
			}
			c.proveWrite(t)
		})
	}
}

type membershipRecoveryNode struct {
	raw              *raft.RawNode
	store            *raft.MemoryStorage
	registry         *StaticRegistry
	conf             *pb.ConfState
	applied, version uint64
}
type membershipRecoveryCluster struct {
	group   raftmember.GroupKey
	grant   membershipgrant.Grant
	nodes   map[uint64]*membershipRecoveryNode
	drop    func(*pb.Message) bool
	dropped int
}

func newMembershipRecoveryCluster(t *testing.T) *membershipRecoveryCluster {
	t.Helper()
	c := &membershipRecoveryCluster{group: testGroup(121), nodes: make(map[uint64]*membershipRecoveryNode)}
	c.grant = authorityTestGrant(c.group)
	for _, id := range []uint64{1, 2, 3, 4} {
		n := &membershipRecoveryNode{store: raft.NewMemoryStorage(), conf: &pb.ConfState{Voters: []uint64{1, 2, 4}}, applied: 5, version: 5}
		if err := n.store.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(5), Term: proto.Uint64(5), ConfState: proto.Clone(n.conf).(*pb.ConfState)}}); err != nil {
			t.Fatal(err)
		}
		if err := n.store.SetHardState(&pb.HardState{Term: proto.Uint64(5), Commit: proto.Uint64(5)}); err != nil {
			t.Fatal(err)
		}
		c.nodes[id] = n
		c.open(t, id)
	}
	return c
}
func (c *membershipRecoveryCluster) open(t *testing.T, id uint64) {
	t.Helper()
	n := c.nodes[id]
	members := make([]Member, 4)
	for i := range members {
		member := uint64(i + 1)
		role := MemberEnrolled
		if slices.Contains(n.conf.Voters, member) {
			role = MemberVoter
		} else if slices.Contains(n.conf.Learners, member) {
			role = MemberLearner
		}
		members[i] = Member{Group: c.group, ReplicaSetVersion: n.version, MemberID: member, Node: testNode(byte(member)), Role: role}
	}
	var err error
	n.registry, err = NewStaticRegistry(testNode(byte(id)), members, Limits{MaxGroups: 1, MaxMembers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err = n.registry.InstallTransitionGrant(c.grant); err != nil {
		t.Fatal(err)
	}
	if err = n.registry.PublishCommittedAuthorityWithReplay(c.group, n.version, n.conf, &retainedConfigurationLog{store: n.store}); err != nil {
		t.Fatal(err)
	}
	cfg := raftmodel.NewConfig(id, n.store, n.applied)
	n.raw, err = raft.NewRawNode(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.publishPromotion(t, n)
}
func (c *membershipRecoveryCluster) reopen(t *testing.T, id uint64) {
	t.Helper()
	n := c.nodes[id]
	if _, err := n.store.CreateSnapshot(n.applied, n.conf, nil); err != nil && err != raft.ErrSnapOutOfDate {
		t.Fatal(err)
	}
	c.open(t, id)
}
func (c *membershipRecoveryCluster) publishPromotion(t *testing.T, n *membershipRecoveryNode) {
	t.Helper()
	if !slices.Contains(n.conf.Learners, c.grant.TargetMember) {
		return
	}
	last, _ := n.store.LastIndex()
	if last <= n.version {
		return
	}
	entries, err := n.store.Entries(n.version+1, last+1, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.GetType() == pb.EntryNormal {
			continue
		}
		kind, id, digest, err := openSingleConfChange(entry)
		if err == nil && kind == pb.ConfChangeAddNode && id == c.grant.TargetMember && digest == c.grant.Digest() {
			if err = n.registry.PublishDurablePromotion(c.group, raftmember.DurablePromotionProof{Version: entry.GetIndex(), TargetMember: id, AuthorizationDigest: digest}); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
}
func (c *membershipRecoveryCluster) settle(t *testing.T) {
	t.Helper()
	for round := 0; round < 1000; round++ {
		progress := false
		var messages []*pb.Message
		for _, id := range []uint64{1, 2, 3, 4} {
			n := c.nodes[id]
			if !n.raw.HasReady() {
				continue
			}
			progress = true
			r := n.raw.Ready()
			if !raft.IsEmptySnap(r.Snapshot) {
				t.Fatal("unexpected in-band snapshot")
			}
			if err := n.store.Append(r.Entries); err != nil {
				t.Fatal(err)
			}
			if !raft.IsEmptyHardState(r.HardState) {
				if err := n.store.SetHardState(r.HardState); err != nil {
					t.Fatal(err)
				}
			}
			for _, entry := range r.CommittedEntries {
				if entry.GetType() == pb.EntryConfChange {
					var change pb.ConfChange
					if err := proto.Unmarshal(entry.Data, &change); err != nil {
						t.Fatal(err)
					}
					n.conf = n.raw.ApplyConfChange(&change)
					n.version = entry.GetIndex()
					if err := n.registry.PublishCommittedAuthorityWithReplay(c.group, n.version, n.conf, &retainedConfigurationLog{store: n.store}); err != nil {
						t.Fatalf("node%d publish%d: %v", id, n.version, err)
					}
				}
				n.applied = entry.GetIndex()
			}
			c.publishPromotion(t, n)
			messages = append(messages, r.Messages...)
			n.raw.Advance(r)
		}
		for _, message := range messages {
			if c.drop != nil && c.drop(message) {
				c.dropped++
				continue
			}
			from, to := message.GetFrom(), message.GetTo()
			sender, receiver := c.nodes[from], c.nodes[to]
			frame, _, err := sender.registry.EncodeOutbound(nil, raftmember.OutboundMessage{Group: c.group, From: from, To: to, Message: message})
			if err != nil {
				if errors.Is(err, errRetiredOutboundSource) || errors.Is(err, errRetiredOutboundDestination) {
					continue
				}
				t.Fatalf("encode %s %d->%d: %v", message.GetType(), from, to, err)
			}
			inbound, err := receiver.registry.DecodeInbound(testPeerIdentity(receiver.registry, testNode(byte(from))), frame)
			if err != nil {
				// Removed members are excluded even while a surviving voter is
				// still catching up to the removal. That refusal is expected.
				view, _ := receiver.registry.currentAuthority(c.group)
				if view.roles[from] == MemberEnrolled {
					continue
				}
				// A voter that missed promotion may refuse that candidate's
				// election packet until it has the exact local durable witness.
				// The remaining quorum must still elect and replicate the entry.
				switch message.GetType() {
				case pb.MsgVote, pb.MsgPreVote, pb.MsgVoteResp, pb.MsgPreVoteResp:
					if errors.Is(err, ErrUnauthorized) && (!votingMember(view, from) || !votingMember(view, to)) {
						continue
					}
				}
				t.Fatalf("decode %s %d->%d: %v", message.GetType(), from, to, err)
			}
			if err = receiver.raw.Step(inbound.Message); err != nil && err != raft.ErrStepPeerNotFound {
				t.Fatal(err)
			}
		}
		if !progress {
			return
		}
	}
	t.Fatal("Raft did not quiesce")
}
func (c *membershipRecoveryCluster) tick(t *testing.T) {
	for _, n := range c.nodes {
		n.raw.Tick()
	}
	c.settle(t)
}
func (c *membershipRecoveryCluster) elect(t *testing.T, id uint64) {
	t.Helper()
	if c.nodes[id].raw.BasicStatus().RaftState == raft.StateLeader {
		return
	}
	if err := c.nodes[id].raw.Campaign(); err != nil {
		t.Fatal(err)
	}
	c.settle(t)
	for attempt := 0; attempt < 100; attempt++ {
		for _, n := range c.nodes {
			if n.raw.BasicStatus().RaftState == raft.StateLeader {
				return
			}
		}
		c.tick(t)
	}
	t.Fatal("no leader after bounded election")
}
func (c *membershipRecoveryCluster) leader(t *testing.T) *membershipRecoveryNode {
	t.Helper()
	for _, n := range c.nodes {
		if n.raw.BasicStatus().RaftState == raft.StateLeader {
			return n
		}
	}
	t.Fatal("no leader")
	return nil
}
func (c *membershipRecoveryCluster) change(t *testing.T, stage int) {
	t.Helper()
	kind := []pb.ConfChangeType{pb.ConfChangeAddLearnerNode, pb.ConfChangeAddNode, pb.ConfChangeRemoveNode}[stage]
	member := c.grant.TargetMember
	if stage == 2 {
		member = c.grant.SourceMember
	}
	digest := c.grant.Digest()
	if err := c.leader(t).raw.ProposeConfChange(&pb.ConfChange{Type: kind.Enum(), NodeId: proto.Uint64(member), Context: digest[:]}); err != nil {
		t.Fatal(err)
	}
	c.settle(t)
}
func (c *membershipRecoveryCluster) proveWrite(t *testing.T) {
	t.Helper()
	leader := c.leader(t)
	if err := leader.raw.Propose([]byte("durable-after-membership-recovery")); err != nil {
		t.Fatal(err)
	}
	c.settle(t)
	last, _ := leader.store.LastIndex()
	for _, id := range leader.conf.Voters {
		if c.nodes[id].applied != last {
			t.Fatalf("voter%d applied%d, want%d", id, c.nodes[id].applied, last)
		}
	}
}
