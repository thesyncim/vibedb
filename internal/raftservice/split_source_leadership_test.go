package raftservice

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

type splitLeadershipHost struct {
	ownerHost
	publication raftmodel.Publication
	status      raftmember.RuntimeStatus
	target      uint64
}

func (host *splitLeadershipHost) Publication(raftmember.GroupKey) (raftmodel.Publication, error) {
	return host.publication, nil
}
func (host *splitLeadershipHost) Status(raftmember.GroupKey) (raftmember.RuntimeStatus, error) {
	return host.status, nil
}
func (host *splitLeadershipHost) TransferLeader(_ raftmember.GroupKey, target uint64) error {
	host.target = target
	return nil
}

func TestSplitSourceLeadershipRequiresExactLeaderAndExistingVoter(t *testing.T) {
	fence := ServingFence{Group: peerServerTestGroup(), AllocationGeneration: 1,
		Command: CommandFence{ReplicaSetVersion: 1}, MemberID: 1, StoreID: [16]byte{1}, NodeIncarnation: 1, Term: 2}
	for _, test := range []struct {
		name   string
		mutate func(*splitLeadershipHost, *ServingFence)
		target uint64
		valid  bool
	}{
		{name: "exact", target: 2, valid: true},
		{name: "outsider", target: 4},
		{name: "self", target: 1},
		{name: "stale term", target: 2, mutate: func(_ *splitLeadershipHost, f *ServingFence) { f.Term++ }},
		{name: "stale incarnation", target: 2, mutate: func(_ *splitLeadershipHost, f *ServingFence) { f.NodeIncarnation++ }},
		{name: "follower", target: 2, mutate: func(h *splitLeadershipHost, _ *ServingFence) { h.status.LeaderID = 3 }},
		{name: "joint", target: 2, mutate: func(h *splitLeadershipHost, _ *ServingFence) {
			h.publication.ConfState.VotersOutgoing = []uint64{1, 2, 3}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			host := &splitLeadershipHost{publication: raftmodel.Publication{ReplicaSetVersion: 1, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}, status: raftmember.RuntimeStatus{MemberID: 1, LeaderID: 1, Term: 2}}
			owner := &Owner{host: host, members: map[raftmember.GroupKey]ownerMember{fence.Group: {identity: raftmember.RuntimeIdentity{Group: fence.Group, AllocationGeneration: 1, MemberID: 1, StoreID: fence.StoreID, NodeIncarnation: 1}, command: fence.Command}}}
			attempt := fence
			if test.mutate != nil {
				test.mutate(host, &attempt)
			}
			err := owner.transferSplitSourceLeadership(attempt, test.target)
			if (err == nil) != test.valid || test.valid && host.target != 2 || !test.valid && host.target != 0 {
				t.Fatalf("target=%d err=%v", host.target, err)
			}
		})
	}
}
