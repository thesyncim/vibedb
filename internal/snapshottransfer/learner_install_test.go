package snapshottransfer

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

func TestExactLearnerConfStateRequiresTargetLearnerAndSourceVoter(t *testing.T) {
	autoLeave := true
	valid := &pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}}
	if !exactLearnerConfState(valid, 1, 3) {
		t.Fatal("rejected exact learner configuration")
	}
	for name, conf := range map[string]*pb.ConfState{
		"target voter":    {Voters: []uint64{1, 3}},
		"source learner":  {Voters: []uint64{2}, Learners: []uint64{1, 3}},
		"target absent":   {Voters: []uint64{1, 2}},
		"joint":           {Voters: []uint64{1}, VotersOutgoing: []uint64{1}, Learners: []uint64{3}},
		"learners next":   {Voters: []uint64{1}, LearnersNext: []uint64{3}},
		"automatic leave": {Voters: []uint64{1}, Learners: []uint64{3}, AutoLeave: &autoLeave},
	} {
		t.Run(name, func(t *testing.T) {
			if exactLearnerConfState(conf, 1, 3) {
				t.Fatalf("accepted %+v", conf)
			}
		})
	}
}
