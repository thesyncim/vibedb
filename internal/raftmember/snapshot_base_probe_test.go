package raftmember

import (
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestPipelinedRuntimeProbesRetainedBaseAfterLostFollowerProgress(t *testing.T) {
	identity := testWALIdentity(242)
	local, peer := identity.MemberID, identity.MemberID+1
	fixture := newRuntimeFixtureWithPipeline(t, 242, []uint64{local, peer}, testWALOptions(), true)
	owner := fixture.runtime
	wake := make(chan struct{}, 1)
	owner.SetPipelinedWake(func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	})
	var messages []*pb.Message
	var rejectProbe bool
	transportFailure := errors.New("probe transport interrupted")
	drain := func() error {
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		var workspace ReadyWorkspace
		for {
			result, err := owner.DriveReady(&workspace, func(out OutboundMessage) error {
				messages = append(messages, proto.Clone(out.Message).(*pb.Message))
				if rejectProbe && out.Message.GetType() == pb.MsgApp && len(out.Message.Entries) == 0 && out.Message.GetIndex() == 1 {
					return transportFailure
				}
				return nil
			}, settleTestApplied)
			if err != nil {
				return err
			}
			if !result.Progressed() {
				if owner.pipelined.quiescent() {
					return nil
				}
				select {
				case <-wake:
				case <-deadline.C:
					return errors.New("pipeline did not settle")
				}
			}
		}
	}
	mustDrain := func() {
		t.Helper()
		if err := drain(); err != nil {
			t.Fatal(err)
		}
	}
	respond := func(kind pb.MessageType, index uint64, reject bool) {
		t.Helper()
		status, err := owner.Status()
		if err != nil {
			t.Fatal(err)
		}
		message := &pb.Message{Type: kind.Enum(), From: &peer, To: &local, Term: runtimeUint64Ptr(status.Term), Index: runtimeUint64Ptr(index)}
		if reject {
			message.Reject = &reject
			message.RejectHint = runtimeUint64Ptr(0)
		}
		if err := owner.StepMessage(message); err != nil {
			t.Fatal(err)
		}
	}
	mustDrain()
	if err := owner.Campaign(); err != nil {
		t.Fatal(err)
	}
	mustDrain()
	for _, pair := range [][2]pb.MessageType{{pb.MsgPreVote, pb.MsgPreVoteResp}, {pb.MsgVote, pb.MsgVoteResp}} {
		var request *pb.Message
		for _, message := range messages {
			if message.GetType() == pair[0] && message.GetTo() == peer {
				request = message
			}
		}
		if request == nil {
			t.Fatalf("missing %s", pair[0])
		}
		term := request.GetTerm()
		if err := owner.StepMessage(&pb.Message{Type: pair[1].Enum(), From: &peer, To: &local, Term: &term}); err != nil {
			t.Fatal(err)
		}
		mustDrain()
	}
	// The leader has a sealed base at index 1, but no longer knows that the
	// restarted peer retains it. Rejection moves its probe below FirstIndex.
	messages = nil
	rejectProbe = true
	respond(pb.MsgAppResp, 1, true)
	if err := drain(); !errors.Is(err, transportFailure) || owner.Failure() != nil {
		t.Fatalf("send refusal poisoned runtime: %v failure=%v", err, owner.Failure())
	}
	progress, found := owner.node.Progress(peer)
	if !found || progress.Match != 0 || progress.PendingSnapshot != 1 {
		t.Fatalf("failed send changed progress: %+v", progress)
	}
	rejectProbe = false
	mustDrain()
	var probe *pb.Message
	for _, message := range messages {
		if message.GetType() == pb.MsgSnap || message.Snapshot != nil {
			t.Fatal("snapshot escaped ordinary transport")
		}
		if message.GetType() == pb.MsgApp && message.GetIndex() == 1 && len(message.Entries) == 0 {
			probe = message
		}
	}
	if probe == nil || probe.GetLogTerm() != 1 || probe.GetCommit() != 1 {
		t.Fatalf("missing exact base probe: %v", probe)
	}
	progress, _ = owner.node.Progress(peer)
	if progress.Match != 0 || progress.PendingSnapshot != 0 {
		t.Fatalf("probe asserted installation: %+v", progress)
	}
	// Only the ordinary authenticated append response advances replication.
	respond(pb.MsgAppResp, 1, false)
	mustDrain()
	progress, _ = owner.node.Progress(peer)
	if progress.Match != 1 || progress.PendingSnapshot != 0 || owner.Failure() != nil {
		t.Fatalf("base confirmation did not resume replication: %+v", progress)
	}
	if err := owner.StepMessage(&pb.Message{Type: pb.MsgSnap.Enum(), From: &peer, To: &local, Term: runtimeUint64Ptr(2), Snapshot: &pb.Snapshot{}}); !errors.Is(err, raftmodel.ErrUnsupported) {
		t.Fatalf("in-band installation admitted: %v", err)
	}
}
