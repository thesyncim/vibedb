package rafttransport

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var allocationFrameSink []byte

func TestEncodeOutboundAndPreflightAllocateNothingWithCallerBuffer(t *testing.T) {
	group := testGroup(91)
	sender, _, from, to := frameTestRegistries(t, 2, group)
	message := frameTestMessage(pb.MsgHeartbeat, from, to)
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 0, FrameHeaderBytes+len(payload))
	outbound := raftmember.OutboundMessage{
		Group: group, From: from, To: to, Message: message,
	}
	if got := testing.AllocsPerRun(1000, func() {
		var encodeErr error
		allocationFrameSink, _, encodeErr = sender.EncodeOutbound(buffer[:0], outbound)
		if encodeErr != nil {
			panic(encodeErr)
		}
	}); got != 0 {
		t.Fatalf("EncodeOutbound allocations = %v, want 0", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		if preflightErr := preflightOrdinaryPayload(payload); preflightErr != nil {
			panic(preflightErr)
		}
	}); got != 0 {
		t.Fatalf("preflight allocations = %v, want 0", got)
	}
}
