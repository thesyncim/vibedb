package raftservice

import (
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"testing"
)

func TestSharedReadBarrierRejectsPreInvocationQuorumEvidence(t *testing.T) {
	f := newSharedBarrierFixture(t)
	first := f.read(t, false)
	// This response was generated for the first read before the later read
	// began, but is delayed in transit. The old leader can remain locally in
	// term 2 while a new quorum elects a leader and acknowledges a write at 10.
	old := raftmodel.ReadOutcome{}
	old.Barrier.Context = append([]byte(nil), f.host.contexts[0][:]...)
	old.Barrier.Index = 9
	// A new client invokes its read after that remote write was acknowledged.
	// Local serving generation/status have not changed on the isolated node.
	late := f.read(t, false)
	f.owner.finishReadOutcomes([]raftmodel.ReadOutcome{old})
	settleDeliveryReply(t, first, 9)
	select {
	case reply := <-late.reply:
		if reply.read.generation != nil {
			defer reply.read.generation.release()
		}
		if reply.err == nil {
			t.Fatalf("late read accepted pre-invocation quorum evidence: floor=%d; no new ReadIndex (calls=%d)", reply.read.minimumApplied, f.host.calls)
		}
	default:
	}
	if f.host.calls != 2 {
		t.Fatalf("late read needs its own round, got %d", f.host.calls)
	}
	fresh := raftmodel.ReadOutcome{}
	fresh.Barrier.Context = append([]byte(nil), f.host.contexts[1][:]...)
	fresh.Barrier.Index = 10
	f.owner.finishReadOutcomes([]raftmodel.ReadOutcome{fresh})
	settleDeliveryReply(t, late, 10)
}
