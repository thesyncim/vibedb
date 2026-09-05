package raftmember

import (
	"context"
	"runtime/trace"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

// These diagnostic events carry logical identities, never SQL payloads. The
// disabled path neither formats nor retains state. A retained trace can begin
// or end mid-flight; consumers must report unmatched submissions explicitly.
func (r *Runtime) traceAppendStage(stage string, batch raftmodel.PersistBatch) {
	if !trace.IsEnabled() || len(batch.Entries) == 0 {
		return
	}
	trace.Logf(context.Background(), "raft.append", "event=%s group=%x ready=%d index=%d entries=%d", stage, r.identity.Group.GroupID, batch.ReadyID, batch.Entries[len(batch.Entries)-1].GetIndex(), len(batch.Entries))
}
func (r *Runtime) tracePeerStage(stage string, m *pb.Message) {
	if !trace.IsEnabled() || m == nil || m.GetType() != pb.MsgApp && m.GetType() != pb.MsgAppResp {
		return
	}
	last := m.GetIndex()
	if entries := m.GetEntries(); len(entries) != 0 {
		last = entries[len(entries)-1].GetIndex()
	}
	trace.Logf(context.Background(), "raft.peer", "event=%s group=%x type=%d from=%d to=%d term=%d index=%d last=%d entries=%d reject=%t", stage, r.identity.Group.GroupID, m.GetType(), m.GetFrom(), m.GetTo(), m.GetTerm(), m.GetIndex(), last, len(m.GetEntries()), m.GetReject())
}
