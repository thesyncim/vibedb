package raftmodel

import (
	"testing"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestCommittedPrefixEmptyProbeCannotCommitRetainedSuffix(t *testing.T) {
	for _, atFloor := range []bool{false, true} {
		name := "strictly_before_floor_empty_probe"
		if atFloor {
			name = "at_floor_append_positive_control"
		}
		t.Run(name, func(t *testing.T) {
			node, stable, machine := newTestNode(t, 1, []uint64{1, 2})
			configurationEntry := func(index uint64, kind pb.ConfChangeType) *pb.Entry {
				t.Helper()
				entryType, data, err := pb.MarshalConfChange(&pb.ConfChange{Type: kind.Enum(), NodeId: uint64Ptr(3)})
				if err != nil {
					t.Fatal(err)
				}
				return &pb.Entry{Type: entryType.Enum(), Index: uint64Ptr(index), Term: uint64Ptr(2), Data: data}
			}
			appendMessage := func(index, logTerm, commit uint64, entries ...*pb.Entry) *pb.Message {
				return &pb.Message{Type: pb.MsgApp.Enum(), From: uint64Ptr(2), To: uint64Ptr(1), Term: uint64Ptr(2),
					Index: uint64Ptr(index), LogTerm: uint64Ptr(logTerm), Commit: uint64Ptr(commit), Entries: entries}
			}
			// Establish the floor through the production persist/apply path, then
			// leave a real configuration entry durable but uncommitted above it.
			if err := node.Step(appendMessage(1, 1, 2, configurationEntry(2, pb.ConfChangeAddLearnerNode))); err != nil {
				t.Fatal(err)
			}
			driveAllReady(t, node)
			tail := configurationEntry(3, pb.ConfChangeAddNode)
			if err := node.Step(appendMessage(2, 2, 2, tail)); err != nil {
				t.Fatal(err)
			}
			driveAllReady(t, node)
			// Recovery must preserve both the locally applied membership floor
			// and the unapplied retained suffix used by the positive control.
			var err error
			node, err = NewNode(1, 2, stable, machine)
			if err != nil {
				t.Fatal(err)
			}
			before := node.Published()
			if before.Applied != 2 || before.ReplicaSetVersion != 2 ||
				before.ConfState.Equivalent(&pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}}) != nil {
				t.Fatalf("fixture lacks applied configuration floor: %+v", before)
			}
			calls, batches := len(machine.calls), len(stable.batches)
			// This is the message handed off after transport strips ALL entries
			// from a canonical, authenticated append whose prefix is below the
			// local applied floor. Its advertised commit is deliberately too high.
			message := appendMessage(1, 1, 100)
			wantCommit, wantCalls := uint64(2), calls
			if atFloor {
				// Equality does not take etcd's committed-prefix branch. An append
				// starting here can commit and apply the retained configuration.
				message = appendMessage(2, 2, 100, tail)
				wantCommit, wantCalls = 3, calls+1
			}
			if err := node.Step(message); err != nil {
				t.Fatal(err)
			}
			var responses []*pb.Message
			driveOneReady(t, node, func(message *pb.Message) error {
				responses = append(responses, proto.Clone(message).(*pb.Message))
				return nil
			})
			if ready, err := node.HasReady(); err != nil || ready {
				t.Fatalf("unexpected pending Ready after probe: ready=%t err=%v", ready, err)
			}
			if len(responses) != 1 || responses[0].GetType() != pb.MsgAppResp || responses[0].GetReject() ||
				responses[0].GetFrom() != 1 || responses[0].GetTo() != 2 || responses[0].GetIndex() != wantCommit ||
				len(responses[0].GetEntries()) != 0 {
				t.Fatalf("probe response=%v, want only committed-index ACK %d", responses, wantCommit)
			}
			hard, _, err := stable.InitialState()
			if err != nil || hard.GetCommit() != wantCommit || node.Status().GetCommit() != wantCommit ||
				node.PublishedApplied() != wantCommit || len(machine.calls) != wantCalls {
				t.Fatalf("probe changed unexpected commit/apply state: hard=%v status=%+v calls=%d err=%v", hard, node.Status(), len(machine.calls), err)
			}
			retained, err := stable.Entries(3, 4, MaxInboundMessageBytes)
			if err != nil || len(retained) != 1 || !proto.Equal(retained[0], tail) {
				t.Fatalf("probe changed retained configuration entry: entries=%v err=%v", retained, err)
			}
			if atFloor {
				if node.Published().ReplicaSetVersion != 3 || node.Published().ConfState.Equivalent(&pb.ConfState{Voters: []uint64{1, 2, 3}}) != nil {
					t.Fatalf("boundary positive control did not apply configuration: %+v", node.Published())
				}
				return
			}
			if !equalPublication(node.Published(), before) || machine.snapshotCalls != 0 {
				t.Fatalf("empty probe changed publication: before=%+v after=%+v", before, node.Published())
			}
			for _, batch := range stable.batches[batches:] {
				if len(batch.Entries) != 0 || !raft.IsEmptySnap(batch.Snapshot) || batch.HardState.GetCommit() > before.Applied {
					t.Fatalf("empty probe persisted entries, snapshot or advanced commit: %+v", batch)
				}
			}
		})
	}
}
