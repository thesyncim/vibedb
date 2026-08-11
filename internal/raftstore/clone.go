package raftstore

import pb "go.etcd.io/raft/v3/raftpb"

func uint64Pointer(value uint64) *uint64 { return &value }

func entryTypePointer(value pb.EntryType) *pb.EntryType { return &value }

func boolPointer(value bool) *bool { return &value }

func cloneHardState(state *pb.HardState) *pb.HardState {
	if state == nil {
		return nil
	}
	return &pb.HardState{Term: uint64Pointer(state.GetTerm()), Vote: uint64Pointer(state.GetVote()), Commit: uint64Pointer(state.GetCommit())}
}

func cloneConfState(state *pb.ConfState) *pb.ConfState {
	if state == nil {
		return nil
	}
	result := &pb.ConfState{
		Voters:         append([]uint64(nil), state.GetVoters()...),
		Learners:       append([]uint64(nil), state.GetLearners()...),
		VotersOutgoing: append([]uint64(nil), state.GetVotersOutgoing()...),
		LearnersNext:   append([]uint64(nil), state.GetLearnersNext()...),
	}
	if state.AutoLeave != nil {
		result.AutoLeave = boolPointer(state.GetAutoLeave())
	}
	return result
}

func cloneEntry(entry *pb.Entry) *pb.Entry {
	if entry == nil {
		return nil
	}
	return &pb.Entry{
		Term:  uint64Pointer(entry.GetTerm()),
		Index: uint64Pointer(entry.GetIndex()),
		Type:  entryTypePointer(entry.GetType()),
		Data:  append([]byte(nil), entry.GetData()...),
	}
}

func cloneEntries(entries []*pb.Entry) []*pb.Entry {
	if entries == nil {
		return nil
	}
	result := make([]*pb.Entry, len(entries))
	for index, entry := range entries {
		result[index] = cloneEntry(entry)
	}
	return result
}

func cloneSnapshot(snapshot *pb.Snapshot) *pb.Snapshot {
	if snapshot == nil {
		return nil
	}
	metadata := snapshot.GetMetadata()
	var clonedMetadata *pb.SnapshotMetadata
	if metadata != nil {
		clonedMetadata = &pb.SnapshotMetadata{
			ConfState: cloneConfState(metadata.GetConfState()),
			Index:     uint64Pointer(metadata.GetIndex()),
			Term:      uint64Pointer(metadata.GetTerm()),
		}
	}
	return &pb.Snapshot{Data: append([]byte(nil), snapshot.GetData()...), Metadata: clonedMetadata}
}
