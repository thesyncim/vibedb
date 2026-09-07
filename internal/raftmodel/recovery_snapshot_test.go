package raftmodel

import (
	"testing"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"
)

func TestRecoveryStorageDefersSnapshotsButPreservesRecoveryState(t *testing.T) {
	_, stable, machine := newTestNode(t, 1, []uint64{1, 2, 3})
	// The durable snapshot remains at the original base and configuration,
	// while the state machine has already published a later commit/config.
	// This is the recovery shape that needs the adapter: startup consumes the
	// real durable snapshot, then RawNode must recover from the newer overlay.
	published := Publication{
		Applied:           3,
		DataChainDigest:   machine.pub.DataChainDigest,
		ConfState:         &pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}},
		ReplicaSetVersion: 3,
	}
	term := uint64(2)
	index2, index3 := uint64(2), uint64(3)
	durableCommit := uint64(1)
	entries := []*pb.Entry{
		{Index: &index2, Term: &term, Type: pb.EntryNormal.Enum(), Data: []byte("committed-2")},
		{Index: &index3, Term: &term, Type: pb.EntryNormal.Enum(), Data: []byte("committed-3")},
	}
	if err := stable.Append(entries); err != nil {
		t.Fatalf("append durable suffix: %v", err)
	}
	if err := stable.SetHardState(&pb.HardState{Term: &term, Commit: &durableCommit}); err != nil {
		t.Fatalf("set durable commit: %v", err)
	}
	machine.pub = clonePublication(published)
	recovery := recoveryStorage{
		StableStore: stable,
		confState:   cloneConfState(published.ConfState),
		commitFloor: published.Applied,
	}

	durableSnapshot, err := stable.Snapshot()
	if err != nil {
		t.Fatalf("StableStore.Snapshot() error = %v", err)
	}
	gotSnapshot, err := recovery.Snapshot()
	if gotSnapshot != nil || err != raft.ErrSnapshotTemporarilyUnavailable {
		t.Fatalf("recovery Snapshot() = %v, %v, want nil and exact temporary-unavailable error", gotSnapshot, err)
	}
	if raft.IsEmptySnap(durableSnapshot) {
		t.Fatal("underlying durable snapshot became unavailable")
	}
	durableHardState, _, err := stable.InitialState()
	if err != nil {
		t.Fatalf("read underlying durable HardState: %v", err)
	}
	if durableHardState.GetCommit() != durableCommit {
		t.Fatalf("underlying durable commit = %d, want %d", durableHardState.GetCommit(), durableCommit)
	}
	if durableSnapshot.GetMetadata().GetIndex() != 1 ||
		durableSnapshot.GetMetadata().GetConfState().Equivalent(stable.conf) != nil {
		t.Fatalf("underlying durable snapshot = %v, want original base/configuration", durableSnapshot.GetMetadata())
	}

	hardState, confState, err := recovery.InitialState()
	if err != nil {
		t.Fatalf("recovery InitialState() error = %v", err)
	}
	if hardState.GetCommit() != published.Applied ||
		!proto.Equal(confState, published.ConfState) {
		t.Fatalf("recovery InitialState() = hard=%v conf=%v, want commit=%d conf=%v",
			hardState, confState, published.Applied, published.ConfState)
	}

	node, err := NewNode(1, 2, stable, machine)
	if err != nil {
		t.Fatalf("NewNode() with later publication overlay: %v", err)
	}
	status := node.raw.BasicStatus()
	if status.GetCommit() != published.Applied || status.Applied != published.Applied {
		t.Fatalf("recovered RawNode status = %+v, want commit/applied %d", status, published.Applied)
	}
	var sawVoter2, sawLearner3 bool
	node.raw.WithProgress(func(id uint64, typ raft.ProgressType, _ tracker.Progress) {
		if id == 2 && typ == raft.ProgressTypePeer {
			sawVoter2 = true
		}
		if id == 3 && typ == raft.ProgressTypeLearner {
			sawLearner3 = true
		}
	})
	if !sawVoter2 || !sawLearner3 {
		t.Fatalf("recovered RawNode membership = voter2:%v learner3:%v, want later published configuration", sawVoter2, sawLearner3)
	}
}

// TestRecoveryStorageSuppressesSnapshotAdvertisementAndKeepsQuorumLive uses
// real RawNodes and real MsgApp/MsgAppResp delivery. A real append response is
// held while the leader compacts past that append, then the held peer is
// explicitly probed. The recovery view refuses the unavailable snapshot, the
// delayed response still advances the peer through ordinary replication, and
// a separate peer can remain behind without taking the leader out of service.
func TestRecoveryStorageSuppressesSnapshotAdvertisementAndKeepsQuorumLive(t *testing.T) {
	voters := []uint64{1, 2, 3}
	leaderStorage := recoveryTestStorage(t, voters)
	healthyStorage := recoveryTestStorage(t, voters)
	laggingStorage := recoveryTestStorage(t, voters)
	leaderStable := &fakeStable{
		MemoryStorage: leaderStorage,
		conf:          &pb.ConfState{Voters: append([]uint64(nil), voters...)},
		durableIDs:    make(map[readyKey]struct{}),
	}
	recovery := recoveryStorage{
		StableStore: leaderStable,
		confState:   &pb.ConfState{Voters: append([]uint64(nil), voters...)},
		commitFloor: 1,
	}
	cluster := newRecoveryRawCluster(t,
		newRecoveryRawMember(t, 1, leaderStorage, recovery),
		newRecoveryRawMember(t, 2, healthyStorage, healthyStorage),
		newRecoveryRawMember(t, 3, laggingStorage, laggingStorage),
	)
	leader := cluster.members[0]

	if err := leader.raw.Campaign(); err != nil {
		t.Fatalf("leader Campaign() error = %v", err)
	}
	var delayed *pb.Message
	holdDelayed := false
	dropLaggingAppend := false
	hold := func(message *pb.Message) bool {
		if holdDelayed && message.GetFrom() == 2 && message.GetTo() == 1 &&
			message.GetType() == pb.MsgAppResp {
			if delayed == nil {
				delayed = proto.Clone(message).(*pb.Message)
			}
			return true
		}
		return false
	}
	drop := func(message *pb.Message) bool {
		if dropLaggingAppend && message.GetFrom() == 1 && message.GetTo() == 3 &&
			message.GetType() == pb.MsgApp {
			return true
		}
		return false
	}
	cluster.settle(t, hold, drop)
	if status := leader.raw.BasicStatus(); status.RaftState != raft.StateLeader {
		t.Fatalf("leader status after election = %+v", status)
	}
	committedBeforeCompact := leader.raw.BasicStatus().GetCommit()
	if committedBeforeCompact <= 1 {
		t.Fatalf("leader commit after healthy election = %d, want elected entry", committedBeforeCompact)
	}

	holdDelayed = true
	if err := leader.raw.Propose([]byte("delayed-ordinary-append")); err != nil {
		t.Fatalf("leader Propose(delayed) error = %v", err)
	}
	cluster.settle(t, hold, drop)
	if delayed == nil {
		t.Fatal("healthy peer produced no delayed real MsgAppResp")
	}
	if delayed.GetIndex() <= committedBeforeCompact {
		t.Fatalf("delayed response index = %d, want after committed election index %d", delayed.GetIndex(), committedBeforeCompact)
	}
	delayedIndex := delayed.GetIndex()
	if leader.raw.BasicStatus().GetCommit() < delayedIndex {
		t.Fatalf("healthy peer did not commit delayed append: status=%+v", leader.raw.BasicStatus())
	}

	if _, err := leaderStorage.CreateSnapshot(delayedIndex, &pb.ConfState{Voters: append([]uint64(nil), voters...)}, nil); err != nil {
		t.Fatalf("create durable snapshot through delayed index %d: %v", delayedIndex, err)
	}
	if err := leaderStorage.Compact(delayedIndex); err != nil {
		t.Fatalf("compact through delayed index %d: %v", delayedIndex, err)
	}
	firstAfterCompact, err := leaderStorage.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if firstAfterCompact <= delayedIndex {
		t.Fatalf("leader first index after compaction = %d, want > %d", firstAfterCompact, delayedIndex)
	}
	var beforeProbe tracker.Progress
	clusterProgress(t, leader.raw, 2, nil, &beforeProbe)
	if beforeProbe.Match >= delayedIndex {
		t.Fatalf("delayed peer progress before probe = %+v, want below delayed index %d", beforeProbe, delayedIndex)
	}

	// The held response represents a real append that the peer has persisted,
	// but the leader has not yet observed. ReportUnreachable forces the same
	// peer through the ordinary probe path; its next index is now below the
	// compacted base, so baseline RawNode storage would advertise MsgSnap here.
	leader.raw.ReportUnreachable(2)
	leader.raw.Tick()
	cluster.settle(t, hold, drop)

	holdDelayed = false
	if err := leader.raw.Step(delayed); err != nil {
		t.Fatalf("deliver delayed healthy MsgAppResp: %v", err)
	}
	delayed = nil
	cluster.settle(t, hold, drop)

	var healthyMatch uint64
	clusterProgress(t, leader.raw, 2, &healthyMatch, nil)
	if healthyMatch < delayedIndex || leader.raw.BasicStatus().GetCommit() < delayedIndex {
		t.Fatalf("healthy delayed append match/commit = %d/%d, want at least %d",
			healthyMatch, leader.raw.BasicStatus().GetCommit(), delayedIndex)
	}

	// Keep the next append ordinary for the recovered peer while intentionally
	// losing it to peer 3. This leaves one honest behind peer after a second
	// compaction, demonstrating that suppressing snapshots does not fabricate
	// progress or take away the healthy quorum.
	dropLaggingAppend = true
	if err := leader.raw.Propose([]byte("ordinary-after-compaction")); err != nil {
		t.Fatalf("leader Propose() error = %v", err)
	}
	cluster.settle(t, hold, drop)

	lastAfterProposal, err := leaderStorage.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	status := leader.raw.BasicStatus()
	if status.RaftState != raft.StateLeader || status.GetCommit() < lastAfterProposal {
		t.Fatalf("healthy quorum status = %+v, leader last=%d", status, lastAfterProposal)
	}
	if _, err := leaderStorage.CreateSnapshot(lastAfterProposal, &pb.ConfState{Voters: append([]uint64(nil), voters...)}, nil); err != nil {
		t.Fatalf("create durable snapshot through final append %d: %v", lastAfterProposal, err)
	}
	if err := leaderStorage.Compact(lastAfterProposal); err != nil {
		t.Fatalf("compact through final append %d: %v", lastAfterProposal, err)
	}
	firstAfterFinalCompact, err := leaderStorage.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if firstAfterFinalCompact <= lastAfterProposal {
		t.Fatalf("leader first index after final compaction = %d, want > %d", firstAfterFinalCompact, lastAfterProposal)
	}
	leader.raw.ReportUnreachable(3)
	leader.raw.Tick()
	cluster.settle(t, hold, drop)

	if err := leader.raw.Propose([]byte("quorum-after-snapshot-demand")); err != nil {
		t.Fatalf("leader Propose(after snapshot demand) error = %v", err)
	}
	cluster.settle(t, hold, drop)
	lastAfterDemand, err := leaderStorage.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	status = leader.raw.BasicStatus()
	if status.RaftState != raft.StateLeader || status.GetCommit() < lastAfterDemand {
		t.Fatalf("healthy quorum after unavailable snapshot demand = %+v, leader last=%d", status, lastAfterDemand)
	}

	var lagging tracker.Progress
	clusterProgress(t, leader.raw, 3, nil, &lagging)
	if lagging.Match >= lastAfterProposal || lagging.State == tracker.StateSnapshot || lagging.PendingSnapshot != 0 {
		t.Fatalf("lagging peer progress = %+v, want honestly behind without snapshot state", lagging)
	}
}

type recoveryRawMember struct {
	id     uint64
	raw    *raft.RawNode
	stable *raft.MemoryStorage
}

func newRecoveryRawMember(
	t testing.TB, id uint64, stable *raft.MemoryStorage, storage raft.Storage,
) *recoveryRawMember {
	t.Helper()
	cfg := NewConfig(id, storage, 1)
	raw, err := raft.NewRawNode(&cfg)
	if err != nil {
		t.Fatalf("raft.NewRawNode(%d): %v", id, err)
	}
	return &recoveryRawMember{id: id, raw: raw, stable: stable}
}

func recoveryTestStorage(t testing.TB, voters []uint64) *raft.MemoryStorage {
	t.Helper()
	storage := raft.NewMemoryStorage()
	index, term := uint64(1), uint64(1)
	if err := storage.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
		Index: &index, Term: &term,
		ConfState: &pb.ConfState{Voters: append([]uint64(nil), voters...)},
	}}); err != nil {
		t.Fatalf("ApplySnapshot(): %v", err)
	}
	if err := storage.SetHardState(&pb.HardState{Term: &term, Commit: &index}); err != nil {
		t.Fatalf("SetHardState(): %v", err)
	}
	return storage
}

type recoveryRawCluster struct {
	members []*recoveryRawMember
	byID    map[uint64]*recoveryRawMember
}

func newRecoveryRawCluster(t testing.TB, members ...*recoveryRawMember) *recoveryRawCluster {
	t.Helper()
	byID := make(map[uint64]*recoveryRawMember, len(members))
	for _, member := range members {
		if _, exists := byID[member.id]; exists {
			t.Fatalf("duplicate recovery RawNode member %d", member.id)
		}
		byID[member.id] = member
	}
	return &recoveryRawCluster{members: members, byID: byID}
}

func (cluster *recoveryRawCluster) settle(
	t testing.TB, hold func(*pb.Message) bool, drop func(*pb.Message) bool,
) {
	t.Helper()
	for round := 0; round < 256; round++ {
		progressed := false
		for _, member := range cluster.members {
			messages, ready := member.takeReady(t)
			if !ready {
				continue
			}
			progressed = true
			for _, message := range messages {
				switch message.GetType() {
				case pb.MsgSnap, pb.MsgSnapStatus:
					t.Fatalf("snapshot protocol message escaped recovery containment: %s", message.GetType())
				}
				if hold != nil && hold(message) || drop != nil && drop(message) {
					continue
				}
				target := cluster.byID[message.GetTo()]
				if target == nil {
					t.Fatalf("message %s targets unknown member %d", message.GetType(), message.GetTo())
				}
				if err := target.raw.Step(proto.Clone(message).(*pb.Message)); err != nil {
					t.Fatalf("deliver %s %d->%d: %v", message.GetType(), message.GetFrom(), message.GetTo(), err)
				}
			}
		}
		if !progressed {
			return
		}
	}
	t.Fatal("recovery RawNode cluster did not quiesce")
}

func (member *recoveryRawMember) takeReady(t testing.TB) ([]*pb.Message, bool) {
	t.Helper()
	if !member.raw.HasReady() {
		return nil, false
	}
	ready := member.raw.Ready()
	if !raft.IsEmptySnap(ready.Snapshot) {
		t.Fatalf("member %d produced an in-band snapshot", member.id)
	}
	if len(ready.Entries) != 0 {
		if err := member.stable.Append(ready.Entries); err != nil {
			t.Fatalf("member %d append Ready: %v", member.id, err)
		}
	}
	if !raft.IsEmptyHardState(ready.HardState) {
		if err := member.stable.SetHardState(ready.HardState); err != nil {
			t.Fatalf("member %d persist HardState: %v", member.id, err)
		}
	}
	member.raw.Advance(ready)
	return ready.Messages, true
}

func clusterProgress(
	t testing.TB, raw *raft.RawNode, memberID uint64, match *uint64, progress *tracker.Progress,
) {
	t.Helper()
	found := false
	raw.WithProgress(func(id uint64, _ raft.ProgressType, candidate tracker.Progress) {
		if id != memberID {
			return
		}
		found = true
		if match != nil {
			*match = candidate.Match
		}
		if progress != nil {
			*progress = candidate
		}
	})
	if !found {
		t.Fatalf("leader has no progress record for member %d", memberID)
	}
}
