package raftmember

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	raft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestPipelinedAppendRingCopiesBoundedPrefixBeforeRetirement(t *testing.T) {
	var ring pipelinedAppendRing
	for readyID := uint64(1); readyID <= 17; readyID++ {
		if !ring.push(pipelinedAppendWork{batch: raftmodel.PersistBatch{ReadyID: readyID}}) {
			t.Fatalf("push ReadyID %d failed", readyID)
		}
	}
	var prefix [16]pipelinedAppendWork
	if got := ring.copyPrefix(&prefix); got != len(prefix) {
		t.Fatalf("copyPrefix count=%d want=%d", got, len(prefix))
	}
	if got := ring.len(); got != 17 {
		t.Fatalf("copyPrefix retired entries: ring len=%d want=17", got)
	}
	for index, work := range prefix {
		if got, want := work.batch.ReadyID, uint64(index+1); got != want {
			t.Fatalf("prefix[%d] ReadyID=%d want=%d", index, got, want)
		}
	}
	if ring.popN(18) {
		t.Fatal("popN accepted more work than the ring contains")
	}
	if got := ring.len(); got != 17 {
		t.Fatalf("rejected popN changed ring len=%d want=17", got)
	}
	if !ring.popN(len(prefix)) {
		t.Fatal("popN rejected accepted prefix")
	}
	if got := ring.len(); got != 1 {
		t.Fatalf("after popN ring len=%d want=1", got)
	}
	work, ok := ring.front()
	if !ok || work.batch.ReadyID != 17 {
		t.Fatalf("remaining work=(%+v,%v), want ReadyID 17", work, ok)
	}
}

func TestPipelinedNodeAppendSubmitsQueuedReadyPrefixAfterFirstCompletion(t *testing.T) {
	base, store, p := newPipelinedNodeSeriesHarness(t, 252)
	var syncs atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseFirst := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseFirst()
	store.SetDataSyncForTesting(func(file *os.File) error {
		if syncs.Add(1) == 1 {
			close(entered)
			<-release
		}
		return file.Sync()
	})
	const maxSeries = uint64(raftstore.MaxReadySeries)
	for readyID := uint64(1); readyID <= maxSeries+1; readyID++ {
		message := pipelinedTestAppendMessage(base.MemberID, readyID+1, readyID)
		if readyID == 1 {
			from := base.MemberID
			peer := base.MemberID + 1
			responseTerm := uint64(1)
			message.Responses = []*pb.Message{{
				Type: pb.MsgApp.Enum(), From: &from,
				To: &peer, Term: &responseTerm,
			}}
		}
		if err := p.enqueueAppend(message); err != nil {
			t.Fatalf("enqueue ReadyID %d: %v", readyID, err)
		}
		if readyID == 1 {
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("first node persistence did not enter the sync fence")
			}
		}
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("queued Ready submissions reached persistence before first completion: syncs=%d", got)
	}
	if got := p.appendWork.len(); got != raftstore.MaxReadySeries {
		t.Fatalf("queued append ring len=%d want=%d", got, raftstore.MaxReadySeries)
	}
	if got := p.nodeWorkCount; got != 1 {
		t.Fatalf("first in-flight work count=%d want=1", got)
	}
	releaseFirst()

	result := waitPipelinedAppendResult(t, p)
	if result.Kind != DrivePersisted || result.ReadyID != 1 {
		t.Fatalf("first completion=%+v want DrivePersisted ReadyID 1", result)
	}
	if got := p.nodeWorkCount; got != uint8(raftstore.MaxReadySeries) {
		t.Fatalf("accepted queued series count=%d want=%d", got, raftstore.MaxReadySeries)
	}
	if got := p.appendWork.len(); got != 0 {
		t.Fatalf("accepted queued series remained in ring: len=%d", got)
	}
	if got := p.appendOutstanding; got != raftstore.MaxReadySeries {
		t.Fatalf("outstanding after first completion=%d want=%d", got, raftstore.MaxReadySeries)
	}
	if got := p.appendCompleted.len(); got != 1 {
		t.Fatalf("completed queue after first Ready=%d want=1", got)
	}
	if got := p.directQueue.len(); got != 1 {
		t.Fatalf("durable-term early replication response queue len=%d want=1", got)
	}
	if completion, ok := p.appendCompleted.front(); !ok || completion.responseIndex != 1 {
		t.Fatalf("node append response cursor=%+v present=%t want cursor 1", completion, ok)
	}

	result = waitPipelinedAppendResult(t, p)
	if result.Kind != DrivePersisted || result.ReadyID != maxSeries+1 {
		t.Fatalf("series completion=%+v want final ReadyID %d", result, maxSeries+1)
	}
	if p.appendOutstanding != 0 || p.nodeWorkCount != 0 || p.nodeSubmission {
		t.Fatalf("append state after series completion outstanding=%d active=%d submitting=%t",
			p.appendOutstanding, p.nodeWorkCount, p.nodeSubmission)
	}
	if got := p.appendCompleted.len(); got != raftstore.MaxReadySeries+1 {
		t.Fatalf("completion queue len=%d want=%d", got, raftstore.MaxReadySeries+1)
	}
	for expected := uint64(1); expected <= maxSeries+1; expected++ {
		completion, ok := p.appendCompleted.front()
		if !ok || completion.message == nil || completion.message.GetIndex() != expected+1 {
			t.Fatalf("completion %d=(%+v,%t)", expected, completion, ok)
		}
		p.appendCompleted.pop()
	}
}

func TestPipelinedNodeAppendShrinksUnsupportedSeriesPrefix(t *testing.T) {
	base, store, p := newPipelinedNodeSeriesHarness(t, 251)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseFirst := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseFirst)
	var syncs atomic.Int32
	store.SetDataSyncForTesting(func(file *os.File) error {
		if syncs.Add(1) == 1 {
			close(entered)
			<-release
		}
		return file.Sync()
	})
	if err := p.enqueueAppend(pipelinedTestAppendMessage(base.MemberID, 2, 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first node persistence did not enter the sync fence")
	}
	if err := p.enqueueAppend(pipelinedTestAppendMessage(base.MemberID, 3, 2)); err != nil {
		t.Fatal(err)
	}
	index, term := uint64(1), uint64(1)
	snapshotWork := pipelinedTestAppendMessage(base.MemberID, 4, 3)
	snapshotWork.Entries = nil
	snapshotWork.Snapshot = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{
		Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{base.MemberID}},
	}}
	if err := p.enqueueAppend(snapshotWork); err != nil {
		t.Fatal(err)
	}
	releaseFirst()
	if result := waitPipelinedAppendResult(t, p); result.ReadyID != 1 {
		t.Fatalf("first completion=%+v", result)
	}
	if p.nodeWorkCount != 1 || p.appendWork.len() != 1 {
		t.Fatalf("fallback accepted=%d queued=%d want 1/1", p.nodeWorkCount, p.appendWork.len())
	}
	if result := waitPipelinedAppendResult(t, p); result.ReadyID != 2 {
		t.Fatalf("prefix completion=%+v", result)
	}
	if p.nodeWorkCount != 1 || p.appendWork.len() != 0 {
		t.Fatalf("snapshot singleton accepted=%d queued=%d want 1/0", p.nodeWorkCount, p.appendWork.len())
	}
	if result := waitPipelinedAppendResult(t, p); result.ReadyID != 3 {
		t.Fatalf("snapshot completion=%+v", result)
	}
}

func newPipelinedNodeSeriesHarness(t *testing.T, seed byte) (raftstore.Identity, *raftstore.NodeStore, *pipelinedRuntime) {
	t.Helper()
	base := testWALIdentity(seed)
	nodeID := base.StoreID
	nodeID[0] ^= 0xa5
	node := raftstore.NodeIdentity{
		ClusterID: base.ClusterID, ClusterIncarnation: base.ClusterIncarnation, NodeID: nodeID,
	}
	descriptor := raftstore.GroupDescriptor{
		TopologyRecoveryEpoch: testTopologyRecoveryEpoch,
		AllocationGeneration:  base.AllocationGeneration,
		MemberID:              base.MemberID,
		GroupID:               base.GroupID,
		ShardIncarnation:      base.ShardIncarnation,
		StoreID:               base.StoreID,
		Distribution:          base.Distribution,
		Shard:                 base.Shard,
	}
	index, term := uint64(1), uint64(1)
	store, err := raftstore.CreateNodeStore(
		filepath.Join(t.TempDir(), "node"), node, testWALKey(),
		[]raftstore.NodeBootstrap{{Descriptor: descriptor, Snapshot: &pb.Snapshot{
			Metadata: &pb.SnapshotMetadata{Index: &index, Term: &term,
				ConfState: &pb.ConfState{Voters: []uint64{base.MemberID}}},
		}}},
		raftstore.NodeStoreOptions{
			MaxWaveBytes: 1 << 20, MaxSegmentEvents: 256,
			RecentWaves: 64, MaxEntriesPerGroup: 64, ReaderSlots: 1, MaxGroups: 8,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err = store.BeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	identity := RuntimeIdentity{
		Group: GroupKey{
			ClusterID: base.ClusterID, ClusterIncarnation: base.ClusterIncarnation,
			TopologyRecoveryEpoch: testTopologyRecoveryEpoch,
			ShardIncarnation:      base.ShardIncarnation, GroupID: base.GroupID,
		},
		Distribution: base.Distribution, Shard: base.Shard,
		AllocationGeneration: base.AllocationGeneration, MemberID: base.MemberID,
		StoreID: base.StoreID, NodeIncarnation: 1,
	}
	sequencer, err := raftstore.NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	persistence, err := BindNodeRuntimePersistence(store, sequencer, identity)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{identity: identity, nodePersistence: persistence}
	p := &pipelinedRuntime{runtime: runtime, appendTerm: 1, durableTerm: 1}
	runtime.pipelined = p
	return base, store, p
}

func pipelinedTestAppendMessage(memberID, entryIndex, readyID uint64) *pb.Message {
	term, vote, commit := uint64(1), memberID, uint64(1)
	localAppend := uint64(raft.LocalAppendThread)
	return &pb.Message{
		Type: pb.MsgStorageAppend.Enum(), From: &memberID, To: &localAppend,
		Term: &term, Vote: &vote, Commit: &commit, Index: &entryIndex,
		Entries: []*pb.Entry{{Type: pb.EntryNormal.Enum(), Term: &term, Index: &entryIndex}},
		Context: []byte{byte(readyID)},
	}
}

func waitPipelinedAppendResult(t *testing.T, p *pipelinedRuntime) DriveResult {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		result, consumed, err := p.consumeAppendResult()
		if err != nil {
			t.Fatalf("consume append result: %v", err)
		}
		if consumed {
			return result
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for node append completion")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
