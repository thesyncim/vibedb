package raftmember

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftstore"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	raft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type applyDependencyFixture struct {
	store     *raftstore.NodeStore
	sequencer *raftstore.NodeSubmissionSequencer
	runtimes  [2]*Runtime
	persist   [2]*NodeRuntimePersistence
	raftIDs   [2]raftstore.Identity
	applyIDs  [2]sqldriver.ReplicatedShardStoreIdentity
}

func newApplyDependencyFixture(t *testing.T, seed byte) applyDependencyFixture {
	t.Helper()
	base := testWALIdentity(seed)
	nodeID := base.StoreID
	nodeID[0] ^= 0x5a
	node := raftstore.NodeIdentity{
		ClusterID: base.ClusterID, ClusterIncarnation: base.ClusterIncarnation, NodeID: nodeID,
	}
	boots := make([]raftstore.NodeBootstrap, 2)
	raftIDs := [2]raftstore.Identity{}
	for i := range boots {
		identity := base
		identity.MemberID = uint64(i + 1)
		identity.GroupID[15] += byte(i + 1)
		identity.StoreID[15] += byte(i + 1)
		descriptor := raftstore.GroupDescriptor{
			TopologyRecoveryEpoch: testTopologyRecoveryEpoch,
			AllocationGeneration:  identity.AllocationGeneration,
			MemberID:              identity.MemberID,
			GroupID:               identity.GroupID,
			ShardIncarnation:      identity.ShardIncarnation,
			StoreID:               identity.StoreID,
			Distribution:          identity.Distribution,
			Shard:                 identity.Shard,
		}
		index, term := uint64(1), uint64(1)
		boots[i] = raftstore.NodeBootstrap{Descriptor: descriptor, Snapshot: &pb.Snapshot{
			Metadata: &pb.SnapshotMetadata{
				Index: &index, Term: &term,
				ConfState: &pb.ConfState{Voters: []uint64{identity.MemberID}},
			},
		}}
		raftIDs[i] = identity
	}
	store, err := raftstore.CreateNodeStore(filepath.Join(t.TempDir(), "node"), node, testWALKey(), boots, raftstore.NodeStoreOptions{
		MaxWaveBytes: 1 << 20, MaxSegmentEvents: 256, RecentWaves: 64,
		MaxEntriesPerGroup: 64, ReaderSlots: 1, MaxGroups: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.BeginIncarnations([]uint64{1, 2}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	sequencer, err := raftstore.NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	fixture := applyDependencyFixture{store: store, sequencer: sequencer, raftIDs: raftIDs}
	t.Cleanup(func() {
		for _, runtime := range fixture.runtimes {
			if runtime != nil {
				_ = runtime.Close()
			}
		}
		_ = fixture.sequencer.Close()
		_ = fixture.store.Close()
	})

	for i := range fixture.runtimes {
		group, ok := store.GroupByID(raftIDs[i].GroupID)
		if !ok {
			t.Fatalf("group %d missing", i)
		}
		_, database, _ := prepareSQLRoot(t, raftIDs[i], fmt.Sprintf("apply-dependency-%d", i))
		authority := testAuthorityProfile()
		bound, err := BindPreparedNodeSQL(group, database, authority, "docs")
		skipIfStrictAllocationUnsupported(t, "bind apply-dependency SQL", err)
		if err != nil {
			t.Fatal(err)
		}
		apply, _, err := OpenPreparedNodeApply(group, database, authority, bound, testApplyOptions())
		skipIfStrictAllocationUnsupported(t, "open apply-dependency apply", err)
		if err != nil {
			t.Fatal(err)
		}
		index, term := uint64(1), uint64(1)
		if _, err := apply.InstallSnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term,
			ConfState: &pb.ConfState{Voters: []uint64{raftIDs[i].MemberID}},
		}}); err != nil {
			t.Fatal(err)
		}
		identity, err := RuntimeIdentityFromNodeGroup(group, apply)
		if err != nil {
			t.Fatal(err)
		}
		persistence, err := BindNodeRuntimePersistence(store, sequencer, identity)
		if err != nil {
			t.Fatal(err)
		}
		runtime, err := AdoptNodeRuntime(persistence, database, apply)
		if err != nil {
			if runtime != nil {
				_ = runtime.Close()
			}
			t.Fatal(err)
		}
		fixture.runtimes[i] = runtime
		fixture.persist[i] = persistence
		fixture.applyIDs[i] = bound
	}
	return fixture
}

func driveApplyDependencyRuntime(t *testing.T, runtime *Runtime, persistence *NodeRuntimePersistence, send func(OutboundMessage) error) {
	t.Helper()
	var workspace ReadyWorkspace
	for step := 0; step < 1000; step++ {
		result, err := runtime.DriveReady(&workspace, send, settleTestApplied)
		if err != nil {
			t.Fatalf("DriveReady step %d: %v", step, err)
		}
		if runtime.pipelined != nil && runtime.pipelined.nodeSubmission {
			if _, err := persistence.Wait(); err != nil {
				t.Fatalf("persistence step %d: %v", step, err)
			}
			continue
		}
		if !result.Progressed() {
			return
		}
	}
	t.Fatal("pipelined runtime did not become idle")
}

func driveApplyDependencyStep(
	t *testing.T,
	runtime *Runtime,
	workspace *ReadyWorkspace,
	send func(OutboundMessage) error,
	settle ResultSettlementSink,
) DriveResult {
	t.Helper()
	result, err := runtime.DriveReady(workspace, send, settle)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func drainAppendResponses(t *testing.T, runtime *Runtime, send func(OutboundMessage) error) {
	t.Helper()
	for step := 0; step < 32; step++ {
		_, progressed, err := runtime.pipelined.driveResponses(send)
		if err != nil {
			t.Fatal(err)
		}
		if !progressed {
			return
		}
	}
	t.Fatal("append responses did not drain")
}

func driveApplyDependencyRuntimeWithSettlement(
	t *testing.T,
	runtime *Runtime,
	persistence *NodeRuntimePersistence,
	settle ResultSettlementSink,
) {
	t.Helper()
	var workspace ReadyWorkspace
	for step := 0; step < 1000; step++ {
		result := driveApplyDependencyStep(t, runtime, &workspace, nil, settle)
		if runtime.pipelined != nil && runtime.pipelined.nodeSubmission {
			if _, err := persistence.Wait(); err != nil {
				t.Fatalf("persistence step %d: %v", step, err)
			}
			continue
		}
		if !result.Progressed() {
			return
		}
	}
	t.Fatal("pipelined runtime did not become idle")
}

func TestPipelinedApplyDoesNotWaitForMetadataAppend(t *testing.T) {
	fixture := newApplyDependencyFixture(t, 253)
	leader := fixture.runtimes[0]
	other := fixture.runtimes[1]
	driveApplyDependencyRuntime(t, leader, fixture.persist[0], nil)
	driveApplyDependencyRuntime(t, other, fixture.persist[1], nil)
	if err := leader.Campaign(); err != nil {
		t.Fatal(err)
	}
	driveApplyDependencyRuntime(t, leader, fixture.persist[0], nil)
	if err := other.Campaign(); err != nil {
		t.Fatal(err)
	}
	driveApplyDependencyRuntime(t, other, fixture.persist[1], nil)
	if err := leader.Propose(testApplySessionOpen(fixture.applyIDs[0])); err != nil {
		t.Fatal(err)
	}

	var workspace ReadyWorkspace
	if result := driveApplyDependencyStep(t, leader, &workspace, func(OutboundMessage) error { return nil }, settleTestApplied); result.Kind != DriveCaptured {
		t.Fatalf("entry A capture=%+v, p=%s", result, leader.pipelined)
	}
	if !leader.pipelined.nodeSubmission {
		t.Fatal("entry A was not submitted to the shared node sequencer")
	}
	if _, err := fixture.persist[0].Wait(); err != nil {
		t.Fatalf("entry A persistence: %v", err)
	}
	if result := driveApplyDependencyStep(t, leader, &workspace, func(OutboundMessage) error { return nil }, settleTestApplied); result.Kind != DrivePersisted {
		t.Fatalf("entry A persistence completion=%+v, p=%s", result, leader.pipelined)
	}
	group, ok := fixture.store.GroupByID(fixture.raftIDs[0].GroupID)
	if !ok {
		t.Fatal("leader group disappeared")
	}
	stableA, err := group.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if stableA <= 1 {
		t.Fatalf("entry A did not reach the node log: last=%d", stableA)
	}
	// Deliver A's local append acknowledgement directly. This lets RawNode
	// construct B without letting DriveReady consume the resulting Ready.
	drainAppendResponses(t, leader, func(OutboundMessage) error { return nil })
	if hasReady, err := leader.node.HasReady(); err != nil || !hasReady {
		t.Fatalf("commit-only B Ready=%t err=%v", hasReady, err)
	}
	priorAppendReadyID := leader.pipelined.appendReadyID
	priorAppendTerm, priorAppendVote := leader.pipelined.appendTerm, leader.pipelined.appendVote

	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	fixture.store.SetDataSyncForTesting(func(file *os.File) error {
		blocked := false
		enterOnce.Do(func() {
			close(entered)
			blocked = true
		})
		if blocked {
			<-release
		}
		return file.Sync()
	})
	if err := other.Propose(testApplySessionOpen(fixture.applyIDs[1])); err != nil {
		t.Fatal(err)
	}
	if result := driveApplyDependencyStep(t, other, &workspace, func(OutboundMessage) error { return nil }, settleTestApplied); result.Kind != DriveCaptured {
		t.Fatalf("unrelated group capture=%+v, p=%s", result, other.pipelined)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("unrelated durable wave did not enter the fsync gate")
	}

	// The next Ready contains no entries but advances commit. Its append must
	// wait behind the already-running unrelated wave, while its apply payload
	// is the stable A entry.
	if result := driveApplyDependencyStep(t, leader, &workspace, func(OutboundMessage) error { return nil }, settleTestApplied); result.Kind != DriveCaptured {
		t.Fatalf("metadata B capture=%+v, p=%s", result, leader.pipelined)
	}
	if leader.pipelined.nodeWorkCount != 1 {
		t.Fatalf("metadata B node work count=%d, want 1", leader.pipelined.nodeWorkCount)
	}
	bWork := leader.pipelined.nodeWorks[0]
	if len(bWork.batch.Entries) != 0 || bWork.batch.MustSync || bWork.batch.ReadyID != priorAppendReadyID+1 || bWork.batch.HardState == nil || bWork.batch.HardState.GetTerm() != priorAppendTerm || bWork.batch.HardState.GetVote() != priorAppendVote || bWork.message == nil || len(bWork.message.GetResponses()) != 0 || bWork.message.GetSnapshot() != nil {
		t.Fatalf("metadata B batch=%+v message=%+v", bWork.batch, bWork.message)
	}
	applyWork, ok := leader.pipelined.applyQueue.front()
	if !ok || len(applyWork.message.GetEntries()) == 0 {
		t.Fatalf("metadata B did not carry apply A: queue=%d work=%+v", leader.pipelined.applyQueue.len(), applyWork)
	}
	applyA := applyWork.message.GetEntries()[len(applyWork.message.GetEntries())-1].GetIndex()
	if applyA > stableA {
		t.Fatalf("apply A index=%d exceeds durable A=%d", applyA, stableA)
	}

	var settled []AppliedBatchSource
	settle := func(batch AppliedBatch) error {
		settled = append(settled, batch.Source())
		return nil
	}
	appliedBefore := leader.apply.Applied()
	if applyA <= appliedBefore {
		t.Fatalf("apply A index=%d is not ahead of applied=%d", applyA, appliedBefore)
	}
	progressedBeforeRelease := false
	var blocked DriveResult
	for step := 0; step < 64; step++ {
		blocked = driveApplyDependencyStep(t, leader, &workspace, func(OutboundMessage) error { return nil }, settle)
		if leader.apply.Applied() >= applyA {
			progressedBeforeRelease = true
			break
		}
		if _, done, err := fixture.persist[0].Poll(); err != nil {
			t.Fatal(err)
		} else if done {
			t.Fatalf("metadata B completed while unrelated fsync was held")
		}
	}
	if !progressedBeforeRelease {
		t.Errorf("metadata-only B blocked durable A apply: result=%+v applied=%d requiredReady=%d processedReady=%d", blocked, leader.apply.Applied(), applyWork.requiredAppendReadyID, leader.pipelined.appendProcessedID)
	} else {
		settledA := false
		for _, source := range settled {
			if source.Group == leader.identity.Group && source.LastIndex >= applyA {
				settledA = true
				break
			}
		}
		if !settledA {
			t.Errorf("metadata-only B allowed apply without exact leader settlement: applied=%d settlements=%+v", leader.apply.Applied(), settled)
		}
	}

	releaseOnce.Do(func() { close(release) })
	driveApplyDependencyRuntimeWithSettlement(t, leader, fixture.persist[0], settle)
	driveApplyDependencyRuntimeWithSettlement(t, other, fixture.persist[1], settleTestApplied)
	if leader.apply.Applied() < applyA {
		t.Fatalf("A did not settle after metadata B completion: applied=%d want >=%d", leader.apply.Applied(), applyA)
	}
	settledA := false
	for _, source := range settled {
		if source.Group == leader.identity.Group && source.LastIndex >= applyA {
			settledA = true
			break
		}
	}
	if !settledA {
		t.Fatalf("A apply produced no exact leader settlement: applied=%d settlements=%+v", leader.apply.Applied(), settled)
	}
}

func pipelinedTestMetadataAppendMessage(memberID, readyID uint64) *pb.Message {
	term, vote, commit := uint64(1), memberID, uint64(1)
	localAppend := uint64(raft.LocalAppendThread)
	return &pb.Message{
		Type: pb.MsgStorageAppend.Enum(), From: &memberID, To: &localAppend,
		Term: &term, Vote: &vote, Commit: &commit,
		Context: []byte{byte(readyID)},
	}
}

func TestPipelinedApplyWatermarkRetainsPendingAppend(t *testing.T) {
	base, store, p := newPipelinedNodeSeriesHarness(t, 254)
	p.appendVote = base.MemberID
	p.durableVote = base.MemberID
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	store.SetDataSyncForTesting(func(file *os.File) error {
		blocked := false
		enterOnce.Do(func() {
			close(entered)
			blocked = true
		})
		if blocked {
			<-release
		}
		return file.Sync()
	})

	entryAppend := pipelinedTestAppendMessage(base.MemberID, 2, 1)
	if err := p.enqueueAppend(entryAppend); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("entry append did not enter the fsync gate")
	}
	metadataAppend := pipelinedTestMetadataAppendMessage(base.MemberID, 2)
	if err := p.enqueueAppend(metadataAppend); err != nil {
		t.Fatal(err)
	}
	if p.lastRequiredAppendReadyID != 1 {
		t.Fatalf("pending entry watermark=%d want 1", p.lastRequiredAppendReadyID)
	}
	applyMessage := &pb.Message{
		Type: pb.MsgStorageApply.Enum(), From: &base.MemberID,
		To:      func() *uint64 { target := uint64(raft.LocalApplyThread); return &target }(),
		Entries: entryAppend.Entries,
	}
	if !p.applyQueue.push(pipelinedApplyTask{
		message: applyMessage, requiredAppendReadyID: p.lastRequiredAppendReadyID,
	}) {
		t.Fatal("apply queue rejected pending entry")
	}
	task, ok := p.applyQueue.front()
	if !ok || task.requiredAppendReadyID <= p.appendProcessedID {
		t.Fatalf("pending entry dependency=%+v processed=%d", task, p.appendProcessedID)
	}

	releaseOnce.Do(func() { close(release) })
	if result := waitPipelinedAppendResult(t, p); result.ReadyID != 1 {
		t.Fatalf("entry append completion=%+v", result)
	}
	if task, ok = p.applyQueue.front(); !ok || task.requiredAppendReadyID > p.appendProcessedID {
		t.Fatalf("dependency remained blocked after entry completion: task=%+v processed=%d", task, p.appendProcessedID)
	}
	if result := waitPipelinedAppendResult(t, p); result.ReadyID != 2 {
		t.Fatalf("metadata append completion=%+v", result)
	}
}

func TestPipelinedApplyWatermarkRejectsUnsafeMetadataShapes(t *testing.T) {
	tests := []struct {
		name      string
		message   func(raftstore.Identity) *pb.Message
		waitError bool
		wantEarly bool
	}{
		{
			name: "entries",
			message: func(identity raftstore.Identity) *pb.Message {
				return pipelinedTestAppendMessage(identity.MemberID, 2, 1)
			},
		},
		{
			name: "response",
			message: func(identity raftstore.Identity) *pb.Message {
				message := pipelinedTestMetadataAppendMessage(identity.MemberID, 1)
				term := uint64(1)
				to := identity.MemberID + 1
				message.Responses = []*pb.Message{{Type: pb.MsgApp.Enum(), From: &identity.MemberID, To: &to, Term: &term}}
				return message
			},
			wantEarly: true,
		},
		{
			name: "term-change",
			message: func(identity raftstore.Identity) *pb.Message {
				message := pipelinedTestMetadataAppendMessage(identity.MemberID, 1)
				term := uint64(2)
				message.Term = &term
				return message
			},
		},
		{
			name: "vote-change",
			message: func(identity raftstore.Identity) *pb.Message {
				message := pipelinedTestMetadataAppendMessage(identity.MemberID, 1)
				vote := identity.MemberID + 1
				message.Vote = &vote
				return message
			},
		},
		{
			name: "snapshot",
			message: func(identity raftstore.Identity) *pb.Message {
				message := pipelinedTestMetadataAppendMessage(identity.MemberID, 1)
				index, term := uint64(1), uint64(1)
				message.Snapshot = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{
					Index: &index, Term: &term,
					ConfState: &pb.ConfState{Voters: []uint64{identity.MemberID}},
				}}
				return message
			},
		},
		{
			name: "missing-hard-state",
			message: func(identity raftstore.Identity) *pb.Message {
				term := uint64(0)
				localAppend := uint64(raft.LocalAppendThread)
				return &pb.Message{Type: pb.MsgStorageAppend.Enum(), From: &identity.MemberID, To: &localAppend, Term: nil, Vote: nil, Commit: nil, Context: []byte{1}, Index: &term}
			},
		},
		{
			name: "missing-vote",
			message: func(identity raftstore.Identity) *pb.Message {
				message := pipelinedTestMetadataAppendMessage(identity.MemberID, 1)
				message.Vote = nil
				return message
			},
		},
		{
			name: "missing-commit",
			message: func(identity raftstore.Identity) *pb.Message {
				message := pipelinedTestMetadataAppendMessage(identity.MemberID, 1)
				message.Commit = nil
				return message
			},
			waitError: true,
		},
		{
			name: "regressing-commit",
			message: func(identity raftstore.Identity) *pb.Message {
				message := pipelinedTestMetadataAppendMessage(identity.MemberID, 1)
				commit := uint64(0)
				message.Commit = &commit
				return message
			},
			waitError: true,
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base, _, p := newPipelinedNodeSeriesHarness(t, byte(230+index))
			p.appendVote, p.durableVote, p.appendCommit = base.MemberID, base.MemberID, 1
			if err := p.enqueueAppend(test.message(base)); err != nil {
				t.Fatal(err)
			}
			if p.lastRequiredAppendReadyID != 1 {
				t.Fatalf("unsafe metadata shape did not advance watermark: %d", p.lastRequiredAppendReadyID)
			}
			if test.wantEarly && p.directQueue.len() != 1 {
				t.Fatalf("early response queue=%d want 1", p.directQueue.len())
			}
			if test.waitError {
				if _, err := p.runtime.nodePersistence.Wait(); err == nil {
					t.Fatal("unsafe metadata shape unexpectedly persisted")
				}
				return
			}
			if result := waitPipelinedAppendResult(t, p); result.ReadyID != 1 {
				t.Fatalf("append completion=%+v", result)
			}
		})
	}
}
