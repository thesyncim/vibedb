package raftmodel

import (
	"crypto/sha256"
	"errors"
	"math"
	"slices"
	"testing"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestReadyCannotSendOrAcceptInputBeforePersistence(t *testing.T) {
	node, stable, _ := newTestNode(t, 1, []uint64{1, 2, 3})
	if err := node.Campaign(); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	captured, err := node.CaptureReady()
	if err != nil || !captured {
		t.Fatalf("CaptureReady() = %v, %v", captured, err)
	}

	sent := 0
	if err := node.DrainMessages(func(*pb.Message) error { sent++; return nil }); !errors.Is(err, ErrWrongPhase) {
		t.Fatalf("DrainMessages before persist error = %v, want ErrWrongPhase", err)
	}
	if sent != 0 {
		t.Fatalf("sent %d messages before persistence", sent)
	}
	for name, err := range map[string]error{
		"campaign": node.Campaign(),
		"propose":  node.Propose([]byte("blocked")),
		"read":     node.ReadIndex([]byte("blocked")),
		"step": node.Step(&pb.Message{
			Type: pb.MsgHeartbeat.Enum(),
			From: uint64Ptr(2),
			To:   uint64Ptr(1),
		}),
		"tick": node.Tick(),
	} {
		if !errors.Is(err, ErrWrongPhase) {
			t.Errorf("%s while Ready outstanding error = %v, want ErrWrongPhase", name, err)
		}
	}

	if err := node.PersistReady(); err != nil {
		t.Fatalf("PersistReady() error = %v", err)
	}
	if len(stable.batches) != 1 || stable.batches[0].ReadyID == 0 {
		t.Fatalf("persisted batches = %+v", stable.batches)
	}
	if err := node.DrainMessages(func(*pb.Message) error { sent++; return nil }); err != nil {
		t.Fatalf("DrainMessages() error = %v", err)
	}
	if sent == 0 {
		t.Fatal("campaign produced no outbound messages after persistence")
	}
}

func TestPendingReadyInputIsBoundedBeforeCapture(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1, 2})
	term := uint64(2)
	entries := make([]*pb.Entry, MaxPendingInputUnits)
	for ordinal := range entries {
		entries[ordinal] = &pb.Entry{Term: &term, Index: uint64Ptr(uint64(ordinal + 2))}
	}
	if err := node.Step(&pb.Message{
		Type: pb.MsgApp.Enum(), From: uint64Ptr(2), To: uint64Ptr(1), Term: &term,
		Index: uint64Ptr(1), LogTerm: uint64Ptr(1), Commit: uint64Ptr(1), Entries: entries,
	}); err != nil {
		t.Fatalf("Step(maximum entry batch) error = %v", err)
	}
	if ready, err := node.HasReady(); err != nil || !ready {
		t.Fatalf("HasReady() = %v, %v", ready, err)
	}

	status := node.Status()
	for name, err := range map[string]error{
		"campaign": node.Campaign(),
		"propose":  node.Propose([]byte("second")),
		"read":     node.ReadIndex([]byte("pending")),
		"step": node.Step(&pb.Message{
			Type: pb.MsgHeartbeat.Enum(), From: uint64Ptr(2), To: uint64Ptr(1),
			Term: uint64Ptr(status.GetTerm()), Commit: uint64Ptr(status.GetCommit()),
		}),
		"tick": node.Tick(),
	} {
		if !errors.Is(err, ErrReadyPending) || !errors.Is(err, ErrAdmissionBound) {
			t.Errorf("%s beyond uncaptured Ready bound error = %v", name, err)
		}
	}
	change := &pb.ConfChange{Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: uint64Ptr(2)}
	if err := node.ProposeConfChange(change); !errors.Is(err, ErrReadyPending) || !errors.Is(err, ErrConfChangePending) {
		t.Fatalf("ProposeConfChange with uncaptured Ready error = %v", err)
	}
	if node.PendingReads() != 0 {
		t.Fatalf("rejected ReadIndex retained %d requests", node.PendingReads())
	}

	if captured, err := node.CaptureReady(); err != nil || !captured {
		t.Fatalf("CaptureReady() = %v, %v", captured, err)
	}
	if len(node.ready.Entries) != MaxPendingInputUnits {
		t.Fatalf("captured entries = %d, want %d", len(node.ready.Entries), MaxPendingInputUnits)
	}
}

func TestPendingReadyControlCallsAreBoundedBeforeCapture(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1, 2})
	term := uint64(2)
	heartbeat := &pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: uint64Ptr(2), To: uint64Ptr(1),
		Term: &term, Commit: uint64Ptr(1),
	}
	for call := range MaxPendingInputCalls {
		if err := node.Step(heartbeat); err != nil {
			t.Fatalf("Step(heartbeat %d) error = %v", call, err)
		}
	}
	if err := node.Step(heartbeat); !errors.Is(err, ErrReadyPending) || !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("Step beyond pending call bound error = %v", err)
	}
	if captured, err := node.CaptureReady(); err != nil || !captured {
		t.Fatalf("CaptureReady() = %v, %v", captured, err)
	}
	if len(node.ready.Messages) != MaxPendingInputCalls {
		t.Fatalf("captured messages = %d, want %d", len(node.ready.Messages), MaxPendingInputCalls)
	}
}

func TestPendingReadyPayloadBytesAreBoundedBeforeCapture(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1, 2})
	term := uint64(2)
	heartbeat := &pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: uint64Ptr(2), To: uint64Ptr(1),
		Term: &term, Commit: uint64Ptr(1),
	}
	if err := node.Step(heartbeat); err != nil {
		t.Fatalf("Step(heartbeat) error = %v", err)
	}
	node.pendingInputBytes = MaxPendingInputBytes - 1
	appendOne := &pb.Message{
		Type: pb.MsgApp.Enum(), From: uint64Ptr(2), To: uint64Ptr(1), Term: &term,
		Index: uint64Ptr(1), LogTerm: uint64Ptr(1), Commit: uint64Ptr(1),
		Entries: []*pb.Entry{{Term: &term, Index: uint64Ptr(2), Data: []byte{'x'}}},
	}
	if err := node.Step(appendOne); err != nil {
		t.Fatalf("Step at exact pending byte bound error = %v", err)
	}
	appendTwo := proto.Clone(appendOne).(*pb.Message)
	appendTwo.Index = uint64Ptr(2)
	appendTwo.LogTerm = &term
	appendTwo.Entries[0].Index = uint64Ptr(3)
	if err := node.Step(appendTwo); !errors.Is(err, ErrReadyPending) || !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("Step beyond pending byte bound error = %v", err)
	}
}

func TestNormalProposalsShareOneBoundedUncapturedReady(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1})
	driveCampaign(t, node)

	first := []byte("first")
	second := []byte("second")
	if err := node.Propose(first); err != nil {
		t.Fatalf("Propose(first) error = %v", err)
	}
	if err := node.Propose(second); err != nil {
		t.Fatalf("Propose(second) error = %v", err)
	}
	first[0] = 'X'
	second[0] = 'Y'

	if captured, err := node.CaptureReady(); err != nil || !captured {
		t.Fatalf("CaptureReady() = %v, %v", captured, err)
	}
	if len(node.ready.Entries) != 2 || string(node.ready.Entries[0].GetData()) != "first" ||
		string(node.ready.Entries[1].GetData()) != "second" {
		t.Fatalf(
			"batched Ready entries=%d data=%q/%q",
			len(node.ready.Entries),
			node.ready.Entries[0].GetData(), node.ready.Entries[1].GetData(),
		)
	}
}

func TestCommitMetricsAdvanceOnlyAtCoreCommitAuthority(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1})
	if got := node.CommitMetrics(); got != (CommitMetrics{}) {
		t.Fatalf("recovery metrics = %+v, want zero baseline", got)
	}
	driveCampaign(t, node)
	afterElection := node.CommitMetrics()
	if afterElection.Advancements != 1 || afterElection.Entries != 1 {
		t.Fatalf("election metrics = %+v, want one committed no-op", afterElection)
	}
	if err := node.Propose([]byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := node.Propose([]byte("two")); err != nil {
		t.Fatal(err)
	}
	if got := node.CommitMetrics(); got != afterElection {
		t.Fatalf("unpersisted proposals changed commit metrics: got %+v want %+v", got, afterElection)
	}
	driveAllReady(t, node)
	if got := node.CommitMetrics(); got.Advancements != afterElection.Advancements+1 ||
		got.Entries != afterElection.Entries+2 {
		t.Fatalf("post-authority metrics = %+v, want advancements=%d entries=%d",
			got, afterElection.Advancements+1, afterElection.Entries+2)
	}
}

func BenchmarkNodeObserveCommitAdvancementNoChange(b *testing.B) {
	node, _, _ := newTestNode(b, 1, []uint64{1})
	b.ReportAllocs()
	for b.Loop() {
		node.observeCommitAdvancement()
	}
}

func TestProposalBatchLimitsMatchUncapturedReadyWindow(t *testing.T) {
	if MaxProposalBatchEntries != MaxPendingInputCalls ||
		MaxProposalBatchEntries > MaxPendingInputUnits ||
		MaxProposalBatchBytes != MaxSizePerMsg ||
		MaxProposalBatchBytes >= MaxPendingInputBytes {
		t.Fatalf(
			"proposal batch limits entries=%d bytes=%d; input calls=%d units=%d hard bytes=%d",
			MaxProposalBatchEntries, MaxProposalBatchBytes, MaxPendingInputCalls,
			MaxPendingInputUnits, MaxPendingInputBytes,
		)
	}
}

func TestStepOwnsRetainedMessageGraph(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1, 2})
	term, index, commit := uint64(2), uint64(2), uint64(1)
	data := []byte("transport-owned")
	entry := &pb.Entry{Term: &term, Index: &index, Data: data}
	message := &pb.Message{
		Type: pb.MsgApp.Enum(), From: uint64Ptr(2), To: uint64Ptr(1), Term: &term,
		Index: uint64Ptr(1), LogTerm: uint64Ptr(1), Commit: &commit,
		Entries: []*pb.Entry{entry},
	}
	if err := node.Step(message); err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	data[0] = 'X'
	entry.Data[1] = 'Y'
	*entry.Index = 99
	message.Entries = nil
	if captured, err := node.CaptureReady(); err != nil || !captured {
		t.Fatalf("CaptureReady() = %v, %v", captured, err)
	}
	if len(node.ready.Entries) != 1 || node.ready.Entries[0].GetIndex() != 2 ||
		string(node.ready.Entries[0].GetData()) != "transport-owned" {
		t.Fatalf("retained entry = %+v", node.ready.Entries)
	}
}

func TestStepAdmitsOnlyBoundedRemoteProtocolMessages(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1, 2})
	base := &pb.Message{From: uint64Ptr(2), To: uint64Ptr(1)}
	proposal := proto.Clone(base).(*pb.Message)
	proposal.Type = pb.MsgProp.Enum()
	if err := node.Step(proposal); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("remote proposal error = %v", err)
	}
	wrongTarget := proto.Clone(base).(*pb.Message)
	wrongTarget.Type = pb.MsgHeartbeat.Enum()
	wrongTarget.To = uint64Ptr(2)
	if err := node.Step(wrongTarget); err == nil {
		t.Fatal("accepted wrong-target message")
	}
	reservedSource := proto.Clone(base).(*pb.Message)
	reservedSource.Type = pb.MsgHeartbeat.Enum()
	reservedSource.From = uint64Ptr(raft.LocalAppendThread)
	if err := node.Step(reservedSource); err == nil {
		t.Fatal("accepted local storage-thread identity as remote sender")
	}
	overflow := proto.Clone(base).(*pb.Message)
	overflow.Type = pb.MsgApp.Enum()
	overflow.Entries = make([]*pb.Entry, MaxMessageEntries+1)
	if err := node.Step(overflow); !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("entry-count overflow error = %v", err)
	}
	term, index := uint64(2), uint64(2)
	malformed := proto.Clone(base).(*pb.Message)
	malformed.Type = pb.MsgApp.Enum()
	malformed.Entries = []*pb.Entry{
		{Term: &term, Index: &index}, {Term: &term, Index: uint64Ptr(4)},
	}
	if err := node.Step(malformed); err == nil {
		t.Fatal("accepted gapped inbound entries")
	}
}

func TestRecoveryRejectsMalformedDurableRangesWithoutPanic(t *testing.T) {
	_, stable, machine := newTestNode(t, 1, []uint64{1})
	for name, hard := range map[string]*pb.HardState{
		"below snapshot": {Term: uint64Ptr(1), Commit: uint64Ptr(0)},
		"above last":     {Term: uint64Ptr(1), Commit: uint64Ptr(2)},
	} {
		t.Run(name, func(t *testing.T) {
			copyStable := *stable
			memory := cloneTestMemoryStorage(t, stable.MemoryStorage)
			copyStable.MemoryStorage = memory
			if err := memory.SetHardState(hard); err != nil {
				t.Fatal(err)
			}
			if _, err := NewNode(1, 2, &copyStable, machine); err == nil {
				t.Fatal("NewNode accepted malformed durable commit")
			}
		})
	}
	overflow := &lastIndexStable{fakeStable: stable, last: math.MaxUint64}
	if _, err := NewNode(1, 2, overflow, machine); err == nil {
		t.Fatal("NewNode accepted terminal last index")
	}

	badMachine := *machine
	badMachine.pub.ConfState = &pb.ConfState{Voters: []uint64{1, 1}}
	if _, err := NewNode(1, 2, stable, &badMachine); err == nil {
		t.Fatal("NewNode accepted malformed ConfState")
	}
	zeroVersionMachine := *machine
	zeroVersionMachine.pub.ReplicaSetVersion = 0
	if _, err := NewNode(1, 2, stable, &zeroVersionMachine); err == nil {
		t.Fatal("NewNode accepted nonempty ConfState with zero ReplicaSetVersion")
	}
}

func TestRecoveryReconcilesDurableSnapshotBeforeRawNode(t *testing.T) {
	_, stable, machine := newTestNode(t, 1, []uint64{1})
	index, term := uint64(5), uint64(3)
	snapshot := &pb.Snapshot{
		Data: []byte("durable-snapshot"),
		Metadata: &pb.SnapshotMetadata{
			ConfState: &pb.ConfState{Voters: []uint64{1}},
			Index:     &index,
			Term:      &term,
		},
	}
	if err := stable.MemoryStorage.ApplySnapshot(snapshot); err != nil {
		t.Fatalf("ApplySnapshot() error = %v", err)
	}
	if err := stable.MemoryStorage.SetHardState(&pb.HardState{
		Term: &term, Commit: &index,
	}); err != nil {
		t.Fatalf("SetHardState() error = %v", err)
	}

	restarted, err := NewNode(1, 2, stable, machine)
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}
	if machine.snapshotCalls != 1 {
		t.Fatalf("snapshot installs = %d, want 1", machine.snapshotCalls)
	}
	publication := machine.Published()
	if publication.Applied != index || publication.DataChainDigest != sha256.Sum256(snapshot.GetData()) {
		t.Fatalf("reconciled publication = %+v", publication)
	}
	if got := restarted.Published(); !equalPublication(got, publication) {
		t.Fatalf("node publication = %+v, want %+v", got, publication)
	}
}

func TestReplaceStateMachineRequiresExactQuiescentPublication(t *testing.T) {
	node, _, source := newTestNode(t, 1, []uint64{1})
	target := *source
	target.pub = clonePublication(source.pub)
	if err := node.ReplaceStateMachine(&target); err != nil {
		t.Fatal(err)
	}
	if node.machine != &target {
		t.Fatal("replacement machine was not published")
	}
	mismatch := target
	mismatch.pub.DataChainDigest[0]++
	if err := node.ReplaceStateMachine(&mismatch); !errors.Is(err, ErrPublicationMismatch) {
		t.Fatalf("mismatched replacement error=%v", err)
	}
	node.pendingInputCalls = 1
	if err := node.ReplaceStateMachine(source); !errors.Is(err, ErrReadyPending) {
		t.Fatalf("non-quiescent replacement error=%v", err)
	}
}

func TestRecoveryBootstrapsEmptyMachineFromDurableSnapshot(t *testing.T) {
	_, stable, _ := newTestNode(t, 1, []uint64{1})
	machine := &fakeStateMachine{pub: Publication{ConfState: new(pb.ConfState)}}
	restarted, err := NewNode(1, 2, stable, machine)
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}
	if machine.Applied() != 1 || machine.snapshotCalls != 1 {
		t.Fatalf("bootstrap publication index=%d installs=%d", machine.Applied(), machine.snapshotCalls)
	}
	if got := restarted.Published(); got.ReplicaSetVersion != 1 || !confStateHasMembers(got.ConfState) {
		t.Fatalf("bootstrap publication = %+v", got)
	}
}

func TestRecoveryDurableSnapshotInstallFailureCanRetry(t *testing.T) {
	_, stable, machine := newTestNode(t, 1, []uint64{1})
	index, term := uint64(4), uint64(2)
	snapshot := &pb.Snapshot{
		Data: []byte("retryable-snapshot"),
		Metadata: &pb.SnapshotMetadata{
			ConfState: &pb.ConfState{Voters: []uint64{1}},
			Index:     &index,
			Term:      &term,
		},
	}
	if err := stable.MemoryStorage.ApplySnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := stable.MemoryStorage.SetHardState(&pb.HardState{
		Term: &term, Commit: &index,
	}); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("injected snapshot install failure")
	machine.fail = wantErr
	if _, err := NewNode(1, 2, stable, machine); !errors.Is(err, wantErr) {
		t.Fatalf("NewNode() error = %v, want %v", err, wantErr)
	}
	if machine.Applied() != 1 {
		t.Fatalf("failed install published index %d, want 1", machine.Applied())
	}
	machine.fail = nil
	if _, err := NewNode(1, 2, stable, machine); err != nil {
		t.Fatalf("NewNode() retry error = %v", err)
	}
	if machine.Applied() != index || machine.snapshotCalls != 1 {
		t.Fatalf("retry publication index=%d installs=%d", machine.Applied(), machine.snapshotCalls)
	}
}

func TestRecoveryRevalidatesAlreadyPublishedDurableSnapshot(t *testing.T) {
	_, stable, machine := newTestNode(t, 1, []uint64{1})
	index, term := uint64(6), uint64(4)
	snapshot := &pb.Snapshot{
		Data: []byte("already-published"),
		Metadata: &pb.SnapshotMetadata{
			ConfState: &pb.ConfState{Voters: []uint64{1}},
			Index:     &index,
			Term:      &term,
		},
	}
	if err := stable.MemoryStorage.ApplySnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := stable.MemoryStorage.SetHardState(&pb.HardState{
		Term: &term, Commit: &index,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := machine.InstallSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	if machine.snapshotCalls != 1 {
		t.Fatalf("setup installs = %d, want 1", machine.snapshotCalls)
	}
	if _, err := NewNode(1, 2, stable, machine); err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}
	if machine.snapshotCalls != 2 {
		t.Fatalf("restart exact-snapshot checks=%d, want 2", machine.snapshotCalls)
	}
}

func TestRecoverySettlesSnapshotPublishedBeforeInstallError(t *testing.T) {
	_, stable, machine := newTestNode(t, 1, []uint64{1})
	index, term := uint64(7), uint64(5)
	snapshot := &pb.Snapshot{
		Data: []byte("published-before-error"),
		Metadata: &pb.SnapshotMetadata{
			ConfState: &pb.ConfState{Voters: []uint64{1}},
			Index:     &index,
			Term:      &term,
		},
	}
	if err := stable.MemoryStorage.ApplySnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := stable.MemoryStorage.SetHardState(&pb.HardState{
		Term: &term, Commit: &index,
	}); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("snapshot publication outcome unknown")
	machine.snapshotFailAfterPublish = wantErr
	if _, err := NewNode(1, 2, stable, machine); !errors.Is(err, wantErr) {
		t.Fatalf("NewNode() error = %v, want %v", err, wantErr)
	}
	if machine.Applied() != index || machine.snapshotCalls != 1 {
		t.Fatalf("ambiguous install index=%d calls=%d", machine.Applied(), machine.snapshotCalls)
	}
	machine.snapshotFailAfterPublish = nil
	if _, err := NewNode(1, 2, stable, machine); err != nil {
		t.Fatalf("NewNode() settlement error = %v", err)
	}
	if machine.snapshotCalls != 2 {
		t.Fatalf("settlement exact-snapshot checks=%d, want 2", machine.snapshotCalls)
	}
}

func TestRecoveryRejectsDifferentSnapshotBytesAtPublishedCut(t *testing.T) {
	_, _, machine := newTestNode(t, 1, []uint64{1})
	index, term := uint64(1), uint64(1)
	different := &pb.Snapshot{
		Data: []byte("different-state-at-the-same-cut"),
		Metadata: &pb.SnapshotMetadata{
			ConfState: &pb.ConfState{Voters: []uint64{1}},
			Index:     &index,
			Term:      &term,
		},
	}
	memory := raft.NewMemoryStorage()
	if err := memory.ApplySnapshot(different); err != nil {
		t.Fatal(err)
	}
	if err := memory.SetHardState(&pb.HardState{Term: &term, Commit: &index}); err != nil {
		t.Fatal(err)
	}
	stable := &fakeStable{
		MemoryStorage: memory,
		conf:          cloneConfState(different.GetMetadata().GetConfState()),
		durableIDs:    make(map[readyKey]struct{}),
	}
	if _, err := NewNode(1, 2, stable, machine); err == nil {
		t.Fatal("NewNode accepted different snapshot bytes at the published cut")
	}
}

func TestRecoveryRejectsAmbiguousSnapshotVersionRegression(t *testing.T) {
	_, stable, machine := newTestNode(t, 1, []uint64{1})
	index, term := uint64(8), uint64(6)
	snapshot := &pb.Snapshot{
		Data: []byte("version-bound-snapshot"),
		Metadata: &pb.SnapshotMetadata{
			ConfState: &pb.ConfState{Voters: []uint64{1}},
			Index:     &index,
			Term:      &term,
		},
	}
	if err := stable.MemoryStorage.ApplySnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := stable.MemoryStorage.SetHardState(&pb.HardState{
		Term: &term, Commit: &index,
	}); err != nil {
		t.Fatal(err)
	}
	badVersion := uint64(0)
	wantErr := errors.New("ambiguous invalid snapshot publication")
	machine.snapshotBadVersionAfterPublish = &badVersion
	machine.snapshotFailAfterPublish = wantErr
	if _, err := NewNode(1, 2, stable, machine); !errors.Is(err, wantErr) {
		t.Fatalf("NewNode() error = %v, want %v", err, wantErr)
	}
	machine.snapshotBadVersionAfterPublish = nil
	machine.snapshotFailAfterPublish = nil
	if _, err := NewNode(1, 2, stable, machine); err == nil {
		t.Fatal("NewNode accepted the regressed ambiguous snapshot publication")
	}
}

func TestRecoveryRejectsPublicationMismatchAtDurableSnapshotCut(t *testing.T) {
	_, stable, machine := newTestNode(t, 1, []uint64{1})
	index, term := uint64(3), uint64(2)
	snapshot := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{
		ConfState: &pb.ConfState{Voters: []uint64{1, 2}},
		Index:     &index,
		Term:      &term,
	}}
	if err := stable.MemoryStorage.ApplySnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := stable.MemoryStorage.SetHardState(&pb.HardState{
		Term: &term, Commit: &index,
	}); err != nil {
		t.Fatal(err)
	}
	machine.pub.Applied = index
	if _, err := NewNode(1, 2, stable, machine); err == nil {
		t.Fatal("NewNode accepted a publication whose ConfState differs at the snapshot cut")
	}
}

func TestMessageMicroStepsExposeCrashCuts(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1, 2, 3})
	if err := node.Campaign(); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	if captured, err := node.CaptureReady(); err != nil || !captured {
		t.Fatalf("CaptureReady() = %v, %v", captured, err)
	}
	if len(node.ready.Messages) < 2 {
		t.Fatalf("campaign messages = %d, want at least two", len(node.ready.Messages))
	}
	if err := node.PersistReady(); err != nil {
		t.Fatalf("PersistReady() error = %v", err)
	}
	if err := node.InstallSnapshot(); !errors.Is(err, ErrWrongPhase) {
		t.Fatalf("InstallSnapshot before FinishMessages error = %v", err)
	}
	if err := node.AdvanceReady(); !errors.Is(err, ErrWrongPhase) {
		t.Fatalf("AdvanceReady before FinishMessages error = %v", err)
	}
	if err := node.FinishMessages(); err == nil {
		t.Fatal("FinishMessages succeeded with unsent messages")
	}

	wantSendErr := errors.New("injected transport failure")
	sent, err := node.SendNextMessage(func(*pb.Message) error { return wantSendErr })
	if sent || !errors.Is(err, wantSendErr) {
		t.Fatalf("SendNextMessage(failure) = %v, %v", sent, err)
	}
	if node.messagePos != 0 {
		t.Fatalf("message position advanced after error: %d", node.messagePos)
	}

	var destinations []uint64
	sent, err = node.SendNextMessage(func(message *pb.Message) error {
		destinations = append(destinations, message.GetTo())
		return nil
	})
	if err != nil || !sent {
		t.Fatalf("SendNextMessage(first) = %v, %v", sent, err)
	}
	if node.Phase() != PhasePersisted || node.messagePos != 1 {
		t.Fatalf("first-message crash cut phase=%s position=%d", node.Phase(), node.messagePos)
	}
	if err := node.InstallSnapshot(); !errors.Is(err, ErrWrongPhase) {
		t.Fatalf("InstallSnapshot at message crash cut error = %v", err)
	}

	for {
		sent, err = node.SendNextMessage(func(message *pb.Message) error {
			destinations = append(destinations, message.GetTo())
			return nil
		})
		if err != nil {
			t.Fatalf("SendNextMessage() error = %v", err)
		}
		if !sent {
			break
		}
	}
	if len(destinations) != len(node.ready.Messages) {
		t.Fatalf("sent destinations = %v, messages = %d", destinations, len(node.ready.Messages))
	}
	if err := node.InstallSnapshot(); !errors.Is(err, ErrWrongPhase) {
		t.Fatalf("InstallSnapshot after final send but before finish error = %v", err)
	}
	if err := node.FinishMessages(); err != nil {
		t.Fatalf("FinishMessages() error = %v", err)
	}
	if node.Phase() != PhaseMessagesDrained {
		t.Fatalf("phase after FinishMessages = %s", node.Phase())
	}
}

func TestFinishMessagesHandlesEmptyBatch(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1})
	if err := node.Campaign(); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	if captured, err := node.CaptureReady(); err != nil || !captured {
		t.Fatalf("CaptureReady() = %v, %v", captured, err)
	}
	if len(node.ready.Messages) != 0 {
		t.Fatalf("single-voter campaign messages = %d, want empty", len(node.ready.Messages))
	}
	if err := node.PersistReady(); err != nil {
		t.Fatalf("PersistReady() error = %v", err)
	}
	if sent, err := node.SendNextMessage(nil); err != nil || sent {
		t.Fatalf("SendNextMessage(empty) = %v, %v", sent, err)
	}
	if err := node.FinishMessages(); err != nil {
		t.Fatalf("FinishMessages(empty) error = %v", err)
	}
	if node.Phase() != PhaseMessagesDrained {
		t.Fatalf("phase after empty FinishMessages = %s", node.Phase())
	}
}

func TestSnapshotMessageRejectedBeforeAnySend(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1, 2, 3})
	if err := node.Campaign(); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	if captured, err := node.CaptureReady(); err != nil || !captured {
		t.Fatalf("CaptureReady() = %v, %v", captured, err)
	}
	node.ready.Messages = append(node.ready.Messages, &pb.Message{Type: pb.MsgSnap.Enum()})
	if err := node.PersistReady(); err != nil {
		t.Fatalf("PersistReady() error = %v", err)
	}
	callbackCalls := 0
	sent, err := node.SendNextMessage(func(*pb.Message) error {
		callbackCalls++
		return nil
	})
	if sent || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("SendNextMessage(MsgSnap) = %v, %v", sent, err)
	}
	if callbackCalls != 0 || node.messagePos != 0 {
		t.Fatalf("snapshot rejection callbackCalls=%d messagePos=%d", callbackCalls, node.messagePos)
	}
}

func TestPersistFailureRetainsExactReadyForRetry(t *testing.T) {
	node, stable, _ := newTestNode(t, 1, []uint64{1, 2, 3})
	if err := node.Campaign(); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	if captured, err := node.CaptureReady(); err != nil || !captured {
		t.Fatalf("CaptureReady() = %v, %v", captured, err)
	}
	readyID := node.ReadyID()
	wantErr := errors.New("injected fsync failure")
	stable.fail = wantErr
	if err := node.PersistReady(); !errors.Is(err, wantErr) {
		t.Fatalf("PersistReady() error = %v, want injected failure", err)
	}
	if node.Phase() != PhaseCaptured || node.ReadyID() != readyID {
		t.Fatalf("after failure phase=%s ReadyID=%d, want captured/%d", node.Phase(), node.ReadyID(), readyID)
	}
	if err := node.DrainMessages(func(*pb.Message) error { return nil }); !errors.Is(err, ErrWrongPhase) {
		t.Fatalf("DrainMessages after failed persist error = %v", err)
	}

	stable.fail = nil
	if err := node.PersistReady(); err != nil {
		t.Fatalf("PersistReady retry error = %v", err)
	}
	if len(stable.attemptIDs) != 2 || stable.attemptIDs[0] != readyID || stable.attemptIDs[1] != readyID {
		t.Fatalf("persist attempt IDs = %v, want [%d %d]", stable.attemptIDs, readyID, readyID)
	}
}

func TestCommittedEntriesReplayAfterCrashBeforeApply(t *testing.T) {
	node, stable, machine := newTestNode(t, 1, []uint64{1})
	driveCampaign(t, node)
	before := machine.Applied()
	if err := node.Propose([]byte("replay-me")); err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	captureCommittedReady(t, node, []byte("replay-me"))
	if err := node.PersistReady(); err != nil {
		t.Fatalf("PersistReady() error = %v", err)
	}
	if err := node.DrainMessages(func(*pb.Message) error { return nil }); err != nil {
		t.Fatalf("DrainMessages() error = %v", err)
	}
	if machine.Applied() != before {
		t.Fatalf("command applied before crash cut: got %d, want %d", machine.Applied(), before)
	}

	restarted, err := NewNode(1, 2, stable, machine)
	if err != nil {
		t.Fatalf("NewNode(restart) error = %v", err)
	}
	outcomes := driveAllReady(t, restarted)
	if len(outcomes) != 0 {
		t.Fatalf("unexpected read outcomes = %+v", outcomes)
	}
	if machine.Applied() != before+1 {
		t.Fatalf("replayed applied index = %d, want %d", machine.Applied(), before+1)
	}
	last := machine.calls[len(machine.calls)-1]
	if string(last.data) != "replay-me" {
		t.Fatalf("replayed data = %q", last.data)
	}
}

func TestNoopAndBothConfigurationEntryFormsPublishInOrder(t *testing.T) {
	node, _, machine := newTestNode(t, 1, []uint64{1})
	driveCampaign(t, node)
	if len(machine.calls) != 1 || machine.calls[0].meta.Type != pb.EntryNormal || len(machine.calls[0].data) != 0 {
		t.Fatalf("campaign apply calls = %+v, want one no-op", machine.calls)
	}
	initialDigest := machine.Published().DataChainDigest

	v1 := &pb.ConfChange{
		Type:   pb.ConfChangeAddLearnerNode.Enum(),
		NodeId: uint64Ptr(2),
	}
	if err := node.ProposeConfChange(v1); err != nil {
		t.Fatalf("ProposeConfChange(v1) error = %v", err)
	}
	driveAllReady(t, node)
	v2 := &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{
		Type:   pb.ConfChangeAddLearnerNode.Enum(),
		NodeId: uint64Ptr(3),
	}}}
	if err := node.ProposeConfChange(v2); err != nil {
		t.Fatalf("ProposeConfChange(v2) error = %v", err)
	}
	driveAllReady(t, node)

	if len(machine.calls) != 3 {
		t.Fatalf("apply call count = %d, want 3", len(machine.calls))
	}
	for i, call := range machine.calls {
		if i > 0 && call.meta.Index != machine.calls[i-1].meta.Index+1 {
			t.Fatalf("call indexes are not contiguous: %+v", machine.calls)
		}
	}
	if machine.calls[1].meta.Type != pb.EntryConfChange || machine.calls[2].meta.Type != pb.EntryConfChangeV2 {
		t.Fatalf("configuration entry types = %v, %v", machine.calls[1].meta.Type, machine.calls[2].meta.Type)
	}
	publication := machine.Published()
	if publication.ReplicaSetVersion != machine.calls[2].meta.Index {
		t.Fatalf("ReplicaSetVersion = %d, want %d", publication.ReplicaSetVersion, machine.calls[2].meta.Index)
	}
	if publication.DataChainDigest != initialDigest {
		t.Fatal("configuration entries changed data-chain digest")
	}
	if !slices.Contains(publication.ConfState.Learners, 2) || !slices.Contains(publication.ConfState.Learners, 3) {
		t.Fatalf("published learners = %v, want 2 and 3", publication.ConfState.Learners)
	}
}

func TestAppliedConfigurationRecoversFromPublishedConfState(t *testing.T) {
	node, stable, machine := newTestNode(t, 1, []uint64{1})
	driveCampaign(t, node)
	change := &pb.ConfChange{
		Type:   pb.ConfChangeAddLearnerNode.Enum(),
		NodeId: uint64Ptr(2),
	}
	if err := node.ProposeConfChange(change); err != nil {
		t.Fatalf("ProposeConfChange(add learner) error = %v", err)
	}
	driveAllReady(t, node)
	if slices.Contains(stable.conf.Learners, 2) {
		t.Fatal("test log store unexpectedly updated its stale snapshot ConfState")
	}
	if !slices.Contains(machine.Published().ConfState.Learners, 2) {
		t.Fatal("state-machine publication did not retain applied ConfState")
	}

	restarted, err := NewNode(1, 2, stable, machine)
	if err != nil {
		t.Fatalf("NewNode(restart after configuration) error = %v", err)
	}
	driveCampaign(t, restarted)
	remove := &pb.ConfChange{
		Type:   pb.ConfChangeRemoveNode.Enum(),
		NodeId: uint64Ptr(2),
	}
	if err := restarted.ProposeConfChange(remove); err != nil {
		t.Fatalf("ProposeConfChange(remove recovered learner) error = %v", err)
	}
	driveAllReady(t, restarted)
	if slices.Contains(machine.Published().ConfState.Learners, 2) {
		t.Fatalf("published learners after removal = %v", machine.Published().ConfState.Learners)
	}
}

func TestImplicitJointConsensusIsRejected(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1})
	driveCampaign(t, node)
	change := &pb.ConfChangeV2{
		Transition: pb.ConfChangeTransitionJointImplicit.Enum(),
		Changes: []*pb.ConfChangeSingle{
			{Type: pb.ConfChangeAddNode.Enum(), NodeId: uint64Ptr(2)},
			{Type: pb.ConfChangeAddNode.Enum(), NodeId: uint64Ptr(3)},
		},
	}
	if err := node.ProposeConfChange(change); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("ProposeConfChange(implicit joint) error = %v, want ErrUnsupported", err)
	}
	change.Transition = pb.ConfChangeTransitionAuto.Enum()
	if err := node.ProposeConfChange(change); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("ProposeConfChange(automatic joint) error = %v, want ErrUnsupported", err)
	}
}

func TestConfigurationPreflightRejectsInvalidPublishedTransition(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1})
	driveCampaign(t, node)
	removeLastVoter := &pb.ConfChange{
		Type:   pb.ConfChangeRemoveNode.Enum(),
		NodeId: uint64Ptr(1),
	}
	if err := node.ProposeConfChange(removeLastVoter); err == nil {
		t.Fatal("ProposeConfChange accepted removal of the last voter")
	}
	if err := node.ProposeConfChange(&pb.ConfChangeV2{}); err == nil {
		t.Fatal("ProposeConfChange accepted leaving a non-joint configuration")
	}
}

func TestConfigurationProposalRefusesAmbiguousPendingWork(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1})
	driveCampaign(t, node)
	first := &pb.ConfChange{
		Type:   pb.ConfChangeAddLearnerNode.Enum(),
		NodeId: uint64Ptr(2),
	}
	if err := node.ProposeConfChange(first); err != nil {
		t.Fatalf("first ProposeConfChange error = %v", err)
	}
	second := &pb.ConfChange{
		Type:   pb.ConfChangeAddLearnerNode.Enum(),
		NodeId: uint64Ptr(3),
	}
	if err := node.ProposeConfChange(second); !errors.Is(err, ErrConfChangePending) {
		t.Fatalf("second ProposeConfChange error = %v, want ErrConfChangePending", err)
	}
	driveAllReady(t, node)
	if err := node.ProposeConfChange(second); err != nil {
		t.Fatalf("settled ProposeConfChange error = %v", err)
	}
}

func TestConfigurationMetadataIsRefusedUntilApplyPortCarriesIt(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1})
	driveCampaign(t, node)
	withContext := &pb.ConfChange{
		Type:    pb.ConfChangeAddLearnerNode.Enum(),
		NodeId:  uint64Ptr(2),
		Context: []byte("topology-binding"),
	}
	if err := node.ProposeConfChange(withContext); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("context proposal error = %v, want ErrUnsupported", err)
	}
	update := &pb.ConfChange{
		Type:   pb.ConfChangeUpdateNode.Enum(),
		NodeId: uint64Ptr(1),
	}
	if err := node.ProposeConfChange(update); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("metadata update error = %v, want ErrUnsupported", err)
	}
	reserved := &pb.ConfChange{
		Type: pb.ConfChangeAddNode.Enum(), NodeId: uint64Ptr(raft.LocalApplyThread),
	}
	if err := node.ProposeConfChange(reserved); err == nil {
		t.Fatal("configuration accepted a local storage-thread identity")
	}
}

func TestMembershipTransitionContextRequiresExplicitExactBinding(t *testing.T) {
	exact := make([]byte, MembershipTransitionContextBytes)
	exact[0] = 1
	unbound, _, _ := newTestNode(t, 1, []uint64{1})
	driveCampaign(t, unbound)
	if err := unbound.ProposeConfChange(&pb.ConfChange{
		Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: uint64Ptr(2), Context: exact,
	}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("unbound exact context = %v, want ErrUnsupported", err)
	}

	for _, size := range []int{0, MembershipTransitionContextBytes - 1,
		MembershipTransitionContextBytes + 1} {
		node, _, _ := newTestNode(t, 1, []uint64{1})
		if err := node.BindMembershipTransitionContext(); err != nil {
			t.Fatal(err)
		}
		driveCampaign(t, node)
		change := &pb.ConfChange{Type: pb.ConfChangeAddLearnerNode.Enum(),
			NodeId: uint64Ptr(2), Context: make([]byte, size)}
		if err := node.ProposeConfChange(change); !errors.Is(err, ErrAdmissionBound) {
			t.Fatalf("bound context bytes %d = %v, want ErrAdmissionBound", size, err)
		}
	}

	bound, _, machine := newTestNode(t, 1, []uint64{1})
	if err := bound.BindMembershipTransitionContext(); err != nil {
		t.Fatal(err)
	}
	driveCampaign(t, bound)
	if err := bound.ProposeConfChange(&pb.ConfChange{Type: pb.ConfChangeAddLearnerNode.Enum(),
		NodeId: uint64Ptr(2), Context: append([]byte(nil), exact...)}); err != nil {
		t.Fatalf("bound exact proposal = %v", err)
	}
	driveAllReady(t, bound)
	publication := machine.Published()
	if !slices.Contains(publication.ConfState.GetLearners(), 2) || publication.ReplicaSetVersion == 0 {
		t.Fatalf("bound exact apply publication = %+v", publication)
	}

	v2, _, _ := newTestNode(t, 1, []uint64{1})
	if err := v2.BindMembershipTransitionContext(); err != nil {
		t.Fatal(err)
	}
	driveCampaign(t, v2)
	if err := v2.ProposeConfChange(&pb.ConfChangeV2{Context: append([]byte(nil), exact...),
		Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeAddLearnerNode.Enum(),
			NodeId: uint64Ptr(2)}}}); err != nil {
		t.Fatalf("bound exact v2 proposal = %v", err)
	}
	driveAllReady(t, v2)
}

func TestBoundMembershipTransitionContextRevalidatesCommittedApply(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1})
	if err := node.BindMembershipTransitionContext(); err != nil {
		t.Fatal(err)
	}
	driveCampaign(t, node)
	if err := node.ProposeConfChange(&pb.ConfChange{Type: pb.ConfChangeAddLearnerNode.Enum(),
		NodeId: uint64Ptr(2), Context: make([]byte, MembershipTransitionContextBytes)}); err != nil {
		t.Fatal(err)
	}
	for iteration := 0; iteration < 8; iteration++ {
		captured, err := node.CaptureReady()
		if err != nil || !captured {
			t.Fatalf("capture %d = %t, %v", iteration, captured, err)
		}
		if err = node.PersistReady(); err != nil {
			t.Fatal(err)
		}
		if err = node.DrainMessages(func(*pb.Message) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err = node.InstallSnapshot(); err != nil {
			t.Fatal(err)
		}
		for _, entry := range node.ready.CommittedEntries {
			if entry.GetType() != pb.EntryConfChange {
				continue
			}
			change := new(pb.ConfChange)
			if err = proto.Unmarshal(entry.GetData(), change); err != nil {
				t.Fatal(err)
			}
			change.Context = change.Context[:MembershipTransitionContextBytes-1]
			entry.Data, err = proto.MarshalOptions{Deterministic: true}.Marshal(change)
			if err != nil {
				t.Fatal(err)
			}
			if err = applyCommittedForTest(node); !errors.Is(err, ErrAdmissionBound) {
				t.Fatalf("malformed committed context apply = %v, want ErrAdmissionBound", err)
			}
			return
		}
		if err = applyCommittedForTest(node); err != nil {
			t.Fatal(err)
		}
		if _, err = node.RecordReadStates(); err != nil {
			t.Fatal(err)
		}
		if err = node.AdvanceReady(); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("configuration entry was not committed")
}

func TestConfigurationResultCannotExceedRecoveryMemberBound(t *testing.T) {
	learners := make([]uint64, MaxConfStateMembers-1)
	for i := range learners {
		learners[i] = uint64(i + 2)
	}
	exact := &pb.ConfState{Voters: []uint64{1}, Learners: learners}
	if err := ValidateConfState(exact, 1); err != nil {
		t.Fatalf("exact member bound error = %v", err)
	}
	unsorted := &pb.ConfState{Voters: []uint64{2, 1}}
	if err := ValidateConfState(unsorted, 1); err == nil {
		t.Fatal("unsorted member list passed canonical validation")
	}
	overflow := cloneConfState(exact)
	overflow.Learners = append(overflow.Learners, uint64(MaxConfStateMembers+1))
	if err := ValidateConfState(overflow, 1); !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("member bound overflow error = %v", err)
	}

	node, _, _ := newTestNode(t, 1, []uint64{1})
	node.published.ConfState = &pb.ConfState{
		Voters: []uint64{1}, Learners: slices.Clone(learners[:len(learners)-1]),
	}
	change := &pb.ConfChange{
		Type:   pb.ConfChangeAddLearnerNode.Enum(),
		NodeId: uint64Ptr(MaxConfStateMembers),
	}
	predicted, err := node.preflightConfChange(change, 1)
	if err != nil {
		t.Fatalf("preflight to exact bound error = %v", err)
	}
	if got := len(predicted.GetVoters()) + len(predicted.GetLearners()); got != MaxConfStateMembers {
		t.Fatalf("predicted member references = %d, want %d", got, MaxConfStateMembers)
	}
	node.published.ConfState = predicted
	change.NodeId = uint64Ptr(MaxConfStateMembers + 1)
	if _, err := node.preflightConfChange(change, 1); !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("preflight beyond recovery bound error = %v", err)
	}
	many := &pb.ConfChangeV2{Transition: pb.ConfChangeTransitionJointExplicit.Enum()}
	many.Changes = make([]*pb.ConfChangeSingle, MaxConfStateMembers+1)
	for i := range many.Changes {
		many.Changes[i] = &pb.ConfChangeSingle{
			Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: uint64Ptr(uint64(i + 2)),
		}
	}
	if err := node.validateConfChange(many); !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("configuration change count overflow error = %v", err)
	}
}

func TestInvalidCommittedConfigurationFailsNodeWithoutPanicking(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1})
	driveCampaign(t, node)
	change := &pb.ConfChange{
		Type:   pb.ConfChangeRemoveNode.Enum(),
		NodeId: uint64Ptr(1),
	}
	encoded, err := proto.Marshal(change)
	if err != nil {
		t.Fatal(err)
	}
	publication := node.Published()
	term := node.Status().GetTerm()
	entry := &pb.Entry{
		Type:  pb.EntryConfChange.Enum(),
		Term:  &term,
		Index: uint64Ptr(publication.Applied + 1),
		Data:  encoded,
	}
	if err := node.applyEntry(entry); err == nil {
		t.Fatal("invalid committed configuration was applied")
	}
	if node.Phase() != PhaseFailed || node.Failure() == nil {
		t.Fatalf("invalid configuration phase=%s failure=%v", node.Phase(), node.Failure())
	}
}

func TestReadIndexWaitsForOrderedPublication(t *testing.T) {
	node, _, machine := newTestNode(t, 1, []uint64{1})
	driveCampaign(t, node)
	if err := node.Propose([]byte("visible-at-barrier")); err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	driveAllReady(t, node)
	context := []byte{0x00, 0x7f, 0x80, 0xff}
	wantContext := slices.Clone(context)
	if err := node.ReadIndex(context); err != nil {
		t.Fatalf("ReadIndex() error = %v", err)
	}
	context[0] = 0x42

	if captured, err := node.CaptureReady(); err != nil || !captured {
		t.Fatalf("CaptureReady() = %v, %v", captured, err)
	}
	if err := node.PersistReady(); err != nil {
		t.Fatalf("PersistReady() error = %v", err)
	}
	if err := node.DrainMessages(func(*pb.Message) error { return nil }); err != nil {
		t.Fatalf("DrainMessages() error = %v", err)
	}
	if outcomes, err := node.RecordReadStates(); !errors.Is(err, ErrWrongPhase) || outcomes != nil {
		t.Fatalf("RecordReadStates before apply = %+v, %v", outcomes, err)
	}
	if err := node.InstallSnapshot(); err != nil {
		t.Fatalf("InstallSnapshot() error = %v", err)
	}
	if err := applyCommittedForTest(node); err != nil {
		t.Fatalf("ApplyCommitted() error = %v", err)
	}
	recorded, err := node.RecordNextReadState()
	if err != nil || !recorded {
		t.Fatalf("RecordNextReadState() = %v, %v", recorded, err)
	}
	outcomes, err := node.FinishReadStates()
	if err != nil {
		t.Fatalf("RecordReadStates() error = %v", err)
	}
	if len(outcomes) != 1 {
		t.Fatalf("read outcomes = %+v, want one", outcomes)
	}
	outcome := outcomes[0]
	if outcome.Err != nil {
		t.Fatalf("read outcome error = %v", outcome.Err)
	}
	if !slices.Equal(outcome.Barrier.Context, wantContext) {
		t.Fatalf("read context = %v, want exact %v", outcome.Barrier.Context, wantContext)
	}
	if outcome.Barrier.Incarnation != 1 || outcome.Barrier.Term == 0 {
		t.Fatalf("read barrier identity = %+v", outcome.Barrier)
	}
	if published := machine.Published().Applied; published < outcome.Barrier.Index {
		t.Fatalf("released at published %d before barrier %d", published, outcome.Barrier.Index)
	}
	if err := node.AdvanceReady(); err != nil {
		t.Fatalf("AdvanceReady() error = %v", err)
	}
}

func TestLeaderTransferRequiresConfiguredVoterAndExposesProgress(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1, 2})
	if err := node.TransferLeader(2); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower TransferLeader = %v", err)
	}
	driveCampaignWithPeer(t, node, 2)
	progress, found := node.Progress(2)
	if !found || progress.Learner || progress.Next == 0 {
		t.Fatalf("peer progress = %+v, %t", progress, found)
	}
	if _, found := node.Progress(99); found {
		t.Fatal("unknown member exposed progress")
	}
	if err := node.TransferLeader(1); !errors.Is(err, ErrInvalidTransferee) {
		t.Fatalf("self TransferLeader = %v", err)
	}
	if err := node.TransferLeader(99); !errors.Is(err, ErrInvalidTransferee) {
		t.Fatalf("unknown TransferLeader = %v", err)
	}
	if err := node.TransferLeader(2); err != nil {
		t.Fatal(err)
	}
	if status := node.Status(); status.LeadTransferee != 2 {
		t.Fatalf("lead transferee = %d", status.LeadTransferee)
	}
	if err := node.TransferLeader(3); !errors.Is(err, ErrLeaderTransferPending) {
		t.Fatalf("conflicting TransferLeader = %v", err)
	}
}

func TestReadIndexRejectsLeadershipChangeBeforeRelease(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1, 2})
	driveCampaignWithPeer(t, node, 2)
	context := []byte("leadership-fence")
	if err := node.ReadIndex(context); err != nil {
		t.Fatalf("ReadIndex() error = %v", err)
	}
	status := node.Status()
	nextTerm := status.GetTerm() + 1
	if err := node.Step(&pb.Message{
		Type:   pb.MsgHeartbeat.Enum(),
		From:   uint64Ptr(2),
		To:     uint64Ptr(1),
		Term:   &nextTerm,
		Commit: uint64Ptr(status.GetCommit()),
	}); err != nil {
		t.Fatalf("Step(higher-term heartbeat) error = %v", err)
	}
	outcomes := driveAllReady(t, node)
	if len(outcomes) != 1 || !errors.Is(outcomes[0].Err, ErrReadLeadershipLost) {
		t.Fatalf("read outcomes = %+v, want leadership-lost outcome", outcomes)
	}
}

func TestStepAdmitsOnlyCurrentLeaderTimeoutNow(t *testing.T) {
	newFollower := func(t *testing.T) (*Node, uint64) {
		t.Helper()
		node, _, _ := newTestNode(t, 1, []uint64{1, 2})
		term := node.Status().GetTerm() + 1
		if err := node.Step(&pb.Message{
			Type: pb.MsgHeartbeat.Enum(), From: uint64Ptr(2), To: uint64Ptr(1),
			Term: &term, Commit: uint64Ptr(node.Status().GetCommit()),
		}); err != nil {
			t.Fatal(err)
		}
		driveAllReady(t, node)
		if status := node.Status(); status.Lead != 2 || status.GetTerm() != term {
			t.Fatalf("follower status = %+v", status)
		}
		return node, term
	}
	timeoutNow := func(term uint64) *pb.Message {
		return &pb.Message{
			Type: pb.MsgTimeoutNow.Enum(), From: uint64Ptr(2), To: uint64Ptr(1), Term: &term,
		}
	}

	node, term := newFollower(t)
	if err := node.Step(timeoutNow(term)); err != nil {
		t.Fatalf("current leader TimeoutNow = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*pb.Message)
	}{
		{name: "stale term", mutate: func(message *pb.Message) { *message.Term-- }},
		{name: "wrong source", mutate: func(message *pb.Message) { message.From = uint64Ptr(3) }},
		{name: "wrong destination", mutate: func(message *pb.Message) { message.To = uint64Ptr(3) }},
		{name: "entry", mutate: func(message *pb.Message) { message.Entries = []*pb.Entry{{}} }},
		{name: "snapshot", mutate: func(message *pb.Message) { message.Snapshot = &pb.Snapshot{} }},
		{name: "context", mutate: func(message *pb.Message) { message.Context = []byte("x") }},
		{name: "explicit zero index", mutate: func(message *pb.Message) { message.Index = uint64Ptr(0) }},
		{name: "reject", mutate: func(message *pb.Message) { value := false; message.Reject = &value }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node, term := newFollower(t)
			message := timeoutNow(term)
			test.mutate(message)
			if err := node.Step(message); err == nil {
				t.Fatal("unsafe TimeoutNow was accepted")
			}
		})
	}

	t.Run("learner target", func(t *testing.T) {
		node, _, _ := newTestNodeWithConfState(t, 1, 1, &pb.ConfState{
			Voters: []uint64{2}, Learners: []uint64{1},
		})
		term := node.Status().GetTerm() + 1
		if err := node.Step(&pb.Message{
			Type: pb.MsgHeartbeat.Enum(), From: uint64Ptr(2), To: uint64Ptr(1), Term: &term,
			Commit: uint64Ptr(node.Status().GetCommit()),
		}); err != nil {
			t.Fatal(err)
		}
		driveAllReady(t, node)
		if err := node.Step(timeoutNow(term)); err == nil {
			t.Fatal("learner target accepted TimeoutNow")
		}
	})
}

func TestApplyFailureIsFailStop(t *testing.T) {
	node, _, machine := newTestNode(t, 1, []uint64{1})
	driveCampaign(t, node)
	if err := node.Propose([]byte("fail-apply")); err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	captureCommittedReady(t, node, []byte("fail-apply"))
	if err := node.PersistReady(); err != nil {
		t.Fatalf("PersistReady() error = %v", err)
	}
	if err := node.DrainMessages(func(*pb.Message) error { return nil }); err != nil {
		t.Fatalf("DrainMessages() error = %v", err)
	}
	if err := node.InstallSnapshot(); err != nil {
		t.Fatalf("InstallSnapshot() error = %v", err)
	}
	wantErr := errors.New("injected apply failure")
	machine.fail = wantErr
	if err := applyCommittedForTest(node); !errors.Is(err, wantErr) {
		t.Fatalf("ApplyCommitted() error = %v, want injected error", err)
	}
	if node.Phase() != PhaseFailed || !errors.Is(node.Failure(), wantErr) {
		t.Fatalf("failed node phase=%s failure=%v", node.Phase(), node.Failure())
	}
	if err := node.AdvanceReady(); !errors.Is(err, ErrWrongPhase) {
		t.Fatalf("AdvanceReady after apply failure error = %v", err)
	}
}

func TestApplyNextExposesEntryLevelCrashCuts(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1, 2})
	term, commit := uint64(2), uint64(3)
	if err := node.Step(&pb.Message{
		Type: pb.MsgApp.Enum(), From: uint64Ptr(2), To: uint64Ptr(1), Term: &term,
		Index: uint64Ptr(1), LogTerm: uint64Ptr(1), Commit: &commit,
		Entries: []*pb.Entry{
			{Term: &term, Index: uint64Ptr(2), Data: []byte("first")},
			{Term: &term, Index: uint64Ptr(3), Data: []byte("second")},
		},
	}); err != nil {
		t.Fatalf("Step(two committed entries) error = %v", err)
	}
	captureCommittedReady(t, node, []byte("first"))
	if err := node.PersistReady(); err != nil {
		t.Fatalf("PersistReady() error = %v", err)
	}
	if err := node.DrainMessages(func(*pb.Message) error { return nil }); err != nil {
		t.Fatalf("DrainMessages() error = %v", err)
	}
	if err := node.InstallSnapshot(); err != nil {
		t.Fatalf("InstallSnapshot() error = %v", err)
	}
	firstResult, err := node.ApplyNextBatch(nil)
	if err != nil || firstResult.Applied != 1 || firstResult.Normal.Len() != 1 {
		t.Fatalf("ApplyNextBatch(first) = %+v, %v", firstResult, err)
	}
	first := firstResult.Normal.FinalPublication()
	if err := node.SettleAppliedNormalBatch(firstResult.Normal); err != nil {
		t.Fatalf("settle first = %v", err)
	}
	if node.Phase() != PhaseSnapshotInstalled {
		t.Fatalf("phase after first of two entries = %s", node.Phase())
	}
	secondResult, err := node.ApplyNextBatch(nil)
	if err != nil || secondResult.Applied != 1 || secondResult.Normal.Len() != 1 {
		t.Fatalf("ApplyNextBatch(second) = %+v, %v", secondResult, err)
	}
	second := secondResult.Normal.FinalPublication()
	if err := node.SettleAppliedNormalBatch(secondResult.Normal); err != nil {
		t.Fatalf("settle second = %v", err)
	}
	if second.Applied != first.Applied+1 || node.Phase() != PhaseEntriesApplied {
		t.Fatalf("micro-step publications %d -> %d, phase %s", first.Applied, second.Applied, node.Phase())
	}
	if _, err := node.FinishReadStates(); err != nil {
		t.Fatalf("FinishReadStates() error = %v", err)
	}
	if err := node.AdvanceReady(); err != nil {
		t.Fatalf("AdvanceReady() error = %v", err)
	}
}

func TestSnapshotPersistsBeforeOrderedInstall(t *testing.T) {
	node, stable, machine := newTestNode(t, 1, []uint64{1, 2})
	index, term := uint64(5), uint64(2)
	snapshot := &pb.Snapshot{
		Data: []byte("snapshot-state"),
		Metadata: &pb.SnapshotMetadata{
			ConfState: &pb.ConfState{Voters: []uint64{1, 2}},
			Index:     &index,
			Term:      &term,
		},
	}
	if err := node.Step(&pb.Message{
		Type:     pb.MsgSnap.Enum(),
		From:     uint64Ptr(2),
		To:       uint64Ptr(1),
		Term:     &term,
		Snapshot: snapshot,
	}); err != nil {
		t.Fatalf("Step(snapshot) error = %v", err)
	}
	if captured, err := node.CaptureReady(); err != nil || !captured {
		t.Fatalf("CaptureReady() = %v, %v", captured, err)
	}
	if node.ready.Snapshot.GetMetadata().GetIndex() != index {
		t.Fatalf("Ready snapshot index = %d, want %d", node.ready.Snapshot.GetMetadata().GetIndex(), index)
	}
	if err := node.PersistReady(); err != nil {
		t.Fatalf("PersistReady() error = %v", err)
	}
	if got, err := stable.MemoryStorage.Snapshot(); err != nil || got.GetMetadata().GetIndex() != index {
		t.Fatalf("stable snapshot index = %d, error = %v", got.GetMetadata().GetIndex(), err)
	}
	if machine.Applied() != 1 {
		t.Fatalf("snapshot became reader-visible before install: applied=%d", machine.Applied())
	}
	if err := node.DrainMessages(func(*pb.Message) error { return nil }); err != nil {
		t.Fatalf("DrainMessages() error = %v", err)
	}
	if err := node.InstallSnapshot(); err != nil {
		t.Fatalf("InstallSnapshot() error = %v", err)
	}
	if machine.Applied() != index || node.Published().Applied != index {
		t.Fatalf("installed snapshot machine=%d publication=%d", machine.Applied(), node.Published().Applied)
	}
	if err := applyCommittedForTest(node); err != nil {
		t.Fatalf("ApplyCommitted() error = %v", err)
	}
	if _, err := node.FinishReadStates(); err != nil {
		t.Fatalf("FinishReadStates() error = %v", err)
	}
	if err := node.AdvanceReady(); err != nil {
		t.Fatalf("AdvanceReady() error = %v", err)
	}
}

func TestInboundSnapshotConfStateIsValidatedBeforeCoreStep(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1, 2})
	index, term := uint64(5), uint64(2)
	unknown := []byte{0x98, 0x06, 0x01}
	message := func(state *pb.ConfState, data []byte) *pb.Message {
		return &pb.Message{
			Type: pb.MsgSnap.Enum(), From: uint64Ptr(2), To: uint64Ptr(1), Term: &term,
			Snapshot: &pb.Snapshot{Data: data, Metadata: &pb.SnapshotMetadata{
				ConfState: state, Index: &index, Term: &term,
			}},
		}
	}
	autoLeave := true
	tests := map[string]*pb.Message{
		"nil snapshot":      {Type: pb.MsgSnap.Enum(), From: uint64Ptr(2), To: uint64Ptr(1), Term: &term},
		"no incoming voter": message(&pb.ConfState{}, nil),
		"duplicate voter":   message(&pb.ConfState{Voters: []uint64{1, 1}}, nil),
		"voter learner overlap": message(&pb.ConfState{
			Voters: []uint64{1, 2}, Learners: []uint64{2},
		}, nil),
		"learners-next outside joint": message(&pb.ConfState{
			Voters: []uint64{1}, LearnersNext: []uint64{2},
		}, nil),
		"automatic leave": message(&pb.ConfState{
			Voters: []uint64{1, 2}, AutoLeave: &autoLeave,
		}, nil),
		"oversized data": message(&pb.ConfState{Voters: []uint64{1, 2}}, make([]byte, MaxSnapshotBytes+1)),
	}
	unknownMessage := message(&pb.ConfState{Voters: []uint64{1, 2}}, nil)
	unknownMessage.ProtoReflect().SetUnknown(unknown)
	tests["unknown message fields"] = unknownMessage
	unknownSnapshot := message(&pb.ConfState{Voters: []uint64{1, 2}}, nil)
	unknownSnapshot.GetSnapshot().ProtoReflect().SetUnknown(unknown)
	tests["unknown snapshot fields"] = unknownSnapshot
	unknownMetadata := message(&pb.ConfState{Voters: []uint64{1, 2}}, nil)
	unknownMetadata.GetSnapshot().GetMetadata().ProtoReflect().SetUnknown(unknown)
	tests["unknown snapshot metadata fields"] = unknownMetadata
	unknownConfState := message(&pb.ConfState{Voters: []uint64{1, 2}}, nil)
	unknownConfState.GetSnapshot().GetMetadata().GetConfState().ProtoReflect().SetUnknown(unknown)
	tests["unknown ConfState fields"] = unknownConfState
	for name, malformed := range tests {
		t.Run(name, func(t *testing.T) {
			if err := node.Step(malformed); err == nil {
				t.Fatal("Step accepted malformed snapshot")
			}
			if node.Phase() != PhaseIdle {
				t.Fatalf("malformed snapshot changed phase to %s", node.Phase())
			}
		})
	}
	if err := node.Step(message(&pb.ConfState{Voters: []uint64{1, 2}}, []byte("valid"))); err != nil {
		t.Fatalf("Step(valid snapshot) error = %v", err)
	}
}

func TestInboundEntryStoreIncompatibleFieldsAreRejectedBeforeCoreStep(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1, 2})
	term, index := uint64(2), uint64(2)
	base := func() *pb.Message {
		return &pb.Message{
			Type: pb.MsgApp.Enum(), From: uint64Ptr(2), To: uint64Ptr(1), Term: &term,
			Index: uint64Ptr(1), LogTerm: uint64Ptr(1), Commit: uint64Ptr(1),
			Entries: []*pb.Entry{{Term: &term, Index: &index}},
		}
	}
	tests := map[string]func(*pb.Message){
		"unknown Entry fields": func(message *pb.Message) {
			message.GetEntries()[0].ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x01})
		},
		"maximum message term": func(message *pb.Message) { message.Term = uint64Ptr(math.MaxUint64) },
		"zero message term":    func(message *pb.Message) { message.Term = uint64Ptr(0) },
		"maximum Entry term":   func(message *pb.Message) { message.GetEntries()[0].Term = uint64Ptr(math.MaxUint64) },
		"maximum Entry index":  func(message *pb.Message) { message.GetEntries()[0].Index = uint64Ptr(math.MaxUint64) },
		"local storage responses": func(message *pb.Message) {
			message.Responses = []*pb.Message{{Type: pb.MsgApp.Enum()}}
		},
		"local storage vote": func(message *pb.Message) { message.Vote = uint64Ptr(0) },
		"unexpected context": func(message *pb.Message) { message.Context = []byte("unexpected") },
		"oversized heartbeat context": func(message *pb.Message) {
			message.Type = pb.MsgHeartbeat.Enum()
			message.Entries = nil
			message.Context = make([]byte, MaxReadContextBytes+1)
		},
		"Entry term above message term": func(message *pb.Message) {
			message.GetEntries()[0].Term = uint64Ptr(term + 1)
		},
		"decreasing Entry terms": func(message *pb.Message) {
			message.Entries = append(message.GetEntries(), &pb.Entry{Term: uint64Ptr(term - 1), Index: uint64Ptr(index + 1)})
		},
		"unknown Entry type": func(message *pb.Message) {
			unknownType := pb.EntryType(255)
			message.GetEntries()[0].Type = &unknownType
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			message := base()
			mutate(message)
			if err := node.Step(message); err == nil {
				t.Fatal("Step accepted Store-incompatible message")
			}
			if ready, err := node.HasReady(); err != nil || ready {
				t.Fatalf("HasReady() after rejected message = %v, %v", ready, err)
			}
		})
	}
}

func TestReadAndProposalAdmissionBounds(t *testing.T) {
	node, _, _ := newTestNode(t, 1, []uint64{1, 2})
	driveCampaignWithPeer(t, node, 2)
	if err := admitProposalBytes(MaxProposalBytes); err != nil {
		t.Fatalf("exact-size proposal admission error = %v", err)
	}
	if err := admitProposalBytes(MaxProposalBytes + 1); !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("oversized proposal error = %v, want ErrAdmissionBound", err)
	}
	if err := node.ReadIndex(make([]byte, MaxReadContextBytes+1)); !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("oversized read context error = %v, want ErrAdmissionBound", err)
	}
	for i := range MaxPendingReads {
		context := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		if err := node.ReadIndex(context); err != nil {
			t.Fatalf("ReadIndex(%d) error = %v", i, err)
		}
	}
	if err := node.ReadIndex([]byte("one-too-many")); !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("read count overflow error = %v, want ErrAdmissionBound", err)
	}
	if node.PendingReads() != MaxPendingReads || node.PendingReadBytes() > MaxPendingReadBytes {
		t.Fatalf("pending reads=%d bytes=%d", node.PendingReads(), node.PendingReadBytes())
	}
}

type fakeStable struct {
	*raft.MemoryStorage
	conf       *pb.ConfState
	fail       error
	attemptIDs []uint64
	batches    []PersistBatch
	durableIDs map[readyKey]struct{}
}

func (s *fakeStable) InitialState() (*pb.HardState, *pb.ConfState, error) {
	hardState, _, err := s.MemoryStorage.InitialState()
	return hardState, cloneConfState(s.conf), err
}

func (s *fakeStable) Persist(batch PersistBatch) error {
	s.attemptIDs = append(s.attemptIDs, batch.ReadyID)
	if s.fail != nil {
		return s.fail
	}
	key := readyKey{incarnation: batch.NodeIncarnation, readyID: batch.ReadyID}
	if _, duplicate := s.durableIDs[key]; duplicate {
		return nil
	}
	if !raft.IsEmptySnap(batch.Snapshot) {
		if err := s.MemoryStorage.ApplySnapshot(batch.Snapshot); err != nil {
			return err
		}
		s.conf = cloneConfState(batch.Snapshot.GetMetadata().GetConfState())
	}
	if len(batch.Entries) != 0 {
		if err := s.MemoryStorage.Append(batch.Entries); err != nil {
			return err
		}
	}
	if !raft.IsEmptyHardState(batch.HardState) {
		if err := s.MemoryStorage.SetHardState(batch.HardState); err != nil {
			return err
		}
	}
	s.durableIDs[key] = struct{}{}
	s.batches = append(s.batches, clonePersistBatch(batch))
	return nil
}

type applyCall struct {
	meta ApplyMeta
	data []byte
	conf *pb.ConfState
}

type readyKey struct {
	incarnation uint64
	readyID     uint64
}

type lastIndexStable struct {
	*fakeStable
	last uint64
}

func (s *lastIndexStable) LastIndex() (uint64, error) { return s.last, nil }

type fakeStateMachine struct {
	pub                            Publication
	calls                          []applyCall
	fail                           error
	snapshotFailAfterPublish       error
	snapshotBadVersionAfterPublish *uint64
	snapshotCalls                  int
	snapshotIndex                  uint64
	snapshotTerm                   uint64
	snapshotDigest                 [32]byte
	snapshotReplicaSetVersion      uint64
	snapshotConfState              *pb.ConfState
}

func (m *fakeStateMachine) Applied() uint64 { return m.pub.Applied }

func (m *fakeStateMachine) Published() Publication { return clonePublication(m.pub) }

func (m *fakeStateMachine) ApplyNormal(meta ApplyMeta, data []byte) (Publication, error) {
	if m.fail != nil {
		return Publication{}, m.fail
	}
	m.calls = append(m.calls, applyCall{meta: meta, data: slices.Clone(data)})
	m.pub.Applied = meta.Index
	if len(data) != 0 {
		payload := make([]byte, 0, len(m.pub.DataChainDigest)+len(data))
		payload = append(payload, m.pub.DataChainDigest[:]...)
		payload = append(payload, data...)
		m.pub.DataChainDigest = sha256.Sum256(payload)
	}
	return m.Published(), nil
}

func (m *fakeStateMachine) ApplyConfiguration(meta ApplyMeta, state *pb.ConfState) (Publication, error) {
	if m.fail != nil {
		return Publication{}, m.fail
	}
	m.calls = append(m.calls, applyCall{meta: meta, conf: cloneConfState(state)})
	m.pub.Applied = meta.Index
	m.pub.ConfState = cloneConfState(state)
	m.pub.ReplicaSetVersion = meta.Index
	return m.Published(), nil
}

func (m *fakeStateMachine) InstallSnapshot(snapshot *pb.Snapshot) (Publication, error) {
	if m.fail != nil {
		return Publication{}, m.fail
	}
	m.snapshotCalls++
	metadata := snapshot.GetMetadata()
	index := metadata.GetIndex()
	digest := testSnapshotIdentity(snapshot)
	if m.pub.Applied == index {
		if m.snapshotIndex != index || m.snapshotTerm != metadata.GetTerm() ||
			m.snapshotDigest != digest ||
			m.snapshotReplicaSetVersion != m.pub.ReplicaSetVersion ||
			m.snapshotConfState == nil ||
			m.snapshotConfState.Equivalent(metadata.GetConfState()) != nil {
			return Publication{}, errors.New("snapshot differs from the exact published snapshot")
		}
		return m.Published(), nil
	}
	if m.pub.Applied > index {
		return Publication{}, errors.New("snapshot regresses applied index")
	}
	expectedVersion := m.pub.ReplicaSetVersion
	if m.pub.ConfState == nil || m.pub.ConfState.Equivalent(metadata.GetConfState()) != nil ||
		expectedVersion == 0 && confStateHasMembers(metadata.GetConfState()) {
		expectedVersion = index
	}
	m.snapshotIndex = index
	m.snapshotTerm = metadata.GetTerm()
	m.snapshotDigest = digest
	m.snapshotReplicaSetVersion = expectedVersion
	m.snapshotConfState = cloneConfState(metadata.GetConfState())
	m.pub.Applied = index
	m.pub.DataChainDigest = sha256.Sum256(snapshot.GetData())
	m.pub.ConfState = cloneConfState(metadata.GetConfState())
	m.pub.ReplicaSetVersion = expectedVersion
	if m.snapshotBadVersionAfterPublish != nil {
		m.pub.ReplicaSetVersion = *m.snapshotBadVersionAfterPublish
	}
	if m.snapshotFailAfterPublish != nil {
		return m.Published(), m.snapshotFailAfterPublish
	}
	return m.Published(), nil
}

func newTestNode(t testing.TB, incarnation uint64, voters []uint64) (*Node, *fakeStable, *fakeStateMachine) {
	t.Helper()
	return newTestNodeWithConfState(t, 1, incarnation, &pb.ConfState{Voters: slices.Clone(voters)})
}

func newTestNodeWithConfState(
	t testing.TB,
	memberID, incarnation uint64,
	confState *pb.ConfState,
) (*Node, *fakeStable, *fakeStateMachine) {
	t.Helper()
	index, term := uint64(1), uint64(1)
	snapshot := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{
		ConfState: cloneConfState(confState),
		Index:     &index,
		Term:      &term,
	}}
	memory := raft.NewMemoryStorage()
	if err := memory.ApplySnapshot(snapshot); err != nil {
		t.Fatalf("ApplySnapshot(initial) error = %v", err)
	}
	if err := memory.SetHardState(&pb.HardState{Term: &term, Commit: &index}); err != nil {
		t.Fatalf("SetHardState(initial) error = %v", err)
	}
	stable := &fakeStable{
		MemoryStorage: memory,
		conf:          cloneConfState(confState),
		durableIDs:    make(map[readyKey]struct{}),
	}
	machine := &fakeStateMachine{pub: Publication{
		Applied:           index,
		DataChainDigest:   sha256.Sum256(snapshot.GetData()),
		ConfState:         cloneConfState(confState),
		ReplicaSetVersion: index,
	}, snapshotIndex: index, snapshotTerm: term,
		snapshotDigest:            testSnapshotIdentity(snapshot),
		snapshotReplicaSetVersion: index,
		snapshotConfState:         cloneConfState(confState),
	}
	node, err := NewNode(memberID, incarnation, stable, machine)
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}
	machine.snapshotCalls = 0
	return node, stable, machine
}

func testSnapshotIdentity(snapshot *pb.Snapshot) [32]byte {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
	if err != nil {
		panic(err)
	}
	return sha256.Sum256(encoded)
}

func cloneTestMemoryStorage(t *testing.T, source *raft.MemoryStorage) *raft.MemoryStorage {
	t.Helper()
	hard, _, err := source.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	first, _ := source.FirstIndex()
	last, _ := source.LastIndex()
	next := raft.NewMemoryStorage()
	if !raft.IsEmptySnap(snapshot) {
		if err := next.ApplySnapshot(snapshot); err != nil {
			t.Fatal(err)
		}
	}
	if first <= last {
		entries, entriesErr := source.Entries(first, last+1, math.MaxUint64)
		if entriesErr == nil {
			if err := next.Append(cloneTestEntries(entries)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !raft.IsEmptyHardState(hard) {
		if err := next.SetHardState(proto.Clone(hard).(*pb.HardState)); err != nil {
			t.Fatal(err)
		}
	}
	return next
}

func cloneTestEntries(entries []*pb.Entry) []*pb.Entry {
	cloned := make([]*pb.Entry, len(entries))
	for i, entry := range entries {
		cloned[i] = proto.Clone(entry).(*pb.Entry)
	}
	return cloned
}

func driveCampaign(t *testing.T, node *Node) {
	t.Helper()
	if err := node.Campaign(); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	driveAllReady(t, node)
	status := node.Status()
	if status.RaftState != raft.StateLeader || status.Lead != status.ID {
		t.Fatalf("campaign status = %+v, want local leader", status)
	}
}

func driveCampaignWithPeer(t *testing.T, node *Node, peer uint64) {
	t.Helper()
	if err := node.Campaign(); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	preVote := collectVoteRequest(t, node, peer, pb.MsgPreVote)
	if err := node.Step(voteResponse(preVote, pb.MsgPreVoteResp)); err != nil {
		t.Fatalf("Step(MsgPreVoteResp) error = %v", err)
	}
	vote := collectVoteRequest(t, node, peer, pb.MsgVote)
	if err := node.Step(voteResponse(vote, pb.MsgVoteResp)); err != nil {
		t.Fatalf("Step(MsgVoteResp) error = %v", err)
	}
	driveAllReady(t, node)
	status := node.Status()
	if status.RaftState != raft.StateLeader || status.Lead != status.ID {
		t.Fatalf("campaign status = %+v, want local leader", status)
	}
}

func collectVoteRequest(t *testing.T, node *Node, peer uint64, messageType pb.MessageType) *pb.Message {
	t.Helper()
	var request *pb.Message
	driveOneReady(t, node, func(message *pb.Message) error {
		if message.GetTo() == peer && message.GetType() == messageType {
			request = proto.Clone(message).(*pb.Message)
		}
		return nil
	})
	if request == nil {
		t.Fatalf("Ready contained no %s request to peer %d", messageType, peer)
	}
	return request
}

func voteResponse(request *pb.Message, messageType pb.MessageType) *pb.Message {
	term := request.GetTerm()
	return &pb.Message{
		Type: messageType.Enum(), From: uint64Ptr(request.GetTo()), To: uint64Ptr(request.GetFrom()), Term: &term,
	}
}

func driveOneReady(t *testing.T, node *Node, send func(*pb.Message) error) []ReadOutcome {
	t.Helper()
	if send == nil {
		send = func(*pb.Message) error { return nil }
	}
	captured, err := node.CaptureReady()
	if err != nil || !captured {
		t.Fatalf("CaptureReady() = %v, %v", captured, err)
	}
	if err := node.PersistReady(); err != nil {
		t.Fatalf("PersistReady() error = %v", err)
	}
	if err := node.DrainMessages(send); err != nil {
		t.Fatalf("DrainMessages() error = %v", err)
	}
	if err := node.InstallSnapshot(); err != nil {
		t.Fatalf("InstallSnapshot() error = %v", err)
	}
	if err := applyCommittedForTest(node); err != nil {
		t.Fatalf("ApplyCommitted() error = %v", err)
	}
	outcomes, err := node.RecordReadStates()
	if err != nil {
		t.Fatalf("RecordReadStates() error = %v", err)
	}
	if err := node.AdvanceReady(); err != nil {
		t.Fatalf("AdvanceReady() error = %v", err)
	}
	return outcomes
}

func driveAllReady(t *testing.T, node *Node) []ReadOutcome {
	t.Helper()
	var outcomes []ReadOutcome
	for iteration := 0; iteration < 32; iteration++ {
		hasReady, err := node.HasReady()
		if err != nil {
			t.Fatalf("HasReady() error = %v", err)
		}
		if !hasReady {
			return outcomes
		}
		captured, err := node.CaptureReady()
		if err != nil || !captured {
			t.Fatalf("CaptureReady() = %v, %v", captured, err)
		}
		if err := node.PersistReady(); err != nil {
			t.Fatalf("PersistReady() error = %v", err)
		}
		if err := node.DrainMessages(func(*pb.Message) error { return nil }); err != nil {
			t.Fatalf("DrainMessages() error = %v", err)
		}
		if err := node.InstallSnapshot(); err != nil {
			t.Fatalf("InstallSnapshot() error = %v", err)
		}
		if err := applyCommittedForTest(node); err != nil {
			t.Fatalf("ApplyCommitted() error = %v", err)
		}
		readyOutcomes, err := node.RecordReadStates()
		if err != nil {
			t.Fatalf("RecordReadStates() error = %v", err)
		}
		outcomes = append(outcomes, readyOutcomes...)
		if err := node.AdvanceReady(); err != nil {
			t.Fatalf("AdvanceReady() error = %v", err)
		}
	}
	t.Fatal("Ready processing did not quiesce")
	return nil
}

func captureCommittedReady(t *testing.T, node *Node, data []byte) {
	t.Helper()
	for iteration := 0; iteration < 32; iteration++ {
		hasReady, err := node.HasReady()
		if err != nil {
			t.Fatalf("HasReady() error = %v", err)
		}
		if !hasReady {
			t.Fatalf("no Ready while waiting for committed data %q", data)
		}
		captured, err := node.CaptureReady()
		if err != nil || !captured {
			t.Fatalf("CaptureReady() = %v, %v", captured, err)
		}
		for _, entry := range node.ready.CommittedEntries {
			if slices.Equal(entry.GetData(), data) {
				return
			}
		}
		if err := node.PersistReady(); err != nil {
			t.Fatalf("PersistReady() error = %v", err)
		}
		if err := node.DrainMessages(func(*pb.Message) error { return nil }); err != nil {
			t.Fatalf("DrainMessages() error = %v", err)
		}
		if err := node.InstallSnapshot(); err != nil {
			t.Fatalf("InstallSnapshot() error = %v", err)
		}
		if err := applyCommittedForTest(node); err != nil {
			t.Fatalf("ApplyCommitted() error = %v", err)
		}
		if _, err := node.RecordReadStates(); err != nil {
			t.Fatalf("RecordReadStates() error = %v", err)
		}
		if err := node.AdvanceReady(); err != nil {
			t.Fatalf("AdvanceReady() error = %v", err)
		}
	}
	t.Fatalf("committed data %q did not appear", data)
}

func applyCommittedForTest(node *Node) error {
	var workspace NormalApplyBatchWorkspace
	return node.ApplyCommitted(&workspace, func(AppliedNormalBatch) error { return nil })
}

func clonePersistBatch(batch PersistBatch) PersistBatch {
	clone := PersistBatch{
		NodeIncarnation: batch.NodeIncarnation,
		ReadyID:         batch.ReadyID,
		MustSync:        batch.MustSync,
	}
	if batch.HardState != nil {
		clone.HardState = proto.Clone(batch.HardState).(*pb.HardState)
	}
	if batch.Snapshot != nil {
		clone.Snapshot = proto.Clone(batch.Snapshot).(*pb.Snapshot)
	}
	clone.Entries = make([]*pb.Entry, len(batch.Entries))
	for i, entry := range batch.Entries {
		clone.Entries[i] = proto.Clone(entry).(*pb.Entry)
	}
	return clone
}

func uint64Ptr(value uint64) *uint64 { return &value }
