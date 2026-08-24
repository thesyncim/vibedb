package multiraft

import (
	"encoding/binary"
	"errors"
	"slices"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type fakeReady struct {
	kind         raftmember.DriveKind
	outbound     *pb.Message
	readOutcomes []raftmodel.ReadOutcome
	err          error
}

type fakeRuntime struct {
	identity            raftmember.RuntimeIdentity
	ready               []fakeReady
	inputs              []ProgressKind
	proposals           [][]byte
	confChanges         []*pb.ConfChangeV2
	readContexts        [][]byte
	messages            []*pb.Message
	publication         raftmodel.Publication
	snapshotState       replicatedstate.State
	status              raftmember.RuntimeStatus
	progress            map[uint64]raftmodel.MemberProgress
	transfers           []uint64
	inputErr            error
	proposalErrs        []error
	failure             error
	failureOnReadyError bool
	closeErrs           []error
	closeCalls          int
	driveCalls          int
	statusCalls         int
	readyWorkspace      *raftmember.ReadyWorkspace
	settlementSink      bool
	pendingSettlement   bool
	discardProposals    bool
}

func newFakeRuntime(seed byte) *fakeRuntime {
	key := raftmember.GroupKey{TopologyRecoveryEpoch: uint64(seed) + 1}
	for index := range key.ClusterID {
		key.ClusterID[index] = seed + byte(index) + 1
		key.ClusterIncarnation[index] = seed + byte(index) + 17
		key.ShardIncarnation[index] = seed + byte(index) + 33
		key.GroupID[index] = seed + byte(index) + 49
	}
	runtime := &fakeRuntime{identity: raftmember.RuntimeIdentity{
		Group: key, MemberID: uint64(seed) + 1, NodeIncarnation: 1,
	}}
	runtime.status = raftmember.RuntimeStatus{
		MemberID: runtime.identity.MemberID, LeaderID: runtime.identity.MemberID,
		Term: 1, RaftState: raft.StateLeader,
	}
	return runtime
}

func ignoreProposalGroupTermination(ProposalGroupTermination) {}
func retainProposalGroupPending(raftmember.GroupKey) bool     { return true }

func (runtime *fakeRuntime) Identity() raftmember.RuntimeIdentity { return runtime.identity }
func (runtime *fakeRuntime) Failure() error                       { return runtime.failure }
func (runtime *fakeRuntime) HasPendingResultSettlement() bool     { return runtime.pendingSettlement }

func (runtime *fakeRuntime) Propose(data []byte) error {
	if !runtime.discardProposals {
		runtime.inputs = append(runtime.inputs, ProgressProposal)
		runtime.proposals = append(runtime.proposals, append([]byte(nil), data...))
	}
	if len(runtime.proposalErrs) != 0 {
		err := runtime.proposalErrs[0]
		runtime.proposalErrs[0] = nil
		runtime.proposalErrs = runtime.proposalErrs[1:]
		return err
	}
	return runtime.inputErr
}

func (runtime *fakeRuntime) ProposeConfChange(change pb.ConfChangeI) error {
	if change == nil {
		runtime.confChanges = append(runtime.confChanges, nil)
	} else {
		runtime.confChanges = append(
			runtime.confChanges, proto.Clone(change.AsV2()).(*pb.ConfChangeV2),
		)
	}
	return runtime.inputErr
}

func (runtime *fakeRuntime) ReadIndex(context []byte) error {
	runtime.readContexts = append(runtime.readContexts, append([]byte(nil), context...))
	return runtime.inputErr
}

func (runtime *fakeRuntime) Publication() (raftmodel.Publication, error) {
	return runtime.publication, runtime.inputErr
}

func (runtime *fakeRuntime) SnapshotState() (replicatedstate.State, error) {
	return runtime.snapshotState, runtime.inputErr
}

func (runtime *fakeRuntime) Status() (raftmember.RuntimeStatus, error) {
	runtime.statusCalls++
	return runtime.status, runtime.inputErr
}

func (runtime *fakeRuntime) Progress(memberID uint64) (raftmodel.MemberProgress, bool, error) {
	progress, found := runtime.progress[memberID]
	return progress, found, runtime.inputErr
}

func (runtime *fakeRuntime) TransferLeader(memberID uint64) error {
	runtime.transfers = append(runtime.transfers, memberID)
	return runtime.inputErr
}

func (runtime *fakeRuntime) StepMessage(message *pb.Message) error {
	runtime.inputs = append(runtime.inputs, ProgressMessage)
	runtime.messages = append(runtime.messages, proto.Clone(message).(*pb.Message))
	return runtime.inputErr
}

func (runtime *fakeRuntime) Tick() error {
	runtime.inputs = append(runtime.inputs, ProgressTick)
	return runtime.inputErr
}

func (runtime *fakeRuntime) Campaign() error {
	runtime.inputs = append(runtime.inputs, ProgressCampaign)
	return runtime.inputErr
}

func (runtime *fakeRuntime) DriveReady(
	workspace *raftmember.ReadyWorkspace,
	send func(raftmember.OutboundMessage) error,
	settle raftmember.ResultSettlementSink,
) (raftmember.DriveResult, error) {
	runtime.driveCalls++
	runtime.readyWorkspace = workspace
	runtime.settlementSink = settle != nil
	if len(runtime.ready) == 0 {
		return raftmember.DriveResult{}, nil
	}
	step := runtime.ready[0]
	if step.err != nil {
		if runtime.failureOnReadyError {
			runtime.failure = step.err
		}
		return raftmember.DriveResult{}, step.err
	}
	if step.outbound != nil {
		err := send(raftmember.OutboundMessage{
			Group: runtime.identity.Group, From: step.outbound.GetFrom(),
			To: step.outbound.GetTo(), Message: step.outbound,
		})
		if err != nil {
			return raftmember.DriveResult{}, err
		}
	}
	runtime.ready[0] = fakeReady{}
	runtime.ready = runtime.ready[1:]
	return raftmember.DriveResult{
		Kind: step.kind, ReadyID: 1, ReadOutcomes: step.readOutcomes,
	}, nil
}

func TestHostUsesOneReadyWorkspaceAndExplicitSettlementSinkAtMaximumDensity(t *testing.T) {
	limits := testHostLimits()
	limits.MaxGroups = AbsoluteMaxGroups
	host, err := NewHost(limits)
	if err != nil {
		t.Fatal(err)
	}
	runtimes := make([]*fakeRuntime, AbsoluteMaxGroups)
	for index := range runtimes {
		runtime := newFakeRuntime(byte(index))
		binary.LittleEndian.PutUint64(runtime.identity.Group.GroupID[:8], uint64(index+1))
		runtime.identity.MemberID = uint64(index + 1)
		runtimes[index] = runtime
		if err := host.addRuntime(runtime); err != nil {
			t.Fatalf("add group %d = %v", index, err)
		}
	}
	if progress, done, err := host.RunOne(); err != nil || done ||
		progress.Kind != ProgressNone || progress.Group != (raftmember.GroupKey{}) ||
		progress.ReadOutcomes != nil {
		t.Fatalf("idle maximum-density run = %+v, %v, %v", progress, done, err)
	}
	for index, runtime := range runtimes {
		if runtime.driveCalls != 1 || runtime.readyWorkspace != &host.ready ||
			!runtime.settlementSink {
			t.Fatalf("group %d lane = calls %d workspace %p want %p sink %v",
				index, runtime.driveCalls, runtime.readyWorkspace, &host.ready,
				runtime.settlementSink)
		}
	}
	t.Logf("ReadyWorkspace=%dB Host=%dB groups=%d",
		unsafe.Sizeof(raftmember.ReadyWorkspace{}), unsafe.Sizeof(Host{}), len(runtimes))
}

func (runtime *fakeRuntime) Close() error {
	runtime.closeCalls++
	if len(runtime.closeErrs) == 0 {
		return nil
	}
	err := runtime.closeErrs[0]
	runtime.closeErrs = runtime.closeErrs[1:]
	return err
}

func testHostLimits() Limits {
	return Limits{
		MaxGroups:       16,
		MaxQueueItems:   64,
		MaxQueueBytes:   64 << 20,
		MaxGroupItems:   16,
		MaxGroupBytes:   32 << 20,
		MaxOutboxItems:  16,
		MaxOutboxBytes:  32 << 20,
		MaxPendingTicks: 8,
	}
}

func hostMessage(from, to uint64, context string) *pb.Message {
	term := uint64(1)
	return &pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: hostUint64Ptr(from), To: hostUint64Ptr(to),
		Term: &term, Context: []byte(context),
	}
}

func TestNewHostRejectsUnboundedAndRelationalLimitsBeforeAllocation(t *testing.T) {
	if host, err := NewHost(Limits{}); host != nil || !errors.Is(err, ErrInvalidLimits) {
		t.Fatalf("NewHost(zero) = %v, %v", host, err)
	}
	tooMany := testHostLimits()
	tooMany.MaxGroups = AbsoluteMaxGroups + 1
	if host, err := NewHost(tooMany); host != nil || !errors.Is(err, ErrInvalidLimits) {
		t.Fatalf("NewHost(huge groups) = %v, %v", host, err)
	}
	tooSmall := testHostLimits()
	tooSmall.MaxOutboxBytes = raftmodel.MaxInboundMessageBytes - 1
	if host, err := NewHost(tooSmall); host != nil || !errors.Is(err, ErrInvalidLimits) {
		t.Fatalf("NewHost(small outbox) = %v, %v", host, err)
	}
	relational := testHostLimits()
	relational.MaxGroupItems = relational.MaxQueueItems + 1
	if host, err := NewHost(relational); host != nil || !errors.Is(err, ErrInvalidLimits) {
		t.Fatalf("NewHost(relational) = %v, %v", host, err)
	}
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	if cap(host.order) != 0 || len(host.groups) != 0 {
		t.Fatalf("NewHost eagerly allocated group capacity: cap=%d groups=%d", cap(host.order), len(host.groups))
	}
}

func TestHostAddOwnershipAndQueueBounds(t *testing.T) {
	limits := testHostLimits()
	limits.MaxGroups = 1
	limits.MaxGroupItems = 2
	limits.MaxPendingTicks = 2
	host, err := NewHost(limits)
	if err != nil {
		t.Fatal(err)
	}
	first := newFakeRuntime(1)
	second := newFakeRuntime(2)
	if err := host.addRuntime(first); err != nil {
		t.Fatal(err)
	}
	if err := host.addRuntime(first); !errors.Is(err, ErrGroupExists) {
		t.Fatalf("duplicate Add = %v", err)
	}
	if err := host.addRuntime(second); !errors.Is(err, ErrHostFull) {
		t.Fatalf("full Add = %v", err)
	}
	if second.closeCalls != 0 {
		t.Fatal("failed Add stole caller ownership")
	}
	key := first.identity.Group
	if err := host.RequestTick(key); err != nil {
		t.Fatal(err)
	}
	if err := host.RequestCampaign(key); err != nil {
		t.Fatal(err)
	}
	if err := host.RequestTick(key); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("group item overflow = %v", err)
	}
	if host.queueItems != 2 || host.groups[key].items != 2 {
		t.Fatalf("queue accounting = global %d group %d", host.queueItems, host.groups[key].items)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	if first.closeCalls != 1 || second.closeCalls != 0 {
		t.Fatalf("Close ownership calls = first %d second %d", first.closeCalls, second.closeCalls)
	}
}

func TestHostQueueDetachesAndRotatesInputClasses(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(3)
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	key := runtime.identity.Group
	message := hostMessage(99, runtime.identity.MemberID, "original")
	proposal := []byte("proposal")
	if err := host.EnqueueMessage(key, message); err != nil {
		t.Fatal(err)
	}
	if err := host.EnqueueProposal(key, proposal); err != nil {
		t.Fatal(err)
	}
	if err := host.RequestTick(key); err != nil {
		t.Fatal(err)
	}
	if err := host.RequestCampaign(key); err != nil {
		t.Fatal(err)
	}
	message.Context[0] = 'X'
	proposal[0] = 'X'

	var got []ProgressKind
	for turn := range 8 {
		progress, done, runErr := host.RunOne()
		if runErr != nil || !done {
			t.Fatalf("RunOne = %+v, %t, %v", progress, done, runErr)
		}
		got = append(got, progress.Kind)
		if turn < 4 {
			switch progress.Kind {
			case ProgressMessage:
				if err := host.EnqueueMessage(key, hostMessage(99, runtime.identity.MemberID, "next")); err != nil {
					t.Fatal(err)
				}
			case ProgressProposal:
				if err := host.EnqueueProposal(key, []byte("next")); err != nil {
					t.Fatal(err)
				}
			case ProgressTick:
				if err := host.RequestTick(key); err != nil {
					t.Fatal(err)
				}
			case ProgressCampaign:
				if err := host.RequestCampaign(key); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	want := []ProgressKind{
		ProgressMessage, ProgressProposal, ProgressTick, ProgressCampaign,
		ProgressMessage, ProgressProposal, ProgressTick, ProgressCampaign,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("input class order = %v, want %v", got, want)
	}
	if string(runtime.messages[0].GetContext()) != "original" || string(runtime.proposals[0]) != "proposal" {
		t.Fatal("queued input retained caller aliases")
	}
	if host.queueItems != 0 || host.queueBytes != 0 {
		t.Fatalf("queue accounting after drain = %d, %d", host.queueItems, host.queueBytes)
	}
}

func TestHostSurfacesMembershipReadControlsAndOutcomes(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(4)
	runtime.publication = raftmodel.Publication{Applied: 9, ReplicaSetVersion: 7}
	runtime.status = raftmember.RuntimeStatus{
		MemberID: runtime.identity.MemberID, LeaderID: runtime.identity.MemberID,
		Term: 5, Commit: 9, Applied: 9,
	}
	runtime.progress = map[uint64]raftmodel.MemberProgress{
		99: {Match: 9, Next: 10, RecentActive: true},
	}
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	if _, done, err := host.RunOne(); done || err != nil {
		t.Fatalf("initial Ready probe = %t, %v", done, err)
	}

	member := uint64(99)
	change := &pb.ConfChange{
		Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: &member,
	}
	if err := host.ProposeConfChange(runtime.identity.Group, change); err != nil {
		t.Fatal(err)
	}
	member = 100
	if len(runtime.confChanges) != 1 || runtime.confChanges[0].GetChanges()[0].GetNodeId() != 99 {
		t.Fatalf("detached configuration changes = %+v", runtime.confChanges)
	}

	context := []byte("linearizable-read")
	if err := host.ReadIndex(runtime.identity.Group, context); err != nil {
		t.Fatal(err)
	}
	context[0] = 'X'
	if len(runtime.readContexts) != 1 || string(runtime.readContexts[0]) != "linearizable-read" {
		t.Fatalf("detached read contexts = %q", runtime.readContexts)
	}
	publication, err := host.Publication(runtime.identity.Group)
	if err != nil || publication.Applied != 9 || publication.ReplicaSetVersion != 7 {
		t.Fatalf("Publication = %+v, %v", publication, err)
	}
	runtime.snapshotState = replicatedstate.State{Applied: 9, SnapshotBaseDigest: [32]byte{1}}
	snapshotState, err := host.SnapshotState(runtime.identity.Group)
	if err != nil || snapshotState.Applied != 9 || snapshotState.SnapshotBaseDigest[0] != 1 {
		t.Fatalf("SnapshotState = %+v, %v", snapshotState, err)
	}
	status, err := host.Status(runtime.identity.Group)
	if err != nil || status.Term != 5 || status.Commit != 9 {
		t.Fatalf("Status = %+v, %v", status, err)
	}
	memberProgress, found, err := host.Progress(runtime.identity.Group, 99)
	if err != nil || !found || memberProgress.Match != 9 || !memberProgress.RecentActive {
		t.Fatalf("Progress = %+v, %t, %v", memberProgress, found, err)
	}
	if err := host.TransferLeader(runtime.identity.Group, 99); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(runtime.transfers, []uint64{99}) {
		t.Fatalf("leader transfers = %v", runtime.transfers)
	}

	runtime.ready = []fakeReady{{
		kind: raftmember.DriveReadStatesFinished,
		readOutcomes: []raftmodel.ReadOutcome{{Barrier: raftmodel.ReadBarrier{
			Context: []byte("linearizable-read"), Index: 9, Term: 3, Incarnation: 1,
		}}},
	}}
	progress, done, err := host.RunOne()
	if err != nil || !done || progress.ReadyKind != raftmember.DriveReadStatesFinished ||
		len(progress.ReadOutcomes) != 1 ||
		string(progress.ReadOutcomes[0].Barrier.Context) != "linearizable-read" {
		t.Fatalf("read outcome progress = %+v, %t, %v", progress, done, err)
	}
}

func TestHostAdoptMessageTransfersExactOwnedMessageOnlyOnSuccess(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(73)
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	key := runtime.identity.Group
	message := hostMessage(99, runtime.identity.MemberID, "owned")
	if err := host.AdoptMessage(key, message); err != nil {
		t.Fatal(err)
	}
	queued := host.groups[key].messages.items[0].message
	if queued != message {
		t.Fatal("AdoptMessage cloned instead of transferring the detached message")
	}
	if host.queueItems != 1 {
		t.Fatalf("queue items = %d, want 1", host.queueItems)
	}

	limits := testHostLimits()
	limits.MaxGroupItems = 1
	limits.MaxPendingTicks = 1
	full, err := NewHost(limits)
	if err != nil {
		t.Fatal(err)
	}
	other := newFakeRuntime(74)
	if err := full.addRuntime(other); err != nil {
		t.Fatal(err)
	}
	if err := full.RequestTick(other.identity.Group); err != nil {
		t.Fatal(err)
	}
	rejected := hostMessage(99, other.identity.MemberID, "caller-retains")
	if err := full.AdoptMessage(other.identity.Group, rejected); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("AdoptMessage at bound = %v, want ErrQueueFull", err)
	}
	if got := string(rejected.GetContext()); got != "caller-retains" {
		t.Fatalf("rejected message changed to %q", got)
	}
	if full.groups[other.identity.Group].messages.len() != 0 {
		t.Fatal("failed AdoptMessage retained caller message")
	}
}

func TestHostIdleGroupLeavesRunnableQueue(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(4)
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	if _, done, err := host.RunOne(); done || err != nil {
		t.Fatalf("initial idle probe = %t, %v", done, err)
	}
	if runtime.driveCalls != 1 || host.runnableLen() != 0 {
		t.Fatalf("idle probe calls=%d runnable=%d", runtime.driveCalls, host.runnableLen())
	}
	if _, done, err := host.RunOne(); done || err != nil {
		t.Fatalf("dormant RunOne = %t, %v", done, err)
	}
	if runtime.driveCalls != 1 {
		t.Fatalf("dormant group was polled again: %d calls", runtime.driveCalls)
	}
	if err := host.RequestTick(runtime.identity.Group); err != nil {
		t.Fatal(err)
	}
	progress, done, err := host.RunOne()
	if err != nil || !done || progress.Kind != ProgressTick {
		t.Fatalf("explicit wake = %+v, %t, %v", progress, done, err)
	}
}

func TestHostRunnableQueueReusesCapacityWithoutAllocation(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(5)
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	group := host.groups[runtime.identity.Group]
	if group == nil || cap(host.runnable) == 0 {
		t.Fatal("initial runnable queue has no retained capacity")
	}
	initialCapacity := cap(host.runnable)
	allocs := testing.AllocsPerRun(1_000, func() {
		popped := host.popRunnable()
		if popped != group {
			panic("unexpected runnable group")
		}
		host.wake(popped)
	})
	if allocs != 0 {
		t.Fatalf("runnable pop/wake allocations = %v, want 0", allocs)
	}
	if cap(host.runnable) != initialCapacity || host.runnableLen() != 1 {
		t.Fatalf(
			"runnable queue capacity/length = %d/%d, want %d/1",
			cap(host.runnable), host.runnableLen(), initialCapacity,
		)
	}
}

func TestHostReadyFirstAndGroupRoundRobin(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtimes := []*fakeRuntime{newFakeRuntime(10), newFakeRuntime(20), newFakeRuntime(30)}
	for _, runtime := range runtimes {
		runtime.ready = []fakeReady{{kind: raftmember.DriveCaptured}}
		if err := host.addRuntime(runtime); err != nil {
			t.Fatal(err)
		}
		if err := host.EnqueueProposal(runtime.identity.Group, []byte("queued")); err != nil {
			t.Fatal(err)
		}
	}
	for index, runtime := range runtimes {
		progress, done, runErr := host.RunOne()
		if runErr != nil || !done || progress.Kind != ProgressReady || progress.Group != runtime.identity.Group {
			t.Fatalf("round %d = %+v, %t, %v", index, progress, done, runErr)
		}
	}
	for index, runtime := range runtimes {
		progress, done, runErr := host.RunOne()
		if runErr != nil || !done || progress.Kind != ProgressProposal || progress.Group != runtime.identity.Group {
			t.Fatalf("proposal round %d = %+v, %t, %v", index, progress, done, runErr)
		}
	}
}

func TestHostProposalBatchPreservesGroupAndInputClassFairness(t *testing.T) {
	limits := testHostLimits()
	limits.MaxQueueItems = 128
	limits.MaxGroupItems = 128
	host, err := NewHost(limits)
	if err != nil {
		t.Fatal(err)
	}
	first, second := newFakeRuntime(31), newFakeRuntime(32)
	if err := host.addRuntime(first); err != nil {
		t.Fatal(err)
	}
	for entry := 0; entry < raftmodel.MaxProposalBatchEntries+1; entry++ {
		if err := host.EnqueueProposal(first.identity.Group, []byte{byte(entry)}); err != nil {
			t.Fatalf("EnqueueProposal(%d) error = %v", entry, err)
		}
	}
	if err := host.RequestTick(first.identity.Group); err != nil {
		t.Fatal(err)
	}
	if err := host.addRuntime(second); err != nil {
		t.Fatal(err)
	}
	if err := host.RequestTick(second.identity.Group); err != nil {
		t.Fatal(err)
	}

	progress, done, err := host.RunOne()
	if err != nil || !done || progress.Group != first.identity.Group ||
		progress.Kind != ProgressProposal ||
		progress.ProposalCount != raftmodel.MaxProposalBatchEntries ||
		progress.ProposalBytes != int64(raftmodel.MaxProposalBatchEntries) ||
		len(first.proposals) != raftmodel.MaxProposalBatchEntries {
		t.Fatalf("first proposal batch = %+v, %t, %v; proposals=%d", progress, done, err, len(first.proposals))
	}
	progress, done, err = host.RunOne()
	if err != nil || !done || progress.Group != second.identity.Group || progress.Kind != ProgressTick {
		t.Fatalf("other-group turn = %+v, %t, %v", progress, done, err)
	}
	progress, done, err = host.RunOne()
	if err != nil || !done || progress.Group != first.identity.Group || progress.Kind != ProgressTick {
		t.Fatalf("next input-class turn = %+v, %t, %v", progress, done, err)
	}
	progress, done, err = host.RunOne()
	if err != nil || !done || progress.Group != first.identity.Group ||
		progress.Kind != ProgressProposal || progress.ProposalCount != 1 || progress.ProposalBytes != 1 ||
		len(first.proposals) != raftmodel.MaxProposalBatchEntries+1 {
		t.Fatalf("proposal remainder = %+v, %t, %v; proposals=%d", progress, done, err, len(first.proposals))
	}
	if host.queueItems != 0 || host.queueBytes != 0 {
		t.Fatalf("queue accounting after batches = %d/%d", host.queueItems, host.queueBytes)
	}
}

func TestHostProposalBatchByteTargetAndOversizedFirst(t *testing.T) {
	t.Run("exact target", func(t *testing.T) {
		host, err := NewHost(testHostLimits())
		if err != nil {
			t.Fatal(err)
		}
		runtime := newFakeRuntime(33)
		if err := host.addRuntime(runtime); err != nil {
			t.Fatal(err)
		}
		half := make([]byte, int(raftmodel.MaxProposalBatchBytes/2))
		for _, proposal := range [][]byte{half, half, {1}} {
			if err := host.EnqueueProposal(runtime.identity.Group, proposal); err != nil {
				t.Fatal(err)
			}
		}
		progress, done, err := host.RunOne()
		if err != nil || !done || progress.Kind != ProgressProposal || progress.ProposalCount != 2 ||
			progress.ProposalBytes != raftmodel.MaxProposalBatchBytes || len(runtime.proposals) != 2 {
			t.Fatalf("exact-target batch = %+v, %t, %v; proposals=%d", progress, done, err, len(runtime.proposals))
		}
		if host.groups[runtime.identity.Group].proposals.len() != 1 {
			t.Fatalf("exact-target remainder = %d", host.groups[runtime.identity.Group].proposals.len())
		}
	})

	t.Run("oversized first", func(t *testing.T) {
		host, err := NewHost(testHostLimits())
		if err != nil {
			t.Fatal(err)
		}
		runtime := newFakeRuntime(34)
		if err := host.addRuntime(runtime); err != nil {
			t.Fatal(err)
		}
		oversized := make([]byte, int(raftmodel.MaxProposalBatchBytes)+1)
		if err := host.EnqueueProposal(runtime.identity.Group, oversized); err != nil {
			t.Fatal(err)
		}
		if err := host.EnqueueProposal(runtime.identity.Group, []byte{1}); err != nil {
			t.Fatal(err)
		}
		progress, done, err := host.RunOne()
		if err != nil || !done || progress.Kind != ProgressProposal || progress.ProposalCount != 1 ||
			progress.ProposalBytes != int64(len(oversized)) || len(runtime.proposals) != 1 ||
			len(runtime.proposals[0]) != len(oversized) {
			t.Fatalf("oversized-first batch = %+v, %t, %v; proposals=%d", progress, done, err, len(runtime.proposals))
		}
		if host.groups[runtime.identity.Group].proposals.len() != 1 {
			t.Fatalf("oversized-first remainder = %d", host.groups[runtime.identity.Group].proposals.len())
		}
	})
}

func TestHostOutboxBackpressureDoesNotStarveAnotherGroup(t *testing.T) {
	limits := testHostLimits()
	limits.MaxOutboxItems = 1
	host, err := NewHost(limits)
	if err != nil {
		t.Fatal(err)
	}
	first, second := newFakeRuntime(40), newFakeRuntime(50)
	firstMessage := hostMessage(first.identity.MemberID, 999, "")
	secondMessage := hostMessage(second.identity.MemberID, 999, "")
	first.ready = []fakeReady{{kind: raftmember.DriveMessage, outbound: firstMessage}}
	second.ready = []fakeReady{{kind: raftmember.DriveMessage, outbound: secondMessage}}
	if err := host.addRuntime(first); err != nil {
		t.Fatal(err)
	}
	if err := host.addRuntime(second); err != nil {
		t.Fatal(err)
	}
	if err := host.RequestTick(first.identity.Group); err != nil {
		t.Fatal(err)
	}
	progress, done, err := host.RunOne()
	if err != nil || !done || progress.Group != first.identity.Group || progress.Kind != ProgressReady {
		t.Fatalf("first outbound = %+v, %t, %v", progress, done, err)
	}
	// The second group is blocked on the full outbox, but scanning continues to
	// the first group's queued tick.
	progress, done, err = host.RunOne()
	if err != nil || !done || progress.Group != first.identity.Group || progress.Kind != ProgressTick {
		t.Fatalf("backpressure fairness = %+v, %t, %v", progress, done, err)
	}
	outbound, ok := host.PopOutbound()
	if !ok || !proto.Equal(outbound.Message, firstMessage) {
		t.Fatalf("first PopOutbound = %+v, %t", outbound, ok)
	}
	progress, done, err = host.RunOne()
	if err != nil || !done || progress.Group != second.identity.Group || progress.Kind != ProgressReady {
		t.Fatalf("second outbound retry = %+v, %t, %v", progress, done, err)
	}
	secondMessage.Term = hostUint64Ptr(9)
	outbound, ok = host.PopOutbound()
	if !ok || outbound.Message.GetTerm() != 1 {
		t.Fatalf("outbound clone retained runtime alias: %+v", outbound)
	}
}

func TestHostConsumesInvalidInputAndLatchesRuntimeFailureOnce(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(60)
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	runtime.inputErr = errors.New("invalid queued request")
	for _, proposal := range [][]byte{[]byte("bad"), []byte("later-1"), []byte("later-2")} {
		if err := host.EnqueueProposal(runtime.identity.Group, proposal); err != nil {
			t.Fatal(err)
		}
	}
	progress, done, err := host.RunOne()
	if err == nil || !done || progress.Kind != ProgressProposal ||
		progress.ProposalCount != 1 || progress.ProposalBytes != int64(len("bad")) {
		t.Fatalf("invalid input = %+v, %t, %v", progress, done, err)
	}
	group := host.groups[runtime.identity.Group]
	if host.queueItems != 2 || host.queueBytes != int64(len("later-1")+len("later-2")) ||
		group.items != 2 || group.proposals.len() != 2 || len(runtime.proposals) != 1 {
		t.Fatalf(
			"proposal error accounting global=%d/%d group=%d proposals=%d consumed=%d",
			host.queueItems, host.queueBytes, group.items, group.proposals.len(), len(runtime.proposals),
		)
	}
	runtime.inputErr = nil
	if err := host.EnqueueProposal(runtime.identity.Group, []byte("retained")); err != nil {
		t.Fatal(err)
	}
	if err := host.RequestTick(runtime.identity.Group); err != nil {
		t.Fatal(err)
	}
	runtime.failureOnReadyError = true
	runtime.ready = []fakeReady{{err: errors.Join(raftmember.ErrRuntimeFailed, errors.New("terminal"))}}
	progress, done, err = host.RunOne()
	if err == nil || !done || progress.Kind != ProgressFault {
		t.Fatalf("runtime fault = %+v, %t, %v", progress, done, err)
	}
	if host.queueItems != 0 || host.queueBytes != 0 || host.groups[runtime.identity.Group].items != 0 {
		t.Fatalf("fault retained queue capacity: global=%d/%d group=%d",
			host.queueItems, host.queueBytes, host.groups[runtime.identity.Group].items)
	}
	if _, done, err = host.RunOne(); done || err != nil {
		t.Fatalf("latched fault reran = %t, %v", done, err)
	}
}

func TestHostProposalFaultRetainsConsumedPrefixAccounting(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(61)
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	for _, proposal := range [][]byte{[]byte("fault"), []byte("purged-1"), []byte("purged-2")} {
		if err := host.EnqueueProposal(runtime.identity.Group, proposal); err != nil {
			t.Fatal(err)
		}
	}
	runtime.inputErr = errors.Join(raftmember.ErrRuntimeFailed, errors.New("terminal proposal"))

	progress, done, err := host.RunOne()
	if !errors.Is(err, raftmember.ErrRuntimeFailed) || !done ||
		progress.Kind != ProgressFault || progress.Group != runtime.identity.Group ||
		progress.ProposalCount != 1 || progress.ProposalBytes != int64(len("fault")) {
		t.Fatalf("proposal fault = %+v, %t, %v", progress, done, err)
	}
	if host.queueItems != 0 || host.queueBytes != 0 ||
		host.groups[runtime.identity.Group].proposals.len() != 0 {
		t.Fatalf(
			"proposal fault retained queue: global=%d/%d group=%d",
			host.queueItems, host.queueBytes, host.groups[runtime.identity.Group].proposals.len(),
		)
	}
}

func TestHostRejectsClosedRuntimeAndMisroutedMessageBeforeCharge(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	closed := newFakeRuntime(62)
	closed.failure = raftmember.ErrRuntimeClosed
	if err := host.addRuntime(closed); !errors.Is(err, ErrGroupFaulted) {
		t.Fatalf("Add(closed) = %v", err)
	}
	if closed.closeCalls != 0 {
		t.Fatal("failed closed-runtime Add stole ownership")
	}
	runtime := newFakeRuntime(63)
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	misrouted := hostMessage(99, runtime.identity.MemberID+1, "wrong")
	if err := host.EnqueueMessage(runtime.identity.Group, misrouted); err == nil {
		t.Fatal("misrouted message was accepted")
	}
	if host.queueItems != 0 || host.queueBytes != 0 {
		t.Fatalf("misrouted message charged queue: %d/%d", host.queueItems, host.queueBytes)
	}
}

func TestHostRetriesNonterminalReadyErrorWithoutDroppingPhase(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(61)
	transient := errors.New("transient persist")
	runtime.ready = []fakeReady{{err: transient}}
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	progress, done, err := host.RunOne()
	if !errors.Is(err, transient) || done || progress.Kind != ProgressReady {
		t.Fatalf("transient Ready = %+v, %t, %v", progress, done, err)
	}
	if host.groups[runtime.identity.Group].failure != nil || host.runnableLen() != 1 {
		t.Fatal("nonterminal Ready error was latched or dropped")
	}
	runtime.ready[0].err = nil
	runtime.ready[0].kind = raftmember.DrivePersisted
	progress, done, err = host.RunOne()
	if err != nil || !done || progress.ReadyKind != raftmember.DrivePersisted {
		t.Fatalf("Ready retry = %+v, %t, %v", progress, done, err)
	}
}

func TestHostRejectsRecursiveMessageBeforeCloneAndRetriesClose(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(70)
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	cycle := hostMessage(99, runtime.identity.MemberID, "")
	cycle.Responses = []*pb.Message{cycle}
	if err := host.EnqueueMessage(runtime.identity.Group, cycle); err == nil {
		t.Fatal("recursive message was accepted")
	}
	closeErr := errors.New("retry close")
	runtime.closeErrs = []error{closeErr, nil}
	if err := host.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("first Close = %v", err)
	}
	if runtime.closeCalls != 1 {
		t.Fatalf("first close calls = %d", runtime.closeCalls)
	}
	if err := host.Close(); err != nil {
		t.Fatalf("retry Close = %v", err)
	}
	if runtime.closeCalls != 2 {
		t.Fatalf("retry close calls = %d", runtime.closeCalls)
	}
}

func TestHostRemoveRequiresQuiescenceRetriesCloseAndReusesCapacity(t *testing.T) {
	limits := testHostLimits()
	limits.MaxGroups = 1
	host, err := NewHost(limits)
	if err != nil {
		t.Fatal(err)
	}
	first := newFakeRuntime(71)
	if err := host.addRuntime(first); err != nil {
		t.Fatal(err)
	}
	if err := host.Remove(first.identity.Group); !errors.Is(err, ErrGroupBusy) {
		t.Fatalf("Remove before initial Ready probe = %v", err)
	}
	if _, done, err := host.RunOne(); done || err != nil {
		t.Fatalf("initial Ready probe = %t, %v", done, err)
	}
	if err := host.RequestTick(first.identity.Group); err != nil {
		t.Fatal(err)
	}
	if err := host.Remove(first.identity.Group); !errors.Is(err, ErrGroupBusy) {
		t.Fatalf("Remove with queued tick = %v", err)
	}
	if _, done, err := host.RunOne(); !done || err != nil {
		t.Fatalf("drain tick = %t, %v", done, err)
	}
	if _, done, err := host.RunOne(); done || err != nil {
		t.Fatalf("post-tick Ready probe = %t, %v", done, err)
	}
	closeErr := errors.New("incomplete group close")
	first.closeErrs = []error{closeErr, nil}
	if err := host.Remove(first.identity.Group); !errors.Is(err, closeErr) {
		t.Fatalf("first Remove = %v", err)
	}
	if err := host.RequestTick(first.identity.Group); !errors.Is(err, ErrGroupBusy) {
		t.Fatalf("retiring group accepted input: %v", err)
	}
	if err := host.Remove(first.identity.Group); err != nil {
		t.Fatalf("retry Remove = %v", err)
	}
	if len(host.order) != 0 || len(host.groups) != 0 {
		t.Fatalf("removed group retained: order=%d groups=%d", len(host.order), len(host.groups))
	}
	second := newFakeRuntime(72)
	if err := host.addRuntime(second); err != nil {
		t.Fatalf("reuse group capacity: %v", err)
	}
}

func TestHostTrackedProposalLifecyclePreservesAcceptedPrefixAndProgress(t *testing.T) {
	var admissions []ProposalAdmission
	host, err := NewHostWithServingSinks(
		testHostLimits(), settleNoLocalWaiters,
		func(admission ProposalAdmission) { admissions = append(admissions, admission) },
		ignoreProposalGroupTermination,
		retainProposalGroupPending,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(91)
	runtime.proposalErrs = []error{nil, raftmodel.ErrReadyPending}
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	tokens := []ProposalToken{{1}, {2}, {3}}
	for index := range tokens {
		if err := host.EnqueueTrackedProposal(
			runtime.identity.Group, []byte{byte(index + 1)}, tokens[index],
		); err != nil {
			t.Fatal(err)
		}
	}
	progress, done, err := host.RunOne()
	if !done || !errors.Is(err, raftmodel.ErrReadyPending) ||
		progress.Kind != ProgressProposal || progress.ProposalCount != 2 ||
		progress.ProposalBytes != 2 {
		t.Fatalf("progress = %+v, %v, %v", progress, done, err)
	}
	if len(admissions) != 2 || !admissions[0].Admitted ||
		admissions[0].Token != tokens[0] || admissions[0].Cause != nil ||
		admissions[1].Admitted || admissions[1].Token != tokens[1] ||
		!errors.Is(admissions[1].Cause, raftmodel.ErrReadyPending) {
		t.Fatalf("admissions = %+v", admissions)
	}
	if runtime.statusCalls != 1 {
		t.Fatalf("tracked proposal prefix status calls = %d, want 1", runtime.statusCalls)
	}
	group := host.groups[runtime.identity.Group]
	if group.proposals.len() != 1 || host.queueItems != 1 || group.items != 1 {
		t.Fatalf("remaining queue = proposals %d host %d group %d",
			group.proposals.len(), host.queueItems, group.items)
	}
}

func TestHostFaultPurgeTerminatesEveryTrackedQueueToken(t *testing.T) {
	var admissions []ProposalAdmission
	host, err := NewHostWithServingSinks(
		testHostLimits(), settleNoLocalWaiters,
		func(admission ProposalAdmission) { admissions = append(admissions, admission) },
		ignoreProposalGroupTermination,
		retainProposalGroupPending,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(92)
	runtime.inputErr = raftmember.ErrRuntimeFailed
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	for index := uint64(1); index <= 3; index++ {
		if err := host.EnqueueTrackedProposal(
			runtime.identity.Group, []byte{byte(index)}, ProposalToken{index},
		); err != nil {
			t.Fatal(err)
		}
	}
	progress, done, err := host.RunOne()
	if !done || !errors.Is(err, raftmember.ErrRuntimeFailed) ||
		progress.Kind != ProgressFault || progress.ProposalCount != 1 {
		t.Fatalf("fault = %+v, %v, %v", progress, done, err)
	}
	if len(admissions) != 3 {
		t.Fatalf("admission callbacks = %d", len(admissions))
	}
	for index, admission := range admissions {
		if admission.Admitted || admission.Token != (ProposalToken{uint64(index + 1)}) ||
			admission.Cause == nil {
			t.Fatalf("admission %d = %+v", index, admission)
		}
	}
	if host.queueItems != 0 || host.queueBytes != 0 {
		t.Fatalf("purged accounting = %d, %d", host.queueItems, host.queueBytes)
	}
}

func TestHostClosePendingSettlementPreflightLeavesEveryGroupRunnable(t *testing.T) {
	var admissions []ProposalAdmission
	host, err := NewHostWithServingSinks(
		testHostLimits(), settleNoLocalWaiters,
		func(admission ProposalAdmission) { admissions = append(admissions, admission) },
		ignoreProposalGroupTermination,
		retainProposalGroupPending,
	)
	if err != nil {
		t.Fatal(err)
	}
	first, second := newFakeRuntime(93), newFakeRuntime(94)
	first.pendingSettlement = true
	if err := host.addRuntime(first); err != nil {
		t.Fatal(err)
	}
	if err := host.addRuntime(second); err != nil {
		t.Fatal(err)
	}
	token := ProposalToken{7, 8, 9, 10}
	if err := host.EnqueueTrackedProposal(second.identity.Group, []byte{1, 2}, token); err != nil {
		t.Fatal(err)
	}
	beforeRunnable, beforeItems, beforeBytes := host.runnableLen(), host.queueItems, host.queueBytes
	if err := host.Close(); !errors.Is(err, raftmember.ErrResultSettlementPending) ||
		!errors.Is(err, ErrGroupBusy) {
		t.Fatalf("Close preflight = %v", err)
	}
	if host.closed || host.runnableLen() != beforeRunnable || host.queueItems != beforeItems ||
		host.queueBytes != beforeBytes || first.closeCalls != 0 || second.closeCalls != 0 ||
		len(admissions) != 0 {
		t.Fatalf("Close mutated host: closed=%v runnable=%d items=%d bytes=%d calls=%d/%d callbacks=%d",
			host.closed, host.runnableLen(), host.queueItems, host.queueBytes,
			first.closeCalls, second.closeCalls, len(admissions))
	}
	first.pendingSettlement = false
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	if !host.closed || len(admissions) != 1 || admissions[0].Token != token ||
		admissions[0].Admitted || !errors.Is(admissions[0].Cause, ErrHostClosed) {
		t.Fatalf("terminal close = closed %v callbacks %+v", host.closed, admissions)
	}
}

func TestHostRemovePendingSettlementPreflightDoesNotRetireGroup(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(97)
	runtime.pendingSettlement = true
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	group := host.groups[runtime.identity.Group]
	beforeRunnable := host.runnableLen()
	if err := host.Remove(runtime.identity.Group); !errors.Is(err, ErrGroupBusy) ||
		!errors.Is(err, raftmember.ErrResultSettlementPending) {
		t.Fatalf("Remove preflight = %v", err)
	}
	if group.retiring || group.runtime != runtime || host.runnableLen() != beforeRunnable ||
		runtime.closeCalls != 0 {
		t.Fatalf("Remove mutated group: retiring=%v runtime=%p runnable=%d closes=%d",
			group.retiring, group.runtime, host.runnableLen(), runtime.closeCalls)
	}
	runtime.pendingSettlement = false
	if _, done, err := host.RunOne(); err != nil || done {
		t.Fatalf("drain idle probe = %v, %v", done, err)
	}
	if err := host.Remove(runtime.identity.Group); err != nil {
		t.Fatalf("Remove retry = %v", err)
	}
}

func TestHostTrackedProposalRequiresAtomicServingSinkAndQueueCapacity(t *testing.T) {
	ordinary, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	group := newFakeRuntime(96).identity.Group
	if err := ordinary.EnqueueTrackedProposal(
		group, []byte{1}, ProposalToken{1},
	); !errors.Is(err, ErrProposalSinkRequired) {
		t.Fatalf("ordinary tracked proposal = %v", err)
	}
	if host, err := NewHostWithServingSinks(
		testHostLimits(), nil, func(ProposalAdmission) {}, ignoreProposalGroupTermination,
		retainProposalGroupPending,
	); host != nil ||
		!errors.Is(err, ErrSettlementSinkRequired) {
		t.Fatalf("nil settlement = %v, %v", host, err)
	}
	if host, err := NewHostWithServingSinks(
		testHostLimits(), settleNoLocalWaiters, nil, ignoreProposalGroupTermination,
		retainProposalGroupPending,
	); host != nil ||
		!errors.Is(err, ErrProposalSinkRequired) {
		t.Fatalf("nil lifecycle = %v, %v", host, err)
	}
	if host, err := NewHostWithServingSinks(
		testHostLimits(), settleNoLocalWaiters, func(ProposalAdmission) {}, nil,
		retainProposalGroupPending,
	); host != nil || !errors.Is(err, ErrProposalGroupSinkRequired) {
		t.Fatalf("nil group lifecycle = %v, %v", host, err)
	}
	if host, err := NewHostWithServingSinks(
		testHostLimits(), settleNoLocalWaiters, func(ProposalAdmission) {},
		ignoreProposalGroupTermination, nil,
	); host != nil || !errors.Is(err, ErrProposalPendingRequired) {
		t.Fatalf("nil group pending = %v, %v", host, err)
	}

	limits := testHostLimits()
	limits.MaxQueueItems = 1
	limits.MaxGroupItems = 1
	limits.MaxPendingTicks = 1
	callbacks := 0
	host, err := NewHostWithServingSinks(
		limits, settleNoLocalWaiters, func(ProposalAdmission) { callbacks++ },
		ignoreProposalGroupTermination,
		retainProposalGroupPending,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(95)
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	if err := host.EnqueueTrackedProposal(runtime.identity.Group, []byte{1}, ProposalToken{1}); err != nil {
		t.Fatal(err)
	}
	if err := host.EnqueueTrackedProposal(
		runtime.identity.Group, []byte{2}, ProposalToken{2},
	); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue = %v", err)
	}
	if callbacks != 0 || host.queueItems != 1 || host.groups[runtime.identity.Group].proposals.len() != 1 {
		t.Fatalf("partial tracked publication = callbacks %d items %d", callbacks, host.queueItems)
	}
}

func TestHostTrackedLeadershipFencesFollowerAndTerminatesOldLeaderBeforeRetry(t *testing.T) {
	var admissions []ProposalAdmission
	var terminations []ProposalGroupTermination
	host, err := NewHostWithServingSinks(
		testHostLimits(), settleNoLocalWaiters,
		func(admission ProposalAdmission) { admissions = append(admissions, admission) },
		func(termination ProposalGroupTermination) {
			terminations = append(terminations, termination)
		},
		retainProposalGroupPending,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(98)
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	first := ProposalToken{1, 2, 3, 4}
	if err := host.EnqueueTrackedProposal(runtime.identity.Group, []byte{1}, first); err != nil {
		t.Fatal(err)
	}
	if progress, done, err := host.RunOne(); err != nil || !done ||
		progress.Kind != ProgressProposal {
		t.Fatalf("leader proposal = %+v, %v, %v", progress, done, err)
	}
	if len(admissions) != 1 || !admissions[0].Admitted ||
		host.groups[runtime.identity.Group].trackedLeaderTerm != 1 {
		t.Fatalf("leader admission = %+v term %d", admissions,
			host.groups[runtime.identity.Group].trackedLeaderTerm)
	}

	runtime.status.LeaderID = runtime.identity.MemberID + 1
	runtime.status.RaftState = raft.StateFollower
	runtime.status.Term = 2
	if err := host.RequestTick(runtime.identity.Group); err != nil {
		t.Fatal(err)
	}
	if _, done, err := host.RunOne(); err != nil || !done {
		t.Fatalf("leadership-loss boundary = %v, %v", done, err)
	}
	if len(terminations) != 1 || terminations[0].Group != runtime.identity.Group ||
		terminations[0].Reason != ProposalGroupLeadershipLost {
		t.Fatalf("leadership termination = %+v", terminations)
	}

	follower := ProposalToken{5, 6, 7, 8}
	if err := host.EnqueueTrackedProposal(runtime.identity.Group, []byte{2}, follower); err != nil {
		t.Fatal(err)
	}
	beforeProposals := len(runtime.proposals)
	if progress, done, err := host.RunOne(); err != nil || !done ||
		progress.Kind != ProgressProposal {
		t.Fatalf("follower proposal = %+v, %v, %v", progress, done, err)
	}
	if len(runtime.proposals) != beforeProposals || len(admissions) != 2 ||
		admissions[1].Admitted || !errors.Is(admissions[1].Cause, raftmodel.ErrNotLeader) {
		t.Fatalf("follower admission forwarded into Raft: proposals %d admissions %+v",
			len(runtime.proposals), admissions)
	}

	runtime.status.LeaderID = runtime.identity.MemberID
	runtime.status.RaftState = raft.StateLeader
	runtime.status.Term = 3
	third := ProposalToken{9, 10, 11, 12}
	if err := host.EnqueueTrackedProposal(runtime.identity.Group, []byte{3}, third); err != nil {
		t.Fatal(err)
	}
	if _, done, err := host.RunOne(); err != nil || !done {
		t.Fatalf("new leader proposal = %v, %v", done, err)
	}
	if len(admissions) != 3 || !admissions[2].Admitted ||
		host.groups[runtime.identity.Group].trackedLeaderTerm != 3 {
		t.Fatalf("new leader admission = %+v", admissions)
	}
	if _, done, err := host.RunOne(); err != nil || done {
		t.Fatalf("drain before Remove = %v, %v", done, err)
	}
	if err := host.Remove(runtime.identity.Group); err != nil {
		t.Fatal(err)
	}
	if len(terminations) != 2 || terminations[1].Reason != ProposalGroupRemoved ||
		runtime.closeCalls != 1 {
		t.Fatalf("Remove termination ordering = %+v closes %d", terminations, runtime.closeCalls)
	}
}

func TestHostTrackedLeadershipStopsStatusChecksAfterLastPendingAttempt(t *testing.T) {
	pending := false
	host, err := NewHostWithServingSinks(
		testHostLimits(), settleNoLocalWaiters,
		func(admission ProposalAdmission) {
			if admission.Admitted {
				pending = true
			}
		},
		ignoreProposalGroupTermination,
		func(raftmember.GroupKey) bool { return pending },
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(99)
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	if err := host.EnqueueTrackedProposal(
		runtime.identity.Group, []byte{1}, ProposalToken{1},
	); err != nil {
		t.Fatal(err)
	}
	if _, done, err := host.RunOne(); err != nil || !done {
		t.Fatalf("tracked proposal = %v, %v", done, err)
	}
	if runtime.statusCalls != 1 ||
		host.groups[runtime.identity.Group].trackedLeaderTerm != 1 {
		t.Fatalf("admitted tracking = status %d term %d", runtime.statusCalls,
			host.groups[runtime.identity.Group].trackedLeaderTerm)
	}

	pending = false
	if _, done, err := host.RunOne(); err != nil || done {
		t.Fatalf("pending-clear scheduler turn = %v, %v", done, err)
	}
	if runtime.statusCalls != 1 ||
		host.groups[runtime.identity.Group].trackedLeaderTerm != 0 {
		t.Fatalf("cleared tracking = status %d term %d", runtime.statusCalls,
			host.groups[runtime.identity.Group].trackedLeaderTerm)
	}
	if _, done, err := host.RunOne(); err != nil || done || runtime.statusCalls != 1 {
		t.Fatalf("idle scheduler retained status tax = %v, %v, calls %d",
			done, err, runtime.statusCalls)
	}
}

func TestHostTrackedProposalRunOneWarmAllocations(t *testing.T) {
	host, err := NewHostWithServingSinks(
		testHostLimits(), settleNoLocalWaiters,
		func(ProposalAdmission) {}, ignoreProposalGroupTermination,
		func(raftmember.GroupKey) bool { return false },
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(100)
	runtime.discardProposals = true
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	group := host.groups[runtime.identity.Group]
	data := []byte{1}
	backing := make([]queuedProposal, 1)
	run := func() {
		backing[0] = queuedProposal{data: data, token: ProposalToken{1}, tracked: true}
		group.proposals.items = backing[:1]
		group.proposals.head = 0
		group.items = 1
		group.bytes = 1
		host.queueItems = 1
		host.queueBytes = 1
		if !group.runnable {
			host.wake(group)
		}
		progress, done, runErr := host.RunOne()
		if runErr != nil || !done || progress.Kind != ProgressProposal ||
			progress.ProposalCount != 1 {
			panic("unexpected tracked proposal progress")
		}
	}
	run()
	allocs := testing.AllocsPerRun(1_000, run)
	if allocs != 0 {
		t.Fatalf("warm tracked proposal RunOne allocations = %v, want 0", allocs)
	}
}

func BenchmarkHostTrackedProposalRunOne(b *testing.B) {
	host, err := NewHostWithServingSinks(
		testHostLimits(), settleNoLocalWaiters,
		func(ProposalAdmission) {}, ignoreProposalGroupTermination,
		func(raftmember.GroupKey) bool { return false },
	)
	if err != nil {
		b.Fatal(err)
	}
	runtime := newFakeRuntime(101)
	runtime.discardProposals = true
	if err := host.addRuntime(runtime); err != nil {
		b.Fatal(err)
	}
	group := host.groups[runtime.identity.Group]
	data := []byte{1}
	backing := make([]queuedProposal, 1)
	run := func() {
		backing[0] = queuedProposal{data: data, token: ProposalToken{1}, tracked: true}
		group.proposals.items = backing[:1]
		group.proposals.head = 0
		group.items = 1
		group.bytes = 1
		host.queueItems = 1
		host.queueBytes = 1
		if !group.runnable {
			host.wake(group)
		}
		progress, done, runErr := host.RunOne()
		if runErr != nil || !done || progress.Kind != ProgressProposal ||
			progress.ProposalCount != 1 {
			panic("unexpected tracked proposal progress")
		}
	}
	run()
	b.ReportAllocs()
	b.SetBytes(1)
	b.ReportMetric(float64(unsafe.Sizeof(Host{})), "host-B")
	b.ReportMetric(float64(unsafe.Sizeof(raftmember.ReadyWorkspace{})), "workspace-B")
	b.ResetTimer()
	for range b.N {
		run()
	}
}

func hostUint64Ptr(value uint64) *uint64 { return &value }
