package multiraft

import (
	"errors"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
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
	inputErr            error
	failure             error
	failureOnReadyError bool
	closeErrs           []error
	closeCalls          int
	driveCalls          int
}

func newFakeRuntime(seed byte) *fakeRuntime {
	key := raftmember.GroupKey{TopologyRecoveryEpoch: uint64(seed) + 1}
	for index := range key.ClusterID {
		key.ClusterID[index] = seed + byte(index) + 1
		key.ClusterIncarnation[index] = seed + byte(index) + 17
		key.ShardIncarnation[index] = seed + byte(index) + 33
		key.GroupID[index] = seed + byte(index) + 49
	}
	return &fakeRuntime{identity: raftmember.RuntimeIdentity{
		Group: key, MemberID: uint64(seed) + 1, NodeIncarnation: 1,
	}}
}

func (runtime *fakeRuntime) Identity() raftmember.RuntimeIdentity { return runtime.identity }
func (runtime *fakeRuntime) Failure() error                       { return runtime.failure }

func (runtime *fakeRuntime) Propose(data []byte) error {
	runtime.inputs = append(runtime.inputs, ProgressProposal)
	runtime.proposals = append(runtime.proposals, append([]byte(nil), data...))
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
	send func(raftmember.OutboundMessage) error,
) (raftmember.DriveResult, error) {
	runtime.driveCalls++
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
	if err := host.EnqueueProposal(runtime.identity.Group, []byte("bad")); err != nil {
		t.Fatal(err)
	}
	progress, done, err := host.RunOne()
	if err == nil || !done || progress.Kind != ProgressProposal {
		t.Fatalf("invalid input = %+v, %t, %v", progress, done, err)
	}
	if host.queueItems != 0 {
		t.Fatal("invalid queued input was retained")
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

func hostUint64Ptr(value uint64) *uint64 { return &value }
