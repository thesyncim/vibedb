package raftservice

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	pb "go.etcd.io/raft/v3/raftpb"
)

const leaderTransferTestTimeout = 3 * time.Second

type ownerTransferHost struct {
	ownerHost

	mu           sync.Mutex
	publications map[raftmember.GroupKey]raftmodel.Publication
	statuses     map[raftmember.GroupKey]raftmember.RuntimeStatus
	progress     map[raftmember.GroupKey]raftmodel.MemberProgress

	prepareErr     error
	readyErr       error
	prepareSeen    chan raftmember.GroupKey
	readySeen      chan raftmember.GroupKey
	cancelSeen     chan raftmember.GroupKey
	transferSeen   chan raftmember.GroupKey
	prepareBlocks  map[raftmember.GroupKey]chan struct{}
	prepareBlocked chan raftmember.GroupKey
	readyBlocks    map[raftmember.GroupKey]chan struct{}
	readyBlocked   chan raftmember.GroupKey

	transferRelease chan struct{}
	inboundSeen     chan struct{}
	tickSeen        chan raftmember.GroupKey
	readSeen        chan struct{}
	readContext     []byte
	readGroup       raftmember.GroupKey
	busy            bool
	busyGroup       raftmember.GroupKey
	runOneBlock     bool
	runOneBlocked   chan struct{}
	runOneRelease   chan struct{}
	runOneReleaseDo sync.Once
	idleSeen        chan struct{}

	prepareCalls        map[raftmember.GroupKey]int
	readyCalls          map[raftmember.GroupKey]int
	cancelCalls         map[raftmember.GroupKey]int
	transferCalls       map[raftmember.GroupKey]int
	readyChecksSinceRun int
	maxReadyChecksTurn  int
}

func newOwnerTransferHost() *ownerTransferHost {
	return &ownerTransferHost{
		publications:   make(map[raftmember.GroupKey]raftmodel.Publication),
		statuses:       make(map[raftmember.GroupKey]raftmember.RuntimeStatus),
		progress:       make(map[raftmember.GroupKey]raftmodel.MemberProgress),
		prepareSeen:    make(chan raftmember.GroupKey, 64),
		readySeen:      make(chan raftmember.GroupKey, 64),
		cancelSeen:     make(chan raftmember.GroupKey, 64),
		transferSeen:   make(chan raftmember.GroupKey, 64),
		prepareBlocks:  make(map[raftmember.GroupKey]chan struct{}),
		prepareBlocked: make(chan raftmember.GroupKey, 16),
		readyBlocks:    make(map[raftmember.GroupKey]chan struct{}),
		readyBlocked:   make(chan raftmember.GroupKey, 16),
		inboundSeen:    make(chan struct{}, 8),
		tickSeen:       make(chan raftmember.GroupKey, 64),
		readSeen:       make(chan struct{}, 8),
		idleSeen:       make(chan struct{}, 64),
		prepareCalls:   make(map[raftmember.GroupKey]int),
		readyCalls:     make(map[raftmember.GroupKey]int),
		cancelCalls:    make(map[raftmember.GroupKey]int),
		transferCalls:  make(map[raftmember.GroupKey]int),
	}
}

func signalOwnerTransferGroup(ch chan raftmember.GroupKey, group raftmember.GroupKey) {
	if ch == nil {
		return
	}
	select {
	case ch <- group:
	default:
	}
}

func signalOwnerTransfer(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

func ownerTransferInboundMessage() *pb.Message {
	from, to, term := uint64(2), uint64(1), uint64(1)
	return &pb.Message{Type: pb.MsgHeartbeat.Enum(), From: &from, To: &to, Term: &term}
}

func (host *ownerTransferHost) Publication(group raftmember.GroupKey) (raftmodel.Publication, error) {
	host.mu.Lock()
	defer host.mu.Unlock()
	return host.publications[group], nil
}

func (host *ownerTransferHost) Status(group raftmember.GroupKey) (raftmember.RuntimeStatus, error) {
	host.mu.Lock()
	defer host.mu.Unlock()
	return host.statuses[group], nil
}

func (host *ownerTransferHost) Progress(group raftmember.GroupKey, target uint64) (raftmodel.MemberProgress, bool, error) {
	host.mu.Lock()
	defer host.mu.Unlock()
	progress, found := host.progress[group]
	return progress, found || target != 0, nil
}

func (host *ownerTransferHost) PrepareLeaderTransfer(
	group raftmember.GroupKey, _ uint64,
) (raftmember.LeaderTransferGuard, error) {
	host.mu.Lock()
	host.prepareCalls[group]++
	err := host.prepareErr
	blockRelease := host.prepareBlocks[group]
	host.mu.Unlock()
	signalOwnerTransferGroup(host.prepareSeen, group)
	if blockRelease != nil {
		signalOwnerTransferGroup(host.prepareBlocked, group)
		<-blockRelease
	}
	return raftmember.LeaderTransferGuard{}, err
}

func (host *ownerTransferHost) CheckLeaderTransferReady(
	group raftmember.GroupKey, _ raftmember.LeaderTransferGuard,
) error {
	host.mu.Lock()
	host.readyCalls[group]++
	host.readyChecksSinceRun++
	err := host.readyErr
	blockRelease := host.readyBlocks[group]
	host.mu.Unlock()
	signalOwnerTransferGroup(host.readySeen, group)
	if blockRelease != nil {
		signalOwnerTransferGroup(host.readyBlocked, group)
		<-blockRelease
	}
	return err
}

func (host *ownerTransferHost) TransferLeader(
	group raftmember.GroupKey, _ raftmember.LeaderTransferGuard,
) error {
	host.mu.Lock()
	host.transferCalls[group]++
	host.mu.Unlock()
	signalOwnerTransferGroup(host.transferSeen, group)
	if host.transferRelease != nil {
		<-host.transferRelease
	}
	return nil
}

func (host *ownerTransferHost) CancelLeaderTransfer(
	group raftmember.GroupKey, _ raftmember.LeaderTransferGuard,
) error {
	host.mu.Lock()
	host.cancelCalls[group]++
	host.mu.Unlock()
	signalOwnerTransferGroup(host.cancelSeen, group)
	return nil
}

func (host *ownerTransferHost) AdoptMessage(raftmember.GroupKey, *pb.Message) error {
	signalOwnerTransfer(host.inboundSeen)
	return nil
}

func (host *ownerTransferHost) RequestTick(group raftmember.GroupKey) error {
	signalOwnerTransferGroup(host.tickSeen, group)
	return nil
}

func (host *ownerTransferHost) ReadIndex(group raftmember.GroupKey, context []byte) error {
	host.mu.Lock()
	host.readGroup = group
	host.readContext = append(host.readContext[:0], context...)
	host.mu.Unlock()
	signalOwnerTransfer(host.readSeen)
	return nil
}

func (host *ownerTransferHost) RunOne() (multiraft.Progress, bool, error) {
	host.mu.Lock()
	if host.readyChecksSinceRun > host.maxReadyChecksTurn {
		host.maxReadyChecksTurn = host.readyChecksSinceRun
	}
	host.readyChecksSinceRun = 0
	context := append([]byte(nil), host.readContext...)
	group := host.readGroup
	host.readContext = nil
	status := host.statuses[group]
	block := host.runOneBlock
	blocked := host.runOneBlocked
	release := host.runOneRelease
	host.mu.Unlock()
	if block {
		signalOwnerTransfer(blocked)
		<-release
	}
	if len(context) != 0 {
		return multiraft.Progress{
			Group:     group,
			Kind:      multiraft.ProgressReady,
			ReadyKind: raftmember.DriveReadStatesFinished,
			ReadOutcomes: []raftmodel.ReadOutcome{{Barrier: raftmodel.ReadBarrier{
				Context: context, Index: status.Applied, Term: status.Term, Incarnation: 1,
			}}},
		}, true, nil
	}
	host.mu.Lock()
	busy, busyGroup := host.busy, host.busyGroup
	host.mu.Unlock()
	if busy {
		return multiraft.Progress{Group: busyGroup, Kind: multiraft.ProgressProposal}, true, nil
	}
	signalOwnerTransfer(host.idleSeen)
	return multiraft.Progress{}, false, nil
}

func (host *ownerTransferHost) releaseRunOne() {
	host.runOneReleaseDo.Do(func() { close(host.runOneRelease) })
}

func (host *ownerTransferHost) PopOutbound() (raftmember.OutboundMessage, bool) {
	return raftmember.OutboundMessage{}, false
}

func (host *ownerTransferHost) DurablePromotion(
	raftmember.GroupKey, uint64,
) (raftmember.DurablePromotionProof, bool, error) {
	return raftmember.DurablePromotionProof{}, false, nil
}

func (host *ownerTransferHost) Close() error { return nil }

type ownerTransferAuthority struct {
	mu    sync.Mutex
	grant membershipgrant.Grant
	found bool
}

func (authority *ownerTransferAuthority) CurrentTransitionGrant(
	raftmember.GroupKey,
) (membershipgrant.Grant, bool, error) {
	authority.mu.Lock()
	defer authority.mu.Unlock()
	return authority.grant, authority.found, nil
}

func (*ownerTransferAuthority) PublishCommittedAuthority(
	raftmember.GroupKey, uint64, *pb.ConfState,
) error {
	return nil
}

func (*ownerTransferAuthority) PublishDurablePromotion(
	raftmember.GroupKey, raftmember.DurablePromotionProof,
) error {
	return nil
}

func (*ownerTransferAuthority) ClearDurablePromotion(raftmember.GroupKey) error {
	return nil
}

func (authority *ownerTransferAuthority) revoke() {
	authority.mu.Lock()
	authority.grant = membershipgrant.Grant{}
	authority.found = false
	authority.mu.Unlock()
}

type ownerTransferFixture struct {
	owner     *Owner
	host      *ownerTransferHost
	authority *ownerTransferAuthority
	groups    []raftmember.GroupKey
	fences    []ServingFence
	pulse     chan struct{}
}

func newOwnerTransferFixture(groupCount int, membership bool) *ownerTransferFixture {
	host := newOwnerTransferHost()
	pulse := make(chan struct{}, 8)
	fixture := &ownerTransferFixture{
		host:   host,
		groups: make([]raftmember.GroupKey, groupCount),
		fences: make([]ServingFence, groupCount),
		pulse:  pulse,
	}
	owner := &Owner{
		host:    host,
		groups:  fixture.groups,
		members: make(map[raftmember.GroupKey]ownerMember, groupCount),
		limits: Limits{MaxIngressItems: groupCount + 8, MaxIngressBytes: 1024,
			MaxPendingReadItems: 8, MaxPendingReadBytes: 8, MaxPendingOutboundBytes: 1024},
		ingress:              make(chan ownerRequest, groupCount+8),
		ready:                make(chan struct{}),
		done:                 make(chan struct{}),
		pulse:                pulse,
		pendingReads:         make(map[[16]byte]*readDelivery),
		sharedBarrierWaiters: make(map[[16]byte][]*readDelivery),
		inflightBarrier:      make(map[raftmember.GroupKey]sharedReadBarrier),
		pendingTransfers:     make(map[raftmember.GroupKey]*pendingLeaderTransfer),
		transferNotify:       make(chan struct{}, 1),
	}
	if membership {
		fixture.authority = &ownerTransferAuthority{}
		owner.authority = fixture.authority
	}
	for index := range fixture.groups {
		group := peerServerTestGroup()
		group.GroupID[0] += byte(index)
		fixture.groups[index] = group
		command := CommandFence{ReplicaSetVersion: 7, ActivePolicyGeneration: 1,
			ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1,
			RelationManifestDigest: [32]byte{1}, RoutingVersion: 1, RouteGeneration: 1}
		identity := raftmember.RuntimeIdentity{Group: group, AllocationGeneration: 1,
			MemberID: 1, StoreID: [16]byte{1}, NodeIncarnation: 1,
			RelationManifestDigest: command.RelationManifestDigest}
		fence := ServingFence{Group: group, AllocationGeneration: 1, Command: command,
			MemberID: 1, StoreID: identity.StoreID, NodeIncarnation: 1, Term: 2}
		fixture.fences[index] = fence
		owner.members[group] = ownerMember{identity: identity, command: command,
			generation: &ownerGeneration{}, read: &ownerTestReadSource{}}
		host.publications[group] = raftmodel.Publication{ReplicaSetVersion: 7,
			Applied: 7, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}
		host.statuses[group] = raftmember.RuntimeStatus{MemberID: 1, LeaderID: 1,
			Term: 2, Commit: 7, Applied: 7}
		host.progress[group] = raftmodel.MemberProgress{RecentActive: true, Match: 7, Next: 8}
	}
	if membership {
		group := fixture.groups[0]
		fixture.authority.grant = membershipgrant.Grant{Group: group,
			TransitionID: [16]byte{1}, MetadataEpoch: 1, CatalogGeneration: 1,
			InitialReplicaSetVersion: 7, InitialVoters: [3]uint64{1, 2, 3},
			SourceMember: 1, TargetMember: 2}
		fixture.authority.found = true
	}
	fixture.owner = owner
	return fixture
}

func startOwnerTransferFixture(t *testing.T, fixture *ownerTransferFixture) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- fixture.owner.Run(ctx) }()
	waitOwnerTransfer(t, fixture.owner.ready)
	t.Cleanup(func() {
		cancel()
		if err := waitOwnerTransfer(t, runDone); !errors.Is(err, context.Canceled) {
			t.Fatalf("owner exit = %v", err)
		}
	})
	return cancel
}

func waitOwnerTransfer[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	timer := time.NewTimer(leaderTransferTestTimeout)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		t.Fatal("timed out waiting for owner transfer test event")
		var zero T
		return zero
	}
}

func waitOwnerTransferGroup(
	t *testing.T, ch <-chan raftmember.GroupKey, want raftmember.GroupKey,
) {
	t.Helper()
	timer := time.NewTimer(leaderTransferTestTimeout)
	defer timer.Stop()
	for {
		select {
		case got := <-ch:
			if got == want {
				return
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for transfer group %v", want)
		}
	}
}

func waitOwnerTransferIngressItems(t *testing.T, owner *Owner, want int) {
	t.Helper()
	timer := time.NewTimer(leaderTransferTestTimeout)
	defer timer.Stop()
	for {
		owner.mu.Lock()
		items := owner.ingressItems
		owner.mu.Unlock()
		if items >= want {
			return
		}
		select {
		case <-timer.C:
			t.Fatalf("timed out waiting for transfer ingress items=%d, want at least %d", items, want)
		default:
			runtime.Gosched()
		}
	}
}

func assertOwnerTransferIngressEmpty(t *testing.T, owner *Owner) {
	t.Helper()
	timer := time.NewTimer(leaderTransferTestTimeout)
	defer timer.Stop()
	for {
		owner.mu.Lock()
		items, bytes := owner.ingressItems, owner.ingressBytes
		owner.mu.Unlock()
		if items == 0 && bytes == 0 {
			return
		}
		select {
		case <-timer.C:
			t.Fatalf("transfer ingress retained items=%d bytes=%d", items, bytes)
		default:
			runtime.Gosched()
		}
	}
}

func invokeOwnerTransfer(
	owner *Owner, kind string, ctx context.Context, fence ServingFence,
) error {
	switch kind {
	case "membership":
		return owner.ApplyMembership(ctx, MembershipRequest{Fence: fence,
			Kind: MembershipTransferLeader, TransitionID: [16]byte{1},
			MetadataEpoch: 1, CatalogGeneration: 1, ExpectedReplicaSetVersion: 7,
			SourceMember: 1, TargetMember: 2})
	case "split":
		return owner.TransferSplitSourceLeadership(ctx, fence, 2)
	case "schema":
		return owner.TransferSchemaLeadership(ctx, fence)
	default:
		return ErrInvalidOwner
	}
}

func TestOwnerLeaderTransferCancellationCleansEveryPublicEntryPoint(t *testing.T) {
	for _, kind := range []string{"membership", "split", "schema"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newOwnerTransferFixture(1, kind == "membership")
			fixture.host.readyErr = raftmodel.ErrReadyPending
			startOwnerTransferFixture(t, fixture)
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() {
				result <- invokeOwnerTransfer(fixture.owner, kind, ctx, fixture.fences[0])
			}()
			waitOwnerTransfer(t, fixture.host.prepareSeen)
			waitOwnerTransfer(t, fixture.host.readySeen)
			cancel()
			if err := waitOwnerTransfer(t, result); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled transfer = %v", err)
			}
			waitOwnerTransfer(t, fixture.host.cancelSeen)
			if len(fixture.host.transferSeen) != 0 {
				t.Fatal("canceled transfer reached Host admission")
			}
			assertOwnerTransferIngressEmpty(t, fixture.owner)
		})
	}
}

func TestOwnerLeaderTransferQueuedCancellationSkipsPreparation(t *testing.T) {
	fixture := newOwnerTransferFixture(1, false)
	fixture.host.runOneBlock = true
	fixture.host.runOneBlocked = make(chan struct{}, 1)
	fixture.host.runOneRelease = make(chan struct{})
	startOwnerTransferFixture(t, fixture)
	t.Cleanup(fixture.host.releaseRunOne)
	waitOwnerTransfer(t, fixture.host.runOneBlocked)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- fixture.owner.TransferSplitSourceLeadership(ctx, fixture.fences[0], 2)
	}()
	waitOwnerTransferIngressItems(t, fixture.owner, 1)
	cancel()
	if err := waitOwnerTransfer(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("queued cancellation = %v", err)
	}

	fixture.host.releaseRunOne()
	assertOwnerTransferIngressEmpty(t, fixture.owner)
	fixture.host.mu.Lock()
	prepareCalls := fixture.host.prepareCalls[fixture.groups[0]]
	readyCalls := fixture.host.readyCalls[fixture.groups[0]]
	transferCalls := fixture.host.transferCalls[fixture.groups[0]]
	cancelCalls := fixture.host.cancelCalls[fixture.groups[0]]
	fixture.host.mu.Unlock()
	if prepareCalls != 0 || readyCalls != 0 || transferCalls != 0 || cancelCalls != 0 {
		t.Fatalf("queued cancellation touched Host prepare=%d ready=%d transfer=%d cancel=%d",
			prepareCalls, readyCalls, transferCalls, cancelCalls)
	}
}

func TestOwnerLeaderTransferIdleOwnerRevisitsPendingGroupsWithoutPulse(t *testing.T) {
	transferGroups := pendingTransferVisitQuantum + 2
	fixture := newOwnerTransferFixture(transferGroups, false)
	fixture.host.readyErr = raftmodel.ErrReadyPending
	fixture.host.runOneBlock = true
	fixture.host.runOneBlocked = make(chan struct{}, 1)
	fixture.host.runOneRelease = make(chan struct{})
	startOwnerTransferFixture(t, fixture)
	t.Cleanup(fixture.host.releaseRunOne)
	waitOwnerTransfer(t, fixture.host.runOneBlocked)

	cancels := make([]context.CancelFunc, transferGroups)
	results := make([]chan error, transferGroups)
	for index := 0; index < transferGroups; index++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancels[index] = cancel
		results[index] = make(chan error, 1)
		index := index
		go func() {
			results[index] <- fixture.owner.TransferSplitSourceLeadership(
				ctx, fixture.fences[index], 2,
			)
		}()
	}
	waitOwnerTransferIngressItems(t, fixture.owner, transferGroups)
	fixture.host.releaseRunOne()
	preparedGroups := make(map[raftmember.GroupKey]struct{}, transferGroups)
	for len(preparedGroups) < transferGroups {
		preparedGroups[waitOwnerTransfer(t, fixture.host.prepareSeen)] = struct{}{}
	}

	readyGroups := make(map[raftmember.GroupKey]struct{}, transferGroups)
	for len(readyGroups) < transferGroups {
		readyGroups[waitOwnerTransfer(t, fixture.host.readySeen)] = struct{}{}
	}

	for _, cancel := range cancels {
		cancel()
	}
	canceledGroups := make(map[raftmember.GroupKey]struct{}, transferGroups)
	for len(canceledGroups) < transferGroups {
		canceledGroups[waitOwnerTransfer(t, fixture.host.cancelSeen)] = struct{}{}
	}
	for index := range results {
		if err := waitOwnerTransfer(t, results[index]); !errors.Is(err, context.Canceled) {
			t.Fatalf("transfer %d cancellation = %v", index, err)
		}
	}
	fixture.host.mu.Lock()
	maxVisits := fixture.host.maxReadyChecksTurn
	transferCalls := make(map[raftmember.GroupKey]int, len(fixture.host.transferCalls))
	cancelCalls := make(map[raftmember.GroupKey]int, len(fixture.host.cancelCalls))
	for group, calls := range fixture.host.transferCalls {
		transferCalls[group] = calls
	}
	for group, calls := range fixture.host.cancelCalls {
		cancelCalls[group] = calls
	}
	fixture.host.mu.Unlock()
	if maxVisits > 2 {
		t.Fatalf("idle transfer retries monopolized one Host turn: max=%d groups=%d", maxVisits, transferGroups)
	}
	for index := 0; index < transferGroups; index++ {
		group := fixture.groups[index]
		if _, ok := readyGroups[group]; !ok {
			t.Fatalf("idle transfer group %v was never revisited", group)
		}
		if _, ok := canceledGroups[group]; !ok || cancelCalls[group] != 1 {
			t.Fatalf("idle transfer group %v cancel calls = %d", group, cancelCalls[group])
		}
		if transferCalls[group] != 0 {
			t.Fatalf("canceled idle transfer group %v reached Host %d times", group, transferCalls[group])
		}
	}
	assertOwnerTransferIngressEmpty(t, fixture.owner)
}

func TestOwnerLeaderTransferCancellationSweepRetainsAppend(t *testing.T) {
	const transferGroups = 5
	fixture := newOwnerTransferFixture(transferGroups+1, false)
	fixture.host.readyErr = raftmodel.ErrReadyPending
	fixture.host.runOneBlock = true
	fixture.host.runOneBlocked = make(chan struct{}, 1)
	fixture.host.runOneRelease = make(chan struct{})
	startOwnerTransferFixture(t, fixture)
	t.Cleanup(fixture.host.releaseRunOne)
	waitOwnerTransfer(t, fixture.host.runOneBlocked)

	cancels := make([]context.CancelFunc, transferGroups)
	results := make([]chan error, transferGroups)
	for index := 0; index < transferGroups; index++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancels[index] = cancel
		results[index] = make(chan error, 1)
		index := index
		go func() {
			results[index] <- fixture.owner.TransferSplitSourceLeadership(
				ctx, fixture.fences[index], 2,
			)
		}()
		waitOwnerTransferIngressItems(t, fixture.owner, index+1)
	}

	preparedGroups := make(map[raftmember.GroupKey]struct{}, transferGroups)
	for len(preparedGroups) < transferGroups {
		fixture.host.runOneRelease <- struct{}{}
		preparedGroups[waitOwnerTransfer(t, fixture.host.prepareSeen)] = struct{}{}
		waitOwnerTransfer(t, fixture.host.runOneBlocked)
	}

	const survivorIndex = 3
	const tailIndex = 4
	if _, ok := preparedGroups[fixture.groups[survivorIndex]]; !ok {
		t.Fatalf("survivor transfer was not prepared")
	}
	if _, ok := preparedGroups[fixture.groups[tailIndex]]; !ok {
		t.Fatalf("tail transfer was not prepared")
	}
	readyRelease := make(chan struct{})
	var readyReleaseOnce sync.Once
	releaseReady := func() { readyReleaseOnce.Do(func() { close(readyRelease) }) }
	t.Cleanup(releaseReady)
	fixture.host.mu.Lock()
	fixture.host.readyBlocks[fixture.groups[survivorIndex]] = readyRelease
	fixture.host.mu.Unlock()

	for index := 0; index < survivorIndex; index++ {
		cancels[index]()
	}
	for index := 0; index < survivorIndex; index++ {
		if err := waitOwnerTransfer(t, results[index]); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled transfer %d = %v", index, err)
		}
	}
	fixture.host.runOneRelease <- struct{}{}
	canceledBeforeReady := make(map[raftmember.GroupKey]struct{}, survivorIndex)
	for index := 0; index < survivorIndex; index++ {
		group := waitOwnerTransfer(t, fixture.host.cancelSeen)
		if group != fixture.groups[0] && group != fixture.groups[1] && group != fixture.groups[2] {
			t.Fatalf("unexpected early cancellation for transfer group %v", group)
		}
		canceledBeforeReady[group] = struct{}{}
		waitOwnerTransfer(t, fixture.host.runOneBlocked)
		fixture.host.runOneRelease <- struct{}{}
	}
	if len(canceledBeforeReady) != survivorIndex {
		t.Fatalf("early cancellation groups = %d, want %d", len(canceledBeforeReady), survivorIndex)
	}
	waitOwnerTransferGroup(t, fixture.host.readyBlocked, fixture.groups[survivorIndex])

	appendIndex := transferGroups
	appendGroup := fixture.groups[appendIndex]
	prepareRelease := make(chan struct{})
	var prepareReleaseOnce sync.Once
	releasePrepare := func() { prepareReleaseOnce.Do(func() { close(prepareRelease) }) }
	t.Cleanup(releasePrepare)
	fixture.host.mu.Lock()
	fixture.host.busy = true
	fixture.host.prepareBlocks[appendGroup] = prepareRelease
	fixture.host.mu.Unlock()
	appendCtx, appendCancel := context.WithCancel(context.Background())
	appendResult := make(chan error, 1)
	go func() {
		appendResult <- fixture.owner.TransferSplitSourceLeadership(
			appendCtx, fixture.fences[appendIndex], 2,
		)
	}()
	// The canceled entries have already settled and released their charges; the
	// survivor, tail, and newly appended request are the three live entries.
	waitOwnerTransferIngressItems(t, fixture.owner, 3)
	releaseReady()
	waitOwnerTransfer(t, fixture.host.runOneBlocked)
	fixture.host.runOneRelease <- struct{}{}
	fixture.host.releaseRunOne()
	waitOwnerTransferGroup(t, fixture.host.prepareBlocked, appendGroup)
	fixture.host.mu.Lock()
	fixture.host.busy = false
	fixture.host.mu.Unlock()
	releasePrepare()
	waitOwnerTransferGroup(t, fixture.host.readySeen, appendGroup)

	for index := survivorIndex; index < transferGroups; index++ {
		cancels[index]()
	}
	appendCancel()
	canceledGroups := make(map[raftmember.GroupKey]struct{}, transferGroups+1)
	for index := 0; index < survivorIndex; index++ {
		canceledGroups[fixture.groups[index]] = struct{}{}
	}
	for len(canceledGroups) < transferGroups+1 {
		canceledGroups[waitOwnerTransfer(t, fixture.host.cancelSeen)] = struct{}{}
	}
	for index := survivorIndex; index < transferGroups; index++ {
		if err := waitOwnerTransfer(t, results[index]); !errors.Is(err, context.Canceled) {
			t.Fatalf("transfer %d cancellation = %v", index, err)
		}
	}
	if err := waitOwnerTransfer(t, appendResult); !errors.Is(err, context.Canceled) {
		t.Fatalf("appended transfer cancellation = %v", err)
	}
	fixture.host.mu.Lock()
	transferCalls := make(map[raftmember.GroupKey]int, len(fixture.host.transferCalls))
	cancelCalls := make(map[raftmember.GroupKey]int, len(fixture.host.cancelCalls))
	for group, calls := range fixture.host.transferCalls {
		transferCalls[group] = calls
	}
	for group, calls := range fixture.host.cancelCalls {
		cancelCalls[group] = calls
	}
	fixture.host.mu.Unlock()
	for index := 0; index <= appendIndex; index++ {
		group := fixture.groups[index]
		if _, ok := canceledGroups[group]; !ok || cancelCalls[group] != 1 {
			t.Fatalf("transfer group %v cancel calls = %d", group, cancelCalls[group])
		}
		if transferCalls[group] != 0 {
			t.Fatalf("canceled transfer group %v reached Host %d times", group, transferCalls[group])
		}
	}
	assertOwnerTransferIngressEmpty(t, fixture.owner)
}

func TestOwnerLeaderTransferCancellationAfterDispatchReturnsAdmittedResult(t *testing.T) {
	fixture := newOwnerTransferFixture(1, false)
	fixture.host.readyErr = nil
	fixture.host.transferRelease = make(chan struct{})
	startOwnerTransferFixture(t, fixture)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- fixture.owner.TransferSplitSourceLeadership(ctx, fixture.fences[0], 2)
	}()
	waitOwnerTransfer(t, fixture.host.transferSeen)
	cancel()
	select {
	case err := <-result:
		t.Fatalf("dispatch loser returned before Host admission completed: %v", err)
	default:
	}
	close(fixture.host.transferRelease)
	if err := waitOwnerTransfer(t, result); err != nil {
		t.Fatalf("admitted transfer result = %v", err)
	}
	if len(fixture.host.cancelSeen) != 0 {
		t.Fatal("successful admitted transfer was canceled")
	}
	assertOwnerTransferIngressEmpty(t, fixture.owner)
}

func TestOwnerLeaderTransferRevocationDuringWaitPreventsDispatch(t *testing.T) {
	t.Run("grant", func(t *testing.T) {
		fixture := newOwnerTransferFixture(1, true)
		fixture.host.readyErr = raftmodel.ErrReadyPending
		startOwnerTransferFixture(t, fixture)
		result := make(chan error, 1)
		go func() {
			result <- fixture.owner.ApplyMembership(context.Background(), MembershipRequest{
				Fence: fixture.fences[0], Kind: MembershipTransferLeader,
				TransitionID: [16]byte{1}, MetadataEpoch: 1, CatalogGeneration: 1,
				ExpectedReplicaSetVersion: 7, SourceMember: 1, TargetMember: 2,
			})
		}()
		waitOwnerTransfer(t, fixture.host.readySeen)
		fixture.authority.revoke()
		fixture.host.mu.Lock()
		fixture.host.readyErr = nil
		fixture.host.mu.Unlock()
		fixture.pulse <- struct{}{}
		if err := waitOwnerTransfer(t, result); !errors.Is(err, ErrMembershipUnauthorized) {
			t.Fatalf("revoked grant result = %v", err)
		}
		waitOwnerTransfer(t, fixture.host.cancelSeen)
		if len(fixture.host.transferSeen) != 0 {
			t.Fatal("revoked grant reached Host admission")
		}
		assertOwnerTransferIngressEmpty(t, fixture.owner)
	})

	t.Run("fence", func(t *testing.T) {
		fixture := newOwnerTransferFixture(1, false)
		fixture.host.readyErr = raftmodel.ErrReadyPending
		startOwnerTransferFixture(t, fixture)
		result := make(chan error, 1)
		go func() {
			result <- fixture.owner.TransferSplitSourceLeadership(
				context.Background(), fixture.fences[0], 2,
			)
		}()
		waitOwnerTransfer(t, fixture.host.readySeen)
		fixture.host.mu.Lock()
		status := fixture.host.statuses[fixture.groups[0]]
		status.LeaderID = 3
		fixture.host.statuses[fixture.groups[0]] = status
		fixture.host.readyErr = nil
		fixture.host.mu.Unlock()
		fixture.pulse <- struct{}{}
		err := waitOwnerTransfer(t, result)
		if !errors.Is(err, raftmodel.ErrNotLeader) {
			t.Fatalf("revoked fence result = %v, want NotLeaderError", err)
		}
		waitOwnerTransfer(t, fixture.host.cancelSeen)
		if len(fixture.host.transferSeen) != 0 {
			t.Fatal("revoked fence reached Host admission")
		}
		assertOwnerTransferIngressEmpty(t, fixture.owner)
	})
}

func TestOwnerLeaderTransferFairnessRetainsReadsTicksAndInbound(t *testing.T) {
	const transferGroups = 8
	fixture := newOwnerTransferFixture(transferGroups+1, false)
	fixture.host.busy = true
	fixture.host.busyGroup = fixture.groups[transferGroups]
	fixture.host.readyErr = raftmodel.ErrReadyPending
	startOwnerTransferFixture(t, fixture)

	cancels := make([]context.CancelFunc, transferGroups)
	results := make([]chan error, transferGroups)
	for index := 0; index < transferGroups; index++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancels[index] = cancel
		results[index] = make(chan error, 1)
		index := index
		go func() {
			results[index] <- fixture.owner.TransferSplitSourceLeadership(
				ctx, fixture.fences[index], 2,
			)
		}()
		waitOwnerTransfer(t, fixture.host.prepareSeen)
	}
	readyGroups := make(map[raftmember.GroupKey]struct{}, transferGroups)
	for len(readyGroups) < transferGroups {
		readyGroups[waitOwnerTransfer(t, fixture.host.readySeen)] = struct{}{}
	}

	inbound := rafttransport.Inbound{Group: fixture.groups[0], Message: ownerTransferInboundMessage()}
	inboundResult := make(chan error, 1)
	go func() { inboundResult <- fixture.owner.HandleInbound(context.Background(), inbound) }()
	fixture.pulse <- struct{}{}
	readResult := make(chan error, 1)
	var cut LinearizablePointReadCut
	go func() {
		readResult <- fixture.owner.ReadLinearizablePointInto(context.Background(),
			LinearizablePointReadRequest{Fence: fixture.fences[0],
				Capability: serviceauthz.CapabilityDataRead}, &cut)
	}()
	waitOwnerTransfer(t, fixture.host.inboundSeen)
	waitOwnerTransfer(t, fixture.host.tickSeen)
	waitOwnerTransfer(t, fixture.host.readSeen)
	if err := waitOwnerTransfer(t, inboundResult); err != nil {
		t.Fatalf("inbound progress = %v", err)
	}
	if err := waitOwnerTransfer(t, readResult); err != nil {
		t.Fatalf("read progress = %v", err)
	}
	if cut.Source() == nil {
		t.Fatal("read did not receive a live source")
	}
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}

	for _, cancel := range cancels {
		cancel()
	}
	canceledGroups := make(map[raftmember.GroupKey]struct{}, transferGroups)
	for len(canceledGroups) < transferGroups {
		canceledGroups[waitOwnerTransfer(t, fixture.host.cancelSeen)] = struct{}{}
	}
	for index := 0; index < transferGroups; index++ {
		if err := waitOwnerTransfer(t, results[index]); !errors.Is(err, context.Canceled) {
			t.Fatalf("transfer %d cancellation = %v", index, err)
		}
	}
	fixture.host.mu.Lock()
	maxVisits := fixture.host.maxReadyChecksTurn
	readyCalls := make(map[raftmember.GroupKey]int, len(fixture.host.readyCalls))
	for group, calls := range fixture.host.readyCalls {
		readyCalls[group] = calls
	}
	cancelCalls := make(map[raftmember.GroupKey]int, len(fixture.host.cancelCalls))
	transferCalls := make(map[raftmember.GroupKey]int, len(fixture.host.transferCalls))
	for group, calls := range fixture.host.cancelCalls {
		cancelCalls[group] = calls
	}
	for group, calls := range fixture.host.transferCalls {
		transferCalls[group] = calls
	}
	fixture.host.mu.Unlock()
	if maxVisits > 2 {
		t.Fatalf("transfer readiness visits monopolized one Host turn: max=%d groups=%d", maxVisits, transferGroups)
	}
	for index := 0; index < transferGroups; index++ {
		group := fixture.groups[index]
		if readyCalls[group] == 0 {
			t.Fatalf("transfer group %v was never revisited", group)
		}
		if _, ok := canceledGroups[group]; !ok || cancelCalls[group] != 1 {
			t.Fatalf("transfer group %v cancel calls = %d", group, cancelCalls[group])
		}
		if transferCalls[group] != 0 {
			t.Fatalf("canceled transfer group %v reached Host %d times", group, transferCalls[group])
		}
	}
	assertOwnerTransferIngressEmpty(t, fixture.owner)
}

func TestOwnerLeaderTransferRetainsPendingSettlementUntilAdmission(t *testing.T) {
	fixture := newOwnerTransferFixture(1, false)
	fixture.host.prepareErr = raftmember.ErrResultSettlementPending
	startOwnerTransferFixture(t, fixture)
	result := make(chan error, 1)
	go func() {
		result <- fixture.owner.TransferSplitSourceLeadership(
			context.Background(), fixture.fences[0], 2,
		)
	}()
	waitOwnerTransfer(t, fixture.host.prepareSeen)
	select {
	case err := <-result:
		t.Fatalf("transient settlement refusal was surfaced: %v", err)
	default:
	}
	if len(fixture.host.transferSeen) != 0 {
		t.Fatal("settlement-pending transfer reached Host admission")
	}
	fixture.host.mu.Lock()
	fixture.host.prepareErr = nil
	fixture.host.readyErr = nil
	fixture.host.mu.Unlock()
	fixture.pulse <- struct{}{}
	waitOwnerTransfer(t, fixture.host.transferSeen)
	if err := waitOwnerTransfer(t, result); err != nil {
		t.Fatalf("retained transfer result = %v", err)
	}
	assertOwnerTransferIngressEmpty(t, fixture.owner)
}
