package raftstore

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	pb "go.etcd.io/raft/v3/raftpb"
)

func newTestSequencer(t *testing.T, capacity int, persist func([]NodeReady) error) *NodeSubmissionSequencer {
	t.Helper()
	q, err := NewNodeSubmissionSequencer(&NodeStore{bounds: nodeStoreBounds{
		maxWaveBytes: DefaultNodeMaxWaveBytes, maxSegmentEvents: DefaultNodeMaxSegmentEvents,
		maxEntriesPerGroup: DefaultNodeMaxEntriesPerGroup,
	}}, capacity)
	if err != nil {
		t.Fatal(err)
	}
	q.persist = persist
	t.Cleanup(func() { _ = q.Close() })
	return q
}

func TestNodeSubmissionSequencerBoundsAndCacheLineLayout(t *testing.T) {
	if unsafe.Sizeof(submissionRingIndex{}) != 64 || unsafe.Sizeof(submissionRingSlot{}) != 64 {
		t.Fatalf("index=%d slot=%d", unsafe.Sizeof(submissionRingIndex{}), unsafe.Sizeof(submissionRingSlot{}))
	}
	for _, capacity := range []int{0, 1, 3, (1 << 20) + 1} {
		if _, err := NewNodeSubmissionSequencer(&NodeStore{}, capacity); !errors.Is(err, ErrBounds) {
			t.Fatalf("capacity %d=%v", capacity, err)
		}
	}
	store := &NodeStore{}
	q, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	if !q.Owns(store) || q.Owns(&NodeStore{}) {
		t.Fatal("sequencer accepted the wrong NodeStore owner")
	}
	if _, err = NewNodeSubmissionSequencer(store, 8); !errors.Is(err, ErrInvalid) {
		t.Fatalf("second node-path sequencer=%v", err)
	}
	if err = store.PersistWave([]NodeReady{{GroupID: 1}}); !errors.Is(err, ErrSequencerActive) {
		t.Fatalf("direct Ready bypass=%v", err)
	}
	if _, err = store.BeginIncarnations([]uint64{1}); !errors.Is(err, ErrSequencerActive) {
		t.Fatalf("direct incarnation allocation bypass=%v", err)
	}
	if err = store.PersistIncarnations([]GroupIncarnation{{GroupID: 1, Incarnation: 1}}); !errors.Is(err, ErrSequencerActive) {
		t.Fatalf("direct incarnation retry bypass=%v", err)
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	zero := &NodeStore{}
	if err = zero.Close(); err != nil {
		t.Fatalf("zero-value close=%v", err)
	}
	if err = zero.Close(); err != nil {
		t.Fatalf("zero-value second close=%v", err)
	}
	closing := &NodeStore{}
	closing.closingFlag.Store(true)
	if _, err = NewNodeSubmissionSequencer(closing, 8); !errors.Is(err, ErrClosed) {
		t.Fatalf("sequencer registered after close linearization: %v", err)
	}
}

func TestNodeSubmissionSequencerRejectsUnpreparedAndFailedPrepare(t *testing.T) {
	var persisted atomic.Int32
	q := newTestSequencer(t, 8, func([]NodeReady) error {
		persisted.Add(1)
		return nil
	})
	var cell Submission
	if err := cell.Initialize(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.TrySubmit(&cell); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unprepared submission=%v", err)
	}
	if err := cell.Prepare(NodeReady{GroupID: 1, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.TrySubmit(&cell); err != nil {
		t.Fatal(err)
	}
	if _, err := cell.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := cell.PrepareBeginIncarnations([]uint64{0}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid prepare=%v", err)
	}
	if _, done, _ := cell.Poll(); done {
		t.Fatal("failed prepare exposed stale completion")
	}
	if _, err := q.TrySubmit(&cell); !errors.Is(err, ErrInvalid) {
		t.Fatalf("failed prepare submission=%v", err)
	}
	if got := persisted.Load(); got != 1 {
		t.Fatalf("persisted=%d want=1", got)
	}
}

func preparedSubmission(t *testing.T, group, readyID uint64) *Submission {
	t.Helper()
	s := new(Submission)
	if err := s.Initialize(); err != nil {
		t.Fatal(err)
	}
	if err := s.Prepare(NodeReady{GroupID: group, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: readyID}}); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestNodeSubmissionSequencerFusesFIFOAndCompletesInTicketOrder(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var sizesMu sync.Mutex
	var sizes []int
	q := newTestSequencer(t, 128, func(ready []NodeReady) error {
		call := calls.Add(1)
		if call == 1 {
			close(entered)
			<-release
		}
		sizesMu.Lock()
		sizes = append(sizes, len(ready))
		sizesMu.Unlock()
		return nil
	})
	const producers = 64
	submissions := make([]*Submission, producers)
	tickets := make([]uint64, producers)
	for i := range submissions {
		submissions[i] = preparedSubmission(t, uint64(i+1), 1)
		ticket, err := q.TrySubmit(submissions[i])
		if err != nil {
			t.Fatal(err)
		}
		tickets[i] = ticket
		if i == 0 {
			<-entered
		}
	}
	close(release)
	for i := range submissions {
		ticket, err := submissions[i].Wait()
		if err != nil || ticket != tickets[i] || i > 0 && ticket != tickets[i-1]+1 {
			t.Fatalf("completion %d ticket=%d want=%d err=%v", i, ticket, tickets[i], err)
		}
	}
	sizesMu.Lock()
	defer sizesMu.Unlock()
	if len(sizes) != 5 || sizes[0] != 1 || sizes[1] != MaxPersistGroupBatches || sizes[2] != MaxPersistGroupBatches || sizes[3] != MaxPersistGroupBatches || sizes[4] != 15 {
		t.Fatalf("fused wave sizes=%v", sizes)
	}
}

func TestNodeSubmissionSequencerSplitsWaveBeforeArenaCapacity(t *testing.T) {
	store := &NodeStore{bounds: nodeStoreBounds{
		maxWaveBytes: 1024, maxSegmentEvents: 64, maxEntriesPerGroup: 8,
	}}
	q, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var sizesMu sync.Mutex
	var sizes []int
	q.persist = func(ready []NodeReady) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		sizesMu.Lock()
		sizes = append(sizes, len(ready))
		sizesMu.Unlock()
		return nil
	}

	first := preparedSubmission(t, 1, 1)
	if _, err = q.TrySubmit(first); err != nil {
		t.Fatal(err)
	}
	<-entered
	queued := []*Submission{preparedSubmission(t, 2, 1), preparedSubmission(t, 3, 1)}
	for _, item := range queued {
		item.Ready.Batch.Entries = []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, string(make([]byte, 500)))}
		if _, err = q.TrySubmit(item); err != nil {
			t.Fatal(err)
		}
	}
	close(release)
	for _, item := range append([]*Submission{first}, queued...) {
		if _, err = item.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	sizesMu.Lock()
	defer sizesMu.Unlock()
	if fmt.Sprint(sizes) != "[1 1 1]" {
		t.Fatalf("capacity-aware durability waves=%v want [1 1 1]", sizes)
	}
}

func TestNodeSubmissionSequencerRejectsImpossibleReadyBeforeTicket(t *testing.T) {
	store := &NodeStore{bounds: nodeStoreBounds{
		maxWaveBytes: 1024, maxSegmentEvents: 7, maxEntriesPerGroup: 8,
	}}
	q, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	var calls atomic.Int32
	q.persist = func([]NodeReady) error {
		calls.Add(1)
		return nil
	}

	tooLarge := preparedSubmission(t, 1, 1)
	tooLarge.Ready.Batch.Entries = []*pb.Entry{
		typedEntry(2, 2, pb.EntryNormal, string(make([]byte, 800))),
	}
	if ticket, submitErr := q.TrySubmit(tooLarge); ticket != 0 || !errors.Is(submitErr, ErrBounds) {
		t.Fatalf("oversize submit ticket=%d err=%v", ticket, submitErr)
	}
	if tooLarge.state.Load() != submissionIdle {
		t.Fatalf("oversize submission state=%d want idle", tooLarge.state.Load())
	}

	tooManyEvents := preparedSubmission(t, 1, 1)
	tooManyEvents.Ready.Batch.Entries = []*pb.Entry{
		typedEntry(2, 2, pb.EntryNormal, "a"), typedEntry(3, 2, pb.EntryNormal, "b"),
	}
	if ticket, submitErr := q.TrySubmit(tooManyEvents); ticket != 0 || !errors.Is(submitErr, ErrBounds) {
		t.Fatalf("event-heavy submit ticket=%d err=%v", ticket, submitErr)
	}
	if calls.Load() != 0 {
		t.Fatalf("impossible Ready reached persistence %d times", calls.Load())
	}
}

func TestNodeSubmissionSequencerMPSCConcurrentTickets(t *testing.T) {
	q := newTestSequencer(t, 128, func([]NodeReady) error { return nil })
	const producers = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	tickets := make([]uint64, producers)
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cell := preparedSubmission(t, uint64(i+1), 1)
			<-start
			ticket, err := q.TrySubmit(cell)
			if err != nil {
				t.Errorf("submit %d: %v", i, err)
				return
			}
			tickets[i] = ticket
			if completed, waitErr := cell.Wait(); waitErr != nil || completed != ticket {
				t.Errorf("complete %d: ticket=%d err=%v", i, completed, waitErr)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	sort.Slice(tickets, func(i, j int) bool { return tickets[i] < tickets[j] })
	for i, ticket := range tickets {
		if ticket != uint64(i+1) {
			t.Fatalf("tickets=%v", tickets)
		}
	}
}

func TestNodeSubmissionSequencerFusesRealNodeStoreEngineCalls(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 256, RecentWaves: 64, MaxEntriesPerGroup: 64, ReaderSlots: 1}
	bootstraps := make([]NodeBootstrap, 8)
	for i := range bootstraps {
		group := uint64(i + 1)
		bootstraps[i] = NodeBootstrap{Descriptor: testGroupDescriptor(group), Snapshot: nodeSnapshot(group, 1, 1)}
	}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), bootstraps, options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	groups := make([]uint64, len(bootstraps))
	for i := range groups {
		groups[i] = uint64(i + 1)
	}
	if _, err = store.BeginIncarnations(groups); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var engineCalls atomic.Int32
	var syncCalls atomic.Int32
	store.engine.SetDataSyncForTesting(func(file *os.File) error {
		syncCalls.Add(1)
		return file.Sync()
	})
	store.persistWaveTest = func(wave seglog.Wave) error {
		if engineCalls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return store.engine.PersistWave(wave)
	}
	q, err := NewNodeSubmissionSequencer(store, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	items := make([]*Submission, len(bootstraps))
	for i := range items {
		group := uint64(i + 1)
		entry := typedEntry(2, 2, pb.EntryNormal, string([]byte{byte(group)}))
		hard := hard(2, 2)
		items[i] = preparedSubmission(t, group, 1)
		items[i].Ready.Batch.Entries = []*pb.Entry{entry}
		items[i].Ready.Batch.HardState = hard
		if _, err = q.TrySubmit(items[i]); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			<-entered
		}
	}
	close(release)
	for _, item := range items {
		if _, err = item.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	if engineCalls.Load() != 2 {
		t.Fatalf("engine durability waves=%d want 2 (one singleton + one fused seven-group wave)", engineCalls.Load())
	}
	if syncCalls.Load() != 2 {
		t.Fatalf("data durability syncs=%d want 2 (exactly one per Engine wave)", syncCalls.Load())
	}
}

func TestNodeSubmissionSequencerOrdersIncarnationControlAsWaveBoundary(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 128, RecentWaves: 32, MaxEntriesPerGroup: 32, ReaderSlots: 1}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{
		{Descriptor: testGroupDescriptor(1), Snapshot: nodeSnapshot(1, 1, 1)},
		{Descriptor: testGroupDescriptor(2), Snapshot: nodeSnapshot(2, 1, 1)},
	}, options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var syncs atomic.Int32
	store.engine.SetDataSyncForTesting(func(file *os.File) error {
		syncs.Add(1)
		return file.Sync()
	})
	q, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	control := new(Submission)
	if err = control.Initialize(); err != nil {
		t.Fatal(err)
	}
	if err = control.PrepareBeginIncarnations([]uint64{1, 2}); err != nil {
		t.Fatal(err)
	}
	if _, err = q.TrySubmit(control); err != nil {
		t.Fatal(err)
	}
	if _, err = control.Wait(); err != nil {
		t.Fatal(err)
	}
	incarnations := control.Incarnations()
	if len(incarnations) != 2 || incarnations[0] != (GroupIncarnation{GroupID: 1, Incarnation: 1}) ||
		incarnations[1] != (GroupIncarnation{GroupID: 2, Incarnation: 1}) {
		t.Fatalf("incarnations=%v", incarnations)
	}
	if err = control.PreparePersistIncarnations(incarnations); err != nil {
		t.Fatal(err)
	}
	if _, err = q.TrySubmit(control); err != nil {
		t.Fatal(err)
	}
	if _, err = control.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("idempotent control retry performed sync: %d", got)
	}
	ready := preparedSubmission(t, 1, 1)
	ready.Ready.Batch.Entries = []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "x")}
	ready.Ready.Batch.HardState = hard(2, 2)
	if _, err = q.TrySubmit(ready); err != nil {
		t.Fatal(err)
	}
	if _, err = ready.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := syncs.Load(); got != 2 {
		t.Fatalf("control+Ready durability syncs=%d want 2", got)
	}
}

func TestNodeSubmissionSequencerRegistersExactGroupInTicketStream(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 128, RecentWaves: 32, MaxEntriesPerGroup: 32, ReaderSlots: 1, MaxGroups: 8}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(20), Snapshot: nodeSnapshot(1, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	sequencer, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	var registration Submission
	if err = registration.Initialize(); err != nil {
		t.Fatal(err)
	}
	descriptor := testGroupDescriptor(10)
	if err = registration.PrepareRegisterGroup(descriptor); err != nil {
		t.Fatal(err)
	}
	if _, err = sequencer.TrySubmit(&registration); err != nil {
		t.Fatal(err)
	}
	if _, err = registration.Wait(); err != nil {
		t.Fatal(err)
	}
	got, incarnation, ok := registration.RegisteredGroup()
	if !ok || got.LogKey != 2 || got.GroupID != descriptor.GroupID || incarnation != (GroupIncarnation{GroupID: 2, Incarnation: 1}) {
		t.Fatalf("registered=%+v incarnation=%+v ok=%v", got, incarnation, ok)
	}
	if _, err = store.RegisterGroup(testGroupDescriptor(30)); !errors.Is(err, ErrSequencerActive) {
		t.Fatalf("direct registration fence=%v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSubmissionControlPrepareZeroAlloc(t *testing.T) {
	cell := new(Submission)
	if err := cell.Initialize(); err != nil {
		t.Fatal(err)
	}
	groups := []uint64{1, 2, 3, 4}
	requests := []GroupIncarnation{{1, 1}, {2, 1}, {3, 1}, {4, 1}}
	descriptor := testGroupDescriptor(9)
	allocs := testing.AllocsPerRun(1000, func() {
		if err := cell.PrepareBeginIncarnations(groups); err != nil {
			panic(err)
		}
		if err := cell.PreparePersistIncarnations(requests); err != nil {
			panic(err)
		}
		if err := cell.PrepareRegisterGroup(descriptor); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("control prepare allocations=%f", allocs)
	}
}

func TestNodeStoreCloseDrainsSequencerAndRejectsDirectCalls(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 1}
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(1), Snapshot: nodeSnapshot(1, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.BeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	store.persistWaveTest = func(wave seglog.Wave) error {
		close(entered)
		<-release
		return store.engine.PersistWave(wave)
	}
	q, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	cell := preparedSubmission(t, 1, 1)
	cell.Ready.Batch.Entries = []*pb.Entry{typedEntry(2, 2, pb.EntryNormal, "x")}
	cell.Ready.Batch.HardState = hard(2, 2)
	if _, err = q.TrySubmit(cell); err != nil {
		t.Fatal(err)
	}
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- store.Close() }()
	for !store.closingFlag.Load() {
		time.Sleep(time.Millisecond)
	}
	if err = store.PersistWave([]NodeReady{{GroupID: 1, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 2}}}); !errors.Is(err, ErrClosed) {
		t.Fatalf("direct persist during close=%v", err)
	}
	close(release)
	if _, err = cell.Wait(); err != nil {
		t.Fatalf("accepted completion=%v", err)
	}
	if err = <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestNodeSubmissionSequencerDuplicateGroupIsWaveBoundary(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var groups [][]uint64
	q := newTestSequencer(t, 8, func(ready []NodeReady) error {
		if len(groups) == 0 {
			close(entered)
			<-release
		}
		got := make([]uint64, len(ready))
		for i := range ready {
			got[i] = ready[i].GroupID
		}
		mu.Lock()
		groups = append(groups, got)
		mu.Unlock()
		return nil
	})
	items := []*Submission{preparedSubmission(t, 1, 1), preparedSubmission(t, 1, 2), preparedSubmission(t, 2, 1)}
	if _, err := q.TrySubmit(items[0]); err != nil {
		t.Fatal(err)
	}
	<-entered
	for _, item := range items[1:] {
		if _, err := q.TrySubmit(item); err != nil {
			t.Fatal(err)
		}
	}
	close(release)
	for _, item := range items {
		if _, err := item.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(groups) != 2 || len(groups[0]) != 1 || len(groups[1]) != 2 || groups[1][0] != 1 || groups[1][1] != 2 {
		t.Fatalf("waves=%v", groups)
	}
}

func TestNodeSubmissionSequencerBackpressureAndCloseDrain(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	q := newTestSequencer(t, 2, func([]NodeReady) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return nil
	})
	active := preparedSubmission(t, 1, 1)
	if _, err := q.TrySubmit(active); err != nil {
		t.Fatal(err)
	}
	<-entered
	queued := []*Submission{preparedSubmission(t, 2, 1), preparedSubmission(t, 3, 1)}
	for _, item := range queued {
		if _, err := q.TrySubmit(item); err != nil {
			t.Fatal(err)
		}
	}
	overflow := preparedSubmission(t, 4, 1)
	if _, err := q.TrySubmit(overflow); !errors.Is(err, ErrSubmissionBackpressure) {
		t.Fatalf("overflow=%v", err)
	}
	closed := make(chan error, 1)
	go func() { closed <- q.Close() }()
	for !q.closed.Load() {
		time.Sleep(time.Millisecond)
	}
	if _, err := q.TrySubmit(overflow); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close submit=%v", err)
	}
	close(release)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	for _, item := range append([]*Submission{active}, queued...) {
		if _, err := item.Wait(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNodeSubmissionSequencerPanicPoisonsAndCompletesAccepted(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	q := newTestSequencer(t, 8, func([]NodeReady) error {
		close(entered)
		<-release
		panic("injected")
	})
	first := preparedSubmission(t, 1, 1)
	if _, err := q.TrySubmit(first); err != nil {
		t.Fatal(err)
	}
	<-entered
	second := preparedSubmission(t, 2, 1)
	if _, err := q.TrySubmit(second); err != nil {
		t.Fatal(err)
	}
	close(release)
	for _, item := range []*Submission{first, second} {
		if _, err := item.Wait(); !errors.Is(err, ErrPersistenceUnknown) || !errors.Is(err, ErrSubmissionPanic) {
			t.Fatalf("panic completion=%v", err)
		}
	}
	rejected := preparedSubmission(t, 3, 1)
	if _, err := q.TrySubmit(rejected); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("post-fatal submit=%v", err)
	}
	if err := q.Close(); !errors.Is(err, ErrSubmissionPanic) {
		t.Fatalf("close=%v", err)
	}
}

func TestNodeStoreCloseReturnsFatalSequencerErrorToEveryCaller(t *testing.T) {
	store := &NodeStore{}
	q, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	q.persist = func([]NodeReady) error { return ErrPersistenceUnknown }
	cell := preparedSubmission(t, 1, 1)
	if _, err = q.TrySubmit(cell); err != nil {
		t.Fatal(err)
	}
	if _, err = cell.Wait(); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatal(err)
	}
	first := store.Close()
	second := store.Close()
	if !errors.Is(first, ErrPersistenceUnknown) || !errors.Is(second, ErrPersistenceUnknown) {
		t.Fatalf("first=%v second=%v", first, second)
	}
}

func TestNodeSubmissionSequencerFatalWaitsForClaimedProducer(t *testing.T) {
	entered, releasePersist := make(chan struct{}), make(chan struct{})
	q := newTestSequencer(t, 8, func([]NodeReady) error {
		close(entered)
		<-releasePersist
		return ErrPersistenceUnknown
	})
	first := preparedSubmission(t, 1, 1)
	if _, err := q.TrySubmit(first); err != nil {
		t.Fatal(err)
	}
	<-entered
	claimed, releaseClaim := make(chan struct{}), make(chan struct{})
	q.claimedHookTest = func() {
		close(claimed)
		<-releaseClaim
	}
	second := preparedSubmission(t, 2, 1)
	ticketResult := make(chan error, 1)
	go func() {
		_, err := q.TrySubmit(second)
		ticketResult <- err
	}()
	<-claimed
	close(releasePersist)
	select {
	case <-q.done:
		t.Fatal("worker exited before claimed producer published")
	case <-time.After(10 * time.Millisecond):
	}
	close(releaseClaim)
	if err := <-ticketResult; err != nil {
		t.Fatal(err)
	}
	if _, err := second.Wait(); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("claimed completion=%v", err)
	}
	if err := q.Close(); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("close=%v", err)
	}
}

func TestNodeSubmissionSequencerDefiniteErrorDoesNotPoison(t *testing.T) {
	var calls atomic.Int32
	q := newTestSequencer(t, 8, func([]NodeReady) error {
		if calls.Add(1) == 1 {
			return syscall.EINVAL
		}
		return nil
	})
	first := preparedSubmission(t, 1, 1)
	if _, err := q.TrySubmit(first); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Wait(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("first=%v", err)
	}
	second := preparedSubmission(t, 2, 1)
	if _, err := q.TrySubmit(second); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Wait(); err != nil {
		t.Fatalf("second=%v", err)
	}
}

func TestNodeSubmissionSequencerSequenceWrap(t *testing.T) {
	q := newTestSequencer(t, 8, func([]NodeReady) error { return nil })
	base := uint64(math.MaxUint64 - 2)
	q.head.value.Store(base)
	q.tail.value.Store(base)
	for i := uint64(0); i < uint64(len(q.ring)); i++ {
		q.ring[(base+i)&q.mask].sequence.Store(base + i)
	}
	for i := uint64(0); i < 4; i++ {
		s := preparedSubmission(t, i+1, 1)
		ticket, err := q.TrySubmit(s)
		if err != nil {
			t.Fatal(err)
		}
		if ticket != base+i+1 {
			t.Fatalf("ticket=%d want=%d", ticket, base+i+1)
		}
		if got, err := s.Wait(); err != nil || got != ticket {
			t.Fatalf("completion=%d/%v", got, err)
		}
	}
}

func TestSubmissionWaitRegistersBeforeCompletionAndCannotLeaveStaleToken(t *testing.T) {
	enteredPersist, releasePersist := make(chan struct{}), make(chan struct{})
	var persists atomic.Int32
	q := newTestSequencer(t, 8, func([]NodeReady) error {
		if persists.Add(1) == 1 {
			close(enteredPersist)
			<-releasePersist
		}
		return nil
	})
	completionSwapped, releaseCompletion := make(chan struct{}), make(chan struct{})
	q.completeHookTest = func(_ *Submission, previous uint32) {
		if previous != submissionWaiting {
			panic(fmt.Sprintf("completion displaced state %d, want waiting", previous))
		}
		close(completionSwapped)
		<-releaseCompletion
	}
	cell := preparedSubmission(t, 1, 1)
	if _, err := q.TrySubmit(cell); err != nil {
		t.Fatal(err)
	}
	<-enteredPersist
	waitResult := make(chan error, 1)
	go func() {
		_, err := cell.Wait()
		waitResult <- err
	}()
	deadline := time.Now().Add(time.Second)
	for cell.state.Load() != submissionWaiting {
		if time.Now().After(deadline) {
			t.Fatal("Wait did not register")
		}
	}
	if _, err := cell.Wait(); !errors.Is(err, ErrSubmissionPending) {
		t.Fatalf("second concurrent Wait=%v", err)
	}
	close(releasePersist)
	<-completionSwapped
	select {
	case err := <-waitResult:
		t.Fatalf("Wait returned before its registered notification: %v", err)
	default:
	}
	close(releaseCompletion)
	if err := <-waitResult; err != nil {
		t.Fatal(err)
	}
	if got := len(cell.done); got != 0 {
		t.Fatalf("stale completion tokens=%d", got)
	}

	q.completeHookTest = nil
	if err := cell.Prepare(NodeReady{GroupID: 1, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 2}}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.TrySubmit(cell); err != nil {
		t.Fatal(err)
	}
	if _, err := cell.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := len(cell.done); got != 0 {
		t.Fatalf("reused completion tokens=%d", got)
	}
}

func TestSubmissionCompletionBeforeWaitNeedsNoToken(t *testing.T) {
	completed := make(chan struct{})
	q := newTestSequencer(t, 8, func([]NodeReady) error { return nil })
	q.completeHookTest = func(_ *Submission, previous uint32) {
		if previous != submissionQueued {
			panic(fmt.Sprintf("completion displaced state %d, want queued", previous))
		}
		close(completed)
	}
	cell := preparedSubmission(t, 1, 1)
	if _, err := q.TrySubmit(cell); err != nil {
		t.Fatal(err)
	}
	<-completed
	if got := len(cell.done); got != 0 {
		t.Fatalf("unregistered completion token=%d", got)
	}
	if _, err := cell.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestNodeSubmissionSequencerSustainedMPSCProgress(t *testing.T) {
	q := newTestSequencer(t, 1024, func([]NodeReady) error { return nil })
	const producers, perProducer = 64, 2000
	start := make(chan struct{})
	done := make(chan struct{})
	var workers sync.WaitGroup
	var completed atomic.Uint64
	for producer := 0; producer < producers; producer++ {
		workers.Add(1)
		go func(group uint64) {
			defer workers.Done()
			cell := new(Submission)
			if err := cell.Initialize(); err != nil {
				panic(err)
			}
			<-start
			for readyID := uint64(1); readyID <= perProducer; readyID++ {
				if err := cell.Prepare(NodeReady{GroupID: group, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: readyID}}); err != nil {
					panic(err)
				}
				for {
					if _, err := q.TrySubmit(cell); err == nil {
						break
					} else if !errors.Is(err, ErrSubmissionBackpressure) {
						panic(err)
					}
				}
				if _, err := cell.Wait(); err != nil {
					panic(err)
				}
				completed.Add(1)
			}
		}(uint64(producer + 1))
	}
	go func() {
		workers.Wait()
		close(done)
	}()
	close(start)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		head, tail := q.head.value.Load(), q.tail.value.Load()
		slot := &q.ring[head&q.mask]
		sequence := slot.sequence.Load()
		value := slot.value
		var state uint32
		var ticket uint64
		if value != nil {
			state, ticket = value.state.Load(), value.ticket.Load()
		}
		panic(fmt.Sprintf("sequencer stalled: completed=%d head=%d tail=%d headSlot.sequence=%d headSlot.value=%p submission.state=%d submission.ticket=%d wake=%d submitters=%d closed=%v",
			completed.Load(), head, tail, sequence, value, state, ticket, len(q.wake), q.submitters.Load(), q.closed.Load()))
	}
	if got := completed.Load(); got != producers*perProducer {
		t.Fatalf("completed=%d", got)
	}
}

func TestNodeSubmissionSequencerSubmitWaitZeroAlloc(t *testing.T) {
	q := newTestSequencer(t, 8, func([]NodeReady) error { return nil })
	cell := preparedSubmission(t, 1, 1)
	allocs := testing.AllocsPerRun(1000, func() {
		if err := cell.Prepare(NodeReady{GroupID: 1, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1}}); err != nil {
			panic(err)
		}
		if _, err := q.TrySubmit(cell); err != nil {
			panic(err)
		}
		if _, err := cell.Wait(); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("submit+wait allocations=%f", allocs)
	}
}

func BenchmarkNodeSubmissionSequencer(b *testing.B) {
	for _, producers := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("producers=%d", producers), func(b *testing.B) {
			b.StopTimer()
			q, err := NewNodeSubmissionSequencer(&NodeStore{bounds: nodeStoreBounds{
				maxWaveBytes: DefaultNodeMaxWaveBytes, maxSegmentEvents: DefaultNodeMaxSegmentEvents,
				maxEntriesPerGroup: DefaultNodeMaxEntriesPerGroup,
			}}, 1024)
			if err != nil {
				b.Fatal(err)
			}
			var waves atomic.Uint64
			q.persist = func([]NodeReady) error {
				waves.Add(1)
				return nil
			}
			b.ReportAllocs()
			start := make(chan struct{})
			var ready, done sync.WaitGroup
			ready.Add(producers)
			done.Add(producers)
			for producer := 0; producer < producers; producer++ {
				operations := b.N / producers
				if producer < b.N%producers {
					operations++
				}
				go func(group uint64, operations int) {
					defer done.Done()
					cell := new(Submission)
					_ = cell.Initialize()
					ready.Done()
					<-start
					for operation := 0; operation < operations; operation++ {
						_ = cell.Prepare(NodeReady{GroupID: group, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: uint64(operation + 1)}})
						for {
							if _, err := q.TrySubmit(cell); err == nil {
								break
							}
						}
						_, _ = cell.Wait()
					}
				}(uint64(producer+1), operations)
			}
			ready.Wait()
			b.ResetTimer()
			b.StartTimer()
			close(start)
			done.Wait()
			b.StopTimer()
			if err := q.Close(); err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(waves.Load())/float64(b.N), "waves/op")
		})
	}
}
