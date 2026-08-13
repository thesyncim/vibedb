package durable

import (
	"errors"
	"math/bits"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type primaryJournalAdmissionMutationResult struct {
	changed bool
	err     error
}

func openPrimaryJournalAdmissionTestCollection(t *testing.T, name string) *Collection {
	t.Helper()
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible, RecoveryJournal: true,
		CheckpointStrength: CheckpointFilesystem,
	}
	collection, _, _ := openPreparedPrimaryTestCollection(t, name, options)
	if collection.primaryJournalAdmission == nil || collection.primaryJournalContexts == nil {
		t.Fatal("explicit recovery journal did not construct admission resources")
	}
	if collection.primaryConcurrentStripes != nil {
		t.Fatal("explicit recovery journal admission retained overlay stripes")
	}
	return collection
}

func blockPrimaryJournalAdmissionInitialApply(t *testing.T) (
	entered <-chan struct{}, release func(),
) {
	t.Helper()
	enteredChannel := make(chan struct{})
	releaseChannel := make(chan struct{})
	var enteredOnce sync.Once
	var releaseOnce sync.Once
	primaryJournalAdmissionInitialAppliedHook = func() {
		enteredOnce.Do(func() { close(enteredChannel) })
		<-releaseChannel
	}
	release = func() { releaseOnce.Do(func() { close(releaseChannel) }) }
	t.Cleanup(func() {
		release()
		primaryJournalAdmissionInitialAppliedHook = nil
		primaryJournalAdmissionInitialHandoffHook = nil
		primaryJournalAdmissionRequestAppliedHook = nil
	})
	return enteredChannel, release
}

func waitPrimaryJournalAdmissionQueueCount(
	t *testing.T, admission *primaryJournalAdmission, want int,
) {
	t.Helper()
	deadline := time.Now().Add(concurrentPrimaryTestTimeout)
	for {
		admission.mu.Lock()
		got := admission.count
		admission.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("admission queue count = %d, want %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPrimaryJournalAdmissionFIFOAndGroupHandoffBeforeAwait(t *testing.T) {
	collection := openPrimaryJournalAdmissionTestCollection(
		t, "journal-admission-fifo.vibe",
	)
	entered, release := blockPrimaryJournalAdmissionInitialApply(t)
	var followerApplies atomic.Int32
	followersApplied := make(chan struct{})
	primaryJournalAdmissionRequestAppliedHook = func(*primaryJournalAdmissionRequest) {
		if followerApplies.Add(1) == 4 {
			close(followersApplied)
		}
	}
	primaryJournalAdmissionInitialHandoffHook = func() { <-followersApplied }

	before := collection.Stats()
	pilotDone := make(chan primaryJournalAdmissionMutationResult, 1)
	go func() {
		created, err := collection.Put(
			[]byte("journal-admission-pilot"), []byte(`{"pilot":true}`),
		)
		pilotDone <- primaryJournalAdmissionMutationResult{created, err}
	}()
	select {
	case <-entered:
	case <-time.After(concurrentPrimaryTestTimeout):
		t.Fatal("initial pilot never reached post-Apply seam")
	}

	type followerMutation struct {
		kind primaryMutationKind
		key  []byte
		raw  []byte
		want bool
	}
	followers := []followerMutation{
		{primaryMutationPut, nil, []byte(`{"step":1}`), true},
		{primaryMutationDelete, nil, nil, true},
		{primaryMutationDelete, []byte("journal-admission-missing"), nil, false},
		{primaryMutationPut, nil, []byte(`{"step":3}`), true},
	}
	results := make([]chan primaryJournalAdmissionMutationResult, len(followers))
	key := []byte("journal-admission-fifo-key")
	for index, follower := range followers {
		results[index] = make(chan primaryJournalAdmissionMutationResult, 1)
		go func(index int, follower followerMutation) {
			var changed bool
			var err error
			mutationKey := key
			if follower.key != nil {
				mutationKey = follower.key
			}
			if follower.kind == primaryMutationDelete {
				changed, err = collection.Delete(mutationKey)
			} else {
				changed, err = collection.Put(mutationKey, follower.raw)
			}
			results[index] <- primaryJournalAdmissionMutationResult{changed, err}
		}(index, follower)
		waitPrimaryJournalAdmissionQueueCount(
			t, collection.primaryJournalAdmission, index+1,
		)
	}
	release()
	if result := <-pilotDone; result.err != nil || !result.changed {
		t.Fatalf("pilot result = %+v", result)
	}
	for index, resultChannel := range results {
		select {
		case result := <-resultChannel:
			if result.err != nil || result.changed != followers[index].want {
				t.Fatalf("follower %d result = %+v", index, result)
			}
		case <-time.After(concurrentPrimaryTestTimeout):
			t.Fatalf("follower %d did not complete", index)
		}
	}
	got, found, err := collection.AppendRaw(nil, key)
	if err != nil || !found || string(got) != `{"step":3}` {
		t.Fatalf("FIFO final value = (%s,%v,%v)", got, found, err)
	}
	after := collection.Stats()
	if acks := after.JournalAcks - before.JournalAcks; acks != 4 {
		t.Fatalf("journal acks = %d, want 4 mutations", acks)
	}
	if syncs := after.JournalSyncs - before.JournalSyncs; syncs != 1 {
		t.Fatalf("journal syncs = %d, want one finite group", syncs)
	}
	if after.ConcurrentPrimaryPublishGroups != before.ConcurrentPrimaryPublishGroups {
		t.Fatal("sequential admission changed concurrent-publish telemetry")
	}
}

func TestPrimaryJournalAdmissionCloseDrainsActiveAndPassive(t *testing.T) {
	collection := openPrimaryJournalAdmissionTestCollection(
		t, "journal-admission-close.vibe",
	)
	entered, release := blockPrimaryJournalAdmissionInitialApply(t)
	pilotDone := make(chan error, 1)
	go func() {
		_, err := collection.Put(
			[]byte("journal-admission-close-pilot"), []byte(`{"v":1}`),
		)
		pilotDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(concurrentPrimaryTestTimeout):
		t.Fatal("initial pilot never reached Close seam")
	}
	followerDone := make(chan error, 1)
	go func() {
		_, err := collection.Put(
			[]byte("journal-admission-close-follower"), []byte(`{"v":2}`),
		)
		followerDone <- err
	}()
	waitPrimaryJournalAdmissionQueueCount(t, collection.primaryJournalAdmission, 1)
	closeDone := make(chan error, 1)
	go func() { closeDone <- collection.Close() }()

	select {
	case err := <-followerDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("passive follower error = %v, want ErrClosed", err)
		}
	case <-time.After(concurrentPrimaryTestTimeout):
		t.Fatal("Close did not drain passive follower")
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close passed active pilot before release: %v", err)
	default:
	}
	release()
	select {
	case err := <-pilotDone:
		if err != nil {
			t.Fatalf("active pilot result = %v", err)
		}
	case <-time.After(concurrentPrimaryTestTimeout):
		t.Fatal("active pilot did not complete")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(concurrentPrimaryTestTimeout):
		t.Fatal("Close remained blocked after active pilot acknowledgement")
	}
}

func TestPrimaryJournalAdmissionPutReadinessFootprintPlateaus(t *testing.T) {
	collection := openPrimaryJournalAdmissionTestCollection(
		t, "journal-admission-footprint.vibe",
	)
	pool := collection.primaryJournalContexts
	before := collection.Stats().ConcurrentPrimaryScratchBytes
	if pool.putReady.Load() != 0 {
		t.Fatalf("new admission put-ready mask = %#x", pool.putReady.Load())
	}

	runFollower := func(name string, key, raw []byte) error {
		t.Helper()
		entered, release := blockPrimaryJournalAdmissionInitialApply(t)
		pilotDone := make(chan error, 1)
		go func() {
			_, err := collection.Put(
				[]byte("journal-admission-footprint-pilot-"+name),
				[]byte(`{"pilot":true}`),
			)
			pilotDone <- err
		}()
		<-entered
		followerDone := make(chan error, 1)
		go func() {
			_, err := collection.Put(key, raw)
			followerDone <- err
		}()
		waitPrimaryJournalAdmissionQueueCount(t, collection.primaryJournalAdmission, 1)
		release()
		if err := <-pilotDone; err != nil {
			t.Fatal(err)
		}
		return <-followerDone
	}

	if err := runFollower("invalid", nil, []byte(`{}`)); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("invalid follower error = %v", err)
	}
	if got := collection.Stats().ConcurrentPrimaryScratchBytes; got != before {
		t.Fatalf("invalid follower scratch = %d, want %d", got, before)
	}
	if pool.putReady.Load() != 0 {
		t.Fatalf("invalid follower initialized Put scratch: %#x", pool.putReady.Load())
	}
	if err := runFollower(
		"valid-one", []byte("journal-admission-footprint-one"), []byte(`{"v":1}`),
	); err != nil {
		t.Fatal(err)
	}
	mask := pool.putReady.Load()
	if bits.OnesCount64(mask) != 1 {
		t.Fatalf("first valid follower put-ready mask = %#x", mask)
	}
	afterFirst := collection.Stats().ConcurrentPrimaryScratchBytes
	if want := before + pool.putSlotCapacityBytes(); afterFirst != want {
		t.Fatalf("first Put scratch = %d, want %d", afterFirst, want)
	}
	if err := runFollower(
		"valid-two", []byte("journal-admission-footprint-two"), []byte(`{"v":2}`),
	); err != nil {
		t.Fatal(err)
	}
	if got := collection.Stats().ConcurrentPrimaryScratchBytes; got != afterFirst {
		t.Fatalf("serial follower scratch grew %d -> %d", afterFirst, got)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		context := pool.acquire()
		pool.ensurePut(context)
		pool.release(context)
	})
	if allocs != 0 {
		t.Fatalf("steady claimed Put context allocations = %v", allocs)
	}
}
