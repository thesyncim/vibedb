package durable

import (
	"bytes"
	"errors"
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
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

func openPrimaryJournalAdmissionCohortCollection(
	t *testing.T, name string,
) (*Collection, []string, [][]byte) {
	t.Helper()
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible, RecoveryJournal: true,
		CheckpointStrength: CheckpointFilesystem,
	}
	collection, keys, values := openPreparedPrimaryTestCollection(
		t, name, options,
	)
	if collection.primaryJournalAdmission == nil ||
		collection.primaryJournalContexts == nil ||
		collection.primaryConcurrentStripes != nil {
		t.Fatal("cohort collection admission resources")
	}
	return collection, keys, values
}

func primaryJournalAdmissionEqualSizeValue(value []byte, variant byte) []byte {
	result := append([]byte(nil), value...)
	needle := []byte("primary row")
	at := bytes.Index(result, needle)
	if at < 0 {
		panic("primary cohort fixture spelling")
	}
	result[at] = variant
	return result
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
		primaryJournalCohortBeforePrepareHook = nil
		primaryJournalCohortBeforeAppendHook = nil
		primaryJournalCohortAfterDepositHook = nil
		primaryJournalCohortAfterCutHook = nil
		primaryJournalCohortBeforePressureSealHook = nil
		primaryJournalCohortBeforeForcedSealHook = nil
		primaryJournalCohortAfterTerminalDrainHook = nil
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

func waitPrimaryJournalAdmissionClosed(
	t *testing.T, admission *primaryJournalAdmission,
) {
	t.Helper()
	deadline := time.Now().Add(concurrentPrimaryTestTimeout)
	for admission.observe().phase != primaryJournalAdmissionClosed {
		if time.Now().After(deadline) {
			t.Fatal("admission did not publish Closed")
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
	if collection.primaryJournalCohortCutActive.Load() {
		t.Fatal("sequential admission activated packed-cut reads")
	}
	if acks := after.JournalAcks - before.JournalAcks; acks != 4 {
		t.Fatalf("journal acks = %d, want 4 mutations", acks)
	}
	if syncs := after.JournalSyncs - before.JournalSyncs; syncs != 1 {
		t.Fatalf("journal syncs = %d, want one finite group", syncs)
	}
	if after.JournalCohortPublishGroups != before.JournalCohortPublishGroups ||
		after.ConcurrentPrimaryPublishGroups != before.ConcurrentPrimaryPublishGroups {
		t.Fatal("sequential admission changed cohort/concurrent telemetry")
	}
}

func TestPrimaryJournalAdmissionCohortPublishesOneCutAndFence(t *testing.T) {
	for _, sameKey := range []bool{false, true} {
		for _, size := range []int{2, 8, 32} {
			t.Run(fmt.Sprintf("same-%t/size-%d", sameKey, size), func(t *testing.T) {
				collection, keys, values := openPrimaryJournalAdmissionCohortCollection(
					t, fmt.Sprintf("journal-cohort-%t-%d.vibe", sameKey, size),
				)
				entered, release := blockPrimaryJournalAdmissionInitialApply(t)
				var applied atomic.Int32
				allApplied := make(chan struct{})
				primaryJournalAdmissionRequestAppliedHook = func(
					*primaryJournalAdmissionRequest,
				) {
					if applied.Add(1) == int32(size) {
						close(allApplied)
					}
				}
				primaryJournalAdmissionInitialHandoffHook = func() { <-allApplied }
				before := collection.Stats()
				pilotDone := make(chan primaryJournalAdmissionMutationResult, 1)
				go func() {
					created, err := collection.Put(
						[]byte(keys[len(keys)-1]),
						primaryJournalAdmissionEqualSizeValue(
							values[len(values)-1], 'z',
						),
					)
					pilotDone <- primaryJournalAdmissionMutationResult{created, err}
				}()
				select {
				case <-entered:
				case <-time.After(concurrentPrimaryTestTimeout):
					t.Fatal("cohort pilot never reached Apply seam")
				}

				results := make([]chan primaryJournalAdmissionMutationResult, size)
				for index := 0; index < size; index++ {
					keyAt := index
					if sameKey {
						keyAt = 0
					}
					key := []byte(keys[keyAt])
					value := primaryJournalAdmissionEqualSizeValue(
						values[keyAt], byte('a'+index%25),
					)
					results[index] = make(chan primaryJournalAdmissionMutationResult, 1)
					go func(index int, key, value []byte) {
						created, err := collection.Put(key, value)
						results[index] <- primaryJournalAdmissionMutationResult{created, err}
					}(index, key, value)
					waitPrimaryJournalAdmissionQueueCount(
						t, collection.primaryJournalAdmission, index+1,
					)
				}
				release()
				if result := <-pilotDone; result.err != nil || result.changed {
					t.Fatalf("pilot result = %+v", result)
				}
				for index, resultChannel := range results {
					select {
					case result := <-resultChannel:
						if result.err != nil || result.changed {
							t.Fatalf("cohort %d result = %+v", index, result)
						}
					case <-time.After(concurrentPrimaryTestTimeout):
						t.Fatalf("cohort %d did not finish", index)
					}
				}
				after := collection.Stats()
				if !collection.primaryJournalCohortCutActive.Load() {
					t.Fatal("published cohort did not activate packed-cut reads")
				}
				if groups := after.JournalCohortPublishGroups -
					before.JournalCohortPublishGroups; groups != 1 {
					t.Fatalf("publish groups = %d, want 1", groups)
				}
				if got := after.JournalCohortPublishGroupSize.Sum -
					before.JournalCohortPublishGroupSize.Sum; got != uint64(size) {
					t.Fatalf("publish group records = %d, want %d", got, size)
				}
				if after.ConcurrentPrimaryPublishGroups !=
					before.ConcurrentPrimaryPublishGroups {
					t.Fatal("journal cohort masqueraded as concurrent publisher")
				}
				if acks := after.JournalAcks - before.JournalAcks; acks != uint64(size+1) {
					t.Fatalf("journal acks = %d, want %d", acks, size+1)
				}
				if syncs := after.JournalSyncs - before.JournalSyncs; syncs != 1 {
					t.Fatalf("journal syncs = %d, want 1", syncs)
				}
			})
		}
	}
}

func TestPrimaryJournalAdmissionCohortPressurePublishesExactPrefix(t *testing.T) {
	const size = 8
	for _, stopAt := range []int{0, 1, size / 2, size - 1} {
		t.Run(fmt.Sprintf("prefix-%d", stopAt), func(t *testing.T) {
			collection, keys, values := openPrimaryJournalAdmissionCohortCollection(
				t, fmt.Sprintf("journal-cohort-pressure-%d.vibe", stopAt),
			)
			entered, release := blockPrimaryJournalAdmissionInitialApply(t)
			var applied atomic.Int32
			allApplied := make(chan struct{})
			primaryJournalAdmissionRequestAppliedHook = func(
				*primaryJournalAdmissionRequest,
			) {
				if applied.Add(1) == size {
					close(allApplied)
				}
			}
			primaryJournalAdmissionInitialHandoffHook = func() { <-allApplied }
			var injected atomic.Bool
			primaryJournalCohortBeforePrepareHook = func(index int) error {
				if index == stopAt && injected.CompareAndSwap(false, true) {
					return storeio.ErrPageCachePinned
				}
				return nil
			}
			var cutCount atomic.Int32
			primaryJournalCohortAfterCutHook = func(uint64) { cutCount.Add(1) }
			before := collection.Stats()
			pilotDone := make(chan error, 1)
			go func() {
				_, err := collection.Put(
					[]byte(keys[len(keys)-1]),
					primaryJournalAdmissionEqualSizeValue(values[len(values)-1], 'z'),
				)
				pilotDone <- err
			}()
			select {
			case <-entered:
			case <-time.After(concurrentPrimaryTestTimeout):
				t.Fatal("pressure pilot never reached Apply seam")
			}
			results := make([]chan primaryJournalAdmissionMutationResult, size)
			for index := 0; index < size; index++ {
				key := []byte(keys[index])
				value := primaryJournalAdmissionEqualSizeValue(
					values[index], byte('a'+index),
				)
				results[index] = make(chan primaryJournalAdmissionMutationResult, 1)
				go func(index int, key, value []byte) {
					created, err := collection.Put(key, value)
					results[index] <- primaryJournalAdmissionMutationResult{created, err}
				}(index, key, value)
				waitPrimaryJournalAdmissionQueueCount(
					t, collection.primaryJournalAdmission, index+1,
				)
			}
			release()
			if err := <-pilotDone; err != nil {
				t.Fatal(err)
			}
			for index, resultChannel := range results {
				select {
				case result := <-resultChannel:
					if result.err != nil || result.changed {
						t.Fatalf("pressure request %d = %+v", index, result)
					}
				case <-time.After(concurrentPrimaryTestTimeout):
					t.Fatalf("pressure request %d did not finish", index)
				}
			}
			after := collection.Stats()
			if got := collection.primaryJournalCohortCutActive.Load(); got != (stopAt > 0) {
				t.Fatalf("packed-cut activation = %t, want %t", got, stopAt > 0)
			}
			wantGroups := uint64(0)
			if stopAt != 0 {
				wantGroups = 1
			}
			if got := after.JournalCohortPublishGroups -
				before.JournalCohortPublishGroups; got != wantGroups {
				t.Fatalf("pressure groups = %d, want %d", got, wantGroups)
			}
			if got := after.JournalCohortPublishGroupSize.Sum -
				before.JournalCohortPublishGroupSize.Sum; got != uint64(stopAt) {
				t.Fatalf("pressure prefix = %d, want %d", got, stopAt)
			}
			wantCuts := int32(0)
			if stopAt != 0 {
				wantCuts = 1
			}
			if got := cutCount.Load(); got != wantCuts {
				t.Fatalf("logical cuts = %d, want %d", got, wantCuts)
			}
			if acks := after.JournalAcks - before.JournalAcks; acks != size+1 {
				t.Fatalf("journal acks = %d, want %d", acks, size+1)
			}
			if syncs := after.JournalSyncs - before.JournalSyncs; syncs == 0 || syncs > size+1 {
				t.Fatalf("journal syncs = %d, want 1..%d", syncs, size+1)
			}
		})
	}
}

func TestPrimaryJournalAdmissionCohortLocalPrepareErrorNeverReturnsNilNoop(
	t *testing.T,
) {
	const size = 4
	injectedErr := errors.New("injected local prepare failure")
	for _, failAt := range []int{0, size / 2, size - 1} {
		t.Run(fmt.Sprintf("at-%d", failAt), func(t *testing.T) {
			collection, keys, values := openPrimaryJournalAdmissionCohortCollection(
				t, fmt.Sprintf("journal-cohort-local-%d.vibe", failAt),
			)
			entered, release := blockPrimaryJournalAdmissionInitialApply(t)
			var once atomic.Bool
			primaryJournalCohortBeforePrepareHook = func(index int) error {
				if index == failAt && once.CompareAndSwap(false, true) {
					return injectedErr
				}
				return nil
			}
			pilotDone := make(chan error, 1)
			go func() {
				_, err := collection.Put(
					[]byte(keys[len(keys)-1]),
					primaryJournalAdmissionEqualSizeValue(values[len(values)-1], 'z'),
				)
				pilotDone <- err
			}()
			<-entered
			results := make([]chan primaryJournalAdmissionMutationResult, size)
			wants := make([][]byte, size)
			for index := range size {
				key := []byte(keys[index])
				wants[index] = primaryJournalAdmissionEqualSizeValue(
					values[index], byte('a'+index),
				)
				results[index] = make(chan primaryJournalAdmissionMutationResult, 1)
				go func(index int, key, value []byte) {
					changed, err := collection.Put(key, value)
					results[index] <- primaryJournalAdmissionMutationResult{changed, err}
				}(index, key, wants[index])
				waitPrimaryJournalAdmissionQueueCount(
					t, collection.primaryJournalAdmission, index+1,
				)
			}
			release()
			if err := <-pilotDone; err != nil {
				t.Fatal(err)
			}
			for index := range size {
				result := <-results[index]
				if index == failAt {
					if !errors.Is(result.err, injectedErr) {
						t.Fatalf("request %d error = %v", index, result.err)
					}
				} else if result.err != nil || result.changed {
					t.Fatalf("request %d result = %+v", index, result)
				}
				got, found, err := collection.AppendRaw(nil, []byte(keys[index]))
				if err != nil || !found {
					t.Fatalf("request %d read = (%v,%v)", index, found, err)
				}
				want := wants[index]
				if index == failAt {
					want = values[index]
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("request %d value = %q, want %q", index, got, want)
				}
			}
		})
	}
}

func TestPrimaryJournalAdmissionCohortCloseBetweenPublishAndPressureSeal(
	t *testing.T,
) {
	const size = 4
	for _, forcedConversion := range []bool{false, true} {
		t.Run(fmt.Sprintf("forced-conversion-%t", forcedConversion), func(t *testing.T) {
			collection, keys, values := openPrimaryJournalAdmissionCohortCollection(
				t, fmt.Sprintf("journal-cohort-close-%t.vibe", forcedConversion),
			)
			entered, releasePilot := blockPrimaryJournalAdmissionInitialApply(t)
			seamEntered := make(chan struct{})
			seamRelease := make(chan struct{})
			var seamOnce sync.Once
			blockSeam := func() {
				seamOnce.Do(func() { close(seamEntered) })
				<-seamRelease
			}
			var releaseOnce sync.Once
			releaseSeam := func() { releaseOnce.Do(func() { close(seamRelease) }) }
			t.Cleanup(releaseSeam)
			if forcedConversion {
				primaryJournalCohortBeforeForcedSealHook = blockSeam
			} else {
				primaryJournalCohortBeforePressureSealHook = func(int) { blockSeam() }
			}
			stopAt := 1
			if forcedConversion {
				stopAt = 0
			}
			var pressureOnce atomic.Bool
			primaryJournalCohortBeforePrepareHook = func(index int) error {
				if index == stopAt && pressureOnce.CompareAndSwap(false, true) {
					return storeio.ErrPageCachePinned
				}
				return nil
			}
			pilotDone := make(chan error, 1)
			go func() {
				_, err := collection.Put(
					[]byte(keys[len(keys)-1]),
					primaryJournalAdmissionEqualSizeValue(values[len(values)-1], 'z'),
				)
				pilotDone <- err
			}()
			<-entered
			results := make([]chan primaryJournalAdmissionMutationResult, size)
			for index := range size {
				key := []byte(keys[index])
				value := primaryJournalAdmissionEqualSizeValue(
					values[index], byte('a'+index),
				)
				results[index] = make(chan primaryJournalAdmissionMutationResult, 1)
				go func(index int) {
					changed, err := collection.Put(key, value)
					results[index] <- primaryJournalAdmissionMutationResult{changed, err}
				}(index)
				waitPrimaryJournalAdmissionQueueCount(
					t, collection.primaryJournalAdmission, index+1,
				)
			}
			releasePilot()
			select {
			case <-seamEntered:
			case <-time.After(concurrentPrimaryTestTimeout):
				t.Fatal("cohort did not reach Close seal seam")
			}
			closeDone := make(chan error, 1)
			go func() { closeDone <- collection.Close() }()
			waitPrimaryJournalAdmissionClosed(
				t, collection.primaryJournalAdmission,
			)
			select {
			case err := <-closeDone:
				t.Fatalf("Close passed unsignaled continuation: %v", err)
			default:
			}
			releaseSeam()
			if err := <-pilotDone; err != nil {
				t.Fatal(err)
			}
			for index := range size {
				result := <-results[index]
				if index < 1 {
					if result.err != nil || result.changed {
						t.Fatalf("covered request %d = %+v", index, result)
					}
				} else if !errors.Is(result.err, ErrClosed) {
					t.Fatalf("closed suffix %d = %+v", index, result)
				}
			}
			select {
			case err := <-closeDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(concurrentPrimaryTestTimeout):
				t.Fatal("Close remained blocked after cohort drain")
			}
		})
	}
}

func TestPrimaryJournalAdmissionCohortAppendFailurePoisonsExactSuffix(
	t *testing.T,
) {
	const (
		size   = 8
		failAt = 4
	)
	getFault, restoreFault := installJournalFaultSeam(t)
	t.Cleanup(restoreFault)
	collection, keys, values := openPrimaryJournalAdmissionCohortCollection(
		t, "journal-cohort-append-failure.vibe",
	)
	fault := getFault()
	if fault == nil {
		t.Fatal("journal fault seam not installed")
	}
	entered, release := blockPrimaryJournalAdmissionInitialApply(t)
	terminalDrained := make(chan struct{})
	var terminalOnce sync.Once
	primaryJournalCohortAfterTerminalDrainHook = func() {
		terminalOnce.Do(func() { close(terminalDrained) })
	}
	primaryJournalAdmissionInitialHandoffHook = func() { <-terminalDrained }
	pilotDone := make(chan error, 1)
	go func() {
		_, err := collection.Put(
			[]byte(keys[len(keys)-1]),
			primaryJournalAdmissionEqualSizeValue(values[len(values)-1], 'z'),
		)
		pilotDone <- err
	}()
	<-entered
	results := make([]chan primaryJournalAdmissionMutationResult, size)
	wants := make([][]byte, size)
	for index := range size {
		key := []byte(keys[index])
		wants[index] = primaryJournalAdmissionEqualSizeValue(
			values[index], byte('a'+index),
		)
		results[index] = make(chan primaryJournalAdmissionMutationResult, 1)
		go func(index int, key, value []byte) {
			changed, err := collection.Put(key, value)
			results[index] <- primaryJournalAdmissionMutationResult{changed, err}
		}(index, key, wants[index])
		waitPrimaryJournalAdmissionQueueCount(
			t, collection.primaryJournalAdmission, index+1,
		)
	}
	fault.Program(storeio.JournalFaultPlan{
		Phase:       storeio.JournalFaultENOSPCAppend,
		AppendIndex: fault.Appends() + failAt,
	})
	release()
	if err := <-pilotDone; !errors.Is(err, syscall.ENOSPC) ||
		errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("pilot append poison = %v", err)
	}
	for index := range size {
		result := <-results[index]
		if !errors.Is(result.err, syscall.ENOSPC) ||
			errors.Is(result.err, ErrCommitOutcomeUnknown) {
			t.Fatalf("request %d terminal result = %+v", index, result)
		}
		got, found, err := collection.AppendRaw(nil, []byte(keys[index]))
		if err != nil || !found {
			t.Fatalf("request %d live read = (%v,%v)", index, found, err)
		}
		want := values[index]
		if index < failAt {
			want = wants[index]
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("request %d live value = %q, want %q", index, got, want)
		}
	}
	stats := collection.Stats()
	if stats.JournalCohortPublishGroups != 1 ||
		stats.JournalCohortPublishGroupSize.Sum != failAt {
		t.Fatalf("terminal prefix telemetry = groups %d size %d",
			stats.JournalCohortPublishGroups,
			stats.JournalCohortPublishGroupSize.Sum)
	}
}

func TestPrimaryJournalAdmissionCohortSyncFailureIsUnknownForWholeFence(
	t *testing.T,
) {
	const size = 8
	getFault, restoreFault := installJournalFaultSeam(t)
	t.Cleanup(restoreFault)
	collection, keys, values := openPrimaryJournalAdmissionCohortCollection(
		t, "journal-cohort-sync-failure.vibe",
	)
	fault := getFault()
	entered, release := blockPrimaryJournalAdmissionInitialApply(t)
	var applied atomic.Int32
	allApplied := make(chan struct{})
	primaryJournalAdmissionRequestAppliedHook = func(*primaryJournalAdmissionRequest) {
		if applied.Add(1) == size {
			close(allApplied)
		}
	}
	primaryJournalAdmissionInitialHandoffHook = func() { <-allApplied }
	pilotDone := make(chan error, 1)
	go func() {
		_, err := collection.Put(
			[]byte(keys[len(keys)-1]),
			primaryJournalAdmissionEqualSizeValue(values[len(values)-1], 'z'),
		)
		pilotDone <- err
	}()
	<-entered
	results := make([]chan error, size)
	for index := range size {
		key := []byte(keys[index])
		value := primaryJournalAdmissionEqualSizeValue(
			values[index], byte('a'+index),
		)
		results[index] = make(chan error, 1)
		go func(index int, key, value []byte) {
			_, err := collection.Put(key, value)
			results[index] <- err
		}(index, key, value)
		waitPrimaryJournalAdmissionQueueCount(
			t, collection.primaryJournalAdmission, index+1,
		)
	}
	fault.Program(storeio.JournalFaultPlan{
		Phase: storeio.JournalFaultSyncError, SyncIndex: fault.Syncs(),
	})
	release()
	assertUnknown := func(label string, err error) {
		t.Helper()
		if !errors.Is(err, ErrCommitOutcomeUnknown) ||
			!errors.Is(err, syscall.EIO) {
			t.Fatalf("%s sync result = %v, want unknown+EIO", label, err)
		}
	}
	assertUnknown("pilot", <-pilotDone)
	for index := range size {
		assertUnknown(fmt.Sprintf("request %d", index), <-results[index])
	}
	if syncs := collection.Stats().JournalSyncs; syncs != 0 {
		t.Fatalf("successful journal syncs = %d, want 0", syncs)
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
