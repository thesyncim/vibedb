package durable

import (
	"bytes"
	"errors"
	"fmt"
	"math/bits"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
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

func TestPrimaryJournalAdmissionLargeImmutableBaseDeclinesWithoutScratchGrowth(
	t *testing.T,
) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible, RecoveryJournal: true,
		CheckpointStrength: CheckpointFilesystem,
		InlineValueBytes:   storeio.CommonPrimaryLeafMaxExtentBytes,
		MaxDocumentBytes:   storeio.CommonPrimaryLeafMaxExtentBytes,
	}
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	largeRaw := []byte(`{"v":"` +
		strings.Repeat("x", primaryConcurrentRawScratchLimit+128) + `"}`)
	rows := []struct {
		key string
		raw []byte
	}{
		{"large-base", largeRaw},
		{"small-a", []byte(`{"v":"aa"}`)},
		{"small-b", []byte(`{"v":"bb"}`)},
		{"pilot", []byte(`{"v":"pp"}`)},
	}
	for _, row := range rows {
		if err := builder.Append(row.key, row.raw); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file := createPrimaryPointFile(t, built, options, "journal-large-base.vibe")
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = collection.Close() })
	pool := collection.primaryJournalContexts
	if pool == nil || pool.rawLimit != primaryConcurrentRawScratchLimit {
		t.Fatalf("journal pool raw limit = %v", pool)
	}

	router := collection.primaryRouter.Load()
	route, ok := router.Route([]byte("large-base"))
	if !ok {
		t.Fatal("large base route missing")
	}
	lease, err := router.AcquireLeaf(collection.cache, route)
	if err != nil {
		t.Fatal(err)
	}
	leaf, admitted := storeio.AdmittedCompactPrimaryStripe(
		lease.Page(), collection.storeID, route.Bucket,
	)
	if !admitted {
		lease.Release()
		t.Fatal("large immutable base is not compact")
	}
	rank, found := leaf.FindKey([]byte("large-base"))
	decoded, decodedOK := leaf.AppendValue(nil, rank)
	stableSlot, slotOK := leaf.PostingSlot(rank)
	lease.Release()
	if !found || !decodedOK || !slotOK || len(decoded) <= pool.rawLimit {
		t.Fatalf("large immutable base length = %d found=%t decoded=%t slot=%t, rawLimit=%d",
			len(decoded), found, decodedOK, slotOK, pool.rawLimit)
	}

	// Warm exactly two preparation slots without changing the physical base.
	firstContext := pool.acquire()
	secondContext := pool.acquire()
	if firstContext == nil || secondContext == nil {
		t.Fatal("could not claim two journal preparation contexts")
	}
	pool.ensurePut(firstContext)
	pool.ensurePut(secondContext)
	pool.release(secondContext)
	pool.release(firstContext)
	if ready := bits.OnesCount64(pool.putReady.Load()); ready != 2 {
		t.Fatalf("warmed journal slots = %d, want 2", ready)
	}
	var valueCaps [primaryConcurrentContextLimit]int
	for index := range pool.contexts {
		valueCaps[index] = cap(pool.contexts[index].value)
	}
	before := collection.Stats()

	// Block the literal pilot after Apply, then install a smaller logical overlay
	// value directly. No exclusive mutation can fold it before the detached
	// follower cohort examines the still-large immutable leaf.
	entered, release := blockPrimaryJournalAdmissionInitialApply(t)
	pilotDone := make(chan error, 1)
	go func() {
		_, err := collection.Put([]byte("pilot"), []byte(`{"v":"qq"}`))
		pilotDone <- err
	}()
	<-entered
	logicalView, logicalOK := collection.writerLogicalView()
	if !logicalOK || logicalView.state == nil {
		t.Fatal("pilot logical view unavailable")
	}
	currentLarge := []byte(`{"v":"cc"}`)
	seedGeneration := logicalView.generation + 1
	prepared, err := collection.primaryUnifiedOverlay.prepareWithLeafReservation(
		route.Bucket, route.Hash, seedGeneration,
		[]byte("large-base"), currentLarge,
		len(currentLarge)-len(decoded), 0,
		primaryUnifiedOverlayPut, stableSlot, route.Ref.Length, true,
		storeio.CommonPrimaryUnifiedScalarPatch{},
	)
	if err != nil {
		t.Fatal(err)
	}
	collection.primaryJournalCohortCutActive.Store(true)
	collection.primaryUnifiedOverlay.publish(prepared)
	collection.primaryUnifiedSeen = true
	router.AdvanceGeneration(seedGeneration)
	collection.pageValidator.advanceGeneration(seedGeneration)
	seedCut, ok := packFileLogicalCut(seedGeneration, logicalView.delta)
	if !ok {
		t.Fatal("seed logical cut")
	}
	collection.logicalCut.Store(seedCut)
	if current, disposition, _ := collection.primaryUnifiedOverlay.lookup(
		route.Bucket, route.Hash, []byte("large-base"), seedGeneration,
	); disposition != primaryUnifiedOverlayValue || !bytes.Equal(current, currentLarge) {
		t.Fatalf("large base overlay shape = (%q,%d)", current, disposition)
	}

	keys := []string{"large-base", "small-a"}
	values := [][]byte{[]byte(`{"v":"dd"}`), []byte(`{"v":"ac"}`)}
	results := [2]chan primaryJournalAdmissionMutationResult{
		make(chan primaryJournalAdmissionMutationResult, 1),
		make(chan primaryJournalAdmissionMutationResult, 1),
	}
	for index := range 2 {
		go func(index int) {
			changed, err := collection.Put([]byte(keys[index]), values[index])
			results[index] <- primaryJournalAdmissionMutationResult{changed, err}
		}(index)
		waitPrimaryJournalAdmissionQueueCount(
			t, collection.primaryJournalAdmission, index+1,
		)
	}
	release()
	if err := <-pilotDone; err != nil {
		t.Fatal(err)
	}
	for index := range 2 {
		if result := <-results[index]; result.err != nil || result.changed {
			t.Fatalf("fallback request %d = %+v", index, result)
		}
	}
	after := collection.Stats()
	if after.JournalCohortPublishGroups != before.JournalCohortPublishGroups {
		t.Fatal("oversized immutable base entered cohort instead of mature fallback")
	}
	if after.ConcurrentPrimaryScratchBytes != before.ConcurrentPrimaryScratchBytes {
		t.Fatalf("large-base fallback scratch grew %d -> %d",
			before.ConcurrentPrimaryScratchBytes,
			after.ConcurrentPrimaryScratchBytes)
	}
	if ready := bits.OnesCount64(pool.putReady.Load()); ready != 2 {
		t.Fatalf("large-base fallback initialized %d slots, want plateau 2", ready)
	}
	for index := range pool.contexts {
		if got := cap(pool.contexts[index].value); got != valueCaps[index] {
			t.Fatalf("context %d value cap grew %d -> %d", index, valueCaps[index], got)
		}
	}
	got, found, err := collection.AppendRaw(nil, []byte("large-base"))
	if err != nil || !found || !bytes.Equal(got, []byte(`{"v":"dd"}`)) {
		t.Fatalf("large-base mature fallback = (%q,%t,%v)", got, found, err)
	}
}

func TestPrimaryJournalAdmissionCohortCrashImagesRecoverLegalPrefix(t *testing.T) {
	const size = 3
	type seamCase struct {
		name        string
		kind        string
		index       int
		exactPrefix int
	}
	var cases []seamCase
	for index := range size {
		cases = append(cases,
			seamCase{fmt.Sprintf("before-append-%d", index), "before", index, 1 + index},
			seamCase{fmt.Sprintf("after-deposit-%d", index), "deposit", index, 2 + index},
		)
	}
	cases = append(cases, seamCase{"after-cut-before-add", "cut", 0, 1 + size})
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			options := Options{
				Backend: BackendPortable, ResidentBytes: 32 << 20,
				Durability: DurabilityBufferedVisible, RecoveryJournal: true,
				CheckpointStrength: CheckpointFilesystem,
			}
			collection, keys, values := openPreparedPrimaryTestCollection(
				t, "journal-cohort-crash-"+test.name+".vibe", options,
			)
			entered, releasePilot := blockPrimaryJournalAdmissionInitialApply(t)
			seamEntered := make(chan struct{})
			seamRelease := make(chan struct{})
			var enteredOnce sync.Once
			var releaseOnce sync.Once
			block := func() {
				enteredOnce.Do(func() { close(seamEntered) })
				<-seamRelease
			}
			release := func() { releaseOnce.Do(func() { close(seamRelease) }) }
			t.Cleanup(release)
			switch test.kind {
			case "before":
				primaryJournalCohortBeforeAppendHook = func(index int) error {
					if index == test.index {
						block()
					}
					return nil
				}
			case "deposit":
				primaryJournalCohortAfterDepositHook = func(index int, _ uint64) {
					if index == test.index {
						block()
					}
				}
			case "cut":
				primaryJournalCohortAfterCutHook = func(uint64) { block() }
			default:
				t.Fatalf("unknown seam kind %q", test.kind)
			}

			pilotKey := []byte(keys[len(keys)-1])
			pilotValue := primaryJournalAdmissionEqualSizeValue(
				values[len(values)-1], 'z',
			)
			pilotDone := make(chan error, 1)
			go func() {
				_, err := collection.Put(pilotKey, pilotValue)
				pilotDone <- err
			}()
			<-entered
			results := make([]chan error, size)
			wants := make([][]byte, size)
			for index := range size {
				key := []byte(keys[index])
				wants[index] = primaryJournalAdmissionEqualSizeValue(
					values[index], byte('a'+index),
				)
				results[index] = make(chan error, 1)
				go func(index int, key, value []byte) {
					_, err := collection.Put(key, value)
					results[index] <- err
				}(index, key, wants[index])
				waitPrimaryJournalAdmissionQueueCount(
					t, collection.primaryJournalAdmission, index+1,
				)
			}
			releasePilot()
			select {
			case <-seamEntered:
			case <-time.After(concurrentPrimaryTestTimeout):
				t.Fatal("cohort did not reach crash seam")
			}
			image := captureJournalImage(t, collection.file.Name())
			release()
			if err := <-pilotDone; err != nil {
				t.Fatal(err)
			}
			for index := range size {
				if err := <-results[index]; err != nil {
					t.Fatalf("request %d completed after image: %v", index, err)
				}
			}

			recovered := reopenJournalImage(t, options, image)
			prefix := 0
			seenOld := false
			allKeys := append([]string{keys[len(keys)-1]}, keys[:size]...)
			allOld := append([][]byte{values[len(values)-1]}, values[:size]...)
			allNew := append([][]byte{pilotValue}, wants...)
			for index, key := range allKeys {
				got, found, err := recovered.AppendRaw(nil, []byte(key))
				if err != nil || !found {
					t.Fatalf("recovered %d = (%q,%t,%v)", index, got, found, err)
				}
				switch {
				case bytes.Equal(got, allNew[index]):
					if seenOld {
						t.Fatalf("recovered hole: entry %d new after old prefix", index)
					}
					prefix++
				case bytes.Equal(got, allOld[index]):
					seenOld = true
				default:
					t.Fatalf("recovered entry %d = %q, want old %q or new %q",
						index, got, allOld[index], allNew[index])
				}
			}
			if prefix != test.exactPrefix {
				t.Fatalf("recovered prefix = %d, want exact %d",
					prefix, test.exactPrefix)
			}
		})
	}
}

func TestPrimaryJournalAdmissionPreparedAppendPressureOverwritesUnpublishedSlot(
	t *testing.T,
) {
	const size = 2
	collection, keys, values := openPrimaryJournalAdmissionCohortCollection(
		t, "journal-cohort-prepared-pressure.vibe",
	)
	entered, release := blockPrimaryJournalAdmissionInitialApply(t)
	pilotDone := make(chan error, 1)
	go func() {
		_, err := collection.Put(
			[]byte(keys[len(keys)-1]),
			primaryJournalAdmissionEqualSizeValue(values[len(values)-1], 'z'),
		)
		pilotDone <- err
	}()
	<-entered
	overlay := collection.primaryUnifiedOverlay
	countBefore := overlay.count.Load()
	usedBefore := overlay.used.Load()
	var countAtHook, usedAtHook atomic.Uint32
	var injected atomic.Bool
	primaryJournalCohortBeforeAppendHook = func(index int) error {
		if index == 0 && injected.CompareAndSwap(false, true) {
			countAtHook.Store(overlay.count.Load())
			usedAtHook.Store(overlay.used.Load())
			return storeio.ErrRecoveryJournalFull
		}
		return nil
	}
	results := make([]chan primaryJournalAdmissionMutationResult, size)
	wants := make([][]byte, size)
	expectedBytes := uint32(0)
	for index := range size {
		key := []byte(keys[index])
		wants[index] = primaryJournalAdmissionEqualSizeValue(
			values[index], byte('a'+index),
		)
		expectedBytes += uint32(len(key) + len(wants[index]))
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
		if result := <-results[index]; result.err != nil || result.changed {
			t.Fatalf("pressure fallback %d = %+v", index, result)
		}
	}
	if !injected.Load() {
		t.Fatal("prepared append pressure seam did not fire")
	}
	if countAtHook.Load() != countBefore || usedAtHook.Load() != usedBefore {
		t.Fatalf("unpublished prepare advanced state count %d->%d used %d->%d",
			countBefore, countAtHook.Load(), usedBefore, usedAtHook.Load())
	}
	if got := overlay.count.Load() - countBefore; got != size {
		t.Fatalf("fallback overlay records = %d, want %d without prepared gap", got, size)
	}
	if got := overlay.used.Load() - usedBefore; got != expectedBytes {
		t.Fatalf("fallback arena bytes = %d, want %d overwrite", got, expectedBytes)
	}
	if groups := collection.Stats().JournalCohortPublishGroups; groups != 0 {
		t.Fatalf("append-pressure cohort groups = %d, want 0", groups)
	}
	for index := range size {
		got, found, err := collection.AppendRaw(nil, []byte(keys[index]))
		if err != nil || !found || !bytes.Equal(got, wants[index]) {
			t.Fatalf("fallback value %d = (%q,%t,%v)", index, got, found, err)
		}
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
