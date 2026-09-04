package vibedb

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestTxnClockQuiescenceCannotEraseNewObserver(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	if _, err := db.Collection("watched").Put("k", []byte(`{"n":0}`)); err != nil {
		t.Fatal(err)
	}
	old, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	quiescing := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	db.testBeforeClockQuiesce = func() {
		once.Do(func() {
			close(quiescing)
			<-release
		})
	}
	finished := make(chan error, 1)
	go func() { finished <- old.Rollback() }()
	<-quiescing
	observer, err := db.Begin()
	if err != nil {
		close(release)
		<-finished
		t.Fatal(err)
	}
	defer observer.Rollback()
	_, _, readErr := observer.Collection("watched").Get("k")
	_, writeErr := db.Collection("watched").Put("k", []byte(`{"n":1}`))
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if readErr != nil || writeErr != nil {
		t.Fatalf("read=%v write=%v", readErr, writeErr)
	}
	if _, err := observer.Collection("watched").Put("k", []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}
	if err := observer.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("commit after overlapping quiescence = %v, want conflict", err)
	}
}

func TestTxnClockOverflowDuringValidationCannotEraseConflict(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	if _, err := db.Collection("watched").Put("k", []byte(`{"n":0}`)); err != nil {
		t.Fatal(err)
	}
	observer, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Rollback()
	if _, err := observer.Collection("watched").Put("k", []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("watched").Put("k", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < maxSerializableHistoryCollections; i++ {
		db.recordClockKey(fmt.Sprintf("history_%03d", i), "k")
	}
	db.testAfterTxHistoryGuards = func() {
		// An unrelated publication can overflow the shared history directory
		// while the committing transaction holds only its participant fence.
		db.recordClockKey("overflow", "k")
	}
	if err := observer.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("commit after history disappeared during validation = %v, want conflict", err)
	}
}

func TestTxnClockConcurrentRegistrationSaturatesWithoutWrapping(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	db.txnActiveCount.Store(maxTxnActiveCount - 1)
	first := &Tx{}
	second := &Tx{}
	db.txnActive[1].mu.Lock()
	db.txnActive[2].mu.Lock()
	firstDone := make(chan struct{})
	secondDone := make(chan struct{})
	go func() { db.armClockForBegin(first); close(firstDone) }()
	waitTxnClockCondition(t, func() bool { return db.txnArmTick.Load() == 1 })
	go func() { db.armClockForBegin(second); close(secondDone) }()
	waitTxnClockCondition(t, func() bool { return db.txnArmTick.Load() == 2 })
	db.txnActive[1].mu.Unlock()
	<-firstDone
	db.txnActive[2].mu.Unlock()
	<-secondDone
	if count := db.txnActiveCount.Load(); count != maxTxnActiveCount || !db.txnClockSaturated.Load() {
		t.Errorf("registration wrapped or failed to latch: count=%d saturated=%v", count, db.txnClockSaturated.Load())
	}
	db.finishClock(first, nil)
	db.finishClock(second, nil)
	if count := db.txnActiveCount.Load(); count != maxTxnActiveCount || !db.txnClockSaturated.Load() {
		t.Errorf("finish drained saturation: count=%d saturated=%v", count, db.txnClockSaturated.Load())
	}
}

func TestTxnClockRegistrationKeepsOldestHintConservative(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	db.txnActive[1].mu.Lock()
	tx := &Tx{}
	done := make(chan struct{})
	go func() { db.armClockForBegin(tx); close(done) }()
	waitTxnClockCondition(t, func() bool { return db.txnArmTick.Load() == 1 })
	db.txnRevision.Store(10)
	quiesced := make(chan struct{})
	go func() { db.quiesceClock(); close(quiesced) }()
	db.txnActive[1].mu.Unlock()
	<-done
	<-quiesced
	defer db.finishClock(tx, nil)
	// A quiescent scan may already have advanced the hint before registration.
	// Registered revisions must never precede that safe pruning boundary.
	if tx.beginRev < db.txnOldestHint.Load() {
		t.Fatalf("registered begin=%d behind pruning hint=%d", tx.beginRev, db.txnOldestHint.Load())
	}
}

func TestTxnClockDelayedRecordCannotLowerOverflowFloor(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	if _, err := db.Collection("watched").Put("k", []byte(`{"n":0}`)); err != nil {
		t.Fatal(err)
	}
	holder, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	assigned := make(chan struct{})
	release := make(chan struct{})
	db.testAfterTxnRevisionAssigned = func(revision uint64) {
		if revision == 1 {
			close(assigned)
			<-release
		}
	}
	delayedDone := make(chan error, 1)
	go func() {
		_, err := db.Collection("delayed").Put("k", []byte(`{"n":0}`))
		delayedDone <- err
	}()
	<-assigned
	for i := 0; i < maxSerializableHistoryCollections-1; i++ {
		db.recordClockKey(fmt.Sprintf("history_%03d", i), "k")
	}
	observer, err := db.Begin()
	if err != nil {
		close(release)
		<-delayedDone
		t.Fatal(err)
	}
	defer observer.Rollback()
	_, _, readErr := observer.Collection("watched").Get("k")
	_, writeErr := db.Collection("watched").Put("k", []byte(`{"n":1}`))
	close(release)
	if err := <-delayedDone; err != nil {
		t.Fatal(err)
	}
	if readErr != nil || writeErr != nil {
		t.Fatalf("read=%v write=%v", readErr, writeErr)
	}
	if _, err := observer.Collection("watched").Put("k", []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}
	if err := observer.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("commit after an old reservation reset newer histories = %v, want conflict", err)
	}
}

func TestTxnClockExhaustionConcurrentAllocationsRemainStopped(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	db.txnRevision.Store(maxTxnRevision - 1)
	type allocation struct {
		revision uint64
		ok       bool
	}
	db.clockMu.Lock()
	results := make(chan allocation, 8)
	for range 8 {
		go func() {
			revision, ok := db.nextTxnRevision()
			results <- allocation{revision, ok}
		}()
	}
	waitTxnClockCondition(t, func() bool { return len(results) >= 6 })
	db.clockMu.Unlock()
	accepted := 0
	for range 8 {
		result := <-results
		if result.ok {
			accepted++
			if result.revision != maxTxnRevision {
				t.Errorf("accepted reused revision %d after exhaustion", result.revision)
			}
		}
	}
	if accepted != 1 || db.txnRevision.Load() != maxTxnRevision || !db.txnRevisionStopped.Load() {
		t.Fatalf("exhaustion accepted=%d revision=%d stopped=%v", accepted, db.txnRevision.Load(), db.txnRevisionStopped.Load())
	}
}

func TestTxnClockExhaustionNeverReusesRevisionZero(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	db.txnRevision.Store(maxTxnRevision)
	if revision, ok := db.nextTxnRevision(); ok || revision != maxTxnRevision {
		t.Fatalf("exhausted allocation = %d, %v; want saturated revision and refusal", revision, ok)
	}
	if revision := db.txnRevision.Load(); revision != maxTxnRevision {
		t.Fatalf("global revision wrapped to %d", revision)
	}
}

func TestTxnFinishedStateReleasesCachedDatabaseHandle(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	c := tx.Collection("c")
	if _, err := c.Put("k", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if c.state.coll != nil {
		t.Fatal("escaped finished state retains the owning database through its cached handle")
	}
}

func waitTxnClockCondition(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("clock operation did not reach the controlled interleaving")
		}
		runtime.Gosched()
	}
}
