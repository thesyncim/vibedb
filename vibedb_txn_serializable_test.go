package vibedb

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/txnclock"
)

func TestNativeSerializableWriteSkew(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	c := db.Collection("doctors")
	if _, err := c.Put("alice", []byte(`{"oncall":true}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Put("bob", []byte(`{"oncall":true}`)); err != nil {
		t.Fatal(err)
	}

	tx1, _ := db.Begin()
	tx2, _ := db.Begin()
	if _, ok, err := tx1.Collection("doctors").Get("bob"); err != nil || !ok {
		t.Fatalf("tx1 read bob: ok=%v err=%v", ok, err)
	}
	if _, ok, err := tx2.Collection("doctors").Get("alice"); err != nil || !ok {
		t.Fatalf("tx2 read alice: ok=%v err=%v", ok, err)
	}
	if _, err := tx1.Collection("doctors").Put("alice", []byte(`{"oncall":false}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Collection("doctors").Put("bob", []byte(`{"oncall":false}`)); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("second doctor commit = %v, want conflict", err)
	}
}

func TestNativeSerializableAbsentInsertAndABA(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*Collection) error
	}{
		{name: "absent insert", mutate: func(c *Collection) error {
			_, err := c.Put("watched", []byte(`{"n":1}`))
			return err
		}},
		{name: "ABA", mutate: func(c *Collection) error {
			if _, err := c.Put("watched", []byte(`{"n":2}`)); err != nil {
				return err
			}
			_, err := c.Put("watched", []byte(`{"n":1}`))
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openSerializableMemoryDB(t)
			defer db.Close()
			c := db.Collection("c")
			if test.name == "ABA" {
				if _, err := c.Put("watched", []byte(`{"n":1}`)); err != nil {
					t.Fatal(err)
				}
			}
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := tx.Collection("c").Get("watched"); err != nil {
				t.Fatal(err)
			}
			if err := test.mutate(c); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Collection("other").Put("publish", []byte(`{"n":1}`)); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); !errors.Is(err, ErrTxConflict) {
				t.Fatalf("commit = %v, want conflict", err)
			}
		})
	}
}

func TestNativeSerializableDisjointKeysCommit(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	c := db.Collection("c")
	for _, key := range []string{"a", "b"} {
		if _, err := c.Put(key, []byte(`{"n":0}`)); err != nil {
			t.Fatal(err)
		}
	}
	tx1, _ := db.Begin()
	tx2, _ := db.Begin()
	if _, err := tx1.Collection("c").Put("a", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Collection("c").Put("b", []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("disjoint commit = %v", err)
	}
}

func TestNativeSerializableScanRejectsPhantom(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Collection("scanned").Range(func(string, []byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("scanned").Put("phantom", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Collection("other").Put("publish", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("commit = %v, want phantom conflict", err)
	}
}

func TestNativeSerializableLazyCollectionRace(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := tx.Collection("lazy").Get("k"); err != nil || ok {
		t.Fatalf("lazy miss: ok=%v err=%v", ok, err)
	}
	if _, err := db.Collection("lazy").Put("k", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Collection("other").Put("publish", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("commit = %v, want lazy-collection conflict", err)
	}
}

func TestNativeSerializableReadTrackingEscalatesWithinBounds(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	c := tx.Collection("c")
	for i := 0; i <= maxSerializableReadKeys; i++ {
		if _, _, err := c.Get(fmt.Sprintf("k-%04d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if !c.state.coarseRead {
		t.Fatal("exact read set did not escalate")
	}
	if got := len(c.state.readOrder); got != 0 {
		t.Fatalf("retained exact keys = %d", got)
	}
	if tx.readKeys > maxSerializableReadKeys || tx.readBytes > maxSerializableReadBytes {
		t.Fatalf("transaction retained accounting = %d keys, %d bytes", tx.readKeys, tx.readBytes)
	}

	large := strings.Repeat("x", 220)
	c2 := tx.Collection("bytes")
	for i := 0; !c2.state.coarseRead; i++ {
		key := fmt.Sprintf("%04d-%s", i, large)
		if _, _, err := c2.Get(key); err != nil {
			t.Fatal(err)
		}
		if i > 2000 {
			t.Fatal("byte bound did not escalate")
		}
	}
	for i := 0; i < maxSerializableReadCollections-2; i++ {
		if _, _, err := tx.Collection(fmt.Sprintf("relation_%03d", i)).Get("k"); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := tx.Collection("one_relation_too_many").Get("k"); !errors.Is(err, ErrTxTooLarge) {
		t.Fatalf("read relation bound = %v", err)
	}
	_ = tx.Rollback()
}

func TestNativeSerializableDirectWriterCannotCrossValidationPublication(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	if _, err := db.Collection("c").Put("watched", []byte(`{"n":0}`)); err != nil {
		t.Fatal(err)
	}
	tx, _ := db.Begin()
	if _, _, err := tx.Collection("c").Get("watched"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Collection("c").Put("publish", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}

	validated := make(chan struct{})
	release := make(chan struct{})
	blocked := make(chan struct{}, 1)
	db.testAfterTxValidation = func() {
		close(validated)
		<-release
	}
	db.testDirectMutationBlocked = func(*Collection) { blocked <- struct{}{} }
	commitDone := make(chan error, 1)
	go func() { commitDone <- tx.Commit() }()
	<-validated
	directDone := make(chan error, 1)
	go func() {
		_, err := db.Collection("c").Put("watched", []byte(`{"n":2}`))
		directDone <- err
	}()
	<-blocked
	close(release)
	if err := <-commitDone; err != nil {
		t.Fatalf("transaction commit = %v", err)
	}
	if err := <-directDone; err != nil {
		t.Fatalf("direct write = %v", err)
	}
}

func TestNativeSerializableConcurrentDoctorsDeterministic(t *testing.T) {
	// The same dependency cycle is also exercised through concurrently invoked
	// commits to keep the lock-order path under the race detector.
	db := openSerializableMemoryDB(t)
	defer db.Close()
	for _, key := range []string{"alice", "bob"} {
		if _, err := db.Collection("doctors").Put(key, []byte(`{"oncall":true}`)); err != nil {
			t.Fatal(err)
		}
	}
	tx1, _ := db.Begin()
	tx2, _ := db.Begin()
	_, _, _ = tx1.Collection("doctors").Get("bob")
	_, _, _ = tx2.Collection("doctors").Get("alice")
	_, _ = tx1.Collection("doctors").Put("alice", []byte(`{"oncall":false}`))
	_, _ = tx2.Collection("doctors").Put("bob", []byte(`{"oncall":false}`))
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); <-start; errs <- tx1.Commit() }()
	go func() { defer wg.Done(); <-start; errs <- tx2.Commit() }()
	close(start)
	wg.Wait()
	close(errs)
	var committed, conflicted int
	for err := range errs {
		switch {
		case err == nil:
			committed++
		case errors.Is(err, ErrTxConflict):
			conflicted++
		default:
			t.Fatalf("commit error = %v", err)
		}
	}
	if committed != 1 || conflicted != 1 {
		t.Fatalf("outcomes committed=%d conflicted=%d", committed, conflicted)
	}
}

func TestNativeSerializableProfileParity(t *testing.T) {
	for _, profile := range []Durability{Memory, Buffered, Durable} {
		profile := profile
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			db := openSerializableDB(t, profile)
			defer db.Close()
			c := db.Collection("c")
			if _, err := c.Put("watched", []byte(`{"n":0}`)); err != nil {
				t.Fatal(err)
			}
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, ok, err := tx.Collection("c").Get("watched"); err != nil || !ok {
				t.Fatalf("read: ok=%v err=%v", ok, err)
			}
			if _, err := tx.Collection("c").Put("publish", []byte(`{"n":1}`)); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Put("watched", []byte(`{"n":2}`)); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); !errors.Is(err, ErrTxConflict) {
				t.Fatalf("commit = %v, want conflict", err)
			}
		})
	}
}

func TestNativeSerializableUnrelatedHistoryOverflowDoesNotConflict(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := tx.Collection("a").Get("watched"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Collection("a").Put("publish", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	churn := db.Collection("b")
	for i := 0; i <= txnclock.HistoryKeys; i++ {
		if _, err := churn.Put(fmt.Sprintf("k-%05d", i), []byte(`{"n":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	if history := db.txnHistories["b"]; history == nil || history.Floor == 0 {
		t.Fatal("unrelated collection did not independently overflow")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("unrelated overflow commit = %v", err)
	}
}

func TestNativeSerializableSameCollectionHistoryOverflowConflicts(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	c := tx.Collection("c")
	if _, _, err := c.Get("watched"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Put("publish", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= txnclock.HistoryKeys; i++ {
		if _, err := db.Collection("c").Put(
			fmt.Sprintf("k-%05d", i), []byte(`{"n":1}`),
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("same-collection overflow commit = %v, want conflict", err)
	}
}

func TestNativeSerializableHistoryRelationCapAndCleanup(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Collection("dirty").Put("k", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= maxSerializableHistoryCollections; i++ {
		db.recordClockKey(fmt.Sprintf("relation_%04d", i), "k")
	}
	if db.txnHistoryFloor == 0 || len(db.txnHistories) != 0 {
		t.Fatalf("relation overflow floor=%d histories=%d", db.txnHistoryFloor, len(db.txnHistories))
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("relation-overflow commit = %v, want conflict", err)
	}
	if db.txnActiveCount.Load() != 0 || db.txnHistoryFloor != 0 || db.txnHistories != nil {
		t.Fatalf("last finish retained active=%d floor=%d histories=%d",
			db.txnActiveCount.Load(), db.txnHistoryFloor, len(db.txnHistories))
	}
}

func TestNativeSerializableMultiCollectionPublicationUsesOneRevision(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	observer, _ := db.Begin()
	publisher, _ := db.Begin()
	if _, err := publisher.Collection("a").Put("k", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Collection("b").Put("k", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := publisher.Commit(); err != nil {
		t.Fatal(err)
	}
	db.clockMu.Lock()
	a := db.txnHistories["a"]
	b := db.txnHistories["b"]
	if a == nil || b == nil || a.LastWrite == 0 || a.LastWrite != b.LastWrite {
		db.clockMu.Unlock()
		t.Fatalf("publication revisions a=%v b=%v", a, b)
	}
	db.clockMu.Unlock()
	if err := observer.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeSerializableFinishScrubsEscapedState(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	tx, _ := db.Begin()
	escaped := tx.Collection("c")
	if _, err := escaped.Put("k", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	state := escaped.state
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if state.pending != nil || state.order != nil || state.readSet != nil ||
		state.readOrder != nil || state.keyChunk != nil || state.keyChunks != nil ||
		state.canonical != nil {
		t.Fatal("finished escaped state retained transaction arenas")
	}
	if _, _, err := escaped.Get("k"); !errors.Is(err, ErrTxDone) {
		t.Fatalf("escaped Get = %v", err)
	}
}

func TestNativeSerializableCoordinatorHolderSaturationIsPermanent(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	db.clockMu.Lock()
	db.txnActive = map[uint64]uint64{0: maxTxnActiveCount - 1}
	db.txnActiveCount.Store(maxTxnActiveCount - 1)
	db.clockMu.Unlock()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if db.txnActive[0] != maxTxnActiveCount ||
		db.txnActiveCount.Load() != maxTxnActiveCount {
		t.Fatalf("begin did not saturate bucket=%d active=%d",
			db.txnActive[0], db.txnActiveCount.Load())
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if db.txnActive[0] != maxTxnActiveCount ||
		db.txnActiveCount.Load() != maxTxnActiveCount {
		t.Fatalf("finish decremented saturated bucket=%d active=%d",
			db.txnActive[0], db.txnActiveCount.Load())
	}
	if _, err := db.Collection("c").Put("k", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if history := db.txnHistories["c"]; history == nil || history.LastWrite == 0 {
		t.Fatal("saturated coordinator allowed publication history to disarm")
	}
}

func TestNativeSerializableCoordinatorRevisionExhaustionIsPermanent(t *testing.T) {
	db := openSerializableMemoryDB(t)
	defer db.Close()
	db.clockMu.Lock()
	db.txnRevision = maxTxnRevision - 1
	db.clockMu.Unlock()

	beforeMax, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("c").Put("at-max", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if db.txnRevision != maxTxnRevision || db.txnRevisionStopped {
		t.Fatalf("first maximum publication revision=%d stopped=%v",
			db.txnRevision, db.txnRevisionStopped)
	}
	atMax, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if atMax.beginRev != maxTxnRevision {
		t.Fatalf("begin revision=%d, want maximum", atMax.beginRev)
	}
	if _, err := db.Collection("c").Put("stop", []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}
	if !db.txnRevisionStopped {
		t.Fatal("publication with an active maximum begin did not stop coordinator")
	}
	_ = atMax.Rollback()
	_ = beforeMax.Rollback()
	if db.txnActiveCount.Load() != 0 {
		t.Fatalf("quiescence active=%d", db.txnActiveCount.Load())
	}

	after, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := after.Collection("c").Put("after", []byte(`{"n":3}`)); err != nil {
		t.Fatal(err)
	}
	if err := after.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("commit after stopped quiescence = %v, want conflict", err)
	}
	if !db.txnRevisionStopped {
		t.Fatal("quiescence reopened stopped coordinator")
	}
}

func openSerializableMemoryDB(t *testing.T) *Database {
	t.Helper()
	return openSerializableDB(t, Memory)
}

func openSerializableDB(t *testing.T, profile Durability) *Database {
	t.Helper()
	path := ""
	if profile != Memory {
		path = filepath.Join(t.TempDir(), "db")
	}
	db, err := Open(path, WithDurability(profile))
	if err != nil {
		t.Fatal(err)
	}
	return db
}
