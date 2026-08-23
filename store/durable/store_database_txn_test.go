package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func txnTestOptions() Options {
	o := syncPrimaryJournalTestOptions()
	o.MaxSnapshotLeases = 256
	o.MaxRetiredExtents = 4096
	o.MaxBatchDocuments = 64
	return o
}

func newTxnTestDatabase(t testing.TB, names ...string) *Database {
	t.Helper()
	db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: txnTestOptions()})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	t.Cleanup(func() {
		if log := detachDatabaseTxnLog(db); log != nil {
			_ = log.Close()
		}
		_ = db.Close()
	})
	for _, name := range names {
		if _, err := db.CreateCollection(name, txnTestOptions()); err != nil {
			t.Fatalf("CreateCollection(%s): %v", name, err)
		}
	}
	return db
}

func openTxnNamedCollection(
	t testing.TB, dir, name string, options Options,
) NamedCollection {
	t.Helper()
	path := filepath.Join(dir, name+".vjc")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	t.Cleanup(func() { _ = file.Close() })
	coll, err := Create(file, options)
	if err != nil {
		t.Fatalf("Create(%s): %v", name, err)
	}
	t.Cleanup(func() { _ = coll.Close() })
	return NamedCollection{Name: name, Collection: coll}
}

func mustTxnPut(t testing.TB, batch *WriteBatch, key, doc string) {
	t.Helper()
	if err := batch.Put([]byte(key), []byte(doc)); err != nil {
		t.Fatalf("Put(%s): %v", key, err)
	}
}

func collectionDoc(t testing.TB, c *Collection, key string) (string, bool) {
	t.Helper()
	raw, ok, err := c.AppendRaw(nil, []byte(key))
	if err != nil {
		t.Fatalf("AppendRaw(%s): %v", key, err)
	}
	return string(raw), ok
}

func journalBytes(t testing.TB, c *Collection) []byte {
	t.Helper()
	path := RecoveryJournalPath(c.file.Name())
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read journal: %v", err)
	}
	return data
}

func putTxnPair(t testing.TB, batch *DatabaseBatch, left, right string) error {
	t.Helper()
	a, err := batch.Collection(left)
	if err != nil {
		return err
	}
	b, err := batch.Collection(right)
	if err != nil {
		return err
	}
	mustTxnPut(t, a, "k", `{"n":1}`)
	mustTxnPut(t, b, "k", `{"n":1}`)
	return nil
}

func TestUpdateCollectionsBatchDocumentsHintIsReservationOnly(t *testing.T) {
	dir := t.TempDir()
	a := openTxnNamedCollection(t, dir, "a", txnTestOptions())
	b := openTxnNamedCollection(t, dir, "b", txnTestOptions())
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	a.BatchDocumentsHint, b.BatchDocumentsHint = 1, 1
	if err := UpdateCollections(log, []NamedCollection{a, b}, defaultTxnLimits(), func(batch *DatabaseBatch) error {
		for _, name := range []string{"a", "b"} {
			member, memberErr := batch.Collection(name)
			if memberErr != nil {
				return memberErr
			}
			for index := 0; index < 4; index++ {
				mustTxnPut(t, member, fmt.Sprintf("k%d", index), `{"n":1}`)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("grow beyond reservation hint: %v", err)
	}
	if a.Collection.Len() != 4 || b.Collection.Len() != 4 {
		t.Fatalf("hint truncated publication: a=%d b=%d", a.Collection.Len(), b.Collection.Len())
	}

	called := false
	a.BatchDocumentsHint = a.Collection.MaxBatchDocuments() + 1
	if err := UpdateCollections(log, []NamedCollection{a, b}, defaultTxnLimits(), func(*DatabaseBatch) error {
		called = true
		return nil
	}); !errors.Is(err, ErrTxnParticipant) || called {
		t.Fatalf("invalid hint = %v, callback=%t", err, called)
	}
}

func TestNewTxnLogRejectsExistingMarker(t *testing.T) {
	dir := t.TempDir()
	marker, err := storeio.CreateTxnMarker(
		filepath.Join(dir, txnMarkerFilename), storeio.TxnMarkerOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}

	log, err := NewTxnLog(dir, TxnLogOptions{})
	if log != nil {
		_ = log.Close()
		t.Fatal("NewTxnLog returned an owner for an existing marker")
	}
	if !errors.Is(err, ErrTransactionLogRecoveryRequired) {
		t.Fatalf("NewTxnLog existing marker = %v, want recovery required", err)
	}
}

func TestTxnLogValidateCollectionsIsReadOnlyAndFailsClosed(t *testing.T) {
	dir := t.TempDir()
	a := openTxnNamedCollection(t, dir, "a", txnTestOptions())
	b := openTxnNamedCollection(t, dir, "b", txnTestOptions())
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	if err := log.ValidateCollections([]NamedCollection{a, b}); err != nil {
		t.Fatalf("ValidateCollections: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, txnMarkerFilename)); !os.IsNotExist(err) {
		t.Fatalf("read-only validation minted a decision log: %v", err)
	}

	foreignDir := t.TempDir()
	foreign := openTxnNamedCollection(t, foreignDir, "foreign", txnTestOptions())
	if err := log.ValidateCollections([]NamedCollection{a, foreign}); !errors.Is(err, ErrTransactionLogDirectoryMismatch) {
		t.Fatalf("foreign participant error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, txnMarkerFilename)); !os.IsNotExist(err) {
		t.Fatalf("failed validation minted a decision log: %v", err)
	}
	if err := b.Collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := log.ValidateCollections([]NamedCollection{a, b}); !errors.Is(err, ErrTxnParticipant) {
		t.Fatalf("closed participant error = %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := log.ValidateCollections([]NamedCollection{a}); err == nil {
		t.Fatal("closed transaction log passed validation")
	}
}

func TestTxnLogPinnedDirectoryAndParticipantIdentity(t *testing.T) {
	t.Run("retarget after open", func(t *testing.T) {
		dirA := t.TempDir()
		dirB := t.TempDir()
		a := openTxnNamedCollection(t, dirA, "a", txnTestOptions())
		b := openTxnNamedCollection(t, dirA, "b", txnTestOptions())
		link := filepath.Join(t.TempDir(), "database")
		if err := os.Symlink(dirA, link); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		log, err := NewTxnLog(link, TxnLogOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = log.Close() })
		if err := os.Remove(link); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(dirB, link); err != nil {
			t.Fatal(err)
		}
		if err := UpdateCollections(
			log, []NamedCollection{a, b}, defaultTxnLimits(),
			func(batch *DatabaseBatch) error { return putTxnPair(t, batch, "a", "b") },
		); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(dirA, txnMarkerFilename)); err != nil {
			t.Fatalf("pinned marker missing: %v", err)
		}
		if _, err := os.Stat(filepath.Join(dirB, txnMarkerFilename)); !os.IsNotExist(err) {
			t.Fatalf("retargeted directory marker = %v, want absent", err)
		}
	})

	t.Run("retarget after first proof", func(t *testing.T) {
		dirA := t.TempDir()
		dirB := t.TempDir()
		link := filepath.Join(t.TempDir(), "database")
		if err := os.Symlink(dirA, link); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		a := openTxnNamedCollection(t, link, "a", txnTestOptions())
		b := openTxnNamedCollection(t, link, "b", txnTestOptions())
		log, err := NewTxnLog(link, TxnLogOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = log.Close() })
		commit := func() error {
			return UpdateCollections(
				log, []NamedCollection{a, b}, defaultTxnLimits(),
				func(batch *DatabaseBatch) error {
					return putTxnPair(t, batch, "a", "b")
				},
			)
		}
		if err := commit(); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(link); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(dirB, link); err != nil {
			t.Fatal(err)
		}
		if err := commit(); !errors.Is(err, ErrTransactionLogDirectoryMismatch) {
			t.Fatalf("commit after retarget = %v, want directory mismatch", err)
		}
		// Restore the collection namespace before cleanup closes its handles.
		if err := os.Remove(link); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(dirA, link); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("mixed directories", func(t *testing.T) {
		dirA := t.TempDir()
		dirB := t.TempDir()
		a := openTxnNamedCollection(t, dirA, "a", txnTestOptions())
		b := openTxnNamedCollection(t, dirB, "b", txnTestOptions())
		log, err := NewTxnLog(dirA, TxnLogOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = log.Close() })
		err = UpdateCollections(
			log, []NamedCollection{a, b}, defaultTxnLimits(),
			func(batch *DatabaseBatch) error { return putTxnPair(t, batch, "a", "b") },
		)
		if !errors.Is(err, ErrTransactionLogDirectoryMismatch) {
			t.Fatalf("mixed-directory commit = %v, want directory mismatch", err)
		}
		if _, err := os.Stat(filepath.Join(dirA, txnMarkerFilename)); !os.IsNotExist(err) {
			t.Fatalf("pre-mint refusal left marker = %v, want absent", err)
		}
		if _, ok := collectionDoc(t, a.Collection, "k"); ok {
			t.Fatal("mixed-directory refusal published collection a")
		}
		if _, ok := collectionDoc(t, b.Collection, "k"); ok {
			t.Fatal("mixed-directory refusal published collection b")
		}
	})
}

func TestTxnLogRescanRejectsReplacedMarker(t *testing.T) {
	dir := t.TempDir()
	a := openTxnNamedCollection(t, dir, "a", txnTestOptions())
	b := openTxnNamedCollection(t, dir, "b", txnTestOptions())
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	if err := UpdateCollections(
		log, []NamedCollection{a, b}, defaultTxnLimits(),
		func(batch *DatabaseBatch) error { return putTxnPair(t, batch, "a", "b") },
	); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, txnMarkerFilename)
	if err := os.Rename(path, filepath.Join(dir, "old.vtm")); err != nil {
		t.Skipf("cannot replace an open marker on this platform: %v", err)
	}
	replacement, err := storeio.CreateTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
	if err := UpdateCollections(
		log, []NamedCollection{a, b}, defaultTxnLimits(),
		func(batch *DatabaseBatch) error { return putTxnPair(t, batch, "a", "b") },
	); !errors.Is(err, storeio.ErrTxnMarkerCorrupt) {
		t.Fatalf("commit through replaced marker = %v, want marker corrupt", err)
	}
	log.commitMu.Lock()
	_, err = rescanTxnLogMarker(log)
	log.commitMu.Unlock()
	if !errors.Is(err, storeio.ErrTxnMarkerCorrupt) {
		t.Fatalf("replacement rescan = %v, want marker corrupt", err)
	}
	if log.marker == nil {
		t.Fatal("replacement rescan discarded the original live marker")
	}
}

func TestDatabaseTxnCommitAfterRegisteredCollectionDrop(t *testing.T) {
	db := newTxnTestDatabase(t, "a", "b", "c")
	if err := db.Update(func(batch *DatabaseBatch) error {
		return putTxnPair(t, batch, "a", "b")
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.DropCollection("c"); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(batch *DatabaseBatch) error {
		return putTxnPair(t, batch, "a", "b")
	}); err != nil {
		t.Fatalf("commit after drop: %v", err)
	}
}

func TestTxnLogDetachCollectionDischargesAndCanBeReadopted(t *testing.T) {
	dir := t.TempDir()
	a := openTxnNamedCollection(t, dir, "a", txnTestOptions())
	b := openTxnNamedCollection(t, dir, "b", txnTestOptions())
	members := []NamedCollection{a, b}
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })

	if err := UpdateCollections(
		log, members, defaultTxnLimits(),
		func(batch *DatabaseBatch) error { return putTxnPair(t, batch, "a", "b") },
	); err != nil {
		t.Fatal(err)
	}
	if log.marker == nil || log.marker.Cursor() == 0 {
		t.Fatal("multi-collection commit left no live decision")
	}
	beforeEpoch := log.marker.Header().Epoch
	if err := log.DetachCollection(a.Collection); err != nil {
		t.Fatalf("DetachCollection: %v", err)
	}
	if log.marker.Cursor() != 0 || log.marker.Header().Epoch != beforeEpoch+1 {
		t.Fatalf(
			"marker after detach = cursor %d epoch %d, want empty epoch %d",
			log.marker.Cursor(), log.marker.Header().Epoch, beforeEpoch+1,
		)
	}
	if a.Collection.journal.Cursor() != 0 {
		t.Fatalf("detached collection journal cursor = %d, want 0",
			a.Collection.journal.Cursor())
	}
	log.regMu.Lock()
	_, stillRegistered := log.registered[a.Collection]
	log.regMu.Unlock()
	if stillRegistered {
		t.Fatal("detached collection remains registered")
	}

	if err := log.AdoptCollection(a.Collection); err != nil {
		t.Fatalf("readopt detached collection: %v", err)
	}
	if err := UpdateCollections(
		log, members, defaultTxnLimits(),
		func(batch *DatabaseBatch) error {
			left, err := batch.Collection("a")
			if err != nil {
				return err
			}
			right, err := batch.Collection("b")
			if err != nil {
				return err
			}
			mustTxnPut(t, left, "after", `{"n":2}`)
			mustTxnPut(t, right, "after", `{"n":2}`)
			return nil
		},
	); err != nil {
		t.Fatalf("commit after readopt: %v", err)
	}
}

func TestDatabaseTxnVisibilityAtomicity(t *testing.T) {
	db := newTxnTestDatabase(t, "a", "b", "c")
	var stop atomic.Bool
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			if err := db.Update(func(batch *DatabaseBatch) error {
				a, _ := batch.Collection("a")
				b, _ := batch.Collection("b")
				_ = a.Put([]byte("k"), []byte(`{"n":1}`))
				_ = b.Put([]byte("k"), []byte(`{"n":1}`))
				return nil
			}); err != nil && !errors.Is(err, ErrTxnLogPoisoned) {
				t.Errorf("2-member Update: %v", err)
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			if err := db.Update(func(batch *DatabaseBatch) error {
				a, _ := batch.Collection("a")
				b, _ := batch.Collection("b")
				c, _ := batch.Collection("c")
				_ = a.Put([]byte("k"), []byte(`{"n":2}`))
				_ = b.Put([]byte("k"), []byte(`{"n":2}`))
				_ = c.Put([]byte("k"), []byte(`{"n":2}`))
				return nil
			}); err != nil && !errors.Is(err, ErrTxnLogPoisoned) {
				t.Errorf("3-member Update: %v", err)
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			snap, err := db.Snapshot()
			if err != nil {
				t.Errorf("Snapshot: %v", err)
				return
			}
			a, _ := snap.Collection("a")
			b, _ := snap.Collection("b")
			c, _ := snap.Collection("c")
			av, aok, _ := a.AppendRaw(nil, []byte("k"))
			bv, bok, _ := b.AppendRaw(nil, []byte("k"))
			cv, cok, _ := c.AppendRaw(nil, []byte("k"))
			_ = snap.Close()
			// Both writers always update a and b together, so a cut must never
			// observe them disagree. c is only written by the 3-member commit;
			// a=1,b=1,c=2 is legal after a later 2-member commit.
			if aok != bok || (aok && string(av) != string(bv)) {
				t.Errorf("torn a/b visibility: a=%q,%v b=%q,%v", av, aok, bv, bok)
				return
			}
			_ = cok
			_ = cv
		}
	}()

	time.Sleep(2 * time.Second)
	stop.Store(true)
	wg.Wait()
}

func TestDatabaseTxnDeadlockComposition(t *testing.T) {
	if testing.Short() {
		t.Skip("10s deadlock composition")
	}
	db := newTxnTestDatabase(t, "a", "b")
	a, _ := db.Collection("a")
	b, _ := db.Collection("b")
	var stop atomic.Bool
	var wg sync.WaitGroup
	deadline := time.Now().Add(10 * time.Second)

	start := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				fn()
			}
		}()
	}
	start(func() {
		_, _ = db.Snapshot()
	})
	start(func() {
		_ = db.Update(func(batch *DatabaseBatch) error {
			ab, _ := batch.Collection("a")
			bb, _ := batch.Collection("b")
			_ = ab.Put([]byte("x"), []byte(`{"v":1}`))
			_ = bb.Put([]byte("x"), []byte(`{"v":1}`))
			return nil
		})
	})
	start(func() {
		_, _ = a.Put([]byte("y"), []byte(`{"v":2}`))
	})
	start(func() {
		_, _ = b.Put([]byte("y"), []byte(`{"v":2}`))
	})
	start(func() {
		snap, err := SnapshotCollections([]NamedCollection{
			{Name: "a", Collection: a},
			{Name: "b", Collection: b},
		})
		if err == nil {
			_ = snap.Close()
		}
	})

	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
}

func TestDatabaseTxnLaneRefusals(t *testing.T) {
	cases := []struct {
		name    string
		options Options
	}{
		{
			name: "buffered-volatile",
			options: func() Options {
				o := txnTestOptions()
				o.Durability = DurabilityBufferedVisible
				o.RecoveryJournal = false
				return o
			}(),
		},
		{
			name: "async-cow",
			options: func() Options {
				o := txnTestOptions()
				o.Durability = DurabilityAsyncVisible
				return o
			}(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			a := openTxnNamedCollection(t, dir, "a", tc.options)
			b := openTxnNamedCollection(t, dir, "b", tc.options)
			beforeA := journalBytes(t, a.Collection)
			beforeB := journalBytes(t, b.Collection)
			log, err := NewTxnLog(dir, TxnLogOptions{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })
			err = UpdateCollections(log, []NamedCollection{a, b}, defaultTxnLimits(),
				func(batch *DatabaseBatch) error {
					ab, _ := batch.Collection("a")
					bb, _ := batch.Collection("b")
					mustTxnPut(t, ab, "k", `{"n":1}`)
					mustTxnPut(t, bb, "k", `{"n":1}`)
					return nil
				})
			if !errors.Is(err, ErrDatabaseTransactionUnsupportedLane) {
				t.Fatalf("err=%v want ErrDatabaseTransactionUnsupportedLane", err)
			}
			if !bytes.Equal(beforeA, journalBytes(t, a.Collection)) ||
				!bytes.Equal(beforeB, journalBytes(t, b.Collection)) {
				t.Fatal("lane refusal mutated journal bytes")
			}
		})
	}

	t.Run("chain-fence", func(t *testing.T) {
		dir := t.TempDir()
		async := txnTestOptions()
		async.Durability = DurabilityAsyncVisible
		pathA := filepath.Join(dir, "a.vjc")
		pathB := filepath.Join(dir, "b.vjc")
		for _, path := range []string{pathA, pathB} {
			file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			coll, err := Create(file, async)
			if err != nil {
				t.Fatal(err)
			}
			if err := coll.Close(); err != nil {
				t.Fatal(err)
			}
			_ = file.Close()
		}
		syncOpts := txnTestOptions()
		fileA, err := os.OpenFile(pathA, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = fileA.Close() })
		fileB, err := os.OpenFile(pathB, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = fileB.Close() })
		collA, err := Open(fileA, syncOpts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collA.Close() })
		collB, err := Open(fileB, syncOpts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collB.Close() })
		if !collA.chainFenceSync() || !collB.chainFenceSync() {
			t.Fatal("expected chain-fence sync lane after async→sync reopen")
		}
		log, err := NewTxnLog(dir, TxnLogOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = log.Close() })
		err = UpdateCollections(log, []NamedCollection{
			{Name: "a", Collection: collA},
			{Name: "b", Collection: collB},
		}, defaultTxnLimits(), func(batch *DatabaseBatch) error {
			ab, _ := batch.Collection("a")
			bb, _ := batch.Collection("b")
			mustTxnPut(t, ab, "k", `{"n":1}`)
			mustTxnPut(t, bb, "k", `{"n":1}`)
			return nil
		})
		if !errors.Is(err, ErrDatabaseTransactionUnsupportedLane) {
			t.Fatalf("err=%v want ErrDatabaseTransactionUnsupportedLane", err)
		}
	})

	t.Run("buffered-journal-ok", func(t *testing.T) {
		dir := t.TempDir()
		o := txnTestOptions()
		o.Durability = DurabilityBufferedVisible
		o.RecoveryJournal = true
		a := openTxnNamedCollection(t, dir, "a", o)
		b := openTxnNamedCollection(t, dir, "b", o)
		log, err := NewTxnLog(dir, TxnLogOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = log.Close() })
		if err := UpdateCollections(log, []NamedCollection{a, b}, defaultTxnLimits(),
			func(batch *DatabaseBatch) error {
				ab, _ := batch.Collection("a")
				bb, _ := batch.Collection("b")
				mustTxnPut(t, ab, "k", `{"n":1}`)
				mustTxnPut(t, bb, "k", `{"n":1}`)
				return nil
			}); err != nil {
			t.Fatalf("buffered-journal commit: %v", err)
		}
	})
}

func TestDatabaseTxnBoundsRefusals(t *testing.T) {
	dir := t.TempDir()
	a := openTxnNamedCollection(t, dir, "a", txnTestOptions())
	b := openTxnNamedCollection(t, dir, "b", txnTestOptions())
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	members := []NamedCollection{a, b}
	beforeA := journalBytes(t, a.Collection)
	beforeB := journalBytes(t, b.Collection)

	fill := func(batch *DatabaseBatch) error {
		ab, _ := batch.Collection("a")
		bb, _ := batch.Collection("b")
		mustTxnPut(t, ab, "k", `{"n":1}`)
		mustTxnPut(t, bb, "k", `{"n":1}`)
		return nil
	}
	assertUnchanged := func(t *testing.T) {
		t.Helper()
		if !bytes.Equal(beforeA, journalBytes(t, a.Collection)) ||
			!bytes.Equal(beforeB, journalBytes(t, b.Collection)) {
			t.Fatal("bounds refusal mutated journal bytes")
		}
	}

	t.Run("MaxCollections", func(t *testing.T) {
		err := UpdateCollections(log, members, TxnLimits{
			MaxCollections: 1, MaxDocuments: 100, MaxBytes: 1 << 20,
		}, fill)
		if !errors.Is(err, ErrTxnTooLarge) {
			t.Fatalf("err=%v want ErrTxnTooLarge", err)
		}
		assertUnchanged(t)
	})
	t.Run("MaxDocuments", func(t *testing.T) {
		err := UpdateCollections(log, members, TxnLimits{
			MaxCollections: 16, MaxDocuments: 1, MaxBytes: 1 << 20,
		}, fill)
		if !errors.Is(err, ErrTxnTooLarge) {
			t.Fatalf("err=%v want ErrTxnTooLarge", err)
		}
		assertUnchanged(t)
	})
	t.Run("MaxBytes", func(t *testing.T) {
		err := UpdateCollections(log, members, TxnLimits{
			MaxCollections: 16, MaxDocuments: 100, MaxBytes: 1,
		}, fill)
		if !errors.Is(err, ErrTxnTooLarge) {
			t.Fatalf("err=%v want ErrTxnTooLarge", err)
		}
		assertUnchanged(t)
	})
	t.Run("decision-too-big", func(t *testing.T) {
		// A 512-byte record region fits at most one padded decision whose
		// raw body is ≤512 bytes (≤11 participants). Twelve participants
		// cannot fit an empty log even after recycle.
		tiny := t.TempDir()
		opts := txnTestOptions()
		const n = 12
		parts := make([]NamedCollection, n)
		for i := range parts {
			parts[i] = openTxnNamedCollection(t, tiny, fmt.Sprintf("p%02d", i), opts)
		}
		befores := make([][]byte, n)
		for i := range parts {
			befores[i] = journalBytes(t, parts[i].Collection)
		}
		tinyLog, err := NewTxnLog(tiny, TxnLogOptions{Capacity: 512})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tinyLog.Close() })
		err = UpdateCollections(tinyLog, parts, defaultTxnLimits(),
			func(batch *DatabaseBatch) error {
				for i := range parts {
					wb, _ := batch.Collection(parts[i].Name)
					mustTxnPut(t, wb, "k", `{"n":1}`)
				}
				return nil
			})
		if !errors.Is(err, ErrTxnTooLarge) {
			t.Fatalf("err=%v want ErrTxnTooLarge", err)
		}
		for i := range parts {
			if !bytes.Equal(befores[i], journalBytes(t, parts[i].Collection)) {
				t.Fatalf("participant %d journal mutated", i)
			}
		}
	})
}

func TestDatabaseTxnZeroValueFailClosed(t *testing.T) {
	dir := t.TempDir()
	a := openTxnNamedCollection(t, dir, "a", txnTestOptions())
	b := openTxnNamedCollection(t, dir, "b", txnTestOptions())
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	beforeA := journalBytes(t, a.Collection)
	beforeB := journalBytes(t, b.Collection)
	err = UpdateCollections(log, []NamedCollection{a, b}, TxnLimits{},
		func(batch *DatabaseBatch) error {
			ab, _ := batch.Collection("a")
			bb, _ := batch.Collection("b")
			mustTxnPut(t, ab, "k", `{"n":1}`)
			mustTxnPut(t, bb, "k", `{"n":1}`)
			return nil
		})
	if !errors.Is(err, ErrTxnLimitsRequired) {
		t.Fatalf("err=%v want ErrTxnLimitsRequired", err)
	}
	if !bytes.Equal(beforeA, journalBytes(t, a.Collection)) ||
		!bytes.Equal(beforeB, journalBytes(t, b.Collection)) {
		t.Fatal("zero-limits refusal mutated journals")
	}

	db := newTxnTestDatabase(t, "a", "b")
	if err := db.Update(func(batch *DatabaseBatch) error {
		ab, _ := batch.Collection("a")
		bb, _ := batch.Collection("b")
		mustTxnPut(t, ab, "k", `{"n":1}`)
		mustTxnPut(t, bb, "k", `{"n":1}`)
		return nil
	}); err != nil {
		t.Fatalf("Database.Update with defaults: %v", err)
	}
	ca, _ := db.Collection("a")
	cb, _ := db.Collection("b")
	if doc, ok := collectionDoc(t, ca, "k"); !ok || doc != `{"n":1}` {
		t.Fatalf("a=%q,%v", doc, ok)
	}
	if doc, ok := collectionDoc(t, cb, "k"); !ok || doc != `{"n":1}` {
		t.Fatalf("b=%q,%v", doc, ok)
	}
}

func TestDatabaseTxnStageUnwind(t *testing.T) {
	dir := t.TempDir()
	a := openTxnNamedCollection(t, dir, "a", txnTestOptions())
	b := openTxnNamedCollection(t, dir, "b", txnTestOptions())
	c := openTxnNamedCollection(t, dir, "c", txnTestOptions())
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	before := [][]byte{
		journalBytes(t, a.Collection),
		journalBytes(t, b.Collection),
		journalBytes(t, c.Collection),
	}
	sentinel := errors.New("injected stage failure")
	prev := databaseTxnAfterStageHook
	databaseTxnAfterStageHook = func(index int, name string) error {
		if index == 1 {
			return sentinel
		}
		return nil
	}
	t.Cleanup(func() { databaseTxnAfterStageHook = prev })

	err = UpdateCollections(log, []NamedCollection{a, b, c}, defaultTxnLimits(),
		func(batch *DatabaseBatch) error {
			for _, name := range []string{"a", "b", "c"} {
				wb, _ := batch.Collection(name)
				mustTxnPut(t, wb, "k", `{"n":1}`)
			}
			return nil
		})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v want %v", err, sentinel)
	}
	for i, coll := range []*Collection{a.Collection, b.Collection, c.Collection} {
		if !bytes.Equal(before[i], journalBytes(t, coll)) {
			t.Fatalf("member %d journal changed after stage unwind", i)
		}
		if coll.PersistenceError() != nil {
			t.Fatalf("member %d unexpectedly poisoned", i)
		}
		if _, ok := collectionDoc(t, coll, "k"); ok {
			t.Fatalf("member %d published after unwind", i)
		}
	}
}

func TestDatabaseTxnPreparePoisonClassification(t *testing.T) {
	cases := []struct {
		name  string
		phase storeio.JournalFaultPhase
	}{
		{"append", storeio.JournalFaultENOSPCAppend},
		{"sync", storeio.JournalFaultSyncError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			a := openTxnNamedCollection(t, dir, "a", txnTestOptions())
			b := openTxnNamedCollection(t, dir, "b", txnTestOptions())
			log, err := NewTxnLog(dir, TxnLogOptions{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })

			fj := storeio.NewFaultJournal(b.Collection.journal)
			if tc.phase == storeio.JournalFaultSyncError {
				fj.Program(storeio.JournalFaultPlan{Phase: tc.phase, SyncIndex: 0})
			} else {
				fj.Program(storeio.JournalFaultPlan{Phase: tc.phase, AppendIndex: 0})
			}

			err = UpdateCollections(log, []NamedCollection{a, b}, defaultTxnLimits(),
				func(batch *DatabaseBatch) error {
					ab, _ := batch.Collection("a")
					bb, _ := batch.Collection("b")
					mustTxnPut(t, ab, "k", `{"n":1}`)
					mustTxnPut(t, bb, "k", `{"n":1}`)
					return nil
				})
			if err == nil {
				t.Fatal("expected prepare failure")
			}
			if !errors.Is(err, ErrCommitOutcomeUnknown) {
				t.Fatalf("prepare failure = %v, want outcome unknown", err)
			}
			persistence := b.Collection.PersistenceError()
			if persistence == nil {
				t.Fatal("expected sticky persistence poison on failing member")
			}
			if !errors.Is(persistence, ErrCommitOutcomeUnknown) {
				t.Fatalf("sticky poison = %v, want outcome unknown", persistence)
			}
			if _, err := b.Collection.Put(
				[]byte("later"), []byte(`{"n":2}`),
			); !errors.Is(err, ErrCommitOutcomeUnknown) {
				t.Fatalf("poisoned member later write = %v, want outcome unknown", err)
			}
		})
	}
}

func TestDatabaseTxnDecisionSyncPoisonsCatalog(t *testing.T) {
	db := newTxnTestDatabase(t, "a", "b", "c")
	var fm *storeio.FaultTxnMarker
	prev := databaseTxnAfterMintHook
	databaseTxnAfterMintHook = func(l *TxnLog) {
		fm = storeio.NewFaultTxnMarker(l.marker)
		fm.Program(storeio.TxnMarkerFaultPlan{
			Phase: storeio.TxnMarkerFaultSyncError, SyncIndex: 0,
		})
	}
	t.Cleanup(func() { databaseTxnAfterMintHook = prev })

	err := db.Update(func(batch *DatabaseBatch) error {
		ab, _ := batch.Collection("a")
		bb, _ := batch.Collection("b")
		mustTxnPut(t, ab, "k", `{"n":1}`)
		mustTxnPut(t, bb, "k", `{"n":1}`)
		return nil
	})
	if !errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("err=%v want ErrCommitOutcomeUnknown", err)
	}
	if fm == nil || !fm.Faulted() {
		t.Fatal("decision sync fault did not fire")
	}
	for _, name := range []string{"a", "b", "c"} {
		coll, _ := db.Collection(name)
		persistence := coll.PersistenceError()
		if !errors.Is(persistence, ErrCommitOutcomeUnknown) {
			t.Fatalf("%s poison=%v want ErrCommitOutcomeUnknown", name, persistence)
		}
		if _, err := coll.Put([]byte("later"), []byte(`{"n":2}`)); !errors.Is(err, ErrCommitOutcomeUnknown) {
			t.Fatalf("%s later write=%v", name, err)
		}
	}
}

func TestDatabaseTxnLifecycleSeam(t *testing.T) {
	dir := t.TempDir()
	db1, err := OpenDatabase(dir, DatabaseOptions{Options: txnTestOptions()})
	if err != nil {
		t.Fatal(err)
	}
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	attachDatabaseTxnLog(db1, log)
	if got := lookupDatabaseTxnLog(db1); got != log {
		t.Fatal("lookup after attach mismatch")
	}
	detached := detachDatabaseTxnLog(db1)
	if detached != log {
		t.Fatal("detach returned wrong log")
	}
	if lookupDatabaseTxnLog(db1) != nil {
		t.Fatal("lookup after detach still present")
	}
	_ = log.Close()
	_ = db1.Close()

	db2, err := OpenDatabase(dir, DatabaseOptions{Options: txnTestOptions()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if l := detachDatabaseTxnLog(db2); l != nil {
			_ = l.Close()
		}
		_ = db2.Close()
	})
	log2, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	attachDatabaseTxnLog(db2, log2)
	if got := lookupDatabaseTxnLog(db2); got != log2 {
		t.Fatal("second attach/lookup failed")
	}
}

func TestDatabaseTxnSingleMemberRouting(t *testing.T) {
	opts := txnTestOptions()
	// Build one empty store+journal pair, then clone the bytes so both paths
	// start from identical StoreID/JournalID identity (random mint would
	// otherwise make cross-file journal comparison meaningless).
	seedDir := t.TempDir()
	seed := openTxnNamedCollection(t, seedDir, "seed", opts)
	seedStore, err := os.ReadFile(seed.Collection.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	seedJournal := journalBytes(t, seed.Collection)
	_ = seed.Collection.Close()

	cloneOpen := func(t testing.TB, dir, name string) *Collection {
		t.Helper()
		path := filepath.Join(dir, name+".vjc")
		if err := os.WriteFile(path, seedStore, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(RecoveryJournalPath(path), seedJournal, 0o600); err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		coll, err := Open(file, opts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = coll.Close() })
		return coll
	}

	oracleDir := t.TempDir()
	oracle := cloneOpen(t, oracleDir, "only")
	if err := oracle.Update(func(batch *WriteBatch) error {
		return batch.Put([]byte("k"), []byte(`{"n":1}`))
	}); err != nil {
		t.Fatal(err)
	}
	wantJournal := journalBytes(t, oracle)

	targetDir := t.TempDir()
	target := cloneOpen(t, targetDir, "only")
	idlePath := filepath.Join(targetDir, "idle.vjc")
	idleFile, err := os.OpenFile(idlePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idleFile.Close() })
	idle, err := Create(idleFile, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idle.Close() })
	log, err := NewTxnLog(targetDir, TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	if err := UpdateCollections(log, []NamedCollection{
		{Name: "only", Collection: target},
		{Name: "idle", Collection: idle},
	}, defaultTxnLimits(), func(batch *DatabaseBatch) error {
		wb, _ := batch.Collection("only")
		return wb.Put([]byte("k"), []byte(`{"n":1}`))
	}); err != nil {
		t.Fatalf("single-dirty UpdateCollections: %v", err)
	}
	gotJournal := journalBytes(t, target)
	if !bytes.Equal(wantJournal, gotJournal) {
		t.Fatalf("journal bytes diverge from Collection.Update")
	}
	if _, err := os.Stat(filepath.Join(targetDir, txnMarkerFilename)); !os.IsNotExist(err) {
		t.Fatalf("single-dirty path minted txn.vtm: %v", err)
	}
}

func TestDatabaseTxnMintFence(t *testing.T) {
	phases := []storeio.TxnMarkerFaultPhase{
		storeio.TxnMarkerFaultCreateHeaderWrite,
		storeio.TxnMarkerFaultCreateFileSync,
		storeio.TxnMarkerFaultCreateParentDirSync,
	}
	for _, phase := range phases {
		t.Run(fmt.Sprintf("fail-%d", phase), func(t *testing.T) {
			dir := t.TempDir()
			a := openTxnNamedCollection(t, dir, "a", txnTestOptions())
			b := openTxnNamedCollection(t, dir, "b", txnTestOptions())
			beforeA := journalBytes(t, a.Collection)
			beforeB := journalBytes(t, b.Collection)
			log, err := NewTxnLog(dir, TxnLogOptions{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })
			storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{Phase: phase})
			t.Cleanup(func() {
				storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{})
			})
			err = UpdateCollections(log, []NamedCollection{a, b}, defaultTxnLimits(),
				func(batch *DatabaseBatch) error {
					ab, _ := batch.Collection("a")
					bb, _ := batch.Collection("b")
					mustTxnPut(t, ab, "k", `{"n":1}`)
					mustTxnPut(t, bb, "k", `{"n":1}`)
					return nil
				})
			if err == nil {
				t.Fatal("expected mint failure")
			}
			if errors.Is(err, ErrCommitOutcomeUnknown) {
				t.Fatalf("mint failure classified unknown: %v", err)
			}
			if !storeio.TxnMarkerCreateFaulted() {
				t.Fatal("create fault did not fire")
			}
			if !bytes.Equal(beforeA, journalBytes(t, a.Collection)) ||
				!bytes.Equal(beforeB, journalBytes(t, b.Collection)) {
				t.Fatal("mint failure mutated journals")
			}
			if _, ok := collectionDoc(t, a.Collection, "k"); ok {
				t.Fatal("mint failure published rows")
			}
		})
	}

	t.Run("parent-dir-fsync-before-prepare", func(t *testing.T) {
		dir := t.TempDir()
		a := openTxnNamedCollection(t, dir, "a", txnTestOptions())
		b := openTxnNamedCollection(t, dir, "b", txnTestOptions())
		fjA := storeio.NewFaultJournal(a.Collection.journal)
		fjB := storeio.NewFaultJournal(b.Collection.journal)
		log, err := NewTxnLog(dir, TxnLogOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = log.Close() })

		var mintSawZeroAppends atomic.Bool
		prevMint := databaseTxnAfterMintHook
		databaseTxnAfterMintHook = func(*TxnLog) {
			// CreateTxnMarker returns only after parent-directory fsync.
			mintSawZeroAppends.Store(fjA.Appends() == 0 && fjB.Appends() == 0)
		}
		t.Cleanup(func() { databaseTxnAfterMintHook = prevMint })

		if err := UpdateCollections(log, []NamedCollection{a, b}, defaultTxnLimits(),
			func(batch *DatabaseBatch) error {
				ab, _ := batch.Collection("a")
				bb, _ := batch.Collection("b")
				mustTxnPut(t, ab, "k", `{"n":1}`)
				mustTxnPut(t, bb, "k", `{"n":1}`)
				return nil
			}); err != nil {
			t.Fatalf("commit: %v", err)
		}
		if !mintSawZeroAppends.Load() {
			t.Fatal("prepare append preceded mint parent-directory fsync")
		}
		if fjA.Appends() == 0 && fjB.Appends() == 0 {
			t.Fatal("expected prepare appends after mint fence")
		}
	})
}

func TestDatabaseTxnRacingMarkerCreatorIsNotAdopted(t *testing.T) {
	dir := t.TempDir()
	a := openTxnNamedCollection(t, dir, "a", txnTestOptions())
	b := openTxnNamedCollection(t, dir, "b", txnTestOptions())
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	previousStageHook := databaseTxnAfterStageHook
	defer func() { databaseTxnAfterStageHook = previousStageHook }()
	stages := 0
	databaseTxnAfterStageHook = func(_ int, _ string) error {
		stages++
		return nil
	}

	marker, err := storeio.CreateTxnMarker(
		filepath.Join(dir, txnMarkerFilename), storeio.TxnMarkerOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	update := func(batch *DatabaseBatch) error {
		ab, _ := batch.Collection("a")
		bb, _ := batch.Collection("b")
		mustTxnPut(t, ab, "k", `{"n":1}`)
		mustTxnPut(t, bb, "k", `{"n":1}`)
		return nil
	}
	if err := UpdateCollections(
		log, []NamedCollection{a, b}, defaultTxnLimits(), update,
	); !errors.Is(err, ErrTransactionLogRecoveryRequired) {
		t.Fatalf("racing creator update = %v, want recovery required", err)
	}
	if stages != 0 {
		t.Fatalf("racing creator reached %d participant stages", stages)
	}
	if log.marker != nil {
		t.Fatal("fresh log adopted racing transaction marker")
	}
	if err := UpdateCollections(
		log, []NamedCollection{a, b}, defaultTxnLimits(), update,
	); !errors.Is(err, ErrTransactionLogRecoveryRequired) {
		t.Fatalf("poisoned retry = %v, want recovery required", err)
	}
	if stages != 0 {
		t.Fatalf("retry after unknown mint reached %d participant stages", stages)
	}
	if replacement, err := NewTxnLog(
		dir, TxnLogOptions{},
	); !errors.Is(err, ErrTransactionLogRecoveryRequired) {
		if replacement != nil {
			_ = replacement.Close()
		}
		t.Fatalf("NewTxnLog over mint residue = %v, want recovery required", err)
	}
}

func TestDatabaseTxnAllocationBudget(t *testing.T) {
	dir := t.TempDir()
	a := openTxnNamedCollection(t, dir, "a", txnTestOptions())
	b := openTxnNamedCollection(t, dir, "b", txnTestOptions())
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	members := []NamedCollection{a, b}
	limits := defaultTxnLimits()
	workload := func(batch *DatabaseBatch) error {
		ab, _ := batch.Collection("a")
		bb, _ := batch.Collection("b")
		if err := ab.Put([]byte("k"), []byte(`{"n":1}`)); err != nil {
			return err
		}
		return bb.Put([]byte("k"), []byte(`{"n":1}`))
	}
	// Warm the mint, journals, and publish paths.
	if err := UpdateCollections(log, members, limits, workload); err != nil {
		t.Fatalf("warm: %v", err)
	}
	const perParticipantBudget = 64
	allocs := testing.AllocsPerRun(50, func() {
		if err := UpdateCollections(log, members, limits, workload); err != nil {
			t.Fatalf("commit: %v", err)
		}
	})
	// Budget is stated per participant for the K=2 path.
	if allocs > float64(2*perParticipantBudget) {
		t.Fatalf("K=2 allocations = %.2f, want ≤ %d (2×%d per participant)",
			allocs, 2*perParticipantBudget, perParticipantBudget)
	}
}

func TestDatabaseTxnHappyPathPublish(t *testing.T) {
	db := newTxnTestDatabase(t, "orders", "customers")
	if err := db.Update(func(batch *DatabaseBatch) error {
		orders, err := batch.Collection("orders")
		if err != nil {
			return err
		}
		customers, err := batch.Collection("customers")
		if err != nil {
			return err
		}
		if err := orders.Put([]byte("o1"), []byte(`{"customer":"c1"}`)); err != nil {
			return err
		}
		return customers.Put([]byte("c1"), []byte(`{"tier":"pro"}`))
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	orders, _ := db.Collection("orders")
	customers, _ := db.Collection("customers")
	if doc, ok := collectionDoc(t, orders, "o1"); !ok || doc != `{"customer":"c1"}` {
		t.Fatalf("orders=%q,%v", doc, ok)
	}
	if doc, ok := collectionDoc(t, customers, "c1"); !ok || doc != `{"tier":"pro"}` {
		t.Fatalf("customers=%q,%v", doc, ok)
	}
}
