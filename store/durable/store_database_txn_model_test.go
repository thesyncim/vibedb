package durable

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// TestDatabaseTxnLinearizedModel runs a seeded randomized workload mixing
// multi-member UpdateCollections, single-member updates, autocommit point
// writes, and induced crashes (journal + marker seams). After each reopen the
// recovered state must equal the in-memory model at a legal acknowledged-commit
// prefix for the sync-journal lane — every acknowledged commit present, never a
// third (torn) value.

func TestDatabaseTxnLinearizedModel(t *testing.T) {
	const seed = int64(0xdb7701)
	t.Logf("TestDatabaseTxnLinearizedModel seed=%d", seed)
	rng := rand.New(rand.NewSource(seed))

	type modelState map[string]map[string]string // collection → key → doc
	cloneModel := func(src modelState) modelState {
		out := modelState{"a": {}, "b": {}}
		for name, keys := range src {
			for k, v := range keys {
				out[name][k] = v
			}
		}
		return out
	}
	setModel := func(dst modelState, name, key, doc string) {
		dst[name][key] = doc
	}
	statesEqual := func(a, b modelState) bool {
		for _, name := range []string{"a", "b"} {
			if len(a[name]) != len(b[name]) {
				return false
			}
			for k, v := range a[name] {
				if b[name][k] != v {
					return false
				}
			}
		}
		return true
	}
	readState := func(d *Database) modelState {
		t.Helper()
		out := modelState{"a": {}, "b": {}}
		for _, name := range []string{"a", "b"} {
			coll, ok := d.Collection(name)
			if !ok {
				t.Fatalf("missing %s", name)
			}
			snap, err := coll.Snapshot()
			if err != nil {
				t.Fatalf("Snapshot(%s): %v", name, err)
			}
			err = snap.RangeRaw(func(key, value []byte) error {
				out[name][string(key)] = string(value)
				return nil
			})
			_ = snap.Close()
			if err != nil {
				t.Fatalf("RangeRaw(%s): %v", name, err)
			}
		}
		return out
	}
	assertEqualsModel := func(d *Database, want modelState, label string) {
		t.Helper()
		got := readState(d)
		if !statesEqual(got, want) {
			t.Fatalf("%s: recovered %#v want %#v", label, got, want)
		}
	}
	assertLegalPrefix := func(d *Database, legal ...modelState) {
		t.Helper()
		got := readState(d)
		for _, want := range legal {
			if statesEqual(got, want) {
				return
			}
		}
		t.Fatalf("recovered %#v is not a legal acknowledged prefix among %d candidates",
			got, len(legal))
	}

	openFresh := func(model modelState) (*Database, string) {
		t.Helper()
		dir := t.TempDir()
		db, err := OpenDatabase(dir, DatabaseOptions{Options: txnTestOptions()})
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"a", "b"} {
			if _, err := db.CreateCollection(name, txnTestOptions()); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Update(func(batch *DatabaseBatch) error {
			a, _ := batch.Collection("a")
			b, _ := batch.Collection("b")
			for k, v := range model["a"] {
				if err := a.Put([]byte(k), []byte(v)); err != nil {
					return err
				}
			}
			for k, v := range model["b"] {
				if err := b.Put([]byte(k), []byte(v)); err != nil {
					return err
				}
			}
			// Keep both members dirty so the mint path stays exercised.
			if err := a.Put([]byte("_keep"), []byte(`{"k":1}`)); err != nil {
				return err
			}
			return b.Put([]byte("_keep"), []byte(`{"k":1}`))
		}); err != nil {
			t.Fatalf("seed fresh database: %v", err)
		}
		model["a"]["_keep"] = `{"k":1}`
		model["b"]["_keep"] = `{"k":1}`
		return db, dir
	}

	model := modelState{"a": {}, "b": {}}
	db, dir := openFresh(model)
	defer func() { _ = db.Close() }()

	ensureMintedMarker := func() *storeio.TxnMarker {
		t.Helper()
		log := lookupDatabaseTxnLog(db)
		if log != nil && log.marker != nil {
			return log.marker
		}
		if err := db.Update(func(batch *DatabaseBatch) error {
			a, _ := batch.Collection("a")
			b, _ := batch.Collection("b")
			_ = a.Put([]byte("_mint"), []byte(`{"k":1}`))
			return b.Put([]byte("_mint"), []byte(`{"k":1}`))
		}); err != nil {
			t.Fatalf("mint fence commit: %v", err)
		}
		model["a"]["_mint"] = `{"k":1}`
		model["b"]["_mint"] = `{"k":1}`
		log = lookupDatabaseTxnLog(db)
		if log == nil || log.marker == nil {
			t.Fatal("expected minted txn marker")
		}
		return log.marker
	}

	reopenFromImage := func(want modelState, label string) {
		t.Helper()
		img := cloneDatabaseDir(t, db.Dir())
		_ = db.Close()
		var err error
		db, err = OpenDatabase(img, DatabaseOptions{Options: txnTestOptions()})
		if err != nil {
			t.Fatalf("%s reopen: %v", label, err)
		}
		dir = img
		assertEqualsModel(db, want, label)
	}

	const steps = 40
	for step := 0; step < steps; step++ {
		label := fmt.Sprintf("step %d", step)
		switch rng.Intn(8) {
		case 0, 1, 2:
			key := fmt.Sprintf("m/%d", step)
			doc := fmt.Sprintf(`{"s":%d}`, step)
			if err := db.Update(func(batch *DatabaseBatch) error {
				a, _ := batch.Collection("a")
				b, _ := batch.Collection("b")
				if err := a.Put([]byte(key), []byte(doc)); err != nil {
					return err
				}
				return b.Put([]byte(key), []byte(doc))
			}); err != nil {
				t.Fatalf("%s multi Update: %v", label, err)
			}
			setModel(model, "a", key, doc)
			setModel(model, "b", key, doc)

		case 3:
			name := []string{"a", "b"}[rng.Intn(2)]
			key := fmt.Sprintf("s/%s/%d", name, step)
			doc := fmt.Sprintf(`{"s":%d}`, step)
			coll, _ := db.Collection(name)
			if err := coll.Update(func(batch *WriteBatch) error {
				return batch.Put([]byte(key), []byte(doc))
			}); err != nil {
				t.Fatalf("%s single Update(%s): %v", label, name, err)
			}
			setModel(model, name, key, doc)

		case 4:
			name := []string{"a", "b"}[rng.Intn(2)]
			key := fmt.Sprintf("p/%s/%d", name, step)
			doc := fmt.Sprintf(`{"s":%d}`, step)
			coll, _ := db.Collection(name)
			if _, err := coll.Put([]byte(key), []byte(doc)); err != nil {
				t.Fatalf("%s Put(%s): %v", label, name, err)
			}
			setModel(model, name, key, doc)

		case 5:
			// A prepare append failure is outcome-unknown in-process because the
			// checksummed body may have landed without all padding. With no marker
			// decision, reopen resolves it as aborted; the model stays unchanged.
			before := cloneModel(model)
			b, _ := db.Collection("b")
			fj := storeio.NewFaultJournal(b.journal)
			fj.Program(storeio.JournalFaultPlan{
				Phase: storeio.JournalFaultENOSPCAppend, AppendIndex: 0,
			})
			key := fmt.Sprintf("m/%d", step)
			doc := fmt.Sprintf(`{"s":%d}`, step)
			err := db.Update(func(batch *DatabaseBatch) error {
				a, _ := batch.Collection("a")
				bb, _ := batch.Collection("b")
				_ = a.Put([]byte(key), []byte(doc))
				return bb.Put([]byte(key), []byte(doc))
			})
			if err == nil {
				t.Fatalf("%s: expected prepare failure", label)
			}
			if !errors.Is(err, ErrCommitOutcomeUnknown) {
				t.Fatalf("%s: prepare failure=%v want outcome unknown", label, err)
			}
			reopenFromImage(before, label+" prepare-fault")
			model = before
			// Poisoned handles cannot continue; rebuild a fresh writable database
			// at the recovered acknowledged prefix.
			_ = db.Close()
			db, dir = openFresh(model)
			assertEqualsModel(db, model, label+" reseeded")

		case 6:
			// Torn decision append: the fault seam persists a byte prefix and
			// returns success to the dying process. Abandon that process and
			// reopen from disk — recovery must truncate to presumed abort.
			marker := ensureMintedMarker()
			before := cloneModel(model)
			fm := storeio.NewFaultTxnMarker(marker)
			fm.Program(storeio.TxnMarkerFaultPlan{
				Phase: storeio.TxnMarkerFaultTornAppend, AppendIndex: 0,
			})
			key := fmt.Sprintf("m/%d", step)
			doc := fmt.Sprintf(`{"s":%d}`, step)
			_ = db.Update(func(batch *DatabaseBatch) error {
				a, _ := batch.Collection("a")
				b, _ := batch.Collection("b")
				_ = a.Put([]byte(key), []byte(doc))
				return b.Put([]byte(key), []byte(doc))
			})
			if fm == nil || !fm.Faulted() {
				t.Fatalf("%s: torn decision fault did not fire", label)
			}
			reopenFromImage(before, label+" torn-decision")
			model = before

		case 7:
			// Decision sync fault: unknown outcome; reopen is before or after,
			// never a torn third value.
			marker := ensureMintedMarker()
			before := cloneModel(model)
			key := fmt.Sprintf("m/%d", step)
			doc := fmt.Sprintf(`{"s":%d}`, step)
			after := cloneModel(model)
			setModel(after, "a", key, doc)
			setModel(after, "b", key, doc)

			fm := storeio.NewFaultTxnMarker(marker)
			fm.Program(storeio.TxnMarkerFaultPlan{
				Phase: storeio.TxnMarkerFaultSyncError, SyncIndex: 0,
			})
			err := db.Update(func(batch *DatabaseBatch) error {
				a, _ := batch.Collection("a")
				b, _ := batch.Collection("b")
				_ = a.Put([]byte(key), []byte(doc))
				return b.Put([]byte(key), []byte(doc))
			})
			if !errors.Is(err, ErrCommitOutcomeUnknown) {
				t.Fatalf("%s: err=%v want ErrCommitOutcomeUnknown (faulted=%v)",
					label, err, fm != nil && fm.Faulted())
			}
			img := cloneDatabaseDir(t, db.Dir())
			_ = db.Close()
			var openErr error
			db, openErr = OpenDatabase(img, DatabaseOptions{Options: txnTestOptions()})
			if openErr != nil {
				t.Fatalf("%s reopen after sync fault: %v", label, openErr)
			}
			dir = img
			assertLegalPrefix(db, before, after)
			model = readState(db)
			// Catalog may be poisoned in-process; rebuild if further writes fail.
			if err := db.Update(func(batch *DatabaseBatch) error {
				a, _ := batch.Collection("a")
				b, _ := batch.Collection("b")
				_ = a.Put([]byte("_probe"), []byte(`{"k":1}`))
				return b.Put([]byte("_probe"), []byte(`{"k":1}`))
			}); err != nil {
				_ = db.Close()
				db, dir = openFresh(model)
			} else {
				model["a"]["_probe"] = `{"k":1}`
				model["b"]["_probe"] = `{"k":1}`
			}
		}

		if step%9 == 8 {
			img := cloneDatabaseDir(t, db.Dir())
			_ = db.Close()
			var openErr error
			db, openErr = OpenDatabase(img, DatabaseOptions{Options: txnTestOptions()})
			if openErr != nil {
				t.Fatalf("%s periodic reopen: %v", label, openErr)
			}
			dir = img
			assertEqualsModel(db, model, label+" periodic reopen")
		}
	}

	assertEqualsModel(db, model, "final")
	img := cloneDatabaseDir(t, dir)
	_ = db.Close()
	final, err := OpenDatabase(img, DatabaseOptions{Options: txnTestOptions()})
	if err != nil {
		t.Fatalf("final reopen: %v", err)
	}
	defer final.Close()
	assertEqualsModel(final, model, "final reopen")
}
