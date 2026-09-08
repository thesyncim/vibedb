package durable

import (
	"errors"
	"fmt"
	"testing"
)

func TestDatabaseTxnDeferredStructuralPublicationGenericSplit(t *testing.T) {
	db := newTxnTestDatabase(t, "a", "b")
	a, ok := db.Collection("a")
	if !ok {
		t.Fatal("missing collection a")
	}
	b, ok := db.Collection("b")
	if !ok {
		t.Fatal("missing collection b")
	}

	putRows := func(first int) error {
		return db.Update(func(batch *DatabaseBatch) error {
			primary, err := batch.Collection("a")
			if err != nil {
				return err
			}
			secondary, err := batch.Collection("b")
			if err != nil {
				return err
			}
			for row := first; row < first+64; row++ {
				key := fmt.Sprintf("row-%08d", row)
				value := fmt.Sprintf(`{"n":%d}`, row)
				if err := primary.Put([]byte(key), []byte(value)); err != nil {
					return err
				}
			}
			return secondary.Put(
				[]byte(fmt.Sprintf("heartbeat-%08d", first)),
				[]byte(`{"n":1}`),
			)
		})
	}

	for batch := 0; batch < 4; batch++ {
		if err := putRows(batch * 64); err != nil {
			t.Fatalf("seed batch %d: %v", batch, err)
		}
	}
	if got := a.Len(); got != 256 {
		t.Fatalf("seed rows = %d, want 256", got)
	}

	oldSnapshot, err := a.Snapshot()
	if err != nil {
		t.Fatalf("capture old snapshot: %v", err)
	}
	t.Cleanup(func() { _ = oldSnapshot.Close() })
	if oldSnapshot.Len() != 256 {
		t.Fatalf("old snapshot rows = %d, want 256", oldSnapshot.Len())
	}
	assertOldSnapshot := func(phase string) {
		t.Helper()
		for row := 0; row < 256; row++ {
			key := []byte(fmt.Sprintf("row-%08d", row))
			want := []byte(fmt.Sprintf(`{"n":%d}`, row))
			got, found, err := oldSnapshot.AppendRaw(nil, key)
			if err != nil {
				t.Fatalf("%s old snapshot row %d: %v", phase, row, err)
			}
			if !found || string(got) != string(want) {
				t.Fatalf("%s old snapshot row %d = %q,%v, want %q,true",
					phase, row, got, found, want)
			}
		}
		got, found, err := oldSnapshot.AppendRaw(nil, []byte("row-00000256"))
		if err != nil {
			t.Fatalf("%s old snapshot absent row: %v", phase, err)
		}
		if found || got != nil {
			t.Fatalf("%s old snapshot saw row 256 = %q,%v", phase, got, found)
		}
	}
	assertOldSnapshot("initial")

	beforeFailure := a.Stats()
	var stageCalls int
	sentinel := errors.New("stop generic structural stage before decision")
	previousHook := databaseTxnAfterStageHook
	databaseTxnAfterStageHook = func(index int, name string) error {
		stageCalls++
		if name == "a" {
			return sentinel
		}
		return nil
	}
	failure := putRows(256)
	databaseTxnAfterStageHook = previousHook
	if failure != sentinel {
		t.Fatalf("pre-decision structural failure = %v, want %v", failure, sentinel)
	}
	if stageCalls == 0 {
		t.Fatal("pre-decision hook did not observe staged member")
	}
	afterFailure := a.Stats()
	if afterFailure.PublishedGeneration != beforeFailure.PublishedGeneration ||
		afterFailure.FileEnd != beforeFailure.FileEnd ||
		afterFailure.PrimaryLeafSplits != beforeFailure.PrimaryLeafSplits {
		t.Fatalf("failed structural stage changed state: before=%+v after=%+v",
			beforeFailure, afterFailure)
	}
	// The structural staging path retains its existing preflush. It may advance
	// the durable root before the hook aborts the conditional transaction, but
	// it must not publish the prospective rows or split shape.
	if a.Len() != 256 || b.Len() != 4 {
		t.Fatalf("failed structural stage published rows: a=%d b=%d", a.Len(), b.Len())
	}
	assertOldSnapshot("after failed stage")

	if err := putRows(256); err != nil {
		t.Fatalf("retry generic structural split: %v", err)
	}
	afterSplit := a.Stats()
	if a.Len() != 320 || b.Len() != 5 {
		t.Fatalf("split publication rows: a=%d b=%d, want 320,5", a.Len(), b.Len())
	}
	if afterSplit.PublishedGeneration != beforeFailure.PublishedGeneration+1 ||
		afterSplit.FileEnd <= beforeFailure.FileEnd ||
		afterSplit.PrimaryLeafSplits <= beforeFailure.PrimaryLeafSplits {
		t.Fatalf("generic structural publication did not advance: before=%+v after=%+v",
			beforeFailure, afterSplit)
	}
	for row := 0; row < 320; row++ {
		key := []byte(fmt.Sprintf("row-%08d", row))
		want := []byte(fmt.Sprintf(`{"n":%d}`, row))
		got, found := collectionDoc(t, a, string(key))
		if !found || got != string(want) {
			t.Fatalf("current row %d = %q,%v, want %q,true", row, got, found, want)
		}
	}
	assertOldSnapshot("after split")

	beforeRepeat := a.Stats()
	if err := db.Update(func(batch *DatabaseBatch) error {
		primary, err := batch.Collection("a")
		if err != nil {
			return err
		}
		secondary, err := batch.Collection("b")
		if err != nil {
			return err
		}
		if err := primary.Put([]byte("row-00000000"), []byte(`{"n":9000}`)); err != nil {
			return err
		}
		return secondary.Put([]byte("heartbeat-repeat"), []byte(`{"n":2}`))
	}); err != nil {
		t.Fatalf("generic repeat write: %v", err)
	}
	if after := a.Stats(); after.PublishedGeneration <= beforeRepeat.PublishedGeneration ||
		a.Len() != 320 || b.Len() != 6 {
		t.Fatalf("generic repeat publication: before=%+v after=%+v a=%d b=%d",
			beforeRepeat, a.Stats(), a.Len(), b.Len())
	}
	if got, found := collectionDoc(t, a, "row-00000000"); !found || got != `{"n":9000}` {
		t.Fatalf("repeat row = %q,%v", got, found)
	}
	assertOldSnapshot("after repeat")
	if err := oldSnapshot.Close(); err != nil {
		t.Fatalf("close old snapshot: %v", err)
	}

	image := cloneDatabaseDir(t, db.Dir())
	reopened := reopenTxnDatabase(t, image)
	reopenedA, ok := reopened.Collection("a")
	if !ok {
		t.Fatal("reopened collection a missing")
	}
	reopenedB, ok := reopened.Collection("b")
	if !ok {
		t.Fatal("reopened collection b missing")
	}
	if reopenedA.Len() != 320 || reopenedB.Len() != 6 {
		t.Fatalf("reopened rows: a=%d b=%d, want 320,6", reopenedA.Len(), reopenedB.Len())
	}
	for row := 0; row < 320; row++ {
		key := fmt.Sprintf("row-%08d", row)
		want := fmt.Sprintf(`{"n":%d}`, row)
		if row == 0 {
			want = `{"n":9000}`
		}
		got, found := collectionDoc(t, reopenedA, key)
		if !found || got != want {
			t.Fatalf("reopened row %d = %q,%v, want %q,true", row, got, found, want)
		}
	}
}
