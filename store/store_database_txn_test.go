package store

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibejson"
)

func TestUpdateCollectionsPublishesAllOrNothing(t *testing.T) {
	var db Database
	orders := mustCollection(t, &db, "orders")
	customers := mustCollection(t, &db, "customers")

	err := UpdateCollections([]*Collection{orders, customers}, func(b *DatabaseBatch) error {
		ob, err := b.Collection("orders")
		if err != nil {
			return err
		}
		cb, err := b.Collection("customers")
		if err != nil {
			return err
		}
		if err := ob.Put("o1", []byte(`{"customer":"c1"}`)); err != nil {
			return err
		}
		return cb.Put("c1", []byte(`{"tier":"pro"}`))
	})
	if err != nil {
		t.Fatalf("UpdateCollections: %v", err)
	}

	snapshot := db.Snapshot()
	ordersView, _ := snapshot.Collection("orders")
	customersView, _ := snapshot.Collection("customers")
	if raw, ok := ordersView.GetRaw("o1"); !ok || string(raw.Bytes()) != `{"customer":"c1"}` {
		t.Fatalf("orders o1=%q,%v", raw.Bytes(), ok)
	}
	if raw, ok := customersView.GetRaw("c1"); !ok || string(raw.Bytes()) != `{"tier":"pro"}` {
		t.Fatalf("customers c1=%q,%v", raw.Bytes(), ok)
	}
}

func TestUpdateCollectionsRejectsFnErrorWithoutPublish(t *testing.T) {
	var db Database
	orders := mustCollection(t, &db, "orders")
	customers := mustCollection(t, &db, "customers")
	sentinel := errors.New("boom")

	err := UpdateCollections([]*Collection{orders, customers}, func(b *DatabaseBatch) error {
		ob, _ := b.Collection("orders")
		cb, _ := b.Collection("customers")
		_ = ob.Put("o1", []byte(`{"n":1}`))
		_ = cb.Put("c1", []byte(`{"n":1}`))
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v want %v", err, sentinel)
	}
	if orders.Len() != 0 || customers.Len() != 0 {
		t.Fatalf("partial publish: orders=%d customers=%d", orders.Len(), customers.Len())
	}
}

func TestUpdateCollectionsRejectsMalformedJSONWithoutPublish(t *testing.T) {
	var db Database
	orders := mustCollection(t, &db, "orders")
	customers := mustCollection(t, &db, "customers")
	if _, err := orders.Put("keep", []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	before := orders.Generation()

	err := UpdateCollections([]*Collection{orders, customers}, func(b *DatabaseBatch) error {
		ob, _ := b.Collection("orders")
		cb, _ := b.Collection("customers")
		if err := ob.Put("o1", []byte(`{"n":1}`)); err != nil {
			return err
		}
		return cb.Put("bad", []byte(`{`))
	})
	if err == nil {
		t.Fatal("expected malformed JSON error")
	}
	if orders.Generation() != before || orders.Len() != 1 {
		t.Fatalf("orders changed: gen=%d len=%d", orders.Generation(), orders.Len())
	}
	if customers.Len() != 0 {
		t.Fatalf("customers published: len=%d", customers.Len())
	}
}

func TestUpdateCollectionsAdmissionBounds(t *testing.T) {
	var db Database
	a := mustCollection(t, &db, "a")
	b := mustCollection(t, &db, "b")

	err := UpdateCollections([]*Collection{a, b}, func(batch *DatabaseBatch) error {
		wb, err := batch.Collection("a")
		if err != nil {
			return err
		}
		for i := 0; i < defaultHeapMaxBatchDocuments+1; i++ {
			if err := wb.Put(fmt.Sprintf("k%d", i), []byte(`{"n":1}`)); err != nil {
				if !errors.Is(err, ErrBatchTooLarge) {
					t.Fatalf("Put=%v want ErrBatchTooLarge", err)
				}
				return err
			}
		}
		return nil
	})
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("err=%v want ErrBatchTooLarge", err)
	}
	if a.Len() != 0 || b.Len() != 0 {
		t.Fatal("admission refusal published rows")
	}
}

func TestUpdateCollectionsTargetCountBound(t *testing.T) {
	var db Database
	parts := make([]*Collection, defaultHeapMaxTxnCollections+1)
	for i := range parts {
		parts[i] = mustCollection(t, &db, fmt.Sprintf("c%02d", i))
	}
	err := UpdateCollections(parts, func(*DatabaseBatch) error { return nil })
	if !errors.Is(err, ErrTxnTooLarge) {
		t.Fatalf("err=%v want ErrTxnTooLarge", err)
	}
}

func TestUpdateCollectionsUnknownTarget(t *testing.T) {
	var db Database
	a := mustCollection(t, &db, "a")
	err := UpdateCollections([]*Collection{a}, func(b *DatabaseBatch) error {
		_, err := b.Collection("missing")
		return err
	})
	if !errors.Is(err, ErrTxnCollection) {
		t.Fatalf("err=%v want ErrTxnCollection", err)
	}
}

func TestUpdateCollectionsLoneTargetCallerMutation(t *testing.T) {
	var db Database
	original := mustCollection(t, &db, "original")
	targets := []*Collection{original}

	err := UpdateCollections(targets, func(batch *DatabaseBatch) error {
		targets[0] = nil
		wb, err := batch.Collection("original")
		if err != nil {
			return err
		}
		return wb.Put("k", []byte(`{"n":1}`))
	})
	if err != nil {
		t.Fatalf("UpdateCollections: %v", err)
	}
	if raw, ok := mustSnapshotRaw(t, original, "k"); !ok || string(raw) != `{"n":1}` {
		t.Fatalf("original.k=%q,%v", raw, ok)
	}
}

func TestUpdateCollectionsBatchClosedAfterReturn(t *testing.T) {
	var db Database
	a := mustCollection(t, &db, "a")
	var escaped *WriteBatch
	if err := UpdateCollections([]*Collection{a}, func(b *DatabaseBatch) error {
		wb, err := b.Collection("a")
		if err != nil {
			return err
		}
		escaped = wb
		return wb.Put("k", []byte(`{"n":1}`))
	}); err != nil {
		t.Fatalf("UpdateCollections: %v", err)
	}
	if err := escaped.Put("k2", []byte(`{"n":2}`)); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("escaped Put=%v want ErrBatchClosed", err)
	}
}

func TestDatabaseUpdateConvenience(t *testing.T) {
	var db Database
	mustCollection(t, &db, "orders")
	mustCollection(t, &db, "customers")
	if err := db.Update(func(b *DatabaseBatch) error {
		ob, err := b.Collection("orders")
		if err != nil {
			return err
		}
		cb, err := b.Collection("customers")
		if err != nil {
			return err
		}
		if err := ob.Put("o1", []byte(`{"n":1}`)); err != nil {
			return err
		}
		return cb.Put("c1", []byte(`{"n":1}`))
	}); err != nil {
		t.Fatalf("Database.Update: %v", err)
	}
	if got := db.Snapshot().Len(); got != 2 {
		t.Fatalf("Len=%d", got)
	}
}

func TestDatabaseUpdateCallbackMayChangeCatalog(t *testing.T) {
	var db Database
	mustCollection(t, &db, "existing")

	done := make(chan error, 1)
	go func() {
		done <- db.Update(func(*DatabaseBatch) error {
			_, err := db.CreateCollection("created", Options{})
			return err
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Database.Update callback catalog change: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Database.Update held the catalog lock while invoking its callback")
	}
	if _, ok := db.Collection("created"); !ok {
		t.Fatal("callback-created collection was not published")
	}
}

// Given concurrent multi-collection commits and Database.Snapshot cuts, every
// cut observes each transaction all-or-nothing: the paired keys either both
// appear or neither does.
func TestUpdateCollectionsSnapshotCutAllOrNothing(t *testing.T) {
	const (
		writers = 4
		commits = 200
	)
	var db Database
	left := mustCollection(t, &db, "left")
	right := mustCollection(t, &db, "right")
	// A third collection is mutated by ordinary Put so single-collection
	// writers remain live beside multi-collection commits.
	other := mustCollection(t, &db, "other")

	var (
		writersWG   sync.WaitGroup
		helpersWG   sync.WaitGroup
		stop        = make(chan struct{})
		commitsDone atomic.Int64
		pointWrites atomic.Int64
	)

	for w := 0; w < writers; w++ {
		writersWG.Add(1)
		go func(w int) {
			defer writersWG.Done()
			for i := 0; i < commits; i++ {
				key := fmt.Sprintf("w%d-%d", w, i)
				doc := fmt.Appendf(nil, `{"w":%d,"i":%d}`, w, i)
				err := UpdateCollections([]*Collection{left, right}, func(b *DatabaseBatch) error {
					lb, err := b.Collection("left")
					if err != nil {
						return err
					}
					rb, err := b.Collection("right")
					if err != nil {
						return err
					}
					if err := lb.Put(key, doc); err != nil {
						return err
					}
					return rb.Put(key, doc)
				})
				if err != nil {
					t.Errorf("UpdateCollections: %v", err)
					return
				}
				commitsDone.Add(1)
			}
		}(w)
	}

	helpersWG.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := other.Put(fmt.Sprintf("p%d", i), []byte(`{"v":1}`)); err != nil {
				t.Errorf("Put: %v", err)
				return
			}
			pointWrites.Add(1)
		}
	})

	helpersWG.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			snapshot := db.Snapshot()
			leftView, _ := snapshot.Collection("left")
			rightView, _ := snapshot.Collection("right")
			leftKeys := map[string]struct{}{}
			leftView.Range(func(key string, _ vibejson.RawValue) bool {
				leftKeys[key] = struct{}{}
				return true
			})
			torn := false
			rightView.Range(func(key string, _ vibejson.RawValue) bool {
				if _, ok := leftKeys[key]; !ok {
					t.Errorf("snapshot tear: right has %q, left does not", key)
					torn = true
					return false
				}
				delete(leftKeys, key)
				return true
			})
			if torn || t.Failed() {
				return
			}
			for key := range leftKeys {
				t.Errorf("snapshot tear: left has %q, right does not", key)
				return
			}
		}
	})

	writersWG.Wait()
	close(stop)
	helpersWG.Wait()

	if commitsDone.Load() != int64(writers*commits) {
		t.Fatalf("commits=%d want %d", commitsDone.Load(), writers*commits)
	}
	if pointWrites.Load() == 0 {
		t.Fatal("single-collection writer made no progress")
	}
	final := db.Snapshot()
	leftView, _ := final.Collection("left")
	rightView, _ := final.Collection("right")
	if leftView.Len() != writers*commits || rightView.Len() != writers*commits {
		t.Fatalf("final lens left=%d right=%d", leftView.Len(), rightView.Len())
	}
}

func TestUpdateCollectionsSingleCollectionReadersUnaffected(t *testing.T) {
	var db Database
	a := mustCollection(t, &db, "a")
	b := mustCollection(t, &db, "b")
	if _, err := a.Put("seed", []byte(`{"n":0}`)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	snap, err := a.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, ok := a.GetRaw("seed"); !ok {
				t.Errorf("seed missing under concurrent multi-collection writes")
				return
			}
			if raw, ok := snap.GetRaw("seed"); !ok || string(raw.Bytes()) != `{"n":0}` {
				t.Errorf("stable snapshot moved: %q,%v", raw.Bytes(), ok)
				return
			}
			if _, err := a.Put("seed", []byte(`{"n":1}`)); err != nil {
				t.Errorf("Put: %v", err)
				return
			}
		}
	})

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("k%d", i)
		if err := UpdateCollections([]*Collection{a, b}, func(batch *DatabaseBatch) error {
			ab, _ := batch.Collection("a")
			bb, _ := batch.Collection("b")
			if err := ab.Put(key, []byte(`{"x":1}`)); err != nil {
				return err
			}
			return bb.Put(key, []byte(`{"x":1}`))
		}); err != nil {
			t.Fatalf("UpdateCollections: %v", err)
		}
	}
	close(stop)
	wg.Wait()
}

func TestMultiCollectionDeleteAndReplace(t *testing.T) {
	var db Database
	a := mustCollection(t, &db, "a")
	b := mustCollection(t, &db, "b")
	if err := UpdateCollections([]*Collection{a, b}, func(batch *DatabaseBatch) error {
		ab, _ := batch.Collection("a")
		bb, _ := batch.Collection("b")
		if err := ab.Put("k", []byte(`{"n":1}`)); err != nil {
			return err
		}
		return bb.Put("k", []byte(`{"n":1}`))
	}); err != nil {
		t.Fatalf("seed txn: %v", err)
	}
	if err := UpdateCollections([]*Collection{a, b}, func(batch *DatabaseBatch) error {
		ab, _ := batch.Collection("a")
		bb, _ := batch.Collection("b")
		if err := ab.Put("k", []byte(`{"n":2}`)); err != nil {
			return err
		}
		return bb.Delete("k")
	}); err != nil {
		t.Fatalf("mutate txn: %v", err)
	}
	if raw, ok := mustSnapshotRaw(t, a, "k"); !ok || string(raw) != `{"n":2}` {
		t.Fatalf("a.k=%q,%v", raw, ok)
	}
	if _, ok := mustSnapshotRaw(t, b, "k"); ok {
		t.Fatal("b.k still present")
	}
}

func mustSnapshotRaw(t *testing.T, c *Collection, key string) ([]byte, bool) {
	t.Helper()
	snap, err := c.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	raw, ok := snap.GetRaw(key)
	if !ok {
		return nil, false
	}
	return append([]byte(nil), raw.Bytes()...), true
}
