package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func mustCollection(t testing.TB, d *Database, name string) *Collection {
	t.Helper()
	c, err := d.CreateCollection(name, Options{ChunkDocuments: 8})
	if err != nil {
		t.Fatalf("CreateCollection(%s): %v", name, err)
	}
	return c
}

// Given a database of several collections, when a snapshot is taken, then it
// captures each one and resolves them by name in catalog order.
func TestDatabaseSnapshotCapturesEveryCollection(t *testing.T) {
	var db Database
	orders := mustCollection(t, &db, "orders")
	customers := mustCollection(t, &db, "customers")

	if _, err := orders.Put("o1", []byte(`{"customer":"c1"}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := customers.Put("c1", []byte(`{"tier":"pro"}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	snapshot := db.Snapshot()
	if snapshot.Len() != 2 {
		t.Fatalf("Len=%d want 2", snapshot.Len())
	}
	if names := snapshot.AppendNames(nil); len(names) != 2 || names[0] != "customers" || names[1] != "orders" {
		t.Fatalf("AppendNames=%v want [customers orders]", names)
	}
	view, ok := snapshot.Collection("orders")
	if !ok {
		t.Fatal("orders absent from the snapshot")
	}
	if raw, found := view.GetRaw("o1"); !found || string(raw.Bytes()) != `{"customer":"c1"}` {
		t.Fatalf("GetRaw(o1)=%q,%v", raw.Bytes(), found)
	}
	if _, ok := snapshot.Collection("absent"); ok {
		t.Fatal("an uncataloged name resolved")
	}
}

// Given a snapshot, when the database is mutated afterwards, then the snapshot
// keeps the state it captured — the ordinary immutability contract, extended
// across collections.
func TestDatabaseSnapshotIsIndependentOfLaterMutation(t *testing.T) {
	var db Database
	orders := mustCollection(t, &db, "orders")
	if _, err := orders.Put("o1", []byte(`{"n":1}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	snapshot := db.Snapshot()

	if _, err := orders.Put("o1", []byte(`{"n":2}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := orders.Put("o2", []byte(`{"n":3}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	mustCollection(t, &db, "later")
	db.DropCollection("orders")

	view, ok := snapshot.Collection("orders")
	if !ok {
		t.Fatal("a dropped collection must remain in a snapshot taken before the drop")
	}
	if raw, _ := view.GetRaw("o1"); string(raw.Bytes()) != `{"n":1}` {
		t.Fatalf("GetRaw(o1)=%q want the captured value", raw.Bytes())
	}
	if _, found := view.GetRaw("o2"); found {
		t.Fatal("a key written after the snapshot is visible in it")
	}
	if _, ok := snapshot.Collection("later"); ok {
		t.Fatal("a collection created after the snapshot is present in it")
	}
}

// Given a zero Database and a zero DatabaseSnapshot, when they are used, then
// they behave as empty rather than panicking.
func TestDatabaseSnapshotZeroValues(t *testing.T) {
	var db Database
	if snapshot := db.Snapshot(); snapshot.Len() != 0 {
		t.Fatalf("empty database: Len=%d want 0", snapshot.Len())
	}
	var zero DatabaseSnapshot
	if zero.Len() != 0 || zero.AppendNames(nil) != nil {
		t.Fatal("the zero snapshot is not empty")
	}
	if _, ok := zero.Collection("anything"); ok {
		t.Fatal("the zero snapshot resolved a name")
	}
	zero.All(func(string, Snapshot) bool {
		t.Fatal("the zero snapshot iterated an entry")
		return false
	})
	var nilDB *Database
	if snapshot := nilDB.Snapshot(); snapshot.Len() != 0 {
		t.Fatal("a nil database produced a non-empty snapshot")
	}
}

// Given writers mutating every collection concurrently, when snapshots are
// taken throughout, then each one observes a single instant.
//
// The invariant is enforced by construction rather than asserted
// statistically. Writers bracket each Put with two counters: started is
// incremented before the write begins, finished after it has been published.
// That ordering is what makes the bound sound in both directions.
//
//	finished(before the capture)  <=  documents observed  <=  started(after it)
//
// The lower bound holds because a finished write was published before the
// capture began, so its document must be visible. The upper bound holds
// because a visible document's write had necessarily started before the
// capture read it. Counting only completed writes would make the upper bound
// racy — a writer can publish its state and be preempted before recording the
// completion, so a legitimately visible document would appear to exceed the
// count. A skewed multi-pointer read breaks these bounds by summing states
// that never coexisted.
func TestDatabaseSnapshotIsASingleInstant(t *testing.T) {
	const (
		collections = 4
		writers     = 4
		writes      = 300
	)
	var db Database
	handles := make([]*Collection, collections)
	for i := range handles {
		handles[i] = mustCollection(t, &db, fmt.Sprintf("c%d", i))
	}

	var started, finished atomic.Int64

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range writes {
				c := handles[(w+i)%collections]
				started.Add(1)
				if _, err := c.Put(fmt.Sprintf("w%d-%d", w, i), fmt.Appendf(nil, `{"w":%d,"i":%d}`, w, i)); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				finished.Add(1)
			}
		}(w)
	}

	var wgReader sync.WaitGroup
	wgReader.Go(func() {
		captures := 0
		for {
			select {
			case <-stop:
				t.Logf("verified %d captures against the commit bounds", captures)
				return
			default:
			}
			lower := finished.Load()
			snapshot := db.Snapshot()
			upper := started.Load()
			captures++

			total := 0
			snapshot.All(func(_ string, view Snapshot) bool {
				total += view.Len()
				return true
			})
			if int64(total) < lower || int64(total) > upper {
				t.Errorf("snapshot observed %d documents, outside [%d, %d]: "+
					"the collections were not read at one instant", total, lower, upper)
				return
			}
		}
	})

	wg.Wait()
	close(stop)
	wgReader.Wait()

	final := db.Snapshot()
	total := 0
	final.All(func(_ string, view Snapshot) bool {
		total += view.Len()
		return true
	})
	if want := writers * writes; total != want {
		t.Fatalf("final snapshot holds %d documents, want %d", total, want)
	}
}

// Given collections whose names sort differently from their map iteration
// order, when snapshots are taken concurrently from several goroutines, then
// the fixed lock order keeps the capture deadlock-free.
func TestDatabaseSnapshotConcurrentCapturesDoNotDeadlock(t *testing.T) {
	var db Database
	for _, name := range []string{"zeta", "alpha", "mu", "beta", "omega"} {
		c := mustCollection(t, &db, name)
		if _, err := c.Put("k", []byte(`{"v":1}`)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 200 {
				if snapshot := db.Snapshot(); snapshot.Len() != 5 {
					t.Errorf("Len=%d want 5", snapshot.Len())
					return
				}
			}
		})
	}
	wg.Wait()
}
