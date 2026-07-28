package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testDatabaseOptions widen the single-collection test geometry where a
// multi-collection capture needs it: one capture holds a lease on every
// collection at once, and several concurrent captures multiply that, so the
// eight-lease bound the single-file tests pin would be exhausted by the
// capture itself rather than by anything the test means to exercise.
func testDatabaseOptions() Options {
	options := testFileStoreOptions()
	options.MaxSnapshotLeases = 256
	options.MaxRetiredExtents = 4096
	return options
}

func newTestDatabase(t testing.TB, names ...string) *Database {
	t.Helper()
	db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, name := range names {
		if _, err := db.CreateCollection(name, testDatabaseOptions()); err != nil {
			t.Fatalf("CreateCollection(%s): %v", name, err)
		}
	}
	return db
}

func mustPut(t testing.TB, c *Collection, key, doc string) {
	t.Helper()
	if _, err := c.Put(key, []byte(doc)); err != nil {
		t.Fatalf("Put(%s): %v", key, err)
	}
}

// Given a database of several collections, when a snapshot is taken, then it
// captures each one and resolves them by name in catalog order.
func TestDurableDatabaseSnapshotCapturesEveryCollection(t *testing.T) {
	db := newTestDatabase(t, "orders", "customers")
	orders, _ := db.Collection("orders")
	customers, _ := db.Collection("customers")
	mustPut(t, orders, "o1", `{"customer":"c1"}`)
	mustPut(t, customers, "c1", `{"tier":"pro"}`)

	snapshot, err := db.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer func() { _ = snapshot.Close() }()

	if snapshot.Len() != 2 {
		t.Fatalf("Len=%d want 2", snapshot.Len())
	}
	names := snapshot.AppendNames(nil)
	if len(names) != 2 || names[0] != "customers" || names[1] != "orders" {
		t.Fatalf("AppendNames=%v want [customers orders]", names)
	}
	view, ok := snapshot.Collection("orders")
	if !ok {
		t.Fatal("orders absent from the snapshot")
	}
	raw, found, err := view.AppendRaw(nil, "o1")
	if err != nil || !found || string(raw) != `{"customer":"c1"}` {
		t.Fatalf("AppendRaw(o1)=%q,%v,%v", raw, found, err)
	}
	if _, ok := snapshot.Collection("absent"); ok {
		t.Fatal("an uncataloged name resolved")
	}
}

// Given a snapshot, when the collections are mutated afterwards, then the
// snapshot keeps the state it captured.
func TestDurableDatabaseSnapshotIsIndependentOfLaterMutation(t *testing.T) {
	db := newTestDatabase(t, "orders")
	orders, _ := db.Collection("orders")
	mustPut(t, orders, "o1", `{"n":1}`)

	snapshot, err := db.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer func() { _ = snapshot.Close() }()

	mustPut(t, orders, "o1", `{"n":2}`)
	mustPut(t, orders, "o2", `{"n":3}`)
	if _, err := db.CreateCollection("later", testDatabaseOptions()); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	view, _ := snapshot.Collection("orders")
	raw, _, err := view.AppendRaw(nil, "o1")
	if err != nil {
		t.Fatalf("AppendRaw: %v", err)
	}
	if string(raw) != `{"n":1}` {
		t.Fatalf("AppendRaw(o1)=%q want the captured value", raw)
	}
	if _, found, _ := view.AppendRaw(nil, "o2"); found {
		t.Fatal("a key written after the snapshot is visible in it")
	}
	if _, ok := snapshot.Collection("later"); ok {
		t.Fatal("a collection created after the snapshot is present in it")
	}
}

// Given writers committing to every collection concurrently, when snapshots are
// taken throughout, then every capture stays within the commit bounds.
//
// Writers bracket each Put with two counters: started before the write begins,
// finished after it has been published. That ordering is what makes the bound
// sound in both directions.
//
//	finished(before the capture)  <=  documents observed  <=  started(after it)
//
// The lower bound holds because a finished write was published before the
// capture began, so its document must be visible. The upper bound holds because
// a visible document's write had necessarily started before the capture read
// it. A skewed multi-collection read breaks these bounds by summing generations
// that never coexisted.
//
// What this test is worth is worth stating plainly, because the heap catalog's
// identically shaped test is worth more. There, writes are cheap enough that
// the skew window is a real fraction of the loop and an unsynchronized capture
// is caught. Here a Put costs an fsync, so the window between two independently
// taken snapshots is nanoseconds against milliseconds of write latency, and a
// deliberately skewing implementation passes this test — it was tried. So this
// is an end-to-end sanity check on the bounds and on lease reuse, and it is not
// the evidence that the cut is a cut.
// TestDurableDatabaseSnapshotHoldsEveryGateAtOnce is.
func TestDurableDatabaseSnapshotIsASingleInstant(t *testing.T) {
	const (
		collections = 3
		writers     = 3
		writes      = 60
	)
	names := make([]string, collections)
	for i := range names {
		names[i] = fmt.Sprintf("c%d", i)
	}
	db := newTestDatabase(t, names...)
	handles := make([]*Collection, collections)
	for i, name := range names {
		handles[i], _ = db.Collection(name)
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

	var reader sync.WaitGroup
	reader.Go(func() {
		var capture DatabaseSnapshot
		defer func() { _ = capture.Close() }()
		captures := 0
		for {
			select {
			case <-stop:
				t.Logf("verified %d captures against the commit bounds", captures)
				return
			default:
			}
			lower := finished.Load()
			if err := db.SnapshotInto(&capture); err != nil {
				t.Errorf("SnapshotInto: %v", err)
				return
			}
			upper := started.Load()
			captures++

			total := uint64(0)
			capture.All(func(_ string, view *Snapshot) bool {
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
	reader.Wait()

	final, err := db.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer func() { _ = final.Close() }()
	total := uint64(0)
	final.All(func(_ string, view *Snapshot) bool {
		total += view.Len()
		return true
	})
	if want := uint64(writers * writes); total != want {
		t.Fatalf("final snapshot holds %d documents, want %d", total, want)
	}
}

// Given a commit in flight on the last collection in name order, when a
// capture runs, then it is already holding the first collection's publication
// gate — every gate is held at once, not one at a time.
//
// This is the deterministic proof that the capture cannot skew, and it exists
// because the statistical form above is not one. A durable Put costs an fsync,
// so the window between two independently taken snapshots is nanoseconds
// against a write latency of milliseconds; a loop of concurrent writers and
// captures never lands in it, and a version that takes N independent snapshots
// passes that test comfortably. Asserting the lock protocol directly is what
// closes the gap, and the protocol is the whole mechanism: skew is possible
// exactly when some collection's gate is free while another's lease is being
// taken.
//
// The gate's write side is what a commit holds across its state swap, so
// taking it here stands in for a commit publishing on "b" without needing to
// pause one mid-flight. "a" sorts first, so a correct capture holds "a" on the
// read side while it blocks on "b", and a write-lock attempt on "a" must fail.
// A capture that released "a" before reaching "b" leaves that attempt
// succeeding forever, which is what the deadline detects.
func TestDurableDatabaseSnapshotHoldsEveryGateAtOnce(t *testing.T) {
	db := newTestDatabase(t, "a", "b")
	a, _ := db.Collection("a")
	b, _ := db.Collection("b")
	mustPut(t, a, "k", `{"v":1}`)
	mustPut(t, b, "k", `{"v":1}`)

	b.snapshotGate.Lock()

	done := make(chan error, 1)
	go func() {
		var capture DatabaseSnapshot
		err := db.SnapshotInto(&capture)
		if closeErr := capture.Close(); err == nil {
			err = closeErr
		}
		done <- err
	}()

	held := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		if a.snapshotGate.TryLock() {
			a.snapshotGate.Unlock()
			time.Sleep(200 * time.Microsecond)
			continue
		}
		held = true
		break
	}
	b.snapshotGate.Unlock()

	if err := <-done; err != nil {
		t.Fatalf("SnapshotInto: %v", err)
	}
	if !held {
		t.Fatal("the capture blocked on the second collection's gate without " +
			"holding the first one's: the two leases are taken at different " +
			"instants, so a commit between them would skew the cut")
	}
}

// Given collections whose names sort differently from their map iteration
// order, when snapshots are taken concurrently from several goroutines, then
// the fixed acquisition order keeps the capture deadlock-free.
func TestDurableDatabaseSnapshotConcurrentCapturesDoNotDeadlock(t *testing.T) {
	db := newTestDatabase(t, "zeta", "alpha", "mu", "beta", "omega")
	db.All(func(_ string, c *Collection) bool {
		mustPut(t, c, "k", `{"v":1}`)
		return true
	})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			var capture DatabaseSnapshot
			defer func() { _ = capture.Close() }()
			for range 100 {
				if err := db.SnapshotInto(&capture); err != nil {
					t.Errorf("SnapshotInto: %v", err)
					return
				}
				if capture.Len() != 5 {
					t.Errorf("Len=%d want 5", capture.Len())
					return
				}
			}
		})
	}
	wg.Wait()
}

// Given a database directory written by one process, when it is reopened, then
// every collection comes back with its committed contents.
func TestDurableDatabaseReopensItsDirectory(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDatabase(dir, DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	for _, name := range []string{"orders", "customers"} {
		c, err := db.CreateCollection(name, testDatabaseOptions())
		if err != nil {
			t.Fatalf("CreateCollection: %v", err)
		}
		mustPut(t, c, name+"-1", fmt.Sprintf(`{"in":%q}`, name))
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenDatabase(dir, DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if names := reopened.Names(nil); len(names) != 2 || names[0] != "customers" || names[1] != "orders" {
		t.Fatalf("Names=%v want [customers orders]", names)
	}
	snapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer func() { _ = snapshot.Close() }()
	view, _ := snapshot.Collection("orders")
	raw, found, err := view.AppendRaw(nil, "orders-1")
	if err != nil || !found || string(raw) != `{"in":"orders"}` {
		t.Fatalf("AppendRaw=%q,%v,%v", raw, found, err)
	}
}

// Given a catalog, when names are created, dropped, and re-created, then the
// file layout follows and a dropped name is reusable.
func TestDurableDatabaseCatalogLifecycle(t *testing.T) {
	db := newTestDatabase(t, "orders")
	if _, err := db.CreateCollection("orders", testDatabaseOptions()); err != ErrCollectionExists {
		t.Fatalf("duplicate create: %v want %v", err, ErrCollectionExists)
	}
	path := filepath.Join(db.Dir(), "orders"+collectionFileSuffix)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("collection file missing: %v", err)
	}
	if err := db.DropCollection("orders"); err != nil {
		t.Fatalf("DropCollection: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("dropped collection file survives: %v", err)
	}
	if _, ok := db.Collection("orders"); ok {
		t.Fatal("a dropped name still resolves")
	}
	if err := db.DropCollection("orders"); err != nil {
		t.Fatalf("dropping an absent name: %v", err)
	}
	if _, err := db.CreateCollection("orders", testDatabaseOptions()); err != nil {
		t.Fatalf("re-create after drop: %v", err)
	}
}

// Given names that would escape the database directory or collide with its
// own file convention, when they are used, then creation is refused.
func TestDurableDatabaseRejectsUnsafeCollectionNames(t *testing.T) {
	db := newTestDatabase(t)
	for _, name := range []string{
		"", ".", "..", "a/b", "a" + string(os.PathSeparator) + "b",
		"x" + collectionFileSuffix, "nul\x00byte", "\xff\xfe",
	} {
		if _, err := db.CreateCollection(name, testDatabaseOptions()); err != ErrCollectionName {
			t.Errorf("CreateCollection(%q)=%v want %v", name, err, ErrCollectionName)
		}
	}
}

// Given a zero and a closed Database, when they are used, then they report
// closure rather than panicking.
func TestDurableDatabaseZeroAndClosed(t *testing.T) {
	var nilDB *Database
	if _, err := nilDB.Snapshot(); err != ErrDatabaseClosed {
		t.Fatalf("nil Snapshot=%v", err)
	}
	if nilDB.Len() != 0 || nilDB.Names(nil) != nil || nilDB.Dir() != "" {
		t.Fatal("a nil database reported contents")
	}
	if err := nilDB.Close(); err != nil {
		t.Fatalf("nil Close=%v", err)
	}

	db := newTestDatabase(t, "orders")
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := db.Snapshot(); err != ErrDatabaseClosed {
		t.Fatalf("closed Snapshot=%v", err)
	}
	if _, err := db.CreateCollection("more", testDatabaseOptions()); err != ErrDatabaseClosed {
		t.Fatalf("closed CreateCollection=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close=%v", err)
	}

	var zero DatabaseSnapshot
	if zero.Len() != 0 || zero.AppendNames(nil) != nil {
		t.Fatal("the zero snapshot is not empty")
	}
	if _, ok := zero.Collection("anything"); ok {
		t.Fatal("the zero snapshot resolved a name")
	}
	zero.All(func(string, *Snapshot) bool {
		t.Fatal("the zero snapshot iterated an entry")
		return false
	})
	if err := zero.Close(); err != nil {
		t.Fatalf("zero Close=%v", err)
	}
}

// Given a capture written into reused storage, when SnapshotInto is called
// again, then the previous capture's leases are released rather than leaked.
//
// The bound is what proves it: with N leases configured, a loop of more than N
// captures that never released would fail on the first capture past the bound.
func TestDurableDatabaseSnapshotIntoReleasesThePreviousCapture(t *testing.T) {
	options := testDatabaseOptions()
	options.MaxSnapshotLeases = 4
	db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: options})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer func() { _ = db.Close() }()
	c, err := db.CreateCollection("orders", options)
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	mustPut(t, c, "o1", `{"n":1}`)

	var capture DatabaseSnapshot
	for i := range 40 {
		if err := db.SnapshotInto(&capture); err != nil {
			t.Fatalf("SnapshotInto #%d: %v", i, err)
		}
		if capture.Len() != 1 {
			t.Fatalf("Len=%d want 1", capture.Len())
		}
	}
	if err := capture.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := capture.Close(); err != nil {
		t.Fatalf("idempotent Close: %v", err)
	}
}
