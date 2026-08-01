package durable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/internal/storeio"
)

func testCollectionFilename(t testing.TB, name string) string {
	t.Helper()
	filename, ok := collectionname.Encode(name)
	if !ok {
		t.Fatalf("invalid test collection name %q", name)
	}
	return filename
}

func TestDurableDatabaseAppliesConfiguredCollectionFileMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose Unix permission bits")
	}
	dir := t.TempDir()
	db, err := OpenDatabase(dir, DatabaseOptions{
		Options: testDatabaseOptions(), FileMode: 0o700,
	})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.CreateCollection("private", testDatabaseOptions()); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	filename := testCollectionFilename(t, "private")
	info, err := os.Stat(filepath.Join(dir, filename))
	if err != nil {
		t.Fatalf("Stat collection: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("collection mode = %#o, want %#o", got, os.FileMode(0o700))
	}
	journalInfo, err := os.Stat(filepath.Join(
		dir, filename+recoveryJournalSuffix,
	))
	if err != nil {
		t.Fatalf("Stat recovery journal: %v", err)
	}
	if got := journalInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("recovery journal mode = %#o, want %#o", got, os.FileMode(0o700))
	}
}

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
	if _, err := c.Put([]byte(key), []byte(doc)); err != nil {
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
	raw, found, err := view.AppendRaw(nil, []byte("o1"))
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
	raw, _, err := view.AppendRaw(nil, []byte("o1"))
	if err != nil {
		t.Fatalf("AppendRaw: %v", err)
	}
	if string(raw) != `{"n":1}` {
		t.Fatalf("AppendRaw(o1)=%q want the captured value", raw)
	}
	if _, found, _ := view.AppendRaw(nil, []byte("o2")); found {
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
				if _, err := c.Put([]byte(fmt.Sprintf("w%d-%d", w, i)), fmt.Appendf(nil, `{"w":%d,"i":%d}`, w, i)); err != nil {
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
	raw, found, err := view.AppendRaw(nil, []byte("orders-1"))
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
	path := filepath.Join(db.Dir(), testCollectionFilename(t, "orders"))
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("collection file missing: %v", err)
	}
	journalPath := RecoveryJournalPath(path)
	if _, err := os.Stat(journalPath); err != nil {
		t.Fatalf("collection journal missing: %v", err)
	}
	if err := db.DropCollection("orders"); err != nil {
		t.Fatalf("DropCollection: %v", err)
	}
	for _, removed := range []string{path, journalPath} {
		if _, err := os.Stat(removed); !os.IsNotExist(err) {
			t.Fatalf("dropped collection file %q survives: %v", removed, err)
		}
	}
	if _, ok := db.Collection("orders"); ok {
		t.Fatal("a dropped name still resolves")
	}
	if err := db.DropCollection("orders"); err != nil {
		t.Fatalf("dropping an absent name: %v", err)
	}
	reopened, err := OpenDatabase(db.Dir(), DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		t.Fatalf("reopen after drop: %v", err)
	}
	if got := reopened.Names(nil); len(got) != 0 {
		t.Fatalf("reopen after drop found %q", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopen after drop: %v", err)
	}
	if _, err := db.CreateCollection("orders", testDatabaseOptions()); err != nil {
		t.Fatalf("re-create after drop: %v", err)
	}
	for _, recreated := range []string{path, journalPath} {
		if _, err := os.Stat(recreated); err != nil {
			t.Fatalf("recreated collection file %q missing: %v", recreated, err)
		}
	}
}

func TestDurableDatabaseDropHidesNameWhileSnapshotDelaysClose(t *testing.T) {
	db := newTestDatabase(t, "orders")
	orders, _ := db.Collection("orders")
	mustPut(t, orders, "1", `{"ok":true}`)
	snapshot, err := orders.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	first := db.DropCollection("orders")
	if !errors.Is(first, storeio.ErrLeasesActive) {
		_ = snapshot.Close()
		t.Fatalf("DropCollection with active snapshot = %v, want %v",
			first, storeio.ErrLeasesActive)
	}
	if _, ok := db.Collection("orders"); ok || db.Len() != 0 || len(db.Names(nil)) != 0 {
		_ = snapshot.Close()
		t.Fatalf("pending delete remained visible: collection %v len %d names %q",
			ok, db.Len(), db.Names(nil))
	}
	cut, err := db.Snapshot()
	if err != nil {
		_ = snapshot.Close()
		t.Fatal(err)
	}
	if cut.Len() != 0 {
		_ = cut.Close()
		_ = snapshot.Close()
		t.Fatalf("database snapshot retained pending delete: %d collections", cut.Len())
	}
	if err := cut.Close(); err != nil {
		_ = snapshot.Close()
		t.Fatal(err)
	}
	if _, err := db.CreateCollection("orders", testDatabaseOptions()); !errors.Is(err, ErrCollectionExists) {
		_ = snapshot.Close()
		t.Fatalf("pending-delete name reuse = %v, want %v", err, ErrCollectionExists)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.DropCollection("orders"); err != nil {
		t.Fatalf("DropCollection retry: %v", err)
	}
	if _, err := db.CreateCollection("orders", testDatabaseOptions()); err != nil {
		t.Fatalf("name was not reusable after completed drop: %v", err)
	}
}

func TestDurableDatabaseDropRemainsRetryableDuringJournalCleanup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not unlink an open recovery journal")
	}
	db := newTestDatabase(t, "orders")
	primary := filepath.Join(db.Dir(), testCollectionFilename(t, "orders"))
	journal := RecoveryJournalPath(primary)
	if err := os.Remove(journal); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(journal, 0o700); err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(journal, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := db.DropCollection("orders"); err == nil {
		t.Fatal("DropCollection succeeded with non-removable journal path")
	}
	if _, ok := db.Collection("orders"); ok || db.Len() != 0 {
		t.Fatal("pending-delete collection remained visible")
	}
	if _, err := db.CreateCollection("orders", testDatabaseOptions()); err != ErrCollectionExists {
		t.Fatalf("create over pending delete = %v, want ErrCollectionExists", err)
	}
	if _, err := os.Stat(primary); !os.IsNotExist(err) {
		t.Fatalf("primary was not durably removed before journal cleanup: %v", err)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(journal); err != nil {
		t.Fatal(err)
	}
	if err := db.DropCollection("orders"); err != nil {
		t.Fatalf("retry DropCollection: %v", err)
	}
	if _, err := db.CreateCollection("orders", testDatabaseOptions()); err != nil {
		t.Fatalf("create after completed retry: %v", err)
	}
}

func TestDurableDatabaseDropCompletesAfterTerminalPersistenceError(t *testing.T) {
	getFault, restore := installJournalFaultSeam(t)
	defer restore()
	dir := t.TempDir()
	options := testDatabaseOptions()
	db, err := OpenDatabase(dir, DatabaseOptions{Options: options})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection, err := db.CreateCollection("orders", options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put([]byte("baseline"), []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	fault := getFault()
	if fault == nil {
		t.Fatal("journal fault seam was not installed")
	}
	fault.Program(storeio.JournalFaultPlan{
		Phase:       storeio.JournalFaultENOSPCAppend,
		AppendIndex: fault.Appends(),
	})
	if _, err := collection.Put([]byte("rejected"), []byte(`{"n":2}`)); err == nil {
		t.Fatal("faulted mutation succeeded")
	}
	persistErr := collection.PersistenceError()
	if persistErr == nil || !fault.Faulted() {
		t.Fatalf("terminal persistence state = %v, fired=%v", persistErr, fault.Faulted())
	}
	primary := filepath.Join(dir, testCollectionFilename(t, "orders"))
	journal := RecoveryJournalPath(primary)
	dropErr := db.DropCollection("orders")
	if !errors.Is(dropErr, persistErr) {
		t.Fatalf("DropCollection error = %v, want terminal %v", dropErr, persistErr)
	}
	if !collection.CloseCompleted() {
		t.Fatal("terminal DropCollection left engine cleanup incomplete")
	}
	if _, ok := db.Collection("orders"); ok || db.Len() != 0 {
		t.Fatal("terminal DropCollection left the name cataloged")
	}
	for _, removed := range []string{primary, journal} {
		if _, err := os.Stat(removed); !os.IsNotExist(err) {
			t.Fatalf("terminal DropCollection retained %q: %v", removed, err)
		}
	}
	if err := db.DropCollection("orders"); err != nil {
		t.Fatalf("retry after completed terminal DropCollection: %v", err)
	}
	if _, err := db.CreateCollection("orders", options); err != nil {
		t.Fatalf("recreate after terminal DropCollection: %v", err)
	}
}

func TestDurableDatabaseOpenRemovesCanonicalOrphanJournals(t *testing.T) {
	dir := t.TempDir()
	options := testDatabaseOptions()
	db, err := OpenDatabase(dir, DatabaseOptions{Options: options})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCollection("orders", options); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	primary := filepath.Join(dir, testCollectionFilename(t, "orders"))
	journal := RecoveryJournalPath(primary)
	if err := os.Remove(primary); err != nil {
		t.Fatal(err)
	}
	unrelated := filepath.Join(dir, "application.rjournal")
	if err := os.WriteFile(unrelated, []byte("owned elsewhere"), 0o600); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenDatabase(dir, DatabaseOptions{Options: options})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Len() != 0 {
		t.Fatalf("catalog length after orphan cleanup = %d, want 0", reopened.Len())
	}
	if _, err := os.Stat(journal); !os.IsNotExist(err) {
		t.Fatalf("canonical orphan journal survived startup: %v", err)
	}
	if contents, err := os.ReadFile(unrelated); err != nil || string(contents) != "owned elsewhere" {
		t.Fatalf("unrelated sidecar changed = %q, %v", contents, err)
	}
}

func TestDurableDatabaseOpenFailsWhenOrphanJournalCleanupFails(t *testing.T) {
	dir := t.TempDir()
	filename := testCollectionFilename(t, "orders") + collectionname.JournalSuffix
	orphan := filepath.Join(dir, filename)
	if err := os.Mkdir(orphan, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphan, "blocker"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDatabase(dir, DatabaseOptions{Options: testDatabaseOptions()}); err == nil {
		t.Fatal("OpenDatabase accepted an orphan journal it could not remove")
	}
}

func TestDurableDatabaseCloseRetainsOwnershipUntilSnapshotRelease(t *testing.T) {
	db := newTestDatabase(t, "orders")
	dir := db.Dir()
	collection, _ := db.Collection("orders")
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	entry := db.collections["orders"]
	first := db.Close()
	if !errors.Is(first, storeio.ErrLeasesActive) {
		_ = snapshot.Close()
		t.Fatalf("first Database.Close = %v, want %v", first, storeio.ErrLeasesActive)
	}
	if db.CloseCompleted() || !db.closed || db.collections == nil || entry.closeDone {
		_ = snapshot.Close()
		t.Fatalf("incomplete database close = completed %v closed %v collections nil %v entry done %v",
			db.CloseCompleted(), db.closed, db.collections == nil, entry.closeDone)
	}
	if _, err := entry.file.Stat(); err != nil {
		_ = snapshot.Close()
		t.Fatalf("owner descriptor closed while engine teardown was retryable: %v", err)
	}
	if _, ok := db.Collection("orders"); ok {
		_ = snapshot.Close()
		t.Fatal("closing database still admitted catalog lookup")
	}
	if _, err := db.CreateCollection("later", testDatabaseOptions()); !errors.Is(err, ErrDatabaseClosed) {
		_ = snapshot.Close()
		t.Fatalf("create during closing = %v, want %v", err, ErrDatabaseClosed)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	second := db.Close()
	if second != nil {
		t.Fatalf("retry Database.Close = %v", second)
	}
	if !db.CloseCompleted() || db.collections != nil || !entry.closeDone {
		t.Fatalf("completed database close = completed %v collections nil %v entry done %v",
			db.CloseCompleted(), db.collections == nil, entry.closeDone)
	}
	if _, err := entry.file.Stat(); err == nil {
		t.Fatal("owner descriptor remained open after completed teardown")
	}
	if repeated := db.Close(); repeated != second {
		t.Fatalf("repeated Database.Close = %v, want cached exact %v", repeated, second)
	}
	reopened, err := OpenDatabase(dir, DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		t.Fatalf("reopen after retry-completed close: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDurableDatabaseConcurrentCloseConvergesAfterRetry(t *testing.T) {
	db := newTestDatabase(t, "orders")
	collection, _ := db.Collection("orders")
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	const callers = 8
	run := func() []error {
		results := make([]error, callers)
		var wait sync.WaitGroup
		for i := range results {
			wait.Add(1)
			go func() {
				defer wait.Done()
				results[i] = db.Close()
			}()
		}
		wait.Wait()
		return results
	}
	for i, err := range run() {
		if !errors.Is(err, storeio.ErrLeasesActive) {
			_ = snapshot.Close()
			t.Fatalf("concurrent blocked Close %d = %v", i, err)
		}
	}
	if db.CloseCompleted() {
		_ = snapshot.Close()
		t.Fatal("concurrent blocked closes reported completion")
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	for i, err := range run() {
		if err != nil {
			t.Fatalf("concurrent completing Close %d = %v", i, err)
		}
	}
	if !db.CloseCompleted() {
		t.Fatal("concurrent Close retries did not converge")
	}
}

// Given names with no canonical UTF-8 representation inside the portable byte
// bound, when they are used, then creation is refused.
func TestDurableDatabaseRejectsUnrepresentableCollectionNames(t *testing.T) {
	db := newTestDatabase(t)
	for _, name := range []string{
		"", "\xff\xfe", strings.Repeat("x", MaxCollectionNameBytes+1),
		strings.Repeat("\u00e9", MaxCollectionNameBytes/2+1),
	} {
		if _, err := db.CreateCollection(name, testDatabaseOptions()); err != ErrCollectionName {
			t.Errorf("CreateCollection(%q)=%v want %v", name, err, ErrCollectionName)
		}
	}
}

func TestDurableDatabasePortableCollectionFilenameRoundTrip(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDatabase(dir, DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		t.Fatal(err)
	}
	names := []string{
		"CON", "Orders", "a/b", `a\b`, "name. ",
		"caf\u00e9", "cafe\u0301", strings.Repeat("x", MaxCollectionNameBytes),
	}
	for _, name := range names {
		collection, err := db.CreateCollection(name, testDatabaseOptions())
		if err != nil {
			t.Fatalf("CreateCollection(%q): %v", name, err)
		}
		mustPut(t, collection, "key", `{"ok":true}`)
		filename := testCollectionFilename(t, name)
		if filename != strings.ToLower(filename) ||
			!strings.HasPrefix(filename, collectionFilePrefix) ||
			!strings.HasSuffix(filename, collectionFileSuffix) {
			t.Fatalf("non-portable filename %q for %q", filename, name)
		}
		if got := len(filename + recoveryJournalSuffix); got > 255 {
			t.Fatalf("paired filename for %q is %d bytes", name, got)
		}
		if _, err := os.Stat(filepath.Join(dir, filename)); err != nil {
			t.Fatalf("encoded primary for %q: %v", name, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDatabase(dir, DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	want := slices.Clone(names)
	slices.Sort(want)
	if got := reopened.Names(nil); !slices.Equal(got, want) {
		t.Fatalf("reopened names = %q, want %q", got, want)
	}
}

func TestDurableDatabaseRejectsDirectNameLayout(t *testing.T) {
	dir := t.TempDir()
	legacy := filepath.Join(dir, "orders"+collectionFileSuffix)
	if err := os.WriteFile(legacy, []byte("not a catalog collection"), 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDatabase(dir, DatabaseOptions{Options: testDatabaseOptions()})
	if !errors.Is(err, ErrUnsupportedDatabaseLayout) {
		if db != nil {
			_ = db.Close()
		}
		t.Fatalf("direct-name OpenDatabase = %v, want unsupported layout", err)
	}
}

func TestDurableDatabaseRejectsEncodedSymlinkPrimary(t *testing.T) {
	dir := t.TempDir()
	outsideDir := t.TempDir()
	outside := filepath.Join(outsideDir, "outside.vjc")
	const contents = "outside must remain untouched"
	if err := os.WriteFile(outside, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	base := testCollectionFilename(t, "escaped")
	primary := filepath.Join(dir, base)
	if err := os.Symlink(outside, primary); err != nil {
		t.Skipf("symbolic links unavailable: %v", err)
	}
	db, err := OpenDatabase(dir, DatabaseOptions{Options: testDatabaseOptions()})
	if db != nil {
		_ = db.Close()
	}
	if !errors.Is(err, ErrUnsupportedDatabaseLayout) {
		t.Fatalf("encoded-symlink OpenDatabase = %v, want unsupported layout", err)
	}
	got, readErr := os.ReadFile(outside)
	if readErr != nil || string(got) != contents {
		t.Fatalf("outside target changed: contents %q, error %v", got, readErr)
	}
	if _, statErr := os.Stat(primary + recoveryJournalSuffix); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("lexical recovery journal was created: %v", statErr)
	}
}

func TestDurableDatabaseRejectsEncodedDirectoryPrimary(t *testing.T) {
	dir := t.TempDir()
	base := testCollectionFilename(t, "directory")
	if err := os.Mkdir(filepath.Join(dir, base), 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDatabase(dir, DatabaseOptions{Options: testDatabaseOptions()})
	if db != nil {
		_ = db.Close()
	}
	if !errors.Is(err, ErrUnsupportedDatabaseLayout) {
		t.Fatalf("encoded-directory OpenDatabase = %v, want unsupported layout", err)
	}
}

func TestDurableDatabaseRejectsCaseAliasesBeforeOrphanCleanup(t *testing.T) {
	canonical := testCollectionFilename(t, "o")
	variants := []string{
		strings.ToUpper(canonical[:1]) + canonical[1:],
		strings.Replace(canonical, "6f", "6F", 1),
		strings.TrimSuffix(canonical, collectionFileSuffix) + ".VJC",
	}
	for _, variant := range variants {
		t.Run(variant, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, variant), []byte("alias"), 0o600); err != nil {
				t.Fatal(err)
			}
			journal := filepath.Join(dir, canonical+recoveryJournalSuffix)
			if err := os.WriteFile(journal, []byte("must survive"), 0o600); err != nil {
				t.Fatal(err)
			}
			db, err := OpenDatabase(dir, DatabaseOptions{Options: testDatabaseOptions()})
			if db != nil {
				_ = db.Close()
			}
			if !errors.Is(err, ErrUnsupportedDatabaseLayout) {
				t.Fatalf("OpenDatabase = %v, want unsupported layout", err)
			}
			if got, readErr := os.ReadFile(journal); readErr != nil || string(got) != "must survive" {
				t.Fatalf("validation followed orphan cleanup: contents %q, error %v", got, readErr)
			}
		})
	}

	t.Run("journal suffix", func(t *testing.T) {
		dir := t.TempDir()
		alias := canonical + ".RJOURNAL"
		if err := os.WriteFile(filepath.Join(dir, alias), []byte("alias"), 0o600); err != nil {
			t.Fatal(err)
		}
		db, err := OpenDatabase(dir, DatabaseOptions{Options: testDatabaseOptions()})
		if db != nil {
			_ = db.Close()
		}
		if !errors.Is(err, ErrUnsupportedDatabaseLayout) {
			t.Fatalf("OpenDatabase = %v, want unsupported layout", err)
		}
	})
}

func TestDurableDatabaseFreezesRelativeDirectoryBeforeWorkingDirectoryChanges(t *testing.T) {
	base := t.TempDir()
	other := t.TempDir()
	t.Chdir(base)
	db, err := OpenDatabase("catalog", DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		t.Fatal(err)
	}
	wantDir, err := filepath.EvalSymlinks(filepath.Join(base, "catalog"))
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if db.Dir() != wantDir || !filepath.IsAbs(db.Dir()) {
		_ = db.Close()
		t.Fatalf("database directory = %q, want stable absolute %q", db.Dir(), wantDir)
	}

	t.Chdir(other)
	collection, err := db.CreateCollection("after-chdir", testDatabaseOptions())
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	mustPut(t, collection, "key", `{"ok":true}`)
	baseName := testCollectionFilename(t, "after-chdir")
	if _, err := os.Stat(filepath.Join(wantDir, baseName)); err != nil {
		_ = db.Close()
		t.Fatalf("collection was not created in original directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(other, "catalog", baseName)); !errors.Is(err, os.ErrNotExist) {
		_ = db.Close()
		t.Fatalf("working-directory change received database state: %v", err)
	}
	if err := db.DropCollection("after-chdir"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(wantDir, baseName)); !errors.Is(err, os.ErrNotExist) {
		_ = db.Close()
		t.Fatalf("DropCollection left original primary: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
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
// TestDurableDatabaseSnapshotMaterializesDeferredLanes is the database-level
// variant of TestFileSnapshotPointScanAgreement. A DatabaseSnapshot captures
// every member collection under one held set of writers and gates, and a
// deferred-canonical lane's captured primary root can trail its live router — a
// point read walks the captured root's live structures while an ordered scan
// walks its sealed parents, so the two disagree until the pending parents are
// folded in. The single-collection [Collection.Snapshot] materializes them under
// one writer hold; the database capture has to do the same for every member
// without opening a window between a member's materialize and its pin. If it
// does not, a member's scan comes up short of its own point reads exactly as the
// collection-level bug did — and that is what surfaced as a durable join
// returning zero rows for a clear match, because a join reads its inner side
// through the database cut's ordered scan.
//
// Both deferred lanes are covered, and the cut's stability is pinned the same
// way the collection test pins it: mutating any member after the capture must
// not move what the capture sees, because the lease holds the captured
// generation against the reclaimer.
func TestDurableDatabaseSnapshotMaterializesDeferredLanes(t *testing.T) {
	syncJournal := testDatabaseOptions()
	syncJournal.Durability = DurabilitySync
	buffered := testDatabaseOptions()
	buffered.Durability = DurabilityBufferedVisible
	buffered.CheckpointStrength = CheckpointFilesystem

	for _, mode := range []struct {
		name string
		opts Options
	}{
		{"sync-journal", syncJournal},
		{"buffered", buffered},
	} {
		t.Run(mode.name, func(t *testing.T) {
			db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: mode.opts})
			if err != nil {
				t.Fatalf("OpenDatabase: %v", err)
			}
			defer func() { _ = db.Close() }()

			names := []string{"a", "b"}
			for _, name := range names {
				coll, err := db.CreateCollection(name, mode.opts)
				if err != nil {
					t.Fatalf("CreateCollection(%s): %v", name, err)
				}
				// Seed then update the same key so the second Put stages a deferred
				// parent on the sealed graph — the exact configuration whose captured
				// root trails its router.
				mustPut(t, coll, "k", `{"v":0}`)
				mustPut(t, coll, "k", `{"v":1}`)
			}

			cut, err := db.Snapshot()
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			defer func() { _ = cut.Close() }()

			scanKey := func(snap *Snapshot) []byte {
				t.Helper()
				var scanned []byte
				if _, err := snap.RangeRawBuffer(nil, func(key, value []byte) error {
					if string(key) == "k" {
						scanned = append([]byte(nil), value...)
					}
					return nil
				}); err != nil {
					t.Fatalf("RangeRawBuffer: %v", err)
				}
				return scanned
			}

			for _, name := range names {
				snap, ok := cut.Collection(name)
				if !ok {
					t.Fatalf("collection %q absent from the cut", name)
				}
				point, ok, err := snap.AppendRaw(nil, []byte("k"))
				if err != nil || !ok {
					t.Fatalf("%s: snapshot point read: ok=%v err=%v", name, ok, err)
				}
				scanned := scanKey(snap)
				if string(point) != string(scanned) {
					t.Errorf("%s: SNAPSHOT SELF-DISAGREEMENT: point=%s scan=%s", name, point, scanned)
				}
				if string(point) != `{"v":1}` {
					t.Errorf("%s: snapshot lost the acknowledged write: point=%s", name, point)
				}
			}

			// Mutate every member after the cut; neither member of the cut may move.
			for _, name := range names {
				coll, _ := db.Collection(name)
				mustPut(t, coll, "k", `{"v":2}`)
			}
			for _, name := range names {
				snap, _ := cut.Collection(name)
				point, ok, err := snap.AppendRaw(nil, []byte("k"))
				if err != nil || !ok {
					t.Fatalf("%s: old-cut point read after new put: ok=%v err=%v", name, ok, err)
				}
				if string(point) != `{"v":1}` {
					t.Errorf("%s: CUT MOVED after a later mutation: point=%s (want {\"v\":1})", name, point)
				}
				if scanned := scanKey(snap); string(scanned) != `{"v":1}` {
					t.Errorf("%s: CUT SCAN MOVED after a later mutation: scan=%s (want {\"v\":1})", name, scanned)
				}
			}
		})
	}
}

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
