package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// openSyncPrimaryStructuralStore builds a seeded ordered-primary store on the
// journal-backed synchronous lane and opens it for mutation. The tests drive
// split, empty-reclaim, and batch-split transactions across durable-root changes.
func openSyncPrimaryStructuralStore(t *testing.T) (*Collection, *os.File, string) {
	t.Helper()
	options := syncPrimaryJournalTestOptions()
	dir := t.TempDir()
	path := filepath.Join(dir, "store.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(seedPrimaryCollection(t), file, options); err != nil {
		t.Fatalf("CreateFromPrimary: %v", err)
	}
	_ = file.Close()
	file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	coll, err := Open(file, options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return coll, file, path
}

// reopenSyncPrimaryStore closes the live collection and file (a clean
// checkpointing close), then reopens the same path so a reopen replays whatever
// the journal still holds. It proves the structural mutations survive as durable
// state, not merely as a live in-memory graph.
func reopenSyncPrimaryStore(
	t *testing.T, coll *Collection, file *os.File, path string,
) (*Collection, *os.File) {
	t.Helper()
	if err := coll.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_ = file.Close()
	reopened, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := Open(reopened, syncPrimaryJournalTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return rc, reopened
}

// TestFilePrimarySyncLeafSplitSignal is the sync-lane analogue of
// TestFilePrimaryLeafSplitSignal. On the journal-backed
// synchronous lane it inserts well past several narrow-leaf capacities so multiple
// leaf splits fire, and asserts every Put is accepted -- ErrPrimaryLeafSplitRequired
// must never surface, and neither the tablet-root identity nor the overlapping
// retired-extent page-write rejection may appear on the second or later split. It
// then proves the acknowledgements were journal-durable and reopens to confirm the
// split graph survives.
func TestFilePrimarySyncLeafSplitSignal(t *testing.T) {
	coll, file, path := openSyncPrimaryStructuralStore(t)

	// One narrow leaf holds ~195 live keys; 600 sequential inserts drive at least
	// three splits, so the run crosses the exact second-split boundary the root-publication bug
	// failed at (~320 keys) and several beyond it.
	const inserts = 600
	oracle := map[string]string{"seed": `{"v":0}`}
	for i := 0; i < inserts; i++ {
		key := fmt.Sprintf("key-%06d", i)
		value := journalValue(i)
		created, putErr := coll.Put([]byte(key), value)
		if errors.Is(putErr, ErrPrimaryLeafSplitRequired) {
			t.Fatalf("sync put %q surfaced a leaf-split signal", key)
		}
		if putErr != nil || !created {
			t.Fatalf("sync put %q = created %v, err %v [splits so far=%d]",
				key, created, putErr, coll.Stats().PrimaryLeafSplits)
		}
		oracle[key] = string(value)
	}

	stats := coll.Stats()
	if stats.PrimaryLeafSplits < 3 {
		t.Fatalf("expected at least three leaf splits across %d sync inserts, got %d",
			inserts, stats.PrimaryLeafSplits)
	}
	if stats.PrimaryMacroSplitRequired != 0 {
		t.Fatalf("unexpected macro-split at leaf scale: %d", stats.PrimaryMacroSplitRequired)
	}
	// The lane must be the journal sync lane: acknowledgements accrue through the
	// journal and never take the retired committer chain fence.
	if stats.JournalAcks == 0 {
		t.Fatalf("sync inserts took no journal acknowledgements (journal=%d chain=%d)",
			stats.JournalAcks, stats.ChainAcks)
	}
	if stats.ChainAcks != 0 {
		t.Fatalf("sync structural lane must not take the retired chain fence, got chain=%d",
			stats.ChainAcks)
	}

	// Every key is readable on the live store immediately (visibility follows the
	// synced acknowledgement).
	buf := make([]byte, 0, 32)
	for key, want := range oracle {
		got, ok, readErr := coll.AppendRaw(buf[:0], []byte(key))
		if readErr != nil || !ok || string(got) != want {
			t.Fatalf("live readback %q = %q,%v,%v want %q", key, got, ok, readErr, want)
		}
		buf = got
	}

	// A clean close checkpoints; the reopen replays what remains and must recover
	// the identical split graph.
	rc, rf := reopenSyncPrimaryStore(t, coll, file, path)
	defer rf.Close()
	defer rc.Close()
	if got := snapshotCollectionContent(t, rc); !mapsEqual(got, oracle) {
		t.Fatalf("reopen after sync splits recovered %d keys, want %d", len(got), len(oracle))
	}
}

// TestFilePrimarySyncRoutedSplitDifferential is the sync-lane analogue of
// TestFilePrimaryRoutedSplitDifferential. It net-grows a primary graph in shuffled
// order so splits fire across the whole keyspace, validates every insert
// immediately, then asserts a byte-exact point differential and an ordered-scan
// differential against a map oracle. The shuffle guarantees splits land on
// interior leaves, whose tablet-root rewrite is exactly the second-split path
// whose root publication this test guards.
func TestFilePrimarySyncRoutedSplitDifferential(t *testing.T) {
	target := 1200
	if testing.Short() {
		target = 500
	}
	coll, file, path := openSyncPrimaryStructuralStore(t)

	// Shuffle the insert order (xorshift Fisher-Yates, no import) so splits land
	// across the whole keyspace instead of always at the tail.
	order := make([]int, target)
	for i := range order {
		order[i] = i
	}
	rng := uint64(0x9e3779b97f4a7c15)
	for i := target - 1; i > 0; i-- {
		rng ^= rng << 7
		rng ^= rng >> 9
		rng ^= rng << 8
		j := int(rng % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	oracle := map[string]string{"seed": `{"v":0}`}
	buffer := make([]byte, 0, 64)
	for at, id := range order {
		key := fmt.Sprintf("rk-%08d", id)
		value := []byte(fmt.Sprintf(`{"id":%d,"n":%d}`, id, at))
		created, putErr := coll.Put([]byte(key), value)
		if putErr != nil || !created {
			t.Fatalf("insert %d %q = created %v, err %v", at, key, created, putErr)
		}
		oracle[key] = string(value)
		got, ok, readErr := coll.AppendRaw(buffer[:0], []byte(key))
		if readErr != nil || !ok || !bytes.Equal(got, value) {
			t.Fatalf("post-insert read %q = %q,%v,%v want %q", key, got, ok, readErr, value)
		}
		buffer = got
	}
	if stats := coll.Stats(); stats.PrimaryLeafSplits == 0 {
		t.Fatalf("net growth to %d keys fired no leaf splits", target)
	}
	if got := int(coll.Len()); got != len(oracle) {
		t.Fatalf("Len = %d, want %d", got, len(oracle))
	}

	// Point differential: every oracle key resolves byte-exact.
	for key, want := range oracle {
		got, ok, readErr := coll.AppendRaw(buffer[:0], []byte(key))
		if readErr != nil || !ok || string(got) != want {
			t.Fatalf("point differential %q = %q,%v,%v want %q", key, got, ok, readErr, want)
		}
		buffer = got
	}

	// Ordered-scan differential: the full scan yields exactly the oracle keys in
	// strict lexical order with byte-exact values.
	snapshot, err := coll.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	var prevKey string
	if err := snapshot.RangeRaw(func(key, value []byte) error {
		if seen > 0 && string(key) <= prevKey {
			t.Fatalf("scan order violation: %q after %q", key, prevKey)
		}
		prevKey = string(key)
		want, ok := oracle[string(key)]
		if !ok || string(value) != want {
			t.Fatalf("scan differential %q = %q, want %q (present=%v)", key, value, want, ok)
		}
		seen++
		return nil
	}); err != nil {
		snapshot.Close()
		t.Fatal(err)
	}
	snapshot.Close()
	if seen != len(oracle) {
		t.Fatalf("scan visited %d keys, want %d", seen, len(oracle))
	}

	// Durability: the shuffled split graph must survive a reopen intact.
	rc, rf := reopenSyncPrimaryStore(t, coll, file, path)
	defer rf.Close()
	defer rc.Close()
	if got := snapshotCollectionContent(t, rc); !mapsEqual(got, oracle) {
		t.Fatalf("reopen after sync shuffled splits recovered %d keys, want %d",
			len(got), len(oracle))
	}
}

// TestFilePrimarySyncEmptyLeafReclaim exercises delete-side empty-leaf
// structural transactions on the journal-backed synchronous lane. It grows the
// graph past several split points, then empties leaves in the low keyspace.
// Content is validated against an oracle throughout and after a reopen.
func TestFilePrimarySyncEmptyLeafReclaim(t *testing.T) {
	coll, file, path := openSyncPrimaryStructuralStore(t)

	const grow = 700
	oracle := map[string]string{"seed": `{"v":0}`}
	keys := make([]string, 0, grow)
	for i := 0; i < grow; i++ {
		key := fmt.Sprintf("key-%06d", i)
		value := journalValue(i)
		if _, err := coll.Put([]byte(key), value); err != nil {
			t.Fatalf("grow put %q: %v", key, err)
		}
		oracle[key] = string(value)
		keys = append(keys, key)
	}
	if splits := coll.Stats().PrimaryLeafSplits; splits == 0 {
		t.Fatal("growth phase fired no leaf splits")
	}

	// Sweep the lowest 45% in ascending key order. Concentrating the deletes
	// empties whole low-keyspace leaves while the upper leaves stay occupied.
	sort.Strings(keys)
	shrink := grow * 45 / 100
	buf := make([]byte, 0, 32)
	for i := 0; i < shrink; i++ {
		key := keys[i]
		deleted, err := coll.Delete([]byte(key))
		if err != nil {
			t.Fatalf("delete %q: %v", key, err)
		}
		if !deleted {
			t.Fatalf("delete %q reported nothing removed", key)
		}
		delete(oracle, key)
		// The delete is visible immediately, and a surviving key remains readable
		// (proves empty-leaf reclaim preserved neighbours).
		if _, ok, err := coll.AppendRaw(buf[:0], []byte(key)); err != nil || ok {
			t.Fatalf("deleted key %q still visible: ok=%v err=%v", key, ok, err)
		}
	}

	stats := coll.Stats()
	if stats.PrimaryEmptyReclaims == 0 {
		t.Fatal("delete-heavy sync phase reclaimed no empty leaves")
	}
	if stats.ChainAcks != 0 {
		t.Fatalf("sync structural lane must not take the retired chain fence, got chain=%d",
			stats.ChainAcks)
	}
	t.Logf("sync empty reclaim: splits=%d reclaims=%d empty=%d",
		stats.PrimaryLeafSplits, stats.PrimaryEmptyReclaims,
		stats.PrimaryEmptyLeaves)

	// Live differential over every surviving key.
	for key, want := range oracle {
		got, ok, readErr := coll.AppendRaw(buf[:0], []byte(key))
		if readErr != nil || !ok || string(got) != want {
			t.Fatalf("post-reclaim readback %q = %q,%v,%v want %q", key, got, ok, readErr, want)
		}
		buf = got
	}

	// Durability across the empty-leaf reclaim transactions.
	rc, rf := reopenSyncPrimaryStore(t, coll, file, path)
	defer rf.Close()
	defer rc.Close()
	if got := snapshotCollectionContent(t, rc); !mapsEqual(got, oracle) {
		t.Fatalf("reopen after sync empty reclaim recovered %d keys, want %d",
			len(got), len(oracle))
	}
}

// TestFilePrimarySyncBatchSplit drives the transactional batch path across leaf
// boundaries on the journal-backed synchronous lane. A single Update whose members
// span more than one leaf's capacity commits its member splits as separate
// structural transactions before the one atomic publish; every one of those splits
// must advance the deferred durable root, so a multi-split batch guards the same
// root-publication invariant. Batches amortize the per-mutation sync into one group
// commit, so this stays fast while still crossing several split points. The batched
// result must equal the identical sequential application and survive a reopen.
func TestFilePrimarySyncBatchSplit(t *testing.T) {
	// perBatch stays under the collection's 64-document batch bound; total spans
	// several narrow-leaf capacities so a batch fills the tail leaf mid-Update and
	// commits its member split as a separate structural transaction before the one
	// atomic publish.
	const total = 900
	const perBatch = 60

	// Sequential oracle: apply every key one Put at a time on its own sync store.
	sequential, seqFile, _ := openSyncPrimaryStructuralStore(t)
	defer seqFile.Close()
	oracle := map[string]string{"seed": `{"v":0}`}
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("bk-%06d", i)
		value := journalValue(i)
		if _, err := sequential.Put([]byte(key), value); err != nil {
			t.Fatalf("sequential put %q: %v", key, err)
		}
		oracle[key] = string(value)
	}
	if splits := sequential.Stats().PrimaryLeafSplits; splits < 3 {
		t.Fatalf("sequential oracle fired only %d splits, want >=3", splits)
	}
	if err := sequential.Close(); err != nil {
		t.Fatalf("close sequential: %v", err)
	}

	// Batched: apply the same keys in groups, each group one transactional Update
	// that internally splits as it fills leaves.
	batched, batFile, path := openSyncPrimaryStructuralStore(t)
	for start := 0; start < total; start += perBatch {
		end := start + perBatch
		if end > total {
			end = total
		}
		if err := batched.Update(func(b *WriteBatch) error {
			for i := start; i < end; i++ {
				if err := b.Put([]byte(fmt.Sprintf("bk-%06d", i)), journalValue(i)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("batch update [%d,%d): %v [splits so far=%d]",
				start, end, err, batched.Stats().PrimaryLeafSplits)
		}
	}

	stats := batched.Stats()
	if stats.PrimaryLeafSplits < 3 {
		t.Fatalf("batched inserts fired only %d splits, want >=3 (member splits per batch)",
			stats.PrimaryLeafSplits)
	}
	if stats.ChainAcks != 0 {
		t.Fatalf("sync batch lane must not take the retired chain fence, got chain=%d",
			stats.ChainAcks)
	}
	if got := int(batched.Len()); got != len(oracle) {
		t.Fatalf("batched Len = %d, want %d", got, len(oracle))
	}
	if got := primaryStoreContent(t, batched); !mapsEqual(got, oracle) {
		t.Fatalf("batched content diverged from sequential oracle: got %d keys want %d",
			len(got), len(oracle))
	}

	// Durability: the batch-split graph must survive a reopen intact.
	rc, rf := reopenSyncPrimaryStore(t, batched, batFile, path)
	defer rf.Close()
	defer rc.Close()
	if got := snapshotCollectionContent(t, rc); !mapsEqual(got, oracle) {
		t.Fatalf("reopen after sync batch splits recovered %d keys, want %d",
			len(got), len(oracle))
	}
}

// TestFilePrimarySyncSplitCrashReplay copies the store and its journal mid-flight,
// after several splits have fired but with no clean close, so the on-disk root is
// whatever the last structural checkpoint made durable and the journal still holds
// the puts that trailed it. Reopening that crash image must replay to the exact
// live key set, and a second reopen (now checkpointed) must be identical: the
// sync-lane structural durable-root advance leaves a recoverable image and replay
// is deterministic. This is the sync-lane cover for the retirement fix's crash
// window -- the buffered split crash boundary exercises the same durableState
// advance without the journal, this exercises it with the journal recycle the sync
// lane adds.
func TestFilePrimarySyncSplitCrashReplay(t *testing.T) {
	coll, file, path := openSyncPrimaryStructuralStore(t)

	const inserts = 500
	oracle := map[string]string{"seed": `{"v":0}`}
	for i := 0; i < inserts; i++ {
		key := fmt.Sprintf("key-%06d", i)
		value := journalValue(i)
		if _, err := coll.Put([]byte(key), value); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
		oracle[key] = string(value)
	}
	if splits := coll.Stats().PrimaryLeafSplits; splits < 2 {
		t.Fatalf("expected multiple splits before the crash copy, got %d", splits)
	}

	// Simulate a crash: copy the store and journal with no clean close, so the root
	// is the last structural checkpoint and the journal carries the trailing puts.
	dir := filepath.Dir(path)
	crashStore := filepath.Join(dir, "crash.vibe")
	copyFileForCrash(t, path, crashStore)
	copyFileForCrash(t, path+".rjournal", crashStore+".rjournal")

	// Close the live writer only after capturing the image.
	if err := coll.Close(); err != nil {
		t.Fatalf("close live: %v", err)
	}
	_ = file.Close()

	verifyReplay := func(label string) {
		cf, err := os.OpenFile(crashStore, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		rc, err := Open(cf, syncPrimaryJournalTestOptions())
		if err != nil {
			t.Fatalf("%s: reopen crash image: %v", label, err)
		}
		if got := snapshotCollectionContent(t, rc); !mapsEqual(got, oracle) {
			t.Fatalf("%s: recovered %d keys, want %d", label, len(got), len(oracle))
		}
		if err := rc.Close(); err != nil {
			t.Fatalf("%s: close: %v", label, err)
		}
		_ = cf.Close()
	}
	verifyReplay("first-open-replay")
	// The first reopen checkpointed and recycled; the second must recover the
	// identical state with nothing left to replay.
	verifyReplay("second-open-idempotent")
}
