package durable

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func openPortableStorePageDB(t testing.TB, path string, maximum uint32) *StorePageDB {
	t.Helper()
	db, err := OpenStorePageDB(path, StorePageDBOptions{
		Open: StorePageOpenOptions{
			ResidentBytes: 8 * int64(maximum), MaxDocumentPageBytes: maximum,
		},
		CommitBackend: StorePageCommitPortable,
	})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestStorePageDBUpdateDeleteRecovery(t *testing.T) {
	fs, original := buildStorePageTestData(t, 10, 4)
	path, initialBytes := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 8192})
	db := openPortableStorePageDB(t, path, 8192)
	initialGeneration := db.Generation()
	if db.root.FreeChunkHint != 2 {
		t.Fatalf("initial free-chunk hint = %d, want partial chunk 2", db.root.FreeChunkHint)
	}

	key := "account:00000003"
	large := `{"id":3,"payload":"` + strings.Repeat("x", 5000) + `"}`
	created, err := db.Put(key, []byte(large))
	if err != nil || created {
		t.Fatalf("Put update = (%v,%v)", created, err)
	}
	if db.Generation() != initialGeneration+1 || db.DurableGeneration() != db.Generation() {
		t.Fatalf("generation = visible %d durable %d, started %d",
			db.Generation(), db.DurableGeneration(), initialGeneration)
	}
	buffer := make([]byte, 0, len(large))
	got, ok, err := db.AppendRaw(buffer, key)
	if err != nil || !ok || string(got) != large {
		t.Fatalf("updated read = (%d bytes,%v,%v)", len(got), ok, err)
	}

	unchangedGeneration := db.Generation()
	if _, err := db.Put(key, []byte(large)); err != nil || db.Generation() != unchangedGeneration {
		t.Fatalf("idempotent update = generation %d, err %v", db.Generation(), err)
	}
	if created, err := db.Put("missing", []byte(`{"v":1}`)); err != nil || !created {
		t.Fatalf("missing Put = (%v,%v)", created, err)
	}
	if got, ok, err := db.AppendRaw(buffer[:0], "missing"); err != nil || !ok || string(got) != `{"v":1}` {
		t.Fatalf("inserted read = (%q,%v,%v)", got, ok, err)
	}
	afterInsertGeneration := db.Generation()
	if _, err := db.Put(key, []byte(`{"broken":`)); !errors.Is(err, ErrStorePageInvalidJSON) {
		t.Fatalf("invalid Put = %v", err)
	}
	if db.Generation() != afterInsertGeneration {
		t.Fatalf("failed Put changed generation to %d", db.Generation())
	}

	deletedKey := "account:00000004"
	deleted, err := db.Delete(deletedKey)
	if err != nil || !deleted {
		t.Fatalf("Delete = (%v,%v)", deleted, err)
	}
	if deleted, err := db.Delete(deletedKey); err != nil || deleted {
		t.Fatalf("second Delete = (%v,%v)", deleted, err)
	}
	if _, ok, err := db.AppendRaw(buffer[:0], deletedKey); err != nil || ok {
		t.Fatalf("deleted read = (%v,%v)", ok, err)
	}
	if db.root.FreeChunkHint != 1 {
		t.Fatalf("delete free-chunk hint = %d, want deleted chunk 1", db.root.FreeChunkHint)
	}
	stats := db.Stats()
	if stats.Documents != uint64(len(original)) || stats.Generation != initialGeneration+3 ||
		stats.DurableGeneration != stats.Generation || stats.FileBytes <= uint64(initialBytes) ||
		stats.CommitBackend != StorePageCommitPortable || stats.DeviceCommits != 3 ||
		stats.Cache.ResidentBytes > stats.Cache.ReservedBytes ||
		stats.Cache.ReservedBytes > stats.Cache.CapacityBytes {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.AppendRaw(nil, key); !errors.Is(err, ErrStorePageClosed) {
		t.Fatalf("read after Close = %v", err)
	}

	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 4 * 8192, MaxDocumentPageBytes: 8192,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got, ok, err = reader.AppendRaw(buffer[:0], key)
	if err != nil || !ok || string(got) != large || reader.Generation() != initialGeneration+3 {
		t.Fatalf("reopened update = (%d bytes,%v,%v), generation %d", len(got), ok, err, reader.Generation())
	}
	if _, ok, err := reader.AppendRaw(nil, deletedKey); err != nil || ok {
		t.Fatalf("reopened delete = (%v,%v)", ok, err)
	}
}

func TestStorePageDBMutationsUseExactQuantumExtents(t *testing.T) {
	collection, _ := buildStorePageTestData(t, 1, 1)
	path, _ := writeStorePageTestFile(t, collection, StorePageWriteOptions{
		MaxDocumentPageBytes: 16 << 10,
	})
	db := openPortableStorePageDB(t, path, 16<<10)
	key := "account:00000000"
	value := []byte(`{"payload":"` + strings.Repeat("x", 9<<10) + `"}`)
	if created, err := db.Put(key, value); err != nil || created {
		t.Fatalf("exact mutation Put = (%v,%v)", created, err)
	}
	document, ok, err := resolveStoreDocumentPage(
		db.pages.Load(), db.root.ChunkDirectory, 0,
	)
	if err != nil || !ok || document.Length != 12<<10 {
		t.Fatalf("exact mutation document = (%+v,%v,%v), want 12 KiB", document, ok, err)
	}
	got, ok, err := db.AppendRaw(nil, key)
	if err != nil || !ok || !bytes.Equal(got, value) {
		t.Fatalf("exact mutation read = (%d,%v,%v)", len(got), ok, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 2 * 16 << 10, MaxDocumentPageBytes: 16 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	document, ok, err = resolveStoreDocumentPage(
		reader.pages.Load(), reader.root.ChunkDirectory, 0,
	)
	if err != nil || !ok || document.Length != 12<<10 {
		t.Fatalf("reopened exact document = (%+v,%v,%v), want 12 KiB", document, ok, err)
	}
	got, ok, err = reader.AppendRaw(nil, key)
	if err != nil || !ok || !bytes.Equal(got, value) {
		t.Fatalf("reopened exact mutation read = (%d,%v,%v)", len(got), ok, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStorePageDBDeletesLastChunkAndDatabase(t *testing.T) {
	fs, _ := buildStorePageTestData(t, 3, 2)
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	db := openPortableStorePageDB(t, path, 4096)
	for i := range 3 {
		key := fmt.Sprintf("account:%08d", i)
		deleted, err := db.Delete(key)
		if err != nil || !deleted {
			t.Fatalf("Delete(%q) = (%v,%v)", key, deleted, err)
		}
	}
	if db.Len() != 0 {
		t.Fatalf("Len = %d, want 0", db.Len())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 2 * 4096, MaxDocumentPageBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.Len() != 0 {
		t.Fatalf("reopened Len = %d, want 0", reader.Len())
	}
}

func TestStorePageDBInsertEmptyGrowAndReuseStableSlot(t *testing.T) {
	fs, err := store.New(store.Options{ChunkDocuments: 4})
	if err != nil {
		t.Fatal(err)
	}
	path, initialBytes := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	db := openPortableStorePageDB(t, path, 4096)
	for i := range 10 {
		key := fmt.Sprintf("insert:%02d", i)
		doc := []byte(fmt.Sprintf(`{"id":%d}`, i))
		if created, err := db.Put(key, doc); err != nil || !created {
			t.Fatalf("Put(%q) = (%v,%v)", key, created, err)
		}
		if i == 0 {
			const wantAppend = 4 * storePageQuantum // document, radix root, key root, state root
			if got := db.Stats().FileBytes - uint64(initialBytes); got != uint64(wantAppend) {
				t.Fatalf("first insert appended %d bytes, want %d", got, wantAppend)
			}
		}
	}
	if db.Len() != 10 || db.root.ChunkHighWater != 3 || db.root.LiveChunks != 3 || db.root.FreeChunkHint != 2 {
		t.Fatalf("grown root = %+v", db.root)
	}
	if deleted, err := db.Delete("insert:01"); err != nil || !deleted {
		t.Fatalf("Delete = (%v,%v)", deleted, err)
	}
	if db.root.FreeChunkHint != 0 {
		t.Fatalf("delete free hint = %d, want 0", db.root.FreeChunkHint)
	}
	if created, err := db.Put("reused", []byte(`{"reused":true}`)); err != nil || !created {
		t.Fatalf("reused Put = (%v,%v)", created, err)
	}
	prepared := StorePageKey{key: "reused", storeID: db.storeID, hash: storeio.KeyHash(db.storeID, "reused")}
	value, ok, err := lookupStorePageKey(db.pages.Load(), db.root.KeyDirectory, db.root.ChunkDirectory, prepared, &prepared)
	if err != nil || !ok {
		t.Fatalf("reused lookup = (%v,%v)", ok, err)
	}
	if err := value.Close(); err != nil {
		t.Fatal(err)
	}
	if prepared.chunk != 0 || prepared.slot != 1 || db.root.ChunkHighWater != 3 {
		t.Fatalf("reused location = (%d,%d), high-water %d", prepared.chunk, prepared.slot, db.root.ChunkHighWater)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 4 * 4096, MaxDocumentPageBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, key := range []string{"insert:00", "insert:09", "reused"} {
		if _, ok, err := reader.AppendRaw(nil, key); err != nil || !ok {
			t.Fatalf("reopened %q = (%v,%v)", key, ok, err)
		}
	}
	if _, ok, err := reader.AppendRaw(nil, "insert:01"); err != nil || ok {
		t.Fatalf("reopened deleted key = (%v,%v)", ok, err)
	}
}

func TestStorePageDBInsertGrowsChunkRadixRoot(t *testing.T) {
	fs, want := buildStorePageTestData(t, 256, 4) // 64 full chunks under a shift-zero root
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	db := openPortableStorePageDB(t, path, 4096)
	if created, err := db.Put("radix-boundary", []byte(`{"boundary":64}`)); err != nil || !created {
		t.Fatalf("boundary Put = (%v,%v)", created, err)
	}
	if db.root.ChunkHighWater != 65 || db.root.LiveChunks != 65 || db.root.FreeChunkHint != 64 {
		t.Fatalf("boundary root = %+v", db.root)
	}
	lease, err := db.pages.Load().Cache().Pin(db.root.ChunkDirectory)
	if err != nil {
		t.Fatal(err)
	}
	rootView := storeio.AdmittedChunkDirectoryPage(lease.Bytes())
	if rootView.Header().Shift != 6 || rootView.Len() != 2 {
		t.Fatalf("radix root = shift %d, children %d", rootView.Header().Shift, rootView.Len())
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"account:00000000", "account:00000255", "radix-boundary"} {
		got, ok, err := db.AppendRaw(nil, key)
		if err != nil || !ok {
			t.Fatalf("lookup %q = (%v,%v)", key, ok, err)
		}
		if expected, exists := want[key]; exists && string(got) != expected {
			t.Fatalf("lookup %q = %q, want %q", key, got, expected)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStorePageDBInsertSplitsFullKeyLeaf(t *testing.T) {
	fs, want := buildStorePageTestData(t, storePageDBKeyLeafCapacity, 64)
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 16 << 10})
	db := openPortableStorePageDB(t, path, 16<<10)
	if created, err := db.Put("key-leaf-split", []byte(`{"split":true}`)); err != nil || !created {
		t.Fatalf("split Put = (%v,%v)", created, err)
	}
	lease, err := db.pages.Load().Cache().Pin(db.root.KeyDirectory)
	if err != nil {
		t.Fatal(err)
	}
	rootView := storeio.AdmittedPageKeyDirectory(lease.Bytes())
	if rootView.Header().Level != 1 || rootView.Len() != 2 {
		t.Fatalf("split key root = level %d, children %d", rootView.Header().Level, rootView.Len())
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"account:00000000", fmt.Sprintf("account:%08d", storePageDBKeyLeafCapacity-1), "key-leaf-split"} {
		got, ok, err := db.AppendRaw(nil, key)
		if err != nil || !ok {
			t.Fatalf("lookup %q = (%v,%v)", key, ok, err)
		}
		if expected, exists := want[key]; exists && string(got) != expected {
			t.Fatalf("lookup %q = %q, want %q", key, got, expected)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStorePageDBInsertSplitsFullKeyBranch(t *testing.T) {
	rows := storePageDBKeyLeafCapacity * storePageDBKeyBranchCapacity
	fs, want := buildStorePageTestData(t, rows, 64)
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 16 << 10})
	db := openPortableStorePageDB(t, path, 16<<10)
	if created, err := db.Put("key-branch-split", []byte(`{"split":"branch"}`)); err != nil || !created {
		t.Fatalf("branch split Put = (%v,%v)", created, err)
	}
	lease, err := db.pages.Load().Cache().Pin(db.root.KeyDirectory)
	if err != nil {
		t.Fatal(err)
	}
	rootView := storeio.AdmittedPageKeyDirectory(lease.Bytes())
	if rootView.Header().Level != 2 || rootView.Len() != 2 {
		t.Fatalf("branch split root = level %d, children %d", rootView.Header().Level, rootView.Len())
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"account:00000000", fmt.Sprintf("account:%08d", rows/2),
		fmt.Sprintf("account:%08d", rows-1), "key-branch-split"} {
		got, ok, err := db.AppendRaw(nil, key)
		if err != nil || !ok {
			t.Fatalf("lookup %q = (%v,%v)", key, ok, err)
		}
		if expected, exists := want[key]; exists && string(got) != expected {
			t.Fatalf("lookup %q = %q, want %q", key, got, expected)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStorePageDBMixedMutationReopenDifferential(t *testing.T) {
	fs, err := store.New(store.Options{ChunkDocuments: 8})
	if err != nil {
		t.Fatal(err)
	}
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	db := openPortableStorePageDB(t, path, 4096)
	model := make(map[string]string)
	put := func(key, doc string) {
		t.Helper()
		_, existed := model[key]
		created, err := db.Put(key, []byte(doc))
		if err != nil || created == existed {
			t.Fatalf("Put(%q) = (%v,%v), existed %v", key, created, err, existed)
		}
		model[key] = doc
	}
	for i := range storePageDBKeyLeafCapacity + 13 {
		put(fmt.Sprintf("model:%03d", i), fmt.Sprintf(`{"id":%d,"version":0}`, i))
	}
	rng := rand.New(rand.NewSource(0x51ab1e))
	for operation := range 240 {
		id := rng.Intn(320)
		key := fmt.Sprintf("model:%03d", id)
		switch rng.Intn(4) {
		case 0:
			deleted, err := db.Delete(key)
			_, existed := model[key]
			if err != nil || deleted != existed {
				t.Fatalf("Delete(%q) = (%v,%v), existed %v", key, deleted, err, existed)
			}
			delete(model, key)
		default:
			put(key, fmt.Sprintf(`{"id":%d,"version":%d}`, id, operation+1))
		}
		if operation%40 == 39 {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = openPortableStorePageDB(t, path, 4096)
		}
		if db.Len() != uint64(len(model)) || db.root.FreeChunkHint > db.root.ChunkHighWater {
			t.Fatalf("operation %d root/model = len %d/%d hint %d high-water %d",
				operation, db.Len(), len(model), db.root.FreeChunkHint, db.root.ChunkHighWater)
		}
	}
	defer db.Close()
	for id := range 320 {
		key := fmt.Sprintf("model:%03d", id)
		got, ok, err := db.AppendRaw(nil, key)
		want, exists := model[key]
		if err != nil || ok != exists || exists && string(got) != want {
			t.Fatalf("final %q = (%q,%v,%v), want (%q,%v)", key, got, ok, err, want, exists)
		}
	}
}

func TestStorePageDBDeleteCopiesMultiLevelKeyPath(t *testing.T) {
	fs, want := buildStorePageTestData(t, 512, 16)
	path, initialBytes := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	db := openPortableStorePageDB(t, path, 4096)
	deletedKey := "account:00000246"
	if deleted, err := db.Delete(deletedKey); err != nil || !deleted {
		t.Fatalf("Delete = (%v,%v)", deleted, err)
	}
	stats := db.Stats()
	const wantGrowth = uint64(5 * storePageQuantum) // document, chunk radix, key leaf+branch, state
	if growth := stats.FileBytes - uint64(initialBytes); growth != wantGrowth {
		t.Fatalf("delete COW growth = %d bytes, want %d", growth, wantGrowth)
	}
	for _, key := range []string{"account:00000100", "account:00000400"} {
		if deleted, err := db.Delete(key); err != nil || !deleted {
			t.Fatalf("Delete(%q) = (%v,%v)", key, deleted, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 4 * 4096, MaxDocumentPageBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, key := range []string{deletedKey, "account:00000100", "account:00000400"} {
		if _, ok, err := reader.AppendRaw(nil, key); err != nil || ok {
			t.Fatalf("deleted key %q = (%v,%v)", key, ok, err)
		}
	}
	for _, key := range []string{"account:00000245", "account:00000247", "account:00000511"} {
		got, ok, err := reader.AppendRaw(nil, key)
		if err != nil || !ok || string(got) != want[key] {
			t.Fatalf("neighbor %q = (%q,%v,%v)", key, got, ok, err)
		}
	}
}

func TestStorePageDBDeleteRewritesCollisionSuccessorLeaf(t *testing.T) {
	const rows = 520
	const chunkDocuments = 16
	targetRow := 300 // beyond the first 247-entry key leaf
	target := fmt.Sprintf("account:%08d", targetRow)
	fs, want := buildStorePageTestData(t, rows, chunkDocuments)
	path, initialBytes := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	rewriteStorePageKeyCollisionRun(t, path, target, rows, chunkDocuments)

	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 4 * 4096, MaxDocumentPageBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := reader.AppendRaw(nil, target)
	if err != nil || !ok || string(got) != want[target] {
		t.Fatalf("collision successor lookup = (%q,%v,%v)", got, ok, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	collisionBytes := info.Size()
	if collisionBytes <= initialBytes {
		t.Fatalf("collision generation size = %d, initial %d", collisionBytes, initialBytes)
	}
	db := openPortableStorePageDB(t, path, 4096)
	if deleted, err := db.Delete(target); err != nil || !deleted {
		t.Fatalf("Delete collision successor = (%v,%v)", deleted, err)
	}
	const wantGrowth = int64(5 * storePageQuantum) // document, chunk, key leaf+branch, state
	if growth := int64(db.Stats().FileBytes) - collisionBytes; growth != wantGrowth {
		t.Fatalf("collision delete growth = %d, want %d", growth, wantGrowth)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err = OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 4 * 4096, MaxDocumentPageBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, ok, err := reader.AppendRaw(nil, target); err != nil || ok {
		t.Fatalf("deleted collision key = (%v,%v)", ok, err)
	}
	location := storeio.PageKeyLocation{Hash: storeio.KeyHash(reader.root.StoreID, target),
		Chunk: uint32(targetRow / chunkDocuments), Slot: uint8(targetRow % chunkDocuments)}
	total, matches := countStorePageKeyEntries(t, reader, location)
	if total != rows-1 || matches != 0 {
		t.Fatalf("visible key tree = %d entries, %d deleted-location matches", total, matches)
	}
}

// rewriteStorePageKeyCollisionRun appends a valid generation whose key tree
// routes every location through one synthetic hash. Document bytes remain
// unchanged, so exact-key checks make only target observable. This constructs
// a deterministic cross-leaf collision without weakening the production hash.
func rewriteStorePageKeyCollisionRun(t testing.TB, path, target string, rows, chunkDocuments int) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var scratch [storePageQuantum]byte
	super, root, _, err := storeio.RecoverStateRoot(file, storePageQuantum, scratch[:])
	if err != nil {
		t.Fatal(err)
	}
	generation := root.Generation + 1
	entries := make([]storeio.PageKeyLocation, rows)
	hash := storeio.KeyHash(root.StoreID, target)
	for row := range rows {
		entries[row] = storeio.PageKeyLocation{Hash: hash, Chunk: uint32(row / chunkDocuments), Slot: uint8(row % chunkDocuments)}
	}
	offset := super.FileEnd
	nextLogical := root.NextLogicalID
	plans, keyRoot := planStoreKeyDirectories(entries, generation, &nextLogical, &offset)
	stateOffset := offset
	fileEnd := stateOffset + uint64(storePageQuantum)
	if err := file.Truncate(int64(fileEnd)); err != nil {
		t.Fatal(err)
	}
	for _, plan := range plans {
		header := storeio.PageKeyDirectoryHeader{
			StoreID: root.StoreID, Generation: generation, LogicalID: plan.ref.LogicalID,
			PageSize: storePageQuantum, MinHash: plan.minHash, MaxHash: plan.maxHash,
			Level: plan.level, Next: plan.next,
		}
		var page []byte
		if plan.level == 0 {
			page, err = storeio.EncodePageKeyLeaf(scratch[:], header, plan.leaf,
				fileEnd, nextLogical, root.ChunkHighWater, root.ChunkDocuments)

		} else {
			page, err = storeio.EncodePageKeyBranch(scratch[:], header, plan.branches, fileEnd, nextLogical)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := writeStorePageAt(file, page, plan.ref.Offset); err != nil {
			t.Fatal(err)
		}
	}
	next := root
	next.Generation = generation
	next.NextLogicalID = nextLogical
	next.KeyDirectory = keyRoot
	statePage, err := storeio.EncodeStateRootPage(scratch[:], next, fileEnd)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeStorePageAt(file, statePage, stateOffset); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	stateChecksum := storeio.PageChecksum(statePage)
	rootPage := scratch[:]
	clear(rootPage)
	if _, err := storeio.EncodeSuperblock(rootPage[:storeio.SuperblockSize], storeio.Superblock{
		StoreID: root.StoreID, Generation: generation, StateOffset: stateOffset,
		StateLength: storePageQuantum, StateChecksum: stateChecksum,
		FileEnd: fileEnd, PageSize: storePageQuantum,
	}); err != nil {
		t.Fatal(err)
	}
	rootOffset, err := storeio.SuperblockOffset(generation, storePageQuantum)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeStorePageAt(file, rootPage, uint64(rootOffset)); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
}

func countStorePageKeyEntries(t testing.TB, reader *StorePageReader, target storeio.PageKeyLocation) (total, matches int) {
	t.Helper()
	var walk func(storeio.PageRef)
	walk = func(ref storeio.PageRef) {
		lease, err := reader.pages.Load().Cache().Pin(ref)
		if err != nil {
			t.Fatal(err)
		}
		view := storeio.AdmittedPageKeyDirectory(lease.Bytes())
		if view.Header().Level == 0 {
			for rank := 0; rank < view.Len(); rank++ {
				entry, ok := view.LocationAt(rank)
				if !ok {
					t.Fatal("invalid admitted key location")
				}
				total++
				if entry == target {
					matches++
				}
			}
			if err := lease.Close(); err != nil {
				t.Fatal(err)
			}
			return
		}
		children := make([]storeio.PageRef, view.Len())
		for rank := range children {
			branch, ok := view.BranchAt(rank)
			if !ok {
				t.Fatal("invalid admitted key branch")
			}
			children[rank] = branch.Child
		}
		if err := lease.Close(); err != nil {
			t.Fatal(err)
		}
		for _, child := range children {
			walk(child)
		}
	}
	walk(reader.root.KeyDirectory)
	return total, matches
}

func TestStorePageDBRootFallback(t *testing.T) {
	fs, original := buildStorePageTestData(t, 4, 4)
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	db := openPortableStorePageDB(t, path, 4096)
	key := "account:00000001"
	if _, err := db.Put(key, []byte(`{"changed":true}`)); err != nil {
		t.Fatal(err)
	}
	newGeneration := db.Generation()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	offset, err := storeio.SuperblockOffset(newGeneration, storePageQuantum)
	if err != nil {
		t.Fatal(err)
	}
	var torn [storeio.SuperblockSize]byte
	if _, err := file.WriteAt(torn[:], offset); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 2 * 4096, MaxDocumentPageBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got, ok, err := reader.AppendRaw(nil, key)
	if err != nil || !ok || string(got) != original[key] || reader.Generation() != newGeneration-1 {
		t.Fatalf("fallback read = (%q,%v,%v), generation %d", got, ok, err, reader.Generation())
	}
}

func TestStorePageDBInsertRootFallback(t *testing.T) {
	fs, _ := buildStorePageTestData(t, 4, 4)
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	db := openPortableStorePageDB(t, path, 4096)
	if created, err := db.Put("not-in-older-root", []byte(`{"new":true}`)); err != nil || !created {
		t.Fatalf("insert = (%v,%v)", created, err)
	}
	newGeneration := db.Generation()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	offset, err := storeio.SuperblockOffset(newGeneration, storePageQuantum)
	if err != nil {
		t.Fatal(err)
	}
	var torn [storeio.SuperblockSize]byte
	if _, err := file.WriteAt(torn[:], offset); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 2 * 4096, MaxDocumentPageBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, ok, err := reader.AppendRaw(nil, "not-in-older-root"); err != nil || ok {
		t.Fatalf("fallback inserted key = (%v,%v)", ok, err)
	}
	if reader.Generation() != newGeneration-1 || reader.Len() != 4 {
		t.Fatalf("fallback root = generation %d, len %d", reader.Generation(), reader.Len())
	}
}

func TestStorePageDBEnforcesSingleWriter(t *testing.T) {
	fs, _ := buildStorePageTestData(t, 4, 4)
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	first := openPortableStorePageDB(t, path, 4096)
	if second, err := OpenStorePageDB(path, StorePageDBOptions{
		Open: StorePageOpenOptions{
			ResidentBytes: 2 * 4096, MaxDocumentPageBytes: 4096,
		},
		CommitBackend: StorePageCommitPortable,
	}); !errors.Is(err, ErrStorePageWriterLocked) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("second writer = %v, want %v", err, ErrStorePageWriterLocked)
	}
	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 2 * 4096, MaxDocumentPageBytes: 4096,
	})
	if err != nil {
		t.Fatalf("reader alongside writer: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := openPortableStorePageDB(t, path, 4096)
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStorePageDBConcurrentReadersAndWriter(t *testing.T) {
	fs, _ := buildStorePageTestData(t, 128, 16)
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	db := openPortableStorePageDB(t, path, 4096)
	defer db.Close()

	var stop atomic.Bool
	var failed atomic.Pointer[error]
	var readers sync.WaitGroup
	for worker := 0; worker < min(runtime.GOMAXPROCS(0), 8); worker++ {
		readers.Add(1)
		go func(id int) {
			defer readers.Done()
			key := fmt.Sprintf("account:%08d", id)
			buffer := make([]byte, 0, 128)
			for !stop.Load() {
				got, ok, err := db.AppendRaw(buffer[:0], key)
				if err != nil || !ok || !vibejson.Valid(got) {
					failure := fmt.Errorf("reader %d = (%q,%v,%v)", id, got, ok, err)
					failed.CompareAndSwap(nil, &failure)
					return
				}
			}
		}(worker)
	}
	key := "account:00000000"
	for i := range 32 {
		doc := []byte(fmt.Sprintf(`{"id":0,"version":%d}`, i))
		if _, err := db.Put(key, doc); err != nil {
			t.Fatal(err)
		}
		insertKey := fmt.Sprintf("concurrent-insert:%02d", i)
		if created, err := db.Put(insertKey, []byte(`{"inserted":true}`)); err != nil || !created {
			t.Fatalf("concurrent insert %q = (%v,%v)", insertKey, created, err)
		}
	}
	stop.Store(true)
	readers.Wait()
	if failure := failed.Load(); failure != nil {
		t.Fatal(*failure)
	}
}

func TestStorePageDBAppendRawSteadyAllocation(t *testing.T) {
	fs, want := buildStorePageTestData(t, 64, 16)
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	db := openPortableStorePageDB(t, path, 4096)
	defer db.Close()
	key := "account:00000007"
	buffer := make([]byte, 0, 256)
	if _, ok, err := db.AppendRaw(buffer[:0], key); err != nil || !ok {
		t.Fatalf("warm read = (%v,%v)", ok, err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		got, ok, err := db.AppendRaw(buffer[:0], key)
		if err != nil || !ok || !bytes.Equal(got, []byte(want[key])) {
			panic("unexpected resident read")
		}
	})
	if allocs != 0 {
		t.Fatalf("AppendRaw allocations = %g, want 0", allocs)
	}
}

func TestStorePageDBInsertSteadyAllocation(t *testing.T) {
	fs, err := store.New(store.Options{ChunkDocuments: 16})
	if err != nil {
		t.Fatal(err)
	}
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	db := openPortableStorePageDB(t, path, 4096)
	defer db.Close()
	var keys [16]string
	for i := range keys {
		keys[i] = fmt.Sprintf("zero-alloc-insert:%02d", i)
	}
	doc := []byte(`{"v":1}`)
	index := 0
	allocs := testing.AllocsPerRun(10, func() {
		created, err := db.Put(keys[index], doc)
		if err != nil || !created {
			panic("unexpected durable insert")
		}
		index++
	})
	if allocs != 0 {
		t.Fatalf("durable insert allocations = %g, want 0", allocs)
	}
}

func TestStorePageDBMoreThanHundredTimesResidentBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("large mutable bounded-residency smoke")
	}
	fs, _ := buildStorePageTestData(t, 4096, 16)
	path, size := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	const resident = int64(2 * 4096)
	if size <= 100*resident {
		t.Fatalf("test image = %d bytes, need >100x %d-byte cache", size, resident)
	}
	db, err := OpenStorePageDB(path, StorePageDBOptions{
		Open: StorePageOpenOptions{
			ResidentBytes: resident, MaxDocumentPageBytes: 4096,
		},
		CommitBackend: StorePageCommitPortable,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	key := "account:00002048"
	want := []byte(`{"id":2048,"mutable":true}`)
	if _, err := db.Put(key, want); err != nil {
		t.Fatal(err)
	}
	if created, err := db.Put("pressure-insert", []byte(`{"beyond_ram":true}`)); err != nil || !created {
		t.Fatalf("pressure insert = (%v,%v)", created, err)
	}
	got, ok, err := db.AppendRaw(make([]byte, 0, len(want)), key)
	if err != nil || !ok || !bytes.Equal(got, want) {
		t.Fatalf("updated pressure read = (%q,%v,%v)", got, ok, err)
	}
	if got, ok, err := db.AppendRaw(nil, "pressure-insert"); err != nil || !ok || string(got) != `{"beyond_ram":true}` {
		t.Fatalf("inserted pressure read = (%q,%v,%v)", got, ok, err)
	}
	stats := db.Stats()
	if stats.Cache.CapacityBytes != uint64(resident) || stats.Cache.ResidentBytes > uint64(resident) ||
		stats.FileBytes <= uint64(size) {
		t.Fatalf("bounded mutable stats = %+v", stats)
	}
	const minimumGrowth = uint64(8 * storePageQuantum) // update plus insert paths and states
	if growth := stats.FileBytes - uint64(size); growth < minimumGrowth {
		t.Fatalf("two-mutation COW growth = %d bytes, want at least %d", growth, minimumGrowth)
	}
}
