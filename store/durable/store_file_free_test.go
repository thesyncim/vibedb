package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// freeSetFromFile replays the durable free set from the bytes on disk, by hand.
//
// It deliberately shares no code with ReplayFreeLog: the mirror invariant is
// that the set the store holds in memory and the set the file describes are the
// same set, and an assertion built on the same traversal the writer trusts
// would pass whenever both are wrong together, which is precisely the case that
// corrupts. A map keyed by offset with last-record-wins is the definition of
// what a chain means, stated independently of how the store applies it.
func freeSetFromFile(t *testing.T, path string, pageSize int) []storeio.FreeExtent {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	// Recover the authoritative root the way Open does. On the deferred-canonical
	// lanes the fixed superblock is settled through the materialization capsules,
	// so a bare RecoverInlineStateRoot that trusts only the superblock and its
	// direct refs finds no valid root; RecoverMutableInlineStateRoot resolves the
	// capsules against each candidate. Its scratch must hold one MaxPageSize extent.
	bootstrap, err := storeio.DiscoverMutableInlineBootstrap(file)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	recovery, err := storeio.RecoverMutableInlineStateRoot(
		file, bootstrap.PageSize, bootstrap.MaterializationDamageGranule,
		make([]byte, bootstrap.MaxPageSize),
	)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	root, state := recovery.Root, recovery.State
	external := root.FreeDelta.ExternalPrev()
	if external == (storeio.PageRef{}) && root.FreeDelta.IndexHead() == (storeio.PageRef{}) &&
		root.FreeDelta.Len() == 0 {
		return nil
	}
	readPage := func(offset uint64, length uint32) []byte {
		page := make([]byte, length)
		if _, err := file.ReadAt(page, int64(offset)); err != nil {
			t.Fatalf("read page at %d: %v", offset, err)
		}
		return page
	}
	var chain [][]storeio.FreeDelta
	indexHead := root.FreeDelta.IndexHead()
	for ref := external; ref != (storeio.PageRef{}); {
		view, openErr := storeio.OpenFreeDeltaPage(
			readPage(ref.Offset, ref.Length), root.FileEnd, state.NextLogicalID,
		)
		if openErr != nil {
			t.Fatalf("open delta at %d: %v", ref.Offset, openErr)
		}
		records := make([]storeio.FreeDelta, view.Len())
		for i := range records {
			records[i], _ = view.DeltaAt(i)
		}
		chain = append(chain, records)
		ref = view.Prev()
		if len(chain) > storeio.FreeLogMaxChainPages {
			t.Fatalf("delta chain exceeds %d pages", storeio.FreeLogMaxChainPages)
		}
	}
	// Walk the index chain from its newest page back, collecting segments, then
	// read every segment. This deliberately re-derives the traversal rather than
	// calling the store's: a mirror assertion built on the writer's own walk
	// passes whenever both are wrong together.
	var segments []storeio.FreeSegment
	for ref := indexHead; ref != (storeio.PageRef{}); {
		view, openErr := storeio.OpenFreeIndexPage(readPage(ref.Offset, ref.Length), root.FileEnd, state.NextLogicalID)
		if openErr != nil {
			t.Fatalf("open index at %d: %v", ref.Offset, openErr)
		}
		page := make([]storeio.FreeSegment, view.Len())
		for i := range page {
			page[i], _ = view.SegmentAt(i)
		}
		segments = append(page, segments...)
		ref = view.Prev()
		if len(segments) > storeio.FreeLogMaxIndexPages*storeio.FreeIndexRecordCapacity(uint32(pageSize)) {
			t.Fatalf("index names more than %d segments", len(segments))
		}
	}
	final := make(map[uint64]storeio.FreeExtent)
	for _, segment := range segments {
		view, openErr := storeio.OpenFreeImagePage(
			readPage(segment.Ref.Offset, segment.Ref.Length), root.FileEnd, state.NextLogicalID)
		if openErr != nil {
			t.Fatalf("open segment at %d: %v", segment.Ref.Offset, openErr)
		}
		for i := range view.Len() {
			extent, _ := view.ExtentAt(i)
			final[extent.Offset] = extent
		}
	}
	for i := len(chain) - 1; i >= 0; i-- {
		for _, delta := range chain[i] {
			if delta.Op == storeio.FreeOpDelete {
				delete(final, delta.Extent.Offset)
				continue
			}
			final[delta.Extent.Offset] = delta.Extent
		}
	}
	for i := 0; i < root.FreeDelta.Len(); i++ {
		delta, _ := root.FreeDelta.DeltaAt(i)
		if delta.Op == storeio.FreeOpDelete {
			delete(final, delta.Extent.Offset)
			continue
		}
		final[delta.Extent.Offset] = delta.Extent
	}
	out := make([]storeio.FreeExtent, 0, len(final))
	for _, extent := range final {
		out = append(out, extent)
	}
	slices.SortFunc(out, func(a, b storeio.FreeExtent) int {
		switch {
		case a.Offset < b.Offset:
			return -1
		case a.Offset > b.Offset:
			return 1
		default:
			return 0
		}
	})
	return out
}

// assertFreeSetMirror checks the invariant every commit must preserve: the free
// set replayed from disk equals the one the allocator is about to hand out from.
// A drift in either direction is a bug, but they are not equally bad — a
// durable set larger than the in-memory one advertises live space — so the
// comparison is exact rather than one-sided.
// It reports how many extents it compared, or -1 when there was nothing to
// compare, so a caller can prove the assertion was not vacuous.
func assertFreeSetMirror(t *testing.T, fs *Collection, context string) int {
	t.Helper()
	if !fs.freeLoaded {
		// Nothing has been replayed yet, so the in-memory set is not a claim
		// about the file and there is nothing to compare it against.
		return -1
	}
	if err := flushPhysicalForTest(fs); err != nil {
		t.Fatalf("%s: physical flush: %v", context, err)
	}
	durable := freeSetFromFile(t, fs.file.Name(), fs.options.PageSize)
	// The durable set is the whole free set, reusable and fenced alike: a
	// retirement is written down by the commit that makes it, and only the
	// generation stamp says who may take it. Comparing against c.reusable alone
	// would have to tolerate a durable set larger than memory, which is exactly
	// the direction that advertises live space, so the fenced half is added back
	// instead of the comparison being loosened.
	memory := append(append([]storeio.FreeExtent(nil), fs.reusable...),
		fs.reclaimer.AppendPending(nil)...)
	// Exact never-durable retirements have already been removed from the
	// reclaimer, but intentionally wait in this bounded handoff until the next
	// allocator refresh merges them into reusable. They remain part of the
	// authoritative in-memory free set throughout that interval.
	memory = append(memory, fs.retirementAbsorbed...)
	slices.SortFunc(memory, func(a, b storeio.FreeExtent) int {
		switch {
		case a.Offset < b.Offset:
			return -1
		case a.Offset > b.Offset:
			return 1
		default:
			return 0
		}
	})
	if len(durable) != len(memory) {
		t.Fatalf("%s: durable free set has %d extents %+v, memory has %d %+v",
			context, len(durable), durable, len(memory), memory)
	}
	for i := range memory {
		if durable[i] != memory[i] {
			t.Fatalf("%s: free extent %d = durable %+v, memory %+v",
				context, i, durable[i], memory[i])
		}
	}
	assertFreeSetDisjointFromRoots(t, fs, durable, context)
	return len(durable)
}

// assertFreeSetDisjointFromRoots is the cheap half of the disjointness
// invariant: the pages the store cannot function without must never be inside
// advertised free space. The exhaustive half is
// TestFileStoreFreeSpaceHoldsNothingReachable, which overwrites every free byte
// and demands the store still reads.
func assertFreeSetDisjointFromRoots(t *testing.T, fs *Collection, free []storeio.FreeExtent, context string) {
	t.Helper()
	state := fs.state.Load()
	roots := []struct {
		name string
		ref  storeio.PageRef
	}{
		{"primary root", state.root.PrimaryRoot},
		{"exact index root", state.root.ExactIndexRoot},
		{"page catalog head", state.root.PageCatalogHead},
		{"free log head", state.freeHead},
	}
	for _, segment := range fs.freeSegments {
		roots = append(roots, struct {
			name string
			ref  storeio.PageRef
		}{"free image segment", segment.Ref})
	}
	for _, page := range fs.freeIndexPages {
		roots = append(roots, struct {
			name string
			ref  storeio.PageRef
		}{"free index page", page})
	}
	for _, page := range fs.freeDeltaPages {
		roots = append(roots, struct {
			name string
			ref  storeio.PageRef
		}{"free delta page", page})
	}
	for _, root := range roots {
		if root.ref == (storeio.PageRef{}) {
			continue
		}
		end := root.ref.Offset + uint64(root.ref.Length)
		for _, extent := range free {
			if root.ref.Offset < extent.Offset+extent.Length && extent.Offset < end {
				t.Fatalf("%s: %s at [%d,%d) overlaps free extent %+v",
					context, root.name, root.ref.Offset, end, extent)
			}
		}
	}
}

// freeChurnRound rewrites every key and then deletes a rotating third of them.
// The rotation matters: a workload that touches one key retires a near-constant
// handful of extents per commit, which the old one-edit-per-commit free tree
// could keep up with, so it never exhibited the loss this change exists to fix.
func freeChurnRound(t *testing.T, fs *Collection, keys, round int) {
	t.Helper()
	for key := range keys {
		padding := strings.Repeat("x", 120+(round*37+key*53)%300)
		doc := fmt.Sprintf(`{"round":%d,"key":%d,"status":%q,"padding":%q}`,
			round, key, [3]string{"active", "idle", "paused"}[(round+key)%3], padding)
		if _, err := fs.Put([]byte(fmt.Sprintf("key-%02d", key)), []byte(doc)); err != nil {
			t.Fatalf("round %d put %d: %v", round, key, err)
		}
	}
	for key := round % 3; key < keys; key += 3 {
		if _, err := fs.Delete([]byte(fmt.Sprintf("key-%02d", key))); err != nil {
			t.Fatalf("round %d delete %d: %v", round, key, err)
		}
	}
}

// Given the same logical content written in one session and across many
// close/reopen cycles, when the files are compared, then the multi-session file
// exceeds the single-session one by no more than the extents each Close was
// still holding back for the alternate recovery root.
//
// This is the headline defect. Extents reclaimed past the first lived only in
// memory, because a commit could persist exactly one free-tree edit, so every
// restart abandoned the rest: identical write volume cost 6.3 MiB in one
// session and 23.9 MiB across eight. The workload has to touch many keys — a
// single hot key retires few enough extents per commit that one edit keeps up,
// and the loss never appears.
//
// The bound is calibrated from the store's own accounting rather than chosen,
// because a ratio is not a test here: the defect also inflates the
// single-session file, so before this change eight sessions were 2.26x one
// session at 4.54 MiB, and after it they are 2.52x one session at 1.02 MiB. The
// honest question is not how the two files compare to each other but what a
// restart is allowed to cost, and the only thing a restart may still cost is
// ExtentReclaimer's pending set: extents retired too recently for the alternate
// superblock to have let go, which live in memory and are not written down by
// this change. Everything else — every extent the free set had already accepted
// — must survive, and the difference between the two is what this asserts.
func TestFileStoreFreeSetSurvivesRestartsWithoutGrowingTheFile(t *testing.T) {
	const (
		keys     = 48
		rounds   = 8
		sessions = 8
	)
	options := testFileStoreOptions()
	options.ResidentBytes = 8 << 20
	options.BufferCount = 1024
	options.MaxRetiredExtents = 4096

	single, err := os.CreateTemp(t.TempDir(), "free-restart-single-*")
	if err != nil {
		t.Fatal(err)
	}
	defer single.Close()
	oneSession, err := Create(single, options)
	if err != nil {
		t.Fatal(err)
	}
	for round := range rounds {
		freeChurnRound(t, oneSession, keys, round)
	}
	if err := oneSession.Close(); err != nil {
		t.Fatal(err)
	}
	singleSize := fileSizeOf(t, single.Name())

	split, err := os.CreateTemp(t.TempDir(), "free-restart-split-*")
	if err != nil {
		t.Fatal(err)
	}
	defer split.Close()
	created, err := Create(split, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := created.Close(); err != nil {
		t.Fatal(err)
	}
	abandoned := uint64(0)
	for round := range sessions {
		session, openErr := Open(split, options)
		if openErr != nil {
			t.Fatalf("session %d open: %v", round, openErr)
		}
		freeChurnRound(t, session, keys, round)
		assertFreeSetMirror(t, session, fmt.Sprintf("session %d", round))
		abandoned += session.Stats().PendingRetiredBytes
		if err := session.Close(); err != nil {
			t.Fatalf("session %d close: %v", round, err)
		}
	}
	splitSize := fileSizeOf(t, split.Name())

	// One root page per restart plus one largest class-5 extent overall is the
	// bounded geometry slack: each reopened store's first root may land before
	// reclamation has anything to offer it, and the multi-session packing
	// boundary may retain one alternate wide leaf that a continuous session
	// reuses internally. Anything beyond that must be explained by extents still
	// fenced at Close.
	allowance := abandoned +
		uint64(sessions*options.PageSize+options.MaxPageSize)
	t.Logf("one session = %d bytes, %d sessions = %d bytes, excess %d, allowance %d "+
		"(%d bytes of extents were still fenced across the eight Closes)",
		singleSize, sessions, splitSize, splitSize-singleSize, allowance, abandoned)
	if uint64(splitSize) > uint64(singleSize)+allowance {
		t.Fatalf("%d sessions produced %d bytes against %d for one session, an excess of %d "+
			"that the %d bytes fenced at Close cannot account for",
			sessions, splitSize, singleSize, splitSize-singleSize, abandoned)
	}
}

func fileSizeOf(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

// Given a store whose advertised free space is overwritten byte for byte with
// garbage, when it is reopened, then every document and index answer
// is unchanged.
//
// This is the disjointness invariant stated as the failure it prevents rather
// than as a traversal: if any free extent overlapped a page reachable from the
// published root — a directory node, a document, an overflow piece, a posting
// list, a float64 stripe, the state root, or the free log's own pages — the
// overwrite destroys it and the reopen cannot answer. Asserting it this way
// covers every page kind, including the ones a hand-written walker would forget.
func TestFileStoreFreeSpaceHoldsNothingReachable(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "free-disjoint-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.ResidentBytes = 8 << 20
	options.BufferCount = 1024
	options.MaxRetiredExtents = 4096
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	const keys = 48
	for round := range 6 {
		freeChurnRound(t, fs, keys, round)
	}
	expected := make(map[string]string, keys)
	for key := range keys {
		name := fmt.Sprintf("key-%02d", key)
		value, ok, getErr := fs.AppendRaw(nil, []byte(name))
		if getErr != nil {
			t.Fatal(getErr)
		}
		if ok {
			expected[name] = string(value)
		}
	}
	// Capture the durable free set only after a checkpoint has folded every
	// journaled acknowledgement into the store root. On the synchronous lane a Put
	// acknowledges through a synced journal record whose store-root fold is
	// deferred, so a free set read while the journal still leads the checkpointed
	// root names extents a later commit has already reused; overwriting those would
	// destroy live pages and the reopen would fall back to an older root the
	// recycled journal now leads. assertFreeSetMirror flushes fs first, so the root
	// it leaves is the one freeSetFromFile reads here.
	assertFreeSetMirror(t, fs, "before overwrite")
	free := freeSetFromFile(t, file.Name(), options.PageSize)
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	if len(free) == 0 {
		t.Fatal("workload advertised no free space, so the sweep proves nothing")
	}
	poisoned := uint64(0)
	for _, extent := range free {
		garbage := make([]byte, extent.Length)
		for i := range garbage {
			garbage[i] = 0xDD
		}
		if _, err := file.WriteAt(garbage, int64(extent.Offset)); err != nil {
			t.Fatal(err)
		}
		poisoned += extent.Length
	}
	t.Logf("overwrote %d free extents totalling %d bytes", len(free), poisoned)

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatalf("reopen after overwriting free space: %v", err)
	}
	defer reopened.Close()
	for name, want := range expected {
		got, ok, getErr := reopened.AppendRaw(nil, []byte(name))
		if getErr != nil || !ok || string(got) != want {
			t.Fatalf("%s after overwriting free space = (%q,%v,%v)", name, got, ok, getErr)
		}
	}
	if got := reopened.Len(); got != uint64(len(expected)) {
		t.Fatalf("document count after overwriting free space = %d, want %d", got, len(expected))
	}
	// The reopened store must also be able to keep writing: the free set it
	// replayed is the one that was just proven to hold nothing live.
	for round := 6; round < 8; round++ {
		freeChurnRound(t, reopened, keys, round)
		assertFreeSetMirror(t, reopened, fmt.Sprintf("round %d after overwrite", round))
	}
}

// Given a fragmented free set, when a commit needs more space than any single
// extent holds, then it draws from several and the file does not grow.
func TestFileStoreCommitSpansSeveralFreeExtents(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "free-multi-extent-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.ResidentBytes = 8 << 20
	options.BufferCount = 1024
	options.MaxRetiredExtents = 4096
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	const keys = 48
	// Churn to steady state before measuring, checkpointing every round so the
	// physical layout settles under the same per-round cadence the measurement loop
	// below uses. The first few rounds grow the file as the root leaf widens into a
	// MaxPageSize frame no fragmented free extent can serve and the retirement
	// reclaimer fills its in-flight fence; past that the physical FileEnd plateaus,
	// which is the reuse property this test asserts. On the journaled lane a Put
	// also stages frames in a volatile append region far past the durable FileEnd,
	// so each checkpoint materializes that region into reused space.
	for round := range 15 {
		freeChurnRound(t, fs, keys, round)
		if err := fs.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if len(fs.reusable) < 2 {
		t.Fatalf("workload left %d free extents, need a fragmented set", len(fs.reusable))
	}
	var largest, total uint64
	for _, extent := range fs.reusable {
		largest = max(largest, extent.Length)
		total += extent.Length
	}
	before := fs.state.Load().super.FileEnd
	spread, grew := 0, false
	for round := 15; round < 25; round++ {
		reuseBefore := len(fs.reusable)
		freeChurnRound(t, fs, keys, round)
		// assertFreeSetMirror flushes, so the FileEnd sampled after it is the durable
		// physical high-water, comparable to the pre-loop checkpoint above.
		assertFreeSetMirror(t, fs, fmt.Sprintf("round %d", round))
		if reuseBefore >= 2 {
			spread++
		}
		if fs.state.Load().super.FileEnd > before {
			grew = true
		}
	}
	t.Logf("largest free extent %d, total free %d, FileEnd %d -> %d over %d fragmented rounds",
		largest, total, before, fs.state.Load().super.FileEnd, spread)
	if grew {
		t.Fatalf("FileEnd advanced from %d to %d while %d bytes were free across %d extents",
			before, fs.state.Load().super.FileEnd, total, len(fs.reusable))
	}
}

func TestFileStoreLazyFreeSegmentPromotionCyclesBoundedArena(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "free-lazy-promotion-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	// FallbackGeneration is the committer's alternate-root floor: the generation of
	// the older durable superblock, below which a retired extent is reusable. It
	// advances only when a physical commit publishes a new durable root, and the
	// synchronous primary lane defers that publish from a Put to a checkpoint. Two
	// checkpoints leave both superblock slots at generation two or later, so the
	// generation-one extents this promotion test plants fall below the floor and
	// are reusable.
	if _, err := fs.Put([]byte("a"), []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := fs.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Put([]byte("b"), []byte(`{"v":2}`)); err != nil {
		t.Fatal(err)
	}
	if err := fs.Flush(); err != nil {
		t.Fatal(err)
	}
	if floor := fs.committer.FallbackGeneration(); floor <= 1 {
		t.Fatalf("fallback generation = %d, need generation 1 reusable", floor)
	}

	pageSize := uint64(options.PageSize)
	low := []storeio.FreeExtent{
		{Offset: 16 * pageSize, Length: pageSize, RetiredGeneration: 1},
		{Offset: 18 * pageSize, Length: pageSize, RetiredGeneration: 1},
	}
	high := []storeio.FreeExtent{
		{Offset: 64 * pageSize, Length: pageSize, RetiredGeneration: 1},
		{Offset: 66 * pageSize, Length: pageSize, RetiredGeneration: 1},
	}
	current := *fs.state.Load()
	current.super.FileEnd = max(current.super.FileEnd, 128*pageSize)
	logical := current.root.NextLogicalID
	current.root.NextLogicalID += 2
	if err := file.Truncate(int64(current.super.FileEnd)); err != nil {
		t.Fatal(err)
	}
	writeSegment := func(
		offset uint64, logicalID uint64, extents []storeio.FreeExtent,
	) storeio.PageRef {
		t.Helper()
		page := make([]byte, options.PageSize)
		if _, encodeErr := storeio.EncodeFreeImagePage(
			page,
			storeio.FreeLogHeader{
				StoreID: fs.storeID, Generation: current.root.Generation,
				LogicalID: logicalID, PageSize: uint32(options.PageSize),
			},
			extents, current.super.FileEnd, current.root.NextLogicalID,
		); encodeErr != nil {
			t.Fatal(encodeErr)
		}
		if _, writeErr := file.WriteAt(page, int64(offset)); writeErr != nil {
			t.Fatal(writeErr)
		}
		return storeio.PageRef{
			Offset: offset, LogicalID: logicalID,
			Generation: current.root.Generation,
			Length:     uint32(options.PageSize), Kind: storeio.PageFreeImage,
		}
	}
	lowRef := writeSegment(120*pageSize, logical, low)
	highRef := writeSegment(121*pageSize, logical+1, high)
	fs.state.Store(&current)
	fs.pageValidator.update(&current)

	fs.freeSegments = append(fs.freeSegments[:0],
		storeio.FreeSegment{
			Ref: lowRef, FirstOffset: low[0].Offset,
			LargestFree: pageSize, Count: uint32(len(low)),
		},
		storeio.FreeSegment{
			Ref: highRef, FirstOffset: high[0].Offset,
			LargestFree: pageSize, Count: uint32(len(high)),
		},
	)
	fs.freeResident = append(fs.freeResident[:0], false, true)
	fs.freeDirty = append(fs.freeDirty[:0], false, false)
	fs.reusable = append(fs.reusable[:0], high...)
	// Two slots force promotion to evict one immutable clean segment before it
	// can load the other. The production arena is larger but follows the same
	// path whenever its configured bound is full.
	fs.reusable = fs.reusable[:len(fs.reusable):len(fs.reusable)]
	fs.resetFreeNonResident()
	if err := fs.rebuildReusableIndex(); err != nil {
		t.Fatal(err)
	}

	reusable, changed, err := fs.promoteReusable(pageSize, high[0].Offset, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("first promotion reported no resident-set change")
	}
	if !slices.Equal(reusable, low) {
		t.Fatalf("first promotion = %+v, want low segment %+v", reusable, low)
	}
	if got, want := fs.freeResident, []bool{true, false}; !slices.Equal(got, want) {
		t.Fatalf("residency after first promotion = %v, want %v", got, want)
	}
	if rank, ok := fs.freeExtentIndex.FirstFit(fs.reusable, pageSize); !ok ||
		rank != 0 || fs.reusable[rank].Offset != low[0].Offset {
		t.Fatalf("indexed first fit after promotion = %d, %v in %+v", rank, ok, fs.reusable)
	}

	// Model publication consuming the promoted segment. Its dirty resident
	// marker prevents eviction/reload from the stale page, while the freed
	// slots let the previously evicted high segment become resident in turn.
	for i := range fs.reusable {
		fs.reusable[i].Length = 0
	}
	fs.freeDirty[0] = true
	fs.finalizeReusable()
	reusable, changed, err = fs.promoteReusable(pageSize, ^uint64(0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("second promotion reported no resident-set change")
	}
	if !slices.Equal(reusable, high) {
		t.Fatalf("second promotion = %+v, want high segment %+v", reusable, high)
	}
	if fs.freeNonResident != 0 {
		t.Fatalf("nonresident segments = %d, want 0 after cycling both", fs.freeNonResident)
	}
}

func TestFileStoreResidentIndexedFirstFitAllocatesNothing(t *testing.T) {
	reusable := []storeio.FreeExtent{
		{Offset: 16 << 10, Length: 4 << 10, RetiredGeneration: 1},
		{Offset: 32 << 10, Length: 64 << 10, RetiredGeneration: 1},
	}
	maxima := make([]uint64, storeio.FreeExtentIndexCapacity(len(reusable)))
	c := &Collection{reusable: reusable, freeExtentMaxima: maxima}
	if err := c.rebuildReusableIndex(); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(1_000, func() {
		resident, changed, err := c.promoteReusable(4<<10, reusable[0].Offset, nil)
		if err != nil {
			panic(err)
		}
		if changed {
			panic("resident-only lookup changed the set")
		}
		rank, ok := c.freeExtentIndex.FirstFit(resident, 4<<10)
		if !ok || rank != 0 {
			panic("indexed first fit failed")
		}
	})
	if allocations != 0 {
		t.Fatalf("resident indexed first fit allocations = %g, want 0", allocations)
	}
}

// Repeated churn over the same bounded set of offsets should remain entirely
// inside the cumulative fixed root. This is the common-case space result: the
// latest operation per offset replaces its predecessor without allocating
// routine delta pages or weakening the exact free-set mirror.
func TestFileStoreInlineFreeLogAbsorbsBoundedChurn(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "free-fold-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.ResidentBytes = 8 << 20
	options.BufferCount = 1024
	options.MaxRetiredExtents = 4096
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	const keys = 32
	longest := 0
	for round := range 12 {
		previous := len(fs.freeDeltaPages)
		freeChurnRound(t, fs, keys, round)
		longest = max(longest, previous)
		if len(fs.freeDeltaPages) > storeio.FreeLogMaxChainPages {
			t.Fatalf("round %d left a %d-page chain, bound is %d",
				round, len(fs.freeDeltaPages), storeio.FreeLogMaxChainPages)
		}
		if len(fs.freeIndexPages) > storeio.FreeLogMaxIndexPages {
			t.Fatalf("round %d left a %d-page index, bound is %d",
				round, len(fs.freeIndexPages), storeio.FreeLogMaxIndexPages)
		}
		assertFreeSetMirror(t, fs, fmt.Sprintf("round %d", round))
	}
	if longest != 0 || len(fs.freeDeltaPages) != 0 {
		t.Fatalf("bounded churn allocated an external delta chain: longest=%d final=%d",
			longest, len(fs.freeDeltaPages))
	}
	if fs.inlineFree.Len() == 0 {
		t.Fatal("bounded churn left no cumulative inline free records")
	}
	t.Logf("%d inline records, no external delta pages", fs.inlineFree.Len())
}

func TestFileStoreInlineFreeLogSpillsAndReopens(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "free-inline-spill-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.BufferCount = 4096
	options.MaxRetiredExtents = 4096
	// A batch cannot split a fresh leaf mid-fold, so the seed batch's documents
	// must fit one leaf. Sixteen ~1.2 KiB documents stay within that budget; the
	// collection has split into many leaves by the time the later batches land,
	// and the spill this test asserts comes from the retirement volume of deleting
	// every key, not from any single batch's width.
	options.MaxBatchDocuments = 16
	options.InlineValueBytes = 2048
	options.ResidentBytes = 32 << 20
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}

	// Class 5's denser leaves and exact retirement coalescing keep the former
	// 512 small-row workload below the 106-record inline root. A wider corpus
	// still runs quickly but creates enough independently retired extents to
	// prove the external spill/replay path remains live.
	const documents = 2048
	for base := 0; base < documents; base += options.MaxBatchDocuments {
		if err := fs.Update(func(batch *WriteBatch) error {
			for i := base; i < base+options.MaxBatchDocuments; i++ {
				value := []byte(fmt.Sprintf(
					`{"id":%d,"padding":%q}`, i, strings.Repeat("x", 1200),
				))
				if err := batch.Put([]byte(fmt.Sprintf("spill-%03d", i)), value); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for base := 0; base < documents; base += options.MaxBatchDocuments {
		if err := fs.Update(func(batch *WriteBatch) error {
			for i := base; i < base+options.MaxBatchDocuments; i++ {
				if err := batch.Delete([]byte(fmt.Sprintf("spill-%03d", i))); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.Flush(); err != nil {
		t.Fatal(err)
	}
	if fs.inlineFree.ExternalPrev() == (storeio.PageRef{}) ||
		len(fs.freeDeltaPages) == 0 {
		t.Fatalf("large random diff did not spill: inline=%d external=%d",
			fs.inlineFree.Len(), len(fs.freeDeltaPages))
	}
	want := freeSetFromFile(t, file.Name(), options.PageSize)
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.refreshReusable(reopened.state.Load()); err != nil {
		t.Fatal(err)
	}
	got := append([]storeio.FreeExtent(nil), reopened.reusable...)
	got = append(got, reopened.reclaimer.AppendPending(nil)...)
	slices.SortFunc(got, compareFreeExtentOffset)
	if !slices.Equal(got, want) {
		t.Fatalf("replayed spill differs:\n got %+v\nwant %+v", got, want)
	}
}

// Given a free set spanning several segments of which one has changed, when the
// next fold is planned, then only that segment is rebuilt and every other keeps
// its exact durable page.
//
// This is the property that replaced the linked image, and it is asserted here
// rather than through a workload because a workload cannot reach it cheaply:
// coalescing keeps an ordinary churn's free set inside one segment, and a
// single-segment set has nothing to carry forward. The plan is the whole
// decision, so testing it directly tests the thing.
//
// Two ways to get it wrong are both fatal, in opposite directions. Rebuilding an
// unchanged segment is the old cost — a fold that rewrites everything is what
// forced the free set to fit sixteen pages, about 2,700 extents, roughly 11 MiB
// of trackable space, which a multi-terabyte store exhausts. Carrying a changed
// segment forward is worse: its page still describes extents the store has since
// handed out, so the next open reads a free set overlapping live pages.
func TestFreeFoldPlanRebuildsOnlyDirtySegments(t *testing.T) {
	const pageSize = 4096
	perSegment := storeio.FreeImageRecordCapacity(pageSize)
	c := &Collection{
		freeImagePerPage: perSegment,
		freeIndexPerPage: storeio.FreeIndexRecordCapacity(pageSize),
	}
	// Three segments' worth of extents, one page each, laid out so that every
	// segment owns a contiguous, non-overlapping run of offsets.
	total := 3 * perSegment
	live := make([]storeio.FreeExtent, total)
	for i := range live {
		live[i] = storeio.FreeExtent{
			Offset: uint64(2*i+4) * pageSize, Length: pageSize, RetiredGeneration: 1,
		}
	}
	for i := range 3 {
		first := live[i*perSegment]
		c.freeSegments = append(c.freeSegments, storeio.FreeSegment{
			Ref: storeio.PageRef{
				Offset: uint64(1<<20 + i*pageSize), LogicalID: uint64(100 + i), Generation: 3,
				Length: pageSize, Kind: storeio.PageFreeImage,
			},
			FirstOffset: first.Offset, LargestFree: pageSize, Count: uint32(perSegment),
		})
	}
	c.resetFreeDirty()

	// A change inside the middle segment's offset range dirties that segment and
	// no other.
	c.markFreeDirty(live[perSegment+3].Offset)
	if c.freeDirtyCount != 1 || !c.freeDirty[1] {
		t.Fatalf("marking one offset dirtied %d segments %v", c.freeDirtyCount, c.freeDirty)
	}
	plan, err := c.planFreeFold(live)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.rebuilt) != 1 {
		t.Fatalf("plan rebuilds %d segment pages, want 1: a fold that rewrites the whole "+
			"image is the cost the segment index removed", len(plan.rebuilt))
	}
	if got := plan.rebuilt[0]; got != [2]int{perSegment, 2 * perSegment} {
		t.Fatalf("rebuilt span = %v, want the middle segment's range [%d,%d)",
			got, perSegment, 2*perSegment)
	}
	if len(plan.order) != 3 {
		t.Fatalf("plan publishes %d segments, want 3", len(plan.order))
	}
	for rank, slot := range plan.order {
		wantFresh := rank == 1
		if slot.fresh != wantFresh {
			t.Fatalf("segment %d fresh = %v, want %v", rank, slot.fresh, wantFresh)
		}
		if !wantFresh && slot.carried != c.freeSegments[rank] {
			t.Fatalf("segment %d carried %+v, want %+v", rank, slot.carried, c.freeSegments[rank])
		}
	}

	// Splitting is the case a tree could not survive here. A segment that has
	// outgrown one page becomes two, which appends one entry to an array the fold
	// rewrites whole — so nothing propagates and no second allocation round is
	// needed to describe the first.
	grown := make([]storeio.FreeExtent, 0, total+perSegment)
	grown = append(grown, live[:perSegment]...)
	for i := range perSegment {
		grown = append(grown, storeio.FreeExtent{
			Offset:            live[perSegment].Offset + uint64(2*i+1)*pageSize,
			Length:            pageSize,
			RetiredGeneration: 1,
		})
		grown = append(grown, live[perSegment+i])
	}
	grown = append(grown, live[2*perSegment:]...)
	slices.SortFunc(grown, func(a, b storeio.FreeExtent) int {
		switch {
		case a.Offset < b.Offset:
			return -1
		case a.Offset > b.Offset:
			return 1
		default:
			return 0
		}
	})
	split, err := c.planFreeFold(grown)
	if err != nil {
		t.Fatal(err)
	}
	if len(split.rebuilt) != 2 {
		t.Fatalf("a segment holding %d extents split into %d pages, want 2",
			2*perSegment, len(split.rebuilt))
	}
	if len(split.order) != 4 {
		t.Fatalf("split published %d segments, want 4", len(split.order))
	}
}

// Given more dirty segments than one fold may rewrite, when the fold is planned,
// then it reports capacity rather than allocating past the transaction reserve.
//
// Failing closed here is deliberate and matches the rest of this subsystem:
// under-reporting free space leaks and is recoverable, while a transaction that
// discovers halfway through that it needs one more page has already staged pages
// it cannot describe.
func TestFreeFoldPlanRefusesToExceedTheFoldReserve(t *testing.T) {
	const pageSize = 4096
	perSegment := storeio.FreeImageRecordCapacity(pageSize)
	c := &Collection{
		freeImagePerPage: perSegment,
		freeIndexPerPage: storeio.FreeIndexRecordCapacity(pageSize),
	}
	segments := storeio.FreeLogMaxFoldSegments + 1
	live := make([]storeio.FreeExtent, segments*perSegment)
	for i := range live {
		live[i] = storeio.FreeExtent{
			Offset: uint64(2*i+4) * pageSize, Length: pageSize, RetiredGeneration: 1,
		}
	}
	for i := range segments {
		c.freeSegments = append(c.freeSegments, storeio.FreeSegment{
			Ref: storeio.PageRef{
				Offset: uint64(1<<24 + i*pageSize), LogicalID: uint64(100 + i), Generation: 3,
				Length: pageSize, Kind: storeio.PageFreeImage,
			},
			FirstOffset: live[i*perSegment].Offset, LargestFree: pageSize,
			Count: uint32(perSegment),
		})
	}
	c.resetFreeDirty()
	for i := range segments {
		c.markFreeDirty(live[i*perSegment].Offset)
	}
	if _, err := c.planFreeFold(live); !errors.Is(err, storeio.ErrRetiredExtentCapacity) {
		t.Fatalf("planning %d dirty segments against a reserve of %d = %v, want %v",
			segments, storeio.FreeLogMaxFoldSegments, err, storeio.ErrRetiredExtentCapacity)
	}
}

// Given a fold that rewrote one of three segments, when the superseded pages are
// handed to the reclaimer, then only the rewritten segment's page is retired,
// and a superseded page landing in a segment that had not changed pulls that
// segment into the fold as well.
//
// Both halves point the same dangerous way. A carried segment is still named by
// the published index, so retiring it advertises as free the very bytes the next
// open reads the free set out of. And a superseded page is free space the moment
// the fold publishes, so the segment that owns its offset has to describe it —
// if that segment is carried forward unchanged, the page is retired in memory
// and described nowhere, which is the restart leak one page at a time.
//
// When the image was a linked list every fold replaced every page and both
// hazards were impossible by construction; carrying segments forward is what
// created them.
func TestRetireFreeLogPagesSparesCarriedSegments(t *testing.T) {
	const pageSize = 4096
	newCollection := func() *Collection {
		c := &Collection{
			freeImagePerPage: storeio.FreeImageRecordCapacity(pageSize),
			freeIndexPerPage: storeio.FreeIndexRecordCapacity(pageSize),
			retireScratch:    make([]storeio.FreeExtent, 0, 16),
		}
		for i := range 3 {
			c.freeSegments = append(c.freeSegments, storeio.FreeSegment{
				// Each segment's own page sits inside the offset range that segment
				// owns, which is what an allocator taking from the lowest extent
				// that fits naturally produces. A segment page filed under some
				// other segment would drag that segment into every fold.
				Ref: storeio.PageRef{
					Offset: uint64(1<<(16+i)) + 3*pageSize, LogicalID: uint64(100 + i),
					Generation: 3, Length: pageSize, Kind: storeio.PageFreeImage,
				},
				FirstOffset: uint64(1 << (16 + i)), LargestFree: pageSize, Count: 1,
			})
		}
		c.resetFreeDirty()
		return c
	}
	state := &fileStoreState{root: storeio.StateRoot{Generation: 7}}

	// The superseded index and chain pages are placed inside the range of the
	// segment that is already dirty, so the only segment the fold must rewrite is
	// the one whose extents actually moved.
	c := newCollection()
	inside := c.freeSegments[1].FirstOffset
	c.freeIndexPages = append(c.freeIndexPages, storeio.PageRef{
		Offset: inside + pageSize, LogicalID: 200, Generation: 3,
		Length: pageSize, Kind: storeio.PageFreeIndex,
	})
	c.freeDeltaPages = append(c.freeDeltaPages, storeio.PageRef{
		Offset: inside + 2*pageSize, LogicalID: 300, Generation: 3,
		Length: pageSize, Kind: storeio.PageFreeDelta,
	})
	c.freeDirty[1] = true
	c.freeDirtyCount = 1
	if err := c.retireFreeLogPages(state, true); err != nil {
		t.Fatal(err)
	}
	retired := make(map[uint64]bool, len(c.retireScratch))
	for _, extent := range c.retireScratch {
		if extent.RetiredGeneration != 7 {
			t.Fatalf("retired %+v at the wrong generation; the fence is what keeps the "+
				"alternate recovery root readable", extent)
		}
		retired[extent.Offset] = true
	}
	if !retired[c.freeSegments[1].Ref.Offset] {
		t.Fatal("the rewritten segment's page was not retired, so every fold leaks one page")
	}
	for _, rank := range []int{0, 2} {
		if retired[c.freeSegments[rank].Ref.Offset] {
			t.Fatalf("segment %d was carried forward by the new index and retired anyway: "+
				"the store would hand out the page its own free set is read from", rank)
		}
		if c.freeDirty[rank] {
			t.Fatalf("segment %d was dirtied by pages that do not fall in its range", rank)
		}
	}
	if !retired[c.freeIndexPages[0].Offset] || !retired[c.freeDeltaPages[0].Offset] {
		t.Fatalf("the superseded index and chain pages must be retired: %+v", c.retireScratch)
	}
	if len(c.retireScratch) != 3 {
		t.Fatalf("retired %d pages %+v, want the one rewritten segment plus the index and chain",
			len(c.retireScratch), c.retireScratch)
	}

	// Now the other half: the superseded chain page falls inside a segment that
	// had not changed, so that segment has to join the fold or the page it just
	// freed would be described by nothing.
	spread := newCollection()
	spread.freeDeltaPages = append(spread.freeDeltaPages, storeio.PageRef{
		Offset: spread.freeSegments[2].FirstOffset + pageSize, LogicalID: 300, Generation: 3,
		Length: pageSize, Kind: storeio.PageFreeDelta,
	})
	spread.freeDirty[0] = true
	spread.freeDirtyCount = 1
	if err := spread.retireFreeLogPages(state, true); err != nil {
		t.Fatal(err)
	}
	if !spread.freeDirty[2] {
		t.Fatal("a chain page freed inside an unchanged segment left that segment carried " +
			"forward, so the page it freed is retired in memory and described on disk nowhere")
	}
	spreadRetired := make(map[uint64]bool, len(spread.retireScratch))
	for _, extent := range spread.retireScratch {
		spreadRetired[extent.Offset] = true
	}
	if !spreadRetired[spread.freeSegments[2].Ref.Offset] {
		t.Fatal("segment 2 joined the fold but its superseded page was not retired")
	}
	if spreadRetired[spread.freeSegments[1].Ref.Offset] {
		t.Fatal("segment 1 was untouched by both the change and the freed page, and was retired")
	}
}

// Given a store reopened many times with one small write per session, when the
// file is measured after each cycle, then it stops growing.
//
// This is the restart leak stated as what it looked like: thirty open-write-close
// cycles over constant content grew the file by exactly three pages every time,
// forever, and the store reported the same 12,288 bytes of pending retirements at
// every Close. Those pages were the ones the session's last commits had retired.
// They were free, but they were free only in the reclaimer's memory — an extent
// stayed there until no reader and no recovery root could reach the generation
// that retired it, which for the final commits of a session is never, because
// there is no further commit — so Close threw them away and the next open could
// not find them.
//
// The fix is that a retirement is written down by the commit that makes it, and
// the replay puts it back into the reclaimer rather than into the allocator's
// array when its generation is still fenced. The assertion is deliberately on
// file size rather than on any counter the fix itself maintains, and the growth
// during the first cycles is measured rather than assumed so that a store which
// simply stopped reclaiming would fail this too.
func TestFileStoreSurvivesManyRestartsWithoutGrowingTheFile(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "free-restart-loop-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.ResidentBytes = 8 << 20
	options.BufferCount = 1024
	options.MaxRetiredExtents = 4096
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	const keys = 24
	for round := range 4 {
		freeChurnRound(t, fs, keys, round)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}

	const cycles = 30
	// The first few cycles may still settle: a reopened store cannot reclaim what
	// its own predecessor retired until the alternate superblock has moved past
	// it, which takes two generations. Growth is measured from the point that
	// settling is over, so the assertion is about the steady state and not about
	// how fast it is reached.
	const settle = 6
	var settled int64
	for cycle := range cycles {
		session, openErr := Open(file, options)
		if openErr != nil {
			t.Fatalf("cycle %d open: %v", cycle, openErr)
		}
		if _, err := session.Put([]byte("key-00"), []byte(`{"cycle":1}`)); err != nil {
			t.Fatalf("cycle %d put: %v", cycle, err)
		}
		assertFreeSetMirror(t, session, fmt.Sprintf("cycle %d", cycle))
		if err := session.Close(); err != nil {
			t.Fatalf("cycle %d close: %v", cycle, err)
		}
		size := fileSizeOf(t, file.Name())
		if cycle == settle {
			settled = size
		}
		if cycle > settle && size != settled {
			t.Fatalf("cycle %d left the file at %d bytes against %d after cycle %d, "+
				"a growth of %d bytes over constant content: every restart is abandoning "+
				"the extents its last commits retired",
				cycle, size, settled, settle, size-settled)
		}
	}
	if settled == 0 {
		t.Fatal("no cycle established a baseline, so the assertion never ran")
	}
	t.Logf("%d open-write-close cycles held the file at %d bytes", cycles-settle, settled)
}

// Given a snapshot held open while the store churns, when reclamation metadata
// fills, then the write that fills it is refused with an error naming the
// snapshot, releasing the snapshot restores writes, and nothing is lost.
func TestFileStoreLongHeldSnapshotCostsBoundedBackpressure(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "free-long-snapshot-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.MaxRetiredExtents = 1024
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	const keys = 16
	body := strings.Repeat("x", 300)
	put := func(round, i int) error {
		_, err := fs.Put([]byte(fmt.Sprintf("k%02d", i)), fmt.Appendf(nil,
			`{"round":%d,"v":%q}`, round, body))
		return err
	}
	for round := range 40 {
		for i := range keys {
			if err := put(round, i); err != nil {
				t.Fatalf("warm-up Put failed at round %d: %v", round, err)
			}
		}
	}

	pinned, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	pinnedGeneration := fs.Generation()
	writes, failure := 0, error(nil)
	for round := 40; round < 4000 && failure == nil; round++ {
		for i := range keys {
			if err := put(round, i); err != nil {
				failure = err
				break
			}
			writes++
		}
	}
	if failure == nil {
		pinned.Close()
		t.Fatal("a snapshot pinned for 3,960 rounds never produced backpressure, so this " +
			"measures nothing")
	}
	if !errors.Is(failure, storeio.ErrRetiredExtentCapacity) {
		pinned.Close()
		t.Fatalf("long-held snapshot failed with %v, want bounded retirement backpressure", failure)
	}
	// The error has to say which snapshot is responsible. "Retired extent
	// capacity exhausted" on its own reads like corruption, and an operator
	// cannot act on it.
	if message := failure.Error(); !strings.Contains(message, "snapshot") ||
		!strings.Contains(message, fmt.Sprintf("generation %d", pinnedGeneration)) {
		pinned.Close()
		t.Fatalf("backpressure did not name the pinning snapshot at generation %d: %v",
			pinnedGeneration, failure)
	}
	stats := fs.Stats()
	t.Logf("a snapshot pinned at generation %d absorbed %d writes before backpressure; "+
		"%d bytes of retired extents were fenced, arena capacity %d extents, "+
		"file %d bytes",
		pinnedGeneration, writes, stats.PendingRetiredBytes, options.MaxRetiredExtents,
		stats.FileEnd)

	// Releasing the reader must clear it, and every document must survive: the
	// resumed reclamation must not have handed out space the snapshot reached.
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	for round := 5000; round < 5016; round++ {
		for i := range keys {
			if err := put(round, i); err != nil {
				t.Fatalf("Put still failing at round %d after the snapshot was released: %v",
					round, err)
			}
		}
	}
	for i := range keys {
		key := fmt.Sprintf("k%02d", i)
		got, ok, getErr := fs.AppendRaw(nil, []byte(key))
		if getErr != nil || !ok || !bytes.Contains(got, []byte(`"round":5015`)) {
			t.Fatalf("AppendRaw(%s) after recovery = (%s,%v,%v)", key, got, ok, getErr)
		}
	}
	assertFreeSetMirror(t, fs, "after the pinned snapshot was released")
}

func TestFileStoreRetirementOverflowNeverAbandonsSpace(t *testing.T) {
	leases, err := storeio.NewGenerationLeases(storeio.GenerationLeaseOptions{MaxLeases: 1})
	if err != nil {
		t.Fatal(err)
	}
	reclaimer, err := storeio.NewExtentReclaimer(
		leases,
		storeio.ExtentReclaimerOptions{MaxRetiredExtents: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := reclaimer.Retire(storeio.FreeExtent{
		Offset: 4096, Length: 4096, RetiredGeneration: 1,
	}); err != nil {
		t.Fatal(err)
	}
	collection := &Collection{
		leases: leases, reclaimer: reclaimer,
		retireScratch: []storeio.FreeExtent{{
			Offset: 8192, Length: 4096, RetiredGeneration: 2,
		}},
	}
	err = collection.absorbRetirementPressure(storeio.ErrRetiredExtentCapacity)
	if !errors.Is(err, storeio.ErrRetiredExtentCapacity) {
		t.Fatalf("overflow = %v, want ErrRetiredExtentCapacity", err)
	}
	for _, want := range []string{"nothing was published", "nothing", "abandoned", "MaxRetiredExtents"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("overflow error %q does not contain %q", err, want)
		}
	}
	if got := reclaimer.Stats().Pending; got != 1 {
		t.Fatalf("pending retirements = %d, want original extent only", got)
	}
}

func TestNeverDurableRetirementOutputRespectsFreeSetRoom(t *testing.T) {
	c := &Collection{
		freeSetLimit: 3,
		reusable: []storeio.FreeExtent{
			{Offset: 4096, Length: 4096, RetiredGeneration: 1},
			{Offset: 8192, Length: 4096, RetiredGeneration: 1},
			// A whole-consumed transaction entry disappears in
			// finalizeReusable. Until then the hot-path bound deliberately
			// counts it, avoiding an O(n) live-entry scan per mutation.
			{Offset: 12288, RetiredGeneration: 1},
		},
		freeExtentMaxima:   make([]uint64, storeio.FreeExtentIndexCapacity(3)),
		retirementAbsorbed: make([]storeio.FreeExtent, 1, 4),
	}
	if got := cap(c.neverDurableRetirementOutput()) -
		len(c.neverDurableRetirementOutput()); got != 0 {
		t.Fatalf("output room with two live + one absorbed = %d, want 0", got)
	}
	c.reusable[1].Length = 0
	output := c.neverDurableRetirementOutput()
	if got := cap(output) - len(output); got != 0 {
		t.Fatalf("conservative output room before finalize = %d, want 0", got)
	}
	c.finalizeReusable()
	output = c.neverDurableRetirementOutput()
	if got := cap(output) - len(output); got != 1 {
		t.Fatalf("output room after finalize = %d, want 1", got)
	}
}

// Given any commit that retires pages, when the file is read back, then every
// extent that commit retired is already described there.
//
// This is a belt-and-suspenders ordering guarantee. A retirement is recorded by
// syncFreeLogFor before reserveFileRetirements hands the same list to the
// reclaimer. A full retirement table now refuses the unpublished commit instead
// of forgetting the extents, but recovery must still never observe a committed
// root without the matching retirement metadata.
//
// Provoking overflow directly is not practical in this geometry and that is worth
// knowing: with no reader pinning the floor, reclamation drains the table every
// commit, so it only fills when a single commit retires more pages than the
// whole table holds.
func TestFileStoreRetirementsAreDurableBeforeTheyAreReserved(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "free-retire-order-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.ResidentBytes = 8 << 20
	options.BufferCount = 1024
	options.MaxRetiredExtents = 1024
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	const keys = 20
	checked, retired := 0, 0
	for round := range 6 {
		freeChurnRound(t, fs, keys, round)
		if err := fs.Flush(); err != nil {
			t.Fatal(err)
		}
		// c.retireScratch still holds the last commit's list: it is reset at the
		// start of the next one, which is what makes this observable at all.
		if len(fs.retireScratch) == 0 {
			continue
		}
		durable := freeSetFromFile(t, file.Name(), options.PageSize)
		for _, extent := range fs.retireScratch {
			found := false
			for _, free := range durable {
				if free.Offset <= extent.Offset &&
					extent.Offset+extent.Length <= free.Offset+free.Length {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("round %d retired %+v and the file does not describe it as free; "+
					"the reclaimer is the only record of it, so losing it there loses it",
					round, extent)
			}
			retired++
		}
		checked++
	}
	if checked == 0 || retired == 0 {
		t.Fatalf("%d rounds compared %d retirements, so this proves nothing", checked, retired)
	}
	t.Logf("%d commits, %d retired extents, every one already described by the file "+
		"before the reclaimer was asked to hold it", checked, retired)
}

// Given a segment the store has never read, when a fold is planned, then that
// segment is carried forward untouched rather than rebuilt.
//
// This is the failure lazy residency introduces, and it destroys data rather
// than leaking it. A fold rebuilds a segment from the in-memory set. Memory
// holds nothing for a segment that was never read, so the span the plan computes
// for it is empty, and rebuilding from an empty span publishes an index that no
// longer mentions any of the extents that segment held — a hundred and
// sixty-five of them, silently, at the first fold after an open.
//
// The count check that guards a stale resident segment is what makes this
// sharp: it exists to force a rebuild whenever the published count disagrees
// with what memory holds, and for a non-resident segment it always disagrees.
// So residency has to be consulted before the count, not after.
func TestFreeFoldPlanCarriesUnreadSegmentsForward(t *testing.T) {
	const pageSize = 4096
	perSegment := storeio.FreeImageRecordCapacity(pageSize)
	c := &Collection{
		freeImagePerPage: perSegment,
		freeIndexPerPage: storeio.FreeIndexRecordCapacity(pageSize),
	}
	// Three segments, each owning a megabyte of offset space so their ranges
	// cannot overlap whatever extents the test puts in them.
	const span = 1 << 20
	const held = 3
	for i := range 3 {
		c.freeSegments = append(c.freeSegments, storeio.FreeSegment{
			Ref: storeio.PageRef{
				Offset: uint64((i + 1) * span), LogicalID: uint64(100 + i), Generation: 3,
				Length: pageSize, Kind: storeio.PageFreeImage,
			},
			FirstOffset: uint64((i+1)*span + pageSize), LargestFree: pageSize, Count: held,
		})
	}
	c.resetFreeDirty()
	// The middle segment was never read, so memory holds none of its extents.
	c.freeResident = []bool{true, false, true}
	c.freeReadBack = []bool{false, false, false}
	// Memory holds the first and last segments only, and the first is dirty.
	live := make([]storeio.FreeExtent, 0, 2*held)
	for _, segment := range []int{0, 2} {
		for i := range held {
			live = append(live, storeio.FreeExtent{
				Offset:            c.freeSegments[segment].FirstOffset + uint64(2*i)*pageSize,
				Length:            pageSize,
				RetiredGeneration: 1,
			})
		}
	}
	c.markFreeDirty(live[0].Offset)
	_ = perSegment

	plan, err := c.planFreeFold(live)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.order) != 3 {
		t.Fatalf("plan publishes %d segments, want 3", len(plan.order))
	}
	if plan.order[1].fresh {
		t.Fatal("the unread segment was rebuilt; memory holds none of its extents, so the " +
			"published index would stop naming all of them")
	}
	if plan.order[1].carried != c.freeSegments[1] {
		t.Fatalf("the unread segment was carried as %+v, want %+v",
			plan.order[1].carried, c.freeSegments[1])
	}
	if plan.order[1].resident {
		t.Fatal("a carried unread segment must stay unread, or the next fold will trust a " +
			"span that does not describe it")
	}
	// The resident dirty segment must still be rebuilt, so the guard has not
	// simply disabled folding.
	if !plan.order[0].fresh {
		t.Fatal("the resident dirty segment was carried forward instead of rebuilt")
	}
	// And the resident clean one whose span matches its count is carried, which
	// is the ordinary case the whole design exists for.
	if plan.order[2].fresh {
		t.Fatal("a resident segment matching its published count was rebuilt anyway")
	}
}
