package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// Fault injection for the durable free set.
//
// This subsystem has produced two severe failures in this project's history, and
// both were shaped like a torn write. One was a permanent write outage where
// reclamation refused to act and thereby removed its own drain. The other was a
// group commit that elided a root write holding a group's high-water extent,
// which made stores unopenable. Neither showed up in a clean-shutdown test.
//
// Two things changed here that make this coverage mandatory rather than
// thorough. The image is no longer a linked list that every fold replaced whole:
// a fold now writes some segments, republishes an index that names both the new
// pages and pages it did not touch, and truncates the delta chain — three
// dependent writes where there used to be one self-contained rewrite. And a
// retirement is now durable in the commit that makes it, so a torn commit can
// leave the file claiming an extent is free while the root that survived still
// points at it. That is the one failure this package cannot recover from:
// under-reporting free space leaks, over-reporting hands out live pages.
//
// So the assertion is not that the store opens. It is that whichever generation
// survived, every byte that generation's own free set advertises can be
// destroyed without the store noticing — which is the definition of "no extent
// is both live and free", stated as the failure rather than as a traversal.

// Given a commit torn at every write point, when the resulting image is
// recovered, then it opens at exactly one of the two generations, its free set
// replays and mirrors memory, every byte that free set advertises is provably
// dead, and the store can still write.
func TestFileStoreFreeSetSurvivesCrashAtEveryWritePoint(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "free-crash-source-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	// This sweep tears the store's own commit at every write point and requires
	// recovery from the alternate superblock alone. The synchronous primary lane
	// acknowledges through a synced journal and defers its store root to a
	// checkpoint, so a torn store copy captures nothing and references a journal it
	// was never paired with. The async lane is journal-free and its committer
	// writes each generation's pages and root to the device, which is exactly the
	// per-commit tearing this test models.
	options.Durability = DurabilityAsyncVisible
	options.ResidentBytes = 8 << 20
	options.BufferCount = 1024
	options.MaxRetiredExtents = 1024
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	// Enough churn that the free set is non-empty, the delta chain has grown, and
	// the commits under test are ordinary steady-state ones rather than the first
	// few a fresh store makes.
	const keys = 20
	for round := range 5 {
		freeChurnRound(t, collection, keys, round)
	}
	// Drain the background committer so the first captured image below reflects
	// the whole churn rather than a partially persisted tail.
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}

	// Tear several consecutive commits, not one. A fold and a plain chain append
	// write completely different page sets — a fold republishes the index and
	// truncates the chain — and only a fold exercises carrying a segment forward
	// by reference across a torn write.
	folds, torn := 0, 0
	for attempt := range 8 {
		if attempt == 0 {
			collection.freeFoldRequired = true
		}
		before, readErr := os.ReadFile(file.Name())
		if readErr != nil {
			t.Fatal(readErr)
		}
		segmentsBefore := append([]storeio.FreeSegment(nil), collection.freeSegments...)
		chainBefore := len(collection.freeDeltaPages)
		oldGeneration := collection.Generation()

		key := fmt.Sprintf("key-%02d", attempt%keys)
		doc := []byte(fmt.Sprintf(`{"crash":%d,"padding":%q}`,
			attempt, strings.Repeat("q", 200+attempt*300)))
		if _, err := collection.Put([]byte(key), doc); err != nil {
			t.Fatal(err)
		}
		if err := collection.Flush(); err != nil {
			t.Fatal(err)
		}
		newGeneration := collection.Generation()
		after, readErr := os.ReadFile(file.Name())
		if readErr != nil {
			t.Fatal(readErr)
		}
		folded := len(collection.freeDeltaPages) < chainBefore ||
			!sameSegmentRefs(segmentsBefore, collection.freeSegments)
		if folded {
			folds++
		}

		torn += tearCommitAtEveryWritePoint(t, options, before, after,
			oldGeneration, newGeneration,
			fmt.Sprintf("commit-%d(folded=%v)", attempt, folded))
	}
	if folds == 0 {
		t.Fatal("no commit under test folded the free log, so the index republish and " +
			"chain truncation — the writes this change added — were never torn")
	}
	if torn < 40 {
		t.Fatalf("only %d torn images were exercised across 8 commits, which is too few "+
			"to have covered every changed page", torn)
	}
	t.Logf("tore %d images across 8 commits, %d of which folded the free log", torn, folds)
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
}

// newestInlineSuperblockSlot recovers the inline superblock slot Open would
// select from the store at path — the slot holding the newest durable root. The
// committer alternates slots against its last physical commit, so this is the
// only reliable way to name the slot a crash test must destroy to force the
// fallback root.
func newestInlineSuperblockSlot(t *testing.T, path string, pageSize int) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	bootstrap, err := storeio.DiscoverMutableInlineBootstrap(f)
	if err != nil {
		t.Fatalf("bootstrap %s: %v", path, err)
	}
	recovery, err := storeio.RecoverMutableInlineStateRoot(
		f, bootstrap.PageSize, bootstrap.MaterializationDamageGranule,
		make([]byte, bootstrap.MaxPageSize),
	)
	if err != nil {
		t.Fatalf("recover %s: %v", path, err)
	}
	return recovery.RootSlot
}

func sameSegmentRefs(a, b []storeio.FreeSegment) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Ref != b[i].Ref {
			return false
		}
	}
	return true
}

// tearCommitAtEveryWritePoint builds one image per write point of a commit and
// checks each. The data phase is a sorted positional write vector, so a crash
// can land before, inside, or after any changed page; the superblock is written
// afterwards, byte by byte, so a crash can land anywhere inside it. Both devices
// commit in that order, which is why the same two sweeps model both.
func tearCommitAtEveryWritePoint(
	t *testing.T, options Options, before, after []byte,
	oldGeneration, newGeneration uint64, label string,
) int {
	t.Helper()
	pageSize := options.PageSize
	dataStart := testMutableDataStart(pageSize)
	checked := 0
	for _, cut := range changedPageCrashCuts(before, after, dataStart, pageSize) {
		image := make([]byte, max(len(before), len(after)))
		copy(image, before)
		copy(image[dataStart:dataStart+cut], after[dataStart:dataStart+cut])
		assertCrashImageFreeSetIsDead(t, image, options, oldGeneration, newGeneration,
			fmt.Sprintf("%s/data-cut-%d", label, cut))
		checked++
	}
	// The root write is the moment the new generation becomes the truth. A cut
	// inside it is the case the group-commit bug produced: a superblock that
	// names a state root whose own high-water extent was never written.
	rootOffset := int((newGeneration-1)&1) * pageSize
	for _, cut := range []int{0, 1, storeio.InlineSuperblockSize / 2, storeio.InlineSuperblockSize - 1, storeio.InlineSuperblockSize} {
		image := append([]byte(nil), after...)
		copy(image[rootOffset:rootOffset+pageSize], before[rootOffset:rootOffset+pageSize])
		copy(image[rootOffset:rootOffset+cut], after[rootOffset:rootOffset+cut])
		assertCrashImageFreeSetIsDead(t, image, options, oldGeneration, newGeneration,
			fmt.Sprintf("%s/root-cut-%d", label, cut))
		checked++
	}
	return checked
}

// assertCrashImageFreeSetIsDead recovers one torn image and destroys every byte
// its own free set advertises.
//
// Overwriting is the whole point. A walker that checked the free set against the
// roots it happens to know about would miss the page kinds it forgot, and this
// subsystem's failures have always been in the page kind nobody listed. Writing
// garbage over every advertised free byte and then demanding the store still
// answers covers every reachable page by construction — including the free log's
// own segments, index, and chain, which is exactly what a fold that retired a
// segment it had carried forward would put there.
//
// Fenced extents are advertised too, and they are pages the alternate recovery
// root can still reach. Destroying them is still the right test: recovery
// selects one superblock, and the assertion is about what that generation can
// reach. An extent fenced against the other root must never be handed out, which
// is what the generation stamp and the reclaimer enforce, and what the write at
// the end of this function would expose.
func assertCrashImageFreeSetIsDead(
	t *testing.T, image []byte, options Options, oldGeneration, newGeneration uint64, name string,
) {
	t.Helper()
	path := filepath.Join(t.TempDir(), strings.ReplaceAll(name, "/", "_"))
	if err := os.WriteFile(path, image, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatalf("%s recovery: %v", name, err)
	}
	generation := collection.Generation()
	if generation != oldGeneration && generation != newGeneration {
		_ = collection.Close()
		_ = file.Close()
		t.Fatalf("%s recovered generation %d, want %d or %d",
			name, generation, oldGeneration, newGeneration)
	}
	// Replaying is what turns the bytes into a claim. A chain that cannot replay
	// is a store that cannot write, which is the outage half of this subsystem's
	// history.
	if err := collection.refreshReusable(collection.state.Load()); err != nil {
		_ = collection.Close()
		_ = file.Close()
		t.Fatalf("%s free-log replay after recovery: %v", name, err)
	}
	assertFreeSetMirror(t, collection, name)
	expected := make(map[string]string, 32)
	for i := range 32 {
		key := fmt.Sprintf("key-%02d", i)
		value, ok, getErr := collection.AppendRaw(nil, []byte(key))
		if getErr != nil {
			t.Fatalf("%s read %s: %v", name, key, getErr)
		}
		if ok {
			expected[key] = string(value)
		}
	}
	count := collection.Len()
	if err := collection.Close(); err != nil {
		t.Fatalf("%s close: %v", name, err)
	}

	free := freeSetFromFile(t, path, options.PageSize)
	poisoned := uint64(0)
	for _, extent := range free {
		garbage := make([]byte, extent.Length)
		for i := range garbage {
			garbage[i] = 0xDD
		}
		if _, err := file.WriteAt(garbage, int64(extent.Offset)); err != nil {
			t.Fatalf("%s poison %+v: %v", name, extent, err)
		}
		poisoned += extent.Length
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := Open(reopened, options)
	if err != nil {
		t.Fatalf("%s reopen after destroying %d bytes of advertised free space: %v",
			name, poisoned, err)
	}
	defer after.Close()
	if got := after.Generation(); got != generation {
		t.Fatalf("%s recovered generation %d before poisoning and %d after: the free set "+
			"held a page the selected root needed", name, generation, got)
	}
	for key, want := range expected {
		got, ok, getErr := after.AppendRaw(nil, []byte(key))
		if getErr != nil || !ok || string(got) != want {
			t.Fatalf("%s %s after destroying its free space = (%q,%v,%v), want %q",
				name, key, got, ok, getErr, want)
		}
	}
	if got := after.Len(); got != count {
		t.Fatalf("%s document count %d after destroying its free space, want %d",
			name, got, count)
	}
	// Writing is the other half. A recovered store whose free set overlaps a live
	// page reads correctly right up until the moment it allocates from it, so the
	// read sweep alone would pass on exactly the image that corrupts.
	if _, err := after.Put([]byte("crash-probe"), []byte(`{"probe":true}`)); err != nil {
		t.Fatalf("%s write after recovery: %v", name, err)
	}
	assertFreeSetMirror(t, after, name+" after writing")
	if got, ok, getErr := after.AppendRaw(nil, []byte("crash-probe")); getErr != nil || !ok ||
		string(got) != `{"probe":true}` {
		t.Fatalf("%s probe read back = (%q,%v,%v)", name, got, ok, getErr)
	}
	for key, want := range expected {
		got, ok, getErr := after.AppendRaw(nil, []byte(key))
		if getErr != nil || !ok || string(got) != want {
			t.Fatalf("%s %s after writing into recovered free space = (%q,%v,%v), want %q",
				name, key, got, ok, getErr, want)
		}
	}
}

// Given a store that keeps writing, when the newest superblock is destroyed at
// any point, then the alternate root still opens and still reads every document
// it knew about.
//
// This is the invariant the durable free set now leans on, and it is not the one
// the torn-commit sweep checks. Retirements are written down by the commit that
// makes them, so the file advertises as free a set of extents that includes
// pages the *previous* root still reaches. Nothing on disk stops an allocator
// from taking one; what stops it is the generation stamp, applied once at open
// to decide which extents go to the allocator and which go back to the
// reclaimer. If that split is wrong — or missing — the store reads perfectly and
// writes happily right up to the moment a superblock is lost, and then the
// fallback root points at pages that were handed out generations ago.
//
// A single crash cannot expose that, because a single crash leaves the selected
// root intact. It takes losing the root the store had been protecting against.
func TestFileStoreAlternateRootSurvivesFreeSpaceReuse(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "free-alternate-root-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	// Losing the newest superblock must fall back to the one before it, recovered
	// from the store alone. The synchronous lane journals and defers its store
	// root, so run this on the journal-free async lane whose committer writes each
	// generation's root to the device (see TestFileStoreFreeSetSurvivesCrashAtEveryWritePoint).
	options.Durability = DurabilityAsyncVisible
	options.ResidentBytes = 8 << 20
	options.BufferCount = 1024
	options.MaxRetiredExtents = 1024
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	const keys = 20
	for round := range 4 {
		freeChurnRound(t, collection, keys, round)
	}

	checked := 0
	for attempt := range 12 {
		key := fmt.Sprintf("key-%02d", attempt%keys)
		doc := []byte(fmt.Sprintf(`{"alt":%d,"padding":%q}`,
			attempt, strings.Repeat("w", 300+attempt*250)))
		if _, err := collection.Put([]byte(key), doc); err != nil {
			t.Fatal(err)
		}
		if err := collection.Flush(); err != nil {
			t.Fatal(err)
		}
		generation := collection.Generation()
		expected := make(map[string]string, keys)
		for i := range keys {
			name := fmt.Sprintf("key-%02d", i)
			value, ok, getErr := collection.AppendRaw(nil, []byte(name))
			if getErr != nil {
				t.Fatal(getErr)
			}
			if ok {
				expected[name] = string(value)
			}
		}
		image, readErr := os.ReadFile(file.Name())
		if readErr != nil {
			t.Fatal(readErr)
		}

		// Destroy the superblock this generation published. Recovery must fall
		// back to the one before it, which is the root every fenced extent in the
		// free set is being held for. The committer writes each root to the slot
		// opposite its last physical commit rather than by generation parity, so the
		// newest slot is the one recovery would select from the pristine image, not
		// a value computed from the generation number.
		newest := newestInlineSuperblockSlot(t, file.Name(), options.PageSize)
		damaged := append([]byte(nil), image...)
		for i := range storeio.InlineSuperblockSize {
			damaged[newest*options.PageSize+i] = 0xEE
		}
		path := filepath.Join(t.TempDir(), fmt.Sprintf("alt-%d", attempt))
		if err := os.WriteFile(path, damaged, 0o600); err != nil {
			t.Fatal(err)
		}
		handle, openErr := os.OpenFile(path, os.O_RDWR, 0o600)
		if openErr != nil {
			t.Fatal(openErr)
		}
		fallback, openErr := Open(handle, options)
		if openErr != nil {
			_ = handle.Close()
			t.Fatalf("attempt %d: losing the newest superblock made the store unopenable: %v",
				attempt, openErr)
		}
		if got := fallback.Generation(); got >= generation {
			t.Fatalf("attempt %d recovered generation %d, want one older than %d",
				attempt, got, generation)
		}
		// The fallback root is one generation behind, so the key this commit wrote
		// may hold an older value or not exist there at all — the churn deletes a
		// rotating third of the keys, so a Put can be a resurrection. Every other
		// key must be present and exactly right: an absent or wrong one means its
		// page was handed out while this root still pointed at it.
		for name, want := range expected {
			got, ok, getErr := fallback.AppendRaw(nil, []byte(name))
			if getErr != nil {
				t.Fatalf("attempt %d: reading %s from the alternate root: %v", attempt, name, getErr)
			}
			if name == key {
				continue
			}
			if !ok || string(got) != want {
				t.Fatalf("attempt %d: %s from the alternate root = (%q,%v), want %q; "+
					"its page was reused while that root still reached it",
					attempt, name, got, ok, want)
			}
		}
		// The fallback must also be able to carry on, which means its own free set
		// replayed and holds nothing it needs.
		if err := fallback.refreshReusable(fallback.state.Load()); err != nil {
			t.Fatalf("attempt %d: alternate root free-log replay: %v", attempt, err)
		}
		if _, err := fallback.Put([]byte("alt-probe"), []byte(`{"probe":1}`)); err != nil {
			t.Fatalf("attempt %d: alternate root cannot write: %v", attempt, err)
		}
		assertFreeSetMirror(t, fallback, fmt.Sprintf("alternate root %d", attempt))
		if err := fallback.Close(); err != nil {
			t.Fatal(err)
		}
		if err := handle.Close(); err != nil {
			t.Fatal(err)
		}
		checked++
	}
	if checked != 12 {
		t.Fatalf("only %d of 12 commits were checked against a lost superblock", checked)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
}
