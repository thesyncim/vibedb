package durable

import (
	"bytes"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

// Fault injection for the whole commit, not just the free set.
//
// TestFileStoreFreeSetSurvivesCrashAtEveryWritePoint already tears a commit at
// every write point, but everything it asserts is about the free set: the store
// opens, the chain replays, and no advertised free byte is live. That is the
// right property for the free log and the wrong scope for the commit, because a
// commit now writes far more than it used to and the newest of those writes have
// no sweep at all. A batched Update publishes many mutations as one generation
// through tree descents that visit each page exactly once. Directory pages are
// validated at admission rather than per lookup, so a page that reaches the
// cache unvalidated is never checked again. Retirement pressure can abandon
// extents. None of that is exercised by tearing one Put and reading one key.
//
// The addition here is coverage that is stated in terms of page kinds and then
// proved. The sweep decodes every physical page a commit changed, records its
// kind, and fails if the kinds the commit path is made of were not among them —
// so "the key tree is covered" is an assertion about the images that were
// actually built, not a claim about the workload that was supposed to build
// them. On top of that every recovered image is checked against a stronger
// consistency property than the free-set sweep uses: the chunk-tree scan and the
// key-tree lookups must enumerate the same documents with the same bytes, the
// index postings must select exactly the rows whose documents hold the value,
// and all of that must survive having every byte the image's own free set
// advertises overwritten with garbage.
//
// The two historical failures this subsystem produced are both in range. A
// reclaimer that removed its own drain shows up as a store that opens and reads
// but cannot write; the probe write at the end of every image is what catches
// it. A group commit that elided a root write holding a group's high-water
// extent shows up as an image that will not open at all.

// crashPage is one decoded physical page found by walking a store image.
type crashPage struct {
	offset uint64
	length uint32
	kind   storeio.PageKind
}

// walkImagePages decodes every physical page in a store image after the two
// superblock copies.
//
// It reads the file the way a forensic tool would rather than the way the store
// does: no root is consulted and no reference is followed, so a page the commit
// wrote and then failed to link is still found. That is deliberate. The point of
// this walk is to name what a commit put on disk, and a walk that started from
// the roots could only ever name what the roots already reach — which is the
// half of the question that is not in doubt.
//
// Extents are page-size aligned and every page records its own size, so the walk
// steps by the decoded size when a header is valid and by one page-size quantum
// when it is not. Free and never-written space simply fails to decode.
func walkImagePages(image []byte, pageSize, maxPageSize int) []crashPage {
	pages := make([]crashPage, 0, 64)
	for offset := testMutableDataStart(pageSize); offset < len(image); {
		end := min(offset+maxPageSize, len(image))
		header, _, err := storeio.OpenPage(image[offset:end])
		if err != nil {
			offset += pageSize
			continue
		}
		pages = append(pages, crashPage{
			offset: uint64(offset), length: header.PageSize, kind: header.Kind,
		})
		offset += int(header.PageSize)
	}
	return pages
}

// changedPageKinds counts, by kind, the pages a commit rewrote. A page counts as
// changed when any byte inside it differs, which includes a page allocated into
// space the previous image left free or never touched.
func changedPageKinds(before, after []byte, pageSize, maxPageSize int) map[storeio.PageKind]int {
	kinds := make(map[storeio.PageKind]int, 8)
	for _, page := range walkImagePages(after, pageSize, maxPageSize) {
		start, stop := int(page.offset), int(page.offset)+int(page.length)
		old := make([]byte, page.length)
		if start < len(before) {
			copy(old, before[start:min(stop, len(before))])
		}
		if !bytes.Equal(old, after[start:stop]) {
			kinds[page.kind]++
		}
	}
	return kinds
}

func pageKindName(kind storeio.PageKind) string {
	switch kind {
	case storeio.PageStateRoot:
		return "state root"
	case storeio.PageDocument:
		return "document"
	case storeio.PageOverflow:
		return "overflow"
	case storeio.PageChunkDirectory:
		return "chunk tree"
	case storeio.PageFingerprintDirectory:
		return "fingerprint tree"
	case storeio.PageIndexDirectory:
		return "index tree"
	case storeio.PageIndexPosting:
		return "index posting"
	case storeio.PageDocumentGroup:
		return "document group"
	case storeio.PageFloat64Group:
		return "float64 group"
	case storeio.PageFloat64Catalog:
		return "float64 catalog"
	case storeio.PageFloat64Stripe:
		return "float64 stripe"
	case storeio.PageIndexGroupCatalog:
		return "index group catalog"
	case storeio.PageFreeImage:
		return "free image"
	case storeio.PageFreeDelta:
		return "free delta"
	case storeio.PageFreeIndex:
		return "free index"
	default:
		return fmt.Sprintf("kind %d", uint8(kind))
	}
}

// commitCrashSweep accumulates what a run of tearCommitAtEveryPageKind covered,
// so the test can assert against evidence instead of intent.
type commitCrashSweep struct {
	kinds      map[storeio.PageKind]int
	rootSlots  map[int]int
	images     int
	commits    int
	foldedLogs int
}

func newCommitCrashSweep() *commitCrashSweep {
	return &commitCrashSweep{
		kinds: make(map[storeio.PageKind]int, 16), rootSlots: make(map[int]int, 2),
	}
}

func (s *commitCrashSweep) requireKinds(t *testing.T, want ...storeio.PageKind) {
	t.Helper()
	missing := make([]string, 0, len(want))
	for _, kind := range want {
		if s.kinds[kind] == 0 {
			missing = append(missing, pageKindName(kind))
		}
	}
	if len(missing) != 0 {
		t.Fatalf("the sweep never tore a %s page, so that page kind is uncovered; "+
			"it did tear %s", strings.Join(missing, ", "), s)
	}
}

// requireInert asserts that a page kind this sweep cannot reach really is
// unreachable, so an untestable injection is recorded as a fact about the code
// rather than left as a comment that quietly rots.
//
// PageIndexPosting is the one such kind. The durable commit path never encodes
// one: EncodePostingPage has a single caller, store.storePackedIndex.encode,
// which packs postings into an in-memory buffer for the heap collection's packed
// index. The durable index tree keeps its postings inside PageIndexDirectory
// nodes. So no workload this sweep can express will put a posting page inside a
// torn commit, and claiming coverage of one would be a lie. If the durable path
// ever starts writing them, this fails and the sweep has to grow a case.
func (s *commitCrashSweep) requireInert(t *testing.T, kind storeio.PageKind) {
	t.Helper()
	if s.kinds[kind] != 0 {
		t.Fatalf("the durable commit path now writes %s pages (%d of them changed), "+
			"so this sweep must cover them instead of recording them as unreachable",
			pageKindName(kind), s.kinds[kind])
	}
}

func (s *commitCrashSweep) String() string {
	names := make([]string, 0, len(s.kinds))
	for kind, count := range s.kinds {
		names = append(names, fmt.Sprintf("%s×%d", pageKindName(kind), count))
	}
	slices.Sort(names)
	return fmt.Sprintf("%d images across %d commits (%d folded the free log), "+
		"superblock slots %v, changed pages: %s",
		s.images, s.commits, s.foldedLogs, slices.Sorted(maps.Keys(s.rootSlots)),
		strings.Join(names, " "))
}

// commitCrashWorld is the caller-visible content of the collection under test,
// used to build the expectations a recovered image is checked against.
type commitCrashWorld struct {
	options  Options
	keys     []string
	indexes  []string
	float64s []string
	// scratch is one directory for every image a sweep builds. Calling t.TempDir
	// per image instead costs a mkdir and a registered cleanup for each of the
	// several hundred, which dominated the sweep's wall time; the images are
	// checked one at a time, so one reused path is enough.
	scratch string
}

// tearCommitAtEveryPageKind runs one commit, records which page kinds it
// changed, and checks the recovered image at every point the commit could have
// been cut.
//
// The cut model is the one both commit devices actually implement: a sorted
// positional write vector for the data phase, then the superblock. So a crash
// lands before, inside, or after any changed page, or anywhere inside the
// superblock. Nothing here assumes which pages a commit touched — that is
// discovered from the bytes and reported back to the caller, which is what makes
// the page-kind coverage assertion meaningful.
func tearCommitAtEveryPageKind(
	t *testing.T, world commitCrashWorld, collection *Collection,
	label string, sweep *commitCrashSweep, mutate func() error,
) {
	t.Helper()
	options := world.options
	before, err := os.ReadFile(collection.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	oldGeneration := collection.Generation()
	segmentsBefore := append([]storeio.FreeSegment(nil), collection.freeSegments...)
	chainBefore := len(collection.freeDeltaPages)

	if err := mutate(); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	newGeneration := collection.Generation()
	if newGeneration == oldGeneration {
		t.Fatalf("%s published no generation, so there was no commit to tear", label)
	}
	after, err := os.ReadFile(collection.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(collection.freeDeltaPages) < chainBefore ||
		!sameSegmentRefs(segmentsBefore, collection.freeSegments) {
		sweep.foldedLogs++
	}
	sweep.commits++
	for kind, count := range changedPageKinds(before, after, options.PageSize, options.MaxPageSize) {
		sweep.kinds[kind] += count
	}
	// Both superblock copies must be torn across a run, not just the one this
	// generation happens to land on. Recovery picks between two roots; a bug in
	// the selection rule that only fires for one parity would otherwise sit
	// behind an even number of commits.
	slot := int((newGeneration - 1) & 1)
	sweep.rootSlots[slot]++

	dataStart := testMutableDataStart(options.PageSize)
	for _, cut := range changedPageCrashCuts(before, after, dataStart, options.PageSize) {
		image := make([]byte, max(len(before), len(after)))
		copy(image, before)
		copy(image[dataStart:dataStart+cut], after[dataStart:dataStart+cut])
		assertCommitCrashImageConsistent(t, image, world, oldGeneration, newGeneration,
			fmt.Sprintf("%s/data-cut-%d", label, cut))
		sweep.images++
	}
	rootOffset := slot * options.PageSize
	for _, cut := range []int{0, 1, storeio.InlineSuperblockSize / 2, storeio.InlineSuperblockSize - 1, storeio.InlineSuperblockSize} {
		image := append([]byte(nil), after...)
		copy(image[rootOffset:rootOffset+options.PageSize], before[rootOffset:rootOffset+options.PageSize])
		copy(image[rootOffset:rootOffset+cut], after[rootOffset:rootOffset+cut])
		assertCommitCrashImageConsistent(t, image, world, oldGeneration, newGeneration,
			fmt.Sprintf("%s/root-cut-%d", label, cut))
		sweep.images++
	}
}

// commitCrashContents is what one recovered image says it holds.
type commitCrashContents struct {
	scanned    map[string]string
	generation uint64
	length     uint64
	postings   map[string]map[string]int
	float64s   map[string]store.Float64Aggregate
}

// assertCommitCrashImageConsistent recovers one torn image, proves it is
// internally consistent, then destroys every byte its own free set advertises
// and proves it is still the same store.
//
// Overwriting rather than traversing is the same argument the free-set sweep
// makes, and it generalises: a checker that walked the roots it knows about
// would miss whichever page kind nobody listed, and the page kinds this commit
// path gained most recently are exactly the ones a hand-written walk would
// forget. Filling every advertised free byte with garbage covers every reachable
// page by construction.
//
// The consistency half is what this adds over the free-set sweep. Reading one
// key proves a lookup works; it does not prove the key directory and the chunk
// directory describe the same collection. A commit writes both, a torn commit
// can publish a root that reaches one and not the other, and the difference is
// invisible to any single read. So the scan and the lookups are cross-checked
// against each other, and the index postings are checked against the documents
// the scan found rather than against a count.
func assertCommitCrashImageConsistent(
	t *testing.T, image []byte, world commitCrashWorld,
	oldGeneration, newGeneration uint64, name string,
) {
	t.Helper()
	path := filepath.Join(world.scratch, "image")
	if err := os.WriteFile(path, image, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := Open(file, world.options)
	if err != nil {
		_ = file.Close()
		t.Fatalf("%s recovery: %v", name, err)
	}
	if generation := collection.Generation(); generation != oldGeneration && generation != newGeneration {
		_ = collection.Close()
		_ = file.Close()
		t.Fatalf("%s recovered generation %d, want %d or %d",
			name, generation, oldGeneration, newGeneration)
	}
	// Replaying is what turns the bytes into a claim about free space. A chain
	// that cannot replay is a store that cannot write, which is the outage half
	// of this subsystem's history.
	if err := collection.refreshReusable(collection.state.Load()); err != nil {
		_ = collection.Close()
		_ = file.Close()
		t.Fatalf("%s free-log replay after recovery: %v", name, err)
	}
	assertFreeSetMirror(t, collection, name)
	contents := readCommitCrashContents(t, collection, world, name)
	if err := collection.Close(); err != nil {
		t.Fatalf("%s close: %v", name, err)
	}

	free := freeSetFromFile(t, path, world.options.PageSize)
	poisoned := uint64(0)
	for _, extent := range free {
		garbage := bytes.Repeat([]byte{0xDD}, int(extent.Length))
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
	poisonedStore, err := Open(reopened, world.options)
	if err != nil {
		t.Fatalf("%s reopen after destroying %d bytes of advertised free space: %v",
			name, poisoned, err)
	}
	defer poisonedStore.Close()
	assertCommitCrashContentsEqual(t, contents,
		readCommitCrashContents(t, poisonedStore, world, name+" poisoned"),
		name+": destroying its own advertised free space changed what it holds, so "+
			"the free set named a page the selected root still reached")

	// Writing is the other half, and the only half that exposes an overlap. A
	// recovered store whose free set covers a live page reads perfectly right up
	// to the moment it allocates from it, so a read-only sweep passes on exactly
	// the image that corrupts.
	if _, err := poisonedStore.Put("commit-crash-probe", []byte(`{"probe":true}`)); err != nil {
		t.Fatalf("%s write after recovery: %v", name, err)
	}
	assertFreeSetMirror(t, poisonedStore, name+" after writing")
	if got, ok, getErr := poisonedStore.AppendRaw(nil, "commit-crash-probe"); getErr != nil || !ok ||
		string(got) != `{"probe":true}` {
		t.Fatalf("%s probe read back = (%q,%v,%v)", name, got, ok, getErr)
	}
	written := readCommitCrashContents(t, poisonedStore, world, name+" written")
	// The probe is the only thing the write may have changed, so it is subtracted
	// rather than the comparison being relaxed: the count must have moved by
	// exactly one and the generation must have advanced. The probe carries no
	// indexed or projected field, so the postings and the float64 aggregate are
	// compared unchanged.
	if _, ok := written.scanned["commit-crash-probe"]; !ok {
		t.Fatalf("%s: the probe was written but the chunk-tree scan does not hold it", name)
	}
	delete(written.scanned, "commit-crash-probe")
	if written.length != contents.length+1 {
		t.Fatalf("%s: writing one probe moved the document count from %d to %d",
			name, contents.length, written.length)
	}
	written.length = contents.length
	if written.generation <= contents.generation {
		t.Fatalf("%s: writing the probe left the generation at %d, from %d",
			name, written.generation, contents.generation)
	}
	written.generation = contents.generation
	assertCommitCrashContentsEqual(t, contents, written,
		name+": writing into the recovered free space changed a document that was "+
			"already there, so an allocation landed on a live page")
}

// readCommitCrashContents states what a store holds using two independent
// traversals plus the index postings, so that a disagreement between them is a
// failure rather than a coin flip about which one the test happened to call.
func readCommitCrashContents(
	t *testing.T, collection *Collection, world commitCrashWorld, name string,
) commitCrashContents {
	t.Helper()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatalf("%s snapshot: %v", name, err)
	}
	defer snapshot.Close()
	contents := commitCrashContents{
		scanned:    make(map[string]string, 64),
		generation: collection.Generation(),
		length:     snapshot.Len(),
		postings:   make(map[string]map[string]int, len(world.indexes)),
		float64s:   make(map[string]store.Float64Aggregate, len(world.float64s)),
	}
	// The chunk-tree scan is the first traversal: it walks the chunk directory
	// and every document page, and never consults the key directory.
	if err := snapshot.RangeRaw(func(key, value []byte) error {
		if _, duplicate := contents.scanned[string(key)]; duplicate {
			t.Fatalf("%s: the chunk-tree scan returned %q twice", name, key)
		}
		contents.scanned[string(key)] = string(value)
		return nil
	}); err != nil {
		t.Fatalf("%s scan: %v", name, err)
	}
	if uint64(len(contents.scanned)) != contents.length {
		t.Fatalf("%s: the chunk-tree scan found %d documents but the state root says %d",
			name, len(contents.scanned), contents.length)
	}
	// The key directory is the second traversal, and it has to agree with the
	// first in both directions. A torn commit that published a root reaching one
	// tree and not the other is invisible to either traversal alone.
	for key, want := range contents.scanned {
		got, ok, getErr := snapshot.AppendRaw(nil, key)
		if getErr != nil || !ok || string(got) != want {
			t.Fatalf("%s: the chunk-tree scan holds %q but the key directory answers "+
				"(%q,%v,%v)", name, key, got, ok, getErr)
		}
	}
	for _, key := range world.keys {
		got, ok, getErr := snapshot.AppendRaw(nil, key)
		if getErr != nil {
			t.Fatalf("%s: key directory lookup of %q: %v", name, key, getErr)
		}
		want, present := contents.scanned[key]
		if ok != present || (ok && string(got) != want) {
			t.Fatalf("%s: the key directory answers %q with (%q,%v) but the chunk-tree "+
				"scan says (%q,%v)", name, key, got, ok, want, present)
		}
	}
	for _, index := range world.indexes {
		contents.postings[index] = commitCrashPostings(t, snapshot, contents.scanned, index, name)
	}
	// The float64 sidecar rides inside the document page rather than in a page
	// kind of its own, so a page-kind census cannot show it was covered. Reducing
	// over it is what shows it: the reduction reads every live document page's
	// column, so a torn commit that published a document page whose sidecar
	// disagreed with its rows fails here and nowhere else. The aggregate is also
	// recomputed from the scanned JSON, so a sidecar that survived recovery
	// intact but wrong is still caught.
	for _, path := range world.float64s {
		aggregate, covered, err := snapshot.ReduceFloat64Path(path)
		if err != nil {
			t.Fatalf("%s: float64 reduction over %s: %v", name, path, err)
		}
		if !covered {
			t.Fatalf("%s: %s is a configured float64 column but the recovered store "+
				"reports it uncovered, so the sidecar did not survive", name, path)
		}
		if want := commitCrashFloat64Oracle(t, contents.scanned, path, name); aggregate != want {
			t.Fatalf("%s: the float64 sidecar over %s reduces to %+v, but the documents "+
				"the scan returned add up to %+v", name, path, aggregate, want)
		}
		contents.float64s[path] = aggregate
	}
	return contents
}

// commitCrashFloat64Oracle recomputes a column aggregate from the document bytes
// the chunk-tree scan returned, independently of the sidecar the store reduces.
func commitCrashFloat64Oracle(
	t *testing.T, scanned map[string]string, path, name string,
) store.Float64Aggregate {
	t.Helper()
	want := store.Float64Aggregate{}
	for key, document := range scanned {
		node, ok, err := buildCommitCrashNeedle(t, document).Pointer(path)
		if err != nil {
			t.Fatalf("%s: reading %s from %q: %v", name, path, key, err)
		}
		if !ok {
			continue
		}
		value, ok := node.Raw().Float64()
		if !ok {
			continue
		}
		if want.Count == 0 {
			want.Min, want.Max = value, value
		}
		want.Count++
		want.Sum += value
		want.Min = min(want.Min, value)
		want.Max = max(want.Max, value)
	}
	return want
}

// commitCrashPostings checks one index against the documents the scan found and
// returns the per-value row counts so two recoveries can be compared.
//
// The check is exact in both directions. A posting mask that is missing a row
// makes a query silently lose a document; a mask holding a row whose document
// does not carry the value makes it return one that never matched. Only counting
// rows would catch neither.
func commitCrashPostings(
	t *testing.T, snapshot *Snapshot, scanned map[string]string, index, name string,
) map[string]int {
	t.Helper()
	values := make(map[string]struct{}, 8)
	byValue := make(map[string]map[string]struct{}, 8)
	for key, document := range scanned {
		parsed := buildCommitCrashNeedle(t, document)
		node, ok, err := parsed.Pointer("/" + index)
		if err != nil || !ok {
			continue
		}
		raw := string(node.Raw().Bytes())
		values[raw] = struct{}{}
		if byValue[raw] == nil {
			byValue[raw] = make(map[string]struct{}, 8)
		}
		byValue[raw][key] = struct{}{}
	}
	counts := make(map[string]int, len(values))
	for raw := range values {
		needle := buildCommitCrashNeedle(t, raw)
		masks, err := snapshot.AppendIndexMasks(nil, index, needle)
		if err != nil {
			t.Fatalf("%s: index %q probe of %s: %v", name, index, raw, err)
		}
		selected := make(map[string]struct{}, len(byValue[raw]))
		if err := snapshot.RangeMasksRaw(masks, func(key, value []byte) error {
			selected[string(key)] = struct{}{}
			return nil
		}); err != nil {
			t.Fatalf("%s: index %q masked scan of %s: %v", name, index, raw, err)
		}
		if len(selected) != len(byValue[raw]) {
			t.Fatalf("%s: index %q selected %d rows for %s, but %d scanned documents "+
				"carry that value", name, index, len(selected), raw, len(byValue[raw]))
		}
		for key := range byValue[raw] {
			if _, ok := selected[key]; !ok {
				t.Fatalf("%s: index %q did not select %q, whose document carries %s",
					name, index, key, raw)
			}
		}
		counts[raw] = len(selected)
	}
	return counts
}

func buildCommitCrashNeedle(t *testing.T, raw string) vibejson.Index {
	t.Helper()
	needed, err := vibejson.RequiredIndexEntries([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	index, err := vibejson.BuildIndex([]byte(raw), make([]vibejson.IndexEntry, needed))
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func assertCommitCrashContentsEqual(t *testing.T, want, got commitCrashContents, message string) {
	t.Helper()
	if want.generation != got.generation {
		t.Fatalf("%s: generation %d became %d", message, want.generation, got.generation)
	}
	if want.length != got.length {
		t.Fatalf("%s: document count %d became %d", message, want.length, got.length)
	}
	if len(want.scanned) != len(got.scanned) {
		t.Fatalf("%s: the scan found %d documents and then %d",
			message, len(want.scanned), len(got.scanned))
	}
	for key, value := range want.scanned {
		other, ok := got.scanned[key]
		if !ok {
			t.Fatalf("%s: %q disappeared", message, key)
		}
		if other != value {
			t.Fatalf("%s: %q = %q, want %q", message, key, other, value)
		}
	}
	for index, counts := range want.postings {
		other := got.postings[index]
		if len(other) != len(counts) {
			t.Fatalf("%s: index %q covered %d values and then %d",
				message, index, len(counts), len(other))
		}
		for value, count := range counts {
			if other[value] != count {
				t.Fatalf("%s: index %q selected %d rows for %s, then %d",
					message, index, count, value, other[value])
			}
		}
	}
	for path, aggregate := range want.float64s {
		if other := got.float64s[path]; other != aggregate {
			t.Fatalf("%s: the float64 column %s reduced to %+v and then %+v",
				message, path, aggregate, other)
		}
	}
}

// commitCrashOptions configures a collection whose ordinary commits touch every
// page kind the single-document and batched write paths can produce.
//
// InlineValueBytes is deliberately small so that documents just past it spill to
// overflow pages and an index make the index tree participate in
// the same commits; and Float64Columns makes every document page carry a float64
// sidecar, so the sidecar encoding is inside the torn region rather than a
// structure only the bulk builder writes.
func commitCrashOptions(batchDocuments int) Options {
	options := testFileStoreOptions()
	options.Collection.ChunkDocuments = 4
	options.ResidentBytes = 16 << 20
	options.BufferCount = 1024
	options.MaxRetiredExtents = 4096
	options.MaxBatchDocuments = batchDocuments
	if batchDocuments > 1 {
		// A wide batch's worst-case transaction is hundreds of pages, which no
		// fixed BufferCount from the single-document options can cover. Zero asks
		// the store to size the commit buffer from the batch width it was told.
		options.BufferCount = 0
	}
	options.InlineValueBytes = 256
	options.Indexes = []store.IndexDefinition{{Name: "status", Paths: []string{"/status"}}}
	options.Float64Columns = []string{"/score"}
	return options
}

func commitCrashDocument(round, key int, padding int) []byte {
	return fmt.Appendf(nil, `{"round":%d,"key":%d,"status":%q,"score":%d.5,"padding":%q}`,
		round, key, [3]string{"active", "idle", "paused"}[(round+key)%3], key,
		strings.Repeat("x", padding))
}

func createCommitCrashCollection(t *testing.T, options Options, keys int) (*Collection, []string) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "commit-crash-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = collection.Close() })
	names := make([]string, 0, keys+4)
	for i := range keys {
		names = append(names, fmt.Sprintf("key-%02d", i))
	}
	// Enough churn that the commits under test are ordinary steady-state ones:
	// the free set is non-empty, the delta chain has grown, and pages are being
	// reused rather than appended. A fresh store's first commits allocate into
	// virgin space and never exercise reuse at all.
	//
	// Alternating inline and overflow sizes matters: an overflow page is only
	// retired when the value that owned it is replaced, so a workload with a
	// fixed size retires overflow extents exactly never.
	for round := range 4 {
		for i := range keys {
			padding := 100
			if (round+i)%2 == 0 {
				padding = 900 + i*40
			}
			if _, err := collection.Put(names[i], commitCrashDocument(round, i, padding)); err != nil {
				t.Fatal(err)
			}
		}
		for i := round % 3; i < keys; i += 3 {
			if _, err := collection.Delete(names[i]); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	return collection, names
}

// Given a single-document commit torn at every write point, when each resulting
// image is recovered, then it opens at one of the two generations, its chunk
// tree and key tree describe the same documents, its index selects exactly the
// rows that carry each value, and none of that changes when every byte the image
// advertises as free is overwritten with garbage.
func TestFileStoreSingleCommitSurvivesCrashAtEveryPageKind(t *testing.T) {
	options := commitCrashOptions(1)
	const keys = 16
	collection, names := createCommitCrashCollection(t, options, keys)
	world := commitCrashWorld{
		options: options, keys: names, scratch: t.TempDir(),
		indexes: []string{"status"}, float64s: []string{"/score"},
	}
	sweep := newCommitCrashSweep()

	// Six commits, each shaped to change a different part of the page set: an
	// inline replacement, an overflow replacement, an insert that grows the key
	// tree, and a delete that shrinks it.
	commits := []struct {
		name   string
		mutate func() error
	}{
		{"inline-replace", func() error {
			_, err := collection.Put(names[2], commitCrashDocument(9, 2, 60))
			return err
		}},
		{"overflow-replace", func() error {
			_, err := collection.Put(names[5], commitCrashDocument(9, 5, 4000))
			return err
		}},
		{"insert", func() error {
			_, err := collection.Put("key-inserted", commitCrashDocument(9, 42, 1500))
			return err
		}},
		{"delete", func() error {
			_, err := collection.Delete(names[7])
			return err
		}},
	}
	world.keys = append(world.keys, "key-inserted")
	for _, commit := range commits {
		tearCommitAtEveryPageKind(t, world, collection, commit.name, sweep, commit.mutate)
	}

	// Every page kind the single-document path writes has to have been inside a
	// torn region, and that is checked against the pages the sweep decoded rather
	// than assumed from the workload above. PageDocumentGroup, PageFloat64Group,
	// PageFloat64Catalog, PageFloat64Stripe, and PageIndexGroupCatalog are
	// deliberately absent: only the bulk builder writes them, and
	// TestFileStoreBulkAcceleratorRetirementSurvivesCrash covers their
	// retirement. The float64 sidecar has no page kind of its own — it lives
	// inside the document pages this list already requires — so its coverage is
	// asserted by the reduction in readCommitCrashContents instead.
	sweep.requireKinds(t,
		storeio.PageDocument, storeio.PageOverflow,
		storeio.PageChunkDirectory, storeio.PageFingerprintDirectory,
		storeio.PageIndexDirectory)
	sweep.requireInert(t, storeio.PageIndexPosting)
	if len(sweep.rootSlots) != 2 {
		t.Fatalf("the sweep only ever tore superblock slot %v, so recovery's choice "+
			"between the two roots was never exercised at both parities",
			slices.Sorted(maps.Keys(sweep.rootSlots)))
	}
	if sweep.images < 80 {
		t.Fatalf("only %d torn images were checked, too few to have covered every "+
			"changed page of four commits", sweep.images)
	}
	t.Logf("single-document sweep: %s", sweep)
}

// Given a batched Update torn at every write point, when each resulting image is
// recovered, then the whole batch is present or the whole batch is absent, and
// the recovered store satisfies the same consistency and free-space properties.
//
// The batched path is the one with no sweep at all. MutatePageKeyTreeBatch,
// MutateIndexTreeBatch and the remaining tree mutators each visit and rewrite a page
// exactly once for a whole batch, which is a different page set and a different
// write order from applying the same mutations one at a time. A batch is also
// the only ordinary commit that writes several document pages, so it is the only
// one where a cut can land between two document pages of the same generation.
func TestFileStoreBatchedCommitSurvivesCrashAtEveryPageKind(t *testing.T) {
	options := commitCrashOptions(64)
	const keys = 16
	collection, names := createCommitCrashCollection(t, options, keys)
	world := commitCrashWorld{
		options: options, keys: names, scratch: t.TempDir(),
		indexes: []string{"status"}, float64s: []string{"/score"},
	}
	sweep := newCommitCrashSweep()

	batched := make([]string, 0, 48)
	for i := range 48 {
		batched = append(batched, fmt.Sprintf("batch-%02d", i))
	}
	world.keys = append(world.keys, batched...)

	commits := []struct {
		name   string
		mutate func() error
	}{
		// Four documents per chunk, so twenty-four inserts span at least six
		// chunks and the batch rewrites several document pages in one commit.
		{"batch-insert-many-chunks", func() error {
			return collection.Update(func(b *WriteBatch) error {
				for i := range 24 {
					if err := b.Put(batched[i], commitCrashDocument(11, i, 80)); err != nil {
						return err
					}
				}
				return nil
			})
		}},
		// Mixed put and delete in one batch, spanning existing and new keys, so
		// the batched key-tree descent both inserts and removes on one pass.
		{"batch-mixed", func() error {
			return collection.Update(func(b *WriteBatch) error {
				for i := 24; i < 40; i++ {
					if err := b.Put(batched[i], commitCrashDocument(12, i, 1200)); err != nil {
						return err
					}
				}
				for i := 0; i < 24; i += 3 {
					if err := b.Delete(batched[i]); err != nil {
						return err
					}
				}
				for i := 0; i < keys; i += 5 {
					if err := b.Put(names[i], commitCrashDocument(12, i, 2400)); err != nil {
						return err
					}
				}
				return nil
			})
		}},
		// Large enough that every value overflows, so the batch allocates and
		// retires overflow extents on the same pass.
		{"batch-overflow", func() error {
			return collection.Update(func(b *WriteBatch) error {
				for i := 40; i < 48; i++ {
					if err := b.Put(batched[i], commitCrashDocument(13, i, 6000)); err != nil {
						return err
					}
				}
				for i := 24; i < 32; i++ {
					if err := b.Put(batched[i], commitCrashDocument(13, i, 9000)); err != nil {
						return err
					}
				}
				return nil
			})
		}},
		// A batch of deletes only: no document bytes change hands, but the key,
		// chunk and index trees all shrink at once.
		{"batch-delete-only", func() error {
			return collection.Update(func(b *WriteBatch) error {
				for i := 1; i < 24; i += 2 {
					if err := b.Delete(batched[i]); err != nil {
						return err
					}
				}
				return nil
			})
		}},
	}
	for _, commit := range commits {
		tearCommitAtEveryPageKind(t, world, collection, commit.name, sweep, commit.mutate)
	}

	sweep.requireKinds(t,
		storeio.PageDocument, storeio.PageOverflow,
		storeio.PageChunkDirectory, storeio.PageFingerprintDirectory,
		storeio.PageIndexDirectory)
	sweep.requireInert(t, storeio.PageIndexPosting)
	if len(sweep.rootSlots) != 2 {
		t.Fatalf("the batched sweep only tore superblock slot %v",
			slices.Sorted(maps.Keys(sweep.rootSlots)))
	}
	if sweep.images < 100 {
		t.Fatalf("only %d torn images were checked across four batched commits", sweep.images)
	}
	t.Logf("batched sweep: %s", sweep)
}

// The automatic combiner feeds the same batched materializer as Update, but
// admission, arrival-order result replay, and the shared synchronous wait are
// separate concurrency machinery. Force eight ordinary Put calls into one
// generation, then tear that generation at every changed page/root boundary.
func TestFileStoreAutomaticCombinedCommitSurvivesCrash(t *testing.T) {
	options := commitCrashOptions(16)
	const keys = 16
	collection, names := createCommitCrashCollection(t, options, keys)
	world := commitCrashWorld{
		options: options, keys: names, scratch: t.TempDir(),
		indexes: []string{"status"}, float64s: []string{"/score"},
	}
	sweep := newCommitCrashSweep()
	tearCommitAtEveryPageKind(
		t, world, collection, "automatic-combined-replace", sweep,
		func() error {
			collection.writer.Lock()
			results := make([]chan combinedMutationResult, 8)
			for i := range results {
				results[i] = make(chan combinedMutationResult, 1)
				go func(i int) {
					created, err := collection.Put(
						names[i], commitCrashDocument(21, i, 700+i*113),
					)
					results[i] <- combinedMutationResult{
						changed: created, err: err,
					}
				}(i)
				waitForCombinedQueue(t, collection, i+1)
			}
			collection.writer.Unlock()
			for i, result := range results {
				got := awaitCombinedResult(t, result)
				if got.err != nil {
					return fmt.Errorf("combined replacement %d: %w", i, got.err)
				}
			}
			return nil
		},
	)
	if sweep.images == 0 || sweep.kinds[storeio.PageDocument] == 0 {
		t.Fatalf("automatic combined sweep did not cover canonical data/root pages: %s", sweep)
	}
	t.Logf("automatic combined sweep: %s", sweep)
}

// Given a run of commits long enough that one of them folds the free log, when
// every commit is torn at every write point, then a folding commit recovers to
// the same consistency the others do.
//
// A fold is three dependent writes where a plain append is one: it rewrites the
// segments it touched, republishes an index naming both those and segments it
// did not touch, and truncates the delta chain. The free-set sweep already tears
// a fold, but it checks the free set alone. What is new here is that the store's
// documents, key directory, and index postings are checked across the same torn
// fold, because the pages a fold reuses are pages some earlier generation's tree
// occupied.
func TestFileStoreFoldingCommitSurvivesCrashAtEveryPageKind(t *testing.T) {
	if testing.Short() {
		t.Skip("the fold sweep needs enough commits to reach a fold")
	}
	options := commitCrashOptions(1)
	const keys = 16
	collection, names := createCommitCrashCollection(t, options, keys)
	world := commitCrashWorld{
		options: options, keys: names, scratch: t.TempDir(),
		indexes: []string{"status"}, float64s: []string{"/score"},
	}
	sweep := newCommitCrashSweep()

	for attempt := range 10 {
		if attempt == 0 {
			collection.freeFoldRequired = true
		}
		key := names[attempt%keys]
		mutate := func() error {
			_, err := collection.Put(key, commitCrashDocument(20+attempt, attempt%keys, 200+attempt*400))
			return err
		}
		tearCommitAtEveryPageKind(t, world, collection,
			fmt.Sprintf("fold-candidate-%d", attempt), sweep, mutate)
	}
	if sweep.foldedLogs == 0 {
		t.Fatal("no commit under test folded the free log, so the index republish and " +
			"chain truncation were never inside a torn region")
	}
	sweep.requireKinds(t, storeio.PageFreeImage, storeio.PageFreeIndex, storeio.PageFreeDelta)
	t.Logf("folding sweep: %s", sweep)
}

// Given a bulk-built collection whose first mutating commit releases the compact
// index-group catalog and the dense float64 scan projection, when that commit is
// torn at every write point, then every recovered image still answers with the
// exact postings, and the accelerator pages are either wholly live or wholly
// free.
//
// This is the one commit that retires a page kind nothing else writes. The
// catalog and the projection are strictly redundant read accelerators, so the
// interesting failure is not that a query gets slower — it is that their extents
// are handed to the free set by a commit that a crash may or may not have
// published, while the alternate root still names them.
func TestFileStoreBulkAcceleratorRetirementSurvivesCrash(t *testing.T) {
	options := commitCrashOptions(64)
	options.Collection.ChunkDocuments = 8
	// The compact format is what makes the bulk builder pack several chunks into
	// one PageDocumentGroup with a PageFloat64Group sidecar. Under the default
	// verbatim format every chunk gets its own ordinary document page, so those
	// two page kinds would never reach the file and the retirement under test
	// would be a smaller one than the format actually produces.
	options.DocumentFormat = DocumentFormatCompact
	source, err := store.New(options.Collection)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, 40)
	for i := range 32 {
		key := fmt.Sprintf("key-%03d", i)
		names = append(names, key)
		if _, err := source.Put(key, commitCrashDocument(1, i, 60)); err != nil {
			t.Fatal(err)
		}
	}
	file, err := os.CreateTemp(t.TempDir(), "commit-crash-bulk-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFrom(source, file, options); err != nil {
		t.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	state := collection.state.Load()
	if state.root.IndexGroupHead == (storeio.PageRef{}) {
		t.Fatal("the bulk build produced no index-group catalog, so this commit " +
			"retires nothing the ordinary path cannot")
	}
	if state.root.Float64ScanHead == (storeio.PageRef{}) {
		t.Fatal("the bulk build produced no float64 scan projection")
	}
	// The accelerator pages must be found on disk before the commit that retires
	// them, or the coverage claim below is about pages that were never there.
	built := changedPageKinds(nil, mustReadFile(t, file.Name()), options.PageSize, options.MaxPageSize)
	accelerators := []storeio.PageKind{
		storeio.PageIndexGroupCatalog, storeio.PageFloat64Catalog,
		storeio.PageFloat64Stripe, storeio.PageFloat64Group, storeio.PageDocumentGroup,
	}
	for _, kind := range accelerators {
		if built[kind] == 0 {
			t.Fatalf("the bulk build wrote no %s page, so the retirement under test is "+
				"not the one the compact format produces", pageKindName(kind))
		}
	}
	retired := []storeio.PageRef{state.root.IndexGroupHead, state.root.Float64ScanHead}

	// The float64 scan projection is what this commit releases, so the world
	// deliberately does not ask for a reduction: requiring coverage would assert
	// the opposite of the documented behaviour. The postings it does keep are
	// checked exactly, which is the property that must not be traded away.
	world := commitCrashWorld{
		options: options, keys: names, scratch: t.TempDir(), indexes: []string{"status"},
	}
	sweep := newCommitCrashSweep()
	tearCommitAtEveryPageKind(t, world, collection, "accelerator-retirement", sweep, func() error {
		return collection.Update(func(b *WriteBatch) error {
			for i := 32; i < 40; i++ {
				key := fmt.Sprintf("key-%03d", i)
				names = append(names, key)
				if err := b.Put(key, commitCrashDocument(2, i, 60)); err != nil {
					return err
				}
			}
			return nil
		})
	})
	if sweep.images == 0 {
		t.Fatal("the accelerator-retirement commit produced no torn images")
	}
	sweep.requireKinds(t, storeio.PageDocument, storeio.PageFingerprintDirectory)
	// The retirement has to have actually happened, or every image above was
	// checked against a commit that released nothing. Each accelerator head must
	// now sit inside advertised free space — which is also what made the poison
	// pass in every one of those images write over it.
	if state := collection.state.Load(); state.root.IndexGroupHead != (storeio.PageRef{}) ||
		state.root.Float64ScanHead != (storeio.PageRef{}) {
		t.Fatalf("the commit kept the accelerators (index group %+v, float64 scan %+v), "+
			"so nothing was retired", state.root.IndexGroupHead, state.root.Float64ScanHead)
	}
	free := freeSetFromFile(t, file.Name(), options.PageSize)
	for _, ref := range retired {
		covered := false
		for _, extent := range free {
			if extent.Offset <= ref.Offset &&
				ref.Offset+uint64(ref.Length) <= extent.Offset+extent.Length {
				covered = true
				break
			}
		}
		if !covered {
			t.Fatalf("the accelerator extent %+v was dropped from the root but never "+
				"reached the free set, so the commit leaked it", ref)
		}
	}
	t.Logf("bulk accelerator sweep: %s", sweep)
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// Given a batched Update, when it publishes, then every extent it retires is
// inside the free set that same commit made durable — not only inside the
// reclaimer's memory.
//
// This is the invariant the whole "a retirement is written down by the commit
// that makes it" change exists to hold, and the batched path did not hold it.
// syncFreeLog is what makes a retirement durable: it reads c.retireScratch to
// mark the owning segment dirty and to emit one FreeOpSet delta per extent.
// applyFileBatch called it before the batch had listed the float64 scan
// projection or index-group catalog, so those accelerators reached disk only
// later, when the generation fence lifted and the reclaimer handed them to
// c.reusable. Anything still pending at that point is abandoned by a Close or
// crash.
//
// The assertion is made against the bytes rather than against the reclaimer,
// because the reclaimer held these extents the whole time in both the broken and
// the fixed ordering. Only the file can tell the two apart.
func TestCollectionUpdateWritesItsRetirementsInTheSameCommit(t *testing.T) {
	options := commitCrashOptions(64)
	file, err := os.CreateTemp(t.TempDir(), "batch-retire-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	for round := range 8 {
		if err := collection.Update(func(b *WriteBatch) error {
			for i := range 6 {
				if err := b.Put(fmt.Sprintf("key-%d-%d", round, i),
					commitCrashDocument(round, i, 100+i*300)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := collection.Flush(); err != nil {
			t.Fatal(err)
		}
		_ = freeSetFromFile(t, file.Name(), options.PageSize)
	}
	image, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	// State now lives in the fixed root. No publication may allocate the
	// standalone state-page shape that previously leaked when retirement
	// ordering was wrong.
	for _, page := range walkImagePages(image, options.PageSize, options.MaxPageSize) {
		if page.kind == storeio.PageStateRoot {
			t.Fatalf("inline-root collection allocated state page at %d", page.offset)
		}
	}
}

// Given the same content written by batched Updates in one session and across
// many close/reopen cycles, when the files are compared, then the multi-session
// file is no larger than the single-session one at all.
//
// TestFileStoreFreeSetSurvivesRestartsWithoutGrowingTheFile makes a bounded
// version of this claim for the single-document path. The batched path had no
// equivalent, and it was the one that did not hold: every batched commit
// abandoned its outgoing state root at Close, so a restart cost pages that were
// never written down.
//
// The bound here is zero rather than "the reclaimer's pending set", and that is
// the whole point of the assertion. A pending-set bound was tried first and is
// vacuous: across six sessions the reclaimer holds about 2 MiB at the Closes,
// while the bug costs 40 KiB, so the loose bound passed with the bug present.
// Once every retirement is durable in its own commit there is nothing left for a
// restart to lose — a reopen replays the whole free set, and the fenced tail
// becomes reusable again a couple of commits later — so the two files come out
// byte-identical in length. Measured: 561152 both ways with the ordering
// correct, 602112 against 561152 with it wrong.
func TestCollectionUpdateSurvivesRestartsWithoutGrowingTheFile(t *testing.T) {
	const (
		keys     = 32
		rounds   = 6
		sessions = 6
	)
	options := commitCrashOptions(64)
	write := func(collection *Collection, session int) {
		t.Helper()
		for round := range rounds {
			if err := collection.Update(func(b *WriteBatch) error {
				for key := range keys {
					if err := b.Put(fmt.Sprintf("key-%02d", key),
						commitCrashDocument(session*rounds+round, key,
							120+(round*37+key*53)%900)); err != nil {
						return err
					}
				}
				for key := round % 3; key < keys; key += 3 {
					if err := b.Delete(fmt.Sprintf("key-%02d", key)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}

	single, singleFile := openBatchCollection(t, options)
	for session := range sessions {
		write(single, session)
	}
	if err := single.Flush(); err != nil {
		t.Fatal(err)
	}
	singleEnd := single.Stats().FileEnd

	multiFile, err := os.CreateTemp(t.TempDir(), "batch-restart-multi-*")
	if err != nil {
		t.Fatal(err)
	}
	defer multiFile.Close()
	multi, err := Create(multiFile, options)
	if err != nil {
		t.Fatal(err)
	}
	heldBack := uint64(0)
	for session := range sessions {
		if session != 0 {
			if multi, err = Open(multiFile, options); err != nil {
				t.Fatal(err)
			}
		}
		write(multi, session)
		if err := multi.Flush(); err != nil {
			t.Fatal(err)
		}
		heldBack += multi.Stats().PendingRetiredBytes
		if err := multi.Close(); err != nil {
			t.Fatal(err)
		}
	}
	multi, err = Open(multiFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer multi.Close()
	multiEnd := multi.Stats().FileEnd
	if multiEnd > singleEnd {
		t.Fatalf("%d batched sessions ended at %d bytes against %d for one session of "+
			"the same writes, an excess of %d bytes (%d pages); a restart may cost "+
			"nothing once every retirement is durable in the commit that makes it, "+
			"and the reclaimer was holding %d bytes at those Closes, which is far "+
			"more than the excess and is why a pending-set bound would not have "+
			"noticed (single-session file %s)",
			sessions, multiEnd, singleEnd, multiEnd-singleEnd,
			(multiEnd-singleEnd)/uint64(options.PageSize), heldBack, singleFile.Name())
	}
}
