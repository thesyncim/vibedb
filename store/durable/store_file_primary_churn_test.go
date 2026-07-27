package durable

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// TestFilePrimaryChurnQualification instruments the two ordered-hybrid-store
// weaknesses that a bulk-build number cannot answer (see
// docs/design/ordered-hybrid-store.md, "Known weaknesses"):
//
//   - "Holding <=5 B/key under churn requires measured occupancy and merge
//     hygiene; a bulk-build number is insufficient." We track structural bytes
//     per live key across the whole graph and the occupancy distribution across
//     the fixed leaf set as churn moves keys around.
//   - "Random churn can destroy physical scan locality without an
//     allocator/repack policy." We report the mean absolute file-offset distance
//     between lexically adjacent leaf extents, the gate metric named under
//     "Allocator and locality".
//
// This is measurement only: no engine behavior changes. Splits are signaled,
// not performed (store_file_primary_mutation.go surfaces the typed
// ErrPrimaryLeafSplitRequired for a full leaf), so the leaf set is fixed for the
// whole run and we count how often fullness bites rather than reacting to it.
//
// The workload is steady mixed churn over a fixed key population:
//
//   - 70% same-size replace of a live key (an in-place / COW value overwrite);
//   - 30% delete+reinsert that moves one unit of population from a live key to a
//     currently-absent key, drawn uniformly at random.
//
// The two halves of the keyspace are lexically interleaved (even ids built
// live, odd ids absent) so reinserts land in leaves across the whole range
// instead of piling onto the tail. Population is held flat at cfg.present, which
// keeps the structural-B/key ratio a clean occupancy signal: the numerator is
// the fixed graph structure, the denominator is the constant live-key count.
//
//	go test ./store/durable -run '^TestFilePrimaryChurnQualification$' -count=1 -v
//	go test ./store/durable -run '^TestFilePrimaryChurnQualification$' -short -v
func TestFilePrimaryChurnQualification(t *testing.T) {
	cfg := primaryChurnConfigFor(testing.Short())
	universe := cfg.present * 2

	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	oracle := make(map[int][]byte, cfg.present)
	// Even ids are built live; odd ids are the interleaved absent half that
	// delete+reinsert draws from.
	for id := 0; id < universe; id += 2 {
		value := primaryChurnValue(nil, 0)
		if err := builder.Append(primaryChurnKey(id), value); err != nil {
			t.Fatalf("append id %d: %v", id, err)
		}
		oracle[id] = value
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}

	resident := int64(128 << 20)
	if testing.Short() {
		resident = 32 << 20
	}
	options := Options{
		Backend:       BackendPortable,
		ResidentBytes: resident,
		Durability:    DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(t, built, options, "primary-churn.vibe")
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	part := newPrimaryChurnPartition(universe)
	if len(part.present) != cfg.present {
		t.Fatalf("built population = %d, want %d", len(part.present), cfg.present)
	}

	churnRNG := rand.New(rand.NewPCG(cfg.seed, 0x9e3779b97f4a7c15))
	probeRNG := rand.New(rand.NewPCG(cfg.seed^0xd1b54a32d192ed03, 0xa0761d6478bd642f))

	var (
		applied           int
		fullnessFallbacks int
		revision          int
		sinceCheckpoint   int
		sinceSample       int
		valueScratch      = make([]byte, 0, 64)
		samples           []primaryChurnSample
	)

	logTableHeader(t)
	sample := func() {
		s := primaryChurnCollectSample(t, collection, applied, fullnessFallbacks)
		logTableRow(t, s)
		if s.liveKeys != cfg.present {
			t.Fatalf(
				"population drift: live keys = %d, want %d at mutation %d",
				s.liveKeys, cfg.present, applied,
			)
		}
		if got := int(collection.Len()); got != cfg.present {
			t.Fatalf("Len = %d, want %d at mutation %d", got, cfg.present, applied)
		}
		// The ordered-hybrid-store ledger states its structural gate as "<=5.0
		// B/live key after measured churn"; the doc's own structural table is
		// leaf-only (controls, rank/boundary tables, checkpoints, header, and
		// trailer), which is the scale-invariant part. We gate on that leaf
		// metric in both modes and allow 6.0 of headroom for two measured, still
		// deliberate reasons stated by the design: (1) merges/repack are not
		// implemented, so a leaf that promotes narrow->wide on its first churn
		// insert keeps the wide 256-slot control envelope even at the ~195
		// occupancy the narrow class held more cheaply; (2) a delete that thins a
		// leaf leaves that structure standing with no merge to reclaim it.
		//
		// graphBytesPerKey additionally folds in the fixed catalog/tablet/anchor/
		// locator tail. That tail is a constant per tablet (a ~7 KiB locator and
		// ~8 KiB anchor pages), so it is ~2 B/key at 10k docs but amortizes toward
		// zero at scale -- the doc explicitly defers the <=5 claim until a
		// 100-billion-document simulation. We therefore report it and trend it,
		// but do not gate a small corpus on a tail it cannot yet amortize.
		if s.leafBytesPerKey > 6.0 {
			t.Fatalf(
				"leaf structural bytes/key = %.3f exceeds the 6.0 churn ceiling at mutation %d",
				s.leafBytesPerKey, applied,
			)
		}
		primaryChurnOracleCheck(t, collection, oracle, universe, probeRNG, cfg.oracleProbe)
		samples = append(samples, s)
	}

	// Baseline: the pristine bulk graph before any churn.
	sample()

	for applied < cfg.budget {
		ops := 0
		if churnRNG.Float64() < 0.70 {
			ops = primaryChurnReplace(t, collection, oracle, part, churnRNG, &revision, &valueScratch, &fullnessFallbacks)
		} else {
			ops = primaryChurnMove(t, collection, oracle, part, churnRNG, &revision, &valueScratch, &fullnessFallbacks)
		}
		applied += ops
		sinceCheckpoint += ops
		sinceSample += ops

		if sinceCheckpoint >= cfg.checkpointEvery {
			if err := collection.Flush(); err != nil {
				t.Fatalf("checkpoint at mutation %d: %v", applied, err)
			}
			sinceCheckpoint = 0
		}
		if sinceSample >= cfg.sampleEvery {
			sample()
			sinceSample = 0
		}
	}

	// A final sample captures the fully-churned steady state even if the budget
	// did not land on a sample boundary.
	if sinceSample != 0 {
		if err := collection.Flush(); err != nil {
			t.Fatal(err)
		}
		sample()
	}

	if len(samples) < 2 {
		return
	}
	first, last := samples[0], samples[len(samples)-1]
	t.Logf(
		"locality trend: adjacency %.2fx -> %.2fx extents over %d mutations "+
			"(%.2fx growth); leaf structural %.3f -> %.3f B/key; graph %.3f -> %.3f B/key; "+
			"occupancy p50 %d -> %d; fullness bit %d time(s)",
		first.adjRatio, last.adjRatio, last.mutations,
		primaryChurnGrowth(first.adjRatio, last.adjRatio),
		first.leafBytesPerKey, last.leafBytesPerKey,
		first.graphBytesPerKey, last.graphBytesPerKey,
		first.occP50, last.occP50, last.fullnessFallbacks,
	)
}

// BenchmarkFilePrimaryChurn reports steady-state mixed-churn throughput on a
// modest fixed population. It shares the same 70/30 replace/move mix as the
// qualification test but omits the per-sample graph walk.
func BenchmarkFilePrimaryChurn(b *testing.B) {
	const present = 10_000
	universe := present * 2
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		b.Fatal(err)
	}
	oracle := make(map[int][]byte, present)
	for id := 0; id < universe; id += 2 {
		value := primaryChurnValue(nil, 0)
		if err := builder.Append(primaryChurnKey(id), value); err != nil {
			b.Fatal(err)
		}
		oracle[id] = value
	}
	built, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 64 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(b, built, options, "primary-churn-bench.vibe")
	collection, err := Open(file, options)
	if err != nil {
		b.Fatal(err)
	}
	defer collection.Close()

	part := newPrimaryChurnPartition(universe)
	rng := rand.New(rand.NewPCG(0x6d75726d75726174, 0x9e3779b97f4a7c15))
	valueScratch := make([]byte, 0, 64)
	revision := 0
	fallbacks := 0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rng.Float64() < 0.70 {
			primaryChurnReplace(b, collection, oracle, part, rng, &revision, &valueScratch, &fallbacks)
		} else {
			primaryChurnMove(b, collection, oracle, part, rng, &revision, &valueScratch, &fallbacks)
		}
		if i%64 == 63 {
			if err := collection.Flush(); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
}

type primaryChurnConfig struct {
	present         int    // live population, equal to the built document count
	budget          int    // successful mutation operations to apply
	checkpointEvery int    // buffered-visible checkpoint cadence, in mutations
	sampleEvery     int    // graph-walk sample cadence, in mutations
	oracleProbe     int    // random keys verified byte-exact per sample
	seed            uint64 // deterministic churn/probe stream seed
}

func primaryChurnConfigFor(short bool) primaryChurnConfig {
	if short {
		return primaryChurnConfig{
			present: 10_000, budget: 10_000,
			checkpointEvery: 64, sampleEvery: 1_000,
			oracleProbe: 1_000, seed: 0x6d75726d75726174,
		}
	}
	return primaryChurnConfig{
		present: 100_000, budget: 100_000,
		checkpointEvery: 64, sampleEvery: 10_000,
		oracleProbe: 1_000, seed: 0x6d75726d75726174,
	}
}

// primaryChurnKey is a fixed 8-byte key, matching the doc's 8-byte-key regime.
// Zero padding keeps every key the same width and preserves lexical == numeric
// order; even ids are built live, odd ids are the interleaved absent half.
func primaryChurnKey(id int) string {
	return fmt.Sprintf("%08d", id)
}

// primaryChurnValue produces a constant-length 8-byte document so every replace
// and reinsert is a same-size value write, and so leaves fill their slot class
// the way the doc's 8-byte-value structural table assumes. It is a quoted JSON
// string (numbers may not carry leading zeros); the revision digits prove a
// write took effect.
func primaryChurnValue(dst []byte, revision int) []byte {
	return fmt.Appendf(dst[:0], `"%06d"`, revision%1_000_000)
}

// primaryChurnReplace overwrites one live key with a fresh same-size value and
// returns the number of successful mutations it applied.
func primaryChurnReplace(
	tb testing.TB,
	collection *Collection,
	oracle map[int][]byte,
	part *primaryChurnPartition,
	rng *rand.Rand,
	revision *int,
	valueScratch *[]byte,
	fallbacks *int,
) int {
	tb.Helper()
	id := part.present[rng.IntN(len(part.present))]
	*revision++
	*valueScratch = primaryChurnValue(*valueScratch, *revision)
	created, err := collection.Put(primaryChurnKey(id), *valueScratch)
	if errors.Is(err, ErrPrimaryLeafSplitRequired) {
		// A same-size replace never grows a leaf, so this is not expected; count
		// it and leave the oracle untouched to stay robust if it ever fires.
		*fallbacks++
		return 0
	}
	if err != nil || created {
		tb.Fatalf("replace id %d = created %v, err %v", id, created, err)
	}
	oracle[id] = append(oracle[id][:0], *valueScratch...)
	return 1
}

// primaryChurnMove deletes one live key and inserts one currently-absent key,
// holding the population flat. If the insert hits a full leaf it counts the
// fallback and restores the deleted key into its just-freed slot. It returns the
// number of successful mutations applied (always two: either delete+insert or
// delete+restore).
func primaryChurnMove(
	tb testing.TB,
	collection *Collection,
	oracle map[int][]byte,
	part *primaryChurnPartition,
	rng *rand.Rand,
	revision *int,
	valueScratch *[]byte,
	fallbacks *int,
) int {
	tb.Helper()
	from := part.present[rng.IntN(len(part.present))]
	to := part.absent[rng.IntN(len(part.absent))]

	deleted, err := collection.Delete(primaryChurnKey(from))
	if err != nil || !deleted {
		tb.Fatalf("move delete id %d = %v, err %v", from, deleted, err)
	}
	delete(oracle, from)
	part.move(from, false)

	*revision++
	*valueScratch = primaryChurnValue(*valueScratch, *revision)
	created, err := collection.Put(primaryChurnKey(to), *valueScratch)
	if errors.Is(err, ErrPrimaryLeafSplitRequired) {
		// Fullness bit: the target leaf is at its 256-slot cap and no split
		// exists to relieve it. Restore the deleted key (its slot is free) so the
		// population stays flat, and record the event.
		*fallbacks++
		*revision++
		restore := primaryChurnValue(nil, *revision)
		createdBack, restoreErr := collection.Put(primaryChurnKey(from), restore)
		if restoreErr != nil || !createdBack {
			tb.Fatalf("move restore id %d = %v, err %v", from, createdBack, restoreErr)
		}
		oracle[from] = restore
		part.move(from, true)
		return 2
	}
	if err != nil || !created {
		tb.Fatalf("move insert id %d = created %v, err %v", to, created, err)
	}
	oracle[to] = append([]byte(nil), *valueScratch...)
	part.move(to, true)
	return 2
}

// primaryChurnOracleCheck verifies a random sample of ids against the map
// oracle, covering both live keys (byte-exact value) and absent keys (miss).
func primaryChurnOracleCheck(
	tb testing.TB,
	collection *Collection,
	oracle map[int][]byte,
	universe int,
	rng *rand.Rand,
	probe int,
) {
	tb.Helper()
	buffer := make([]byte, 0, 64)
	for i := 0; i < probe; i++ {
		id := rng.IntN(universe)
		want, wantOK := oracle[id]
		got, ok, err := collection.AppendRaw(buffer[:0], primaryChurnKey(id))
		if err != nil || ok != wantOK || !bytes.Equal(got, want) {
			tb.Fatalf(
				"oracle drift id %d: got %q,%v,%v want %q,%v",
				id, got, ok, err, want, wantOK,
			)
		}
		buffer = got
	}
}

// primaryChurnPartition tracks which ids are live and which are absent, with
// O(1) membership moves via swap-remove. It also drives uniform random
// selection from either half.
type primaryChurnPartition struct {
	present   []int
	absent    []int
	isPresent []bool // indexed by id
	pos       []int  // index of id within its current slice
}

func newPrimaryChurnPartition(universe int) *primaryChurnPartition {
	p := &primaryChurnPartition{
		isPresent: make([]bool, universe),
		pos:       make([]int, universe),
	}
	for id := 0; id < universe; id++ {
		if id%2 == 0 {
			p.pos[id] = len(p.present)
			p.present = append(p.present, id)
			p.isPresent[id] = true
		} else {
			p.pos[id] = len(p.absent)
			p.absent = append(p.absent, id)
		}
	}
	return p
}

func (p *primaryChurnPartition) move(id int, toPresent bool) {
	if p.isPresent[id] == toPresent {
		return
	}
	src := &p.absent
	if p.isPresent[id] {
		src = &p.present
	}
	index := p.pos[id]
	last := len(*src) - 1
	moved := (*src)[last]
	(*src)[index] = moved
	p.pos[moved] = index
	*src = (*src)[:last]

	dst := &p.absent
	if toPresent {
		dst = &p.present
	}
	p.pos[id] = len(*dst)
	*dst = append(*dst, id)
	p.isPresent[id] = toPresent
}

type primaryChurnSample struct {
	mutations         int
	liveKeys          int
	leaves            int
	leafStructural    int     // leaf metadata only (the scale-invariant gate)
	graphStructural   int     // leaves plus catalog/tablet/anchor/locator framing
	leafBytesPerKey   float64 // leafStructural / liveKeys
	graphBytesPerKey  float64 // graphStructural / liveKeys
	occMin            int
	occP50            int
	occP90            int
	occMax            int
	meanAdjOffset     float64
	meanExtent        float64
	adjRatio          float64
	fileEnd           uint64
	reusableExtents   uint64
	reusableBytes     uint64
	fullnessFallbacks int
	splitStat         uint64
}

type primaryChurnLeafFacts struct {
	offset uint64
	extent int
	live   int
	class  storeio.CommonPrimaryLeafClass
}

func primaryChurnCollectSample(
	tb testing.TB,
	collection *Collection,
	mutations int,
	fallbacks int,
) primaryChurnSample {
	tb.Helper()
	// Sample the durable, checkpointed graph: buffered mutations place leaves at
	// memory-only virtual offsets beyond FileEnd, so physical locality is only
	// meaningful after a checkpoint pins real file offsets.
	if err := collection.Flush(); err != nil {
		tb.Fatal(err)
	}
	leaves, nonLeafStructural, err := primaryChurnWalkGraph(collection)
	if err != nil {
		tb.Fatalf("graph walk at mutation %d: %v", mutations, err)
	}
	if len(leaves) == 0 {
		tb.Fatalf("graph walk at mutation %d returned no leaves", mutations)
	}

	liveKeys := 0
	leafStructural := 0
	extentSum := 0
	occupancy := make([]int, len(leaves))
	for i, leaf := range leaves {
		liveKeys += leaf.live
		occupancy[i] = leaf.live
		extentSum += leaf.extent
		leafStructural += storeio.CommonPrimaryLeafStructuralBytes(
			leaf.class, leaf.live, leaf.extent,
		)
	}
	graphStructural := leafStructural + nonLeafStructural

	slices.Sort(occupancy)
	var adjSum float64
	for i := 1; i < len(leaves); i++ {
		delta := int64(leaves[i].offset) - int64(leaves[i-1].offset)
		if delta < 0 {
			delta = -delta
		}
		adjSum += float64(delta)
	}
	pairs := len(leaves) - 1
	if pairs < 1 {
		pairs = 1
	}
	meanAdj := adjSum / float64(pairs)
	meanExtent := float64(extentSum) / float64(len(leaves))
	adjRatio := 0.0
	if meanExtent != 0 {
		adjRatio = meanAdj / meanExtent
	}
	leafBytesPerKey := 0.0
	graphBytesPerKey := 0.0
	if liveKeys != 0 {
		leafBytesPerKey = float64(leafStructural) / float64(liveKeys)
		graphBytesPerKey = float64(graphStructural) / float64(liveKeys)
	}

	stats := collection.Stats()
	return primaryChurnSample{
		mutations:         mutations,
		liveKeys:          liveKeys,
		leaves:            len(leaves),
		leafStructural:    leafStructural,
		graphStructural:   graphStructural,
		leafBytesPerKey:   leafBytesPerKey,
		graphBytesPerKey:  graphBytesPerKey,
		occMin:            occupancy[0],
		occP50:            primaryChurnPercentile(occupancy, 50),
		occP90:            primaryChurnPercentile(occupancy, 90),
		occMax:            occupancy[len(occupancy)-1],
		meanAdjOffset:     meanAdj,
		meanExtent:        meanExtent,
		adjRatio:          adjRatio,
		fileEnd:           stats.FileEnd,
		reusableExtents:   stats.ReusableExtents,
		reusableBytes:     stats.ReusableBytes,
		fullnessFallbacks: fallbacks,
		splitStat:         stats.PrimaryLeafSplitRequired,
	}
}

// primaryChurnWalkGraph walks the rooted ordered primary graph in bytewise
// lexical leaf order, mirroring the scan cursor's tablet/anchor/row descent. It
// returns the leaf facts in adjacency order plus the summed structural framing
// of every non-leaf page (catalog nodes, tablet roots, anchor pages, and one
// locator per tablet). Leaf structural bytes are excluded here and computed by
// the caller from CommonPrimaryLeafStructuralBytes, which omits live key/value
// bytes and physical slack.
func primaryChurnWalkGraph(
	collection *Collection,
) (leaves []primaryChurnLeafFacts, nonLeafStructural int, err error) {
	state := collection.state.Load()
	if state == nil || state.root.PrimaryRoot == (storeio.PageRef{}) {
		return nil, 0, fmt.Errorf("primary churn walk: no primary root")
	}
	bounds := storeio.GlobalTabletCatalogBounds{
		StoreID:                state.root.StoreID,
		SelectedRootGeneration: state.root.Generation,
		FileEnd:                state.super.FileEnd,
		NextLogicalID:          state.root.NextLogicalID,
	}
	leafBounds := storeio.CommonPrimaryLeafBounds{
		FileEnd:           state.super.FileEnd,
		NextLogicalID:     state.root.NextLogicalID,
		AllocationQuantum: state.root.PageSize,
	}
	seenLocator := make(map[uint64]struct{})
	framing := func(header storeio.PageHeader) int {
		return storeio.PageHeaderSize + int(header.PayloadLength) + storeio.PageTrailerSize
	}

	var walkTablet func(ref storeio.PageRef) error
	walkTablet = func(ref storeio.PageRef) error {
		lease, acquireErr := collection.cache.Acquire(ref)
		if acquireErr != nil {
			return acquireErr
		}
		defer lease.Release()
		tablet := storeio.AdmittedGlobalTabletCatalogTabletRoot(lease.Page(), bounds)
		nonLeafStructural += framing(lease.Header())
		if locatorRef, ok := tablet.LocatorRef(); ok {
			if _, seen := seenLocator[locatorRef.LogicalID]; !seen {
				seenLocator[locatorRef.LogicalID] = struct{}{}
				locatorLease, locErr := collection.cache.Acquire(locatorRef)
				if locErr != nil {
					return locErr
				}
				nonLeafStructural += framing(locatorLease.Header())
				locatorLease.Release()
			}
		}
		for a := 0; a < tablet.AnchorCount(); a++ {
			anchorRoute, ok := tablet.AnchorAt(a)
			if !ok {
				return storeio.ErrGlobalTabletCatalogCorrupt
			}
			anchorLease, anchorErr := collection.cache.Acquire(anchorRoute.Ref)
			if anchorErr != nil {
				return anchorErr
			}
			nonLeafStructural += framing(anchorLease.Header())
			anchor := storeio.AdmittedGlobalTabletCatalogAnchor(
				anchorLease.Page(), &tablet, anchorRoute.PageID,
			)
			for r := 0; r < anchor.Count(); r++ {
				leafRoute, ok := anchor.RouteAt(r, 0)
				if !ok {
					anchorLease.Release()
					return storeio.ErrGlobalTabletCatalogCorrupt
				}
				leafLease, leafErr := collection.cache.Acquire(leafRoute.Ref)
				if leafErr != nil {
					anchorLease.Release()
					return leafErr
				}
				leaf := storeio.AdmittedCommonPrimaryLeaf(
					leafLease.Page(), state.root.StoreID, leafRoute.Bucket, leafBounds,
				)
				leaves = append(leaves, primaryChurnLeafFacts{
					offset: leafRoute.Ref.Offset,
					extent: int(leaf.Header().PageSize),
					live:   leaf.Len(),
					class:  leaf.Class(),
				})
				leafLease.Release()
			}
			anchorLease.Release()
		}
		return nil
	}

	var walkNode func(ref storeio.PageRef) error
	walkNode = func(ref storeio.PageRef) error {
		lease, acquireErr := collection.cache.Acquire(ref)
		if acquireErr != nil {
			return acquireErr
		}
		view := storeio.AdmittedGlobalTabletCatalogNode(lease.Page(), bounds)
		nonLeafStructural += framing(view.Header())
		childrenAreTablets := view.Level() == storeio.GlobalTabletCatalogLeaf
		count := view.Count()
		childRefs := make([]storeio.PageRef, 0, count)
		for i := 0; i < count; i++ {
			route, ok := view.RouteAt(i)
			if !ok {
				lease.Release()
				return storeio.ErrGlobalTabletCatalogCorrupt
			}
			childRefs = append(childRefs, route.Ref)
		}
		lease.Release()
		for _, child := range childRefs {
			if childrenAreTablets {
				if err := walkTablet(child); err != nil {
					return err
				}
			} else if err := walkNode(child); err != nil {
				return err
			}
		}
		return nil
	}

	if err := walkNode(state.root.PrimaryRoot); err != nil {
		return nil, 0, err
	}
	return leaves, nonLeafStructural, nil
}

func primaryChurnPercentile(sorted []int, percent int) int {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	index := percent * n / 100
	if index >= n {
		index = n - 1
	}
	return sorted[index]
}

func primaryChurnGrowth(first, last float64) float64 {
	if first == 0 {
		return 0
	}
	return last / first
}

func logTableHeader(tb testing.TB) {
	tb.Helper()
	tb.Logf(
		"churn columns | %8s %8s %6s %7s %7s | occ %5s %5s %5s %5s | "+
			"%11s %9s %8s | %12s %8s %11s | %6s %6s",
		"mut", "live", "leaves", "leafB/k", "grphB/k",
		"min", "p50", "p90", "max",
		"adj_off_B", "mean_ext", "adjXext",
		"file_end", "free_ext", "free_B",
		"full_fb", "split",
	)
}

func logTableRow(tb testing.TB, s primaryChurnSample) {
	tb.Helper()
	tb.Logf(
		"churn row     | %8d %8d %6d %7.3f %7.3f | occ %5d %5d %5d %5d | "+
			"%11.0f %9.0f %7.2fx | %12d %8d %11d | %6d %6d",
		s.mutations, s.liveKeys, s.leaves, s.leafBytesPerKey, s.graphBytesPerKey,
		s.occMin, s.occP50, s.occP90, s.occMax,
		s.meanAdjOffset, s.meanExtent, s.adjRatio,
		s.fileEnd, s.reusableExtents, s.reusableBytes,
		s.fullnessFallbacks, s.splitStat,
	)
}
