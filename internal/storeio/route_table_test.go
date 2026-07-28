package storeio

import (
	"encoding/binary"
	"math/rand/v2"
	"sort"
	"testing"
)

func routeBucketEntries(count int, seed uint64) []RouteBucketTableEntry {
	random := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	entries := make([]RouteBucketTableEntry, count)
	for i := range entries {
		entries[i] = RouteBucketTableEntry{
			Hash: random.Uint64(), RowID: uint16(i),
		}
	}
	return entries
}

func TestRouteBucketTableFullGeometryAndExactLookup(t *testing.T) {
	entries := routeBucketEntries(routeBucketTableMaxRows, 44)
	table, err := BuildRouteBucketTable(entries)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	if table.Len() != routeBucketTableMaxRows || table.Buckets() != routeBucketTableMaxBuckets || table.Pages() != 70 {
		t.Fatalf("table=(rows=%d buckets=%d pages=%d)", table.Len(), table.Buckets(), table.Pages())
	}
	accounting := table.Accounting()
	if accounting.PageBytes != 70*RouteBucketTablePageBytes || accounting.RootDirectoryBytes != 32 || accounting.BlockMapBytes != 70*12 || accounting.ResidentHandleBytes != 70*routeBucketTableBlockHandleBytes || accounting.TotalBytes <= accounting.PageBytes+accounting.RootDirectoryBytes+accounting.BlockMapBytes {
		t.Fatalf("accounting=%+v", accounting)
	}
	if per := float64(accounting.TotalBytes) / float64(len(entries)); per > 4.45 {
		t.Fatalf("all-in bytes/current=%.6f", per)
	} else {
		t.Logf("all-in: pages=%d root=%d map=%d total=%d B bytes/current=%.6f", accounting.PageBytes, accounting.RootDirectoryBytes, accounting.BlockMapBytes, accounting.TotalBytes, per)
	}
	for _, entry := range entries {
		row, found := table.Lookup(entry.Hash, func(row uint16) bool { return row == entry.RowID })
		if !found || row != entry.RowID {
			t.Fatalf("row %d lookup=(%d,%v)", entry.RowID, row, found)
		}
	}
	for page, block := range table.pages {
		if !routeBucketTablePageValid(block.Bytes(), page, table.Buckets(), table.Pages()) {
			t.Fatalf("invalid page %d", page)
		}
	}
}

func TestRouteBucketTableBuildReliabilityAndPathBounds(t *testing.T) {
	const tables = 100
	failures := 0
	paths, works, pages := make([]int, 0), make([]int, 0), make([]int, 0)
	for seed := uint64(1); seed <= tables; seed++ {
		entries := routeBucketEntries(routeBucketTableMaxRows, seed)
		buckets := routeBucketTableBucketsFor(len(entries))
		table, err := newRouteBucketTable(buckets, len(entries))
		if err != nil {
			t.Fatal(err)
		}
		ok := true
		for _, entry := range entries {
			placed, path, work, touched := routeBucketTableInsert(table, entry.Hash, routeBucketTableEntryWord(entry))
			if work > routeBucketTableSearchBudget {
				t.Fatalf("work=%d K=%d", work, routeBucketTableSearchBudget)
			}
			if path != 0 {
				paths = append(paths, path)
				works = append(works, work)
				pages = append(pages, touched)
			}
			if !placed {
				t.Logf("seed=%d failed row=%d work=%d buckets=%d", seed, entry.RowID, work, buckets)
				ok = false
				break
			}
		}
		if !ok {
			failures++
		}
		_ = table.Close()
	}
	sort.Ints(paths)
	sort.Ints(works)
	sort.Ints(pages)
	p99 := func(values []int) int {
		if len(values) == 0 {
			return 0
		}
		return values[len(values)*99/100]
	}
	t.Logf("K=%d full tables=%d failures=%d relocations=%d p99-path-buckets=%d p99-work=%d p99-pages=%d", routeBucketTableSearchBudget, tables, failures, len(paths), p99(paths), p99(works), p99(pages))
	if failures != 0 {
		t.Fatalf("full table build failures=%d/%d", failures, tables)
	}
}

func TestRouteBucketTableMissRateAcrossSeeds(t *testing.T) {
	const tables, misses = 20, 1_000_000
	rates := make([]float64, 0, tables)
	for seed := uint64(1); seed <= tables; seed++ {
		table, err := BuildRouteBucketTable(routeBucketEntries(routeBucketTableMaxRows, seed))
		if err != nil {
			t.Fatal(err)
		}
		checks := 0
		random := rand.New(rand.NewPCG(seed^0x5511, seed^0xa552))
		for range misses {
			table.Lookup(random.Uint64(), func(uint16) bool { checks++; return false })
		}
		_ = table.Close()
		rates = append(rates, float64(checks)/misses)
	}
	sort.Float64s(rates)
	p99, worst := rates[len(rates)*99/100], rates[len(rates)-1]
	t.Logf("%d full-table miss samples x %d: p99=%.6f worst=%.6f no-acquire worst=%.4f%%", tables, misses, p99, worst, 100*(1-worst))
	if worst > .001 {
		t.Fatalf("no-acquire gate failed at worst seed %.6f", worst)
	}
}

func TestRouteBucketTable31Over32AdmissionExperiment(t *testing.T) {
	// This is intentionally not the production admission policy. It determines
	// whether the extra density has a credible tail before it can be promoted.
	const tables = 100
	buckets := (routeBucketTableMaxRows*32 + routeBucketTableBucketSlots*31 - 1) / (routeBucketTableBucketSlots * 31)
	failures, maxWork, maxPages := 0, 0, 0
	works, pagesTouched := make([]int, 0), make([]int, 0)
	for seed := uint64(1); seed <= tables; seed++ {
		table, err := newRouteBucketTable(buckets, routeBucketTableMaxRows)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range routeBucketEntries(routeBucketTableMaxRows, seed) {
			placed, _, work, pages := routeBucketTableInsert(table, entry.Hash, routeBucketTableEntryWord(entry))
			if work != 0 {
				works = append(works, work)
				pagesTouched = append(pagesTouched, pages)
			}
			if work > maxWork {
				maxWork = work
			}
			if pages > maxPages {
				maxPages = pages
			}
			if !placed {
				failures++
				break
			}
		}
		_ = table.Close()
	}
	sort.Ints(works)
	sort.Ints(pagesTouched)
	p99 := func(values []int) int {
		if len(values) == 0 {
			return 0
		}
		return values[len(values)*99/100]
	}
	t.Logf("31/32 experiment: buckets=%d pages=%d failures=%d/%d p99-work=%d p99-pages=%d max-work=%d max-pages=%d; reject density: COW tail is too wide, production remains 15/16", buckets, (buckets+routeBucketTableBucketsPerPage-1)/routeBucketTableBucketsPerPage, failures, tables, p99(works), p99(pagesTouched), maxWork, maxPages)
}

func TestRouteBucketTableCOWMutationAccountingModel(t *testing.T) {
	// The immutable writer is not wired yet; this models its precise page
	// replacement set from the route operation. Delete rewrites the one page
	// holding the row. Insert uses the bounded relocation's returned
	// unique-page count. Every snapshot retains the replaced pages plus one
	// protected root and one 12-byte durable PageRef update.
	entries := routeBucketEntries(60_000, 808)
	table, err := BuildRouteBucketTable(entries)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	placed, _, _, insertPages := routeBucketTableInsert(table, 0xfedcba9876543210, uint32(65_000)|uint32(routeBucketTableTag(0xfedcba9876543210))<<16)
	if !placed {
		t.Fatal("model insert incomplete")
	}
	const singlePageMutation = RouteBucketTablePageBytes + routeBucketTableRootBytes + routeBucketTableBlockMapBytes
	changedPages := max(1, insertPages)
	insertRetained := changedPages*RouteBucketTablePageBytes + routeBucketTableRootBytes + changedPages*routeBucketTableBlockMapBytes
	t.Logf("COW model: insert touched=%d pages retained=%d B; delete=1 page retained=%d B", changedPages, insertRetained, singlePageMutation)
}

func TestRouteBucketTableRejectsResealedNonzeroPageTail(t *testing.T) {
	table, err := BuildRouteBucketTable(routeBucketEntries(routeBucketTableMaxRows, 19))
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	page := table.Pages() - 1
	data := table.pages[page].Bytes()
	active := int(binary.LittleEndian.Uint16(data[12:14])) * routeBucketTableBucketSlots
	binary.LittleEndian.PutUint32(data[routeBucketTablePageHeaderBytes+active*4:], 0)
	binary.LittleEndian.PutUint32(data[24:28], routeBucketTablePageChecksum(data))
	if routeBucketTablePageValid(data, page, table.Buckets(), table.Pages()) {
		t.Fatal("resealed nonzero trailing slot accepted")
	}
}

func TestRouteBucketTableMissGateAndDuplicateRows(t *testing.T) {
	if _, err := BuildRouteBucketTable([]RouteBucketTableEntry{{RowID: 3}, {RowID: 3}}); err == nil {
		t.Fatal("duplicate row accepted")
	}
	table, err := BuildRouteBucketTable(routeBucketEntries(routeBucketTableMaxRows, 5150))
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	checks := 0
	random := rand.New(rand.NewPCG(77, 88))
	for range 1_000_000 {
		if _, found := table.Lookup(random.Uint64(), func(uint16) bool { checks++; return false }); found {
			t.Fatal("miss found")
		}
	}
	rate := float64(checks) / 1_000_000
	t.Logf("1M random misses: verifier calls=%d rate=%.6f no-acquire=%.4f%%", checks, rate, 100*(1-rate))
	if rate > .001 {
		t.Fatalf("no-acquire gate failed %.6f", rate)
	}
}

func TestRouteBucketTableBudgetFailsClosed(t *testing.T) {
	const buckets = 2048
	table, err := newRouteBucketTable(buckets, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	tags := make([]uint16, buckets)
	found := make([]bool, buckets)
	for candidate := range 1 << 16 {
		sum := int(routeBucketMix(uint64(candidate)^0xd6e8feb86659fd93) % buckets)
		if !found[sum] {
			tags[sum] = uint16(candidate)
			found[sum] = true
		}
	}
	for bucket := range buckets {
		sum := (2*bucket + 1) % buckets
		if !found[sum] {
			t.Fatalf("missing tag sum %d", sum)
		}
		for slot := range routeBucketTableBucketSlots {
			table.put(bucket, slot, uint32(bucket*routeBucketTableBucketSlots+slot)|uint32(tags[sum])<<16)
		}
	}
	placed, path, work, _ := routeBucketTableRelocate(table, 0, uint32(10_000)|uint32(7)<<16, routeBucketTableSearchBudget)
	if placed || path != 0 || work != routeBucketTableSearchBudget {
		t.Fatalf("budget=(%v,%d,%d)", placed, path, work)
	}
	for bucket := range buckets {
		for slot := range routeBucketTableBucketSlots {
			if uint16(table.word(bucket, slot)) != uint16(bucket*routeBucketTableBucketSlots+slot) {
				t.Fatal("failed relocation mutated table")
			}
		}
	}
}

func BenchmarkRouteBucketTableLookup(b *testing.B) {
	entries := routeBucketEntries(routeBucketTableMaxRows, 9)
	table, err := BuildRouteBucketTable(entries)
	if err != nil {
		b.Fatal(err)
	}
	defer table.Close()
	misses := routeBucketEntries(len(entries), 10)
	for _, miss := range []bool{false, true} {
		b.Run("miss="+map[bool]string{false: "false", true: "true"}[miss], func(b *testing.B) {
			at, checks := 0, 0
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				entry := entries[at]
				if miss {
					entry = misses[at]
				}
				row, found := table.Lookup(entry.Hash, func(row uint16) bool { checks++; return !miss && row == entry.RowID })
				if found != !miss || found && row != entry.RowID {
					b.Fatal("lookup")
				}
				at++
				if at == len(entries) {
					at = 0
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(checks)/float64(b.N), "verify/op")
		})
	}
}
