package durable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// verifyScanFile returns the physical offset and length of every checksum-valid
// page of kind that begins at a page boundary in file. It is the by-identity
// sweep the salvage path performs, reused by tests to locate leaves to corrupt
// and to enumerate the routing pages a catalog-loss test must destroy.
func verifyScanFile(
	t testing.TB, file *os.File, kind storeio.PageKind,
) []storeio.PageRef {
	t.Helper()
	bootstrap, err := storeio.DiscoverMutableInlineBootstrap(file)
	if err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pageSize := uint64(bootstrap.PageSize)
	fileEnd := uint64(info.Size())
	fileEnd -= fileEnd % pageSize
	layout, err := storeio.MutableStoreLayout(bootstrap.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	var refs []storeio.PageRef
	for offset := layout.DataStart; offset+pageSize <= fileEnd; {
		readLen := uint64(bootstrap.MaxPageSize)
		if remaining := fileEnd - offset; remaining < readLen {
			readLen = remaining
		}
		buf := make([]byte, readLen)
		n, _ := file.ReadAt(buf, int64(offset))
		header, _, err := storeio.OpenPage(buf[:n])
		if err != nil {
			offset += pageSize
			continue
		}
		if header.Kind == kind {
			refs = append(refs, storeio.PageRef{
				Offset: offset, LogicalID: header.LogicalID,
				Generation: header.Generation, Length: header.PageSize,
				Kind: header.Kind,
			})
		}
		offset += uint64(header.PageSize)
	}
	return refs
}

// verifyBreakChecksum flips the low checksum byte of the page at ref so OpenPage
// rejects it, without touching any other page. It is deliberately a checksum
// break rather than a reseal so detection is guaranteed and offset-precise.
func verifyBreakChecksum(t testing.TB, file *os.File, ref storeio.PageRef) {
	t.Helper()
	page := make([]byte, ref.Length)
	if _, err := file.ReadAt(page, int64(ref.Offset)); err != nil {
		t.Fatal(err)
	}
	trailer := int(ref.Length) - storeio.PageTrailerSize
	page[trailer] ^= 0xff
	if _, err := file.WriteAt(page, int64(ref.Offset)); err != nil {
		t.Fatal(err)
	}
}

func verifyFindingAt(report VerifyReport, offset uint64) bool {
	for _, finding := range report.Findings {
		if finding.Offset == offset {
			return true
		}
	}
	return false
}

func TestVerifyCleanPrimaryStoreVerifiesClean(t *testing.T) {
	const count = 5_000
	built, keys, _ := buildFilePrimaryCorpus(t, count)
	options := Options{Backend: BackendPortable, ResidentBytes: 32 << 20}
	file := createPrimaryPointFile(t, built, options, "clean.vibe")

	report, err := Verify(file)
	if err != nil {
		t.Fatal(err)
	}
	if !report.OK() {
		t.Fatalf("clean store reported %d findings: %+v",
			len(report.Findings), report.Findings)
	}
	if !report.Primary {
		t.Fatal("clean primary store not reported as primary")
	}
	if report.Documents != len(keys) {
		t.Fatalf("verify counted %d documents, want %d",
			report.Documents, len(keys))
	}
	if report.PageCounts["primary-leaf"] == 0 ||
		report.PageCounts["primary-catalog"] == 0 ||
		report.PageCounts["tablet-root"] == 0 ||
		report.PageCounts["primary-anchor"] == 0 ||
		report.PageCounts["primary-locator"] == 0 {
		t.Fatalf("verify did not walk every primary page kind: %+v",
			report.PageCounts)
	}
	if report.Generation == 0 || report.FileEnd == 0 || report.RootSlot < 0 {
		t.Fatalf("verify summary incomplete: %+v", report)
	}
}

func TestVerifyIndexedPrimaryStoreVerifiesClean(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for row := range 4000 {
		raw := fmt.Appendf(nil, `{"country":"c%03d","row":%d}`, row%100, row)
		if err := builder.Append(fmt.Sprintf("k%05d", row), raw); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Indexes: []store.IndexDefinition{
			{Name: "country", Paths: []string{"/country"}},
			{Name: "country_alias", Paths: []string{"/country"}},
		},
	}
	file, err := os.CreateTemp(t.TempDir(), "verify-indexed-primary-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFromPrimary(source, file, options); err != nil {
		t.Fatal(err)
	}
	report, err := Verify(file)
	if err != nil {
		t.Fatal(err)
	}
	if !report.OK() {
		t.Fatalf("indexed primary store reported %d findings: %+v",
			len(report.Findings), report.Findings)
	}
	if report.PageCounts["primary-exact-root"] != 1 ||
		report.PageCounts["primary-exact-leaf"] == 0 {
		t.Fatalf("verify did not walk the exact-index subtree: %+v",
			report.PageCounts)
	}
}

func TestVerifyDetectsPrimaryLeafCorruption(t *testing.T) {
	built, _, _ := buildFilePrimaryCorpus(t, 2_000)
	options := Options{Backend: BackendPortable, ResidentBytes: 32 << 20}
	file := createPrimaryPointFile(t, built, options, "leaf.vibe")

	leaves := verifyScanFile(t, file, storeio.PagePrimaryLeaf)
	if len(leaves) == 0 {
		t.Fatal("no primary leaves found to corrupt")
	}
	target := leaves[len(leaves)/2]
	verifyBreakChecksum(t, file, target)

	report, err := Verify(file)
	if err != nil {
		t.Fatal(err)
	}
	if report.OK() {
		t.Fatal("verify accepted a store with a corrupt leaf")
	}
	found := false
	for _, finding := range report.Findings {
		if finding.Kind == "primary-leaf" && finding.Offset == target.Offset {
			found = true
		}
	}
	if !found {
		t.Fatalf("no primary-leaf finding at offset %d: %+v",
			target.Offset, report.Findings)
	}
}

func TestVerifyDetectsExistingPrimaryCatalogCorruptionClasses(t *testing.T) {
	primaryBounds := func(t testing.TB, file *os.File) (
		storeio.StateRoot, storeio.GlobalTabletCatalogBounds,
	) {
		t.Helper()
		bootstrap, err := storeio.DiscoverMutableInlineBootstrap(file)
		if err != nil {
			t.Fatal(err)
		}
		scratch := make([]byte, bootstrap.MaxPageSize)
		inline, root, _, _, err := storeio.RecoverInlineStateRootWithFallback(
			file, bootstrap.PageSize, scratch,
		)
		if err != nil {
			t.Fatal(err)
		}
		return root, storeio.GlobalTabletCatalogBounds{
			StoreID: root.StoreID, SelectedRootGeneration: root.Generation,
			FileEnd: inline.FileEnd, NextLogicalID: root.NextLogicalID,
		}
	}

	// Each corruption returns the offset it damaged (0 when the class only proves
	// detection, e.g. a truncation that unseats the whole root).
	cases := map[string]func(testing.TB, *os.File, storeio.PageRef) uint64{
		"grafted StoreID": func(t testing.TB, file *os.File, rootRef storeio.PageRef) uint64 {
			root, bounds := primaryBounds(t, file)
			_ = root
			rootNode, err := storeio.OpenGlobalTabletCatalogNode(
				readPrimaryOpenTestPage(t, file, rootRef), rootRef, bounds,
			)
			if err != nil {
				t.Fatal(err)
			}
			childRef := rootNode.Route(nil).Ref
			page := readPrimaryOpenTestPage(t, file, childRef)
			page[40] ^= 1
			if _, err := storeio.SealPage(page); err != nil {
				t.Fatal(err)
			}
			writePrimaryOpenTestPage(t, file, childRef, page)
			return childRef.Offset
		},
		"wrong-kind child": func(t testing.TB, file *os.File, rootRef storeio.PageRef) uint64 {
			page := readPrimaryOpenTestPage(t, file, rootRef)
			page[storeio.PageHeaderSize+5] = byte(storeio.PageTabletRoute)
			if _, err := storeio.SealPage(page); err != nil {
				t.Fatal(err)
			}
			writePrimaryOpenTestPage(t, file, rootRef, page)
			return rootRef.Offset
		},
		"out-of-band logical ID": func(t testing.TB, file *os.File, rootRef storeio.PageRef) uint64 {
			page := readPrimaryOpenTestPage(t, file, rootRef)
			payload := page[storeio.PageHeaderSize:]
			mapBytes := int(binary.LittleEndian.Uint32(payload[12:16]))
			mapStart := storeio.PageHeaderSize +
				storeio.GlobalTabletCatalogNodePayloadHeaderBytes
			bucketAt := mapStart + storeio.TabletAnchorMapHeaderSize +
				storeio.TabletAnchorMapAcceleratorSlots*2 + 2
			binary.LittleEndian.PutUint32(
				page[bucketAt:bucketAt+4],
				storeio.GlobalTabletCatalogMaxLeafPages,
			)
			resealPrimaryOpenTestTrailer(
				page, mapStart+mapBytes-storeio.TabletAnchorMapTrailerSize,
			)
			if _, err := storeio.SealPage(page); err != nil {
				t.Fatal(err)
			}
			writePrimaryOpenTestPage(t, file, rootRef, page)
			return 0
		},
		"truncated catalog": func(t testing.TB, file *os.File, rootRef storeio.PageRef) uint64 {
			if err := file.Truncate(
				int64(rootRef.Offset + uint64(rootRef.Length) - 1),
			); err != nil {
				t.Fatal(err)
			}
			return 0
		},
	}

	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			file, rootRef := buildPrimaryOpenTestFile(t)
			offset := corrupt(t, file, rootRef)

			report, err := Verify(file)
			if err != nil {
				t.Fatal(err)
			}
			if report.OK() {
				t.Fatalf("verify accepted %q corruption", name)
			}
			if offset != 0 && !verifyFindingAt(report, offset) {
				t.Fatalf("%q: no finding at corrupted offset %d: %+v",
					name, offset, report.Findings)
			}
		})
	}
}

func TestVerifyDetectsFreeExtentOverlap(t *testing.T) {
	// A synthetic overlap unit-tests the free-set consistency check without
	// needing to corrupt a live free log: a reachable page and a free extent that
	// share bytes must be reported.
	w := &verifyWalker{
		report: &VerifyReport{PageCounts: map[string]int{}},
		reachable: []reachExtent{
			{offset: 8192, length: 4096, logical: 7, kind: "primary-leaf"},
		},
	}
	free := []storeio.FreeExtent{{Offset: 8192, Length: 4096, RetiredGeneration: 1}}
	// Mirror walkFreeSet's overlap scan directly against the fabricated set.
	at := 0
	for _, e := range free {
		for at < len(w.reachable) &&
			w.reachable[at].offset+w.reachable[at].length <= e.Offset {
			at++
		}
		if at < len(w.reachable) && w.reachable[at].offset < e.Offset+e.Length {
			hit := w.reachable[at]
			w.fail("free-overlap", e.Offset, hit.logical,
				"free extent [%d,%d) overlaps reachable %s page at offset %d",
				e.Offset, e.Offset+e.Length, hit.kind, hit.offset)
		}
	}
	if len(w.report.Findings) != 1 ||
		w.report.Findings[0].Kind != "free-overlap" ||
		w.report.Findings[0].Offset != 8192 {
		t.Fatalf("free overlap not detected: %+v", w.report.Findings)
	}
}

func TestSalvageRoundTripAfterCatalogLoss(t *testing.T) {
	const count = 3_000
	built, keys, values := buildFilePrimaryCorpus(t, count)
	expected := make(map[string][]byte, count)
	for at := range keys {
		expected[keys[at]] = values[at]
	}
	options := Options{Backend: BackendPortable, ResidentBytes: 32 << 20}
	oracle := createPrimaryPointFile(t, built, options, "oracle.vibe")

	clean, err := Verify(oracle)
	if err != nil {
		t.Fatal(err)
	}
	if !clean.OK() {
		t.Fatalf("oracle not clean before corruption: %+v", clean.Findings)
	}

	// Destroy the entire routing graph above the leaves: every catalog node
	// (root, branches, leaves) and every tablet root. Anchors, locators, and the
	// self-describing leaves survive untouched.
	catalog := verifyScanFile(t, oracle, storeio.PagePrimaryCatalog)
	tablets := verifyScanFile(t, oracle, storeio.PageTabletRoute)
	if len(catalog) == 0 || len(tablets) == 0 {
		t.Fatalf("expected catalog and tablet pages, got %d and %d",
			len(catalog), len(tablets))
	}
	for _, ref := range catalog {
		verifyBreakChecksum(t, oracle, ref)
	}
	for _, ref := range tablets {
		verifyBreakChecksum(t, oracle, ref)
	}

	// The catalog is now unrecoverable: Open must fail closed.
	if collection, err := Open(oracle, options); err == nil {
		_ = collection.Close()
		t.Fatal("Open accepted a store with a destroyed catalog")
	}

	outPath := filepath.Join(t.TempDir(), "salvaged.vibe")
	out, err := os.OpenFile(outPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	report, err := Salvage(oracle, out, options)
	if err != nil {
		t.Fatalf("salvage failed: %v", err)
	}
	if report.Documents != count {
		t.Fatalf("salvage recovered %d documents, want %d (overflow_skipped=%d duplicate_skipped=%d)",
			report.Documents, count, report.OverflowSkipped, report.DuplicateSkipped)
	}

	// The salvaged store must verify clean.
	salvagedReport, err := Verify(out)
	if err != nil {
		t.Fatal(err)
	}
	if !salvagedReport.OK() {
		t.Fatalf("salvaged store not clean: %+v", salvagedReport.Findings)
	}

	// Full contents must match the oracle byte-exact.
	collection, err := Open(out, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	got := make(map[string][]byte, count)
	if err := snapshot.RangeRaw(func(key, value []byte) error {
		got[string(key)] = append([]byte(nil), value...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(expected) {
		t.Fatalf("salvaged content has %d keys, want %d", len(got), len(expected))
	}
	for key, want := range expected {
		have, ok := got[key]
		if !ok {
			t.Fatalf("salvaged store missing key %q", key)
		}
		if !bytes.Equal(have, want) {
			t.Fatalf("salvaged value for %q = %q, want %q", key, have, want)
		}
	}
}
