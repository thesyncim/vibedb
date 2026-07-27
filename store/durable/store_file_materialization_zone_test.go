package durable

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func materializationZoneOptions() Options {
	return Options{
		Collection:                   store.Options{ChunkDocuments: 4},
		Durability:                   DurabilityAsyncVisible,
		MaterializationDamageGranule: 512,
		DisableMutationCombining:     true,
	}
}

func TestFileMaterializationUpdatesDocumentAndZoneLeafTogether(t *testing.T) {
	previous := store.SetZonePruning(true)
	defer store.SetZonePruning(previous)

	file, err := os.CreateTemp(t.TempDir(), "materialization-zone-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := materializationZoneOptions()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	wantScratch := uint64(
		2 * (collection.options.MaxPageSize + collection.options.PageSize),
	)
	if got := collection.Stats().MaterializationScratchBytes; got != wantScratch {
		t.Fatalf("materialization scratch = %d, want %d", got, wantScratch)
	}

	const key = "zoned"
	beforeValue := []byte(`{"zone":"aaaa","payload":"11111111"}`)
	afterValue := []byte(`{"zone":"bbbb","payload":"22222222"}`)
	if _, err := collection.Put(key, beforeValue); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	beforeImage := readMaterializationTestImage(t, file)
	beforeState := collection.visibleState.Load()
	documentRef, leafRef, beforeZone := materializationTestRefs(
		t, collection, beforeState, key,
	)
	beforeStats := collection.Stats()

	if _, err := collection.Put(key, afterValue); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	afterState := collection.visibleState.Load()
	afterDocumentRef, afterLeafRef, afterZone := materializationTestRefs(
		t, collection, afterState, key,
	)
	if afterDocumentRef != documentRef || afterLeafRef != leafRef ||
		afterState.chunkRoot != beforeState.chunkRoot {
		t.Fatalf(
			"canonical identities changed: document %+v -> %+v, leaf %+v -> %+v, root %+v -> %+v",
			documentRef, afterDocumentRef, leafRef, afterLeafRef,
			beforeState.chunkRoot, afterState.chunkRoot,
		)
	}
	if afterZone == beforeZone {
		t.Fatal("tracked zone change left the directory summary byte-identical")
	}
	requireFileMaterializationValue(t, collection, key, afterValue)
	requireMaterializationZoneKeeps(t, collection, "zone", "bbbb")

	afterStats := collection.Stats()
	if afterStats.MaterializationUpdates-beforeStats.MaterializationUpdates != 1 ||
		afterStats.MaterializedBatches-beforeStats.MaterializedBatches != 1 {
		t.Fatalf("two-target materialization stats = %+v, before %+v",
			afterStats, beforeStats)
	}
	targetBytes := afterStats.MaterializationTargetBytes -
		beforeStats.MaterializationTargetBytes
	if targetBytes < 2*512 ||
		targetBytes > storeio.MaterializationJournalMaxData ||
		targetBytes >= uint64(documentRef.Length+leafRef.Length) {
		t.Fatalf(
			"two-target sparse bytes = %d, full images = %d, capsule max = %d",
			targetBytes, documentRef.Length+leafRef.Length,
			storeio.MaterializationJournalMaxData,
		)
	}

	afterImage := readMaterializationTestImage(t, file)
	journal, journalOffset := latestMaterializationTestJournal(
		t, afterImage, uint32(collection.options.PageSize),
	)
	if journal.Len() != 2 ||
		journal.PatchLen() > storeio.MaterializationJournalMaxPatches ||
		journal.DataLen() > storeio.MaterializationJournalMaxData {
		t.Fatalf(
			"journal geometry = %d targets, %d patches, %d bytes",
			journal.Len(), journal.PatchLen(), journal.DataLen(),
		)
	}
	first, _ := journal.TargetAt(0)
	second, _ := journal.TargetAt(1)
	if first.Ref.Offset+uint64(first.Ref.Length) > second.Ref.Offset {
		t.Fatalf("journal targets are not in physical order: %+v, %+v",
			first.Ref, second.Ref)
	}
	var journalDocument, journalLeaf storeio.PageRef
	for _, target := range [...]storeio.MaterializationTarget{first, second} {
		switch target.Ref.Kind {
		case storeio.PageDocument:
			journalDocument = target.Ref
		case storeio.PageChunkDirectory:
			journalLeaf = target.Ref
		}
	}
	if journalDocument != documentRef || journalLeaf != leafRef {
		t.Fatalf("journal targets = %+v, %+v; want document %+v, leaf %+v",
			first.Ref, second.Ref, documentRef, leafRef)
	}

	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	requireFileMaterializationValue(t, reopened, key, afterValue)
	requireMaterializationZoneKeeps(t, reopened, "zone", "bbbb")
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	// Model a crash after both canonical targets reached storage but before
	// the alternate root did. Recovery must roll back both pages from the one
	// capsule, leaving neither a new document/old zone nor its inverse.
	crashImage := append([]byte(nil), beforeImage...)
	copy(
		crashImage[journalOffset:journalOffset+storeio.MaterializationJournalSize],
		afterImage[journalOffset:journalOffset+storeio.MaterializationJournalSize],
	)
	for rank := 0; rank < journal.Len(); rank++ {
		target, ok := journal.TargetAt(rank)
		if !ok {
			t.Fatalf("missing journal target %d", rank)
		}
		start := int(target.Ref.Offset)
		end := start + int(target.Ref.Length)
		copy(crashImage[start:end], afterImage[start:end])
	}
	crashFile, err := os.CreateTemp(t.TempDir(), "materialization-zone-crash-*")
	if err != nil {
		t.Fatal(err)
	}
	defer crashFile.Close()
	if _, err := crashFile.Write(crashImage); err != nil {
		t.Fatal(err)
	}
	if _, err := crashFile.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(crashFile, options)
	if err != nil {
		t.Fatal(err)
	}
	requireFileMaterializationValue(t, recovered, key, beforeValue)
	requireMaterializationZoneKeeps(t, recovered, "zone", "aaaa")
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileMaterializationZoneLeafSnapshotAndBusyFallback(t *testing.T) {
	previous := store.SetZonePruning(true)
	defer store.SetZonePruning(previous)

	file, err := os.CreateTemp(t.TempDir(), "materialization-zone-fallback-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, materializationZoneOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	const key = "zoned-fallback"
	values := [][]byte{
		[]byte(`{"zone":"aaaa","payload":"11111111"}`),
		[]byte(`{"zone":"bbbb","payload":"22222222"}`),
		[]byte(`{"zone":"cccc","payload":"33333333"}`),
		[]byte(`{"zone":"dddd","payload":"44444444"}`),
	}
	if _, err := collection.Put(key, values[0]); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}

	state := collection.visibleState.Load()
	_, leafRef, _ := materializationTestRefs(t, collection, state, key)
	if !collection.cache.Invalidate(leafRef) {
		t.Fatal("failed to evict the clean zone leaf")
	}
	if _, resident, err := collection.cache.TryAcquireResident(leafRef); err != nil ||
		resident {
		t.Fatalf("evicted zone leaf resident = %v, %v", resident, err)
	}
	coldBase := collection.Stats()
	if _, err := collection.Put(key, values[1]); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	coldStats := collection.Stats()
	if coldStats.PageReads != coldBase.PageReads+1 ||
		coldStats.MaterializationUpdates !=
			coldBase.MaterializationUpdates+1 ||
		coldStats.MaterializationFallbacks !=
			coldBase.MaterializationFallbacks {
		t.Fatalf("cold resolved materialization: before %+v, after %+v",
			coldBase, coldStats)
	}
	requireFileMaterializationValue(t, collection, key, values[1])
	requireMaterializationZoneKeeps(t, collection, "zone", "bbbb")

	pinned, resident, err := collection.cache.TryAcquireResident(leafRef)
	if err != nil || !resident {
		t.Fatalf("pin resident zone leaf = %v, %v", resident, err)
	}
	if _, err := collection.Put(key, values[2]); err != nil {
		pinned.Release()
		t.Fatal(err)
	}
	pinned.Release()
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	requireFileMaterializationValue(t, collection, key, values[2])
	requireMaterializationZoneKeeps(t, collection, "zone", "cccc")

	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put(key, values[3]); err != nil {
		snapshot.Close()
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		snapshot.Close()
		t.Fatal(err)
	}
	snapshotValue, found, err := snapshot.AppendRaw(nil, key)
	if err != nil || !found || !bytes.Equal(snapshotValue, values[2]) {
		t.Fatalf("snapshot value = %q, %v, %v; want %q",
			snapshotValue, found, err, values[2])
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	requireFileMaterializationValue(t, collection, key, values[3])
	requireMaterializationZoneKeeps(t, collection, "zone", "dddd")
	stats := collection.Stats()
	if stats.MaterializationBusySkips == 0 ||
		stats.MaterializationSnapshotSkips == 0 ||
		stats.MaterializationFallbacks < 2 {
		t.Fatalf("zone fallback stats = %+v", stats)
	}
}

func TestFileMaterializationTargetRanksFollowPhysicalOrder(t *testing.T) {
	for _, test := range []struct {
		name                   string
		document, leaf         storeio.PageRef
		documentRank, leafRank uint16
	}{
		{
			name: "document-first",
			document: storeio.PageRef{
				Offset: 4096, Length: 4096, Kind: storeio.PageDocument,
			},
			leaf: storeio.PageRef{
				Offset: 8192, Length: 4096, Kind: storeio.PageChunkDirectory,
			},
			documentRank: 0, leafRank: 1,
		},
		{
			name: "reused-leaf-first",
			document: storeio.PageRef{
				Offset: 8192, Length: 4096, Kind: storeio.PageDocument,
			},
			leaf: storeio.PageRef{
				Offset: 4096, Length: 4096, Kind: storeio.PageChunkDirectory,
			},
			documentRank: 1, leafRank: 0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			documentRank, leafRank := fileMaterializationTargetRanks(
				test.document, test.leaf,
			)
			if documentRank != test.documentRank ||
				leafRank != test.leafRank {
				t.Fatalf("ranks = document %d, leaf %d; want %d, %d",
					documentRank, leafRank,
					test.documentRank, test.leafRank)
			}
		})
	}
}

func TestFileMaterializationColdRandomResolverAdmitsExactLeaf(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "materialization-cold-random-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := materializationZoneOptions()
	options.Collection.ChunkDocuments = 1
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	const rows = 32
	before := []byte(`{"zone":"fixed","payload":1.0}`)
	after := []byte(`{"zone":"fixed","payload":1e0}`)
	var documentRefs, leafRefs [rows]storeio.PageRef
	for index := range rows {
		key := fmt.Sprintf("cold-%02d", index)
		if _, err := collection.Put(key, before); err != nil {
			t.Fatal(err)
		}
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	state := collection.visibleState.Load()
	for index := range rows {
		key := fmt.Sprintf("cold-%02d", index)
		documentRefs[index], leafRefs[index], _ =
			materializationTestRefs(t, collection, state, key)
	}

	base := collection.Stats()
	for iteration := range rows {
		index := iteration * 13 & (rows - 1)
		if !collection.cache.Invalidate(documentRefs[index]) ||
			!collection.cache.Invalidate(leafRefs[index]) {
			t.Fatalf("invalidate cold update %d", index)
		}
		key := fmt.Sprintf("cold-%02d", index)
		if _, err := collection.Put(key, after); err != nil {
			t.Fatal(err)
		}
		if err := collection.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	stats := collection.Stats()
	if got := stats.MaterializationAttempts - base.MaterializationAttempts; got != rows {
		t.Fatalf("cold attempts = %d, want %d", got, rows)
	}
	if got := stats.MaterializationUpdates - base.MaterializationUpdates; got != rows {
		t.Fatalf("cold materializations = %d, want %d", got, rows)
	}
	if got := stats.MaterializationFallbacks - base.MaterializationFallbacks; got != 0 {
		t.Fatalf("cold fallbacks = %d, want 0", got)
	}
	if got := stats.PageReads - base.PageReads; got != 2*rows {
		t.Fatalf("cold page reads = %d, want %d (one leaf + one document/update)",
			got, 2*rows)
	}
}

func materializationTestRefs(
	t testing.TB,
	collection *Collection,
	state *fileStoreState,
	key string,
) (storeio.PageRef, storeio.PageRef, storeio.ChunkZone) {
	t.Helper()
	match, found, err := collection.resolveFileFingerprint(
		state, []byte(key),
	)
	if err != nil || !found {
		t.Fatalf("resolve %q = %v, %v", key, found, err)
	}
	documentRef := match.documentRef
	location := match.keyLocation()
	match.Release()
	leafRef, gotDocumentRef, view, lease, found, err :=
		storeio.LookupResidentChunkTreeLeaf(
			collection.cache, state.chunkRoot, location.Chunk,
			storeio.ChunkTreeBounds{
				FileEnd:       state.super.FileEnd,
				NextLogicalID: state.root.NextLogicalID,
			},
		)
	if err != nil || !found {
		t.Fatalf("resident chunk leaf = %v, %v", found, err)
	}
	defer lease.Release()
	if gotDocumentRef != documentRef {
		t.Fatalf("chunk leaf document = %+v, want %+v",
			gotDocumentRef, documentRef)
	}
	return documentRef, leafRef, view.Zone(location.Chunk)
}

func requireMaterializationZoneKeeps(
	t testing.TB,
	collection *Collection,
	path, value string,
) {
	t.Helper()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	code := store.ZoneCodeString(value)
	kept, total, err := snapshot.ZoneChunksScanned(
		zoneProbe(path, store.ZoneOpEq, code, code),
	)
	if err != nil {
		t.Fatal(err)
	}
	if total == 0 || kept != total {
		t.Fatalf("zone probe %s=%q kept %d of %d chunks",
			path, value, kept, total)
	}
}

func readMaterializationTestImage(t testing.TB, file *os.File) []byte {
	t.Helper()
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	image := make([]byte, info.Size())
	if _, err := file.ReadAt(image, 0); err != nil {
		t.Fatal(err)
	}
	return image
}

func latestMaterializationTestJournal(
	t testing.TB,
	image []byte,
	pageSize uint32,
) (storeio.MaterializationJournalView, int) {
	t.Helper()
	layout, err := storeio.MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	firstOffset := int(layout.MaterializationJournalOffsets[0])
	secondOffset := int(layout.MaterializationJournalOffsets[1])
	journal, slot, err := storeio.SelectMaterializationJournal(
		image[firstOffset:firstOffset+storeio.MaterializationJournalSize],
		image[secondOffset:secondOffset+storeio.MaterializationJournalSize],
	)
	if err != nil {
		t.Fatal(err)
	}
	if slot == 0 {
		return journal, firstOffset
	}
	return journal, secondOffset
}
