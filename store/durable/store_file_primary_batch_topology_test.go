package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func primaryLargeTopologyOptions(documents int) Options {
	options := testBatchOptions(documents)
	options.ResidentBytes = 256 << 20
	options.MaxRetiredExtents = 1 << 18
	options.MaxSnapshotLeases = 16
	return options
}

func primaryBatchTabletAnchorRefs(
	collection *Collection, key []byte,
) (map[uint8]storeio.PageRef, uint8, error) {
	state := collection.state.Load()
	if state == nil {
		return nil, 0, ErrClosed
	}
	resident, err := collection.currentPrimaryResidentRoute(state, key)
	if err != nil {
		return nil, 0, err
	}
	var path filePrimaryMutationPath
	if err := collection.acquirePrimaryRoutingPath(
		&path, state, key, resident,
	); err != nil {
		return nil, 0, err
	}
	defer path.Release()
	refs := make(map[uint8]storeio.PageRef, path.tablet.AnchorCount())
	for rank := 0; rank < path.tablet.AnchorCount(); rank++ {
		anchor, ok := path.tablet.AnchorAt(rank)
		if !ok {
			return nil, 0, storeio.ErrGlobalTabletCatalogCorrupt
		}
		refs[anchor.PageID] = anchor.Ref
	}
	return refs, path.anchorRoute.PageID, nil
}

// TestPrimaryBatchTopologyRejectsSingleUnencodableDocument distinguishes a
// valid JSON value inside the configured API bound from one that can actually
// fit the maximum 64 KiB class-5 extent. A lone row cannot be subdivided, so the
// batch returns the public document-size error without publishing a shape.
func TestPrimaryBatchTopologyRejectsSingleUnencodableDocument(t *testing.T) {
	options := primaryLargeTopologyOptions(1)
	options.InlineValueBytes = 64 << 10
	options.MaxDocumentBytes = 64 << 10
	collection, _ := openBatchCollection(t, options)
	generation := collection.Generation()
	document := make([]byte, 0, 65536)
	document = append(document, `{"v":"`...)
	document = appendWideJSONSafePattern(document, 65500, 0)
	document = append(document, `"}`...)
	if len(document) > options.MaxDocumentBytes {
		t.Fatalf("fixture document = %d, bound %d", len(document), options.MaxDocumentBytes)
	}
	err := collection.Update(func(batch *WriteBatch) error {
		return batch.Put([]byte("too-wide"), document)
	})
	if !errors.Is(err, ErrDocumentTooLarge) {
		t.Fatalf("single unencodable Update = %v, want ErrDocumentTooLarge", err)
	}
	if got := collection.Generation(); got != generation {
		t.Fatalf("single unencodable generation = %d, want %d", got, generation)
	}
	if got := collection.Len(); got != 0 {
		t.Fatalf("single unencodable rows = %d, want 0", got)
	}
	if _, err := collection.Put([]byte("after"), []byte(`{"v":1}`)); err != nil {
		t.Fatalf("write after single unencodable batch: %v", err)
	}
}

// TestPrimaryBatchTopologyLocalizesTwoWaySplit keeps a K=2 batch's structural
// generation inside one selected anchor for both a full-anchor overflow and a
// non-full anchor. The held snapshot and exact postings prove each replacement
// is content-equivalent until the following logical publication; unchanged
// anchor refs prove neither case rebuilt the tablet.
func TestPrimaryBatchTopologyLocalizesTwoWaySplit(t *testing.T) {
	t.Run("anchor-overflow", func(t *testing.T) {
		primaryBatchTopologyLocalizedTwoWaySplit(t, 10, true)
	})
	t.Run("anchor-local", func(t *testing.T) {
		primaryBatchTopologyLocalizedTwoWaySplit(t, 256, false)
	})
}

func primaryBatchTopologyLocalizedTwoWaySplit(
	t *testing.T, targetRow int, wantNewAnchor bool,
) {
	t.Helper()
	const (
		seeded    = storeio.SegmentedTabletRouterRowsPerPage + 1
		batchRows = 64
	)
	options := primaryLargeTopologyOptions(batchRows)
	options.ResidentBytes = 512 << 20
	options.MaxBatchBytes = 1 << 20
	options.InlineValueBytes = 64 << 10
	options.MaxDocumentBytes = 64 << 10
	options.Indexes = []store.IndexDefinition{{
		Name: "group", Paths: []string{"/group"},
	}}

	const seedPayload = 60 << 10
	records := make([]PrimaryBulkBytesRecord, seeded)
	for row := range records {
		value := fmt.Appendf(nil,
			`{"group":"seed","n":%d,"payload":"`, row,
		)
		value = appendWideJSONSafePattern(value, seedPayload, row*37+11)
		value = append(value, `"}`...)
		records[row] = PrimaryBulkBytesRecord{
			Key:   fmt.Appendf(nil, "row-%06d", row),
			Value: value,
		}
	}
	seedKeys := make([]string, seeded)
	for row := range seedKeys {
		seedKeys[row] = string(records[row].Key)
	}
	file, err := os.CreateTemp(t.TempDir(), "primary-batch-localized-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFromByteRecords(records, file, options); err != nil {
		t.Fatalf("CreateFromByteRecords(%d): %v", seeded, err)
	}
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	router := collection.primaryRouter.Load()
	if router == nil {
		t.Fatal("bulk build did not publish a primary router")
	}
	if router.Len() <= storeio.SegmentedTabletRouterRowsPerPage {
		t.Fatalf("bulk leaves = %d, want more than one anchor page", router.Len())
	}

	// Every seed value is near the maximum extent, so each occupies one leaf.
	// Row 10 selects the full first anchor and exercises its one-page overflow;
	// row 256 selects the non-full second anchor and exercises an in-place COW.
	targetKey := fmt.Appendf(nil, "row-%06d", targetRow)
	resident, ok := router.Route(targetKey)
	if !ok {
		t.Fatalf("target route %q is missing", targetKey)
	}
	oldAnchors, oldPageID, refsErr := primaryBatchTabletAnchorRefs(
		collection, targetKey,
	)
	if refsErr != nil {
		t.Fatal(refsErr)
	}
	if len(oldAnchors) < 2 {
		t.Fatalf("target tablet anchor count = %d, want at least 2", len(oldAnchors))
	}
	batchKeys := make([][]byte, batchRows)
	for at := range batchKeys {
		batchKeys[at] = fmt.Appendf(nil, "%s-batch-%02d", targetKey, at)
		batchRoute, routeOK := router.Route(batchKeys[at])
		if !routeOK || batchRoute.Bucket != resident.Bucket {
			t.Fatalf("batch key %q left source leaf", batchKeys[at])
		}
	}

	before, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	oldRaw, oldFound, err := before.AppendRaw(nil, targetKey)
	if err != nil || !oldFound || !bytes.Contains(oldRaw, []byte(`"group":"seed"`)) {
		t.Fatalf("held seed snapshot %q = %q,%v,%v", targetKey, oldRaw, oldFound, err)
	}
	if _, found, readErr := before.AppendRaw(nil, batchKeys[0]); readErr != nil || found {
		t.Fatalf("held snapshot already contains batch row: found=%v err=%v", found, readErr)
	}
	for _, key := range batchKeys[1:] {
		if _, found, readErr := before.AppendRaw(nil, key); readErr != nil || found {
			t.Fatalf("held snapshot contains batch row %q: found=%v err=%v", key, found, readErr)
		}
	}
	seedNeedle := primaryExactTestNeedle(t, `"seed"`)
	if got := primaryExactSnapshotKeys(t, before, "group", seedNeedle); !slices.Equal(got, seedKeys) {
		t.Fatalf("held seed postings = %d, want %d", len(got), len(seedKeys))
	}
	const batchPayload = 128
	batchValues := make([][]byte, batchRows)
	for at := range batchValues {
		value := fmt.Appendf(nil,
			`{"group":"batch","n":%d,"payload":"`, at,
		)
		value = appendWideJSONSafePattern(value, batchPayload, at*53+97)
		batchValues[at] = append(value, `"}`...)
	}
	batchNeedle := primaryExactTestNeedle(t, `"batch"`)
	if got := primaryExactSnapshotKeys(t, before, "group", batchNeedle); len(got) != 0 {
		t.Fatalf("held batch postings = %d, want 0", len(got))
	}
	wantBatch := make([]string, batchRows)
	for at := range batchKeys {
		wantBatch[at] = string(batchKeys[at])
	}
	beforeStats := collection.Stats()
	if err := collection.Update(func(batch *WriteBatch) error {
		for at := range batchKeys {
			if err := batch.Put(batchKeys[at], batchValues[at]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("localized two-way Update: %v", err)
	}
	if _, found, readErr := before.AppendRaw(nil, batchKeys[0]); readErr != nil || found {
		t.Fatalf("held snapshot gained batch row after update: found=%v err=%v", found, readErr)
	}
	for _, key := range batchKeys[1:] {
		if _, found, readErr := before.AppendRaw(nil, key); readErr != nil || found {
			t.Fatalf("held snapshot gained batch row %q after update: found=%v err=%v", key, found, readErr)
		}
	}
	if got := primaryExactSnapshotKeys(t, before, "group", batchNeedle); len(got) != 0 {
		t.Fatalf("held batch postings after update = %d, want 0", len(got))
	}
	afterStats := collection.Stats()
	if got := afterStats.PrimaryLeafSplits - beforeStats.PrimaryLeafSplits; got != 1 {
		t.Fatalf("localized structural splits = %d, want 1", got)
	}
	if got := afterStats.PrimaryTabletRoutingRebuilds - beforeStats.PrimaryTabletRoutingRebuilds; got != 0 {
		t.Fatalf("localized tablet routing rebuilds = %d, want 0 (staged=%d retired=%d oldAnchors=%d oldPage=%d)",
			got,
			afterStats.PrimaryStructuralRoutingStagedBytes-beforeStats.PrimaryStructuralRoutingStagedBytes,
			afterStats.PrimaryStructuralRoutingRetiredBytes-beforeStats.PrimaryStructuralRoutingRetiredBytes,
			len(oldAnchors), oldPageID,
		)
	}
	const routingBase = uint64(
		storeio.SegmentedTabletRouterAnchorPageBytes +
			storeio.GlobalTabletCatalogLocatorBytes +
			storeio.GlobalTabletCatalogTabletBytes,
	)
	routingStaged := afterStats.PrimaryStructuralRoutingStagedBytes -
		beforeStats.PrimaryStructuralRoutingStagedBytes
	wantRoutingStaged := routingBase
	if wantNewAnchor {
		wantRoutingStaged += storeio.SegmentedTabletRouterAnchorPageBytes
	}
	if routingStaged != wantRoutingStaged {
		t.Fatalf("localized routing staged = %d, want %d", routingStaged, wantRoutingStaged)
	}
	if got := afterStats.PrimaryStructuralRoutingRetiredBytes -
		beforeStats.PrimaryStructuralRoutingRetiredBytes; got != routingBase {
		t.Fatalf("localized routing retired = %d, want %d", got, routingBase)
	}
	if oldRaw, oldFound, readErr := before.AppendRaw(nil, targetKey); readErr != nil || !oldFound || !bytes.Equal(oldRaw, records[targetRow].Value) {
		t.Fatalf("held seed snapshot after update = %q,%v,%v", oldRaw, oldFound, readErr)
	}
	if _, found, readErr := before.AppendRaw(nil, batchKeys[0]); readErr != nil || found {
		t.Fatalf("held snapshot gained batch row: found=%v err=%v", found, readErr)
	}
	if got := primaryExactSnapshotKeys(t, before, "group", seedNeedle); !slices.Equal(got, seedKeys) {
		t.Fatalf("held seed postings after update = %d, want %d", len(got), len(seedKeys))
	}
	for row, key := range seedKeys {
		got, found, readErr := before.AppendRaw(nil, []byte(key))
		if readErr != nil || !found || !bytes.Equal(got, records[row].Value) {
			t.Fatalf("held seed row %q = %q,%v,%v", key, got, found, readErr)
		}
	}
	for at, key := range batchKeys {
		got, found, readErr := collection.AppendRaw(nil, key)
		if readErr != nil || !found || !bytes.Equal(got, batchValues[at]) {
			t.Fatalf("live batch row before Flush %q = %q,%v,%v", key, got, found, readErr)
		}
	}
	for row, key := range seedKeys {
		got, found, readErr := collection.AppendRaw(nil, []byte(key))
		if readErr != nil || !found || !bytes.Equal(got, records[row].Value) {
			t.Fatalf("live seed row before Flush %q = %q,%v,%v", key, got, found, readErr)
		}
	}
	if got := primaryExactTestKeys(t, collection, "group", batchNeedle); !slices.Equal(got, wantBatch) {
		t.Fatalf("live batch postings before Flush = %d, want %d", len(got), len(wantBatch))
	}
	if got := primaryExactTestKeys(t, collection, "group", seedNeedle); !slices.Equal(got, seedKeys) {
		t.Fatalf("live seed postings before Flush = %d, want %d", len(got), len(seedKeys))
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := before.Close(); err != nil {
		t.Fatal(err)
	}
	afterAnchors, _, refsErr := primaryBatchTabletAnchorRefs(collection, targetKey)
	if refsErr != nil {
		t.Fatal(refsErr)
	}
	afterRouter := collection.primaryRouter.Load()
	if afterRouter == nil {
		t.Fatal("localized update removed the resident router")
	}
	if got := afterRouter.Len(); got != seeded+1 {
		t.Fatalf("localized resident leaves = %d, want %d", got, seeded+1)
	}
	wantAnchorCount := len(oldAnchors)
	if wantNewAnchor {
		wantAnchorCount++
	}
	if len(afterAnchors) != wantAnchorCount {
		t.Fatalf("localized anchor count = %d, want %d", len(afterAnchors), wantAnchorCount)
	}
	for pageID, oldRef := range oldAnchors {
		if pageID == oldPageID {
			continue
		}
		if afterAnchors[pageID] != oldRef {
			t.Fatalf("unaffected anchor page %d changed from %+v to %+v",
				pageID, oldRef, afterAnchors[pageID])
		}
	}
	if afterAnchors[oldPageID] == oldAnchors[oldPageID] {
		t.Fatalf("selected anchor page %d was not copy-on-written", oldPageID)
	}
	for at, key := range batchKeys {
		got, found, readErr := collection.AppendRaw(nil, key)
		if readErr != nil || !found || !bytes.Equal(got, batchValues[at]) {
			t.Fatalf("live batch row %q = %q,%v,%v", key, got, found, readErr)
		}
	}
	if got := primaryExactTestKeys(t, collection, "group", batchNeedle); !slices.Equal(got, wantBatch) {
		t.Fatalf("live batch postings = %d, want %d", len(got), len(wantBatch))
	}
	if got := primaryExactTestKeys(t, collection, "group", seedNeedle); !slices.Equal(got, seedKeys) {
		t.Fatalf("live seed postings = %d, want %d", len(got), len(seedKeys))
	}
	for row, key := range seedKeys {
		got, found, readErr := collection.AppendRaw(nil, []byte(key))
		if readErr != nil || !found || !bytes.Equal(got, records[row].Value) {
			t.Fatalf("live seed row %q = %q,%v,%v", key, got, found, readErr)
		}
	}

	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatalf("reopen localized batch: %v", err)
	}
	defer reopened.Close()
	if got := reopened.Len(); got != seeded+batchRows {
		t.Fatalf("reopened rows = %d, want %d", got, seeded+batchRows)
	}
	if router := reopened.primaryRouter.Load(); router == nil {
		t.Fatal("reopen removed the localized resident router")
	} else if got := router.Len(); got != seeded+1 {
		t.Fatalf("reopened localized resident leaves = %d, want %d", got, seeded+1)
	}
	if got := primaryExactTestKeys(t, reopened, "group", batchNeedle); !slices.Equal(got, wantBatch) {
		t.Fatalf("reopened batch postings = %d, want %d", len(got), len(wantBatch))
	}
	if got := primaryExactTestKeys(t, reopened, "group", seedNeedle); !slices.Equal(got, seedKeys) {
		t.Fatalf("reopened seed postings = %d, want %d", len(got), len(seedKeys))
	}
	for row, key := range seedKeys {
		got, found, readErr := reopened.AppendRaw(nil, []byte(key))
		if readErr != nil || !found || !bytes.Equal(got, records[row].Value) {
			t.Fatalf("reopened seed row %q = %q,%v,%v", key, got, found, readErr)
		}
	}
	for at, key := range batchKeys {
		got, found, readErr := reopened.AppendRaw(nil, key)
		if readErr != nil || !found || !bytes.Equal(got, batchValues[at]) {
			t.Fatalf("reopened batch row %q = %q,%v,%v", key, got, found, readErr)
		}
	}
}

// TestPrimaryBatchTopologyLocalizesKWayBatch64 exercises the exact 64-row,
// 256-byte workload used by the structural benchmark. The canonical planner
// produces more than two output leaves for these batches; a selected anchor is
// therefore replaced in one COW publication while the old snapshot remains
// empty. Fixed routing bytes and zero full-tablet rebuilds prove that the
// localized K-way path was selected, while exact primary and posting checks
// cover the current, durable, and reopened images.
func TestPrimaryBatchTopologyLocalizesKWayBatch64(t *testing.T) {
	const (
		batches = 16
		rows    = batches * batch64Rows
	)
	options := benchBatchOptions(batch64Rows)
	options.Indexes = []store.IndexDefinition{
		{Name: "id", Paths: []string{"/id"}},
	}
	collection, file := openBatchCollection(t, options)
	before, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()

	keys := make([][]byte, rows)
	values := make([][]byte, rows)
	for row := range rows {
		key := make([]byte, len("row-"), len("row-")+batch64KeyDigits)
		copy(key, "row-")
		keys[row] = appendBatch64FixedUint(key, uint64(row))
		value := fmt.Appendf(nil, `{"id":%d,"value":"`, row)
		value = appendBatch64VariedPayload(value, uint64(row))
		values[row] = append(value, `"}`...)
	}

	start := collection.Stats()
	for batchStart := 0; batchStart < rows; batchStart += batch64Rows {
		if err := collection.Update(func(batch *WriteBatch) error {
			for row := batchStart; row < batchStart+batch64Rows; row++ {
				if err := batch.Put(keys[row], values[row]); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("batch %d Update: %v", batchStart/batch64Rows, err)
		}
	}

	if got := collection.Len(); got != rows {
		t.Fatalf("live rows = %d, want %d", got, rows)
	}
	stats := collection.Stats()
	splits := stats.PrimaryLeafSplits - start.PrimaryLeafSplits
	if splits == 0 || splits >= batches {
		t.Fatalf("localized structural splits = %d, want between 1 and %d", splits, batches)
	}
	if got := stats.PrimaryTabletRoutingRebuilds -
		start.PrimaryTabletRoutingRebuilds; got != 0 {
		t.Fatalf("K-way tablet routing rebuilds = %d, want 0", got)
	}
	const routingBase = uint64(
		storeio.SegmentedTabletRouterAnchorPageBytes +
			storeio.GlobalTabletCatalogLocatorBytes +
			storeio.GlobalTabletCatalogTabletBytes,
	)
	if got, want := stats.PrimaryStructuralRoutingStagedBytes-
		start.PrimaryStructuralRoutingStagedBytes, splits*routingBase; got != want {
		t.Fatalf("K-way routing staged bytes = %d, want %d", got, want)
	}
	if got, want := stats.PrimaryStructuralRoutingRetiredBytes-
		start.PrimaryStructuralRoutingRetiredBytes, splits*routingBase; got != want {
		t.Fatalf("K-way routing retired bytes = %d, want %d", got, want)
	}
	router := collection.primaryRouter.Load()
	if router == nil || router.Len() <= int(splits)+1 {
		t.Fatalf("resident K-way leaves = %v, want more than %d", router, splits+1)
	}

	if before.Len() != 0 {
		t.Fatalf("held snapshot rows = %d, want 0", before.Len())
	}
	for row, key := range keys {
		if _, found, readErr := before.AppendRaw(nil, key); readErr != nil || found {
			t.Fatalf("held snapshot row %d = found:%v err:%v, want absent", row, found, readErr)
		}
		needle := primaryExactTestNeedle(t, fmt.Sprintf("%d", row))
		if got := primaryExactSnapshotKeys(t, before, "id", needle); len(got) != 0 {
			t.Fatalf("held snapshot posting %d = %v, want empty", row, got)
		}
	}
	for row, key := range keys {
		got, found, readErr := collection.AppendRaw(nil, key)
		if readErr != nil || !found || !bytes.Equal(got, values[row]) {
			t.Fatalf("live row %d = %q,%v,%v", row, got, found, readErr)
		}
		needle := primaryExactTestNeedle(t, fmt.Sprintf("%d", row))
		if got := primaryExactTestKeys(t, collection, "id", needle); !slices.Equal(got, []string{string(key)}) {
			t.Fatalf("live posting %d = %v, want %q", row, got, key)
		}
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := before.Close(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatalf("reopen K-way batch: %v", err)
	}
	defer reopened.Close()
	if got := reopened.Len(); got != rows {
		t.Fatalf("reopened rows = %d, want %d", got, rows)
	}
	if router := reopened.primaryRouter.Load(); router == nil || router.Len() <= int(splits)+1 {
		t.Fatalf("reopened K-way leaves = %v, want more than %d", router, splits+1)
	}
	for row, key := range keys {
		got, found, readErr := reopened.AppendRaw(nil, key)
		if readErr != nil || !found || !bytes.Equal(got, values[row]) {
			t.Fatalf("reopened row %d = %q,%v,%v", row, got, found, readErr)
		}
		needle := primaryExactTestNeedle(t, fmt.Sprintf("%d", row))
		if got := primaryExactTestKeys(t, reopened, "id", needle); !slices.Equal(got, []string{string(key)}) {
			t.Fatalf("reopened posting %d = %v, want %q", row, got, key)
		}
	}
}

// Long fences must use additional anchor pages rather than refuse a logical
// batch that fits the tablet's byte and identity budgets.
func TestPrimaryBatchTopologyPacksLongFencesAcrossAnchors(t *testing.T) {
	const rows = 3000
	options := primaryLargeTopologyOptions(rows)
	options.MaxKeyBytes = storeio.CommonPrimaryLeafMaxKeyBytes
	collection, file := openBatchCollection(t, options)
	prefix := bytes.Repeat([]byte("p"), 254)
	err := collection.Update(func(batch *WriteBatch) error {
		for i := range rows {
			key := append(append([]byte(nil), prefix...), byte(i>>8), byte(i))
			word := fmt.Sprintf("%08x", uint32(i)*2654435761)
			pad := bytes.Repeat([]byte(word), 55)
			document := fmt.Appendf(nil, `{"v":"%s","n":%d}`, pad, i)
			if err := batch.Put(key, document); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("long-fence Update: %v", err)
	}
	if got := collection.Len(); got != rows {
		t.Fatalf("rows=%d want=%d", got, rows)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for i := range rows {
		key := append(append([]byte(nil), prefix...), byte(i>>8), byte(i))
		word := fmt.Sprintf("%08x", uint32(i)*2654435761)
		want := fmt.Appendf(nil, `{"n":%d,"v":"%s"}`, i, bytes.Repeat([]byte(word), 55))
		got, found, err := reopened.AppendRaw(nil, key)
		if err != nil || !found || !bytes.Equal(got, want) {
			t.Fatalf("reopened row %d found=%v err=%v", i, found, err)
		}
	}
}

// TestPrimaryBatchTopologyShapeSurvivesRejectedLogicalBatch injects a journal
// failure after the content-equivalent K-way shape has committed but before the
// logical batch can publish. Advancing the structural generation is legal;
// exposing even one user row or posting is not. Reopen must recover the empty
// shaped graph without replaying the rejected batch.
func TestPrimaryBatchTopologyShapeSurvivesRejectedLogicalBatch(t *testing.T) {
	const rows = 300
	getFault, restore := installJournalFaultSeam(t)
	defer restore()
	options := primaryLargeTopologyOptions(rows)
	options.Indexes = []store.IndexDefinition{
		{Name: "group", Paths: []string{"/group"}},
	}
	collection, file := openBatchCollection(t, options)
	before, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	startGeneration := collection.Generation()
	startStats := collection.Stats()
	fault := getFault()
	if fault == nil {
		t.Fatal("journal fault seam was not installed")
	}
	fault.Program(storeio.JournalFaultPlan{
		Phase:       storeio.JournalFaultENOSPCAppend,
		AppendIndex: fault.Appends(),
	})
	err = collection.Update(func(batch *WriteBatch) error {
		for i := range rows {
			if err := batch.Put(
				[]byte(fmt.Sprintf("reject-%04d", i)),
				[]byte(fmt.Sprintf(`{"group":"rejected","n":%d}`, i)),
			); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil || !fault.Faulted() {
		t.Fatalf("faulted logical batch = %v, fired=%v", err, fault.Faulted())
	}
	if got := collection.Generation(); got != startGeneration+1 {
		t.Fatalf("post-fault generation = %d, want shape generation %d", got, startGeneration+1)
	}
	afterStats := collection.Stats()
	if got := afterStats.PrimaryLeafSplits - startStats.PrimaryLeafSplits; got != 1 {
		t.Fatalf("rejected batch shape splits = %d, want 1", got)
	}
	if got := afterStats.PrimaryTabletRoutingRebuilds - startStats.PrimaryTabletRoutingRebuilds; got != 0 {
		t.Fatalf("rejected batch shape routing rebuilds = %d, want 0", got)
	}
	const routingBase = uint64(
		storeio.SegmentedTabletRouterAnchorPageBytes +
			storeio.GlobalTabletCatalogLocatorBytes +
			storeio.GlobalTabletCatalogTabletBytes,
	)
	if got := afterStats.PrimaryStructuralRoutingStagedBytes -
		startStats.PrimaryStructuralRoutingStagedBytes; got != routingBase {
		t.Fatalf("rejected batch shape routing staged = %d, want %d", got, routingBase)
	}
	if got := afterStats.PrimaryStructuralRoutingRetiredBytes -
		startStats.PrimaryStructuralRoutingRetiredBytes; got != routingBase {
		t.Fatalf("rejected batch shape routing retired = %d, want %d", got, routingBase)
	}
	if got := collection.Len(); got != 0 {
		t.Fatalf("rejected logical batch exposed %d rows", got)
	}
	oldRows := 0
	if err := before.RangeRaw(func(_, _ []byte) error {
		oldRows++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if oldRows != 0 {
		t.Fatalf("old snapshot scanned %d rows, want 0", oldRows)
	}
	needle := primaryExactTestNeedle(t, `"rejected"`)
	if got := primaryExactSnapshotKeys(t, before, "group", needle); len(got) != 0 {
		t.Fatalf("old snapshot saw rejected postings: %v", got)
	}
	if err := before.Close(); err != nil {
		t.Fatal(err)
	}
	persistenceErr := collection.PersistenceError()
	if persistenceErr == nil {
		t.Fatal("journal failure did not poison the collection")
	}
	closeErr := collection.Close()
	if !errors.Is(closeErr, persistenceErr) {
		t.Fatalf("faulted Close = %v, want sticky %v", closeErr, persistenceErr)
	}
	if repeated := collection.Close(); repeated != closeErr {
		t.Fatalf("repeated faulted Close = %v, want cached exact error %v", repeated, closeErr)
	}

	// The one-shot seam is exhausted. Opening the same physical image must select
	// the durable shape generation and an empty exact index.
	reopened, openErr := Open(file, options)
	if openErr != nil {
		t.Fatalf("reopen content-equivalent shape: %v", openErr)
	}
	defer reopened.Close()
	if got := reopened.Len(); got != 0 {
		t.Fatalf("reopened rejected batch rows = %d", got)
	}
	if got := primaryExactTestKeys(t, reopened, "group", needle); len(got) != 0 {
		t.Fatalf("reopened rejected postings = %v", got)
	}
	if got := reopened.Generation(); got != startGeneration+1 {
		t.Fatalf("reopened generation = %d, want shape %d", got, startGeneration+1)
	}
}

// TestPrimaryBatchTopologyEmptyIndexedThousand is the sparse-batch regression:
// a fresh graph has one empty leaf, yet one atomic Update must route and encode
// one thousand rows without a train of content-visible partial splits. One
// K-way structural generation creates all ranges; the following generation
// publishes rows and exact postings together. The pre-batch snapshot and a
// reopen validate both sides of that boundary.
func TestPrimaryBatchTopologyEmptyIndexedThousand(t *testing.T) {
	const rows = 1000
	options := primaryLargeTopologyOptions(rows)
	options.Indexes = []store.IndexDefinition{
		{Name: "group", Paths: []string{"/group"}},
	}
	collection, file := openBatchCollection(t, options)
	before, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	needle := primaryExactTestNeedle(t, `"all"`)
	if err := collection.Update(func(batch *WriteBatch) error {
		for i := range rows {
			key := fmt.Appendf(nil, "row-%04d", i)
			document := fmt.Appendf(nil, `{"group":"all","n":%d}`, i)
			if err := batch.Put(key, document); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("thousand-row indexed Update: %v", err)
	}
	if got := collection.Len(); got != rows {
		t.Fatalf("live rows = %d, want %d", got, rows)
	}
	if got := collection.Stats().PrimaryLeafSplits; got != 1 {
		t.Fatalf("shape-only structural publications = %d, want 1", got)
	}
	if got := primaryExactTestKeys(t, collection, "group", needle); len(got) != rows {
		t.Fatalf("live indexed rows = %d, want %d", len(got), rows)
	}
	oldRows := 0
	if err := before.RangeRaw(func(_, _ []byte) error {
		oldRows++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if oldRows != 0 || before.Len() != 0 {
		t.Fatalf("pre-batch snapshot = %d scanned/%d len, want empty", oldRows, before.Len())
	}
	if got := primaryExactSnapshotKeys(t, before, "group", needle); len(got) != 0 {
		t.Fatalf("pre-batch snapshot postings = %d, want 0", len(got))
	}
	if err := before.Close(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.Len(); got != rows {
		t.Fatalf("reopened rows = %d, want %d", got, rows)
	}
	if got := primaryExactTestKeys(t, reopened, "group", needle); len(got) != rows {
		t.Fatalf("reopened indexed rows = %d, want %d", len(got), rows)
	}
	for _, i := range []int{0, 255, 256, 511, 999} {
		key := []byte(fmt.Sprintf("row-%04d", i))
		raw, ok, readErr := reopened.AppendRaw(nil, key)
		if readErr != nil || !ok || !bytes.Contains(raw, []byte(`"group":"all"`)) {
			t.Fatalf("reopened %q = %q,%v,%v", key, raw, ok, readErr)
		}
	}
	if report, verifyErr := Verify(file); verifyErr != nil || !report.OK() {
		t.Fatalf("Verify after reopen = %+v, %v", report, verifyErr)
	}
}

// TestPrimaryBatchTopologyValidatesCurrentAndFinalImages combines the two cases
// a prospective-only splitter misses: many current rows are deleted while the
// survivors grow sharply. Every intermediate range must still encode the old
// content, and every final range must encode the larger replacement content.
func TestPrimaryBatchTopologyValidatesCurrentAndFinalImages(t *testing.T) {
	const seeded = 180
	options := primaryLargeTopologyOptions(seeded + 80)
	options.InlineValueBytes = 2048
	options.MaxDocumentBytes = 2048
	options.Indexes = []store.IndexDefinition{
		{Name: "state", Paths: []string{"/state"}},
	}
	collection, _ := openBatchCollection(t, options)
	for i := range seeded {
		key := []byte(fmt.Sprintf("mix-%04d", i))
		document := []byte(fmt.Sprintf(`{"state":"old","n":%d}`, i))
		if _, err := collection.Put(key, document); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	before, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	largePad := bytes.Repeat([]byte("abcdefghij"), 110)
	if err := collection.Update(func(batch *WriteBatch) error {
		for i := range seeded {
			key := []byte(fmt.Sprintf("mix-%04d", i))
			if i%2 == 0 {
				if err := batch.Delete(key); err != nil {
					return err
				}
				continue
			}
			document := fmt.Appendf(
				nil, `{"state":"grown","n":%d,"pad":"%s-%04d"}`,
				i, largePad, i,
			)
			if err := batch.Put(key, document); err != nil {
				return err
			}
		}
		for i := range 80 {
			key := []byte(fmt.Sprintf("mix-new-%04d", i))
			document := fmt.Appendf(
				nil, `{"state":"grown","n":%d,"pad":"%s-new-%04d"}`,
				i, largePad, i,
			)
			if err := batch.Put(key, document); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("delete-heavy growing Update: %v", err)
	}
	if got, want := collection.Len(), uint64(seeded/2+80); got != want {
		t.Fatalf("live rows = %d, want %d", got, want)
	}
	old := primaryExactTestNeedle(t, `"old"`)
	grown := primaryExactTestNeedle(t, `"grown"`)
	if got := primaryExactTestKeys(t, collection, "state", old); len(got) != 0 {
		t.Fatalf("live old postings = %d, want 0", len(got))
	}
	if got, want := primaryExactTestKeys(t, collection, "state", grown), seeded/2+80; len(got) != want {
		t.Fatalf("live grown postings = %d, want %d", len(got), want)
	}
	if got := primaryExactSnapshotKeys(t, before, "state", old); len(got) != seeded {
		t.Fatalf("old snapshot postings = %d, want %d", len(got), seeded)
	}
	for _, i := range []int{0, 1, 178, 179} {
		key := []byte(fmt.Sprintf("mix-%04d", i))
		raw, ok, readErr := collection.AppendRaw(nil, key)
		if i%2 == 0 {
			if readErr != nil || ok {
				t.Fatalf("deleted %q = %q,%v,%v", key, raw, ok, readErr)
			}
		} else if readErr != nil || !ok || !bytes.Contains(raw, []byte(`"state":"grown"`)) {
			t.Fatalf("grown %q = %q,%v,%v", key, raw, ok, readErr)
		}
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
}

// BenchmarkPrimaryBatchTopologyPlan measures only the deterministic structural
// preparation for a sparse one-thousand-row leaf: union construction, canonical
// byte-aware planning of current and final images, and cut refinement.
func BenchmarkPrimaryBatchTopologyPlan(b *testing.B) {
	const rows = 1000
	collection := &Collection{
		storeID:               [16]byte{1},
		primaryUnifiedBuilder: storeio.NewUnifiedPrimaryLeafBuilder(),
	}
	prospective := make([]storeio.CommonPrimaryLeafRecord, rows)
	values := make([][]byte, rows)
	for i := range rows {
		values[i] = fmt.Appendf(
			nil, `{"group":"g%02d","n":%d,"name":"row-%04d"}`,
			i%31, i, i,
		)
		prospective[i] = storeio.CommonPrimaryLeafRecord{
			Key: []byte(fmt.Sprintf("row-%04d", i)),
			Value: storeio.CommonPrimaryLeafValue{
				Inline: values[i],
			},
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		cuts, keys, err := collection.planPrimaryBatchTopologyCuts(
			nil, prospective,
		)
		if err != nil || len(cuts) == 0 || len(keys) != rows {
			b.Fatalf("plan = %d cuts/%d keys, %v", len(cuts), len(keys), err)
		}
	}
}
