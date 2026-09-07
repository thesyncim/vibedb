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

// TestPrimaryBatchTopologyMacroSpillAtTabletBoundary exercises the durable
// batch path at the actual 4096-local-ID boundary. It is opt-in because the
// immutable seed is intentionally large enough to fill one tablet with
// realistic variable-width rows. The update must publish the existing
// content-equivalent sibling spill, then publish its logical rows and exact
// postings as one atomic batch, and both generations must reopen cleanly.
func TestPrimaryBatchTopologyMacroSpillAtTabletBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip("4096-leaf durable regression is omitted from short tests")
	}
	const (
		seeded       = 90_000
		seedPayload  = 3072
		batchRows    = 64
		batchPayload = 3072
	)
	options := primaryLargeTopologyOptions(batchRows)
	options.ResidentBytes = 512 << 20
	options.InlineValueBytes = 8 << 10
	options.MaxDocumentBytes = 8 << 10
	options.MaxBatchBytes = 1 << 20
	options.Indexes = []store.IndexDefinition{{
		Name: "group", Paths: []string{"/group"},
	}}

	records := make([]PrimaryBulkBytesRecord, seeded)
	for row := range records {
		key := fmt.Appendf(nil, "row-%08d", row)
		value := fmt.Appendf(nil, `{"group":"seed","n":%d,"payload":"`, row)
		value = appendWideJSONSafePattern(value, seedPayload, row*37+11)
		value = append(value, `"}`...)
		records[row] = PrimaryBulkBytesRecord{Key: key, Value: value}
	}

	file, err := os.CreateTemp(t.TempDir(), "primary-batch-macro-*")
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
	tabletCounts := make(map[uint32]int)
	for rank := 0; rank < router.Len(); rank++ {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			t.Fatalf("bulk route at rank %d is missing", rank)
		}
		tabletID, _, ok := storeio.SplitTabletLocalIdentityBucket(uint32(route.Bucket))
		if !ok {
			t.Fatalf("bulk route %d has invalid bucket %d", rank, route.Bucket)
		}
		tabletCounts[tabletID]++
	}
	var fullTablet uint32
	for tabletID, count := range tabletCounts {
		if count == storeio.TabletLocalIdentityLocalCount {
			fullTablet = tabletID
			break
		}
	}
	if tabletCounts[fullTablet] != storeio.TabletLocalIdentityLocalCount {
		t.Fatalf("bulk leaf tablets = %v, want one tablet with %d leaves", tabletCounts, storeio.TabletLocalIdentityLocalCount)
	}
	t.Logf("seeded=%d leaves=%d full_tablet=%d", seeded, router.Len(), fullTablet)

	seedKeys := make([]string, seeded)
	for row := range seedKeys {
		seedKeys[row] = fmt.Sprintf("row-%08d", row)
	}

	// Select an interior lexical gap in the saturated tablet. The inserted keys
	// remain in one old leaf, so the batch topology planner must replace that
	// leaf with multiple ranges and therefore needs a fresh local-ID budget.
	var batchKeys [][]byte
	for row := 1; row+1 < seeded && len(batchKeys) == 0; row++ {
		base := fmt.Appendf(nil, "row-%08d", row)
		route, ok := router.Route(base)
		if !ok {
			t.Fatalf("seed route %q is missing", base)
		}
		tabletID, _, ok := storeio.SplitTabletLocalIdentityBucket(uint32(route.Bucket))
		if !ok || tabletID != fullTablet {
			continue
		}
		candidate := fmt.Appendf(nil, "row-%08d-batch-00", row)
		candidateRoute, ok := router.Route(candidate)
		if !ok {
			continue
		}
		candidateTablet, _, ok := storeio.SplitTabletLocalIdentityBucket(uint32(candidateRoute.Bucket))
		if !ok || candidateTablet != fullTablet {
			continue
		}
		batchKeys = make([][]byte, batchRows)
		for at := range batchKeys {
			batchKeys[at] = fmt.Appendf(nil, "row-%08d-batch-%02d", row, at)
			checkRoute, checkOK := router.Route(batchKeys[at])
			if !checkOK {
				batchKeys = nil
				break
			}
			checkTablet, _, checkOK := storeio.SplitTabletLocalIdentityBucket(uint32(checkRoute.Bucket))
			if !checkOK || checkTablet != fullTablet {
				batchKeys = nil
				break
			}
		}
	}
	if len(batchKeys) != batchRows {
		t.Fatalf("could not find an interior gap in full tablet %d", fullTablet)
	}

	// The spill moves the final two leaves of the selected tablet. Retain a
	// handful of its tail rows so the test checks the rows that cross that
	// content-equivalent publication, not only the newly inserted rows.
	tailSeedKeys := make([][]byte, 0, 16)
	for row := seeded - 1; row >= 0 && len(tailSeedKeys) < cap(tailSeedKeys); row-- {
		key := fmt.Appendf(nil, "row-%08d", row)
		route, ok := router.Route(key)
		if !ok {
			t.Fatalf("tail seed route %q is missing", key)
		}
		tabletID, _, ok := storeio.SplitTabletLocalIdentityBucket(uint32(route.Bucket))
		if ok && tabletID == fullTablet {
			tailSeedKeys = append(tailSeedKeys, key)
		}
	}
	if len(tailSeedKeys) != cap(tailSeedKeys) {
		t.Fatalf("full tablet %d has only %d retained tail rows", fullTablet, len(tailSeedKeys))
	}
	slices.Reverse(tailSeedKeys)
	tailSeedValues := make([][]byte, len(tailSeedKeys))
	for at, key := range tailSeedKeys {
		raw, found, readErr := collection.AppendRaw(nil, key)
		if readErr != nil || !found {
			t.Fatalf("capture tail seed %q = %q,%v,%v", key, raw, found, readErr)
		}
		tailSeedValues[at] = bytes.Clone(raw)
	}

	batchValues := make([][]byte, batchRows)
	for at := range batchValues {
		value := fmt.Appendf(nil, `{"group":"batch","n":%d,"payload":"`, at)
		value = appendWideJSONSafePattern(value, batchPayload, at*53+97)
		batchValues[at] = append(value, `"}`...)
	}

	before := collection.Stats()
	if err := collection.Update(func(batch *WriteBatch) error {
		for at, key := range batchKeys {
			if err := batch.Put(key, batchValues[at]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("boundary Update: %v", err)
	}
	after := collection.Stats()
	if after.PrimaryMacroSplits <= before.PrimaryMacroSplits {
		t.Fatalf("boundary Update did not publish a macro spill: before=%d after=%d required=%d",
			before.PrimaryMacroSplits, after.PrimaryMacroSplits,
			after.PrimaryMacroSplitRequired)
	}
	postRouter := collection.primaryRouter.Load()
	if postRouter == nil {
		t.Fatal("boundary Update removed the primary router")
	}
	for _, key := range tailSeedKeys {
		route, ok := postRouter.Route(key)
		if !ok {
			t.Fatalf("moved seed route %q is missing", key)
		}
		tabletID, _, ok := storeio.SplitTabletLocalIdentityBucket(uint32(route.Bucket))
		if !ok || tabletID == fullTablet {
			t.Fatalf("tail seed %q remained in spilled tablet %d (route=%d)", key, fullTablet, tabletID)
		}
	}
	if got := collection.Len(); got != seeded+batchRows {
		t.Fatalf("live rows = %d, want %d", got, seeded+batchRows)
	}
	batchNeedle := primaryExactTestNeedle(t, `"batch"`)
	seedNeedle := primaryExactTestNeedle(t, `"seed"`)
	wantKeys := make([]string, len(batchKeys))
	for at := range batchKeys {
		wantKeys[at] = string(batchKeys[at])
	}
	if got := primaryExactTestKeys(t, collection, "group", batchNeedle); !slices.Equal(got, wantKeys) {
		t.Fatalf("live batch postings = %d, want %d", len(got), len(wantKeys))
	}
	if got := primaryExactTestKeys(t, collection, "group", seedNeedle); !slices.Equal(got, seedKeys) {
		t.Fatalf("live seed postings = %d, want %d", len(got), len(seedKeys))
	}
	for at, key := range batchKeys {
		raw, found, readErr := collection.AppendRaw(nil, key)
		if readErr != nil || !found || !bytes.Equal(raw, batchValues[at]) {
			t.Fatalf("live %q = %q,%v,%v", key, raw, found, readErr)
		}
	}
	for at, key := range tailSeedKeys {
		raw, found, readErr := collection.AppendRaw(nil, key)
		if readErr != nil || !found || !bytes.Equal(raw, tailSeedValues[at]) {
			t.Fatalf("live moved seed %q = %q,%v,%v", key, raw, found, readErr)
		}
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatalf("reopen after boundary batch: %v", err)
	}
	defer reopened.Close()
	if got := reopened.Len(); got != seeded+batchRows {
		t.Fatalf("reopened rows = %d, want %d", got, seeded+batchRows)
	}
	if got := primaryExactTestKeys(t, reopened, "group", batchNeedle); !slices.Equal(got, wantKeys) {
		t.Fatalf("reopened batch postings = %d, want %d", len(got), len(wantKeys))
	}
	if got := primaryExactTestKeys(t, reopened, "group", seedNeedle); !slices.Equal(got, seedKeys) {
		t.Fatalf("reopened seed postings = %d, want %d", len(got), len(seedKeys))
	}
	for at, key := range batchKeys {
		raw, found, readErr := reopened.AppendRaw(nil, key)
		if readErr != nil || !found || !bytes.Equal(raw, batchValues[at]) {
			t.Fatalf("reopened %q = %q,%v,%v", key, raw, found, readErr)
		}
	}
	for at, key := range tailSeedKeys {
		raw, found, readErr := reopened.AppendRaw(nil, key)
		if readErr != nil || !found || !bytes.Equal(raw, tailSeedValues[at]) {
			t.Fatalf("reopened moved seed %q = %q,%v,%v", key, raw, found, readErr)
		}
	}
	if report, verifyErr := Verify(file); verifyErr != nil || !report.OK() {
		t.Fatalf("Verify after boundary reopen = %+v, %v", report, verifyErr)
	}
}

// TestPrimaryBatchTopologyMacroSpillAtAnchorBoundary drives the other bounded
// failure mode: the source tablet has spare local IDs, but long fences make its
// sixteen anchor pages the limiting geometry. Large one-row leaves keep the
// fixture small while making a 64-row insertion cross that byte boundary.
func TestPrimaryBatchTopologyMacroSpillAtAnchorBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip("anchor-boundary durable regression is omitted from short tests")
	}
	const (
		groups       = 144
		rowsPerGroup = 3
		seeded       = groups * rowsPerGroup
		rowPayload   = 50_000
		batchRows    = 16
	)
	options := primaryLargeTopologyOptions(batchRows)
	options.ResidentBytes = 128 << 20
	options.MaxKeyBytes = 256
	options.InlineValueBytes = 60 << 10
	options.MaxDocumentBytes = 60 << 10
	options.MaxBatchBytes = 8 << 20
	options.Indexes = []store.IndexDefinition{{
		Name: "group", Paths: []string{"/group"},
	}}

	records := make([]PrimaryBulkBytesRecord, 0, seeded)
	for group := range groups {
		for row := range rowsPerGroup {
			key := bytes.Repeat([]byte{'p'}, 128)
			key[0] = byte(group)
			key[len(key)-1] = byte(row + 1)
			value := fmt.Appendf(nil, `{"group":"seed","n":%d,"payload":"`, group*rowsPerGroup+row)
			value = appendWideJSONSafePattern(value, rowPayload, group*rowsPerGroup+row)
			value = append(value, `"}`...)
			records = append(records, PrimaryBulkBytesRecord{Key: key, Value: value})
		}
	}

	file, err := os.CreateTemp(t.TempDir(), "primary-batch-anchor-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFromByteRecords(records, file, options); err != nil {
		t.Fatalf("CreateFromByteRecords(%d): %v", seeded, err)
	}
	getFault, restoreFault := installJournalFaultSeam(t)
	defer restoreFault()
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	fault := getFault()
	if fault == nil {
		t.Fatal("journal fault seam was not installed")
	}
	router := collection.primaryRouter.Load()
	if router == nil {
		t.Fatal("anchor build did not publish a primary router")
	}
	if router.Len() != seeded {
		t.Fatalf("anchor seed leaves = %d, want %d", router.Len(), seeded)
	}
	t.Logf("anchor seeded leaves=%d", router.Len())

	base := bytes.Repeat([]byte{'p'}, 128)
	base[0] = 40
	base[len(base)-1] = 1
	batchKeys := make([][]byte, batchRows)
	batchValues := make([][]byte, batchRows)
	for at := range batchKeys {
		batchKeys[at] = append(bytes.Clone(base), fmt.Appendf(nil, "-batch-%02d", at)...)
		value := fmt.Appendf(nil, `{"group":"batch","n":%d,"payload":"`, at)
		value = appendWideJSONSafePattern(value, rowPayload, at*17+3)
		batchValues[at] = append(value, `"}`...)
		if _, ok := router.Route(batchKeys[at]); !ok {
			t.Fatalf("batch route %q is missing", batchKeys[at])
		}
	}

	batchNeedle := primaryExactTestNeedle(t, `"batch"`)
	seedNeedle := primaryExactTestNeedle(t, `"seed"`)
	wantBatch := make([]string, len(batchKeys))
	for at := range batchKeys {
		wantBatch[at] = string(batchKeys[at])
	}
	wantSeed := make([]string, 0, seeded)
	for group := range groups {
		for row := range rowsPerGroup {
			key := bytes.Repeat([]byte{'p'}, 128)
			key[0] = byte(group)
			key[len(key)-1] = byte(row + 1)
			wantSeed = append(wantSeed, string(key))
		}
	}
	checkSeed := func(label string, c *Collection) {
		t.Helper()
		if got := primaryExactTestKeys(t, c, "group", seedNeedle); !slices.Equal(got, wantSeed) {
			t.Fatalf("%s seed postings = %d, want %d", label, len(got), len(wantSeed))
		}
		if got := primaryExactTestKeys(t, c, "group", batchNeedle); len(got) != 0 {
			t.Fatalf("%s batch postings = %d before logical commit, want 0", label, len(got))
		}
	}
	checkCommitted := func(label string, c *Collection) {
		t.Helper()
		if got := primaryExactTestKeys(t, c, "group", batchNeedle); !slices.Equal(got, wantBatch) {
			t.Fatalf("%s batch postings = %d, want %d", label, len(got), len(wantBatch))
		}
		if got := primaryExactTestKeys(t, c, "group", seedNeedle); !slices.Equal(got, wantSeed) {
			t.Fatalf("%s seed postings = %d, want %d", label, len(got), len(wantSeed))
		}
		for at, key := range batchKeys {
			raw, found, readErr := c.AppendRaw(nil, key)
			if readErr != nil || !found || !bytes.Equal(raw, batchValues[at]) {
				t.Fatalf("%s batch row %q = %q,%v,%v", label, key, raw, found, readErr)
			}
		}
	}

	startGeneration := collection.Generation()
	before := collection.Stats()
	fault.Program(storeio.JournalFaultPlan{
		Phase:       storeio.JournalFaultENOSPCAppend,
		AppendIndex: fault.Appends(),
	})
	failedErr := collection.Update(func(batch *WriteBatch) error {
		for at := range batchKeys {
			if err := batch.Put(batchKeys[at], batchValues[at]); err != nil {
				return err
			}
		}
		return nil
	})
	if failedErr == nil || !fault.Faulted() {
		t.Fatalf("anchor-boundary faulted Update = %v, fired=%v", failedErr, fault.Faulted())
	}
	after := collection.Stats()
	if after.PrimaryMacroSplits <= before.PrimaryMacroSplits {
		t.Fatalf("faulted anchor Update did not publish a macro spill: before=%d after=%d required=%d",
			before.PrimaryMacroSplits, after.PrimaryMacroSplits,
			after.PrimaryMacroSplitRequired)
	}
	if got := collection.Generation(); got <= startGeneration {
		t.Fatalf("faulted anchor generation = %d, want shape generation after %d", got, startGeneration)
	}
	if got := collection.Len(); got != seeded {
		t.Fatalf("faulted anchor rows = %d, want unchanged %d", got, seeded)
	}
	checkSeed("faulted live", collection)
	persistenceErr := collection.PersistenceError()
	if persistenceErr == nil {
		t.Fatal("faulted anchor Update did not poison persistence")
	}
	closeErr := collection.Close()
	if !errors.Is(closeErr, persistenceErr) {
		t.Fatalf("faulted anchor Close = %v, want sticky %v", closeErr, persistenceErr)
	}

	recovered, err := Open(file, options)
	if err != nil {
		t.Fatalf("reopen anchor shape after rejected batch: %v", err)
	}
	if got := recovered.Len(); got != seeded {
		t.Fatalf("reopened anchor rows = %d, want unchanged %d", got, seeded)
	}
	checkSeed("reopened shape", recovered)

	// The fault was one-shot. Replaying the exact same batch against the
	// recovered content-equivalent shape must now commit all rows and postings.
	if err := recovered.Update(func(batch *WriteBatch) error {
		for at := range batchKeys {
			if err := batch.Put(batchKeys[at], batchValues[at]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("anchor retry Update: %v", err)
	}
	if got := recovered.Len(); got != seeded+batchRows {
		t.Fatalf("anchor retry rows = %d, want %d", got, seeded+batchRows)
	}
	checkCommitted("anchor retry live", recovered)
	if err := recovered.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	final, err := Open(file, options)
	if err != nil {
		t.Fatalf("reopen anchor retry: %v", err)
	}
	defer final.Close()
	if got := final.Len(); got != seeded+batchRows {
		t.Fatalf("final anchor rows = %d, want %d", got, seeded+batchRows)
	}
	checkCommitted("final anchor", final)
	if report, verifyErr := Verify(file); verifyErr != nil || !report.OK() {
		t.Fatalf("Verify after anchor retry reopen = %+v, %v", report, verifyErr)
	}
}
