package durable

import (
	"bytes"
	"errors"
	"fmt"
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
	document = append(document, bytes.Repeat([]byte("x"), 65500)...)
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

// TestPrimaryBatchTopologyAnchorPreflightRollsBack proves a K-way plan whose
// long split fences cannot fit the tablet anchor grammar is rejected before the
// structural transaction publishes anything. The collection remains writable.
func TestPrimaryBatchTopologyAnchorPreflightRollsBack(t *testing.T) {
	const rows = 200
	options := primaryLargeTopologyOptions(rows)
	options.MaxKeyBytes = storeio.CommonPrimaryLeafMaxKeyBytes
	collection, _ := openBatchCollection(t, options)
	generation := collection.Generation()
	prefix := bytes.Repeat([]byte("p"), 255)
	pad := bytes.Repeat([]byte("x"), 440)
	err := collection.Update(func(batch *WriteBatch) error {
		for i := range rows {
			key := append(append([]byte(nil), prefix...), byte(i))
			document := fmt.Appendf(nil, `{"v":"%s","n":%d}`, pad, i)
			if err := batch.Put(key, document); err != nil {
				return err
			}
		}
		return nil
	})
	if !errors.Is(err, storeio.ErrSegmentedTabletRouterNoSpace) {
		t.Fatalf("long-fence Update = %v, want anchor no-space", err)
	}
	if got := collection.Generation(); got != generation {
		t.Fatalf("failed preflight generation = %d, want %d", got, generation)
	}
	if got := collection.Len(); got != 0 {
		t.Fatalf("failed preflight rows = %d, want 0", got)
	}
	if _, err := collection.Put([]byte("after"), []byte(`{"v":2}`)); err != nil {
		t.Fatalf("write after topology preflight failure: %v", err)
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
	startGeneration := collection.Generation()
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
	if closeErr := collection.Close(); !errors.Is(closeErr, persistenceErr) {
		t.Fatalf("faulted Close = %v, want sticky %v", closeErr, persistenceErr)
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
