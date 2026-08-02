package query

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestRunFileSnapshotParallelSpillDifferential(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "query-file-fs-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	fs, err := durable.Create(file, durable.Options{
		Collection: store.Options{ChunkDocuments: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	set := &store.Segment{ShapeTapes: true, Postings: true}
	for i := range 448 {
		// The label is sized so queries 0 and 3 accumulate past maxSpillFanIn
		// runs against the smallest MemoryBytes the executor accepts, which is
		// the only way the bounded multi-level merge is exercised. It was
		// doubled when ownScalar stopped storing an unescaped string's content
		// twice: a retained row now genuinely costs about half what it did, so
		// the previous size no longer reached the fan-in bound.
		label := fmt.Sprintf("group-%03d-%s", i, strings.Repeat(string(rune('a'+i%26)), 2048))
		doc := []byte(fmt.Sprintf(`{"id":%d,"bucket":%d,"score":%d,"label":%q,"active":%t}`,
			i, i%17, i*3, label, i%3 != 0))
		if _, err := set.Append(doc); err != nil {
			t.Fatal(err)
		}
		if _, err := fs.Put([]byte(fmt.Sprintf("key-%04d", i)), doc); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	queries := []*Query{
		Select(Path("id"), Path("label")).Where(Cmp("active", Eq, true)).OrderBy("label", Desc).Limit(73),
		Select(Count(), Sum("score"), Avg("score"), Min("score"), Max("score")).Where(Cmp("bucket", Ge, 4)),
		Select(Path("bucket"), Count(), Sum("score"), Avg("score")).GroupBy("bucket").OrderBy("bucket", Desc),
		Select(Path("label"), Count(), Sum("score")).GroupBy("label").OrderBy("label", Asc).Limit(91),
		Select(Path("id")).Where(Cmp("id", Ge, 20)).Limit(19),
	}
	spillDir := t.TempDir()
	for i, q := range queries {
		want, err := q.Run(FromSegment(set))
		if err != nil {
			t.Fatalf("query %d baseline: %v", i, err)
		}
		e := Exec{Options: ExecOptions{
			Workers: 4, BatchRows: 11, BatchBytes: 4 << 10,
			MemoryBytes: 64 << 10, SpillDirectory: spillDir,
		}}
		if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
			t.Fatalf("query %d file execution: %v", i, err)
		}
		got, stats := e.Result, e.Stats
		if gotKey, wantKey := resultKey(got), resultKey(want); gotKey != wantKey {
			t.Fatalf("query %d mismatch:\n got: %s\nwant: %s", i, gotKey, wantKey)
		}
		if stats.RowsScanned != uint64(set.Len()) || stats.Batches < 2 || stats.Workers != 4 {
			t.Fatalf("query %d stats = %+v", i, stats)
		}
		if i == 0 || i == 3 {
			if stats.SpillRuns <= maxSpillFanIn || stats.SpilledBytes == 0 {
				t.Fatalf("query %d did not exercise bounded fan-in spill: %+v", i, stats)
			}
		}
	}
	entries, err := os.ReadDir(spillDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory retained %d files", len(entries))
	}
}

func TestRunFileSnapshotOptions(t *testing.T) {
	q := Select(Count())
	if err := q.RunInto(nil, FromFile(nil)); err == nil {
		t.Fatal("nil Exec accepted")
	}
	var e Exec
	if err := q.RunInto(&e, FromFile(nil)); err == nil {
		t.Fatal("nil snapshot accepted")
	}
	e.Options = ExecOptions{Workers: -1}
	if err := q.RunInto(&e, FromFile(nil)); err == nil {
		t.Fatal("negative worker count accepted")
	}
	e.Options = ExecOptions{MemoryBytes: 1024}
	if err := q.RunInto(&e, FromFile(nil)); err == nil {
		t.Fatal("undersized memory target accepted")
	}
}

func TestRunFileSnapshotPersistentCompoundIndexPushdown(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "query-file-index-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := durable.Options{
		Collection: store.Options{ChunkDocuments: 8, ShapeTapes: true},
		Indexes: []store.IndexDefinition{
			{Name: "tenant_country", Paths: []string{"/tenant", "/profile/geo/country"}},
			{Name: "tenant", Paths: []string{"/tenant"}},
			{Name: "country", Paths: []string{"/profile/geo/country"}},
		},
		Durability:       durable.DurabilityAsyncVisible,
		MaxDocumentBytes: 2048,
	}
	fs, err := durable.Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	set := &store.Segment{ShapeTapes: true, Postings: true}
	for i := range 512 {
		tenant := "other"
		if i%8 == 0 {
			tenant = "acme"
		}
		country := "US"
		if i%16 == 0 {
			country = "PT"
		}
		padding := ""
		if i%17 == 0 {
			padding = strings.Repeat("x", 900)
		}
		doc := []byte(fmt.Sprintf(
			`{"id":%d,"tenant":%q,"profile":{"geo":{"country":%q}},"padding":%q}`,
			i, tenant, country, padding,
		))
		if _, err := set.Append(doc); err != nil {
			t.Fatal(err)
		}
		if _, err := fs.Put([]byte(fmt.Sprintf("key-%04d", i)), doc); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	fs, err = durable.Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	run := func(q *Query) (Result, ExecStats) {
		t.Helper()
		want, err := q.Run(FromSegment(set))
		if err != nil {
			t.Fatal(err)
		}
		e := Exec{Options: ExecOptions{
			Workers: 3, BatchRows: 7, BatchBytes: 16 << 10, MemoryBytes: 1 << 20,
		}}
		if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
			t.Fatal(err)
		}
		if gotKey, wantKey := resultKey(e.Result), resultKey(want); gotKey != wantKey {
			t.Fatalf("indexed file result mismatch:\n got: %s\nwant: %s", gotKey, wantKey)
		}
		return e.Result, e.Stats
	}

	_, stats := run(
		Select(Path("id"), Path("profile.geo.country")).Where(And(
			Cmp("tenant", Eq, "acme"),
			Cmp("profile.geo.country", Eq, "PT"),
		)),
	)
	// CandidateChunks counts the succinct candidate groups the pushdown walked.
	// The ordered primary hash-distributes rows across those groups, so how many a
	// fixed candidate set falls across depends on the per-process hash seed and is
	// not a deterministic property of the query; the candidate and scanned row
	// counts are.
	if !stats.IndexBounded || stats.IndexLookups != 1 ||
		stats.RowsTotal != 512 || stats.CandidateRows != 32 ||
		stats.RowsScanned != 32 {
		t.Fatalf("compound pushdown stats = %+v", stats)
	}

	_, stats = run(
		Select(Count()).Where(And(
			Cmp("tenant", Eq, "absent"),
			Cmp("profile.geo.country", Eq, "PT"),
		)),
	)
	if !stats.IndexBounded || stats.IndexLookups != 1 ||
		stats.CandidateRows != 0 || stats.RowsScanned != 0 || stats.Batches != 0 {
		t.Fatalf("empty compound pushdown stats = %+v", stats)
	}

	result, stats := run(Select(Count()).Where(Cmp("tenant", Eq, "acme")))
	if result.RowCount != 1 || !countIs(result.Columns[0].Cells[0], 64) ||
		!stats.IndexBounded || stats.IndexLookups != 1 ||
		stats.CandidateRows != 64 ||
		stats.IndexPostingPages == 0 ||
		stats.IndexCertificateRows != 64 || stats.IndexRecheckRows != 0 ||
		stats.TokenFilterRows != 0 || stats.TokenFilterFallbackRows != 0 ||
		stats.RowsScanned != 0 || stats.Batches != 0 {
		t.Fatalf("direct exact count = result %s stats %+v", resultKey(result), stats)
	}

	result, stats = run(Select(Count()).Where(Contains("", `{"tenant":"acme"}`)))
	if result.RowCount != 1 || !countIs(result.Columns[0].Cells[0], 64) ||
		!stats.IndexBounded || stats.IndexLookups != 1 ||
		stats.CandidateRows != 64 || stats.IndexCertificateRows != 64 ||
		stats.IndexRecheckRows != 0 ||
		stats.RowsScanned != 0 || stats.Batches != 0 {
		t.Fatalf("direct root containment count = result %s stats %+v", resultKey(result), stats)
	}

	result, stats = run(Select(Count()).Where(Contains("profile.geo", `{"country":"PT"}`)))
	if result.RowCount != 1 || !countIs(result.Columns[0].Cells[0], 32) ||
		!stats.IndexBounded || stats.IndexLookups != 1 ||
		stats.CandidateRows != 32 || stats.IndexCertificateRows != 32 ||
		stats.IndexRecheckRows != 0 ||
		stats.RowsScanned != 0 || stats.Batches != 0 {
		t.Fatalf("direct nested containment count = result %s stats %+v", resultKey(result), stats)
	}

	result, stats = run(Select(Count()).Where(Contains(
		"", `{"tenant":"acme","profile":{"geo":{"country":"PT"}}}`,
	)))
	if result.RowCount != 1 || !countIs(result.Columns[0].Cells[0], 32) ||
		!stats.IndexBounded || stats.IndexLookups != 1 ||
		stats.CandidateRows != 32 || stats.IndexCertificateRows != 32 ||
		stats.IndexRecheckRows != 0 || stats.RowsScanned != 0 {
		t.Fatalf("compound containment count = result %s stats %+v", resultKey(result), stats)
	}

	result, stats = run(Select(Count()).Where(Contains(
		"", `{"tenant":"absent","tenant":"acme"}`,
	)))
	if result.RowCount != 1 || !countIs(result.Columns[0].Cells[0], 64) ||
		!stats.IndexBounded || stats.IndexLookups != 1 ||
		stats.IndexCertificateRows != 64 || stats.IndexRecheckRows != 0 ||
		stats.RowsScanned != 0 {
		t.Fatalf("last-wins containment count = result %s stats %+v", resultKey(result), stats)
	}

	_, stats = run(Select(Count()).Where(Contains(
		"", `{"profile":{"geo":{}}}`,
	)))
	if stats.IndexBounded || stats.IndexLookups != 0 ||
		stats.IndexCertificateRows != 0 || stats.IndexRecheckRows != 0 ||
		stats.RowsScanned != 512 {
		t.Fatalf("empty-object containment incorrectly flattened: %+v", stats)
	}

	_, stats = run(
		Select(Path("id")).Where(Or(
			Cmp("tenant", Eq, "acme"),
			Cmp("profile.geo.country", Eq, "PT"),
		)),
	)
	if !stats.IndexBounded || stats.IndexLookups != 2 ||
		stats.CandidateRows != 64 || stats.RowsScanned != 64 {
		t.Fatalf("bounded OR pushdown stats = %+v", stats)
	}

	_, stats = run(
		Select(Path("id")).Where(Or(
			Cmp("tenant", Eq, "acme"),
			Cmp("id", Ge, 500),
		)),
	)
	// One branch has an exact index and the other has none. A disjunction is only
	// bounded when every branch is; the chunk summaries that once bounded the
	// unindexed branch coarsely were removed with the chunk store, and the ordered
	// primary graph carries no equivalent per-chunk range, so the whole disjunction
	// falls back to a full scan. Correctness is unchanged — run() checks the result
	// against the heap oracle — only the pushdown is coarser.
	if stats.IndexBounded || stats.IndexLookups != 0 || stats.RowsScanned != 512 {
		t.Fatalf("mixed OR pushdown stats = %+v", stats)
	}

	_, stats = run(Select(Count()).Where(Not(Cmp("tenant", Eq, "acme"))))
	if stats.IndexBounded || stats.IndexLookups != 0 || stats.RowsScanned != 512 {
		t.Fatalf("durable NOT fallback stats = %+v", stats)
	}

	// An unindexed range once used chunk summaries to skip chunks whose value
	// range could not hold a match. The chunk store and its summaries were
	// deleted, and the ordered primary graph carries no per-chunk range, so an
	// unindexed predicate is a full scan. run() still checks the result against the
	// heap oracle, so only the pushdown breadth changed, not the answer.
	_, stats = run(Select(Path("id")).Where(Cmp("id", Ge, 500)))
	if stats.IndexBounded || stats.IndexLookups != 0 ||
		stats.RowsTotal != 512 || stats.RowsScanned != 512 {
		t.Fatalf("unindexed range pushdown stats = %+v", stats)
	}
}
