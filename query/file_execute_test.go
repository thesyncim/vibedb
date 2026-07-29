package query

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
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
		if _, err := fs.Put(fmt.Sprintf("key-%04d", i), doc); err != nil {
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

func TestRunFileSnapshotFloat64CoversDoNotAnswerExactAggregates(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "query-file-cover-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	fs, err := durable.Create(file, durable.Options{
		Collection:     store.Options{ChunkDocuments: 4},
		Float64Columns: []string{"/score", "/nested/value"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	set := &store.Segment{ShapeTapes: true}
	for row, document := range []string{
		`{"score":1.5,"nested":{"value":10}}`,
		`{"score":2,"nested":{"value":"text"}}`,
		`{"score":"text","nested":{"value":-3}}`,
		`{"other":7}`,
		`{"score":-1,"nested":{"value":8}}`,
	} {
		raw := []byte(document)
		if _, err := set.Append(raw); err != nil {
			t.Fatal(err)
		}
		if _, err := fs.Put(fmt.Sprintf("k%d", row), raw); err != nil {
			t.Fatal(err)
		}
	}
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
			Workers: 3, BatchRows: 2, MemoryBytes: 64 << 10,
		}}
		if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
			t.Fatal(err)
		}
		if gotKey, wantKey := resultKey(e.Result), resultKey(want); gotKey != wantKey {
			t.Fatalf("covering aggregate mismatch:\n got: %s\nwant: %s", gotKey, wantKey)
		}
		return e.Result, e.Stats
	}

	_, stats := run(Select(
		Count(), Sum("score"), Avg("score"), Min("score"), Max("score"),
		Sum("nested.value"), Max("nested.value"),
	))
	if stats.CoveringColumns != 0 || stats.RowsTotal != uint64(set.Len()) ||
		stats.RowsScanned != uint64(set.Len()) || stats.Batches == 0 {
		t.Fatalf("exact aggregate incorrectly trusted float64 cover: %+v", stats)
	}
	_, stats = run(Select(Sum("score"), Max("score")))
	if stats.CoveringColumns != 0 || stats.RowsScanned != uint64(set.Len()) {
		t.Fatalf("duplicate exact aggregate incorrectly trusted float64 cover: %+v", stats)
	}
	result, stats := run(Select(Sum("score")).Limit(0))
	if result.RowCount != 0 || stats.CoveringColumns != 0 || stats.RowsScanned != 0 {
		t.Fatalf("LIMIT 0 covering result = rows %d stats %+v", result.RowCount, stats)
	}

	// COUNT(path) includes present non-numeric values, and an unconfigured
	// numeric path has no cover. Both shapes must remain on the JSON executor.
	_, stats = run(Select(Count("score")))
	if stats.CoveringColumns != 0 || stats.RowsScanned != uint64(set.Len()) {
		t.Fatalf("COUNT(path) incorrectly used numeric cover: %+v", stats)
	}
	_, stats = run(Select(Sum("other")))
	if stats.CoveringColumns != 0 || stats.RowsScanned != uint64(set.Len()) {
		t.Fatalf("unconfigured SUM incorrectly used cover: %+v", stats)
	}
	_, stats = run(Select(Sum("score")).Where(Cmp("other", Ge, 0)))
	// The predicate is not one the cover can evaluate, so the aggregate must
	// stay on the JSON executor: CoveringColumns is the assertion. RowsScanned
	// is no longer the whole collection because the chunk-summary tier can now
	// narrow which chunks that executor reads on a durable snapshot, which
	// changes how much JSON is parsed and never whether the cover was legal.
	if stats.CoveringColumns != 0 || stats.RowsScanned == 0 {
		t.Fatalf("filtered SUM incorrectly used unfiltered cover: %+v", stats)
	}
}

func TestRunFileSnapshotIndexedScalarGroupsUseBoundedScan(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "query-file-groups-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	fs, err := durable.Create(file, durable.Options{
		Collection: store.Options{ChunkDocuments: 4},
		Indexes: []store.IndexDefinition{{
			Name: "kind", Paths: []string{"/kind"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	documents := []string{
		`{"kind":"a"}`,
		`{"kind":"b"}`,
		`{"kind":"\u0061"}`,
		`{"missing":true}`,
		`{"kind":null}`,
		`{"kind":{"nested":true}}`,
		`{"kind":1}`,
		`{"kind":1.0}`,
	}
	set := &store.Segment{ShapeTapes: true}
	for row, document := range documents {
		raw := []byte(document)
		if _, err := set.Append(raw); err != nil {
			t.Fatal(err)
		}
		if _, err := fs.Put(fmt.Sprintf("k%d", row), raw); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	q := Select(Path("kind"), Count()).GroupBy("kind").OrderBy("kind", Asc)
	want, err := q.Run(FromSegment(set))
	if err != nil {
		t.Fatal(err)
	}
	e := Exec{Options: ExecOptions{Workers: 3, MemoryBytes: 64 << 10}}
	if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	got, stats := e.Result, e.Stats
	if gotKey, wantKey := resultKey(got), resultKey(want); gotKey != wantKey {
		t.Fatalf("index-native groups mismatch:\n got: %s\nwant: %s", gotKey, wantKey)
	}
	if stats.RowsTotal != 8 || stats.RowsScanned != 8 ||
		stats.IndexLookups != 0 || stats.IndexPostingPages != 0 ||
		stats.IndexCertificateRows != 0 || stats.IndexRecheckRows != 0 ||
		stats.Batches == 0 || stats.BufferedBytes == 0 {
		t.Fatalf("bounded grouped-scan stats = %+v", stats)
	}

	// Result strings and raw projections are execution-owned, not aliases of
	// the reusable scan workspace.
	beforeReuse := resultKey(got)
	// A second Exec inherits the first's durable execution storage, so the next
	// execution overwrites that workspace. Its Result is separate, so any
	// surviving alias shows up as a changed first result.
	reuse := Exec{Options: ExecOptions{Workers: 1, MemoryBytes: 64 << 10}}
	reuse.file = e.file
	if err := q.RunInto(&reuse, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	if afterReuse := resultKey(got); afterReuse != beforeReuse {
		t.Fatalf("result changed after workspace reuse:\n got: %s\nwant: %s", afterReuse, beforeReuse)
	}
}

func TestRunFileSnapshotIndexedCatalogScalarGroupsUseBoundedScan(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{
		ChunkDocuments: 4, ShapeTapes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	set := &store.Segment{ShapeTapes: true}
	documents := []string{
		`{"profile":{"kind":"a"}}`,
		`{"profile":{"kind":"\u0061"}}`,
		`{"missing":true}`,
		`{"profile":{"kind":null}}`,
		`{"profile":{"kind":1}}`,
		`{"profile":{"kind":1.0}}`,
	}
	for row, document := range documents {
		raw := []byte(document)
		if err := builder.Append(fmt.Sprintf("k%d", row), raw); err != nil {
			t.Fatal(err)
		}
		if _, err := set.Append(raw); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "query-file-group-catalog-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := durable.Options{
		Collection: store.Options{ChunkDocuments: 4, ShapeTapes: true},
		Indexes: []store.IndexDefinition{{
			Name: "kind", Paths: []string{"/profile/kind"},
		}},
		PageSize: 4096, MaxPageSize: 64 << 10,
	}
	if _, err := durable.CreateFrom(source, file, options); err != nil {
		t.Fatal(err)
	}
	fs, err := durable.Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	q := Select(Path("profile.kind"), Count()).
		GroupBy("profile.kind").
		OrderBy("profile.kind", Asc)
	want, err := q.Run(FromSegment(set))
	if err != nil {
		t.Fatal(err)
	}
	e := Exec{Options: ExecOptions{Workers: 4, MemoryBytes: 64 << 10}}
	if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	got, stats := e.Result, e.Stats
	if gotKey, wantKey := resultKey(got), resultKey(want); gotKey != wantKey {
		t.Fatalf("index catalog groups mismatch:\n got: %s\nwant: %s", gotKey, wantKey)
	}
	if stats.RowsTotal != 6 || stats.RowsScanned != 6 ||
		stats.IndexLookups != 0 || stats.IndexPostingPages != 0 ||
		stats.IndexCertificateRows != 0 || stats.IndexRecheckRows != 0 ||
		stats.Batches == 0 || stats.BufferedBytes == 0 {
		t.Fatalf("bounded catalog grouped-scan stats = %+v", stats)
	}
	reusable := Exec{Options: ExecOptions{Workers: 4, MemoryBytes: 64 << 10}}
	if err := q.RunInto(&reusable, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if err := q.RunInto(&reusable, FromFile(snapshot)); err != nil ||
			reusable.Result.RowCount != 3 {
			panic("reusable catalog grouped scan")
		}
	})
	if allocs != 0 {
		t.Fatalf(
			"reusable catalog grouped-scan allocations = %.2f, want zero",
			allocs,
		)
	}
	if gotKey, wantKey := resultKey(reusable.Result), resultKey(want); gotKey != wantKey {
		t.Fatalf(
			"reusable catalog grouped-scan mismatch:\n got: %s\nwant: %s",
			gotKey, wantKey,
		)
	}
	if reusable.Stats.RowsScanned != 6 ||
		reusable.Stats.IndexPostingPages != 0 {
		t.Fatalf("reusable catalog grouped-scan stats = %+v", reusable.Stats)
	}
	reusable.Release()
	if reusable.Result.RowCount != 0 || reusable.Result.Columns != nil ||
		reusable.Stats != (ExecStats{}) {
		t.Fatalf("released exec = %+v", reusable.Result)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}

	// A non-first scalar mutation transactionally rewrites the index catalog.
	// Group execution remains exact when that indexed generation is read
	// through the work-memory-bounded scan path.
	mutated := []byte(`{"profile":{"kind":"b"}}`)
	if created, err := fs.Put("k1", mutated); err != nil || created {
		t.Fatalf("mutate covered group = (%v,%v)", created, err)
	}
	mutatedSet := &store.Segment{ShapeTapes: true}
	for row, document := range documents {
		raw := []byte(document)
		if row == 1 {
			raw = mutated
		}
		if _, err := mutatedSet.Append(raw); err != nil {
			t.Fatal(err)
		}
	}
	current, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	want, err = q.Run(FromSegment(mutatedSet))
	if err != nil {
		t.Fatal(err)
	}
	e = Exec{Options: ExecOptions{Workers: 4, MemoryBytes: 64 << 10}}
	if err := q.RunInto(&e, FromFile(current)); err != nil {
		t.Fatal(err)
	}
	got, stats = e.Result, e.Stats
	if gotKey, wantKey := resultKey(got), resultKey(want); gotKey != wantKey {
		t.Fatalf(
			"incremental catalog groups mismatch:\n got: %s\nwant: %s",
			gotKey, wantKey,
		)
	}
	if stats.RowsTotal != 6 || stats.RowsScanned != 6 ||
		stats.IndexLookups != 0 || stats.IndexPostingPages != 0 ||
		stats.IndexCertificateRows != 0 || stats.IndexRecheckRows != 0 ||
		stats.Batches == 0 {
		t.Fatalf("incremental catalog grouped-scan stats = %+v", stats)
	}
}

func TestRunFileSnapshotHighCardinalityIndexedGroupsSpillAtWorkMemory(t *testing.T) {
	const documents = 256
	builder, err := store.NewBuilder(store.Options{
		ChunkDocuments: 8, ShapeTapes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for row := range documents {
		if err := builder.Append(
			fmt.Sprintf("k%03d", row),
			fmt.Appendf(nil, `{"kind":"value-%03d"}`, row),
		); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(
		t.TempDir(), "query-file-index-segmented-groups-*",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := durable.Options{
		Collection: store.Options{
			ChunkDocuments: 8, ShapeTapes: true,
		},
		Indexes: []store.IndexDefinition{{
			Name: "kind", Paths: []string{"/kind"},
		}},
		PageSize: 4096, MaxPageSize: 4096,
		MaxKeyBytes: 32, InlineValueBytes: 128,
		MaxDocumentBytes: 1024,
	}
	if _, err := durable.CreateFrom(source, file, options); err != nil {
		t.Fatal(err)
	}
	fs, err := durable.Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	q := Select(Path("kind"), Count()).
		GroupBy("kind").
		OrderBy("kind", Asc)
	const workMemory = int64(64 << 10)
	e := Exec{Options: ExecOptions{Workers: 1, MemoryBytes: workMemory}}
	if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	if e.Result.RowCount != documents ||
		e.Stats.RowsScanned != documents ||
		e.Stats.Batches == 0 ||
		e.Stats.BufferedBytes > workMemory ||
		e.Stats.SpillRuns == 0 ||
		e.Stats.SpilledBytes == 0 ||
		e.Stats.IndexLookups != 0 ||
		e.Stats.IndexPostingPages != 0 ||
		e.Stats.IndexCertificateRows != 0 ||
		e.Stats.IndexRecheckRows != 0 {
		t.Fatalf(
			"bounded high-cardinality query = rows %d stats %+v",
			e.Result.RowCount, e.Stats,
		)
	}
	first, ok := e.Result.Columns[0].Cells[0].Text()
	if !ok || first != "value-000" {
		t.Fatalf("segmented first group = (%q,%v)", first, ok)
	}
	last, ok := e.Result.Columns[0].Cells[documents-1].Text()
	if !ok || last != "value-255" {
		t.Fatalf("segmented last group = (%q,%v)", last, ok)
	}
}

func TestRunFileSnapshotIndexPlanningRespectsWorkMemory(t *testing.T) {
	const (
		documents = 256
		matches   = documents / 16
	)
	builder, err := store.NewBuilder(store.Options{
		ChunkDocuments: 1, ShapeTapes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for row := range documents {
		kind := "miss"
		if row%16 == 0 {
			kind = "hit"
		}
		if err := builder.Append(
			fmt.Sprintf("k%03d", row),
			fmt.Appendf(nil, `{"id":%d,"kind":%q}`, row, kind),
		); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "query-file-index-bound-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := durable.Options{
		Collection: store.Options{
			ChunkDocuments: 1, ShapeTapes: true,
		},
		Indexes: []store.IndexDefinition{{
			Name: "kind", Paths: []string{"/kind"},
		}},
		PageSize: 4096, MaxPageSize: 4096,
		MaxKeyBytes: 32, InlineValueBytes: 128,
		MaxDocumentBytes: 1024,
	}
	if _, err := durable.CreateFrom(source, file, options); err != nil {
		t.Fatal(err)
	}
	fs, err := durable.Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	projection := Select(Path("id")).Where(Cmp("kind", Eq, "hit"))
	compiled, err := projection.compiled()
	if err != nil {
		t.Fatal(err)
	}
	bound, err := snapshot.IndexProbeMemoryBound()
	if err != nil {
		t.Fatal(err)
	}
	candidateBytes := compiled.fileIndexPlanMemoryBytes(
		bound, bound.CandidateWorkspaceBytes,
	)
	if candidateBytes <= minimumFileMemory/2 {
		t.Fatalf(
			"fixture candidate bound = %d, want above tight planner share",
			candidateBytes,
		)
	}

	// One byte below the derived bound declines before the catalog, mask
	// buffers, or durable probe workspace grows. Equality admits the exact same
	// plan and proves the comparison is not an off-by-one rejection.
	var declined Workspace
	var declinedIndex durable.IndexWorkspace
	masks, planned, err := compiled.fileCandidateMasksBounded(
		snapshot, &declinedIndex, &declined, candidateBytes-1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if masks != nil || planned != 0 ||
		len(declined.storeIndexes) != 0 ||
		len(declined.storeMasks) != 0 ||
		declinedIndex.LastProbeStats() != (durable.IndexProbeStats{}) {
		t.Fatalf(
			"below-bound candidate plan grew workspace: masks=%d planned=%d "+
				"indexes=%d buffers=%d probe=%+v",
			len(masks), planned, len(declined.storeIndexes),
			len(declined.storeMasks), declinedIndex.LastProbeStats(),
		)
	}
	var admitted Workspace
	var admittedIndex durable.IndexWorkspace
	masks, planned, err = compiled.fileCandidateMasksBounded(
		snapshot, &admittedIndex, &admitted, candidateBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if planned != candidateBytes || len(masks) != matches ||
		admitted.storeIndexProbes != 1 {
		t.Fatalf(
			"boundary candidate plan = masks %d planned %d probes %d",
			len(masks), planned, admitted.storeIndexProbes,
		)
	}

	exactBytes := compiled.fileIndexPlanMemoryBytes(
		bound, bound.ExactSingleWorkspaceBytes,
	)
	var exactDeclined Workspace
	var exactDeclinedIndex durable.IndexWorkspace
	exactMasks, _, _, _, exact, err :=
		compiled.fileExactCandidateMasksBounded(
			snapshot, &exactDeclinedIndex, &exactDeclined, exactBytes-1,
		)
	if err != nil {
		t.Fatal(err)
	}
	if exact || exactMasks != nil ||
		len(exactDeclined.storeMasks) != 0 ||
		exactDeclinedIndex.LastProbeStats() != (durable.IndexProbeStats{}) {
		t.Fatalf(
			"below-bound exact plan grew probe workspace: exact=%v masks=%d "+
				"buffers=%d probe=%+v",
			exact, len(exactMasks), len(exactDeclined.storeMasks),
			exactDeclinedIndex.LastProbeStats(),
		)
	}
	var exactAdmitted Workspace
	var exactAdmittedIndex durable.IndexWorkspace
	exactMasks, _, _, _, exact, err =
		compiled.fileExactCandidateMasksBounded(
			snapshot, &exactAdmittedIndex, &exactAdmitted, exactBytes,
		)
	if err != nil {
		t.Fatal(err)
	}
	if !exact || len(exactMasks) != matches ||
		exactAdmitted.storeIndexProbes != 1 {
		t.Fatalf(
			"boundary exact plan = exact %v masks %d probes %d",
			exact, len(exactMasks), exactAdmitted.storeIndexProbes,
		)
	}

	// At the minimum public work-memory setting the conservative plan does not
	// fit its half-budget. Both ordinary candidate pushdown and direct COUNT
	// therefore decline to the same exact full scan, with honest zero index
	// statistics and no mask/probe growth.
	projectExec := Exec{Options: ExecOptions{
		Workers: 1, MemoryBytes: minimumFileMemory,
	}}
	if err := projection.RunInto(&projectExec, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	if projectExec.Result.RowCount != matches ||
		projectExec.Stats.RowsScanned != documents ||
		projectExec.Stats.IndexBounded ||
		projectExec.Stats.IndexLookups != 0 ||
		projectExec.Stats.Batches == 0 ||
		len(projectExec.Workspace.storeMasks) != 0 ||
		projectExec.file.index.LastProbeStats() != (durable.IndexProbeStats{}) {
		t.Fatalf(
			"tight projection = rows %d stats %+v buffers %d probe %+v",
			projectExec.Result.RowCount, projectExec.Stats,
			len(projectExec.Workspace.storeMasks),
			projectExec.file.index.LastProbeStats(),
		)
	}

	countExec := Exec{Options: ExecOptions{
		Workers: 1, MemoryBytes: minimumFileMemory,
	}}
	countQuery := Select(Count()).Where(Cmp("kind", Eq, "hit"))
	if err := countQuery.RunInto(&countExec, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	if countExec.Result.RowCount != 1 ||
		!countIs(countExec.Result.Columns[0].Cells[0], matches) ||
		countExec.Stats.RowsScanned != documents ||
		countExec.Stats.IndexBounded ||
		countExec.Stats.IndexLookups != 0 ||
		countExec.Stats.Batches == 0 ||
		len(countExec.Workspace.storeMasks) != 0 ||
		countExec.file.index.LastProbeStats() != (durable.IndexProbeStats{}) {
		t.Fatalf(
			"tight direct count fallback = result %s stats %+v buffers %d probe %+v",
			resultKey(countExec.Result), countExec.Stats,
			len(countExec.Workspace.storeMasks),
			countExec.file.index.LastProbeStats(),
		)
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
		if _, err := fs.Put(fmt.Sprintf("key-%04d", i), doc); err != nil {
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
	if !stats.IndexBounded || stats.IndexLookups != 1 ||
		stats.RowsTotal != 512 || stats.CandidateRows != 32 ||
		stats.RowsScanned != 32 || stats.CandidateChunks != 32 {
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
		stats.CandidateRows != 64 || stats.CandidateChunks != 64 ||
		stats.IndexPostingPages == 0 ||
		stats.IndexCertificateRows != 64 || stats.IndexRecheckRows != 0 ||
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
		stats.CandidateRows != 64 || stats.RowsScanned != 64 || stats.CandidateChunks != 64 {
		t.Fatalf("bounded OR pushdown stats = %+v", stats)
	}

	_, stats = run(
		Select(Path("id")).Where(Or(
			Cmp("tenant", Eq, "acme"),
			Cmp("id", Ge, 500),
		)),
	)
	// One branch has an exact index and the other has none. Before chunk
	// summaries reached this backend the whole disjunction fell back to a full
	// scan, because a disjunction is only bounded when every branch is. The
	// summaries bound the unindexed branch — coarsely, at chunk granularity —
	// so the union is now bounded and the scan reads the chunks that survive
	// either branch instead of all of them.
	if !stats.IndexBounded || stats.IndexLookups != 1 ||
		stats.RowsScanned >= 512 || stats.RowsScanned == 0 {
		t.Fatalf("mixed OR pushdown stats = %+v", stats)
	}

	_, stats = run(Select(Count()).Where(Not(Cmp("tenant", Eq, "acme"))))
	if stats.IndexBounded || stats.IndexLookups != 0 || stats.RowsScanned != 512 {
		t.Fatalf("durable NOT fallback stats = %+v", stats)
	}

	// An unindexed range over a monotonically written column is the shape chunk
	// summaries answer best: no index probe happens, and the scan reads only
	// the chunks whose value range can hold a match.
	_, stats = run(Select(Path("id")).Where(Cmp("id", Ge, 500)))
	if !stats.IndexBounded || stats.IndexLookups != 0 ||
		stats.RowsTotal != 512 || stats.RowsScanned >= 512 || stats.RowsScanned == 0 {
		t.Fatalf("unindexed chunk-summary pushdown stats = %+v", stats)
	}
	// With the summaries switched off the same query is the full scan it always
	// was, which is what proves the two assertions above are about pruning and
	// not about some other plan change.
	previousPruning := store.SetZonePruning(false)
	_, stats = run(Select(Path("id")).Where(Cmp("id", Ge, 500)))
	store.SetZonePruning(previousPruning)
	if stats.IndexBounded || stats.IndexLookups != 0 ||
		stats.RowsTotal != 512 || stats.RowsScanned != 512 {
		t.Fatalf("unindexed fallback stats = %+v", stats)
	}
}

func TestRunFileSnapshotIndexCorruptionFailsClosed(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "query-file-index-corrupt-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := durable.Options{
		Collection: store.Options{ChunkDocuments: 8},
		Indexes: []store.IndexDefinition{{
			Name: "status", Paths: []string{"/status"},
		}},
	}
	fs, err := durable.Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 16 {
		if _, err := fs.Put(
			fmt.Sprintf("key-%02d", i),
			[]byte(fmt.Sprintf(`{"id":%d,"status":"active"}`, i)),
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}

	var rootScratch [64 << 10]byte
	recovery, err := storeio.RecoverMutableInlineStateRoot(
		file, 4096, 0, rootScratch[:],
	)
	if err != nil {
		t.Fatal(err)
	}
	super, root := recovery.Root, recovery.State
	ref := root.IndexDirectory
	var leaf storeio.PageRef
	for {
		page := make([]byte, ref.Length)
		if _, err := file.ReadAt(page, int64(ref.Offset)); err != nil {
			t.Fatal(err)
		}
		view, err := storeio.OpenIndexDirectoryPage(
			page, super.FileEnd, root.NextLogicalID, root.IndexCount,
		)
		if err != nil {
			t.Fatal(err)
		}
		if view.Header().Level == 0 {
			if _, ok := view.EntryAt(0); !ok {
				t.Fatal("current index leaf is empty")
			}
			leaf = ref
			break
		}
		child, ok := view.ChildAt(0)
		if !ok {
			t.Fatal("current index branch is empty")
		}
		ref = child.Ref
	}
	corrupt := make([]byte, leaf.Length)
	if _, err := file.ReadAt(corrupt, int64(leaf.Offset)); err != nil {
		t.Fatal(err)
	}
	certificate := []byte(`"active"`)
	position := bytes.Index(corrupt, certificate)
	if position < 0 {
		t.Fatal("routing certificate is absent")
	}
	copy(corrupt[position:], `xxxxxxxx`)
	if _, err := storeio.SealPage(corrupt); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(corrupt, int64(leaf.Offset)); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
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
	e := Exec{Options: ExecOptions{Workers: 1}}
	err = Select(Count()).Where(Cmp("status", Eq, "active")).RunInto(&e, FromFile(snapshot))
	if !errors.Is(err, storeio.ErrIndexDirectoryCorrupt) {
		t.Fatalf("corrupt index query error = %v, want %v", err, storeio.ErrIndexDirectoryCorrupt)
	}
	if e.Stats.RowsScanned != 0 {
		t.Fatalf("corrupt index silently scanned %d rows", e.Stats.RowsScanned)
	}
}
