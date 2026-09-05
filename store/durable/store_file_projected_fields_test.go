package durable

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

// primaryProjectionTestPaths are RFC 6901 paths used by the projection
// fixture. The id field deliberately differs from the physical primary key so
// a projection that accidentally returns the encoded key is observable.
var primaryProjectionTestPaths = []string{
	"/id",
	"/name",
	"/wide",
	"/decimal",
	"/active",
	"/nullable",
	"/slash~1key",
	"/tilde~0key",
}

type primaryProjectionTestRow struct {
	key    string
	doc    []byte
	values map[string][]byte
}

type primaryProjectionTestFixture struct {
	path       string
	file       *os.File
	collection *Collection
	rows       []primaryProjectionTestRow
}

// primaryProjectionTestDocument intentionally varies key order, whitespace,
// and string escaping while retaining the same selected scalar paths. The
// variation is interleaved in physical order, so a shape-major traversal can
// be detected by comparing the row ordinal with the detached oracle.
func primaryProjectionTestDocument(row int) []byte {
	id := fmt.Sprintf("logical-%05d", row)
	name := fmt.Sprintf("name-%05d", row)
	wide := int64(1<<53) + int64(row)
	if row&1 != 0 {
		wide = -wide
	}
	decimal := []string{"1.50", "-0.25", "9007199254740993.000", "3e-2"}[row&3]
	active := row&1 == 0
	slash := fmt.Sprintf("slash-value-%d", row)
	tilde := fmt.Sprintf("tilde-value-%d", row)
	switch row & 3 {
	case 0:
		return fmt.Appendf(nil,
			`{"id":%q,"name":%q,"wide":%d,"decimal":%s,"active":%t,"nullable":null,"slash/key":%q,"tilde~key":%q}`,
			id, name, wide, decimal, active, slash, tilde,
		)
	case 1:
		// The escaped string includes a quote, slash, backslash, and newline;
		// JSON marshaling here keeps the source valid while retaining a string
		// value that exercises the decoder's escaped-string path.
		escaped := fmt.Sprintf("escaped-%05d \" / \\\\ \\n", row)
		return fmt.Appendf(nil,
			`{ "decimal" : %s, "active" : %t, "wide" : %d, "name" : %q, "id" : %q, "slash/key" : %q, "tilde~key" : %q, "nullable" : null }`,
			decimal, active, wide, escaped, id, slash, tilde,
		)
	case 2:
		// Missing and null are intentionally distinct. The missing nullable
		// value is omitted, while all other requested fields remain scalar.
		return fmt.Appendf(nil,
			`{"tilde~key":%q,"slash/key":%q,"active":%t,"decimal":%s,"wide":%d,"name":%q,"id":%q}`,
			tilde, slash, active, decimal, wide, name, id,
		)
	default:
		return fmt.Appendf(nil,
			`{"name":%q,"id":%q,"nullable":null,"active":%t,"decimal":%s,"wide":%d,"tilde~key":%q,"slash/key":%q}`,
			name, id, active, decimal, wide, tilde, slash,
		)
	}
}

// newPrimaryProjectionTestFixture builds enough rows to cross the compact
// stripe boundary while keeping every ordinary document small. The caller
// may replace the final row after the snapshot used for the native pass has
// been acquired to exercise a late fallback.
func newPrimaryProjectionTestFixture(
	t *testing.T,
	rows int,
	options Options,
) *primaryProjectionTestFixture {
	t.Helper()
	if rows <= 0 {
		t.Fatal("projection fixture requires rows")
	}
	if options.Collection.ChunkDocuments == 0 {
		options.Collection.ChunkDocuments = 64
	}
	keys := make([]string, rows)
	docs := make([][]byte, rows)
	for row := range rows {
		keys[row] = fmt.Sprintf("physical-%08d", row)
		docs[row] = primaryProjectionTestDocument(row)
	}
	file, err := os.CreateTemp(t.TempDir(), "projected-fields-*.vibe")
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	source := &store.Collection{}
	for row := range rows {
		if _, err := source.Put(keys[row], docs[row]); err != nil {
			_ = file.Close()
			t.Fatalf("source Put(%s): %v", keys[row], err)
		}
	}
	if _, err := CreateFromPrimary(source, file, options); err != nil {
		_ = file.Close()
		t.Fatalf("CreateFromPrimary: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatalf("Open: %v", err)
	}

	canonical := canonicalDocs(t, docs)
	want := make([]primaryProjectionTestRow, rows)
	var resolver storeio.UnifiedHoleResolver
	for row := range rows {
		values := make(map[string][]byte, len(primaryProjectionTestPaths))
		for _, path := range primaryProjectionTestPaths {
			if err := resolver.SetPath([]byte(path)); err != nil {
				t.Fatalf("resolver.SetPath(%q): %v", path, err)
			}
			start, end, found, err := resolver.PathSpanOf(canonical[row])
			if err != nil {
				t.Fatalf("PathSpanOf(%q,row=%d): %v", path, row, err)
			}
			if found {
				values[path] = bytes.Clone(canonical[row][start:end])
			}
		}
		want[row] = primaryProjectionTestRow{
			key: keys[row], doc: bytes.Clone(canonical[row]), values: values,
		}
	}

	fixture := &primaryProjectionTestFixture{
		path: path, file: file, collection: collection, rows: want,
	}
	t.Cleanup(func() {
		_ = fixture.collection.Close()
		_ = fixture.file.Close()
	})
	return fixture
}

func (f *primaryProjectionTestFixture) compactLeafCount(t *testing.T) int {
	t.Helper()
	counts := primaryLeafClassCounts(t, f.collection)
	return counts[storeio.CommonPrimaryLeafCompact]
}

func primaryProjectionTestOverflowDocument(row int) []byte {
	long := bytes.Repeat([]byte("overflow projection payload "), 80)
	return fmt.Appendf(nil,
		`{"id":"logical-%05d","name":%q,"wide":%d,"decimal":1.50,"active":true,"nullable":null,"slash/key":"x","tilde~key":"y"}`,
		row, string(long), int64(1<<53)+int64(row),
	)
}

func primaryProjectionTestContainerDocument(row int) []byte {
	return fmt.Appendf(nil,
		`{"id":"logical-%05d","name":"container","wide":{"value":%d},"decimal":1.50,"active":true,"nullable":null,"slash/key":"x","tilde~key":"y"}`,
		row, row,
	)
}

func primaryProjectionTestExpectedRows(
	rows []primaryProjectionTestRow,
	paths []string,
) [][]byte {
	values := make([][]byte, 0, len(rows)*len(paths))
	for _, row := range rows {
		for _, path := range paths {
			values = append(values, row.values[path])
		}
	}
	return values
}

func primaryProjectionTestRowFromDocument(
	t testing.TB,
	key string,
	doc []byte,
) primaryProjectionTestRow {
	t.Helper()
	canonical, err := vibejson.AppendCanonicalize(nil, doc)
	if err != nil {
		t.Fatal(err)
	}
	values := make(map[string][]byte, len(primaryProjectionTestPaths))
	var resolver storeio.UnifiedHoleResolver
	for _, path := range primaryProjectionTestPaths {
		if err := resolver.SetPath([]byte(path)); err != nil {
			t.Fatal(err)
		}
		start, end, found, err := resolver.PathSpanOf(canonical)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			values[path] = bytes.Clone(canonical[start:end])
		}
	}
	return primaryProjectionTestRow{key: key, doc: bytes.Clone(canonical), values: values}
}

var primaryProjectionNativePaths = []string{
	"/id",
	"/name",
	"/wide",
	"/decimal",
	"/active",
	"/slash~1key",
	"/tilde~0key",
}

type primaryProjectionObservedRow struct {
	row    uint64
	values [][]byte
}

type primaryProjectionWorkspace struct {
	shapeSeen  []int
	shapeWork  []storeio.UnifiedProjectionShapeWorkspace
	streamWork []storeio.UnifiedProjectionStreamWorkspace
	fields     []storeio.UnifiedProjectionField
	scratch    []byte
}

func newPrimaryProjectionWorkspace(fieldCount int) *primaryProjectionWorkspace {
	return &primaryProjectionWorkspace{
		shapeSeen:  make([]int, storeio.CompactPrimaryStripeMaxRows),
		shapeWork:  make([]storeio.UnifiedProjectionShapeWorkspace, storeio.CompactPrimaryStripeMaxRows),
		streamWork: make([]storeio.UnifiedProjectionStreamWorkspace, storeio.CompactPrimaryStripeMaxRows*fieldCount),
		fields:     make([]storeio.UnifiedProjectionField, fieldCount),
		scratch:    make([]byte, 0, 16<<10),
	}
}

func collectPrimaryProjection(
	t *testing.T,
	snapshot *Snapshot,
	filter *ProjectionFilter,
	workspace *primaryProjectionWorkspace,
	lower, upper []byte,
	lowerExclusive bool,
	limit int,
) (ProjectedRangeResult, error, []primaryProjectionObservedRow) {
	t.Helper()
	var observed []primaryProjectionObservedRow
	result, scratch, err := snapshot.FilterProjectedRangeWithScratch(
		filter, lower, upper, lowerExclusive,
		workspace.shapeSeen, workspace.shapeWork, workspace.streamWork,
		workspace.fields, workspace.scratch, limit,
		func(row uint64, fields []storeio.UnifiedProjectionField) error {
			values := make([][]byte, len(fields))
			for i := range fields {
				values[i] = bytes.Clone(fields[i].AppendJSON(nil))
			}
			observed = append(observed, primaryProjectionObservedRow{row: row, values: values})
			return nil
		},
	)
	workspace.scratch = scratch
	return result, err, observed
}

func assertPrimaryProjectionRows(
	t testing.TB,
	want []primaryProjectionTestRow,
	paths []string,
	got []primaryProjectionObservedRow,
	start int,
) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("projected callback rows=%d, want %d", len(got), len(want))
	}
	for at, observed := range got {
		if observed.row != uint64(at) {
			t.Fatalf("projected callback row=%d at position %d, want %d", observed.row, at, at)
		}
		row := want[at]
		if len(observed.values) != len(paths) {
			t.Fatalf("projected row %d fields=%d, want %d", at, len(observed.values), len(paths))
		}
		for field, path := range paths {
			wantValue := row.values[path]
			if !bytes.Equal(observed.values[field], wantValue) {
				t.Fatalf("projected row %d path %s=%q, want %q", start+at, path,
					observed.values[field], wantValue)
			}
		}
	}
}

func collectPrimaryProjectionGeneric(
	t testing.TB,
	snapshot *Snapshot,
	rows []primaryProjectionTestRow,
	paths []string,
) [][]byte {
	t.Helper()
	probes := make([]*FieldProbe, len(paths))
	for i, path := range paths {
		probe, err := NewFieldProbe(path)
		if err != nil {
			t.Fatal(err)
		}
		probes[i] = probe
	}
	got := make([][]byte, 0, len(rows)*len(paths))
	for _, row := range rows {
		for _, probe := range probes {
			value, found, err := snapshot.AppendField(nil, probe, []byte(row.key))
			if err != nil {
				t.Fatal(err)
			}
			if found {
				got = append(got, bytes.Clone(value))
			} else {
				got = append(got, nil)
			}
		}
	}
	return got
}

func TestSnapshotFilterProjectedRangePreservesScalarFieldsAcrossLeaves(t *testing.T) {
	const rows = storeio.CompactPrimaryStripeMaxRows + 128
	fixture := newPrimaryProjectionTestFixture(t, rows, Options{
		Collection: store.Options{ChunkDocuments: 64},
	})
	if got := fixture.compactLeafCount(t); got < 2 {
		t.Fatalf("compact leaves=%d, want at least two", got)
	}
	snapshot, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	filter, err := NewProjectionFilter(primaryProjectionNativePaths)
	if err != nil {
		t.Fatal(err)
	}
	workspace := newPrimaryProjectionWorkspace(len(primaryProjectionNativePaths))
	result, err, observed := collectPrimaryProjection(
		t, snapshot, filter, workspace, nil, nil, false, -1,
	)
	if err != nil {
		t.Fatalf("FilterProjectedRangeWithScratch: %v", err)
	}
	if !result.Supported || result.Scanned != rows || result.Stopped {
		t.Fatalf("projected result=%+v, want supported scan of %d rows", result, rows)
	}
	assertPrimaryProjectionRows(t, fixture.rows, primaryProjectionNativePaths, observed, 0)

	// A bounded range and LIMIT must retain the same physical pairing while
	// reusing all caller-owned workspace and value scratch.
	const first, last = 4070, 4120
	result, err, observed = collectPrimaryProjection(
		t, snapshot, filter, workspace,
		[]byte(fixture.rows[first].key), []byte(fixture.rows[last].key), false, -1,
	)
	if err != nil || !result.Supported || result.Scanned != last-first {
		t.Fatalf("bounded projected result=%+v err=%v, want %d rows", result, err, last-first)
	}
	assertPrimaryProjectionRows(t, fixture.rows[first:last], primaryProjectionNativePaths, observed, first)
	result, err, observed = collectPrimaryProjection(
		t, snapshot, filter, workspace, nil, nil, false, 1,
	)
	if err != nil || !result.Supported || !result.Stopped || result.Scanned != 1 {
		t.Fatalf("LIMIT 1 projected result=%+v err=%v", result, err)
	}
	assertPrimaryProjectionRows(t, fixture.rows[:1], primaryProjectionNativePaths, observed, 0)
	result, err, observed = collectPrimaryProjection(
		t, snapshot, filter, workspace, nil, nil, false, 0,
	)
	if err != nil || !result.Supported || !result.Stopped || result.Scanned != 0 || len(observed) != 0 {
		t.Fatalf("LIMIT 0 projected result=%+v err=%v observed=%d", result, err, len(observed))
	}
	result, err, observed = collectPrimaryProjection(
		t, snapshot, filter, workspace,
		[]byte(fixture.rows[10].key), []byte(fixture.rows[10].key), false, -1,
	)
	if err != nil || !result.Supported || result.Stopped || result.Scanned != 0 || len(observed) != 0 {
		t.Fatalf("empty projected range result=%+v err=%v observed=%d", result, err, len(observed))
	}
}

func TestSnapshotFilterProjectedRangeDeclinesUndersizedScratch(t *testing.T) {
	fixture := newPrimaryProjectionTestFixture(t, 64, Options{
		Collection: store.Options{ChunkDocuments: 64},
	})
	snapshot, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	filter, err := NewProjectionFilter(primaryProjectionNativePaths[:2])
	if err != nil {
		t.Fatal(err)
	}
	workspace := newPrimaryProjectionWorkspace(2)
	tiny := make([]byte, 0, 1)
	callbacks := 0
	result, returned, err := snapshot.FilterProjectedRangeWithScratch(
		filter, nil, nil, false,
		workspace.shapeSeen, workspace.shapeWork, workspace.streamWork,
		workspace.fields, tiny, -1,
		func(uint64, []storeio.UnifiedProjectionField) error {
			callbacks++
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Supported || result.Scanned != 0 || result.Stopped {
		t.Fatalf("undersized scratch result=%+v, want declined zero progress", result)
	}
	if callbacks != 0 {
		t.Fatalf("undersized scratch callbacks=%d, want no borrowed rows", callbacks)
	}
	if cap(returned) != cap(tiny) {
		t.Fatalf("undersized scratch grew capacity from %d to %d", cap(tiny), cap(returned))
	}
}

func TestSnapshotFilterProjectedRangeDeclinesLateContainerAndOverflowAtomically(t *testing.T) {
	const rows = storeio.CompactPrimaryStripeMaxRows + 64
	cases := []struct {
		name string
		doc  func(row int) []byte
	}{
		{name: "container", doc: primaryProjectionTestContainerDocument},
		{name: "overflow", doc: primaryProjectionTestOverflowDocument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newPrimaryProjectionTestFixture(t, rows, Options{
				Collection: store.Options{ChunkDocuments: 64}, InlineValueBytes: 256,
			})
			last := rows - 1
			updated := tc.doc(last)
			created, err := fixture.collection.Put([]byte(fixture.rows[last].key), updated)
			if err != nil || created {
				t.Fatalf("late unsupported replacement Put=%t,%v", created, err)
			}
			snapshot, err := fixture.collection.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			filter, err := NewProjectionFilter(primaryProjectionNativePaths)
			if err != nil {
				t.Fatal(err)
			}
			workspace := newPrimaryProjectionWorkspace(len(primaryProjectionNativePaths))
			result, err, partial := collectPrimaryProjection(
				t, snapshot, filter, workspace, nil, nil, false, -1,
			)
			if err != nil {
				t.Fatalf("projected %s: %v", tc.name, err)
			}
			if result.Supported || result.Scanned != 0 || result.Stopped {
				t.Fatalf("projected %s result=%+v, want declined zero progress", tc.name, result)
			}
			if len(partial) == 0 || len(partial) >= rows {
				t.Fatalf("projected %s callbacks=%d, want partial progress", tc.name, len(partial))
			}

			// A caller that observes Supported=false must discard callbacks and
			// use the generic field reader for the complete snapshot.
			wantRows := append([]primaryProjectionTestRow(nil), fixture.rows...)
			wantRows[last].doc = bytes.Clone(updated)
			generic := collectPrimaryProjectionGeneric(t, snapshot, wantRows, primaryProjectionNativePaths)
			if len(generic) != len(wantRows)*len(primaryProjectionNativePaths) {
				t.Fatalf("generic fallback values=%d", len(generic))
			}
			for row := range wantRows {
				for field, path := range primaryProjectionNativePaths {
					value := generic[row*len(primaryProjectionNativePaths)+field]
					if row == last {
						if tc.name == "container" && path == "/wide" && !bytes.Equal(value, []byte(fmt.Sprintf(`{"value":%d}`, row))) {
							t.Fatalf("container fallback /wide=%q", value)
						}
						continue
					}
					if !bytes.Equal(value, wantRows[row].values[path]) {
						t.Fatalf("fallback row=%d path=%s=%q want %q", row, path, value, wantRows[row].values[path])
					}
				}
			}
		})
	}
}

func TestSnapshotFilterProjectedRangeCallbackErrorAndReuse(t *testing.T) {
	const rows = storeio.CompactPrimaryStripeMaxRows + 64
	fixture := newPrimaryProjectionTestFixture(t, rows, Options{
		Collection: store.Options{ChunkDocuments: 64},
	})
	snapshot, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	filter, err := NewProjectionFilter(primaryProjectionNativePaths)
	if err != nil {
		t.Fatal(err)
	}
	workspace := newPrimaryProjectionWorkspace(len(primaryProjectionNativePaths))
	callbackErr := fmt.Errorf("stop projected scan")
	callbacks := 0
	result, scratch, err := snapshot.FilterProjectedRangeWithScratch(
		filter, nil, nil, false,
		workspace.shapeSeen, workspace.shapeWork, workspace.streamWork,
		workspace.fields, workspace.scratch, -1,
		func(row uint64, fields []storeio.UnifiedProjectionField) error {
			callbacks++
			if row == 123 {
				return callbackErr
			}
			return nil
		},
	)
	workspace.scratch = scratch
	if err != callbackErr || result != (ProjectedRangeResult{}) {
		t.Fatalf("callback result=%+v err=%v, want exact callback error and zero result", result, err)
	}
	if callbacks != 124 {
		t.Fatalf("callback count=%d, want 124", callbacks)
	}
	result, err, observed := collectPrimaryProjection(t, snapshot, filter, workspace, nil, nil, false, -1)
	if err != nil || !result.Supported || result.Scanned != rows {
		t.Fatalf("reused projected result=%+v err=%v", result, err)
	}
	assertPrimaryProjectionRows(t, fixture.rows, primaryProjectionNativePaths, observed, 0)
}

func TestSnapshotFilterProjectedRangeMissingAndNullRemainExact(t *testing.T) {
	const rows = storeio.CompactPrimaryStripeMaxRows + 64
	fixture := newPrimaryProjectionTestFixture(t, rows, Options{
		Collection: store.Options{ChunkDocuments: 64},
	})
	snapshot, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	filter, err := NewProjectionFilter([]string{"/nullable"})
	if err != nil {
		t.Fatal(err)
	}
	workspace := newPrimaryProjectionWorkspace(1)
	result, err, observed := collectPrimaryProjection(t, snapshot, filter, workspace, nil, nil, false, -1)
	if err != nil {
		t.Fatal(err)
	}
	if result.Supported {
		if result.Scanned != rows || len(observed) != rows {
			t.Fatalf("native null/missing result=%+v callbacks=%d, want %d rows", result, len(observed), rows)
		}
		for row, got := range observed {
			if len(got.values) != 1 {
				t.Fatalf("row %d fields=%d", row, len(got.values))
			}
			if row%4 == 2 {
				if len(got.values[0]) != 0 {
					t.Fatalf("missing nullable row=%d value=%q, want absent", row, got.values[0])
				}
			} else if !bytes.Equal(got.values[0], []byte("null")) {
				t.Fatalf("null row=%d value=%q", row, got.values[0])
			}
		}
		return
	}
	if result.Scanned != 0 {
		t.Fatalf("declined null/missing result=%+v callbacks=%d, want zero committed progress", result, len(observed))
	}
	generic := collectPrimaryProjectionGeneric(t, snapshot, fixture.rows, []string{"/nullable"})
	for row, value := range generic {
		if row%4 == 2 {
			if value != nil {
				t.Fatalf("generic missing row value=%q", value)
			}
			continue
		}
		if !bytes.Equal(value, []byte("null")) {
			t.Fatalf("generic null row=%d value=%q", row, value)
		}
	}
}

func TestSnapshotFilterProjectedRangeSnapshotIsolationAndReopen(t *testing.T) {
	const rows = storeio.CompactPrimaryStripeMaxRows + 64
	fixture := newPrimaryProjectionTestFixture(t, rows, Options{
		Collection: store.Options{ChunkDocuments: 64},
	})
	oldSnapshot, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer oldSnapshot.Close()

	// The first compact leaf is full by construction. Keep mutations in the
	// underfilled second leaf so replacing a row exercises snapshot visibility
	// without requiring a structural append to a full leaf.
	updatedRow := storeio.CompactPrimaryStripeMaxRows + 5
	deletedRow := storeio.CompactPrimaryStripeMaxRows + 7
	updatedKey := fixture.rows[updatedRow].key
	updatedDoc := []byte(`{ "decimal" : -0.25, "active" : false, "wide" : -9007199254741999, "name" : "updated \"value\"", "id" : "logical-updated", "slash/key" : "updated/slash", "tilde~key" : "updated~tilde", "nullable" : null }`)
	if created, err := fixture.collection.Put([]byte(updatedKey), updatedDoc); err != nil || created {
		t.Fatalf("update Put=%t,%v, want replacement", created, err)
	}
	deletedKey := fixture.rows[deletedRow].key
	if deleted, err := fixture.collection.Delete([]byte(deletedKey)); err != nil || !deleted {
		t.Fatalf("delete=%t,%v, want deletion", deleted, err)
	}
	insertedKey := "physical-99999999"
	insertedDoc := primaryProjectionTestDocument(9999)
	if created, err := fixture.collection.Put([]byte(insertedKey), insertedDoc); err != nil || !created {
		t.Fatalf("insert Put=%t,%v, want creation", created, err)
	}

	filter, err := NewProjectionFilter(primaryProjectionNativePaths)
	if err != nil {
		t.Fatal(err)
	}
	oldWorkspace := newPrimaryProjectionWorkspace(len(primaryProjectionNativePaths))
	oldResult, err, oldObserved := collectPrimaryProjection(
		t, oldSnapshot, filter, oldWorkspace, nil, nil, false, -1,
	)
	if err != nil || !oldResult.Supported || oldResult.Scanned != rows {
		t.Fatalf("old projected snapshot result=%+v err=%v", oldResult, err)
	}
	assertPrimaryProjectionRows(t, fixture.rows, primaryProjectionNativePaths, oldObserved, 0)

	newRows := make([]primaryProjectionTestRow, 0, rows)
	for row, expected := range fixture.rows {
		switch row {
		case updatedRow:
			newRows = append(newRows, primaryProjectionTestRowFromDocument(t, updatedKey, updatedDoc))
		case deletedRow:
			continue
		default:
			newRows = append(newRows, expected)
		}
	}
	newRows = append(newRows, primaryProjectionTestRowFromDocument(t, insertedKey, insertedDoc))
	newSnapshot, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer newSnapshot.Close()
	newWorkspace := newPrimaryProjectionWorkspace(len(primaryProjectionNativePaths))
	newResult, err, newObserved := collectPrimaryProjection(
		t, newSnapshot, filter, newWorkspace, nil, nil, false, -1,
	)
	if err != nil || !newResult.Supported || newResult.Scanned != len(newRows) {
		t.Fatalf("new projected snapshot result=%+v err=%v", newResult, err)
	}
	assertPrimaryProjectionRows(t, newRows, primaryProjectionNativePaths, newObserved, 0)
	if err := oldSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := newSnapshot.Close(); err != nil {
		t.Fatal(err)
	}

	path := fixture.path
	if err := fixture.collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.file.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedFile, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(reopenedFile, Options{Collection: store.Options{ChunkDocuments: 64}})
	if err != nil {
		_ = reopenedFile.Close()
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedSnapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedSnapshot.Close()
	reopenedWorkspace := newPrimaryProjectionWorkspace(len(primaryProjectionNativePaths))
	reopenedResult, err, reopenedObserved := collectPrimaryProjection(
		t, reopenedSnapshot, filter, reopenedWorkspace, nil, nil, false, -1,
	)
	if err != nil || !reopenedResult.Supported || reopenedResult.Scanned != len(newRows) {
		t.Fatalf("reopened projected snapshot result=%+v err=%v", reopenedResult, err)
	}
	assertPrimaryProjectionRows(t, newRows, primaryProjectionNativePaths, reopenedObserved, 0)
}
