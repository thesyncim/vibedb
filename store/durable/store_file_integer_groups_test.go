package durable

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// integerGroupTestRow is the detached oracle for one physical row. The
// durable scan reports row ordinals rather than keys, so keeping the expected
// values in physical key order makes a shape/leaf pairing error observable.
type integerGroupTestRow struct {
	key   string
	group int64
	sum   int64
}

type integerGroupTestFixture struct {
	path       string
	file       *os.File
	collection *Collection
	rows       []integerGroupTestRow
}

func integerGroupTestValues(row, rows int) (group, sum int64) {
	// The group stream has a wide, signed range and remains an exact FOR stream
	// in every stripe. The sum stream is above float64's exact-integer boundary;
	// its small per-stripe span keeps it in the compact FOR lane as well.
	groupOffset := int64((row*73)&4095) - 2048
	group = groupOffset
	if row/storeio.CompactPrimaryStripeMaxRows&1 != 0 {
		group = int64(1)<<53 + groupOffset
	} else {
		group = -(int64(1) << 53) + groupOffset
	}
	sum = int64(1)<<53 + int64((row*149)&4095) - 2048
	if row/storeio.CompactPrimaryStripeMaxRows&1 != 0 {
		sum = -sum
	}
	return group, sum
}

func integerGroupTestDocument(group, sum int64, shaped bool) []byte {
	if shaped {
		return fmt.Appendf(nil,
			`{"group":%d,"sum":%d,"shape":true}`,
			group, sum,
		)
	}
	return fmt.Appendf(nil, `{"group":%d,"sum":%d}`, group, sum)
}

func newIntegerGroupTestFixture(
	t testing.TB,
	rows int,
	options Options,
	document func(row int, group, sum int64) []byte,
) *integerGroupTestFixture {
	t.Helper()
	if rows < 1 {
		t.Fatal("integer group fixture requires rows")
	}
	if options.Collection.ChunkDocuments == 0 {
		options.Collection.ChunkDocuments = 64
	}
	file, err := os.CreateTemp(t.TempDir(), "integer-groups-*.vibe")
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	source := &store.Collection{}
	want := make([]integerGroupTestRow, rows)
	for row := range rows {
		group, sum := integerGroupTestValues(row, rows)
		key := fmt.Sprintf("row-%08d", row)
		doc := integerGroupTestDocument(group, sum, row&1 != 0)
		if document != nil {
			doc = document(row, group, sum)
		}
		if _, err := source.Put(key, doc); err != nil {
			_ = file.Close()
			t.Fatalf("source Put(%s): %v", key, err)
		}
		want[row] = integerGroupTestRow{key: key, group: group, sum: sum}
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
	fixture := &integerGroupTestFixture{
		path: path, file: file, collection: collection, rows: want,
	}
	t.Cleanup(func() {
		_ = fixture.collection.Close()
		_ = fixture.file.Close()
	})
	return fixture
}

type integerGroupObservedRow struct {
	row   uint64
	group int64
	sum   int64
}

func collectIntegerGroupRows(
	t testing.TB,
	snapshot *Snapshot,
	filter *IntegerGroupFilter,
) (FilterIntegerGroupsResult, error, []integerGroupObservedRow) {
	t.Helper()
	var observed []integerGroupObservedRow
	result, err := snapshot.FilterIntegerGroups(
		filter,
		func(row uint64, group, sum int64) error {
			observed = append(observed, integerGroupObservedRow{
				row: row, group: group, sum: sum,
			})
			return nil
		},
	)
	return result, err, observed
}

func assertIntegerGroupRows(
	t testing.TB,
	want []integerGroupTestRow,
	got []integerGroupObservedRow,
	withSum bool,
) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("callback rows=%d, want %d", len(got), len(want))
	}
	for at, row := range got {
		if row.row != uint64(at) || row.group != want[at].group ||
			(withSum && row.sum != want[at].sum) || (!withSum && row.sum != 0) {
			t.Fatalf("callback row %d = (%d,%d,%d), want (%d,%d,%d)",
				at, row.row, row.group, row.sum, at, want[at].group,
				func() int64 {
					if withSum {
						return want[at].sum
					}
					return 0
				}(),
			)
		}
	}
}

func TestSnapshotFilterIntegerGroupsPairsPhysicalRowsAcrossLeavesAndShapes(t *testing.T) {
	const rows = 2 * storeio.CompactPrimaryStripeMaxRows
	fixture := newIntegerGroupTestFixture(t, rows, Options{
		Collection: store.Options{ChunkDocuments: 64},
	}, nil)

	counts := primaryLeafClassCounts(t, fixture.collection)
	if counts[storeio.CommonPrimaryLeafCompact] < 2 {
		t.Fatalf("compact leaves=%d, want at least two: %v",
			counts[storeio.CommonPrimaryLeafCompact], counts)
	}
	snapshot, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if snapshot.Len() != rows {
		t.Fatalf("snapshot Len=%d, want %d", snapshot.Len(), rows)
	}

	filter, err := NewIntegerGroupFilter("/group", "/sum")
	if err != nil {
		t.Fatal(err)
	}
	result, err, observed := collectIntegerGroupRows(t, snapshot, filter)
	if err != nil {
		t.Fatalf("FilterIntegerGroups: %v", err)
	}
	if !result.Supported || result.Scanned != rows {
		t.Fatalf("result=%+v, want supported scan of %d rows", result, rows)
	}
	assertIntegerGroupRows(t, fixture.rows, observed, true)

	countFilter, err := NewIntegerGroupFilter("/group", "")
	if err != nil {
		t.Fatal(err)
	}
	countResult, err, countObserved := collectIntegerGroupRows(t, snapshot, countFilter)
	if err != nil {
		t.Fatalf("COUNT FilterIntegerGroups: %v", err)
	}
	if !countResult.Supported || countResult.Scanned != rows {
		t.Fatalf("COUNT result=%+v, want supported scan of %d rows", countResult, rows)
	}
	assertIntegerGroupRows(t, fixture.rows, countObserved, false)
}

func TestSnapshotFilterIntegerGroupsDeclinesLateUnsupportedRowsAtomically(t *testing.T) {
	const rows = 2 * storeio.CompactPrimaryStripeMaxRows
	cases := []struct {
		name string
		doc  func(row int, group, sum int64) []byte
	}{
		{
			name: "missing",
			doc: func(row int, group, sum int64) []byte {
				if row == rows-1 {
					return fmt.Appendf(nil, `{"sum":%d}`, sum)
				}
				return integerGroupTestDocument(group, sum, row&1 != 0)
			},
		},
		{
			name: "null",
			doc: func(row int, group, sum int64) []byte {
				if row == rows-1 {
					return fmt.Appendf(nil, `{"group":null,"sum":%d}`, sum)
				}
				return integerGroupTestDocument(group, sum, row&1 != 0)
			},
		},
		{
			name: "decimal",
			doc: func(row int, group, sum int64) []byte {
				if row == rows-1 {
					return fmt.Appendf(nil, `{"group":1.5,"sum":%d}`, sum)
				}
				return integerGroupTestDocument(group, sum, row&1 != 0)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newIntegerGroupTestFixture(t, rows, Options{
				Collection: store.Options{ChunkDocuments: 64},
			}, tc.doc)
			snapshot, err := fixture.collection.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			filter, err := NewIntegerGroupFilter("/group", "/sum")
			if err != nil {
				t.Fatal(err)
			}
			result, err, partial := collectIntegerGroupRows(t, snapshot, filter)
			if err != nil {
				t.Fatalf("FilterIntegerGroups: %v", err)
			}
			if result.Supported || result.Scanned != 0 {
				t.Fatalf("result=%+v, want unsupported with zero progress", result)
			}
			if len(partial) == 0 {
				t.Fatal("unsupported late row produced no callback progress to discard")
			}
			if len(partial) >= rows {
				t.Fatalf("unsupported late row callback count=%d, want a declined tail", len(partial))
			}
		})
	}

	t.Run("overflow", func(t *testing.T) {
		options := Options{
			Collection:       store.Options{ChunkDocuments: 64},
			InlineValueBytes: 256,
			MaxDocumentBytes: 8 << 10,
		}
		fixture := newIntegerGroupTestFixture(t, rows, options, nil)
		group, sum := integerGroupTestValues(rows-1, rows)
		long := fmt.Appendf(nil,
			`{"group":%d,"sum":%d,"pad":%q}`,
			group, sum, strings.Repeat("x", 1024),
		)
		if created, err := fixture.collection.Put(
			[]byte(fixture.rows[rows-1].key), long,
		); err != nil || created {
			t.Fatalf("overflow replacement Put=%t,%v, want replacement", created, err)
		}
		snapshot, err := fixture.collection.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		defer snapshot.Close()
		filter, err := NewIntegerGroupFilter("/group", "/sum")
		if err != nil {
			t.Fatal(err)
		}
		result, err, partial := collectIntegerGroupRows(t, snapshot, filter)
		if err != nil {
			t.Fatalf("FilterIntegerGroups overflow: %v", err)
		}
		if result.Supported || result.Scanned != 0 {
			t.Fatalf("overflow result=%+v, want unsupported with zero progress", result)
		}
		if len(partial) == 0 || len(partial) >= rows {
			t.Fatalf("overflow callback count=%d, want partial progress", len(partial))
		}
	})
}

func TestSnapshotFilterIntegerGroupsCallbackErrorAndCursorReuse(t *testing.T) {
	const rows = storeio.CompactPrimaryStripeMaxRows
	fixture := newIntegerGroupTestFixture(t, rows, Options{
		Collection: store.Options{ChunkDocuments: 64},
	}, nil)
	snapshot, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	filter, err := NewIntegerGroupFilter("/group", "/sum")
	if err != nil {
		t.Fatal(err)
	}
	callbackErr := errors.New("stop integer group scan")
	callbacks := 0
	result, err := snapshot.FilterIntegerGroups(filter, func(row uint64, group, sum int64) error {
		callbacks++
		if row == 123 {
			return callbackErr
		}
		return nil
	})
	if !errors.Is(err, callbackErr) || err != callbackErr {
		t.Fatalf("callback error=%v, want exact %v", err, callbackErr)
	}
	if result != (FilterIntegerGroupsResult{}) {
		t.Fatalf("callback-error result=%+v, want zero", result)
	}
	if callbacks != 124 {
		t.Fatalf("callback count=%d, want 124", callbacks)
	}

	result, err, observed := collectIntegerGroupRows(t, snapshot, filter)
	if err != nil {
		t.Fatalf("reused FilterIntegerGroups: %v", err)
	}
	if !result.Supported || result.Scanned != rows {
		t.Fatalf("reused result=%+v, want supported scan of %d rows", result, rows)
	}
	assertIntegerGroupRows(t, fixture.rows, observed, true)
}

func TestSnapshotFilterIntegerGroupsIsolatesOlderSnapshotFromMutation(t *testing.T) {
	const rows = storeio.CompactPrimaryStripeMaxRows
	fixture := newIntegerGroupTestFixture(t, rows, Options{
		Collection: store.Options{ChunkDocuments: 64},
	}, nil)
	oldSnapshot, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer oldSnapshot.Close()

	updatedGroup := fixture.rows[3].group + 1
	updatedSum := fixture.rows[3].sum + 1
	updatedDoc := integerGroupTestDocument(updatedGroup, updatedSum, true)
	if created, err := fixture.collection.Put(
		[]byte(fixture.rows[3].key), updatedDoc,
	); err != nil || created {
		t.Fatalf("update Put=%t,%v, want replacement", created, err)
	}
	newKey := "row-99999999"
	newGroup := fixture.rows[42].group + 2
	newSum := fixture.rows[42].sum + 2
	if created, err := fixture.collection.Put(
		[]byte(newKey), integerGroupTestDocument(newGroup, newSum, false),
	); err != nil || !created {
		t.Fatalf("insert Put=%t,%v, want creation", created, err)
	}
	if deleted, err := fixture.collection.Delete([]byte(fixture.rows[1].key)); err != nil || !deleted {
		t.Fatalf("delete=%t,%v, want deletion", deleted, err)
	}

	oldFilter, err := NewIntegerGroupFilter("/group", "/sum")
	if err != nil {
		t.Fatal(err)
	}
	oldResult, err, oldObserved := collectIntegerGroupRows(t, oldSnapshot, oldFilter)
	if err != nil {
		t.Fatalf("old FilterIntegerGroups: %v", err)
	}
	if !oldResult.Supported || oldResult.Scanned != rows {
		t.Fatalf("old result=%+v, want supported scan of %d rows", oldResult, rows)
	}
	assertIntegerGroupRows(t, fixture.rows, oldObserved, true)

	newSnapshot, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer newSnapshot.Close()
	newFilter, err := NewIntegerGroupFilter("/group", "/sum")
	if err != nil {
		t.Fatal(err)
	}
	newResult, err, newObserved := collectIntegerGroupRows(t, newSnapshot, newFilter)
	if err != nil {
		t.Fatalf("new FilterIntegerGroups: %v", err)
	}
	if !newResult.Supported || newResult.Scanned != rows {
		t.Fatalf("new result=%+v, want supported scan of %d rows", newResult, rows)
	}

	wantNew := make([]integerGroupTestRow, 0, rows)
	for row, expected := range fixture.rows {
		if row == 1 {
			continue
		}
		if row == 3 {
			expected.group, expected.sum = updatedGroup, updatedSum
		}
		wantNew = append(wantNew, expected)
	}
	wantNew = append(wantNew, integerGroupTestRow{
		key: newKey, group: newGroup, sum: newSum,
	})
	assertIntegerGroupRows(t, wantNew, newObserved, true)
}

func TestSnapshotFilterIntegerGroupsCloseReopenPreservesCurrentData(t *testing.T) {
	const rows = storeio.CompactPrimaryStripeMaxRows
	fixture := newIntegerGroupTestFixture(t, rows, Options{
		Collection: store.Options{ChunkDocuments: 64},
	}, nil)
	options := Options{Collection: store.Options{ChunkDocuments: 64}}
	updatedGroup := fixture.rows[5].group + 1
	updatedSum := fixture.rows[5].sum + 1
	if _, err := fixture.collection.Put(
		[]byte(fixture.rows[5].key),
		integerGroupTestDocument(updatedGroup, updatedSum, false),
	); err != nil {
		t.Fatalf("update before reopen: %v", err)
	}
	if deleted, err := fixture.collection.Delete([]byte(fixture.rows[7].key)); err != nil || !deleted {
		t.Fatalf("delete before reopen=%t,%v", deleted, err)
	}
	newKey := "row-99999999"
	newGroup := fixture.rows[77].group + 2
	newSum := fixture.rows[77].sum + 2
	if _, err := fixture.collection.Put(
		[]byte(newKey), integerGroupTestDocument(newGroup, newSum, true),
	); err != nil {
		t.Fatalf("insert before reopen: %v", err)
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
	defer reopenedFile.Close()
	reopened, err := Open(reopenedFile, options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	reopenedSnapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedSnapshot.Close()
	filter, err := NewIntegerGroupFilter("/group", "/sum")
	if err != nil {
		t.Fatal(err)
	}
	result, err, observed := collectIntegerGroupRows(t, reopenedSnapshot, filter)
	if err != nil {
		t.Fatalf("reopened FilterIntegerGroups: %v", err)
	}
	if !result.Supported || result.Scanned != rows {
		t.Fatalf("reopened result=%+v, want supported scan of %d rows", result, rows)
	}
	want := make([]integerGroupTestRow, 0, rows)
	for row, expected := range fixture.rows {
		if row == 7 {
			continue
		}
		if row == 5 {
			expected.group, expected.sum = updatedGroup, updatedSum
		}
		want = append(want, expected)
	}
	want = append(want, integerGroupTestRow{key: newKey, group: newGroup, sum: newSum})
	assertIntegerGroupRows(t, want, observed, true)
}
