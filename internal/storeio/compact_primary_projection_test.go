package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestCompactProjectionFrontBoundsRestartPeakBeforeAppend(t *testing.T) {
	values := make([][]byte, compactStreamRestart)
	values[0] = []byte(`"` + strings.Repeat("x", 4094) + `"`)
	for row := 1; row < len(values); row++ {
		values[row] = []byte(`"x"`)
	}
	view := compactCodecRoundTrip(t, encodeCompactFront(values), values)
	if view.kind != compactStreamFront {
		t.Fatalf("front stream kind=%d", view.kind)
	}

	length, peak, ok := compactProjectionValueLen(view, 1)
	if !ok || length != len(values[1]) || peak <= length {
		t.Fatalf("target length=%d peak=%d ok=%v, want short target with long restart peak", length, peak, ok)
	}

	// The query lane makes this check before appendValue. A destination that
	// fits the target but not the restart spelling must remain untouched and
	// must not allocate while deciding to decline.
	scratch := make([]byte, 0, length)
	capBefore := cap(scratch)
	lenBefore := len(scratch)
	allocs := testing.AllocsPerRun(100, func() {
		gotLength, gotPeak, gotOK := compactProjectionValueLen(view, 1)
		if !gotOK || gotLength != length || gotPeak != peak {
			t.Fatalf("inconsistent projection bound length=%d peak=%d", gotLength, gotPeak)
		}
		if gotPeak > cap(scratch)-len(scratch) {
			return
		}
		var appended bool
		scratch, appended = view.appendValue(scratch[:0], 1)
		if !appended {
			t.Fatal("front append unexpectedly declined")
		}
	})
	if allocs != 0 {
		t.Fatalf("projection bound allocations=%v, want zero", allocs)
	}
	if cap(scratch) != capBefore || lenBefore != 0 {
		t.Fatalf("scratch changed after bounded decline cap=%d/%d len=%d/%d", cap(scratch), capBefore, len(scratch), lenBefore)
	}
}

func TestCompactProjectionAcceptsSmallPerLeafWorkspace(t *testing.T) {
	const rows = 32
	records := make([]CommonPrimaryLeafRecord, rows)
	for row := range records {
		records[row] = CommonPrimaryLeafRecord{
			Key: []byte(fmt.Sprintf("row-%03d", row)),
			Value: CommonPrimaryLeafValue{Inline: fmt.Appendf(nil,
				`{"id":"logical-%03d","score":%d}`, row, row)},
		}
	}
	view := compactProjectionTestView(t, records)
	if view.shapeCount != 1 {
		t.Fatalf("shape count=%d, want one", view.shapeCount)
	}
	filter, err := NewUnifiedProjectionFilter([][]byte{[]byte("/id"), []byte("/score")})
	if err != nil {
		t.Fatal(err)
	}
	seen := make([]int, 1)
	shapes := make([]UnifiedProjectionShapeWorkspace, 1)
	streams := make([]UnifiedProjectionStreamWorkspace, 2)
	fields := make([]UnifiedProjectionField, 2)
	callbacks := 0
	supported, stopped, _, err := view.VisitResolvedProjection(
		filter.resolvers, seen, shapes, streams, fields, make([]byte, 0, 1024), -1,
		func(row int, fields []UnifiedProjectionField) error {
			callbacks++
			if row != callbacks-1 || len(fields) != 2 {
				t.Fatalf("callback row=%d fields=%d at=%d", row, len(fields), callbacks)
			}
			return nil
		},
	)
	if err != nil || !supported || stopped || callbacks != rows {
		t.Fatalf("projection supported=%v stopped=%v callbacks=%d err=%v", supported, stopped, callbacks, err)
	}
	smallScratch := make([]byte, 0, 1)
	supported, stopped, smallScratch, err = view.VisitResolvedProjection(
		filter.resolvers, seen, shapes, streams, fields, smallScratch, -1,
		func(int, []UnifiedProjectionField) error {
			t.Fatal("small projection scratch published a row")
			return nil
		},
	)
	if err != nil || supported || stopped || cap(smallScratch) != 1 || len(smallScratch) != 0 {
		t.Fatalf("small scratch supported=%v stopped=%v len/cap=%d/%d err=%v", supported, stopped, len(smallScratch), cap(smallScratch), err)
	}
}

func compactProjectionTestView(t testing.TB, records []CommonPrimaryLeafRecord) CompactPrimaryStripeView {
	t.Helper()
	builder := NewUnifiedPrimaryLeafBuilder()
	payload, err := BuildCompactPrimaryStripePayload(records, builder)
	if err != nil {
		t.Fatal(err)
	}
	extent := int(physicalPageQuantum)
	for extent < PageHeaderSize+len(payload)+PageTrailerSize {
		extent <<= 1
	}
	storeID := unifiedTestStoreID()
	page, err := EncodeCompactPrimaryStripe(
		make([]byte, extent),
		CommonPrimaryLeafHeader{
			StoreID: storeID, Generation: 1, Bucket: 0, PageSize: uint32(extent),
		}, records, builder,
	)
	if err != nil {
		t.Fatal(err)
	}
	logicalID, _ := CommonPrimaryLeafLogicalID(0)
	view, err := OpenCompactPrimaryStripe(
		page, storeID, 0,
		PageRef{Offset: 4096, Length: uint32(extent), LogicalID: logicalID,
			Generation: 1, Kind: PagePrimaryLeaf},
		1, unifiedTestBounds(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return view
}

func TestCompactProjectionCursorBoundsPairingAndRelease(t *testing.T) {
	records := make([]CommonPrimaryLeafRecord, 16)
	for row := range records {
		value := fmt.Appendf(nil, `{"id":"logical-%03d","score":%d}`, row, row)
		if row%2 != 0 {
			value = fmt.Appendf(nil, `{"score":%d,"extra":true,"id":"logical-%03d"}`, row, row)
		}
		records[row] = CommonPrimaryLeafRecord{Key: fmt.Appendf(nil, "row-%03d", row), Value: CommonPrimaryLeafValue{Inline: value}}
	}
	view := compactProjectionTestView(t, records)
	if view.shapeCount != 2 {
		t.Fatalf("need interleaved shapes, got %d", view.shapeCount)
	}
	f, err := NewUnifiedProjectionFilter([][]byte{[]byte("/id"), []byte("/score")})
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("callback canceled")
	for _, tc := range []struct {
		name    string
		upper   int
		limit   int
		fail    bool
		rows    int
		stopped bool
	}{
		{name: "upper", upper: 9, limit: -1, rows: 6},
		{name: "empty", upper: 3, limit: -1},
		{name: "limit", upper: 9, limit: 2, rows: 2, stopped: true},
		{name: "exact_limit", upper: 9, limit: 6, rows: 6, stopped: true},
		{name: "callback_error", upper: 9, limit: -1, fail: true, rows: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A successor is intentionally unavailable: reaching this leaf's
			// upper bound must finish without loading another catalog page.
			cursor := PrimaryGraphCursor{leaf: view, row: 3, upper: fmt.Appendf(nil, "row-%03d", tc.upper), depth: 1}
			defer cursor.Close()
			seen := make([]int, 2)
			shapes := make([]UnifiedProjectionShapeWorkspace, 2)
			streams := make([]UnifiedProjectionStreamWorkspace, 4)
			fields := make([]UnifiedProjectionField, 2)
			progress := UnifiedProjectionProgress{}
			callbacks := 0
			ok, stopped, scratch, err := cursor.VisitProjected(f, &progress, seen, shapes, streams, fields, make([]byte, 0, 128), tc.limit,
				func(row uint64, fields []UnifiedProjectionField) error {
					physical := 3 + callbacks
					if row != uint64(callbacks) || string(fields[0].AppendJSON(nil)) != fmt.Sprintf(`"logical-%03d"`, physical) || string(fields[1].AppendJSON(nil)) != fmt.Sprint(physical) {
						t.Fatalf("row=%d physical=%d fields=%q/%q", row, physical, fields[0].AppendJSON(nil), fields[1].AppendJSON(nil))
					}
					callbacks++
					if tc.fail {
						return wantErr
					}
					return nil
				})
			if callbacks != tc.rows || stopped != tc.stopped || ok == tc.fail || tc.fail && !errors.Is(err, wantErr) || !tc.fail && err != nil || cap(scratch) != 128 {
				t.Fatalf("callbacks=%d ok=%v stopped=%v err=%v cap=%d", callbacks, ok, stopped, err, cap(scratch))
			}
			if tc.fail && progress.Scanned != 0 || !tc.fail && progress.Scanned != tc.rows {
				t.Fatalf("progress=%+v", progress)
			}
			for _, stream := range streams {
				if !reflect.ValueOf(stream).IsZero() {
					t.Fatal("retained page-backed stream")
				}
			}
			for _, field := range fields {
				if field.JSON != nil {
					t.Fatal("retained borrowed field")
				}
			}
		})
	}
}

func TestCompactProjectionRejectsWrappedAlphabetLength(t *testing.T) {
	data := binary.LittleEndian.AppendUint32(nil, 4)
	data = append(data, 1, 64) // base length 1, one 64-bit delta
	data = binary.LittleEndian.AppendUint64(data, math.MaxUint64)
	v := compactStreamView{kind: compactStreamAlphabet, count: 1, dictCount: 1, dictDir: []byte{1, 0}, dictData: []byte("x"), data: data}
	if _, _, ok := compactProjectionValueLen(v, 0); ok {
		t.Fatal("wrapped alphabet length was accepted")
	}
}

func TestCompactProjectionResolverNeverGrowsFilter(t *testing.T) {
	for _, tc := range []struct {
		name     string
		document string
		path     string
		want     bool
	}{
		{name: "escaped_path", document: `{"a\u002fb":{"x~y":"hello\\world"},"score":1.25}`, path: "/a~1b/x~0y", want: true},
		{name: "array_path", document: `{"items":[{"id":1},{"id":2}],"score":1e2}`, path: "/items/1/id", want: true},
		{name: "wide_skeleton", document: fmt.Sprintf(`{"%s":1,"id":2}`, strings.Repeat("x", 2048)), path: "/id"},
		{name: "large_tape", document: `{"items":[` + strings.Repeat("1,", 130) + `1],"id":2}`, path: "/id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := compactProjectionTestView(t, []CommonPrimaryLeafRecord{{Key: []byte("key"), Value: CommonPrimaryLeafValue{Inline: []byte(tc.document)}}})
			f, err := NewUnifiedProjectionFilter([][]byte{[]byte(tc.path)})
			if err != nil {
				t.Fatal(err)
			}
			shapes := make([]UnifiedProjectionShapeWorkspace, 1)
			streams := make([]UnifiedProjectionStreamWorkspace, 1)
			seen := make([]int, 1)
			fields := make([]UnifiedProjectionField, 1)
			scratch := make([]byte, 0, 128)
			visit := func(int, []UnifiedProjectionField) error { return nil }
			allocs := testing.AllocsPerRun(100, func() {
				ok, _, _, err := view.VisitResolvedProjection(f.resolvers, seen, shapes, streams, fields, scratch, -1, visit)
				if err != nil || ok != tc.want {
					t.Fatalf("supported=%v want=%v err=%v", ok, tc.want, err)
				}
			})
			if allocs != 0 {
				t.Fatalf("projection allocations=%v", allocs)
			}
			r := &f.resolvers[0]
			if cap(r.filled) != 0 || cap(r.entries) != 0 || cap(r.keyScratch) != 0 {
				t.Fatal("projection grew retained resolver arenas")
			}
		})
	}
}
