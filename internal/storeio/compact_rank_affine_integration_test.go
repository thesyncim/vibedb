package storeio

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/thesyncim/vibejson"
)

func compactRankAffineFixture(t testing.TB, step int64) (CompactPrimaryStripeView, []CommonPrimaryLeafRecord) {
	t.Helper()
	records := make([]CommonPrimaryLeafRecord, 1024)
	for row := range records {
		value := int64(10000) + step*int64(row)
		raw := fmt.Appendf(nil, `{"id":%d,"name":"item-%08d-end"`, value, value)
		if row%7 < 3 {
			raw = fmt.Appendf(raw, `,"only":%d`, value)
		}
		if row%11 < 4 {
			raw = append(raw, `,"extra":true`...)
		}
		raw = append(raw, '}')
		records[row] = CommonPrimaryLeafRecord{Key: fmt.Appendf(nil, "row-%04d", row), Value: CommonPrimaryLeafValue{Inline: raw}}
	}
	view := compactProjectionTestView(t, records)
	resolver := UnifiedHoleResolver{}
	if err := resolver.SetPath([]byte("/id")); err != nil {
		t.Fatal(err)
	}
	for shape := 0; shape < view.shapeCount; shape++ {
		entry, _ := view.shapeEntry(shape)
		hole := resolver.resolveCompactTemplate(entry.template)
		stream, ok := compactProjectionStreamAt(entry, hole, view.rows)
		if !ok || stream.kind != compactStreamRankAffine || !stream.rankAffineIsNumber() {
			t.Fatalf("shape %d did not select native rank integer: kind=%d ok=%v", shape, stream.kind, ok)
		}
	}
	return view, records
}

func TestCompactRankAffineNativePaths(t *testing.T) {
	for _, step := range []int64{7, -7} {
		t.Run(fmt.Sprint(step), func(t *testing.T) {
			view, records := compactRankAffineFixture(t, step)
			var decoder CompactPrimaryScanDecoder
			canonical := make([]byte, 0, 512)
			output := make([]byte, 0, 512)
			// Random/reverse point access and independent sequential state must agree
			// with the canonical source, including at every restart boundary.
			for row := len(records) - 1; row >= 0; row-- {
				canonical, _ = vibejson.AppendCanonicalize(canonical[:0], records[row].Value.Inline)
				got, ok := view.AppendValue(output[:0], row)
				if !ok || !bytes.Equal(got, canonical) {
					t.Fatalf("point row=%d", row)
				}
				if size, ok := view.valueLength(row); !ok || size != len(canonical) {
					t.Fatalf("length row=%d", row)
				}
			}
			ordinals := make([]int, view.shapeCount)
			for row := range records {
				canonical, _ = vibejson.AppendCanonicalize(canonical[:0], records[row].Value.Inline)
				shape := view.rowShape(row)
				got, ok := decoder.appendValue(output[:0], &view, view.header.Bucket, row, shape, ordinals[shape])
				ordinals[shape]++
				if !ok || !bytes.Equal(got, canonical) {
					t.Fatalf("scan row=%d", row)
				}
			}
			filter, err := NewUnifiedProjectionFilter([][]byte{[]byte("/id"), []byte("/name"), []byte("/only")})
			if err != nil {
				t.Fatal(err)
			}
			seen := make([]int, view.shapeCount)
			shapes := make([]UnifiedProjectionShapeWorkspace, view.shapeCount)
			streams := make([]UnifiedProjectionStreamWorkspace, 3*view.shapeCount)
			fields := make([]UnifiedProjectionField, 3)
			callbacks := 0
			supported, stopped, _, err := view.VisitResolvedProjection(filter.resolvers, seen, shapes, streams, fields, output[:0], -1,
				func(row int, got []UnifiedProjectionField) error {
					value := int64(10000) + step*int64(row)
					if got[0].Kind != UnifiedProjectionFieldInteger || got[0].Integer != value {
						t.Fatalf("projection id row=%d: %+v", row, got[0])
					}
					if !bytes.Equal(got[1].JSON, fmt.Appendf(nil, `"item-%08d-end"`, value)) {
						t.Fatalf("projection name row=%d", row)
					}
					if row%7 < 3 {
						if got[2].Kind != UnifiedProjectionFieldInteger || got[2].Integer != value {
							t.Fatalf("projection only row=%d", row)
						}
					} else if got[2].Kind != UnifiedProjectionFieldMissing {
						t.Fatalf("missing projection row=%d", row)
					}
					callbacks++
					return nil
				})
			if err != nil || !supported || stopped || callbacks != len(records) {
				t.Fatalf("projection supported=%v stopped=%v callbacks=%d err=%v", supported, stopped, callbacks, err)
			}
			for _, path := range []string{"/id", "/only"} {
				var resolver UnifiedHoleResolver
				if err := resolver.SetPath([]byte(path)); err != nil {
					t.Fatal(err)
				}
				values := make([]int64, 0, len(records))
				for row := range records {
					if path == "/id" || row%7 < 3 {
						values = append(values, 10000+step*int64(row))
					}
				}
				for _, needle := range []int64{math.MinInt64, -1, 0, 2839, 3000, 9999, 10000, 10001, 10007, 10021, 11000, 17161, 18000, math.MaxInt64} {
					equal := 0
					for _, value := range values {
						if value == needle {
							equal++
						}
					}
					if got, ok := view.CountResolvedIntegerEqual(&resolver, needle); !ok || got != equal {
						t.Fatalf("integer equal %s %d: %d/%v want %d", path, needle, got, ok, equal)
					}
					raw := strconv.AppendInt(nil, needle, 10)
					if got, _, ok := view.CountResolvedSpellingEqual(&resolver, raw, output[:0]); !ok || got != equal {
						t.Fatalf("spelling equal %s %d: %d/%v", path, needle, got, ok)
					}
					if got, _, _, ok := view.CountResolvedNumberEqual(&resolver, raw, needle, true, output[:0], nil); !ok || got != equal {
						t.Fatalf("number equal %s %d: %d/%v", path, needle, got, ok)
					}
					for op := UnifiedIntegerLess; op <= UnifiedIntegerGreaterEqual; op++ {
						want := 0
						for _, value := range values {
							if op == UnifiedIntegerLess && value < needle || op == UnifiedIntegerLessEqual && value <= needle || op == UnifiedIntegerGreater && value > needle || op == UnifiedIntegerGreaterEqual && value >= needle {
								want++
							}
						}
						if got, ok := view.CountResolvedIntegerOrdered(&resolver, needle, op); !ok || got != want {
							t.Fatalf("ordered %s %d %d: %d/%v want %d", path, needle, op, got, ok, want)
						}
					}
				}
				for _, interval := range []UnifiedIntegerInterval{{Lower: math.MinInt64, UpperUnbounded: true}, {Lower: 9999, Upper: 10022}, {Lower: 10000, Upper: 10000}, {Lower: 11000, Upper: 10000}, {Lower: 17000, UpperUnbounded: true}} {
					want := 0
					for _, value := range values {
						if value >= interval.Lower && (interval.UpperUnbounded || value < interval.Upper) {
							want++
						}
					}
					if got, ok := view.CountResolvedIntegerInterval(&resolver, interval); !ok || got != want {
						t.Fatalf("interval %s %+v: %d/%v want %d", path, interval, got, ok, want)
					}
				}
				minimum, maximum := values[0], values[0]
				for _, value := range values {
					minimum = min(minimum, value)
					maximum = max(maximum, value)
				}
				if got, ok := view.CountResolvedIntegerExtrema(&resolver); !ok || !got.Found || got.Min != minimum || got.Max != maximum {
					t.Fatalf("extrema %s %+v/%v want %d..%d", path, got, ok, minimum, maximum)
				}
				if got, _, _, ok := view.CountResolvedNumberEqual(&resolver, []byte("1.5"), 0, false, output[:0], nil); !ok || got != 0 {
					t.Fatalf("fraction %s %d/%v", path, got, ok)
				}
			}
			var id UnifiedHoleResolver
			_ = id.SetPath([]byte("/id"))
			callbacks = 0
			supported, err = view.VisitResolvedIntegerGroups(&id, &id, seen, make([]IntegerGroupShapeWorkspace, view.shapeCount), func(row int, key, sum int64) error {
				want := 10000 + step*int64(row)
				if key != want || sum != want {
					t.Fatalf("group row %d: %d %d want %d", row, key, sum, want)
				}
				callbacks++
				return nil
			})
			if err != nil || !supported || callbacks != len(records) {
				t.Fatalf("groups %v %d %v", supported, callbacks, err)
			}
		})
	}
}

func TestCompactRankAffinePatchBreakAndRestore(t *testing.T) {
	view, records := compactRankAffineFixture(t, 7)
	const rank = 731
	original := append([]byte(nil), records[rank].Value.Inline...)
	oldSnapshot, ok := view.AppendValue(nil, rank)
	if !ok {
		t.Fatal("source value")
	}
	changed := bytes.Replace(original, []byte(`"id":15117`), []byte(`"id":99999999`), 1)
	if bytes.Equal(original, changed) {
		t.Fatal("fixture id")
	}
	for at, value := range [][]byte{changed, original} {
		canonical, err := vibejson.AppendCanonicalize(nil, value)
		if err != nil {
			t.Fatal(err)
		}
		certificate := unifiedScalarCanonicalIndex(t, canonical)
		patch, _, resolved, err := view.PatchStableCanonicalReplacementScalarPatch(records[rank].Key, 0, certificate, make([]byte, 0, 512))
		if err != nil || !resolved || !patch.valid() || patch.exact() {
			t.Fatalf("patch admission %v %v %v", resolved, patch, err)
		}
		generation := uint64(at + 2)
		fast, ok, err := view.PatchCompactPrimaryStripeReplacements(make([]byte, CommonPrimaryLeafMaxExtentBytes), generation,
			[]CommonPrimaryUnifiedReplacement{{Key: records[rank].Key, Value: canonical, ScalarPatch: patch}}, NewUnifiedPrimaryLeafBuilder())
		if err != nil || !ok {
			t.Fatalf("patch result %v %v", ok, err)
		}
		before, _ := view.AppendValue(nil, rank)
		if at == 0 && !bytes.Equal(before, oldSnapshot) {
			t.Fatal("old page changed")
		}
		records[rank].Value.Inline = value
		want, err := EncodeBestCompactPrimaryStripe(make([]byte, CommonPrimaryLeafMaxExtentBytes), CommonPrimaryLeafHeader{StoreID: unifiedTestStoreID(), Generation: generation, Bucket: 0}, unifiedTestStoreID(), records, NewUnifiedPrimaryLeafBuilder())
		if err != nil || !bytes.Equal(fast, want) {
			t.Fatalf("patch differs from full planner pass %d: err=%v", at, err)
		}
		logical, _ := CommonPrimaryLeafLogicalID(0)
		view, err = OpenCompactPrimaryStripe(fast, unifiedTestStoreID(), 0, PageRef{Offset: 4096, Length: uint32(len(fast)), LogicalID: logical, Generation: generation, Kind: PagePrimaryLeaf}, generation, unifiedTestBounds())
		if err != nil {
			t.Fatal(err)
		}
	}
}
