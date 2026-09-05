package storeio

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestCompactIntegerSpellingDecodersBoundMemory(t *testing.T) {
	for _, kind := range []string{"dictionary", "front", "alphabet", "prefix"} {
		t.Run(kind, func(t *testing.T) {
			encode := func(values [][]byte) compactStreamEncoding {
				switch kind {
				case "dictionary":
					return encodeCompactDictionary(values)
				case "front":
					return encodeCompactFront(values)
				case "alphabet":
					var scratch compactStreamScratch
					out, ok := scratch.encodeAlphabet(0, values, 0)
					if !ok {
						t.Fatal("alphabet encoding declined")
					}
					return out
				default:
					out, ok := encodeCompactPrefixInt(values)
					if !ok {
						t.Fatal("prefix encoding declined")
					}
					return out
				}
			}
			values := [][]byte{[]byte("123401"), []byte("123402"), []byte("123403")}
			view := compactCodecRoundTrip(t, encode(values), values)
			for row, raw := range values {
				want, _ := strconv.ParseInt(string(raw), 10, 64)
				got, ok := unifiedCompactIntegerValue(view, row)
				if !ok || got != want {
					t.Fatalf("row %d = %d/%t, want %d", row, got, ok, want)
				}
			}
			long := strings.Repeat("x", 4096)
			values = [][]byte{[]byte(long + "1"), []byte(long + "2"), []byte(long + "3")}
			view = compactCodecRoundTrip(t, encode(values), values)
			if allocs := testing.AllocsPerRun(10, func() {
				if _, ok := unifiedCompactIntegerValue(view, 2); ok {
					t.Fatal("long noninteger accepted")
				}
			}); allocs != 0 {
				t.Fatalf("long token rejection allocated %g times", allocs)
			}
		})
	}
}

func TestCompactIntegerGroupsBoundedResolverNeverGrows(t *testing.T) {
	for _, tc := range []struct {
		name, groupPath, sumPath, narrow string
	}{
		{name: "plain", groupPath: "/g", sumPath: "/s", narrow: `{"g":3,"s":7}`},
		{name: "escaped", groupPath: "/g~1k", sumPath: "/s~0v", narrow: `{"g\u002fk":3,"s~v":7}`},
		{name: "count_only", groupPath: "/g", narrow: `{"g":3,"s":7}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			group := &UnifiedHoleResolver{}
			if err := group.SetPath([]byte(tc.groupPath)); err != nil {
				t.Fatal(err)
			}
			var sum *UnifiedHoleResolver
			if tc.sumPath != "" {
				sum = &UnifiedHoleResolver{}
				if err := sum.SetPath([]byte(tc.sumPath)); err != nil {
					t.Fatal(err)
				}
			}
			makeView := func(document string) CompactPrimaryStripeView {
				return compactProjectionTestView(t, []CommonPrimaryLeafRecord{{Key: []byte("key"), Value: CommonPrimaryLeafValue{Inline: []byte(document)}}})
			}
			narrow := makeView(tc.narrow)
			wide := makeView(strings.TrimSuffix(tc.narrow, "}") + fmt.Sprintf(`,"%s":0}`, strings.Repeat("x", 2048)))
			largeTape := makeView(strings.TrimSuffix(tc.narrow, "}") + `,"items":[` + strings.Repeat("1,", 130) + `1]}`)
			seen := make([]int, 128)
			shapes := make([]IntegerGroupShapeWorkspace, 1)
			callbacks := 0
			visit := func(row int, key, value int64) error {
				wantSum := int64(7)
				if sum == nil {
					wantSum = 0
				}
				if row != 0 || key != 3 || value != wantSum {
					t.Fatalf("row=%d key=%d sum=%d", row, key, value)
				}
				callbacks++
				return nil
			}
			// Alternate fresh wider snapshots with accepted scans through the
			// same filter. Neither admission nor decline may grow its arenas.
			allocs := testing.AllocsPerRun(100, func() {
				for _, view := range []*CompactPrimaryStripeView{&narrow, &wide, &largeTape, &narrow} {
					callbacks = 0
					ok, err := view.VisitResolvedIntegerGroups(group, sum, seen, shapes, visit)
					want := view == &narrow
					if err != nil || ok != want || want && callbacks != 1 || !want && callbacks != 0 {
						t.Fatalf("supported=%v want=%v callbacks=%d err=%v", ok, want, callbacks, err)
					}
				}
			})
			if allocs != 0 {
				t.Fatalf("group resolver allocations=%v", allocs)
			}
			for _, resolver := range []*UnifiedHoleResolver{group, sum} {
				if resolver != nil && (cap(resolver.filled) != 0 || cap(resolver.entries) != 0 || cap(resolver.keyScratch) != 0) {
					t.Fatal("group resolver grew retained arenas")
				}
			}
		})
	}
}

func TestCompactIntegerGroupsUseLastDuplicateMember(t *testing.T) {
	group := &UnifiedHoleResolver{}
	if err := group.SetPath([]byte("/g")); err != nil {
		t.Fatal(err)
	}
	view := compactProjectionTestView(t, []CommonPrimaryLeafRecord{{
		Key:   []byte("key"),
		Value: CommonPrimaryLeafValue{Inline: []byte(`{"g":1,"g":3}`)},
	}})
	seen := make([]int, view.shapeCount)
	shapes := make([]IntegerGroupShapeWorkspace, view.shapeCount)
	callbacks := 0
	supported, err := view.VisitResolvedIntegerGroups(group, nil, seen, shapes,
		func(row int, key, sum int64) error {
			callbacks++
			if row != 0 || key != 3 || sum != 0 {
				t.Fatalf("row=%d key=%d sum=%d, want last duplicate key 3", row, key, sum)
			}
			return nil
		})
	if err != nil || !supported || callbacks != 1 {
		t.Fatalf("supported=%v callbacks=%d err=%v", supported, callbacks, err)
	}
}

func TestUnifiedCanonicalIntegerBoundaries(t *testing.T) {
	for _, raw := range []string{"9223372036854775807", "-9223372036854775808", "0", "-1"} {
		want, _ := strconv.ParseInt(raw, 10, 64)
		if got, ok := unifiedCanonicalInt64Value([]byte(raw)); !ok || got != want {
			t.Fatalf("%s = %d/%t", raw, got, ok)
		}
	}
	for _, raw := range []string{"9223372036854775808", "-9223372036854775809", "-0", "1.0", "1e0", `"1"`, "01"} {
		if _, ok := unifiedCanonicalInt64Value([]byte(raw)); ok {
			t.Fatalf("accepted %q", raw)
		}
	}
}
