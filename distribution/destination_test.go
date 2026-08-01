package distribution

// DestinationSet.Normalize is the canonicalizer every downstream dedup relies
// on, so all four of its documented behaviors are pinned here: points are
// sorted and exact-deduplicated, ranges are sorted and merged across both
// overlap and adjacency, points that fall inside a merged range are dropped,
// and a malformed range is rejected with the typed *DestinationError. Because
// the whole point of a canonical form is that it is a fixed point, a
// determinism check asserts normalizing twice reproduces the first result
// byte-for-byte.

import (
	"errors"
	"math"
	"slices"
	"testing"
)

// destEqual compares two normalized sets. KeyspacePoint and KeyRange are both
// comparable, so slices.Equal is an exact structural compare.
func destEqual(a, b DestinationSet) bool {
	return slices.Equal(a.Points, b.Points) && slices.Equal(a.Ranges, b.Ranges)
}

func TestDestinationSetNormalize(t *testing.T) {
	cases := []struct {
		name string
		in   DestinationSet
		want DestinationSet
	}{
		{
			name: "empty",
			in:   DestinationSet{},
			want: DestinationSet{},
		},
		{
			name: "points sorted and exact-deduplicated",
			in:   DestinationSet{Points: []KeyspacePoint{pt(5), pt(1), pt(5), pt(3), pt(1)}},
			want: DestinationSet{Points: []KeyspacePoint{pt(1), pt(3), pt(5)}},
		},
		{
			name: "ranges sorted then merged across overlap and adjacency",
			in: DestinationSet{Ranges: []KeyRange{
				{Start: pt(0x30), End: kend(0x40)}, // adjacent to the merge of the next two
				{Start: pt(0x10), End: kend(0x25)}, // overlaps the following
				{Start: pt(0x20), End: kend(0x30)},
				{Start: pt(0x50), End: kend(0x60)}, // disjoint, stays separate
			}},
			want: DestinationSet{Ranges: []KeyRange{
				{Start: pt(0x10), End: kend(0x40)},
				{Start: pt(0x50), End: kend(0x60)},
			}},
		},
		{
			name: "points covered by a range are dropped, boundaries respected",
			in: DestinationSet{
				Ranges: []KeyRange{{Start: pt(0x10), End: kend(0x20)}},
				// 0x08 before; 0x10 == inclusive start (covered); 0x18 inside;
				// 0x20 == exclusive end (kept); 0x25 after.
				Points: []KeyspacePoint{pt(0x08), pt(0x10), pt(0x18), pt(0x20), pt(0x25)},
			},
			want: DestinationSet{
				Ranges: []KeyRange{{Start: pt(0x10), End: kend(0x20)}},
				Points: []KeyspacePoint{pt(0x08), pt(0x20), pt(0x25)},
			},
		},
		{
			name: "max point covered by a max-ended range, no successor",
			in: DestinationSet{
				Ranges: []KeyRange{{Start: pt(hb(0xf0)), End: maxKeyEnd}},
				Points: []KeyspacePoint{pt(math.MaxUint64), pt(hb(0xf0)), pt(0x10)},
			},
			want: DestinationSet{
				Ranges: []KeyRange{{Start: pt(hb(0xf0)), End: maxKeyEnd}},
				Points: []KeyspacePoint{pt(0x10)},
			},
		},
		{
			name: "max-ended range absorbs an overlapping concrete range",
			in: DestinationSet{Ranges: []KeyRange{
				{Start: pt(hb(0xf0)), End: maxKeyEnd},
				{Start: pt(hb(0xe0)), End: kend(hb(0xf8))},
			}},
			want: DestinationSet{Ranges: []KeyRange{
				{Start: pt(hb(0xe0)), End: maxKeyEnd},
			}},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := c.in.Normalize()
			if err != nil {
				t.Fatalf("Normalize: %v", err)
			}
			if !destEqual(got, c.want) {
				t.Fatalf("Normalize = %+v, want %+v", got, c.want)
			}

			// Idempotence: the canonical form is a fixed point.
			again, err := got.Normalize()
			if err != nil {
				t.Fatalf("Normalize(normalized): %v", err)
			}
			if !destEqual(again, got) {
				t.Fatalf("Normalize is not idempotent: %+v then %+v", got, again)
			}

			// Repeatability: normalizing the same input twice agrees exactly.
			repeat, err := c.in.Normalize()
			if err != nil {
				t.Fatalf("Normalize (repeat): %v", err)
			}
			if !destEqual(repeat, got) {
				t.Fatalf("Normalize is nondeterministic: %+v vs %+v", got, repeat)
			}
		})
	}
}

// TestDestinationSetNormalizeDoesNotMutateReceiver documents that Normalize
// returns a fresh value: the caller's original Points/Ranges slices are left
// exactly as passed, so a set may be normalized without disturbing a shared
// input.
func TestDestinationSetNormalizeDoesNotMutateReceiver(t *testing.T) {
	points := []KeyspacePoint{pt(9), pt(1), pt(9)}
	ranges := []KeyRange{{Start: pt(0x30), End: kend(0x40)}, {Start: pt(0x10), End: kend(0x35)}}
	d := DestinationSet{Points: slices.Clone(points), Ranges: slices.Clone(ranges)}

	if _, err := d.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if !slices.Equal(d.Points, points) || !slices.Equal(d.Ranges, ranges) {
		t.Fatalf("Normalize mutated its receiver: points %v ranges %v", d.Points, d.Ranges)
	}
}

// TestDestinationSetNormalizeRejectsMalformedRange asserts a range whose start
// is not below its concrete end fails closed with the typed error rather than
// being silently normalized.
func TestDestinationSetNormalizeRejectsMalformedRange(t *testing.T) {
	d := DestinationSet{Ranges: []KeyRange{
		{Start: pt(0x10), End: kend(0x20)},
		{Start: pt(0x80), End: kend(0x40)}, // start >= end
	}}
	got, err := d.Normalize()
	if err == nil {
		t.Fatal("Normalize accepted a malformed range, want rejection")
	}
	if !errors.Is(err, ErrInvalidDestination) {
		t.Fatalf("error %v does not match ErrInvalidDestination", err)
	}
	var de *DestinationError
	if !errors.As(err, &de) {
		t.Fatalf("error is %T, want *DestinationError", err)
	}
	if got.Points != nil || got.Ranges != nil {
		t.Fatalf("Normalize returned a non-zero set alongside an error: %+v", got)
	}
}
