package storeio

import (
	"bytes"
	"errors"
	"fmt"
)

// AppendCommonPrimaryUnifiedLeafStarts appends the inclusive row index of each
// range selected by the canonical class-5 packing planner. The first appended
// index is zero. Records must be strictly key ordered. Their Slot fields are
// planner scratch and may be changed; keys and values are only borrowed.
//
// builder is caller-owned reusable workspace. The planner is deterministic in
// (seed, records) and is exactly the planner used by BuildPrimaryGraph: each
// range contains at most 256 rows and selects the extent with the best encoded
// bytes-per-row ratio. Runtime topology preparation therefore cannot acquire a
// second, subtly different opinion about byte-aware primary-leaf boundaries.
func AppendCommonPrimaryUnifiedLeafStarts(
	dst []int,
	builder *UnifiedPrimaryLeafBuilder,
	seed [16]byte,
	records []CommonPrimaryLeafRecord,
) ([]int, error) {
	if builder == nil || seed == ([16]byte{}) || len(records) == 0 {
		return dst, fmt.Errorf("%w: unified span plan input", ErrInvalidWrite)
	}
	for at := range records {
		if len(records[at].Key) == 0 ||
			len(records[at].Key) > CommonPrimaryLeafMaxKeyBytes ||
			at != 0 && bytes.Compare(records[at-1].Key, records[at].Key) >= 0 {
			return dst, fmt.Errorf(
				"%w: non-canonical unified span records", ErrInvalidWrite,
			)
		}
	}
	mark := len(dst)
	for first := 0; first < len(records); {
		dst = append(dst, first)
		last := min(first+CommonPrimaryLeafWideSlots, len(records))
		count, extent, err := planUnifiedLeaf(
			builder, seed, records[first:last],
		)
		if err != nil {
			return dst[:mark], err
		}
		if count < 1 || count > last-first || extent == 0 {
			return dst[:mark], fmt.Errorf(
				"%w: unified span planner made no progress", ErrInvalidWrite,
			)
		}
		first += count
	}
	return dst, nil
}

// AppendCommonPrimaryCompactLeafStarts appends the inclusive row index of
// each unplaced compact-stripe range. Scan-oriented primary leaves do not
// retain the 256-slot geometry used by exact and tin index probes, so their
// topology uses the same byte-aware encoder as the runtime compact leaf path
// and may admit up to CompactPrimaryStripeMaxRows when the 64 KiB extent fits.
// Records must be strictly key ordered. Keys and values are borrowed.
//
// builder is caller-owned reusable workspace. Planning is deterministic in
// records and uses the canonical compact value preparation, including mixed
// inline/overflow rows and collection summary paths.
func AppendCommonPrimaryCompactLeafStarts(
	dst []int,
	builder *UnifiedPrimaryLeafBuilder,
	seed [16]byte,
	records []CommonPrimaryLeafRecord,
) ([]int, error) {
	if builder == nil || seed == ([16]byte{}) || len(records) == 0 {
		return dst, fmt.Errorf("%w: compact span plan input", ErrInvalidWrite)
	}
	for at := range records {
		if len(records[at].Key) == 0 ||
			len(records[at].Key) > CommonPrimaryLeafMaxKeyBytes ||
			at != 0 && bytes.Compare(records[at-1].Key, records[at].Key) >= 0 {
			return dst, fmt.Errorf(
				"%w: non-canonical compact span records", ErrInvalidWrite,
			)
		}
	}
	mark := len(dst)
	planner := PrimaryValueLeafWindowPlanner{builder: builder}
	for first := 0; first < len(records); {
		dst = append(dst, first)
		last := min(first+CompactPrimaryStripeMaxRows, len(records))
		count, _, _, err := planner.PlanUnplaced(
			records[first:last], CommonPrimaryLeafMaxExtentBytes,
		)
		if err != nil {
			return dst[:mark], err
		}
		if count < 1 || count > last-first {
			return dst[:mark], fmt.Errorf(
				"%w: compact span planner made no progress", ErrInvalidWrite,
			)
		}
		if count <= CommonPrimaryLeafWideSlots {
			placed, placeErr := maxPrimaryLeafPlacedPrefix(
				seed, records[first:first+count],
			)
			if placeErr != nil {
				return dst[:mark], placeErr
			}
			if placed < 1 {
				return dst[:mark], fmt.Errorf(
					"%w: compact span rows do not place", ErrInvalidWrite,
				)
			}
			count = placed
		}
		first += count
	}
	return dst, nil
}

// maxPrimaryLeafPlacedPrefix proves runtime slot placement for an unplaced
// planner window that remains within the indexed geometry. Placement scratch
// is copied so topology planning never changes source record Slot fields.
func maxPrimaryLeafPlacedPrefix(
	seed [16]byte,
	records []CommonPrimaryLeafRecord,
) (int, error) {
	if seed == ([16]byte{}) || len(records) == 0 ||
		len(records) > CommonPrimaryLeafWideSlots {
		return 0, fmt.Errorf("%w: placement proof input", ErrInvalidWrite)
	}
	var scratch [CommonPrimaryLeafWideSlots]CommonPrimaryLeafRecord
	best := 0
	for low, high := 1, len(records); low <= high; {
		middle := (low + high) / 2
		candidate := scratch[:middle]
		copy(candidate, records[:middle])
		err := PlaceCommonPrimaryLeafRecords(
			CommonPrimaryLeafWide, seed, candidate,
		)
		if err == nil {
			best = middle
			low = middle + 1
			continue
		}
		if errors.Is(err, ErrCommonPrimaryLeafFull) ||
			errors.Is(err, ErrCommonPrimaryLeafNeedsWide) {
			high = middle - 1
			continue
		}
		return 0, err
	}
	return best, nil
}
