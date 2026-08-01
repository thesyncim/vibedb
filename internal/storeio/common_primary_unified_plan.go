package storeio

import (
	"bytes"
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
