package storeio

import (
	"bytes"
	"errors"
	"fmt"
)

// ValidateSegmentedTabletRouterLeafGeometry validates the identity-independent
// portion of a complete tablet rebuild before a write transaction allocates a
// page. It covers the local-ID namespace, lexical order, anchor-page count,
// compressed anchor fence arenas, and the root's anchor-floor arena.
//
// PageRef identity is intentionally excluded: structural callers invoke this
// preflight before the replacement leaves and anchors have physical handles.
// EncodeSegmentedTabletRouter performs the complete identity validation once
// those handles exist.
func ValidateSegmentedTabletRouterLeafGeometry(
	leaves []SegmentedTabletRouterLeaf,
) error {
	_, _, err := PlanSegmentedTabletRouterAnchors(leaves)
	return err
}

func validateSegmentedTabletRouterLeafIdentities(leaves []SegmentedTabletRouterLeaf) error {
	if len(leaves) == 0 ||
		len(leaves) > TabletLocalIdentityLocalCount ||
		len(leaves) > SegmentedTabletRouterMaxPages*SegmentedTabletRouterRowsPerPage ||
		len(leaves[0].Fence) != 0 {
		return fmt.Errorf(
			"%w: segmented router leaf geometry", ErrInvalidWrite,
		)
	}
	var used [TabletLocalIdentityLocalCount / 64]uint64
	for rank := range leaves {
		leaf := &leaves[rank]
		if leaf.LocalID >= TabletLocalIdentityLocalCount ||
			rank != 0 && (len(leaf.Fence) == 0 ||
				bytes.Compare(leaves[rank-1].Fence, leaf.Fence) >= 0) {
			return fmt.Errorf(
				"%w: non-canonical leaf at rank %d", ErrInvalidWrite, rank,
			)
		}
		word, bit := leaf.LocalID>>6, uint64(1)<<(leaf.LocalID&63)
		if used[word]&bit != 0 {
			return fmt.Errorf("%w: duplicate LocalID", ErrInvalidWrite)
		}
		used[word] |= bit
	}

	return nil
}

// PlanSegmentedTabletRouterAnchors packs lexical leaf fences by both encoded
// bytes and row slots. The returned ends are exclusive leaf offsets. Existing
// full pages retain their encoding; incompressible fences use another existing
// anchor ID rather than failing below the tablet's physical capacity.
func PlanSegmentedTabletRouterAnchors(leaves []SegmentedTabletRouterLeaf) (ends [SegmentedTabletRouterMaxPages]int, count int, err error) {
	if err := validateSegmentedTabletRouterLeafIdentities(leaves); err != nil {
		return ends, 0, err
	}
	return planSegmentedTabletRouterAnchors(leaves)
}

func planSegmentedTabletRouterAnchors(leaves []SegmentedTabletRouterLeaf) (ends [SegmentedTabletRouterMaxPages]int, count int, err error) {
	rootKeyBytes := 0
	for first := 0; first < len(leaves); {
		if count == len(ends) {
			return ends, 0, fmt.Errorf("%w: anchor page count", ErrSegmentedTabletRouterNoSpace)
		}
		last := min(first+SegmentedTabletRouterRowsPerPage, len(leaves))
		if fitErr := validateSegmentedTabletAnchorFenceGeometry(leaves[first:last]); fitErr != nil {
			if !errors.Is(fitErr, ErrSegmentedTabletRouterNoSpace) {
				return ends, 0, fitErr
			}
			lo, hi := first+1, last
			if err := validateSegmentedTabletAnchorFenceGeometry(leaves[first:lo]); err != nil {
				return ends, 0, err
			}
			for lo+1 < hi {
				mid := lo + (hi-lo)/2
				if err := validateSegmentedTabletAnchorFenceGeometry(leaves[first:mid]); err == nil {
					lo = mid
				} else if errors.Is(err, ErrSegmentedTabletRouterNoSpace) {
					hi = mid
				} else {
					return ends, 0, err
				}
			}
			last = lo
		}
		rootKeyBytes += len(leaves[first].Fence)
		if rootKeyBytes > segmentedTabletRouterRootTrailerAt-
			segmentedTabletRouterRootKeysAt {
			return ends, 0, fmt.Errorf(
				"%w: root fence arena", ErrSegmentedTabletRouterNoSpace,
			)
		}
		ends[count] = last
		count++
		first = last
	}
	return ends, count, nil
}

// validateSegmentedTabletAnchorFenceGeometry mirrors the exact fence-byte
// arithmetic in segmentedTabletRouterEncodeAnchor. Keeping it beside the
// public preflight makes the allocation boundary explicit while the encoder
// remains the final authority over the resulting bytes.
func validateSegmentedTabletAnchorFenceGeometry(
	leaves []SegmentedTabletRouterLeaf,
) error {
	if len(leaves) == 0 || len(leaves) > SegmentedTabletRouterRowsPerPage {
		return fmt.Errorf("%w: anchor leaf count", ErrInvalidWrite)
	}
	common := len(leaves[0].Fence)
	for rank := 1; rank < len(leaves); rank++ {
		common = min(
			common,
			segmentedTabletRouterFencePrefix(
				segmentedTabletRouterFence{a: leaves[0].Fence},
				segmentedTabletRouterFence{a: leaves[rank].Fence},
			),
		)
	}
	common = min(common, 255)
	keyAt := common
	var restart []byte
	for rank := range leaves {
		fence := leaves[rank].Fence
		shared := 0
		if rank%segmentedTabletRouterRestart == 0 {
			restart = fence
		} else {
			shared = commonPrefixBytes(restart, fence) - common
			shared = max(0, shared)
		}
		suffix := len(fence) - common - shared
		if shared > 255 || suffix < 0 || suffix > 255 ||
			keyAt+1+suffix > segmentedTabletRouterAnchorKeyCapacity {
			return fmt.Errorf(
				"%w: compressed anchor fence arena",
				ErrSegmentedTabletRouterNoSpace,
			)
		}
		keyAt += 1 + suffix
	}
	return nil
}

func commonPrefixBytes(a, b []byte) int {
	at := 0
	for at < len(a) && at < len(b) && a[at] == b[at] {
		at++
	}
	return at
}
