package storeio

import (
	"bytes"
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

	pageCount := (len(leaves) + SegmentedTabletRouterRowsPerPage - 1) /
		SegmentedTabletRouterRowsPerPage
	rootKeyBytes := 0
	for pageID := 0; pageID < pageCount; pageID++ {
		first := pageID * SegmentedTabletRouterRowsPerPage
		last := min(first+SegmentedTabletRouterRowsPerPage, len(leaves))
		rootKeyBytes += len(leaves[first].Fence)
		if rootKeyBytes > segmentedTabletRouterRootTrailerAt-
			segmentedTabletRouterRootKeysAt {
			return fmt.Errorf(
				"%w: root fence arena", ErrSegmentedTabletRouterNoSpace,
			)
		}
		if err := validateSegmentedTabletAnchorFenceGeometry(
			leaves[first:last],
		); err != nil {
			return err
		}
	}
	return nil
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
	if common > 255 {
		return fmt.Errorf("%w: anchor common prefix", ErrInvalidWrite)
	}
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
