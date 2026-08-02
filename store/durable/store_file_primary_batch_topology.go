package durable

import (
	"bytes"
	"fmt"
	"slices"
	"sort"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// preparePrimaryBatchTopology performs the shape-only half of an oversized
// primary batch. It replaces one source leaf with K deterministic ranges in a
// single structural publication, redistributing only rows that are already
// published. The caller then re-plans and publishes the logical batch at the
// following generation.
//
// This two-generation protocol preserves the batch's content atomicity: the
// first generation is content-equivalent and crash-safe on its own; the second
// contains every logical mutation and its one journal record. It also avoids a
// retry train of median splits. K is derived once from the canonical byte-aware
// class-5 planner and is bounded by the configured batch and tablet namespaces.
func (c *Collection) preparePrimaryBatchTopology(
	_ *fileStoreState,
	batch *WriteBatch,
	offendingKey []byte,
) error {
	if batch == nil || len(offendingKey) == 0 {
		return storeio.ErrInvalidWrite
	}
	// A structural transaction rebuilds sealed parent pages. Materialize every
	// deferred leaf first, then derive the topology from that exact published cut.
	if err := c.flushPendingForStructural(); err != nil {
		return err
	}
	state := c.state.Load()
	if state == nil || state.root.PrimaryRoot == (storeio.PageRef{}) {
		return ErrClosed
	}
	if err := c.planPrimaryBatch(state, batch); err != nil {
		return err
	}
	route, err := c.currentPrimaryResidentRoute(state, offendingKey)
	if err != nil {
		return err
	}
	leafIndex := -1
	for at := range c.batchPrimaryLeaves {
		if c.batchPrimaryLeaves[at].resident.Bucket == route.Bucket {
			leafIndex = at
			break
		}
	}
	if leafIndex < 0 {
		return storeio.ErrSegmentedTabletRouterCorrupt
	}

	var path filePrimaryMutationPath
	if err := c.acquirePrimaryRoutingPath(
		&path, state, offendingKey, route,
	); err != nil {
		return err
	}
	defer path.Release()
	currentLeaves, err := c.enumerateTabletLeaves(&path.tablet)
	if err != nil {
		return err
	}
	sourceIndex := structuralIndexOfBucket(currentLeaves, route.Bucket)
	if sourceIndex < 0 {
		return storeio.ErrSegmentedTabletRouterCorrupt
	}

	stripe, ok := storeio.AdmittedCompactPrimaryStripe(
		path.leafLease.Page(), c.storeID, route.Bucket,
	)
	if !ok {
		return storeio.ErrCommonPrimaryLeafCorrupt
	}
	baseRows, err := stripe.RenderRecordsWithScratch(
		c.primaryLeafMutationScratch,
	)
	if err != nil {
		return err
	}
	prospective, applied, err := c.mergePrimaryBatchLeafRows(
		baseRows, &c.batchPrimaryLeaves[leafIndex], c.structuralRows[:0],
	)
	if err != nil {
		return err
	}
	if applied == 0 || len(prospective) == 0 {
		// Deletes can shrink a leaf and never require topology preparation. An
		// oversized signal without a live prospective row is an internal drift.
		return storeio.ErrInvalidWrite
	}
	c.structuralRows = prospective

	cuts, unionKeys, err := c.planPrimaryBatchTopologyCuts(
		baseRows, prospective,
	)
	if err != nil {
		return err
	}
	if len(cuts) == 0 {
		// buildPrimaryBatchLeaf already proved the complete final image cannot
		// encode. If the shared canonical planner found no legal subdivision, the
		// only possible cause is one row that cannot fit any leaf extent.
		return fmt.Errorf(
			"%w: primary row %q cannot fit one leaf",
			ErrDocumentTooLarge, prospective[0].Key,
		)
	}

	floors := make([][]byte, len(cuts)+1)
	floors[0] = currentLeaves[sourceIndex].fence
	for at, cut := range cuts {
		position := sort.Search(len(unionKeys), func(i int) bool {
			return bytes.Compare(unionKeys[i], cut) >= 0
		})
		if position <= 0 || position >= len(unionKeys) ||
			!bytes.Equal(unionKeys[position], cut) {
			return storeio.ErrInvalidWrite
		}
		floor := make([]byte, len(cut))
		floor, err = storeio.ShortestPrimaryFence(
			floor, unionKeys[position-1], cut,
		)
		if err != nil {
			return err
		}
		floors[at+1] = floor
	}

	localIDs, ok := primaryBatchTopologyLocalIDs(
		currentLeaves, currentLeaves[sourceIndex].localID, len(floors),
	)
	if !ok {
		c.primaryMacroSplitRequired.Add(1)
		return ErrPrimaryMacroSplitRequired
	}
	finalLeafCount := len(currentLeaves) - 1 + len(floors)
	if finalLeafCount > storeio.TabletLocalIdentityLocalCount ||
		(finalLeafCount+storeio.SegmentedTabletRouterRowsPerPage-1)/
			storeio.SegmentedTabletRouterRowsPerPage >
			storeio.SegmentedTabletRouterMaxPages {
		c.primaryMacroSplitRequired.Add(1)
		return ErrPrimaryMacroSplitRequired
	}
	if err := c.preflightPrimaryBatchTopologyCapacity(
		&path, len(floors), finalLeafCount,
	); err != nil {
		return err
	}

	// Build the identity/fence-only final tablet now. This validates every
	// anchor and root fence arena before commitPrimaryStructural allocates its
	// first page. Physical refs for the K replacement leaves are filled by the
	// transaction stager below.
	geometry := make(
		[]storeio.SegmentedTabletRouterLeaf, 0, finalLeafCount,
	)
	for at := range currentLeaves {
		if at != sourceIndex {
			geometry = append(geometry, storeio.SegmentedTabletRouterLeaf{
				LocalID: currentLeaves[at].localID,
				Fence:   currentLeaves[at].fence,
			})
			continue
		}
		for rank := range floors {
			geometry = append(geometry, storeio.SegmentedTabletRouterLeaf{
				LocalID: localIDs[rank], Fence: floors[rank],
			})
		}
	}
	if err := storeio.ValidateSegmentedTabletRouterLeafGeometry(geometry); err != nil {
		return err
	}

	tabletID := path.tablet.TabletID()
	generation := state.root.Generation + 1
	return c.commitPrimaryStructural(
		state, &path, structuralSplit,
		func(tx *storeio.WriteTransaction) (
			[]storeio.SegmentedTabletRouterLeaf, []storeio.PageRef, error,
		) {
			// Fences partition keyspace, not merely the prospective rows. This is
			// load-bearing for deletes: a row removed by the logical batch still
			// exists in this content-equivalent generation and must land in the
			// range its key routes to.
			encoded := make([]storeio.PageRef, len(floors))
			baseAt := 0
			for rank := range floors {
				baseEnd := len(baseRows)
				if rank+1 < len(floors) {
					baseEnd = primaryBatchTopologyLowerBound(
						baseRows, baseAt, floors[rank+1],
					)
				}
				bucketU, bucketOK := storeio.MakeTabletLocalIdentityBucket(
					tabletID, uint32(localIDs[rank]),
				)
				if !bucketOK {
					return nil, nil, storeio.ErrSegmentedTabletRouterCorrupt
				}
				ref, encodeErr := c.encodeStructuralLeaf(
					tx, generation, storeio.BucketID(bucketU),
					baseRows[baseAt:baseEnd],
				)
				if encodeErr != nil {
					return nil, nil, encodeErr
				}
				encoded[rank] = ref
				baseAt = baseEnd
			}
			if baseAt != len(baseRows) {
				return nil, nil, storeio.ErrInvalidWrite
			}

			final := make(
				[]storeio.SegmentedTabletRouterLeaf, 0, finalLeafCount,
			)
			for at := range currentLeaves {
				if at != sourceIndex {
					final = append(final, storeio.SegmentedTabletRouterLeaf{
						LocalID: currentLeaves[at].localID,
						Fence:   currentLeaves[at].fence,
						Ref:     currentLeaves[at].ref,
						Zone:    currentLeaves[at].zone,
					})
					continue
				}
				for rank := range floors {
					zone := storeio.BucketZone{}
					if rank == 0 {
						zone = currentLeaves[at].zone
					}
					final = append(final, storeio.SegmentedTabletRouterLeaf{
						LocalID: localIDs[rank], Fence: floors[rank],
						Ref: encoded[rank], Zone: zone,
					})
				}
			}
			return final, []storeio.PageRef{currentLeaves[sourceIndex].ref}, nil
		},
	)
}

// planPrimaryBatchTopologyCuts refines a shared cut set until every resulting
// key range is one canonical class-5 span for both the currently published rows
// and the prospective post-batch rows. Planning only the latter is insufficient:
// a delete or a size-changing replacement can remove the compression context
// that made the content-equivalent intermediate fit. Planning only the former
// fails symmetrically for inserts and growth.
func (c *Collection) planPrimaryBatchTopologyCuts(
	current, prospective []storeio.CommonPrimaryLeafRecord,
) (cuts, unionKeys [][]byte, err error) {
	// The sparse-empty case is important enough to keep direct: the prospective
	// image is already strictly sorted and its canonical spans are self-validating.
	// There is no current content for another dataset's cuts to subdivide, so a
	// second complete planner pass could not refine the answer.
	if len(current) == 0 {
		unionKeys = make([][]byte, len(prospective))
		for at := range prospective {
			unionKeys[at] = prospective[at].Key
		}
		starts, planErr := storeio.AppendCommonPrimaryUnifiedLeafStarts(
			nil, c.primaryUnifiedBuilder, c.storeID, prospective,
		)
		if planErr != nil {
			for at := range prospective {
				_, singleErr := storeio.AppendCommonPrimaryUnifiedLeafStarts(
					nil, c.primaryUnifiedBuilder, c.storeID,
					prospective[at:at+1],
				)
				if singleErr != nil {
					return nil, nil, fmt.Errorf(
						"%w: primary row %q cannot fit one leaf: %v",
						ErrDocumentTooLarge, prospective[at].Key, singleErr,
					)
				}
			}
			return nil, nil, planErr
		}
		cuts = make([][]byte, 0, max(0, len(starts)-1))
		for at := 1; at < len(starts); at++ {
			cuts = append(cuts, prospective[starts[at]].Key)
		}
		return cuts, unionKeys, nil
	}

	unionKeys = make([][]byte, 0, len(current)+len(prospective))
	for at := range current {
		unionKeys = append(unionKeys, current[at].Key)
	}
	for at := range prospective {
		unionKeys = append(unionKeys, prospective[at].Key)
	}
	slices.SortFunc(unionKeys, bytes.Compare)
	unionKeys = slices.CompactFunc(unionKeys, bytes.Equal)
	if len(unionKeys) == 0 {
		return nil, nil, storeio.ErrInvalidWrite
	}

	var starts []int
	datasets := [...]struct {
		rows        []storeio.CommonPrimaryLeafRecord
		prospective bool
	}{
		{rows: current},
		{rows: prospective, prospective: true},
	}
	var additions [][]byte
	for {
		before := len(cuts)
		additions = additions[:0]
		for _, dataset := range datasets {
			at := 0
			for rangeIndex := 0; rangeIndex <= len(cuts); rangeIndex++ {
				end := len(dataset.rows)
				if rangeIndex < len(cuts) {
					end = primaryBatchTopologyLowerBound(
						dataset.rows, at, cuts[rangeIndex],
					)
				}
				if end > at {
					starts, err = storeio.AppendCommonPrimaryUnifiedLeafStarts(
						starts[:0], c.primaryUnifiedBuilder, c.storeID,
						dataset.rows[at:end],
					)
					if err != nil {
						if dataset.prospective {
							for rowAt := at; rowAt < end; rowAt++ {
								_, singleErr :=
									storeio.AppendCommonPrimaryUnifiedLeafStarts(
										nil, c.primaryUnifiedBuilder, c.storeID,
										dataset.rows[rowAt:rowAt+1],
									)
								if singleErr != nil {
									return nil, nil, fmt.Errorf(
										"%w: primary row %q cannot fit one leaf: %v",
										ErrDocumentTooLarge,
										dataset.rows[rowAt].Key, singleErr,
									)
								}
							}
						}
						return nil, nil, err
					}
					for startAt := 1; startAt < len(starts); startAt++ {
						additions = append(
							additions,
							dataset.rows[at+starts[startAt]].Key,
						)
					}
				}
				at = end
			}
		}
		cuts = append(cuts, additions...)
		slices.SortFunc(cuts, bytes.Compare)
		cuts = slices.CompactFunc(cuts, bytes.Equal)
		if len(cuts) == before {
			break
		}
		if len(cuts) >= len(unionKeys) {
			return nil, nil, storeio.ErrInvalidWrite
		}
	}
	return cuts, unionKeys, nil
}

func primaryBatchTopologyLowerBound(
	rows []storeio.CommonPrimaryLeafRecord, first int, key []byte,
) int {
	return first + sort.Search(len(rows)-first, func(offset int) bool {
		return bytes.Compare(rows[first+offset].Key, key) >= 0
	})
}

func primaryBatchTopologyLocalIDs(
	current []structuralLeaf, source uint16, count int,
) ([]uint16, bool) {
	if count < 1 || count > storeio.TabletLocalIdentityLocalCount {
		return nil, false
	}
	var used [storeio.TabletLocalIdentityLocalCount / 64]uint64
	for at := range current {
		id := current[at].localID
		used[id>>6] |= uint64(1) << (id & 63)
	}
	result := make([]uint16, count)
	result[0] = source
	at := 1
	for id := 0; id < storeio.TabletLocalIdentityLocalCount && at < count; id++ {
		if used[id>>6]&(uint64(1)<<uint(id&63)) != 0 {
			continue
		}
		result[at] = uint16(id)
		used[id>>6] |= uint64(1) << uint(id&63)
		at++
	}
	return result, at == count
}

// preflightPrimaryBatchTopologyCapacity proves the K-way primary topology fits
// the fixed transaction and retirement arenas before its stager writes a leaf.
// Exact-index and free-log staging retain their own complete bounded checks in
// commitPrimaryStructural; option normalization guarantees those maxima fit the
// same transaction reservation.
func (c *Collection) preflightPrimaryBatchTopologyCapacity(
	path *filePrimaryMutationPath,
	newLeaves, finalLeafCount int,
) error {
	anchorPages := (finalLeafCount + storeio.SegmentedTabletRouterRowsPerPage - 1) /
		storeio.SegmentedTabletRouterRowsPerPage
	// K leaves, every rebuilt anchor, locator, tablet root, catalog leaf, optional
	// branch, and the global primary root.
	pages := newLeaves + anchorPages + 4
	retirements := 1 + path.tablet.AnchorCount() + 4
	if path.hasBranch {
		pages++
		retirements++
	}
	if pages > c.options.maxTransactionPages {
		return fmt.Errorf(
			"%w: primary topology needs %d of %d transaction pages",
			storeio.ErrTooManyPages, pages, c.options.maxTransactionPages,
		)
	}
	if retirements > cap(c.retireScratch) ||
		retirements > c.options.MaxRetiredExtents {
		return fmt.Errorf(
			"%w: primary topology needs %d retirement slots",
			storeio.ErrRetiredExtentCapacity, retirements,
		)
	}
	return nil
}
