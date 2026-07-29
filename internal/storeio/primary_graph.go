package storeio

import (
	"bytes"
	"errors"
	"fmt"
	"math"
)

// PrimaryGraphRecord is one immutable key/value row supplied to
// BuildPrimaryGraph. Both slices are borrowed until the builder returns.
type PrimaryGraphRecord struct {
	Key   []byte
	Value []byte
}

// PrimaryRecord is the concise spelling used by store/durable bulk callers.
type PrimaryRecord = PrimaryGraphRecord

// PrimaryGraphPlacement is the posting-stable location assigned to one input
// record by the bottom-up builder: the leaf's stable BucketID and the row's
// stable slot within it (see VisitPrimaryLeafPostingRows for the per-class slot
// model). Exact-index posting tiles are keyed by this placement.
type PrimaryGraphPlacement struct {
	Bucket BucketID
	Slot   uint8
}

// PrimaryLeafClassPolicy controls the aligned class selected by a bottom-up
// build. Adaptive is the zero value: it first targets the 4 KiB narrow class
// and uses the 8 KiB wide class only when the same rows exceed that budget.
type PrimaryLeafClassPolicy uint8

const (
	PrimaryLeafAdaptive PrimaryLeafClassPolicy = iota
	PrimaryLeafNarrow
	PrimaryLeafWide
	// PrimaryLeafCompact stages every leaf as the compact document-group class
	// (CommonPrimaryLeafCompact), the ordered-primary form of
	// DocumentFormatCompact. It is orthogonal to the succinct narrow/wide extent
	// choice: a compact leaf packs its rows into whichever power-of-two extent
	// best fills, and a run that cannot be grouped (a single trailing row, or a
	// document too large to co-locate) falls back to an adaptive raw leaf, since
	// readers dispatch per leaf on the class byte.
	PrimaryLeafCompact
)

const (
	// PrimaryLeafDefault documents the zero-value production policy.
	PrimaryLeafDefault = PrimaryLeafAdaptive
	// PrimaryLeafNarrowOnly is an explicit synonym useful in option parsing.
	PrimaryLeafNarrowOnly = PrimaryLeafNarrow
	// PrimaryLeafWideOnly is an explicit synonym useful in option parsing.
	PrimaryLeafWideOnly = PrimaryLeafWide
)

type primaryLeafPlan struct {
	first   int
	last    int
	class   CommonPrimaryLeafClass
	records []CommonPrimaryLeafRecord
	// extent is the chosen physical page size for a compact leaf, whose extent
	// is packing-dependent rather than fixed by class. Zero means "derive from
	// class" (narrow=4 KiB, wide=8 KiB, template=4 KiB).
	extent int
}

type primaryBuiltLeaf struct {
	firstKey []byte
	lastKey  []byte
	ref      PageRef
}

type primaryCatalogChild struct {
	floor []byte
	id    uint32
	ref   PageRef
}

// ShortestPrimaryFence returns the shortest prefix of rightMin that is
// strictly greater than leftMax. The caller owns dst and the result aliases it.
func ShortestPrimaryFence(dst, leftMax, rightMin []byte) ([]byte, error) {
	if bytes.Compare(leftMax, rightMin) >= 0 {
		return nil, fmt.Errorf("%w: overlapping primary ranges", ErrInvalidWrite)
	}
	common := 0
	for common < len(leftMax) && common < len(rightMin) &&
		leftMax[common] == rightMin[common] {
		common++
	}
	length := common + 1
	if length > len(rightMin) || len(dst) < length {
		return nil, fmt.Errorf("%w: primary fence destination", ErrInvalidWrite)
	}
	copy(dst, rightMin[:length])
	return dst[:length], nil
}

// PrimaryGraphBuildStats reports how many leaves the bulk builder staged in
// each leaf class. LeavesByClass is indexed by CommonPrimaryLeafClass value
// (1 narrow, 2 wide, 3 template-columnar, 4 compact); index 0 is unused. It
// exists so a caller can assert the adopted class mix deterministically without
// re-reading the durable graph.
type PrimaryGraphBuildStats struct {
	LeavesByClass [5]int
}

// BuildPrimaryGraph deterministically stages one complete ordered primary
// graph in tx. Records must be strictly bytewise lexical and contain inline
// non-empty values. Passing no policy selects PrimaryLeafAdaptive; at most one
// explicit policy is accepted.
//
// The function stages leaves, segmented tablet pages, and catalog levels in
// bottom-up order. It does not publish tx or modify a StateRoot; the returned
// PagePrimaryCatalog reference is suitable for StateRoot.PrimaryRoot.
func BuildPrimaryGraph(
	tx *WriteTransaction,
	records []PrimaryGraphRecord,
	policy ...PrimaryLeafClassPolicy,
) (PageRef, error) {
	ref, _, err := BuildPrimaryGraphWithStats(tx, records, policy...)
	return ref, err
}

// BuildPrimaryGraphWithStats is BuildPrimaryGraph plus the per-class leaf split
// decided during planning. Under the adaptive policy a candidate leaf is staged
// as the template-columnar class when that class packs strictly more documents
// into a 4 KiB page than the raw leaf would — a measured saving well past the
// adoption threshold, since the same documents otherwise require the 8 KiB wide
// class. Explicit narrow/wide policies never select the template class.
// BuildPrimaryGraphPlaced is BuildPrimaryGraph with caller-owned placement
// output: placements must have exactly one element per input record, and each
// receives the posting-stable location the builder assigned that row. It is the
// entry the ordered-primary exact-index build uses to key posting tiles.
func BuildPrimaryGraphPlaced(
	tx *WriteTransaction,
	records []PrimaryGraphRecord,
	placements []PrimaryGraphPlacement,
	policy ...PrimaryLeafClassPolicy,
) (PageRef, error) {
	if len(placements) != len(records) {
		return PageRef{}, fmt.Errorf("%w: primary placement output", ErrInvalidWrite)
	}
	ref, _, err := buildPrimaryGraphPlaced(tx, records, placements, policy...)
	return ref, err
}

// EmptyPrimaryGraphPageCount is the exact number of transaction pages
// BuildEmptyPrimaryGraph stages: one leaf, one tablet (anchor + locator +
// route), one catalog leaf, and the catalog root. A creation transaction
// reserves this plus its exact-index and state-root pages.
const EmptyPrimaryGraphPageCount = 1 + 3 + 1 + 1

// BuildEmptyPrimaryGraph stages a valid ordered primary graph that holds no
// documents: one empty narrow leaf (tablet 0, local 0) spanning the entire key
// range, its single-anchor tablet, and a one-child catalog root. It is the
// creation-time counterpart of BuildPrimaryGraph — a freshly created collection
// is a primary-layout store from its first byte, and its first Put routes to
// this empty leaf and fills it exactly as a runtime insert fills a leaf a delete
// emptied. Both halves are already exercised in production (a single-document
// build produces one leaf/tablet/catalog; a delete of the last row produces an
// empty leaf), so this only composes them. The returned reference is suitable
// for StateRoot.PrimaryRoot.
func BuildEmptyPrimaryGraph(tx *WriteTransaction) (PageRef, error) {
	if tx == nil || !tx.active || tx.options.PageSize != physicalPageQuantum ||
		tx.options.StoreID == ([16]byte{}) || tx.options.Generation == 0 ||
		tx.nextID < PrimaryFirstDynamicLogicalID {
		return PageRef{}, fmt.Errorf("%w: empty primary graph transaction", ErrInvalidWrite)
	}
	bucket, ok := MakeTabletLocalIdentityBucket(0, 0)
	logicalID, logicalOK := CommonPrimaryLeafLogicalID(BucketID(bucket))
	if !ok || !logicalOK {
		return PageRef{}, fmt.Errorf("%w: empty primary leaf identity", ErrInvalidWrite)
	}
	page, err := tx.Allocate(
		PagePrimaryLeaf, CommonPrimaryLeafNarrowBytes, logicalID,
	)
	if err != nil {
		return PageRef{}, err
	}
	if _, err := EncodeCommonPrimaryLeaf(
		page.Bytes(), CommonPrimaryLeafNarrow,
		CommonPrimaryLeafHeader{
			StoreID: tx.options.StoreID, Generation: tx.options.Generation,
			Bucket: BucketID(bucket), PageSize: CommonPrimaryLeafNarrowBytes,
		},
		tx.options.StoreID, nil,
		CommonPrimaryLeafBounds{
			FileEnd:           tx.fileEnd,
			NextLogicalID:     tx.nextID,
			AllocationQuantum: tx.options.PageSize,
		},
	); err != nil {
		return PageRef{}, err
	}
	if err := page.Stage(); err != nil {
		return PageRef{}, err
	}
	tablets, err := buildPrimaryTablets(tx, []primaryBuiltLeaf{{ref: page.Ref()}})
	if err != nil {
		return PageRef{}, err
	}
	return buildPrimaryCatalog(tx, tablets)
}

func BuildPrimaryGraphWithStats(
	tx *WriteTransaction,
	records []PrimaryGraphRecord,
	policy ...PrimaryLeafClassPolicy,
) (PageRef, PrimaryGraphBuildStats, error) {
	return buildPrimaryGraphPlaced(tx, records, nil, policy...)
}

func buildPrimaryGraphPlaced(
	tx *WriteTransaction,
	records []PrimaryGraphRecord,
	placements []PrimaryGraphPlacement,
	policy ...PrimaryLeafClassPolicy,
) (PageRef, PrimaryGraphBuildStats, error) {
	var stats PrimaryGraphBuildStats
	if tx == nil || !tx.active || tx.options.PageSize != physicalPageQuantum ||
		tx.options.StoreID == ([16]byte{}) ||
		tx.options.Generation == 0 ||
		tx.nextID < PrimaryFirstDynamicLogicalID ||
		len(records) == 0 || len(policy) > 1 {
		return PageRef{}, stats, fmt.Errorf("%w: primary graph transaction or input", ErrInvalidWrite)
	}
	selected := PrimaryLeafAdaptive
	if len(policy) == 1 {
		selected = policy[0]
	}
	if selected > PrimaryLeafCompact {
		return PageRef{}, stats, fmt.Errorf("%w: primary leaf policy", ErrInvalidWrite)
	}
	for at := range records {
		if len(records[at].Key) == 0 ||
			len(records[at].Key) > CommonPrimaryLeafMaxKeyBytes ||
			len(records[at].Value) == 0 ||
			at != 0 && bytes.Compare(records[at-1].Key, records[at].Key) >= 0 {
			return PageRef{}, stats, fmt.Errorf("%w: non-canonical primary records", ErrInvalidWrite)
		}
	}

	plans, err := planPrimaryLeaves(tx, records, selected)
	if err != nil {
		return PageRef{}, stats, err
	}
	if len(plans) > TabletLocalIdentityTabletCount*TabletLocalIdentityLocalCount {
		return PageRef{}, stats, fmt.Errorf("%w: primary leaf namespace exhausted", ErrInvalidWrite)
	}
	for at := range plans {
		if class := plans[at].class; int(class) < len(stats.LeavesByClass) {
			stats.LeavesByClass[class]++
		}
	}
	built, err := buildPrimaryLeaves(tx, records, plans, placements)
	if err != nil {
		return PageRef{}, stats, err
	}
	tablets, err := buildPrimaryTablets(tx, built)
	if err != nil {
		return PageRef{}, stats, err
	}
	root, err := buildPrimaryCatalog(tx, tablets)
	return root, stats, err
}

// PrimaryGraphPageCount returns the exact number of transaction pages
// BuildPrimaryGraph will stage for records and policy. Bulk callers use it to
// reserve one bounded commit without guessing from document count or average
// value width.
func PrimaryGraphPageCount(
	storeID [16]byte,
	records []PrimaryGraphRecord,
	policy ...PrimaryLeafClassPolicy,
) (int, error) {
	if storeID == ([16]byte{}) || len(records) == 0 || len(policy) > 1 {
		return 0, fmt.Errorf("%w: primary graph count input", ErrInvalidWrite)
	}
	selected := PrimaryLeafAdaptive
	if len(policy) == 1 {
		selected = policy[0]
	}
	if selected > PrimaryLeafCompact {
		return 0, fmt.Errorf("%w: primary leaf policy", ErrInvalidWrite)
	}
	for at := range records {
		if len(records[at].Key) == 0 ||
			len(records[at].Key) > CommonPrimaryLeafMaxKeyBytes ||
			len(records[at].Value) == 0 ||
			at != 0 && bytes.Compare(records[at-1].Key, records[at].Key) >= 0 {
			return 0, fmt.Errorf("%w: non-canonical primary records", ErrInvalidWrite)
		}
	}
	layout, err := MutableStoreLayout(physicalPageQuantum)
	if err != nil {
		return 0, err
	}
	measurement := &WriteTransaction{
		options: WriteTransactionOptions{
			StoreID: storeID, Generation: 1,
			PageSize: physicalPageQuantum,
		},
		fileEnd: layout.DataStart,
		nextID:  PrimaryFirstDynamicLogicalID,
	}
	plans, err := planPrimaryLeaves(measurement, records, selected)
	if err != nil {
		return 0, err
	}
	leafCount := len(plans)
	if leafCount > TabletLocalIdentityTabletCount*TabletLocalIdentityLocalCount {
		return 0, fmt.Errorf("%w: primary leaf namespace exhausted", ErrInvalidWrite)
	}
	tabletCount := (leafCount + TabletLocalIdentityLocalCount - 1) /
		TabletLocalIdentityLocalCount
	anchorCount := (leafCount + SegmentedTabletRouterRowsPerPage - 1) /
		SegmentedTabletRouterRowsPerPage
	leafFanout := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogNodeBytes, CommonPrimaryLeafMaxKeyBytes,
	)
	rootFanout := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogRootBytes, CommonPrimaryLeafMaxKeyBytes,
	)
	if leafFanout == 0 || rootFanout == 0 {
		return 0, fmt.Errorf("%w: primary catalog geometry", ErrInvalidWrite)
	}
	catalogLeaves := (tabletCount + leafFanout - 1) / leafFanout
	catalogBranches := 0
	rootChildren := catalogLeaves
	if catalogLeaves > rootFanout {
		catalogBranches = (catalogLeaves + leafFanout - 1) / leafFanout
		rootChildren = catalogBranches
	}
	if catalogLeaves > GlobalTabletCatalogMaxLeafPages ||
		catalogBranches > GlobalTabletCatalogMaxBranchPages ||
		rootChildren > rootFanout {
		return 0, fmt.Errorf("%w: primary catalog capacity", ErrInvalidWrite)
	}
	return leafCount + anchorCount + 2*tabletCount +
		catalogLeaves + catalogBranches + 1, nil
}

func planPrimaryLeaves(
	tx *WriteTransaction,
	records []PrimaryGraphRecord,
	policy PrimaryLeafClassPolicy,
) ([]primaryLeafPlan, error) {
	if policy == PrimaryLeafCompact {
		return planCompactPrimaryLeaves(tx, records)
	}
	plans := make([]primaryLeafPlan, 0,
		(len(records)+CommonPrimaryLeafNarrowLive-1)/CommonPrimaryLeafNarrowLive)
	scratch := make([]byte, CommonPrimaryLeafWideBytes)
	bounds := CommonPrimaryLeafBounds{
		FileEnd:           tx.fileEnd,
		NextLogicalID:     tx.nextID,
		AllocationQuantum: tx.options.PageSize,
	}
	candidateAt := func(count int, first int) []CommonPrimaryLeafRecord {
		candidate := make([]CommonPrimaryLeafRecord, count)
		for at := range candidate {
			row := records[first+at]
			candidate[at] = CommonPrimaryLeafRecord{
				Key: row.Key, Value: CommonPrimaryLeafValue{Inline: row.Value},
			}
		}
		return candidate
	}
	// count keeps the largest fitting batch found, and best/bestClass keep its
	// already-placed candidate so the winning leaf is never re-placed.
	var (
		count     int
		best      []CommonPrimaryLeafRecord
		bestClass CommonPrimaryLeafClass
	)
	consider := func(n, first int) (bool, error) {
		candidate := candidateAt(n, first)
		class, fitErr := fitPrimaryLeaf(scratch, tx, bounds, candidate, policy)
		if fitErr == nil {
			if n > count {
				count, best, bestClass = n, candidate, class
			}
			return true, nil
		}
		if !errors.Is(fitErr, ErrCommonPrimaryLeafFull) &&
			!errors.Is(fitErr, ErrCommonPrimaryLeafNeedsWide) {
			return false, fitErr
		}
		return false, nil
	}
	for first := 0; first < len(records); {
		hi := min(CommonPrimaryLeafNarrowLive, len(records)-first)
		count, best, bestClass = 0, nil, 0
		// A leaf only grows as records are added, so "these count records fit a leaf
		// class" is monotone in count: if count fits, count-1 fits. Binary-search
		// the largest fitting count. The old code decremented one record at a time,
		// re-hashing and re-encoding a full O(NarrowLive) candidate on every step,
		// so a large-document corpus that packs one row per leaf paid
		// O(NarrowLive^2) placement work per leaf -- the dominant cost of the
		// larger-than-cache build. Probing the midpoint keeps every trial candidate
		// no larger than the true answer's neighbourhood, so a one-row-per-leaf
		// corpus never pays to place a near-full candidate it will reject; that is
		// why a plain binary search beats a hi-first fast path here, where placement
		// cost grows super-linearly in the candidate size. A non-fit error other
		// than full/needs-wide is a real failure and aborts the plan.
		for lo, high := 1, hi; lo <= high; {
			mid := (lo + high) / 2
			fits, err := consider(mid, first)
			if err != nil {
				return nil, err
			}
			if fits {
				lo = mid + 1
			} else {
				high = mid - 1
			}
		}
		if count == 0 {
			return nil, fmt.Errorf("%w: primary record does not fit wide leaf", ErrInvalidWrite)
		}
		chosen := primaryLeafPlan{
			first: first, last: first + count,
			class: bestClass, records: best,
		}
		// Template-columnar class selection is per leaf and adaptive-only. The raw
		// packing above already fills the class it chose (narrow or wide); the
		// template class wins when it stores the same documents at a lower page
		// cost per document. A template leaf fills one 4 KiB page, so its per-doc
		// cost is 4096/tcCount, and it is adopted only when that is at least 25%
		// below the raw leaf's page cost per document — the adoption threshold.
		// tcCount is capped at what a raw wide leaf can hold so a later mutation
		// can always de-template it back into the raw envelope.
		if policy == PrimaryLeafAdaptive {
			rawCount := count
			rawPageBytes := CommonPrimaryLeafNarrowBytes
			if chosen.class == CommonPrimaryLeafWide {
				rawPageBytes = CommonPrimaryLeafWideBytes
			}
			tcCount := planTemplateLeafCount(records, first)
			// 4096/tcCount <= 0.75 * rawPageBytes/rawCount, cross-multiplied with
			// the 3/4 factor to stay in integer arithmetic.
			if tcCount > 0 &&
				4*CommonPrimaryLeafNarrowBytes*rawCount <=
					3*rawPageBytes*tcCount {
				tcRecords := make([]CommonPrimaryLeafRecord, tcCount)
				for at := range tcRecords {
					row := records[first+at]
					tcRecords[at] = CommonPrimaryLeafRecord{
						Key:   row.Key,
						Value: CommonPrimaryLeafValue{Inline: row.Value},
					}
				}
				chosen = primaryLeafPlan{
					first: first, last: first + tcCount,
					class: CommonPrimaryLeafTemplate, records: tcRecords,
				}
			}
		}
		plans = append(plans, chosen)
		first = chosen.last
	}
	return plans, nil
}

// compactLeafExtents are the physical extents a compact leaf may occupy, from
// the 4 KiB minimum to the 64 KiB maximum. A leaf picks the one that packs its
// rows at the lowest byte-per-row cost, so small documents fill a small extent
// densely while large ones spread into a larger extent rather than wasting a
// fixed one.
var compactLeafExtents = [...]int{
	CommonPrimaryLeafNarrowBytes, CommonPrimaryLeafWideBytes,
	16 << 10, 32 << 10, 64 << 10,
}

// planCompactPrimaryLeaves packs records into compact document-group leaves.
// Each leaf takes the row count and extent that minimise byte-per-row within the
// row-slot ceiling, and any run that cannot be grouped — a lone trailing row, or
// a document too large to co-locate with a neighbour — falls back to one adaptive
// raw leaf, which reads dispatch to on the class byte just like the compact ones.
func planCompactPrimaryLeaves(
	tx *WriteTransaction,
	records []PrimaryGraphRecord,
) ([]primaryLeafPlan, error) {
	var plans []primaryLeafPlan
	builder := NewCompactPrimaryLeafBuilder()
	scratch := make([]byte, CommonPrimaryLeafWideBytes)
	bounds := CommonPrimaryLeafBounds{
		FileEnd:           tx.fileEnd,
		NextLogicalID:     tx.nextID,
		AllocationQuantum: tx.options.PageSize,
	}
	window := make([]CommonPrimaryLeafRecord, 0, CommonPrimaryCompactLeafMaxRows)
	rawLeaf := func(first int) (primaryLeafPlan, error) {
		candidate := []CommonPrimaryLeafRecord{{
			Key:   records[first].Key,
			Value: CommonPrimaryLeafValue{Inline: records[first].Value},
		}}
		class, err := fitPrimaryLeaf(scratch, tx, bounds, candidate, PrimaryLeafAdaptive)
		if err != nil {
			return primaryLeafPlan{}, fmt.Errorf(
				"%w: compact leaf raw fallback", err)
		}
		return primaryLeafPlan{
			first: first, last: first + 1, class: class, records: candidate,
		}, nil
	}
	for first := 0; first < len(records); {
		remaining := len(records) - first
		if remaining >= 2 {
			hi := min(CommonPrimaryCompactLeafMaxRows, remaining)
			window = window[:0]
			for at := range hi {
				window = append(window, CommonPrimaryLeafRecord{
					Key:   records[first+at].Key,
					Value: CommonPrimaryLeafValue{Inline: records[first+at].Value},
				})
			}
			count, extent, err := planCompactLeaf(builder, window)
			if err != nil {
				return nil, err
			}
			if count >= 2 {
				plans = append(plans, primaryLeafPlan{
					first: first, last: first + count,
					class:   CommonPrimaryLeafCompact,
					records: append([]CommonPrimaryLeafRecord(nil), window[:count]...),
					extent:  extent,
				})
				first += count
				continue
			}
		}
		plan, err := rawLeaf(first)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
		first++
	}
	return plans, nil
}

// planCompactLeaf chooses the row count and extent for one compact leaf from the
// front of window, minimising byte-per-row. It returns count 0 when not even two
// rows can be grouped into the largest extent, signalling a raw fallback.
func planCompactLeaf(
	builder *CompactPrimaryLeafBuilder, window []CommonPrimaryLeafRecord,
) (int, int, error) {
	if len(window) < 2 {
		return 0, 0, nil
	}
	// A compact leaf de-compacts into a raw wide leaf on its first mutation, so
	// it must never hold more rows than a 64 KiB wide leaf can carry raw.
	hiRows := commonPrimaryLeafRawWideCapacity(window, 64<<10)
	if hiRows < 2 {
		return 0, 0, nil
	}
	window = window[:hiRows]
	if err := builder.extractRecords(window); err != nil {
		return 0, 0, err
	}
	memo := make(map[int]int, len(window))
	imageBytes := func(count int) (int, error) {
		if cached, ok := memo[count]; ok {
			return cached, nil
		}
		size, err := builder.prefixImageBytes(count)
		if err != nil {
			return 0, err
		}
		memo[count] = size
		return size, nil
	}
	// maxFitting returns the largest count in [2, hi] whose image fits cap, or 0.
	// The image grows monotonically with count, so a binary search suffices.
	maxFitting := func(capacity, hi int) (int, error) {
		if hi < 2 {
			return 0, nil
		}
		best := 0
		lo, high := 2, hi
		for lo <= high {
			mid := (lo + high) / 2
			size, err := imageBytes(mid)
			if err != nil {
				return 0, err
			}
			if size <= capacity {
				best = mid
				lo = mid + 1
			} else {
				high = mid - 1
			}
		}
		return best, nil
	}
	bestCount, bestExtent := 0, 0
	bestRatio := math.MaxFloat64
	for _, extent := range compactLeafExtents {
		capacity := extent - PageHeaderSize - PageTrailerSize
		count, err := maxFitting(capacity, len(window))
		if err != nil {
			return 0, 0, err
		}
		if count < 2 {
			continue
		}
		ratio := float64(extent) / float64(count)
		if ratio < bestRatio {
			bestCount, bestExtent, bestRatio = count, extent, ratio
		}
	}
	return bestCount, bestExtent, nil
}

// planTemplateLeafCount returns the largest number of records from first that a
// template-columnar leaf should hold: the most that fit one 4 KiB template page,
// capped at the raw wide-leaf capacity (so the leaf can be de-templated back for
// mutation) and the template row-count ceiling. It returns 0 when not even one
// record fits. The template image grows monotonically with the record count, so
// the bound is found by binary search.
func planTemplateLeafCount(records []PrimaryGraphRecord, first int) int {
	tcPayloadCap := CommonPrimaryLeafNarrowBytes - PageHeaderSize - PageTrailerSize
	maxCount := min(
		len(records)-first,
		min(templateColumnarLeafSlots-1, primaryLeafRawWideCapacity(records, first)),
	)
	if maxCount < 1 {
		return 0
	}
	scratch := make([]CommonPrimaryLeafRecord, 0, maxCount)
	fits := func(count int) bool {
		scratch = scratch[:0]
		for at := range count {
			row := records[first+at]
			scratch = append(scratch, CommonPrimaryLeafRecord{
				Key: row.Key, Value: CommonPrimaryLeafValue{Inline: row.Value},
			})
		}
		payload, err := TemplateColumnarLeafImagePayloadBytes(scratch)
		return err == nil && payload <= tcPayloadCap
	}
	if !fits(1) {
		return 0
	}
	low, high, best := 1, maxCount, 1
	for low <= high {
		mid := (low + high) / 2
		if fits(mid) {
			best = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return best
}

// primaryLeafRawWideCapacity returns the largest number of records from first
// that fit one raw wide leaf. The de-templating fallback re-encodes a template
// leaf into this envelope, so a template leaf never holds more.
func primaryLeafRawWideCapacity(
	records []PrimaryGraphRecord, first int,
) int {
	capacity := CommonPrimaryLeafWideBytes - PageHeaderSize - PageTrailerSize
	recordBytes := 0
	count := 0
	for count < CommonPrimaryLeafWideSlots && first+count < len(records) {
		row := records[first+count]
		grown := recordBytes + len(row.Key) + len(row.Value)
		if len(row.Key) >= commonPrimaryLeafEscapeLength {
			grown++
		}
		layout := commonPrimaryLeafLayoutFor(
			CommonPrimaryLeafWide, count+1, CommonPrimaryLeafWideBytes,
		)
		if layout.heapStart+grown > capacity {
			break
		}
		recordBytes = grown
		count++
	}
	return count
}

// commonPrimaryLeafRawWideCapacity returns the largest prefix of records that
// fits one raw wide leaf of extentBytes. A compact leaf de-compacts back into a
// raw wide envelope on its first mutation, so the compact planner caps a leaf's
// rows at this value for the 64 KiB maximum extent — otherwise a full compact
// leaf could not be rewritten as a raw leaf and a Put on it would fail.
func commonPrimaryLeafRawWideCapacity(
	records []CommonPrimaryLeafRecord, extentBytes int,
) int {
	capacity := extentBytes - PageHeaderSize - PageTrailerSize
	recordBytes := 0
	count := 0
	for count < CommonPrimaryLeafWideSlots && count < len(records) {
		row := records[count]
		grown := recordBytes + len(row.Key) + len(row.Value.Inline)
		if len(row.Key) >= commonPrimaryLeafEscapeLength {
			grown++
		}
		layout := commonPrimaryLeafLayoutFor(
			CommonPrimaryLeafWide, count+1, extentBytes,
		)
		if layout.heapStart+grown > capacity {
			break
		}
		recordBytes = grown
		count++
	}
	return count
}

func fitPrimaryLeaf(
	scratch []byte,
	tx *WriteTransaction,
	bounds CommonPrimaryLeafBounds,
	records []CommonPrimaryLeafRecord,
	policy PrimaryLeafClassPolicy,
) (CommonPrimaryLeafClass, error) {
	try := func(class CommonPrimaryLeafClass, pageSize uint32) error {
		if err := PlaceCommonPrimaryLeafRecords(
			class, tx.options.StoreID, records,
		); err != nil {
			return err
		}
		_, err := EncodeCommonPrimaryLeaf(
			scratch[:pageSize], class,
			CommonPrimaryLeafHeader{
				StoreID: tx.options.StoreID, Generation: tx.options.Generation,
				Bucket: 0, PageSize: pageSize,
			},
			tx.options.StoreID, records, bounds,
		)
		return err
	}
	switch policy {
	case PrimaryLeafNarrow:
		err := try(CommonPrimaryLeafNarrow, CommonPrimaryLeafNarrowBytes)
		return CommonPrimaryLeafNarrow, err
	case PrimaryLeafWide:
		err := try(CommonPrimaryLeafWide, CommonPrimaryLeafWideBytes)
		return CommonPrimaryLeafWide, err
	default:
		if err := try(
			CommonPrimaryLeafNarrow, CommonPrimaryLeafNarrowBytes,
		); err == nil {
			return CommonPrimaryLeafNarrow, nil
		} else if !errors.Is(err, ErrCommonPrimaryLeafNeedsWide) &&
			!errors.Is(err, ErrCommonPrimaryLeafFull) {
			return 0, err
		}
		err := try(CommonPrimaryLeafWide, CommonPrimaryLeafWideBytes)
		return CommonPrimaryLeafWide, err
	}
}

func buildPrimaryLeaves(
	tx *WriteTransaction,
	input []PrimaryGraphRecord,
	plans []primaryLeafPlan,
	placements []PrimaryGraphPlacement,
) ([]primaryBuiltLeaf, error) {
	built := make([]primaryBuiltLeaf, len(plans))
	bounds := CommonPrimaryLeafBounds{
		NextLogicalID:     tx.nextID,
		AllocationQuantum: tx.options.PageSize,
	}
	var compactBuilder *CompactPrimaryLeafBuilder
	for rank := range plans {
		tabletID := uint32(rank / TabletLocalIdentityLocalCount)
		localID := uint32(rank % TabletLocalIdentityLocalCount)
		bucket, ok := MakeTabletLocalIdentityBucket(tabletID, localID)
		logicalID, logicalOK := CommonPrimaryLeafLogicalID(BucketID(bucket))
		if !ok || !logicalOK {
			return nil, fmt.Errorf("%w: primary leaf identity", ErrInvalidWrite)
		}
		pageSize := uint32(CommonPrimaryLeafNarrowBytes)
		switch plans[rank].class {
		case CommonPrimaryLeafWide:
			pageSize = CommonPrimaryLeafWideBytes
		case CommonPrimaryLeafCompact:
			pageSize = uint32(plans[rank].extent)
		}
		page, err := tx.Allocate(PagePrimaryLeaf, pageSize, logicalID)
		if err != nil {
			return nil, err
		}
		bounds.FileEnd = tx.fileEnd
		leafHeader := CommonPrimaryLeafHeader{
			StoreID: tx.options.StoreID, Generation: tx.options.Generation,
			Bucket: BucketID(bucket), PageSize: pageSize,
		}
		switch plans[rank].class {
		case CommonPrimaryLeafTemplate:
			if _, err := EncodeCommonPrimaryTemplateLeaf(
				page.Bytes(), leafHeader, plans[rank].records, bounds,
			); err != nil {
				return nil, err
			}
		case CommonPrimaryLeafCompact:
			if compactBuilder == nil {
				compactBuilder = NewCompactPrimaryLeafBuilder()
			}
			if _, err := EncodeCommonPrimaryCompactLeaf(
				page.Bytes(), leafHeader, plans[rank].records, bounds, compactBuilder,
			); err != nil {
				return nil, err
			}
		default:
			if _, err := EncodeCommonPrimaryLeaf(
				page.Bytes(), plans[rank].class, leafHeader,
				tx.options.StoreID, plans[rank].records, bounds,
			); err != nil {
				return nil, err
			}
		}
		if placements != nil {
			if err := recordPrimaryPlacements(
				placements, input, plans[rank], BucketID(bucket),
				page.Bytes(), tx.options.StoreID, bounds,
			); err != nil {
				return nil, err
			}
		}
		if err := page.Stage(); err != nil {
			return nil, err
		}
		built[rank] = primaryBuiltLeaf{
			firstKey: input[plans[rank].first].Key,
			lastKey:  input[plans[rank].last-1].Key,
			ref:      page.Ref(),
		}
	}
	return built, nil
}

// recordPrimaryPlacements fills the posting-stable slot for every input row of
// one just-encoded leaf. The template and compact classes assign slot = ordinal
// within the leaf (the lexical rank, which VisitPrimaryLeafPostingRows yields for
// those classes); a succinct leaf resolves the row's stable hash-directory slot
// from the encoded image, so the placement matches exactly what a later lookup
// recovers.
func recordPrimaryPlacements(
	placements []PrimaryGraphPlacement,
	input []PrimaryGraphRecord,
	plan primaryLeafPlan,
	bucket BucketID,
	page []byte,
	storeID [16]byte,
	bounds CommonPrimaryLeafBounds,
) error {
	if plan.class == CommonPrimaryLeafTemplate ||
		plan.class == CommonPrimaryLeafCompact {
		for at := plan.first; at < plan.last; at++ {
			placements[at] = PrimaryGraphPlacement{
				Bucket: bucket, Slot: uint8(at - plan.first),
			}
		}
		return nil
	}
	view := AdmittedCommonPrimaryLeaf(page, storeID, bucket, bounds)
	for at := plan.first; at < plan.last; at++ {
		key := input[at].Key
		slot, _, _, found := view.LookupRawHashed(
			KeyHashBytes(storeID, key), key,
		)
		if !found {
			return fmt.Errorf("%w: primary placement lookup", ErrInvalidWrite)
		}
		placements[at] = PrimaryGraphPlacement{Bucket: bucket, Slot: slot}
	}
	return nil
}

func buildPrimaryTablets(
	tx *WriteTransaction,
	leaves []primaryBuiltLeaf,
) ([]primaryCatalogChild, error) {
	tabletCount := (len(leaves) + TabletLocalIdentityLocalCount - 1) /
		TabletLocalIdentityLocalCount
	if tabletCount > TabletLocalIdentityTabletCount {
		return nil, fmt.Errorf("%w: tablet namespace exhausted", ErrInvalidWrite)
	}
	tablets := make([]primaryCatalogChild, tabletCount)
	var previousTabletMax []byte
	for tabletAt := range tabletCount {
		first := tabletAt * TabletLocalIdentityLocalCount
		last := min(first+TabletLocalIdentityLocalCount, len(leaves))
		tabletLeaves := leaves[first:last]
		tabletID := uint32(tabletAt)
		fences := make([][]byte, len(tabletLeaves))
		routerLeaves := make([]SegmentedTabletRouterLeaf, len(tabletLeaves))
		locatorEntries := make([]GlobalTabletCatalogLocatorEntry, len(tabletLeaves))
		for rank := range tabletLeaves {
			if rank != 0 {
				fence := make([]byte, len(tabletLeaves[rank].firstKey))
				var err error
				fences[rank], err = ShortestPrimaryFence(
					fence, tabletLeaves[rank-1].lastKey,
					tabletLeaves[rank].firstKey,
				)
				if err != nil {
					return nil, err
				}
			}
			localID := uint16(rank)
			routerLeaves[rank] = SegmentedTabletRouterLeaf{
				LocalID: localID, Fence: fences[rank],
				Ref: tabletLeaves[rank].ref,
			}
			locatorEntries[rank] = GlobalTabletCatalogLocatorEntry{
				LocalID: localID,
				PageID:  uint8(rank / SegmentedTabletRouterRowsPerPage),
				RowSlot: uint8(rank % SegmentedTabletRouterRowsPerPage),
				State:   GlobalTabletCatalogLocatorLive,
			}
		}

		pageCount := (len(tabletLeaves) + SegmentedTabletRouterRowsPerPage - 1) /
			SegmentedTabletRouterRowsPerPage
		anchorPages := make([]TransactionPage, pageCount)
		anchorRefs := make([]PageRef, pageCount)
		for pageID := range pageCount {
			logicalID, _ := GlobalTabletCatalogAnchorLogicalID(
				tabletID, uint8(pageID),
			)
			page, err := tx.Allocate(
				PagePrimaryAnchor, SegmentedTabletRouterAnchorPageBytes,
				logicalID,
			)
			if err != nil {
				return nil, err
			}
			anchorPages[pageID] = page
			anchorRefs[pageID] = page.Ref()
		}
		locatorLogical, _ := GlobalTabletCatalogLocatorLogicalID(tabletID)
		locatorPage, err := tx.Allocate(
			PagePrimaryLocator, GlobalTabletCatalogLocatorBytes, locatorLogical,
		)
		if err != nil {
			return nil, err
		}
		routeLogical, _ := GlobalTabletCatalogTabletRootLogicalID(tabletID)
		routePage, err := tx.Allocate(
			PageTabletRoute, GlobalTabletCatalogTabletBytes, routeLogical,
		)
		if err != nil {
			return nil, err
		}

		rawRoot := make([]byte, SegmentedTabletRouterRootBytes)
		rawLocator := make([]byte, SegmentedTabletRouterLocatorBytes)
		rawAnchors := make(
			[]byte, pageCount*SegmentedTabletRouterAnchorPageBytes,
		)
		header := SegmentedTabletRouterHeader{
			StoreID: tx.options.StoreID, TabletID: tabletID,
			Generation: tx.options.Generation,
			AnchorKind: PagePrimaryAnchor, LeafKind: PagePrimaryLeaf,
		}
		if _, _, _, _, err := EncodeSegmentedTabletRouter(
			rawRoot, rawLocator, rawAnchors, header, anchorRefs, routerLeaves,
		); err != nil {
			return nil, err
		}
		for pageID := range anchorPages {
			start := pageID * SegmentedTabletRouterAnchorPageBytes
			copy(
				anchorPages[pageID].Bytes(),
				rawAnchors[start:start+SegmentedTabletRouterAnchorPageBytes],
			)
			if err := anchorPages[pageID].Stage(); err != nil {
				return nil, err
			}
		}

		bounds := primaryCatalogBounds(tx)
		if _, err := EncodeGlobalTabletCatalogLocator(
			locatorPage.Bytes(),
			PageHeader{
				StoreID: tx.options.StoreID, Generation: tx.options.Generation,
				LogicalID: locatorLogical,
				PageSize:  GlobalTabletCatalogLocatorBytes,
				PayloadLength: GlobalTabletCatalogLocatorHeader +
					globalTabletCatalogPackedBytes,
				Kind: PagePrimaryLocator,
			},
			bounds, tabletID, tx.options.Generation, locatorEntries,
		); err != nil {
			return nil, err
		}
		if err := locatorPage.Stage(); err != nil {
			return nil, err
		}
		if _, err := EncodeGlobalTabletCatalogTabletRoot(
			routePage.Bytes(),
			PageHeader{
				StoreID: tx.options.StoreID, Generation: tx.options.Generation,
				LogicalID: routeLogical, PageSize: GlobalTabletCatalogTabletBytes,
				PayloadLength: GlobalTabletCatalogRootHeader +
					SegmentedTabletRouterRootBytes,
				Kind: PageTabletRoute,
			},
			bounds, locatorPage.Ref(), rawRoot,
		); err != nil {
			return nil, err
		}
		if err := routePage.Stage(); err != nil {
			return nil, err
		}

		var floor []byte
		if tabletAt != 0 {
			floor = make([]byte, len(tabletLeaves[0].firstKey))
			floor, err = ShortestPrimaryFence(
				floor, previousTabletMax, tabletLeaves[0].firstKey,
			)
			if err != nil {
				return nil, err
			}
		}
		tablets[tabletAt] = primaryCatalogChild{
			floor: floor, id: tabletID, ref: routePage.Ref(),
		}
		previousTabletMax = tabletLeaves[len(tabletLeaves)-1].lastKey
	}
	return tablets, nil
}

func buildPrimaryCatalog(
	tx *WriteTransaction,
	tablets []primaryCatalogChild,
) (PageRef, error) {
	leafFanout := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogNodeBytes, CommonPrimaryLeafMaxKeyBytes,
	)
	rootFanout := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogRootBytes, CommonPrimaryLeafMaxKeyBytes,
	)
	if leafFanout == 0 || rootFanout == 0 {
		return PageRef{}, fmt.Errorf("%w: primary catalog geometry", ErrInvalidWrite)
	}
	leaves, err := buildPrimaryCatalogLevel(
		tx, GlobalTabletCatalogLeaf, tablets, leafFanout,
	)
	if err != nil {
		return PageRef{}, err
	}
	rootChildren := leaves
	rootChildLevel := GlobalTabletCatalogLeaf
	if len(leaves) > rootFanout {
		rootChildren, err = buildPrimaryCatalogLevel(
			tx, GlobalTabletCatalogBranch, leaves, leafFanout,
		)
		if err != nil {
			return PageRef{}, err
		}
		rootChildLevel = GlobalTabletCatalogBranch
	}
	if len(rootChildren) > rootFanout {
		return PageRef{}, fmt.Errorf("%w: primary catalog root capacity", ErrInvalidWrite)
	}
	rootPage, err := tx.Allocate(
		PagePrimaryCatalog, GlobalTabletCatalogRootBytes,
		GlobalTabletCatalogRootLogicalID,
	)
	if err != nil {
		return PageRef{}, err
	}
	entries := primaryCatalogEntries(rootChildren)
	if _, err := EncodeGlobalTabletCatalogNode(
		rootPage.Bytes(),
		GlobalTabletCatalogNodeHeader{
			StoreID: tx.options.StoreID, Generation: tx.options.Generation,
			LogicalID: GlobalTabletCatalogRootLogicalID,
			Level:     GlobalTabletCatalogRoot, RootChildLevel: rootChildLevel,
			Kind: PagePrimaryCatalog, ChildKind: PagePrimaryCatalog,
			ChildLength: GlobalTabletCatalogNodeBytes,
			Bounds:      primaryCatalogBounds(tx),
		},
		entries,
	); err != nil {
		return PageRef{}, err
	}
	if err := rootPage.Stage(); err != nil {
		return PageRef{}, err
	}
	return rootPage.Ref(), nil
}

func buildPrimaryCatalogLevel(
	tx *WriteTransaction,
	level GlobalTabletCatalogNodeLevel,
	children []primaryCatalogChild,
	fanout int,
) ([]primaryCatalogChild, error) {
	count := (len(children) + fanout - 1) / fanout
	limit := GlobalTabletCatalogMaxLeafPages
	if level == GlobalTabletCatalogBranch {
		limit = GlobalTabletCatalogMaxBranchPages
	}
	if count > limit {
		return nil, fmt.Errorf("%w: primary catalog level capacity", ErrInvalidWrite)
	}
	result := make([]primaryCatalogChild, count)
	for pageID := range count {
		first := pageID * fanout
		last := min(first+fanout, len(children))
		logicalID, ok := GlobalTabletCatalogCatalogLeafLogicalID(uint32(pageID))
		if level == GlobalTabletCatalogBranch {
			logicalID, ok = GlobalTabletCatalogCatalogBranchLogicalID(uint32(pageID))
		}
		if !ok {
			return nil, fmt.Errorf("%w: primary catalog logical ID", ErrInvalidWrite)
		}
		page, err := tx.Allocate(
			PagePrimaryCatalog, GlobalTabletCatalogNodeBytes, logicalID,
		)
		if err != nil {
			return nil, err
		}
		childKind := PageTabletRoute
		childLength := uint32(GlobalTabletCatalogTabletBytes)
		if level == GlobalTabletCatalogBranch {
			childKind = PagePrimaryCatalog
			childLength = GlobalTabletCatalogNodeBytes
		}
		if _, err := EncodeGlobalTabletCatalogNode(
			page.Bytes(),
			GlobalTabletCatalogNodeHeader{
				StoreID: tx.options.StoreID, Generation: tx.options.Generation,
				LogicalID: logicalID, PageID: uint32(pageID), Level: level,
				Kind: PagePrimaryCatalog, ChildKind: childKind,
				ChildLength: childLength, Bounds: primaryCatalogBounds(tx),
			},
			primaryCatalogEntries(children[first:last]),
		); err != nil {
			return nil, err
		}
		if err := page.Stage(); err != nil {
			return nil, err
		}
		result[pageID] = primaryCatalogChild{
			floor: children[first].floor, id: uint32(pageID), ref: page.Ref(),
		}
	}
	return result, nil
}

func primaryCatalogEntries(
	children []primaryCatalogChild,
) []GlobalTabletCatalogNodeEntry {
	entries := make([]GlobalTabletCatalogNodeEntry, len(children))
	for at := range children {
		entries[at] = GlobalTabletCatalogNodeEntry{
			ID: children[at].id, Ref: children[at].ref,
		}
		if at != 0 {
			entries[at].Floor = children[at].floor
		}
	}
	return entries
}

func primaryCatalogBounds(tx *WriteTransaction) GlobalTabletCatalogBounds {
	// The catalog codec's admission context reserves room for the eventual
	// 64 KiB root. Tiny graphs can stage their tablet pages before physical
	// FileEnd reaches that size; the bottom-up build always allocates the root
	// before publication, so this is the exact prospective lower bound.
	fileEnd := max(tx.fileEnd, uint64(GlobalTabletCatalogRootBytes))
	return GlobalTabletCatalogBounds{
		StoreID:                tx.options.StoreID,
		SelectedRootGeneration: tx.options.Generation,
		FileEnd:                fileEnd,
		NextLogicalID:          tx.nextID,
	}
}
