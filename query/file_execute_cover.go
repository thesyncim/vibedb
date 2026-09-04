package query

import (
	"fmt"
	"math/bits"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

type directFileIndexStats struct {
	rows         uint64
	rechecks     uint64
	certificates uint64
	lookups      int
	postingPages int
	chunks       int
	bounded      bool
}

type directFileTokenStats struct {
	scanned  uint64
	token    uint64
	fallback uint64
}

// runDirectFileIndexedCount recognizes COUNT(*) over a predicate whose entire
// truth set is covered by persistent exact indexes. The exact probe performs
// collision rechecks once, after which popcount answers the aggregate without
// rebuilding Segments, extracting columns, or evaluating @> a second time.
//
// An object containment needle made entirely of scalar leaves is eligible
// because compilation proves it equivalent to exact nested path equalities.
// A matching compound index can certify the complete conjunction in one
// probe. Arrays and empty objects retain the structural containment evaluator.
func (p *plan) runDirectFileIndexedCount(
	snapshot *durable.Snapshot,
	e *Exec,
	memoryBytes int64,
) (directFileIndexStats, bool, error) {
	if err := e.Workspace.checkCanceled(); err != nil {
		return directFileIndexStats{}, true, err
	}
	if p.where == nil || p.grouped || !p.singleRow {
		return directFileIndexStats{}, false, nil
	}
	if p.requiresSQLDomainScan() {
		return directFileIndexStats{}, false, nil
	}
	for _, column := range p.columns {
		if column.agg != aggCount || column.value >= 0 {
			return directFileIndexStats{}, false, nil
		}
	}
	if p.hasLimit && p.limit == 0 {
		return directFileIndexStats{}, true, prepareResult(&e.Result, p, 0)
	}
	masks, rechecks, certificates, postingPages, exact, err :=
		p.fileExactCandidateMasksBounded(
			snapshot, &e.file.index, &e.Workspace, memoryBytes,
		)
	if err != nil {
		return directFileIndexStats{
			rechecks: rechecks, certificates: certificates,
			postingPages: postingPages, bounded: true,
		}, true, err
	}
	if err := e.Workspace.checkCanceled(); err != nil {
		return directFileIndexStats{
			rechecks: rechecks, certificates: certificates,
			postingPages: postingPages, bounded: true,
		}, true, err
	}
	if !exact {
		return directFileIndexStats{}, false, nil
	}
	var rows uint64
	chunks := 0
	for i, mask := range masks {
		if err := cancellationCheckpoint(e.Options.Cancel, i); err != nil {
			return directFileIndexStats{
				rows: rows, rechecks: rechecks, certificates: certificates,
				lookups: e.Workspace.storeIndexProbes, postingPages: postingPages,
				chunks: chunks, bounded: true,
			}, true, err
		}
		count := bits.OnesCount64(mask.Bits)
		if count == 0 {
			continue
		}
		rows += uint64(count)
		chunks++
	}
	if rows > uint64(^uint(0)>>1) {
		return directFileIndexStats{
			rows: rows, rechecks: rechecks, certificates: certificates,
			lookups: e.Workspace.storeIndexProbes, postingPages: postingPages,
			chunks: chunks, bounded: true,
		}, true, store.ErrTooLarge
	}
	e.file.accs = resize(e.file.accs, len(p.columns))
	resetAggs(e.file.accs)
	for i := range e.file.accs {
		e.file.accs[i].count = int(rows)
	}
	if err := prepareResult(&e.Result, p, 1); err != nil {
		return directFileIndexStats{
			rows: rows, rechecks: rechecks, certificates: certificates,
			lookups: e.Workspace.storeIndexProbes, postingPages: postingPages,
			chunks: chunks, bounded: true,
		}, true, err
	}
	if err := p.fillAggregateCells(&e.Result, 0, e.file.accs, nil, &e.Workspace); err != nil {
		return directFileIndexStats{
			rows: rows, rechecks: rechecks, certificates: certificates,
			lookups: e.Workspace.storeIndexProbes, postingPages: postingPages,
			chunks: chunks, bounded: true,
		}, true, err
	}
	return directFileIndexStats{
		rows: rows, rechecks: rechecks, certificates: certificates,
		lookups: e.Workspace.storeIndexProbes, postingPages: postingPages,
		chunks: chunks, bounded: true,
	}, true, nil
}

// runDirectFileAggregate recognizes unfiltered COUNT(*). Persistent numeric
// covers contain float64 reductions and are deliberately not authoritative for
// exact SQL SUM/AVG/MIN/MAX.
func (p *plan) runDirectFileAggregate(
	snapshot *durable.Snapshot,
	e *Exec,
) (coveringColumns int, handled bool, err error) {
	if err := e.Workspace.checkCanceled(); err != nil {
		return 0, true, err
	}
	if p.where != nil || p.grouped || !p.singleRow {
		return 0, false, nil
	}
	if p.hasLimit && p.limit == 0 {
		return 0, true, prepareResult(&e.Result, p, 0)
	}
	for _, column := range p.columns {
		if column.agg != aggCount || column.value >= 0 {
			return 0, false, nil
		}
	}
	if snapshot.Len() > uint64(^uint(0)>>1) {
		return 0, true, store.ErrTooLarge
	}

	e.file.accs = resize(e.file.accs, len(p.columns))
	resetAggs(e.file.accs)
	for resultColumn := range p.columns {
		e.file.accs[resultColumn].count = int(snapshot.Len())
	}

	if err := prepareResult(&e.Result, p, 1); err != nil {
		return 0, true, err
	}
	if err := p.fillAggregateCells(&e.Result, 0, e.file.accs, nil, &e.Workspace); err != nil {
		return 0, true, err
	}
	return 0, true, nil
}

func durableIntegerOrder(op Op) (durable.IntegerOrder, bool) {
	switch op {
	case Lt:
		return durable.IntegerLess, true
	case Le:
		return durable.IntegerLessEqual, true
	case Gt:
		return durable.IntegerGreater, true
	case Ge:
		return durable.IntegerGreaterEqual, true
	default:
		return 0, false
	}
}

// runDirectFileTokenIntegerOrderCount answers the narrow unindexed integer
// COUNT shape from durable FOR streams. Eligibility is intentionally strict:
// only a plain integer literal and one ordered comparison can enter, and the
// durable lane declines the whole snapshot if any present target stream is
// not exact FOR. The generic executor remains authoritative for every other
// numeric domain and for mixed or mutable storage.
func (p *plan) runDirectFileTokenIntegerOrderCount(
	snapshot *durable.Snapshot,
	e *Exec,
) (directFileTokenStats, bool, error) {
	if err := e.Workspace.checkCanceled(); err != nil {
		return directFileTokenStats{}, true, err
	}
	path, lit, queryOp, ok := p.scalarCountIntegerOrderPath()
	if !ok || e.Options.Cancel != nil {
		return directFileTokenStats{}, false, nil
	}
	op, ok := durableIntegerOrder(queryOp)
	if !ok {
		return directFileTokenStats{}, false, nil
	}
	storagePath := path.indexPath()
	if storagePath == "" || storagePath == "/" {
		// UnifiedHoleResolver addresses named fields below the document root. A
		// root or empty-name comparison retains the generic path program.
		return directFileTokenStats{}, false, nil
	}
	if p.hasLimit && p.limit == 0 {
		return directFileTokenStats{}, true, prepareResult(&e.Result, p, 0)
	}
	if snapshot.Len() > uint64(^uint(0)>>1) {
		return directFileTokenStats{}, true, store.ErrTooLarge
	}
	filter, err := e.file.tokenIntegerOrderFilterFor(
		storagePath, lit.ival, op,
	)
	if err != nil {
		return directFileTokenStats{}, true, err
	}
	filtered, err := snapshot.FilterIntegerOrderCount(filter)
	if err != nil {
		return directFileTokenStats{}, true, err
	}
	if !filtered.Supported {
		return directFileTokenStats{}, false, nil
	}
	if filtered.Scanned != int(snapshot.Len()) ||
		filtered.Matched < 0 || filtered.Matched > filtered.Scanned {
		return directFileTokenStats{}, true, fmt.Errorf(
			"query: durable integer filter returned invalid progress: scanned=%d matched=%d rows=%d",
			filtered.Scanned, filtered.Matched, snapshot.Len(),
		)
	}
	stats := directFileTokenStats{
		scanned: uint64(filtered.Scanned), token: uint64(filtered.Scanned),
	}
	e.file.accs = resize(e.file.accs, len(p.columns))
	resetAggs(e.file.accs)
	for i := range e.file.accs {
		e.file.accs[i].count = filtered.Matched
	}
	if err := prepareResult(&e.Result, p, 1); err != nil {
		return stats, true, err
	}
	if err := p.fillAggregateCells(
		&e.Result, 0, e.file.accs, nil, &e.Workspace,
	); err != nil {
		return stats, true, err
	}
	return stats, true, nil
}

// runDirectFileTokenScalarCount answers the common unindexed
// COUNT(*) WHERE field = scalar shape by scanning durable leaf tokens in
// storage order. This is not candidate pruning: FilterEqCount visits every
// live row, resolves a field once per leaf template, and reports each row that
// had to fall back to canonical document rendering.
//
// The storage token comparator has exact query equality semantics for strings,
// booleans, and numbers, including equivalent decimal spellings without
// float64 rounding. A configured cancellation flag declines this lane until
// the storage cursor exposes cooperative checkpoints.
func (p *plan) runDirectFileTokenScalarCount(
	snapshot *durable.Snapshot,
	e *Exec,
) (directFileTokenStats, bool, error) {
	if err := e.Workspace.checkCanceled(); err != nil {
		return directFileTokenStats{}, true, err
	}
	path, lit, ok := p.scalarCountEqualityPath()
	if !ok || e.Options.Cancel != nil ||
		(lit.kind != kindString && lit.kind != kindBool && lit.kind != kindNumber) {
		return directFileTokenStats{}, false, nil
	}
	if p.hasLimit && p.limit == 0 {
		return directFileTokenStats{}, true, prepareResult(&e.Result, p, 0)
	}
	if snapshot.Len() > uint64(^uint(0)>>1) {
		return directFileTokenStats{}, true, store.ErrTooLarge
	}
	filter, err := e.file.tokenEqFilterFor(path.indexPath(), p.where.needle.Src)
	if err != nil {
		return directFileTokenStats{}, true, err
	}
	filtered, err := snapshot.FilterEqCount(filter)
	if err != nil {
		return directFileTokenStats{}, true, err
	}
	if filtered.Scanned != int(snapshot.Len()) ||
		filtered.Fallback < 0 || filtered.Fallback > filtered.Scanned ||
		filtered.Matched < 0 || filtered.Matched > filtered.Scanned {
		return directFileTokenStats{}, true, fmt.Errorf(
			"query: durable token filter returned invalid progress: scanned=%d fallback=%d matched=%d rows=%d",
			filtered.Scanned, filtered.Fallback, filtered.Matched, snapshot.Len(),
		)
	}
	stats := directFileTokenStats{
		scanned:  uint64(filtered.Scanned),
		token:    uint64(filtered.Scanned - filtered.Fallback),
		fallback: uint64(filtered.Fallback),
	}

	e.file.accs = resize(e.file.accs, len(p.columns))
	resetAggs(e.file.accs)
	for i := range e.file.accs {
		e.file.accs[i].count = filtered.Matched
	}
	if err := prepareResult(&e.Result, p, 1); err != nil {
		return stats, true, err
	}
	if err := p.fillAggregateCells(
		&e.Result, 0, e.file.accs, nil, &e.Workspace,
	); err != nil {
		return stats, true, err
	}
	return stats, true, nil
}
