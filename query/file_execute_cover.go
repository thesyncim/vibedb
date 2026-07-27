package query

import (
	"math/bits"

	"github.com/thesyncim/vibejson"
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

type directFileGroupStats struct {
	rechecks     uint64
	certificates uint64
	lookups      int
	postingPages int
	groups       int
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
) (directFileIndexStats, bool, error) {
	if p.where == nil || p.grouped || !p.singleRow {
		return directFileIndexStats{}, false, nil
	}
	for _, column := range p.columns {
		if column.agg != aggCount || column.value >= 0 {
			return directFileIndexStats{}, false, nil
		}
	}
	if p.hasLimit && p.limit == 0 {
		prepareResult(&e.Result, p, 0)
		return directFileIndexStats{}, true, nil
	}
	masks, rechecks, certificates, postingPages, exact, err := p.fileExactCandidateMasks(
		snapshot, &e.file.index, &e.Workspace,
	)
	if err != nil {
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
	for _, mask := range masks {
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
	clear(e.file.accs)
	for i := range e.file.accs {
		e.file.accs[i].count = int(rows)
	}
	prepareResult(&e.Result, p, 1)
	p.fillAggregateCells(&e.Result, 0, e.file.accs, nil, &e.Workspace)
	return directFileIndexStats{
		rows: rows, rechecks: rechecks, certificates: certificates,
		lookups: e.Workspace.storeIndexProbes, postingPages: postingPages,
		chunks: chunks, bounded: true,
	}, true, nil
}

// runDirectFileAggregate recognizes an unfiltered scalar aggregate whose
// numeric inputs all have persistent typed covers. It bypasses worker startup,
// JSON admission, parsing, and per-row validity columns. COUNT(path) declines
// because a numeric cover intentionally omits present non-numeric values.
func (p *plan) runDirectFileAggregate(
	snapshot *durable.Snapshot,
	e *Exec,
) (coveringColumns int, handled bool, err error) {
	if p.where != nil || p.grouped || !p.singleRow {
		return 0, false, nil
	}
	for _, column := range p.columns {
		switch column.agg {
		case aggCount:
			if column.value >= 0 {
				return 0, false, nil
			}
		case aggSum, aggAvg, aggMin, aggMax:
			if column.num < 0 || column.num >= len(p.numPaths) {
				return 0, false, nil
			}
		default:
			return 0, false, nil
		}
	}
	if p.hasLimit && p.limit == 0 {
		prepareResult(&e.Result, p, 0)
		return 0, true, nil
	}
	if snapshot.Len() > uint64(^uint(0)>>1) {
		return 0, true, store.ErrTooLarge
	}

	e.file.reductions = resize(e.file.reductions, len(p.numPaths))
	clear(e.file.reductions)
	e.file.coverPaths = resize(e.file.coverPaths, len(p.numPaths))
	for i, path := range p.numPaths {
		e.file.coverPaths[i] = path.indexPath()
	}
	covered, reduceErr := snapshot.ReduceFloat64PathsInto(e.file.reductions, e.file.coverPaths)
	if reduceErr != nil {
		return 0, true, reduceErr
	}
	if !covered {
		return 0, false, nil
	}
	coveringColumns = len(p.numPaths)
	e.file.accs = resize(e.file.accs, len(p.columns))
	clear(e.file.accs)
	for resultColumn, column := range p.columns {
		if column.agg == aggCount {
			e.file.accs[resultColumn].count = int(snapshot.Len())
			continue
		}
		reduction := e.file.reductions[column.num]
		e.file.accs[resultColumn] = aggAcc{
			n: reduction.Count, sum: reduction.Sum,
			min: reduction.Min, max: reduction.Max,
		}
	}

	prepareResult(&e.Result, p, 1)
	p.fillAggregateCells(&e.Result, 0, e.file.accs, nil, &e.Workspace)
	return coveringColumns, true, nil
}

// runDirectFileIndexGroups lowers an unfiltered scalar COUNT GROUP BY to a
// matching single-column exact index. A compact generation can answer from
// bounded O(groups) aggregate catalog pages. Otherwise stable-slot postings
// and their exact representatives decide certified rows; missing, container,
// legacy, and collision-marked rows form a residual bitmap and take the
// ordinary compiled-pointer path. Both lanes preserve exact group semantics
// and first-row ordering.
func (p *plan) runDirectFileIndexGroups(
	snapshot *durable.Snapshot,
	e *Exec,
) (directFileGroupStats, bool, error) {
	if p.where != nil || !p.grouped || len(p.groupCols) != 1 {
		return directFileGroupStats{}, false, nil
	}
	for _, column := range p.columns {
		switch {
		case column.agg == aggNone && column.slot == 0:
		case column.agg == aggCount && column.value < 0:
		default:
			return directFileGroupStats{}, false, nil
		}
	}
	if snapshot.Len() > uint64(^uint(0)>>1) {
		return directFileGroupStats{}, true, store.ErrTooLarge
	}
	path := p.valuePaths[p.groupCols[0]]
	indexes := snapshot.AppendIndexes(e.Workspace.storeIndexes[:0])
	e.Workspace.storeIndexes = indexes
	indexName := ""
	for _, index := range indexes {
		if index.Kind == store.IndexExact &&
			index.State == store.IndexReady &&
			index.ColumnCount == 1 &&
			index.Columns[0] == path.indexPath() {
			indexName = index.Name
			break
		}
	}
	if indexName == "" {
		return directFileGroupStats{}, false, nil
	}
	if p.hasLimit && p.limit == 0 {
		prepareResult(&e.Result, p, 0)
		return directFileGroupStats{lookups: 1}, true, nil
	}

	e.file.indexGroups = e.file.indexGroups[:0]
	e.file.indexResidual = e.file.indexResidual[:0]
	var ok bool
	var err error
	e.file.indexGroups, e.file.indexResidual, ok, err =
		snapshot.AppendIndexScalarGroupsInto(
			e.file.indexGroups, e.file.indexResidual, &e.file.index, indexName,
		)
	probe := e.file.index.LastProbeStats()
	stats := directFileGroupStats{
		certificates: probe.CertificateRows,
		lookups:      1,
		postingPages: probe.PostingPages,
	}
	if err != nil {
		return stats, true, err
	}
	if !ok {
		return directFileGroupStats{}, false, nil
	}

	e.Workspace.groupKey = e.Workspace.groupKey[:0]
	e.Workspace.text = e.Workspace.text[:0]
	e.Workspace.interner.Reset()
	groupCount := 0
	add := func(value scalar, count int, first uint64) {
		e.Workspace.groupKey = appendGroupKey(e.Workspace.groupKey[:0], value)
		id := int(e.Workspace.interner.Intern(e.Workspace.groupKey))
		if id == groupCount {
			e.file.fileGroups = resize(e.file.fileGroups, groupCount+1)
			group := &e.file.fileGroups[id]
			group.key = ""
			group.scalars = resize(group.scalars, 1)
			// The scalar borrows the file execution workspace only until
			// fileGroupResultInto packs it into caller-owned Result storage.
			group.scalars[0] = value
			group.accs = resize(group.accs, len(p.columns))
			clear(group.accs)
			group.first = first
			group.bytes = 0
			groupCount++
		} else if first < e.file.fileGroups[id].first {
			e.file.fileGroups[id].first = first
		}
		for column, planColumn := range p.columns {
			if planColumn.agg == aggCount {
				e.file.fileGroups[id].accs[column].count += count
			}
		}
	}
	for _, group := range e.file.indexGroups {
		add(
			classifyRawInto(group.Value, &e.Workspace.text),
			int(group.Count), group.First,
		)
	}

	e.file.overflow, err = snapshot.RangeMasksRawRowsBuffer(
		e.file.indexResidual, e.file.overflow[:0],
		func(row store.Location, _ []byte, document []byte) error {
			raw, found, pointerErr := path.pointerForStore().GetRaw(document)
			if pointerErr != nil {
				return pointerErr
			}
			if !found {
				raw = vibejson.RawValue{}
			}
			add(
				classifyRawInto(raw, &e.Workspace.text), 1,
				uint64(row.Chunk)<<6|uint64(row.Slot),
			)
			stats.rechecks++
			return nil
		},
	)
	if err != nil {
		return stats, true, err
	}
	e.file.fileGroups = e.file.fileGroups[:groupCount]
	stats.groups = groupCount
	e.Result = p.fileGroupResultInto(e.Result, e.file.fileGroups)
	return stats, true, nil
}
