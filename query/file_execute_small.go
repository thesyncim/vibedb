package query

import (
	"errors"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

const fileSmallMaxRows = 256

var errFileSmallDeclined = errors.New("query: small candidate batch exceeded byte target")

// Small indexed reads run on the caller. A certified primary order lets each
// batch go straight into the result and stops scanning at LIMIT. An unordered
// candidate set fits in one batch, so its ordinary sort remains authoritative.
// Large candidate documents decline before evaluation and use the spill path.
type fileSmallScan struct {
	work    Workspace
	docs    store.Segment
	slots   [1]fileSlot
	arena   fileArena
	row     func([]byte, []byte) error
	p       *plan
	e       *Exec
	stats   ExecStats
	opts    normalizedFileOptions
	batch   fileBatch
	ordered bool
	payload int64
}

func (p *plan) runFileSmall(e *Exec, snapshot *durable.Snapshot, span *FileRangeSource, masks []store.Mask, opts normalizedFileOptions, stats *ExecStats, ordered bool) (bool, error) {
	if e.file.small == nil {
		e.file.small = &fileSmallScan{}
		e.file.small.row = e.file.small.appendRow
	}
	s := e.file.small
	s.p, s.e, s.stats, s.opts, s.ordered, s.payload = p, e, *stats, opts, ordered, 0
	s.work.heapWorkParent = &e.Workspace.heapWorkBudget
	s.work.heapWorkTextReserved = 0
	s.work.cancel = e.Options.Cancel
	s.work.eval.setWork(&e.Workspace.heapWorkBudget)
	s.work.eval.bindTo(nil)
	s.work.eval.bindMarks(nil)
	s.work.eval.bindCorrelations(e.Workspace.correlations)
	s.batch = takeFileBatch(s.slots[:], 0, 0)
	defer func() {
		s.slots[0].batch = s.batch
		clear(s.arena.heads) // borrowed batch values must not retain old arenas
		*stats = s.stats
		s.p, s.e = nil, nil
		s.work.heapWorkParent = nil
		s.work.eval.setWork(nil)
		s.work.eval.bindCorrelations(nil)
		s.work.cancel = nil
	}()
	if err := prepareResult(&e.Result, p, 0); err != nil {
		return true, err
	}
	var err error
	if span != nil && masks != nil {
		e.file.overflow, err = snapshot.RangeMasksBoundsRawBuffer(masks, span.lower, span.upper, e.file.overflow[:0], span.lowerExclusive, s.row)
	} else if span != nil {
		e.file.overflow, err = snapshot.RangeBoundsRawBuffer(span.lower, span.upper, e.file.overflow[:0], span.lowerExclusive, s.row)
	} else {
		e.file.overflow, err = snapshot.RangeMasksRawBuffer(masks, e.file.overflow[:0], s.row)
	}
	if errors.Is(err, errFileSmallDeclined) {
		// No batch has been evaluated on this lane before its complete candidate
		// set fits, so falling back cannot hide predicate errors or partial output.
		s.stats.RowsScanned, s.stats.Batches, s.stats.PeakBatchRows, s.stats.PeakBatchBytes, s.stats.BufferedBytes = 0, 0, 0, 0, 0
		return false, nil
	}
	if errors.Is(err, errFileExecutionStopped) {
		err = nil
	} else if err == nil {
		err = s.flush()
	}
	if errors.Is(err, errFileExecutionStopped) {
		err = nil
	}
	s.stats.Workers = 1
	if err == nil {
		err = s.work.checkCanceled()
	}
	return true, err
}

// runValidatedRawInto executes one storage-validated point document without
// rebuilding it into a Segment. The same raw-row evaluator used by small
// durable ranges keeps the complete predicate authoritative; its Segment
// fallback covers complex paths and uncommon JSON roots.
func (p *plan) runValidatedRawInto(e *Exec, raw []byte) error {
	if e.file.small == nil {
		e.file.small = &fileSmallScan{}
		e.file.small.row = e.file.small.appendRow
	}
	if p.grouped || p.hasAggregate {
		docs := &e.file.small.docs
		docs.Reset()
		if _, err := docs.Append(raw); err != nil {
			return err
		}
		e.Stats = ExecStats{}
		return p.runInto(&e.Result, docs, &e.Workspace, e.Options.Workers)
	}
	s := e.file.small
	s.p, s.e, s.stats, s.opts, s.ordered, s.payload = p, e, ExecStats{}, normalizedFileOptions{}, false, 0
	s.work.heapWorkParent = &e.Workspace.heapWorkBudget
	s.work.heapWorkTextReserved = 0
	s.work.cancel = e.Options.Cancel
	s.work.eval.setWork(&e.Workspace.heapWorkBudget)
	s.work.eval.bindTo(nil)
	s.work.eval.bindMarks(nil)
	s.work.eval.bindCorrelations(e.Workspace.correlations)
	s.batch = takeFileBatch(s.slots[:], 0, 0)
	defer func() {
		s.slots[0].batch = s.batch
		clear(s.arena.heads)
		e.Stats = s.stats
		s.p, s.e = nil, nil
		s.work.heapWorkParent = nil
		s.work.eval.setWork(nil)
		s.work.eval.bindCorrelations(nil)
		s.work.cancel = nil
	}()
	if err := prepareResult(&e.Result, p, 0); err != nil {
		return err
	}
	s.batch.data = raw
	s.batch.ends = append(s.batch.ends, len(raw))
	s.batch.bytes = int64(len(raw))
	s.stats = ExecStats{
		Workers: 1, RowsTotal: 1, RowsScanned: 1, CandidateRows: 1,
		PeakBatchRows: 1, PeakBatchBytes: s.batch.bytes,
	}
	err := s.flush()
	if err == nil {
		err = s.work.checkCanceled()
	}
	return err
}

func (s *fileSmallScan) appendRow(_, value []byte) error {
	if err := s.work.checkCanceled(); err != nil {
		return err
	}
	// A secondary candidate batch must fit as a whole because its storage order
	// need not agree with ORDER BY. Preserve the general executor for large rows.
	if !s.ordered && s.batch.bytes+int64(len(value)) > s.opts.batchBytes {
		return errFileSmallDeclined
	}
	if err := reserveFileBatchRow(&s.batch, &s.slots[0], len(value)); err != nil {
		return err
	}
	s.batch.data = append(s.batch.data, value...)
	s.batch.ends = append(s.batch.ends, len(s.batch.data))
	s.batch.bytes += int64(len(value))
	s.stats.RowsScanned++
	s.stats.PeakBatchRows = max(s.stats.PeakBatchRows, len(s.batch.ends))
	s.stats.PeakBatchBytes = max(s.stats.PeakBatchBytes, s.batch.bytes)
	if s.ordered && (len(s.batch.ends) >= min(s.opts.batchRows, s.p.limit-s.e.Result.RowCount) || s.batch.bytes >= s.opts.batchBytes) {
		return s.flush()
	}
	return nil
}

func (s *fileSmallScan) flush() error {
	if len(s.batch.ends) == 0 {
		return nil
	}
	clear(s.arena.heads)
	s.arena.rewind()
	mode := filePartialBorrowed
	if s.ordered {
		mode = filePartialOrdered
	}
	part := s.p.makeFilePartial(s.batch, &s.work, &s.docs, s.slots[:], &s.arena, &s.e.Workspace.aggregateBudget, mode)
	s.stats.Batches++
	s.stats.BufferedBytes = max(s.stats.BufferedBytes, part.bytes)
	if part.err != nil {
		return part.err
	}
	if s.ordered {
		result := &s.e.Result
		for i, row := range part.rows {
			if err := cancellationCheckpoint(s.work.cancel, i); err != nil {
				return err
			}
			payload, ok := fileResultPayloadBytes(row)
			if !ok {
				return result.resultByteBudgetError(result.RowCount+1, int64(^uint64(0)>>1))
			}
			s.payload = saturatedBytes(s.payload, payload)
			required, err := result.checkResultBudget(len(s.p.columns), result.RowCount+1, s.payload)
			if err != nil {
				return err
			}
			result.resultBytesUsed = required
			for col := range s.p.columns {
				cell := result.ownFileCell(cellFromScalar(row.values[col]))
				result.Columns[col].Cells = append(result.Columns[col].Cells, cell)
			}
			result.RowCount++
			if result.RowCount == s.p.limit {
				return errFileExecutionStopped
			}
		}
	} else {
		var err error
		s.e.Result, err = s.p.fileRowResultInto(s.e.Result, part.rows, s.e.Options.Cancel)
		if err != nil {
			return err
		}
	}
	s.batch.base += uint64(len(s.batch.ends))
	s.batch.data, s.batch.ends, s.batch.bytes = s.batch.data[:0], s.batch.ends[:0], 0
	return nil
}

// makeFileRawRowsPartial runs the synchronous small-page lane directly over
// validated JSON bytes when every referenced path is one top-level member. It
// preserves the complete compiled predicate; only the temporary Segment and
// its structural tape are omitted. Unsupported roots and escaped object keys
// decline before any result row is published, so the ordinary path remains
// authoritative for the full JSON Pointer surface.
func (p *plan) makeFileRawRowsPartial(
	batch fileBatch,
	w *Workspace,
	slot *fileSlot,
	arena *fileArena,
	mode filePartialMode,
) (filePartial, bool) {
	part := filePartial{seq: batch.seq}
	if len(p.valuePaths) == 0 {
		return part, false
	}
	for _, path := range p.valuePaths {
		if _, ok := rawTopLevelPathName(path); !ok {
			return part, false
		}
	}

	rows := len(batch.ends)
	w.raws = resize(w.raws, len(p.valuePaths))
	for col := range p.valuePaths {
		raws := resize(w.raws[col], rows)
		clear(raws)
		w.raws[col] = raws
	}
	start := 0
	for row, end := range batch.ends {
		if err := cancellationCheckpoint(w.cancel, row); err != nil {
			part.err = err
			return part, true
		}
		if !rawTopLevelScalars(batch.data[start:end], p.valuePaths, w.raws, row) {
			return filePartial{}, false
		}
		start = end
	}

	ctx := &w.ctx
	ctx.s, ctx.rows = nil, rows
	ctx.values = resize(ctx.values, len(p.valuePaths))
	classify := func(cols []int, text *[]byte) error {
		need := 0
		for _, col := range cols {
			escaped, err := escapedTextBytesCancelable(w.raws[col], w.cancel)
			if err != nil {
				return err
			}
			need += escaped
		}
		return ctx.classifyColumns(cols, text, need, w)
	}
	w.text, w.lateText = w.text[:0], w.lateText[:0]
	if err := classify(p.filterCols, &w.text); err != nil {
		part.err = err
		return part, true
	}
	if err := classify(p.lateCols, &w.lateText); err != nil {
		part.err = err
		return part, true
	}
	selected, err := p.selectRows(ctx, nil, false, w)
	if err != nil {
		part.err = err
		return part, true
	}
	if err := w.eval.firstError(); err != nil {
		part.err = err
		return part, true
	}
	return p.makeFileRowsPartial(part, selected, ctx, batch, slot, arena, mode, w), true
}

func (p *plan) makeFileRowsPartial(
	part filePartial,
	selected []int,
	ctx *execCtx,
	batch fileBatch,
	slot *fileSlot,
	arena *fileArena,
	mode filePartialMode,
	w *Workspace,
) filePartial {
	if len(p.order) != 0 && mode != filePartialOrdered {
		if err := w.checkCanceled(); err != nil {
			part.err = err
			return part
		}
		slices.SortStableFunc(selected, func(a, b int) int { return p.compareRows(ctx, a, b) })
		if err := w.checkCanceled(); err != nil {
			part.err = err
			return part
		}
	}
	if p.hasLimit && len(selected) > p.limit {
		selected = selected[:p.limit]
	}
	// Reserve the whole batch's scalar-header slab before handing out the
	// first row. Growing it a row at a time leaves every superseded array live
	// through the rows already pointing into it.
	order := p.order
	if mode != filePartialDetached {
		order = nil
	}
	headsPerRow := len(p.columns) + len(order)
	if headsPerRow != 0 && len(selected) > int(^uint(0)>>1)/headsPerRow {
		part.err = fmt.Errorf("query: durable result scalar arena overflows int")
		return part
	}
	if err := arena.reserveHeads(len(selected) * headsPerRow); err != nil {
		part.err = err
		return part
	}
	prev := slot.rows
	rows := prev[:0]
	for at, row := range selected {
		if err := cancellationCheckpoint(w.cancel, at); err != nil {
			part.err = err
			return part
		}
		r := fileRow{ordinal: batch.base + uint64(row)}
		var used int64
		var arenaErr error
		if mode != filePartialDetached {
			r.values, arenaErr = arena.takeHeads(len(p.columns))
			if arenaErr != nil {
				part.err = arenaErr
				return part
			}
			for col := range p.columns {
				r.values[col] = scalar{}
				if p.columns[col].value >= 0 {
					r.values[col] = ctx.values[p.columns[col].value][row]
				}
				used += scalarBytes(r.values[col])
			}
		} else {
			r.values, used, arenaErr = ownScalars(arena, ctx, row, p.columns, order, nil)
		}
		if arenaErr != nil {
			part.err = arenaErr
			return part
		}
		r.order = r.values[len(p.columns):len(r.values):len(r.values)]
		r.values = r.values[:len(p.columns):len(p.columns)]
		part.bytes += fileRowStructBytes + used
		rows = append(rows, r)
	}
	clearTail(prev, len(rows))
	slot.rows, part.rows = rows, rows
	return part
}
