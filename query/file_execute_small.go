package query

import (
	"errors"

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
