package query

import "unsafe"

// ResetReadForReuse releases execution state while retaining bounded, scrubbed
// result arrays and scalar scan buffers. It is only valid after the cursor has
// closed. No source, plan, cancellation hook, or document view survives.
func (e *Exec) ResetReadForReuse(maxBytes int64) {
	if e == nil {
		return
	}
	result := e.Result
	e.Result = Result{}
	result.ResetForReuse(maxBytes)
	small := e.file.small
	if small != nil {
		bytes := smallReadReuseBytes(small)
		if bytes < 0 || bytes > maxBytes-result.ReuseCapacityBytes() {
			small = nil
		}
	}
	if small != nil {
		// Keep only these explicitly owned arrays. Scrub their full capacity,
		// including columns left behind by a previous, wider execution.
		raws := scrubReadReuseColumns(small.work.raws)
		values := scrubReadReuseColumns(small.work.ctx.values)
		selected := scrubReadReuseSlice(small.work.selected)
		text := scrubReadReuseSlice(small.work.text)
		lateText := scrubReadReuseSlice(small.work.lateText)
		ends := scrubReadReuseSlice(small.slots[0].batch.ends)
		small.releaseFileProjection()
		small.work.Release()
		row := small.row // method value owns only this same scan object.
		*small = fileSmallScan{row: row}
		small.work.raws, small.work.ctx.values = raws, values
		small.work.selected, small.work.text, small.work.lateText = selected, text, lateText
		small.slots[0].batch.ends = ends
		e.file.small = nil
	}
	e.Release()
	*e = Exec{Result: result}
	e.file.small = small
}

// ReadReuseCapacityBytes reports retained dynamic capacity after
// ResetReadForReuse, excluding the embedded Exec value.
func (e *Exec) ReadReuseCapacityBytes() int64 {
	if e == nil {
		return 0
	}
	return readReuseAdd(e.Result.ReuseCapacityBytes(), smallReadReuseBytes(e.file.small))
}

func scrubReadReuseColumns[T any](columns [][]T) [][]T {
	all := columns[:cap(columns)]
	for i := range all {
		all[i] = scrubReadReuseSlice(all[i])
	}
	return columns[:0]
}

func smallReadReuseBytes(s *fileSmallScan) int64 {
	if s == nil {
		return 0
	}
	// Include the method-value closure in addition to the scan object.
	total := int64(unsafe.Sizeof(*s)) + 32
	for _, size := range []int64{
		readReuseColumnBytes(s.work.raws), readReuseColumnBytes(s.work.ctx.values),
		readReuseStatementSliceBytes(s.work.selected),
		readReuseStatementSliceBytes(s.work.text), readReuseStatementSliceBytes(s.work.lateText),
		readReuseStatementSliceBytes(s.slots[0].batch.ends),
	} {
		total = readReuseAdd(total, size)
	}
	return total
}

func readReuseColumnBytes[T any](columns [][]T) int64 {
	total := readReuseStatementSliceBytes(columns)
	for _, column := range columns[:cap(columns)] {
		total = readReuseAdd(total, readReuseStatementSliceBytes(column))
	}
	return total
}
