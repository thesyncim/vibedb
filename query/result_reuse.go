package query

import "math"

// ReuseCapacityBytes reports owned backing-array capacity, unlike RetainedBytes
// which accounts for the current logical result. Call ResetForReuse before
// retaining a result across source lifetimes: cells and headers can borrow
// arbitrary source storage until they are scrubbed.
func (r *Result) ReuseCapacityBytes() int64 {
	if r == nil {
		return 0
	}
	total, ok := resultSize(cap(r.Columns), resultColumnBytes)
	if !ok {
		return math.MaxInt64
	}
	for _, column := range r.Columns[:cap(r.Columns)] {
		n, ok := resultSize(cap(column.Cells), resultCellBytes)
		if !ok || total > math.MaxInt64-n {
			return math.MaxInt64
		}
		total += n
	}
	if total > math.MaxInt64-int64(cap(r.fileData)) {
		return math.MaxInt64
	}
	return total + int64(cap(r.fileData))
}

// ResetForReuse severs all borrowed cells, headers, and budget state while
// keeping only owned backing arrays within maxBytes. Oversized results are
// released. It invalidates every prior result view and is single-consumer.
func (r *Result) ResetForReuse(maxBytes int64) {
	if r == nil {
		return
	}
	if maxBytes <= 0 || r.ReuseCapacityBytes() > maxBytes {
		r.Release()
		return
	}
	for i := range r.Columns[:cap(r.Columns)] {
		column := &r.Columns[:cap(r.Columns)][i]
		clear(column.Cells[:cap(column.Cells)])
		column.Cells = column.Cells[:0]
		column.Header = ""
	}
	clear(r.fileData)
	columns, data := r.Columns[:0], r.fileData[:0]
	*r = Result{Columns: columns, fileData: data}
}
