package query

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"unsafe"
)

const (
	// DefaultResultRows is the maximum number of rows one execution
	// materializes when ExecOptions.ResultRows is zero.
	DefaultResultRows = 100_000
	// DefaultResultBytes is the maximum logical result storage one execution
	// materializes when ExecOptions.ResultBytes is zero.
	DefaultResultBytes int64 = 64 << 20
)

// unorderedTrimThreshold returns 2*limit+1 without overflowing int. A LIMIT
// near MaxInt can never be reached by an in-memory slice, so MaxInt is the
// correct saturated threshold.
func unorderedTrimThreshold(limit int) int {
	maxInt := int(^uint(0) >> 1)
	if limit > (maxInt-1)/2 {
		return maxInt
	}
	return limit*2 + 1
}

// ErrResultBudget is the sentinel wrapped by [ResultBudgetError].
var ErrResultBudget = errors.New("query: result exceeds execution budget")

// ResultBudgetError reports that materializing a query result would exceed a
// configured row or byte limit. It is returned before the rejected cell slice
// or durable payload is grown.
type ResultBudgetError struct {
	Rows      int
	RowLimit  int
	Bytes     int64
	ByteLimit int64
}

func (e *ResultBudgetError) Error() string {
	switch {
	case e.RowLimit >= 0 && e.Rows > e.RowLimit:
		return fmt.Sprintf(
			"query: result has %d rows, exceeding the execution limit of %d: %v",
			e.Rows, e.RowLimit, ErrResultBudget,
		)
	default:
		return fmt.Sprintf(
			"query: result needs %d bytes, exceeding the execution limit of %d: %v",
			e.Bytes, e.ByteLimit, ErrResultBudget,
		)
	}
}

// Unwrap lets callers classify the error with errors.Is.
func (e *ResultBudgetError) Unwrap() error { return ErrResultBudget }

const (
	resultColumnBytes = int64(unsafe.Sizeof(ResultColumn{}))
	resultCellBytes   = int64(unsafe.Sizeof(Cell{}))
)

func normalizeResultBudget(options ExecOptions) (int, int64, error) {
	rows := options.ResultRows
	switch {
	case rows < -1:
		return 0, 0, fmt.Errorf(
			"query: ResultRows must be -1, zero, or positive, got %d", rows,
		)
	case rows == 0:
		rows = DefaultResultRows
	}
	bytes := options.ResultBytes
	switch {
	case bytes < -1:
		return 0, 0, fmt.Errorf(
			"query: ResultBytes must be -1, zero, or positive, got %d", bytes,
		)
	case bytes == 0:
		bytes = DefaultResultBytes
	}
	return rows, bytes, nil
}

func (r *Result) beginResultBudget(rows int, bytes int64) {
	r.resultRowsLimit = rows
	r.resultBytesLimit = bytes
	r.resultBytesUsed = 0
	r.fileData = r.fileData[:0]
}

func (r *Result) admitResultShape(columns, rows int) error {
	required, err := r.checkResultBudget(columns, rows, 0)
	if err != nil {
		r.abortResult()
		return err
	}
	r.resultBytesUsed = required
	return nil
}

func (r *Result) checkResultBudget(columns, rows int, payloadBytes int64) (int64, error) {
	if r.resultRowsLimit >= 0 && rows > r.resultRowsLimit {
		return 0, &ResultBudgetError{Rows: rows, RowLimit: r.resultRowsLimit}
	}

	columnBytes, ok := resultSize(columns, resultColumnBytes)
	if !ok {
		return 0, r.resultByteBudgetError(rows, math.MaxInt64)
	}
	cells, ok := resultProduct(columns, rows)
	if !ok {
		return 0, r.resultByteBudgetError(rows, math.MaxInt64)
	}
	cellBytes, ok := resultSize(cells, resultCellBytes)
	if !ok || columnBytes > math.MaxInt64-cellBytes {
		return 0, r.resultByteBudgetError(rows, math.MaxInt64)
	}
	required := columnBytes + cellBytes
	if payloadBytes < 0 || required > math.MaxInt64-payloadBytes {
		return 0, r.resultByteBudgetError(rows, math.MaxInt64)
	}
	required += payloadBytes
	if r.resultBytesLimit >= 0 && required > r.resultBytesLimit {
		return 0, r.resultByteBudgetError(rows, required)
	}
	return required, nil
}

func (r *Result) resultByteBudgetError(rows int, bytes int64) error {
	return &ResultBudgetError{
		Rows: rows, RowLimit: r.resultRowsLimit,
		Bytes: bytes, ByteLimit: r.resultBytesLimit,
	}
}

func (r *Result) admitResultBytes(additional int64) error {
	if additional < 0 || r.resultBytesUsed > math.MaxInt64-additional {
		err := &ResultBudgetError{
			Rows: r.RowCount, RowLimit: r.resultRowsLimit,
			Bytes: math.MaxInt64, ByteLimit: r.resultBytesLimit,
		}
		r.abortResult()
		return err
	}
	required := r.resultBytesUsed + additional
	if r.resultBytesLimit >= 0 && required > r.resultBytesLimit {
		err := &ResultBudgetError{
			Rows: r.RowCount, RowLimit: r.resultRowsLimit,
			Bytes: required, ByteLimit: r.resultBytesLimit,
		}
		r.abortResult()
		return err
	}
	r.resultBytesUsed = required
	return nil
}

func (r *Result) admitResultCell(cell Cell) error {
	return r.admitResultBytes(resultCellPayloadBytes(cell))
}

func resultCellPayloadBytes(cell Cell) int64 {
	bytes := int64(len(cell.raw)) + int64(len(cell.text))
	if len(cell.raw) != 0 || cell.kind != TypeNumber {
		return bytes
	}
	var scratch [32]byte
	dst := scratch[:0]
	switch {
	case cell.flag&cellInteger != 0:
		dst = strconv.AppendInt(dst, int64(cell.word), 10)
	case cell.flag&cellNumberRaw == 0:
		dst = strconv.AppendFloat(dst, math.Float64frombits(cell.word), 'g', -1, 64)
	}
	return bytes + int64(len(dst))
}

func (r *Result) abortResult() {
	for i := range r.Columns {
		clear(r.Columns[i].Cells)
		r.Columns[i].Cells = r.Columns[i].Cells[:0]
	}
	r.RowCount = 0
	r.fileData = r.fileData[:0]
	r.resultBytesUsed = 0
}

func resultProduct(a, b int) (int, bool) {
	if a < 0 || b < 0 || (a != 0 && b > int(^uint(0)>>1)/a) {
		return 0, false
	}
	return a * b, true
}

func resultSize(count int, size int64) (int64, bool) {
	if count < 0 || (size != 0 && int64(count) > math.MaxInt64/size) {
		return 0, false
	}
	return int64(count) * size, true
}
