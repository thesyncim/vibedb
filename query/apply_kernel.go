package query

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"unsafe"
)

const (
	// DefaultApplyRows bounds the complete APPLY output when MaxRows is zero.
	DefaultApplyRows = 100_000
	// DefaultApplyBytes bounds owned output and per-left-row workspace when
	// MaxBytes is zero.
	DefaultApplyBytes int64 = 64 << 20
	// DefaultApplyCacheEntries bounds exact parameter tuples when
	// MaxCacheEntries is zero.
	DefaultApplyCacheEntries = 4_096
	// DefaultApplyCacheBytes bounds owned memoized keys and right relations when
	// MaxCacheBytes is zero.
	DefaultApplyCacheBytes int64 = 32 << 20
)

var (
	// ErrApplyConfig classifies an invalid physical APPLY configuration.
	ErrApplyConfig = errors.New("query: invalid apply configuration")
	// ErrApplyProgram reports a missing parameter/right-side implementation.
	ErrApplyProgram = errors.New("query: apply program is nil")
	// ErrApplySource reports a missing or malformed left relation.
	ErrApplySource = errors.New("query: apply source is invalid")
	// ErrApplyInUse reports concurrent or re-entrant use of one kernel.
	ErrApplyInUse = errors.New("query: apply kernel is already executing")
	// ErrApplyBinderInactive reports parameter emission outside Bind.
	ErrApplyBinderInactive = errors.New("query: apply parameter binder is inactive")
	// ErrApplyAppenderInactive reports right-row emission outside Right.
	ErrApplyAppenderInactive = errors.New("query: apply row appender is inactive")
	// ErrApplyParameterCount classifies an incomplete or overfull parameter tuple.
	ErrApplyParameterCount = errors.New("query: apply parameter count mismatch")
	// ErrApplyRightArity classifies a malformed right-side row.
	ErrApplyRightArity = errors.New("query: apply right row arity mismatch")
	// ErrApplyRows classifies exhaustion of the output-row limit.
	ErrApplyRows = errors.New("query: apply row limit exceeded")
	// ErrApplyBytes classifies exhaustion of the output/workspace byte limit.
	ErrApplyBytes = errors.New("query: apply byte limit exceeded")
	// ErrApplyCacheBudget classifies exhaustion of a memoization limit.
	ErrApplyCacheBudget = errors.New("query: apply memoization cache limit exceeded")
	// ErrApplySize classifies integer or address-space overflow while sizing.
	ErrApplySize = errors.New("query: apply size overflow")
)

// ApplyJoinKind selects the behavior when one left row produces no right rows.
type ApplyJoinKind uint8

const (
	// ApplyCross implements CROSS APPLY: an empty right relation removes its
	// corresponding left row.
	ApplyCross ApplyJoinKind = iota
	// ApplyLeft implements OUTER APPLY / LEFT JOIN LATERAL: an empty right
	// relation emits one row with explicit SQL NULLs on the right.
	ApplyLeft
	// ApplyOuter is the SQL Server spelling alias for ApplyLeft.
	ApplyOuter = ApplyLeft
)

func (k ApplyJoinKind) valid() bool { return k == ApplyCross || k == ApplyLeft }

// ApplyMemoization selects whether equal parameter tuples share one right-side
// evaluation inside a Run.
type ApplyMemoization uint8

const (
	// ApplyMemoizationNone evaluates Right once per left row and performs no
	// cache-specific hashing, copying, or allocation.
	ApplyMemoizationNone ApplyMemoization = iota
	// ApplyMemoizationExact memoizes by SQL IS NOT DISTINCT FROM row identity.
	// Equal decimal spellings and NULL/missing keys share an entry.
	ApplyMemoizationExact
)

func (m ApplyMemoization) valid() bool {
	return m == ApplyMemoizationNone || m == ApplyMemoizationExact
}

// ApplyOptions describes one physical APPLY execution.
//
// Zero selects a finite default for every limit. Minus one disables that one
// limit; values below minus one are invalid. At least one output limit must be
// finite. Exact memoization additionally requires a finite entry or byte limit.
type ApplyOptions struct {
	Kind             ApplyJoinKind
	RightColumns     int
	ParameterColumns int
	Memoization      ApplyMemoization
	MaxRows          int
	MaxBytes         int64
	MaxCacheEntries  int
	MaxCacheBytes    int64
	Cancel           *CancelFlag
}

type applyOptions struct {
	kind             ApplyJoinKind
	rightColumns     int
	parameterColumns int
	memoization      ApplyMemoization
	maxRows          int
	maxBytes         int64
	maxCacheEntries  int
	maxCacheBytes    int64
	cancel           *CancelFlag
}

func normalizeApplyOptions(options ApplyOptions) (applyOptions, error) {
	if !options.Kind.valid() {
		return applyOptions{}, fmt.Errorf(
			"query: apply kind %d is invalid: %w", options.Kind, ErrApplyConfig,
		)
	}
	if options.RightColumns <= 0 {
		return applyOptions{}, fmt.Errorf(
			"query: apply needs a positive right column count, got %d: %w",
			options.RightColumns, ErrApplyConfig,
		)
	}
	if options.ParameterColumns < 0 {
		return applyOptions{}, fmt.Errorf(
			"query: apply parameter column count is negative: %w", ErrApplyConfig,
		)
	}
	if !options.Memoization.valid() {
		return applyOptions{}, fmt.Errorf(
			"query: apply memoization mode %d is invalid: %w",
			options.Memoization, ErrApplyConfig,
		)
	}
	rows := options.MaxRows
	if rows == 0 {
		rows = DefaultApplyRows
	} else if rows < -1 {
		return applyOptions{}, fmt.Errorf(
			"query: apply MaxRows must be -1, zero, or positive, got %d: %w",
			rows, ErrApplyConfig,
		)
	}
	bytes := options.MaxBytes
	if bytes == 0 {
		bytes = DefaultApplyBytes
	} else if bytes < -1 {
		return applyOptions{}, fmt.Errorf(
			"query: apply MaxBytes must be -1, zero, or positive, got %d: %w",
			bytes, ErrApplyConfig,
		)
	}
	if rows < 0 && bytes < 0 {
		return applyOptions{}, fmt.Errorf(
			"query: apply execution must retain a finite row or byte limit: %w",
			ErrApplyConfig,
		)
	}
	cacheEntries := options.MaxCacheEntries
	if cacheEntries == 0 {
		cacheEntries = DefaultApplyCacheEntries
	} else if cacheEntries < -1 {
		return applyOptions{}, fmt.Errorf(
			"query: apply MaxCacheEntries must be -1, zero, or positive, got %d: %w",
			cacheEntries, ErrApplyConfig,
		)
	}
	cacheBytes := options.MaxCacheBytes
	if cacheBytes == 0 {
		cacheBytes = DefaultApplyCacheBytes
	} else if cacheBytes < -1 {
		return applyOptions{}, fmt.Errorf(
			"query: apply MaxCacheBytes must be -1, zero, or positive, got %d: %w",
			cacheBytes, ErrApplyConfig,
		)
	}
	if options.Memoization == ApplyMemoizationExact &&
		cacheEntries < 0 && cacheBytes < 0 {
		return applyOptions{}, fmt.Errorf(
			"query: memoized apply must retain a finite cache limit: %w",
			ErrApplyConfig,
		)
	}
	return applyOptions{
		kind:             options.Kind,
		rightColumns:     options.RightColumns,
		parameterColumns: options.ParameterColumns,
		memoization:      options.Memoization,
		maxRows:          rows,
		maxBytes:         bytes,
		maxCacheEntries:  cacheEntries,
		maxCacheBytes:    cacheBytes,
		cancel:           options.Cancel,
	}, nil
}

// ApplySource is a synchronous read-only left relation. Cell storage must stay
// valid until Run returns. RecursiveResult and ApplyResult satisfy this shape,
// allowing physical APPLY chains without adapter allocation.
type ApplySource interface {
	Rows() int
	Columns() int
	Cell(row, column int) Cell
}

// ApplyProgram is the lowering-neutral correlated-right boundary. Bind emits
// the positional parameter tuple for one left row. Right evaluates the lateral
// relation for that tuple and emits zero or more right rows.
//
// Calls are synchronous, serial, and left-major. Bind is skipped when
// ParameterColumns is zero; exact memoization then evaluates Right once and
// replays that uncorrelated relation for every left row. Implementations may
// lend Cell storage only until Append returns because the kernel immediately
// owns every materialized value.
type ApplyProgram interface {
	Bind(ApplyLeftRow, *ApplyParameterBinder) error
	Right(ApplyParameters, *ApplyRightAppender) error
}

// ApplyParameterCountError reports the prepared tuple width and emitted width.
type ApplyParameterCountError struct {
	Columns int
	Got     int
}

func (e *ApplyParameterCountError) Error() string {
	return fmt.Sprintf(
		"query: apply bound %d parameters, want %d: %v",
		e.Got, e.Columns, ErrApplyParameterCount,
	)
}

func (e *ApplyParameterCountError) Unwrap() error { return ErrApplyParameterCount }

// ApplyRightArityError reports the prepared right width and emitted width.
type ApplyRightArityError struct {
	Columns int
	Got     int
}

func (e *ApplyRightArityError) Error() string {
	return fmt.Sprintf(
		"query: apply right row has %d columns, want %d: %v",
		e.Got, e.Columns, ErrApplyRightArity,
	)
}

func (e *ApplyRightArityError) Unwrap() error { return ErrApplyRightArity }

// ApplyRowBudgetError reports the first output cardinality that was refused.
type ApplyRowBudgetError struct {
	Rows  int
	Limit int
}

func (e *ApplyRowBudgetError) Error() string {
	return fmt.Sprintf(
		"query: apply needs %d output rows, exceeding the limit of %d: %v",
		e.Rows, e.Limit, ErrApplyRows,
	)
}

func (e *ApplyRowBudgetError) Unwrap() error { return ErrApplyRows }

// ApplyByteBudgetError reports the first owned resource that was refused.
type ApplyByteBudgetError struct {
	Resource string
	Bytes    int64
	Limit    int64
}

func (e *ApplyByteBudgetError) Error() string {
	return fmt.Sprintf(
		"query: apply %s needs %d bytes, exceeding the limit of %d: %v",
		e.Resource, e.Bytes, e.Limit, ErrApplyBytes,
	)
}

func (e *ApplyByteBudgetError) Unwrap() error { return ErrApplyBytes }

// ApplyCacheBudgetError reports the projected exact memoization footprint.
type ApplyCacheBudgetError struct {
	Entries    int
	EntryLimit int
	Bytes      int64
	ByteLimit  int64
}

func (e *ApplyCacheBudgetError) Error() string {
	if e.EntryLimit >= 0 && e.Entries > e.EntryLimit {
		return fmt.Sprintf(
			"query: apply memoization needs %d entries, exceeding the limit of %d: %v",
			e.Entries, e.EntryLimit, ErrApplyCacheBudget,
		)
	}
	return fmt.Sprintf(
		"query: apply memoization needs %d bytes, exceeding the limit of %d: %v",
		e.Bytes, e.ByteLimit, ErrApplyCacheBudget,
	)
}

func (e *ApplyCacheBudgetError) Unwrap() error { return ErrApplyCacheBudget }

// ApplyKernel is a reusable single-owner physical CROSS/LEFT APPLY executor.
// Its zero value is ready. Concurrent or re-entrant Run calls on one kernel are
// rejected; independent kernels have no shared mutable execution state.
type ApplyKernel struct {
	running atomic.Bool

	options applyOptions
	result  recursiveSpool
	inner   recursiveSpool
	key     recursiveSpool
	cache   applyMemoCache

	leftCells      []Cell
	parameterCells []Cell
	outputCells    []Cell
	leftPayload    int64
	pendingRows    int
	pendingBytes   int64
	scratchBytes   int64

	binder   ApplyParameterBinder
	appender ApplyRightAppender
}

// Run executes a lateral physical relation in deterministic left-major order.
// The returned relation remains valid until the next Run or Release on k.
func (k *ApplyKernel) Run(
	source ApplySource,
	program ApplyProgram,
	options ApplyOptions,
) (result ApplyResult, err error) {
	if k == nil {
		return ApplyResult{}, fmt.Errorf("query: nil apply kernel: %w", ErrApplyConfig)
	}
	if !k.running.CompareAndSwap(false, true) {
		return ApplyResult{}, ErrApplyInUse
	}
	defer k.running.Store(false)

	k.resetRun()
	if source == nil {
		return ApplyResult{}, ErrApplySource
	}
	if program == nil {
		return ApplyResult{}, ErrApplyProgram
	}
	k.options, err = normalizeApplyOptions(options)
	if err != nil {
		return ApplyResult{}, err
	}
	leftRows, leftColumns := source.Rows(), source.Columns()
	if leftRows < 0 || leftColumns < 0 {
		return ApplyResult{}, fmt.Errorf(
			"query: apply source shape is %dx%d: %w", leftRows, leftColumns, ErrApplySource,
		)
	}
	outputColumns, ok := checkedRecursiveAdd(leftColumns, k.options.rightColumns)
	if !ok || outputColumns <= 0 {
		return ApplyResult{}, ErrApplySize
	}
	if err = k.prepareRunStorage(leftColumns, outputColumns); err != nil {
		return ApplyResult{}, err
	}
	if k.options.memoization == ApplyMemoizationExact {
		k.cache.begin(k.options.parameterColumns, k.options.rightColumns)
	} else {
		k.cache.reset()
	}
	defer func() {
		k.binder.deactivate()
		k.appender.deactivate()
		if err != nil {
			k.result.reset()
			k.inner.reset()
			k.key.reset()
			k.cache.reset()
			result = ApplyResult{}
		}
	}()

	for leftRow := 0; leftRow < leftRows; leftRow++ {
		if err = cancellationError(k.options.cancel); err != nil {
			return ApplyResult{}, err
		}
		left := ApplyLeftRow{cells: k.leftCells}
		if err = k.loadLeft(source, leftRow); err != nil {
			return ApplyResult{}, err
		}
		parameters := ApplyParameters{cells: k.parameterCells[:0]}
		if k.options.parameterColumns != 0 {
			k.parameterCells = k.parameterCells[:0]
			k.binder.activate(k)
			callbackErr := program.Bind(left, &k.binder)
			if err = k.binder.finish(callbackErr); err != nil {
				return ApplyResult{}, err
			}
			parameters.cells = k.parameterCells
		}

		var hash uint64
		if k.options.memoization == ApplyMemoizationExact {
			if err = k.ownKey(parameters); err != nil {
				return ApplyResult{}, err
			}
			hash, err = k.cache.hashKey(&k.key, k.options.cancel)
			if err != nil {
				return ApplyResult{}, err
			}
			entry, found, findErr := k.cache.find(
				hash, &k.key, k.options.cancel,
			)
			if findErr != nil {
				return ApplyResult{}, findErr
			}
			if found {
				if err = k.publishRight(
					&k.cache.values, entry.valueStart, entry.valueRows, false,
				); err != nil {
					return ApplyResult{}, err
				}
				continue
			}
			if err = k.cache.admitEntry(k.options); err != nil {
				return ApplyResult{}, err
			}
		}

		k.inner.reset()
		k.pendingRows = 0
		k.pendingBytes = 0
		k.appender.activate(k)
		callbackErr := program.Right(parameters, &k.appender)
		if err = k.appender.finish(callbackErr); err != nil {
			return ApplyResult{}, err
		}
		if k.options.memoization == ApplyMemoizationExact {
			if err = k.cache.store(hash, &k.key, &k.inner, k.options); err != nil {
				return ApplyResult{}, err
			}
		}
		if err = k.publishRight(&k.inner, 0, k.inner.rows, true); err != nil {
			return ApplyResult{}, err
		}
	}
	if err = cancellationError(k.options.cancel); err != nil {
		return ApplyResult{}, err
	}
	return ApplyResult{view: recursiveView{
		spool: &k.result, rows: k.result.rows,
	}}, nil
}

func (k *ApplyKernel) resetRun() {
	k.result.reset()
	k.inner.reset()
	k.key.reset()
	k.cache.reset()
	k.parameterCells = k.parameterCells[:0]
	k.leftPayload = 0
	k.pendingRows = 0
	k.pendingBytes = 0
	k.scratchBytes = 0
	k.binder.deactivate()
	k.appender.deactivate()
}

func (k *ApplyKernel) prepareRunStorage(leftColumns, outputColumns int) error {
	scratchCells, ok := checkedRecursiveAdd(leftColumns, outputColumns)
	if !ok {
		return ErrApplySize
	}
	scratchCells, ok = checkedRecursiveAdd(scratchCells, k.options.parameterColumns)
	if !ok {
		return ErrApplySize
	}
	k.scratchBytes = saturatedProduct(int64(scratchCells), applyCellBytes)
	if k.scratchBytes == math.MaxInt64 {
		return ErrApplySize
	}
	if err := k.admitBytes("tuple workspace", k.scratchBytes); err != nil {
		return err
	}
	var err error
	k.leftCells, err = applyCellScratch(k.leftCells, leftColumns)
	if err != nil {
		return err
	}
	k.parameterCells, err = applyCellScratch(
		k.parameterCells, k.options.parameterColumns,
	)
	if err != nil {
		return err
	}
	k.parameterCells = k.parameterCells[:0]
	k.outputCells, err = applyCellScratch(k.outputCells, outputColumns)
	if err != nil {
		return err
	}
	k.result.columns = outputColumns
	k.inner.columns = k.options.rightColumns
	k.key.columns = k.options.parameterColumns
	return nil
}

func applyCellScratch(cells []Cell, need int) ([]Cell, error) {
	if need < 0 {
		return cells, ErrApplySize
	}
	var err error
	cells, err = reserveRecursiveSlice(cells[:0], need)
	if err != nil {
		return cells, ErrApplySize
	}
	return cells[:need], nil
}

func (k *ApplyKernel) loadLeft(source ApplySource, row int) error {
	for column := range k.leftCells {
		if err := cancellationCheckpoint(k.options.cancel, column); err != nil {
			return err
		}
		cell := source.Cell(row, column)
		k.leftCells[column] = cell
		k.outputCells[column] = cell
	}
	payload, err := measureRecursiveRow(k.leftCells, k.options.cancel)
	if err != nil {
		return err
	}
	k.leftPayload = payload
	return cancellationError(k.options.cancel)
}

func (k *ApplyKernel) ownKey(parameters ApplyParameters) error {
	k.key.reset()
	if k.options.parameterColumns == 0 {
		return cancellationError(k.options.cancel)
	}
	payload, err := measureRecursiveRow(parameters.cells, k.options.cancel)
	if err != nil {
		return err
	}
	bytes := recursiveRowRetainedBytes(k.options.parameterColumns, payload)
	if bytes == math.MaxInt64 {
		return ErrApplySize
	}
	bytes = saturatedBytes(k.scratchBytes, bytes)
	if bytes == math.MaxInt64 {
		return ErrApplySize
	}
	if err := k.admitBytes("parameter tuple", bytes); err != nil {
		return err
	}
	return k.key.appendMeasuredRow(parameters.cells, payload, k.options.cancel)
}

func (k *ApplyKernel) appendRight(row []Cell) error {
	if len(row) != k.options.rightColumns {
		return &ApplyRightArityError{Columns: k.options.rightColumns, Got: len(row)}
	}
	payload, err := measureRecursiveRow(row, k.options.cancel)
	if err != nil {
		return err
	}
	innerRowBytes := recursiveRowRetainedBytes(k.options.rightColumns, payload)
	outputPayload := saturatedBytes(k.leftPayload, payload)
	outputRowBytes := recursiveRowRetainedBytes(k.result.columns, outputPayload)
	if innerRowBytes == math.MaxInt64 || outputPayload == math.MaxInt64 ||
		outputRowBytes == math.MaxInt64 {
		return ErrApplySize
	}
	innerBytes := saturatedBytes(
		k.scratchBytes,
		saturatedBytes(k.result.retainedBytes(),
			saturatedBytes(k.inner.retainedBytes(), innerRowBytes)),
	)
	if innerBytes == math.MaxInt64 {
		return ErrApplySize
	}
	if err := k.admitBytes("right-row workspace", innerBytes); err != nil {
		return err
	}
	if err := k.admitOutput(outputRowBytes); err != nil {
		return err
	}
	if err := k.inner.appendMeasuredRow(row, payload, k.options.cancel); err != nil {
		return err
	}
	k.pendingRows++
	k.pendingBytes = saturatedBytes(k.pendingBytes, outputRowBytes)
	if k.pendingBytes == math.MaxInt64 {
		return ErrApplySize
	}
	return cancellationError(k.options.cancel)
}

func (k *ApplyKernel) admitOutput(rowBytes int64) error {
	rows, ok := checkedRecursiveAdd(k.result.rows, k.pendingRows)
	if !ok {
		return ErrApplySize
	}
	rows, ok = checkedRecursiveAdd(rows, 1)
	if !ok {
		return ErrApplySize
	}
	if k.options.maxRows >= 0 && rows > k.options.maxRows {
		return &ApplyRowBudgetError{Rows: rows, Limit: k.options.maxRows}
	}
	bytes := saturatedBytes(k.scratchBytes, saturatedBytes(
		saturatedBytes(k.result.retainedBytes(), k.pendingBytes), rowBytes,
	))
	if bytes == math.MaxInt64 {
		return ErrApplySize
	}
	return k.admitBytes("result", bytes)
}

func (k *ApplyKernel) admitBytes(resource string, bytes int64) error {
	if bytes < 0 || bytes == math.MaxInt64 {
		return ErrApplySize
	}
	if k.options.maxBytes >= 0 && bytes > k.options.maxBytes {
		return &ApplyByteBudgetError{
			Resource: resource, Bytes: bytes, Limit: k.options.maxBytes,
		}
	}
	return nil
}

func (k *ApplyKernel) publishRight(
	relation *recursiveSpool,
	start, rows int,
	reserved bool,
) error {
	if rows == 0 {
		if k.options.kind == ApplyCross {
			return cancellationError(k.options.cancel)
		}
		// The right callback cannot reserve a row it did not emit. Null
		// extension therefore always performs its own output admission.
		return k.publishNullExtended(false)
	}
	if relation == nil || start < 0 || rows < 0 || start > relation.rows-rows {
		return ErrApplySize
	}
	leftColumns := len(k.leftCells)
	for offset := 0; offset < rows; offset++ {
		if err := cancellationCheckpoint(k.options.cancel, offset); err != nil {
			return err
		}
		rightRow := start + offset
		for column := 0; column < k.options.rightColumns; column++ {
			k.outputCells[leftColumns+column] = relation.cell(rightRow, column)
		}
		payload := saturatedBytes(k.leftPayload, applySpoolRowPayload(relation, rightRow))
		rowBytes := recursiveRowRetainedBytes(k.result.columns, payload)
		if payload == math.MaxInt64 || rowBytes == math.MaxInt64 {
			return ErrApplySize
		}
		if !reserved {
			if err := k.admitOutput(rowBytes); err != nil {
				return err
			}
		}
		if err := k.result.appendMeasuredRow(
			k.outputCells, payload, k.options.cancel,
		); err != nil {
			return err
		}
	}
	if reserved {
		k.pendingRows = 0
		k.pendingBytes = 0
	}
	return cancellationError(k.options.cancel)
}

func (k *ApplyKernel) publishNullExtended(reserved bool) error {
	leftColumns := len(k.leftCells)
	for column := 0; column < k.options.rightColumns; column++ {
		k.outputCells[leftColumns+column] = nullCell()
	}
	nullPayload := saturatedProduct(int64(k.options.rightColumns), int64(len(nullBytes)))
	payload := saturatedBytes(k.leftPayload, nullPayload)
	rowBytes := recursiveRowRetainedBytes(k.result.columns, payload)
	if payload == math.MaxInt64 || rowBytes == math.MaxInt64 {
		return ErrApplySize
	}
	if !reserved {
		if err := k.admitOutput(rowBytes); err != nil {
			return err
		}
	}
	if err := k.result.appendMeasuredRow(k.outputCells, payload, k.options.cancel); err != nil {
		return err
	}
	if reserved {
		k.pendingRows = 0
		k.pendingBytes = 0
	}
	return cancellationError(k.options.cancel)
}

func applySpoolRowPayload(spool *recursiveSpool, row int) int64 {
	if spool == nil || row < 0 || row >= spool.rows {
		return math.MaxInt64
	}
	start := 0
	if row != 0 {
		start = spool.ends[row-1]
	}
	if spool.ends[row] < start {
		return math.MaxInt64
	}
	return int64(spool.ends[row] - start)
}

// Release drops all high-water storage retained by k. It must not race Run.
func (k *ApplyKernel) Release() {
	if k == nil {
		return
	}
	k.result.release()
	k.inner.release()
	k.key.release()
	k.cache.release()
	k.leftCells = nil
	k.parameterCells = nil
	k.outputCells = nil
	k.options = applyOptions{}
	k.leftPayload = 0
	k.pendingRows = 0
	k.pendingBytes = 0
	k.scratchBytes = 0
	k.binder = ApplyParameterBinder{}
	k.appender = ApplyRightAppender{}
}

// ApplyLeftRow is the current left tuple passed to Bind.
type ApplyLeftRow struct{ cells []Cell }

// Columns returns the tuple width.
func (r ApplyLeftRow) Columns() int { return len(r.cells) }

// Cell returns a borrowed cell or SQL NULL for an invalid ordinal.
func (r ApplyLeftRow) Cell(column int) Cell {
	if column < 0 || column >= len(r.cells) {
		return nullCell()
	}
	return r.cells[column]
}

// Missing reports whether a NULL came from an absent JSON path.
func (r ApplyLeftRow) Missing(column int) bool {
	return column >= 0 && column < len(r.cells) &&
		r.cells[column].kind == TypeNull && r.cells[column].flag&cellMissing != 0
}

// ApplyParameters is the immutable positional tuple passed to Right.
type ApplyParameters struct{ cells []Cell }

// Columns returns the parameter width.
func (p ApplyParameters) Columns() int { return len(p.cells) }

// Cell returns a borrowed parameter or SQL NULL for an invalid ordinal.
func (p ApplyParameters) Cell(column int) Cell {
	if column < 0 || column >= len(p.cells) {
		return nullCell()
	}
	return p.cells[column]
}

// Missing reports whether a NULL parameter came from an absent JSON path.
func (p ApplyParameters) Missing(column int) bool {
	return column >= 0 && column < len(p.cells) &&
		p.cells[column].kind == TypeNull && p.cells[column].flag&cellMissing != 0
}

// ApplyParameterBinder emits one positional parameter tuple during Bind.
type ApplyParameterBinder struct {
	kernel *ApplyKernel
	err    error
	active bool
}

func (b *ApplyParameterBinder) activate(kernel *ApplyKernel) {
	b.kernel = kernel
	b.err = nil
	b.active = true
}

func (b *ApplyParameterBinder) deactivate() {
	b.kernel = nil
	b.err = nil
	b.active = false
}

// Append binds the next positional parameter.
func (b *ApplyParameterBinder) Append(cell Cell) error {
	if b == nil || !b.active || b.kernel == nil {
		return ErrApplyBinderInactive
	}
	if b.err != nil {
		return b.err
	}
	if err := cancellationError(b.kernel.options.cancel); err != nil {
		b.err = err
		return err
	}
	if len(b.kernel.parameterCells) >= b.kernel.options.parameterColumns {
		b.err = &ApplyParameterCountError{
			Columns: b.kernel.options.parameterColumns,
			Got:     len(b.kernel.parameterCells) + 1,
		}
		return b.err
	}
	b.kernel.parameterCells = append(b.kernel.parameterCells, cell)
	return nil
}

// Checkpoint observes cooperative cancellation during a long Bind callback.
func (b *ApplyParameterBinder) Checkpoint() error {
	if b == nil || !b.active || b.kernel == nil {
		return ErrApplyBinderInactive
	}
	if b.err == nil {
		b.err = cancellationError(b.kernel.options.cancel)
	}
	return b.err
}

func (b *ApplyParameterBinder) finish(callbackErr error) error {
	b.active = false
	bindErr := b.err
	if bindErr == nil && len(b.kernel.parameterCells) != b.kernel.options.parameterColumns {
		bindErr = &ApplyParameterCountError{
			Columns: b.kernel.options.parameterColumns,
			Got:     len(b.kernel.parameterCells),
		}
	}
	if bindErr != nil && callbackErr != nil && !errors.Is(callbackErr, bindErr) {
		return errors.Join(callbackErr, bindErr)
	}
	if bindErr != nil {
		return bindErr
	}
	return callbackErr
}

// ApplyRightAppender owns right rows emitted during Right.
type ApplyRightAppender struct {
	kernel *ApplyKernel
	err    error
	active bool
}

func (a *ApplyRightAppender) activate(kernel *ApplyKernel) {
	a.kernel = kernel
	a.err = nil
	a.active = true
}

func (a *ApplyRightAppender) deactivate() {
	a.kernel = nil
	a.err = nil
	a.active = false
}

// AppendRow copies one positional right row.
func (a *ApplyRightAppender) AppendRow(row []Cell) error {
	if a == nil || !a.active || a.kernel == nil {
		return ErrApplyAppenderInactive
	}
	if a.err != nil {
		return a.err
	}
	a.err = a.kernel.appendRight(row)
	return a.err
}

// Checkpoint observes cooperative cancellation during a long Right callback.
func (a *ApplyRightAppender) Checkpoint() error {
	if a == nil || !a.active || a.kernel == nil {
		return ErrApplyAppenderInactive
	}
	if a.err == nil {
		a.err = cancellationError(a.kernel.options.cancel)
	}
	return a.err
}

func (a *ApplyRightAppender) finish(callbackErr error) error {
	a.active = false
	emitErr := a.err
	if emitErr != nil && callbackErr != nil && !errors.Is(callbackErr, emitErr) {
		return errors.Join(callbackErr, emitErr)
	}
	if emitErr != nil {
		return emitErr
	}
	return callbackErr
}

// ApplyResult is the complete left-major physical relation. It borrows its
// kernel's owned result spool until the next Run or Release.
type ApplyResult struct{ view recursiveView }

// Rows returns the output cardinality.
func (r ApplyResult) Rows() int { return r.view.rows }

// Columns returns left columns followed by right columns.
func (r ApplyResult) Columns() int { return r.view.columns() }

// Cell returns an output cell or SQL NULL for an invalid ordinal.
func (r ApplyResult) Cell(row, column int) Cell { return r.view.cell(row, column) }

// Missing reports whether an output NULL retains the missing-path marker.
func (r ApplyResult) Missing(row, column int) bool { return r.view.missing(row, column) }

const applyCellBytes = int64(unsafe.Sizeof(Cell{}))

var (
	_ ApplySource = ApplyResult{}
	_ ApplySource = RecursiveResult{}
)
