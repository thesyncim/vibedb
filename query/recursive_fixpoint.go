package query

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
)

const (
	// DefaultRecursiveIterations bounds recursive-term evaluations. The anchor
	// term is not an iteration.
	DefaultRecursiveIterations = 1_000
	// DefaultRecursiveRows bounds the complete fixpoint result.
	DefaultRecursiveRows = 100_000
	// DefaultRecursiveBytes bounds the owned result and DISTINCT identity
	// workspace retained by one fixpoint execution.
	DefaultRecursiveBytes int64 = 64 << 20
)

var (
	// ErrRecursiveConfig classifies an invalid physical fixpoint configuration.
	ErrRecursiveConfig = errors.New("query: invalid recursive fixpoint configuration")
	// ErrRecursiveProgram classifies a missing anchor/step implementation.
	ErrRecursiveProgram = errors.New("query: recursive fixpoint program is nil")
	// ErrRecursiveInUse reports concurrent or re-entrant use of one kernel.
	ErrRecursiveInUse = errors.New("query: recursive fixpoint is already executing")
	// ErrRecursiveAppenderInactive reports use of an emitter outside its callback.
	ErrRecursiveAppenderInactive = errors.New("query: recursive appender is inactive")
	// ErrRecursiveArity classifies a row whose ordinal width differs from the CTE.
	ErrRecursiveArity = errors.New("query: recursive row arity mismatch")
	// ErrRecursiveDepth classifies exhaustion of the recursive-iteration limit.
	ErrRecursiveDepth = errors.New("query: recursive iteration limit exceeded")
	// ErrRecursiveRows classifies exhaustion of the recursive result-row limit.
	ErrRecursiveRows = errors.New("query: recursive row limit exceeded")
	// ErrRecursiveBytes classifies exhaustion of the recursive storage limit.
	ErrRecursiveBytes = errors.New("query: recursive byte limit exceeded")
	// ErrRecursiveSize classifies integer or address-space overflow while sizing.
	ErrRecursiveSize = errors.New("query: recursive fixpoint size overflow")
)

// RecursiveUnionMode selects the admission rule between the anchor and
// recursive terms. Both modes preserve first-production order.
type RecursiveUnionMode uint8

const (
	// RecursiveUnionAll admits every emitted row.
	RecursiveUnionAll RecursiveUnionMode = iota
	// RecursiveUnionDistinct admits the first row in each exact SQL row-identity
	// class and suppresses later duplicates before they enter the next delta.
	RecursiveUnionDistinct
)

func (m RecursiveUnionMode) valid() bool {
	return m == RecursiveUnionAll || m == RecursiveUnionDistinct
}

// RecursiveFixpointOptions are the physical limits and shape of one run.
//
// Zero selects the finite default for each limit. Minus one disables that one
// limit; values below minus one are invalid. At least one of MaxIterations,
// MaxRows, or MaxBytes must remain finite. Columns must be positive.
type RecursiveFixpointOptions struct {
	Columns       int
	Union         RecursiveUnionMode
	MaxIterations int
	MaxRows       int
	MaxBytes      int64
	Cancel        *CancelFlag
}

type recursiveFixpointOptions struct {
	columns       int
	union         RecursiveUnionMode
	maxIterations int
	maxRows       int
	maxBytes      int64
	cancel        *CancelFlag
}

func normalizeRecursiveFixpointOptions(options RecursiveFixpointOptions) (recursiveFixpointOptions, error) {
	if options.Columns <= 0 {
		return recursiveFixpointOptions{}, fmt.Errorf(
			"query: recursive fixpoint needs a positive column count, got %d: %w",
			options.Columns, ErrRecursiveConfig,
		)
	}
	if !options.Union.valid() {
		return recursiveFixpointOptions{}, fmt.Errorf(
			"query: recursive union mode %d is invalid: %w",
			options.Union, ErrRecursiveConfig,
		)
	}
	iterations := options.MaxIterations
	if iterations == 0 {
		iterations = DefaultRecursiveIterations
	} else if iterations < -1 {
		return recursiveFixpointOptions{}, fmt.Errorf(
			"query: recursive MaxIterations must be -1, zero, or positive, got %d: %w",
			iterations, ErrRecursiveConfig,
		)
	}
	rows := options.MaxRows
	if rows == 0 {
		rows = DefaultRecursiveRows
	} else if rows < -1 {
		return recursiveFixpointOptions{}, fmt.Errorf(
			"query: recursive MaxRows must be -1, zero, or positive, got %d: %w",
			rows, ErrRecursiveConfig,
		)
	}
	bytes := options.MaxBytes
	if bytes == 0 {
		bytes = DefaultRecursiveBytes
	} else if bytes < -1 {
		return recursiveFixpointOptions{}, fmt.Errorf(
			"query: recursive MaxBytes must be -1, zero, or positive, got %d: %w",
			bytes, ErrRecursiveConfig,
		)
	}
	if iterations < 0 && rows < 0 && bytes < 0 {
		return recursiveFixpointOptions{}, fmt.Errorf(
			"query: recursive execution must retain at least one finite limit: %w",
			ErrRecursiveConfig,
		)
	}
	return recursiveFixpointOptions{
		columns:       options.Columns,
		union:         options.Union,
		maxIterations: iterations,
		maxRows:       rows,
		maxBytes:      bytes,
		cancel:        options.Cancel,
	}, nil
}

// RecursiveFixpointProgram is the lowering-neutral boundary of the physical
// recursive operator. Anchor emits the non-recursive term. Step receives only
// the previous breadth-first delta and emits the next candidate level.
//
// Implementations are called synchronously and serially. They may lend Cell
// storage to AppendRow only for that call: the appender copies every admitted
// value immediately. A long-running callback should call appender.Checkpoint;
// AppendRow performs the same cancellation check itself.
type RecursiveFixpointProgram interface {
	Anchor(*RecursiveAppender) error
	Step(RecursiveDelta, *RecursiveAppender) error
}

// RecursiveArityError reports the prepared width and one emitted row's width.
type RecursiveArityError struct {
	Columns int
	Got     int
}

func (e *RecursiveArityError) Error() string {
	return fmt.Sprintf(
		"query: recursive row has %d columns, want %d: %v",
		e.Got, e.Columns, ErrRecursiveArity,
	)
}

func (e *RecursiveArityError) Unwrap() error { return ErrRecursiveArity }

// RecursiveDepthError reports the recursive term evaluation that the
// configured limit refused. Iteration is one-based; the anchor is iteration 0.
type RecursiveDepthError struct {
	Iteration int
	Limit     int
}

func (e *RecursiveDepthError) Error() string {
	return fmt.Sprintf(
		"query: recursive term needs iteration %d, exceeding the limit of %d: %v",
		e.Iteration, e.Limit, ErrRecursiveDepth,
	)
}

func (e *RecursiveDepthError) Unwrap() error { return ErrRecursiveDepth }

// RecursiveRowBudgetError reports the first result cardinality that could not
// be admitted.
type RecursiveRowBudgetError struct {
	Rows  int
	Limit int
}

func (e *RecursiveRowBudgetError) Error() string {
	return fmt.Sprintf(
		"query: recursive fixpoint needs %d rows, exceeding the limit of %d: %v",
		e.Rows, e.Limit, ErrRecursiveRows,
	)
}

func (e *RecursiveRowBudgetError) Unwrap() error { return ErrRecursiveRows }

// RecursiveByteBudgetError reports the first owned result plus DISTINCT-index
// footprint that could not be admitted.
type RecursiveByteBudgetError struct {
	Bytes int64
	Limit int64
}

func (e *RecursiveByteBudgetError) Error() string {
	return fmt.Sprintf(
		"query: recursive fixpoint needs %d bytes, exceeding the limit of %d: %v",
		e.Bytes, e.Limit, ErrRecursiveBytes,
	)
}

func (e *RecursiveByteBudgetError) Unwrap() error { return ErrRecursiveBytes }

// RecursiveFixpoint is a reusable, single-owner physical recursive-CTE
// executor. Its zero value is ready. Run is safe against accidental concurrent
// or re-entrant calls: one proceeds and the other returns ErrRecursiveInUse.
// Release must be called only when no Run is active.
//
// The result, working level, delta view, and DISTINCT index retain their
// high-water storage across runs. A failed run publishes no partial relation
// and leaves the kernel ready for another run.
type RecursiveFixpoint struct {
	running atomic.Bool

	options  recursiveFixpointOptions
	result   recursiveSpool
	working  recursiveSpool
	identity recursiveIdentity
	appender RecursiveAppender
}

// Run evaluates program to a fixpoint in deterministic breadth-first order.
// The returned result remains valid until the next Run or Release on f.
func (f *RecursiveFixpoint) Run(
	program RecursiveFixpointProgram,
	options RecursiveFixpointOptions,
) (result RecursiveResult, err error) {
	if f == nil {
		return RecursiveResult{}, fmt.Errorf("query: nil recursive fixpoint: %w", ErrRecursiveConfig)
	}
	if !f.running.CompareAndSwap(false, true) {
		return RecursiveResult{}, ErrRecursiveInUse
	}
	defer f.running.Store(false)

	f.result.reset()
	f.working.reset()
	f.appender.deactivate()

	if program == nil {
		return RecursiveResult{}, ErrRecursiveProgram
	}
	f.options, err = normalizeRecursiveFixpointOptions(options)
	if err != nil {
		return RecursiveResult{}, err
	}
	f.result.columns = f.options.columns
	f.working.columns = f.options.columns
	if f.options.union == RecursiveUnionDistinct {
		// begin both clears the prior run's membership and initializes the
		// process-local hash seed. It must occur exactly once per DISTINCT run.
		f.identity.begin()
	} else {
		f.identity.reset()
	}
	defer func() {
		f.appender.deactivate()
		if err != nil {
			f.result.reset()
			f.working.reset()
			f.identity.reset()
			result = RecursiveResult{}
		}
	}()
	if err = cancellationError(f.options.cancel); err != nil {
		return RecursiveResult{}, err
	}

	f.appender.activate(f, 0)
	callbackErr := program.Anchor(&f.appender)
	if err = f.appender.finish(callbackErr); err != nil {
		return RecursiveResult{}, err
	}
	delta, publishErr := f.publishWorking(0)
	if publishErr != nil {
		err = publishErr
		return RecursiveResult{}, err
	}

	iterations := 0
	for delta.view.rows != 0 {
		if err = cancellationError(f.options.cancel); err != nil {
			return RecursiveResult{}, err
		}
		if f.options.maxIterations >= 0 && iterations >= f.options.maxIterations {
			err = &RecursiveDepthError{
				Iteration: iterations + 1,
				Limit:     f.options.maxIterations,
			}
			return RecursiveResult{}, err
		}
		iteration := iterations + 1
		f.appender.activate(f, iteration)
		callbackErr = program.Step(delta, &f.appender)
		if err = f.appender.finish(callbackErr); err != nil {
			return RecursiveResult{}, err
		}
		iterations = iteration
		delta, publishErr = f.publishWorking(iteration)
		if publishErr != nil {
			err = publishErr
			return RecursiveResult{}, err
		}
	}
	if err = cancellationError(f.options.cancel); err != nil {
		return RecursiveResult{}, err
	}
	return RecursiveResult{
		view:       recursiveView{spool: &f.result, rows: f.result.rows},
		iterations: iterations,
	}, nil
}

func (f *RecursiveFixpoint) publishWorking(iteration int) (RecursiveDelta, error) {
	if f.working.rows == 0 {
		return RecursiveDelta{}, cancellationError(f.options.cancel)
	}
	start := f.result.rows
	rows := f.working.rows
	if err := f.result.appendSpool(&f.working, f.options.cancel); err != nil {
		return RecursiveDelta{}, err
	}
	if f.options.union == RecursiveUnionDistinct {
		f.identity.promoteWorking(start)
	}
	f.working.reset()
	return RecursiveDelta{view: recursiveView{
		spool: &f.result, start: start, rows: rows,
	}, iteration: iteration}, cancellationError(f.options.cancel)
}

// Release drops every high-water allocation retained by f. A later Run starts
// from the ready zero-value state again.
func (f *RecursiveFixpoint) Release() {
	if f == nil {
		return
	}
	f.result.release()
	f.working.release()
	f.identity.release()
	f.options = recursiveFixpointOptions{}
	f.appender = RecursiveAppender{}
}

// RecursiveAppender is valid only during one Anchor or Step callback. Its zero
// value and retained uses outside that interval return ErrRecursiveAppenderInactive.
type RecursiveAppender struct {
	fixpoint  *RecursiveFixpoint
	iteration int
	err       error
	active    bool
}

func (a *RecursiveAppender) activate(f *RecursiveFixpoint, iteration int) {
	a.fixpoint = f
	a.iteration = iteration
	a.err = nil
	a.active = true
}

func (a *RecursiveAppender) deactivate() {
	a.fixpoint = nil
	a.iteration = 0
	a.err = nil
	a.active = false
}

func (a *RecursiveAppender) finish(callbackErr error) error {
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

// Iteration returns zero in Anchor and the one-based recursive-term evaluation
// number in Step.
func (a *RecursiveAppender) Iteration() int {
	if a == nil || !a.active {
		return 0
	}
	return a.iteration
}

// Checkpoint observes the run's cooperative cancellation signal. Callback
// implementations should use it inside long loops that do not call AppendRow.
func (a *RecursiveAppender) Checkpoint() error {
	if a == nil || !a.active || a.fixpoint == nil {
		return ErrRecursiveAppenderInactive
	}
	if a.err != nil {
		return a.err
	}
	a.err = cancellationError(a.fixpoint.options.cancel)
	return a.err
}

// AppendRow copies and conditionally admits one positional row. DISTINCT
// duplicate detection uses exact scalar equality: missing and explicit NULL
// share SQL's NULL identity, decimal spellings compare by exact numeric value,
// strings compare by decoded contents, and containers by exact source bytes.
func (a *RecursiveAppender) AppendRow(row []Cell) error {
	if a == nil || !a.active || a.fixpoint == nil {
		return ErrRecursiveAppenderInactive
	}
	if a.err != nil {
		return a.err
	}
	a.err = a.fixpoint.appendRow(row, a.iteration)
	return a.err
}

func (f *RecursiveFixpoint) appendRow(row []Cell, iteration int) error {
	if err := cancellationError(f.options.cancel); err != nil {
		return err
	}
	if len(row) != f.options.columns {
		return &RecursiveArityError{Columns: f.options.columns, Got: len(row)}
	}
	payload, err := measureRecursiveRow(row, f.options.cancel)
	if err != nil {
		return err
	}
	rowBytes := recursiveRowRetainedBytes(f.options.columns, payload)
	if rowBytes == math.MaxInt64 {
		return ErrRecursiveSize
	}
	if f.options.union == RecursiveUnionAll {
		if err := f.admitIterationAndRows(iteration, 1); err != nil {
			return err
		}
		if err := f.admitBytes(rowBytes, 0); err != nil {
			return err
		}
		return f.working.appendMeasuredRow(row, payload, f.options.cancel)
	}

	// DISTINCT must classify a candidate before row/depth admission: a duplicate
	// at a saturated limit does not enlarge the fixpoint and is still admissible.
	if err := f.admitBytes(rowBytes, f.identity.retainedBytes()); err != nil {
		return err
	}
	rowIndex := f.working.rows
	if err := f.working.appendMeasuredRow(row, payload, f.options.cancel); err != nil {
		return err
	}
	hash, err := hashRecursiveRow(
		f.identity.seed, &f.working, rowIndex, f.options.columns, f.options.cancel,
	)
	if err != nil {
		f.working.rollbackLastRow()
		return err
	}
	if _, found, findErr := f.identity.find(
		hash, &f.working, rowIndex, &f.result, &f.working,
		f.options.columns, f.options.cancel,
	); findErr != nil {
		f.working.rollbackLastRow()
		return findErr
	} else if found {
		f.working.rollbackLastRow()
		return nil
	}
	if err := f.admitIterationAndRows(iteration, 0); err != nil {
		f.working.rollbackLastRow()
		return err
	}
	indexBytes, err := f.identity.retainedBytesForInsert()
	if err != nil {
		f.working.rollbackLastRow()
		return err
	}
	if err := f.admitBytes(0, indexBytes); err != nil {
		f.working.rollbackLastRow()
		return err
	}
	if err := f.identity.insert(hash, rowIndex, true, f.options.cancel); err != nil {
		f.working.rollbackLastRow()
		return err
	}
	return nil
}

func (f *RecursiveFixpoint) admitIterationAndRows(iteration, additional int) error {
	if f.options.maxIterations >= 0 && iteration > f.options.maxIterations {
		return &RecursiveDepthError{Iteration: iteration, Limit: f.options.maxIterations}
	}
	if f.options.maxRows >= 0 {
		working, ok := checkedRecursiveAdd(f.working.rows, additional)
		if !ok {
			return ErrRecursiveSize
		}
		rows, ok := checkedRecursiveAdd(f.result.rows, working)
		if !ok {
			return ErrRecursiveSize
		}
		if rows > f.options.maxRows {
			return &RecursiveRowBudgetError{Rows: rows, Limit: f.options.maxRows}
		}
	}
	return nil
}

func (f *RecursiveFixpoint) admitBytes(additional, indexBytes int64) error {
	working := saturatedBytes(f.working.retainedBytes(), additional)
	required := saturatedBytes(
		saturatedBytes(f.result.retainedBytes(), working), indexBytes,
	)
	if required == math.MaxInt64 {
		return ErrRecursiveSize
	}
	if f.options.maxBytes >= 0 && required > f.options.maxBytes {
		return &RecursiveByteBudgetError{Bytes: required, Limit: f.options.maxBytes}
	}
	return nil
}

type recursiveView struct {
	spool *recursiveSpool
	start int
	rows  int
}

func (v recursiveView) columns() int {
	if v.spool == nil {
		return 0
	}
	return v.spool.columns
}

func (v recursiveView) cell(row, column int) Cell {
	if v.spool == nil || row < 0 || row >= v.rows ||
		column < 0 || column >= v.spool.columns {
		return nullCell()
	}
	return v.spool.cell(v.start+row, column)
}

func (v recursiveView) missing(row, column int) bool {
	if v.spool == nil || row < 0 || row >= v.rows ||
		column < 0 || column >= v.spool.columns {
		return false
	}
	return v.spool.missing(v.start+row, column)
}

// RecursiveDelta is the previous breadth-first level passed to Step. It is a
// read-only zero-copy view and is valid only for that callback.
type RecursiveDelta struct {
	view      recursiveView
	iteration int
}

// Rows returns the number of rows in this level.
func (d RecursiveDelta) Rows() int { return d.view.rows }

// Columns returns the positional width of every row.
func (d RecursiveDelta) Columns() int { return d.view.columns() }

// Iteration returns the level's generation: zero for anchor rows.
func (d RecursiveDelta) Iteration() int { return d.iteration }

// Cell returns one level cell, or SQL NULL for an invalid ordinal.
func (d RecursiveDelta) Cell(row, column int) Cell { return d.view.cell(row, column) }

// Missing reports whether a NULL cell came from the engine's missing-path
// marker rather than an explicit JSON null.
func (d RecursiveDelta) Missing(row, column int) bool {
	return d.view.missing(row, column)
}

// RecursiveResult is the complete breadth-first fixpoint. It borrows its
// kernel's owned result spool until the next Run or Release.
type RecursiveResult struct {
	view       recursiveView
	iterations int
}

// Rows returns the complete result cardinality.
func (r RecursiveResult) Rows() int { return r.view.rows }

// Columns returns the positional width of every row.
func (r RecursiveResult) Columns() int { return r.view.columns() }

// Iterations returns the number of recursive Step evaluations completed.
func (r RecursiveResult) Iterations() int { return r.iterations }

// Cell returns one result cell, or SQL NULL for an invalid ordinal.
func (r RecursiveResult) Cell(row, column int) Cell { return r.view.cell(row, column) }

// Missing reports whether a NULL result cell retains the missing-path marker.
func (r RecursiveResult) Missing(row, column int) bool {
	return r.view.missing(row, column)
}
