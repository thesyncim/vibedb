package query

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync/atomic"
)

var (
	// ErrRecursiveCTEConfig classifies an invalid lowering-neutral descriptor
	// or an execution against the wrong statement frame.
	ErrRecursiveCTEConfig = errors.New("query: invalid recursive CTE runtime configuration")
	// ErrRecursiveCTEArity classifies incompatible anchor, recursive-term, and
	// explicit alias widths.
	ErrRecursiveCTEArity = errors.New("query: recursive CTE term arity mismatch")
	// ErrRecursiveCTEReentry identifies a dependency cycle, re-entrant call, or
	// concurrent use of one single-owner runtime.
	ErrRecursiveCTEReentry = errors.New("query: recursive CTE runtime re-entry")
)

// RecursiveCTEMaterialization controls reuse inside one statement execution.
// Shared evaluates once and returns the same exact snapshot to every reference;
// ReferenceLocal reevaluates for each reference and invalidates the prior view.
type RecursiveCTEMaterialization uint8

const (
	RecursiveCTEReferenceLocal RecursiveCTEMaterialization = iota
	RecursiveCTEShared
)

func (m RecursiveCTEMaterialization) valid() bool {
	return m == RecursiveCTEReferenceLocal || m == RecursiveCTEShared
}

// RecursiveCTELimits are the physical fixpoint limits. Zero selects the same
// finite defaults as RecursiveFixpointOptions; minus one disables one limit.
type RecursiveCTELimits struct {
	MaxIterations int
	MaxRows       int
	MaxBytes      int64
}

// RecursiveCTETerm is the prepared, lowering-neutral boundary used by the
// physical recursive runtime. Implementations must be safe for concurrent use
// when one descriptor is executed by independent runtimes.
//
// RunRecursiveCTETerm receives the original coherent base source on every
// invocation. Recursive invocations additionally receive exactly the previous
// breadth-first delta. This two-input contract is what lets a future Statement
// adapter bind the delta as the recursive relation while resolving ordinary
// base collections from the original database snapshot.
type RecursiveCTETerm interface {
	RecursiveCTEColumns() []string
	RunRecursiveCTETerm(*Exec, RecursiveCTETermInput) error
}

// RecursiveCTETermInput is valid only for one synchronous term invocation.
// Base is the exact Source passed to the runtime; it is never reconstructed or
// resnapshotted. Delta is invalid for the anchor and names only the previous
// breadth-first level for recursive invocations.
type RecursiveCTETermInput struct {
	Base      Source
	Delta     RecursiveCTEDelta
	Iteration int
}

// Collection resolves name from the same coherent database snapshot as Base.
// It never takes a new snapshot. A non-database Base cannot resolve a sibling
// collection and returns the same diagnostic ordinary joins use.
func (i RecursiveCTETermInput) Collection(name string) (Source, error) {
	if name == "" {
		return Source{}, fmt.Errorf(
			"query: recursive CTE term requested an empty base collection: %w",
			ErrRecursiveCTEConfig,
		)
	}
	switch i.Base.kind {
	case sourceDatabase, sourceFileDatabase:
		return i.Base.subquerySource(i.Base.name, name)
	default:
		if name == i.Base.name && i.Base.kind != sourceInvalid {
			return i.Base, nil
		}
		return Source{}, fmt.Errorf(
			"query: recursive CTE term cannot resolve base collection %q without a coherent database Source: %w",
			name, ErrRecursiveCTEConfig,
		)
	}
}

// RecursiveCTEDelta is a read-only columnar binding of one breadth-first
// generation. Its Source is accepted by an ordinary delta-only Query. The
// value and Source are valid only until RunRecursiveCTETerm returns.
type RecursiveCTEDelta struct {
	relation  *relationSpool
	iteration int
}

func (d RecursiveCTEDelta) Valid() bool { return d.relation != nil }

func (d RecursiveCTEDelta) Rows() int {
	if d.relation == nil {
		return 0
	}
	return d.relation.rows
}

func (d RecursiveCTEDelta) Columns() int {
	if d.relation == nil {
		return 0
	}
	return len(d.relation.columns)
}

func (d RecursiveCTEDelta) Iteration() int { return d.iteration }

func (d RecursiveCTEDelta) Cell(row, column int) Cell {
	if d.relation == nil || row < 0 || row >= d.relation.rows ||
		column < 0 || column >= len(d.relation.columns) {
		return nullCell()
	}
	return cellFromScalar(d.relation.columns[column][row])
}

func (d RecursiveCTEDelta) Missing(row, column int) bool {
	return d.relation != nil && row >= 0 && row < d.relation.rows &&
		column >= 0 && column < len(d.relation.columns) &&
		d.relation.columns[column][row].kind == kindNull &&
		d.relation.columns[column][row].raw == nil
}

func (d RecursiveCTEDelta) Source() Source {
	if d.relation == nil {
		return Source{}
	}
	return fromRelationSpool(d.relation)
}

// RecursiveCTEPreparedTerm is an immutable, separately compiled Query term.
// For the anchor it runs against input.Base. For the recursive term it runs
// against input.Delta, making this the fast convenience for delta-only terms.
// A Query that must join delta to base collections cannot use this adapter,
// because Query has only one root Source; use RecursiveCTECallbackTerm now and
// the future Statement relation-binding adapter after SQL lowering lands.
// Names are stable copies of the compiled output headers. The query is borrowed
// and must outlive every descriptor and runtime using the term.
type RecursiveCTEPreparedTerm struct {
	query *Query
	names []string
}

// PrepareRecursiveCTETerm compiles query and snapshots its output schema. This
// is deliberately independent of SQL parsing: future WITH RECURSIVE lowering
// can build the two Query plans and install the recursive ordinal binding.
func PrepareRecursiveCTETerm(query *Query) (*RecursiveCTEPreparedTerm, error) {
	if query == nil {
		return nil, fmt.Errorf("query: recursive CTE term is nil: %w", ErrRecursiveCTEConfig)
	}
	if err := query.Prepare(); err != nil {
		return nil, err
	}
	schema := query.AppendSchema(nil)
	if len(schema) == 0 {
		return nil, fmt.Errorf("query: recursive CTE term has no outputs: %w", ErrRecursiveCTEConfig)
	}
	term := &RecursiveCTEPreparedTerm{query: query, names: make([]string, len(schema))}
	for ordinal := range schema {
		term.names[ordinal] = strings.Clone(schema[ordinal].Header)
	}
	return term, nil
}

// Columns returns read-only output headers in ordinal order.
func (t *RecursiveCTEPreparedTerm) Columns() []string {
	if t == nil {
		return nil
	}
	return t.names[:len(t.names):len(t.names)]
}

func (t *RecursiveCTEPreparedTerm) RecursiveCTEColumns() []string { return t.Columns() }

func (t *RecursiveCTEPreparedTerm) RunRecursiveCTETerm(
	exec *Exec,
	input RecursiveCTETermInput,
) error {
	if t == nil || t.query == nil {
		return fmt.Errorf("query: recursive CTE term is released or nil: %w", ErrRecursiveCTEConfig)
	}
	source := input.Base
	if input.Delta.Valid() {
		source = input.Delta.Source()
	}
	return t.query.RunInto(exec, source)
}

// RecursiveCTETermFunc implements one already-prepared physical term. It must
// publish its complete columnar output in the supplied Exec and must not retain
// input.Base, input.Delta, or cells borrowed from them beyond the call.
type RecursiveCTETermFunc func(*Exec, RecursiveCTETermInput) error

// RecursiveCTECallbackTerm adapts a prepared physical callback to the runtime.
// It is the current honest extension point for base+delta execution while SQL
// Statement lowering lacks a recursive relation-binding slot.
type RecursiveCTECallbackTerm struct {
	names []string
	run   RecursiveCTETermFunc
}

func PrepareRecursiveCTECallbackTerm(
	columns []string,
	run RecursiveCTETermFunc,
) (*RecursiveCTECallbackTerm, error) {
	if len(columns) == 0 || run == nil {
		return nil, fmt.Errorf(
			"query: recursive CTE callback needs columns and an implementation: %w",
			ErrRecursiveCTEConfig,
		)
	}
	term := &RecursiveCTECallbackTerm{
		names: make([]string, len(columns)), run: run,
	}
	for ordinal := range columns {
		term.names[ordinal] = strings.Clone(columns[ordinal])
	}
	return term, nil
}

func (t *RecursiveCTECallbackTerm) RecursiveCTEColumns() []string {
	if t == nil {
		return nil
	}
	return t.names[:len(t.names):len(t.names)]
}

func (t *RecursiveCTECallbackTerm) RunRecursiveCTETerm(
	exec *Exec,
	input RecursiveCTETermInput,
) error {
	if t == nil || t.run == nil {
		return fmt.Errorf("query: recursive CTE callback is nil: %w", ErrRecursiveCTEConfig)
	}
	return t.run(exec, input)
}

// RecursiveCTEDescriptor is immutable physical metadata for one recursive CTE.
// The anchor and recursive term are prepared separately. A Query-adapted
// recursive term addresses delta columns by ordinal relation paths (/0, /1,
// ...); a general term receives both base and delta through its input.
type RecursiveCTEDescriptor struct {
	name          string
	columns       []string
	anchor        RecursiveCTETerm
	recursive     RecursiveCTETerm
	union         RecursiveUnionMode
	materialize   RecursiveCTEMaterialization
	limits        recursiveFixpointOptions
	configuredMax int64
}

// PrepareRecursiveCTEDescriptor validates stable arity and snapshots output
// names. aliases may be empty, in which case anchor headers become the CTE
// headers; otherwise its width must exactly match both terms.
func PrepareRecursiveCTEDescriptor(
	name string,
	aliases []string,
	anchor, recursive RecursiveCTETerm,
	union RecursiveUnionMode,
	materialization RecursiveCTEMaterialization,
	limits RecursiveCTELimits,
) (*RecursiveCTEDescriptor, error) {
	if recursiveCTETermNil(anchor) || recursiveCTETermNil(recursive) {
		return nil, fmt.Errorf(
			"query: recursive CTE %q has a nil prepared term: %w",
			name, ErrRecursiveCTEConfig,
		)
	}
	return prepareRecursiveCTEDescriptor(
		name, aliases, anchor, recursive, union, materialization, limits,
	)
}

func prepareRecursiveCTEDescriptor(
	name string,
	aliases []string,
	anchor, recursive RecursiveCTETerm,
	union RecursiveUnionMode,
	materialization RecursiveCTEMaterialization,
	limits RecursiveCTELimits,
) (*RecursiveCTEDescriptor, error) {
	if name == "" || recursiveCTETermNil(anchor) || recursiveCTETermNil(recursive) || !union.valid() ||
		!materialization.valid() {
		return nil, fmt.Errorf("query: malformed recursive CTE descriptor: %w", ErrRecursiveCTEConfig)
	}
	anchorColumns, recursiveColumns := anchor.RecursiveCTEColumns(), recursive.RecursiveCTEColumns()
	if len(anchorColumns) == 0 {
		return nil, fmt.Errorf(
			"query: recursive CTE %q anchor has no outputs: %w",
			name, ErrRecursiveCTEConfig,
		)
	}
	if len(anchorColumns) != len(recursiveColumns) {
		return nil, &RecursiveCTEArityError{
			Name: name, Term: "recursive", Expected: len(anchorColumns),
			Actual: len(recursiveColumns),
		}
	}
	if len(aliases) != 0 && len(aliases) != len(anchorColumns) {
		return nil, &RecursiveCTEArityError{
			Name: name, Term: "aliases", Expected: len(anchorColumns), Actual: len(aliases),
		}
	}
	options, err := normalizeRecursiveFixpointOptions(RecursiveFixpointOptions{
		Columns: len(anchorColumns), Union: union,
		MaxIterations: limits.MaxIterations,
		MaxRows:       limits.MaxRows, MaxBytes: limits.MaxBytes,
	})
	if err != nil {
		return nil, fmt.Errorf(
			"query: recursive CTE %q limits are invalid: %w: %w",
			name, ErrRecursiveCTEConfig, err,
		)
	}
	descriptor := &RecursiveCTEDescriptor{
		name: strings.Clone(name), anchor: anchor, recursive: recursive,
		union: union, materialize: materialization, limits: options,
		configuredMax: options.maxBytes,
		columns:       make([]string, len(anchorColumns)),
	}
	for ordinal := range descriptor.columns {
		header := anchorColumns[ordinal]
		if len(aliases) != 0 {
			header = aliases[ordinal]
		}
		descriptor.columns[ordinal] = strings.Clone(header)
	}
	return descriptor, nil
}

func recursiveCTETermNil(term RecursiveCTETerm) bool {
	if term == nil {
		return true
	}
	value := reflect.ValueOf(term)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Name returns the immutable CTE name used by diagnostics.
func (d *RecursiveCTEDescriptor) Name() string {
	if d == nil {
		return ""
	}
	return d.name
}

// Columns returns the stable result names in ordinal order.
func (d *RecursiveCTEDescriptor) Columns() []string {
	if d == nil {
		return nil
	}
	return d.columns[:len(d.columns):len(d.columns)]
}

// RecursiveCTEArityError identifies the descriptor component whose width does
// not match the anchor's prepared output width.
type RecursiveCTEArityError struct {
	Name     string
	Term     string
	Expected int
	Actual   int
}

func (e *RecursiveCTEArityError) Error() string {
	return fmt.Sprintf(
		"query: recursive CTE %q %s has %d columns, want %d: %v",
		e.Name, e.Term, e.Actual, e.Expected, ErrRecursiveCTEArity,
	)
}

func (e *RecursiveCTEArityError) Unwrap() error { return ErrRecursiveCTEArity }

// RecursiveCTEReentryError names the CTE whose runtime was entered while it
// was already evaluating. It unwraps both the bridge-specific classification
// and the underlying fixpoint single-owner classification.
type RecursiveCTEReentryError struct{ Name string }

func (e *RecursiveCTEReentryError) Error() string {
	return fmt.Sprintf(
		"query: recursive CTE %q is already evaluating (dependency cycle, re-entry, or concurrent use): %v",
		e.Name, ErrRecursiveCTEReentry,
	)
}

func (e *RecursiveCTEReentryError) Unwrap() []error {
	return []error{ErrRecursiveCTEReentry, ErrRecursiveInUse}
}

// RecursiveCTEIntermediateError reports that the shared statement account,
// rather than the descriptor's own MaxBytes, constrained the fixpoint. It
// remains matchable as both ErrIntermediateBudget and ErrRecursiveBytes.
type RecursiveCTEIntermediateError struct {
	Name  string
	Bytes int64
	Limit int64
}

func (e *RecursiveCTEIntermediateError) Error() string {
	return fmt.Sprintf(
		"query: recursive CTE %q needs %d bytes inside statement intermediate limit %d: %v",
		e.Name, e.Bytes, e.Limit, ErrIntermediateBudget,
	)
}

func (e *RecursiveCTEIntermediateError) Unwrap() []error {
	return []error{ErrIntermediateBudget, ErrRecursiveBytes}
}

type recursiveCTERuntimeState uint8

const (
	recursiveCTEIdle recursiveCTERuntimeState = iota
	recursiveCTEReady
)

// RecursiveCTERuntime is one prepared definition's reusable execution state.
// It is single-owner; independent runtimes may execute one immutable descriptor
// concurrently. Release must not race an execution.
type RecursiveCTERuntime struct {
	running atomic.Bool

	fixpoint RecursiveFixpoint
	program  recursiveCTEProgram

	anchorExec    Exec
	recursiveExec Exec
	row           []Cell
	delta         relationSpool
	snapshot      relationSpool

	state      recursiveCTERuntimeState
	descriptor *RecursiveCTEDescriptor
	frame      *statementFrame

	fixpointBytes int64
	snapshotBytes int64
	evaluations   uint64
}

// RecursiveCTEResult is an exact columnar snapshot of one fixpoint. It borrows
// the runtime until the next reference-local evaluation, owning-statement
// cleanup, or Release. Shared references in one frame return the same relation
// pointer.
type RecursiveCTEResult struct {
	names      []string
	relation   *relationSpool
	iterations int
	evaluation uint64
}

func (r RecursiveCTEResult) Rows() int {
	if r.relation == nil {
		return 0
	}
	return r.relation.rows
}

func (r RecursiveCTEResult) Columns() []string {
	return r.names[:len(r.names):len(r.names)]
}

func (r RecursiveCTEResult) Cell(row, column int) Cell {
	if r.relation == nil || row < 0 || row >= r.relation.rows ||
		column < 0 || column >= len(r.relation.columns) {
		return nullCell()
	}
	return cellFromScalar(r.relation.columns[column][row])
}

func (r RecursiveCTEResult) Missing(row, column int) bool {
	return r.relation != nil && row >= 0 && row < r.relation.rows &&
		column >= 0 && column < len(r.relation.columns) &&
		r.relation.columns[column][row].kind == kindNull &&
		r.relation.columns[column][row].raw == nil
}

func (r RecursiveCTEResult) Iterations() int    { return r.iterations }
func (r RecursiveCTEResult) Evaluation() uint64 { return r.evaluation }

// source is the lowering bridge used by ordinary CTE references.
func (r RecursiveCTEResult) source() Source { return fromRelationSpool(r.relation) }

// execute evaluates one descriptor inside an existing statement frame. The
// caller consumes reference-local results before the next execute and calls
// releaseExecution when the owning statement execution ends.
func (r *RecursiveCTERuntime) execute(
	descriptor *RecursiveCTEDescriptor,
	anchorSource Source,
	frame *statementFrame,
	options ExecOptions,
) (result RecursiveCTEResult, err error) {
	if r == nil || descriptor == nil || frame == nil {
		return RecursiveCTEResult{}, fmt.Errorf("query: nil recursive CTE runtime input: %w", ErrRecursiveCTEConfig)
	}
	if !r.running.CompareAndSwap(false, true) {
		return RecursiveCTEResult{}, &RecursiveCTEReentryError{Name: descriptor.name}
	}
	defer r.running.Store(false)

	if r.frame != nil && r.frame != frame {
		return RecursiveCTEResult{}, fmt.Errorf(
			"query: recursive CTE runtime still belongs to another execution frame: %w",
			ErrRecursiveCTEConfig,
		)
	}
	if r.state == recursiveCTEReady && r.descriptor != descriptor {
		return RecursiveCTEResult{}, fmt.Errorf(
			"query: recursive CTE runtime is bound to %q, not %q: %w",
			r.descriptor.name, descriptor.name, ErrRecursiveCTEConfig,
		)
	}
	if r.state == recursiveCTEReady && descriptor.materialize == RecursiveCTEShared {
		if err := cancellationError(options.Cancel); err != nil {
			return RecursiveCTEResult{}, err
		}
		return r.currentResult(), nil
	}
	r.resetExecution(frame)
	r.descriptor = descriptor
	r.frame = frame
	r.evaluations++

	r.program = recursiveCTEProgram{
		runtime: r, descriptor: descriptor, anchorSource: anchorSource,
		frame: frame, options: options, configuredMax: descriptor.configuredMax,
	}
	fixpointOptions := RecursiveFixpointOptions{
		Columns: descriptor.limits.columns, Union: descriptor.union,
		MaxIterations: descriptor.limits.maxIterations,
		MaxRows:       descriptor.limits.maxRows,
		MaxBytes:      descriptor.limits.maxBytes,
		Cancel:        options.Cancel,
	}
	fixpoint, runErr := r.fixpoint.Run(&r.program, fixpointOptions)
	if runErr != nil {
		frameLimited := r.program.frameLimited
		frameBaseBytes := r.program.frameBaseBytes
		r.abortExecution(frame)
		if byteErr := new(RecursiveByteBudgetError); errors.As(runErr, &byteErr) &&
			frameLimited {
			return RecursiveCTEResult{}, &RecursiveCTEIntermediateError{
				Name: descriptor.name,
				Bytes: saturatedBytes(
					frameBaseBytes, byteErr.Bytes,
				),
				Limit: frame.intermediate.limit,
			}
		}
		return RecursiveCTEResult{}, runErr
	}
	r.program.iterations = fixpoint.Iterations()
	if err = r.syncFixpointCharge(frame); err != nil {
		r.abortExecution(frame)
		return RecursiveCTEResult{}, err
	}
	if err = r.publishSnapshot(fixpoint, descriptor.columns, frame, options.Cancel); err != nil {
		r.abortExecution(frame)
		return RecursiveCTEResult{}, err
	}
	r.state = recursiveCTEReady
	return r.currentResult(), nil
}

func (r *RecursiveCTERuntime) currentResult() RecursiveCTEResult {
	if r == nil || r.state != recursiveCTEReady || r.descriptor == nil {
		return RecursiveCTEResult{}
	}
	return RecursiveCTEResult{
		names: r.descriptor.columns, relation: &r.snapshot,
		iterations: r.program.iterations, evaluation: r.evaluations,
	}
}

type recursiveCTEProgram struct {
	runtime        *RecursiveCTERuntime
	descriptor     *RecursiveCTEDescriptor
	anchorSource   Source
	frame          *statementFrame
	options        ExecOptions
	configuredMax  int64
	iterations     int
	frameLimited   bool
	frameBaseBytes int64
}

func (p *recursiveCTEProgram) Anchor(out *RecursiveAppender) error {
	return p.runTerm(
		p.descriptor.anchor,
		&p.runtime.anchorExec,
		RecursiveCTETermInput{Base: p.anchorSource},
		out,
	)
}

func (p *recursiveCTEProgram) Step(delta RecursiveDelta, out *RecursiveAppender) error {
	if err := p.runtime.syncFixpointCharge(p.frame); err != nil {
		return err
	}
	charge, err := p.runtime.bindDelta(delta, p.frame, p.options.Cancel)
	if err != nil {
		return err
	}
	defer func() {
		p.runtime.delta.reset()
		p.frame.intermediate.release(charge)
	}()
	p.iterations = delta.Iteration() + 1
	return p.runTerm(
		p.descriptor.recursive, &p.runtime.recursiveExec,
		RecursiveCTETermInput{
			Base: p.anchorSource,
			Delta: RecursiveCTEDelta{
				relation: &p.runtime.delta, iteration: delta.Iteration(),
			},
			Iteration: delta.Iteration() + 1,
		},
		out,
	)
}

func (p *recursiveCTEProgram) runTerm(
	term RecursiveCTETerm,
	exec *Exec,
	input RecursiveCTETermInput,
	out *RecursiveAppender,
) (err error) {
	if err := cancellationError(p.options.Cancel); err != nil {
		return err
	}
	clearExecBorrowedViews(exec)
	exec.Options = p.options
	exec.Options.ResultRows = -1
	remaining := p.frame.intermediate.remaining()
	if remaining == 0 {
		return &IntermediateBudgetError{
			Resource: "recursive CTE term result",
			Bytes:    saturatedBytes(p.frame.intermediate.used, 1),
			Limit:    p.frame.intermediate.limit,
		}
	}
	exec.Options.ResultBytes = remaining
	if err := term.RunRecursiveCTETerm(exec, input); err != nil {
		var resultErr *ResultBudgetError
		if remaining >= 0 && errors.As(err, &resultErr) && resultErr.ByteLimit == remaining {
			return &IntermediateBudgetError{
				Resource: "recursive CTE term result",
				Bytes:    saturatedBytes(p.frame.intermediate.used, resultErr.Bytes),
				Limit:    p.frame.intermediate.limit,
			}
		}
		return err
	}
	resultBytes, validateErr := validateRecursiveCTETermResult(
		exec, len(p.descriptor.columns), p.descriptor.name, p.options.Cancel,
	)
	if validateErr != nil {
		clearExecBorrowedViews(exec)
		return validateErr
	}
	if err := p.frame.intermediate.reserve("recursive CTE term result", resultBytes); err != nil {
		clearExecBorrowedViews(exec)
		return err
	}
	defer func() {
		p.frame.intermediate.release(resultBytes)
		clearExecBorrowedViews(exec)
	}()

	columns := len(p.descriptor.columns)
	if cap(p.runtime.row) < columns {
		p.runtime.row = make([]Cell, columns)
	} else {
		p.runtime.row = p.runtime.row[:columns]
	}
	p.installByteLimit()
	for row := 0; row < exec.Result.RowCount; row++ {
		if err := cancellationCheckpoint(p.options.Cancel, row); err != nil {
			return err
		}
		for column := 0; column < columns; column++ {
			if row >= len(exec.Result.Columns[column].Cells) {
				return &RecursiveCTEArityError{
					Name: p.descriptor.name, Term: "execution column", Expected: exec.Result.RowCount,
					Actual: len(exec.Result.Columns[column].Cells),
				}
			}
			p.runtime.row[column] = exec.Result.Columns[column].Cells[row]
		}
		if err := out.AppendRow(p.runtime.row); err != nil {
			return err
		}
	}
	return p.runtime.syncFixpointCharge(p.frame)
}

func validateRecursiveCTETermResult(
	exec *Exec,
	columns int,
	name string,
	cancel *CancelFlag,
) (int64, error) {
	if exec == nil || exec.Result.RowCount < 0 {
		return 0, fmt.Errorf(
			"query: recursive CTE %q returned a negative row count: %w",
			name, ErrRecursiveCTEConfig,
		)
	}
	if len(exec.Result.Columns) != columns {
		return 0, &RecursiveCTEArityError{
			Name: name, Term: "execution", Expected: columns,
			Actual: len(exec.Result.Columns),
		}
	}
	payload := int64(0)
	for column := 0; column < columns; column++ {
		cells := exec.Result.Columns[column].Cells
		if len(cells) != exec.Result.RowCount {
			return 0, &RecursiveCTEArityError{
				Name: name, Term: "execution column", Expected: exec.Result.RowCount,
				Actual: len(cells),
			}
		}
		for row := range cells {
			if err := cancellationCheckpoint(cancel, row); err != nil {
				return 0, err
			}
			if cells[row].kind < TypeNull || cells[row].kind > TypeJSON {
				return 0, fmt.Errorf(
					"query: recursive CTE %q returned invalid cell type %d: %w",
					name, cells[row].kind, ErrRecursiveCTEConfig,
				)
			}
			payload = saturatedBytes(payload, resultCellPayloadBytes(cells[row]))
			if payload == math.MaxInt64 {
				return 0, ErrRecursiveSize
			}
		}
	}
	required, err := exec.Result.checkResultBudget(
		columns, exec.Result.RowCount, payload,
	)
	if err != nil {
		return 0, err
	}
	if required != exec.Result.resultBytesUsed {
		return 0, fmt.Errorf(
			"query: recursive CTE %q term reported %d result bytes, measured %d: %w",
			name, exec.Result.resultBytesUsed, required, ErrRecursiveCTEConfig,
		)
	}
	return required, cancellationError(cancel)
}

func (p *recursiveCTEProgram) installByteLimit() {
	current := recursiveCTEFixpointBytes(&p.runtime.fixpoint)
	available := p.configuredMax
	if remaining := p.frame.intermediate.remaining(); remaining >= 0 {
		frameMax := saturatedBytes(current, remaining)
		if available < 0 || frameMax < available {
			available = frameMax
			p.frameLimited = true
			p.frameBaseBytes = max(p.frame.intermediate.used-current, 0)
		}
	}
	p.runtime.fixpoint.options.maxBytes = available
}

func recursiveCTEFixpointBytes(f *RecursiveFixpoint) int64 {
	if f == nil {
		return 0
	}
	return saturatedBytes(
		saturatedBytes(f.result.retainedBytes(), f.working.retainedBytes()),
		f.identity.retainedBytes(),
	)
}

func (r *RecursiveCTERuntime) syncFixpointCharge(frame *statementFrame) error {
	current := recursiveCTEFixpointBytes(&r.fixpoint)
	if current == math.MaxInt64 {
		return ErrRecursiveSize
	}
	if current > r.fixpointBytes {
		if err := frame.intermediate.reserve(
			"recursive CTE fixpoint", current-r.fixpointBytes,
		); err != nil {
			return err
		}
	} else {
		frame.intermediate.release(r.fixpointBytes - current)
	}
	r.fixpointBytes = current
	return nil
}

func (r *RecursiveCTERuntime) bindDelta(
	delta RecursiveDelta,
	frame *statementFrame,
	cancel *CancelFlag,
) (int64, error) {
	return r.bindRecursiveView(delta.view, &r.delta, frame, cancel, "recursive CTE delta view")
}

func (r *RecursiveCTERuntime) bindRecursiveView(
	view recursiveView,
	destination *relationSpool,
	frame *statementFrame,
	cancel *CancelFlag,
	resource string,
) (charge int64, err error) {
	destination.reset()
	columns := view.columns()
	charge = relationSpoolRetainedBytes(view.rows, columns, 0)
	if charge == math.MaxInt64 {
		return 0, ErrRecursiveSize
	}
	if err := frame.intermediate.reserve(resource, charge); err != nil {
		return 0, err
	}
	reserved := charge
	defer func() {
		if err != nil {
			destination.reset()
			// A return such as "return 0, err" assigns the named charge result
			// before deferred functions run. Keep the accepted reservation in a
			// separate local so every partial publication releases it exactly.
			frame.intermediate.release(reserved)
			charge = 0
		}
	}()
	if err = destination.begin(view.rows, columns, 0); err != nil {
		return 0, err
	}
	for column := 0; column < columns; column++ {
		for row := 0; row < view.rows; row++ {
			if err = cancellationCheckpoint(cancel, row); err != nil {
				return 0, err
			}
			destination.columns[column][row] = view.spool.scalar(view.start+row, column)
		}
	}
	return charge, cancellationError(cancel)
}

func (r *RecursiveCTERuntime) publishSnapshot(
	result RecursiveResult,
	names []string,
	frame *statementFrame,
	cancel *CancelFlag,
) error {
	charge, err := r.bindRecursiveView(
		result.view, &r.snapshot, frame, cancel, "recursive CTE result view",
	)
	if err != nil {
		return err
	}
	if len(names) != result.Columns() {
		r.snapshot.reset()
		frame.intermediate.release(charge)
		return &RecursiveCTEArityError{
			Name: r.descriptor.name, Term: "result", Expected: len(names), Actual: result.Columns(),
		}
	}
	r.snapshotBytes = charge
	return nil
}

func (r *RecursiveCTERuntime) resetExecution(frame *statementFrame) {
	if r == nil {
		return
	}
	clearExecBorrowedViews(&r.anchorExec)
	clearExecBorrowedViews(&r.recursiveExec)
	r.snapshot.reset()
	frame.intermediate.release(r.snapshotBytes)
	r.snapshotBytes = 0
	frame.intermediate.release(r.fixpointBytes)
	r.fixpointBytes = 0
	r.fixpoint.result.reset()
	r.fixpoint.working.reset()
	r.fixpoint.identity.reset()
	r.fixpoint.options = recursiveFixpointOptions{}
	r.delta.reset()
	clear(r.row)
	r.anchorExec.Options = ExecOptions{}
	r.recursiveExec.Options = ExecOptions{}
	r.program = recursiveCTEProgram{}
	r.state = recursiveCTEIdle
	r.descriptor = nil
	r.frame = nil
}

func (r *RecursiveCTERuntime) abortExecution(frame *statementFrame) {
	if r == nil {
		return
	}
	clearExecBorrowedViews(&r.anchorExec)
	clearExecBorrowedViews(&r.recursiveExec)
	r.delta.reset()
	r.snapshot.reset()
	frame.intermediate.release(r.snapshotBytes)
	frame.intermediate.release(r.fixpointBytes)
	r.snapshotBytes = 0
	r.fixpointBytes = 0
	r.fixpoint.result.reset()
	r.fixpoint.working.reset()
	r.fixpoint.identity.reset()
	r.fixpoint.options = recursiveFixpointOptions{}
	clear(r.row)
	r.anchorExec.Options = ExecOptions{}
	r.recursiveExec.Options = ExecOptions{}
	r.program = recursiveCTEProgram{}
	r.state = recursiveCTEIdle
	r.descriptor = nil
	r.frame = nil
}

// releaseExecution ends one statement execution while retaining warmed
// storage. It is idempotent for the matching frame.
func (r *RecursiveCTERuntime) releaseExecution(frame *statementFrame) {
	if r == nil || frame == nil || r.frame != frame {
		return
	}
	r.abortExecution(frame)
}

// Evaluations returns term-pair evaluations since the last Release. Shared
// cache hits do not increment it.
func (r *RecursiveCTERuntime) Evaluations() uint64 {
	if r == nil {
		return 0
	}
	return r.evaluations
}

// Release drops all retained high-water storage. It is idempotent and must not
// race execute.
func (r *RecursiveCTERuntime) Release() {
	if r == nil {
		return
	}
	if r.frame != nil {
		r.releaseExecution(r.frame)
	}
	r.fixpoint.Release()
	r.anchorExec.Release()
	r.recursiveExec.Release()
	r.delta.release()
	r.snapshot.release()
	*r = RecursiveCTERuntime{}
}
