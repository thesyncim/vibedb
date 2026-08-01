package query

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
)

var errSetStatementConfig = errors.New("query: invalid prepared set statement")

// setStatementRunner is the prepare-neutral contract implemented by Statement
// and by focused runtime tests. Runners stay owned by lowering; a descriptor
// borrows their immutable metadata and a runtime reuses their execution state.
type setStatementRunner interface {
	Columns() []string
	NumParams() int
	Collection() string
	AppendSchema([]OutputColumn) []OutputColumn
	runIntoFrame(
		*Exec, Source, []any, *statementFrame, string,
	) (Cursor, error)
	releaseRelations(*statementFrame)
}

type setStatementLeaf struct {
	runner    setStatementRunner
	paramBase int
}

// setStatementDescriptor is immutable prepared metadata. plan owns its node
// slice; names and schema are copied from the syntactic first operand, as SQL
// requires, while every operation remains ordinal.
type setStatementDescriptor struct {
	plan    SetTreePlan
	leaves  []setStatementLeaf
	names   []string
	schema  []OutputColumn
	params  int
	driving string
}

func prepareSetStatementDescriptor(
	plan SetTreePlan,
	leaves []setStatementLeaf,
	firstOperand, params int,
) (*setStatementDescriptor, error) {
	if len(leaves) == 0 || firstOperand < 0 || firstOperand >= len(leaves) || params < 0 {
		return nil, fmt.Errorf(
			"query: set statement has %d leaves, first operand %d, and %d parameters: %w",
			len(leaves), firstOperand, params, errSetStatementConfig,
		)
	}

	d := &setStatementDescriptor{
		plan: SetTreePlan{
			Nodes: append([]SetTreeNode(nil), plan.Nodes...),
			Root:  plan.Root,
		},
		leaves: append([]setStatementLeaf(nil), leaves...),
		params: params,
	}
	for leaf := range d.leaves {
		runner := d.leaves[leaf].runner
		if runner == nil {
			return nil, fmt.Errorf(
				"query: set statement leaf %d has no prepared runner: %w",
				leaf, errSetStatementConfig,
			)
		}
		count := runner.NumParams()
		base := d.leaves[leaf].paramBase
		if count < 0 || base < 0 || base > params || count > params-base {
			return nil, fmt.Errorf(
				"query: set statement leaf %d parameter range [%d,%d) exceeds %d: %w",
				leaf, base, saturatedSetStatementParamEnd(base, count), params,
				errSetStatementConfig,
			)
		}
	}

	first := d.leaves[firstOperand].runner
	d.names = append(d.names, first.Columns()...)
	d.schema = first.AppendSchema(d.schema[:0])
	if len(d.schema) != len(d.names) {
		return nil, fmt.Errorf(
			"query: first set operand has %d names and %d schema columns: %w",
			len(d.names), len(d.schema), errSetStatementConfig,
		)
	}
	for column := range d.schema {
		d.schema[column].Header = d.names[column]
		d.schema[column].Ordinal = uint32(column)
	}
	d.driving = first.Collection()

	var validator SetTreeExecutor
	validator.options = setTreeOptions{
		maxRows: -1, maxBytes: -1, maxDepth: -1, maxNodes: -1,
	}
	maxSlots, err := validator.preflightPlan(d.plan)
	if err == nil {
		err = validator.preparePlanStorage(len(d.plan.Nodes), maxSlots)
	}
	if err == nil {
		err = validator.validatePlan(d.plan)
	}
	validator.Release()
	if err != nil {
		return nil, err
	}

	used := make([]bool, len(d.leaves))
	for nodeIndex, node := range d.plan.Nodes {
		if node.Kind != SetTreeLeafNode {
			continue
		}
		if node.Source < 0 || node.Source >= len(d.leaves) {
			return nil, fmt.Errorf(
				"query: set-expression node %d names leaf %d outside %d runners: %w",
				nodeIndex, node.Source, len(d.leaves), ErrSetTreePlan,
			)
		}
		width := len(d.leaves[node.Source].runner.Columns())
		if node.Columns != width {
			return nil, &SetTreeArityError{
				Node: nodeIndex, Left: node.Columns, Right: width,
			}
		}
		used[node.Source] = true
	}
	for leaf := range used {
		if !used[leaf] {
			return nil, fmt.Errorf(
				"query: prepared set leaf %d is disconnected: %w",
				leaf, ErrSetTreePlan,
			)
		}
	}
	if !used[firstOperand] {
		return nil, fmt.Errorf(
			"query: first set operand %d is not in the plan: %w",
			firstOperand, ErrSetTreePlan,
		)
	}
	return d, nil
}

func saturatedSetStatementParamEnd(base, count int) int {
	if base < 0 || count < 0 || base > math.MaxInt-count {
		return math.MaxInt
	}
	return base + count
}

func (d *setStatementDescriptor) Columns() []string {
	if d == nil {
		return nil
	}
	return d.names[:len(d.names):len(d.names)]
}

func (d *setStatementDescriptor) AppendSchema(dst []OutputColumn) []OutputColumn {
	if d == nil {
		return dst
	}
	return append(dst, d.schema...)
}

func (d *setStatementDescriptor) NumParams() int {
	if d == nil {
		return 0
	}
	return d.params
}

// setStatementRuntime owns all mutable prepared execution state. It is
// single-consumer; independent runtimes over independently prepared runners
// share no state and may execute concurrently.
type setStatementRuntime struct {
	running atomic.Bool
	desc    *setStatementDescriptor
	tree    SetTreeExecutor
	execs   []Exec
	cursor  Statement

	parent *Exec
	source Source
	args   []any
	peak   int64
}

func (r *setStatementRuntime) prepare(desc *setStatementDescriptor) error {
	if r == nil || desc == nil {
		return fmt.Errorf("query: nil prepared set runtime or descriptor: %w", errSetStatementConfig)
	}
	if r.running.Load() {
		return ErrSetTreeInUse
	}
	if r.desc != desc {
		r.Release()
	}
	r.desc = desc
	r.execs = resize(r.execs, len(desc.leaves))
	r.cursor.outputs = len(desc.names)
	return nil
}

func (r *setStatementRuntime) runIntoFrame(
	parent *Exec,
	src Source,
	args []any,
	frame *statementFrame,
) (cursor Cursor, err error) {
	if r == nil || r.desc == nil {
		return Cursor{}, fmt.Errorf("query: unprepared set statement runtime: %w", errSetStatementConfig)
	}
	if parent == nil || frame == nil {
		return Cursor{}, fmt.Errorf("query: set statement requires Exec and frame: %w", errSetStatementConfig)
	}
	if !r.running.CompareAndSwap(false, true) {
		return Cursor{}, ErrSetTreeInUse
	}
	defer r.running.Store(false)

	r.peak = 0
	clearExecBorrowedViews(parent)
	parent.Stats = ExecStats{}
	if len(args) != r.desc.params {
		return Cursor{}, fmt.Errorf(
			"query: set statement has %d placeholder(s) and %d argument(s) were bound",
			r.desc.params, len(args),
		)
	}
	if err := cancellationError(parent.Options.Cancel); err != nil {
		return Cursor{}, err
	}

	r.parent, r.source, r.args = parent, src, args
	r.peak = frame.intermediate.used
	defer func() {
		r.parent = nil
		r.source = Source{}
		r.args = nil
		if err != nil {
			r.peak = 0
			clearExecBorrowedViews(parent)
		}
	}()
	options := SetTreeOptions{Cancel: parent.Options.Cancel, MaxBytes: -1}
	if err = r.tree.runInFrame(r.desc.plan, r, options, frame); err != nil {
		return Cursor{}, err
	}
	return r.cursor.cursor(&parent.Result), nil
}

func (r *setStatementRuntime) materializeSetTreeLeaf(
	source, columns int,
	dst *relationSpool,
	frame *statementFrame,
	cancel *CancelFlag,
) (rows int, charge int64, err error) {
	if source < 0 || source >= len(r.desc.leaves) {
		return 0, 0, fmt.Errorf(
			"query: set-expression leaf %d has no prepared runner: %w",
			source, ErrSetTreeSource,
		)
	}
	leaf := &r.desc.leaves[source]
	if width := len(leaf.runner.Columns()); width != columns {
		return 0, 0, &SetTreeArityError{
			Node: source, Left: columns, Right: width,
		}
	}
	count := leaf.runner.NumParams()
	base := leaf.paramBase
	if count < 0 || base < 0 || base > len(r.args) || count > len(r.args)-base {
		return 0, 0, fmt.Errorf(
			"query: invalid prepared set leaf %d placeholder range: %w",
			source, errSetStatementConfig,
		)
	}
	leafSource, err := setStatementLeafSource(
		r.source, r.desc.driving, leaf.runner.Collection(),
	)
	if err != nil {
		return 0, 0, err
	}
	exec := &r.execs[source]
	exec.Options = r.parent.Options
	resultBytes := int64(0)
	reserved := false
	defer func() {
		clearExecBorrowedViews(exec)
		leaf.runner.releaseRelations(frame)
		if reserved {
			frame.intermediate.release(resultBytes)
		}
	}()
	cursor, err := leaf.runner.runIntoFrame(
		exec, leafSource, r.args[base:base+count], frame,
		"set-expression leaf result",
	)
	if err != nil {
		return 0, 0, err
	}
	mergeSetStatementStats(&r.parent.Stats, exec.Stats)
	resultBytes = exec.Result.resultBytesUsed
	if err = frame.intermediate.reserve("set-expression leaf result", resultBytes); err != nil {
		return 0, 0, err
	}
	reserved = true
	r.observeIntermediate(frame.intermediate.used)
	if len(exec.Result.Columns) != columns {
		return 0, 0, &SetTreeArityError{
			Node: source, Left: columns, Right: len(exec.Result.Columns),
		}
	}
	charge, err = dst.materialize(
		cursor, columns, frame, cancel, "set-expression leaf spool",
	)
	if err != nil {
		return 0, 0, err
	}
	r.observeIntermediate(frame.intermediate.used)
	return dst.rows, charge, nil
}

func setStatementLeafSource(src Source, outer, collection string) (Source, error) {
	if src.kind == sourceFileOverlay && collection == outer {
		return src, nil
	}
	return src.subquerySource(outer, collection)
}

func (r *setStatementRuntime) consumeSetTreeResult(
	result SetTreeResult,
	cancel *CancelFlag,
) error {
	r.observeIntermediate(r.tree.PeakBytes())
	if result.Columns() != len(r.desc.names) {
		return &SetTreeArityError{
			Node: r.desc.plan.Root, Left: len(r.desc.names), Right: result.Columns(),
		}
	}
	return materializeSetStatementResult(
		&r.parent.Result, result, r.desc.names, r.parent.Options, cancel,
	)
}

func (r *setStatementRuntime) observeIntermediate(bytes int64) {
	if bytes > r.peak {
		r.peak = bytes
	}
}

func (r *setStatementRuntime) PeakIntermediateBytes() int64 {
	if r == nil {
		return 0
	}
	return r.peak
}

func materializeSetStatementResult(
	dst *Result,
	source SetTreeResult,
	names []string,
	options ExecOptions,
	cancel *CancelFlag,
) (err error) {
	rows, columns := source.Rows(), source.Columns()
	rowLimit, byteLimit, err := normalizeResultBudget(options)
	if err != nil {
		return err
	}
	dst.beginResultBudget(rowLimit, byteLimit)
	defer func() {
		if err != nil {
			dst.abortResult()
		}
	}()

	payload := int64(0)
	for row := 0; row < rows; row++ {
		if err = cancellationCheckpoint(cancel, row); err != nil {
			return err
		}
		for column := 0; column < columns; column++ {
			if err = cancellationCheckpoint(cancel, column); err != nil {
				return err
			}
			payload = saturatedBytes(
				payload, resultCellPayloadBytes(source.Cell(row, column)),
			)
		}
	}
	required, err := dst.checkResultBudget(columns, rows, payload)
	if err != nil {
		return err
	}
	if payload > int64(math.MaxInt) {
		return dst.resultByteBudgetError(rows, math.MaxInt64)
	}
	if err = cancellationError(cancel); err != nil {
		return err
	}

	if cap(dst.Columns) < columns {
		dst.Columns = make([]ResultColumn, columns)
	} else {
		for column := columns; column < len(dst.Columns); column++ {
			clear(dst.Columns[column].Cells)
			dst.Columns[column] = ResultColumn{}
		}
		dst.Columns = dst.Columns[:columns]
	}
	for column := 0; column < columns; column++ {
		cells := dst.Columns[column].Cells
		if rows < len(cells) {
			clear(cells[rows:])
		}
		dst.Columns[column].Header = names[column]
		dst.Columns[column].Cells = resize(cells, rows)
	}
	if cap(dst.fileData) < int(payload) {
		dst.fileData = make([]byte, 0, int(payload))
	} else {
		dst.fileData = dst.fileData[:0]
	}
	dst.resultBytesUsed = required
	for row := 0; row < rows; row++ {
		if err = cancellationCheckpoint(cancel, row); err != nil {
			return err
		}
		for column := 0; column < columns; column++ {
			if err = cancellationCheckpoint(cancel, column); err != nil {
				return err
			}
			dst.Columns[column].Cells[row] = dst.ownFileCell(source.Cell(row, column))
		}
	}
	if int64(len(dst.fileData)) != payload {
		return fmt.Errorf(
			"query: set statement result copied %d payload bytes after sizing %d: %w",
			len(dst.fileData), payload, errRelationSpoolSizing,
		)
	}
	if err = cancellationError(cancel); err != nil {
		return err
	}
	dst.RowCount = rows
	return nil
}

func mergeSetStatementStats(dst *ExecStats, src ExecStats) {
	dst.Workers = max(dst.Workers, src.Workers)
	dst.RowsTotal = saturatedSetStatementUint64(dst.RowsTotal, src.RowsTotal)
	dst.RowsScanned = saturatedSetStatementUint64(dst.RowsScanned, src.RowsScanned)
	dst.Batches = saturatedSetStatementUint64(dst.Batches, src.Batches)
	dst.PeakBatchRows = max(dst.PeakBatchRows, src.PeakBatchRows)
	dst.PeakBatchBytes = max(dst.PeakBatchBytes, src.PeakBatchBytes)
	dst.BufferedBytes = max(dst.BufferedBytes, src.BufferedBytes)
	dst.SpillRuns = saturatedSetStatementUint64(dst.SpillRuns, src.SpillRuns)
	dst.SpilledBytes = saturatedBytes(dst.SpilledBytes, src.SpilledBytes)
	dst.IndexBounded = dst.IndexBounded || src.IndexBounded
	dst.IndexLookups = saturatedSetStatementInt(dst.IndexLookups, src.IndexLookups)
	dst.IndexPostingPages = saturatedSetStatementInt(dst.IndexPostingPages, src.IndexPostingPages)
	dst.IndexCertificateRows = saturatedSetStatementUint64(
		dst.IndexCertificateRows, src.IndexCertificateRows,
	)
	dst.IndexRecheckRows = saturatedSetStatementUint64(dst.IndexRecheckRows, src.IndexRecheckRows)
	dst.CandidateRows = saturatedSetStatementUint64(dst.CandidateRows, src.CandidateRows)
	dst.CandidateChunks = saturatedSetStatementInt(dst.CandidateChunks, src.CandidateChunks)
	dst.CoveringColumns = saturatedSetStatementInt(dst.CoveringColumns, src.CoveringColumns)
	dst.JoinMemberships = saturatedSetStatementInt(dst.JoinMemberships, src.JoinMemberships)
	dst.JoinLookups = saturatedSetStatementInt(dst.JoinLookups, src.JoinLookups)
	dst.JoinKeys = saturatedSetStatementUint64(dst.JoinKeys, src.JoinKeys)
	dst.JoinProbes = saturatedSetStatementUint64(dst.JoinProbes, src.JoinProbes)
	dst.JoinFilters = saturatedSetStatementInt(dst.JoinFilters, src.JoinFilters)
	dst.JoinFilterKeys = saturatedSetStatementUint64(dst.JoinFilterKeys, src.JoinFilterKeys)
	dst.JoinFilterRejected = saturatedSetStatementUint64(
		dst.JoinFilterRejected, src.JoinFilterRejected,
	)
	dst.JoinBuilds = saturatedSetStatementInt(dst.JoinBuilds, src.JoinBuilds)
	dst.JoinBuildRows = saturatedSetStatementUint64(dst.JoinBuildRows, src.JoinBuildRows)
	dst.JoinPairs = saturatedSetStatementUint64(dst.JoinPairs, src.JoinPairs)
}

func saturatedSetStatementUint64(left, right uint64) uint64 {
	if left > math.MaxUint64-right {
		return math.MaxUint64
	}
	return left + right
}

func saturatedSetStatementInt(left, right int) int {
	if right > 0 && left > math.MaxInt-right {
		return math.MaxInt
	}
	if right < 0 && left < math.MinInt-right {
		return math.MinInt
	}
	return left + right
}

func (r *setStatementRuntime) Release() {
	if r == nil || r.running.Load() {
		return
	}
	r.tree.Release()
	for exec := range r.execs {
		r.execs[exec].Release()
	}
	r.execs = nil
	r.cursor = Statement{}
	r.desc = nil
	r.parent = nil
	r.source = Source{}
	r.args = nil
	r.peak = 0
}
