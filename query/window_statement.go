package query

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson/x/byteview"
)

// ErrWindowArgument identifies a runtime window-function argument that is
// outside the SQL domain admitted by its function or frame.
var ErrWindowArgument = errors.New("query: invalid window argument")

// WindowArgumentError preserves the clause-specific diagnostic while giving
// protocol adapters a stable class for SQLSTATE 22023.
type WindowArgumentError struct {
	Clause string
	Cause  error
}

func (e *WindowArgumentError) Error() string {
	if e == nil {
		return ErrWindowArgument.Error()
	}
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return fmt.Sprintf("query: %s must be greater than zero", e.Clause)
}

func (e *WindowArgumentError) Unwrap() []error {
	if e == nil || e.Cause == nil {
		return []error{ErrWindowArgument}
	}
	return []error{ErrWindowArgument, e.Cause}
}

// statementWindow is the prepared SQL window pipeline. It exists only for a
// statement whose SELECT list contains OVER; ordinary statements retain a nil
// nested pointer and never enter this code.
//
// input executes FROM/WHERE/GROUP/HAVING without final ordering or limits and
// materializes the smallest ordinal relation needed by projections and window
// specifications. Stages sharing PARTITION BY and window ORDER BY execute in
// one windowPlan, which is the physical sort-reuse boundary. The final Query
// projects ordinary and analytic columns, applies DISTINCT/final ORDER BY, and
// enforces OFFSET/LIMIT over the completed window result.
type statementWindow struct {
	inputTree    sqlast.SelectStmt
	inputColumns []sqlast.ResultColumn
	input        *Statement
	inputExec    Exec
	inputSpool   relationSpool
	activeBytes  int64

	stages  []statementWindowStage
	outputs []statementWindowOutput
	inputs  []statementWindowInput

	originalInput []int
	outputStart   []int
	orderInput    []int
	ordinalSpec   []string
	specData      []byte
	viewColumns   [][]scalar
	view          relationSpool

	lastRows uint64
}

type statementWindowInput struct {
	path    *sqlast.PathExpr
	column  int
	ordinal int
}

type statementWindowOutput struct {
	ordinal        int
	name           string
	reduction      Reduction
	valueType      ValueType
	representation OutputRepresentation
}

type statementWindowStage struct {
	spec                 *sqlast.WindowSpec
	exprs                []*sqlast.WindowExpr
	plan                 windowPlan
	executor             windowExecutor
	active               int64
	base                 int
	requiresNumericRange bool
}

func selectHasWindows(tree *sqlast.SelectStmt) bool {
	if tree == nil {
		return false
	}
	for i := range tree.Columns {
		if tree.Columns[i].Window != nil {
			return true
		}
	}
	return false
}

func (s *Statement) window() *statementWindow {
	if s == nil || s.nested == nil {
		return nil
	}
	return s.nested.window
}

func (s *Statement) prepareWindow(
	ctes *statementCTEs,
	argBase int,
	correlation *lateralPrepareFrame,
) error {
	w := new(statementWindow)
	s.ensureNested().window = w
	w.originalInput = make([]int, len(s.tree.Columns))
	w.outputStart = make([]int, len(s.tree.Columns))
	for i := range w.originalInput {
		w.originalInput[i] = -1
		w.outputStart[i] = -1
	}

	for i := range s.tree.Columns {
		column := &s.tree.Columns[i]
		if column.Window != nil {
			continue
		}
		w.originalInput[i] = len(w.inputColumns)
		w.inputColumns = append(w.inputColumns, *column)
	}
	for i := range s.tree.Columns {
		window := s.tree.Columns[i].Window
		if window == nil {
			continue
		}
		w.ensureInput(window.Argument)
		for _, path := range window.Spec.PartitionBy {
			w.ensureInput(path)
		}
		for j := range window.Spec.OrderBy {
			w.ensureInput(window.Spec.OrderBy[j].Path)
		}
	}
	w.orderInput = make([]int, len(s.tree.OrderBy))
	for i := range s.tree.OrderBy {
		term := &s.tree.OrderBy[i]
		w.orderInput[i] = -1
		if term.Scalar != nil {
			w.orderInput[i] = len(w.inputColumns)
			w.inputColumns = append(w.inputColumns, sqlast.ResultColumn{
				Scalar: term.Scalar, Pos: term.Pos,
			})
		} else if term.Output == 0 {
			w.ensureInput(term.Path)
		}
	}
	if len(w.inputColumns) == 0 {
		// The scan still needs one output through which its row cardinality can
		// be materialized. A whole-document projection is valid for every
		// physical, derived, CTE, and joined source shape.
		w.inputColumns = append(w.inputColumns, sqlast.ResultColumn{
			Path: &sqlast.PathExpr{Source: 0, Pos: s.tree.Columns[0].Pos},
			Pos:  s.tree.Columns[0].Pos,
		})
	}

	w.inputTree = *s.tree
	w.inputTree.Columns = w.inputColumns
	w.inputTree.Distinct = false
	w.inputTree.OrderBy = nil
	w.inputTree.Limit = nil
	w.inputTree.Offset = nil
	if s.tree.Having != nil {
		var err error
		w.inputTree.Having, err = cloneWindowHaving(s.tree.Having, w.originalInput)
		if err != nil {
			return err
		}
	}
	input, err := prepareTreeInCorrelationContext(
		s.text, &w.inputTree, s.subqueryLimit, ctes, argBase, correlation,
		s.parameterTypeHints,
		unknownOutputPrepareMode{preserveDocument: s.preserveDocumentUnknown},
	)
	if err != nil {
		return err
	}
	w.input = input

	starts, err := windowInputStarts(input, &w.inputTree)
	if err != nil {
		return err
	}
	for i := range w.inputs {
		w.inputs[i].ordinal = starts[w.inputs[i].column]
	}
	for i, column := range w.orderInput {
		if column >= 0 {
			w.orderInput[i] = starts[column]
		}
	}
	inputColumns := len(input.Columns())
	for i := range s.tree.Columns {
		window := s.tree.Columns[i].Window
		if window == nil {
			continue
		}
		stage := w.stageFor(window)
		stage.exprs = append(stage.exprs, window)
	}

	next := inputColumns
	for i := range w.stages {
		stage := &w.stages[i]
		stage.base = next
		stage.plan.partition = make([]int, len(stage.spec.PartitionBy))
		for j, path := range stage.spec.PartitionBy {
			stage.plan.partition[j] = w.inputOrdinal(path)
		}
		stage.plan.order = make([]windowOrderKey, len(stage.spec.OrderBy))
		for j := range stage.spec.OrderBy {
			term := &stage.spec.OrderBy[j]
			nulls := windowNullsLast
			if term.Nulls == sqlast.WindowNullsFirst ||
				term.Nulls == sqlast.WindowNullsDefault && term.Desc {
				nulls = windowNullsFirst
			}
			stage.plan.order[j] = windowOrderKey{
				column: w.inputOrdinal(term.Path), descending: term.Desc, nulls: nulls,
			}
		}
		stage.plan.functions = make([]windowFunctionSpec, len(stage.exprs))
		for j, expr := range stage.exprs {
			kind, err := lowerWindowFunctionKind(expr.Kind)
			if err != nil {
				return err
			}
			stage.plan.functions[j] = windowFunctionSpec{
				kind: kind, column: w.inputOrdinal(expr.Argument),
			}
			if expr.Argument == nil {
				stage.plan.functions[j].column = -1
			}
			stage.requiresNumericRange = stage.requiresNumericRange ||
				sqlWindowFunctionUsesFrame(expr.Kind) &&
					expr.Spec.Frame.Explicit &&
					expr.Spec.Frame.Unit == sqlast.WindowFrameRange &&
					windowFrameHasSQLRangeOffset(expr.Spec.Frame)
		}
		next += len(stage.exprs)
	}

	w.buildOrdinalSpecs(next)
	visible := 0
	inputNames := input.Columns()
	inputSchema := input.AppendSchema(nil)
	if len(inputSchema) != len(inputNames) {
		return fmt.Errorf(
			"query: window input schema has %d columns for %d names",
			len(inputSchema), len(inputNames),
		)
	}
	for i := range s.tree.Columns {
		w.outputStart[i] = visible
		column := &s.tree.Columns[i]
		if column.Window != nil {
			stage, function := w.windowLocation(column.Window)
			name := column.Alias
			if name == "" {
				name = windowOutputName(column.Window.Kind)
			}
			w.outputs = append(w.outputs, statementWindowOutput{
				ordinal: w.stages[stage].base + function,
				name:    name,
			})
			w.outputs[len(w.outputs)-1].reduction,
				w.outputs[len(w.outputs)-1].valueType = windowOutputSchema(column.Window.Kind)
			if windowFunctionPreservesInputSchema(column.Window.Kind) {
				ordinal := w.inputOrdinal(column.Window.Argument)
				if ordinal < 0 || ordinal >= len(inputSchema) {
					return fmt.Errorf(
						"query: window argument ordinal %d exceeds input schema width %d",
						ordinal, len(inputSchema),
					)
				}
				w.outputs[len(w.outputs)-1].reduction = inputSchema[ordinal].Reduction
				w.outputs[len(w.outputs)-1].valueType = inputSchema[ordinal].Type
				w.outputs[len(w.outputs)-1].representation = inputSchema[ordinal].Representation
			}
			visible++
			continue
		}
		inputColumn := w.originalInput[i]
		start, end := starts[inputColumn], starts[inputColumn+1]
		for ordinal := start; ordinal < end; ordinal++ {
			if ordinal < 0 || ordinal >= len(inputSchema) {
				return fmt.Errorf(
					"query: window output ordinal %d exceeds input schema width %d",
					ordinal, len(inputSchema),
				)
			}
			w.outputs = append(w.outputs, statementWindowOutput{
				ordinal:        ordinal,
				name:           inputNames[ordinal],
				reduction:      inputSchema[ordinal].Reduction,
				valueType:      inputSchema[ordinal].Type,
				representation: inputSchema[ordinal].Representation,
			})
			visible++
		}
	}
	s.names = reserve(s.names[:0], len(w.outputs))
	for i := range w.outputs {
		s.names = append(s.names, w.outputs[i].name)
	}
	s.outputs = len(w.outputs)
	return nil
}

func windowFunctionPreservesInputSchema(kind sqlast.WindowFunctionKind) bool {
	switch kind {
	case sqlast.WindowLag, sqlast.WindowLead, sqlast.WindowFirstValue,
		sqlast.WindowLastValue, sqlast.WindowNthValue:
		return true
	default:
		return false
	}
}

func windowOutputSchema(kind sqlast.WindowFunctionKind) (Reduction, ValueType) {
	switch kind {
	case sqlast.WindowRowNumber, sqlast.WindowRank, sqlast.WindowDenseRank,
		sqlast.WindowNTile, sqlast.WindowCount:
		return ReductionWindowInteger, TypeNumber
	case sqlast.WindowPercentRank, sqlast.WindowCumeDist,
		sqlast.WindowSum, sqlast.WindowAvg, sqlast.WindowMin, sqlast.WindowMax:
		return ReductionWindowNumber, TypeNumber
	default:
		return ReductionNone, TypeAny
	}
}

func lowerWindowFunctionKind(
	kind sqlast.WindowFunctionKind,
) (windowFunctionKind, error) {
	switch kind {
	case sqlast.WindowRowNumber:
		return windowRowNumber, nil
	case sqlast.WindowRank:
		return windowRank, nil
	case sqlast.WindowDenseRank:
		return windowDenseRank, nil
	case sqlast.WindowLag:
		return windowLag, nil
	case sqlast.WindowLead:
		return windowLead, nil
	case sqlast.WindowCount:
		return windowCount, nil
	case sqlast.WindowSum:
		return windowSum, nil
	case sqlast.WindowAvg:
		return windowAvg, nil
	case sqlast.WindowMin:
		return windowMin, nil
	case sqlast.WindowMax:
		return windowMax, nil
	case sqlast.WindowNTile:
		return windowNTile, nil
	case sqlast.WindowPercentRank:
		return windowPercentRank, nil
	case sqlast.WindowCumeDist:
		return windowCumeDist, nil
	case sqlast.WindowFirstValue:
		return windowFirstValue, nil
	case sqlast.WindowLastValue:
		return windowLastValue, nil
	case sqlast.WindowNthValue:
		return windowNthValue, nil
	default:
		return 0, fmt.Errorf("query: unsupported SQL window function kind %d", kind)
	}
}

func sqlWindowFunctionUsesFrame(kind sqlast.WindowFunctionKind) bool {
	switch kind {
	case sqlast.WindowCount, sqlast.WindowSum, sqlast.WindowAvg,
		sqlast.WindowMin, sqlast.WindowMax, sqlast.WindowFirstValue,
		sqlast.WindowLastValue, sqlast.WindowNthValue:
		return true
	default:
		return false
	}
}

func (w *statementWindow) ensureInput(path *sqlast.PathExpr) int {
	if path == nil {
		return -1
	}
	for i := range w.inputs {
		if sameWindowPath(w.inputs[i].path, path) {
			return w.inputs[i].column
		}
	}
	column := -1
	for i := range w.inputColumns {
		candidate := &w.inputColumns[i]
		if candidate.Window == nil && candidate.Agg == sqlast.AggNone &&
			sameWindowPath(candidate.Path, path) {
			column = i
			break
		}
	}
	if column < 0 {
		column = len(w.inputColumns)
		w.inputColumns = append(w.inputColumns, sqlast.ResultColumn{
			Path: path, Pos: path.Pos,
		})
	}
	w.inputs = append(w.inputs, statementWindowInput{path: path, column: column})
	return column
}

func (w *statementWindow) inputOrdinal(path *sqlast.PathExpr) int {
	if path == nil {
		return -1
	}
	for i := range w.inputs {
		if sameWindowPath(w.inputs[i].path, path) {
			return w.inputs[i].ordinal
		}
	}
	return -1
}

func sameWindowPath(left, right *sqlast.PathExpr) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Source != right.Source || left.MergedUsing != right.MergedUsing ||
		len(left.Segments) != len(right.Segments) {
		return false
	}
	for i := range left.Segments {
		if left.Segments[i] != right.Segments[i] {
			return false
		}
	}
	return true
}

func sameWindowSortSpec(left, right *sqlast.WindowSpec) bool {
	if left == nil || right == nil || len(left.PartitionBy) != len(right.PartitionBy) ||
		len(left.OrderBy) != len(right.OrderBy) {
		return false
	}
	for i := range left.PartitionBy {
		if !sameWindowPath(left.PartitionBy[i], right.PartitionBy[i]) {
			return false
		}
	}
	for i := range left.OrderBy {
		a, b := &left.OrderBy[i], &right.OrderBy[i]
		if a.Desc != b.Desc || a.Nulls != b.Nulls || !sameWindowPath(a.Path, b.Path) {
			return false
		}
	}
	return true
}

func (w *statementWindow) stageFor(expr *sqlast.WindowExpr) *statementWindowStage {
	for i := range w.stages {
		if sameWindowSortSpec(w.stages[i].spec, &expr.Spec) {
			return &w.stages[i]
		}
	}
	w.stages = append(w.stages, statementWindowStage{spec: &expr.Spec})
	return &w.stages[len(w.stages)-1]
}

func (w *statementWindow) windowLocation(expr *sqlast.WindowExpr) (int, int) {
	for i := range w.stages {
		for j := range w.stages[i].exprs {
			if w.stages[i].exprs[j] == expr {
				return i, j
			}
		}
	}
	return -1, -1
}

func windowInputStarts(
	input *Statement,
	tree *sqlast.SelectStmt,
) ([]int, error) {
	starts := make([]int, len(tree.Columns)+1)
	outputs := 0
	for i := range tree.Columns {
		starts[i] = outputs
		column := &tree.Columns[i]
		width := 1
		if input.hasRelationBinding() && column.Agg == sqlast.AggNone &&
			column.Path != nil && len(column.Path.Segments) == 0 {
			width = len(input.relationBindingForSource(column.Path.Source).names)
		}
		outputs += width
	}
	starts[len(tree.Columns)] = outputs
	if outputs != len(input.Columns()) {
		return nil, fmt.Errorf(
			"query: window input schema has %d columns, mapped %d",
			len(input.Columns()), outputs,
		)
	}
	return starts, nil
}

func cloneWindowHaving(
	expr *sqlast.Expr,
	columns []int,
) (*sqlast.Expr, error) {
	if expr == nil {
		return nil, nil
	}
	clone := *expr
	if clone.Column >= 0 {
		if clone.Column >= len(columns) || columns[clone.Column] < 0 {
			return nil, fmt.Errorf("query: HAVING cannot read a window output")
		}
		clone.Column = columns[clone.Column]
	}
	if len(expr.Kids) != 0 {
		clone.Kids = make([]*sqlast.Expr, len(expr.Kids))
		for i := range expr.Kids {
			kid, err := cloneWindowHaving(expr.Kids[i], columns)
			if err != nil {
				return nil, err
			}
			clone.Kids[i] = kid
		}
	}
	return &clone, nil
}

func (w *statementWindow) buildOrdinalSpecs(columns int) {
	w.ordinalSpec = make([]string, columns)
	for i := range w.ordinalSpec {
		start := len(w.specData)
		w.specData = append(w.specData, '/')
		w.specData = strconv.AppendInt(w.specData, int64(i), 10)
		w.ordinalSpec[i] = byteview.String(w.specData[start:len(w.specData):len(w.specData)])
	}
}

func windowOutputName(kind sqlast.WindowFunctionKind) string {
	return strings.ToLower(kind.String())
}

func (w *statementWindow) bind(owner *Statement, args []any) error {
	for i := range w.stages {
		stage := &w.stages[i]
		for j, expr := range stage.exprs {
			function := &stage.plan.functions[j]
			function.offset = 0
			function.buckets = 0
			function.nth = 0
			function.frame = windowRowsFrame{}
			function.defaultVal = scalar{}
			function.hasDefault = false
			switch expr.Kind {
			case sqlast.WindowNTile:
				buckets, err := owner.positiveWindowCount(
					expr.Buckets, args, "NTILE bucket count",
				)
				if err != nil {
					return err
				}
				function.buckets = buckets
			case sqlast.WindowNthValue:
				nth, err := owner.positiveWindowCount(
					expr.Nth, args, "NTH_VALUE position",
				)
				if err != nil {
					return err
				}
				function.nth = nth
			case sqlast.WindowLag, sqlast.WindowLead:
				offset := 1
				if expr.HasOffset {
					var err error
					offset, err = owner.windowCount(expr.Offset, args, expr.Kind.String()+" offset")
					if err != nil {
						return err
					}
				}
				function.offset = offset
				if expr.HasDefault {
					function.hasDefault = true
					if expr.DefaultNull {
						function.defaultVal = scalar{kind: kindNull}
					} else {
						value, known, err := owner.operand(expr.Default, args)
						if err != nil {
							return err
						}
						if !known {
							function.defaultVal = scalar{kind: kindNull}
						} else {
							literal, err := owner.c.makeLiteral(value)
							if err != nil {
								return err
							}
							function.defaultVal = classifyLiteral(literal)
						}
					}
				}
			}
			if !sqlWindowFunctionUsesFrame(expr.Kind) {
				continue
			}
			if !expr.Spec.Frame.Explicit {
				function.frame.unit = windowFrameRows
				function.frame.start = windowFrameBound{kind: windowUnboundedPreceding}
				if len(expr.Spec.OrderBy) == 0 {
					function.frame.end = windowFrameBound{kind: windowUnboundedFollowing}
				} else {
					// SQL's ordered default is RANGE UNBOUNDED PRECEDING TO
					// CURRENT ROW. With no numeric RANGE offset, peer GROUPS
					// is exactly equivalent and includes every tie.
					function.frame.unit = windowFrameGroups
					function.frame.end = windowFrameBound{kind: windowCurrentRow}
				}
				continue
			}
			frame, err := owner.lowerWindowFrame(
				expr.Spec.Frame, args, len(expr.Spec.OrderBy),
			)
			if err != nil {
				return err
			}
			function.frame = frame
		}
	}
	return nil
}

func (s *Statement) positiveWindowCount(
	operand sqlast.Operand,
	args []any,
	clause string,
) (int, error) {
	count, err := s.count(operand, args, clause)
	if err != nil {
		return 0, &WindowArgumentError{Clause: clause, Cause: err}
	}
	if count != 0 {
		return count, nil
	}
	if s.prepareMode {
		// Prepare binds neutral zero stand-ins to placeholders. Preserve early
		// shape validation without turning the stand-in into a false value error;
		// every real execution is checked again before the kernel runs.
		return 1, nil
	}
	return 0, &WindowArgumentError{Clause: clause}
}

func (s *Statement) lowerWindowFrame(
	frame sqlast.WindowFrame,
	args []any,
	orderKeys int,
) (windowRowsFrame, error) {
	start, err := s.lowerWindowBound(frame.Unit, frame.Start, args)
	if err != nil {
		return windowRowsFrame{}, err
	}
	end, err := s.lowerWindowBound(frame.Unit, frame.End, args)
	if err != nil {
		return windowRowsFrame{}, err
	}
	physical := windowRowsFrame{start: start, end: end}
	switch frame.Unit {
	case sqlast.WindowFrameRows:
		physical.unit = windowFrameRows
	case sqlast.WindowFrameGroups:
		physical.unit = windowFrameGroups
	case sqlast.WindowFrameRange:
		physical.unit = windowFrameRange
	default:
		return windowRowsFrame{}, fmt.Errorf(
			"query: unsupported SQL window frame unit %d at byte %d",
			frame.Unit, frame.Pos,
		)
	}
	switch frame.Exclusion {
	case sqlast.WindowExcludeNoOthers:
		physical.exclusion = windowExcludeNoOthers
	case sqlast.WindowExcludeCurrentRow:
		physical.exclusion = windowExcludeCurrentRow
	case sqlast.WindowExcludeGroup:
		physical.exclusion = windowExcludeGroup
	case sqlast.WindowExcludeTies:
		physical.exclusion = windowExcludeTies
	default:
		return windowRowsFrame{}, fmt.Errorf(
			"query: unsupported SQL window frame exclusion %d at byte %d",
			frame.Exclusion, frame.ExclusionPos,
		)
	}
	if err := validateWindowFrame(physical, orderKeys); err != nil {
		unit := "ROWS"
		switch frame.Unit {
		case sqlast.WindowFrameGroups:
			unit = "GROUPS"
		case sqlast.WindowFrameRange:
			unit = "RANGE"
		}
		return windowRowsFrame{}, &WindowArgumentError{
			Clause: fmt.Sprintf("%s frame at byte %d", unit, frame.Pos),
			Cause:  err,
		}
	}
	return physical, nil
}

func (s *Statement) lowerWindowBound(
	unit sqlast.WindowFrameUnit,
	bound sqlast.WindowFrameBound,
	args []any,
) (windowFrameBound, error) {
	physical := windowFrameBound{}
	switch bound.Kind {
	case sqlast.WindowUnboundedPreceding:
		physical.kind = windowUnboundedPreceding
	case sqlast.WindowPreceding:
		physical.kind = windowPreceding
	case sqlast.WindowCurrentRow:
		physical.kind = windowCurrentRow
	case sqlast.WindowFollowing:
		physical.kind = windowFollowing
	case sqlast.WindowUnboundedFollowing:
		physical.kind = windowUnboundedFollowing
	default:
		return windowFrameBound{}, fmt.Errorf(
			"query: unsupported SQL window frame bound %d at byte %d",
			bound.Kind, bound.Pos,
		)
	}
	if bound.Kind != sqlast.WindowPreceding && bound.Kind != sqlast.WindowFollowing {
		return physical, nil
	}
	if unit == sqlast.WindowFrameRange {
		offset, err := s.windowRangeOffset(bound.Offset, args)
		if err != nil {
			return windowFrameBound{}, err
		}
		physical.rangeOffset = offset
		return physical, nil
	}
	offset, err := s.windowCount(bound.Offset, args, "window frame offset")
	if err != nil {
		return windowFrameBound{}, err
	}
	physical.offset = offset
	return physical, nil
}

func (s *Statement) windowRangeOffset(
	operand sqlast.Operand,
	args []any,
) (scalar, error) {
	value, known, err := s.operand(operand, args)
	if err != nil {
		return scalar{}, &WindowArgumentError{
			Clause: "RANGE frame offset", Cause: err,
		}
	}
	if !known {
		return scalar{}, &WindowArgumentError{
			Clause: "RANGE frame offset",
			Cause:  fmt.Errorf("query: RANGE frame offset must be an exact non-negative number"),
		}
	}
	literal, err := s.c.makeLiteral(value)
	if err != nil {
		return scalar{}, &WindowArgumentError{
			Clause: "RANGE frame offset", Cause: err,
		}
	}
	offset := classifyLiteral(literal)
	if offset.kind != kindNumber {
		return scalar{}, &WindowArgumentError{
			Clause: "RANGE frame offset",
			Cause:  fmt.Errorf("query: RANGE frame offset must be an exact non-negative number"),
		}
	}
	decimal := parseDecimal(offset.num)
	if decimal.neg && !decimal.zero {
		return scalar{}, &WindowArgumentError{
			Clause: "RANGE frame offset",
			Cause:  fmt.Errorf("query: RANGE frame offset must not be negative"),
		}
	}
	return offset, nil
}

func (s *Statement) windowCount(
	operand sqlast.Operand,
	args []any,
	clause string,
) (int, error) {
	count, err := s.count(operand, args, clause)
	if err != nil {
		return 0, &WindowArgumentError{Clause: clause, Cause: err}
	}
	return count, nil
}

func (w *statementWindow) run(
	owner *Statement,
	parent *Exec,
	src Source,
	args []any,
	frame *statementFrame,
	intermediateResource string,
	correlations []scalar,
	bindPlan bool,
) (Cursor, error) {
	w.releaseExecution(frame)
	if err := cancellationError(parent.Options.Cancel); err != nil {
		return Cursor{}, err
	}
	// Bind before any stage runs. Window plans retain their prepared metadata,
	// so executing first would make a parameterized statement observe the
	// previous execution's offsets, frame bounds, bucket count, NTH position,
	// and default values.
	if bindPlan {
		if err := owner.bind(args); err != nil {
			return Cursor{}, err
		}
	}
	w.inputExec.Options = parent.Options
	var cursor Cursor
	var err error
	if len(correlations) != 0 {
		cursor, err = w.input.runIntoCorrelationFrame(
			&w.inputExec, src, args, frame, "window input result",
			correlations, bindPlan,
		)
	} else {
		cursor, err = w.input.runIntoFrame(
			&w.inputExec, src, args, frame, "window input result",
		)
	}
	if err != nil {
		w.releaseExecution(frame)
		return Cursor{}, err
	}
	resultBytes := w.inputExec.Result.resultBytesUsed
	if err := frame.intermediate.reserve("window input result", resultBytes); err != nil {
		w.releaseExecution(frame)
		return Cursor{}, err
	}
	charge, materializeErr := w.inputSpool.materialize(
		cursor, len(w.input.Columns()), frame, parent.Options.Cancel,
		"window input spool",
	)
	frame.intermediate.release(resultBytes)
	clearExecBorrowedViews(&w.inputExec)
	w.input.releaseRelations(frame)
	if materializeErr != nil {
		w.releaseExecution(frame)
		return Cursor{}, materializeErr
	}
	w.activeBytes = charge

	for i := range w.stages {
		stage := &w.stages[i]
		if err = stage.validateRangeOrder(
			&w.inputSpool, parent.Options.Cancel,
		); err != nil {
			w.releaseExecution(frame)
			return Cursor{}, err
		}
		stage.active, err = stage.executor.execute(
			&w.inputSpool, &stage.plan, frame, parent.Options.Cancel,
		)
		if err != nil {
			w.releaseExecution(frame)
			return Cursor{}, err
		}
	}
	w.bindView()
	if intermediateResource != "" {
		remaining := frame.intermediate.remaining()
		if remaining == 0 {
			w.releaseExecution(frame)
			return Cursor{}, &IntermediateBudgetError{
				Resource: intermediateResource,
				Bytes:    saturatedBytes(frame.intermediate.used, 1),
				Limit:    frame.intermediate.limit,
			}
		}
		parent.Options.ResultBytes = remaining
	}
	inputStats := w.inputExec.Stats
	if err := owner.q.RunInto(parent, fromRelationSpool(&w.view)); err != nil {
		clearExecBorrowedViews(parent)
		w.releaseExecution(frame)
		var resultErr *ResultBudgetError
		if intermediateResource != "" && errors.As(err, &resultErr) &&
			resultErr.ByteLimit == parent.Options.ResultBytes {
			return Cursor{}, &IntermediateBudgetError{
				Resource: intermediateResource,
				Bytes: saturatedBytes(
					frame.intermediate.used, resultErr.Bytes,
				),
				Limit: frame.intermediate.limit,
			}
		}
		return Cursor{}, err
	}
	parent.Stats = inputStats
	w.lastRows = uint64(w.inputSpool.rows)
	return owner.cursor(&parent.Result), nil
}

func windowFrameHasSQLRangeOffset(frame sqlast.WindowFrame) bool {
	return frame.Start.Kind == sqlast.WindowPreceding ||
		frame.Start.Kind == sqlast.WindowFollowing ||
		frame.End.Kind == sqlast.WindowPreceding ||
		frame.End.Kind == sqlast.WindowFollowing
}

func (s *statementWindowStage) validateRangeOrder(
	input *relationSpool,
	cancel *CancelFlag,
) error {
	if s == nil || !s.requiresNumericRange {
		return nil
	}
	if len(s.plan.order) != 1 {
		return &WindowArgumentError{
			Clause: "RANGE frame",
			Cause:  fmt.Errorf("query: a RANGE offset requires exactly one ORDER BY key"),
		}
	}
	column := s.plan.order[0].column
	if input == nil || column < 0 || column >= len(input.columns) {
		return &WindowArgumentError{
			Clause: "RANGE ORDER BY",
			Cause:  fmt.Errorf("query: RANGE ORDER BY has no input column"),
		}
	}
	for row, value := range input.columns[column] {
		if err := cancellationCheckpoint(cancel, row); err != nil {
			return err
		}
		if value.kind != kindNull && value.kind != kindNumber {
			return &WindowArgumentError{
				Clause: "RANGE ORDER BY",
				Cause: fmt.Errorf(
					"query: RANGE ORDER BY row %d is not numeric", row,
				),
			}
		}
	}
	return cancellationError(cancel)
}

func (w *statementWindow) bindView() {
	w.viewColumns = w.viewColumns[:0]
	w.viewColumns = append(w.viewColumns, w.inputSpool.columns...)
	for i := range w.stages {
		w.viewColumns = append(w.viewColumns, w.stages[i].executor.result.columns...)
	}
	w.view.columns = w.viewColumns
	w.view.rows = w.inputSpool.rows
	w.view.data = nil
	w.view.plannedData = 0
}

func (w *statementWindow) releaseExecution(frame *statementFrame) {
	if w == nil {
		return
	}
	clearExecBorrowedViews(&w.inputExec)
	if w.input != nil {
		w.input.releaseRelations(frame)
	}
	w.inputSpool.reset()
	frame.intermediate.release(w.activeBytes)
	w.activeBytes = 0
	for i := range w.stages {
		stage := &w.stages[i]
		stage.executor.result.reset()
		frame.intermediate.release(stage.active)
		stage.active = 0
	}
	w.view.columns = nil
	w.view.rows = 0
	w.viewColumns = w.viewColumns[:0]
}

func (w *statementWindow) discardExecution() {
	if w == nil {
		return
	}
	clearExecBorrowedViews(&w.inputExec)
	if w.input != nil {
		w.input.discardRelations()
	}
	w.inputSpool.reset()
	w.activeBytes = 0
	for i := range w.stages {
		w.stages[i].executor.result.reset()
		w.stages[i].active = 0
	}
	w.view.columns = nil
	w.view.rows = 0
	w.viewColumns = w.viewColumns[:0]
	w.lastRows = 0
}

func (w *statementWindow) release() {
	if w == nil {
		return
	}
	if w.input != nil {
		w.input.Release()
	}
	w.inputExec.Release()
	w.inputSpool.release()
	for i := range w.stages {
		w.stages[i].executor.release()
	}
	*w = statementWindow{}
}
