package query

import (
	"fmt"
	"math"

	sqlast "github.com/thesyncim/vibedb/sql"
)

type setSQLValueKind uint8

const (
	setSQLLiteralValue setSQLValueKind = iota
	setSQLNullValue
	setSQLParamValue
)

type setSQLPreparedValue struct {
	kind    setSQLValueKind
	literal literal
	ordinal int
}

// setSQLValuesRunner is a source-independent prepared leaf. It resolves each
// scalar exactly once into retained row-major storage, validates the complete
// shape and result budget, then publishes columnar output atomically.
type setSQLValuesRunner struct {
	base   int
	params int
	rows   int
	names  []string
	schema []OutputColumn
	values []setSQLPreparedValue
	bound  []Cell

	literals compiler
	binder   Statement
	cursor   Statement
}

func (r *setSQLValuesRunner) prepare(expr *sqlast.SetExpr) error {
	if r == nil || expr == nil || expr.Kind != sqlast.SetValuesExpr ||
		expr.Values == nil || expr.First == nil || expr.Columns <= 0 ||
		expr.ParamBase < 0 || expr.Params < 0 {
		return fmt.Errorf("query: invalid VALUES set leaf: %w", ErrSetTreePlan)
	}
	r.base, r.params, r.rows = expr.ParamBase, expr.Params, len(expr.Values.Rows)
	r.names = resize(r.names, expr.Columns)
	r.schema = resize(r.schema, expr.Columns)
	types := make([]ValueType, expr.Columns)
	dynamic := make([]bool, expr.Columns)
	for column := 0; column < expr.Columns; column++ {
		name := ""
		if column < len(expr.First.Columns) {
			name = expr.First.Columns[column].Alias
		}
		if name == "" {
			return fmt.Errorf(
				"query: VALUES output column %d has no stable name: %w",
				column, ErrSetTreePlan,
			)
		}
		r.names[column] = name
		r.schema[column] = OutputColumn{
			Header: name, Ordinal: uint32(column), Type: TypeNull,
		}
		types[column] = TypeNull
	}

	total, ok := resultProduct(r.rows, expr.Columns)
	if !ok {
		return fmt.Errorf("query: VALUES shape overflows int: %w", ErrSetTreePlan)
	}
	r.values = resize(r.values, total)
	at := 0
	for row := range expr.Values.Rows {
		values := expr.Values.Rows[row].Values
		if len(values) != expr.Columns {
			return fmt.Errorf(
				"query: VALUES row %d has %d columns, want %d: %w",
				row, len(values), expr.Columns, ErrSetTreeArity,
			)
		}
		for column := range values {
			prepared, valueType, isDynamic, err := r.prepareValue(values[column])
			if err != nil {
				return err
			}
			r.values[at] = prepared
			at++
			if isDynamic {
				dynamic[column] = true
				continue
			}
			if valueType == TypeNull {
				continue
			}
			if types[column] == TypeNull {
				types[column] = valueType
			} else if types[column] != valueType {
				dynamic[column] = true
			}
		}
	}
	for column := range r.schema {
		if dynamic[column] {
			r.schema[column].Type = TypeAny
		} else {
			r.schema[column].Type = types[column]
		}
	}
	r.cursor.outputs = expr.Columns
	return nil
}

func (r *setSQLValuesRunner) prepareValue(
	value sqlast.SetValue,
) (setSQLPreparedValue, ValueType, bool, error) {
	if value.Null {
		return setSQLPreparedValue{kind: setSQLNullValue}, TypeNull, false, nil
	}
	switch value.Operand.Kind {
	case sqlast.OperandParam:
		ordinal := value.Operand.Ordinal - r.base
		if ordinal < 0 || ordinal >= r.params {
			return setSQLPreparedValue{}, TypeAny, true, fmt.Errorf(
				"query: VALUES placeholder at byte %d is outside [%d,%d): %w",
				value.Pos, r.base, r.base+r.params, ErrSetTreePlan,
			)
		}
		return setSQLPreparedValue{
			kind: setSQLParamValue, ordinal: ordinal,
		}, TypeAny, true, nil
	case sqlast.OperandString:
		return setSQLPreparedValue{
			kind:    setSQLLiteralValue,
			literal: literal{kind: kindString, sval: value.Operand.Text},
		}, TypeString, false, nil
	case sqlast.OperandNumber:
		number, err := r.literals.numberLiteral(Number(value.Operand.Text))
		if err != nil {
			return setSQLPreparedValue{}, TypeAny, false, fmt.Errorf(
				"query: VALUES number at byte %d: %w", value.Pos, err,
			)
		}
		return setSQLPreparedValue{
			kind: setSQLLiteralValue, literal: number,
		}, TypeNumber, false, nil
	case sqlast.OperandBool:
		return setSQLPreparedValue{
			kind:    setSQLLiteralValue,
			literal: literal{kind: kindBool, bval: value.Operand.Bool},
		}, TypeBool, false, nil
	default:
		return setSQLPreparedValue{}, TypeAny, false, fmt.Errorf(
			"query: VALUES scalar at byte %d has operand kind %d: %w",
			value.Pos, value.Operand.Kind, ErrSetTreePlan,
		)
	}
}

func (r *setSQLValuesRunner) Columns() []string {
	if r == nil {
		return nil
	}
	return r.names[:len(r.names):len(r.names)]
}

func (r *setSQLValuesRunner) NumParams() int {
	if r == nil {
		return 0
	}
	return r.params
}

func (*setSQLValuesRunner) Collection() string { return "" }

func (r *setSQLValuesRunner) AppendSchema(dst []OutputColumn) []OutputColumn {
	if r == nil {
		return dst
	}
	return append(dst, r.schema...)
}

func (*setSQLValuesRunner) setStatementSourceIndependent() {}

func (r *setSQLValuesRunner) runIntoFrame(
	exec *Exec,
	_ Source,
	args []any,
	_ *statementFrame,
	_ string,
) (cursor Cursor, err error) {
	if r == nil || exec == nil {
		return Cursor{}, fmt.Errorf("query: VALUES execution requires a prepared runner and Exec")
	}
	clearExecBorrowedViews(exec)
	exec.Stats = ExecStats{}
	if len(args) != r.params {
		return Cursor{}, fmt.Errorf(
			"query: VALUES operand has %d placeholder(s) and %d argument(s) were bound",
			r.params, len(args),
		)
	}
	if err := cancellationError(exec.Options.Cancel); err != nil {
		return Cursor{}, err
	}
	rowLimit, byteLimit, err := normalizeResultBudget(exec.Options)
	if err != nil {
		return Cursor{}, err
	}
	result := &exec.Result
	result.beginResultBudget(rowLimit, byteLimit)
	defer func() {
		if err != nil {
			result.abortResult()
		}
	}()

	r.binder.c.rewind()
	r.bound = resize(r.bound, len(r.values))
	payload := int64(0)
	for index := range r.values {
		if err = cancellationCheckpoint(exec.Options.Cancel, index); err != nil {
			return Cursor{}, err
		}
		cell, resolveErr := r.resolveValue(r.values[index], args)
		if resolveErr != nil {
			return Cursor{}, resolveErr
		}
		r.bound[index] = cell
		payload = saturatedBytes(payload, resultCellPayloadBytes(cell))
	}
	required, err := result.checkResultBudget(len(r.names), r.rows, payload)
	if err != nil {
		return Cursor{}, err
	}
	if payload > int64(math.MaxInt) {
		return Cursor{}, result.resultByteBudgetError(r.rows, math.MaxInt64)
	}
	if err = cancellationError(exec.Options.Cancel); err != nil {
		return Cursor{}, err
	}

	columns := len(r.names)
	if cap(result.Columns) < columns {
		result.Columns = make([]ResultColumn, columns)
	} else {
		for column := columns; column < len(result.Columns); column++ {
			clear(result.Columns[column].Cells)
			result.Columns[column] = ResultColumn{}
		}
		result.Columns = result.Columns[:columns]
	}
	for column := 0; column < columns; column++ {
		cells := result.Columns[column].Cells
		if r.rows < len(cells) {
			clear(cells[r.rows:])
		}
		cells = resize(cells, r.rows)
		result.Columns[column].Header = r.names[column]
		result.Columns[column].Cells = cells
	}
	for row := 0; row < r.rows; row++ {
		if err = cancellationCheckpoint(exec.Options.Cancel, row); err != nil {
			return Cursor{}, err
		}
		for column := 0; column < columns; column++ {
			result.Columns[column].Cells[row] = r.bound[row*columns+column]
		}
	}
	result.RowCount = r.rows
	result.resultBytesUsed = required
	return r.cursor.cursor(result), nil
}

func (r *setSQLValuesRunner) resolveValue(
	value setSQLPreparedValue,
	args []any,
) (Cell, error) {
	switch value.kind {
	case setSQLNullValue:
		return nullCell(), nil
	case setSQLLiteralValue:
		return setSQLCellFromLiteral(value.literal), nil
	case setSQLParamValue:
		if value.ordinal < 0 || value.ordinal >= len(args) {
			return Cell{}, fmt.Errorf("query: invalid VALUES placeholder range")
		}
		argument, known, err := r.binder.argument(args[value.ordinal])
		if err != nil {
			return Cell{}, err
		}
		if !known {
			return nullCell(), nil
		}
		literalValue, err := r.binder.c.makeLiteral(argument)
		if err != nil {
			return Cell{}, err
		}
		return setSQLCellFromLiteral(literalValue), nil
	default:
		return Cell{}, fmt.Errorf("query: invalid prepared VALUES scalar kind %d", value.kind)
	}
}

func setSQLCellFromLiteral(value literal) Cell {
	return cellFromScalar(scalar{
		kind: value.kind, bval: value.bval, num: value.num, raw: value.num,
		isInt: value.isInt, ival: value.ival, sval: value.sval,
	})
}

func (r *setSQLValuesRunner) bindForExplain(args []any) error {
	if r == nil || len(args) != r.params {
		return queryExplainError("query: explain argument count does not match VALUES operand")
	}
	r.binder.c.rewind()
	for index := range r.values {
		if r.values[index].kind != setSQLParamValue {
			continue
		}
		if _, err := r.resolveValue(r.values[index], args); err != nil {
			return err
		}
	}
	return nil
}

func (*setSQLValuesRunner) releaseRelations(*statementFrame) {}

func (r *setSQLValuesRunner) Release() {
	if r == nil {
		return
	}
	r.literals.release()
	r.binder.c.release()
	*r = setSQLValuesRunner{}
}
