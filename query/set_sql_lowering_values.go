package query

import (
	"fmt"
	"math"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

type setSQLValueKind uint8

const (
	setSQLLiteralValue setSQLValueKind = iota
	setSQLNullValue
	setSQLParamValue
	setSQLDocumentParamValue
)

type setSQLPreparedValue struct {
	kind    setSQLValueKind
	literal literal
	ordinal int
}

type setSQLParameterCast struct {
	target sqlast.ScalarCastTarget
	pos    int
	active bool
}

type setSQLDocumentParamMetadata struct {
	position  int
	parameter int
}

type setSQLTypedColumn struct {
	valueType      ValueType
	representation OutputRepresentation
	target         sqlast.ScalarCastTarget
	active         bool
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
	// documentParams is nil for every ordinary VALUES statement. INSERT query
	// sources allocate it once at prepare so runtime diagnostics retain both the
	// authored position and the owning statement's parameter identity without
	// widening every prepared scalar value or adding cost to the absent path.
	documentParams []setSQLDocumentParamMetadata
	// parameterCasts is likewise absent for ordinary VALUES. Typed SQL columns
	// allocate one compact entry per placeholder, rather than widening and
	// copying every row-major prepared value on the common execution path.
	parameterCasts []setSQLParameterCast

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
	var typedColumns []setSQLTypedColumn
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
			value := values[column]
			prepared, valueType, isDynamic, err := r.prepareValue(value)
			if err != nil {
				return err
			}
			if value.TypedConstant {
				typedType, representation, ok := setTypedConstantMetadata(value.Cast)
				if !ok {
					return fmt.Errorf(
						"query: VALUES typed constant at byte %d has cast target %d: %w",
						value.Pos, value.Cast, ErrSetTreePlan,
					)
				}
				if typedColumns == nil {
					typedColumns = make([]setSQLTypedColumn, expr.Columns)
				}
				typed := &typedColumns[column]
				if typed.active && typed.target != value.Cast {
					return &ScalarTypeError{
						Pos: value.Pos, Operation: "VALUES common type",
						Left: typed.valueType, Right: typedType,
					}
				}
				*typed = setSQLTypedColumn{
					valueType: typedType, representation: representation,
					target: value.Cast, active: true,
				}
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
		if typedColumns != nil && typedColumns[column].active {
			typed := typedColumns[column]
			if err := r.applyTypedColumn(
				expr, column, typed.target, typed.valueType,
			); err != nil {
				return err
			}
			r.schema[column].Type = typed.valueType
			r.schema[column].Representation = typed.representation
		} else if dynamic[column] {
			r.schema[column].Type = TypeAny
		} else {
			r.schema[column].Type = types[column]
		}
	}
	r.cursor.outputs = expr.Columns
	return nil
}

// applyTypedColumn performs PostgreSQL's common-type coercion for the bounded
// VALUES scalar surface. Bare string literals are unknown until a common type
// is selected, and parameters inherit that selected type. Concrete bool and
// number values never acquire an unrelated type implicitly.
func (r *setSQLValuesRunner) applyTypedColumn(
	expr *sqlast.SetExpr,
	column int,
	target sqlast.ScalarCastTarget,
	targetType ValueType,
) error {
	for row := range expr.Values.Rows {
		value := expr.Values.Rows[row].Values[column]
		prepared := &r.values[row*expr.Columns+column]
		if value.Null || value.TypedConstant {
			continue
		}
		switch value.Operand.Kind {
		case sqlast.OperandParam:
			if prepared.ordinal < 0 || prepared.ordinal >= r.params {
				return fmt.Errorf("query: invalid typed VALUES parameter ordinal %d: %w",
					prepared.ordinal, ErrSetTreePlan)
			}
			if r.parameterCasts == nil {
				r.parameterCasts = make([]setSQLParameterCast, r.params)
			}
			metadata := &r.parameterCasts[prepared.ordinal]
			if metadata.active && metadata.target != target {
				return &ScalarTypeError{
					Pos: value.Pos, Operation: "VALUES parameter common type",
					Left:  setSQLValueTypeForCast(metadata.target),
					Right: targetType,
				}
			}
			*metadata = setSQLParameterCast{
				target: target, pos: value.Pos, active: true,
			}
		case sqlast.OperandString:
			if target == sqlast.ScalarCastText {
				continue
			}
			if target != sqlast.ScalarCastBoolean {
				return fmt.Errorf(
					"query: VALUES typed column has unsupported target %d: %w",
					target, ErrSetTreePlan,
				)
			}
			cast, err := castScalarBoolean(value.Pos, statementScalarValue{
				value: classifyLiteral(prepared.literal),
			})
			if err != nil {
				return err
			}
			prepared.literal = literal{kind: kindBool, bval: cast.value.bval}
		case sqlast.OperandBool:
			if targetType == TypeBool {
				continue
			}
			return &ScalarTypeError{
				Pos: value.Pos, Operation: "VALUES common type",
				Left: targetType, Right: TypeBool,
			}
		case sqlast.OperandNumber:
			return &ScalarTypeError{
				Pos: value.Pos, Operation: "VALUES common type",
				Left: targetType, Right: TypeNumber,
			}
		default:
			return fmt.Errorf(
				"query: VALUES typed column has operand kind %d: %w",
				value.Operand.Kind, ErrSetTreePlan,
			)
		}
	}
	return nil
}

// applySetCommonColumn installs a common type selected by an enclosing set
// node. Keeping this on the VALUES runner preserves each authored parameter
// position and performs known-string boolean typinput during preparation;
// runtime binding then remains the same allocation-free per-value path used by
// a typed constant inside VALUES itself.
func (r *setSQLValuesRunner) applySetCommonColumn(
	expr *sqlast.SetExpr,
	column int,
	target OutputRepresentation,
) error {
	if r == nil || expr == nil || expr.Values == nil ||
		column < 0 || column >= len(r.schema) {
		return fmt.Errorf("query: invalid VALUES common-type column %d: %w",
			column, ErrSetTreePlan)
	}
	var cast sqlast.ScalarCastTarget
	var valueType ValueType
	switch target {
	case OutputSQLBool:
		cast, valueType = sqlast.ScalarCastBoolean, TypeBool
	case OutputSQLText, OutputSQLVarchar, OutputSQLName, OutputSQLBPChar:
		cast, valueType = sqlast.ScalarCastText, TypeString
	default:
		return &ScalarTypeError{
			Pos: expr.Pos, Operation: "VALUES common type",
			Left:  r.schema[column].Type,
			Right: setSQLValueTypeForRepresentation(target),
		}
	}
	if err := r.applyTypedColumn(expr, column, cast, valueType); err != nil {
		return err
	}
	r.schema[column].Type = valueType
	r.schema[column].Representation = target
	return nil
}

func setTypedConstantMetadata(
	target sqlast.ScalarCastTarget,
) (ValueType, OutputRepresentation, bool) {
	switch target {
	case sqlast.ScalarCastText:
		return TypeString, OutputSQLText, true
	case sqlast.ScalarCastBoolean:
		return TypeBool, OutputSQLBool, true
	default:
		return TypeAny, OutputJSON, false
	}
}

func setSQLValueTypeForCast(target sqlast.ScalarCastTarget) ValueType {
	switch target {
	case sqlast.ScalarCastText:
		return TypeString
	case sqlast.ScalarCastBoolean:
		return TypeBool
	case sqlast.ScalarCastNumeric:
		return TypeNumber
	default:
		return TypeAny
	}
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
		cell, resolveErr := r.resolveValue(&r.values[index], args)
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
	value *setSQLPreparedValue,
	args []any,
) (Cell, error) {
	if value == nil {
		return Cell{}, fmt.Errorf("query: nil prepared VALUES scalar")
	}
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
		if value.ordinal >= 0 && value.ordinal < len(r.parameterCasts) &&
			r.parameterCasts[value.ordinal].active {
			metadata := r.parameterCasts[value.ordinal]
			left := statementScalarValue{value: classifyLiteral(literalValue)}
			switch metadata.target {
			case sqlast.ScalarCastText:
				return cellFromScalar(castScalarText(left).value), nil
			case sqlast.ScalarCastBoolean:
				cast, castErr := castScalarBoolean(metadata.pos, left)
				if castErr != nil {
					return Cell{}, castErr
				}
				return cellFromScalar(cast.value), nil
			default:
				return Cell{}, fmt.Errorf(
					"query: invalid VALUES parameter cast target %d", metadata.target,
				)
			}
		}
		return setSQLCellFromLiteral(literalValue), nil
	case setSQLDocumentParamValue:
		return r.resolveDocumentValue(value.ordinal, args)
	default:
		return Cell{}, fmt.Errorf("query: invalid prepared VALUES scalar kind %d", value.kind)
	}
}

func (r *setSQLValuesRunner) resolveDocumentValue(
	ordinal int,
	args []any,
) (Cell, error) {
	position := 0
	parameter := ordinal + 1
	if ordinal >= 0 && ordinal < len(r.documentParams) {
		metadata := r.documentParams[ordinal]
		position = metadata.position - 1
		if metadata.parameter != 0 {
			parameter = metadata.parameter
		}
	}
	if ordinal < 0 || ordinal >= len(args) {
		return Cell{}, &InsertSelectDocumentParameterError{
			Pos: position, Parameter: parameter,
			Cause: fmt.Errorf("placeholder is outside the bound argument range"),
		}
	}
	var raw []byte
	switch value := args[ordinal].(type) {
	case string:
		raw = byteview.Bytes(value)
	case []byte:
		raw = value
	case *string:
		if value != nil {
			raw = byteview.Bytes(*value)
		}
	case *[]byte:
		if value != nil {
			raw = *value
		}
	}
	if raw == nil {
		return Cell{}, &InsertSelectDocumentParameterError{
			Pos: position, Parameter: parameter,
			Cause: fmt.Errorf(
				"must be string, []byte, *string, or *[]byte containing JSON; got %T",
				args[ordinal],
			),
		}
	}
	if err := vibejson.Validate(raw); err != nil {
		return Cell{}, &InsertSelectDocumentParameterError{
			Pos: position, Parameter: parameter,
			Cause: fmt.Errorf("is not one valid JSON value: %w", err),
		}
	}
	// The result may outlive the caller's bind buffer until its cursor closes.
	// Own the exact JSON spelling in the runner's reusable compiler arena before
	// exposing it as a cell. Warm execution reuses that arena and allocates zero.
	owned := r.binder.c.bytes(raw)
	value := classifyRawInto(vibejson.RawValue{Src: owned}, &r.binder.c.tmp)
	if value.kind == kindString {
		// Escaped strings decode through tmp, which the next value may reuse.
		// Interning all document strings makes ownership independent of spelling
		// while value.raw retains the exact JSON representation.
		value.sval = r.binder.c.internString(value.sval)
		value.raw = owned
	}
	return cellFromScalar(value), nil
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
		if r.values[index].kind != setSQLParamValue &&
			r.values[index].kind != setSQLDocumentParamValue {
			continue
		}
		if _, err := r.resolveValue(&r.values[index], args); err != nil {
			return err
		}
	}
	return nil
}

// markDocumentOutput upgrades parameter cells only after this VALUES runtime
// has been selected on the output lineage of a validated one-column INSERT
// query source. The scalar enum carries the mode without increasing prepared-
// value size; the position sidecar exists only on this feature path.
func (r *setSQLValuesRunner) markDocumentOutput(
	expression *sqlast.SetExpr,
	output int,
	rootArgBase int,
	positions []int,
) {
	if r == nil || expression == nil || expression.Values == nil ||
		output < 0 || output >= expression.Columns || len(r.values) == 0 {
		return
	}
	for row := range expression.Values.Rows {
		values := expression.Values.Rows[row].Values
		if output >= len(values) || values[output].Null ||
			values[output].Operand.Kind != sqlast.OperandParam {
			continue
		}
		value := &values[output]
		index := row*expression.Columns + output
		if index >= len(r.values) || r.values[index].kind != setSQLParamValue {
			continue
		}
		ordinal := value.Operand.Ordinal - r.base
		if ordinal < 0 || ordinal >= r.params ||
			r.values[index].ordinal != ordinal {
			continue
		}
		if r.documentParams == nil {
			r.documentParams = make([]setSQLDocumentParamMetadata, r.params)
		}
		r.values[index].kind = setSQLDocumentParamValue
		// Store Pos+1 so a placeholder at byte zero remains distinguishable from
		// an unrecorded entry while the externally reported position stays exact.
		absolute := rootArgBase + value.Operand.Ordinal
		r.documentParams[ordinal] = setSQLDocumentParamMetadata{
			position: value.Operand.Pos + 1, parameter: absolute + 1,
		}
		if absolute >= 0 && absolute < len(positions) {
			positions[absolute] = value.Operand.Pos + 1
		}
	}
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
