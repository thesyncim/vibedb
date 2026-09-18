package gateway

import (
	"math"
	"strconv"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

// preparedDirectInt64Delta recognizes UPDATE shapes for which the replicated
// apply point can evaluate every RHS without a gateway preimage: one or more
// distinct declared top-level INTEGER columns are each added to or subtracted
// from by one exact integer constant. Each self-delta is evaluated against the
// original row, preserving SQL's simultaneous assignment rule. The caller has
// already established the stricter exact-primary-key/no-global-index direct
// candidate. A missing table declaration or any expression outside this closed
// shape falls back to the existing materialized, digest-guarded path.
func preparedDirectInt64Delta(
	snapshot *Snapshot,
	statement *replicatedSQLBoundStatement,
	profile ReplicatedTableProfile,
) ([]byte, bool) {
	if snapshot == nil || statement == nil || statement.prepared == nil ||
		statement.bound == nil || profile.Relation == 0 ||
		statement.profile.Relation != profile.Relation ||
		statement.prepared.statement.Kind != sqlast.KindUpdate ||
		statement.bound.kind != sqlast.KindUpdate ||
		len(statement.prepared.writeGlobalIndexes) != 0 ||
		len(statement.prepared.statement.Update.Assignments) == 0 ||
		len(statement.prepared.statement.Update.Assignments) != len(statement.bound.updateAssignments) {
		return nil, false
	}
	update := statement.prepared.statement.Update
	if !replicatedSQLExactPrimaryFilter(update.Filter, profile.PrimaryKey) ||
		replicatedSQLUpdateAssignsPrimary(update, profile.PrimaryKey) {
		return nil, false
	}
	info, ok := snapshot.declaredTableInfo(profile.Table)
	if !ok {
		return nil, false
	}
	assignments := update.Assignments
	if len(assignments) == 1 {
		assignment := assignments[0]
		if assignment.Expr == nil || assignment.Value.Kind != sqlast.OperandExpression ||
			assignment.Column == "" || !declaredIntegerColumn(info, assignment.Column) {
			return nil, false
		}
		delta, ok := scalarInt64Delta(assignment.Expr, assignment.Column, statement.bound.updateArgs)
		if !ok {
			return nil, false
		}
		descriptor, err := replication.AppendJSONInt64Delta(nil, assignment.Column, delta)
		return descriptor, err == nil
	}
	fields := make([]replication.JSONInt64DeltaField, len(assignments))
	for index, assignment := range assignments {
		if assignment.Expr == nil || assignment.Value.Kind != sqlast.OperandExpression ||
			assignment.Column == "" || !declaredIntegerColumn(info, assignment.Column) {
			return nil, false
		}
		for prior := 0; prior < index; prior++ {
			if fields[prior].Column == assignment.Column {
				return nil, false
			}
		}
		delta, ok := scalarInt64Delta(assignment.Expr, assignment.Column, statement.bound.updateArgs)
		if !ok {
			return nil, false
		}
		fields[index] = replication.JSONInt64DeltaField{Column: assignment.Column, Delta: delta}
	}
	descriptor, err := replication.AppendJSONInt64Deltas(nil, fields)
	return descriptor, err == nil
}

func declaredIntegerColumn(info sqldriver.TableInfo, column string) bool {
	pointer := appendJSONColumnPointer(nil, column)
	for _, declared := range info.Columns {
		if declared.Path == string(pointer) &&
			declared.Types&sqlast.TypeInteger != 0 &&
			declared.Types&sqlast.TypeNumber == 0 {
			return true
		}
	}
	return false
}

func appendJSONColumnPointer(dst []byte, column string) []byte {
	dst = append(dst, '/')
	for i := 0; i < len(column); i++ {
		switch column[i] {
		case '~':
			dst = append(dst, '~', '0')
		case '/':
			dst = append(dst, '~', '1')
		default:
			dst = append(dst, column[i])
		}
	}
	return dst
}

func scalarInt64Delta(expr *sqlast.ScalarExpr, column string, args []any) (int64, bool) {
	if expr == nil || expr.Kind != sqlast.ScalarBinary {
		return 0, false
	}
	leftPath := scalarPathIsColumn(expr.Left, column)
	rightPath := scalarPathIsColumn(expr.Right, column)
	switch expr.Op {
	case sqlast.ScalarAdd:
		if leftPath {
			return scalarExactInt64(expr.Right, args)
		}
		if rightPath {
			return scalarExactInt64(expr.Left, args)
		}
	case sqlast.ScalarSubtract:
		if !leftPath {
			return 0, false
		}
		right, ok := scalarExactInt64(expr.Right, args)
		if !ok || right == math.MinInt64 {
			return 0, false
		}
		return -right, true
	}
	return 0, false
}

func scalarPathIsColumn(expr *sqlast.ScalarExpr, column string) bool {
	return expr != nil && expr.Kind == sqlast.ScalarPath && expr.Path != nil &&
		expr.Path.Source == 0 && expr.Path.MergedUsing == 0 &&
		len(expr.Path.Segments) == 1 && !expr.Path.Segments[0].IsIndex &&
		expr.Path.Segments[0].Key == column
}

func scalarExactInt64(expr *sqlast.ScalarExpr, args []any) (int64, bool) {
	if expr == nil {
		return 0, false
	}
	switch expr.Kind {
	case sqlast.ScalarLiteral:
		return operandExactInt64(expr.Value, args)
	case sqlast.ScalarUnary:
		value, ok := scalarExactInt64(expr.Left, args)
		if !ok {
			return 0, false
		}
		switch expr.Op {
		case sqlast.ScalarPositive:
			return value, true
		case sqlast.ScalarNegative:
			if value == math.MinInt64 {
				return 0, false
			}
			return -value, true
		}
	}
	return 0, false
}

func operandExactInt64(operand sqlast.Operand, args []any) (int64, bool) {
	switch operand.Kind {
	case sqlast.OperandNumber:
		return parseExactInt64(operand.Text)
	case sqlast.OperandParam:
		if operand.Ordinal < 0 || operand.Ordinal >= len(args) {
			return 0, false
		}
		return runtimeExactInt64(args[operand.Ordinal])
	default:
		return 0, false
	}
}

func runtimeExactInt64(value any) (int64, bool) {
	switch value := value.(type) {
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case uint:
		if uint64(value) > math.MaxInt64 {
			return 0, false
		}
		return int64(value), true
	case uint8:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		if value > math.MaxInt64 {
			return 0, false
		}
		return int64(value), true
	case query.Number:
		return parseExactInt64(string(value))
	case vibejson.RawValue:
		return value.Int64()
	case *int64:
		if value == nil {
			return 0, false
		}
		return *value, true
	case *query.Number:
		if value == nil {
			return 0, false
		}
		return parseExactInt64(string(*value))
	case *float64:
		return 0, false
	}
	return 0, false
}

func parseExactInt64(text string) (int64, bool) {
	if text == "" {
		return 0, false
	}
	value, err := strconv.ParseInt(text, 10, 64)
	return value, err == nil
}
