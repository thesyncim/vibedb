package query

import (
	"bytes"
	"unsafe"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson/x/byteview"
)

// The prepared-read reuse lane keeps the parsed statement and immutable path
// metadata, then rebuilds a binding-specific Query on the next execution. The
// limits below are eligibility limits, not SQL grammar limits: a larger or
// richer statement remains on the ordinary prepared path.
const (
	readReuseMaxSQLBytes       = 64 << 10
	readReuseMaxParams         = 16
	readReuseMaxColumns        = 64
	readReuseMaxPathSegments   = 16
	readReuseMaxPredicateNodes = 32
	readReuseMaxOperandBytes   = 4 << 10
)

// ResetReadBindingsForReuse releases all binding-specific compiler and query
// state while retaining the immutable parsed preparation. It is deliberately
// narrower than Statement.Release: text, tree, output/type metadata, and the
// rendered path cache remain available for the next bind, while compiler
// arenas, Query plans, arguments, HAVING programs, and lowering scratch are
// dropped. The returned count includes the Statement value and the capacities
// of owned retained slices (names, parameter metadata, and path metadata).
//
// AST storage and the source text backing are borrowed from the SQL parser and
// are excluded here; callers holding a parser must add
// Parser.ReadPreparationRetainedBytes. Compiler/query backing is excluded
// because it is released before the count is taken. A false result leaves the
// statement unchanged, so callers can fall back to ordinary statement
// teardown without losing a prepared plan.
func (s *Statement) ResetReadBindingsForReuse() (retainedBytes int64, ok bool) {
	if !readReuseStatementShape(s) {
		return 0, false
	}
	retainedBytes, ok = readReuseStatementBytes(s)
	if !ok || !readReuseRebasePathMetadata(s) {
		return 0, false
	}

	s.c.release()
	s.q = Query{}
	clear(s.args)
	s.args = nil
	s.stack = nil
	s.having = havingProgram{}
	s.cached = false
	s.lowerErr = nil
	s.offset = 0
	s.limit = 0
	s.hasLimit = false
	s.driverLimit = false
	s.joinFilter = false
	s.prepareMode = false
	s.subqueryLimit = 0
	s.parameterTypeHints = nil
	s.preserveDocumentUnknown = false

	return retainedBytes, true
}

func readReuseStatementShape(s *Statement) bool {
	if s == nil || s.tree == nil || s.prepareMode || s.nested != nil ||
		s.correlation != nil || s.parameterTypeHints != nil ||
		s.preserveDocumentUnknown || s.paramBase != 0 ||
		len(s.text) > readReuseMaxSQLBytes {
		return false
	}
	if s.params < 0 || s.params != s.tree.Params || s.params > readReuseMaxParams {
		return false
	}
	tree := s.tree
	if tree.With != nil || tree.Set != nil || tree.Distinct ||
		len(tree.From) != 1 || tree.GroupBy != nil || tree.Having != nil ||
		len(tree.Windows) != 0 || tree.Offset != nil || s.requiresCatalog ||
		s.drivingPredicate == nil || s.drivingPredicate != tree.Where {
		return false
	}
	ref := &tree.From[0]
	if ref.Kind != sqlast.RelationCollection || ref.Query != nil ||
		ref.Lateral != nil || ref.Join != sqlast.JoinNone || ref.On != nil ||
		ref.UnresolvedCTE.Kind != sqlast.CTEReferenceNone || ref.Name == "" {
		return false
	}
	if len(tree.Columns) == 0 || len(tree.Columns) > readReuseMaxColumns {
		return false
	}
	for i := range tree.Columns {
		column := &tree.Columns[i]
		if column.Agg != sqlast.AggNone || column.Path == nil ||
			column.Window != nil || column.Scalar != nil ||
			!readReuseScalarPath(column.Path) {
			return false
		}
	}
	if len(tree.OrderBy) > 1 {
		return false
	}
	for i := range tree.OrderBy {
		term := &tree.OrderBy[i]
		if term.Desc || term.Output != 0 || term.Scalar != nil ||
			!readReuseScalarPath(term.Path) {
			return false
		}
	}
	if tree.Limit != nil && !readReuseCountOperand(*tree.Limit, s.params) {
		return false
	}
	_, ok := readReusePredicate(tree.Where, s.params, 0)
	return ok
}

// Appending path/header text can replace specBuf's backing array while earlier
// strings still point into the old one. The current buffer contains every
// rendered spelling, so rebase those strings before reporting its capacity as
// the sole owned text arena. Aliases borrow the separately accounted AST.
func readReuseRebasePathMetadata(s *Statement) bool {
	if s.outputs != len(s.tree.Columns) || len(s.names) != s.outputs {
		return false
	}
	find := func(text string) int { return bytes.Index(s.specBuf, byteview.Bytes(text)) }
	for _, spec := range s.specs {
		if find(spec.text) < 0 {
			return false
		}
	}
	for i, name := range s.names {
		if s.tree.Columns[i].Alias == "" && find(name) < 0 {
			return false
		}
	}
	// Preflight is complete: a declined reset must leave the statement intact.
	rebase := func(text string) string {
		start := find(text)
		return byteview.String(s.specBuf[start : start+len(text) : start+len(text)])
	}
	for i := range s.specs {
		s.specs[i].text = rebase(s.specs[i].text)
	}
	for i, name := range s.names {
		if s.tree.Columns[i].Alias == "" {
			s.names[i] = rebase(name)
		}
	}
	return true
}

func readReuseScalarPath(path *sqlast.PathExpr) bool {
	return path != nil && path.Source == 0 && path.MergedUsing == 0 &&
		len(path.Segments) > 0 && len(path.Segments) <= readReuseMaxPathSegments
}

func readReuseCountOperand(operand sqlast.Operand, params int) bool {
	switch operand.Kind {
	case sqlast.OperandParam:
		return operand.Ordinal >= 0 && operand.Ordinal < params
	case sqlast.OperandString, sqlast.OperandNumber, sqlast.OperandBool:
		return len(operand.Text) <= readReuseMaxOperandBytes
	default:
		return false
	}
}

// readReusePredicate accepts only direct path comparisons and bounded
// conjunctions. The storage-native driver still proves that one of these
// paths is the physical primary key; this package only rules out predicates
// whose lowering can depend on a runtime domain, subquery, or post-scan stage.
func readReusePredicate(expr *sqlast.Expr, params, depth int) (int, bool) {
	if expr == nil || depth > readReuseMaxPredicateNodes {
		return 0, false
	}
	switch expr.Kind {
	case sqlast.ExprCompare:
		if !readReuseScalarPath(expr.Path) || expr.RightPath != nil ||
			expr.ScalarLeft != nil || expr.ScalarRight != nil ||
			expr.Subquery != nil || expr.Agg != sqlast.AggNone ||
			(expr.Op != sqlast.OpEq && expr.Op != sqlast.OpLt &&
				expr.Op != sqlast.OpLe && expr.Op != sqlast.OpGt &&
				expr.Op != sqlast.OpGe) ||
			!readReuseCountOperand(expr.Value, params) {
			return 0, false
		}
		return 1, true
	case sqlast.ExprBetween:
		if !readReuseScalarPath(expr.Path) || expr.Subquery != nil ||
			expr.RightPath != nil || expr.ScalarLeft != nil ||
			expr.ScalarRight != nil || expr.Agg != sqlast.AggNone ||
			len(expr.List) != 2 ||
			!readReuseCountOperand(expr.List[0], params) ||
			!readReuseCountOperand(expr.List[1], params) {
			return 0, false
		}
		return 1, true
	case sqlast.ExprAnd:
		if len(expr.Kids) == 0 {
			return 0, false
		}
		total := 0
		for i := range expr.Kids {
			n, ok := readReusePredicate(expr.Kids[i], params, depth+1)
			if !ok {
				return 0, false
			}
			total += n
			if total > readReuseMaxPredicateNodes {
				return 0, false
			}
		}
		return total, true
	default:
		return 0, false
	}
}

func readReuseStatementBytes(s *Statement) (int64, bool) {
	total := int64(unsafe.Sizeof(*s))
	add := func(n int64) bool {
		if n < 0 || total > maxReadReuseInt64-n {
			return false
		}
		total += n
		return true
	}
	for _, n := range []int64{
		readReuseStatementSliceBytes(s.names),
		readReuseStatementSliceBytes(s.paramTypes),
		readReuseStatementSliceBytes(s.paramTypePositions),
		readReuseStatementSliceBytes(s.paramTypeTargetDefaults),
		readReuseStatementSliceBytes(s.specBuf),
		readReuseStatementSliceBytes(s.specs),
	} {
		if !add(n) {
			return 0, false
		}
	}
	return total, true
}

func readReuseStatementSliceBytes[T any](slice []T) int64 {
	if len(slice) == 0 && cap(slice) == 0 {
		return 0
	}
	count := uint64(cap(slice))
	size := uint64(unsafe.Sizeof(*new(T)))
	if size == 0 || count == 0 {
		return 0
	}
	product := count * size
	if product/size != count || product > maxReadReuseUint64 {
		return -1
	}
	return int64(product)
}

const maxReadReuseUint64 = uint64(^uint64(0) >> 1)
const maxReadReuseInt64 = int64(^uint64(0) >> 1)
