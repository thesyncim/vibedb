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
	readReuseMaxCompilerBytes  = 64 << 10
)

// ResetReadBindingsForReuse releases binding-specific query state while
// retaining the immutable parsed preparation and, when it fits the bounded
// read-reuse budget, scrubbed compiler storage for the next bind. It is
// deliberately narrower than Statement.Release: text, tree, output/type
// metadata, and the rendered path cache remain available for the next bind.
// The returned count includes the Statement value, owned metadata capacities,
// and any bounded compiler storage retained by the reset.
//
// AST storage and the source text backing are borrowed from the SQL parser and
// are excluded here; callers holding a parser must add
// Parser.ReadPreparationRetainedBytes. A compiler whose storage exceeds the
// bounded budget is released while the parsed preparation remains reusable. A
// false result leaves the statement unchanged, so callers can fall back to
// ordinary statement teardown without losing a prepared plan.
func (s *Statement) ResetReadBindingsForReuse() (retainedBytes int64, ok bool) {
	if !readReuseStatementShape(s) {
		return 0, false
	}
	retainedBytes, ok = readReuseStatementBytes(s)
	if !ok || !readReuseRebasePathMetadata(s) {
		return 0, false
	}

	compilerBytes, compilerRetained := s.c.resetReadReuse()
	if compilerRetained {
		// Statement already charges its embedded compiler value. The compiler
		// budget includes that object for a complete storage decision, but only
		// its dynamic backing and separately allocated plan/result objects belong
		// in the additional retained-byte count.
		compilerObjectBytes := int64(unsafe.Sizeof(s.c))
		if compilerBytes < compilerObjectBytes {
			compilerRetained = false
			compilerBytes = 0
		} else {
			compilerBytes -= compilerObjectBytes
		}
		if !compilerRetained || compilerBytes < 0 || retainedBytes > maxReadReuseInt64-compilerBytes {
			// The compiler has already been scrubbed, so this cannot be a false
			// preflight result. Release it and retain only the immutable read
			// preparation whose count was established above.
			s.c.release()
		} else {
			retainedBytes += compilerBytes
		}
	}
	s.q = Query{}
	s.args = dropReadReuseSlice(s.args)
	s.stack = scrubReadReuseSlice(s.stack)
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
		s.correlation != nil || s.c.oneShot || s.parameterTypeHints != nil ||
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
		readReuseStatementSliceBytes(s.stack),
	} {
		if !add(n) {
			return 0, false
		}
	}
	return total, true
}

// readReuseCompilerBytes accounts only the compiler storage the direct-read
// reset is willing to retain. The compiler value is embedded in Statement and
// is included here so the bounded compiler decision charges the full object;
// ResetReadBindingsForReuse subtracts that embedded value before adding the
// dynamic storage to the Statement count. The plan/result objects are separate
// allocations and are charged explicitly.
func readReuseCompilerBytes(c *compiler) (int64, bool) {
	if c == nil || c.oneShot {
		return 0, false
	}
	total := int64(unsafe.Sizeof(*c))
	add := func(n int64) bool {
		if n < 0 || total > maxReadReuseInt64-n {
			return false
		}
		total += n
		return true
	}
	for _, n := range []int64{
		readReuseStatementSliceBytes(c.tmp),
		readReuseChunkBytes(&c.text),
		readReuseChunkBytes(&c.tape),
		readReuseChunkBytes(&c.nodes),
		readReuseChunkBytes(&c.kids),
		readReuseChunkBytes(&c.lits),
		readReuseChunkBytes(&c.idxs),
		readReuseChunkBytes(&c.boxes),
		readReuseChunkBytes(&c.nums),
		readReuseChunkBytes(&c.strs),
		readReuseChunkBytes(&c.tree),
		readReuseStatementSliceBytes(c.columns),
		readReuseStatementSliceBytes(c.groupBy),
		readReuseStatementSliceBytes(c.orderBy),
		readReuseStatementSliceBytes(c.headers),
		readReuseStatementSliceBytes(c.planCols),
		readReuseStatementSliceBytes(c.planOrder),
		readReuseStatementSliceBytes(c.groupCols),
		readReuseStatementSliceBytes(c.filterCols),
		readReuseStatementSliceBytes(c.lateCols),
		readReuseStatementSliceBytes(c.lateNums),
		readReuseStatementSliceBytes(c.values.paths),
		readReuseStatementSliceBytes(c.numbers.paths),
		readReuseStatementSliceBytes(c.paths.entries),
	} {
		if !add(n) {
			return 0, false
		}
	}
	for _, entry := range c.paths.entries {
		if entry.err != nil {
			continue // reset drops failed compilations and their error graphs.
		}
		p := entry.path
		// Count aliases separately: conservative accounting also covers dotted
		// paths with separately rendered pointers and decoded escape tokens.
		for _, text := range []string{entry.spec, p.spec, p.name, p.key.Key, p.pointer.String()} {
			if !add(int64(len(text))) {
				return 0, false
			}
		}
		if !add(readReuseStatementSliceBytes(p.pointer.Tokens)) {
			return 0, false
		}
		for _, token := range p.pointer.Tokens {
			if !add(int64(len(token.Text))) {
				return 0, false
			}
		}
	}
	if c.plan != nil && !add(int64(unsafe.Sizeof(*c.plan))) {
		return 0, false
	}
	if c.result != nil && !add(int64(unsafe.Sizeof(*c.result))) {
		return 0, false
	}
	return total, true
}

// readReuseChunkBytes accounts an arena's directory and every chunk's full
// backing capacity. Keeping the unused tail is useful for the next compile,
// but it must be charged even when no element in that tail was used.
func readReuseChunkBytes[T any](arena *chunkArena[T]) int64 {
	if arena == nil {
		return 0
	}
	total := readReuseStatementSliceBytes(arena.chunks)
	for _, chunk := range arena.chunks {
		total = readReuseAdd(total, readReuseStatementSliceBytes(chunk))
	}
	return total
}

func readReuseAdd(a, b int64) int64 {
	if a < 0 || b < 0 || a > maxReadReuseInt64-b {
		return -1
	}
	return a + b
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
