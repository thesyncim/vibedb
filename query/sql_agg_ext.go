package query

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"math"
	"math/big"
	"slices"
	"strconv"
	"strings"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson/x/byteview"
)

type extAggColumnSpec struct {
	header      string
	agg         sqlast.AggKind
	distinct    bool
	separator   string
	inputCol    int // >= 0 for group key or aggregate argument path
	filterCol   int // >= 0 if FILTER (WHERE ...) is attached
	isGroupKey  bool
	groupKeyIdx int
	hidden      bool // true for HAVING-only aggregate/group-key dependencies
}

type extAggOrderSpec struct {
	colIdx     int
	desc       bool
	nullsFirst bool
}

// extKeyTable is a zero-allocation-on-warm-run open-addressed byte-key hash
// table backed by a single contiguous byte arena. Entries reference key slices
// by (offset, length) so arena growth never invalidates stored keys and the
// table retains no Go pointers inside its per-entry arrays.
type extKeyTable struct {
	buckets  []int32
	hashes   []uint64
	keyOff   []int
	keyLen   []int
	keyArena []byte
	seed     maphash.Seed
	seeded   bool
}

func (t *extKeyTable) reset() {
	if len(t.hashes) == 0 {
		return
	}
	for i := range t.buckets {
		t.buckets[i] = -1
	}
	t.hashes = t.hashes[:0]
	t.keyOff = t.keyOff[:0]
	t.keyLen = t.keyLen[:0]
	t.keyArena = t.keyArena[:0]
}

func (t *extKeyTable) release() {
	t.buckets = nil
	t.hashes = nil
	t.keyOff = nil
	t.keyLen = nil
	t.keyArena = nil
}

func (t *extKeyTable) grow(newCap int) {
	if cap(t.buckets) >= newCap {
		t.buckets = t.buckets[:newCap]
	} else {
		t.buckets = make([]int32, newCap)
	}
	for i := range t.buckets {
		t.buckets[i] = -1
	}
	mask := uint64(newCap - 1)
	for i, h := range t.hashes {
		slot := h & mask
		for t.buckets[slot] >= 0 {
			slot = (slot + 1) & mask
		}
		t.buckets[slot] = int32(i)
	}
}

func (t *extKeyTable) findOrInsert(key []byte) (int32, bool) {
	if !t.seeded {
		t.seed = maphash.MakeSeed()
		t.seeded = true
	}
	if len(t.buckets) == 0 {
		t.grow(16)
	}
	h := maphash.Bytes(t.seed, key)
	mask := uint64(len(t.buckets) - 1)
	slot := h & mask
	for {
		idx := t.buckets[slot]
		if idx < 0 {
			break
		}
		if t.hashes[idx] == h {
			off := t.keyOff[idx]
			ln := t.keyLen[idx]
			if ln == len(key) && bytes.Equal(t.keyArena[off:off+ln], key) {
				return idx, false
			}
		}
		slot = (slot + 1) & mask
	}
	if (len(t.hashes)+1)*2 > len(t.buckets) {
		t.grow(len(t.buckets) * 2)
		mask = uint64(len(t.buckets) - 1)
		slot = h & mask
		for t.buckets[slot] >= 0 {
			slot = (slot + 1) & mask
		}
	}
	idx := int32(len(t.hashes))
	off := len(t.keyArena)
	t.keyArena = append(t.keyArena, key...)
	t.hashes = append(t.hashes, h)
	t.keyOff = append(t.keyOff, off)
	t.keyLen = append(t.keyLen, len(key))
	t.buckets[slot] = idx
	return idx, true
}

func appendExtKeyCell(dst []byte, c Cell) []byte {
	if c.IsNull() {
		return append(dst, 0)
	}
	dst = append(dst, 1)
	lenAt := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	dst = c.AppendJSON(dst)
	binary.LittleEndian.PutUint32(dst[lenAt:lenAt+4], uint32(len(dst)-lenAt-4))
	return dst
}

func appendExtSingleKeyCell(dst []byte, c Cell) []byte {
	if c.IsNull() {
		return append(dst, 0)
	}
	dst = append(dst, 1)
	return c.AppendJSON(dst)
}

type extAggStrNode struct {
	off    uint32
	length uint32
	next   uint32 // 1-based index into strNodes; 0 = end
}

type extAggElemNode struct {
	cell Cell
	next uint32 // 1-based index into elemNodes; 0 = end
}

// extAggState holds one aggregate accumulator for one group. The zero value is
// a valid empty accumulator so clear(ext.states) resets all groups in bulk.
type extAggState struct {
	count    int64
	intSum   int64
	ratSum   *big.Rat
	minCell  Cell
	maxCell  Cell
	strHead  uint32 // 1-based index into strNodes; 0 = empty
	strTail  uint32
	elemHead uint32 // 1-based index into elemNodes; 0 = empty
	elemTail uint32
	hasNum   bool
	useRat   bool
	hasMin   bool
	hasMax   bool
	boolVal  bool
	hasBool  bool
}

type statementExtAgg struct {
	input        *Statement
	grouped      bool
	groupCols    []int
	specs        []extAggColumnSpec
	visibleCount int
	names        []string
	having       *sqlast.Expr
	havingColMap map[*sqlast.Expr]int
	orderBy      []extAggOrderSpec
	limit        *sqlast.Operand
	offset       *sqlast.Operand

	// Reusable per-statement execution workspaces, cleared between runs and
	// released by Statement.Release.
	inExec        Exec
	groupTable    extKeyTable
	distinctTable extKeyTable
	keyScratch    []byte
	valArena      []byte
	groupKeys     []Cell
	states        []extAggState
	strArena      []byte
	strNodes      []extAggStrNode
	elemNodes     []extAggElemNode
	finalCells    []Cell
	surviving     []int32
	ratScratch    big.Rat
	ratDivScratch big.Rat
	result        Result
}

func (ext *statementExtAgg) release() {
	if ext == nil {
		return
	}
	if ext.input != nil {
		ext.input.Release()
		ext.input = nil
	}
	ext.inExec.Release()
	ext.groupTable.release()
	ext.distinctTable.release()
	ext.keyScratch = nil
	ext.valArena = nil
	clear(ext.groupKeys)
	ext.groupKeys = nil
	clear(ext.states)
	ext.states = nil
	ext.strArena = nil
	ext.strNodes = nil
	clear(ext.elemNodes)
	ext.elemNodes = nil
	clear(ext.finalCells)
	ext.finalCells = nil
	ext.surviving = nil
	ext.result.Release()
}

func exprHasExtendedAgg(e *sqlast.Expr) bool {
	if e == nil {
		return false
	}
	if e.AggDistinct || e.Agg > sqlast.AggMax {
		return true
	}
	for _, kid := range e.Kids {
		if exprHasExtendedAgg(kid) {
			return true
		}
	}
	return false
}

func hasExtendedAggregate(tree *sqlast.SelectStmt) bool {
	if tree == nil {
		return false
	}
	if len(tree.GroupByScalarExprs()) > 0 {
		return true
	}
	if exprHasExtendedAgg(tree.Having) {
		return true
	}
	for i := range tree.Columns {
		c := &tree.Columns[i]
		if c.AggDistinct || c.AggFilter != nil || c.Agg > sqlast.AggMax {
			return true
		}
	}
	return false
}

func prepareExtendedAggregateStatement(
	src string,
	tree *sqlast.SelectStmt,
	subqueryLimit uint8,
	ctes *statementCTEs,
	argBase int,
	correlation *lateralPrepareFrame,
	parameterTypes []ParameterType,
) (*Statement, error) {
	groupByScalars := tree.GroupByScalarExprs()
	inputTree := *tree
	inputTree.GroupBy = nil
	inputTree.Extensions = nil
	inputTree.Having = nil
	inputTree.OrderBy = nil
	inputTree.Limit = nil
	inputTree.Offset = nil
	inputTree.Distinct = false
	inputTree.Columns = nil

	ext := &statementExtAgg{
		grouped: len(tree.GroupBy) > 0 || len(groupByScalars) > 0,
		having:  tree.Having,
		limit:   tree.Limit,
		offset:  tree.Offset,
	}

	// 1. Add all GROUP BY paths and scalar expressions to inputTree.
	for _, gpath := range tree.GroupBy {
		idx := len(inputTree.Columns)
		inputTree.Columns = append(inputTree.Columns, sqlast.ResultColumn{
			Path: gpath,
			Pos:  gpath.Pos,
		})
		ext.groupCols = append(ext.groupCols, idx)
	}
	scalarGroupBase := len(tree.GroupBy)
	for _, gscalar := range groupByScalars {
		idx := len(inputTree.Columns)
		inputTree.Columns = append(inputTree.Columns, sqlast.ResultColumn{
			Scalar: gscalar,
			Pos:    gscalar.Pos,
		})
		ext.groupCols = append(ext.groupCols, idx)
	}

	// 2. Build output specs & add aggregate input paths + FILTER boolean columns.
	for i := range tree.Columns {
		col := &tree.Columns[i]
		header := col.Alias
		if header == "" {
			if col.Agg == sqlast.AggNone && col.Path != nil {
				header = string(col.Path.AppendSpec(nil))
			} else if col.Agg == sqlast.AggNone && col.Scalar != nil {
				header = fmt.Sprintf("col_%d", i+1)
			} else if col.Agg == sqlast.AggCount && col.Path == nil {
				header = "count(*)"
			} else if col.Path != nil {
				header = strings.ToLower(col.Agg.String()) + "(" + string(col.Path.AppendSpec(nil)) + ")"
			} else {
				header = strings.ToLower(col.Agg.String())
			}
		}
		ext.names = append(ext.names, header)
		spec := extAggColumnSpec{
			header:    header,
			agg:       col.Agg,
			distinct:  col.AggDistinct,
			separator: col.AggSeparator,
			inputCol:  -1,
			filterCol: -1,
		}
		if col.Agg == sqlast.AggNone {
			spec.isGroupKey = true
			spec.groupKeyIdx = -1
			if col.Scalar != nil {
				for gi, gscalar := range groupByScalars {
					if sqlast.SameScalarExpr(col.Scalar, gscalar) {
						spec.groupKeyIdx = scalarGroupBase + gi
						spec.inputCol = ext.groupCols[scalarGroupBase+gi]
						break
					}
				}
				if spec.inputCol < 0 {
					spec.inputCol = len(inputTree.Columns)
					inputTree.Columns = append(inputTree.Columns, sqlast.ResultColumn{Scalar: col.Scalar, Pos: col.Pos})
				}
			} else if col.Path != nil {
				for gi, gpath := range tree.GroupBy {
					if gpath != nil && col.Path.Spec() == gpath.Spec() {
						spec.groupKeyIdx = gi
						spec.inputCol = ext.groupCols[gi]
						break
					}
				}
				if spec.inputCol < 0 {
					spec.inputCol = len(inputTree.Columns)
					inputTree.Columns = append(inputTree.Columns, sqlast.ResultColumn{Path: col.Path, Pos: col.Pos})
				}
			}
		} else {
			if col.Path != nil {
				spec.inputCol = len(inputTree.Columns)
				inputTree.Columns = append(inputTree.Columns, sqlast.ResultColumn{Path: col.Path, Pos: col.Pos})
			}
			if col.AggFilter != nil {
				spec.filterCol = len(inputTree.Columns)
				trueLit := &sqlast.ScalarExpr{Kind: sqlast.ScalarLiteral, Value: sqlast.Operand{Kind: sqlast.OperandBool, Bool: true}, Pos: col.Pos}
				falseLit := &sqlast.ScalarExpr{Kind: sqlast.ScalarLiteral, Value: sqlast.Operand{Kind: sqlast.OperandBool, Bool: false}, Pos: col.Pos}
				caseExpr := &sqlast.ScalarExpr{
					Kind: sqlast.ScalarCase,
					Whens: []sqlast.ScalarWhen{{
						Predicate: col.AggFilter,
						Result:    trueLit,
						Pos:       col.Pos,
					}},
					Else: falseLit,
					Pos:  col.Pos,
				}
				inputTree.Columns = append(inputTree.Columns, sqlast.ResultColumn{
					Scalar: caseExpr,
					Pos:    col.Pos,
				})
			}
		}
		ext.specs = append(ext.specs, spec)
	}
	ext.visibleCount = len(ext.specs)

	// 2b. Collect any hidden aggregate or group-key dependencies from HAVING.
	if ext.having != nil {
		ext.havingColMap = make(map[*sqlast.Expr]int)
		var collectHaving func(e *sqlast.Expr)
		collectHaving = func(e *sqlast.Expr) {
			if e == nil {
				return
			}
			switch e.Kind {
			case sqlast.ExprAnd, sqlast.ExprOr, sqlast.ExprNot:
				for _, kid := range e.Kids {
					collectHaving(kid)
				}
				return
			}
			if e.Column >= 0 && e.Column < len(ext.specs) &&
				(!e.AggDistinct || ext.specs[e.Column].distinct) {
				ext.havingColMap[e] = e.Column
				return
			}
			if e.Agg != sqlast.AggNone {
				// Add hidden aggregate spec.
				inputCol := -1
				if e.Path != nil {
					inputCol = len(inputTree.Columns)
					inputTree.Columns = append(inputTree.Columns, sqlast.ResultColumn{
						Path: e.Path,
						Pos:  e.Pos,
					})
				}
				idx := len(ext.specs)
				ext.specs = append(ext.specs, extAggColumnSpec{
					agg:       e.Agg,
					distinct:  e.AggDistinct,
					inputCol:  inputCol,
					filterCol: -1,
					hidden:    true,
				})
				ext.havingColMap[e] = idx
				return
			}
			if e.Path != nil {
				// Match group-by key path.
				spec := e.Path.Spec()
				for gIdx, gpath := range tree.GroupBy {
					if gpath.Spec() == spec {
						idx := len(ext.specs)
						ext.specs = append(ext.specs, extAggColumnSpec{
							isGroupKey:  true,
							groupKeyIdx: gIdx,
							inputCol:    -1,
							filterCol:   -1,
							hidden:      true,
						})
						ext.havingColMap[e] = idx
						return
					}
				}
			}
		}
		collectHaving(ext.having)
	}

	// 3. Resolve ORDER BY terms against output columns.
	for _, term := range tree.OrderBy {
		colIdx := -1
		if term.Output > 0 && term.Output <= len(ext.specs) {
			colIdx = term.Output - 1
		} else if term.Path != nil {
			spec := term.Path.Spec()
			for i, col := range tree.Columns {
				if (col.Alias != "" && col.Alias == spec) ||
					(col.Agg == sqlast.AggNone && col.Path != nil && col.Path.Spec() == spec) {
					colIdx = i
					break
				}
			}
		} else if term.Scalar != nil {
			for i, col := range tree.Columns {
				if col.Scalar != nil && sqlast.SameScalarExpr(col.Scalar, term.Scalar) {
					colIdx = i
					break
				}
			}
		}
		if colIdx >= 0 {
			nullsFirst := term.Desc
			if term.Nulls == sqlast.WindowNullsFirst {
				nullsFirst = true
			} else if term.Nulls == sqlast.WindowNullsLast {
				nullsFirst = false
			}
			ext.orderBy = append(ext.orderBy, extAggOrderSpec{
				colIdx:     colIdx,
				desc:       term.Desc,
				nullsFirst: nullsFirst,
			})
		}
	}

	inputStmt, err := prepareTreeInCorrelationContext(
		src, &inputTree, subqueryLimit, ctes, argBase, correlation, parameterTypes,
	)
	if err != nil {
		return nil, err
	}
	ext.input = inputStmt

	s := &Statement{
		text:    src,
		tree:    tree,
		params:  tree.Params,
		names:   ext.names,
		outputs: len(ext.names),
		extAgg:  ext,
	}
	return s, nil
}

func (ext *statementExtAgg) resetExecution() {
	ext.groupTable.reset()
	ext.distinctTable.reset()
	clear(ext.groupKeys)
	ext.groupKeys = ext.groupKeys[:0]
	clear(ext.states)
	ext.states = ext.states[:0]
	ext.strArena = ext.strArena[:0]
	ext.strNodes = ext.strNodes[:0]
	clear(ext.elemNodes)
	ext.elemNodes = ext.elemNodes[:0]
	clear(ext.finalCells)
	ext.finalCells = ext.finalCells[:0]
	ext.surviving = ext.surviving[:0]
	for i := range ext.result.Columns {
		clear(ext.result.Columns[i].Cells)
		ext.result.Columns[i].Cells = ext.result.Columns[i].Cells[:0]
	}
	ext.result.RowCount = 0
	ext.valArena = ext.valArena[:0]
}

func (ext *statementExtAgg) appendZeroStates(n int) {
	need := len(ext.states) + n
	if cap(ext.states) >= need {
		prev := len(ext.states)
		ext.states = ext.states[:need]
		clear(ext.states[prev:need])
		return
	}
	// Grow geometrically so repeated group insertions stay amortized O(1).
	newCap := cap(ext.states) * 2
	if newCap < need {
		newCap = need
	}
	grown := make([]extAggState, need, newCap)
	copy(grown, ext.states)
	ext.states = grown
}

func (s *Statement) runExtAggInto(e *Exec, src Source, args []any) (Cursor, error) {
	ext := s.extAgg
	ext.resetExecution()
	cancel := e.Options.Cancel
	if err := cancellationError(cancel); err != nil {
		return Cursor{}, err
	}

	ext.inExec.Options = e.Options
	cursor, err := ext.input.RunInto(&ext.inExec, src, args)
	e.Stats = ext.inExec.Stats
	if err != nil {
		return Cursor{}, err
	}

	numSpecs := len(ext.specs)
	numGroupCols := len(ext.groupCols)
	singleGroupCol := numGroupCols == 1
	numGroups := 0

	for {
		ok, err := cursor.NextWithCancel(cancel)
		if err != nil {
			return Cursor{}, err
		}
		if !ok {
			break
		}
		var gIdx int
		if ext.grouped {
			kb := ext.keyScratch[:0]
			if singleGroupCol {
				kb = appendExtSingleKeyCell(kb, cursor.Cell(ext.groupCols[0]))
			} else {
				for _, colIdx := range ext.groupCols {
					kb = appendExtKeyCell(kb, cursor.Cell(colIdx))
				}
			}
			ext.keyScratch = kb
			idx, inserted := ext.groupTable.findOrInsert(kb)
			gIdx = int(idx)
			if inserted {
				for _, colIdx := range ext.groupCols {
					var detached Cell
					detached, ext.valArena = ownResultCellInto(ext.valArena, cursor.Cell(colIdx))
					ext.groupKeys = append(ext.groupKeys, detached)
				}
				ext.appendZeroStates(numSpecs)
				numGroups++
			}
		} else {
			gIdx = 0
			if numGroups == 0 {
				ext.appendZeroStates(numSpecs)
				numGroups = 1
			}
		}

		stateBase := gIdx * numSpecs
		for i := range numSpecs {
			sp := &ext.specs[i]
			if sp.isGroupKey {
				continue
			}
			if sp.filterCol >= 0 {
				b, ok := cursor.Cell(sp.filterCol).Bool()
				if !ok || !b {
					continue
				}
			}
			st := &ext.states[stateBase+i]
			if sp.agg == sqlast.AggCount && sp.inputCol < 0 {
				st.count++
				continue
			}
			cell := cursor.Cell(sp.inputCol)
			if cell.IsNull() {
				continue
			}
			if sp.distinct {
				dk := ext.keyScratch[:0]
				dk = binary.LittleEndian.AppendUint32(dk, uint32(gIdx))
				dk = binary.LittleEndian.AppendUint16(dk, uint16(i))
				dk = cell.AppendJSON(dk)
				ext.keyScratch = dk
				if _, inserted := ext.distinctTable.findOrInsert(dk); !inserted {
					continue
				}
			}
			switch sp.agg {
			case sqlast.AggCount:
				st.count++
			case sqlast.AggSum, sqlast.AggAvg:
				ext.accumulateNumber(st, cell)
			case sqlast.AggMin:
				if !st.hasMin || compareExtAggCells(cell, st.minCell) < 0 {
					st.minCell, ext.valArena = ownResultCellInto(ext.valArena, cell)
					st.hasMin = true
				}
			case sqlast.AggMax:
				if !st.hasMax || compareExtAggCells(cell, st.maxCell) > 0 {
					st.maxCell, ext.valArena = ownResultCellInto(ext.valArena, cell)
					st.hasMax = true
				}
			case sqlast.AggStringAgg:
				off := uint32(len(ext.strArena))
				if tb, ok := cell.TextBytes(); ok {
					ext.strArena = append(ext.strArena, tb...)
				} else {
					ext.strArena = cell.AppendJSON(ext.strArena)
				}
				ln := uint32(len(ext.strArena)) - off
				nodeID := uint32(len(ext.strNodes)) + 1
				ext.strNodes = append(ext.strNodes, extAggStrNode{off: off, length: ln})
				if st.strHead == 0 {
					st.strHead = nodeID
				} else {
					ext.strNodes[st.strTail-1].next = nodeID
				}
				st.strTail = nodeID
			case sqlast.AggArrayAgg, sqlast.AggJsonAgg:
				var detached Cell
				detached, ext.valArena = ownResultCellInto(ext.valArena, cell)
				nodeID := uint32(len(ext.elemNodes)) + 1
				ext.elemNodes = append(ext.elemNodes, extAggElemNode{cell: detached})
				if st.elemHead == 0 {
					st.elemHead = nodeID
				} else {
					ext.elemNodes[st.elemTail-1].next = nodeID
				}
				st.elemTail = nodeID
			case sqlast.AggBoolAnd:
				if b, ok := cell.Bool(); ok {
					if !st.hasBool {
						st.boolVal = b
						st.hasBool = true
					} else {
						st.boolVal = st.boolVal && b
					}
				}
			case sqlast.AggBoolOr:
				if b, ok := cell.Bool(); ok {
					if !st.hasBool {
						st.boolVal = b
						st.hasBool = true
					} else {
						st.boolVal = st.boolVal || b
					}
				}
			}
		}
	}

	if !ext.grouped && numGroups == 0 {
		ext.appendZeroStates(numSpecs)
		numGroups = 1
	}

	// Finalize all groups into a flat matrix so HAVING and ORDER BY operate
	// without per-row slice allocations.
	totalFinal := numGroups * numSpecs
	if cap(ext.finalCells) < totalFinal {
		ext.finalCells = make([]Cell, totalFinal)
	} else {
		ext.finalCells = ext.finalCells[:totalFinal]
	}

	for g := range numGroups {
		if err := cancellationCheckpoint(cancel, g); err != nil {
			return Cursor{}, err
		}
		base := g * numSpecs
		row := ext.finalCells[base : base+numSpecs]
		keyBase := g * numGroupCols
		for i := range numSpecs {
			sp := &ext.specs[i]
			if sp.isGroupKey {
				if sp.groupKeyIdx >= 0 && sp.groupKeyIdx < numGroupCols {
					row[i] = ext.groupKeys[keyBase+sp.groupKeyIdx]
				} else {
					row[i] = NullCell()
				}
				continue
			}
			st := &ext.states[base+i]
			row[i] = ext.finalizeState(sp, st)
		}
		if ext.having != nil && !ext.evalHaving(ext.having, row, args) {
			continue
		}
		ext.surviving = append(ext.surviving, int32(g))
	}

	surviving := ext.surviving
	if len(ext.orderBy) > 0 && len(surviving) > 1 {
		slices.SortStableFunc(surviving, func(a, b int32) int {
			rowA := ext.finalCells[int(a)*numSpecs:]
			rowB := ext.finalCells[int(b)*numSpecs:]
			for _, ord := range ext.orderBy {
				ca, cb := rowA[ord.colIdx], rowB[ord.colIdx]
				aNull, bNull := ca.IsNull(), cb.IsNull()
				if aNull && bNull {
					continue
				}
				if aNull {
					if ord.nullsFirst {
						return -1
					}
					return 1
				}
				if bNull {
					if ord.nullsFirst {
						return 1
					}
					return -1
				}
				c := compareExtAggCells(ca, cb)
				if c != 0 {
					if ord.desc {
						return -c
					}
					return c
				}
			}
			return 0
		})
	}

	if ext.offset != nil {
		off, err := resolveExtAggIntOperand(ext.offset, args)
		if err != nil {
			return Cursor{}, err
		}
		if off > 0 {
			if off >= len(surviving) {
				surviving = nil
			} else {
				surviving = surviving[off:]
			}
		}
	}
	if ext.limit != nil {
		lim, err := resolveExtAggIntOperand(ext.limit, args)
		if err != nil {
			return Cursor{}, err
		}
		if lim >= 0 && lim < len(surviving) {
			surviving = surviving[:lim]
		}
	}

	out := &ext.result
	out.RowCount = len(surviving)
	vis := ext.visibleCount
	if vis == 0 {
		vis = numSpecs
	}
	if len(out.Columns) != vis {
		out.Columns = make([]ResultColumn, vis)
	}
	for i := range vis {
		out.Columns[i].Header = ext.specs[i].header
		cells := out.Columns[i].Cells
		if cap(cells) < len(surviving) {
			cells = make([]Cell, len(surviving))
		} else {
			cells = cells[:len(surviving)]
		}
		for r, g := range surviving {
			cells[r] = ext.finalCells[int(g)*numSpecs+i]
		}
		out.Columns[i].Cells = cells
	}
	return NewResultCursor(out)
}

func (ext *statementExtAgg) accumulateNumber(st *extAggState, cell Cell) {
	if cell.Type() != TypeNumber {
		return
	}
	if !st.useRat {
		if iv, ok := cell.Int64(); ok {
			sum := st.intSum + iv
			if (iv > 0 && sum < st.intSum) || (iv < 0 && sum > st.intSum) {
				// Signed int64 overflow: promote to exact big.Rat.
				st.ratSum = new(big.Rat).SetInt64(st.intSum)
				st.useRat = true
				ext.ratScratch.SetInt64(iv)
				st.ratSum.Add(st.ratSum, &ext.ratScratch)
			} else {
				st.intSum = sum
			}
			st.count++
			st.hasNum = true
			return
		}
	}
	// Exact rational fallback for fractional or out-of-range numbers.
	if iv, ok := cell.Int64(); ok {
		ext.ratScratch.SetInt64(iv)
	} else {
		var scratch [64]byte
		raw := cell.Payload()
		if raw == nil {
			raw = cell.AppendJSON(scratch[:0])
		}
		if _, ok := ext.ratScratch.SetString(byteview.String(raw)); !ok {
			return
		}
	}
	if !st.useRat {
		st.ratSum = new(big.Rat)
		if st.hasNum {
			st.ratSum.SetInt64(st.intSum)
		}
		st.useRat = true
	}
	st.ratSum.Add(st.ratSum, &ext.ratScratch)
	st.count++
	st.hasNum = true
}

func (ext *statementExtAgg) finalizeState(sp *extAggColumnSpec, st *extAggState) Cell {
	switch sp.agg {
	case sqlast.AggCount:
		return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(st.count)}
	case sqlast.AggSum:
		if !st.hasNum {
			return NullCell()
		}
		if !st.useRat {
			return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(st.intSum)}
		}
		return ratToCellInto(st.ratSum, &ext.valArena)
	case sqlast.AggAvg:
		if !st.hasNum || st.count == 0 {
			return NullCell()
		}
		if !st.useRat {
			if st.intSum%st.count == 0 {
				return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(st.intSum / st.count)}
			}
			const exactFloatIntLimit = int64(1) << 53
			if st.intSum >= -exactFloatIntLimit && st.intSum <= exactFloatIntLimit && st.count <= exactFloatIntLimit {
				f := float64(st.intSum) / float64(st.count)
				return Cell{kind: TypeNumber, word: math.Float64bits(f)}
			}
			ext.ratScratch.SetFrac64(st.intSum, st.count)
			return ratToCellInto(&ext.ratScratch, &ext.valArena)
		}
		ext.ratDivScratch.SetInt64(st.count)
		ext.ratScratch.Quo(st.ratSum, &ext.ratDivScratch)
		return ratToCellInto(&ext.ratScratch, &ext.valArena)
	case sqlast.AggMin:
		if !st.hasMin {
			return NullCell()
		}
		return st.minCell
	case sqlast.AggMax:
		if !st.hasMax {
			return NullCell()
		}
		return st.maxCell
	case sqlast.AggStringAgg:
		if st.strHead == 0 {
			return NullCell()
		}
		rawStart := len(ext.valArena)
		ext.valArena = append(ext.valArena, '"')
		escaped := false
		sepBytes := byteview.Bytes(sp.separator)
		first := true
		for idx := st.strHead; idx != 0; idx = ext.strNodes[idx-1].next {
			if !first && len(sepBytes) > 0 {
				var sepEsc bool
				ext.valArena, sepEsc = appendJSONStringContent(ext.valArena, sepBytes)
				escaped = escaped || sepEsc
			}
			first = false
			nd := ext.strNodes[idx-1]
			var segEsc bool
			ext.valArena, segEsc = appendJSONStringContent(ext.valArena, ext.strArena[nd.off:nd.off+nd.length])
			escaped = escaped || segEsc
		}
		ext.valArena = append(ext.valArena, '"')
		raw := ext.valArena[rawStart:len(ext.valArena):len(ext.valArena)]
		var text string
		if !escaped {
			text = byteview.String(raw[1 : len(raw)-1])
		} else {
			textStart := len(ext.valArena)
			first = true
			for idx := st.strHead; idx != 0; idx = ext.strNodes[idx-1].next {
				if !first && len(sepBytes) > 0 {
					ext.valArena = append(ext.valArena, sepBytes...)
				}
				first = false
				nd := ext.strNodes[idx-1]
				ext.valArena = append(ext.valArena, ext.strArena[nd.off:nd.off+nd.length]...)
			}
			text = byteview.String(ext.valArena[textStart:len(ext.valArena):len(ext.valArena)])
		}
		return Cell{kind: TypeString, raw: raw, text: text}
	case sqlast.AggArrayAgg, sqlast.AggJsonAgg:
		if st.elemHead == 0 {
			return NullCell()
		}
		start := len(ext.valArena)
		ext.valArena = append(ext.valArena, '[')
		first := true
		for idx := st.elemHead; idx != 0; idx = ext.elemNodes[idx-1].next {
			if !first {
				ext.valArena = append(ext.valArena, ',')
			}
			first = false
			ext.valArena = ext.elemNodes[idx-1].cell.AppendJSON(ext.valArena)
		}
		ext.valArena = append(ext.valArena, ']')
		return Cell{kind: TypeJSON, raw: ext.valArena[start:len(ext.valArena):len(ext.valArena)]}
	case sqlast.AggBoolAnd, sqlast.AggBoolOr:
		if !st.hasBool {
			return NullCell()
		}
		if st.boolVal {
			return Cell{kind: TypeBool, flag: cellTrue, raw: trueBytes}
		}
		return Cell{kind: TypeBool, raw: falseBytes}
	default:
		return NullCell()
	}
}

func appendJSONStringContent(dst []byte, s []byte) ([]byte, bool) {
	const hex = "0123456789abcdef"
	escaped := false
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		escaped = true
		if i > start {
			dst = append(dst, s[start:i]...)
		}
		switch c {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
		}
		start = i + 1
	}
	if start < len(s) {
		dst = append(dst, s[start:]...)
	}
	return dst, escaped
}

func (ext *statementExtAgg) evalHaving(e *sqlast.Expr, row []Cell, args []any) bool {
	if e == nil {
		return true
	}
	switch e.Kind {
	case sqlast.ExprAnd:
		for _, kid := range e.Kids {
			if !ext.evalHaving(kid, row, args) {
				return false
			}
		}
		return true
	case sqlast.ExprOr:
		for _, kid := range e.Kids {
			if ext.evalHaving(kid, row, args) {
				return true
			}
		}
		return false
	case sqlast.ExprNot:
		if len(e.Kids) == 1 {
			return !ext.evalHaving(e.Kids[0], row, args)
		}
		return false
	}
	colIdx, ok := ext.havingColMap[e]
	if !ok || colIdx < 0 || colIdx >= len(row) {
		return false
	}
	left := row[colIdx]
	switch e.Kind {
	case sqlast.ExprIsNull:
		if e.Negated {
			return !left.IsNull()
		}
		return left.IsNull()
	case sqlast.ExprCompare:
		if left.IsNull() {
			return false
		}
		right, rok := operandToExtAggCell(e.Value, args)
		if !rok || right.IsNull() {
			return false
		}
		c := compareExtAggCells(left, right)
		switch e.Op {
		case sqlast.OpEq:
			return c == 0
		case sqlast.OpNe:
			return c != 0
		case sqlast.OpLt:
			return c < 0
		case sqlast.OpLe:
			return c <= 0
		case sqlast.OpGt:
			return c > 0
		case sqlast.OpGe:
			return c >= 0
		}
	case sqlast.ExprBetween:
		if left.IsNull() || len(e.List) < 2 {
			return false
		}
		lo, lok := operandToExtAggCell(e.List[0], args)
		hi, hok := operandToExtAggCell(e.List[1], args)
		if !lok || !hok || lo.IsNull() || hi.IsNull() {
			return false
		}
		in := compareExtAggCells(left, lo) >= 0 && compareExtAggCells(left, hi) <= 0
		if e.Negated {
			return !in
		}
		return in
	case sqlast.ExprIn:
		if left.IsNull() {
			return false
		}
		matched := false
		for _, op := range e.List {
			v, vok := operandToExtAggCell(op, args)
			if vok && !v.IsNull() && compareExtAggCells(left, v) == 0 {
				matched = true
				break
			}
		}
		if e.Negated {
			return !matched
		}
		return matched
	}
	return false
}

func operandToExtAggCell(op sqlast.Operand, args []any) (Cell, bool) {
	switch op.Kind {
	case sqlast.OperandNull:
		return NullCell(), true
	case sqlast.OperandNumber:
		if v, err := strconv.ParseInt(op.Text, 10, 64); err == nil {
			return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(v)}, true
		}
		return Cell{kind: TypeNumber, flag: cellNumberRaw, raw: byteview.Bytes(op.Text)}, true
	case sqlast.OperandBool:
		if op.Bool {
			return Cell{kind: TypeBool, flag: cellTrue, raw: trueBytes}, true
		}
		return Cell{kind: TypeBool, raw: falseBytes}, true
	case sqlast.OperandString:
		return Cell{kind: TypeString, text: op.Text}, true
	case sqlast.OperandParam:
		if op.Ordinal < 0 || op.Ordinal >= len(args) {
			return Cell{}, false
		}
		return paramToExtAggCell(args[op.Ordinal])
	}
	return Cell{}, false
}

func paramToExtAggCell(v any) (Cell, bool) {
	if v == nil {
		return NullCell(), true
	}
	switch x := v.(type) {
	case bool:
		if x {
			return Cell{kind: TypeBool, flag: cellTrue, raw: trueBytes}, true
		}
		return Cell{kind: TypeBool, raw: falseBytes}, true
	case *bool:
		if x == nil {
			return NullCell(), true
		}
		if *x {
			return Cell{kind: TypeBool, flag: cellTrue, raw: trueBytes}, true
		}
		return Cell{kind: TypeBool, raw: falseBytes}, true
	case int:
		return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(int64(x))}, true
	case int64:
		return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(x)}, true
	case *int64:
		if x == nil {
			return NullCell(), true
		}
		return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(*x)}, true
	case int32:
		return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(int64(x))}, true
	case int16:
		return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(int64(x))}, true
	case int8:
		return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(int64(x))}, true
	case uint64:
		if x <= math.MaxInt64 {
			return Cell{kind: TypeNumber, flag: cellInteger, word: x}, true
		}
		return Cell{kind: TypeNumber, flag: cellNumberRaw, raw: strconv.AppendUint(nil, x, 10)}, true
	case uint:
		if uint64(x) <= math.MaxInt64 {
			return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(x)}, true
		}
		return Cell{kind: TypeNumber, flag: cellNumberRaw, raw: strconv.AppendUint(nil, uint64(x), 10)}, true
	case uint32:
		return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(x)}, true
	case uint16:
		return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(x)}, true
	case uint8:
		return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(x)}, true
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return Cell{}, false
		}
		return Cell{kind: TypeNumber, word: math.Float64bits(x)}, true
	case *float64:
		if x == nil {
			return NullCell(), true
		}
		if math.IsNaN(*x) || math.IsInf(*x, 0) {
			return Cell{}, false
		}
		return Cell{kind: TypeNumber, word: math.Float64bits(*x)}, true
	case float32:
		f := float64(x)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return Cell{}, false
		}
		return Cell{kind: TypeNumber, word: math.Float64bits(f)}, true
	case string:
		return Cell{kind: TypeString, text: x}, true
	case *string:
		if x == nil {
			return NullCell(), true
		}
		return Cell{kind: TypeString, text: *x}, true
	case Number:
		if iv, err := strconv.ParseInt(string(x), 10, 64); err == nil {
			return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(iv)}, true
		}
		if !validJSONNumber(string(x)) {
			return Cell{}, false
		}
		return Cell{kind: TypeNumber, flag: cellNumberRaw, raw: byteview.Bytes(string(x))}, true
	case *Number:
		if x == nil {
			return NullCell(), true
		}
		if iv, err := strconv.ParseInt(string(*x), 10, 64); err == nil {
			return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(iv)}, true
		}
		if !validJSONNumber(string(*x)) {
			return Cell{}, false
		}
		return Cell{kind: TypeNumber, flag: cellNumberRaw, raw: byteview.Bytes(string(*x))}, true
	case []byte:
		c, err := ParseJSONCell(x)
		return c, err == nil
	}
	return Cell{}, false
}

func resolveExtAggIntOperand(op *sqlast.Operand, args []any) (int, error) {
	if op == nil {
		return 0, nil
	}
	if op.Kind == sqlast.OperandNumber {
		v, err := strconv.Atoi(op.Text)
		return v, err
	}
	if op.Kind == sqlast.OperandParam && op.Ordinal >= 0 && op.Ordinal < len(args) {
		switch v := args[op.Ordinal].(type) {
		case int:
			return v, nil
		case int64:
			return int(v), nil
		case *int64:
			if v != nil {
				return int(*v), nil
			}
		case float64:
			return int(v), nil
		case Number:
			return strconv.Atoi(string(v))
		}
	}
	return 0, nil
}

func ratToCellInto(r *big.Rat, arena *[]byte) Cell {
	if r.IsInt() {
		num := r.Num()
		if num.IsInt64() {
			return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(num.Int64())}
		}
		start := len(*arena)
		*arena = num.Append(*arena, 10)
		raw := (*arena)[start:len(*arena):len(*arena)]
		return Cell{kind: TypeNumber, flag: cellNumberRaw, raw: raw}
	}
	f, _ := r.Float64()
	return Cell{kind: TypeNumber, word: math.Float64bits(f)}
}

func compareExtAggCells(a, b Cell) int {
	if a.kind == TypeNumber && b.kind == TypeNumber {
		if a.flag&cellInteger != 0 && b.flag&cellInteger != 0 {
			return cmp.Compare(int64(a.word), int64(b.word))
		}
		var scratchA, scratchB [64]byte
		ba := a.raw
		if ba == nil {
			ba = a.AppendJSON(scratchA[:0])
		}
		bb := b.raw
		if bb == nil {
			bb = b.AppendJSON(scratchB[:0])
		}
		return compareNumberBytes(ba, bb)
	}
	if a.kind == TypeString && b.kind == TypeString {
		return cmp.Compare(a.text, b.text)
	}
	if a.kind == TypeBool && b.kind == TypeBool {
		ba := a.flag&cellTrue != 0
		bb := b.flag&cellTrue != 0
		if ba == bb {
			return 0
		}
		if !ba {
			return -1
		}
		return 1
	}
	var scratchA, scratchB [64]byte
	ba := a.AppendJSON(scratchA[:0])
	bb := b.AppendJSON(scratchB[:0])
	return bytes.Compare(ba, bb)
}
