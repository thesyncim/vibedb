package query

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"

	sqlast "github.com/thesyncim/vibedb/sql"
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

type statementExtAgg struct {
	input         *Statement
	grouped       bool
	groupCols     []int
	specs         []extAggColumnSpec
	visibleCount  int
	names         []string
	having        *sqlast.Expr
	havingColMap  map[*sqlast.Expr]int
	orderBy       []extAggOrderSpec
	limit         *sqlast.Operand
	offset        *sqlast.Operand
	result        Result
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

type extAggState struct {
	count   int64
	sum     *big.Rat
	hasNum  bool
	minCell Cell
	maxCell Cell
	hasMin  bool
	hasMax  bool
	strs    []string
	elems   []Cell
	boolVal bool
	hasBool bool
	seen    map[string]struct{}
}

func (s *Statement) runExtAggInto(e *Exec, src Source, args []any) (Cursor, error) {
	ext := s.extAgg
	inExec := &Exec{Options: e.Options}
	defer inExec.Release()
	cursor, err := ext.input.RunInto(inExec, src, args)
	if err != nil {
		return Cursor{}, err
	}

	type groupEntry struct {
		keys   []Cell
		states []extAggState
	}
	var order []string
	groups := make(map[string]*groupEntry)

	for cursor.Next() {
		var gkey string
		var keyCells []Cell
		if ext.grouped {
			var kb []byte
			keyCells = make([]Cell, len(ext.groupCols))
			for i, colIdx := range ext.groupCols {
				c := cursor.Cell(colIdx)
				keyCells[i] = cloneCellValue(c)
				if c.IsNull() {
					kb = append(kb, 0)
				} else {
					kb = append(kb, 1)
					kb = c.AppendJSON(kb)
				}
				kb = append(kb, 0xff)
			}
			gkey = string(kb)
		}
		entry, exists := groups[gkey]
		if !exists {
			states := make([]extAggState, len(ext.specs))
			for i, sp := range ext.specs {
				if sp.distinct {
					states[i].seen = make(map[string]struct{})
				}
			}
			entry = &groupEntry{keys: keyCells, states: states}
			groups[gkey] = entry
			order = append(order, gkey)
		}
		for i, sp := range ext.specs {
			if sp.isGroupKey {
				continue
			}
			if sp.filterCol >= 0 {
				fc := cursor.Cell(sp.filterCol)
				b, ok := fc.Bool()
				if !ok || !b {
					continue
				}
			}
			st := &entry.states[i]
			if sp.agg == sqlast.AggCount && sp.inputCol < 0 {
				st.count++
				continue
			}
			cell := cursor.Cell(sp.inputCol)
			if cell.IsNull() {
				continue
			}
			if sp.distinct {
				rawKey := string(cell.AppendJSON(nil))
				if _, dup := st.seen[rawKey]; dup {
					continue
				}
				st.seen[rawKey] = struct{}{}
			}
			switch sp.agg {
			case sqlast.AggCount:
				st.count++
			case sqlast.AggSum, sqlast.AggAvg:
				if r, ok := cellToRat(cell); ok {
					if st.sum == nil {
						st.sum = new(big.Rat)
					}
					st.sum.Add(st.sum, r)
					st.count++
					st.hasNum = true
				}
			case sqlast.AggMin:
				if !st.hasMin || compareExtAggCells(cell, st.minCell) < 0 {
					st.minCell = cloneCellValue(cell)
					st.hasMin = true
				}
			case sqlast.AggMax:
				if !st.hasMax || compareExtAggCells(cell, st.maxCell) > 0 {
					st.maxCell = cloneCellValue(cell)
					st.hasMax = true
				}
			case sqlast.AggStringAgg:
				raw := cell.AppendJSON(nil)
				var unq string
				if cell.Type() == TypeString && json.Unmarshal(raw, &unq) == nil {
					st.strs = append(st.strs, unq)
				} else {
					st.strs = append(st.strs, string(raw))
				}
			case sqlast.AggArrayAgg, sqlast.AggJsonAgg:
				st.elems = append(st.elems, cloneCellValue(cell))
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

	if !ext.grouped && len(order) == 0 {
		states := make([]extAggState, len(ext.specs))
		groups[""] = &groupEntry{states: states}
		order = append(order, "")
	}

	// Build finalized rows so we can filter HAVING and sort/slice cleanly.
	rows := make([][]Cell, 0, len(order))
	for _, gkey := range order {
		entry := groups[gkey]
		row := make([]Cell, len(ext.specs))
		for i, sp := range ext.specs {
			if sp.isGroupKey {
				if sp.groupKeyIdx >= 0 && sp.groupKeyIdx < len(entry.keys) {
					row[i] = entry.keys[sp.groupKeyIdx]
				} else {
					row[i] = NullCell()
				}
				continue
			}
			st := &entry.states[i]
			switch sp.agg {
			case sqlast.AggCount:
				c, _ := ParseJSONCell([]byte(strconv.FormatInt(st.count, 10)))
				row[i] = c
			case sqlast.AggSum:
				if !st.hasNum {
					row[i] = NullCell()
				} else {
					row[i] = ratToCell(st.sum)
				}
			case sqlast.AggAvg:
				if !st.hasNum || st.count == 0 {
					row[i] = NullCell()
				} else {
					avg := new(big.Rat).Quo(st.sum, big.NewRat(st.count, 1))
					row[i] = ratToCell(avg)
				}
			case sqlast.AggMin:
				if !st.hasMin {
					row[i] = NullCell()
				} else {
					row[i] = st.minCell
				}
			case sqlast.AggMax:
				if !st.hasMax {
					row[i] = NullCell()
				} else {
					row[i] = st.maxCell
				}
			case sqlast.AggStringAgg:
				if len(st.strs) == 0 {
					row[i] = NullCell()
				} else {
					b, _ := json.Marshal(strings.Join(st.strs, sp.separator))
					c, _ := ParseJSONCell(b)
					row[i] = c
				}
			case sqlast.AggArrayAgg, sqlast.AggJsonAgg:
				if len(st.elems) == 0 {
					row[i] = NullCell()
				} else {
					var buf []byte
					buf = append(buf, '[')
					for idx, el := range st.elems {
						if idx > 0 {
							buf = append(buf, ',')
						}
						buf = el.AppendJSON(buf)
					}
					buf = append(buf, ']')
					c, _ := ParseJSONCell(buf)
					row[i] = c
				}
			case sqlast.AggBoolAnd, sqlast.AggBoolOr:
				if !st.hasBool {
					row[i] = NullCell()
				} else if st.boolVal {
					c, _ := ParseJSONCell([]byte("true"))
					row[i] = c
				} else {
					c, _ := ParseJSONCell([]byte("false"))
					row[i] = c
				}
			}
		}
		if ext.having != nil && !evalExtAggHaving(ext.having, row, ext.havingColMap, args) {
			continue
		}
		rows = append(rows, row)
	}

	if len(ext.orderBy) > 0 {
		sort.SliceStable(rows, func(i, j int) bool {
			for _, ord := range ext.orderBy {
				a, b := rows[i][ord.colIdx], rows[j][ord.colIdx]
				if a.IsNull() && b.IsNull() {
					continue
				}
				if a.IsNull() {
					return ord.nullsFirst
				}
				if b.IsNull() {
					return !ord.nullsFirst
				}
				cmp := compareExtAggCells(a, b)
				if cmp != 0 {
					if ord.desc {
						return cmp > 0
					}
					return cmp < 0
				}
			}
			return false
		})
	}

	if ext.offset != nil {
		off, err := resolveExtAggIntOperand(ext.offset, args)
		if err != nil {
			return Cursor{}, err
		}
		if off > 0 {
			if off >= len(rows) {
				rows = nil
			} else {
				rows = rows[off:]
			}
		}
	}
	if ext.limit != nil {
		lim, err := resolveExtAggIntOperand(ext.limit, args)
		if err != nil {
			return Cursor{}, err
		}
		if lim >= 0 && lim < len(rows) {
			rows = rows[:lim]
		}
	}

	out := &ext.result
	out.RowCount = len(rows)
	vis := ext.visibleCount
	if vis == 0 {
		vis = len(ext.specs)
	}
	out.Columns = make([]ResultColumn, vis)
	for i := range vis {
		sp := ext.specs[i]
		cells := make([]Cell, len(rows))
		for r, row := range rows {
			cells[r] = row[i]
		}
		out.Columns[i] = ResultColumn{
			Header: sp.header,
			Cells:  cells,
		}
	}
	return NewResultCursor(out)
}

func evalExtAggHaving(e *sqlast.Expr, row []Cell, colMap map[*sqlast.Expr]int, args []any) bool {
	if e == nil {
		return true
	}
	switch e.Kind {
	case sqlast.ExprAnd:
		for _, kid := range e.Kids {
			if !evalExtAggHaving(kid, row, colMap, args) {
				return false
			}
		}
		return true
	case sqlast.ExprOr:
		for _, kid := range e.Kids {
			if evalExtAggHaving(kid, row, colMap, args) {
				return true
			}
		}
		return false
	case sqlast.ExprNot:
		if len(e.Kids) == 1 {
			return !evalExtAggHaving(e.Kids[0], row, colMap, args)
		}
		return false
	}
	colIdx, ok := colMap[e]
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
		cmp := compareExtAggCells(left, right)
		switch e.Op {
		case sqlast.OpEq:
			return cmp == 0
		case sqlast.OpNe:
			return cmp != 0
		case sqlast.OpLt:
			return cmp < 0
		case sqlast.OpLe:
			return cmp <= 0
		case sqlast.OpGt:
			return cmp > 0
		case sqlast.OpGe:
			return cmp >= 0
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
		c, err := ParseJSONCell([]byte(op.Text))
		return c, err == nil
	case sqlast.OperandBool:
		if op.Bool {
			c, err := ParseJSONCell([]byte("true"))
			return c, err == nil
		}
		c, err := ParseJSONCell([]byte("false"))
		return c, err == nil
	case sqlast.OperandString:
		b, err := json.Marshal(op.Text)
		if err != nil {
			return Cell{}, false
		}
		c, err := ParseJSONCell(b)
		return c, err == nil
	case sqlast.OperandParam:
		if op.Ordinal < 0 || op.Ordinal >= len(args) {
			return Cell{}, false
		}
		v := args[op.Ordinal]
		if v == nil {
			return NullCell(), true
		}
		b, err := json.Marshal(v)
		if err != nil {
			return Cell{}, false
		}
		c, err := ParseJSONCell(b)
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
		case float64:
			return int(v), nil
		}
	}
	return 0, nil
}

func cellToRat(c Cell) (*big.Rat, bool) {
	if c.Type() != TypeNumber {
		return nil, false
	}
	r, ok := new(big.Rat).SetString(string(c.AppendJSON(nil)))
	return r, ok
}

func ratToCell(r *big.Rat) Cell {
	if r.IsInt() {
		c, _ := ParseJSONCell([]byte(r.Num().String()))
		return c
	}
	f, _ := r.Float64()
	c, _ := ParseJSONCell([]byte(strconv.FormatFloat(f, 'g', -1, 64)))
	return c
}

func cloneCellValue(c Cell) Cell {
	if c.IsNull() {
		return NullCell()
	}
	raw := c.AppendJSON(nil)
	cloned, err := ParseJSONCell(raw)
	if err != nil {
		return NullCell()
	}
	return cloned
}

func compareExtAggCells(a, b Cell) int {
	if a.Type() == TypeNumber && b.Type() == TypeNumber {
		ra, oka := cellToRat(a)
		rb, okb := cellToRat(b)
		if oka && okb {
			return ra.Cmp(rb)
		}
	}
	if a.Type() == TypeString && b.Type() == TypeString {
		sa, sb := a.String(), b.String()
		if sa < sb {
			return -1
		}
		if sa > sb {
			return 1
		}
		return 0
	}
	if ba, oka := a.Bool(); oka {
		if bb, okb := b.Bool(); okb {
			if !ba && bb {
				return -1
			}
			if ba && !bb {
				return 1
			}
			return 0
		}
	}
	sa := string(a.AppendJSON(nil))
	sb := string(b.AppendJSON(nil))
	if sa < sb {
		return -1
	}
	if sa > sb {
		return 1
	}
	return 0
}
