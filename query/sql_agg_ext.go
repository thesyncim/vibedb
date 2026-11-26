package query

import (
	"fmt"
	"math/big"
	"strconv"

	sqlast "github.com/thesyncim/vibedb/sql"
)

type extAggColumnSpec struct {
	header      string
	agg         sqlast.AggKind
	distinct    bool
	inputCol    int // >= 0 for group key or aggregate argument path
	filterCol   int // >= 0 if FILTER (WHERE ...) is attached
	isGroupKey  bool
	groupKeyIdx int
}

type statementExtAgg struct {
	input      *Statement
	grouped    bool
	groupCols  []int
	specs      []extAggColumnSpec
	names      []string
	result     Result
}

func hasExtendedAggregate(tree *sqlast.SelectStmt) bool {
	if tree == nil {
		return false
	}
	for i := range tree.Columns {
		if tree.Columns[i].AggDistinct || tree.Columns[i].AggFilter != nil {
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
	inputTree := *tree
	inputTree.GroupBy = nil
	inputTree.Having = nil
	inputTree.OrderBy = nil
	inputTree.Limit = nil
	inputTree.Offset = nil
	inputTree.Distinct = false
	inputTree.Columns = nil

	ext := &statementExtAgg{
		grouped: len(tree.GroupBy) > 0,
	}

	// 1. Add all GROUP BY paths to inputTree.
	for _, gpath := range tree.GroupBy {
		idx := len(inputTree.Columns)
		inputTree.Columns = append(inputTree.Columns, sqlast.ResultColumn{
			Path: gpath,
			Pos:  gpath.Pos,
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
			} else if col.Agg == sqlast.AggCount && col.Path == nil {
				header = "count(*)"
			} else if col.Path != nil {
				header = aggName(col.Agg) + "(" + string(col.Path.AppendSpec(nil)) + ")"
			} else {
				header = aggName(col.Agg)
			}
		}
		ext.names = append(ext.names, header)
		spec := extAggColumnSpec{
			header:    header,
			agg:       col.Agg,
			distinct:  col.AggDistinct,
			inputCol:  -1,
			filterCol: -1,
		}
		if col.Agg == sqlast.AggNone {
			spec.isGroupKey = true
			spec.groupKeyIdx = -1
			for gi, gpath := range tree.GroupBy {
				if col.Path != nil && gpath != nil && col.Path.Spec() == gpath.Spec() {
					spec.groupKeyIdx = gi
					spec.inputCol = ext.groupCols[gi]
					break
				}
			}
			if spec.inputCol < 0 && col.Path != nil {
				spec.inputCol = len(inputTree.Columns)
				inputTree.Columns = append(inputTree.Columns, sqlast.ResultColumn{Path: col.Path, Pos: col.Pos})
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
				keyCells[i] = c
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
			}
		}
	}

	if !ext.grouped && len(order) == 0 {
		states := make([]extAggState, len(ext.specs))
		groups[""] = &groupEntry{states: states}
		order = append(order, "")
	}

	out := &ext.result
	out.RowCount = len(order)
	out.Columns = make([]ResultColumn, len(ext.specs))
	for i, sp := range ext.specs {
		out.Columns[i] = ResultColumn{
			Header: sp.header,
			Cells:  make([]Cell, len(order)),
		}
	}
	for r, gkey := range order {
		entry := groups[gkey]
		for i, sp := range ext.specs {
			if sp.isGroupKey {
				if sp.groupKeyIdx >= 0 && sp.groupKeyIdx < len(entry.keys) {
					out.Columns[i].Cells[r] = cloneCellValue(entry.keys[sp.groupKeyIdx])
				} else {
					out.Columns[i].Cells[r] = NullCell()
				}
				continue
			}
			st := &entry.states[i]
			switch sp.agg {
			case sqlast.AggCount:
				c, _ := ParseJSONCell([]byte(strconv.FormatInt(st.count, 10)))
				out.Columns[i].Cells[r] = c
			case sqlast.AggSum:
				if !st.hasNum {
					out.Columns[i].Cells[r] = NullCell()
				} else {
					out.Columns[i].Cells[r] = ratToCell(st.sum)
				}
			case sqlast.AggAvg:
				if !st.hasNum || st.count == 0 {
					out.Columns[i].Cells[r] = NullCell()
				} else {
					avg := new(big.Rat).Quo(st.sum, big.NewRat(st.count, 1))
					out.Columns[i].Cells[r] = ratToCell(avg)
				}
			case sqlast.AggMin:
				if !st.hasMin {
					out.Columns[i].Cells[r] = NullCell()
				} else {
					out.Columns[i].Cells[r] = st.minCell
				}
			case sqlast.AggMax:
				if !st.hasMax {
					out.Columns[i].Cells[r] = NullCell()
				} else {
					out.Columns[i].Cells[r] = st.maxCell
				}
			}
		}
	}
	return NewResultCursor(out)
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

var _ = fmt.Sprintf
