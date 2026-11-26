package query

import (
	"fmt"
	"strconv"

	sqlast "github.com/thesyncim/vibedb/sql"
)

type statementDistinctOn struct {
	input         *Statement
	visibleCount  int
	onColIndices  []int
	limitOperand  *sqlast.Operand
	offsetOperand *sqlast.Operand
	result        Result
}

func prepareDistinctOnStatement(
	src string,
	tree *sqlast.SelectStmt,
	subqueryLimit uint8,
	ctes *statementCTEs,
	argBase int,
	correlation *lateralPrepareFrame,
	parameterTypes []ParameterType,
) (*Statement, error) {
	distinctOn := tree.DistinctOnPaths()
	inputTree := *tree
	inputTree.Extensions = nil
	inputTree.Distinct = false
	inputTree.Limit = nil
	inputTree.Offset = nil
	inputTree.Columns = append([]sqlast.ResultColumn(nil), tree.Columns...)

	visibleCount := len(tree.Columns)
	onIndices := make([]int, len(distinctOn))
	for i, onPath := range distinctOn {
		found := -1
		for ci := range tree.Columns {
			c := &tree.Columns[ci]
			if c.Agg == sqlast.AggNone && c.Scalar == nil && c.Window == nil &&
				c.Path != nil && onPath != nil && c.Path.Spec() == onPath.Spec() {
				found = ci
				break
			}
		}
		if found >= 0 {
			onIndices[i] = found
		} else {
			onIndices[i] = len(inputTree.Columns)
			inputTree.Columns = append(inputTree.Columns, sqlast.ResultColumn{
				Path: onPath,
				Pos:  onPath.Pos,
			})
		}
	}

	inputStmt, err := prepareTreeInCorrelationContext(
		src, &inputTree, subqueryLimit, ctes, argBase, correlation, parameterTypes,
	)
	if err != nil {
		return nil, err
	}

	visNames := append([]string(nil), inputStmt.Columns()[:visibleCount]...)
	d := &statementDistinctOn{
		input:         inputStmt,
		visibleCount:  visibleCount,
		onColIndices:  onIndices,
		limitOperand:  tree.Limit,
		offsetOperand: tree.Offset,
	}
	return &Statement{
		text:       src,
		tree:       tree,
		params:     tree.Params,
		names:      visNames,
		outputs:    visibleCount,
		distinctOn: d,
	}, nil
}

func (s *Statement) runDistinctOnInto(e *Exec, src Source, args []any) (Cursor, error) {
	d := s.distinctOn
	inExec := &Exec{Options: e.Options}
	defer inExec.Release()
	cursor, err := d.input.RunInto(inExec, src, args)
	if err != nil {
		return Cursor{}, err
	}

	offset, err := evalRowBoundOperand(d.offsetOperand, args)
	if err != nil {
		return Cursor{}, err
	}
	hasLimit := d.limitOperand != nil
	limit, err := evalRowBoundOperand(d.limitOperand, args)
	if err != nil {
		return Cursor{}, err
	}

	seen := make(map[string]struct{})
	var rows [][]Cell
	skipped := 0
	for cursor.Next() {
		var kb []byte
		for _, idx := range d.onColIndices {
			c := cursor.Cell(idx)
			if c.IsNull() {
				kb = append(kb, 0)
			} else {
				kb = append(kb, 1)
				kb = c.AppendJSON(kb)
			}
			kb = append(kb, 0xff)
		}
		key := string(kb)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		if skipped < offset {
			skipped++
			continue
		}
		if hasLimit && len(rows) >= limit {
			break
		}
		row := make([]Cell, d.visibleCount)
		for i := 0; i < d.visibleCount; i++ {
			row[i] = cloneCellValue(cursor.Cell(i))
		}
		rows = append(rows, row)
	}

	out := &d.result
	out.RowCount = len(rows)
	out.Columns = make([]ResultColumn, d.visibleCount)
	for i := 0; i < d.visibleCount; i++ {
		out.Columns[i] = ResultColumn{
			Header: s.names[i],
			Cells:  make([]Cell, len(rows)),
		}
	}
	for r, row := range rows {
		for c := 0; c < d.visibleCount; c++ {
			out.Columns[c].Cells[r] = row[c]
		}
	}
	return NewResultCursor(out)
}

func evalRowBoundOperand(op *sqlast.Operand, args []any) (int, error) {
	if op == nil {
		return 0, nil
	}
	switch op.Kind {
	case sqlast.OperandNumber:
		n, err := strconv.Atoi(op.Text)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("query: invalid row count %q", op.Text)
		}
		return n, nil
	case sqlast.OperandParam:
		if op.Ordinal < 0 || op.Ordinal >= len(args) {
			return 0, fmt.Errorf("query: missing parameter %d", op.Ordinal)
		}
		switch v := args[op.Ordinal].(type) {
		case int:
			return v, nil
		case int64:
			return int(v), nil
		case Number:
			n, err := strconv.Atoi(string(v))
			if err != nil || n < 0 {
				return 0, fmt.Errorf("query: invalid row count parameter %q", v)
			}
			return n, nil
		default:
			return 0, fmt.Errorf("query: invalid row count parameter type %T", v)
		}
	default:
		return 0, fmt.Errorf("query: unsupported row count operand")
	}
}
