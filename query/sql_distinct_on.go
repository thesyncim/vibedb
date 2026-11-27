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

	// Reusable per-statement execution workspaces, cleared between runs and
	// released by Statement.Release.
	inExec     Exec
	seen       extKeyTable
	keyScratch []byte
	valArena   []byte
	result     Result
}

func (d *statementDistinctOn) release() {
	if d == nil {
		return
	}
	if d.input != nil {
		d.input.Release()
		d.input = nil
	}
	d.inExec.Release()
	d.seen.release()
	d.keyScratch = nil
	d.valArena = nil
	d.result.Release()
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
	d.seen.reset()
	out := &d.result
	if len(out.Columns) != d.visibleCount {
		out.Columns = make([]ResultColumn, d.visibleCount)
		for i := range d.visibleCount {
			out.Columns[i].Header = s.names[i]
		}
	} else {
		for i := range out.Columns {
			clear(out.Columns[i].Cells)
			out.Columns[i].Cells = out.Columns[i].Cells[:0]
			out.Columns[i].Header = s.names[i]
		}
	}
	out.RowCount = 0
	d.valArena = d.valArena[:0]
	cancel := e.Options.Cancel
	if err := cancellationError(cancel); err != nil {
		return Cursor{}, err
	}

	d.inExec.Options = e.Options
	cursor, err := d.input.RunInto(&d.inExec, src, args)
	e.Stats = d.inExec.Stats
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

	singleKey := len(d.onColIndices) == 1
	skipped := 0
	rowCount := 0
	for {
		ok, err := cursor.NextWithCancel(cancel)
		if err != nil {
			return Cursor{}, err
		}
		if !ok {
			break
		}
		kb := d.keyScratch[:0]
		if singleKey {
			kb = appendExtSingleKeyCell(kb, cursor.Cell(d.onColIndices[0]))
		} else {
			for _, idx := range d.onColIndices {
				kb = appendExtKeyCell(kb, cursor.Cell(idx))
			}
		}
		d.keyScratch = kb
		if _, inserted := d.seen.findOrInsert(kb); !inserted {
			continue
		}
		if skipped < offset {
			skipped++
			continue
		}
		if hasLimit && rowCount >= limit {
			break
		}
		for i := range d.visibleCount {
			var detached Cell
			detached, d.valArena = ownResultCellInto(d.valArena, cursor.Cell(i))
			out.Columns[i].Cells = append(out.Columns[i].Cells, detached)
		}
		rowCount++
	}

	out.RowCount = rowCount
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
			if v < 0 {
				return 0, fmt.Errorf("query: invalid row count %d", v)
			}
			return v, nil
		case int64:
			if v < 0 {
				return 0, fmt.Errorf("query: invalid row count %d", v)
			}
			return int(v), nil
		case *int64:
			if v != nil {
				if *v < 0 {
					return 0, fmt.Errorf("query: invalid row count %d", *v)
				}
				return int(*v), nil
			}
			return 0, fmt.Errorf("query: invalid row count parameter type %T", v)
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
