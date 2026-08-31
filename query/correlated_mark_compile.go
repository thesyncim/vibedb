package query

import "fmt"

// compileMarks lowers proof-backed correlated predicate subqueries into one
// immutable address map per mark and one hidden predicate leaf per binding.
// The leaf itself carries only a slot. Its children are non-evaluated column
// references used by readsColumn, keeping the ordinary compiledPredicate and
// scalar layouts unchanged while still pulling every composite key and value
// probe into the driving filter phase.
func (c *compiler) compileMarks(
	q *Query,
	p *plan,
	values *pathRegistry,
) ([]*compiledPredicate, error) {
	if len(q.marks) == 0 {
		return nil, nil
	}
	p.marks = reserve(c.planMarks[:0], len(q.marks))
	nodes := c.kids.alloc(len(q.marks))[:0]
	for i := range q.marks {
		compiled, err := c.compileMark(q.marks[i], i, values)
		if err != nil {
			return nil, err
		}
		p.marks = append(p.marks, compiled)

		refs := c.kids.alloc(len(compiled.outer) + btoi(compiled.probe >= 0))[:0]
		for _, col := range compiled.outer {
			ref := c.nodes.one()
			*ref = compiledPredicate{kind: predMarkRef, col: col}
			refs = append(refs, ref)
		}
		if compiled.probe >= 0 && !hasMarkRef(refs, compiled.probe) {
			ref := c.nodes.one()
			*ref = compiledPredicate{kind: predMarkRef, col: compiled.probe}
			refs = append(refs, ref)
		}
		node := c.nodes.one()
		*node = compiledPredicate{kind: predMarkBound, slot: compiled.slot, kids: refs}
		nodes = append(nodes, node)
	}
	c.planMarks = p.marks
	return nodes, nil
}

func btoi(v bool) int {
	if v {
		return 1
	}
	return 0
}

func hasMarkRef(refs []*compiledPredicate, col int) bool {
	for _, ref := range refs {
		if ref.col == col {
			return true
		}
	}
	return false
}

func (c *compiler) compileMark(
	m correlatedMark,
	index int,
	values *pathRegistry,
) (planMark, error) {
	if m.collection == "" {
		return planMark{}, fmt.Errorf("query: correlated mark[%d] has no inner collection", index)
	}
	if len(m.keys) == 0 {
		return planMark{}, fmt.Errorf("query: correlated mark[%d] has no correlation key", index)
	}
	if m.kind > correlatedMarkScalar {
		return planMark{}, fmt.Errorf("query: correlated mark[%d] has invalid mode %d", index, m.kind)
	}

	var outerCols, innerCols, keyPositions []int
	var keyOuterFirst []bool
	if index < len(c.planMarks) {
		outerCols = c.planMarks[index].outer[:0]
		innerCols = c.planMarks[index].innerKeys[:0]
		keyPositions = c.planMarks[index].keyPositions[:0]
		keyOuterFirst = c.planMarks[index].keyOuterFirst[:0]
	}
	outerCols = reserve(outerCols, len(m.keys))
	innerCols = reserve(innerCols, len(m.keys))
	keyPositions = reserve(keyPositions, len(m.keys))
	keyOuterFirst = reserve(keyOuterFirst, len(m.keys))
	innerReg := c.markRegistry(index)
	for keyIndex := range m.keys {
		key := m.keys[keyIndex]
		if key.outer == "" || key.inner == "" {
			return planMark{}, fmt.Errorf(
				"query: correlated mark[%d] key[%d] must name both outer and inner paths",
				index, keyIndex,
			)
		}
		outer, err := c.addPath(values, key.outer)
		if err != nil {
			return planMark{}, err
		}
		inner, err := c.addPath(innerReg, key.inner)
		if err != nil {
			return planMark{}, err
		}
		outerCols = append(outerCols, outer)
		innerCols = append(innerCols, inner)
		keyPositions = append(keyPositions, key.operatorPos)
		keyOuterFirst = append(keyOuterFirst, key.outerFirst)
	}

	probe, value := -1, -1
	switch m.kind {
	case correlatedMarkIn, correlatedMarkNotIn, correlatedMarkScalar:
		if m.probe == "" || m.project == "" {
			return planMark{}, fmt.Errorf(
				"query: correlated mark[%d] value mode requires probe and projection paths", index,
			)
		}
		var err error
		if probe, err = c.addPath(values, m.probe); err != nil {
			return planMark{}, err
		}
		if value, err = c.addPath(innerReg, m.project); err != nil {
			return planMark{}, err
		}
	}

	innerPlan := c.markPlan(index)
	filterCols := innerPlan.filterCols[:0]
	*innerPlan = plan{fanOutJoin: -1}
	if m.hasWhere {
		where, err := c.compilePredicate(m.where, innerReg)
		if err != nil {
			return planMark{}, err
		}
		innerPlan.where = where
	}
	innerPlan.runtimeSQLPaths = innerPlan.where.hasRuntimeSQLPathComparison()
	innerPlan.valuePaths = innerReg.paths
	filterCols = reserve(filterCols, len(innerReg.paths))
	for col := range innerReg.paths {
		filterCols = append(filterCols, col)
	}
	innerPlan.filterCols = filterCols

	return planMark{
		collection:    m.collection,
		inner:         innerPlan,
		outer:         outerCols,
		innerKeys:     innerCols,
		keyPositions:  keyPositions,
		keyOuterFirst: keyOuterFirst,
		probe:         probe,
		value:         value,
		slot:          index,
		kind:          m.kind,
		op:            m.op,
		authoredOp:    m.authoredOp,
		valuePos:      m.valuePos,
	}, nil
}

func (c *compiler) markRegistry(index int) *pathRegistry {
	if c.oneShot {
		return new(pathRegistry)
	}
	for len(c.markRegs) <= index {
		c.markRegs = append(c.markRegs, new(pathRegistry))
	}
	c.markRegs[index].reset()
	return c.markRegs[index]
}

func (c *compiler) markPlan(index int) *plan {
	if c.oneShot {
		return new(plan)
	}
	for len(c.markPlans) <= index {
		c.markPlans = append(c.markPlans, new(plan))
	}
	return c.markPlans[index]
}
