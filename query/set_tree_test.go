package query

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"github.com/thesyncim/vibejson"
)

type setTreeTestSource struct {
	rows    [][]Cell
	columns int
	onCell  func(row, column int)
}

func (s *setTreeTestSource) Rows() int { return len(s.rows) }

func (s *setTreeTestSource) Columns() int {
	if s.columns != 0 || len(s.rows) == 0 {
		return s.columns
	}
	return len(s.rows[0])

}

func (s *setTreeTestSource) Cell(row, column int) Cell {
	if s.onCell != nil {
		s.onCell(row, column)
	}
	return s.rows[row][column]
}

type setTreeTestProgram struct {
	sources []SetTreeSource
	calls   int
	errAt   int
	err     error
}

func (p *setTreeTestProgram) Leaf(source int, _ *CancelFlag) (SetTreeSource, error) {
	p.calls++
	if p.err != nil && source == p.errAt {
		return nil, p.err
	}
	if source < 0 || source >= len(p.sources) {
		return nil, fmt.Errorf("test source %d is out of range", source)
	}
	return p.sources[source], nil
}

func setTreeTestJSONCell(src string) Cell {
	if src == "<missing>" {
		return Cell{kind: TypeNull, flag: cellMissing, raw: nullBytes}
	}
	var decoded []byte
	return cellFromScalar(classifyRawInto(
		vibejson.RawValue{Src: []byte(src)}, &decoded,
	))
}

func setTreeTestSourceRows(rows ...[]string) *setTreeTestSource {
	source := &setTreeTestSource{rows: make([][]Cell, len(rows))}
	for row := range rows {
		source.rows[row] = make([]Cell, len(rows[row]))
		for column, value := range rows[row] {
			source.rows[row][column] = setTreeTestJSONCell(value)
		}
	}
	return source
}

func setTreeResultJSON(result SetTreeResult) [][]string {
	rows := make([][]string, result.Rows())
	for row := range rows {
		rows[row] = make([]string, result.Columns())
		for column := range rows[row] {
			if result.Missing(row, column) {
				rows[row][column] = "<missing>"
				continue
			}
			rows[row][column] = string(result.Cell(row, column).JSON())
		}
	}
	return rows
}

func TestSetTreeBuilderSQLPrecedenceAndLeftAssociativity(t *testing.T) {
	t.Run("INTERSECT above UNION", func(t *testing.T) {
		sources := []SetTreeSource{
			setTreeTestSourceRows([]string{`1`}),
			setTreeTestSourceRows([]string{`2`}, []string{`3`}),
			setTreeTestSourceRows([]string{`3`}),
		}
		var builder SetTreeBuilder
		plan, err := builder.BuildChain(
			[]SetTreeLeafSpec{{0, 1}, {1, 1}, {2, 1}},
			[]SetTreeOperation{SetTreeUnionDistinct, SetTreeIntersectDistinct},
		)
		if err != nil {
			t.Fatal(err)
		}
		var executor SetTreeExecutor
		result, err := executor.Run(plan, &setTreeTestProgram{sources: sources}, SetTreeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		want := [][]string{{`1`}, {`3`}}
		if got := setTreeResultJSON(result); !reflect.DeepEqual(got, want) {
			t.Fatalf("A UNION B INTERSECT C = %v, want %v", got, want)
		}
	})

	t.Run("UNION and EXCEPT left associative", func(t *testing.T) {
		sources := []SetTreeSource{
			setTreeTestSourceRows([]string{`1`}, []string{`2`}),
			setTreeTestSourceRows([]string{`2`}),
			setTreeTestSourceRows([]string{`2`}),
		}
		var builder SetTreeBuilder
		plan, err := builder.BuildChain(
			[]SetTreeLeafSpec{{0, 1}, {1, 1}, {2, 1}},
			[]SetTreeOperation{SetTreeExceptDistinct, SetTreeUnionDistinct},
		)
		if err != nil {
			t.Fatal(err)
		}
		var executor SetTreeExecutor
		result, err := executor.Run(plan, &setTreeTestProgram{sources: sources}, SetTreeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		want := [][]string{{`1`}, {`2`}}
		if got := setTreeResultJSON(result); !reflect.DeepEqual(got, want) {
			t.Fatalf("A EXCEPT B UNION C = %v, want %v", got, want)
		}
	})
}

func TestSetTreeExplicitParenthesizedShapeOverridesPrecedence(t *testing.T) {
	// (A UNION B) INTERSECT C rather than A UNION (B INTERSECT C).
	plan := SetTreePlan{
		Nodes: []SetTreeNode{
			NewSetTreeLeaf(0, 1),
			NewSetTreeLeaf(1, 1),
			NewSetTreeBinary(SetTreeUnionDistinct, 0, 1),
			NewSetTreeLeaf(2, 1),
			NewSetTreeBinary(SetTreeIntersectDistinct, 2, 3),
		},
		Root: 4,
	}
	program := &setTreeTestProgram{sources: []SetTreeSource{
		setTreeTestSourceRows([]string{`1`}),
		setTreeTestSourceRows([]string{`2`}, []string{`3`}),
		setTreeTestSourceRows([]string{`3`}),
	}}
	var executor SetTreeExecutor
	result, err := executor.Run(plan, program, SetTreeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setTreeResultJSON(result), [][]string{{`3`}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("parenthesized result = %v, want %v", got, want)
	}
}

func TestSetTreeAllSixModesAndStableOrder(t *testing.T) {
	left := setTreeTestSourceRows(
		[]string{`1`}, []string{`1.0`}, []string{`2`},
		[]string{`null`}, []string{`<missing>`},
		[]string{`{"x":1}`}, []string{`{"x":1}`},
	)
	right := setTreeTestSourceRows(
		[]string{`1e0`}, []string{`3`}, []string{`null`}, []string{`{"x":1}`},
	)
	tests := []struct {
		operation SetTreeOperation
		want      [][]string
	}{
		{SetTreeUnionAll, [][]string{
			{`1`}, {`1.0`}, {`2`}, {`null`}, {`<missing>`}, {`{"x":1}`}, {`{"x":1}`},
			{`1e0`}, {`3`}, {`null`}, {`{"x":1}`},
		}},
		{SetTreeUnionDistinct, [][]string{{`1`}, {`2`}, {`null`}, {`{"x":1}`}, {`3`}}},
		{SetTreeIntersectAll, [][]string{{`1`}, {`null`}, {`{"x":1}`}}},
		{SetTreeIntersectDistinct, [][]string{{`1`}, {`null`}, {`{"x":1}`}}},
		{SetTreeExceptAll, [][]string{{`1.0`}, {`2`}, {`<missing>`}, {`{"x":1}`}}},
		{SetTreeExceptDistinct, [][]string{{`2`}}},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("mode=%d", test.operation), func(t *testing.T) {
			plan := SetTreePlan{Nodes: []SetTreeNode{
				NewSetTreeLeaf(0, 1), NewSetTreeLeaf(1, 1),
				NewSetTreeBinary(test.operation, 0, 1),
			}, Root: 2}
			var executor SetTreeExecutor
			result, err := executor.Run(plan, &setTreeTestProgram{
				sources: []SetTreeSource{left, right},
			}, SetTreeOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if got := setTreeResultJSON(result); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("result = %v, want %v", got, test.want)
			}
		})
	}
}

func TestSetTreeZeroColumnRowsPreserveCardinality(t *testing.T) {
	tests := []struct {
		operation   SetTreeOperation
		left, right int
		want        int
	}{
		{SetTreeUnionAll, 3, 2, 5},
		{SetTreeUnionDistinct, 3, 2, 1},
		{SetTreeIntersectAll, 3, 2, 2},
		{SetTreeIntersectDistinct, 3, 2, 1},
		{SetTreeExceptAll, 3, 2, 1},
		{SetTreeExceptDistinct, 3, 2, 0},
	}
	for _, test := range tests {
		leftRows := make([][]Cell, test.left)
		rightRows := make([][]Cell, test.right)
		program := &setTreeTestProgram{sources: []SetTreeSource{
			&setTreeTestSource{rows: leftRows},
			&setTreeTestSource{rows: rightRows},
		}}
		plan := SetTreePlan{Nodes: []SetTreeNode{
			NewSetTreeLeaf(0, 0), NewSetTreeLeaf(1, 0),
			NewSetTreeBinary(test.operation, 0, 1),
		}, Root: 2}
		var executor SetTreeExecutor
		result, err := executor.Run(plan, program, SetTreeOptions{})
		if err != nil {
			t.Fatalf("mode %d: %v", test.operation, err)
		}
		if result.Rows() != test.want || result.Columns() != 0 {
			t.Fatalf("mode %d zero-column result = %dx%d, want %dx0",
				test.operation, result.Rows(), result.Columns(), test.want)
		}
	}
}

func TestSetTreeArbitraryChainReusesAndReleasesIntermediateSlots(t *testing.T) {
	const leaves = 64
	specs := make([]SetTreeLeafSpec, leaves)
	operations := make([]SetTreeOperation, leaves-1)
	sources := make([]SetTreeSource, leaves)
	for index := range leaves {
		specs[index] = SetTreeLeafSpec{Source: index, Columns: 1}
		sources[index] = setTreeTestSourceRows([]string{fmt.Sprintf("%d", index)})
		if index != leaves-1 {
			operations[index] = SetTreeUnionAll
		}
	}
	var builder SetTreeBuilder
	plan, err := builder.BuildChain(specs, operations)
	if err != nil {
		t.Fatal(err)
	}
	var executor SetTreeExecutor
	result, err := executor.Run(plan, &setTreeTestProgram{sources: sources}, SetTreeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows() != leaves {
		t.Fatalf("chain rows = %d, want %d", result.Rows(), leaves)
	}
	for row := range leaves {
		value, ok := result.Cell(row, 0).Int64()
		if !ok || value != int64(row) {
			t.Fatalf("row %d = %d/%v", row, value, ok)
		}
	}
	if len(executor.slots) != 3 {
		t.Fatalf("%d-node left-associative tree retained %d live slots, want 3",
			len(plan.Nodes), len(executor.slots))
	}
	active := 0
	for slot := range executor.slots {
		if executor.slots[slot].active {
			active++
		}
	}
	if active != 1 {
		t.Fatalf("active slots after root publication = %d, want 1", active)
	}
	rootCharge := executor.slots[executor.rootSlot].charge
	if executor.frame.intermediate.used != rootCharge {
		t.Fatalf("retained bytes = %d, want root-only %d",
			executor.frame.intermediate.used, rootCharge)
	}
}

func TestSetTreeOrdinalArityPropagationRejectsBeforeLeafExecution(t *testing.T) {
	plan := SetTreePlan{Nodes: []SetTreeNode{
		NewSetTreeLeaf(0, 1), NewSetTreeLeaf(1, 2),
		NewSetTreeBinary(SetTreeUnionAll, 0, 1),
	}, Root: 2}
	program := &setTreeTestProgram{}
	var executor SetTreeExecutor
	result, err := executor.Run(plan, program, SetTreeOptions{})
	if !errors.Is(err, ErrSetTreeArity) || result.Rows() != 0 || program.calls != 0 {
		t.Fatalf("static arity = rows %d calls %d err %v", result.Rows(), program.calls, err)
	}
	var arity *SetTreeArityError
	if !errors.As(err, &arity) || arity.Node != 2 || arity.Left != 1 || arity.Right != 2 {
		t.Fatalf("typed arity = %#v", arity)
	}

	plan = SetTreePlan{Nodes: []SetTreeNode{NewSetTreeLeaf(0, 1)}, Root: 0}
	program.sources = []SetTreeSource{setTreeTestSourceRows([]string{`1`, `2`})}
	result, err = executor.Run(plan, program, SetTreeOptions{})
	if !errors.Is(err, ErrSetTreeArity) || result.Rows() != 0 {
		t.Fatalf("runtime arity = rows %d err %v", result.Rows(), err)
	}
}

func TestSetTreeTotalLimitsAreTypedAndPublishNothing(t *testing.T) {
	basePlan := SetTreePlan{Nodes: []SetTreeNode{
		NewSetTreeLeaf(0, 1), NewSetTreeLeaf(1, 1),
		NewSetTreeBinary(SetTreeUnionAll, 0, 1),
	}, Root: 2}
	program := &setTreeTestProgram{sources: []SetTreeSource{
		setTreeTestSourceRows([]string{`1`}), setTreeTestSourceRows([]string{`2`}),
	}}
	tests := []struct {
		name    string
		plan    SetTreePlan
		options SetTreeOptions
		is      error
	}{
		{name: "rows", plan: basePlan, options: SetTreeOptions{MaxRows: 3}, is: ErrSetTreeRows},
		{name: "bytes", plan: basePlan, options: SetTreeOptions{MaxBytes: 1}, is: ErrSetTreeBytes},
		{name: "nodes", plan: basePlan, options: SetTreeOptions{MaxNodes: 2}, is: ErrSetTreeNodes},
		{name: "depth", plan: basePlan, options: SetTreeOptions{MaxDepth: 1}, is: ErrSetTreeDepth},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var executor SetTreeExecutor
			result, err := executor.Run(test.plan, program, test.options)
			if !errors.Is(err, test.is) {
				t.Fatalf("error = %v, want %v", err, test.is)
			}
			if result.Rows() != 0 || executor.rootSlot != -1 || executor.frame.intermediate.used != 0 {
				t.Fatalf("partial result root=%d rows=%d bytes=%d",
					executor.rootSlot, result.Rows(), executor.frame.intermediate.used)
			}
			for slot := range executor.slots {
				if executor.slots[slot].active {
					t.Fatalf("slot %d remained active", slot)
				}
			}
		})
	}

	var executor SetTreeExecutor
	result, err := executor.Run(basePlan, program, SetTreeOptions{MaxRows: 4})
	if err != nil || result.Rows() != 2 {
		t.Fatalf("exact total-row limit = rows %d err %v", result.Rows(), err)
	}
}

func TestSetTreeCancellationAndLeafFailureClearEarlierIntermediates(t *testing.T) {
	plan := SetTreePlan{Nodes: []SetTreeNode{
		NewSetTreeLeaf(0, 1), NewSetTreeLeaf(1, 1),
		NewSetTreeBinary(SetTreeUnionAll, 0, 1),
	}, Root: 2}
	first := setTreeTestSourceRows([]string{`1`})
	second := setTreeTestSourceRows([]string{`2`})
	var cancel CancelFlag
	second.onCell = func(row, column int) {
		if row == 0 && column == 0 {
			cancel.Cancel()
		}
	}
	program := &setTreeTestProgram{sources: []SetTreeSource{first, second}}
	var executor SetTreeExecutor
	result, err := executor.Run(plan, program, SetTreeOptions{Cancel: &cancel})
	if !errors.Is(err, ErrCanceled) || result.Rows() != 0 || executor.frame.intermediate.used != 0 {
		t.Fatalf("cancellation = rows %d bytes %d err %v",
			result.Rows(), executor.frame.intermediate.used, err)
	}

	cancel.Reset()
	second.onCell = nil
	leafErr := errors.New("leaf failed")
	program.errAt, program.err = 1, leafErr
	result, err = executor.Run(plan, program, SetTreeOptions{Cancel: &cancel})
	if !errors.Is(err, leafErr) || result.Rows() != 0 || executor.frame.intermediate.used != 0 {
		t.Fatalf("leaf failure = rows %d bytes %d err %v",
			result.Rows(), executor.frame.intermediate.used, err)
	}

	program.err = nil
	result, err = executor.Run(plan, program, SetTreeOptions{Cancel: &cancel})
	if err != nil || result.Rows() != 2 {
		t.Fatalf("reuse after failure = rows %d err %v", result.Rows(), err)
	}
}

func TestSetTreeExactIdentityAcrossArbitraryChain(t *testing.T) {
	sources := []SetTreeSource{
		setTreeTestSourceRows(
			[]string{`1`}, []string{`null`}, []string{`9007199254740992`},
			[]string{`9007199254740993`}, []string{`"a"`}, []string{`{"x":1}`},
		),
		setTreeTestSourceRows(
			[]string{`1.00`}, []string{`<missing>`}, []string{`"\u0061"`},
			[]string{`{ "x":1}`},
		),
		setTreeTestSourceRows(
			[]string{`10e-1`}, []string{`9007199254740992.0`}, []string{`2`},
		),
	}
	var builder SetTreeBuilder
	plan, err := builder.BuildChain(
		[]SetTreeLeafSpec{{0, 1}, {1, 1}, {2, 1}},
		[]SetTreeOperation{SetTreeUnionDistinct, SetTreeUnionDistinct},
	)
	if err != nil {
		t.Fatal(err)
	}
	var executor SetTreeExecutor
	result, err := executor.Run(plan, &setTreeTestProgram{sources: sources}, SetTreeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{`1`}, {`null`}, {`9007199254740992`}, {`9007199254740993`},
		{`"a"`}, {`{"x":1}`}, {`{ "x":1}`}, {`2`},
	}
	if got := setTreeResultJSON(result); !reflect.DeepEqual(got, want) {
		t.Fatalf("exact chain = %v, want %v", got, want)
	}
	if result.Missing(1, 0) {
		t.Fatal("first explicit NULL representative changed to missing")
	}
}

func TestSetTreeMalformedPlansAndConfiguration(t *testing.T) {
	validProgram := &setTreeTestProgram{sources: []SetTreeSource{
		setTreeTestSourceRows([]string{`1`}),
	}}
	tests := []SetTreePlan{
		{},
		{Nodes: []SetTreeNode{NewSetTreeLeaf(0, 1)}, Root: -1},
		{Nodes: []SetTreeNode{{Kind: SetTreeNodeKind(99)}}, Root: 0},
		{Nodes: []SetTreeNode{
			NewSetTreeLeaf(0, 1), NewSetTreeBinary(SetTreeUnionAll, 0, 0),
		}, Root: 1},
		{Nodes: []SetTreeNode{
			NewSetTreeLeaf(0, 1), NewSetTreeLeaf(0, 1), NewSetTreeLeaf(0, 1),
			NewSetTreeBinary(SetTreeUnionAll, 0, 1),
			NewSetTreeBinary(SetTreeUnionAll, 0, 2),
		}, Root: 4},
	}
	for index, plan := range tests {
		var executor SetTreeExecutor
		if _, err := executor.Run(plan, validProgram, SetTreeOptions{}); !errors.Is(err, ErrSetTreePlan) {
			t.Fatalf("plan %d error = %v, want invalid plan", index, err)
		}
	}

	validPlan := SetTreePlan{Nodes: []SetTreeNode{NewSetTreeLeaf(0, 1)}, Root: 0}
	for _, options := range []SetTreeOptions{
		{MaxRows: -2}, {MaxBytes: -2}, {MaxDepth: -2}, {MaxNodes: -2},
		{MaxRows: -1, MaxBytes: -1, MaxDepth: -1, MaxNodes: -1},
	} {
		var executor SetTreeExecutor
		if _, err := executor.Run(validPlan, validProgram, options); !errors.Is(err, ErrSetTreeConfig) {
			t.Fatalf("options %+v error = %v, want config", options, err)
		}
	}
	var executor SetTreeExecutor
	if _, err := executor.Run(validPlan, nil, SetTreeOptions{}); !errors.Is(err, ErrSetTreeProgram) {
		t.Fatalf("nil program error = %v", err)
	}
}

func TestSetTreeReleaseDropsStorageAndAllowsReuse(t *testing.T) {
	plan := SetTreePlan{Nodes: []SetTreeNode{
		NewSetTreeLeaf(0, 1), NewSetTreeLeaf(1, 1),
		NewSetTreeBinary(SetTreeUnionDistinct, 0, 1),
	}, Root: 2}
	program := &setTreeTestProgram{sources: []SetTreeSource{
		setTreeTestSourceRows([]string{`1`}, []string{`2`}),
		setTreeTestSourceRows([]string{`2`}, []string{`3`}),
	}}
	var executor SetTreeExecutor
	if _, err := executor.Run(plan, program, SetTreeOptions{}); err != nil {
		t.Fatal(err)
	}
	if cap(executor.slots) == 0 || cap(executor.nodeSlots) == 0 {
		t.Fatal("run did not retain tree storage")
	}
	executor.Release()
	if cap(executor.slots) != 0 || cap(executor.nodeSlots) != 0 ||
		cap(executor.free) != 0 || executor.frame.intermediate.used != 0 {
		t.Fatal("Release retained tree storage or charge")
	}
	result, err := executor.Run(plan, program, SetTreeOptions{})
	if err != nil || result.Rows() != 3 {
		t.Fatalf("post-Release run = rows %d err %v", result.Rows(), err)
	}
}

func TestSetTreeIndependentExecutorsRaceSafelyAndSameExecutorRejectsOverlap(t *testing.T) {
	plan := SetTreePlan{Nodes: []SetTreeNode{NewSetTreeLeaf(0, 1)}, Root: 0}
	entered := make(chan struct{})
	release := make(chan struct{})
	blocking := &setTreeBlockingProgram{
		source: setTreeTestSourceRows([]string{`1`}), entered: entered, release: release,
	}
	var executor SetTreeExecutor
	done := make(chan error, 1)
	go func() {
		_, err := executor.Run(plan, blocking, SetTreeOptions{})
		done <- err
	}()
	<-entered
	if result, err := executor.Run(plan, blocking, SetTreeOptions{}); !errors.Is(err, ErrSetTreeInUse) || result.Rows() != 0 {
		t.Fatalf("overlapping run = rows %d err %v", result.Rows(), err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	const workers = 16
	var wait sync.WaitGroup
	wait.Add(workers)
	errs := make(chan error, workers)
	for worker := range workers {
		worker := worker
		go func() {
			defer wait.Done()
			source := setTreeTestSourceRows([]string{fmt.Sprintf("%d", worker)})
			program := &setTreeTestProgram{sources: []SetTreeSource{source}}
			var local SetTreeExecutor
			result, err := local.Run(plan, program, SetTreeOptions{})
			if err == nil {
				value, ok := result.Cell(0, 0).Int64()
				if result.Rows() != 1 || !ok || value != int64(worker) {
					err = fmt.Errorf("worker %d got rows=%d value=%d ok=%v",
						worker, result.Rows(), value, ok)
				}
			}
			errs <- err
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

type setTreeBlockingProgram struct {
	source  SetTreeSource
	entered chan struct{}
	release chan struct{}
}

func (p *setTreeBlockingProgram) Leaf(_ int, _ *CancelFlag) (SetTreeSource, error) {
	close(p.entered)
	<-p.release
	return p.source, nil
}

func TestSetTreeRandomizedDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(0x7e7e))
	for trial := 0; trial < 300; trial++ {
		leafCount := 2 + rng.Intn(8)
		specs := make([]SetTreeLeafSpec, leafCount)
		operations := make([]SetTreeOperation, leafCount-1)
		sources := make([]SetTreeSource, leafCount)
		referenceLeaves := make([][]int, leafCount)
		for leaf := range leafCount {
			specs[leaf] = SetTreeLeafSpec{Source: leaf, Columns: 1}
			rows := rng.Intn(12)
			cells := make([][]Cell, rows)
			referenceLeaves[leaf] = make([]int, rows)
			for row := range rows {
				value := rng.Intn(7) - 3
				referenceLeaves[leaf][row] = value
				cells[row] = []Cell{{
					kind: TypeNumber, flag: cellInteger, word: uint64(int64(value)),
				}}
			}
			sources[leaf] = &setTreeTestSource{rows: cells, columns: 1}
			if leaf != leafCount-1 {
				operations[leaf] = SetTreeOperation(rng.Intn(6))
			}
		}
		var builder SetTreeBuilder
		plan, err := builder.BuildChain(specs, operations)
		if err != nil {
			t.Fatal(err)
		}
		want := referenceSetTree(plan, referenceLeaves)
		var executor SetTreeExecutor
		result, err := executor.Run(plan, &setTreeTestProgram{sources: sources}, SetTreeOptions{})
		if err != nil {
			t.Fatalf("trial %d: %v", trial, err)
		}
		got := make([]int, result.Rows())
		for row := range got {
			value, ok := result.Cell(row, 0).Int64()
			if !ok {
				t.Fatalf("trial %d row %d is not integer", trial, row)
			}
			got[row] = int(value)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("trial %d ops=%v got=%v want=%v", trial, operations, got, want)
		}
	}
}

func referenceSetTree(plan SetTreePlan, leaves [][]int) []int {
	results := make([][]int, len(plan.Nodes))
	for index, node := range plan.Nodes {
		if node.Kind == SetTreeLeafNode {
			results[index] = append([]int(nil), leaves[node.Source]...)
			continue
		}
		results[index] = referenceSetTreeBinary(
			node.Operation, results[node.Left], results[node.Right],
		)
	}
	return results[plan.Root]
}

func referenceSetTreeBinary(operation SetTreeOperation, left, right []int) []int {
	result := make([]int, 0, len(left)+len(right))
	switch operation {
	case SetTreeUnionAll:
		result = append(result, left...)
		result = append(result, right...)
	case SetTreeUnionDistinct:
		seen := make(map[int]bool)
		for _, side := range [][]int{left, right} {
			for _, value := range side {
				if !seen[value] {
					seen[value] = true
					result = append(result, value)
				}
			}
		}
	case SetTreeIntersectAll, SetTreeIntersectDistinct:
		counts := make(map[int]int)
		for _, value := range right {
			counts[value]++
		}
		emitted := make(map[int]bool)
		for _, value := range left {
			if operation == SetTreeIntersectDistinct {
				if counts[value] != 0 && !emitted[value] {
					emitted[value] = true
					result = append(result, value)
				}
				continue
			}
			if counts[value] != 0 {
				counts[value]--
				result = append(result, value)
			}
		}
	case SetTreeExceptAll, SetTreeExceptDistinct:
		counts := make(map[int]int)
		for _, value := range right {
			counts[value]++
		}
		emitted := make(map[int]bool)
		for _, value := range left {
			if operation == SetTreeExceptDistinct {
				if counts[value] == 0 && !emitted[value] {
					emitted[value] = true
					result = append(result, value)
				}
				continue
			}
			if counts[value] != 0 {
				counts[value]--
				continue
			}
			result = append(result, value)
		}
	}
	return result
}

func TestSetTreeResultCellsRemainBorrowedAndExact(t *testing.T) {
	plan := SetTreePlan{Nodes: []SetTreeNode{NewSetTreeLeaf(0, 2)}, Root: 0}
	program := &setTreeTestProgram{sources: []SetTreeSource{
		setTreeTestSourceRows([]string{`1.00`, `"a\n"`}),
	}}
	var executor SetTreeExecutor
	result, err := executor.Run(plan, program, SetTreeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.Cell(0, 0).JSON(), []byte(`1.00`)) ||
		!bytes.Equal(result.Cell(0, 1).JSON(), []byte(`"a\n"`)) {
		t.Fatalf("exact result cells = %q / %q",
			result.Cell(0, 0).JSON(), result.Cell(0, 1).JSON())
	}
}
