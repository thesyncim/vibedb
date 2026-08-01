package query

import (
	"errors"
	"reflect"
	"testing"
)

type setTreeFuzzProgram struct{ sources [4]SetTreeSource }

func newSetTreeFuzzProgram() *setTreeFuzzProgram {
	return &setTreeFuzzProgram{sources: [4]SetTreeSource{
		setTreeTestSourceRows(
			[]string{`1`}, []string{`1.0`}, []string{`null`}, []string{`<missing>`},
		),
		setTreeTestSourceRows([]string{`2`}, []string{`3`}, []string{`2`}),
		&setTreeTestSource{rows: [][]Cell{{}, {}}, columns: 0},
		setTreeTestSourceRows([]string{`1`, `"a"`}, []string{`2`, `"b"`}),
	}}
}

func (p *setTreeFuzzProgram) Leaf(source int, _ *CancelFlag) (SetTreeSource, error) {
	if source < 0 || source >= len(p.sources) {
		return nil, ErrSetTreeSource
	}
	return p.sources[source], nil
}

type setTreeFuzzOutcome struct {
	errorClass int
	message    string
	rows       [][]string
	columns    int
}

func executeSetTreeFuzzPlan(
	t *testing.T,
	executor *SetTreeExecutor,
	plan SetTreePlan,
	program SetTreeProgram,
) setTreeFuzzOutcome {
	t.Helper()
	result, err := executor.Run(plan, program, SetTreeOptions{
		MaxRows: 10_000, MaxBytes: 4 << 20, MaxDepth: 64, MaxNodes: 64,
	})
	if err != nil {
		class := classifySetTreeFuzzError(err)
		if class == 0 {
			t.Fatalf("untyped set-tree rejection: %T: %v", err, err)
		}
		return setTreeFuzzOutcome{errorClass: class, message: err.Error()}
	}
	return setTreeFuzzOutcome{
		rows: setTreeResultJSON(result), columns: result.Columns(),
	}
}

func classifySetTreeFuzzError(err error) int {
	known := [...]error{
		ErrSetTreeConfig,
		ErrSetTreePlan,
		ErrSetTreeProgram,
		ErrSetTreeSource,
		ErrSetTreeInUse,
		ErrSetTreeArity,
		ErrSetTreeRows,
		ErrSetTreeBytes,
		ErrSetTreeDepth,
		ErrSetTreeNodes,
		ErrSetTreeSize,
		ErrCanceled,
	}
	for index, target := range known {
		if errors.Is(err, target) {
			return index + 1
		}
	}
	return 0
}

func FuzzSetTreeTopologicalPlans(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1, 0, 0, 1, 0, 0, 0, 0x80})
	f.Add([]byte{
		3,
		0, 0, 1, 0, 0, 0,
		0, 1, 1, 0, 0, 0,
		1, 0, 0, 0x80, 0x81, 1,
		0x80,
	})
	f.Fuzz(func(t *testing.T, data []byte) {
		plan := decodeSetTreeFuzzPlan(data)
		program := newSetTreeFuzzProgram()
		var executor SetTreeExecutor
		first := executeSetTreeFuzzPlan(t, &executor, plan, program)
		second := executeSetTreeFuzzPlan(t, &executor, plan, program)
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("nondeterministic plan outcome:\nfirst=%#v\nsecond=%#v", first, second)
		}
	})
}

func decodeSetTreeFuzzPlan(data []byte) SetTreePlan {
	if len(data) == 0 {
		return SetTreePlan{}
	}
	nodeCount := int(data[0] % 24)
	nodes := make([]SetTreeNode, nodeCount)
	at := 1
	for node := range nodes {
		kindByte := setTreeFuzzByte(data, at)
		sourceByte := setTreeFuzzByte(data, at+1)
		columnsByte := setTreeFuzzByte(data, at+2)
		leftByte := setTreeFuzzByte(data, at+3)
		rightByte := setTreeFuzzByte(data, at+4)
		operationByte := setTreeFuzzByte(data, at+5)
		at += 6

		kind := SetTreeNodeKind(kindByte % 4)
		left, right := int(int8(leftByte)), int(int8(rightByte))
		if node != 0 && leftByte&0x80 != 0 {
			left = int(leftByte&0x7f) % node
		}
		if node != 0 && rightByte&0x80 != 0 {
			right = int(rightByte&0x7f) % node
		}
		nodes[node] = SetTreeNode{
			Kind:      kind,
			Source:    int(int8(sourceByte)),
			Columns:   int(int8(columnsByte)),
			Left:      left,
			Right:     right,
			Operation: SetTreeOperation(operationByte % 9),
		}
	}
	rootByte := setTreeFuzzByte(data, at)
	root := int(int8(rootByte))
	if nodeCount != 0 && rootByte&0x80 != 0 {
		root = nodeCount - 1
	}
	return SetTreePlan{Nodes: nodes, Root: root}
}

func FuzzSetTreeBuilderChains(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1, 0, 0, 1})
	f.Add([]byte{3, 2, 0, 1, 1, 1, 2, 1, 0, 2})
	f.Fuzz(func(t *testing.T, data []byte) {
		leaves, operations := decodeSetTreeBuilderFuzzInput(data)
		var builder SetTreeBuilder
		first, firstErr := builder.BuildChain(leaves, operations)
		firstNodes := append([]SetTreeNode(nil), first.Nodes...)
		firstRoot := first.Root
		second, secondErr := builder.BuildChain(leaves, operations)

		if (firstErr == nil) != (secondErr == nil) {
			t.Fatalf("builder success changed: %v / %v", firstErr, secondErr)
		}
		if firstErr != nil {
			if !errors.Is(firstErr, ErrSetTreePlan) || !errors.Is(secondErr, ErrSetTreePlan) {
				t.Fatalf("untyped builder errors: %T %v / %T %v",
					firstErr, firstErr, secondErr, secondErr)
			}
			if firstErr.Error() != secondErr.Error() {
				t.Fatalf("nondeterministic builder errors: %q / %q",
					firstErr, secondErr)
			}
			return
		}
		if firstRoot != second.Root || !reflect.DeepEqual(firstNodes, second.Nodes) {
			t.Fatalf("nondeterministic builder plans:\n%v/%d\n%v/%d",
				firstNodes, firstRoot, second.Nodes, second.Root)
		}

		program := newSetTreeFuzzProgram()
		var executor SetTreeExecutor
		plan := SetTreePlan{Nodes: second.Nodes, Root: second.Root}
		firstOutcome := executeSetTreeFuzzPlan(t, &executor, plan, program)
		secondOutcome := executeSetTreeFuzzPlan(t, &executor, plan, program)
		if !reflect.DeepEqual(firstOutcome, secondOutcome) {
			t.Fatalf("nondeterministic built-plan outcome:\n%#v\n%#v",
				firstOutcome, secondOutcome)
		}
	})
}

func decodeSetTreeBuilderFuzzInput(data []byte) ([]SetTreeLeafSpec, []SetTreeOperation) {
	if len(data) == 0 {
		return nil, nil
	}
	leaves := int(data[0] % 24)
	operationCount := int(setTreeFuzzByte(data, 1) % 24)
	specs := make([]SetTreeLeafSpec, leaves)
	at := 2
	for leaf := range specs {
		specs[leaf] = SetTreeLeafSpec{
			Source:  int(int8(setTreeFuzzByte(data, at))),
			Columns: int(int8(setTreeFuzzByte(data, at+1))),
		}
		at += 2
	}
	operations := make([]SetTreeOperation, operationCount)
	for operation := range operations {
		operations[operation] = SetTreeOperation(setTreeFuzzByte(data, at+operation) % 9)
	}
	return specs, operations
}

func setTreeFuzzByte(data []byte, at int) byte {
	if len(data) == 0 {
		return 0
	}
	return data[at%len(data)]
}
