package query

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

var recursiveGraphOutputColumns = []string{"node"}

type recursiveGraphAnchorPhysicalTerm struct {
	start Cell
}

func (t *recursiveGraphAnchorPhysicalTerm) RecursiveCTEColumns() []string {
	return recursiveGraphOutputColumns
}

func (t *recursiveGraphAnchorPhysicalTerm) RunRecursiveCTETerm(
	exec *Exec,
	input RecursiveCTETermInput,
) error {
	if input.Delta.Valid() || input.Iteration != 0 {
		return fmt.Errorf("anchor received a recursive delta")
	}
	if err := prepareRecursiveCTETestResult(exec, recursiveGraphOutputColumns, 1); err != nil {
		return err
	}
	if err := exec.Result.admitResultCell(t.start); err != nil {
		return err
	}
	exec.Result.Columns[0].Cells[0] = t.start
	return cancellationError(exec.Options.Cancel)
}

type recursiveGraphStepPhysicalTerm struct{}

func (*recursiveGraphStepPhysicalTerm) RecursiveCTEColumns() []string {
	return recursiveGraphOutputColumns
}

func (*recursiveGraphStepPhysicalTerm) RunRecursiveCTETerm(
	exec *Exec,
	input RecursiveCTETermInput,
) error {
	if !input.Delta.Valid() || input.Base.kind != sourceRelationSpool {
		return fmt.Errorf("recursive graph step needs base and delta relations")
	}
	base := (*relationSpool)(input.Base.payload)
	if base == nil || len(base.columns) != 2 {
		return fmt.Errorf("recursive graph base has invalid shape")
	}
	rows := 0
	for edge := 0; edge < base.rows; edge++ {
		if err := cancellationCheckpoint(exec.Options.Cancel, edge); err != nil {
			return err
		}
		from := cellFromScalar(base.columns[0][edge])
		for delta := 0; delta < input.Delta.Rows(); delta++ {
			if compareScalar(
				recursiveCTEScalarFromCell(from),
				recursiveCTEScalarFromCell(input.Delta.Cell(delta, 0)),
			) == 0 {
				rows++
				break
			}
		}
	}
	if err := prepareRecursiveCTETestResult(
		exec, recursiveGraphOutputColumns, rows,
	); err != nil {
		return err
	}
	write := 0
	for edge := 0; edge < base.rows; edge++ {
		if err := cancellationCheckpoint(exec.Options.Cancel, edge); err != nil {
			return err
		}
		from := cellFromScalar(base.columns[0][edge])
		matched := false
		for delta := 0; delta < input.Delta.Rows(); delta++ {
			if compareScalar(
				recursiveCTEScalarFromCell(from),
				recursiveCTEScalarFromCell(input.Delta.Cell(delta, 0)),
			) == 0 {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		to := cellFromScalar(base.columns[1][edge])
		if err := exec.Result.admitResultCell(to); err != nil {
			return err
		}
		exec.Result.Columns[0].Cells[write] = to
		write++
	}
	return cancellationError(exec.Options.Cancel)
}

func recursiveGraphIntegerCell(value int) Cell {
	raw := strconv.AppendInt(nil, int64(value), 10)
	return Cell{
		kind: TypeNumber, flag: cellInteger, word: uint64(value), raw: raw,
	}
}

func buildRecursiveGraphRelation(
	tb testing.TB,
	edges [][2]int,
) relationSpool {
	tb.Helper()
	rows := make([][]string, len(edges))
	for row, edge := range edges {
		rows[row] = []string{strconv.Itoa(edge[0]), strconv.Itoa(edge[1])}
	}
	if len(rows) != 0 {
		return buildRelationSpoolForTest(tb, rows)
	}
	var empty relationSpool
	if err := empty.begin(0, 2, 0); err != nil {
		tb.Fatal(err)
	}
	return empty
}

func recursiveGraphOracle(start int, edges [][2]int) []int {
	seen := map[int]bool{start: true}
	result := []int{start}
	delta := []int{start}
	for len(delta) != 0 {
		next := make([]int, 0)
		for _, edge := range edges {
			fromDelta := false
			for _, node := range delta {
				if edge[0] == node {
					fromDelta = true
					break
				}
			}
			if fromDelta && !seen[edge[1]] {
				seen[edge[1]] = true
				next = append(next, edge[1])
				result = append(result, edge[1])
			}
		}
		delta = next
	}
	return result
}

func assertRecursiveGraphDifferential(
	tb testing.TB,
	start int,
	edges [][2]int,
) {
	tb.Helper()
	base := buildRecursiveGraphRelation(tb, edges)
	defer base.release()
	anchor := &recursiveGraphAnchorPhysicalTerm{start: recursiveGraphIntegerCell(start)}
	recursive := &recursiveGraphStepPhysicalTerm{}
	descriptor, err := PrepareRecursiveCTEDescriptor(
		"graph", nil, anchor, recursive, RecursiveUnionDistinct,
		RecursiveCTEReferenceLocal,
		RecursiveCTELimits{MaxIterations: 64, MaxRows: 128, MaxBytes: -1},
	)
	if err != nil {
		tb.Fatal(err)
	}
	options := ExecOptions{IntermediateBytes: -1}
	frame := beginRecursiveCTEFrame(tb, options)
	var runtime RecursiveCTERuntime
	result, err := runtime.execute(
		descriptor, fromRelationSpool(&base), frame, options,
	)
	if err != nil {
		tb.Fatal(err)
	}
	want := recursiveGraphOracle(start, edges)
	if result.Rows() != len(want) {
		tb.Fatalf("recursive graph rows = %d, want %d for %v",
			result.Rows(), len(want), edges)
	}
	for row, expected := range want {
		got, ok := result.Cell(row, 0).Int64()
		if !ok || got != int64(expected) {
			tb.Fatalf("recursive graph row %d = %d/%v, want %d for %v",
				row, got, ok, expected, edges)
		}
	}
	runtime.releaseExecution(frame)
	if frame.intermediate.used != 0 {
		tb.Fatalf("recursive graph retained %d statement bytes", frame.intermediate.used)
	}
}

func TestRecursiveCTEGraphRandomizedDifferential(t *testing.T) {
	random := rand.New(rand.NewSource(0x5eed))
	for trial := 0; trial < 200; trial++ {
		nodes := 1 + random.Intn(12)
		edges := make([][2]int, random.Intn(48))
		for edge := range edges {
			edges[edge] = [2]int{random.Intn(nodes), random.Intn(nodes)}
		}
		assertRecursiveGraphDifferential(t, random.Intn(nodes), edges)
	}
}

func FuzzRecursiveCTEGraphDifferential(f *testing.F) {
	f.Add(byte(0), []byte{0, 1, 1, 2, 2, 3, 3, 0})
	f.Add(byte(3), []byte{3, 1, 3, 2, 1, 4, 2, 4})
	f.Add(byte(0), []byte{})
	f.Fuzz(func(t *testing.T, startByte byte, data []byte) {
		const nodes = 8
		if len(data) > 64 {
			data = data[:64]
		}
		edges := make([][2]int, len(data)/2)
		for edge := range edges {
			edges[edge] = [2]int{
				int(data[edge*2]) % nodes,
				int(data[edge*2+1]) % nodes,
			}
		}
		assertRecursiveGraphDifferential(t, int(startByte)%nodes, edges)
	})
}
