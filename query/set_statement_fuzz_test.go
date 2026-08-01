package query

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

type setStatementFuzzToken struct {
	json  string
	class byte
}

// The oracle knows only this finite value domain. class is SQL set identity:
// exact-decimal numeric aliases, decoded string aliases, and NULL/missing
// collapse, while containers retain exact JSON identity.
var setStatementFuzzTokens = [...]setStatementFuzzToken{
	{`null`, 0}, {`<missing>`, 0},
	{`0`, 1}, {`-0`, 1}, {`0.00e4`, 1},
	{`1`, 2}, {`1.0`, 2}, {`1e0`, 2}, {`-1`, 3},
	{`9007199254740992`, 4}, {`9007199254740992.0`, 4},
	{`9007199254740993`, 5},
	{`"a"`, 6}, {`"\u0061"`, 6}, {`"a\n"`, 7},
	{`true`, 8}, {`false`, 9},
	{`{"x":1}`, 10}, {`{ "x":1 }`, 11}, {`[1,2]`, 12},
	{`"é"`, 13}, {`"\u00e9"`, 13},
}

type setStatementFuzzFault uint8

const (
	setStatementFuzzHealthy setStatementFuzzFault = iota
	setStatementFuzzError
	setStatementFuzzCancel
	setStatementFuzzExtraColumn
	setStatementFuzzFewerColumns
	setStatementFuzzShortColumn
	setStatementFuzzNegativeRows
	setStatementFuzzHugeRows
	setStatementFuzzNegativeBytes
	setStatementFuzzDetachedCursor
)

var errSetStatementFuzzLeaf = errors.New("set statement fuzz leaf failure")

type setStatementFuzzRunner struct {
	names     []string
	rows      [][]Cell
	tokens    [][]byte
	statement Statement
	fault     setStatementFuzzFault
	cancel    *CancelFlag
}

func newSetStatementFuzzRunner(names []string, rows [][]byte) *setStatementFuzzRunner {
	runner := &setStatementFuzzRunner{
		names:  append([]string(nil), names...),
		tokens: rows,
		rows:   make([][]Cell, len(rows)),
	}
	runner.statement.outputs = len(names)
	for row := range rows {
		runner.rows[row] = make([]Cell, len(rows[row]))
		for column, token := range rows[row] {
			runner.rows[row][column] = setTreeTestJSONCell(
				setStatementFuzzTokens[token].json,
			)
		}
	}
	return runner
}

func (r *setStatementFuzzRunner) Columns() []string { return r.names }
func (r *setStatementFuzzRunner) NumParams() int    { return 1 }
func (r *setStatementFuzzRunner) Collection() string {
	return "docs"
}
func (r *setStatementFuzzRunner) AppendSchema(dst []OutputColumn) []OutputColumn {
	for column, name := range r.names {
		dst = append(dst, OutputColumn{Header: name, Ordinal: uint32(column)})
	}
	return dst
}
func (r *setStatementFuzzRunner) releaseRelations(*statementFrame) {}

func (r *setStatementFuzzRunner) selected(args []any) (start, rows int, err error) {
	if len(args) != 1 {
		return 0, 0, fmt.Errorf("fuzz runner received %d arguments", len(args))
	}
	value, ok := args[0].(int64)
	if !ok {
		return 0, 0, fmt.Errorf("fuzz runner received argument %T", args[0])
	}
	if len(r.rows) == 0 {
		return 0, 0, nil
	}
	u := uint64(value)
	rows = 1 + int(u%uint64(len(r.rows)))
	start = int((u / uint64(len(r.rows))) % uint64(len(r.rows)))
	return start, rows, nil
}

func (r *setStatementFuzzRunner) runIntoFrame(
	exec *Exec,
	_ Source,
	args []any,
	_ *statementFrame,
	_ string,
) (Cursor, error) {
	if r.fault == setStatementFuzzError {
		return Cursor{}, errSetStatementFuzzLeaf
	}
	start, rows, err := r.selected(args)
	if err != nil {
		return Cursor{}, err
	}
	rowLimit, byteLimit, err := normalizeResultBudget(exec.Options)
	if err != nil {
		return Cursor{}, err
	}
	result := &exec.Result
	result.beginResultBudget(rowLimit, byteLimit)
	columns := len(r.names)
	if err := result.admitResultShape(columns, rows); err != nil {
		return Cursor{}, err
	}
	if cap(result.Columns) < columns {
		result.Columns = make([]ResultColumn, columns)
	} else {
		for column := columns; column < len(result.Columns); column++ {
			clear(result.Columns[column].Cells)
			result.Columns[column] = ResultColumn{}
		}
		result.Columns = result.Columns[:columns]
	}
	for column := range result.Columns {
		cells := result.Columns[column].Cells
		if rows < len(cells) {
			clear(cells[rows:])
		}
		result.Columns[column].Header = r.names[column]
		result.Columns[column].Cells = resize(cells, rows)
	}
	for row := 0; row < rows; row++ {
		sourceRow := (start + row) % len(r.rows)
		for column := 0; column < columns; column++ {
			cell := r.rows[sourceRow][column]
			if err := result.admitResultCell(cell); err != nil {
				return Cursor{}, err
			}
			result.Columns[column].Cells[row] = cell
		}
	}
	result.RowCount = rows
	cursor := r.statement.cursor(result)

	switch r.fault {
	case setStatementFuzzCancel:
		if r.cancel != nil {
			r.cancel.Cancel()
		}
	case setStatementFuzzExtraColumn:
		result.Columns = resize(result.Columns, columns+1)
		result.Columns[columns].Header = "unexpected"
		result.Columns[columns].Cells = resize(result.Columns[columns].Cells, rows)
		for row := range result.Columns[columns].Cells {
			result.Columns[columns].Cells[row] = nullCell()
		}
	case setStatementFuzzFewerColumns:
		result.Columns = result.Columns[:columns-1]
	case setStatementFuzzShortColumn:
		if rows == 0 {
			result.RowCount = 1
		} else {
			result.Columns[0].Cells = result.Columns[0].Cells[:rows-1]
		}
	case setStatementFuzzNegativeRows:
		result.RowCount = -1
	case setStatementFuzzHugeRows:
		result.RowCount = math.MaxInt
	case setStatementFuzzNegativeBytes:
		result.resultBytesUsed = -1
	case setStatementFuzzDetachedCursor:
		cursor = Cursor{}
	}
	return cursor, nil
}

type setStatementFuzzBytes struct {
	data  []byte
	at    int
	state uint64
}

func newSetStatementFuzzBytes(data []byte) setStatementFuzzBytes {
	state := uint64(0x9e3779b97f4a7c15) ^ uint64(len(data))
	for _, value := range data {
		state ^= uint64(value) + 0x9e3779b97f4a7c15 + state<<6 + state>>2
	}
	return setStatementFuzzBytes{data: data, state: state}
}

func (r *setStatementFuzzBytes) next() byte {
	r.state ^= r.state << 13
	r.state ^= r.state >> 7
	r.state ^= r.state << 17
	value := byte(r.state >> 29)
	if len(r.data) != 0 {
		value ^= r.data[r.at%len(r.data)]
		r.at++
	}
	return value
}

type setStatementFuzzScenario struct {
	plan    SetTreePlan
	desc    *setStatementDescriptor
	runners []*setStatementFuzzRunner
	args    []any
	source  Source
}

func decodeSetStatementFuzzScenario(data []byte) (*setStatementFuzzScenario, error) {
	random := newSetStatementFuzzBytes(data)
	leaves := 2 + int(random.next()%5)
	columns := 1 + int(random.next()%3)
	firstNames := make([]string, columns)
	for column := range firstNames {
		if column < 2 {
			firstNames[column] = "duplicate"
		} else {
			firstNames[column] = fmt.Sprintf("column_%d", column)
		}
	}

	runners := make([]*setStatementFuzzRunner, leaves)
	bindings := make([]setStatementLeaf, leaves)
	args := make([]any, leaves)
	bases := make([]int, leaves)
	for index := range bases {
		bases[index] = index
		args[index] = int64(index*257) + int64(int8(random.next()))
	}
	for index := leaves - 1; index > 0; index-- {
		other := int(random.next()) % (index + 1)
		bases[index], bases[other] = bases[other], bases[index]
	}
	for leaf := range runners {
		rowCount := int(random.next() % 6)
		rows := make([][]byte, rowCount)
		for row := range rows {
			rows[row] = make([]byte, columns)
			for column := range rows[row] {
				rows[row][column] = random.next() % byte(len(setStatementFuzzTokens))
			}
		}
		names := firstNames
		if leaf != 0 {
			names = make([]string, columns)
			for column := range names {
				names[column] = fmt.Sprintf("leaf_%d_column_%d", leaf, column)
			}
		}
		runners[leaf] = newSetStatementFuzzRunner(names, rows)
		bindings[leaf] = setStatementLeaf{runner: runners[leaf], paramBase: bases[leaf]}
	}

	nodes := make([]SetTreeNode, leaves, leaves*2-1)
	active := make([]int, leaves)
	for leaf := range leaves {
		nodes[leaf] = NewSetTreeLeaf(leaf, columns)
		active[leaf] = leaf
	}
	for len(active) > 1 {
		leftAt := int(random.next()) % len(active)
		left := active[leftAt]
		active = append(active[:leftAt], active[leftAt+1:]...)
		rightAt := int(random.next()) % len(active)
		right := active[rightAt]
		active = append(active[:rightAt], active[rightAt+1:]...)
		nodes = append(nodes, NewSetTreeBinary(
			SetTreeOperation(random.next()%6), left, right,
		))
		active = append(active, len(nodes)-1)
	}
	plan := SetTreePlan{Nodes: nodes, Root: len(nodes) - 1}
	desc, err := prepareSetStatementDescriptor(plan, bindings, 0, len(args))
	if err != nil {
		return nil, err
	}
	return &setStatementFuzzScenario{
		plan: plan, desc: desc, runners: runners, args: args,
		source: FromSegment(&store.Segment{}),
	}, nil
}

type setStatementFuzzRow []byte

func setStatementFuzzSelection(
	runner *setStatementFuzzRunner,
	arg any,
) []setStatementFuzzRow {
	start, rows, err := runner.selected([]any{arg})
	if err != nil {
		panic(err)
	}
	selected := make([]setStatementFuzzRow, rows)
	for row := range selected {
		selected[row] = runner.tokens[(start+row)%len(runner.tokens)]
	}
	return selected
}

func setStatementFuzzOracle(
	scenario *setStatementFuzzScenario,
	args []any,
) []setStatementFuzzRow {
	results := make([][]setStatementFuzzRow, len(scenario.plan.Nodes))
	for index, node := range scenario.plan.Nodes {
		if node.Kind == SetTreeLeafNode {
			leaf := scenario.desc.leaves[node.Source]
			results[index] = setStatementFuzzSelection(
				scenario.runners[node.Source], args[leaf.paramBase],
			)
			continue
		}
		results[index] = setStatementFuzzBinary(
			node.Operation, results[node.Left], results[node.Right],
		)
	}
	return results[scenario.plan.Root]
}

func setStatementFuzzRowKey(row setStatementFuzzRow) string {
	key := make([]byte, len(row))
	for column, token := range row {
		key[column] = setStatementFuzzTokens[token].class
	}
	return string(key)
}

func setStatementFuzzBinary(
	operation SetTreeOperation,
	left, right []setStatementFuzzRow,
) []setStatementFuzzRow {
	result := make([]setStatementFuzzRow, 0, len(left)+len(right))
	switch operation {
	case SetTreeUnionAll:
		result = append(result, left...)
		result = append(result, right...)
	case SetTreeUnionDistinct:
		seen := make(map[string]bool)
		for _, side := range [][]setStatementFuzzRow{left, right} {
			for _, row := range side {
				key := setStatementFuzzRowKey(row)
				if !seen[key] {
					seen[key] = true
					result = append(result, row)
				}
			}
		}
	case SetTreeIntersectAll, SetTreeIntersectDistinct:
		counts := make(map[string]int)
		for _, row := range right {
			counts[setStatementFuzzRowKey(row)]++
		}
		emitted := make(map[string]bool)
		for _, row := range left {
			key := setStatementFuzzRowKey(row)
			if operation == SetTreeIntersectDistinct {
				if counts[key] != 0 && !emitted[key] {
					emitted[key] = true
					result = append(result, row)
				}
				continue
			}
			if counts[key] != 0 {
				counts[key]--
				result = append(result, row)
			}
		}
	case SetTreeExceptAll, SetTreeExceptDistinct:
		counts := make(map[string]int)
		for _, row := range right {
			counts[setStatementFuzzRowKey(row)]++
		}
		emitted := make(map[string]bool)
		for _, row := range left {
			key := setStatementFuzzRowKey(row)
			if operation == SetTreeExceptDistinct {
				if counts[key] == 0 && !emitted[key] {
					emitted[key] = true
					result = append(result, row)
				}
				continue
			}
			if counts[key] != 0 {
				counts[key]--
				continue
			}
			result = append(result, row)
		}
	}
	return result
}

func assertSetStatementFuzzResult(
	t *testing.T,
	result *Result,
	names []string,
	want []setStatementFuzzRow,
) {
	t.Helper()
	if result.RowCount != len(want) || len(result.Columns) != len(names) {
		t.Fatalf("result shape = %dx%d, want %dx%d",
			result.RowCount, len(result.Columns), len(want), len(names))
	}
	for column, name := range names {
		if result.Columns[column].Header != name {
			t.Fatalf("column %d header = %q, want %q",
				column, result.Columns[column].Header, name)
		}
	}
	for row := range want {
		for column, tokenIndex := range want[row] {
			cell := result.Columns[column].Cells[row]
			token := setStatementFuzzTokens[tokenIndex]
			if token.json == "<missing>" {
				if cell.kind != TypeNull || cell.flag&cellMissing == 0 {
					t.Fatalf("cell %d/%d = %q flags=%d, want missing",
						row, column, cell.JSON(), cell.flag)
				}
				continue
			}
			if cell.flag&cellMissing != 0 || !bytes.Equal(cell.JSON(), []byte(token.json)) {
				t.Fatalf("cell %d/%d = %q flags=%d, want %s",
					row, column, cell.JSON(), cell.flag, token.json)
			}
		}
	}
}

func assertSetStatementFuzzCleanup(
	t *testing.T,
	runtime *setStatementRuntime,
	parent *Exec,
	frame *statementFrame,
	prior int64,
	err error,
) {
	t.Helper()
	if frame.intermediate.used != prior {
		t.Fatalf("frame retained %d bytes, want prior reservation %d",
			frame.intermediate.used, prior)
	}
	if err != nil {
		if parent.Result.RowCount != 0 || parent.Result.resultBytesUsed != 0 {
			t.Fatalf("failure published rows=%d bytes=%d",
				parent.Result.RowCount, parent.Result.resultBytesUsed)
		}
		for column := range parent.Result.Columns {
			if len(parent.Result.Columns[column].Cells) != 0 {
				t.Fatalf("failure retained %d cells in parent column %d",
					len(parent.Result.Columns[column].Cells), column)
			}
		}
		if runtime.PeakIntermediateBytes() != 0 {
			t.Fatalf("failure retained peak %d", runtime.PeakIntermediateBytes())
		}
	}
	for leaf := range runtime.execs {
		if runtime.execs[leaf].Result.RowCount != 0 ||
			runtime.execs[leaf].Result.resultBytesUsed != 0 {
			t.Fatalf("leaf %d retained rows=%d bytes=%d", leaf,
				runtime.execs[leaf].Result.RowCount,
				runtime.execs[leaf].Result.resultBytesUsed)
		}
	}
	for slot := range runtime.tree.slots {
		if runtime.tree.slots[slot].active {
			t.Fatalf("set-tree slot %d remained active", slot)
		}
	}
	if runtime.parent != nil || runtime.args != nil || runtime.source.kind != sourceInvalid {
		t.Fatal("runtime retained borrowed execution state")
	}
	frame.intermediate.release(prior)
	if frame.intermediate.used != 0 {
		t.Fatalf("frame release left %d bytes", frame.intermediate.used)
	}
}

func runSetStatementFuzzAttempt(
	t *testing.T,
	runtime *setStatementRuntime,
	parent *Exec,
	scenario *setStatementFuzzScenario,
	args []any,
	options ExecOptions,
	prior int64,
) (Cursor, error) {
	t.Helper()
	parent.Options = options
	var frame statementFrame
	if err := frame.begin(options); err != nil {
		t.Fatal(err)
	}
	frame.args = args
	if err := frame.intermediate.reserve("fuzz prior relation", prior); err != nil {
		t.Fatal(err)
	}
	cursor, err := runtime.runIntoFrame(parent, scenario.source, args, &frame)
	assertSetStatementFuzzCleanup(t, runtime, parent, &frame, prior, err)
	return cursor, err
}

func runSetStatementFuzzSuccess(
	t *testing.T,
	runtime *setStatementRuntime,
	parent *Exec,
	scenario *setStatementFuzzScenario,
	prior int64,
) {
	t.Helper()
	_, err := runSetStatementFuzzAttempt(
		t, runtime, parent, scenario, scenario.args,
		ExecOptions{IntermediateBytes: -1, ResultRows: -1, ResultBytes: -1}, prior,
	)
	if err != nil {
		t.Fatalf("healthy prepared execution: %v", err)
	}
	assertSetStatementFuzzResult(
		t, &parent.Result, scenario.desc.Columns(),
		setStatementFuzzOracle(scenario, scenario.args),
	)
}

type setStatementFuzzAction uint8

const (
	setStatementFuzzActionSuccess setStatementFuzzAction = iota
	setStatementFuzzActionError
	setStatementFuzzActionCancel
	setStatementFuzzActionExtraColumn
	setStatementFuzzActionFewerColumns
	setStatementFuzzActionShortColumn
	setStatementFuzzActionNegativeRows
	setStatementFuzzActionHugeRows
	setStatementFuzzActionNegativeBytes
	setStatementFuzzActionDetachedCursor
	setStatementFuzzActionResultBudget
	setStatementFuzzActionIntermediateBudget
	setStatementFuzzActionWrongArguments
	setStatementFuzzActionCount
)

func runSetStatementFuzzAction(
	t *testing.T,
	runtime *setStatementRuntime,
	parent *Exec,
	scenario *setStatementFuzzScenario,
	action setStatementFuzzAction,
	random *setStatementFuzzBytes,
) {
	t.Helper()
	argument := int(random.next()) % len(scenario.args)
	scenario.args[argument] = int64(argument*911) + int64(int8(random.next()))
	if action == setStatementFuzzActionSuccess {
		runSetStatementFuzzSuccess(t, runtime, parent, scenario, 1)
		return
	}
	target := int(random.next()) % len(scenario.runners)
	runner := scenario.runners[target]
	prior := int64(1 + random.next()%7)
	options := ExecOptions{IntermediateBytes: -1, ResultRows: -1, ResultBytes: -1}
	args := scenario.args
	var cancel CancelFlag
	var targetError error

	switch action {
	case setStatementFuzzActionError:
		runner.fault = setStatementFuzzError
		targetError = errSetStatementFuzzLeaf
	case setStatementFuzzActionCancel:
		runner.fault = setStatementFuzzCancel
		runner.cancel = &cancel
		options.Cancel = &cancel
		targetError = ErrCanceled
	case setStatementFuzzActionExtraColumn:
		runner.fault = setStatementFuzzExtraColumn
		targetError = ErrSetTreeArity
	case setStatementFuzzActionFewerColumns:
		runner.fault = setStatementFuzzFewerColumns
		targetError = ErrSetTreeArity
	case setStatementFuzzActionShortColumn:
		runner.fault = setStatementFuzzShortColumn
		targetError = ErrSetTreeSource
	case setStatementFuzzActionNegativeRows:
		runner.fault = setStatementFuzzNegativeRows
		targetError = ErrSetTreeSource
	case setStatementFuzzActionHugeRows:
		runner.fault = setStatementFuzzHugeRows
		targetError = ErrSetTreeSource
	case setStatementFuzzActionNegativeBytes:
		runner.fault = setStatementFuzzNegativeBytes
		targetError = ErrSetTreeSource
	case setStatementFuzzActionDetachedCursor:
		runner.fault = setStatementFuzzDetachedCursor
		targetError = ErrSetTreeSource
	case setStatementFuzzActionResultBudget:
		options.ResultBytes = 1
		targetError = ErrResultBudget
	case setStatementFuzzActionIntermediateBudget:
		options.IntermediateBytes = prior
		targetError = ErrIntermediateBudget
	case setStatementFuzzActionWrongArguments:
		args = scenario.args[:len(scenario.args)-1]
		targetError = errSetStatementConfig
	default:
		t.Fatalf("unknown fuzz action %d", action)
	}

	_, err := runSetStatementFuzzAttempt(
		t, runtime, parent, scenario, args, options, prior,
	)
	if action == setStatementFuzzActionWrongArguments {
		if err == nil {
			t.Fatal("wrong argument count succeeded")
		}
	} else if !errors.Is(err, targetError) {
		t.Fatalf("action %d error = %T %v, want %v", action, err, err, targetError)
	}
	runner.fault = setStatementFuzzHealthy
	runner.cancel = nil
	cancel.Reset()
	runSetStatementFuzzSuccess(t, runtime, parent, scenario, prior)
}

func FuzzSetStatementPreparedStateful(f *testing.F) {
	f.Add([]byte{})
	for action := setStatementFuzzActionSuccess; action < setStatementFuzzActionCount; action++ {
		f.Add([]byte{byte(action), byte(action*17 + 3), 0xff, 0, 1, 2, 3, 5, 8, 13})
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		action := setStatementFuzzActionSuccess
		scenarioData := data
		if len(data) != 0 {
			action = setStatementFuzzAction(data[0] % byte(setStatementFuzzActionCount))
			scenarioData = data[1:]
		}
		scenario, err := decodeSetStatementFuzzScenario(scenarioData)
		if err != nil {
			t.Fatal(err)
		}
		var runtime setStatementRuntime
		if err := runtime.prepare(scenario.desc); err != nil {
			t.Fatal(err)
		}
		defer runtime.Release()
		var parent Exec
		defer parent.Release()

		runSetStatementFuzzSuccess(t, &runtime, &parent, scenario, 1)
		random := newSetStatementFuzzBytes(scenarioData)
		runSetStatementFuzzAction(
			t, &runtime, &parent, scenario, action, &random,
		)
		second := setStatementFuzzAction(random.next() % byte(setStatementFuzzActionCount))
		runSetStatementFuzzAction(
			t, &runtime, &parent, scenario, second, &random,
		)

		bad := append([]setStatementLeaf(nil), scenario.desc.leaves...)
		bad[0].paramBase = math.MaxInt
		if _, err := prepareSetStatementDescriptor(
			scenario.plan, bad, 0, len(scenario.args),
		); !errors.Is(err, errSetStatementConfig) {
			t.Fatalf("overflowing parameter base error = %v", err)
		}
	})
}

func TestSetStatementAdversarialTreeWarmReuseAllocations(t *testing.T) {
	rows := [][][]byte{
		{{5}, {8}, {9}}, {{9}, {11}, {12}}, {{5}, {8}, {9}},
		{{8}, {11}, {15}}, {{5}, {8}, {11}}, {{8}, {9}, {12}}, {{15}},
	}
	runners := make([]*setStatementFuzzRunner, len(rows))
	bindings := make([]setStatementLeaf, len(rows))
	args := make([]any, len(rows))
	for leaf := range runners {
		runners[leaf] = newSetStatementFuzzRunner([]string{"value"}, rows[leaf])
		bindings[leaf] = setStatementLeaf{
			runner: runners[leaf], paramBase: len(rows) - 1 - leaf,
		}
		args[leaf] = int64(2)
	}
	plan := SetTreePlan{Nodes: []SetTreeNode{
		NewSetTreeLeaf(0, 1), NewSetTreeLeaf(1, 1), NewSetTreeLeaf(2, 1),
		NewSetTreeLeaf(3, 1), NewSetTreeLeaf(4, 1), NewSetTreeLeaf(5, 1),
		NewSetTreeLeaf(6, 1),
		NewSetTreeBinary(SetTreeUnionAll, 0, 1),
		NewSetTreeBinary(SetTreeUnionDistinct, 2, 3),
		NewSetTreeBinary(SetTreeIntersectAll, 4, 5),
		NewSetTreeBinary(SetTreeIntersectDistinct, 7, 8),
		NewSetTreeBinary(SetTreeExceptAll, 10, 9),
		NewSetTreeBinary(SetTreeExceptDistinct, 11, 6),
	}, Root: 12}
	desc, err := prepareSetStatementDescriptor(plan, bindings, 0, len(args))
	if err != nil {
		t.Fatal(err)
	}
	var runtime setStatementRuntime
	if err := runtime.prepare(desc); err != nil {
		t.Fatal(err)
	}
	defer runtime.Release()
	var parent Exec
	defer parent.Release()
	parent.Options = ExecOptions{
		IntermediateBytes: -1, ResultRows: -1, ResultBytes: -1,
	}
	var frame statementFrame
	var cursor Cursor
	runErr := error(nil)
	source := FromSegment(&store.Segment{})
	run := func() {
		runErr = frame.begin(parent.Options)
		if runErr != nil {
			return
		}
		frame.args = args
		cursor, runErr = runtime.runIntoFrame(
			&parent, source, args, &frame,
		)
	}
	run()
	if runErr != nil || parent.Result.RowCount == 0 || frame.intermediate.used != 0 {
		t.Fatalf("warmup rows=%d bytes=%d err=%v",
			parent.Result.RowCount, frame.intermediate.used, runErr)
	}
	wantRows := parent.Result.RowCount
	allocations := testing.AllocsPerRun(100, run)
	if runErr != nil || parent.Result.RowCount != wantRows || frame.intermediate.used != 0 {
		t.Fatalf("warmed rows=%d/%d bytes=%d err=%v",
			parent.Result.RowCount, wantRows, frame.intermediate.used, runErr)
	}
	if allocations != 0 {
		t.Fatalf("warmed adversarial set statement allocations = %.2f, want 0", allocations)
	}
	if cursor.res != &parent.Result {
		t.Fatal("warmed cursor detached from parent result")
	}
}
