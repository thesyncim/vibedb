package query

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/pginput"
	"github.com/thesyncim/vibejson/x/byteview"
)

var ErrRecursiveCTEStatement = errors.New(
	"query: invalid prepared recursive CTE statement term",
)

// RecursiveCTEStatementTermOptions bind one separately prepared Statement to
// the physical recursive runtime. ParamBase is the term's absolute placeholder
// offset in the owning statement frame. RecursiveRelation is empty for an
// anchor and names the prepared CTE relation replaced by the previous delta in
// a recursive term.
type RecursiveCTEStatementTermOptions struct {
	ParamBase         int
	RecursiveRelation string
}

// RecursiveCTEStatementParameterError reports an invalid absolute placeholder
// slice. It remains classifiable as both a Statement-adapter and recursive-CTE
// configuration error.
type RecursiveCTEStatementParameterError struct {
	ParamBase int
	Params    int
	Bound     int
}

func (e *RecursiveCTEStatementParameterError) Error() string {
	return fmt.Sprintf(
		"query: recursive statement term placeholder range [%d,%d) is outside %d bound values: %v",
		e.ParamBase, saturatedIntAdd(e.ParamBase, e.Params), e.Bound,
		ErrRecursiveCTEStatement,
	)
}

func (e *RecursiveCTEStatementParameterError) Unwrap() []error {
	return []error{ErrRecursiveCTEStatement, ErrRecursiveCTEConfig}
}

// RecursiveCTEStatementTerm adapts one prepared Statement to RecursiveCTETerm.
// The Statement is borrowed and must outlive the adapter. Both are
// single-consumer; independent concurrent executions use independent prepared
// Statements, adapters, and runtimes while sharing the immutable database
// snapshot if desired.
//
// Execute through RecursiveCTERuntime.executeStatementTerms so the adapter can
// borrow the owning statementFrame. Calling RunRecursiveCTETerm without that
// scoped binding returns ErrRecursiveCTEStatement.
type RecursiveCTEStatementTerm struct {
	statement *Statement
	target    *statementCTE
	names     []string
	relation  string
	paramBase int

	active  atomic.Bool
	frame   *statementFrame
	binding statementRecursiveBinding

	// Result ownership alternates between two arenas. The input Result may use
	// the previous arena while the next cursor-visible result is copied, so a
	// single arena would overwrite values that have not yet been visited.
	owned   [2][]byte
	current int8
	// coercions and coerceArena exist only when recursive UNION analysis made a
	// direct unknown output inherit the anchor's BOOL or string-category type.
	// Ordinary recursive terms retain nil sidecars and never enter this path.
	coercions   []setStatementColumnCoercion
	coerceArena []byte
}

// PrepareRecursiveCTEStatementTerm validates and snapshots one Statement term.
// It does not parse SQL and does not imply WITH RECURSIVE grammar support.
func PrepareRecursiveCTEStatementTerm(
	statement *Statement,
	options RecursiveCTEStatementTermOptions,
) (*RecursiveCTEStatementTerm, error) {
	if statement == nil || len(statement.Columns()) == 0 || options.ParamBase < 0 {
		return nil, fmt.Errorf(
			"query: recursive statement term has an invalid statement or ParamBase: %w",
			ErrRecursiveCTEStatement,
		)
	}
	params := statement.NumParams()
	if options.ParamBase > math.MaxInt-params {
		return nil, &RecursiveCTEStatementParameterError{
			ParamBase: options.ParamBase, Params: params, Bound: -1,
		}
	}
	term := &RecursiveCTEStatementTerm{
		statement: statement,
		relation:  strings.Clone(options.RecursiveRelation),
		paramBase: options.ParamBase,
		current:   -1,
		names:     make([]string, len(statement.Columns())),
	}
	for ordinal, name := range statement.Columns() {
		term.names[ordinal] = strings.Clone(name)
	}
	if term.relation == "" {
		return term, nil
	}
	catalog := statement.cteCatalog()
	matches := 0
	if catalog != nil {
		for _, definition := range catalog.defs {
			if definition == nil || definition.definition == nil ||
				definition.definition.Name != term.relation {
				continue
			}
			term.target = definition
			matches++
		}
	}
	if matches != 1 || term.target == nil || term.target.references == 0 {
		term.Release()
		return nil, fmt.Errorf(
			"query: recursive relation %q resolves to %d referenced prepared CTE definitions: %w",
			options.RecursiveRelation, matches, ErrRecursiveCTEStatement,
		)
	}
	if term.target.recursiveOwner != nil {
		term.Release()
		return nil, fmt.Errorf(
			"query: recursive relation %q already has a prepared adapter: %w",
			options.RecursiveRelation, ErrRecursiveCTEStatement,
		)
	}
	// A target with exactly one direct passthrough reference may already have
	// compiled that reference away. Mark ownership first, then rebuild the
	// Statement that owns that exact reference (which can be a nested CTE body,
	// not necessarily the top-level borrowed Statement). Non-fused references
	// already cross materializeInto and need no rebuild.
	fusionStatement := term.target.firstReference.owner
	needsRelower := fusionStatement != nil && fusionStatement.canFuseCTE()
	term.target.recursiveOwner = term
	if needsRelower {
		previousPrepareMode := fusionStatement.prepareMode
		fusionStatement.prepareMode = true
		if err := fusionStatement.lower(fusionStatement.args); err != nil {
			fusionStatement.prepareMode = previousPrepareMode
			term.target.recursiveOwner = nil
			term.Release()
			return nil, err
		}
		fusionStatement.prepareMode = previousPrepareMode
	}
	return term, nil
}

func (t *RecursiveCTEStatementTerm) RecursiveCTEColumns() []string {
	if t == nil {
		return nil
	}
	return t.names[:len(t.names):len(t.names)]
}

func (t *RecursiveCTEStatementTerm) begin(
	frame *statementFrame,
	recursiveColumns []string,
) error {
	if t == nil || t.statement == nil || frame == nil {
		return fmt.Errorf("query: nil recursive statement term execution: %w", ErrRecursiveCTEStatement)
	}
	if !t.active.CompareAndSwap(false, true) {
		return &RecursiveCTEReentryError{Name: t.relation}
	}
	fail := true
	defer func() {
		if fail {
			t.active.Store(false)
		}
	}()
	params := t.statement.NumParams()
	if t.paramBase < 0 || t.paramBase > len(frame.args) ||
		params > len(frame.args)-t.paramBase {
		return &RecursiveCTEStatementParameterError{
			ParamBase: t.paramBase, Params: params, Bound: len(frame.args),
		}
	}
	if t.target != nil {
		if len(t.target.names) != len(recursiveColumns) {
			return &RecursiveCTEArityError{
				Name: t.relation, Term: "recursive relation",
				Expected: len(recursiveColumns), Actual: len(t.target.names),
			}
		}
		for ordinal := range recursiveColumns {
			if t.target.names[ordinal] != recursiveColumns[ordinal] {
				return fmt.Errorf(
					"query: recursive relation %q column %d is %q, want %q: %w",
					t.relation, ordinal, t.target.names[ordinal],
					recursiveColumns[ordinal], ErrRecursiveCTEStatement,
				)
			}
		}
	}
	t.frame = frame
	fail = false
	return nil
}

func (t *RecursiveCTEStatementTerm) end() {
	if t == nil || !t.active.Load() {
		return
	}
	t.frame = nil
	t.active.Store(false)
}

// executeStatementTerms is the Statement-aware entry point future recursive
// lowering calls. It installs the one existing statement frame on each adapter
// for exactly the synchronous fixpoint execution, then delegates every
// publication and materialization policy to RecursiveCTERuntime.execute.
func (r *RecursiveCTERuntime) executeStatementTerms(
	descriptor *RecursiveCTEDescriptor,
	base Source,
	frame *statementFrame,
	options ExecOptions,
) (RecursiveCTEResult, error) {
	if descriptor == nil {
		return RecursiveCTEResult{}, fmt.Errorf(
			"query: nil recursive Statement descriptor: %w",
			ErrRecursiveCTEStatement,
		)
	}
	anchor, _ := descriptor.anchor.(*RecursiveCTEStatementTerm)
	recursive, _ := descriptor.recursive.(*RecursiveCTEStatementTerm)
	if anchor != nil {
		if err := anchor.begin(frame, descriptor.columns); err != nil {
			return RecursiveCTEResult{}, err
		}
		defer anchor.end()
	}
	if recursive != nil && recursive != anchor {
		if err := recursive.begin(frame, descriptor.columns); err != nil {
			return RecursiveCTEResult{}, err
		}
		defer recursive.end()
	}
	return r.execute(descriptor, base, frame, options)
}

func (t *RecursiveCTEStatementTerm) RunRecursiveCTETerm(
	exec *Exec,
	input RecursiveCTETermInput,
) (err error) {
	if t == nil || t.statement == nil || exec == nil ||
		!t.active.Load() || t.frame == nil {
		return fmt.Errorf(
			"query: recursive Statement term ran outside its statement frame: %w",
			ErrRecursiveCTEStatement,
		)
	}
	if err := cancellationError(exec.Options.Cancel); err != nil {
		return err
	}
	if t.target == nil && input.Delta.Valid() {
		return fmt.Errorf(
			"query: anchor Statement term received a recursive delta: %w",
			ErrRecursiveCTEStatement,
		)
	}
	if t.target != nil && !input.Delta.Valid() {
		return fmt.Errorf(
			"query: recursive Statement term %q has no delta: %w",
			t.relation, ErrRecursiveCTEStatement,
		)
	}
	frame := t.frame
	params := t.statement.NumParams()
	args := frame.args[t.paramBase : t.paramBase+params]
	// Separately prepared terms number nested CTE and subquery placeholders
	// from zero. Rebase only the borrowed argument view for this synchronous
	// invocation; the budget account and frame identity remain the owner's one
	// existing statementFrame.
	outerArgs := frame.args
	frame.args = args
	defer func() { frame.args = outerArgs }()
	if t.target != nil {
		if t.target.recursiveBinding != nil {
			return &RecursiveCTEReentryError{Name: t.relation}
		}
		t.binding = statementRecursiveBinding{
			relation: input.Delta.relation,
			frame:    frame,
			name:     t.relation,
		}
		t.target.recursiveBinding = &t.binding
	}
	defer func() {
		// The published Result owns every variable-width value before relation
		// cleanup. This order is the lifetime boundary that lets delta and joined
		// base spools be reset immediately after the term returns.
		t.statement.releaseRelations(frame)
		if t.target != nil {
			t.target.recursiveBinding = nil
		}
		t.binding = statementRecursiveBinding{}
		if err != nil {
			clearExecBorrowedViews(exec)
		}
	}()
	cursor, err := t.statement.runIntoFrame(
		exec, input.Base, args, frame, "recursive CTE Statement term result",
	)
	if err != nil {
		return err
	}
	if err := t.coerceResult(&exec.Result, exec.Options.Cancel); err != nil {
		return err
	}
	return t.ownResult(exec, cursor, exec.Options.Cancel)
}

func (t *RecursiveCTEStatementTerm) coerceResult(
	result *Result,
	cancel *CancelFlag,
) error {
	if t == nil || result == nil || len(t.coercions) == 0 {
		return nil
	}
	t.coerceArena = t.coerceArena[:0]
	visited := 0
	for column := range result.Columns {
		if column >= len(t.coercions) ||
			t.coercions[column].target == OutputJSON {
			continue
		}
		coercion := t.coercions[column]
		for row := range result.Columns[column].Cells {
			if err := cancellationCheckpoint(cancel, visited); err != nil {
				return err
			}
			visited++
			cell, err := t.coerceCell(
				result.Columns[column].Cells[row], coercion,
			)
			if err != nil {
				return err
			}
			result.Columns[column].Cells[row] = cell
		}
	}
	return cancellationError(cancel)
}

func (t *RecursiveCTEStatementTerm) coerceCell(
	cell Cell,
	coercion setStatementColumnCoercion,
) (Cell, error) {
	if cell.kind == TypeNull {
		return cell, nil
	}
	switch coercion.target {
	case OutputSQLBool:
		if cell.kind == TypeBool {
			return cell, nil
		}
		if cell.kind != TypeString {
			return Cell{}, &ScalarTypeError{
				Pos: coercion.pos, Operation: "recursive UNION boolean coercion",
				Left: cell.kind, Right: TypeBool,
			}
		}
		value, ok := pginput.Boolean(cell.text)
		if !ok {
			return Cell{}, &ScalarInvalidTextError{
				Pos: coercion.pos, Target: "BOOLEAN",
			}
		}
		return cellFromScalar(scalar{kind: kindBool, bval: value}), nil
	case OutputSQLText, OutputSQLVarchar, OutputSQLName, OutputSQLBPChar:
		switch cell.kind {
		case TypeString:
			return cell, nil
		case TypeBool:
			text := "false"
			if cell.flag&cellTrue != 0 {
				text = "true"
			}
			return Cell{kind: TypeString, text: text}, nil
		case TypeNumber:
			raw := cell.raw
			if len(raw) == 0 {
				start := len(t.coerceArena)
				t.coerceArena = cell.AppendJSON(t.coerceArena)
				raw = t.coerceArena[start:len(t.coerceArena):len(t.coerceArena)]
			}
			return Cell{kind: TypeString, text: byteview.String(raw)}, nil
		default:
			return Cell{}, &ScalarTypeError{
				Pos: coercion.pos, Operation: "recursive UNION text coercion",
				Left: cell.kind, Right: TypeString,
			}
		}
	default:
		return Cell{}, fmt.Errorf(
			"query: recursive UNION has invalid coercion target %d: %w",
			coercion.target, ErrRecursiveCTEStatement,
		)
	}
}

func (t *RecursiveCTEStatementTerm) ownResult(
	exec *Exec,
	cursor Cursor,
	cancel *CancelFlag,
) (err error) {
	columns := len(t.names)
	if len(exec.Result.Columns) != columns {
		return &RecursiveCTEArityError{
			Name: t.relation, Term: "Statement result",
			Expected: columns, Actual: len(exec.Result.Columns),
		}
	}
	rows := 0
	payload := int64(0)
	copyBytes := int64(0)
	for {
		next, nextErr := cursor.nextWithCancel(cancel)
		if nextErr != nil {
			return nextErr
		}
		if !next {
			break
		}
		if rows == math.MaxInt {
			return ErrRecursiveSize
		}
		rows++
		for column := 0; column < columns; column++ {
			if err := cancellationCheckpoint(cancel, column); err != nil {
				return err
			}
			cell := cursor.Cell(column)
			payload = saturatedBytes(payload, resultCellPayloadBytes(cell))
			copyBytes = saturatedBytes(
				copyBytes, int64(len(cell.raw))+int64(len(cell.text)),
			)
			if payload == math.MaxInt64 || copyBytes == math.MaxInt64 {
				return ErrRecursiveSize
			}
		}
	}
	required, err := exec.Result.checkResultBudget(columns, rows, payload)
	if err != nil {
		return err
	}
	if copyBytes > int64(math.MaxInt) {
		return ErrRecursiveSize
	}
	if t.current >= 0 {
		t.owned[t.current] = exec.Result.fileData
	}
	destination := int8(0)
	if t.current == 0 {
		destination = 1
	}
	data := t.owned[destination]
	need := int(copyBytes)
	if cap(data) < need {
		data = make([]byte, 0, need)
	} else {
		data = data[:0]
	}
	// Retain newly grown capacity even if cancellation interrupts publication.
	t.owned[destination] = data
	write := 0
	cursor = t.statement.cursor(&exec.Result)
	for {
		next, nextErr := cursor.nextWithCancel(cancel)
		if nextErr != nil {
			return nextErr
		}
		if !next {
			break
		}
		for column := 0; column < columns; column++ {
			if err := cancellationCheckpoint(cancel, column); err != nil {
				return err
			}
			cell := cursor.Cell(column)
			cell, data = ownRecursiveStatementCell(cell, data)
			exec.Result.Columns[column].Cells[write] = cell
		}
		write++
	}
	if write != rows || len(data) != need {
		return fmt.Errorf(
			"query: recursive Statement result changed during publication: %w",
			ErrRecursiveCTEStatement,
		)
	}
	for column := range exec.Result.Columns {
		cells := exec.Result.Columns[column].Cells
		if column < columns {
			if rows < len(cells) {
				clear(cells[rows:])
			}
			exec.Result.Columns[column].Cells = cells[:rows]
			exec.Result.Columns[column].Header = t.names[column]
			continue
		}
		clear(cells)
		exec.Result.Columns[column] = ResultColumn{}
	}
	exec.Result.Columns = exec.Result.Columns[:columns]
	exec.Result.RowCount = rows
	exec.Result.resultBytesUsed = required
	exec.Result.fileData = data
	t.owned[destination] = data
	t.current = destination
	return cancellationError(cancel)
}

func ownRecursiveStatementCell(cell Cell, data []byte) (Cell, []byte) {
	if len(cell.raw) != 0 {
		start := len(data)
		data = append(data, cell.raw...)
		cell.raw = data[start:len(data):len(data)]
	}
	if len(cell.text) != 0 {
		start := len(data)
		data = append(data, cell.text...)
		cell.text = byteview.String(data[start:len(data):len(data)])
	}
	return cell, data
}

// Release drops adapter-owned result arenas and schema copies. It does not
// release the borrowed Statement. Release is idempotent and must not race an
// execution. A same-owner call made while execution is active is a no-op, so
// it cannot detach the live delta binding; call Release again after execution.
func (t *RecursiveCTEStatementTerm) Release() {
	if t == nil {
		return
	}
	if t.active.Load() {
		return
	}
	if t.target != nil && t.target.recursiveBinding == &t.binding {
		t.target.recursiveBinding = nil
	}
	if t.target != nil && t.target.recursiveOwner == t {
		t.target.recursiveOwner = nil
	}
	t.statement = nil
	t.target = nil
	t.names = nil
	t.relation = ""
	t.paramBase = 0
	t.frame = nil
	t.binding = statementRecursiveBinding{}
	t.owned = [2][]byte{}
	t.coercions = nil
	t.coerceArena = nil
	t.current = -1
	t.active.Store(false)
}

type statementRecursiveBinding struct {
	relation *relationSpool
	frame    *statementFrame
	name     string
}

func (b *statementRecursiveBinding) materializeInto(
	destination *relationSpool,
	frame *statementFrame,
	cancel *CancelFlag,
	resource string,
) (charge int64, err error) {
	if b == nil || b.relation == nil || destination == nil || frame == nil ||
		b.frame != frame {
		return 0, fmt.Errorf(
			"query: recursive relation %q is outside its statement frame: %w",
			b.name, ErrRecursiveCTEStatement,
		)
	}
	source := b.relation
	rows, columns := source.rows, len(source.columns)
	destination.reset()
	charge = relationSpoolRetainedBytes(rows, columns, 0)
	if charge == math.MaxInt64 {
		return 0, ErrRecursiveSize
	}
	if err = frame.intermediate.reserve(resource, charge); err != nil {
		return 0, err
	}
	reserved := charge
	defer func() {
		if err != nil {
			destination.reset()
			frame.intermediate.release(reserved)
			charge = 0
		}
	}()
	if err = cancellationError(cancel); err != nil {
		return 0, err
	}
	if err = destination.begin(rows, columns, 0); err != nil {
		return 0, err
	}
	for column := 0; column < columns; column++ {
		for row := 0; row < rows; row++ {
			if err = cancellationCheckpoint(cancel, row); err != nil {
				return 0, err
			}
			destination.columns[column][row] = source.columns[column][row]
		}
	}
	return charge, cancellationError(cancel)
}

func saturatedIntAdd(left, right int) int {
	if right > 0 && left > math.MaxInt-right {
		return math.MaxInt
	}
	if right < 0 && left < math.MinInt-right {
		return math.MinInt
	}
	return left + right
}
