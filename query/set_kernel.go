package query

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"math/bits"
	"unsafe"
)

// setOperation is the physical SQL set-operation mode. It is deliberately
// independent of the SQL AST: lowering only has to select one mode and pass two
// ordinal relation spools into setExecutor.execute.
type setOperation uint8

const (
	setUnionAll setOperation = iota
	setUnionDistinct
	setIntersectAll
	setIntersectDistinct
	setExceptAll
	setExceptDistinct
)

var (
	errSetArity = errors.New("query: set-operation arity mismatch")
	errSetInput = errors.New("query: malformed set-operation relation")
	errSetMode  = errors.New("query: invalid set-operation mode")
	errSetSize  = errors.New("query: set-operation size overflow")
	errSetAlias = errors.New("query: set-operation output aliases an input")
)

// setArityError reports the exact ordinal widths that failed compatibility.
// Names are intentionally absent: SQL set compatibility is positional.
type setArityError struct {
	Left  int
	Right int
}

func (e *setArityError) Error() string {
	return fmt.Sprintf(
		"query: set-operation inputs have %d and %d columns: %v",
		e.Left, e.Right, errSetArity,
	)
}

func (e *setArityError) Unwrap() error { return errSetArity }

func (op setOperation) valid() bool {
	return op <= setExceptDistinct
}

func (op setOperation) resource() string {
	switch op {
	case setUnionAll:
		return "UNION ALL result"
	case setUnionDistinct:
		return "UNION result"
	case setIntersectAll:
		return "INTERSECT ALL result"
	case setIntersectDistinct:
		return "INTERSECT result"
	case setExceptAll:
		return "EXCEPT ALL result"
	case setExceptDistinct:
		return "EXCEPT result"
	default:
		return "set-operation result"
	}
}

type setRowSide uint8

const (
	setRowLeft setRowSide = iota
	setRowRight
)

// setRowRef identifies one input row without retaining an interface or an
// allocation-bearing closure in the publication loop.
type setRowRef struct {
	row  int
	side setRowSide
}

// setHashEntry is one exact row-equivalence class. count is the right-side
// multiplicity for INTERSECT/EXCEPT; used records consumed multiplicity or the
// one DISTINCT emission. source+row identify the exact representative used to
// resolve hash collisions component by component.
type setHashEntry struct {
	hash   uint64
	row    int
	count  int
	used   int
	source setRowSide
}

// setWorkspace owns all transient set planning storage. Its zero value is
// ready. reset retains high-water capacities for prepared execution; release
// deterministically drops them.
type setWorkspace struct {
	seed      maphash.Seed
	seedReady bool
	slots     []uint32
	entries   []setHashEntry
	selected  []setRowRef
}

func (w *setWorkspace) reset() {
	if w == nil {
		return
	}
	w.entries = w.entries[:0]
	w.selected = w.selected[:0]
}

func (w *setWorkspace) release() {
	if w == nil {
		return
	}
	*w = setWorkspace{}
}

// setExecutor is the reusable physical operator later AST lowering embeds per
// set node. It has single-owner execution state and is not safe for concurrent
// calls. result is fully owned and remains valid until the next execute or
// release. execute returns the result's retained intermediate charge; the
// caller releases that charge from the shared statement frame after the
// consumer is finished, matching relationSpool.materialize.
type setExecutor struct {
	workspace setWorkspace
	result    relationSpool
}

func (e *setExecutor) relation() *relationSpool {
	if e == nil {
		return nil
	}
	return &e.result
}

func (e *setExecutor) release() {
	if e == nil {
		return
	}
	e.workspace.release()
	e.result.release()
}

// execute applies op over left and right. Every input and output column is
// addressed by ordinal. A read-only sizing pass admits the worst-case output
// and exact workspace before any backing slice grows. Planning then determines
// the exact stable output, publication owns every selected scalar, and only the
// actual output charge remains reserved on success. Any error after input
// admission leaves result logically empty and releases the complete
// reservation. errSetAlias is rejected before admission and preserves result
// because the result is itself the aliased input.
func (e *setExecutor) execute(
	op setOperation,
	left, right *relationSpool,
	frame *statementFrame,
	cancel *CancelFlag,
) (charge int64, err error) {
	if e == nil || frame == nil {
		return 0, fmt.Errorf("%w: nil executor or statement frame", errSetInput)
	}
	if left == &e.result || right == &e.result {
		return 0, errSetAlias
	}
	e.result.reset()
	e.workspace.reset()
	if !op.valid() {
		return 0, errSetMode
	}
	columns, err := validateSetInputs(left, right)
	if err != nil {
		return 0, err
	}
	if err := cancellationError(cancel); err != nil {
		return 0, err
	}

	shape, err := measureSetExecution(op, left, right, columns)
	if err != nil {
		return 0, err
	}
	if err := frame.intermediate.reserve(op.resource(), shape.totalCharge); err != nil {
		return 0, err
	}
	reserved := true
	defer func() {
		if err != nil {
			e.result.reset()
			e.workspace.reset()
		}
		if reserved {
			frame.intermediate.release(shape.totalCharge)
		}
	}()

	if err = e.workspace.prepare(shape); err != nil {
		return 0, err
	}
	if err = e.workspace.plan(op, left, right, columns, cancel); err != nil {
		return 0, err
	}
	payload, payloadErr := measureSetOutputPayload(
		e.workspace.selected, left, right, columns, cancel,
	)
	if payloadErr != nil {
		return 0, payloadErr
	}
	charge = relationSpoolRetainedBytes(
		len(e.workspace.selected), columns, payload,
	)
	if charge == math.MaxInt64 || charge > shape.outputCharge {
		return 0, fmt.Errorf(
			"%w: measured output charge %d exceeds admitted %d",
			errSetSize, charge, shape.outputCharge,
		)
	}
	if err = e.result.begin(
		len(e.workspace.selected), columns, payload,
	); err != nil {
		return 0, err
	}
	if err = publishSetOutput(
		&e.result, e.workspace.selected, left, right, columns, cancel,
	); err != nil {
		return 0, err
	}
	if len(e.result.data) != e.result.plannedData {
		return 0, e.result.sizingError(0)
	}
	if err = cancellationError(cancel); err != nil {
		return 0, err
	}

	// Workspace and unused worst-case output allowance cease to be logically
	// live after publication. Their capacities remain warmed in the executor.
	frame.intermediate.release(shape.totalCharge - charge)
	reserved = false
	e.workspace.reset()
	return charge, nil
}

func validateSetInputs(left, right *relationSpool) (int, error) {
	if left == nil || right == nil {
		return 0, fmt.Errorf("%w: nil input", errSetInput)
	}
	if left.rows < 0 || right.rows < 0 || left.plannedData < 0 ||
		right.plannedData < 0 || len(left.data) != left.plannedData ||
		len(right.data) != right.plannedData {
		return 0, errSetInput
	}
	if len(left.columns) != len(right.columns) {
		return 0, &setArityError{
			Left: len(left.columns), Right: len(right.columns),
		}
	}
	for column := range left.columns {
		if len(left.columns[column]) != left.rows ||
			len(right.columns[column]) != right.rows {
			return 0, fmt.Errorf(
				"%w: column %d has lengths %d/%d for row counts %d/%d",
				errSetInput, column,
				len(left.columns[column]), len(right.columns[column]),
				left.rows, right.rows,
			)
		}
	}
	return len(left.columns), nil
}

type setExecutionShape struct {
	maxOutput    int
	maxEntries   int
	slotCapacity int
	outputCharge int64
	workCharge   int64
	totalCharge  int64
}

func measureSetExecution(
	op setOperation,
	left, right *relationSpool,
	columns int,
) (setExecutionShape, error) {
	leftRows, rightRows := left.rows, right.rows
	rowsSum, ok := checkedSetAdd(leftRows, rightRows)
	if !ok {
		return setExecutionShape{}, errSetSize
	}
	shape := setExecutionShape{}
	switch op {
	case setUnionAll:
		shape.maxOutput = rowsSum
	case setUnionDistinct:
		shape.maxOutput, shape.maxEntries = rowsSum, rowsSum
	case setIntersectAll, setIntersectDistinct, setExceptAll:
		shape.maxOutput, shape.maxEntries = leftRows, rightRows
	case setExceptDistinct:
		shape.maxOutput, shape.maxEntries = leftRows, rowsSum
	default:
		return setExecutionShape{}, errSetMode
	}
	var err error
	shape.slotCapacity, err = setTableCapacity(shape.maxEntries)
	if err != nil {
		return setExecutionShape{}, err
	}
	payload := int64(len(left.data))
	if op == setUnionAll || op == setUnionDistinct {
		payload = saturatedBytes(payload, int64(len(right.data)))
	}
	if payload > int64(math.MaxInt) {
		return setExecutionShape{}, errSetSize
	}
	shape.outputCharge = relationSpoolRetainedBytes(
		shape.maxOutput, columns, payload,
	)
	shape.workCharge = setWorkspaceRetainedBytes(
		shape.slotCapacity, shape.maxEntries, shape.maxOutput,
	)
	shape.totalCharge = saturatedBytes(shape.outputCharge, shape.workCharge)
	if payload == math.MaxInt64 || shape.outputCharge == math.MaxInt64 ||
		shape.workCharge == math.MaxInt64 || shape.totalCharge == math.MaxInt64 {
		return setExecutionShape{}, errSetSize
	}
	return shape, nil
}

func checkedSetAdd(left, right int) (int, bool) {
	if left < 0 || right < 0 || left > math.MaxInt-right {
		return 0, false
	}
	return left + right, true
}

func setTableCapacity(entries int) (int, error) {
	if entries < 0 {
		return 0, errSetSize
	}
	if entries == 0 {
		return 0, nil
	}
	// slots store entry index + 1 in uint32; reserve zero as the empty marker.
	if uint64(entries) >= uint64(math.MaxUint32) || entries > math.MaxInt/2 {
		return 0, errSetSize
	}
	need := entries * 2
	capacity := 1
	for capacity < need {
		if capacity > math.MaxInt/2 {
			return 0, errSetSize
		}
		capacity <<= 1
	}
	return capacity, nil
}

func setWorkspaceRetainedBytes(slots, entries, selected int) int64 {
	if slots < 0 || entries < 0 || selected < 0 {
		return math.MaxInt64
	}
	return saturatedBytes(
		saturatedProduct(int64(slots), int64(unsafe.Sizeof(uint32(0)))),
		saturatedBytes(
			saturatedProduct(
				int64(entries), int64(unsafe.Sizeof(setHashEntry{})),
			),
			saturatedProduct(
				int64(selected), int64(unsafe.Sizeof(setRowRef{})),
			),
		),
	)
}

func (w *setWorkspace) prepare(shape setExecutionShape) error {
	if !w.seedReady && shape.maxEntries != 0 {
		w.seed = maphash.MakeSeed()
		w.seedReady = true
	}
	if cap(w.slots) < shape.slotCapacity {
		w.slots = make([]uint32, shape.slotCapacity)
	} else {
		w.slots = w.slots[:shape.slotCapacity]
		clear(w.slots)
	}
	if cap(w.entries) < shape.maxEntries {
		w.entries = make([]setHashEntry, 0, shape.maxEntries)
	} else {
		w.entries = w.entries[:0]
	}
	if cap(w.selected) < shape.maxOutput {
		w.selected = make([]setRowRef, 0, shape.maxOutput)
	} else {
		w.selected = w.selected[:0]
	}
	return nil
}

func (w *setWorkspace) plan(
	op setOperation,
	left, right *relationSpool,
	columns int,
	cancel *CancelFlag,
) error {
	if op == setUnionAll {
		for row := 0; row < left.rows; row++ {
			if err := cancellationCheckpoint(cancel, row); err != nil {
				return err
			}
			w.selected = append(w.selected, setRowRef{row: row, side: setRowLeft})
		}
		for row := 0; row < right.rows; row++ {
			if err := cancellationCheckpoint(cancel, row); err != nil {
				return err
			}
			w.selected = append(w.selected, setRowRef{row: row, side: setRowRight})
		}
		return cancellationError(cancel)
	}
	if op == setUnionDistinct {
		if err := w.appendUnionDistinct(left, right, columns, cancel); err != nil {
			return err
		}
		return cancellationError(cancel)
	}
	if len(w.slots) == 0 {
		if op == setExceptAll {
			for row := 0; row < left.rows; row++ {
				if err := cancellationCheckpoint(cancel, row); err != nil {
					return err
				}
				w.selected = append(
					w.selected, setRowRef{row: row, side: setRowLeft},
				)
			}
		}
		return cancellationError(cancel)
	}

	if err := w.countRightRows(left, right, columns, cancel); err != nil {
		return err
	}
	for row := 0; row < left.rows; row++ {
		if err := cancellationCheckpoint(cancel, row); err != nil {
			return err
		}
		hash, hashErr := hashSetRow(w.seed, left, row, columns, cancel)
		if hashErr != nil {
			return hashErr
		}
		entry, found, findErr := w.find(
			hash, left, row, left, right, columns, cancel,
		)
		if findErr != nil {
			return findErr
		}
		switch op {
		case setIntersectAll:
			if found && entry.used < entry.count {
				entry.used++
				w.selected = append(w.selected, setRowRef{row: row, side: setRowLeft})
			}
		case setIntersectDistinct:
			if found && entry.count != 0 && entry.used == 0 {
				entry.used = 1
				w.selected = append(w.selected, setRowRef{row: row, side: setRowLeft})
			}
		case setExceptAll:
			if found && entry.used < entry.count {
				entry.used++
				continue
			}
			w.selected = append(w.selected, setRowRef{row: row, side: setRowLeft})
		case setExceptDistinct:
			if found {
				if entry.count != 0 || entry.used != 0 {
					continue
				}
				entry.used = 1
				w.selected = append(w.selected, setRowRef{row: row, side: setRowLeft})
				continue
			}
			entry, findErr = w.insert(hash, setRowLeft, row, cancel)
			if findErr != nil {
				return findErr
			}
			entry.used = 1
			w.selected = append(w.selected, setRowRef{row: row, side: setRowLeft})
		default:
			return errSetMode
		}
	}
	return cancellationError(cancel)
}

func (w *setWorkspace) appendUnionDistinct(
	left, right *relationSpool,
	columns int,
	cancel *CancelFlag,
) error {
	for _, source := range [...]setRowSide{setRowLeft, setRowRight} {
		relation := left
		if source == setRowRight {
			relation = right
		}
		for row := 0; row < relation.rows; row++ {
			if err := cancellationCheckpoint(cancel, row); err != nil {
				return err
			}
			hash, err := hashSetRow(w.seed, relation, row, columns, cancel)
			if err != nil {
				return err
			}
			if _, found, err := w.find(
				hash, relation, row, left, right, columns, cancel,
			); err != nil {
				return err
			} else if found {
				continue
			}
			if _, err := w.insert(hash, source, row, cancel); err != nil {
				return err
			}
			w.selected = append(w.selected, setRowRef{row: row, side: source})
		}
	}
	return nil
}

func (w *setWorkspace) countRightRows(
	left, right *relationSpool,
	columns int,
	cancel *CancelFlag,
) error {
	for row := 0; row < right.rows; row++ {
		if err := cancellationCheckpoint(cancel, row); err != nil {
			return err
		}
		hash, err := hashSetRow(w.seed, right, row, columns, cancel)
		if err != nil {
			return err
		}
		entry, found, err := w.find(
			hash, right, row, left, right, columns, cancel,
		)
		if err != nil {
			return err
		}
		if !found {
			entry, err = w.insert(hash, setRowRight, row, cancel)
			if err != nil {
				return err
			}
		}
		if entry.count == math.MaxInt {
			return errSetSize
		}
		entry.count++
	}
	return cancellationError(cancel)
}

func (w *setWorkspace) find(
	hash uint64,
	relation *relationSpool,
	row int,
	left, right *relationSpool,
	columns int,
	cancel *CancelFlag,
) (*setHashEntry, bool, error) {
	if len(w.slots) == 0 {
		return nil, false, cancellationError(cancel)
	}
	mask := uint64(len(w.slots) - 1)
	for slot, probes := hash&mask, 0; probes < len(w.slots); probes++ {
		if err := cancellationCheckpoint(cancel, probes); err != nil {
			return nil, false, err
		}
		stored := w.slots[slot]
		if stored == 0 {
			return nil, false, nil
		}
		entry := &w.entries[stored-1]
		if entry.hash == hash {
			candidate := left
			if entry.source == setRowRight {
				candidate = right
			}
			equal, err := setRowsEqual(
				candidate, entry.row, relation, row, columns, cancel,
			)
			if err != nil {
				return nil, false, err
			}
			if equal {
				return entry, true, nil
			}
		}
		slot = (slot + 1) & mask
	}
	return nil, false, errSetSize
}

func (w *setWorkspace) insert(
	hash uint64,
	source setRowSide,
	row int,
	cancel *CancelFlag,
) (*setHashEntry, error) {
	if len(w.slots) == 0 || uint64(len(w.entries)) >= uint64(math.MaxUint32) {
		return nil, errSetSize
	}
	mask := uint64(len(w.slots) - 1)
	slot := hash & mask
	for probes := 0; probes < len(w.slots); probes++ {
		if err := cancellationCheckpoint(cancel, probes); err != nil {
			return nil, err
		}
		if w.slots[slot] == 0 {
			index := len(w.entries)
			w.entries = append(w.entries, setHashEntry{
				hash: hash, row: row, source: source,
			})
			w.slots[slot] = uint32(index + 1)
			return &w.entries[index], nil
		}
		slot = (slot + 1) & mask
	}
	return nil, errSetSize
}

func setRowsEqual(
	left *relationSpool,
	leftRow int,
	right *relationSpool,
	rightRow int,
	columns int,
	cancel *CancelFlag,
) (bool, error) {
	for column := 0; column < columns; column++ {
		if err := cancellationCheckpoint(cancel, column); err != nil {
			return false, err
		}
		// compareScalar's NULL equality is SQL IS NOT DISTINCT FROM semantics:
		// explicit null and the engine's missing marker share one duplicate class.
		if compareScalar(
			left.columns[column][leftRow],
			right.columns[column][rightRow],
		) != 0 {
			return false, nil
		}
	}
	return true, cancellationError(cancel)
}

func hashSetRow(
	seed maphash.Seed,
	relation *relationSpool,
	row, columns int,
	cancel *CancelFlag,
) (uint64, error) {
	hash := uint64(0x243f6a8885a308d3) ^ uint64(columns)
	for column := 0; column < columns; column++ {
		if err := cancellationCheckpoint(cancel, column); err != nil {
			return 0, err
		}
		value := relation.columns[column][row]
		valueHash := uint64(0x9e3779b97f4a7c15)
		if value.kind != kindNull {
			valueHash = hashJoinValue(seed, value)
		}
		valueHash ^= uint64(value.kind) * 0xbf58476d1ce4e5b9
		hash ^= valueHash + 0x9e3779b97f4a7c15 + uint64(column)
		hash = bits.RotateLeft64(hash, 27)*0x94d049bb133111eb +
			0x52dce729
	}
	hash ^= hash >> 30
	hash *= 0xbf58476d1ce4e5b9
	hash ^= hash >> 27
	hash *= 0x94d049bb133111eb
	return hash ^ (hash >> 31), cancellationError(cancel)
}

func measureSetOutputPayload(
	selected []setRowRef,
	left, right *relationSpool,
	columns int,
	cancel *CancelFlag,
) (int64, error) {
	payload := int64(0)
	for at, ref := range selected {
		if err := cancellationCheckpoint(cancel, at); err != nil {
			return 0, err
		}
		relation := left
		if ref.side == setRowRight {
			relation = right
		}
		for column := 0; column < columns; column++ {
			cell := cellFromScalar(relation.columns[column][ref.row])
			bytes, err := relationCellOwnedBytesCancelable(cell, cancel)
			if err != nil {
				return 0, err
			}
			payload = saturatedBytes(payload, int64(bytes))
			if payload == math.MaxInt64 {
				return 0, errSetSize
			}
		}
	}
	return payload, cancellationError(cancel)
}

func publishSetOutput(
	dst *relationSpool,
	selected []setRowRef,
	left, right *relationSpool,
	columns int,
	cancel *CancelFlag,
) error {
	for outputRow, ref := range selected {
		if err := cancellationCheckpoint(cancel, outputRow); err != nil {
			return err
		}
		relation := left
		if ref.side == setRowRight {
			relation = right
		}
		for column := 0; column < columns; column++ {
			if err := cancellationCheckpoint(cancel, column); err != nil {
				return err
			}
			owned, err := dst.ownCell(
				cellFromScalar(relation.columns[column][ref.row]), cancel,
			)
			if err != nil {
				return err
			}
			dst.columns[column][outputRow] = owned
		}
	}
	return cancellationError(cancel)
}
