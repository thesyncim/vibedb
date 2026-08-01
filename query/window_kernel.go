package query

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/bits"
	"strconv"
	"unsafe"
)

// windowFunctionKind is independent of the SQL AST. Lowering supplies only
// ordinal input columns and already-validated window metadata.
type windowFunctionKind uint8

const (
	windowRowNumber windowFunctionKind = iota
	windowRank
	windowDenseRank
	windowLag
	windowLead
	windowCount
	windowSum
	windowAvg
	windowMin
	windowMax
	windowNTile
	windowPercentRank
	windowCumeDist
	windowFirstValue
	windowLastValue
	windowNthValue
)

type windowNullOrder uint8

const (
	windowNullsFirst windowNullOrder = iota
	windowNullsLast
)

type windowOrderKey struct {
	column     int
	descending bool
	nulls      windowNullOrder
}

type windowFrameBoundKind uint8

const (
	windowUnboundedPreceding windowFrameBoundKind = iota
	windowPreceding
	windowCurrentRow
	windowFollowing
	windowUnboundedFollowing
)

type windowFrameBound struct {
	kind        windowFrameBoundKind
	offset      int
	rangeOffset scalar
}

type windowRowsFrame struct {
	unit      windowFrameUnit
	start     windowFrameBound
	end       windowFrameBound
	exclusion windowFrameExclusion
}

type windowFrameUnit uint8

const (
	windowFrameRows windowFrameUnit = iota
	windowFrameGroups
	windowFrameRange
)

type windowFrameExclusion uint8

const (
	windowExcludeNoOthers windowFrameExclusion = iota
	windowExcludeCurrentRow
	windowExcludeGroup
	windowExcludeTies
)

type windowFunctionSpec struct {
	kind       windowFunctionKind
	column     int
	offset     int
	buckets    int
	nth        int
	frame      windowRowsFrame
	defaultVal scalar
	hasDefault bool
}

// windowPlan is borrowed for one synchronous execution. The kernel never
// retains these slices, which lets prepared lowering own metadata inline.
type windowPlan struct {
	partition []int
	order     []windowOrderKey
	functions []windowFunctionSpec
}

var (
	errWindowInput = errors.New("query: malformed window input")
	errWindowPlan  = errors.New("query: invalid window plan")
	errWindowFrame = errors.New("query: invalid window frame")
	errWindowSize  = errors.New("query: window size overflow")
	errWindowAlias = errors.New("query: window output aliases input")
)

// windowExecutor is a single-owner prepared physical operator. Its output has
// one column per function, indexed by original input row ordinal; the internal
// PARTITION/ORDER permutation never reorders the surrounding relation.
// execute is not safe for concurrent calls on one executor. Independent
// executors may read the same immutable relation concurrently.
type windowExecutor struct {
	order       []int
	sortScratch []int
	deque       []int
	extrema     []int
	groups      []int
	numberOut   []byte
	negative    []byte

	aggregate       numberAcc
	rangeTarget     decimalSum
	rangeCandidate  decimalSum
	aggregateLease  aggregateLease
	aggregateBudget aggregateBudget

	result relationSpool
}

func (e *windowExecutor) relation() *relationSpool {
	if e == nil {
		return nil
	}
	return &e.result
}

// release drops every retained high-water buffer. It is idempotent; the zero
// value and a released executor are both ready for another execution.
func (e *windowExecutor) release() {
	if e == nil {
		return
	}
	*e = windowExecutor{}
}

type windowExecutionShape struct {
	rows              int
	functions         int
	needsDeque        bool
	extremaEntries    int
	needsGroups       bool
	aggregateBytes    int64
	numberOutputBytes int
	negativeBytes     int
	workCharge        int64
}

// execute computes every function without exposing partially populated
// output. Workspace admission precedes workspace growth, exact output
// admission precedes result growth, and all reservations unwind on failure.
// The returned charge remains in frame until the caller finishes consuming
// result and releases it, matching relationSpool.materialize and setExecutor.
// errWindowAlias is rejected before reset so an aliased input is preserved.
func (e *windowExecutor) execute(
	input *relationSpool,
	plan *windowPlan,
	frame *statementFrame,
	cancel *CancelFlag,
) (charge int64, err error) {
	if e == nil || frame == nil {
		return 0, fmt.Errorf("%w: nil executor or statement frame", errWindowInput)
	}
	if input == &e.result {
		return 0, errWindowAlias
	}
	e.result.reset()
	e.resetTransient()
	if err := cancellationError(cancel); err != nil {
		return 0, err
	}
	shape, err := measureWindowExecution(input, plan, cancel)
	if err != nil {
		return 0, err
	}
	if err := frame.intermediate.reserve("window workspace", shape.workCharge); err != nil {
		return 0, err
	}
	workReserved := true
	resultReserved := false
	defer func() {
		if err != nil {
			e.result.reset()
			e.resetTransient()
		}
		if resultReserved {
			frame.intermediate.release(charge)
		}
		if workReserved {
			frame.intermediate.release(shape.workCharge)
		}
	}()

	if err = e.prepare(shape); err != nil {
		return 0, err
	}
	if err = e.sortRows(input, plan, cancel); err != nil {
		return 0, err
	}
	measure := windowOutputWriter{measure: true, cancel: cancel}
	if err = e.walk(input, plan, shape, &measure, cancel); err != nil {
		return 0, err
	}
	charge = relationSpoolRetainedBytes(
		shape.rows, shape.functions, measure.payload,
	)
	if charge == math.MaxInt64 || measure.payload > int64(math.MaxInt) {
		return 0, errWindowSize
	}
	if err = frame.intermediate.reserve("window result", charge); err != nil {
		return 0, err
	}
	resultReserved = true
	if err = e.result.begin(shape.rows, shape.functions, measure.payload); err != nil {
		return 0, err
	}
	writer := windowOutputWriter{dst: &e.result, cancel: cancel}
	if err = e.walk(input, plan, shape, &writer, cancel); err != nil {
		return 0, err
	}
	if writer.payload != measure.payload || len(e.result.data) != e.result.plannedData {
		return 0, fmt.Errorf(
			"%w: measured payload %d, published %d/%d",
			errWindowSize, measure.payload, writer.payload, len(e.result.data),
		)
	}
	if err = cancellationError(cancel); err != nil {
		return 0, err
	}

	frame.intermediate.release(shape.workCharge)
	workReserved = false
	resultReserved = false
	e.resetTransient()
	return charge, nil
}

func (e *windowExecutor) resetTransient() {
	if e == nil {
		return
	}
	e.order = e.order[:0]
	e.sortScratch = e.sortScratch[:0]
	e.deque = e.deque[:0]
	e.extrema = e.extrema[:0]
	e.groups = e.groups[:0]
	e.numberOut = e.numberOut[:0]
	e.negative = e.negative[:0]
	e.aggregate.reset()
	e.rangeTarget.reset()
	e.rangeCandidate.reset()
}

func measureWindowExecution(
	input *relationSpool,
	plan *windowPlan,
	cancel *CancelFlag,
) (windowExecutionShape, error) {
	if input == nil || plan == nil {
		return windowExecutionShape{}, fmt.Errorf("%w: nil input or plan", errWindowInput)
	}
	if input.rows < 0 || input.plannedData < 0 || len(input.data) != input.plannedData {
		return windowExecutionShape{}, errWindowInput
	}
	for column := range input.columns {
		if len(input.columns[column]) != input.rows {
			return windowExecutionShape{}, fmt.Errorf(
				"%w: column %d has %d cells for %d rows",
				errWindowInput, column, len(input.columns[column]), input.rows,
			)
		}
	}
	columns := len(input.columns)
	for at, column := range plan.partition {
		if err := cancellationCheckpoint(cancel, at); err != nil {
			return windowExecutionShape{}, err
		}
		if column < 0 || column >= columns {
			return windowExecutionShape{}, fmt.Errorf(
				"%w: PARTITION BY column %d", errWindowPlan, column,
			)
		}
	}
	for at, key := range plan.order {
		if err := cancellationCheckpoint(cancel, at); err != nil {
			return windowExecutionShape{}, err
		}
		if key.column < 0 || key.column >= columns || key.nulls > windowNullsLast {
			return windowExecutionShape{}, fmt.Errorf(
				"%w: ORDER BY key %d", errWindowPlan, at,
			)
		}
	}

	shape := windowExecutionShape{rows: input.rows, functions: len(plan.functions)}
	maxNumberBytes := 0
	maxCompactWeight := int64(0)
	needsExact := false
	needsNumberBuffers := false
	sawNumber := false
	for at, function := range plan.functions {
		if err := cancellationCheckpoint(cancel, at); err != nil {
			return windowExecutionShape{}, err
		}
		if function.kind > windowNthValue || function.offset < 0 ||
			function.buckets < 0 || function.nth < 0 {
			return windowExecutionShape{}, fmt.Errorf(
				"%w: function %d", errWindowPlan, at,
			)
		}
		switch function.kind {
		case windowRowNumber, windowRank, windowDenseRank,
			windowPercentRank, windowCumeDist:
			if function.column != -1 || function.offset != 0 || function.buckets != 0 ||
				function.nth != 0 || function.hasDefault {
				return windowExecutionShape{}, fmt.Errorf(
					"%w: ranking function %d has input column", errWindowPlan, at,
				)
			}
			if function.kind == windowPercentRank || function.kind == windowCumeDist {
				needsNumberBuffers = true
			}
		case windowNTile:
			if function.column != -1 || function.offset != 0 || function.buckets <= 0 ||
				function.nth != 0 || function.hasDefault {
				return windowExecutionShape{}, fmt.Errorf(
					"%w: NTILE function %d metadata", errWindowPlan, at,
				)
			}
		case windowLag, windowLead:
			if function.column < 0 || function.column >= columns ||
				function.buckets != 0 || function.nth != 0 {
				return windowExecutionShape{}, fmt.Errorf(
					"%w: offset function %d column", errWindowPlan, at,
				)
			}
		case windowCount:
			if function.column < -1 || function.column >= columns ||
				function.offset != 0 || function.buckets != 0 || function.nth != 0 ||
				function.hasDefault {
				return windowExecutionShape{}, fmt.Errorf(
					"%w: COUNT function %d column", errWindowPlan, at,
				)
			}
		case windowSum, windowAvg, windowMin, windowMax:
			if function.column < 0 || function.column >= columns ||
				function.offset != 0 || function.buckets != 0 || function.nth != 0 ||
				function.hasDefault {
				return windowExecutionShape{}, fmt.Errorf(
					"%w: aggregate function %d column", errWindowPlan, at,
				)
			}
			if function.kind != windowMin && function.kind != windowMax {
				needsExact = true
				for row, value := range input.columns[function.column] {
					if err := cancellationCheckpoint(cancel, row); err != nil {
						return windowExecutionShape{}, err
					}
					if value.kind != kindNumber {
						continue
					}
					sawNumber = true
					maxNumberBytes = max(maxNumberBytes, len(value.num))
					d := parseDecimal(value.num)
					if !d.zero && !d.weight.wide {
						weight := d.weight.compact
						if weight < 0 {
							if weight == math.MinInt64 {
								maxCompactWeight = math.MaxInt64
							} else {
								weight = -weight
							}
						}
						maxCompactWeight = max(maxCompactWeight, weight)
					}
				}
			}
		case windowFirstValue, windowLastValue, windowNthValue:
			if function.column < 0 || function.column >= columns ||
				function.offset != 0 || function.buckets != 0 || function.hasDefault ||
				(function.kind == windowNthValue) != (function.nth > 0) {
				return windowExecutionShape{}, fmt.Errorf(
					"%w: value function %d metadata", errWindowPlan, at,
				)
			}
		}
		if !windowFunctionUsesFrame(function.kind) {
			continue
		}
		if err := validateWindowFramePlan(function.frame, len(plan.order)); err != nil {
			return windowExecutionShape{}, err
		}
		if function.kind == windowMin || function.kind == windowMax {
			if function.frame.exclusion == windowExcludeNoOthers {
				shape.needsDeque = true
			} else {
				entries, ok := windowExtremaEntries(shape.rows)
				if !ok {
					return windowExecutionShape{}, errWindowSize
				}
				shape.extremaEntries = max(shape.extremaEntries, entries)
			}
		}
		shape.needsGroups = shape.needsGroups || windowFrameNeedsGroups(function.frame)
		if function.frame.unit != windowFrameRange || !windowFrameHasOffset(function.frame) {
			continue
		}
		needsExact = true
		needsNumberBuffers = true
		orderColumn := input.columns[plan.order[0].column]
		for row, value := range orderColumn {
			if err := cancellationCheckpoint(cancel, row); err != nil {
				return windowExecutionShape{}, err
			}
			if value.kind == kindNull {
				continue
			}
			if value.kind != kindNumber {
				return windowExecutionShape{}, fmt.Errorf(
					"%w: RANGE ORDER BY row %d is not numeric", errWindowPlan, row,
				)
			}
			measureWindowNumber(value, &sawNumber, &maxNumberBytes, &maxCompactWeight)
		}
		if function.frame.start.kind == windowPreceding ||
			function.frame.start.kind == windowFollowing {
			measureWindowNumber(
				function.frame.start.rangeOffset,
				&sawNumber, &maxNumberBytes, &maxCompactWeight,
			)
		}
		if function.frame.end.kind == windowPreceding ||
			function.frame.end.kind == windowFollowing {
			measureWindowNumber(
				function.frame.end.rangeOffset,
				&sawNumber, &maxNumberBytes, &maxCompactWeight,
			)
		}
	}

	intBytes := saturatedProduct(int64(shape.rows), int64(unsafe.Sizeof(int(0))))
	work := saturatedBytes(intBytes, intBytes) // order and stable-sort scratch
	if shape.needsDeque {
		work = saturatedBytes(work, intBytes)
	}
	if shape.extremaEntries != 0 {
		work = saturatedBytes(work, saturatedProduct(
			int64(shape.extremaEntries), int64(unsafe.Sizeof(int(0))),
		))
	}
	if shape.needsGroups {
		if shape.rows == math.MaxInt {
			return windowExecutionShape{}, errWindowSize
		}
		groupBytes := saturatedProduct(
			int64(shape.rows+1), int64(unsafe.Sizeof(int(0))),
		)
		work = saturatedBytes(work, groupBytes)
	}
	if needsNumberBuffers {
		shape.numberOutputBytes = 512
		shape.negativeBytes = averageDigits + 2
	}
	if needsExact && sawNumber {
		base := saturatedBytes(
			int64(len(input.data)),
			saturatedBytes(int64(shape.rows), maxCompactWeight),
		)
		base = saturatedBytes(
			base, saturatedBytes(int64(maxNumberBytes), averageDigits+256),
		)
		shape.aggregateBytes = saturatedBytes(
			4*aggregateAccBaseBytes, saturatedProduct(base, 16),
		)
		shape.aggregateBytes = min(shape.aggregateBytes, defaultAggregateBytes)
		outputBound := saturatedBytes(
			saturatedBytes(int64(maxNumberBytes), 256),
			min(maxCompactWeight, maxFixedAggregateJSON),
		)
		if outputBound > int64(math.MaxInt) {
			return windowExecutionShape{}, errWindowSize
		}
		shape.numberOutputBytes = max(shape.numberOutputBytes, max(int(outputBound), 512))
		if maxNumberBytes >= math.MaxInt {
			return windowExecutionShape{}, errWindowSize
		}
		shape.negativeBytes = max(
			maxNumberBytes+1, max(shape.numberOutputBytes+1, averageDigits+2),
		)
		work = saturatedBytes(work, shape.aggregateBytes)
	}
	if shape.numberOutputBytes != 0 {
		work = saturatedBytes(work, int64(shape.numberOutputBytes))
		work = saturatedBytes(work, int64(shape.negativeBytes))
	}
	if work == math.MaxInt64 || shape.aggregateBytes == math.MaxInt64 {
		return windowExecutionShape{}, errWindowSize
	}
	shape.workCharge = work
	return shape, cancellationError(cancel)
}

func validateWindowFrame(frame windowRowsFrame, orderKeyCount ...int) error {
	if len(orderKeyCount) > 1 {
		return errWindowFrame
	}
	count := -1
	if len(orderKeyCount) == 1 {
		count = orderKeyCount[0]
	}
	return validateWindowFramePlan(frame, count)
}

func validateWindowFramePlan(frame windowRowsFrame, orderKeys int) error {
	if frame.unit > windowFrameRange || frame.exclusion > windowExcludeTies ||
		frame.start.kind > windowUnboundedFollowing || frame.end.kind > windowUnboundedFollowing {
		return errWindowFrame
	}
	if frame.start.kind == windowUnboundedFollowing ||
		frame.end.kind == windowUnboundedPreceding {
		return errWindowFrame
	}
	if !validateWindowFrameBound(frame.unit, frame.start) ||
		!validateWindowFrameBound(frame.unit, frame.end) {
		return errWindowFrame
	}
	if frame.unit == windowFrameRange && windowFrameHasOffset(frame) && orderKeys != 1 {
		return errWindowFrame
	}
	if compareWindowBounds(frame.unit, frame.start, frame.end) > 0 {
		return errWindowFrame
	}
	return nil
}

func validateWindowFrameBound(unit windowFrameUnit, bound windowFrameBound) bool {
	bounded := bound.kind == windowPreceding || bound.kind == windowFollowing
	if unit == windowFrameRange {
		if !bounded {
			return bound.offset == 0 && windowScalarUnset(bound.rangeOffset)
		}
		if bound.offset != 0 || bound.rangeOffset.kind != kindNumber ||
			len(bound.rangeOffset.num) == 0 {
			return false
		}
		offset := parseDecimal(bound.rangeOffset.num)
		return offset.zero || !offset.neg
	}
	if !windowScalarUnset(bound.rangeOffset) {
		return false
	}
	if !bounded {
		return bound.offset == 0
	}
	return bound.offset >= 0
}

func windowScalarUnset(value scalar) bool {
	return value.kind == kindNull && !value.bval && len(value.num) == 0 &&
		!value.isInt && value.ival == 0 && value.sval == "" && value.raw == nil
}

func windowFrameHasOffset(frame windowRowsFrame) bool {
	return frame.start.kind == windowPreceding || frame.start.kind == windowFollowing ||
		frame.end.kind == windowPreceding || frame.end.kind == windowFollowing
}

func windowFrameNeedsGroups(frame windowRowsFrame) bool {
	return frame.unit == windowFrameGroups || frame.unit == windowFrameRange ||
		frame.exclusion == windowExcludeGroup || frame.exclusion == windowExcludeTies
}

func compareWindowBounds(unit windowFrameUnit, left, right windowFrameBound) int {
	left = normalizeZeroWindowBound(left)
	right = normalizeZeroWindowBound(right)
	if left.kind != right.kind {
		if left.kind < right.kind {
			return -1
		}
		return 1
	}
	switch left.kind {
	case windowPreceding:
		if unit == windowFrameRange {
			return compareScalar(right.rangeOffset, left.rangeOffset)
		}
		return compareInts(right.offset, left.offset)
	case windowFollowing:
		if unit == windowFrameRange {
			return compareScalar(left.rangeOffset, right.rangeOffset)
		}
		return compareInts(left.offset, right.offset)
	default:
		return 0
	}
}

func normalizeZeroWindowBound(bound windowFrameBound) windowFrameBound {
	zero := bound.offset == 0
	if bound.rangeOffset.kind == kindNumber {
		zero = parseDecimal(bound.rangeOffset.num).zero
	}
	if (bound.kind == windowPreceding || bound.kind == windowFollowing) && zero {
		bound.kind = windowCurrentRow
	}
	return bound
}

func compareInts(left, right int) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func measureWindowNumber(
	value scalar,
	sawNumber *bool,
	maxNumberBytes *int,
	maxCompactWeight *int64,
) {
	*sawNumber = true
	*maxNumberBytes = max(*maxNumberBytes, len(value.num))
	d := parseDecimal(value.num)
	if d.zero || d.weight.wide {
		return
	}
	weight := d.weight.compact
	if weight < 0 {
		if weight == math.MinInt64 {
			weight = math.MaxInt64
		} else {
			weight = -weight
		}
	}
	*maxCompactWeight = max(*maxCompactWeight, weight)
}

func (e *windowExecutor) prepare(shape windowExecutionShape) error {
	if cap(e.order) < shape.rows {
		e.order = make([]int, shape.rows)
	} else {
		e.order = e.order[:shape.rows]
	}
	if cap(e.sortScratch) < shape.rows {
		e.sortScratch = make([]int, shape.rows)
	} else {
		e.sortScratch = e.sortScratch[:shape.rows]
	}
	if shape.needsDeque {
		if cap(e.deque) < shape.rows {
			e.deque = make([]int, 0, shape.rows)
		} else {
			e.deque = e.deque[:0]
		}
	} else {
		e.deque = e.deque[:0]
	}
	if shape.extremaEntries != 0 {
		if cap(e.extrema) < shape.extremaEntries {
			e.extrema = make([]int, shape.extremaEntries)
		} else {
			e.extrema = e.extrema[:shape.extremaEntries]
		}
	} else {
		e.extrema = e.extrema[:0]
	}
	if shape.needsGroups {
		need := shape.rows + 1
		if cap(e.groups) < need {
			e.groups = make([]int, 0, need)
		} else {
			e.groups = e.groups[:0]
		}
	} else {
		e.groups = e.groups[:0]
	}
	if cap(e.numberOut) < shape.numberOutputBytes {
		e.numberOut = make([]byte, 0, shape.numberOutputBytes)
	} else {
		e.numberOut = e.numberOut[:0]
	}
	if cap(e.negative) < shape.negativeBytes {
		e.negative = make([]byte, 0, shape.negativeBytes)
	} else {
		e.negative = e.negative[:0]
	}
	return nil
}

func (e *windowExecutor) sortRows(
	input *relationSpool,
	plan *windowPlan,
	cancel *CancelFlag,
) error {
	for row := range e.order {
		if err := cancellationCheckpoint(cancel, row); err != nil {
			return err
		}
		e.order[row] = row
	}
	rows := len(e.order)
	if rows < 2 {
		return cancellationError(cancel)
	}
	src, dst := e.order, e.sortScratch
	for width := 1; width < rows; {
		for left := 0; left < rows; {
			middle := left + min(width, rows-left)
			right := middle + min(width, rows-middle)
			i, j := left, middle
			for out := left; out < right; out++ {
				if err := cancellationCheckpoint(cancel, out); err != nil {
					return err
				}
				takeLeft := j == right
				if !takeLeft && i < middle {
					comparison, err := compareWindowRows(
						input, plan, src[i], src[j], cancel,
					)
					if err != nil {
						return err
					}
					takeLeft = comparison <= 0
				}
				if takeLeft {
					dst[out] = src[i]
					i++
				} else {
					dst[out] = src[j]
					j++
				}
			}
			if right == rows {
				break
			}
			left = right
		}
		src, dst = dst, src
		if width > rows/2 {
			width = rows
		} else {
			width *= 2
		}
	}
	if len(src) != 0 && unsafe.SliceData(src) != unsafe.SliceData(e.order) {
		for row := range src {
			if err := cancellationCheckpoint(cancel, row); err != nil {
				return err
			}
			e.order[row] = src[row]
		}
	}
	return cancellationError(cancel)
}

func compareWindowRows(
	input *relationSpool,
	plan *windowPlan,
	left, right int,
	cancel *CancelFlag,
) (int, error) {
	for at, column := range plan.partition {
		if err := cancellationCheckpoint(cancel, at); err != nil {
			return 0, err
		}
		if comparison := compareScalar(
			input.columns[column][left], input.columns[column][right],
		); comparison != 0 {
			return comparison, nil
		}
	}
	for at, key := range plan.order {
		if err := cancellationCheckpoint(cancel, at); err != nil {
			return 0, err
		}
		if comparison := compareWindowOrderValues(
			input.columns[key.column][left], input.columns[key.column][right], key,
		); comparison != 0 {
			return comparison, nil
		}
	}
	return 0, cancellationError(cancel)
}

func compareWindowOrderValues(left, right scalar, key windowOrderKey) int {
	leftNull, rightNull := left.kind == kindNull, right.kind == kindNull
	if leftNull || rightNull {
		if leftNull == rightNull {
			return 0
		}
		if leftNull == (key.nulls == windowNullsFirst) {
			return -1
		}
		return 1
	}
	comparison := compareScalar(left, right)
	if key.descending {
		return -comparison
	}
	return comparison
}

func windowPartitionEqual(
	input *relationSpool,
	columns []int,
	left, right int,
	cancel *CancelFlag,
) (bool, error) {
	for at, column := range columns {
		if err := cancellationCheckpoint(cancel, at); err != nil {
			return false, err
		}
		if compareScalar(input.columns[column][left], input.columns[column][right]) != 0 {
			return false, nil
		}
	}
	return true, cancellationError(cancel)
}

func windowPeers(
	input *relationSpool,
	keys []windowOrderKey,
	left, right int,
	cancel *CancelFlag,
) (bool, error) {
	for at, key := range keys {
		if err := cancellationCheckpoint(cancel, at); err != nil {
			return false, err
		}
		if compareWindowOrderValues(
			input.columns[key.column][left], input.columns[key.column][right], key,
		) != 0 {
			return false, nil
		}
	}
	return true, cancellationError(cancel)
}

type windowOutputWriter struct {
	dst     *relationSpool
	payload int64
	measure bool
	cancel  *CancelFlag
}

func (w *windowOutputWriter) put(column, row int, cell Cell) error {
	bytes, err := relationCellOwnedBytesCancelable(cell, w.cancel)
	if err != nil {
		return err
	}
	w.payload = saturatedBytes(w.payload, int64(bytes))
	if w.payload == math.MaxInt64 {
		return errWindowSize
	}
	if w.measure {
		return nil
	}
	owned, err := w.dst.ownCell(cell, w.cancel)
	if err != nil {
		return err
	}
	w.dst.columns[column][row] = owned
	return nil
}

func (e *windowExecutor) walk(
	input *relationSpool,
	plan *windowPlan,
	shape windowExecutionShape,
	writer *windowOutputWriter,
	cancel *CancelFlag,
) error {
	e.aggregateBudget.begin(shape.aggregateBytes)
	for column, function := range plan.functions {
		if err := cancellationCheckpoint(cancel, column); err != nil {
			return err
		}
		for start := 0; start < len(e.order); {
			end := start + 1
			for end < len(e.order) {
				equal, err := windowPartitionEqual(
					input, plan.partition, e.order[start], e.order[end], cancel,
				)
				if err != nil {
					return err
				}
				if !equal {
					break
				}
				end++
			}
			if err := e.walkPartition(
				input, plan.order, function, column, start, end, writer, cancel,
			); err != nil {
				return err
			}
			start = end
		}
	}
	return cancellationError(cancel)
}

func (e *windowExecutor) walkPartition(
	input *relationSpool,
	order []windowOrderKey,
	function windowFunctionSpec,
	outputColumn, start, end int,
	writer *windowOutputWriter,
	cancel *CancelFlag,
) error {
	count := end - start
	e.groups = e.groups[:0]
	if windowFunctionUsesFrame(function.kind) && windowFrameNeedsGroups(function.frame) {
		if err := e.buildWindowGroups(input, order, start, end, cancel); err != nil {
			return err
		}
	}
	switch function.kind {
	case windowRowNumber:
		for position := 0; position < count; position++ {
			if err := cancellationCheckpoint(cancel, position); err != nil {
				return err
			}
			row := e.order[start+position]
			if err := writer.put(outputColumn, row, windowIntegerCell(position+1)); err != nil {
				return err
			}
		}
	case windowRank, windowDenseRank, windowPercentRank:
		rank, dense := 1, 1
		for position := 0; position < count; position++ {
			if err := cancellationCheckpoint(cancel, position); err != nil {
				return err
			}
			if position != 0 {
				peer, err := windowPeers(
					input, order, e.order[start+position-1], e.order[start+position], cancel,
				)
				if err != nil {
					return err
				}
				if !peer {
					rank = position + 1
					dense++
				}
			}
			cell := windowIntegerCell(rank)
			if function.kind == windowDenseRank {
				cell = windowIntegerCell(dense)
			} else if function.kind == windowPercentRank {
				var err error
				cell, err = e.windowRatioCell(rank-1, max(count-1, 1))
				if err != nil {
					return err
				}
			}
			if err := writer.put(outputColumn, e.order[start+position], cell); err != nil {
				return err
			}
		}
	case windowCumeDist:
		for groupStart := 0; groupStart < count; {
			groupEnd := groupStart + 1
			for groupEnd < count {
				peer, err := windowPeers(
					input, order, e.order[start+groupStart], e.order[start+groupEnd], cancel,
				)
				if err != nil {
					return err
				}
				if !peer {
					break
				}
				groupEnd++
			}
			cell, err := e.windowRatioCell(groupEnd, count)
			if err != nil {
				return err
			}
			for position := groupStart; position < groupEnd; position++ {
				if err := cancellationCheckpoint(cancel, position); err != nil {
					return err
				}
				if err := writer.put(outputColumn, e.order[start+position], cell); err != nil {
					return err
				}
			}
			groupStart = groupEnd
		}
	case windowNTile:
		for position := 0; position < count; position++ {
			if err := cancellationCheckpoint(cancel, position); err != nil {
				return err
			}
			tile, err := windowTile(position, count, function.buckets)
			if err != nil {
				return err
			}
			if err := writer.put(
				outputColumn, e.order[start+position], windowIntegerCell(tile),
			); err != nil {
				return err
			}
		}
	case windowLag, windowLead:
		for position := 0; position < count; position++ {
			if err := cancellationCheckpoint(cancel, position); err != nil {
				return err
			}
			target, ok := windowOffsetPosition(
				position, count, function.offset, function.kind == windowLead,
			)
			cell := nullCell()
			if ok {
				cell = cellFromScalar(input.columns[function.column][e.order[start+target]])
			} else if function.hasDefault {
				cell = cellFromScalar(function.defaultVal)
			}
			if err := writer.put(outputColumn, e.order[start+position], cell); err != nil {
				return err
			}
		}
	case windowCount:
		return e.walkCount(input, order, function, outputColumn, start, end, writer, cancel)
	case windowSum, windowAvg:
		return e.walkExact(input, order, function, outputColumn, start, end, writer, cancel)
	case windowMin, windowMax:
		return e.walkExtreme(input, order, function, outputColumn, start, end, writer, cancel)
	case windowFirstValue, windowLastValue, windowNthValue:
		group := 0
		for position := 0; position < count; position++ {
			if err := cancellationCheckpoint(cancel, position); err != nil {
				return err
			}
			selection, err := e.resolveWindowFrameSelection(
				input, order, function.frame, start, position, count, &group, cancel,
			)
			if err != nil {
				return err
			}
			target, ok := selection.positionAt(0)
			switch function.kind {
			case windowLastValue:
				target, ok = selection.positionAt(selection.count() - 1)
			case windowNthValue:
				target, ok = selection.positionAt(function.nth - 1)
			}
			cell := nullCell()
			if ok {
				cell = cellFromScalar(input.columns[function.column][e.order[start+target]])
			}
			if err := writer.put(outputColumn, e.order[start+position], cell); err != nil {
				return err
			}
		}
	default:
		return errWindowPlan
	}
	return cancellationError(cancel)
}

func windowFunctionUsesFrame(kind windowFunctionKind) bool {
	return kind == windowCount || kind == windowSum || kind == windowAvg ||
		kind == windowMin || kind == windowMax || kind == windowFirstValue ||
		kind == windowLastValue || kind == windowNthValue
}

// windowFrameSelection is one contiguous frame with at most one contiguous
// exclusion. EXCLUDE TIES keeps the current row inside that excluded peer run.
// This representation is sufficient for every SQL frame exclusion while
// keeping row loops allocation-free and allowing sliding aggregates to retain
// their contiguous base-frame state.
type windowFrameSelection struct {
	lo, hi         int
	skipLo, skipHi int
	keep           int
}

func (s windowFrameSelection) excluded(position int) bool {
	return position >= s.skipLo && position < s.skipHi && position != s.keep
}

func (s windowFrameSelection) count() int {
	count := s.hi - s.lo - (s.skipHi - s.skipLo)
	if s.keep >= s.skipLo && s.keep < s.skipHi {
		count++
	}
	return count
}

func (s windowFrameSelection) positionAt(ordinal int) (int, bool) {
	if ordinal < 0 || ordinal >= s.count() {
		return 0, false
	}
	first := s.skipLo - s.lo
	if ordinal < first {
		return s.lo + ordinal, true
	}
	ordinal -= first
	if s.keep >= s.skipLo && s.keep < s.skipHi {
		if ordinal == 0 {
			return s.keep, true
		}
		ordinal--
	}
	return s.skipHi + ordinal, true
}

func windowExtremaEntries(rows int) (int, bool) {
	if rows <= 0 {
		return 0, rows == 0
	}
	leaves := 1
	for leaves < rows {
		if leaves > math.MaxInt/2 {
			return 0, false
		}
		leaves *= 2
	}
	if leaves > math.MaxInt/2 {
		return 0, false
	}
	return leaves * 2, true
}

func windowTile(position, rows, buckets int) (int, error) {
	if rows <= 0 || position < 0 || position >= rows || buckets <= 0 {
		return 0, errWindowPlan
	}
	base, extra := rows/buckets, rows%buckets
	if base == 0 {
		return position + 1, nil
	}
	if extra != 0 && base+1 > math.MaxInt/extra {
		return 0, errWindowSize
	}
	cutoff := (base + 1) * extra
	if position < cutoff {
		return position/(base+1) + 1, nil
	}
	return extra + (position-cutoff)/base + 1, nil
}

func (e *windowExecutor) windowRatioCell(numerator, denominator int) (Cell, error) {
	if numerator < 0 || denominator <= 0 {
		return Cell{}, errWindowSize
	}
	if numerator == 0 {
		e.numberOut = append(e.numberOut[:0], '0')
		return Cell{
			kind: TypeNumber, flag: cellNumberRaw | cellInteger,
			raw: e.numberOut, word: 0,
		}, nil
	}
	e.numberOut = strconv.AppendUint(e.numberOut[:0], uint64(numerator), 10)
	cell, ok, err := e.divideWindowAverage(
		false, e.numberOut, uint64(denominator), 0,
	)
	if err != nil {
		return Cell{}, err
	}
	if !ok {
		return Cell{}, errWindowSize
	}
	return cell, nil
}

func windowIntegerCell(value int) Cell {
	return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(value)}
}

func windowOffsetPosition(position, rows, offset int, lead bool) (int, bool) {
	if lead {
		if offset > rows-1-position {
			return 0, false
		}
		return position + offset, true
	}
	if offset > position {
		return 0, false
	}
	return position - offset, true
}

func resolveWindowFrame(frame windowRowsFrame, position, rows int) (int, int) {
	group := 0
	return resolveWindowFrameAt(frame, position, rows, nil, &group)
}

func (e *windowExecutor) resolveWindowFrameSelection(
	input *relationSpool,
	order []windowOrderKey,
	frame windowRowsFrame,
	partitionStart, position, rows int,
	group *int,
	cancel *CancelFlag,
) (windowFrameSelection, error) {
	if len(e.groups) >= 2 {
		advanceWindowGroup(e.groups, position, group)
	}
	var lo, hi int
	var err error
	if frame.unit == windowFrameRange {
		lo, hi, err = e.resolveWindowRange(
			input, order, frame, partitionStart, position, rows, *group, cancel,
		)
	} else {
		lo, hi = resolveWindowFrameAt(frame, position, rows, e.groups, group)
	}
	if err != nil {
		return windowFrameSelection{}, err
	}
	selection := windowFrameSelection{
		lo: lo, hi: hi, skipLo: hi, skipHi: hi, keep: -1,
	}
	switch frame.exclusion {
	case windowExcludeNoOthers:
		return selection, nil
	case windowExcludeCurrentRow:
		selection.skipLo = max(lo, position)
		selection.skipHi = min(hi, position+1)
	case windowExcludeGroup, windowExcludeTies:
		peerLo, peerHi := e.groups[*group], e.groups[*group+1]
		selection.skipLo = max(lo, peerLo)
		selection.skipHi = min(hi, peerHi)
		if frame.exclusion == windowExcludeTies && position >= lo && position < hi {
			selection.keep = position
		}
	}
	if selection.skipHi < selection.skipLo {
		selection.skipLo, selection.skipHi = hi, hi
		selection.keep = -1
	}
	return selection, cancellationError(cancel)
}

func advanceWindowGroup(groups []int, position int, group *int) {
	for *group+1 < len(groups)-1 && position >= groups[*group+1] {
		*group = *group + 1
	}
}

func (e *windowExecutor) resolveWindowRange(
	input *relationSpool,
	order []windowOrderKey,
	frame windowRowsFrame,
	partitionStart, position, rows, group int,
	cancel *CancelFlag,
) (int, int, error) {
	lo, err := e.resolveWindowRangeBound(
		input, order, frame.start, true, partitionStart, position, rows, group, cancel,
	)
	if err != nil {
		return 0, 0, err
	}
	hi, err := e.resolveWindowRangeBound(
		input, order, frame.end, false, partitionStart, position, rows, group, cancel,
	)
	if err != nil {
		return 0, 0, err
	}
	if hi < lo {
		hi = lo
	}
	return lo, hi, cancellationError(cancel)
}

func (e *windowExecutor) resolveWindowRangeBound(
	input *relationSpool,
	order []windowOrderKey,
	bound windowFrameBound,
	start bool,
	partitionStart, position, rows, group int,
	cancel *CancelFlag,
) (int, error) {
	switch bound.kind {
	case windowUnboundedPreceding:
		return 0, nil
	case windowCurrentRow:
		if start {
			return e.groups[group], nil
		}
		return e.groups[group+1], nil
	case windowUnboundedFollowing:
		return rows, nil
	}

	key := order[0]
	current := input.columns[key.column][e.order[partitionStart+position]]
	if current.kind == kindNull {
		if start {
			return e.groups[group], nil
		}
		return e.groups[group+1], nil
	}
	subtract := bound.kind == windowPreceding
	if key.descending {
		subtract = !subtract
	}
	target, wideTarget, err := e.windowRangeTarget(
		current, bound.rangeOffset, subtract, cancel,
	)
	if err != nil {
		return 0, err
	}
	lo, hi := 0, rows
	for lo < hi {
		if err := cancellationCheckpoint(cancel, lo); err != nil {
			return 0, err
		}
		middle := lo + (hi-lo)/2
		candidate := input.columns[key.column][e.order[partitionStart+middle]]
		comparison := 0
		if wideTarget {
			comparison, err = e.compareWideWindowRangeTarget(candidate, key, cancel)
			if err != nil {
				return 0, err
			}
		} else {
			comparison = compareWindowOrderValues(candidate, target, key)
		}
		if comparison < 0 || !start && comparison == 0 {
			lo = middle + 1
		} else {
			hi = middle
		}
	}
	return lo, cancellationError(cancel)
}

func (e *windowExecutor) windowRangeTarget(
	current, offset scalar,
	subtract bool,
	cancel *CancelFlag,
) (scalar, bool, error) {
	e.rangeTarget.reset()
	currentCoeff, currentScale, currentOK := windowCompactDecimal(current)
	offsetCoeff, offsetScale, offsetOK := windowCompactDecimal(offset)
	compact := currentOK && offsetOK
	if compact {
		if subtract {
			if offsetCoeff == math.MinInt64 {
				compact = false
			} else {
				offsetCoeff = -offsetCoeff
			}
		}
		if compact {
			scale := min(currentScale, offsetScale)
			leftShift, leftShiftOK := windowScaleDifference(currentScale, scale)
			rightShift, rightShiftOK := windowScaleDifference(offsetScale, scale)
			left, leftOK := multiplyPow10Int64(currentCoeff, leftShift)
			right, rightOK := multiplyPow10Int64(offsetCoeff, rightShift)
			coefficient, sumOK := checkedAddInt64(left, right)
			compact = leftShiftOK && rightShiftOK && leftOK && rightOK && sumOK
			if compact {
				e.rangeTarget.set = true
				e.rangeTarget.smallCoeff = coefficient
				e.rangeTarget.smallScale = scale
				e.rangeTarget.digits = intDigits64(coefficient)
			}
		}
	}
	if !compact {
		if err := e.setWideWindowRangeTarget(current, offset, subtract, cancel); err != nil {
			return scalar{}, false, err
		}
		return scalar{}, true, nil
	}
	cell, err := e.windowDecimalCell(&e.rangeTarget)
	if err != nil {
		return scalar{}, false, err
	}
	target := scalar{kind: kindNumber, num: cell.raw, raw: cell.raw}
	if integer, ok := cell.Int64(); ok {
		target.isInt, target.ival = true, integer
	}
	return target, false, nil
}

func windowCompactDecimal(value scalar) (coefficient, scale int64, ok bool) {
	if value.isInt {
		return value.ival, 0, true
	}
	d := parseDecimal(value.num)
	if d.zero {
		return 0, 0, true
	}
	digits := len(d.intDigits) + len(d.fracDigits)
	if digits > 18 || d.weight.wide {
		return 0, 0, false
	}
	for at := 0; at < digits; at++ {
		digit, _ := significantDigitAt(&d, at)
		coefficient = coefficient*10 + int64(digit-'0')
	}
	if d.neg {
		coefficient = -coefficient
	}
	scale, ok = checkedAddInt64(d.weight.compact, -int64(digits-1))
	return coefficient, scale, ok
}

func windowScaleDifference(high, low int64) (int64, bool) {
	if high < low {
		return 0, false
	}
	if low == math.MinInt64 {
		if high >= 0 {
			return 0, false
		}
		return high - low, true
	}
	return checkedAddInt64(high, -low)
}

func (e *windowExecutor) setWideWindowRangeTarget(
	current, offset scalar,
	subtract bool,
	cancel *CancelFlag,
) error {
	s := &e.rangeTarget
	inputBytes := saturatedBytes(int64(len(current.num)), int64(len(offset.num)))
	initialNeed := saturatedBytes(
		aggregateAccBaseBytes,
		saturatedProduct(saturatedBytes(inputBytes, 64), 16),
	)
	if initialNeed == math.MaxInt64 {
		return errWindowSize
	}
	if err := e.aggregateLease.reserve(&e.aggregateBudget, initialNeed); err != nil {
		return err
	}

	currentDecimal := parseDecimal(current.num)
	offsetDecimal := parseDecimal(offset.num)
	currentDigits, err := setWindowBigDecimal(
		&s.coeff, &s.scale, &currentDecimal, &s.aux, cancel,
	)
	if err != nil {
		return err
	}
	offsetDigits, err := setWindowBigDecimal(
		&s.termCoeff, &s.termScale, &offsetDecimal, &s.aux, cancel,
	)
	if err != nil {
		return err
	}
	if subtract {
		s.termCoeff.Neg(&s.termCoeff)
	}

	comparison := s.scale.Cmp(&s.termScale)
	shift := int64(0)
	switch {
	case comparison > 0:
		s.tmp.Sub(&s.scale, &s.termScale)
	case comparison < 0:
		s.tmp.Sub(&s.termScale, &s.scale)
	}
	if comparison != 0 {
		var ok bool
		shift, ok = boundedBigShift(&s.tmp, e.aggregateBudget.limit)
		if !ok {
			return budgetError(&e.aggregateBudget, math.MaxInt64)
		}
	}
	predicted := saturatedBytes(int64(max(currentDigits, offsetDigits)), shift)
	predicted = saturatedBytes(predicted, 1)
	need := saturatedBytes(
		aggregateAccBaseBytes,
		saturatedProduct(saturatedBytes(predicted, inputBytes+64), 16),
	)
	if need == math.MaxInt64 {
		return errWindowSize
	}
	if err := e.aggregateLease.reserve(&e.aggregateBudget, need); err != nil {
		return err
	}
	switch {
	case comparison > 0:
		if err := multiplyWindowPow10(&s.coeff, shift, &s.aux, cancel); err != nil {
			return err
		}
		s.scale.Set(&s.termScale)
	case comparison < 0:
		if err := multiplyWindowPow10(&s.termCoeff, shift, &s.aux, cancel); err != nil {
			return err
		}
	}
	s.coeff.Add(&s.coeff, &s.termCoeff)
	s.set = true
	s.big = true
	if s.coeff.Sign() == 0 {
		s.scale.SetInt64(0)
		s.digits = 1
		return cancellationError(cancel)
	}
	if predicted > int64(math.MaxInt) {
		return errWindowSize
	}
	s.digits = int(predicted)
	return cancellationError(cancel)
}

func (e *windowExecutor) compareWideWindowRangeTarget(
	candidate scalar,
	key windowOrderKey,
	cancel *CancelFlag,
) (int, error) {
	if candidate.kind == kindNull {
		if key.nulls == windowNullsFirst {
			return -1, nil
		}
		return 1, nil
	}
	c := &e.rangeCandidate
	c.reset()
	initialNeed := saturatedBytes(
		aggregateAccBaseBytes,
		saturatedProduct(saturatedBytes(int64(len(candidate.num)), 64), 16),
	)
	if initialNeed == math.MaxInt64 {
		return 0, errWindowSize
	}
	if err := e.aggregateLease.reserve(&e.aggregateBudget, initialNeed); err != nil {
		return 0, err
	}
	parsed := parseDecimal(candidate.num)
	digits, err := setWindowBigDecimal(
		&c.coeff, &c.scale, &parsed, &c.aux, cancel,
	)
	if err != nil {
		return 0, err
	}
	comparison := c.scale.Cmp(&e.rangeTarget.scale)
	shift := int64(0)
	if comparison > 0 {
		c.tmp.Sub(&c.scale, &e.rangeTarget.scale)
	} else if comparison < 0 {
		c.tmp.Sub(&e.rangeTarget.scale, &c.scale)
	}
	if comparison != 0 {
		var ok bool
		shift, ok = boundedBigShift(&c.tmp, e.aggregateBudget.limit)
		if !ok {
			return 0, budgetError(&e.aggregateBudget, math.MaxInt64)
		}
	}
	predicted := saturatedBytes(int64(max(digits, e.rangeTarget.digits)), shift)
	need := saturatedBytes(
		aggregateAccBaseBytes,
		saturatedProduct(saturatedBytes(predicted, int64(len(candidate.num))+64), 16),
	)
	if need == math.MaxInt64 {
		return 0, errWindowSize
	}
	if err := e.aggregateLease.reserve(&e.aggregateBudget, need); err != nil {
		return 0, err
	}

	var result int
	switch {
	case comparison > 0:
		c.termCoeff.Set(&c.coeff)
		if err := multiplyWindowPow10(&c.termCoeff, shift, &c.aux, cancel); err != nil {
			return 0, err
		}
		result = c.termCoeff.Cmp(&e.rangeTarget.coeff)
	case comparison < 0:
		c.termCoeff.Set(&e.rangeTarget.coeff)
		if err := multiplyWindowPow10(&c.termCoeff, shift, &c.aux, cancel); err != nil {
			return 0, err
		}
		result = c.coeff.Cmp(&c.termCoeff)
	default:
		result = c.coeff.Cmp(&e.rangeTarget.coeff)
	}
	if key.descending {
		result = -result
	}
	return result, cancellationError(cancel)
}

func setWindowBigDecimal(
	coefficient, scale *big.Int,
	value *decimal,
	scratch *big.Int,
	cancel *CancelFlag,
) (int, error) {
	coefficient.SetInt64(0)
	scale.SetInt64(0)
	if value.zero {
		return 1, cancellationError(cancel)
	}
	digits := len(value.intDigits) + len(value.fracDigits)
	for at := 0; at < digits; at++ {
		if err := cancellationCheckpoint(cancel, at); err != nil {
			return 0, err
		}
		digit, _ := significantDigitAt(value, at)
		coefficient.Mul(coefficient, aggregateBigTen)
		scratch.SetInt64(int64(digit - '0'))
		coefficient.Add(coefficient, scratch)
	}
	if value.neg {
		coefficient.Neg(coefficient)
	}
	if !value.weight.wide {
		scale.SetInt64(value.weight.compact)
	} else {
		for at, count := 0, weightMagnitudeLen(&value.weight); at < count; at++ {
			if err := cancellationCheckpoint(cancel, at); err != nil {
				return 0, err
			}
			scale.Mul(scale, aggregateBigTen)
			scratch.SetInt64(int64(weightMagnitudeDigit(&value.weight, at) - '0'))
			scale.Add(scale, scratch)
		}
		if value.weight.neg {
			scale.Neg(scale)
		}
	}
	scratch.SetInt64(int64(digits - 1))
	scale.Sub(scale, scratch)
	return digits, cancellationError(cancel)
}

func multiplyWindowPow10(
	value *big.Int,
	shift int64,
	scratch *big.Int,
	cancel *CancelFlag,
) error {
	const chunkDigits = int64(18)
	const chunk = int64(1_000_000_000_000_000_000)
	scratch.SetInt64(chunk)
	for at := int64(0); shift >= chunkDigits; at++ {
		if err := cancellationCheckpoint(cancel, int(at)); err != nil {
			return err
		}
		value.Mul(value, scratch)
		shift -= chunkDigits
	}
	if shift != 0 {
		power := int64(1)
		for range shift {
			power *= 10
		}
		scratch.SetInt64(power)
		value.Mul(value, scratch)
	}
	return cancellationError(cancel)
}

func resolveWindowFrameAt(
	frame windowRowsFrame,
	position, rows int,
	groups []int,
	group *int,
) (int, int) {
	if frame.unit == windowFrameGroups {
		advanceWindowGroup(groups, position, group)
		start := resolveWindowGroupStart(frame.start, *group, rows, groups)
		end := resolveWindowGroupEnd(frame.end, *group, rows, groups)
		if end < start {
			end = start
		}
		return start, end
	}
	start := resolveWindowStart(frame.start, position, rows)
	end := resolveWindowEnd(frame.end, position, rows)
	if end < start {
		end = start
	}
	return start, end
}

func resolveWindowGroupStart(
	bound windowFrameBound,
	group, rows int,
	groups []int,
) int {
	switch bound.kind {
	case windowUnboundedPreceding:
		return 0
	case windowPreceding:
		if bound.offset > group {
			return 0
		}
		return groups[group-bound.offset]
	case windowCurrentRow:
		return groups[group]
	case windowFollowing:
		if bound.offset >= len(groups)-1-group {
			return rows
		}
		return groups[group+bound.offset]
	default:
		return rows
	}
}

func resolveWindowGroupEnd(
	bound windowFrameBound,
	group, rows int,
	groups []int,
) int {
	switch bound.kind {
	case windowPreceding:
		if bound.offset > group {
			return 0
		}
		return groups[group-bound.offset+1]
	case windowCurrentRow:
		return groups[group+1]
	case windowFollowing:
		if bound.offset >= len(groups)-2-group {
			return rows
		}
		return groups[group+bound.offset+1]
	case windowUnboundedFollowing:
		return rows
	default:
		return 0
	}
}

func (e *windowExecutor) buildWindowGroups(
	input *relationSpool,
	order []windowOrderKey,
	start, end int,
	cancel *CancelFlag,
) error {
	e.groups = e.groups[:0]
	e.groups = append(e.groups, 0)
	for position := start + 1; position < end; position++ {
		if err := cancellationCheckpoint(cancel, position-start); err != nil {
			return err
		}
		peer, err := windowPeers(
			input, order, e.order[position-1], e.order[position], cancel,
		)
		if err != nil {
			return err
		}
		if !peer {
			e.groups = append(e.groups, position-start)
		}
	}
	e.groups = append(e.groups, end-start)
	return cancellationError(cancel)
}

func resolveWindowStart(bound windowFrameBound, position, rows int) int {
	switch bound.kind {
	case windowUnboundedPreceding:
		return 0
	case windowPreceding:
		if bound.offset > position {
			return 0
		}
		return position - bound.offset
	case windowCurrentRow:
		return position
	case windowFollowing:
		if bound.offset >= rows-position {
			return rows
		}
		return position + bound.offset
	default:
		return rows
	}
}

func resolveWindowEnd(bound windowFrameBound, position, rows int) int {
	switch bound.kind {
	case windowPreceding:
		if bound.offset > position {
			return 0
		}
		return position - bound.offset + 1
	case windowCurrentRow:
		return position + 1
	case windowFollowing:
		if bound.offset >= rows-1-position {
			return rows
		}
		return position + bound.offset + 1
	case windowUnboundedFollowing:
		return rows
	default:
		return 0
	}
}

func (e *windowExecutor) walkCount(
	input *relationSpool,
	order []windowOrderKey,
	function windowFunctionSpec,
	outputColumn, partitionStart, partitionEnd int,
	writer *windowOutputWriter,
	cancel *CancelFlag,
) error {
	rows := partitionEnd - partitionStart
	lo, hi, count := 0, 0, 0
	group := 0
	for position := 0; position < rows; position++ {
		if err := cancellationCheckpoint(cancel, position); err != nil {
			return err
		}
		selection, err := e.resolveWindowFrameSelection(
			input, order, function.frame, partitionStart, position, rows, &group, cancel,
		)
		if err != nil {
			return err
		}
		wantLo, wantHi := selection.lo, selection.hi
		for hi < wantHi {
			if err := cancellationCheckpoint(cancel, hi); err != nil {
				return err
			}
			if function.column < 0 || input.columns[function.column][e.order[partitionStart+hi]].kind != kindNull {
				if count == math.MaxInt {
					return errWindowSize
				}
				count++
			}
			hi++
		}
		for lo < wantLo {
			if err := cancellationCheckpoint(cancel, lo); err != nil {
				return err
			}
			if function.column < 0 || input.columns[function.column][e.order[partitionStart+lo]].kind != kindNull {
				count--
			}
			lo++
		}
		selectedCount := count
		for at := selection.skipLo; at < selection.skipHi; at++ {
			if err := cancellationCheckpoint(cancel, at); err != nil {
				return err
			}
			if selection.excluded(at) &&
				(function.column < 0 || input.columns[function.column][e.order[partitionStart+at]].kind != kindNull) {
				selectedCount--
			}
		}
		if err := writer.put(
			outputColumn, e.order[partitionStart+position], windowIntegerCell(selectedCount),
		); err != nil {
			return err
		}
	}
	return cancellationError(cancel)
}

func (e *windowExecutor) walkExact(
	input *relationSpool,
	order []windowOrderKey,
	function windowFunctionSpec,
	outputColumn, partitionStart, partitionEnd int,
	writer *windowOutputWriter,
	cancel *CancelFlag,
) error {
	e.aggregate.reset()
	rows := partitionEnd - partitionStart
	lo, hi := 0, 0
	group := 0
	for position := 0; position < rows; position++ {
		if err := cancellationCheckpoint(cancel, position); err != nil {
			return err
		}
		selection, err := e.resolveWindowFrameSelection(
			input, order, function.frame, partitionStart, position, rows, &group, cancel,
		)
		if err != nil {
			return err
		}
		wantLo, wantHi := selection.lo, selection.hi
		for hi < wantHi {
			if err := cancellationCheckpoint(cancel, hi); err != nil {
				return err
			}
			value := input.columns[function.column][e.order[partitionStart+hi]]
			if value.kind == kindNumber {
				if err := e.aggregate.sum.add(
					value, &e.aggregateLease, &e.aggregateBudget,
				); err != nil {
					return err
				}
				e.aggregate.n++
			}
			hi++
		}
		for lo < wantLo {
			if err := cancellationCheckpoint(cancel, lo); err != nil {
				return err
			}
			value := input.columns[function.column][e.order[partitionStart+lo]]
			if value.kind == kindNumber {
				if err := e.subtractWindowNumber(value); err != nil {
					return err
				}
				e.aggregate.n--
				if e.aggregate.n == 0 {
					e.aggregate.sum.reset()
				}
			}
			lo++
		}
		for at := selection.skipLo; at < selection.skipHi; at++ {
			if err := cancellationCheckpoint(cancel, at); err != nil {
				return err
			}
			if !selection.excluded(at) {
				continue
			}
			value := input.columns[function.column][e.order[partitionStart+at]]
			if value.kind == kindNumber {
				if err := e.subtractWindowNumber(value); err != nil {
					return err
				}
				e.aggregate.n--
				if e.aggregate.n == 0 {
					e.aggregate.sum.reset()
				}
			}
		}
		cell, err := e.windowExactCell(function.kind)
		if err != nil {
			return err
		}
		if err := writer.put(outputColumn, e.order[partitionStart+position], cell); err != nil {
			return err
		}
		for at := selection.skipLo; at < selection.skipHi; at++ {
			if err := cancellationCheckpoint(cancel, at); err != nil {
				return err
			}
			if !selection.excluded(at) {
				continue
			}
			value := input.columns[function.column][e.order[partitionStart+at]]
			if value.kind == kindNumber {
				if err := e.aggregate.sum.add(
					value, &e.aggregateLease, &e.aggregateBudget,
				); err != nil {
					return err
				}
				e.aggregate.n++
			}
		}
	}
	return cancellationError(cancel)
}

func (e *windowExecutor) subtractWindowNumber(value scalar) error {
	negated, err := e.negatedWindowNumber(value)
	if err != nil {
		return err
	}
	return e.aggregate.sum.add(negated, &e.aggregateLease, &e.aggregateBudget)
}

func (e *windowExecutor) negatedWindowNumber(value scalar) (scalar, error) {
	negated := value
	negated.isInt = false
	if value.isInt && value.ival != math.MinInt64 {
		negated.isInt = true
		negated.ival = -value.ival
	}
	if len(value.num) != 0 && value.num[0] == '-' {
		negated.num = value.num[1:]
		negated.raw = negated.num
	} else {
		e.negative = e.negative[:0]
		if cap(e.negative) < len(value.num)+1 {
			return scalar{}, errWindowSize
		}
		e.negative = append(e.negative, '-')
		e.negative = append(e.negative, value.num...)
		negated.num = e.negative
		negated.raw = e.negative
	}
	return negated, nil
}

func (e *windowExecutor) windowExactCell(kind windowFunctionKind) (Cell, error) {
	if e.aggregate.n == 0 {
		return nullCell(), nil
	}
	value := &e.aggregate.sum
	if kind == windowAvg && e.aggregate.n != 1 {
		if cell, ok, err := e.windowAverageCellFast(); ok || err != nil {
			return cell, err
		}
		average, err := e.aggregate.averageOf(
			&e.aggregate.sum, e.aggregate.n, &e.aggregateLease, &e.aggregateBudget,
		)
		if err != nil {
			return Cell{}, err
		}
		value = average
	}
	return e.windowDecimalCell(value)
}

func (e *windowExecutor) windowDecimalCell(value *decimalSum) (Cell, error) {
	negative := value.sign() < 0
	e.negative = e.negative[:0]
	if value.big {
		if value.digits+1 > cap(e.negative) {
			return Cell{}, errWindowSize
		}
		e.negative = value.coeff.Append(e.negative, 10)
		digits := e.negative
		if len(e.negative) != 0 && e.negative[0] == '-' {
			digits = e.negative[1:]
		}
		if !value.scale.IsInt64() {
			need := saturatedBytes(
				int64(len(digits)+boolInt(negative)+1),
				int64(value.scale.BitLen()+2),
			)
			if need > int64(cap(e.numberOut)) {
				return Cell{}, errWindowSize
			}
			e.numberOut = e.numberOut[:0]
			if negative {
				e.numberOut = append(e.numberOut, '-')
			}
			e.numberOut = append(e.numberOut, digits...)
			e.numberOut = append(e.numberOut, 'e')
			e.numberOut = value.scale.Append(e.numberOut, 10)
			return Cell{kind: TypeNumber, flag: cellNumberRaw, raw: e.numberOut}, nil
		}
		scale := value.scale.Int64()
		for len(digits) > 1 && digits[len(digits)-1] == '0' {
			if scale == math.MaxInt64 {
				return Cell{}, errWindowSize
			}
			digits = digits[:len(digits)-1]
			scale++
		}
		return e.formatWindowAverage(negative, digits, scale)
	}
	e.negative = strconv.AppendUint(e.negative, absInt64(value.smallCoeff), 10)
	digits := e.negative
	scale := value.smallScale
	for len(digits) > 1 && digits[len(digits)-1] == '0' {
		if scale == math.MaxInt64 {
			return Cell{}, errWindowSize
		}
		digits = digits[:len(digits)-1]
		scale++
	}
	return e.formatWindowAverage(negative, digits, scale)
}

// windowAverageCellFast performs exact long division directly over the
// accumulator's decimal coefficient. It covers compact and arbitrary-width
// coefficients with an int64 scale without math/big division, emitting a
// finite quotient exactly when it has at most averageDigits significant
// digits and otherwise applying the engine's 34-digit ties-to-even policy.
func (e *windowExecutor) windowAverageCellFast() (Cell, bool, error) {
	sum := &e.aggregate.sum
	if e.aggregate.n <= 1 {
		return Cell{}, false, nil
	}
	negative := sum.sign() < 0
	baseScale := sum.smallScale
	e.numberOut = e.numberOut[:0]
	var numerator []byte
	if sum.big {
		if !sum.scale.IsInt64() || sum.digits+1 > cap(e.numberOut) {
			return Cell{}, false, nil
		}
		baseScale = sum.scale.Int64()
		e.numberOut = sum.coeff.Append(e.numberOut, 10)
		numerator = e.numberOut
		if len(e.numberOut) != 0 && e.numberOut[0] == '-' {
			numerator = e.numberOut[1:]
		}
	} else {
		coefficient := sum.smallCoeff
		if coefficient == 0 {
			e.numberOut = append(e.numberOut, '0')
			return Cell{
				kind: TypeNumber, flag: cellNumberRaw | cellInteger,
				raw: e.numberOut, word: 0,
			}, true, nil
		}
		e.numberOut = strconv.AppendUint(e.numberOut, absInt64(coefficient), 10)
		numerator = e.numberOut
	}
	if len(numerator) == 0 {
		return Cell{}, false, nil
	}
	return e.divideWindowAverage(
		negative, numerator, uint64(e.aggregate.n), baseScale,
	)
}

func (e *windowExecutor) divideWindowAverage(
	negative bool,
	numerator []byte,
	denominator uint64,
	baseScale int64,
) (Cell, bool, error) {
	if denominator == 0 {
		return Cell{}, false, errWindowSize
	}
	digits := e.negative[:0]
	remainder := uint64(0)
	weight := int64(-1)
	started := false
	tailNonZero := false
	for at, encoded := range numerator {
		digit, next, ok := windowDivideDigit(remainder, encoded-'0', denominator)
		if !ok {
			return Cell{}, false, nil
		}
		remainder = next
		if !started && digit == 0 {
			continue
		}
		if !started {
			started = true
			weight = int64(len(numerator) - 1 - at)
		}
		if len(digits) < averageDigits+1 {
			digits = append(digits, digit+'0')
		} else if digit != 0 {
			tailNonZero = true
		}
	}
	for !started || remainder != 0 && len(digits) < averageDigits+1 {
		digit, next, ok := windowDivideDigit(remainder, 0, denominator)
		if !ok {
			return Cell{}, false, nil
		}
		remainder = next
		if !started && digit == 0 {
			weight--
			continue
		}
		if !started {
			started = true
		}
		digits = append(digits, digit+'0')
	}
	tailNonZero = tailNonZero || remainder != 0
	if len(digits) == 0 {
		e.numberOut = append(e.numberOut[:0], '0')
		return Cell{
			kind: TypeNumber, flag: cellNumberRaw | cellInteger,
			raw: e.numberOut, word: 0,
		}, true, nil
	}
	if len(digits) > averageDigits {
		guard := digits[averageDigits]
		digits = digits[:averageDigits]
		round := guard > '5' || guard == '5' &&
			(tailNonZero || (digits[len(digits)-1]-'0')&1 != 0)
		if round {
			at := len(digits) - 1
			for at >= 0 && digits[at] == '9' {
				digits[at] = '0'
				at--
			}
			if at >= 0 {
				digits[at]++
			} else {
				if len(digits) == cap(digits) {
					return Cell{}, false, nil
				}
				digits = append(digits, 0)
				copy(digits[1:], digits[:len(digits)-1])
				digits[0] = '1'
				weight++
			}
		}
	}
	scale, ok := checkedAddInt64(baseScale, weight)
	if !ok {
		return Cell{}, false, nil
	}
	scale, ok = checkedAddInt64(scale, -int64(len(digits)-1))
	if !ok {
		return Cell{}, false, nil
	}
	for len(digits) > 1 && digits[len(digits)-1] == '0' {
		if scale == math.MaxInt64 {
			return Cell{}, false, nil
		}
		digits = digits[:len(digits)-1]
		scale++
	}
	cell, err := e.formatWindowAverage(negative, digits, scale)
	return cell, true, err
}

func windowDivideDigit(
	remainder uint64,
	digit byte,
	denominator uint64,
) (byte, uint64, bool) {
	high, low := bits.Mul64(remainder, 10)
	low, carry := bits.Add64(low, uint64(digit), 0)
	high += carry
	if high >= denominator {
		return 0, 0, false
	}
	quotient, next := bits.Div64(high, low, denominator)
	if quotient > 9 {
		return 0, 0, false
	}
	return byte(quotient), next, true
}

func (e *windowExecutor) formatWindowAverage(
	negative bool,
	digits []byte,
	scale int64,
) (Cell, error) {
	sign := int64(0)
	if negative {
		sign = 1
	}
	width := saturatedBytes(sign, int64(len(digits)))
	fixed := true
	switch {
	case scale >= 0:
		width = saturatedBytes(width, scale)
	case scale == math.MinInt64:
		fixed = false
	case scale > -int64(len(digits)):
		width = saturatedBytes(width, 1)
	default:
		width = saturatedBytes(width, saturatedBytes(2, -scale-int64(len(digits))))
	}
	fixed = fixed && width <= maxFixedAggregateJSON
	e.numberOut = e.numberOut[:0]
	if fixed {
		if width > int64(cap(e.numberOut)) {
			return Cell{}, errWindowSize
		}
		if negative {
			e.numberOut = append(e.numberOut, '-')
		}
		switch {
		case scale >= 0:
			e.numberOut = append(e.numberOut, digits...)
			for range scale {
				e.numberOut = append(e.numberOut, '0')
			}
		case scale > -int64(len(digits)):
			point := len(digits) + int(scale)
			e.numberOut = append(e.numberOut, digits[:point]...)
			e.numberOut = append(e.numberOut, '.')
			e.numberOut = append(e.numberOut, digits[point:]...)
		default:
			e.numberOut = append(e.numberOut, '0', '.')
			for range -scale - int64(len(digits)) {
				e.numberOut = append(e.numberOut, '0')
			}
			e.numberOut = append(e.numberOut, digits...)
		}
	} else {
		need := saturatedBytes(saturatedBytes(sign, int64(len(digits))), 32)
		if need > int64(cap(e.numberOut)) {
			return Cell{}, errWindowSize
		}
		if negative {
			e.numberOut = append(e.numberOut, '-')
		}
		e.numberOut = append(e.numberOut, digits...)
		if scale != 0 {
			e.numberOut = append(e.numberOut, 'e')
			e.numberOut = strconv.AppendInt(e.numberOut, scale, 10)
		}
	}
	cell := Cell{kind: TypeNumber, flag: cellNumberRaw, raw: e.numberOut}
	if integer, ok := windowAverageInt64(negative, digits, scale); ok {
		cell.flag |= cellInteger
		cell.word = uint64(integer)
	}
	return cell, nil
}

func windowAverageInt64(negative bool, digits []byte, scale int64) (int64, bool) {
	if scale < 0 || scale > 18 {
		return 0, false
	}
	limit := uint64(math.MaxInt64)
	if negative {
		limit++
	}
	value := uint64(0)
	for _, digit := range digits {
		if value > (limit-uint64(digit-'0'))/10 {
			return 0, false
		}
		value = value*10 + uint64(digit-'0')
	}
	for range scale {
		if value > limit/10 {
			return 0, false
		}
		value *= 10
	}
	if negative {
		if value == uint64(math.MaxInt64)+1 {
			return math.MinInt64, true
		}
		return -int64(value), true
	}
	return int64(value), true
}

func (e *windowExecutor) walkExtreme(
	input *relationSpool,
	order []windowOrderKey,
	function windowFunctionSpec,
	outputColumn, partitionStart, partitionEnd int,
	writer *windowOutputWriter,
	cancel *CancelFlag,
) error {
	if function.frame.exclusion != windowExcludeNoOthers {
		return e.walkExcludedExtreme(
			input, order, function, outputColumn,
			partitionStart, partitionEnd, writer, cancel,
		)
	}
	e.deque = e.deque[:0]
	head := 0
	rows := partitionEnd - partitionStart
	lo, hi := 0, 0
	group := 0
	for position := 0; position < rows; position++ {
		if err := cancellationCheckpoint(cancel, position); err != nil {
			return err
		}
		selection, err := e.resolveWindowFrameSelection(
			input, order, function.frame, partitionStart, position, rows, &group, cancel,
		)
		if err != nil {
			return err
		}
		wantLo, wantHi := selection.lo, selection.hi
		for hi < wantHi {
			if err := cancellationCheckpoint(cancel, hi); err != nil {
				return err
			}
			value := input.columns[function.column][e.order[partitionStart+hi]]
			if value.kind == kindNumber {
				for len(e.deque) > head {
					lastPosition := e.deque[len(e.deque)-1]
					last := input.columns[function.column][e.order[partitionStart+lastPosition]]
					comparison := compareScalar(last, value)
					worse := comparison > 0
					if function.kind == windowMax {
						worse = comparison < 0
					}
					if !worse {
						break
					}
					e.deque = e.deque[:len(e.deque)-1]
				}
				e.deque = append(e.deque, hi)
			}
			hi++
		}
		for lo < wantLo {
			if err := cancellationCheckpoint(cancel, lo); err != nil {
				return err
			}
			if head < len(e.deque) && e.deque[head] == lo {
				head++
			}
			lo++
		}
		cell := nullCell()
		for at := head; at < len(e.deque); at++ {
			position := e.deque[at]
			if selection.excluded(position) {
				continue
			}
			row := e.order[partitionStart+position]
			cell = cellFromScalar(input.columns[function.column][row])
			break
		}
		if err := writer.put(outputColumn, e.order[partitionStart+position], cell); err != nil {
			return err
		}
	}
	return cancellationError(cancel)
}

func (e *windowExecutor) walkExcludedExtreme(
	input *relationSpool,
	order []windowOrderKey,
	function windowFunctionSpec,
	outputColumn, partitionStart, partitionEnd int,
	writer *windowOutputWriter,
	cancel *CancelFlag,
) error {
	rows := partitionEnd - partitionStart
	entries, ok := windowExtremaEntries(rows)
	if !ok || entries > len(e.extrema) {
		return errWindowSize
	}
	tree := e.extrema[:entries]
	for at := range tree {
		if err := cancellationCheckpoint(cancel, at); err != nil {
			return err
		}
		tree[at] = -1
	}
	leaves := entries / 2
	for position := 0; position < rows; position++ {
		value := input.columns[function.column][e.order[partitionStart+position]]
		if value.kind == kindNumber {
			tree[leaves+position] = position
		}
	}
	for node := leaves - 1; node > 0; node-- {
		if err := cancellationCheckpoint(cancel, node); err != nil {
			return err
		}
		tree[node] = e.chooseWindowExtreme(
			input, function, partitionStart, tree[node*2], tree[node*2+1],
		)
	}

	group := 0
	for position := 0; position < rows; position++ {
		if err := cancellationCheckpoint(cancel, position); err != nil {
			return err
		}
		selection, err := e.resolveWindowFrameSelection(
			input, order, function.frame, partitionStart, position, rows, &group, cancel,
		)
		if err != nil {
			return err
		}
		best, err := e.queryWindowExtreme(
			input, function, partitionStart, tree, leaves,
			selection.lo, selection.skipLo, cancel,
		)
		if err != nil {
			return err
		}
		if selection.keep >= selection.skipLo && selection.keep < selection.skipHi {
			value := input.columns[function.column][e.order[partitionStart+selection.keep]]
			if value.kind == kindNumber {
				best = e.chooseWindowExtreme(
					input, function, partitionStart, best, selection.keep,
				)
			}
		}
		tail, err := e.queryWindowExtreme(
			input, function, partitionStart, tree, leaves,
			selection.skipHi, selection.hi, cancel,
		)
		if err != nil {
			return err
		}
		best = e.chooseWindowExtreme(input, function, partitionStart, best, tail)
		cell := nullCell()
		if best >= 0 {
			row := e.order[partitionStart+best]
			cell = cellFromScalar(input.columns[function.column][row])
		}
		if err := writer.put(outputColumn, e.order[partitionStart+position], cell); err != nil {
			return err
		}
	}
	return cancellationError(cancel)
}

func (e *windowExecutor) queryWindowExtreme(
	input *relationSpool,
	function windowFunctionSpec,
	partitionStart int,
	tree []int,
	leaves, lo, hi int,
	cancel *CancelFlag,
) (int, error) {
	best := -1
	left, right := leaves+lo, leaves+hi
	for left < right {
		if err := cancellationCheckpoint(cancel, left); err != nil {
			return -1, err
		}
		if left&1 != 0 {
			best = e.chooseWindowExtreme(input, function, partitionStart, best, tree[left])
			left++
		}
		if right&1 != 0 {
			right--
			best = e.chooseWindowExtreme(input, function, partitionStart, best, tree[right])
		}
		left /= 2
		right /= 2
	}
	return best, cancellationError(cancel)
}

func (e *windowExecutor) chooseWindowExtreme(
	input *relationSpool,
	function windowFunctionSpec,
	partitionStart, left, right int,
) int {
	if left < 0 {
		return right
	}
	if right < 0 {
		return left
	}
	leftValue := input.columns[function.column][e.order[partitionStart+left]]
	rightValue := input.columns[function.column][e.order[partitionStart+right]]
	comparison := compareScalar(leftValue, rightValue)
	if function.kind == windowMax {
		comparison = -comparison
	}
	if comparison < 0 || comparison == 0 && left < right {
		return left
	}
	return right
}
