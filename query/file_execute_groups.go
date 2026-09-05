package query

import (
	"errors"
	"math"
	"strings"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

var (
	errIntegerGroupsDeclined = errors.New("query: integer group lane declined")
	errIntegerGroupsOverflow = errors.New("query: integer group lane overflow")
)

const (
	integerGroupSlotBytes  = int64(unsafe.Sizeof(integerGroupSlot{}))
	integerGroupValueBytes = int64(unsafe.Sizeof(integerGroupValue{}))
	integerGroupFileBytes  = int64(unsafe.Sizeof(fileGroup{}))
)

// integerGroupsPlan is the small proof the durable native lane needs after
// compilation. The generic executor remains authoritative unless every
// selected value is one outer integer GROUP BY path and every reduction is a
// repeated COUNT(*) or SUM of one shared outer integer path.
type integerGroupsPlan struct {
	group  compiledPath
	sum    compiledPath
	hasSum bool
}

func (p *plan) integerGroupsPlan() (integerGroupsPlan, bool) {
	if p == nil || p.where != nil || !p.grouped || len(p.groupCols) != 1 ||
		len(p.joins) != 0 || len(p.marks) != 0 || p.runtimeSQLPaths ||
		p.requiresSQLDomainScan() {
		return integerGroupsPlan{}, false
	}
	groupIndex := p.groupCols[0]
	if groupIndex < 0 || groupIndex >= len(p.valuePaths) {
		return integerGroupsPlan{}, false
	}
	group := p.valuePaths[groupIndex]
	if group.join != joinPathOuter || group.indexPath() == "" ||
		group.indexPath() == "/" {
		return integerGroupsPlan{}, false
	}
	for _, order := range p.order {
		if order.slot != 0 || order.value != groupIndex {
			return integerGroupsPlan{}, false
		}
	}
	result := integerGroupsPlan{group: group}
	hasAggregate := false
	for _, column := range p.columns {
		switch column.agg {
		case aggNone:
			if column.value != groupIndex || column.slot != 0 {
				return integerGroupsPlan{}, false
			}
		case aggCount:
			hasAggregate = true
			if column.value >= 0 {
				return integerGroupsPlan{}, false
			}
		case aggSum:
			hasAggregate = true
			if column.num < 0 || column.num >= len(p.numPaths) {
				return integerGroupsPlan{}, false
			}
			sum := p.numPaths[column.num]
			if sum.join != joinPathOuter || sum.indexPath() == "" ||
				sum.indexPath() == "/" {
				return integerGroupsPlan{}, false
			}
			if !result.hasSum {
				result.sum, result.hasSum = sum, true
			} else if result.sum.indexPath() != sum.indexPath() {
				return integerGroupsPlan{}, false
			}
		default:
			return integerGroupsPlan{}, false
		}
	}
	if !hasAggregate {
		return integerGroupsPlan{}, false
	}
	return result, true
}

type integerGroupSlot struct {
	key   int64
	group int32
}

type integerGroupValue struct {
	key   int64
	count int
	sum   int64
	first uint64
}

// integerGroupWorkspace is execution-owned. It deliberately uses an open
// addressing table instead of a Go map so the native lane can prove every
// allocation against MemoryBytes and can reset a declined attempt before the
// generic spill executor starts.
type integerGroupWorkspace struct {
	slots     []integerGroupSlot
	groups    []integerGroupValue
	file      []fileGroup
	scalars   []scalar
	accs      []aggAcc
	shapeSeen []int
	shapeWork []storeio.IntegerGroupShapeWorkspace
	columns   int
}

func (w *integerGroupWorkspace) release() {
	if w == nil {
		return
	}
	w.slots = nil
	w.groups = nil
	w.file = nil
	w.scalars = nil
	w.accs = nil
	w.shapeSeen = nil
	w.shapeWork = nil
	w.columns = 0
}

func (w *integerGroupWorkspace) reset() {
	if w == nil {
		return
	}
	clear(w.slots)
	for i := range w.slots {
		w.slots[i].group = -1
	}
	w.groups = w.groups[:0]
	clear(w.file)
	w.file = w.file[:0]
	clear(w.scalars)
	w.scalars = w.scalars[:0]
	resetAggs(w.accs)
	w.accs = w.accs[:0]
	w.columns = 0
}

func integerGroupAddBytes(dst *int64, n int64) bool {
	if n < 0 || *dst > math.MaxInt64-n {
		return false
	}
	*dst += n
	return true
}

func integerGroupMulBytes(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || (a != 0 && b > math.MaxInt64/a) {
		return 0, false
	}
	return a * b, true
}

func integerGroupResidentBytes(
	slots, groups, file, scalars, accs int,
	shapeBytes int64,
) (int64, bool) {
	bytes := shapeBytes
	for _, part := range [][2]int64{
		{int64(slots), integerGroupSlotBytes},
		{int64(groups), integerGroupValueBytes},
		{int64(file), integerGroupFileBytes},
		{int64(scalars), scalarStructBytes},
		{int64(accs), aggAccStructBytes},
	} {
		count, size := part[0], part[1]
		part, ok := integerGroupMulBytes(count, size)
		if !ok || !integerGroupAddBytes(&bytes, part) {
			return 0, false
		}
	}
	return bytes, true
}

func integerGroupHash(key int64) uint64 {
	x := uint64(key)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	return x ^ (x >> 33)
}

func integerGroupMaxInt() int { return int(^uint(0) >> 1) }

// ensureCapacity grows all native state as one bounded reservation. Existing
// slots are rehashed because their mask depends on the open-addressing width.
func (w *integerGroupWorkspace) ensureCapacity(
	required int, memoryBytes int64,
) error {
	if required < 0 || w.columns < 1 {
		return errIntegerGroupsDeclined
	}
	if required == 0 {
		required = 1
	}
	if required > integerGroupMaxInt()/2 {
		return errIntegerGroupsDeclined
	}
	slots := cap(w.slots)
	if slots < 16 {
		slots = 16
	}
	for required*10 > slots*7 {
		if slots > integerGroupMaxInt()/2 {
			return errIntegerGroupsDeclined
		}
		slots *= 2
	}
	groups := cap(w.groups)
	if groups < required {
		groups = required
	}
	usable := slots * 7 / 10
	if groups < usable {
		groups = usable
	}
	file := max(cap(w.file), groups)
	scalars := max(cap(w.scalars), groups)
	accsNeed, ok := integerGroupMulBytes(int64(groups), int64(w.columns))
	if !ok || accsNeed > int64(integerGroupMaxInt()) {
		return errIntegerGroupsDeclined
	}
	accs := max(cap(w.accs), int(accsNeed))
	shapeBytes := storeio.IntegerGroupScratchBytes(
		storeio.CompactPrimaryStripeMaxRows,
	)
	resident, ok := integerGroupResidentBytes(
		slots, groups, file, scalars, accs, shapeBytes,
	)
	if !ok || resident > memoryBytes {
		return errIntegerGroupsDeclined
	}
	if slots != cap(w.slots) {
		oldGroups := w.groups
		w.slots = make([]integerGroupSlot, slots)
		for i := range w.slots {
			w.slots[i].group = -1
		}
		for id := range oldGroups {
			at := int(integerGroupHash(oldGroups[id].key) & uint64(slots-1))
			for w.slots[at].group >= 0 {
				at = (at + 1) & (slots - 1)
			}
			w.slots[at] = integerGroupSlot{key: oldGroups[id].key, group: int32(id)}
		}
	}
	if groups > cap(w.groups) {
		old := w.groups
		w.groups = make([]integerGroupValue, len(old), groups)
		copy(w.groups, old)
	}
	if file > cap(w.file) {
		old := w.file
		w.file = make([]fileGroup, len(old), file)
		copy(w.file, old)
	}
	if scalars > cap(w.scalars) {
		old := w.scalars
		w.scalars = make([]scalar, len(old), scalars)
		copy(w.scalars, old)
	}
	if accs > cap(w.accs) {
		old := w.accs
		w.accs = make([]aggAcc, len(old), accs)
		copy(w.accs, old)
	}
	return nil
}

func (w *integerGroupWorkspace) prepare(columns int, memoryBytes int64) bool {
	w.reset()
	if columns < 1 || memoryBytes < 0 {
		return false
	}
	w.columns = columns
	shapeBytes := storeio.IntegerGroupScratchBytes(
		storeio.CompactPrimaryStripeMaxRows,
	)
	if bytes, ok := integerGroupResidentBytes(
		max(16, cap(w.slots)), max(1, cap(w.groups)),
		max(1, cap(w.file)), max(1, cap(w.scalars)),
		max(columns, cap(w.accs)), shapeBytes,
	); !ok || bytes > memoryBytes {
		w.columns = 0
		return false
	}
	if cap(w.shapeSeen) < storeio.CompactPrimaryStripeMaxRows {
		w.shapeSeen = make([]int, storeio.CompactPrimaryStripeMaxRows)
	}
	if cap(w.shapeWork) < storeio.CompactPrimaryStripeMaxRows {
		w.shapeWork = make(
			[]storeio.IntegerGroupShapeWorkspace,
			storeio.CompactPrimaryStripeMaxRows,
		)
	}
	if err := w.ensureCapacity(1, memoryBytes); err != nil {
		w.columns = 0
		return false
	}
	return true
}

func checkedIntegerSum(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b || b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}

func (w *integerGroupWorkspace) add(
	key, sum int64, row uint64, withSum bool, memoryBytes int64,
) error {
	if len(w.groups)*10+10 > cap(w.slots)*7 {
		if err := w.ensureCapacity(len(w.groups)+1, memoryBytes); err != nil {
			return err
		}
	}
	at := int(integerGroupHash(key) & uint64(cap(w.slots)-1))
	for {
		slot := &w.slots[at]
		if slot.group < 0 {
			if len(w.groups) >= integerGroupMaxInt() || len(w.groups) > int(1<<31-1) {
				return errIntegerGroupsDeclined
			}
			id := int32(len(w.groups))
			w.groups = append(w.groups, integerGroupValue{
				key: key, first: row,
			})
			slot.key, slot.group = key, id
		}
		if slot.key == key {
			g := &w.groups[slot.group]
			if g.count == integerGroupMaxInt() {
				return errIntegerGroupsDeclined
			}
			g.count++
			if row < g.first {
				g.first = row
			}
			if withSum {
				var ok bool
				g.sum, ok = checkedIntegerSum(g.sum, sum)
				if !ok {
					return errIntegerGroupsOverflow
				}
			}
			return nil
		}
		at = (at + 1) & (cap(w.slots) - 1)
	}
}

func (w *integerGroupWorkspace) groupsInto(
	p *plan, budget *aggregateBudget,
) ([]fileGroup, error) {
	clear(w.file)
	clear(w.scalars)
	resetAggs(w.accs)
	n := len(w.groups)
	if n == 0 {
		w.file = w.file[:0]
		return w.file, nil
	}
	w.file = w.file[:n]
	w.scalars = w.scalars[:n]
	if len(p.columns) != 0 && n > integerGroupMaxInt()/len(p.columns) {
		return nil, errIntegerGroupsDeclined
	}
	needAcc := n * len(p.columns)
	if needAcc > cap(w.accs) {
		return nil, errIntegerGroupsDeclined
	}
	w.accs = w.accs[:needAcc]
	resetAggs(w.accs)
	for id, source := range w.groups {
		w.scalars[id] = scalar{
			kind: kindNumber, isInt: true, ival: source.key,
		}
		accs := w.accs[id*len(p.columns) : (id+1)*len(p.columns)]
		for col, column := range p.columns {
			switch column.agg {
			case aggCount:
				accs[col].count = source.count
			case aggSum:
				number, err := accs[col].number(budget)
				if err != nil {
					return nil, err
				}
				number.n = source.count
				number.sum.set = true
				number.sum.big = false
				number.sum.smallCoeff = source.sum
				number.sum.smallScale = 0
				number.sum.digits = intDigits64(source.sum)
			}
		}
		w.file[id] = fileGroup{
			scalars: w.scalars[id : id+1],
			accs:    accs,
			first:   source.first,
			bytes:   integerGroupFileBytes,
		}
	}
	return w.file, nil
}

func (w *fileWorkspace) integerGroupFilterFor(
	groupPath, sumPath string,
) (*durable.IntegerGroupFilter, error) {
	if w.integerGroupFilter != nil && w.integerGroupPath == groupPath &&
		w.integerGroupSumPath == sumPath {
		return w.integerGroupFilter, nil
	}
	filter, err := durable.NewIntegerGroupFilter(groupPath, sumPath)
	if err != nil {
		return nil, err
	}
	w.integerGroupFilter = filter
	w.integerGroupPath = strings.Clone(groupPath)
	w.integerGroupSumPath = strings.Clone(sumPath)
	return filter, nil
}

type directFileIntegerGroupStats struct {
	scanned uint64
	bytes   int64
}

func (p *plan) runDirectFileIntegerGroups(
	snapshot *durable.Snapshot,
	e *Exec,
	memoryBytes int64,
) (directFileIntegerGroupStats, bool, error) {
	if err := e.Workspace.checkCanceled(); err != nil {
		return directFileIntegerGroupStats{}, true, err
	}
	shape, eligible := p.integerGroupsPlan()
	if !eligible {
		return directFileIntegerGroupStats{}, false, nil
	}
	if p.hasLimit && p.limit == 0 {
		return directFileIntegerGroupStats{}, true, prepareResult(&e.Result, p, 0)
	}
	if snapshot.Len() == 0 {
		return directFileIntegerGroupStats{}, true, prepareResult(&e.Result, p, 0)
	}
	if snapshot.Len() > uint64(integerGroupMaxInt()) {
		return directFileIntegerGroupStats{}, true, store.ErrTooLarge
	}
	workspace := &e.file.integerGroups
	if !workspace.prepare(len(p.columns), memoryBytes) {
		// A declined native attempt must not leave its caller-owned scratch
		// counted nowhere while the generic executor starts its own budget.
		workspace.release()
		return directFileIntegerGroupStats{}, false, nil
	}
	groupPath := shape.group.indexPath()
	sumPath := ""
	if shape.hasSum {
		sumPath = shape.sum.indexPath()
	}
	filter, err := e.file.integerGroupFilterFor(groupPath, sumPath)
	if err != nil {
		workspace.release()
		return directFileIntegerGroupStats{}, true, err
	}
	var scanErr error
	filtered, err := snapshot.FilterIntegerGroupsWithScratch(
		filter, workspace.shapeSeen, workspace.shapeWork,
		func(row uint64, key, sum int64) error {
			if scanErr != nil {
				return scanErr
			}
			if err := cancellationCheckpoint(e.Options.Cancel, int(row)); err != nil {
				scanErr = err
				return err
			}
			if err := workspace.add(
				key, sum, row, shape.hasSum, memoryBytes,
			); err != nil {
				scanErr = err
				return err
			}
			return nil
		},
	)
	if err != nil {
		if scanErr != nil {
			err = scanErr
		}
		if errors.Is(err, errIntegerGroupsDeclined) ||
			errors.Is(err, errIntegerGroupsOverflow) {
			workspace.release()
			return directFileIntegerGroupStats{}, false, nil
		}
		workspace.reset()
		return directFileIntegerGroupStats{}, true, err
	}
	if !filtered.Supported || filtered.Scanned != int(snapshot.Len()) {
		workspace.release()
		return directFileIntegerGroupStats{}, false, nil
	}
	if err := e.Workspace.checkCanceled(); err != nil {
		workspace.reset()
		return directFileIntegerGroupStats{}, true, err
	}
	groups, err := workspace.groupsInto(p, &e.Workspace.aggregateBudget)
	if err != nil {
		workspace.reset()
		return directFileIntegerGroupStats{}, true, err
	}
	result, err := p.fileGroupResultInto(e.Result, groups, &e.Workspace)
	e.Result = result
	if err != nil {
		workspace.reset()
		return directFileIntegerGroupStats{}, true, err
	}
	bytes, _ := integerGroupResidentBytes(
		cap(workspace.slots), cap(workspace.groups), cap(workspace.file),
		cap(workspace.scalars), cap(workspace.accs),
		storeio.IntegerGroupScratchBytes(storeio.CompactPrimaryStripeMaxRows),
	)
	return directFileIntegerGroupStats{
		scanned: uint64(filtered.Scanned), bytes: bytes,
	}, true, nil
}
