package query

import (
	"fmt"
	"hash/maphash"
	"math/bits"
	"unsafe"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

const markEmpty = int32(-1)

var correlatedMarkCardinalityError = &CardinalityViolationError{}

// markGroup is one non-NULL composite correlation tuple. rows is capped at
// two: EXISTS needs only presence, while scalar comparison needs to distinguish
// zero, one, and more-than-one authored rows (including duplicate and NULL
// projections). key addresses width consecutive retained scalars.
type markGroup struct {
	hash    uint64
	key     int
	next    int32
	rows    uint8
	hasNull bool
	first   scalar
}

// markValue is one deduplicated non-NULL IN projection within a group.
type markValue struct {
	hash  uint64
	group int32
	next  int32
	value scalar
}

type markFileSide struct {
	docs     store.Segment
	data     []byte
	ends     []int
	overflow []byte
}

// markBinding is built completely on the caller goroutine and then shared
// read-only by every outer filter worker. All retained variable-width values
// live in text; the nested scan workspace and durable batch are private to the
// build and never participate in probes.
type markBinding struct {
	plan planMark

	groups       []markGroup
	keys         []scalar
	groupBuckets []int32
	groupMask    uint32

	values       []markValue
	valueBuckets []int32
	valueMask    uint32

	text          []byte
	textReserved  int64
	groupReserved int64
	valueReserved int64
	leftEntries   []vibejson.IndexEntry
	rightEntries  []vibejson.IndexEntry
	leftReserved  int64
	rightReserved int64
	seed          maphash.Seed
	seeded        bool

	scan  Workspace
	masks []store.Mask
	rows  []store.Location
	file  markFileSide
}

func (b *markBinding) reset() {
	clear(b.groups)
	clear(b.keys)
	clear(b.values)
	b.groups = b.groups[:0]
	b.keys = b.keys[:0]
	b.values = b.values[:0]
	b.groupBuckets = b.groupBuckets[:0]
	b.valueBuckets = b.valueBuckets[:0]
	b.groupMask = 0
	b.valueMask = 0
	b.text = b.text[:0]
	b.textReserved = 0
	b.groupReserved = 0
	b.valueReserved = 0
	b.leftReserved = 0
	b.rightReserved = 0
	b.plan = planMark{}
	b.masks = b.masks[:0]
	b.rows = b.rows[:0]
	b.scan.clearBorrowedViews()
	b.scan.cancel = nil
	b.scan.heapWorkParent = nil
	b.scan.heapWorkTextReserved = 0
	b.file.docs.Reset()
	b.file.data = b.file.data[:0]
	b.file.ends = b.file.ends[:0]
}

func (w *Workspace) resetMarkBindings() {
	for i := range w.marks {
		w.marks[i].reset()
	}
	w.eval.bindMarks(nil)
	if w.pool != nil {
		for i := range w.pool.workers {
			w.pool.workers[i].eval.bindMarks(nil)
		}
	}
}

func (s *evalScratch) bindMarks(marks []markBinding) { s.marks = marks }

func (b *markBinding) ensureSeed() {
	if !b.seeded {
		b.seed = maphash.MakeSeed()
		b.seeded = true
	}
}

func markMix(hash, part uint64, index int) uint64 {
	hash ^= bits.RotateLeft64(part+uint64(index)*0x517cc1b727220a95, (index*17+11)&63)
	return hash * 0x9ddfea08eb382d69
}

func (b *markBinding) hashInner(cols [][]scalar, row int, work *heapWorkBudget) (uint64, bool, error) {
	hash := uint64(0x9e3779b97f4a7c15)
	for i, col := range b.plan.innerKeys {
		value := cols[col][row]
		if value.kind == kindNull {
			return 0, false, nil
		}
		part, err := b.hashBuildScalar(value, work)
		if err != nil {
			return 0, false, err
		}
		hash = markMix(hash, part, i)
	}
	return hash, true, nil
}

func (b *markBinding) hashOuter(cols [][]scalar, row int, scratch *evalScratch) (uint64, bool) {
	hash := uint64(0x9e3779b97f4a7c15)
	for i, col := range b.plan.outer {
		value := cols[col][row]
		if value.kind == kindNull {
			return 0, false
		}
		part, ok := scratch.hashMarkScalar(b.seed, value)
		if !ok {
			return 0, false
		}
		hash = markMix(hash, part, i)
	}
	return hash, true
}

func (b *markBinding) sameInnerKey(group *markGroup, cols [][]scalar, row int, work *heapWorkBudget) (bool, error) {
	for i, col := range b.plan.innerKeys {
		equal, err := b.equalBuildScalars(b.keys[group.key+i], cols[col][row], work)
		if err != nil || !equal {
			return false, err
		}
	}
	return true, nil
}

func (b *markBinding) sameOuterKey(group *markGroup, cols [][]scalar, row int, scratch *evalScratch) bool {
	for i, col := range b.plan.outer {
		if !scratch.equalMarkScalars(b.keys[group.key+i], cols[col][row]) {
			return false
		}
	}
	return true
}

func (b *markBinding) findInnerGroup(hash uint64, cols [][]scalar, row int, work *heapWorkBudget) (int32, error) {
	if len(b.groupBuckets) == 0 {
		return markEmpty, nil
	}
	for group := b.groupBuckets[uint32(hash)&b.groupMask]; group != markEmpty; group = b.groups[group].next {
		g := &b.groups[group]
		if g.hash == hash {
			equal, err := b.sameInnerKey(g, cols, row, work)
			if err != nil || equal {
				return group, err
			}
		}
	}
	return markEmpty, nil
}

func (b *markBinding) findOuterGroup(hash uint64, cols [][]scalar, row int, scratch *evalScratch) int32 {
	if len(b.groupBuckets) == 0 {
		return markEmpty
	}
	for group := b.groupBuckets[uint32(hash)&b.groupMask]; group != markEmpty; group = b.groups[group].next {
		g := &b.groups[group]
		if g.hash == hash && b.sameOuterKey(g, cols, row, scratch) {
			return group
		}
	}
	return markEmpty
}

func markDirectorySize(entries int) (int, error) {
	maxInt := int(^uint(0) >> 1)
	if entries < 0 || int64(entries) > maxJoinBuildEntries || entries > maxInt/2 {
		return 0, fmt.Errorf("query: correlated mark exceeds %d addressable entries", maxJoinBuildEntries)
	}
	// resize uses an eight-element minimum capacity. Starting the logical
	// directory at four keeps the 2x directory reservation large enough to
	// cover that first physical allocation as well as every later geometric
	// replacement peak.
	want := 4
	target := entries * 2
	for want < target {
		if want > maxInt/2 {
			return 0, fmt.Errorf("query: correlated mark hash directory overflows int")
		}
		want *= 2
	}
	return want, nil
}

func (b *markBinding) growGroups(entries int, work *heapWorkBudget) error {
	want, err := markDirectorySize(entries)
	if err != nil || want <= len(b.groupBuckets) {
		return err
	}
	if work != nil {
		required := saturatedProduct(int64(want), int64(unsafe.Sizeof(int32(0)))*2)
		if err := work.admitHighWater("correlated mark group directory", required, &b.groupReserved); err != nil {
			return err
		}
	}
	b.groupBuckets = resize(b.groupBuckets, want)
	for i := range b.groupBuckets {
		b.groupBuckets[i] = markEmpty
	}
	b.groupMask = uint32(want - 1)
	for i := len(b.groups) - 1; i >= 0; i-- {
		slot := uint32(b.groups[i].hash) & b.groupMask
		b.groups[i].next = b.groupBuckets[slot]
		b.groupBuckets[slot] = int32(i)
	}
	return nil
}

func (b *markBinding) growValues(entries int, work *heapWorkBudget) error {
	want, err := markDirectorySize(entries)
	if err != nil || want <= len(b.valueBuckets) {
		return err
	}
	if work != nil {
		required := saturatedProduct(int64(want), int64(unsafe.Sizeof(int32(0)))*2)
		if err := work.admitHighWater("correlated mark value directory", required, &b.valueReserved); err != nil {
			return err
		}
	}
	b.valueBuckets = resize(b.valueBuckets, want)
	for i := range b.valueBuckets {
		b.valueBuckets[i] = markEmpty
	}
	b.valueMask = uint32(want - 1)
	for i := len(b.values) - 1; i >= 0; i-- {
		slot := uint32(b.values[i].hash) & b.valueMask
		b.values[i].next = b.valueBuckets[slot]
		b.valueBuckets[slot] = int32(i)
	}
	return nil
}

func buildMarkIndex(
	src []byte,
	entries *[]vibejson.IndexEntry,
	reserved *int64,
	work *heapWorkBudget,
) (vibejson.Index, error) {
	need, err := vibejson.RequiredIndexEntries(src)
	if err != nil {
		return vibejson.Index{}, err
	}
	if work != nil {
		required := saturatedProduct(int64(need), int64(unsafe.Sizeof(vibejson.IndexEntry{}))*2)
		if err := work.admitHighWater("correlated mark structural JSON tape", required, reserved); err != nil {
			return vibejson.Index{}, err
		}
	}
	if cap(*entries) < need {
		*entries = make([]vibejson.IndexEntry, need)
	} else {
		*entries = (*entries)[:need]
	}
	return vibejson.BuildIndex(src, *entries)
}

func (b *markBinding) hashBuildScalar(value scalar, work *heapWorkBudget) (uint64, error) {
	if value.kind != kindContainer {
		return hashJoinValue(b.seed, value), nil
	}
	index, err := buildMarkIndex(value.raw, &b.leftEntries, &b.leftReserved, work)
	if err != nil {
		return 0, err
	}
	return hashMarkNode(b.seed, index.Root()), nil
}

func (b *markBinding) equalBuildScalars(a, c scalar, work *heapWorkBudget) (bool, error) {
	if a.kind != kindContainer || c.kind != kindContainer {
		return compareScalar(a, c) == 0, nil
	}
	left, err := buildMarkIndex(a.raw, &b.leftEntries, &b.leftReserved, work)
	if err != nil {
		return false, err
	}
	right, err := buildMarkIndex(c.raw, &b.rightEntries, &b.rightReserved, work)
	if err != nil {
		return false, err
	}
	return equalMarkNodes(left.Root(), right.Root()), nil
}

func hashMarkNode(seed maphash.Seed, node vibejson.Node) uint64 {
	switch node.Kind() {
	case document.Null:
		return 0x243f6a8885a308d3
	case document.Bool:
		value, _ := node.Bool()
		if value {
			return 0x9e3779b97f4a7c15
		}
		return 0xbf58476d1ce4e5b9
	case document.Number:
		number, _ := node.NumberBytes()
		return hashExactJoinNumber(seed, number)
	case document.String:
		var hash maphash.Hash
		hash.SetSeed(seed)
		_ = hash.WriteByte(byte(document.String))
		if clean, ok := node.StringBytes(); ok {
			_, _ = hash.Write(clean)
		} else {
			raw := node.Raw().Bytes()
			iter := vibejson.JSONStringByteIter{Raw: raw[1 : len(raw)-1]}
			for {
				value, ok := iter.Next()
				if !ok {
					break
				}
				_ = hash.WriteByte(value)
			}
		}
		return hash.Sum64()
	case document.Array:
		hash := uint64(0x13198a2e03707344)
		iter, _ := node.ArrayIter()
		for index := 0; ; index++ {
			value, ok := iter.Next()
			if !ok {
				return markMix(hash, uint64(index), index)
			}
			hash = markMix(hash, hashMarkNode(seed, value), index)
		}
	case document.Object:
		// Three commutative accumulators make member order irrelevant while
		// retaining count and multiplicity. The hash only chooses a chain;
		// equalMarkNodes remains authoritative on every candidate.
		var sum, xor, square uint64
		count := 0
		iter, _ := node.ObjectIter()
		for {
			key, value, ok := iter.Next()
			if !ok {
				break
			}
			pair := markMix(hashMarkNode(seed, key), hashMarkNode(seed, value), 1)
			mixed := pair ^ bits.RotateLeft64(pair, int(pair&63))
			sum += mixed
			xor ^= mixed
			square += mixed * mixed
			count++
		}
		var hash maphash.Hash
		hash.SetSeed(seed)
		_ = hash.WriteByte(byte(document.Object))
		hashJoinUint(&hash, uint64(count))
		hashJoinUint(&hash, sum)
		hashJoinUint(&hash, xor)
		hashJoinUint(&hash, square)
		return hash.Sum64()
	default:
		return 0
	}
}

func equalMarkNodes(a, b vibejson.Node) bool {
	if a.Kind() != b.Kind() {
		return false
	}
	switch a.Kind() {
	case document.Null:
		return true
	case document.Bool:
		av, _ := a.Bool()
		bv, _ := b.Bool()
		return av == bv
	case document.Number:
		av, _ := a.NumberBytes()
		bv, _ := b.NumberBytes()
		return vibejson.JSONNumberEqual(av, bv)
	case document.String:
		return vibejson.RawJSONStringEqual(
			a.Raw().Bytes(), a.Entry.Flags(), b.Raw().Bytes(), b.Entry.Flags(),
		)
	case document.Array:
		ai, _ := a.ArrayIter()
		bi, _ := b.ArrayIter()
		for {
			av, aok := ai.Next()
			bv, bok := bi.Next()
			if aok != bok {
				return false
			}
			if !aok {
				return true
			}
			if !equalMarkNodes(av, bv) {
				return false
			}
		}
	case document.Object:
		an, _ := a.ObjectLen()
		bn, _ := b.ObjectLen()
		if an != bn {
			return false
		}
		// Canonical object order sorts decoded keys stably: order between
		// different keys is irrelevant, while duplicate values under one key
		// retain their authored order. Locate the corresponding occurrence of
		// each key without allocating a sort or matched bitmap.
		outer, _ := a.ObjectIter()
		outerIndex := 0
		for {
			key, value, ok := outer.Next()
			if !ok {
				return true
			}
			occurrence := 0
			prefix, _ := a.ObjectIter()
			for index := 0; index <= outerIndex; index++ {
				candidate, _, _ := prefix.Next()
				if equalMarkNodes(key, candidate) {
					occurrence++
				}
			}
			right, _ := b.ObjectIter()
			seen, found := 0, false
			for {
				rk, rv, ok := right.Next()
				if !ok {
					break
				}
				if !equalMarkNodes(key, rk) {
					continue
				}
				seen++
				if seen == occurrence {
					found = equalMarkNodes(value, rv)
					break
				}
			}
			if !found {
				return false
			}
			outerIndex++
		}
	default:
		return false
	}
}

func (s *evalScratch) hashMarkScalar(seed maphash.Seed, value scalar) (uint64, bool) {
	if value.kind != kindContainer {
		return hashJoinValue(seed, value), true
	}
	index, err := buildMarkIndex(value.raw, &s.markLeftEntries, &s.markLeftReserved, s.work)
	if err != nil {
		s.parkError(err)
		return 0, false
	}
	return hashMarkNode(seed, index.Root()), true
}

func (s *evalScratch) equalMarkScalars(a, b scalar) bool {
	if a.kind != kindContainer || b.kind != kindContainer {
		return compareScalar(a, b) == 0
	}
	left, err := buildMarkIndex(a.raw, &s.markLeftEntries, &s.markLeftReserved, s.work)
	if err != nil {
		s.parkError(err)
		return false
	}
	right, err := buildMarkIndex(b.raw, &s.markRightEntries, &s.markRightReserved, s.work)
	if err != nil {
		s.parkError(err)
		return false
	}
	return equalMarkNodes(left.Root(), right.Root())
}

func markScalarPayloadBytes(value scalar) int {
	switch value.kind {
	case kindNumber:
		return len(value.num)
	case kindString:
		return len(value.sval)
	case kindContainer:
		return len(value.raw)
	default:
		return 0
	}
}

// reserveText makes the next retained-value append safe for every scalar that
// already views the arena. Letting append grow text by itself would leave
// earlier scalars keeping each abandoned geometric generation alive. Growing
// under our control admits the exact old+new peak, copies once, and rebases all
// views so the old generation is immediately unreachable and the final arena
// is reusable by a warm execution.
func (b *markBinding) reserveText(additional int, work *heapWorkBudget) error {
	if additional < 0 || len(b.text) > int(^uint(0)>>1)-additional {
		return fmt.Errorf("query: correlated mark retained values exceed the address space")
	}
	required := len(b.text) + additional
	if required <= cap(b.text) {
		if work == nil {
			return nil
		}
		return work.admitHighWater(
			"correlated mark retained values", int64(required), &b.textReserved,
		)
	}

	nextCapacity := required
	if nextCapacity < 64 {
		nextCapacity = 64
	}
	if current := cap(b.text); current <= int(^uint(0)>>1)/2 {
		if doubled := current * 2; doubled > nextCapacity {
			nextCapacity = doubled
		}
	}
	peak := saturatedBytes(int64(cap(b.text)), int64(nextCapacity))
	if work != nil {
		if err := work.admitHighWater(
			"correlated mark retained values", peak, &b.textReserved,
		); err != nil {
			return err
		}
	}

	old := b.text
	next := make([]byte, len(old), nextCapacity)
	copy(next, old)
	for i := range b.keys {
		rebaseMarkScalar(&b.keys[i], old, next)
	}
	for i := range b.values {
		rebaseMarkScalar(&b.values[i].value, old, next)
	}
	for i := range b.groups {
		rebaseMarkScalar(&b.groups[i].first, old, next)
	}
	b.text = next
	return nil
}

func rebaseMarkScalar(value *scalar, old, next []byte) {
	n := markScalarPayloadBytes(*value)
	if n == 0 {
		return
	}
	base := uintptr(unsafe.Pointer(unsafe.SliceData(old)))
	var address uintptr
	switch value.kind {
	case kindNumber:
		address = uintptr(unsafe.Pointer(unsafe.SliceData(value.num)))
	case kindString:
		address = uintptr(unsafe.Pointer(unsafe.StringData(value.sval)))
	case kindContainer:
		address = uintptr(unsafe.Pointer(unsafe.SliceData(value.raw)))
	default:
		return
	}
	start := int(address - base)
	owned := next[start : start+n : start+n]
	switch value.kind {
	case kindNumber:
		value.num, value.raw = owned, owned
	case kindString:
		value.sval = byteview.String(owned)
	case kindContainer:
		value.raw = owned
	}
}

func (b *markBinding) copyScalar(value scalar, work *heapWorkBudget) (scalar, error) {
	var src []byte
	switch value.kind {
	case kindNumber:
		src = value.num
	case kindString:
		src = byteview.Bytes(value.sval)
	case kindContainer:
		src = value.raw
	default:
		return scalar{kind: value.kind, bval: value.bval, isInt: value.isInt, ival: value.ival}, nil
	}
	if err := b.reserveText(len(src), work); err != nil {
		return scalar{}, err
	}
	start := len(b.text)
	b.text = append(b.text, src...)
	owned := b.text[start:len(b.text):len(b.text)]
	switch value.kind {
	case kindNumber:
		return scalar{kind: kindNumber, num: owned, raw: owned, isInt: value.isInt, ival: value.ival}, nil
	case kindString:
		return scalar{kind: kindString, sval: byteview.String(owned)}, nil
	default:
		return scalar{kind: kindContainer, raw: owned}, nil
	}
}

func (b *markBinding) addGroup(hash uint64, cols [][]scalar, row int, work *heapWorkBudget) (int32, error) {
	if len(b.groups) >= int64ToInt(maxJoinBuildEntries) {
		return markEmpty, fmt.Errorf("query: correlated mark exceeds %d groups", maxJoinBuildEntries)
	}
	if err := b.growGroups(len(b.groups)+1, work); err != nil {
		return markEmpty, err
	}
	if work != nil {
		fixed := int64(unsafe.Sizeof(markGroup{})) +
			int64(len(b.plan.innerKeys))*int64(unsafe.Sizeof(scalar{}))
		if err := work.admit("correlated mark grouped state", saturatedProduct(fixed, 2)); err != nil {
			return markEmpty, err
		}
	}
	base := len(b.keys)
	for _, col := range b.plan.innerKeys {
		value, err := b.copyScalar(cols[col][row], work)
		if err != nil {
			return markEmpty, err
		}
		b.keys = append(b.keys, value)
	}
	id := int32(len(b.groups))
	slot := uint32(hash) & b.groupMask
	b.groups = append(b.groups, markGroup{hash: hash, key: base, next: b.groupBuckets[slot]})
	b.groupBuckets[slot] = id
	return id, nil
}

func (b *markBinding) valueHashBuild(group int32, value scalar, work *heapWorkBudget) (uint64, error) {
	part, err := b.hashBuildScalar(value, work)
	if err != nil {
		return 0, err
	}
	return markMix(b.groups[group].hash, part, len(b.plan.innerKeys)), nil
}

func (b *markBinding) hasValueBuild(group int32, hash uint64, value scalar, work *heapWorkBudget) (bool, error) {
	if len(b.valueBuckets) == 0 {
		return false, nil
	}
	for entry := b.valueBuckets[uint32(hash)&b.valueMask]; entry != markEmpty; entry = b.values[entry].next {
		candidate := &b.values[entry]
		if candidate.group == group && candidate.hash == hash {
			equal, err := b.equalBuildScalars(candidate.value, value, work)
			if err != nil || equal {
				return equal, err
			}
		}
	}
	return false, nil
}

func (b *markBinding) valueHashProbe(group int32, value scalar, scratch *evalScratch) (uint64, bool) {
	part, ok := scratch.hashMarkScalar(b.seed, value)
	if !ok {
		return 0, false
	}
	return markMix(b.groups[group].hash, part, len(b.plan.innerKeys)), true
}

func (b *markBinding) hasValueProbe(group int32, hash uint64, value scalar, scratch *evalScratch) bool {
	if len(b.valueBuckets) == 0 {
		return false
	}
	for entry := b.valueBuckets[uint32(hash)&b.valueMask]; entry != markEmpty; entry = b.values[entry].next {
		candidate := &b.values[entry]
		if candidate.group == group && candidate.hash == hash &&
			scratch.equalMarkScalars(candidate.value, value) {
			return true
		}
	}
	return false
}

func (b *markBinding) addValue(group int32, value scalar, work *heapWorkBudget) error {
	hash, err := b.valueHashBuild(group, value, work)
	if err != nil {
		return err
	}
	found, err := b.hasValueBuild(group, hash, value, work)
	if err != nil {
		return err
	}
	if found {
		return nil
	}
	if len(b.values) >= int64ToInt(maxJoinBuildEntries) {
		return fmt.Errorf("query: correlated mark exceeds %d distinct values", maxJoinBuildEntries)
	}
	if err := b.growValues(len(b.values)+1, work); err != nil {
		return err
	}
	if work != nil {
		if err := work.admit(
			"correlated mark membership state",
			saturatedProduct(int64(unsafe.Sizeof(markValue{})), 2),
		); err != nil {
			return err
		}
	}
	owned, err := b.copyScalar(value, work)
	if err != nil {
		return err
	}
	id := int32(len(b.values))
	slot := uint32(hash) & b.valueMask
	b.values = append(b.values, markValue{
		hash: hash, group: group, next: b.valueBuckets[slot], value: owned,
	})
	b.valueBuckets[slot] = id
	return nil
}

func (b *markBinding) addInnerRow(cols [][]scalar, row int, work *heapWorkBudget) error {
	hash, addressable, err := b.hashInner(cols, row, work)
	if err != nil {
		return err
	}
	if !addressable {
		return nil
	}
	group, err := b.findInnerGroup(hash, cols, row, work)
	if err != nil {
		return err
	}
	if group == markEmpty {
		group, err = b.addGroup(hash, cols, row, work)
		if err != nil {
			return err
		}
	}
	g := &b.groups[group]
	if g.rows < 2 {
		g.rows++
	}
	if b.plan.value < 0 {
		return nil
	}
	projected := cols[b.plan.value][row]
	switch b.plan.kind {
	case correlatedMarkIn, correlatedMarkNotIn:
		if projected.kind == kindNull {
			g.hasNull = true
			return nil
		}
		return b.addValue(group, projected, work)
	case correlatedMarkScalar:
		if g.rows == 1 {
			first, err := b.copyScalar(projected, work)
			if err != nil {
				return err
			}
			g.first = first
		}
	}
	return nil
}

func (b *markBinding) matches(cols [][]scalar, row int, scratch *evalScratch) bool {
	hash, addressable := b.hashOuter(cols, row, scratch)
	group := markEmpty
	if addressable {
		group = b.findOuterGroup(hash, cols, row, scratch)
	}
	switch b.plan.kind {
	case correlatedMarkExists:
		return group != markEmpty
	case correlatedMarkNotExists:
		return group == markEmpty
	case correlatedMarkIn:
		if group == markEmpty {
			return false
		}
		probe := cols[b.plan.probe][row]
		if probe.kind == kindNull {
			return false
		}
		hash, ok := b.valueHashProbe(group, probe, scratch)
		return ok && b.hasValueProbe(group, hash, probe, scratch)
	case correlatedMarkNotIn:
		if group == markEmpty {
			// Equality correlation makes the child empty for every NULL/missing
			// outer key. SQL NULL NOT IN (empty) is TRUE.
			return true
		}
		probe := cols[b.plan.probe][row]
		if probe.kind == kindNull || b.groups[group].hasNull {
			return false
		}
		hash, ok := b.valueHashProbe(group, probe, scratch)
		return ok && !b.hasValueProbe(group, hash, probe, scratch)
	default: // correlatedMarkScalar
		if group == markEmpty {
			return false
		}
		g := &b.groups[group]
		// Cardinality is a property of authored rows, not distinct projected
		// values. It is checked before either operand can short-circuit on NULL,
		// but only after this outer row actually addressed the group.
		if g.rows > 1 {
			scratch.parkError(correlatedMarkCardinalityError)
			return false
		}
		probe := cols[b.plan.probe][row]
		if probe.kind == kindNull || g.first.kind == kindNull {
			return false
		}
		return evalCmp(probe, b.plan.op, g.first)
	}
}

func (p *plan) validateHeapMarkDependencies(catalog store.DatabaseSnapshot) error {
	for i := range p.marks {
		if _, ok := catalog.Collection(p.marks[i].collection); !ok {
			return fmt.Errorf(
				"query: correlated subquery: collection %q is not in the database snapshot",
				p.marks[i].collection,
			)
		}
	}
	return nil
}

func (p *plan) bindMarks(
	w *Workspace,
	outer store.Snapshot,
	catalog store.DatabaseSnapshot,
	work *heapWorkBudget,
) error {
	if len(p.marks) == 0 {
		w.eval.bindMarks(nil)
		return nil
	}
	if err := p.validateHeapMarkDependencies(catalog); err != nil {
		return err
	}
	for len(w.marks) < len(p.marks) {
		w.marks = append(w.marks, markBinding{})
	}
	if outer.Len() == 0 {
		w.eval.bindMarks(nil)
		return nil
	}
	for i := range p.marks {
		if err := cancellationCheckpoint(w.cancel, i); err != nil {
			return err
		}
		inner, _ := catalog.Collection(p.marks[i].collection)
		b := &w.marks[p.marks[i].slot]
		b.reset()
		b.plan = p.marks[i]
		b.ensureSeed()
		b.scan.cancel = w.cancel
		if err := b.collectHeap(inner, work); err != nil {
			return err
		}
	}
	w.eval.bindMarks(w.marks[:len(p.marks)])
	return nil
}

func (b *markBinding) collectHeap(inner store.Snapshot, work *heapWorkBudget) error {
	scan := &b.scan
	scan.heapWorkParent = work
	scan.heapWorkTextReserved = 0
	scan.eval.setWork(work)
	scan.eval.bindTo(nil)
	scan.eval.bindMarks(nil)
	scan.text = scan.text[:0]
	if work != nil {
		if err := work.admit(
			"correlated mark live-mask workspace",
			saturatedProduct(
				saturatedProduct(int64(inner.Chunks()), int64(unsafe.Sizeof(store.Mask{}))),
				2,
			),
		); err != nil {
			return err
		}
		if err := work.admitJoinBatch(
			b.plan.inner, min(inner.Len(), joinBatchRows), heapWorkSnapshot, false,
		); err != nil {
			return err
		}
	}
	b.masks = inner.AppendLiveMasks(b.masks[:0])
	b.rows = b.rows[:0]
	for i, mask := range b.masks {
		if err := cancellationCheckpoint(scan.cancel, i); err != nil {
			return err
		}
		for word := mask.Bits; word != 0; word &= word - 1 {
			b.rows = append(b.rows, store.Location{Chunk: mask.Chunk, Slot: uint8(bits.TrailingZeros64(word))})
			if len(b.rows) == joinBatchRows {
				if err := b.drainHeap(inner, work); err != nil {
					return err
				}
			}
		}
	}
	if err := b.drainHeap(inner, work); err != nil {
		return err
	}
	scan.clearBorrowedViews()
	return scan.checkCanceled()
}

func (b *markBinding) drainHeap(inner store.Snapshot, work *heapWorkBudget) error {
	if len(b.rows) == 0 {
		return nil
	}
	scan := &b.scan
	ctx := &scan.ctx
	ctx.s, ctx.rows = nil, len(b.rows)
	if err := ctx.extractSnapshotValues(
		b.plan.inner, inner, b.plan.inner.filterCols, b.rows, true, &scan.text, scan,
	); err != nil {
		return err
	}
	selected, err := b.plan.inner.selectRows(ctx, nil, true, scan)
	if err != nil {
		return err
	}
	if err := scan.eval.firstError(); err != nil {
		return err
	}
	for at, row := range selected {
		if err := cancellationCheckpoint(scan.cancel, at); err != nil {
			return err
		}
		if err := b.addInnerRow(ctx.values, row, work); err != nil {
			return err
		}
	}
	b.rows = b.rows[:0]
	scan.text = scan.text[:0]
	return nil
}

func (p *plan) validateFileMarkDependencies(catalog durable.DatabaseSnapshot) error {
	for i := range p.marks {
		if _, ok := catalog.Collection(p.marks[i].collection); !ok {
			return fmt.Errorf(
				"query: correlated subquery: collection %q is not in the database snapshot",
				p.marks[i].collection,
			)
		}
	}
	return nil
}

func (p *plan) bindFileMarks(
	w *Workspace,
	outer *durable.Snapshot,
	catalog durable.DatabaseSnapshot,
	work *heapWorkBudget,
) error {
	if len(p.marks) == 0 {
		w.eval.bindMarks(nil)
		return nil
	}
	if err := p.validateFileMarkDependencies(catalog); err != nil {
		return err
	}
	for len(w.marks) < len(p.marks) {
		w.marks = append(w.marks, markBinding{})
	}
	if outer == nil || outer.Len() == 0 {
		w.eval.bindMarks(nil)
		return nil
	}
	for i := range p.marks {
		if err := cancellationCheckpoint(w.cancel, i); err != nil {
			return err
		}
		inner, _ := catalog.Collection(p.marks[i].collection)
		b := &w.marks[p.marks[i].slot]
		b.reset()
		b.plan = p.marks[i]
		b.ensureSeed()
		b.scan.cancel = w.cancel
		if inner != nil {
			if err := b.collectFile(inner, work); err != nil {
				return err
			}
		}
	}
	w.eval.bindMarks(w.marks[:len(p.marks)])
	return nil
}

func (b *markBinding) collectFile(inner *durable.Snapshot, work *heapWorkBudget) error {
	scan := &b.scan
	scan.heapWorkParent = work
	scan.heapWorkTextReserved = 0
	scan.eval.setWork(work)
	scan.eval.bindTo(nil)
	scan.eval.bindMarks(nil)
	scan.text = scan.text[:0]
	b.file.data = b.file.data[:0]
	b.file.ends = b.file.ends[:0]
	var batchBudget fileJoinBatchBudget
	appendRow := func(_ []byte, value []byte) error {
		if err := scan.checkCanceled(); err != nil {
			return err
		}
		if err := batchBudget.admitRow(work, b.plan.inner, nil, value, false); err != nil {
			return err
		}
		b.file.data = append(b.file.data, value...)
		b.file.ends = append(b.file.ends, len(b.file.data))
		if len(b.file.ends) < joinBatchRows {
			return nil
		}
		if err := b.drainFile(work); err != nil {
			return err
		}
		batchBudget.resetBatch()
		return nil
	}
	var err error
	b.file.overflow, err = inner.RangeRawBuffer(b.file.overflow[:0], appendRow)
	if err != nil {
		return err
	}
	if err := b.drainFile(work); err != nil {
		return err
	}
	scan.clearBorrowedViews()
	return scan.checkCanceled()
}

func (b *markBinding) drainFile(work *heapWorkBudget) error {
	if len(b.file.ends) == 0 {
		return nil
	}
	scan := &b.scan
	b.file.docs.Reset()
	start := 0
	for row, end := range b.file.ends {
		if err := cancellationCheckpoint(scan.cancel, row); err != nil {
			return err
		}
		if _, err := b.file.docs.Append(b.file.data[start:end]); err != nil {
			return err
		}
		start = end
	}
	ctx := &scan.ctx
	ctx.s, ctx.rows = &b.file.docs, b.file.docs.Len()
	scan.text = scan.text[:0]
	if err := ctx.extract(b.plan.inner, nil, scan); err != nil {
		return err
	}
	selected, err := b.plan.inner.selectRows(ctx, nil, false, scan)
	if err != nil {
		return err
	}
	if err := scan.eval.firstError(); err != nil {
		return err
	}
	for at, row := range selected {
		if err := cancellationCheckpoint(scan.cancel, at); err != nil {
			return err
		}
		if err := b.addInnerRow(ctx.values, row, work); err != nil {
			return err
		}
	}
	b.file.data = b.file.data[:0]
	b.file.ends = b.file.ends[:0]
	scan.text = scan.text[:0]
	return nil
}
