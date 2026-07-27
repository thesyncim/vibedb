package query

import (
	"math/bits"
	"slices"

	"github.com/thesyncim/vibejson/x/byteview"
	"github.com/thesyncim/vibedb/store"
)

// Snapshot-only execution lanes live here. Each lane recognizes one complete
// plan shape, produces the final Result, and otherwise declines without
// changing query semantics. The generic executor remains the sole fallback.

// runDirectSnapshotAggregate reduces top-level numeric fields without
// materializing raw, numeric, validity, or selected-row columns.
func (p *plan) runDirectSnapshotAggregate(dst *Result, snapshot store.Snapshot, w *Workspace) bool {
	if p.where != nil || p.grouped || !p.singleRow {
		return false
	}
	for _, col := range p.columns {
		switch col.agg {
		case aggCount:
			if col.value >= 0 {
				return false
			}
		case aggSum, aggAvg, aggMin, aggMax:
			if col.num < 0 || col.num >= len(p.numPaths) || !p.numPaths[col.num].single {
				return false
			}
		default:
			return false
		}
	}

	w.reductions = resize(w.reductions, len(p.numPaths))
	clear(w.reductions)
	w.reduced = resize(w.reduced, len(p.numPaths))
	clear(w.reduced)
	w.accs = resize(w.accs, len(p.columns))
	clear(w.accs)
	for c, col := range p.columns {
		if col.agg == aggCount {
			w.accs[c].count = snapshot.Len()
			continue
		}
		if !w.reduced[col.num] {
			w.reductions[col.num] = snapshot.ReduceFieldFloat64(p.numPaths[col.num].name, &w.ctx.cache)
			w.reduced[col.num] = true
		}
		r := w.reductions[col.num]
		w.accs[c] = aggAcc{n: r.Count, sum: r.Sum, min: r.Min, max: r.Max}
	}

	rows := 1
	if p.hasLimit && p.limit == 0 {
		rows = 0
	}
	prepareResult(dst, p, rows)
	if rows != 0 {
		p.fillAggregateCells(dst, 0, w.accs, nil, w)
	}
	return true
}

// runDirectSnapshotIndexedCount answers COUNT(*) from exact index masks when
// the complete predicate is indexable. It deliberately asks for collision-
// rechecked masks: unlike the generic executor there is no later JSON
// predicate pass to remove candidate false positives.
func (p *plan) runDirectSnapshotIndexedCount(dst *Result, snapshot store.Snapshot, w *Workspace) (bool, error) {
	if p.where == nil || p.grouped || !p.singleRow {
		return false, nil
	}
	for _, col := range p.columns {
		if col.agg != aggCount || col.value >= 0 {
			return false, nil
		}
	}
	masks, exact, err := p.storeCandidateMasksMode(snapshot, w, true)
	if err != nil {
		return true, err
	}
	if masks == nil || !exact {
		return false, nil
	}
	count := 0
	for _, mask := range masks {
		count += bits.OnesCount64(mask.Bits)
	}
	w.accs = resize(w.accs, len(p.columns))
	for i := range w.accs {
		w.accs[i] = aggAcc{count: count}
	}
	rows := 1
	if p.hasLimit && p.limit == 0 {
		rows = 0
	}
	prepareResult(dst, p, rows)
	if rows != 0 {
		p.fillAggregateCells(dst, 0, w.accs, nil, w)
	}
	return true, nil
}

// runDirectSnapshotStringCountGroups lowers a categorical COUNT GROUP BY to
// one borrowed field gather and one pointer-free dense-ID table. Missing and
// null form one group. Other value kinds decline to the generic executor.
func (p *plan) runDirectSnapshotStringCountGroups(dst *Result, snapshot store.Snapshot, w *Workspace) (bool, error) {
	if p.where != nil || !p.grouped || len(p.groupCols) != 1 || len(p.columns) != 2 {
		return false, nil
	}
	projected, countColumn := false, -1
	for i, col := range p.columns {
		switch {
		case col.agg == aggNone && col.slot == 0:
			projected = true
		case col.agg == aggCount && col.value < 0:
			countColumn = i
		default:
			return false, nil
		}
	}
	path := p.valuePaths[p.groupCols[0]]
	if !projected || countColumn < 0 || !path.single {
		return false, nil
	}

	// The fused field gather, not the general pointer walk: path.single is
	// already required above, and the two disagree on a document whose root is
	// an array, where a named pointer token is an error and an absent member is
	// simply absent. Every other extraction of a single top-level field —
	// Segment scans, snapshot scans, and narrowed snapshot gathers — routes
	// through the field primitive, so this lane must too or a GROUP BY over a
	// collection holding one array-rooted document fails where the identical
	// query over the same documents in a Segment succeeds.
	w.raws = resize(w.raws, 1)
	raws := snapshot.AppendField(w.raws[0][:0], path.name, &w.ctx.cache)
	w.raws[0] = raws
	w.resetStringGroups()
	groupCount := 0
	for _, raw := range raws {
		var value scalar
		var hash uint32
		if content, clean := raw.StringBytes(); clean {
			value = scalar{kind: kindString, sval: byteview.String(content), raw: raw.Bytes()}
			hash = stringGroupHash(value.sval)
		} else if len(raw.Bytes()) == 0 || raw.IsNull() {
			value = scalar{kind: kindNull, raw: raw.Bytes()}
			hash = 0x9e3779b9
		} else {
			return false, nil
		}

		id, fresh := w.internCategoricalGroup(value, hash, groupCount)
		if fresh {
			w.groups = resize(w.groups, groupCount+1)
			g := &w.groups[id]
			g.scalars = resize(g.scalars, 1)
			g.scalars[0] = value
			g.accs = resize(g.accs, len(p.columns))
			clear(g.accs)
			groupCount++
		}
		w.groups[id].accs[countColumn].count++
	}

	w.groups = w.groups[:groupCount]
	w.groupOrder = resize(w.groupOrder[:0], groupCount)
	for i := range w.groupOrder {
		w.groupOrder[i] = i
	}
	if len(p.order) != 0 {
		slices.SortStableFunc(w.groupOrder, func(a, b int) int {
			return p.compareGroups(&w.groups[a], &w.groups[b])
		})
	}
	if p.hasLimit && len(w.groupOrder) > p.limit {
		w.groupOrder = w.groupOrder[:p.limit]
	}
	prepareResult(dst, p, len(w.groupOrder))
	for row, id := range w.groupOrder {
		g := &w.groups[id]
		p.fillAggregateCells(dst, row, g.accs, g, w)
	}
	return true, nil
}

func (w *Workspace) resetStringGroups() {
	if len(w.stringSlot) == 0 {
		w.stringSlot = make([]uint32, 64)
	} else {
		clear(w.stringSlot)
	}
	w.stringHash = w.stringHash[:0]
}

func (w *Workspace) internCategoricalGroup(value scalar, hash uint32, count int) (int, bool) {
	for {
		mask := uint32(len(w.stringSlot) - 1)
		for slot := hash & mask; ; slot = (slot + 1) & mask {
			stored := w.stringSlot[slot]
			if stored == 0 {
				if (count+1)*4 >= len(w.stringSlot)*3 {
					w.growStringGroups()
					break
				}
				w.stringSlot[slot] = uint32(count) + 1
				w.stringHash = append(w.stringHash, hash)
				return count, true
			}
			id := int(stored - 1)
			storedValue := w.groups[id].scalars[0]
			if w.stringHash[id] == hash && storedValue.kind == value.kind && storedValue.sval == value.sval {
				return id, false
			}
		}
	}
}

func (w *Workspace) growStringGroups() {
	table := make([]uint32, len(w.stringSlot)*2)
	mask := uint32(len(table) - 1)
	for id, hash := range w.stringHash {
		slot := hash & mask
		for table[slot] != 0 {
			slot = (slot + 1) & mask
		}
		table[slot] = uint32(id) + 1
	}
	w.stringSlot = table
}

func stringGroupHash(text string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(text); i++ {
		hash = (hash ^ uint32(text[i])) * 16777619
	}
	hash ^= hash >> 16
	hash *= 0x7feb352d
	return hash ^ hash>>15
}
