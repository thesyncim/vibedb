package store

import "github.com/thesyncim/vibejson"

// Snapshot column gathers used by the query engine. They preserve collection scan
// order while delegating JSON path resolution to each immutable Segment chunk,
// so shape tapes, compiled keys, value dictionaries, and duplicate-key
// semantics stay centralized in the existing column primitives.

// AppendField appends the top-level object member named name for every live
// row in collection order. An absent member appends a zero RawValue. It is the
// collection-wide form of [ShapeCache.AppendField]: shape and structural-template
// routing are reused independently in every immutable chunk, and values
// borrow s. cache belongs to the calling worker; nil uses an invocation-local
// cache. With sufficient dst capacity the operation allocates nothing.
func (s Snapshot) AppendField(dst []vibejson.RawValue, name string, cache *ShapeCache) []vibejson.RawValue {
	if s.state == nil {
		return dst
	}
	var local ShapeCache
	if cache == nil {
		cache = &local
	}
	s.state.Chunks.Each(func(_ uint32, chunk *Chunk) bool {
		dst = cache.AppendField(dst, &chunk.Docs, name)
		return true
	})
	return dst
}

// AppendPointer appends one value per live Snapshot row in chunk/slot order.
// An absent path appends a zero RawValue. Values borrow s. With sufficient dst
// capacity the operation allocates nothing.
func (s Snapshot) AppendPointer(dst []vibejson.RawValue, pointer vibejson.CompiledPointer) ([]vibejson.RawValue, error) {
	mark := len(dst)
	if s.state == nil {
		return dst, nil
	}
	var scanErr error
	s.state.Chunks.Each(func(_ uint32, chunk *Chunk) bool {
		var err error
		dst, err = chunk.Docs.AppendPointer(dst, pointer)
		if err != nil {
			scanErr = err
			return false
		}
		return true
	})
	if scanErr != nil {
		return dst[:mark], scanErr
	}
	return dst, nil
}

// AppendFieldFloat64 appends one typed numeric cell and validity bit per live
// row for a top-level object field. It is the fused collection scan counterpart of
// [ShapeCache.AppendFieldFloat64]: shape routing, span lookup, and number
// parsing happen in one pass without a []RawValue intermediate. cache belongs
// to the calling worker; nil uses an invocation-local cache. With sufficient
// output capacity the operation allocates nothing.
func (s Snapshot) AppendFieldFloat64(dst []float64, valid []bool, name string, cache *ShapeCache) ([]float64, []bool) {
	if s.state == nil {
		return dst, valid
	}
	var local ShapeCache
	if cache == nil {
		cache = &local
	}
	s.state.Chunks.Each(func(_ uint32, chunk *Chunk) bool {
		dst, valid = cache.AppendFieldFloat64(dst, valid, &chunk.Docs, name)
		return true
	})
	return dst, valid
}

// ReduceFieldFloat64 fuses one top-level numeric field scan into aggregate
// state without materializing a RawValue, float, validity, or row column.
// cache has the same ownership contract as AppendFieldFloat64.
func (s Snapshot) ReduceFieldFloat64(name string, cache *ShapeCache) Float64Aggregate {
	var total Float64Aggregate
	if s.state == nil {
		return total
	}
	var local ShapeCache
	if cache == nil {
		cache = &local
	}
	s.state.Chunks.Each(func(_ uint32, chunk *Chunk) bool {
		part := cache.ReduceFieldFloat64(&chunk.Docs, name)
		if part.Count != 0 {
			if total.Count == 0 {
				total.Min, total.Max = part.Min, part.Max
			} else {
				if part.Min < total.Min {
					total.Min = part.Min
				}
				if part.Max > total.Max {
					total.Max = part.Max
				}
			}
			total.Count += part.Count
			total.Sum += part.Sum
		}
		return true
	})
	return total
}

// AppendPointerRows is the sparse-gather form of [Snapshot.AppendPointer].
// Rows may be in any order and may repeat. Invalid or stale addresses panic;
// callers must use rows produced by this Snapshot.
func (s Snapshot) AppendPointerRows(dst []vibejson.RawValue, rows []Location, pointer vibejson.CompiledPointer) ([]vibejson.RawValue, error) {
	mark := len(dst)
	for first := 0; first < len(rows); {
		chunkID := rows[first].Chunk
		chunk := (*Chunk)(nil)
		if s.state != nil {
			chunk = s.state.Chunks.Get(chunkID)
		}
		if chunk == nil {
			panic("vibejson: Location chunk is not live in Snapshot")
		}
		var ords [MaxChunkDocuments]int
		last := first
		for last < len(rows) && rows[last].Chunk == chunkID && last-first < len(ords) {
			slot := int(rows[last].Slot)
			if slot >= len(chunk.Ord) || chunk.Live&(uint64(1)<<uint(slot)) == 0 {
				panic("vibejson: Location slot is not live in Snapshot")
			}
			ords[last-first] = int(chunk.Ord[slot])
			last++
		}
		var err error
		dst, err = chunk.Docs.AppendPointerRows(dst, ords[:last-first], pointer)
		if err != nil {
			return dst[:mark], err
		}
		first = last
	}
	return dst, nil
}

// AppendFieldRows is the sparse-gather form of [Snapshot.AppendField]: it
// resolves one top-level member name for the addressed rows only. Rows must be
// grouped by chunk in ascending order, exactly as [Snapshot.AppendPointerRows]
// requires, and invalid or stale addresses panic.
//
// It exists so a query that narrows to a row set still resolves a single-field
// path through the fused shape-routed column primitive rather than through the
// general JSON Pointer walk. That is not only faster: the two disagree on a
// document whose root is an array, where a named pointer token is a pointer
// error and an absent member is simply absent. Routing both the full-scan and
// the narrowed extraction through the same primitive is what keeps a query's
// result — and whether it reports an error at all — independent of how much of
// the collection the planner decided to read.
func (s Snapshot) AppendFieldRows(dst []vibejson.RawValue, rows []Location, name string, cache *ShapeCache) []vibejson.RawValue {
	var local ShapeCache
	if cache == nil {
		cache = &local
	}
	for first := 0; first < len(rows); {
		chunkID := rows[first].Chunk
		chunk := (*Chunk)(nil)
		if s.state != nil {
			chunk = s.state.Chunks.Get(chunkID)
		}
		if chunk == nil {
			panic("vibejson: Location chunk is not live in Snapshot")
		}
		var ords [MaxChunkDocuments]int
		last := first
		for last < len(rows) && rows[last].Chunk == chunkID && last-first < len(ords) {
			slot := int(rows[last].Slot)
			if slot >= len(chunk.Ord) || chunk.Live&(uint64(1)<<uint(slot)) == 0 {
				panic("vibejson: Location slot is not live in Snapshot")
			}
			ords[last-first] = int(chunk.Ord[slot])
			last++
		}
		dst = cache.AppendFieldRows(dst, &chunk.Docs, ords[:last-first], name)
		first = last
	}
	return dst
}

// AppendRowKeys appends the keys addressed by rows. With sufficient capacity
// it allocates nothing.
func (s Snapshot) AppendRowKeys(dst []string, rows []Location) []string {
	for _, row := range rows {
		chunk := (*Chunk)(nil)
		if s.state != nil {
			chunk = s.state.Chunks.Get(row.Chunk)
		}
		if chunk == nil || int(row.Slot) >= len(chunk.Ord) || chunk.Live&(uint64(1)<<row.Slot) == 0 {
			panic("vibejson: Location is not live in Snapshot")
		}
		dst = append(dst, chunk.Key(int(row.Slot)))
	}
	return dst
}
