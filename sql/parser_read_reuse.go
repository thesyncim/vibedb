package sql

import "unsafe"

// ReadPreparationRetainedBytes reports the storage a parser keeps for a
// prepared read. The count includes the Parser value, every arena directory
// and chunk capacity, and every retained clause/scratch slice capacity. It
// deliberately excludes the backing store of lexer.src and the destination
// AST: both are borrowed by the caller, while the arena bytes they reference
// are counted here. A parser with optional parser state or a cancellation hook
// declines because those states have additional ownership graphs that are not
// part of the bounded direct-read preparation contract.
//
// The method does not change the parser. Callers must clear a cancellation
// hook with SetCancellationCheck(nil) before retaining a parser, or keep the
// parser out of the reuse pool. A false result is a safe decline, never a
// partial byte estimate.
func (p *Parser) ReadPreparationRetainedBytes() (retainedBytes int64, ok bool) {
	if p == nil || !readReuseParserShape(p) {
		return 0, false
	}

	total := uint64(unsafe.Sizeof(*p))
	add := func(n int64) bool {
		if n < 0 {
			return false
		}
		const maxInt64 = uint64(^uint64(0) >> 1)
		if uint64(n) > maxInt64-total {
			return false
		}
		total += uint64(n)
		return true
	}
	addArena := func(ok *bool, n int64) {
		if *ok {
			*ok = add(n)
		}
	}
	ok = true
	addArena(&ok, readReuseChunkArenaBytes(&p.text))
	addArena(&ok, readReuseChunkArenaBytes(&p.exprs))
	addArena(&ok, readReuseChunkArenaBytes(&p.kids))
	addArena(&ok, readReuseChunkArenaBytes(&p.ops))
	addArena(&ok, readReuseChunkArenaBytes(&p.paths))
	addArena(&ok, readReuseChunkArenaBytes(&p.segs))
	addArena(&ok, readReuseChunkArenaBytes(&p.conds))
	addArena(&ok, readReuseChunkArenaBytes(&p.keys))
	addArena(&ok, readReuseChunkArenaBytes(&p.ctes))
	addArena(&ok, readReuseChunkArenaBytes(&p.names))
	addArena(&ok, readReuseChunkArenaBytes(&p.ints))

	addSlice := func(n int64) {
		if ok {
			ok = add(n)
		}
	}
	addSlice(readReuseSliceBytes(p.columns))
	addSlice(readReuseSliceBytes(p.from))
	addSlice(readReuseSliceBytes(p.groupBy))
	addSlice(readReuseSliceBytes(p.orderBy))
	addSlice(readReuseSliceBytes(p.rows))
	addSlice(readReuseSliceBytes(p.updateAssignments))
	addSlice(readReuseSliceBytes(p.conflictAssignments))
	addSlice(readReuseSliceBytes(p.cols))
	addSlice(readReuseSliceBytes(p.keyPaths))
	addSlice(readReuseSliceBytes(p.idxPaths))
	addSlice(readReuseSliceBytes(p.cteScratch))
	addSlice(readReuseSliceBytes(p.cteNameScratch))
	addSlice(readReuseSliceBytes(p.cteAliasPosScratch))
	addSlice(readReuseSliceBytes(p.joinKeyScratch))
	addSlice(readReuseSliceBytes(p.joinNameScratch))
	addSlice(readReuseSliceBytes(p.filterColumns))
	addSlice(readReuseSliceBytes(p.filterFrom))
	addSlice(readReuseSliceBytes(p.filterGroupBy))
	addSlice(readReuseSliceBytes(p.filterOrderBy))
	addSlice(readReuseSliceBytes(p.mutationOrderBy))
	addSlice(readReuseSliceBytes(p.segScratch))
	addSlice(readReuseSliceBytes(p.kidStack))
	addSlice(readReuseSliceBytes(p.opScratch))
	addSlice(readReuseSliceBytes(p.pending))
	addSlice(readReuseSliceBytes(p.tmp))
	if !ok {
		return 0, false
	}
	return int64(total), true
}

// readReuseParserShape is intentionally conservative. The bounded native-read
// lane only needs the ordinary SELECT parser state; rejecting an optional state
// avoids claiming ownership for nested parser arenas, correlation links,
// window/set/scalar state, or a cancellation closure that can retain a context.
func readReuseParserShape(p *Parser) bool {
	return p.cancel == nil &&
		p.lx.cancel == nil &&
		p.lx.cancelErr == nil &&
		p.nested == nil &&
		p.window == nil &&
		p.correlation == nil &&
		p.set == nil &&
		p.scalar == nil &&
		p.hiddenMutationTable == "" && p.hiddenMutationAlias == "" &&
		p.outerCTEs == nil &&
		p.activeCTEs.outer == nil &&
		len(p.activeCTEs.defs) == 0 &&
		cap(p.activeCTEs.defs) == 0
}

func readReuseSliceBytes[T any](slice []T) int64 {
	return readReuseProduct(cap(slice), unsafe.Sizeof(*new(T)))
}

func readReuseChunkArenaBytes[T any](arena *chunkArena[T]) int64 {
	if arena == nil {
		return 0
	}
	total := readReuseProduct(cap(arena.chunks), unsafe.Sizeof([]T{}))
	for _, chunk := range arena.chunks {
		total = readReuseAdd(total, readReuseProduct(cap(chunk), unsafe.Sizeof(*new(T))))
	}
	return total
}

func readReuseProduct(count int, size uintptr) int64 {
	if count < 0 {
		return -1
	}
	if count == 0 || size == 0 {
		return 0
	}
	const maxInt64 = uint64(^uint64(0) >> 1)
	product := uint64(count) * uint64(size)
	if product/uint64(count) != uint64(size) || product > maxInt64 {
		return -1
	}
	return int64(product)
}

func readReuseAdd(a, b int64) int64 {
	if a < 0 || b < 0 {
		return -1
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	if a > maxInt64-b {
		return -1
	}
	return a + b
}
