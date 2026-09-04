package query

import (
	"cmp"
	"slices"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// A negative col encodes numeric column -col-1; other columns address raws.
// Sorting the borrowed names once per batch avoids comparing every selected
// path against every document member on wide projections. The directory is
// bounded by plan width, retained by the worker, and rebuilt for compiler reuse.
type fileColumn struct {
	name string
	col  int
}

// Durable-only scratch stays out of the heap and join workspace layouts.
type fileWorkerWorkspace struct {
	Workspace
	columns []fileColumn
}

// extractFileColumns borrows values directly from an already validated durable
// batch. The partial-result builder detaches every retained value before the
// batch ring can reuse these bytes. Nested paths and escaped field names use
// the structural extractor. No decoded-text budget is spent until every row
// has been gathered, so falling back cannot charge the batch twice.
func (worker *fileWorkerWorkspace) extractFileColumns(p *plan, batch fileBatch) (bool, error) {
	w := &worker.Workspace
	ctx := &w.ctx
	for _, paths := range [][]compiledPath{p.valuePaths, p.numPaths} {
		for _, cp := range paths {
			if !cp.single || cp.join != joinPathOuter {
				return false, nil
			}
		}
	}
	worker.columns = resize(worker.columns, len(p.valuePaths)+len(p.numPaths))
	for c, cp := range p.valuePaths {
		worker.columns[c] = fileColumn{name: cp.name, col: c}
	}
	for c, cp := range p.numPaths {
		worker.columns[len(p.valuePaths)+c] = fileColumn{name: cp.name, col: -c - 1}
	}
	slices.SortFunc(worker.columns, func(a, b fileColumn) int { return cmp.Compare(a.name, b.name) })
	ctx.s, ctx.rows = nil, len(batch.ends)
	w.raws = resize(w.raws, len(p.valuePaths))
	ctx.values = resize(ctx.values, len(p.valuePaths))
	for c := range w.raws {
		w.raws[c] = resize(w.raws[c], ctx.rows)
		clear(w.raws[c])
	}
	ctx.nums = resize(ctx.nums, len(p.numPaths))
	for c := range ctx.nums {
		ctx.nums[c].vals = resize(ctx.nums[c].vals, ctx.rows)
		clear(ctx.nums[c].vals)
	}
	start := 0
	for row, end := range batch.ends {
		if err := cancellationCheckpoint(w.cancel, row); err != nil {
			return true, err
		}
		if !ctx.gatherFileObject(batch.data[start:end], row, w, worker.columns) {
			return false, nil
		}
		start = end
	}
	for phase, cols := range [][]int{p.filterCols, p.lateCols} {
		text := &w.text
		if phase == 1 {
			text = &w.lateText
		}
		*text = (*text)[:0]
		need := 0
		for _, c := range cols {
			n, err := escapedTextBytesCancelable(w.raws[c], w.cancel)
			if err != nil {
				return true, err
			}
			need += n
		}
		if err := ctx.classifyColumns(cols, text, need, w); err != nil {
			return true, err
		}
	}
	return true, w.checkCanceled()
}

// gatherFileObject visits each member, including duplicates: the last member
// wins even when it replaces a number with null or another nonnumeric value.
// This is a walker of validated JSON, not a replacement validation boundary.
func (ctx *execCtx) gatherFileObject(src []byte, row int, w *Workspace, columns []fileColumn) bool {
	i := rawSkipSpace(src, 0)
	if i >= len(src) {
		return false
	}
	if src[i] != '{' {
		return true // Top-level field extraction from a nonobject is missing.
	}
	i = rawSkipSpace(src, i+1)
	if i < len(src) && src[i] == '}' {
		return rawSkipSpace(src, i+1) == len(src)
	}
	for i < len(src) {
		if src[i] != '"' {
			return false
		}
		keyEnd, escaped, ok := rawScanString(src, i)
		if !ok || escaped {
			return false
		}
		key := src[i+1 : keyEnd-1]
		i = rawSkipSpace(src, keyEnd)
		if i >= len(src) || src[i] != ':' {
			return false
		}
		i = rawSkipSpace(src, i+1)
		end, ok := rawSkipValue(src, i)
		if !ok {
			return false
		}
		raw := vibejson.RawValue{Src: src[i:end]}
		first, last := 0, len(columns)
		// Tiny projections are cheaper as length-first equality checks than
		// ordered string comparisons. Beyond four columns, use logarithmic
		// lookup so a wide projection never multiplies both schema widths.
		if last > 4 {
			name := byteview.String(key)
			at, found := slices.BinarySearchFunc(columns, name, func(c fileColumn, name string) int {
				return cmp.Compare(c.name, name)
			})
			first, last = at, at
			if found {
				for last < len(columns) && columns[last].name == name {
					last++
				}
			}
		}
		for at := first; at < last; at++ {
			if rawEqualString(key, columns[at].name) {
				c := columns[at].col
				if c >= 0 {
					w.raws[c][row] = raw
					continue
				}
				value := scalar{}
				if raw.Kind() == document.Number {
					value = classifyRawInto(raw, nil)
				}
				ctx.nums[-c-1].vals[row] = value
			}
		}
		i = rawSkipSpace(src, end)
		if i >= len(src) {
			return false
		}
		switch src[i] {
		case '}':
			return rawSkipSpace(src, i+1) == len(src)
		case ',':
			i = rawSkipSpace(src, i+1)
		default:
			return false
		}
	}
	return false
}
