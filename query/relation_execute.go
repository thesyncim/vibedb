package query

import (
	"fmt"
	"strconv"

	"github.com/thesyncim/vibejson"
)

// runRelationInto executes the existing immutable plan over a columnar
// relation spool. The dispatch is outside every row loop: ordinary Segment,
// heap, and durable executions never inspect relation state, while relation
// roots are lent directly to the mature predicate/reduction kernels without a
// JSON object encode, parse, or scalar reclassification pass.
func (p *plan) runRelationInto(
	dst *Result,
	spool *relationSpool,
	w *Workspace,
) (err error) {
	if err := w.checkCanceled(); err != nil {
		return err
	}
	if p.hasLimit && p.limit == 0 {
		return prepareResult(dst, p, 0)
	}
	if err := w.activeHeapWorkBudget().admitPlanner(
		p, spool.rows, heapWorkSegment, false, 0,
	); err != nil {
		return err
	}
	if err := w.activeHeapWorkBudget().admitRows(
		p, spool.rows, heapWorkSegment, 1,
	); err != nil {
		return err
	}

	w.candidateUsed = 0
	w.text = w.text[:0]
	w.lateText = w.lateText[:0]
	w.groupKey = w.groupKey[:0]
	w.groupOrder = w.groupOrder[:0]
	w.interner.Reset()

	ctx := &w.ctx
	ctx.s = nil
	ctx.rows = spool.rows
	// Root columns below alias spool storage. Unbind only those slice headers
	// before returning so the next generic Workspace cleanup cannot clear the
	// statement-owned relation. Nested-path columns remain Workspace-owned and
	// retain their warmed capacity.
	defer p.unbindRelationRoots(ctx)

	if err := ctx.extractRelationValues(
		p, spool, p.filterCols, &w.text, w,
	); err != nil {
		return err
	}
	selected, err := p.selectRows(ctx, nil, false, w)
	if err != nil {
		return err
	}
	if err := ctx.extractRelationValues(
		p, spool, p.lateCols, &w.lateText, w,
	); err != nil {
		return err
	}
	if err := ctx.extractRelationNums(p, spool, w); err != nil {
		return err
	}
	return p.emit(dst, ctx, selected, w)
}

func (ctx *execCtx) extractRelationValues(
	p *plan,
	spool *relationSpool,
	cols []int,
	text *[]byte,
	w *Workspace,
) error {
	w.raws = resize(w.raws, len(p.valuePaths))
	ctx.values = resize(ctx.values, len(p.valuePaths))
	if len(cols) == 0 {
		return nil
	}

	textNeed := 0
	for at, column := range cols {
		if err := cancellationCheckpoint(w.cancel, at); err != nil {
			return err
		}
		ordinal, root, suffix, err := relationPath(p.valuePaths[column], spool)
		if err != nil {
			return err
		}
		if root {
			ctx.values[column] = spool.columns[ordinal]
			continue
		}
		raws, err := appendRelationPathColumn(
			w.raws[column][:0], spool.columns[ordinal], suffix, w.cancel,
		)
		if err != nil {
			return err
		}
		w.raws[column] = raws
		escaped, err := escapedTextBytesCancelable(raws, w.cancel)
		if err != nil {
			return err
		}
		if escaped > int(^uint(0)>>1)-textNeed {
			return fmt.Errorf("query: relation decoded-text size overflows int")
		}
		textNeed += escaped
	}

	if err := w.joinPairBudget.admitText(textNeed); err != nil {
		return err
	}
	if err := w.admitDecodedText(textNeed); err != nil {
		return err
	}
	if cap(*text) < textNeed {
		*text = make([]byte, 0, growCap(cap(*text), textNeed))
	}
	for at, column := range cols {
		if err := cancellationCheckpoint(w.cancel, at); err != nil {
			return err
		}
		_, root, _, err := relationPath(p.valuePaths[column], spool)
		if err != nil {
			return err
		}
		if root {
			continue
		}
		raws := w.raws[column]
		values := resize(ctx.values[column], len(raws))
		for row, raw := range raws {
			if err := cancellationCheckpoint(w.cancel, row); err != nil {
				return err
			}
			values[row] = classifyRawInto(raw, text)
		}
		ctx.values[column] = values
	}
	return w.checkCanceled()
}

func (ctx *execCtx) extractRelationNums(
	p *plan,
	spool *relationSpool,
	w *Workspace,
) error {
	ctx.nums = resize(ctx.nums, len(p.numPaths))
	for column, path := range p.numPaths {
		if err := cancellationCheckpoint(w.cancel, column); err != nil {
			return err
		}
		ordinal, root, suffix, err := relationPath(path, spool)
		if err != nil {
			return err
		}
		if root {
			ctx.nums[column].vals = spool.columns[ordinal]
			continue
		}
		raws, err := appendRelationPathColumn(
			w.numRaws[:0], spool.columns[ordinal], suffix, w.cancel,
		)
		if err != nil {
			return err
		}
		w.numRaws = raws
		ctx.nums[column], err = numericRawsCancelable(
			ctx.nums[column], raws, w.cancel,
		)
		if err != nil {
			return err
		}
	}
	return w.checkCanceled()
}

func relationPath(
	path compiledPath,
	spool *relationSpool,
) (ordinal int, root bool, suffix vibejson.CompiledPointer, err error) {
	tokens := path.pointer.Tokens
	if len(tokens) == 0 {
		return 0, false, vibejson.CompiledPointer{}, fmt.Errorf(
			"query: relation path %q does not name an output ordinal", path.spec,
		)
	}
	ordinal, err = strconv.Atoi(tokens[0].Text)
	if err != nil || ordinal < 0 || ordinal >= len(spool.columns) {
		return 0, false, vibejson.CompiledPointer{}, fmt.Errorf(
			"query: relation path %q names invalid output ordinal %q",
			path.spec, tokens[0].Text,
		)
	}
	if len(tokens) == 1 {
		return ordinal, true, vibejson.CompiledPointer{}, nil
	}
	return ordinal, false, vibejson.CompiledPointer{
		Tokens: tokens[1:],
	}, nil
}

func appendRelationPathColumn(
	dst []vibejson.RawValue,
	column []scalar,
	pointer vibejson.CompiledPointer,
	cancel *CancelFlag,
) ([]vibejson.RawValue, error) {
	dst = resize(dst, len(column))
	for row, value := range column {
		if err := cancellationCheckpoint(cancel, row); err != nil {
			return dst[:row], err
		}
		raw := value.raw
		if len(raw) == 0 {
			raw = nullBytes
		}
		resolved, ok, err := pointer.GetRawTrusted(raw)
		if err != nil {
			return dst[:row], err
		}
		if !ok {
			dst[row] = vibejson.RawValue{}
			continue
		}
		dst[row] = resolved
	}
	return dst, cancellationError(cancel)
}

func (p *plan) unbindRelationRoots(ctx *execCtx) {
	for column, path := range p.valuePaths {
		if len(path.pointer.Tokens) == 1 && column < len(ctx.values) {
			ctx.values[column] = nil
		}
	}
	for column, path := range p.numPaths {
		if len(path.pointer.Tokens) == 1 && column < len(ctx.nums) {
			ctx.nums[column].vals = nil
		}
	}
}
