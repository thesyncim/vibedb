package query

import (
	"fmt"
	"math"
	"unsafe"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson"
)

// statementCorrelationPlan is immutable lowering metadata for one correlated
// statement. The parser keeps exact path-occurrence identity in its sidecar;
// LATERAL cloning preserves that property here by recording the exact cloned
// comparison node which consumes each slot. Live values never enter this plan.
type statementCorrelationPlan struct {
	references []statementCorrelationReference
	slots      int
}

type statementCorrelationReference struct {
	expr       *sqlast.Expr
	slot       int
	comparison boundPathComparison
}

func (p *statementCorrelationPlan) reference(
	expr *sqlast.Expr,
) (*statementCorrelationReference, bool) {
	if p == nil || expr == nil {
		return nil, false
	}
	for i := range p.references {
		if p.references[i].expr == expr {
			return &p.references[i], true
		}
	}
	return nil, false
}

// correlationCompareForm lowers a local-to-outer comparison without turning
// the outer value into an authored SQL parameter. predCmpPathBound is TRUE only
// when both operands have the same live scalar SQL domain and the comparison
// accepts; the guard is TRUE only over that same non-NULL domain. The ordinary
// leaf resolver can then derive SQL FALSE as guard AND NOT(pred), preserving
// UNKNOWN under NOT.
func (s *Statement) correlationCompareForm(
	expr *sqlast.Expr,
	reference *statementCorrelationReference,
) (leafForm, error) {
	if expr == nil || expr.Path == nil {
		return leafForm{}, fmt.Errorf("query: correlated path comparison has no local operand")
	}
	if reference == nil || s.correlation == nil || reference.slot < 0 ||
		reference.slot >= s.correlation.slots {
		slot := -1
		if reference != nil {
			slot = reference.slot
		}
		return leafForm{}, fmt.Errorf(
			"query: invalid prepared correlation slot %d", slot,
		)
	}
	spec := s.spec(expr.Path)
	return leafForm{
		pred: compareBoundPath(
			spec, Op(expr.Op), reference.slot, &reference.comparison,
		),
		guard: Predicate{
			kind: predAnd,
			kids: s.c.pair(
				s.c.not(IsNull(spec)),
				Predicate{kind: predCorrelationKnown, slot: int32(reference.slot)},
			),
		},
	}, nil
}

// bindCorrelations publishes one immutable tuple to an execution Workspace.
// The shallow scalar copy is intentional: variable-width views borrow the
// containing APPLY spool only for this synchronous child execution. The next
// clearBorrowedViews severs every view while retaining the slice capacity.
func (w *Workspace) bindCorrelations(values []scalar) error {
	if len(values) == 0 {
		w.resetCorrelationBindings()
		return nil
	}
	if cap(w.correlations) < len(values) {
		w.correlations = make([]scalar, len(values))
	} else {
		w.correlations = w.correlations[:len(values)]
	}
	copy(w.correlations, values)
	w.eval.bindCorrelations(w.correlations)
	return nil
}

// buildCorrelationNeedles renders each non-null scalar slot once per APPLY
// activation. Both Segment postings and durable exact indexes consume the same
// immutable Index value. Storage is retained at its high-water mark, so a warm
// execution performs no allocation; the active heap-work budget admits growth
// before any backing array changes.
func (w *Workspace) buildCorrelationNeedles() error {
	count := len(w.correlations)
	w.correlationNeedleBytes = 0
	if count == 0 {
		w.correlationNeedles = w.correlationNeedles[:0]
		return nil
	}
	payload := 0
	for i := range w.correlations {
		value := w.correlations[i]
		if correlationNeedleKind(value.kind) && len(value.raw) != 0 {
			continue
		}
		switch value.kind {
		case kindNull:
		case kindBool:
			payload += len(falseNeedle)
		case kindNumber:
			payload += len(value.num)
		case kindString:
			payload += jsonStringEncodedBytes(value.sval)
		}
	}
	required := int64(payload)
	required = saturatedBytes(required,
		saturatedProduct(int64(count), int64(unsafe.Sizeof(vibejson.Index{}))))
	required = saturatedBytes(required,
		saturatedProduct(int64(count+1), int64(unsafe.Sizeof(int(0)))))
	required = saturatedBytes(required,
		saturatedProduct(int64(count), int64(unsafe.Sizeof(vibejson.IndexEntry{}))))
	if required == math.MaxInt64 {
		return &WorkBudgetError{Resource: "correlation slot exact needles", Bytes: required, Limit: w.activeHeapWorkBudget().limit}
	}
	if err := w.activeHeapWorkBudget().admitHighWater(
		"correlation slot exact needles", required, &w.correlationNeedleBytes,
	); err != nil {
		return err
	}
	if cap(w.correlationNeedleText) < payload {
		w.correlationNeedleText = make([]byte, 0, payload)
	} else {
		w.correlationNeedleText = w.correlationNeedleText[:0]
	}
	w.correlationStarts = resize(w.correlationStarts, count+1)
	for i := range w.correlations {
		w.correlationStarts[i] = len(w.correlationNeedleText)
		value := w.correlations[i]
		if correlationNeedleKind(value.kind) && len(value.raw) != 0 {
			continue
		}
		switch value.kind {
		case kindNull:
		case kindBool:
			if value.bval {
				w.correlationNeedleText = append(w.correlationNeedleText, trueNeedle...)
			} else {
				w.correlationNeedleText = append(w.correlationNeedleText, falseNeedle...)
			}
		case kindNumber:
			w.correlationNeedleText = append(w.correlationNeedleText, value.num...)
		case kindString:
			w.correlationNeedleText = appendJSONString(w.correlationNeedleText, value.sval)
		}
	}
	w.correlationStarts[count] = len(w.correlationNeedleText)
	w.correlationEntries = resize(w.correlationEntries, count)
	w.correlationNeedles = resize(w.correlationNeedles, count)
	clear(w.correlationNeedles)
	for i := range w.correlations {
		if !correlationNeedleKind(w.correlations[i].kind) {
			continue
		}
		src := w.correlations[i].raw
		if len(src) == 0 {
			start, end := w.correlationStarts[i], w.correlationStarts[i+1]
			src = w.correlationNeedleText[start:end:end]
		}
		index, err := vibejson.BuildIndex(
			src,
			w.correlationEntries[i:i+1:i+1],
		)
		if err != nil {
			return fmt.Errorf("query: build correlation slot %d exact needle: %w", i, err)
		}
		w.correlationNeedles[i] = index
	}
	return nil
}

func (w *Workspace) correlationNeedle(slot int) (vibejson.Index, bool) {
	if w == nil || slot < 0 || slot >= len(w.correlations) ||
		!correlationNeedleKind(w.correlations[slot].kind) || slot >= len(w.correlationNeedles) {
		return vibejson.Index{}, false
	}
	return w.correlationNeedles[slot], true
}

func correlationNeedleKind(kind scalarKind) bool {
	return kind == kindBool || kind == kindNumber || kind == kindString
}

func (w *Workspace) resetCorrelationBindings() {
	clear(w.correlations)
	w.correlations = w.correlations[:0]
	clear(w.correlationNeedles)
	w.correlationNeedles = w.correlationNeedles[:0]
	w.correlationNeedleText = w.correlationNeedleText[:0]
	w.correlationStarts = w.correlationStarts[:0]
	w.correlationEntries = w.correlationEntries[:0]
	w.eval.bindCorrelations(nil)
	if w.pool != nil {
		for i := range w.pool.workers {
			w.pool.workers[i].eval.bindCorrelations(nil)
		}
	}
}

func (w *Workspace) correlationScalar(slot int) (scalar, error) {
	if w == nil || slot < 0 || slot >= len(w.correlations) {
		return scalar{}, fmt.Errorf("query: inherited correlation slot %d is not active", slot)
	}
	return w.correlations[slot], nil
}

func (s *evalScratch) bindCorrelations(values []scalar) {
	s.correlations = values
}
