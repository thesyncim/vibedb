package query

import (
	"fmt"

	sqlast "github.com/thesyncim/vibedb/sql"
)

// lateralPrepareFrame is the cold lexical link between a LATERAL child being
// prepared and the APPLY adapter that invokes it. The SQL AST deliberately
// keeps relation-local bindings: a depth-two binding on a nested LATERAL is
// the same value as a depth-one binding retained by its containing LATERAL.
// Linking those prepared slots preserves that identity without flattening
// aliases, renumbering authored placeholders, or adding state to statements
// that do not use nested correlation.
type lateralPrepareFrame struct {
	apply *statementLateral
}

// lateralInheritedBinding names one immutable prepared slot in the containing
// APPLY adapter. The scalar stored in that slot remains live for the complete
// nested child execution; no bytes are copied and no independent budget charge
// is invented for the borrowed value.
type lateralInheritedBinding struct {
	apply   *statementLateral
	binding int
}

func (f *lateralPrepareFrame) resolve(
	text string,
	binding *sqlast.LateralBinding,
) (lateralInheritedBinding, error) {
	if f == nil || f.apply == nil || binding == nil || binding.Depth <= 1 {
		pos := 0
		if binding != nil {
			pos = binding.Pos
		}
		return lateralInheritedBinding{}, sqlast.NewFeatureNotSupportedError(
			text, pos,
			"nested LATERAL correlation has no containing APPLY frame",
		)
	}
	ancestor := *binding
	ancestor.Depth--
	for i := range f.apply.spec.Bindings {
		candidate := &f.apply.spec.Bindings[i]
		if candidate.Depth == ancestor.Depth &&
			candidate.Source == ancestor.Source &&
			lateralSegmentsEqual(candidate.Segments, ancestor.Segments) {
			if i >= len(f.apply.bindingUse) {
				return lateralInheritedBinding{}, fmt.Errorf(
					"query: containing LATERAL binding state is incomplete",
				)
			}
			f.apply.bindingUse[i] = true
			return lateralInheritedBinding{apply: f.apply, binding: i}, nil
		}
	}
	return lateralInheritedBinding{}, sqlast.NewFeatureNotSupportedError(
		text, binding.Pos,
		"nested LATERAL binding is absent from its containing lexical APPLY frame",
	)
}

func (b lateralInheritedBinding) scalar() (scalar, error) {
	if b.apply == nil || !b.apply.bindingReady ||
		b.binding < 0 || b.binding >= len(b.apply.slots) {
		return scalar{}, fmt.Errorf(
			"query: inherited LATERAL binding is not active",
		)
	}
	return b.apply.slots[b.binding].value, nil
}
