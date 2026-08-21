package query

import "github.com/thesyncim/vibedb/store/durable"

// fileDataSkippingFilter extracts only conjunctive immutable scalar
// comparisons. OR/NOT and dynamic bounds are ignored wholesale at their node:
// using one arm to reject a stripe that another arm could match would be
// unsound. The complete predicate remains the row authority for kept stripes.
func (p *plan) fileDataSkippingFilter(
	snapshot *durable.Snapshot,
	filter *durable.DataSkippingFilter,
	storage []durable.DataSkippingPredicate,
) (bool, error) {
	if p == nil || p.where == nil || snapshot == nil || filter == nil {
		return false, nil
	}
	predicates := appendDataSkippingPredicates(
		storage[:0], p.where, p.valuePaths,
	)
	if len(predicates) == 0 {
		return false, nil
	}
	return snapshot.CompileDataSkippingFilter(filter, predicates)
}

func appendDataSkippingPredicates(
	dst []durable.DataSkippingPredicate,
	predicate *compiledPredicate,
	paths []compiledPath,
) []durable.DataSkippingPredicate {
	if predicate == nil || len(dst) == cap(dst) {
		return dst
	}
	switch predicate.kind {
	case predAnd:
		for _, child := range predicate.kids {
			dst = appendDataSkippingPredicates(dst, child, paths)
			if len(dst) == cap(dst) {
				break
			}
		}
		return dst
	case predCmp:
		if predicate.col < 0 || predicate.col >= len(paths) ||
			paths[predicate.col].join != joinPathOuter ||
			len(predicate.needle.Root().Raw().Bytes()) == 0 {
			return dst
		}
		op, ok := dataSkippingOp(predicate.op)
		if !ok {
			return dst
		}
		return append(dst, durable.DataSkippingPredicate{
			Path: paths[predicate.col].indexPath(),
			Op:   op, Value: predicate.needle,
		})
	default:
		return dst
	}
}

func dataSkippingOp(op Op) (durable.DataSkippingOp, bool) {
	switch op {
	case Eq:
		return durable.DataSkippingEqual, true
	case Lt:
		return durable.DataSkippingLess, true
	case Le:
		return durable.DataSkippingLessEqual, true
	case Gt:
		return durable.DataSkippingGreater, true
	case Ge:
		return durable.DataSkippingGreaterEqual, true
	default:
		return 0, false
	}
}
