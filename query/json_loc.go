package query

import "strconv"

// Error breadcrumbs.
//
// The lowering reports where in a query document a problem is: "select[1]",
// "where.score.$gte", "orderBy[0].team". Building those strings as the walk
// descends — which is what this front end originally did, with one Sprintf per
// object member and array element — spends allocations on the success path to
// describe errors that never happen.
//
// A loc is the path instead of its rendering: a fixed-size list of steps
// passed and copied by value, never heap-allocated, and turned into a string
// only by the error constructors that need one.

// locSteps bounds a breadcrumb's depth. The grammar's deepest position is a
// membership alternative inside a join's inner filter —
// join[i].where.<path>.$in[j] — at six steps, so the bound is reached exactly
// and never exceeded. A deeper path would silently stop recording rather than
// misreport, which locStep's overflow guard preserves.
const locSteps = 6

// A locStep is one component of a breadcrumb: a clause or operator name, a
// document member, or an element index.
type locStep struct {
	name  string
	key   qkey
	index int
	isKey bool
	isNum bool
}

// A loc is a breadcrumb into a query document, rendered only on failure.
type loc struct {
	steps [locSteps]locStep
	n     int
}

// at starts a breadcrumb at a named clause.
func at(clause string) loc {
	var l loc
	return l.name(clause)
}

// push appends a step, ignoring anything past the bound so a breadcrumb can
// never overflow into a neighbouring step.
func (l loc) push(step locStep) loc {
	if l.n >= locSteps {
		return l
	}
	l.steps[l.n] = step
	l.n++
	return l
}

// name descends into a named component: a clause or an operator.
func (l loc) name(s string) loc { return l.push(locStep{name: s}) }

// field descends into a document member.
func (l loc) field(k qkey) loc { return l.push(locStep{key: k, isKey: true}) }

// elem descends into an array element.
func (l loc) elem(i int) loc { return l.push(locStep{index: i, isNum: true}) }

// String renders the breadcrumb, naming the document root when empty.
func (l loc) String() string {
	if l.n == 0 {
		return "query document"
	}
	var b []byte
	for i := range l.n {
		step := l.steps[i]
		switch {
		case step.isNum:
			b = append(b, '[')
			b = strconv.AppendInt(b, int64(step.index), 10)
			b = append(b, ']')
		case step.isKey:
			if len(b) != 0 {
				b = append(b, '.')
			}
			b = append(b, step.key.String()...)
		default:
			if len(b) != 0 {
				b = append(b, '.')
			}
			b = append(b, step.name...)
		}
	}
	return string(b)
}
