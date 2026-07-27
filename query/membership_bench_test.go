package query

import (
	"fmt"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// Membership against the disjunction of equalities it replaces.
//
// BenchmarkMembership is the end-to-end check that the two are now the same
// query: compilePredicate rewrites a disjunction of equalities on one path
// into a membership, so Or and In should measure alike at every size. A gap
// that opens as the set grows means the rewrite stopped firing.
//
// BenchmarkMembershipEval isolates what the rewrite buys, which the end-to-end
// form cannot: per row, a disjunction compares against its alternatives until
// one matches, while a membership searches a sorted set. Measuring it at the
// predicate level keeps column extraction — identical for both, and the
// dominant cost of a scan — out of the number.

func benchmarkMembershipSegment(b *testing.B, rows int) *store.Segment {
	b.Helper()
	set := &store.Segment{}
	for i := range rows {
		if _, err := set.Append(fmt.Appendf(nil, `{"id":%d,"pad":"xxxxxxxxxxxx"}`, i)); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
	return set
}

// membershipValues returns size alternatives drawn from a 4096-row domain,
// spread so the set is neither a prefix nor a contiguous run.
func membershipValues(size int) []any {
	values := make([]any, 0, size)
	for i := range size {
		values = append(values, (i*7)%4096)
	}
	return values
}

func BenchmarkMembership(b *testing.B) {
	const rows = 4096
	set := benchmarkMembershipSegment(b, rows)

	for _, size := range []int{4, 16, 64, 256} {
		values := membershipValues(size)

		alternatives := make([]Predicate, 0, size)
		for _, v := range values {
			alternatives = append(alternatives, Cmp("id", Eq, v))
		}

		forms := []struct {
			name string
			q    *Query
		}{
			{"In", Select(Count()).Where(In("id", values...))},
			{"Or", Select(Count()).Where(Or(alternatives...))},
		}
		for _, form := range forms {
			b.Run(fmt.Sprintf("values=%d/%s", size, form.name), func(b *testing.B) {
				var e Exec
				src := FromSegment(set)
				if err := form.q.RunInto(&e, src); err != nil {
					b.Fatalf("RunInto: %v", err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if err := form.q.RunInto(&e, src); err != nil {
						b.Fatalf("RunInto: %v", err)
					}
				}
			})
		}
	}
}

// BenchmarkMembershipEval measures the per-row decision alone: a membership's
// search over sorted alternatives against a disjunction's walk of equalities,
// both over the same classified cells and the same comparator.
func BenchmarkMembershipEval(b *testing.B) {
	const rows = 1024

	var c Compiler
	for _, size := range []int{4, 16, 64, 256} {
		values := membershipValues(size)

		// Both forms read one column, so the same extracted cells feed each.
		cells := make([]scalar, rows)
		for i := range cells {
			lit, err := c.makeLiteral((i * 13) % 4096)
			if err != nil {
				b.Fatalf("makeLiteral: %v", err)
			}
			cells[i] = classifyLiteral(lit)
		}
		cols := [][]scalar{cells}

		membership := &compiledPredicate{kind: predIn, col: 0, op: Eq}
		disjunction := &compiledPredicate{kind: predOr}
		for _, v := range values {
			lit, err := c.makeLiteral(v)
			if err != nil {
				b.Fatalf("makeLiteral: %v", err)
			}
			membership.lits = append(membership.lits, classifyLiteral(lit))
			disjunction.kids = append(disjunction.kids, &compiledPredicate{
				kind: predCmp, col: 0, op: Eq, lit: classifyLiteral(lit),
			})
		}
		slices.SortFunc(membership.lits, compareScalar)

		forms := []struct {
			name string
			p    *compiledPredicate
		}{
			{"In", membership},
			{"Or", disjunction},
		}
		for _, form := range forms {
			b.Run(fmt.Sprintf("values=%d/%s", size, form.name), func(b *testing.B) {
				var s evalScratch
				matched := 0
				b.ReportAllocs()
				for b.Loop() {
					for row := range rows {
						if form.p.eval(cols, row, &s) {
							matched++
						}
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/row")
				// Consume the result so the decision cannot be eliminated as
				// dead code, which would make both forms measure as nothing.
				if matched < 0 {
					b.Fatalf("unreachable: matched=%d", matched)
				}
			})
		}
	}
}
