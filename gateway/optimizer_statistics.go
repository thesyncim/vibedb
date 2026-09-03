package gateway

import (
	"math"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	queryplanner "github.com/thesyncim/vibedb/planner"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// Keep parameter binding proportional to the query predicates, not the number
// of published columns. The shared constraint compiler still decides whether
// each selected predicate can actually bind to a finite domain.
func appendStatisticsPredicatePaths(dst, published []string, expr *sqlast.Expr) []string {
	if expr == nil {
		return dst
	}
	if expr.Kind == sqlast.ExprAnd {
		for _, child := range expr.Kids {
			dst = appendStatisticsPredicatePaths(dst, published, child)
		}
		return dst
	}
	if !(expr.Kind == sqlast.ExprCompare && expr.Op == sqlast.OpEq || expr.Kind == sqlast.ExprIn && !expr.Negated) || expr.Path == nil || expr.Path.Source != 0 {
		return dst
	}
	path := string(expr.Path.AppendPointer(nil))
	if slices.Contains(published, path) && !slices.Contains(dst, path) {
		dst = append(dst, path)
	}
	return dst
}

func boundJointSelectivity(stats TableStatistic, paths []string, domains distribution.BoundConstraints) (Estimate, bool) {
	// Canonical tuples are cold planning data. Keep the common single-key route
	// on the existing allocation-free lookup path.
	if !stats.HasGroups() {
		return Estimate{}, false
	}
	n := 0
	for _, d := range domains {
		if d.Kind == distribution.DomainEmpty {
			return queryplanner.ExactEstimate(0), true
		}
		if d.Kind == distribution.DomainFinite {
			n++
		}
	}
	if n < 1 || n > queryplanner.MaxStatisticsGroupColumns {
		return Estimate{}, false
	}
	constraints := make([]queryplanner.EqualityConstraint, 0, n)
	for i, d := range domains {
		if d.Kind != distribution.DomainFinite || i >= len(paths) {
			continue
		}
		q := queryplanner.EqualityConstraint{Path: paths[i], Values: make([]string, 0, len(d.Values))}
		for _, v := range d.Values {
			canonical, ok := appendBoundStatisticScalar(nil, v)
			if !ok {
				return Estimate{}, false
			}
			q.Values = append(q.Values, string(canonical))
		}
		constraints = append(constraints, q)
	}
	return stats.ConjunctionSelectivity(constraints)
}

// distributedGroupEstimates keeps three different quantities: source partial
// groups (exchange traffic), global groups (final output), and the busiest
// source's partial groups. Summing shard NDVs estimates partials, never global
// NDV: the same group can appear on every source shard.
func distributedGroupEstimates(snap *Snapshot, bound *BoundPlan, route distribution.Route, inputRows float64) (partial, final, maxPartial float64, known bool) {
	final = inputRows
	if len(route.Targets) == 0 {
		return 0, 0, 0, true
	}
	stats, ok := snap.Statistics(bound.table)
	if !ok || len(bound.tables) > 1 || len(bound.groupPaths) == 0 {
		return inputRows, inputRows, inputRows, false
	}
	ndv, ok := stats.GroupDistinct("", bound.groupPaths)
	if !ok {
		return inputRows, inputRows, inputRows, false
	}
	final = min(inputRows, ndv.Upper)
	// Finite filters on grouping columns cap the possible key combinations even
	// when the NDV statistics were collected before predicate application.
	combinations := 1.0
	allFinite := true
	for _, path := range bound.groupPaths {
		found := false
		for i, p := range bound.statPaths {
			if p == path && i < len(bound.statConstraints) {
				d := bound.statConstraints[i]
				if d.Kind == distribution.DomainEmpty {
					return 0, 0, 0, true
				}
				if d.Kind == distribution.DomainFinite {
					combinations = boundedProduct(combinations, float64(len(d.Values)))
					found = true
				}
				break
			}
		}
		if !found {
			allFinite = false
			break
		}
	}
	if allFinite {
		final = min(final, combinations)
	}
	for _, target := range route.Targets {
		local := final
		if rows, exists := stats.PartitionRows(string(target.Shard)); exists {
			local = min(local, rows.Upper)
		}
		if distinct, exists := stats.GroupDistinct(string(target.Shard), bound.groupPaths); exists {
			local = min(local, distinct.Upper)
		}
		partial = boundedAdd(partial, local)
		maxPartial = max(maxPartial, local)
	}
	if bound.groupLocal {
		partial = min(partial, final)
	}
	partial = min(inputRows, partial)
	return partial, min(final, partial), min(maxPartial, partial), true
}

// hashPartitionGroupUpper models the busiest reducer, rather than dividing
// memory by worker count. Bernstein's bound plus a union bound targets a 1%
// overflow probability under uniform hashing of distinct group keys. Repeated
// hot keys are collapsed by shard partial aggregation before this exchange.
func hashPartitionGroupUpper(groups float64, partitions int) float64 {
	if groups <= 0 {
		return 0
	}
	if partitions <= 1 {
		return groups
	}
	mean := groups / float64(partitions)
	logFailure := math.Log(float64(partitions) / .01)
	return min(groups, math.Ceil(mean+math.Sqrt(2*mean*logFailure)+2*logFailure/3))
}

// Match the fixed per-group charge in distributedagg.Merger, plus retained
// key/payload bytes and final output. Exact arithmetic can grow beyond this
// estimate; runtime budgets remain authoritative.
func aggregateStateMemory(metadata distributedPrivate, groups float64) float64 {
	width := max(16, metadata.rowBytes)
	perGroup := boundedAdd(96+float64(len(metadata.aggregates))*192+float64(len(metadata.groupKeys))*16, boundedProduct(width, 2))
	return boundedProduct(groups, perGroup)
}

func (m *distributedCostModel) gatherMemory(rows, width float64) float64 {
	if metadata, ok := m.private[1]; ok && len(metadata.groupKeys) != 0 {
		return boundedProduct(rows, boundedAdd(width, 48+32*float64(len(metadata.aggregates))))
	}
	return width
}

func distributedAccessOrdering(metadata distributedPrivate) []queryplanner.OrderingColumn {
	if len(metadata.groupKeys) != 0 {
		return nil
	}
	return plannerOrdering(metadata.order)
}

func groupedExchangeFeasible(metadata distributedPrivate) bool {
	_, err := makeGroupedExchangeLimits(metadata.profile.withDefaults(), metadata.targets, len(metadata.aggregates))
	return err == nil
}

func (m *distributedCostModel) sortEnforcer(rows, width float64, required queryplanner.PhysicalProperties) queryplanner.Enforcer {
	op := queryplanner.OpSort
	memoryRows := rows
	if metadata, ok := m.private[1]; ok && len(metadata.groupKeys) != 0 && metadata.hasLimit {
		op = queryplanner.OpTopK
		memoryRows = min(rows, boundedAdd(float64(metadata.limit), float64(metadata.offset)))
	}
	return queryplanner.Enforcer{Op: op, Provided: required, Cost: queryplanner.Cost{CPU: boundedProduct(rows, math.Log2(max(2, memoryRows))), Memory: boundedProduct(memoryRows, width)}}
}
