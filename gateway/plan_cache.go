package gateway

import (
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/distribution"
	queryplanner "github.com/thesyncim/vibedb/planner"
)

// executorPlanCacheMax bounds the shared physical-plan cache. Entries retain
// one cloned SQL key plus pointers to immutable plans, so the table stays
// small while covering an application's whole query set.
const executorPlanCacheMax = 128

// executorPlanEntry is one cached physical planning outcome. The prepared
// statement itself stays in the snapshot's own plan cache; only the
// distribution-dependent physical selection is shared here. The target
// identities and effective optimizer memory cap certify that the cost context
// used to admit the plan is unchanged. Plans are immutable, so entries are
// safe for concurrent reads once published.
type executorPlanEntry struct {
	generation        uint64
	routeKind         distribution.RouteKind
	targets           []distribution.Target
	maxAggregateBytes uint64
	physical          *queryplanner.Plan
	planning          queryplanner.OptimizerStatistics
}

// cachedPhysical returns the physical plan cached for sqlText at generation,
// or false. The caller copies the entry into its per-call working state;
// shared memory is never mutated.
func (e *Executor) cachedPhysical(sqlText string, generation uint64) (executorPlanEntry, bool) {
	if e == nil || len(sqlText) > maxCachedSQLBytes {
		return executorPlanEntry{}, false
	}
	e.planMu.RLock()
	entry, ok := e.planCache[sqlText]
	e.planMu.RUnlock()
	if !ok || entry.generation != generation || entry.physical == nil {
		return executorPlanEntry{}, false
	}
	return entry, true
}

// publishPhysical records a working copy's physical plan for reuse. Only the
// shapes routeContextCached itself deems cacheable (no aggregates or grouping
// keys) are published, and overlong SQL bypasses the cache instead of pinning
// unbounded request bytes.
func (e *Executor) publishPhysical(
	sqlText string,
	generation uint64,
	bound *BoundPlan,
	cache *preparedQueryExecution,
) {
	if e == nil || cache == nil || cache.physical == nil || bound == nil ||
		len(bound.aggregates) != 0 || len(bound.groupKeys) != 0 ||
		len(sqlText) > maxCachedSQLBytes {
		return
	}
	// Keep the lock across lookup and publication. On a warm hit the existing
	// immutable entry already owns its key and target slice, so publishing the
	// same physical plan must not clone either one again. The request SQL is
	// used only for the lookup while the lock is held; it is never retained.
	e.planMu.Lock()
	defer e.planMu.Unlock()
	if e.planCache == nil {
		e.planCache = make(map[string]executorPlanEntry, executorPlanCacheMax)
	}
	if existing, exists := e.planCache[sqlText]; exists {
		if existing.generation == generation && existing.routeKind == cache.routeKind &&
			existing.maxAggregateBytes == cache.maxAggregateBytes &&
			existing.physical == cache.physical && slices.Equal(existing.targets, cache.targets) {
			return
		}
		// A replacement is a cold publication. Re-own the request SQL before
		// replacing the old map key; ingress SQL may alias a reused message
		// buffer, and a map assignment must never make that alias observable.
		key := strings.Clone(sqlText)
		delete(e.planCache, sqlText)
		for i := range e.planOrder {
			if e.planOrder[i] == sqlText {
				e.planOrder[i] = key
				break
			}
		}
		e.planCache[key] = executorPlanEntry{
			generation:        generation,
			routeKind:         cache.routeKind,
			targets:           slices.Clone(cache.targets),
			maxAggregateBytes: cache.maxAggregateBytes,
			physical:          cache.physical,
			planning:          cache.planning,
		}
		return
	}
	// New entries own both the SQL key and target slice. Ingress SQL may alias
	// a much larger caller buffer, and the entry outlives the request.
	key := strings.Clone(sqlText)
	entry := executorPlanEntry{
		generation:        generation,
		routeKind:         cache.routeKind,
		targets:           slices.Clone(cache.targets),
		maxAggregateBytes: cache.maxAggregateBytes,
		physical:          cache.physical,
		planning:          cache.planning,
	}
	for len(e.planOrder) >= executorPlanCacheMax {
		delete(e.planCache, e.planOrder[0])
		e.planOrder = append(e.planOrder[:0], e.planOrder[1:]...)
	}
	e.planOrder = append(e.planOrder, key)
	e.planCache[key] = entry
}
