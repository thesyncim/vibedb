package gateway

import (
	"maps"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/distribution"
	queryplanner "github.com/thesyncim/vibedb/planner"
)

// executorPlanCacheMax bounds the shared physical-plan cache. Entries retain
// one cloned SQL key plus pointers to immutable plans, so the table stays
// small while covering an application's whole query set.
const executorPlanCacheMax = 128

// executorPlanSnapshot is an immutable map snapshot published via
// Executor.planSnap for lock-free, zero-allocation concurrent reads across all
// CPU cores without RWMutex cache-line bouncing.
type executorPlanSnapshot struct {
	entries map[string]executorPlanEntry
}

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

func (e executorPlanEntry) matchesPublished(generation uint64, cache *preparedQueryExecution) bool {
	return e.generation == generation &&
		e.routeKind == cache.routeKind &&
		e.maxAggregateBytes == cache.maxAggregateBytes &&
		e.physical == cache.physical &&
		slices.Equal(e.targets, cache.targets)
}

// cachedPhysical returns the physical plan cached for sqlText at generation,
// or false. Reads are completely lock-free via the immutable COW snapshot; the
// caller copies the entry into its per-call working state.
func (e *Executor) cachedPhysical(sqlText string, generation uint64) (executorPlanEntry, bool) {
	if e == nil || len(sqlText) > maxCachedSQLBytes {
		return executorPlanEntry{}, false
	}
	snap := e.planSnap.Load()
	if snap == nil {
		return executorPlanEntry{}, false
	}
	entry, ok := snap.entries[sqlText]
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
	// Lock-free fast path: on a warm hit the immutable snapshot already carries
	// this exact plan, so avoid acquiring planMu or allocating anything.
	if snap := e.planSnap.Load(); snap != nil {
		if existing, exists := snap.entries[sqlText]; exists && existing.matchesPublished(generation, cache) {
			return
		}
	}

	// Cold publication or replacement: serialize mutations under planMu and
	// publish a fresh immutable snapshot for lock-free readers.
	e.planMu.Lock()
	defer e.planMu.Unlock()
	if e.planCache == nil {
		e.planCache = make(map[string]executorPlanEntry, executorPlanCacheMax)
	}
	if existing, exists := e.planCache[sqlText]; exists {
		if existing.matchesPublished(generation, cache) {
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
		e.storePlanSnapshotLocked()
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
	e.storePlanSnapshotLocked()
}

func (e *Executor) storePlanSnapshotLocked() {
	cloned := make(map[string]executorPlanEntry, len(e.planCache))
	maps.Copy(cloned, e.planCache)
	e.planSnap.Store(&executorPlanSnapshot{entries: cloned})
}
