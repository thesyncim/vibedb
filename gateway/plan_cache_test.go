package gateway

import (
	"fmt"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	queryplanner "github.com/thesyncim/vibedb/planner"
)

func TestExecutorPlanCacheConcurrentLockFreeReadsAndEviction(t *testing.T) {
	e := &Executor{}
	plan := &queryplanner.Plan{}
	bound := &BoundPlan{}
	execCache := &preparedQueryExecution{
		routeKind:         distribution.RouteSingle,
		targets:           []distribution.Target{{Shard: "s0"}},
		maxAggregateBytes: 1 << 20,
		physical:          plan,
	}

	// Publish more than executorPlanCacheMax entries concurrently with heavy reads.
	const totalEntries = executorPlanCacheMax + 32
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := range totalEntries {
				sql := fmt.Sprintf("SELECT id FROM t WHERE k = %d", (i+worker*7)%totalEntries)
				e.publishPhysical(sql, 1, bound, execCache)
				if entry, ok := e.cachedPhysical(sql, 1); ok && entry.physical != plan {
					t.Errorf("unexpected physical plan pointer")
				}
			}
		}(g)
	}
	wg.Wait()

	// Warm hit must allocate 0 bytes.
	warmSQL := fmt.Sprintf("SELECT id FROM t WHERE k = %d", totalEntries-1)
	e.publishPhysical(warmSQL, 1, bound, execCache)
	allocs := testing.AllocsPerRun(100, func() {
		if _, ok := e.cachedPhysical(warmSQL, 1); !ok {
			t.Fatal("expected warm plan cache hit")
		}
		e.publishPhysical(warmSQL, 1, bound, execCache)
	})
	if allocs != 0 {
		t.Fatalf("warm cachedPhysical + publishPhysical allocs = %v, want 0", allocs)
	}
}

func BenchmarkExecutorPlanCacheParallel(b *testing.B) {
	e := &Executor{}
	plan := &queryplanner.Plan{}
	bound := &BoundPlan{}
	execCache := &preparedQueryExecution{
		routeKind:         distribution.RouteSingle,
		targets:           []distribution.Target{{Shard: "s0"}},
		maxAggregateBytes: 1 << 20,
		physical:          plan,
	}
	sqls := make([]string, 32)
	for i := range sqls {
		sqls[i] = fmt.Sprintf("SELECT id, name FROM users WHERE tenant_id = 't' AND id = %d", i)
		e.publishPhysical(sqls[i], 1, bound, execCache)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, ok := e.cachedPhysical(sqls[i&31], 1); !ok {
				b.Fatal("miss")
			}
			i++
		}
	})
}
