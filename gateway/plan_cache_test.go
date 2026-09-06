package gateway

import (
	"context"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/shardservice"
)

// TestReleaseShardCallsScrubsShells proves routed shells retain no caller
// data once released: a reborrowed shell is fully zero.
func TestReleaseShardCallsScrubsShells(t *testing.T) {
	req := shardRequestPool.Get().(*shardservice.ShardRequest)
	*req = shardservice.ShardRequest{SQL: "SELECT 1", MaxRows: 7}
	releaseShardCalls([]shardCall{{req: req}})
	again := shardRequestPool.Get().(*shardservice.ShardRequest)
	if !reflect.DeepEqual(*again, shardservice.ShardRequest{}) {
		t.Fatalf("reborrowed shell = %+v, want zero value", again)
	}
	shardRequestPool.Put(again)
}

// TestExecutorPlanCacheReusesPhysicalPlan proves the plain Query path shares
// physical planning across identical statements: the second identical point
// read reuses the same immutable physical plan instead of re-optimizing, and
// a catalog generation change retires the entry.
func TestExecutorPlanCacheReusesPhysicalPlan(t *testing.T) {
	c := newE2ECluster(t)
	holder := NewCatalogHolder(c.snapshot(t, 1))
	client := NewClientWithOptions(c.dialer.dial, ClientOptions{})
	t.Cleanup(func() { _ = client.Close() })
	e := NewExecutor(client, holder, Options{})

	key := c.shards[0].keys[0]
	read := Query{
		SQL: `SELECT n FROM messages WHERE tenant_id = ?`, Params: stringParams(key), Class: ClassInteractive,
	}
	run := func() {
		res, err := e.Query(context.Background(), read)
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(res.Rows))
		}
	}
	run()

	e.planMu.RLock()
	entry, ok := e.planCache[read.SQL]
	size := len(e.planCache)
	e.planMu.RUnlock()
	if !ok {
		t.Fatalf("shared plan cache has no entry for the point read")
	}
	if size != 1 {
		t.Fatalf("shared plan cache size = %d, want 1", size)
	}
	first := entry.physical
	if first == nil {
		t.Fatalf("cached entry has no physical plan")
	}

	run()

	e.planMu.RLock()
	again, ok := e.planCache[read.SQL]
	e.planMu.RUnlock()
	if !ok {
		t.Fatalf("shared plan cache lost the point-read entry")
	}
	if again.physical != first {
		t.Fatalf("second identical read re-optimized instead of reusing the physical plan")
	}
	if again.generation != entry.generation {
		t.Fatalf("entry generation changed without a catalog refresh")
	}
}
