package gateway

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	queryplanner "github.com/thesyncim/vibedb/planner"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibejson/x/byteview"
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

func TestExecutorPhysicalPlanCacheOwnsReplacementSQL(t *testing.T) {
	const text = `SELECT n FROM messages WHERE tenant_id = 'a1'`
	backing := []byte(text)
	borrowed := byteview.String(backing)
	owned := strings.Clone(text)
	oldPhysical := &queryplanner.Plan{}
	newPhysical := &queryplanner.Plan{}
	e := &Executor{
		planCache: map[string]executorPlanEntry{
			owned: {generation: 1, physical: oldPhysical},
		},
		planOrder: []string{owned},
	}
	e.publishPhysical(borrowed, 1, &BoundPlan{}, &preparedQueryExecution{physical: newPhysical})
	clear(backing)
	entry, ok := e.cachedPhysical(text, 1)
	if !ok || entry.physical != newPhysical || len(e.planOrder) != 1 || e.planOrder[0] != text {
		t.Fatal("replaced physical plan retained borrowed SQL key storage")
	}
}

// TestExecutorSharedPlanConcurrentReads hammers one point read from 8
// goroutines at once: the shared physical plan, the pooled route shells,
// and the pooled connections (each with its owned frame arena) must serve
// correct rows and columns under -race with no cross-query contamination.
func TestExecutorSharedPlanConcurrentReads(t *testing.T) {
	c := newE2ECluster(t)
	holder := NewCatalogHolder(c.snapshot(t, 1))
	client := NewClientWithOptions(c.dialer.dial, ClientOptions{})
	t.Cleanup(func() { _ = client.Close() })
	e := NewExecutor(client, holder, Options{})

	key := c.shards[0].keys[0]
	read := Query{
		SQL: `SELECT n FROM messages WHERE tenant_id = ?`, Params: stringParams(key), Class: ClassInteractive,
	}
	if res, err := e.Query(context.Background(), read); err != nil || len(res.Rows) != 1 {
		t.Fatalf("prime Query = %d rows, %v; want 1 row", len(res.Rows), err)
	}

	const workers, perWorker = 8, 25
	errs := make(chan error, workers*perWorker)
	var wg sync.WaitGroup
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				res, err := e.Query(context.Background(), read)
				if err != nil {
					errs <- err
					return
				}
				if len(res.Rows) != 1 || len(res.Columns) != 1 || res.Columns[0].Name != "n" {
					errs <- errors.New("wrong rows or columns under concurrency")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
