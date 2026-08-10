package gateway

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/shardservice"
)

// newRouteExecutor builds an executor over a holder seeded with snap; route
// tests exercise only the routing glue and never dispatch.
func newRouteExecutor(t *testing.T, snap *Snapshot) *Executor {
	t.Helper()
	return NewExecutor(NewClient(nil), NewCatalogHolder(snap), Options{})
}

func routeSQL(t *testing.T, e *Executor, snap *Snapshot, sql string, class OperationClass) (*plan, error) {
	t.Helper()
	prepared, err := snap.Prepare(t.Context(), sql)
	if err != nil {
		return nil, err
	}
	bound, err := prepared.Bind(nil)
	if err != nil {
		return nil, err
	}
	return e.route(snap, &Query{SQL: sql, Class: class}, bound, e.profileFor(class))
}

// TestRouteGlue proves the route glue classifies the physical route, resolves
// the target count, and enforces each operational class's admission: interactive
// refuses an all-shard scatter, batch admits it, an empty domain routes nowhere.
func TestRouteGlue(t *testing.T) {
	snap := testSnapshot(t, 1)
	e := newRouteExecutor(t, snap)

	tests := []struct {
		name      string
		sql       string
		class     OperationClass
		wantKind  distribution.RouteKind
		wantCalls int
		wantErr   error
	}{
		{"single_finite", `SELECT n FROM messages WHERE tenant_id = 'tenant-42'`, ClassInteractive, distribution.RouteSingle, 1, nil},
		{"scatter_batch", `SELECT n FROM messages`, ClassBatch, distribution.RouteScatter, 2, nil},
		{"scatter_rejected_interactive", `SELECT n FROM messages`, ClassInteractive, 0, 0, distribution.ErrScatterRejected},
		{"empty_domain", `SELECT n FROM messages WHERE tenant_id = 'a' AND tenant_id = 'b'`, ClassInteractive, distribution.RouteEmpty, 0, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pl, err := routeSQL(t, e, snap, tc.sql, tc.class)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want errors.Is %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("route: %v", err)
			}
			if pl.kind != tc.wantKind {
				t.Fatalf("kind = %v, want %v", pl.kind, tc.wantKind)
			}
			if len(pl.calls) != tc.wantCalls {
				t.Fatalf("calls = %d, want %d", len(pl.calls), tc.wantCalls)
			}
			for _, c := range pl.calls {
				if c.req.RoutingVersion != 3 {
					t.Fatalf("routing version = %d, want 3 (the pinned manifest generation)", c.req.RoutingVersion)
				}
				if c.req.ReadPolicy != shardservice.ReadStrong {
					t.Fatalf("read policy = %v, want Strong", c.req.ReadPolicy)
				}
				if c.req.ExecutionMode != shardservice.ExecutionReadOnly {
					t.Fatalf("execution mode = %v, want ReadOnly", c.req.ExecutionMode)
				}
			}
		})
	}
}

// TestRouteCarriesShardCoordinates proves a scatter route resolves each target's
// endpoint to an address and carries that target's ownership epoch and shard id,
// in keyspace order, into the per-shard request.
func TestRouteCarriesShardCoordinates(t *testing.T) {
	snap := testSnapshot(t, 1)
	e := newRouteExecutor(t, snap)
	pl, err := routeSQL(t, e, snap, `SELECT n FROM messages`, ClassBatch)
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if pl.scatter != ScatterUnknownRoute {
		t.Fatalf("scatter reason = %v, want UnknownRoute", pl.scatter)
	}
	want := []struct {
		shard distribution.ShardID
		epoch distribution.OwnershipEpoch
		addr  string
	}{
		{"-80", 7, "127.0.0.1:7001"},
		{"80-", 9, "127.0.0.1:7002"},
	}
	if len(pl.calls) != len(want) {
		t.Fatalf("calls = %d, want %d", len(pl.calls), len(want))
	}
	for i, w := range want {
		c := pl.calls[i]
		if c.target.Shard != w.shard || c.req.Shard != w.shard {
			t.Fatalf("call %d shard = %q/%q, want %q", i, c.target.Shard, c.req.Shard, w.shard)
		}
		if c.req.OwnershipEpoch != w.epoch {
			t.Fatalf("call %d epoch = %d, want %d", i, c.req.OwnershipEpoch, w.epoch)
		}
		if c.address != w.addr {
			t.Fatalf("call %d address = %q, want %q", i, c.address, w.addr)
		}
	}
}

// TestRouteUnknownPlacement proves SQL cannot select an arbitrary distribution;
// its physical table must resolve through the pinned planner directory.
func TestRouteUnknownPlacement(t *testing.T) {
	snap := testSnapshot(t, 1)
	_, err := snap.Prepare(t.Context(), `SELECT id FROM absent`)
	if !errors.Is(err, ErrTableNotPlaced) {
		t.Fatalf("err = %v, want errors.Is ErrTableNotPlaced", err)
	}
}

// TestRoutePlanGenerationFence proves compiled routing metadata cannot cross a
// catalog generation boundary.
func TestRoutePlanGenerationFence(t *testing.T) {
	old := testSnapshot(t, 1)
	newer := testSnapshot(t, 2)
	prepared, err := old.Prepare(t.Context(), `SELECT n FROM messages`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	bound, err := prepared.Bind(nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	e := newRouteExecutor(t, newer)
	_, err = e.route(newer, &Query{SQL: `SELECT n FROM messages`}, bound, e.profileFor(ClassBatch))
	if !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("err = %v, want errors.Is ErrInvalidCatalog", err)
	}
}
