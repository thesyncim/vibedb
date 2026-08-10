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

// TestRouteGlue proves the route glue classifies the physical route, resolves
// the target count, and enforces each operational class's admission: interactive
// refuses an all-shard scatter, batch admits it, an empty domain routes nowhere.
func TestRouteGlue(t *testing.T) {
	snap := testSnapshot(t, 1)
	e := newRouteExecutor(t, snap)

	finite, err := distribution.FiniteDomain(distribution.NewString("tenant-42"))
	if err != nil {
		t.Fatalf("FiniteDomain: %v", err)
	}

	tests := []struct {
		name      string
		cons      distribution.BoundConstraints
		class     OperationClass
		wantKind  distribution.RouteKind
		wantCalls int
		wantErr   error
	}{
		{"single_finite", distribution.BoundConstraints{finite}, ClassInteractive, distribution.RouteSingle, 1, nil},
		{"scatter_batch", distribution.BoundConstraints{distribution.UnknownDomain()}, ClassBatch, distribution.RouteScatter, 2, nil},
		{"scatter_rejected_interactive", distribution.BoundConstraints{distribution.UnknownDomain()}, ClassInteractive, 0, 0, distribution.ErrScatterRejected},
		{"empty_domain", distribution.BoundConstraints{distribution.EmptyDomain()}, ClassInteractive, distribution.RouteEmpty, 0, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := &Query{Distribution: "tenant_data", Constraints: tc.cons, SQL: "SELECT 1"}
			pl, err := e.route(snap, q, e.profileFor(tc.class))
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
	q := &Query{
		Distribution: "tenant_data",
		Constraints:  distribution.BoundConstraints{distribution.UnknownDomain()},
		SQL:          "SELECT 1",
	}
	pl, err := e.route(snap, q, e.profileFor(ClassBatch))
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

// TestRouteUnknownDistribution proves routing against a distribution absent from
// the pinned generation fails closed with the catalog sentinel.
func TestRouteUnknownDistribution(t *testing.T) {
	snap := testSnapshot(t, 1)
	e := newRouteExecutor(t, snap)
	q := &Query{Distribution: "absent", Constraints: distribution.BoundConstraints{distribution.UnknownDomain()}}
	_, err := e.route(snap, q, e.profileFor(ClassBatch))
	if !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("err = %v, want errors.Is ErrInvalidCatalog", err)
	}
}

// TestRouteConstraintArityComesFromPinnedCatalog proves a request cannot route
// constraints authored for a different distribution generation.
func TestRouteConstraintArityComesFromPinnedCatalog(t *testing.T) {
	snap := testSnapshot(t, 1)
	e := newRouteExecutor(t, snap)
	q := &Query{
		Distribution: "tenant_data",
		Constraints: distribution.BoundConstraints{
			distribution.UnknownDomain(),
			distribution.UnknownDomain(),
		},
		SQL: "SELECT 1",
	}
	_, err := e.route(snap, q, e.profileFor(ClassBatch))
	if !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("err = %v, want errors.Is ErrInvalidCatalog", err)
	}
}
