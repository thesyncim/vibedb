package gateway

import (
	"context"
	"errors"
	"testing"
)

func TestCatalogDurableRequestRouteResolverAcceptsOnlyExactLogicalAuthority(t *testing.T) {
	snapshot, topology := testRequestLedgerCatalogSnapshot(t, 5)
	resolver, err := NewCatalogDurableRequestRouteResolver(NewCatalogHolder(snapshot))
	if err != nil {
		t.Fatal(err)
	}
	want := topology.Ranges[0].Route
	logical := DurableRequestLogicalParticipant{
		Distribution: want.Distribution, Shard: want.Shard, Group: want.Group,
		RangeIdentity: want.RangeIdentity, LineageDigest: want.LineageDigest,
		ForwardingRuleDigest:   want.ForwardingRuleDigest,
		SchemaGeneration:       want.Command.SchemaGeneration,
		RelationManifestDigest: want.Command.RelationManifestDigest,
	}
	got, err := resolver.ResolveDurableRequestParticipant(context.Background(), logical)
	if err != nil || !sameReplicatedCatalogRoute(got, want) {
		t.Fatalf("resolved route=%+v err=%v", got, err)
	}
	logical.LineageDigest[0]++
	if _, err := resolver.ResolveDurableRequestParticipant(context.Background(), logical); !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("lineage drift error=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.ResolveDurableRequestParticipant(canceled, logical); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled resolution error=%v", err)
	}
}

func TestCatalogDurableSessionRouteRequiresExactPhysicalFence(t *testing.T) {
	snapshot, topology := testRequestLedgerCatalogSnapshot(t, 5)
	resolver, err := NewCatalogDurableRequestRouteResolver(NewCatalogHolder(snapshot))
	if err != nil {
		t.Fatal(err)
	}
	want := topology.Ranges[0].Route
	wave, head, _ := lifecycleRunnerFixture(t)
	fixture := &lifecycleRunnerProposer{}
	exact, _, err := fixture.prepareAcquire(t.Context(), want, wave, head)
	if err != nil {
		t.Fatal(err)
	}
	got, err := resolver.resolveDurableSessionRoute(t.Context(), exact)
	if err != nil || !sameReplicatedCatalogRoute(got, want) {
		t.Fatalf("session route=%+v err=%v", got, err)
	}
	for _, mutate := range []func(*ReplicatedRoute){
		func(route *ReplicatedRoute) { route.AllocationGeneration++ },
		func(route *ReplicatedRoute) { route.Command.OwnershipEpoch++ },
		func(route *ReplicatedRoute) { route.Command.SchemaGeneration++ },
		func(route *ReplicatedRoute) { route.Command.RouteGeneration++ },
	} {
		foreign := want
		mutate(&foreign)
		wrong, _, err := fixture.prepareAcquire(t.Context(), foreign, wave, head)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := resolver.resolveDurableSessionRoute(t.Context(), wrong); !errors.Is(err, ErrDurableRequestConflict) {
			t.Fatalf("foreign fence accepted: %v", err)
		}
	}
	if _, err := resolver.resolveDurableSessionRoute(t.Context(), wave.Command); !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("non-route command accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := resolver.resolveDurableSessionRoute(ctx, exact); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cleanup resolution: %v", err)
	}
}
