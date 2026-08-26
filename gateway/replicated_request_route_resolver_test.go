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
