package gateway

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestReplicatedLogicalSchemaReadsAcrossDistinctShardMachines(t *testing.T) {
	snapshot, _, keys := replicatedSQLSplitTransactionFixture(t)
	fixture := scatterCatalogFixture{snapshot: snapshot}
	fixture.request.MaxResultBytes = 1 << 20
	for _, key := range keys {
		encoded, ok := orderedkey.AppendString(nil, []byte(key), orderedkey.Ascending)
		if !ok {
			t.Fatal("key")
		}
		point := ReplicatedTableBatchPoint{Table: []byte("messages"), Key: encoded}
		var scratch [replication.MaxMutationKeyBytes + 16]byte
		var replicas [ServingReplicaCount]ReplicatedEndpoint
		resolved, ok := snapshot.ResolveReplicatedTableKey(point.Table, point.Key, scratch[:0], replicas[:0])
		if !ok {
			t.Fatal("logical table did not route to both shard schemas")
		}
		fixture.request.Points = append(fixture.request.Points, point)
		fixture.routes = append(fixture.routes, resolved)
	}
	if fixture.routes[0].Route.Command.RelationManifestDigest == fixture.routes[1].Route.Command.RelationManifestDigest ||
		fixture.routes[0].Profile.LogicalSchemaDigest != fixture.routes[1].Profile.LogicalSchemaDigest {
		t.Fatal("machine and logical schema domains collapsed")
	}
	reader := newScatterReader(t, fixture, &scatterReadClient{}, nil, 2)
	result, err := reader.ReadScatterBatch(context.Background(), fixture.request)
	defer result.Release()
	if err != nil || result.Count() != 2 || len(result.Observations) != 2 {
		t.Fatalf("distinct machine read result=%+v error=%v", result, err)
	}
	for i := 0; i < 2; i++ {
		_, found, ok := result.Lookup(i)
		if !ok || !found {
			t.Fatalf("missing result %d", i)
		}
	}
}

func TestReplicatedLogicalSchemaFencesAttachTransitionAndHotRoute(t *testing.T) {
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	build := func(d ReplicatedShardDescriptor, p ReplicatedTableProfile) (*Snapshot, error) {
		return NewSnapshotWithReplicatedTableMetadata(config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{d}, []ReplicatedTableProfile{p})
	}
	snapshot, err := build(descriptor, profile)
	if err != nil {
		t.Fatal(err)
	}
	for _, logical := range []replication.Digest{{}, {99}, replication.Digest(descriptor.Command.RelationManifestDigest)} {
		bad := descriptor
		bad.LogicalSchemaDigest = logical
		if _, err := build(bad, profile); err == nil {
			t.Fatal("mismatched logical schema attached")
		}
	}
	changed := descriptor
	changed.LogicalSchemaDigest[0]++
	changedProfile := profile
	changedProfile.LogicalSchemaDigest = changed.LogicalSchemaDigest
	next, err := build(changed, changedProfile)
	if err != nil {
		t.Fatal(err)
	}
	if validateReplicatedCatalogTransition(snapshot, next) == nil {
		t.Fatal("same-generation logical schema changed")
	}
	changed = descriptor
	changed.Command.RelationManifestDigest[0]++
	next, err = build(changed, profile)
	if err != nil {
		t.Fatal(err)
	}
	if validateReplicatedCatalogTransition(snapshot, next) == nil {
		t.Fatal("same-generation machine schema changed")
	}
	// Hot routing independently checks the compact logical field. Mutation here
	// is test-only: published snapshot internals are immutable in production.
	snapshot.replicatedShards[0].logicalSchema[0]++
	key, _ := orderedkey.AppendString(nil, []byte("key"), orderedkey.Ascending)
	var scratch [replication.MaxMutationKeyBytes + 16]byte
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	if _, ok := snapshot.ResolveReplicatedTableKey([]byte(profile.Table), key, scratch[:0], replicas[:0]); ok {
		t.Fatal("hot route accepted a different logical schema")
	}
}
