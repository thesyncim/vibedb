package gateway

import (
	"bytes"
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestReplicatedCatalogExactRF3RoundTripAndAllocationFreeRoute(t *testing.T) {
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	snapshot, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ReplicatedMetadataBytes() == 0 {
		t.Fatal("replicated metadata reports no retained bytes")
	}
	var storage [ServingReplicaCount]ReplicatedEndpoint
	assertRoute := func(snapshot *Snapshot) {
		t.Helper()
		route, ok := snapshot.ResolveReplicatedRoute(
			descriptor.Distribution, descriptor.Shard, storage[:0],
		)
		if !ok || route.Group != descriptor.Group ||
			route.AllocationGeneration != uint64(descriptor.AllocationGeneration) ||
			route.Command != descriptor.Command ||
			len(route.Replicas) != ServingReplicaCount {
			t.Fatalf("resolved route = %+v,%v", route, ok)
		}
		for ordinal, replica := range route.Replicas {
			want := descriptor.Replicas[ordinal]
			if replica.Member != want.Member || replica.Address != endpoints[want.Endpoint] {
				t.Fatalf("replica %d = %+v", ordinal, replica)
			}
		}
	}
	assertRoute(snapshot)
	if allocations := testing.AllocsPerRun(1000, func() {
		_, _ = snapshot.ResolveReplicatedRoute(
			descriptor.Distribution, descriptor.Shard, storage[:0],
		)
	}); allocations != 0 {
		t.Fatalf("route allocations = %f", allocations)
	}

	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveSnapshot(path, snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	assertRoute(loaded)
}

func TestReplicatedCatalogRejectsGroupChangeWithinAllocation(t *testing.T) {
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	current, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	changed := descriptor
	changed.Group.GroupID[0]++
	next, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 6, nil, nil, []ReplicatedShardDescriptor{changed},
	)
	if err != nil {
		t.Fatal(err)
	}
	current, err = initialCatalogState(current)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := advanceCatalogState(current, next); err == nil {
		t.Fatal("catalog accepted a different Raft group inside one allocation")
	}
}

func TestReplicatedCatalogServingFenceMonotonicAndRosterFrozen(t *testing.T) {
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	current, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	current, err = initialCatalogState(current)
	if err != nil {
		t.Fatal(err)
	}

	advanced := descriptor
	advanced.Command.SchemaGeneration++
	advanced.Command.RelationManifestDigest[0]++
	next, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 6, nil, nil, []ReplicatedShardDescriptor{advanced},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := advanceCatalogState(current, next); err != nil {
		t.Fatalf("monotonic schema fence: %v", err)
	}

	changedRoster := descriptor
	changedRoster.Command.ReplicaSetVersion++
	next, err = NewSnapshotWithReplicatedMetadata(
		config, endpoints, 6, nil, nil, []ReplicatedShardDescriptor{changedRoster},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := advanceCatalogState(current, next); err == nil {
		t.Fatal("catalog implied an unsupported membership transition")
	}
}

func TestReplicatedCommandFenceRegressionChecksEveryGeneration(t *testing.T) {
	old := raftservice.CommandFence{
		ReplicaSetVersion: 11, ActivePolicyGeneration: 12, ProtectionEpoch: 13,
		OwnershipEpoch: 14, SchemaGeneration: 15, RoutingVersion: 16,
		RelationManifestDigest: [32]byte{18}, RouteGeneration: 17,
	}
	cases := []struct {
		name   string
		mutate func(*raftservice.CommandFence)
	}{
		{"replica-set", func(value *raftservice.CommandFence) { value.ReplicaSetVersion-- }},
		{"policy", func(value *raftservice.CommandFence) { value.ActivePolicyGeneration-- }},
		{"protection", func(value *raftservice.CommandFence) { value.ProtectionEpoch-- }},
		{"ownership", func(value *raftservice.CommandFence) { value.OwnershipEpoch-- }},
		{"schema", func(value *raftservice.CommandFence) { value.SchemaGeneration-- }},
		{"same-schema-digest", func(value *raftservice.CommandFence) {
			value.RelationManifestDigest[0]++
		}},
		{"new-schema-same-digest", func(value *raftservice.CommandFence) {
			value.SchemaGeneration++
		}},
		{"routing", func(value *raftservice.CommandFence) { value.RoutingVersion-- }},
		{"route", func(value *raftservice.CommandFence) { value.RouteGeneration-- }},
	}
	if replicatedCommandFenceRegresses(old, old) {
		t.Fatal("equal serving fence regressed")
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			next := old
			test.mutate(&next)
			if !replicatedCommandFenceRegresses(old, next) {
				t.Fatal("regression was accepted")
			}
		})
	}
}

func TestManifestTransitionConsumesExplicitInstalledRF3Fence(t *testing.T) {
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	current, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	manifest := config.Manifests[0]
	shards := make([]distribution.Shard, manifest.ShardCount())
	for ordinal := range shards {
		shards[ordinal], _ = manifest.ShardInfo(ordinal)
	}
	nextManifest, err := distribution.NewManifest(
		manifest.Distribution(), manifest.Version()+1, shards,
	)
	if err != nil {
		t.Fatal(err)
	}
	installed := descriptor
	installed.Command.RoutingVersion = uint64(nextManifest.Version())
	installed.Command.SchemaGeneration++
	installed.Command.RelationManifestDigest[0]++
	next, err := BuildManifestTransitionsWithReplicatedMetadata(
		current, []*distribution.Manifest{nextManifest}, 6,
		[]ReplicatedShardDescriptor{installed},
	)
	if err != nil {
		t.Fatal(err)
	}
	var workspace [ServingReplicaCount]ReplicatedEndpoint
	route, ok := next.ResolveReplicatedRoute(
		descriptor.Distribution, descriptor.Shard, workspace[:0],
	)
	if !ok || route.Command != installed.Command {
		t.Fatalf("installed route = %+v,%v", route, ok)
	}
}

func TestReplicatedCatalogRejectsManifestReplicaMismatch(t *testing.T) {
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	descriptor.Replicas[0].Endpoint, descriptor.Replicas[1].Endpoint =
		descriptor.Replicas[1].Endpoint, descriptor.Replicas[0].Endpoint
	if _, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor},
	); err == nil {
		t.Fatal("catalog accepted reordered replica endpoints")
	}
}

func TestLegacySQLWriteRefusesReplicatedShardBeforeNetwork(t *testing.T) {
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	snapshot, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	dials := 0
	client := NewClient(func(context.Context, string) (net.Conn, error) {
		dials++
		return nil, errors.New("legacy SQL write reached network")
	})
	t.Cleanup(func() { _ = client.Close() })
	executor := NewExecutor(client, NewCatalogHolder(snapshot), Options{})
	key := shardKeyFor(t, snapshot, string(descriptor.Shard))
	_, err = executor.Exec(context.Background(), Query{
		SQL: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
		Params: []shardservice.Param{
			shardservice.StringParam(key), shardservice.NumberParam("1"),
		},
	})
	if !errors.Is(err, ErrReplicatedSQLWriteUnavailable) || dials != 0 {
		t.Fatalf("SQL RF3 write = %v, dials=%d", err, dials)
	}
}

func testReplicatedCatalogInput(
	t testing.TB,
) (distribution.ClusterConfig, map[distribution.EndpointID]string, ReplicatedShardDescriptor) {
	t.Helper()
	config := testConfig(t)
	first, _ := config.Manifests[0].ShardInfo(0)
	second, _ := config.Manifests[0].ShardInfo(1)
	first.Leaders = []distribution.EndpointID{"ep-a", "ep-c", "ep-d"}
	manifest, err := distribution.NewManifest(
		config.Manifests[0].Distribution(), config.Manifests[0].Version(),
		[]distribution.Shard{first, second},
	)
	if err != nil {
		t.Fatal(err)
	}
	config.Manifests[0] = manifest
	endpoints := testEndpoints()
	endpoints["ep-c"] = "127.0.0.1:7003"
	endpoints["ep-d"] = "127.0.0.1:7004"
	group := raftmember.GroupKey{TopologyRecoveryEpoch: 11}
	for ordinal := range group.ClusterID {
		group.ClusterID[ordinal] = byte(ordinal + 1)
		group.ClusterIncarnation[ordinal] = byte(ordinal + 21)
		group.ShardIncarnation[ordinal] = byte(ordinal + 41)
		group.GroupID[ordinal] = byte(ordinal + 61)
	}
	descriptor := ReplicatedShardDescriptor{
		Distribution: manifest.Distribution(), Shard: first.ID, Group: group,
		AllocationGeneration: first.AllocationGeneration,
		Command: raftservice.CommandFence{
			ReplicaSetVersion: 1, ActivePolicyGeneration: 5, ProtectionEpoch: 6,
			OwnershipEpoch: uint64(first.Epoch), SchemaGeneration: 8,
			RelationManifestDigest: [32]byte{9},
			RoutingVersion:         uint64(manifest.Version()), RouteGeneration: 10,
		},
		Replicas: []ReplicatedReplicaDescriptor{
			{Member: 1, Endpoint: "ep-a"},
			{Member: 2, Endpoint: "ep-c"},
			{Member: 3, Endpoint: "ep-d"},
		},
	}
	return config, endpoints, descriptor
}

func TestDecodeFixed16HexRejectsNonCanonicalWidth(t *testing.T) {
	var destination [16]byte
	if err := decodeFixed16Hex("00", &destination); err == nil {
		t.Fatal("short fixed identity accepted")
	}
	if err := decodeFixed16Hex(string(bytes.Repeat([]byte{'0'}, 32)), &destination); err != nil {
		t.Fatal(err)
	}
}
