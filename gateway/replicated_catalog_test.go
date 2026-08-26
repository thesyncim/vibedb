package gateway

import (
	"bytes"
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
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
			route.RangeIdentity != descriptor.RangeIdentity ||
			route.LineageDigest != descriptor.LineageDigest ||
			route.ForwardingRuleDigest != descriptor.ForwardingRuleDigest ||
			len(route.Replicas) != ServingReplicaCount {
			t.Fatalf("resolved route = %+v,%v", route, ok)
		}
		for ordinal, replica := range route.Replicas {
			want := descriptor.Replicas[ordinal]
			if replica.Member != want.Member || replica.NativeEndpoint != string(want.NativeEndpoint) ||
				replica.Address != endpoints[want.NativeEndpoint] {
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

func TestReplicatedCatalogRejectsMissingLogicalRangeAuthority(t *testing.T) {
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	for name, clear := range map[string]func(*ReplicatedShardDescriptor){
		"range identity": func(value *ReplicatedShardDescriptor) {
			value.RangeIdentity = replication.Digest{}
		},
		"lineage digest": func(value *ReplicatedShardDescriptor) {
			value.LineageDigest = replication.Digest{}
		},
		"forwarding rule digest": func(value *ReplicatedShardDescriptor) {
			value.ForwardingRuleDigest = replication.Digest{}
		},
	} {
		t.Run(name, func(t *testing.T) {
			invalid := descriptor
			clear(&invalid)
			if _, err := NewSnapshotWithReplicatedMetadata(
				config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{invalid},
			); err == nil {
				t.Fatal("catalog accepted missing logical range authority")
			}
		})
	}
}

func TestReplicatedCatalogFreezesLogicalRangeAuthorityWithinAllocation(t *testing.T) {
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
	for name, change := range map[string]func(*ReplicatedShardDescriptor){
		"range identity": func(value *ReplicatedShardDescriptor) {
			value.RangeIdentity[1]++
		},
		"lineage digest": func(value *ReplicatedShardDescriptor) {
			value.LineageDigest[1]++
		},
		"forwarding rule digest": func(value *ReplicatedShardDescriptor) {
			value.ForwardingRuleDigest[1]++
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := descriptor
			change(&changed)
			next, err := NewSnapshotWithReplicatedMetadata(
				config, endpoints, 6, nil, nil, []ReplicatedShardDescriptor{changed},
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := advanceCatalogState(current, next); err == nil {
				t.Fatal("catalog accepted changed logical range authority within one allocation")
			}
		})
	}
}

func TestReplicatedCatalogSeparatesEnrolledTargetFromServingRF3(t *testing.T) {
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	endpoints["ep-target"] = "127.0.0.1:7005"
	endpoints["ep-target-native"] = "127.0.0.1:7105"
	endpoints["ep-target-control"] = "127.0.0.1:7205"
	descriptor.EnrolledTarget = &ReplicatedReplicaDescriptor{
		Member: 4, Node: [16]byte{4}, StoreID: [16]byte{14}, NodeIncarnation: 24,
		Endpoint: "ep-target", NativeEndpoint: "ep-target-native",
		ControlEndpoint: "ep-target-control",
	}
	snapshot, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	assertRoutes := func(snapshot *Snapshot) {
		t.Helper()
		var workspace [ServingReplicaCount + 1]ReplicatedEndpoint
		data, ok := snapshot.ResolveReplicatedRoute(
			descriptor.Distribution, descriptor.Shard, workspace[:0],
		)
		if !ok || len(data.Replicas) != ServingReplicaCount ||
			cap(data.Replicas) != ServingReplicaCount {
			t.Fatalf("data route = %+v,%v", data, ok)
		}
		for _, endpoint := range data.Replicas {
			if endpoint.Member == descriptor.EnrolledTarget.Member {
				t.Fatal("enrolled target leaked into the public data route")
			}
		}
		membership, ok := snapshot.ResolveReplicatedMembershipRoute(
			descriptor.Distribution, descriptor.Shard, workspace[:0],
		)
		if !ok || len(membership.Serving.Replicas) != ServingReplicaCount ||
			cap(membership.Serving.Replicas) != ServingReplicaCount ||
			!membership.HasEnrolledTarget ||
			membership.EnrolledTarget.Member != descriptor.EnrolledTarget.Member ||
			membership.EnrolledTarget.Address != endpoints[descriptor.EnrolledTarget.NativeEndpoint] {
			t.Fatalf("membership route = %+v,%v", membership, ok)
		}
	}
	assertRoutes(snapshot)
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveSnapshot(path, snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	assertRoutes(loaded)

	duplicate := descriptor
	target := *descriptor.EnrolledTarget
	target.Member = descriptor.Replicas[0].Member
	duplicate.EnrolledTarget = &target
	if _, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 6, nil, nil, []ReplicatedShardDescriptor{duplicate},
	); err == nil {
		t.Fatal("catalog accepted an enrolled target that repeats a serving member")
	}
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

	identityChanges := []struct {
		name   string
		mutate func(*ReplicatedReplicaDescriptor)
	}{
		{"node", func(replica *ReplicatedReplicaDescriptor) { replica.Node[0]++ }},
		{"store", func(replica *ReplicatedReplicaDescriptor) { replica.StoreID[0]++ }},
		{"incarnation", func(replica *ReplicatedReplicaDescriptor) { replica.NodeIncarnation++ }},
	}
	for _, test := range identityChanges {
		t.Run(test.name, func(t *testing.T) {
			changed := descriptor
			changed.Replicas = append([]ReplicatedReplicaDescriptor(nil), descriptor.Replicas...)
			test.mutate(&changed.Replicas[0])
			next, err := NewSnapshotWithReplicatedMetadata(
				config, endpoints, 6, nil, nil, []ReplicatedShardDescriptor{changed},
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := advanceCatalogState(current, next); err == nil {
				t.Fatal("catalog changed authenticated endpoint identity within one allocation")
			}
		})
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

func TestBuildReplicaReplacementTransitionRequiresExactCertifiedRF3Successor(t *testing.T) {
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
	grant, manifest, target, command := testCertifiedReplicaReplacement(t, current, descriptor)
	next, err := BuildReplicaReplacementTransition(
		current, manifest, 6, grant, target, command,
	)
	if err != nil {
		t.Fatal(err)
	}
	var workspace [ServingReplicaCount]ReplicatedEndpoint
	route, ok := next.ResolveReplicatedRoute(
		descriptor.Distribution, descriptor.Shard, workspace[:0],
	)
	if !ok || route.Command != command || route.Replicas[0].Member != grant.TargetMember ||
		route.Replicas[0].Node != target.Node || route.Replicas[1].Member != 2 ||
		route.Replicas[2].Member != 3 {
		t.Fatalf("replacement route=%+v ok=%v", route, ok)
	}
	if _, err = advanceCatalogState(current, next); err == nil {
		t.Fatal("ordinary catalog transition accepted a certified roster change without its grant")
	}

	wrongTarget := target
	wrongTarget.Member++
	if _, err = BuildReplicaReplacementTransition(
		current, manifest, 6, grant, wrongTarget, command,
	); err == nil {
		t.Fatal("replacement accepted a target outside the grant")
	}
	wrongCommand := command
	wrongCommand.SchemaGeneration++
	wrongCommand.RelationManifestDigest[0]++
	if _, err = BuildReplicaReplacementTransition(
		current, manifest, 6, grant, target, wrongCommand,
	); err == nil {
		t.Fatal("replacement accepted an unrelated serving-fence change")
	}
}

func testCertifiedReplicaReplacement(
	t testing.TB,
	current *Snapshot,
	descriptor ReplicatedShardDescriptor,
) (membershipgrant.Grant, *distribution.Manifest, ReplicatedReplicaDescriptor, raftservice.CommandFence) {
	t.Helper()
	grant := testReplicatedMembershipGrant(descriptor.Group)
	grant.CatalogGeneration = current.Generation()
	grant.InitialReplicaSetVersion = descriptor.Command.ReplicaSetVersion
	grant.InitialRosterDigest = replicatedCatalogInitialRosterDigest(current, 0)
	grant.InitialDescriptorDigest = replicatedCatalogInitialDescriptorDigest(current, 0)
	manifest, ok := current.Manifest(descriptor.Distribution)
	if !ok {
		t.Fatal("replacement manifest missing")
	}
	ordinal, metadata := manifestShardOrdinal(manifest, descriptor.Shard)
	if ordinal < 0 {
		t.Fatal("replacement shard missing")
	}
	nextManifest, err := manifest.ReplaceShardLeader(
		ordinal, manifest.Version()+1, 0, "ep-b", metadata.Epoch+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	target := ReplicatedReplicaDescriptor{
		Member: grant.TargetMember, Node: grant.TargetNode, StoreID: [16]byte{14},
		NodeIncarnation: 24, Endpoint: "ep-b", NativeEndpoint: "ep-b-native",
		ControlEndpoint: "ep-b-control",
	}
	command := descriptor.Command
	command.ReplicaSetVersion += 3
	command.OwnershipEpoch++
	command.RoutingVersion++
	command.RouteGeneration++
	return grant, nextManifest, target, command
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
	endpoints["ep-b-native"] = "127.0.0.1:7102"
	endpoints["ep-a-native"] = "127.0.0.1:7101"
	endpoints["ep-c-native"] = "127.0.0.1:7103"
	endpoints["ep-d-native"] = "127.0.0.1:7104"
	endpoints["ep-b-control"] = "127.0.0.1:7202"
	endpoints["ep-a-control"] = "127.0.0.1:7201"
	endpoints["ep-c-control"] = "127.0.0.1:7203"
	endpoints["ep-d-control"] = "127.0.0.1:7204"
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
		RangeIdentity:        replication.Digest{0x71}, LineageDigest: replication.Digest{0x72},
		ForwardingRuleDigest: replication.Digest{0x73},
		Command: raftservice.CommandFence{
			ReplicaSetVersion: 1, ActivePolicyGeneration: 5, ProtectionEpoch: 6,
			OwnershipEpoch: uint64(first.Epoch), SchemaGeneration: 8,
			RelationManifestDigest: [32]byte{9},
			RoutingVersion:         uint64(manifest.Version()), RouteGeneration: 10,
		},
		Replicas: []ReplicatedReplicaDescriptor{
			{Member: 1, Node: [16]byte{1}, StoreID: [16]byte{11}, NodeIncarnation: 21, Endpoint: "ep-a", NativeEndpoint: "ep-a-native", ControlEndpoint: "ep-a-control"},
			{Member: 2, Node: [16]byte{2}, StoreID: [16]byte{12}, NodeIncarnation: 22, Endpoint: "ep-c", NativeEndpoint: "ep-c-native", ControlEndpoint: "ep-c-control"},
			{Member: 3, Node: [16]byte{3}, StoreID: [16]byte{13}, NodeIncarnation: 23, Endpoint: "ep-d", NativeEndpoint: "ep-d-native", ControlEndpoint: "ep-d-control"},
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
