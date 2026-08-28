package gateway

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

func TestReplicatedSplitOriginRoundTripDetachedAndImmutable(t *testing.T) {
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	parent := descriptor.Group
	parent.GroupID[0]++
	origin := &ReplicatedSplitOrigin{RootGroup: parent, ParentGroup: parent, Operation: [32]byte{1},
		PlanDigest: [32]byte{2}, CutoverDigest: [32]byte{3}, SchemaGeneration: descriptor.Command.SchemaGeneration,
		RelationManifestDigest: descriptor.Command.RelationManifestDigest, Child: 1}
	descriptor.SplitOrigin = origin
	snapshot, err := NewSnapshotWithReplicatedMetadata(config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor})
	if err != nil {
		t.Fatal(err)
	}
	origin.Operation[0]++
	if snapshot.ReplicatedShardDescriptors()[0].SplitOrigin.Operation == origin.Operation {
		t.Fatal("caller alias changed live origin")
	}
	path := filepath.Join(t.TempDir(), "catalog.vibejson")
	if err = SaveSnapshot(path, snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot.ReplicatedShardDescriptors(), loaded.ReplicatedShardDescriptors()) {
		t.Fatal("origin changed on reopen")
	}
	changed := loaded.ReplicatedShardDescriptors()
	changed[0].SplitOrigin.PlanDigest[0]++
	next, err := NewSnapshotWithReplicatedMetadata(config, endpoints, 6, nil, nil, changed)
	if err != nil {
		t.Fatal(err)
	}
	if err = validateReplicatedCatalogTransition(snapshot, next); err == nil {
		t.Fatal("existing allocation provenance rewritten")
	}
	for name, mutate := range map[string]func(*ReplicatedSplitOrigin){
		"foreign cluster": func(o *ReplicatedSplitOrigin) { o.RootGroup.ClusterID[0]++ },
		"same group":      func(o *ReplicatedSplitOrigin) { o.ParentGroup = descriptor.Group },
		"zero operation":  func(o *ReplicatedSplitOrigin) { o.Operation = [32]byte{} },
		"future schema":   func(o *ReplicatedSplitOrigin) { o.SchemaGeneration = descriptor.Command.SchemaGeneration + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			copy := *snapshot.ReplicatedShardDescriptors()[0].SplitOrigin
			mutate(&copy)
			descriptor.SplitOrigin = &copy
			if _, err := NewSnapshotWithReplicatedMetadata(config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor}); err == nil {
				t.Fatal("invalid origin accepted")
			}
		})
	}
}

func TestReplicatedCatalogNativeLeaderRetainsPeerAndRejectsCrossReplicaAlias(t *testing.T) {
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	first, _ := config.Manifests[0].ShardInfo(0)
	second, _ := config.Manifests[0].ShardInfo(1)
	for i := range first.Leaders {
		first.Leaders[i] = descriptor.Replicas[i].NativeEndpoint
	}
	manifest, err := distribution.NewManifest(descriptor.Distribution, config.Manifests[0].Version(), []distribution.Shard{first, second})
	if err != nil {
		t.Fatal(err)
	}
	config.Manifests[0] = manifest
	snapshot, err := NewSnapshotWithReplicatedMetadata(config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot.ReplicatedShardDescriptors()[0].Replicas, descriptor.Replicas) {
		t.Fatal("peer/native coordinates swapped during projection")
	}
	bad := descriptor
	bad.Replicas = append([]ReplicatedReplicaDescriptor(nil), descriptor.Replicas...)
	bad.Replicas[0].NativeEndpoint = bad.Replicas[1].NativeEndpoint
	if _, err := NewSnapshotWithReplicatedMetadata(config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{bad}); err == nil {
		t.Fatal("cross-replica alias accepted")
	}
	bad = descriptor
	bad.Replicas = append([]ReplicatedReplicaDescriptor(nil), descriptor.Replicas...)
	bad.Replicas[0].Endpoint, bad.Replicas[0].NativeEndpoint = bad.Replicas[0].NativeEndpoint, bad.Replicas[0].Endpoint
	next, err := NewSnapshotWithReplicatedMetadata(config, endpoints, 6, nil, nil, []ReplicatedShardDescriptor{bad})
	if err != nil {
		t.Fatal(err)
	}
	if err = validateReplicatedCatalogTransition(snapshot, next); err == nil {
		t.Fatal("peer/native role swap accepted during reload")
	}
}

func TestReplicatedCatalogManifestAdvanceDoesNotInventShardFence(t *testing.T) {
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	current, err := NewSnapshotWithReplicatedMetadata(config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor})
	if err != nil {
		t.Fatal(err)
	}
	first, _ := config.Manifests[0].ShardInfo(0)
	second, _ := config.Manifests[0].ShardInfo(1)
	nextManifest, err := distribution.NewManifest(descriptor.Distribution, config.Manifests[0].Version()+1, []distribution.Shard{first, second})
	if err != nil {
		t.Fatal(err)
	}
	next, err := BuildManifestTransition(current, nextManifest, 6)
	if err != nil {
		t.Fatal(err)
	}
	if next.ReplicatedShardDescriptors()[0].Command != descriptor.Command {
		t.Fatal("manifest advance invented an applied shard fence")
	}
	bad := descriptor
	bad.Command.RoutingVersion = uint64(nextManifest.Version() + 1)
	config.Manifests[0] = nextManifest
	if _, err := NewSnapshotWithReplicatedMetadata(config, endpoints, 6, nil, nil, []ReplicatedShardDescriptor{bad}); err == nil {
		t.Fatal("future shard fence accepted")
	}
}
