package gatewayruntime

import (
	"slices"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
)

func TestGatewayProvisioningRecoveryAfterReplicaMove(t *testing.T) {
	initial, source, profile, work := gatewayHotSplitFactoryFixture(t)
	entry := gatewaySplitSourceFixture(t, source, profile)
	manifest := gatewayReplicaControlManifest{SplitSources: []gatewaySplitSource{entry}}
	addresses := make(map[distribution.EndpointID]string)
	for index, replica := range source.Replicas {
		manifest.Shards = append(manifest.Shards, gateway.ReplicatedEndpoint{Node: replica.Node,
			ControlAddress: "127.0.0.1:" + strconv.Itoa(21+index)})
		manifest.SplitSnapshots = append(manifest.SplitSnapshots, "127.0.0.1:"+strconv.Itoa(9301+index))
		for _, endpoint := range []distribution.EndpointID{replica.Endpoint, replica.NativeEndpoint, replica.ControlEndpoint} {
			addresses[endpoint], _ = initial.Address(endpoint)
		}
	}
	moved := source
	moved.Replicas = slices.Clone(source.Replicas)
	moved.Replicas[0].Member, moved.Replicas[0].Node, moved.Replicas[0].StoreID = 4, [16]byte{4}, [16]byte{44}
	moved.Command.ReplicaSetVersion++
	manifest.Shards = append(manifest.Shards, gateway.ReplicatedEndpoint{Node: moved.Replicas[0].Node, ControlAddress: "127.0.0.1:21"})
	manifest.SplitSnapshots = append(manifest.SplitSnapshots, "127.0.0.1:9304")
	spec, _ := initial.Spec(source.Distribution)
	placement, _ := initial.Placement(profile.Table)
	shards, _ := initial.Manifest(source.Distribution)
	current, err := gateway.NewSnapshotWithReplicatedTableMetadata(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{spec}, Placements: []distribution.TablePlacement{placement},
		Manifests: []*distribution.Manifest{shards}}, addresses, initial.Generation()+1, nil, nil,
		[]gateway.ReplicatedShardDescriptor{moved}, []gateway.ReplicatedTableProfile{profile})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{provisioningCatalog: initial, holder: gateway.NewCatalogHolder(current)}
	serving := runtime.holder.Current()
	if serving == nil || serving.Generation() != current.Generation() {
		t.Fatal("fixture has no current serving catalog")
	}
	factory, err := runtime.newHotSplitFactory(manifest)
	if err != nil {
		t.Fatalf("replica movement invalidated original schema provision: %v", err)
	}
	retained, found := factory.sourceForDescriptor(moved)
	if !found || !retained.SQL.Equal(entry.SQL) || runtime.holder.Current() != serving {
		t.Fatal("recovery changed schema provenance or current serving placement")
	}
	work.Candidate.CatalogGeneration = current.Generation()
	if _, err := factory.BuildHotSplitPlan(t.Context(), current, [32]byte{0x71}, work); err == nil {
		t.Fatal("historical root inventory authorized a replacement without a root")
	}
	// An operator can enroll split roots on the certified replacement roster
	// while retaining the exact original schema image. Placement is current;
	// the immutable provisioning catalog must not pin the old physical hosts.
	manifest.SplitSources[0].Replicas[0].Node = moved.Replicas[0].Node
	manifest.SplitSources[0].Replicas[0].Root = t.TempDir()
	manifest.SplitSources[0].SQL = gatewaySplitSourceSQLFixture(t, moved, profile)
	manifest.SplitSources[0].SQL.Binding.Authority.RouteGeneration++
	factory, err = runtime.newHotSplitFactory(manifest)
	if err != nil {
		t.Fatalf("current split roots rejected after replica movement: %v", err)
	}
	if _, err := factory.BuildHotSplitPlan(t.Context(), current, [32]byte{0x71}, work); err != nil {
		t.Fatalf("current certified roster cannot split after root enrollment: %v", err)
	}
	manifest.SplitSources[0].SQL.UserLimits.MaxKeyBytes++
	if _, err := runtime.newHotSplitFactory(manifest); err == nil {
		t.Fatal("modified original schema proof accepted")
	}
}
