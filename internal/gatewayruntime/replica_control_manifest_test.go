package gatewayruntime

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

func gatewaySplitTemplateFixture() persistedGatewaySplitTemplate {
	return persistedGatewaySplitTemplate{
		MaxSessions: 32, RetryWindow: 8,
		TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20},
		ShardKey:  "/id", MaxBatchDocuments: 64, MaxBatchBytes: 16<<20 + 64*256,
		TupleVersion: uint16(distribution.CurrentTupleVersion), MapperVersion: uint16(distribution.NativeMapperVersion),
	}
}

func gatewayReplicaManifestFixture(t testing.TB) ([]byte, persistedGatewayReplicaControlManifest) {
	t.Helper()
	manifest := persistedGatewayReplicaControlManifest{Generation: 7,
		LocalGateway: persistedGatewayControlEndpoint{Node: "01000000000000000000000000000000",
			Incarnation: 11, ControlAddress: "127.0.0.1:7101"},
		TLS: persistedGatewayReplicaTLS{Certificate: "/tls/cert", Key: "/tls/key",
			Roots: "/tls/roots", IdentityOID: "1.2.3.4", AuthorizationPolicy: "/tls/policy"},
		Bounds: persistedGatewayReplicaBounds{MaxConnections: 32, MaxHandshakes: 8,
			MaxConcurrentDrains: 4, ControllerInterval: 100, ReadTimeout: 1000, WriteTimeout: 1000},
		ShardEndpoints: []persistedGatewayShardControlEndpoint{
			{Node: "0a000000000000000000000000000000", ControlAddress: "127.0.0.1:7201", SplitSnapshotAddress: "127.0.0.1:7301"},
			{Node: "0b000000000000000000000000000000", ControlAddress: "127.0.0.1:7202", SplitSnapshotAddress: "127.0.0.1:7302"},
			{Node: "1f000000000000000000000000000000", ControlAddress: "127.0.0.1:7203", SplitSnapshotAddress: "127.0.0.1:7303"},
			{Node: "20000000000000000000000000000000", ControlAddress: "127.0.0.1:7204", SplitSnapshotAddress: "127.0.0.1:7304"}},
		GatewayEndpoints: []persistedGatewayControlEndpoint{
			{Node: "01000000000000000000000000000000", Incarnation: 11, ControlAddress: "127.0.0.1:7101"},
			{Node: "02000000000000000000000000000000", Incarnation: 12, ControlAddress: "127.0.0.1:7102"}},
		Candidates: []persistedGatewayReplacementCandidate{
			{Member: 31, Node: "1f000000000000000000000000000000", Store: "29000000000000000000000000000000",
				NodeIncarnation: 41, Endpoint: "candidate-a", Load: 2},
			{Member: 32, Node: "20000000000000000000000000000000", Store: "2a000000000000000000000000000000",
				NodeIncarnation: 42, Endpoint: "candidate-b", Load: 3}},
	}
	raw, err := vibejson.Marshal(&manifest)
	if err != nil {
		t.Fatal(err)
	}
	return raw, manifest
}

func TestGatewayReplicaControlManifestCanonicalExactAndInventoryBound(t *testing.T) {
	raw, _ := gatewayReplicaManifestFixture(t)
	var local [16]byte
	local[0] = 1
	manifest, err := openGatewayReplicaControlManifest(raw, local)
	if err != nil || manifest.Generation != 7 || len(manifest.Shards) != 4 ||
		len(manifest.Gateways) != 2 || manifest.Local.Member.Incarnation != 11 {
		t.Fatalf("manifest=%+v err=%v", manifest, err)
	}
	catalog := &gateway.Snapshot{}
	if _, err = manifest.ReplacementCandidates(t.Context(), catalog,
		rebalance.FailureQuorumCertificate{}); err == nil {
		t.Fatal("unfenced candidate inventory accepted")
	}
	// The inventory binds dynamic authority epochs at read time rather than
	// persisting a stale epoch in static placement configuration.
	certificate := rebalance.FailureQuorumCertificate{CatalogGeneration: catalog.Generation(),
		ConfirmedEpoch: 9, Group: raftmember.GroupKey{TopologyRecoveryEpoch: 13}}
	candidates, err := manifest.ReplacementCandidates(t.Context(), catalog, certificate)
	if err != nil || len(candidates) != 2 || candidates[0].HealthEpoch != 9 ||
		candidates[0].TopologyRecoveryEpoch != 13 {
		t.Fatalf("candidates=%+v err=%v", candidates, err)
	}
	for _, invalid := range [][]byte{
		append(append([]byte(nil), raw...), '\n'),
		bytes.Replace(raw, []byte(`"generation":7`), []byte(`"unknown":1,"generation":7`), 1),
		bytes.Replace(raw, []byte(`"node":"01000000000000000000000000000000"`),
			[]byte(`"node":"03000000000000000000000000000000"`), 1),
	} {
		if _, err = openGatewayReplicaControlManifest(invalid, local); err == nil {
			t.Fatalf("invalid canonical manifest accepted: %s", invalid)
		}
	}
}

func TestGatewayReplicaControlManifestAllowsCertifiedSplitOnlyInventory(t *testing.T) {
	_, persisted := gatewayReplicaManifestFixture(t)
	persisted.Candidates = nil
	raw, err := vibejson.Marshal(&persisted)
	if err != nil {
		t.Fatal(err)
	}
	var local [16]byte
	local[0] = 1
	manifest, err := openGatewayReplicaControlManifest(raw, local)
	if err != nil || len(manifest.Candidates) != 0 || len(manifest.Shards) == 0 {
		t.Fatalf("manifest=%+v err=%v", manifest, err)
	}
	catalog := &gateway.Snapshot{}
	certificate := rebalance.FailureQuorumCertificate{CatalogGeneration: catalog.Generation(),
		ConfirmedEpoch: 1, Group: raftmember.GroupKey{TopologyRecoveryEpoch: 1}}
	candidates, err := manifest.ReplacementCandidates(t.Context(), catalog, certificate)
	if err != nil || len(candidates) != 0 {
		t.Fatalf("split-only candidates=%+v err=%v", candidates, err)
	}
}

func TestGatewayReplicaControlManifestBindsServingTargetFromDirectory(t *testing.T) {
	base, descriptor, profile, _ := gatewayHotSplitFactoryFixture(t)
	static := gatewayReplicaControlManifest{
		Shards: []gateway.ReplicatedEndpoint{
			{Node: descriptor.Replicas[0].Node, ControlAddress: "127.0.0.1:21"},
			{Node: descriptor.Replicas[1].Node, ControlAddress: "127.0.0.1:22"},
			{Node: descriptor.Replicas[2].Node, ControlAddress: "127.0.0.1:23"},
		},
		SplitSnapshots: []string{"127.0.0.1:9301", "127.0.0.1:9302", "127.0.0.1:9303"},
	}
	target := descriptor.Replicas[2]
	target.Member = 4
	target.Node = rafttransport.NodeID{4}
	target.StoreID = [16]byte{14}
	target.NodeIncarnation = 24
	target.Endpoint = "peer-d"
	target.NativeEndpoint = "native-d"
	target.ControlEndpoint = "control-d"
	serving := descriptor
	serving.Replicas = append(append([]gateway.ReplicatedReplicaDescriptor(nil), descriptor.Replicas[:2]...), target)
	spec, ok := base.Spec(descriptor.Distribution)
	if !ok {
		t.Fatal("missing distribution spec")
	}
	placement, ok := base.Placement(profile.Table)
	if !ok {
		t.Fatal("missing table placement")
	}
	routing, ok := base.Manifest(descriptor.Distribution)
	if !ok {
		t.Fatal("missing distribution manifest")
	}
	full := distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
	dynamicRouting, err := distribution.NewManifest(descriptor.Distribution, routing.Version(), []distribution.Shard{{
		ID: "all", AllocationGeneration: descriptor.AllocationGeneration, Range: full,
		Leaders: []distribution.EndpointID{"peer-a", "peer-b", "peer-d"}, Epoch: distribution.OwnershipEpoch(descriptor.Command.OwnershipEpoch),
	}})
	if err != nil {
		t.Fatal(err)
	}
	endpoints := map[distribution.EndpointID]string{
		"peer-a": "127.0.0.1:1", "peer-b": "127.0.0.1:2", "peer-d": "127.0.0.1:4",
		"native-a": "127.0.0.1:11", "native-b": "127.0.0.1:12", "native-d": "127.0.0.1:14",
		"control-a": "127.0.0.1:21", "control-b": "127.0.0.1:22", "control-d": targetControlAddress,
	}
	dynamicCatalog, err := gateway.NewSnapshotWithReplicatedTableMetadata(
		distribution.ClusterConfig{Distributions: []distribution.DistributionSpec{spec},
			Placements: []distribution.TablePlacement{placement}, Manifests: []*distribution.Manifest{dynamicRouting}},
		endpoints, base.Generation()+1, nil, nil, []gateway.ReplicatedShardDescriptor{serving},
		[]gateway.ReplicatedTableProfile{profile},
	)
	if err != nil {
		t.Fatal(err)
	}
	node := gateway.NodeRecord{
		NodeID: target.Node, Incarnation: target.NodeIncarnation, ServiceKeyDigest: replication.Digest{4},
		DataEndpoint: "physical-peer", NativeEndpoint: "physical-native", ControlEndpoint: "physical-control",
		DataAddress: "127.0.0.1:7004", NativeAddress: "127.0.0.1:7104", ControlAddress: targetControlAddress,
		SnapshotAddress: "127.0.0.1:7304", FailureDomain: "zone-target", Roles: gateway.NodeRoleStorage,
		Lifecycle: gateway.NodeActive, Revision: 7, CatalogGeneration: dynamicCatalog.Generation(),
	}
	if !node.Valid() {
		t.Fatal("dynamic node fixture is invalid")
	}
	bound, err := bindGatewayReplicaControlManifestToDirectory(static, dynamicCatalog, []gateway.NodeRecord{node})
	if err != nil {
		t.Fatalf("serving target was not bound: %v", err)
	}
	if len(static.Shards) != 3 || len(bound.Shards) != 4 || len(bound.SplitSnapshots) != 4 {
		t.Fatalf("static=%d bound=%d snapshots=%d", len(static.Shards), len(bound.Shards), len(bound.SplitSnapshots))
	}
	var foundTarget bool
	for index, shard := range bound.Shards {
		if shard.Node == target.Node {
			foundTarget = shard.ControlAddress == node.ControlAddress && bound.SplitSnapshots[index] == node.SnapshotAddress
		}
	}
	if !foundTarget {
		t.Fatal("bound manifest lost the authenticated dynamic target address")
	}

	// A pending target is intentionally not promoted into the serving roster;
	// its catalog descriptor remains valid against the unchanged static seed.
	pending := descriptor
	pending.EnrolledTarget = &target
	pendingCatalog, err := gateway.NewSnapshotWithReplicatedTableMetadata(
		distribution.ClusterConfig{Distributions: []distribution.DistributionSpec{spec},
			Placements: []distribution.TablePlacement{placement}, Manifests: []*distribution.Manifest{routing}},
		map[distribution.EndpointID]string{
			"peer-a": "127.0.0.1:1", "peer-b": "127.0.0.1:2", "peer-c": "127.0.0.1:3",
			"native-a": "127.0.0.1:11", "native-b": "127.0.0.1:12", "native-c": "127.0.0.1:13",
			"control-a": "127.0.0.1:21", "control-b": "127.0.0.1:22", "control-c": "127.0.0.1:23",
			"peer-d": "127.0.0.1:4", "native-d": "127.0.0.1:14", "control-d": targetControlAddress,
		}, base.Generation()+1, nil, nil, []gateway.ReplicatedShardDescriptor{pending},
		[]gateway.ReplicatedTableProfile{profile},
	)
	if err != nil {
		t.Fatal(err)
	}
	unchanged, err := bindGatewayReplicaControlManifestToDirectory(static, pendingCatalog, nil)
	if err != nil || len(unchanged.Shards) != len(static.Shards) {
		t.Fatalf("pending target changed static roster: manifest=%+v err=%v", unchanged, err)
	}

	for name, altered := range map[string]gateway.NodeRecord{
		"missing":          node,
		"foreign-control":  func() gateway.NodeRecord { copy := node; copy.ControlAddress = "127.0.0.1:9999"; return copy }(),
		"missing-snapshot": func() gateway.NodeRecord { copy := node; copy.SnapshotAddress = ""; return copy }(),
	} {
		var records []gateway.NodeRecord
		if name != "missing" {
			records = []gateway.NodeRecord{altered}
		}
		if _, bindErr := bindGatewayReplicaControlManifestToDirectory(static, dynamicCatalog, records); bindErr == nil {
			t.Fatalf("%s dynamic binding was accepted", name)
		}
	}
}

const targetControlAddress = "127.0.0.1:24"
