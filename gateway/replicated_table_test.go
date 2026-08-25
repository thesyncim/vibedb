package gateway

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestReplicatedTableProfileRoundTripAndAllocationFreeResolution(t *testing.T) {
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	snapshot, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 5, nil, nil,
		[]ReplicatedShardDescriptor{descriptor}, []ReplicatedTableProfile{profile},
	)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ReplicatedTableMetadataBytes() == 0 ||
		snapshot.ReplicatedMetadataBytes() <= snapshot.ReplicatedTableMetadataBytes() {
		t.Fatalf("metadata bytes = tables %d, all %d",
			snapshot.ReplicatedTableMetadataBytes(), snapshot.ReplicatedMetadataBytes())
	}
	key, ok := orderedkey.AppendString(nil, []byte("customer-17"), orderedkey.Ascending)
	if !ok {
		t.Fatal("ordered key")
	}
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	var scalarScratch [replication.MaxMutationKeyBytes + 16]byte
	assertResolved := func(candidate *Snapshot) ResolvedReplicatedTableKey {
		t.Helper()
		resolved, found := candidate.ResolveReplicatedTableKey(
			[]byte(profile.Table), key, scalarScratch[:0], replicas[:0],
		)
		if !found || resolved.Profile != profile || resolved.Route.Group != descriptor.Group ||
			resolved.Route.AllocationGeneration != uint64(descriptor.AllocationGeneration) ||
			resolved.Route.Command != descriptor.Command ||
			len(resolved.Route.Replicas) != ServingReplicaCount ||
			resolved.RouteID == (replication.Digest{}) {
			t.Fatalf("resolved = %+v,%v", resolved, found)
		}
		return resolved
	}
	want := assertResolved(snapshot)
	if allocations := testing.AllocsPerRun(1000, func() {
		_, _ = snapshot.ResolveReplicatedTableKey(
			[]byte(profile.Table), key, scalarScratch[:0], replicas[:0],
		)
	}); allocations != 0 {
		lookupAllocs := testing.AllocsPerRun(1000, func() {
			_, _ = snapshot.replicatedTableAtBytes([]byte(profile.Table))
		})
		routeAllocs := testing.AllocsPerRun(1000, func() {
			_, _ = snapshot.ResolveReplicatedRoute("data", "all", replicas[:0])
		})
		idAllocs := testing.AllocsPerRun(1000, func() {
			_ = replicatedRouteID(want.Route)
		})
		t.Fatalf("resolve allocations = %f (lookup=%f route=%f id=%f)",
			allocations, lookupAllocs, routeAllocs, idAllocs)
	}
	numberKey, ok := orderedkey.AppendNumber(nil, []byte("17.00e0"), orderedkey.Ascending)
	if !ok {
		t.Fatal("ordered number key")
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		_, _ = snapshot.ResolveReplicatedTableKey(
			[]byte(profile.Table), numberKey, scalarScratch[:0], replicas[:0],
		)
	}); allocations != 0 {
		t.Fatalf("number resolve allocations = %f", allocations)
	}
	if _, found := snapshot.ResolveReplicatedTableKey(
		[]byte(profile.Table), numberKey, scalarScratch[:0], replicas[:0],
	); !found {
		t.Fatal("canonical number key did not resolve")
	}

	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveSnapshot(path, snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := assertResolved(loaded); got.RouteID != want.RouteID {
		t.Fatalf("route id changed across persistence: %x != %x", got.RouteID, want.RouteID)
	}
	document, err := AppendSnapshotDocument(nil, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(document, []byte(`"replicated_tables":[{"max_document_bytes":4194304,"max_key_bytes":256,"primary_key":"/id","relation":1`)) {
		t.Fatalf("replicated_tables is absent or noncanonical: %s", document)
	}
	opened, err := OpenSnapshotDocument(document)
	if err != nil {
		t.Fatal(err)
	}
	assertResolved(opened)
}

func TestReplicatedTableProfileRejectsIncompleteOrMismatchedCatalog(t *testing.T) {
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	cases := []struct {
		name   string
		mutate func(*distribution.ClusterConfig, *ReplicatedShardDescriptor, *ReplicatedTableProfile)
	}{
		{"relation", func(_ *distribution.ClusterConfig, _ *ReplicatedShardDescriptor, p *ReplicatedTableProfile) {
			p.Relation = 2
		}},
		{"primary", func(_ *distribution.ClusterConfig, _ *ReplicatedShardDescriptor, p *ReplicatedTableProfile) {
			p.PrimaryKey = "/other"
		}},
		{"schema", func(_ *distribution.ClusterConfig, _ *ReplicatedShardDescriptor, p *ReplicatedTableProfile) {
			p.SchemaGeneration++
		}},
		{"digest", func(_ *distribution.ClusterConfig, _ *ReplicatedShardDescriptor, p *ReplicatedTableProfile) {
			p.RelationManifestDigest[0]++
		}},
		{"zero-key-limit", func(_ *distribution.ClusterConfig, _ *ReplicatedShardDescriptor, p *ReplicatedTableProfile) {
			p.MaxKeyBytes = 0
		}},
		{"large-key-limit", func(_ *distribution.ClusterConfig, _ *ReplicatedShardDescriptor, p *ReplicatedTableProfile) {
			p.MaxKeyBytes = replication.MaxMutationKeyBytes + 1
		}},
		{"zero-document-limit", func(_ *distribution.ClusterConfig, _ *ReplicatedShardDescriptor, p *ReplicatedTableProfile) {
			p.MaxDocumentBytes = 0
		}},
		{"large-document-limit", func(_ *distribution.ClusterConfig, _ *ReplicatedShardDescriptor, p *ReplicatedTableProfile) {
			p.MaxDocumentBytes = replication.MaxMutationValueBytes + 1
		}},
		{"multi-column-placement", func(c *distribution.ClusterConfig, _ *ReplicatedShardDescriptor, _ *ReplicatedTableProfile) {
			c.Distributions[0].Arity = 2
			c.Placements[0].Columns = []string{"/id", "/region"}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			candidateConfig := cloneConfig(config)
			candidateDescriptor := descriptor
			candidateDescriptor.Replicas = append(
				[]ReplicatedReplicaDescriptor(nil), descriptor.Replicas...,
			)
			candidateProfile := profile
			test.mutate(&candidateConfig, &candidateDescriptor, &candidateProfile)
			_, err := NewSnapshotWithReplicatedTableMetadata(
				candidateConfig, endpoints, 5, nil, nil,
				[]ReplicatedShardDescriptor{candidateDescriptor},
				[]ReplicatedTableProfile{candidateProfile},
			)
			if !errors.Is(err, ErrInvalidCatalog) &&
				!errors.Is(err, distribution.ErrInvalidPlacement) {
				t.Fatalf("error = %v", err)
			}
		})
	}

	if _, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 5, nil, nil, nil, []ReplicatedTableProfile{profile},
	); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("profile without routes = %v", err)
	}
}

func TestReplicatedTableProfileTransitionIsMonotonicAndCannotDisappear(t *testing.T) {
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	current, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 5, nil, nil,
		[]ReplicatedShardDescriptor{descriptor}, []ReplicatedTableProfile{profile},
	)
	if err != nil {
		t.Fatal(err)
	}
	current, err = initialCatalogState(current)
	if err != nil {
		t.Fatal(err)
	}

	without, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 6, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := advanceCatalogState(current, without); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("removed profile = %v", err)
	}

	changedLimits := profile
	changedLimits.MaxDocumentBytes--
	changed, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 6, nil, nil,
		[]ReplicatedShardDescriptor{descriptor}, []ReplicatedTableProfile{changedLimits},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := advanceCatalogState(current, changed); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("changed limits = %v", err)
	}

	advancedDescriptor := descriptor
	advancedDescriptor.Command.SchemaGeneration++
	advancedDescriptor.Command.RelationManifestDigest[0]++
	advancedProfile := profile
	advancedProfile.SchemaGeneration = advancedDescriptor.Command.SchemaGeneration
	advancedProfile.RelationManifestDigest = replication.Digest(
		advancedDescriptor.Command.RelationManifestDigest,
	)
	advanced, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 6, nil, nil,
		[]ReplicatedShardDescriptor{advancedDescriptor},
		[]ReplicatedTableProfile{advancedProfile},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := advanceCatalogState(current, advanced); err != nil {
		t.Fatalf("advanced profile = %v", err)
	}
}

func TestResolveReplicatedTableKeyFailsClosedOnNoncanonicalKey(t *testing.T) {
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	snapshot, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 5, nil, nil,
		[]ReplicatedShardDescriptor{descriptor}, []ReplicatedTableProfile{profile},
	)
	if err != nil {
		t.Fatal(err)
	}
	ascending, _ := orderedkey.AppendString(nil, []byte("customer"), orderedkey.Ascending)
	descending, _ := orderedkey.AppendString(nil, []byte("customer"), orderedkey.Descending)
	composite, _ := orderedkey.AppendString(append([]byte(nil), ascending...), []byte("other"), orderedkey.Ascending)
	boolean, _ := orderedkey.AppendBool(nil, true, orderedkey.Ascending)
	malformed := append([]byte(nil), ascending[:len(ascending)-1]...)
	for name, key := range map[string][]byte{
		"missing-table": ascending,
		"descending":    descending,
		"composite":     composite,
		"boolean":       boolean,
		"malformed":     malformed,
	} {
		t.Run(name, func(t *testing.T) {
			table := []byte(profile.Table)
			if name == "missing-table" {
				table = []byte("other")
			}
			var replicas [ServingReplicaCount]ReplicatedEndpoint
			var scalarScratch [replication.MaxMutationKeyBytes + 16]byte
			if resolved, ok := snapshot.ResolveReplicatedTableKey(
				table, key, scalarScratch[:0], replicas[:0],
			); ok {
				t.Fatalf("resolved = %+v", resolved)
			}
		})
	}
}

func testReplicatedTableInput(
	t testing.TB,
) (distribution.ClusterConfig, map[distribution.EndpointID]string, ReplicatedShardDescriptor, ReplicatedTableProfile) {
	t.Helper()
	manifest, err := distribution.NewManifest("data", 3, []distribution.Shard{{
		ID: "all", AllocationGeneration: 1,
		Range: distribution.KeyRange{
			Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Max: true},
		},
		Leaders: []distribution.EndpointID{"peer-a", "peer-b", "peer-c"}, Epoch: 7,
	}})
	if err != nil {
		t.Fatal(err)
	}
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{
			Name: "data", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
		}},
		Placements: []distribution.TablePlacement{{
			Table: "messages", Distribution: "data", Columns: []string{"/id"},
		}},
		Manifests: []*distribution.Manifest{manifest},
	}
	endpoints := map[distribution.EndpointID]string{
		"peer-a": "127.0.0.1:7001", "peer-b": "127.0.0.1:7002", "peer-c": "127.0.0.1:7003",
		"native-a": "127.0.0.1:7101", "native-b": "127.0.0.1:7102", "native-c": "127.0.0.1:7103",
		"control-a": "127.0.0.1:7201", "control-b": "127.0.0.1:7202", "control-c": "127.0.0.1:7203",
	}
	group := raftmember.GroupKey{TopologyRecoveryEpoch: 11}
	for ordinal := range group.ClusterID {
		group.ClusterID[ordinal] = byte(ordinal + 1)
		group.ClusterIncarnation[ordinal] = byte(ordinal + 21)
		group.ShardIncarnation[ordinal] = byte(ordinal + 41)
		group.GroupID[ordinal] = byte(ordinal + 61)
	}
	descriptor := ReplicatedShardDescriptor{
		Distribution: "data", Shard: "all", Group: group, AllocationGeneration: 1,
		Command: raftservice.CommandFence{
			ReplicaSetVersion: 1, ActivePolicyGeneration: 5, ProtectionEpoch: 6,
			OwnershipEpoch: 7, SchemaGeneration: 8,
			RelationManifestDigest: [32]byte{9}, RoutingVersion: 3, RouteGeneration: 10,
		},
		Replicas: []ReplicatedReplicaDescriptor{
			{Member: 1, Node: [16]byte{1}, StoreID: [16]byte{11}, NodeIncarnation: 21, Endpoint: "peer-a", NativeEndpoint: "native-a", ControlEndpoint: "control-a"},
			{Member: 2, Node: [16]byte{2}, StoreID: [16]byte{12}, NodeIncarnation: 22, Endpoint: "peer-b", NativeEndpoint: "native-b", ControlEndpoint: "control-b"},
			{Member: 3, Node: [16]byte{3}, StoreID: [16]byte{13}, NodeIncarnation: 23, Endpoint: "peer-c", NativeEndpoint: "native-c", ControlEndpoint: "control-c"},
		},
	}
	profile := ReplicatedTableProfile{
		Table: "messages", Relation: 1, PrimaryKey: "/id", SchemaGeneration: 8,
		RelationManifestDigest: replication.Digest{9}, MaxKeyBytes: 256,
		MaxDocumentBytes: 4 << 20,
	}
	return config, endpoints, descriptor, profile
}
