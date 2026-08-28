package main

import (
	"encoding/hex"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

func TestGatewaySplitSourceCanonicalBounds(t *testing.T) {
	_, persisted := gatewayReplicaManifestFixture(t)
	_, source, profile, _ := gatewayHotSplitFactoryFixture(t)
	entry := persistedGatewaySplitSource{ClusterID: source.Group.ClusterID, ClusterIncarnation: source.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: source.Group.TopologyRecoveryEpoch, ShardIncarnation: source.Group.ShardIncarnation, GroupID: source.Group.GroupID,
		SchemaGeneration: profile.SchemaGeneration, RelationManifestDigest: source.Command.RelationManifestDigest, Table: profile.Table,
		SQL: gatewaySplitSourceSQLFixture(t, source, profile), Template: gatewaySplitTemplateFixture()}
	entry.Placement = persistGatewaySplitPlacement(gatewaySplitSourceFixture(t, source, profile).Placement)
	for i := 0; i < 3; i++ {
		entry.Replicas = append(entry.Replicas, persistedGatewaySplitReplica{
			Node: persisted.ShardEndpoints[i].Node, ChildRoot: filepath.Join(t.TempDir(), "split-children")})
	}
	persisted.SplitSources = []persistedGatewaySplitSource{entry}
	check := func(t *testing.T, candidate persistedGatewayReplicaControlManifest, valid bool) {
		t.Helper()
		raw, err := vibejson.Marshal(&candidate)
		if err != nil {
			t.Fatal(err)
		}
		opened, err := openGatewayReplicaControlManifest(raw, [16]byte{1})
		if (err == nil) != valid {
			t.Fatalf("valid=%t err=%v", valid, err)
		}
		if valid && (len(opened.SplitSources) != 1 || opened.SplitSources[0].Group.GroupID != entry.GroupID) {
			t.Fatal("source lost")
		}
	}
	check(t, persisted, true)
	for name, mutate := range map[string]func(*persistedGatewaySplitSource){
		"zero group":                 func(s *persistedGatewaySplitSource) { s.GroupID = [16]byte{} },
		"zero generation":            func(s *persistedGatewaySplitSource) { s.SchemaGeneration = 0 },
		"zero digest":                func(s *persistedGatewaySplitSource) { s.RelationManifestDigest = [32]byte{} },
		"missing placement":          func(s *persistedGatewaySplitSource) { s.Placement = persistedGatewaySplitPlacement{} },
		"rebound narrower placement": func(s *persistedGatewaySplitSource) { s.Placement.RangeEndMax = false; s.Placement.RangeEnd[0] = 0x80 },
		"foreign placement shape":    func(s *persistedGatewaySplitSource) { s.Placement.ShardKey = "/other" },
		"invalid root":               func(s *persistedGatewaySplitSource) { s.Replicas[0].ChildRoot = "/" },
		"unlisted node":              func(s *persistedGatewaySplitSource) { s.Replicas[0].Node = hex.EncodeToString(make([]byte, 16)) },
		"duplicate node":             func(s *persistedGatewaySplitSource) { s.Replicas[1].Node = s.Replicas[0].Node },
		"missing replica":            func(s *persistedGatewaySplitSource) { s.Replicas = s.Replicas[:2] },
	} {
		t.Run(name, func(t *testing.T) {
			copyEntry := entry
			copyEntry.Replicas = append([]persistedGatewaySplitReplica(nil), entry.Replicas...)
			mutate(&copyEntry)
			candidate := persisted
			candidate.SplitSources = []persistedGatewaySplitSource{copyEntry}
			check(t, candidate, false)
		})
	}
	candidate := persisted
	candidate.SplitSources = []persistedGatewaySplitSource{entry, entry}
	check(t, candidate, false)
	if _, err := openGatewaySplitSources(make([]persistedGatewaySplitSource, maxGatewaySplitSources+1), nil); err == nil {
		t.Fatal("unbounded source inventory accepted")
	}
}

func TestGatewaySplitSourceRestartPreservesOriginalPlacementAfterRetainedCut(t *testing.T) {
	initial, source, profile, _ := gatewayHotSplitFactoryFixture(t)
	entry := gatewaySplitSourceFixture(t, source, profile)
	encoded := persistedGatewaySplitSource{ClusterID: source.Group.ClusterID, ClusterIncarnation: source.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: source.Group.TopologyRecoveryEpoch, ShardIncarnation: source.Group.ShardIncarnation, GroupID: source.Group.GroupID,
		SchemaGeneration: entry.SchemaGeneration, RelationManifestDigest: entry.RelationManifestDigest, Table: entry.Table, SQL: entry.SQL,
		Template: entry.Template, Placement: persistGatewaySplitPlacement(entry.Placement)}
	manifest := gatewayReplicaControlManifest{}
	for _, replica := range entry.Replicas {
		encoded.Replicas = append(encoded.Replicas, persistedGatewaySplitReplica{Node: hex.EncodeToString(replica.Node[:]), ChildRoot: replica.Root})
		manifest.Shards = append(manifest.Shards, gateway.ReplicatedEndpoint{Node: replica.Node})
		manifest.SplitSnapshots = append(manifest.SplitSnapshots, "127.0.0.1:9301")
	}
	raw, err := vibejson.Marshal(&encoded)
	if err != nil {
		t.Fatal(err)
	}
	var persisted persistedGatewaySplitSource
	if err := vibejson.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	manifest.SplitSources, err = openGatewaySplitSources([]persistedGatewaySplitSource{persisted}, manifest.Shards)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gatewayHotSplitSources(manifest, initial); err != nil {
		t.Fatal(err)
	}
	oldManifest, _ := initial.Manifest(source.Distribution)
	oldMetadata, _ := oldManifest.ShardMetadataAt(0)
	cut := distribution.KeyspacePoint{0x80}
	narrow := distribution.KeyRange{End: distribution.KeyspaceEnd{Point: cut}}
	left := source
	left.Command.OwnershipEpoch++
	left.Command.RoutingVersion++
	left.Command.RouteGeneration++
	right := left
	right.Shard = "next-child"
	right.AllocationGeneration++
	right.Group.ShardIncarnation[0]++
	right.Group.GroupID[0]++
	right.RangeIdentity[0]++
	right.Command.RelationManifestDigest[0]++
	leaders := make([]distribution.EndpointID, len(source.Replicas))
	addresses := make(map[distribution.EndpointID]string)
	for i, replica := range source.Replicas {
		leaders[i] = replica.Endpoint
		for _, endpoint := range []distribution.EndpointID{replica.Endpoint, replica.NativeEndpoint, replica.ControlEndpoint} {
			addresses[endpoint], err = initial.Address(endpoint)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	nextManifest, err := distribution.NewManifest(source.Distribution, oldManifest.Version()+1, []distribution.Shard{
		{ID: source.Shard, AllocationGeneration: source.AllocationGeneration, Range: narrow, Leaders: leaders, Epoch: oldMetadata.Epoch + 1},
		{ID: right.Shard, AllocationGeneration: right.AllocationGeneration, Range: distribution.KeyRange{Start: cut, End: distribution.KeyspaceEnd{Max: true}}, Leaders: leaders, Epoch: oldMetadata.Epoch + 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	spec, _ := initial.Spec(source.Distribution)
	placement, _ := initial.Placement(profile.Table)
	// This is a metadata/restart contract test, not a fabricated cutover
	// certificate: production publishes these coordinates through Plan proofs.
	next, err := gateway.NewSnapshotWithReplicatedTableMetadata(distribution.ClusterConfig{Distributions: []distribution.DistributionSpec{spec}, Placements: []distribution.TablePlacement{placement}, Manifests: []*distribution.Manifest{nextManifest}},
		addresses, initial.Generation()+1, nil, nil, []gateway.ReplicatedShardDescriptor{left, right}, []gateway.ReplicatedTableProfile{profile})
	if err != nil {
		t.Fatal(err)
	}
	sources, err := gatewayHotSplitSources(manifest, next)
	if err != nil || sources[source.Group].Placement != entry.Placement {
		t.Fatalf("restart rebound immutable source: %v", err)
	}
	if !gatewaySplitSourceRetainedRangeMatches(entry, left, narrow) {
		t.Fatal("certified later cut rejected")
	}
	for _, mutate := range []func(*gateway.ReplicatedShardDescriptor){
		func(d *gateway.ReplicatedShardDescriptor) { d.Command.OwnershipEpoch = source.Command.OwnershipEpoch },
		func(d *gateway.ReplicatedShardDescriptor) { d.Command.RoutingVersion = source.Command.RoutingVersion },
		func(d *gateway.ReplicatedShardDescriptor) { d.Command.RouteGeneration = source.Command.RouteGeneration },
	} {
		bad := left
		mutate(&bad)
		if gatewaySplitSourceRetainedRangeMatches(entry, bad, narrow) {
			t.Fatal("narrowing without later exact authority accepted")
		}
	}
}

func TestGatewaySplitSourceRejectsWrongRoleSchemaAndDescendant(t *testing.T) {
	catalog, source, profile, _ := gatewayHotSplitFactoryFixture(t)
	entry := gatewaySplitSourceFixture(t, source, profile)
	manifest := gatewayReplicaControlManifest{SplitSources: []gatewaySplitSource{entry}}
	for _, replica := range source.Replicas {
		manifest.Shards = append(manifest.Shards, gateway.ReplicatedEndpoint{Node: replica.Node})
		manifest.SplitSnapshots = append(manifest.SplitSnapshots, "127.0.0.1:9000")
	}
	first, err := gatewayHotSplitSources(manifest, catalog)
	if err != nil {
		t.Fatal(err)
	}
	second, err := gatewayHotSplitSources(manifest, catalog)
	if err != nil || !reflect.DeepEqual(first[source.Group], second[source.Group]) {
		t.Fatal("restart changed authority", err)
	}
	for name, mutate := range map[string]func(*gatewaySplitSource){
		"wrong role":         func(s *gatewaySplitSource) { s.Table = "catalog" },
		"wrong generation":   func(s *gatewaySplitSource) { s.SchemaGeneration++ },
		"wrong digest":       func(s *gatewaySplitSource) { s.RelationManifestDigest[0]++ },
		"wrong placement":    func(s *gatewaySplitSource) { s.Template.ShardKey = "/tenant" },
		"wrong group":        func(s *gatewaySplitSource) { s.Group.GroupID[0]++ },
		"wrong node":         func(s *gatewaySplitSource) { s.Replicas[0].Node[0]++ },
		"missing SQL":        func(s *gatewaySplitSource) { s.SQL.Relations = nil },
		"foreign SQL member": func(s *gatewaySplitSource) { s.SQL.Binding.MemberID++ },
		"foreign local indexes": func(s *gatewaySplitSource) {
			s.LocalIndexes = []store.IndexDefinition{{Name: "foreign", Paths: []string{"/foreign"}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			copyEntry := entry
			mutate(&copyEntry)
			candidate := manifest
			candidate.SplitSources = []gatewaySplitSource{copyEntry}
			if _, err := gatewayHotSplitSources(candidate, catalog); err == nil {
				t.Fatal("foreign authority accepted")
			}
		})
	}
	descendant := source.Group
	descendant.GroupID[0]++
	if _, found := first[descendant]; found {
		t.Fatal("unlisted child inherited authority")
	}
	if !gatewaySplitSourceMatches(entry, source, profile) {
		t.Fatal("exact source rejected")
	}
	wrongLogical := profile
	wrongLogical.LogicalSchemaDigest = replication.Digest(source.Command.RelationManifestDigest)
	if gatewaySplitSourceMatches(entry, source, wrongLogical) {
		t.Fatal("machine digest accepted as logical table schema")
	}
	wrongDescriptor := source
	wrongDescriptor.LogicalSchemaDigest[0]++
	if gatewaySplitSourceMatches(entry, wrongDescriptor, profile) {
		t.Fatal("foreign catalog logical schema accepted")
	}
	profile.SchemaGeneration++
	if gatewaySplitSourceMatches(entry, source, profile) {
		t.Fatal("stale authority survived schema change")
	}
}

func TestGatewaySplitSourceLookupBounds(t *testing.T) {
	_, source, profile, _ := gatewayHotSplitFactoryFixture(t)
	entry := gatewaySplitSourceFixture(t, source, profile)
	inventory := make(map[raftmember.GroupKey]gatewaySplitSource, maxGatewaySplitSources)
	for i := 0; i < maxGatewaySplitSources; i++ {
		key := source.Group
		key.GroupID[0], key.GroupID[1] = byte(i), byte(i>>8)
		inventory[key] = entry
	}
	inventory[source.Group] = entry
	lookup := func() {
		selected, ok := inventory[source.Group]
		if !ok || !gatewaySplitSourceMatches(selected, source, profile) {
			panic("source")
		}
	}
	if allocations := testing.AllocsPerRun(1000, lookup); allocations != 0 {
		t.Fatalf("lookup allocations=%g", allocations)
	}
	started := time.Now()
	for i := 0; i < 100_000; i++ {
		lookup()
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("100k bounded inventory lookups=%s", elapsed)
	}
}

func TestGatewaySplitSourceProfileInventoryMatchesCanonicalSlots(t *testing.T) {
	_, source, profile, _ := gatewayHotSplitFactoryFixture(t)
	entry := gatewaySplitSourceFixture(t, source, profile)
	if !gatewaySplitSourceProfilesMatch(entry, []gateway.ReplicatedTableProfile{profile}) {
		t.Fatal("base rejected")
	}
	indexProfile := profile
	indexProfile.Relation, indexProfile.Table, indexProfile.PrimaryKey = 2, "emails", "/key"
	if gatewaySplitSourceProfilesMatch(entry, []gateway.ReplicatedTableProfile{profile, indexProfile}) {
		t.Fatal("public relation absent from exact SQL bundle accepted")
	}
	// This unit checks the inventory projection only. The composed Linux test
	// below uses sealed SQL and recomputes both authenticated schema digests.
	index := entry.SQL.Relations[0]
	index.Relation, index.Kind, index.Table = 2, sqldriver.ReplicatedShardRelationGlobalIndex, indexProfile.Table
	entry.SQL.Relations = append(entry.SQL.Relations, index)
	if !gatewaySplitSourceProfilesMatch(entry, []gateway.ReplicatedTableProfile{profile, indexProfile}) {
		t.Fatal("colocated exact index mistaken for another base")
	}
	for _, mutate := range []func(*gateway.ReplicatedTableProfile){
		func(p *gateway.ReplicatedTableProfile) { p.Table = "foreign" },
		func(p *gateway.ReplicatedTableProfile) { p.Relation = 3 },
		func(p *gateway.ReplicatedTableProfile) { p.MaxKeyBytes++ },
		func(p *gateway.ReplicatedTableProfile) { p.MaxDocumentBytes++ },
		func(p *gateway.ReplicatedTableProfile) { p.SchemaGeneration++ },
		func(p *gateway.ReplicatedTableProfile) { p.LogicalSchemaDigest[0]++ },
	} {
		bad := indexProfile
		mutate(&bad)
		if gatewaySplitSourceProfilesMatch(entry, []gateway.ReplicatedTableProfile{profile, bad}) {
			t.Fatal("unbound index profile accepted")
		}
	}
	if gatewaySplitSourceProfilesMatch(entry, []gateway.ReplicatedTableProfile{profile, profile}) {
		t.Fatal("repeated base accepted")
	}
}

func TestGatewaySplitSourcesSelectSeparateRootsOnSharedHosts(t *testing.T) {
	initial, source, profile, work := gatewayHotSplitFactoryFixture(t)
	other := source
	other.Group.GroupID[0]++
	other.Distribution = "other"
	otherProfile := profile
	otherProfile.Table = "other_messages"
	other.Command.RelationManifestDigest = gatewaySplitSourceDigestFixture(t, other, otherProfile)
	logical, err := sqldriver.ReplicatedRelationManifestDigest(gatewaySplitSourceSQLFixture(t, other, otherProfile))
	if err != nil {
		t.Fatal(err)
	}
	other.LogicalSchemaDigest, otherProfile.LogicalSchemaDigest = replication.Digest(logical), replication.Digest(logical)
	oldManifest, _ := initial.Manifest(source.Distribution)
	otherManifest, err := distribution.NewManifest(other.Distribution, 7, []distribution.Shard{{ID: other.Shard,
		AllocationGeneration: other.AllocationGeneration, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"peer-a", "peer-b", "peer-c"}, Epoch: 13}})
	if err != nil {
		t.Fatal(err)
	}
	endpoints := make(map[distribution.EndpointID]string)
	for _, replica := range source.Replicas {
		for _, id := range []distribution.EndpointID{replica.Endpoint, replica.NativeEndpoint, replica.ControlEndpoint} {
			address, err := initial.Address(id)
			if err != nil {
				t.Fatal(err)
			}
			endpoints[id] = address
		}
	}
	catalog, err := gateway.NewSnapshotWithReplicatedTableMetadata(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: source.Distribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion},
			{Name: other.Distribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion}},
		Placements: []distribution.TablePlacement{{Table: profile.Table, Distribution: source.Distribution, Columns: []string{"/id"}},
			{Table: otherProfile.Table, Distribution: other.Distribution, Columns: []string{"/id"}}},
		Manifests: []*distribution.Manifest{oldManifest, otherManifest}}, endpoints, 9, nil, nil,
		[]gateway.ReplicatedShardDescriptor{source, other}, []gateway.ReplicatedTableProfile{profile, otherProfile})
	if err != nil {
		t.Fatal(err)
	}
	manifest := gatewayReplicaControlManifest{SplitSources: []gatewaySplitSource{
		gatewaySplitSourceFixture(t, source, profile), gatewaySplitSourceFixture(t, other, otherProfile)}}
	manifest.SplitSources[1].Template.MaxSessions++
	for _, replica := range source.Replicas {
		manifest.Shards = append(manifest.Shards, gateway.ReplicatedEndpoint{Node: replica.Node})
		manifest.SplitSnapshots = append(manifest.SplitSnapshots, "127.0.0.1:9000")
	}
	sources, err := gatewayHotSplitSources(manifest, catalog)
	if err != nil {
		t.Fatal(err)
	}
	factory := gatewayHotSplitFactory{sources: sources}
	admission := [32]byte{7}
	split, err := factory.allocateSplit(catalog, admission, work, source)
	if err != nil {
		t.Fatal(err)
	}
	child, _ := split.Child(1)
	for i, item := range []struct {
		descriptor gateway.ReplicatedShardDescriptor
		profile    gateway.ReplicatedTableProfile
	}{{source, profile}, {other, otherProfile}} {
		target, err := factory.buildChildTarget(catalog, admission, 1, child, item.descriptor, item.profile)
		if err != nil {
			t.Fatal(err)
		}
		for j, replica := range target.Replicas {
			if replica.RuntimeRoot != filepath.Join(manifest.SplitSources[i].Replicas[j].Root, hex.EncodeToString(admission[:]), "child-1") || replica.Apply.MaxSessions != manifest.SplitSources[i].Template.MaxSessions {
				t.Fatal("wrong source root/template")
			}
		}
	}
	manifest.SplitSources[1].Replicas[0].Root = manifest.SplitSources[0].Replicas[0].Root
	if _, err := gatewayHotSplitSources(manifest, catalog); err == nil {
		t.Fatal("shared node root authorized across groups")
	}
}
