package gateway

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
)

// point builds a keyspace point whose leading byte is b and remainder zero.
func point(b byte) distribution.KeyspacePoint {
	var p distribution.KeyspacePoint
	p[0] = b
	return p
}

// hexPoint renders point(b) as the big-endian hex the on-disk form stores.
func hexPoint(b byte) string {
	p := point(b)
	return hex.EncodeToString(p[:])
}

// testManifest builds a two-shard manifest splitting the keyspace at 0x80, with
// distinct per-shard leaders and ownership epochs.
func testManifest(t testing.TB, version distribution.RoutingVersion) *distribution.Manifest {
	t.Helper()
	shards := []distribution.Shard{
		{
			ID:                   "-80",
			AllocationGeneration: 1,
			Range:                distribution.KeyRange{Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Point: point(0x80)}},
			Leaders:              []distribution.EndpointID{"ep-a"},
			Epoch:                7,
		},
		{
			ID:                   "80-",
			AllocationGeneration: 2,
			Range:                distribution.KeyRange{Start: point(0x80), End: distribution.KeyspaceEnd{Max: true}},
			Leaders:              []distribution.EndpointID{"ep-b"},
			Epoch:                9,
		},
	}
	m, err := distribution.NewManifest("tenant_data", version, shards)
	if err != nil {
		t.Fatalf("NewManifest: %v", err)
	}
	return m
}

// testConfig builds a one-distribution cluster configuration over testManifest.
func testConfig(t testing.TB) distribution.ClusterConfig {
	t.Helper()
	return distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: 1}},
		Placements:    []distribution.TablePlacement{{Table: "messages", Distribution: "tenant_data", Columns: []string{"/tenant_id"}}},
		Manifests:     []*distribution.Manifest{testManifest(t, 3)},
	}
}

// testEndpoints resolves every leader endpoint the testManifest references.
func testEndpoints() map[distribution.EndpointID]string {
	return map[distribution.EndpointID]string{"ep-a": "127.0.0.1:7001", "ep-b": "127.0.0.1:7002"}
}

// testSnapshot builds a valid snapshot pinned to generation.
func testSnapshot(t testing.TB, generation uint64) *Snapshot {
	t.Helper()
	s, err := NewSnapshot(testConfig(t), testEndpoints(), generation)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	return s
}

func testSnapshotFromConfig(
	t testing.TB,
	config distribution.ClusterConfig,
	endpoints map[distribution.EndpointID]string,
	generation uint64,
) *Snapshot {
	t.Helper()
	snapshot, err := NewSnapshot(config, endpoints, generation)
	if err != nil {
		t.Fatalf("NewSnapshot generation %d: %v", generation, err)
	}
	return snapshot
}

// TestSnapshotRejectsUnsupportedMapperVersion keeps mapper selection inside the
// authoritative generation: a gateway never guesses how to route a revision it
// does not implement.
func TestSnapshotRejectsUnsupportedMapperVersion(t *testing.T) {
	config := testConfig(t)
	config.Distributions[0].MapperVersion = distribution.NativeMapperVersion + 1
	_, err := NewSnapshot(config, testEndpoints(), 1)
	if !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("NewSnapshot err = %v, want ErrInvalidCatalog", err)
	}
}

// TestSnapshotPersistRoundTrip proves a snapshot survives a durable save and
// reload with its generation, manifest geometry, per-shard epochs, endpoint
// membership, and placement intact.
func TestSnapshotPersistRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveSnapshot(path, testSnapshot(t, 5)); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	got, err := LoadSnapshot(path)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	if got.Generation() != 5 {
		t.Fatalf("generation = %d, want 5", got.Generation())
	}
	m, ok := got.Manifest("tenant_data")
	if !ok {
		t.Fatal("loaded manifest tenant_data is missing")
	}
	if m.Version() != 3 || m.ShardCount() != 2 {
		t.Fatalf("manifest version/shards = %d/%d, want 3/2", m.Version(), m.ShardCount())
	}
	if id, ok := m.ResolvePoint(point(0x00)); !ok || id != "-80" {
		t.Fatalf("resolve 0x00 = %q,%v, want -80,true", id, ok)
	}
	if id, ok := m.ResolvePoint(point(0xC0)); !ok || id != "80-" {
		t.Fatalf("resolve 0xC0 = %q,%v, want 80-,true", id, ok)
	}
	if ep, ok := got.OwnershipEpoch("tenant_data", "80-"); !ok || ep != 9 {
		t.Fatalf("OwnershipEpoch(80-) = %d,%v, want 9,true", ep, ok)
	}
	if allocation, ok := got.ShardAllocationGeneration("tenant_data", "80-"); !ok || allocation != 2 {
		t.Fatalf("ShardAllocationGeneration(80-) = %d,%v, want 2,true", allocation, ok)
	}
	if addr, err := got.Address("ep-a"); err != nil || addr != "127.0.0.1:7001" {
		t.Fatalf("Address(ep-a) = %q,%v", addr, err)
	}
	if p, ok := got.Placement("messages"); !ok || len(p.Columns) != 1 || p.Columns[0] != "/tenant_id" {
		t.Fatalf("Placement(messages) = %+v,%v", p, ok)
	}
	if got.indexIDHighWater != 0 || len(got.shardGenerationHighWaters) != 1 ||
		got.shardGenerationHighWaters[0] != 2 {
		t.Fatalf("lineage = index %d shards %v, want 0/[2]",
			got.indexIDHighWater, got.shardGenerationHighWaters)
	}
}

func TestSnapshotPersistsVirtualBucketsAndAffinity(t *testing.T) {
	config := testConfig(t)
	config.Distributions[0].Arity = 2
	config.Distributions[0].BucketBits = distribution.DefaultVirtualBucketBits
	config.Placements[0].Columns = []string{"/tenant_id", "/message_id"}
	config.Placements[0].TenantPath = "/tenant_id"
	config.Placements[0].AffinityGroup = "messaging"
	snapshot, err := NewSnapshot(config, testEndpoints(), 8)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveSnapshot(path, snapshot); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	loaded, err := LoadSnapshot(path)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	spec, ok := loaded.Spec("tenant_data")
	if !ok || spec.BucketBits != distribution.DefaultVirtualBucketBits {
		t.Fatalf("loaded distribution = %+v,%v", spec, ok)
	}
	placement, ok := loaded.Placement("messages")
	if !ok || placement.TenantPath != "/tenant_id" || placement.AffinityGroup != "messaging" {
		t.Fatalf("loaded placement = %+v,%v", placement, ok)
	}
}

func TestSnapshotReadAccessorsDoNotExposeMutableConfig(t *testing.T) {
	snapshot := testSnapshot(t, 1)
	placement, ok := snapshot.Placement("messages")
	if !ok {
		t.Fatal("messages placement is missing")
	}
	placement.Columns[0] = "/corrupted"
	placementAgain, ok := snapshot.Placement("messages")
	if !ok || len(placementAgain.Columns) != 1 || placementAgain.Columns[0] != "/tenant_id" {
		t.Fatalf("mutating Placement result changed snapshot: %+v,%v", placementAgain, ok)
	}

	placementAt, ok := snapshot.PlacementAt(0)
	if !ok {
		t.Fatal("placement ordinal 0 is missing")
	}
	placementAt.Columns[0] = "/also_corrupted"
	placementAgain, _ = snapshot.PlacementAt(0)
	if placementAgain.Columns[0] != "/tenant_id" {
		t.Fatalf("mutating PlacementAt result changed snapshot: %+v", placementAgain)
	}

	if snapshot.DistributionCount() != 1 || snapshot.PlacementCount() != 1 || snapshot.ManifestCount() != 1 {
		t.Fatalf("catalog counts = %d/%d/%d, want 1/1/1",
			snapshot.DistributionCount(), snapshot.PlacementCount(), snapshot.ManifestCount())
	}
	if _, ok := snapshot.DistributionAt(-1); ok {
		t.Fatal("negative distribution ordinal succeeded")
	}
	if _, ok := snapshot.PlacementAt(snapshot.PlacementCount()); ok {
		t.Fatal("out-of-range placement ordinal succeeded")
	}
	if _, ok := snapshot.ManifestAt(snapshot.ManifestCount()); ok {
		t.Fatal("out-of-range manifest ordinal succeeded")
	}
}

func TestSnapshotOwnsExactStringBacking(t *testing.T) {
	tableBacking := strings.Repeat("x", 1<<20) + "messages"
	columnBacking := strings.Repeat("x", 1<<20) + "/tenant_id"
	addressBacking := strings.Repeat("x", 1<<20) + "127.0.0.1:7001"
	table := tableBacking[len(tableBacking)-len("messages"):]
	column := columnBacking[len(columnBacking)-len("/tenant_id"):]
	address := addressBacking[len(addressBacking)-len("127.0.0.1:7001"):]

	config := testConfig(t)
	config.Placements[0].Table = table
	config.Placements[0].Columns[0] = column
	endpoints := testEndpoints()
	endpoints["ep-a"] = address
	snapshot, err := NewSnapshot(config, endpoints, 1)
	if err != nil {
		t.Fatal(err)
	}
	ownedTable := snapshot.config.Placements[0].Table
	ownedColumn := snapshot.config.Placements[0].Columns[0]
	ownedAddress := snapshot.endpoints["ep-a"]
	if unsafe.StringData(ownedTable) == unsafe.StringData(table) ||
		unsafe.StringData(ownedColumn) == unsafe.StringData(column) ||
		unsafe.StringData(ownedAddress) == unsafe.StringData(address) {
		t.Fatal("snapshot retained caller string backing")
	}
}

func TestCatalogTransitionDirectoriesAreCompactAndExact(t *testing.T) {
	snapshot := NewCatalogHolder(testSnapshot(t, 1)).Current()
	if got := unsafe.Sizeof(plannerShardLineageRef{}); got != 8 {
		t.Fatalf("plannerShardLineageRef = %d bytes, want 8", got)
	}
	want := uint64(2*8 + 1*8 + 8) // two shards, one distribution water, index scalar
	if got := snapshot.CatalogTransitionMetadataBytes(); got != want {
		t.Fatalf("transition metadata = %d bytes, want %d", got, want)
	}
}

func TestShardLineageDirectoryIsIdentitySortedIndependentlyOfRanges(t *testing.T) {
	config := testConfig(t)
	shards := manifestShards(t, config.Manifests[0])
	shards[0].ID = "z-left"
	shards[1].ID = "a-right"
	config.Manifests[0] = mustManifest(t, 3, shards)
	snapshot := testSnapshotFromConfig(t, config, testEndpoints(), 1)
	if len(snapshot.shardLineage) != 2 {
		t.Fatalf("shard lineage refs = %d, want 2", len(snapshot.shardLineage))
	}
	firstRef, secondRef := snapshot.shardLineage[0], snapshot.shardLineage[1]
	first, _ := snapshot.config.Manifests[firstRef.manifest].ShardMetadataAt(int(firstRef.shard))
	second, _ := snapshot.config.Manifests[secondRef.manifest].ShardMetadataAt(int(secondRef.shard))
	if first.ID != "a-right" || second.ID != "z-left" {
		t.Fatalf("shard lineage order = %q,%q, want a-right,z-left", first.ID, second.ID)
	}
}

func TestLoadSnapshotRejectsMissingOrContradictoryLineage(t *testing.T) {
	normalized, err := initialCatalogState(testSnapshot(t, 5))
	if err != nil {
		t.Fatal(err)
	}
	base := toPersisted(normalized)
	tests := []struct {
		name   string
		mutate func(*persistedCatalog)
	}{
		{
			name: "missing",
			mutate: func(catalog *persistedCatalog) {
				catalog.Lineage = nil
			},
		},
		{
			name: "wrong_distribution_count",
			mutate: func(catalog *persistedCatalog) {
				catalog.Lineage.ShardGenerationHighWaters = nil
			},
		},
		{
			name: "below_active_shard_allocation",
			mutate: func(catalog *persistedCatalog) {
				catalog.Lineage.ShardGenerationHighWaters[0] = 1
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			catalog := base
			lineage := *base.Lineage
			lineage.ShardGenerationHighWaters = slices.Clone(base.Lineage.ShardGenerationHighWaters)
			catalog.Lineage = &lineage
			tc.mutate(&catalog)
			raw, err := json.Marshal(catalog)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "catalog.json")
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadSnapshot(path); !errors.Is(err, ErrInvalidCatalog) {
				t.Fatalf("LoadSnapshot err=%v, want ErrInvalidCatalog", err)
			}
		})
	}
}

func TestSaveSnapshotRejectsDurableGenerationRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveSnapshot(path, testSnapshot(t, 5)); err != nil {
		t.Fatalf("SaveSnapshot generation 5: %v", err)
	}
	for _, generation := range []uint64{5, 3} {
		if err := SaveSnapshot(path, testSnapshot(t, generation)); !errors.Is(err, ErrCatalogGenerationNotNewer) {
			t.Fatalf("SaveSnapshot generation %d err=%v, want ErrCatalogGenerationNotNewer", generation, err)
		}
	}
	got, err := LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation() != 5 {
		t.Fatalf("durable generation regressed to %d, want 5", got.Generation())
	}
}

func TestSaveSnapshotAfterFencesConcurrentTopology(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveSnapshot(path, testSnapshot(t, 5)); err != nil {
		t.Fatalf("SaveSnapshot generation 5: %v", err)
	}
	if err := SaveSnapshotAfter(path, 4, testSnapshot(t, 6)); !errors.Is(err, ErrCatalogGenerationMismatch) {
		t.Fatalf("SaveSnapshotAfter stale base err=%v, want ErrCatalogGenerationMismatch", err)
	}
	got, err := LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation() != 5 {
		t.Fatalf("stale CAS mutated durable generation to %d, want 5", got.Generation())
	}
	if err := SaveSnapshotAfter(path, 5, testSnapshot(t, 6)); err != nil {
		t.Fatalf("SaveSnapshotAfter generation 5: %v", err)
	}
	got, err = LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation() != 6 {
		t.Fatalf("durable generation = %d, want 6", got.Generation())
	}
}

func TestSaveSnapshotAfterTreatsAbsentCatalogAsGenerationZero(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveSnapshotAfter(path, 1, testSnapshot(t, 2)); !errors.Is(err, ErrCatalogGenerationMismatch) {
		t.Fatalf("SaveSnapshotAfter absent stale base err=%v, want ErrCatalogGenerationMismatch", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed CAS created catalog: %v", err)
	}
	if err := SaveSnapshotAfter(path, 0, testSnapshot(t, 1)); err != nil {
		t.Fatalf("SaveSnapshotAfter generation zero: %v", err)
	}
}

// TestSaveSnapshotConcurrentPublishersConverge proves the writer lease covers
// the generation comparison and rename as one operation. Contending writers
// retry only the typed lease refusal; a generation already superseded by a
// successful publisher terminates normally. The highest generation therefore
// cannot be overwritten by a later successful stale publisher.
func TestSaveSnapshotConcurrentPublishersConverge(t *testing.T) {
	const generations = 32
	path := filepath.Join(t.TempDir(), "catalog.json")
	snapshots := make([]*Snapshot, generations)
	for i := range snapshots {
		snapshots[i] = testSnapshot(t, uint64(i+1))
	}
	start := make(chan struct{})
	results := make(chan error, generations)
	var wg sync.WaitGroup
	for generation := uint64(1); generation <= generations; generation++ {
		snapshot := snapshots[generation-1]
		wg.Add(1)
		go func(generation uint64, snapshot *Snapshot) {
			defer wg.Done()
			<-start
			for attempts := 0; attempts < 100_000; attempts++ {
				err := SaveSnapshot(path, snapshot)
				switch {
				case err == nil, errors.Is(err, ErrCatalogGenerationNotNewer):
					results <- nil
					return
				case errors.Is(err, ErrCatalogWriterLocked):
					runtime.Gosched()
				default:
					results <- err
					return
				}
			}
			results <- errors.New("catalog writer lease did not make progress")
		}(generation, snapshot)
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation() != generations {
		t.Fatalf("durable generation = %d, want %d", got.Generation(), generations)
	}
}

func TestSaveSnapshotUsesOnePinnedParentNamespace(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	link := filepath.Join(t.TempDir(), "catalog-parent")
	if err := os.Symlink(dirA, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	root, base, err := openCatalogRoot(filepath.Join(link, "catalog.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(dirB, link); err != nil {
		t.Fatal(err)
	}
	if err := saveSnapshotAtRoot(root, base, testSnapshot(t, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSnapshot(filepath.Join(dirA, "catalog.json")); err != nil {
		t.Fatalf("pinned directory A was not published: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dirB, "catalog.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retargeted directory B was mutated: %v", err)
	}
}

func TestCatalogPublicationEntryProofRejectsReplacement(t *testing.T) {
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	for _, name := range []string{"catalog.json.lock", ".catalog.json.tmp-proof"} {
		t.Run(name, func(t *testing.T) {
			file, err := openCatalogRootFile(root, name, os.O_RDWR|os.O_CREATE, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			info, err := file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			if err := root.Rename(name, name+".detached"); err != nil {
				t.Fatal(err)
			}
			if err := root.WriteFile(name, []byte("replacement"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := verifyCatalogEntryUnchanged(root, name, info); !errors.Is(err, ErrInvalidCatalog) {
				t.Fatalf("replacement proof err=%v, want ErrInvalidCatalog", err)
			}
		})
	}
}

func TestCatalogTransitionFencesRoutingIdentity(t *testing.T) {
	base := testSnapshot(t, 1)
	tests := []struct {
		name      string
		config    distribution.ClusterConfig
		endpoints map[distribution.EndpointID]string
	}{
		{
			name: "distribution_identity",
			config: func() distribution.ClusterConfig {
				config := testConfig(t)
				config.Distributions[0].Arity = 2
				config.Placements[0].Columns = []string{"/tenant_id", "/bucket"}
				return config
			}(),
			endpoints: testEndpoints(),
		},
		{
			name: "virtual_bucket_identity",
			config: func() distribution.ClusterConfig {
				config := testConfig(t)
				config.Distributions[0].BucketBits = distribution.DefaultVirtualBucketBits - 1
				return config
			}(),
			endpoints: testEndpoints(),
		},
		{
			name: "placement_identity",
			config: func() distribution.ClusterConfig {
				config := testConfig(t)
				config.Placements[0].Columns = []string{"/other"}
				return config
			}(),
			endpoints: testEndpoints(),
		},
		{
			name: "affinity_identity",
			config: func() distribution.ClusterConfig {
				config := testConfig(t)
				config.Placements[0].AffinityGroup = "other"
				return config
			}(),
			endpoints: testEndpoints(),
		},
		{
			name: "placement_removal",
			config: func() distribution.ClusterConfig {
				config := testConfig(t)
				config.Placements = nil
				return config
			}(),
			endpoints: testEndpoints(),
		},
		{
			name: "routing_version_regression",
			config: func() distribution.ClusterConfig {
				config := testConfig(t)
				config.Manifests[0] = testManifest(t, 2)
				return config
			}(),
			endpoints: testEndpoints(),
		},
		{
			name: "same_version_changed_epoch",
			config: func() distribution.ClusterConfig {
				config := testConfig(t)
				shards := manifestShards(t, config.Manifests[0])
				shards[0].Epoch++
				config.Manifests[0] = mustManifest(t, 3, shards)
				return config
			}(),
			endpoints: testEndpoints(),
		},
		{
			name: "active_shard_allocation_change",
			config: func() distribution.ClusterConfig {
				config := testConfig(t)
				shards := manifestShards(t, config.Manifests[0])
				shards[0].AllocationGeneration = 3
				shards[0].Epoch++
				config.Manifests[0] = mustManifest(t, 4, shards)
				return config
			}(),
			endpoints: testEndpoints(),
		},
		{
			name: "higher_version_lower_epoch",
			config: func() distribution.ClusterConfig {
				config := testConfig(t)
				shards := manifestShards(t, config.Manifests[0])
				shards[0].Epoch--
				config.Manifests[0] = mustManifest(t, 4, shards)
				return config
			}(),
			endpoints: testEndpoints(),
		},
		{
			name: "ownership_change_without_epoch",
			config: func() distribution.ClusterConfig {
				config := testConfig(t)
				shards := manifestShards(t, config.Manifests[0])
				shards[0].Leaders = []distribution.EndpointID{"ep-c"}
				config.Manifests[0] = mustManifest(t, 4, shards)
				return config
			}(),
			endpoints: map[distribution.EndpointID]string{
				"ep-a": "127.0.0.1:7001", "ep-b": "127.0.0.1:7002", "ep-c": "127.0.0.1:7003",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			next := testSnapshotFromConfig(t, tc.config, tc.endpoints, 2)
			holder := NewCatalogHolder(base)
			if holder.PublishNewer(next) {
				t.Fatal("unsafe routing transition was published")
			}
		})
	}
}

func manifestShards(t testing.TB, manifest *distribution.Manifest) []distribution.Shard {
	t.Helper()
	shards := make([]distribution.Shard, manifest.ShardCount())
	for i := range shards {
		shard, ok := manifest.ShardInfo(i)
		if !ok {
			t.Fatalf("ShardInfo(%d) missing", i)
		}
		shards[i] = shard
	}
	return shards
}

func mustManifest(
	t testing.TB,
	version distribution.RoutingVersion,
	shards []distribution.Shard,
) *distribution.Manifest {
	t.Helper()
	manifest, err := distribution.NewManifest("tenant_data", version, shards)
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func TestSaveSnapshotRetainsDistributionShardAllocationHighWater(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveSnapshot(path, testSnapshot(t, 1)); err != nil {
		t.Fatal(err)
	}
	config := testConfig(t)
	config.Manifests[0] = mustManifest(t, 4, []distribution.Shard{{
		ID: "all", AllocationGeneration: 3,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"ep-a"}, Epoch: 1,
	}})
	if err := SaveSnapshot(path, testSnapshotFromConfig(t, config, testEndpoints(), 2)); err != nil {
		t.Fatal(err)
	}

	config = testConfig(t)
	config.Manifests[0] = testManifest(t, 5)
	if err := SaveSnapshot(path, testSnapshotFromConfig(t, config, testEndpoints(), 3)); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("reused retired shard identities err=%v, want ErrInvalidCatalog", err)
	}
	shards := manifestShards(t, config.Manifests[0])
	shards[0].AllocationGeneration = 4
	shards[1].AllocationGeneration = 5
	shards[0].Epoch = 1
	shards[1].Epoch = 1
	config.Manifests[0] = mustManifest(t, 5, shards)
	if err := SaveSnapshot(path, testSnapshotFromConfig(t, config, testEndpoints(), 3)); err != nil {
		t.Fatalf("higher ownership incarnations were refused: %v", err)
	}
	loaded, err := LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.shardGenerationHighWaters) != 1 || loaded.shardGenerationHighWaters[0] != 5 {
		t.Fatalf("durable shard allocation high-water = %v, want [5]", loaded.shardGenerationHighWaters)
	}
}

func TestShardAllocationHighWaterSupportsSkippedCatalogGenerations(t *testing.T) {
	build := func(
		catalogGeneration uint64,
		routingVersion distribution.RoutingVersion,
		shardID distribution.ShardID,
		allocation distribution.ShardAllocationGeneration,
	) *Snapshot {
		t.Helper()
		config := testConfig(t)
		config.Manifests[0] = mustManifest(t, routingVersion, []distribution.Shard{{
			ID: shardID, AllocationGeneration: allocation,
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"ep-a"}, Epoch: 1,
		}})
		return testSnapshotFromConfig(t, config, testEndpoints(), catalogGeneration)
	}

	holder := NewCatalogHolder(testSnapshot(t, 1))
	skipped := build(2, 4, "all", 3)
	skipped.catalogLineagePresent = true
	skipped.indexIDHighWater = 0
	skipped.shardGenerationHighWaters = []distribution.ShardAllocationGeneration{10}
	if !holder.PublishNewer(skipped) {
		t.Fatal("generation jump with a monotonic shard allocation high-water was refused")
	}
	if holder.Current().shardGenerationHighWaters[0] != 10 {
		t.Fatalf("shard allocation high-water = %d, want skipped value 10",
			holder.Current().shardGenerationHighWaters[0])
	}
	if holder.PublishNewer(build(3, 5, "new", 5)) {
		t.Fatal("shard allocation below the skipped high-water was admitted")
	}
	if !holder.PublishNewer(build(3, 5, "new", 11)) {
		t.Fatal("shard allocation above the skipped high-water was refused")
	}
	if next, ok := holder.Current().NextShardAllocationGeneration("tenant_data"); !ok || next != 12 {
		t.Fatalf("next shard allocation = %d,%v, want 12,true", next, ok)
	}
}

func TestShardAllocationHighWatersFollowDistributionIdentityAcrossReorder(t *testing.T) {
	manifest := func(name distribution.DistributionName, shard distribution.ShardID, allocation distribution.ShardAllocationGeneration, endpoint distribution.EndpointID) *distribution.Manifest {
		t.Helper()
		result, err := distribution.NewManifest(name, 1, []distribution.Shard{{
			ID: shard, AllocationGeneration: allocation,
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{endpoint}, Epoch: 1,
		}})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	aManifest := manifest("a", "a0", 7, "ep-a")
	bManifest := manifest("b", "b0", 91, "ep-b")
	build := func(generation uint64, reverse bool) *Snapshot {
		t.Helper()
		config := distribution.ClusterConfig{
			Distributions: []distribution.DistributionSpec{
				{Name: "a", Arity: 1, MapperVersion: distribution.NativeMapperVersion},
				{Name: "b", Arity: 1, MapperVersion: distribution.NativeMapperVersion},
			},
			Placements: []distribution.TablePlacement{
				{Table: "a_table", Distribution: "a", Columns: []string{"/tenant_id"}},
				{Table: "b_table", Distribution: "b", Columns: []string{"/tenant_id"}},
			},
			Manifests: []*distribution.Manifest{aManifest, bManifest},
		}
		if reverse {
			slices.Reverse(config.Distributions)
			slices.Reverse(config.Placements)
			slices.Reverse(config.Manifests)
		}
		snapshot, err := NewSnapshot(config, map[distribution.EndpointID]string{
			"ep-a": "127.0.0.1:1", "ep-b": "127.0.0.1:2",
		}, generation)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}

	initial := build(1, false)
	initial.catalogLineagePresent = true
	initial.shardGenerationHighWaters = []distribution.ShardAllocationGeneration{100, 200}
	holder := NewCatalogHolder(initial)
	if !holder.PublishNewer(build(2, true)) {
		t.Fatal("safe distribution reorder was refused")
	}
	if next, ok := holder.Current().NextShardAllocationGeneration("a"); !ok || next != 101 {
		t.Fatalf("a next allocation = %d,%v, want 101,true", next, ok)
	}
	if next, ok := holder.Current().NextShardAllocationGeneration("b"); !ok || next != 201 {
		t.Fatalf("b next allocation = %d,%v, want 201,true", next, ok)
	}
}

func TestNextShardAllocationGenerationUsesActiveOrPersistedHighWater(t *testing.T) {
	fresh := testSnapshot(t, 1)
	if next, ok := fresh.NextShardAllocationGeneration("tenant_data"); !ok || next != 3 {
		t.Fatalf("fresh next shard allocation = %d,%v, want 3,true", next, ok)
	}
	if _, ok := fresh.NextShardAllocationGeneration("missing"); ok {
		t.Fatal("unknown distribution returned an allocation")
	}
	if _, ok := (*Snapshot)(nil).NextShardAllocationGeneration("tenant_data"); ok {
		t.Fatal("nil snapshot returned an allocation")
	}

	exhausted := testSnapshot(t, 2)
	exhausted.catalogLineagePresent = true
	exhausted.shardGenerationHighWaters = []distribution.ShardAllocationGeneration{
		^distribution.ShardAllocationGeneration(0),
	}
	if _, ok := exhausted.NextShardAllocationGeneration("tenant_data"); ok {
		t.Fatal("exhausted shard allocation namespace wrapped")
	}
}

// TestSnapshotPersistDeterministic proves equal snapshots persist to identical
// bytes, so a published generation is a stable artifact.
func TestSnapshotPersistDeterministic(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "a.json")
	second := filepath.Join(dir, "b.json")
	s := testSnapshot(t, 5)
	if err := SaveSnapshot(first, s); err != nil {
		t.Fatalf("SaveSnapshot first: %v", err)
	}
	if err := SaveSnapshot(second, s); err != nil {
		t.Fatalf("SaveSnapshot second: %v", err)
	}
	a, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("persisted bytes differ across identical saves")
	}
}

// TestNewSnapshotRejects proves snapshot validation fails closed: an
// unresolvable leader endpoint and an internally inconsistent configuration both
// error with their sentinel.
func TestNewSnapshotRejects(t *testing.T) {
	tests := []struct {
		name      string
		config    func(t testing.TB) distribution.ClusterConfig
		endpoints map[distribution.EndpointID]string
		sentinel  error
	}{
		{
			name:      "unresolvable_leader_endpoint",
			config:    testConfig,
			endpoints: map[distribution.EndpointID]string{"ep-a": "127.0.0.1:7001"}, // ep-b absent
			sentinel:  ErrInvalidCatalog,
		},
		{
			name: "config_missing_manifest",
			config: func(t testing.TB) distribution.ClusterConfig {
				return distribution.ClusterConfig{
					Distributions: []distribution.DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: 1}},
				}
			},
			endpoints: testEndpoints(),
			sentinel:  distribution.ErrInvalidPlacement,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewSnapshot(tc.config(t), tc.endpoints, 1)
			if !errors.Is(err, tc.sentinel) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.sentinel)
			}
		})
	}
}

// TestLoadSnapshotRejectsCorrupt proves a persisted file with a bad version or a
// manifest that no longer covers the keyspace fails closed on load, so a corrupt
// catalog never routes.
func TestLoadSnapshotRejectsCorrupt(t *testing.T) {
	tests := []struct {
		name     string
		data     persistedCatalog
		sentinel error
	}{
		{
			name:     "unsupported_version",
			data:     persistedCatalog{Version: 99},
			sentinel: ErrInvalidCatalog,
		},
		{
			name: "manifest_gap",
			data: persistedCatalog{
				Version:       catalogVersion,
				Generation:    1,
				Distributions: []persistedDistribution{{Name: "tenant_data", Arity: 1, MapperVersion: 1}},
				Manifests: []persistedManifest{{
					Distribution: "tenant_data",
					Version:      3,
					Shards: []persistedShard{
						{ID: "-40", Start: hexPoint(0x00), End: hexPoint(0x40), Leaders: []string{"ep-a"}},
						{ID: "80-", Start: hexPoint(0x80), EndMax: true, Leaders: []string{"ep-b"}},
					},
				}},
				Endpoints: []persistedEndpoint{{ID: "ep-a", Address: "a"}, {ID: "ep-b", Address: "b"}},
			},
			sentinel: distribution.ErrInvalidManifest,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "catalog.json")
			raw, err := json.MarshalIndent(tc.data, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadSnapshot(path); !errors.Is(err, tc.sentinel) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.sentinel)
			}
		})
	}
}

// TestCatalogHolderPublishAndPin proves a reader that pins one generation keeps
// observing it after a newer generation is published — an operation never mixes
// generations.
func TestCatalogHolderPublishAndPin(t *testing.T) {
	h := NewCatalogHolder(testSnapshot(t, 1))
	pinned := h.Current()
	if pinned.Generation() != 1 {
		t.Fatalf("pinned generation = %d, want 1", pinned.Generation())
	}
	if !h.Publish(testSnapshot(t, 2)) {
		t.Fatal("newer generation was refused")
	}
	if pinned.Generation() != 1 {
		t.Fatalf("pinned generation after publish = %d, want 1", pinned.Generation())
	}
	if h.Current().Generation() != 2 {
		t.Fatalf("current generation = %d, want 2", h.Current().Generation())
	}
	if h.Publish(testSnapshot(t, 1)) {
		t.Fatal("stale generation was accepted")
	}
	if h.Current().Generation() != 2 {
		t.Fatalf("current generation regressed to %d, want 2", h.Current().Generation())
	}
}

// TestCatalogHolderGenerationDrainFence proves publication closes the late-old
// pin race and the schema barrier waits for an already pinned operation.
func TestCatalogHolderGenerationDrainFence(t *testing.T) {
	h := NewCatalogHolder(testSnapshot(t, 1))
	old := h.pinCurrent()
	if old.generation != 1 {
		t.Fatalf("old lease = %+v, want generation 1", old)
	}
	if !h.PublishNewer(testSnapshot(t, 2)) {
		t.Fatal("generation 2 publication was refused")
	}
	late := h.pinCurrent()
	if late.generation != 2 {
		t.Fatalf("late lease = %+v, want generation 2", late)
	}
	defer late.release()

	status := h.DrainStatus(2)
	if status.CurrentGeneration != 2 || status.OldestActiveGeneration != 1 ||
		status.ActiveOlderOperations != 1 {
		t.Fatalf("drain status = %+v, want current=2 oldest=1 active=1", status)
	}
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := h.WaitOlderDrained(timeout, 2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitOlderDrained while pinned error = %v, want deadline", err)
	}

	old.release()
	drained, err := h.WaitOlderDrained(context.Background(), 2)
	if err != nil {
		t.Fatalf("WaitOlderDrained after release: %v", err)
	}
	if drained.CurrentGeneration != 2 || drained.OldestActiveGeneration != 0 ||
		drained.ActiveOlderOperations != 0 {
		t.Fatalf("drained status = %+v, want current=2 with no older operations", drained)
	}
}

// TestCatalogHolderGenerationDrainWaitsForPublication proves a controller can
// begin waiting before its catalog watcher publishes the requested generation.
func TestCatalogHolderGenerationDrainWaitsForPublication(t *testing.T) {
	h := NewCatalogHolder(testSnapshot(t, 1))
	done := make(chan error, 1)
	go func() {
		_, err := h.WaitOlderDrained(context.Background(), 2)
		done <- err
	}()
	if !h.PublishNewer(testSnapshot(t, 2)) {
		t.Fatal("generation 2 publication was refused")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitOlderDrained: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitOlderDrained did not observe publication")
	}
}

// TestCatalogHolderLeaseSteadyStateAllocations keeps the request-level schema
// fence honest: after holder construction and warmup, pin/release must not add
// heap pressure to the routed lane when no schema controller is waiting.
func TestCatalogHolderLeaseSteadyStateAllocations(t *testing.T) {
	h := NewCatalogHolder(testSnapshot(t, 1))
	warm := h.pinCurrent()
	warm.release()
	allocs := testing.AllocsPerRun(1000, func() {
		lease := h.pinCurrent()
		lease.release()
	})
	if allocs != 0 {
		t.Fatalf("pin/release allocations = %g, want 0", allocs)
	}
}

// TestCatalogHolderPublishNewer proves the strongly ordered publication
// primitive installs only strictly newer generations and refuses a stale or
// equal republish.
func TestCatalogHolderPublishNewer(t *testing.T) {
	h := NewCatalogHolder(nil)
	if !h.PublishNewer(testSnapshot(t, 5)) {
		t.Fatal("first publish was refused")
	}
	if h.PublishNewer(testSnapshot(t, 5)) {
		t.Fatal("equal generation was accepted")
	}
	if h.PublishNewer(testSnapshot(t, 3)) {
		t.Fatal("stale generation was accepted")
	}
	if h.PublishNewer(nil) {
		t.Fatal("nil generation was accepted")
	}
	if !h.PublishNewer(testSnapshot(t, 6)) {
		t.Fatal("newer generation was refused")
	}
	if h.Current().Generation() != 6 {
		t.Fatalf("current generation = %d, want 6", h.Current().Generation())
	}
}

func TestCatalogHolderPublishAfterFencesConcurrentTopology(t *testing.T) {
	h := NewCatalogHolder(testSnapshot(t, 5))
	if err := h.PublishAfter(4, testSnapshot(t, 6)); !errors.Is(err, ErrCatalogGenerationMismatch) {
		t.Fatalf("PublishAfter stale base err=%v, want ErrCatalogGenerationMismatch", err)
	}
	if got := h.Current().Generation(); got != 5 {
		t.Fatalf("stale CAS published generation %d, want 5", got)
	}
	if err := h.PublishAfter(5, testSnapshot(t, 6)); err != nil {
		t.Fatalf("PublishAfter generation 5: %v", err)
	}
	if got := h.Current().Generation(); got != 6 {
		t.Fatalf("current generation = %d, want 6", got)
	}
	if err := h.PublishAfter(6, testSnapshot(t, 6)); !errors.Is(err, ErrCatalogGenerationNotNewer) {
		t.Fatalf("PublishAfter equal generation err=%v, want ErrCatalogGenerationNotNewer", err)
	}

	empty := NewCatalogHolder(nil)
	if err := empty.PublishAfter(0, testSnapshot(t, 1)); err != nil {
		t.Fatalf("PublishAfter generation zero: %v", err)
	}
}

func TestBuildManifestTransitionPreservesCatalogAndRaisesOwnershipFence(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	current, err := NewSnapshotWithPlannerMetadata(
		config, endpoints, 5, []IndexDescriptor{testGlobalIndexDescriptor()}, gatewayTestStatistics(),
	)
	if err != nil {
		t.Fatal(err)
	}
	manifest, _ := current.Manifest("tenant_data")
	shards := make([]distribution.Shard, manifest.ShardCount())
	for i := range shards {
		shards[i], _ = manifest.ShardInfo(i)
	}
	shards[0].Leaders = []distribution.EndpointID{"ep-b"}
	shards[0].Epoch++
	nextManifest, err := distribution.NewManifest("tenant_data", manifest.Version()+1, shards)
	if err != nil {
		t.Fatal(err)
	}
	next, err := BuildManifestTransition(current, nextManifest, 6)
	if err != nil {
		t.Fatalf("BuildManifestTransition: %v", err)
	}
	if next.Generation() != 6 || next.PlacementCount() != current.PlacementCount() ||
		next.DistributionCount() != current.DistributionCount() {
		t.Fatalf("next catalog shape = generation %d placements %d distributions %d",
			next.Generation(), next.PlacementCount(), next.DistributionCount())
	}
	routed, _ := next.Manifest("tenant_data")
	shard, _ := routed.ShardInfo(0)
	if routed.Version() != manifest.Version()+1 || shard.Leaders[0] != "ep-b" || shard.Epoch != 8 {
		t.Fatalf("transitioned shard = version %d shard %+v", routed.Version(), shard)
	}
	original, _ := manifest.ShardInfo(0)
	if original.Leaders[0] != "ep-a" || original.Epoch != 7 {
		t.Fatalf("current manifest mutated: %+v", original)
	}
	index, indexOK := next.Index("messages", "by_email")
	statistics, statisticsOK := next.Statistics("messages")
	address, addressErr := next.Address("ep-index-a")
	if !indexOK || !index.Global() || index.Relation != "messages_email_index" ||
		index.LocatorCount != 2 || index.LocatorPaths[1] != "/id" ||
		!statisticsOK || statistics.Rows().Value != 10_000 ||
		addressErr != nil || address != "127.0.0.1:7101" {
		t.Fatalf("preserved metadata = index %+v/%t statistics=%+v/%t address=%q/%v",
			index, indexOK, statistics.Rows(), statisticsOK, address, addressErr)
	}
	if _, err := BuildManifestTransition(current, nextManifest, 5); !errors.Is(err, ErrCatalogGenerationNotNewer) {
		t.Fatalf("equal catalog generation err=%v, want ErrCatalogGenerationNotNewer", err)
	}
}

// TestCatalogHolderAtomicPublicationRace proves concurrent publication and reads
// never expose a torn or mixed generation: every observed snapshot is a whole,
// internally consistent immutable value. It is meaningful under -race.
func TestCatalogHolderAtomicPublicationRace(t *testing.T) {
	const generations = 8
	snaps := make([]*Snapshot, generations)
	for i := range snaps {
		snaps[i] = testSnapshot(t, uint64(i+1))
	}
	h := NewCatalogHolder(snaps[0])

	const (
		publishers = 4
		readers    = 4
		iterations = 2000
	)
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				h.Publish(snaps[(seed+i)%generations])
			}
		}(p)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				cur := h.Current()
				if cur == nil {
					t.Errorf("Current returned nil")
					return
				}
				if g := cur.Generation(); g < 1 || g > generations {
					t.Errorf("generation %d out of range", g)
					return
				}
				if _, err := cur.Address("ep-a"); err != nil {
					t.Errorf("Address(ep-a): %v", err)
					return
				}
				if _, ok := cur.OwnershipEpoch("tenant_data", "-80"); !ok {
					t.Errorf("OwnershipEpoch(-80) missing")
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := h.Current().Generation(); got != generations {
		t.Fatalf("concurrent monotonic publication ended at generation %d, want %d", got, generations)
	}
}
