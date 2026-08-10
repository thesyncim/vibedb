package gateway

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

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
			ID:      "-80",
			Range:   distribution.KeyRange{Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Point: point(0x80)}},
			Leaders: []distribution.EndpointID{"ep-a"},
			Epoch:   7,
		},
		{
			ID:      "80-",
			Range:   distribution.KeyRange{Start: point(0x80), End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"ep-b"},
			Epoch:   9,
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
	if addr, err := got.Address("ep-a"); err != nil || addr != "127.0.0.1:7001" {
		t.Fatalf("Address(ep-a) = %q,%v", addr, err)
	}
	if p, ok := got.Placement("messages"); !ok || len(p.Columns) != 1 || p.Columns[0] != "/tenant_id" {
		t.Fatalf("Placement(messages) = %+v,%v", p, ok)
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
