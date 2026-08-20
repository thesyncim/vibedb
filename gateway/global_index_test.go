package gateway

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

func globalIndexCatalog(t testing.TB) (distribution.ClusterConfig, map[distribution.EndpointID]string) {
	t.Helper()
	config := testConfig(t)
	config.Distributions = append(config.Distributions, distribution.DistributionSpec{
		Name: "message_email_index", Arity: 1,
		MapperVersion: distribution.NativeMapperVersion,
		BucketBits:    distribution.DefaultVirtualBucketBits,
	})
	config.Placements = append(config.Placements, distribution.TablePlacement{
		Table: "messages_email_index", Distribution: "message_email_index",
		Columns: []string{"/email"},
	})
	manifest, err := distribution.NewManifest("message_email_index", 5, []distribution.Shard{
		{
			ID: "idx-80", AllocationGeneration: 3,
			Range:   distribution.KeyRange{Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Point: point(0x80)}},
			Leaders: []distribution.EndpointID{"ep-index-a"}, Epoch: 11,
		},
		{
			ID: "idx-80-", AllocationGeneration: 4,
			Range:   distribution.KeyRange{Start: point(0x80), End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"ep-index-b"}, Epoch: 13,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	config.Manifests = append(config.Manifests, manifest)
	endpoints := testEndpoints()
	endpoints["ep-index-a"] = "127.0.0.1:7101"
	endpoints["ep-index-b"] = "127.0.0.1:7102"
	return config, endpoints
}

func testGlobalIndexDescriptor() IndexDescriptor {
	return IndexDescriptor{
		IndexID: 51, Incarnation: 2, Table: "messages", Name: "by_email",
		Relation: "messages_email_index", Paths: []string{"/email"},
		LocatorPaths: []string{"/tenant_id", "/id"},
		Flags:        IndexGlobal | IndexUnique | IndexOrdered, Lifecycle: IndexReady,
	}
}

func TestGlobalIndexMetadataPersistenceAndFences(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	descriptor := testGlobalIndexDescriptor()
	snapshot, err := NewSnapshotWithIndexes(config, endpoints, 9, []IndexDescriptor{descriptor})
	if err != nil {
		t.Fatal(err)
	}
	descriptor.Relation = "mutated"
	descriptor.LocatorPaths[0] = "/mutated"
	metadata, ok := snapshot.Index("messages", "by_email")
	if !ok || !metadata.Global() || metadata.Relation != "messages_email_index" ||
		metadata.PathCount != 1 || metadata.Paths[0] != "/email" ||
		metadata.LocatorCount != 2 || metadata.LocatorPaths[0] != "/tenant_id" ||
		metadata.LocatorPaths[1] != "/id" {
		t.Fatalf("global metadata = %+v,%v", metadata, ok)
	}
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveSnapshot(path, snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := loaded.Index("messages", "by_email"); !ok || got != metadata {
		t.Fatalf("loaded global metadata = %+v,%v, want %+v", got, ok, metadata)
	}
	holder := NewCatalogHolder(snapshot)
	transitionConfig := cloneConfig(config)
	transitionConfig.Placements = append(transitionConfig.Placements, distribution.TablePlacement{
		Table: "messages_email_index_replacement", Distribution: "message_email_index",
		Columns: []string{"/email"},
	})
	changed := testGlobalIndexDescriptor()
	changed.Relation = "messages_email_index_replacement"
	next, err := NewSnapshotWithIndexes(transitionConfig, endpoints, 10, []IndexDescriptor{changed})
	if err != nil {
		t.Fatal(err)
	}
	if holder.PublishNewer(next) {
		t.Fatal("same global index incarnation changed its physical relation")
	}
	changed = testGlobalIndexDescriptor()
	changed.LocatorPaths = []string{"/id", "/tenant_id"}
	next, err = NewSnapshotWithIndexes(config, endpoints, 10, []IndexDescriptor{changed})
	if err != nil {
		t.Fatal(err)
	}
	if holder.PublishNewer(next) {
		t.Fatal("same global index incarnation changed locator order")
	}

	changed = testGlobalIndexDescriptor()
	changed.Relation = "messages"
	if _, err := NewSnapshotWithIndexes(config, endpoints, 10, []IndexDescriptor{changed}); err == nil {
		t.Fatal("global relation aliasing base table succeeded")
	}
	changed = testGlobalIndexDescriptor()
	changed.LocatorPaths = []string{"/id"}
	if _, err := NewSnapshotWithIndexes(config, endpoints, 10, []IndexDescriptor{changed}); err == nil {
		t.Fatal("global locator missing base placement path succeeded")
	}
	changed = testGlobalIndexDescriptor()
	changed.Flags |= IndexLocal
	if _, err := NewSnapshotWithIndexes(config, endpoints, 10, []IndexDescriptor{changed}); err == nil {
		t.Fatal("index marked both local and global succeeded")
	}
}

func TestGlobalIndexRoutesIndexAndBaseIndependently(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	descriptor := testGlobalIndexDescriptor()
	snapshot, err := NewSnapshotWithIndexes(config, endpoints, 1, []IndexDescriptor{descriptor})
	if err != nil {
		t.Fatal(err)
	}
	program, err := snapshot.CompileGlobalIndex("messages", "by_email")
	if err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"tenant_id":"tenant-7","id":"message-9","email":"a\u0040example.com"}`)
	var workspace GlobalIndexWorkspace
	route, err := program.RouteDocument(document, &workspace)
	if err != nil {
		t.Fatal(err)
	}
	wantKey, err := distribution.CurrentTupleCodec.AppendTuple(nil, []distribution.Scalar{
		distribution.NewString("a@example.com"),
	})
	if err != nil {
		t.Fatal(err)
	}
	wantLocator, err := distribution.CurrentTupleCodec.AppendTuple(nil, []distribution.Scalar{
		distribution.NewString("tenant-7"), distribution.NewString("message-9"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(route.KeyTuple, wantKey) || !bytes.Equal(route.EntryKey, wantKey) ||
		!bytes.Equal(route.LocatorTuple, wantLocator) {
		t.Fatalf("tuples key=%x entry=%x locator=%x, want %x/%x", route.KeyTuple, route.EntryKey, route.LocatorTuple, wantKey, wantLocator)
	}
	indexMapper := distribution.NewNativeMapperWithBucketBits(1, distribution.DefaultVirtualBucketBits)
	wantIndexPoint, _ := indexMapper.PointFor([]distribution.Scalar{distribution.NewString("a@example.com")})
	baseMapper := distribution.NewNativeMapperWithBucketBits(1, distribution.DefaultVirtualBucketBits)
	wantBasePoint, _ := baseMapper.PointFor([]distribution.Scalar{distribution.NewString("tenant-7")})
	if route.IndexPoint != wantIndexPoint || route.BasePoint != wantBasePoint ||
		route.IndexTarget.Shard == "" || route.BaseTarget.Shard == "" ||
		route.IndexAddress == "" || route.BaseAddress == "" ||
		route.IndexScope.End != route.IndexScope.Start+1 ||
		route.BaseScope.End != route.BaseScope.Start+1 {
		t.Fatalf("resolved route = %+v", route)
	}

	// Warm routing reuses the tape, decoded-string arena, and tuple buffers.
	if _, err := program.RouteDocument(document, &workspace); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, routeErr := program.RouteDocument(document, &workspace); routeErr != nil {
			t.Fatal(routeErr)
		}
	}); allocations != 0 {
		t.Fatalf("warm global-index route allocations = %v, want 0", allocations)
	}

	for _, invalid := range [][]byte{
		[]byte(`{"tenant_id":"tenant-7","id":"message-9"}`),
		[]byte(`{"tenant_id":"tenant-7","id":"message-9","email":null}`),
		[]byte(`{"tenant_id":"tenant-7","id":"message-9","email":true}`),
	} {
		if _, err := program.RouteDocument(invalid, &workspace); !errors.Is(err, ErrGlobalIndexDocument) {
			t.Fatalf("invalid document %s err=%v", invalid, err)
		}
	}
}

func TestGlobalNonUniqueEntryKeyIncludesLocator(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	descriptor := testGlobalIndexDescriptor()
	descriptor.Flags &^= IndexUnique
	snapshot, err := NewSnapshotWithIndexes(config, endpoints, 1, []IndexDescriptor{descriptor})
	if err != nil {
		t.Fatal(err)
	}
	program, err := snapshot.CompileGlobalIndex("messages", "by_email")
	if err != nil {
		t.Fatal(err)
	}
	var workspace GlobalIndexWorkspace
	route, err := program.RouteDocument(
		[]byte(`{"tenant_id":"t","id":"m","email":"e"}`), &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte(nil), route.KeyTuple...), route.LocatorTuple...)
	if !bytes.Equal(route.EntryKey, want) {
		t.Fatalf("non-unique entry key = %x, want %x", route.EntryKey, want)
	}
}

func BenchmarkGlobalIndexRouteDocument(b *testing.B) {
	config, endpoints := globalIndexCatalog(b)
	snapshot, err := NewSnapshotWithIndexes(
		config, endpoints, 1, []IndexDescriptor{testGlobalIndexDescriptor()},
	)
	if err != nil {
		b.Fatal(err)
	}
	program, err := snapshot.CompileGlobalIndex("messages", "by_email")
	if err != nil {
		b.Fatal(err)
	}
	document := []byte(`{"tenant_id":"tenant-7","id":"message-9","email":"a@example.com","payload":"some bytes"}`)
	var workspace GlobalIndexWorkspace
	if _, err := program.RouteDocument(document, &workspace); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(document)))
	b.ResetTimer()
	for range b.N {
		if _, err := program.RouteDocument(document, &workspace); err != nil {
			b.Fatal(err)
		}
	}
}
