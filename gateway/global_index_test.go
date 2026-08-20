package gateway

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/shardservice"
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
		PrimaryPath:  "/id",
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
	changed.PrimaryPath = "/tenant_id"
	next, err = NewSnapshotWithIndexes(config, endpoints, 10, []IndexDescriptor{changed})
	if err != nil {
		t.Fatal(err)
	}
	if holder.PublishNewer(next) {
		t.Fatal("same global index incarnation changed primary locator identity")
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
	changed.PrimaryPath = ""
	if _, err := NewSnapshotWithIndexes(config, endpoints, 10, []IndexDescriptor{changed}); err == nil {
		t.Fatal("global locator without a certified primary path succeeded")
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
		!bytes.Equal(route.LocatorTuple, wantLocator) ||
		!bytes.Equal(route.LocatorValue, []byte(`["tenant-7","message-9"]`)) {
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

func TestGlobalIndexRoutesFlatScalarsWithoutDocument(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	snapshot, err := NewSnapshotWithIndexes(
		config, endpoints, 1, []IndexDescriptor{testGlobalIndexDescriptor()},
	)
	if err != nil {
		t.Fatal(err)
	}
	program, err := snapshot.CompileGlobalIndex("messages", "by_email")
	if err != nil {
		t.Fatal(err)
	}
	number, err := distribution.NewNumber("7.00e0")
	if err != nil {
		t.Fatal(err)
	}
	var workspace GlobalIndexWorkspace
	route, err := program.RouteScalars(
		[]distribution.Scalar{distribution.NewString("a@example.com")},
		[]distribution.Scalar{distribution.NewString("tenant"), number},
		&workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(route.LocatorValue, []byte(`["tenant",7.00e0]`)) {
		t.Fatalf("locator value = %s", route.LocatorValue)
	}
	if _, err := program.RouteScalars(nil, nil, &workspace); !errors.Is(err, ErrGlobalIndexDocument) {
		t.Fatalf("invalid scalar arity err = %v", err)
	}
}

func TestGlobalIndexRoutesLookupKeyAndReturnedLocator(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	snapshot, err := NewSnapshotWithIndexes(
		config, endpoints, 1, []IndexDescriptor{testGlobalIndexDescriptor()},
	)
	if err != nil {
		t.Fatal(err)
	}
	program, err := snapshot.CompileGlobalIndex("messages", "by_email")
	if err != nil {
		t.Fatal(err)
	}
	var workspace GlobalIndexWorkspace
	lookupKey := []distribution.Scalar{distribution.NewString("a@example.com")}
	locatorValue := []byte(`["tenant\u002d7","message-9"]`)
	keyRoute, err := program.RouteKey(
		lookupKey, &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(keyRoute.KeyTuple) == 0 || keyRoute.IndexTarget.Shard == "" ||
		keyRoute.IndexAddress == "" || keyRoute.IndexScope.End != keyRoute.IndexScope.Start+1 {
		t.Fatalf("key route = %+v", keyRoute)
	}
	locatorRoute, err := program.RouteLocatorValue(
		locatorValue, &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	if locatorRoute.BaseTarget.Shard == "" || locatorRoute.BaseAddress == "" ||
		locatorRoute.BaseScope.End != locatorRoute.BaseScope.Start+1 ||
		len(locatorRoute.LocatorTuple) == 0 || len(locatorRoute.BasePrimaryKey) == 0 {
		t.Fatalf("locator route = %+v", locatorRoute)
	}
	wantPrimary, ok := orderedkey.AppendString(nil, []byte("message-9"), orderedkey.Ascending)
	if !ok || !bytes.Equal(locatorRoute.BasePrimaryKey, wantPrimary) {
		t.Fatalf("base primary key = %x, want %x", locatorRoute.BasePrimaryKey, wantPrimary)
	}
	documentRoute, err := program.RouteDocument(
		[]byte(`{"tenant_id":"tenant-7","id":"message-9","email":"a@example.com"}`),
		&workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	if locatorRoute.BaseTarget != documentRoute.BaseTarget ||
		!bytes.Equal(locatorRoute.LocatorTuple, documentRoute.LocatorTuple) {
		t.Fatalf("lookup locator route = %+v, document route = %+v", locatorRoute, documentRoute)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, routeErr := program.RouteKey(lookupKey, &workspace); routeErr != nil {
			t.Fatal(routeErr)
		}
		if _, routeErr := program.RouteLocatorValue(locatorValue, &workspace); routeErr != nil {
			t.Fatal(routeErr)
		}
	}); allocations != 0 {
		t.Fatalf("warm global-index lookup routing allocations = %v, want 0", allocations)
	}
}

func TestGlobalIndexFiniteDomainsBatchOneLookupPerIndexShard(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	snapshot, err := NewSnapshotWithIndexes(
		config, endpoints, 1, []IndexDescriptor{testGlobalIndexDescriptor()},
	)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := snapshot.Prepare(
		context.Background(), `SELECT id FROM messages WHERE email IN (?, ?, ?)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := prepared.Bind([]any{"a@example.com", "b@example.com", "c@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if bound.globalIndex == nil {
		t.Fatal("finite global-index domain was not selected")
	}
	calls, keyCount, err := planGlobalIndexShardCalls(
		bound.globalIndex, DefaultProfiles()[ClassBatch],
	)
	if err != nil {
		t.Fatal(err)
	}
	if keyCount != 3 || len(calls) == 0 || len(calls) > 2 {
		t.Fatalf("batched lookup = %d keys across %d calls", keyCount, len(calls))
	}
	total := 0
	seen := make(map[distribution.ShardID]struct{}, len(calls))
	for i := range calls {
		request := calls[i].req.GlobalIndexLookup
		if _, duplicate := seen[calls[i].target.Shard]; duplicate {
			t.Fatalf("duplicate request for index shard %q", calls[i].target.Shard)
		}
		seen[calls[i].target.Shard] = struct{}{}
		if len(request.KeyTuples) == 0 {
			t.Fatalf("empty batched lookup for %q", calls[i].target.Shard)
		}
		for key := 1; key < len(request.KeyTuples); key++ {
			if bytes.Compare(request.KeyTuples[key-1], request.KeyTuples[key]) >= 0 {
				t.Fatalf("unsorted lookup keys for %q", calls[i].target.Shard)
			}
		}
		total += len(request.KeyTuples)
	}
	if total != keyCount {
		t.Fatalf("batched keys = %d, want %d", total, keyCount)
	}

	profile := DefaultProfiles()[ClassBatch]
	profile.Policy.Limits.MaxCandidateMappings = 2
	if _, _, err := planGlobalIndexShardCalls(bound.globalIndex, profile); !errors.Is(
		err, distribution.ErrRouteExpansionLimit,
	) {
		t.Fatalf("finite-domain overflow err = %v", err)
	}
}

func TestGlobalIndexBaseCallsCarrySortedNativePrimaryCandidates(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	snapshot, err := NewSnapshotWithIndexes(
		config, endpoints, 1, []IndexDescriptor{testGlobalIndexDescriptor()},
	)
	if err != nil {
		t.Fatal(err)
	}
	program, err := snapshot.CompileGlobalIndex("messages", "by_email")
	if err != nil {
		t.Fatal(err)
	}
	response := shardservice.RowsResponse(
		[]shardservice.Column{{Name: "locator"}},
		[][]shardservice.Cell{
			{{Bytes: []byte(`["tenant-7","message-z"]`)}},
			{{Bytes: []byte(`["tenant-7","message-a"]`)}},
		},
	)
	calls, _, err := globalIndexBaseCalls(program, response, &Query{
		SQL:    `SELECT id FROM messages WHERE email = ?`,
		Params: []shardservice.Param{shardservice.StringParam("a@example.com")},
	}, DefaultProfiles()[ClassBatch])
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || string(calls[0].req.PrimaryKeyRead.PrimaryPath) != "/id" ||
		len(calls[0].req.PrimaryKeyRead.Keys) != 2 ||
		bytes.Compare(calls[0].req.PrimaryKeyRead.Keys[0], calls[0].req.PrimaryKeyRead.Keys[1]) >= 0 {
		t.Fatalf("base candidate calls = %+v", calls)
	}
	want, _ := orderedkey.AppendString(nil, []byte("message-a"), orderedkey.Ascending)
	if !bytes.Equal(calls[0].req.PrimaryKeyRead.Keys[0], want) {
		t.Fatalf("first candidate = %x, want %x", calls[0].req.PrimaryKeyRead.Keys[0], want)
	}
}

func TestPreparedReadSelectsGlobalIndexOnlyWhenBaseIsNotPointRoutable(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	snapshot, err := NewSnapshotWithIndexes(
		config, endpoints, 1, []IndexDescriptor{testGlobalIndexDescriptor()},
	)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := snapshot.Prepare(
		context.Background(), `SELECT id FROM messages WHERE email = ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := prepared.Bind([]any{"a@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if bound.globalIndex == nil || !useGlobalIndexRead(bound) ||
		bound.globalIndex.program.metadata.Name != "by_email" {
		t.Fatalf("global index bound plan = %+v", bound.globalIndex)
	}

	prepared, err = snapshot.Prepare(
		context.Background(), `SELECT id FROM messages WHERE tenant_id = ? AND email = ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	bound, err = prepared.Bind([]any{"tenant-7", "a@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if bound.globalIndex != nil || useGlobalIndexRead(bound) {
		t.Fatalf("base point route did not retain direct fast path: %+v", bound)
	}
}

func TestExplainGlobalIndexReadShowsIndexScan(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	snapshot, err := NewSnapshotWithIndexes(
		config, endpoints, 7, []IndexDescriptor{testGlobalIndexDescriptor()},
	)
	if err != nil {
		t.Fatal(err)
	}
	executor := newRouteExecutor(t, snapshot)
	explanation, err := executor.Explain(t.Context(), Query{
		SQL:   `SELECT id FROM messages WHERE email = 'a@example.com'`,
		Class: ClassInteractive,
	})
	if err != nil {
		t.Fatal(err)
	}
	if explanation.RouteKind != distribution.RouteTargeted || explanation.Shards != 2 ||
		!strings.Contains(explanation.PhysicalPlan, "index-scan") ||
		explanation.PlanFingerprint == "" {
		t.Fatalf("global index explanation = %+v\n%s", explanation, explanation.PhysicalPlan)
	}
}

func TestGlobalIndexDynamicTargetAdmission(t *testing.T) {
	interactive := DefaultProfiles()[ClassInteractive]
	if _, err := admitGlobalIndexTargets(2, 2, interactive); !errors.Is(err, distribution.ErrScatterRejected) {
		t.Fatalf("interactive all-shard locator set err = %v", err)
	}
	batch := DefaultProfiles()[ClassBatch]
	if kind, err := admitGlobalIndexTargets(2, 2, batch); err != nil || kind != distribution.RouteScatter {
		t.Fatalf("batch all-shard locator set = %v,%v", kind, err)
	}
	if _, err := admitGlobalIndexTargets(
		distribution.DefaultMaxTargetShards+1,
		distribution.DefaultMaxTargetShards+2,
		batch,
	); !errors.Is(err, distribution.ErrTargetShardLimit) {
		t.Fatalf("dynamic target overflow err = %v", err)
	}
}

func TestPreparedInsertExpandsReadyGlobalIndexes(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	snapshot, err := NewSnapshotWithIndexes(
		config, endpoints, 1, []IndexDescriptor{testGlobalIndexDescriptor()},
	)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := snapshot.Prepare(context.Background(),
		`INSERT INTO messages (tenant_id, id, email) VALUES (?, ?, ?)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := prepared.BindWrite([]any{
		[]byte("tenant-7"), []byte("message-9"), []byte("a@example.com"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bound.globalIndexes) != 1 {
		t.Fatalf("global mutations = %d, want 1", len(bound.globalIndexes))
	}
	mutation := bound.globalIndexes[0]
	entry := bound.globalIndexArena[mutation.entryStart:mutation.entryEnd]
	value := bound.globalIndexArena[mutation.valueStart:mutation.valueEnd]
	if mutation.metadata.Name != "by_email" || mutation.target.Shard == "" ||
		mutation.address == "" || mutation.bucketBits == 0 ||
		mutation.scope.End != mutation.scope.Start+1 ||
		!bytes.Equal(value, []byte(`["tenant-7","message-9"]`)) ||
		len(entry) == 0 || entry[0] == 0 {
		t.Fatalf("bound global mutation = %+v entry=%x value=%s", mutation, entry, value)
	}

	whole, err := snapshot.Prepare(context.Background(),
		`INSERT INTO messages VALUES (?)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	wholeBound, err := whole.BindWrite([]any{[]byte(
		`{"tenant_id":"tenant-7","id":"message-9","email":"a@example.com"}`,
	)})
	if err != nil || len(wholeBound.globalIndexes) != 1 {
		t.Fatalf("whole-document global bind = %d,%v", len(wholeBound.globalIndexes), err)
	}

	update, err := snapshot.Prepare(context.Background(),
		`UPDATE messages SET "$doc" = ? WHERE tenant_id = ?`)
	if err != nil || len(update.writeGlobalIndexes) != 1 {
		t.Fatalf("prepare indexed UPDATE = %v, programs=%d", err, len(update.writeGlobalIndexes))
	}
	updateBound, err := update.BindWrite([]any{
		[]byte(`{"tenant_id":"tenant-7","id":"message-9","email":"b@example.com"}`),
		[]byte("tenant-7"),
	})
	if err != nil || len(updateBound.globalIndexes) != 0 {
		t.Fatalf("bind indexed UPDATE before capture = %v, mutations=%d", err, len(updateBound.globalIndexes))
	}
	remove, err := snapshot.Prepare(context.Background(),
		`DELETE FROM messages WHERE tenant_id = ?`)
	if err != nil || len(remove.writeGlobalIndexes) != 1 {
		t.Fatalf("prepare indexed DELETE = %v, programs=%d", err, len(remove.writeGlobalIndexes))
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

func TestGlobalIndexBuildAndDrainLifecyclesStayWriteMaintained(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	for _, lifecycle := range []IndexLifecycle{
		IndexBuilding, IndexCatchingUp, IndexDraining,
	} {
		descriptor := testGlobalIndexDescriptor()
		descriptor.Lifecycle = lifecycle
		snapshot, err := NewSnapshotWithIndexes(
			config, endpoints, uint64(lifecycle), []IndexDescriptor{descriptor},
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := snapshot.CompileGlobalIndex("messages", "by_email"); !errors.Is(err, ErrIndexNotReady) {
			t.Fatalf("%v read compiler = %v", lifecycle, err)
		}
		prepared, err := snapshot.Prepare(context.Background(),
			`INSERT INTO messages (tenant_id, id, email) VALUES (?, ?, ?)`)
		if err != nil {
			t.Fatalf("%v prepare: %v", lifecycle, err)
		}
		bound, err := prepared.BindWrite([]any{
			[]byte("tenant"), []byte("message"), []byte("a@example.com"),
		})
		if err != nil || len(bound.globalIndexes) != 1 {
			t.Fatalf("%v maintained mutations = %d, %v", lifecycle, len(bound.globalIndexes), err)
		}
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

func BenchmarkGlobalIndexRouteScalars(b *testing.B) {
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
	key := []distribution.Scalar{distribution.NewString("a@example.com")}
	locator := []distribution.Scalar{
		distribution.NewString("tenant-7"), distribution.NewString("message-9"),
	}
	var workspace GlobalIndexWorkspace
	if _, err := program.RouteScalars(key, locator, &workspace); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := program.RouteScalars(key, locator, &workspace); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGlobalIndexRouteLookup(b *testing.B) {
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
	key := []distribution.Scalar{distribution.NewString("a@example.com")}
	locator := []byte(`["tenant\u002d7","message-9"]`)
	var workspace GlobalIndexWorkspace
	if _, err := program.RouteKey(key, &workspace); err != nil {
		b.Fatal(err)
	}
	if _, err := program.RouteLocatorValue(locator, &workspace); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(locator)))
	b.ResetTimer()
	for range b.N {
		if _, err := program.RouteKey(key, &workspace); err != nil {
			b.Fatal(err)
		}
		if _, err := program.RouteLocatorValue(locator, &workspace); err != nil {
			b.Fatal(err)
		}
	}
}
