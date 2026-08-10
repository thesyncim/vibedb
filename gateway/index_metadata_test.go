package gateway

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"
)

func testIndexDescriptor() IndexDescriptor {
	return IndexDescriptor{
		IndexID: 41, Incarnation: 7, Table: "messages", Name: "by_tenant_created",
		Paths: []string{"/tenant_id", "/created_at"},
		Flags: IndexLocal | IndexCovering | IndexOrdered, Lifecycle: IndexReady,
	}
}

func TestIndexMetadataRoundTripAndDefensiveCopy(t *testing.T) {
	descriptors := []IndexDescriptor{testIndexDescriptor()}
	snapshot, err := NewSnapshotWithIndexes(testConfig(t), testEndpoints(), 11, descriptors)
	if err != nil {
		t.Fatalf("NewSnapshotWithIndexes: %v", err)
	}

	// Mutating every caller-owned slice after publication cannot change the
	// compact generation.
	descriptors[0].Name = "mutated"
	descriptors[0].Paths[0] = "/mutated"
	descriptors[0].Paths = append(descriptors[0].Paths, "/extra")
	descriptors = append(descriptors, IndexDescriptor{})

	metadata, ok := snapshot.Index("messages", "by_tenant_created")
	if !ok {
		t.Fatal("compact index lookup missed")
	}
	if metadata.IndexID != 41 || metadata.Incarnation != 7 || metadata.Table != "messages" ||
		metadata.PathCount != 2 || metadata.Paths[0] != "/tenant_id" || metadata.Paths[1] != "/created_at" ||
		metadata.Flags != IndexLocal|IndexCovering|IndexOrdered || !metadata.Ready() {
		t.Fatalf("metadata = %+v", metadata)
	}
	if metadata.Paths[2] != "" || metadata.Paths[3] != "" {
		t.Fatalf("unused fixed paths = %q/%q, want empty", metadata.Paths[2], metadata.Paths[3])
	}

	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveSnapshot(path, snapshot); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	loaded, err := LoadSnapshot(path)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	got, ok := loaded.Index("messages", "by_tenant_created")
	if !ok || got != metadata {
		t.Fatalf("loaded metadata = %+v,%v, want %+v,true", got, ok, metadata)
	}
}

func TestIndexMetadataEnumerationAndIncarnationFence(t *testing.T) {
	first := testIndexDescriptor()
	first.Name = "z_last"
	first.Lifecycle = IndexBuilding
	second := testIndexDescriptor()
	second.IndexID, second.Incarnation, second.Name = 42, 8, "a_first"
	second.Lifecycle = IndexCatchingUp
	third := testIndexDescriptor()
	third.IndexID, third.Incarnation, third.Name = 43, 9, "m_middle"
	third.Lifecycle = IndexDraining

	snapshot, err := NewSnapshotWithIndexes(testConfig(t), testEndpoints(), 1,
		[]IndexDescriptor{first, second, third})
	if err != nil {
		t.Fatalf("NewSnapshotWithIndexes: %v", err)
	}
	set := snapshot.Indexes("messages")
	if set.Len() != 3 {
		t.Fatalf("Indexes.Len = %d, want 3", set.Len())
	}
	for i, want := range []struct {
		name      string
		lifecycle IndexLifecycle
	}{{"a_first", IndexCatchingUp}, {"m_middle", IndexDraining}, {"z_last", IndexBuilding}} {
		got, ok := set.At(i)
		if !ok || got.Name != want.name || got.Lifecycle != want.lifecycle {
			t.Fatalf("At(%d) = %+v,%v, want %s/%d", i, got, ok, want.name, want.lifecycle)
		}
	}
	if _, ok := set.At(-1); ok {
		t.Fatal("At(-1) succeeded")
	}
	if _, ok := set.At(3); ok {
		t.Fatal("At(3) succeeded")
	}
	largeOrdinal := uint64(1) << 32
	if uint64(^uint(0)) > math.MaxUint32 {
		if _, ok := set.At(int(largeOrdinal)); ok {
			t.Fatal("At(1<<32) wrapped into the compact ordinal domain")
		}
	}
	if _, ok := snapshot.IndexIncarnation("messages", "a_first", 42, 8); !ok {
		t.Fatal("exact incarnation fence rejected current identity")
	}
	for _, stale := range [][2]uint64{{41, 8}, {42, 7}, {42, 9}} {
		if _, ok := snapshot.IndexIncarnation("messages", "a_first", stale[0], stale[1]); ok {
			t.Fatalf("stale identity %v crossed incarnation fence", stale)
		}
	}
	if snapshot.Indexes("absent").Len() != 0 {
		t.Fatal("missing table has indexes")
	}
	if metadata, ok := snapshot.Index("messages", "absent"); ok || metadata != (IndexMetadata{}) {
		t.Fatalf("missing index = %+v,%v", metadata, ok)
	}
}

func TestLocalUniqueIndexMustContainShardKey(t *testing.T) {
	config := testConfig(t)
	config.Distributions[0].Arity = 2
	config.Placements[0].Columns = []string{"/tenant_id", "/region_id"}
	valid := testIndexDescriptor()
	valid.Flags = IndexLocal | IndexUnique | IndexOrdered
	valid.Paths = []string{"/email", "/region_id", "/tenant_id"}
	if _, err := NewSnapshotWithIndexes(config, testEndpoints(), 1, []IndexDescriptor{valid}); err != nil {
		t.Fatalf("local unique containing complete shard key: %v", err)
	}

	invalid := valid
	invalid.Paths = []string{"/email", "/tenant_id"}
	if _, err := NewSnapshotWithIndexes(config, testEndpoints(), 1, []IndexDescriptor{invalid}); err == nil {
		t.Fatal("local unique missing one shard-key path succeeded")
	}

	// Global uniqueness cannot be certified before global-index enforcement and
	// a distributed transaction fence exist.
	invalid.Flags = IndexUnique
	if _, err := NewSnapshotWithIndexes(config, testEndpoints(), 1, []IndexDescriptor{invalid}); err == nil {
		t.Fatal("non-local unique descriptor succeeded")
	}
}

func TestIndexDescriptorValidation(t *testing.T) {
	valid := testIndexDescriptor()
	tests := []struct {
		name   string
		mutate func(*IndexDescriptor)
	}{
		{"zero id", func(d *IndexDescriptor) { d.IndexID = 0 }},
		{"zero incarnation", func(d *IndexDescriptor) { d.Incarnation = 0 }},
		{"missing table", func(d *IndexDescriptor) { d.Table = "absent" }},
		{"empty table", func(d *IndexDescriptor) { d.Table = "" }},
		{"invalid table utf8", func(d *IndexDescriptor) { d.Table = "\xff" }},
		{"empty name", func(d *IndexDescriptor) { d.Name = "" }},
		{"name nul", func(d *IndexDescriptor) { d.Name = "bad\x00name" }},
		{"invalid name utf8", func(d *IndexDescriptor) { d.Name = "\xff" }},
		{"no paths", func(d *IndexDescriptor) { d.Paths = nil }},
		{"too many paths", func(d *IndexDescriptor) { d.Paths = []string{"/a", "/b", "/c", "/d", "/e"} }},
		{"invalid path", func(d *IndexDescriptor) { d.Paths = []string{"not-a-pointer"} }},
		{"invalid path utf8", func(d *IndexDescriptor) { d.Paths = []string{"/\xff"} }},
		{"duplicate path", func(d *IndexDescriptor) { d.Paths = []string{"/a", "/a"} }},
		{"no locality", func(d *IndexDescriptor) { d.Flags = IndexOrdered }},
		{"unknown flags", func(d *IndexDescriptor) { d.Flags = 1 << 7 }},
		{"invalid lifecycle", func(d *IndexDescriptor) { d.Lifecycle = 0 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			descriptor := valid
			tc.mutate(&descriptor)
			if _, err := NewSnapshotWithIndexes(testConfig(t), testEndpoints(), 1, []IndexDescriptor{descriptor}); err == nil {
				t.Fatal("invalid descriptor succeeded")
			}
		})
	}
}

func TestIndexDescriptorRejectsDuplicateIdentityAndName(t *testing.T) {
	first := testIndexDescriptor()
	duplicateID := testIndexDescriptor()
	duplicateID.Name = "another_name"
	duplicateID.Incarnation++
	if _, err := NewSnapshotWithIndexes(testConfig(t), testEndpoints(), 1,
		[]IndexDescriptor{first, duplicateID}); err == nil {
		t.Fatal("duplicate stable index id succeeded")
	}

	duplicateName := testIndexDescriptor()
	duplicateName.IndexID++
	duplicateName.Incarnation++
	if _, err := NewSnapshotWithIndexes(testConfig(t), testEndpoints(), 1,
		[]IndexDescriptor{first, duplicateName}); err == nil {
		t.Fatal("duplicate table/index name succeeded")
	}
}

func TestCompactPlannerDimensionGuard(t *testing.T) {
	if err := validateCompactPlannerCount("placement", uint64(math.MaxUint32)+1); err == nil {
		t.Fatal("oversized placement directory passed compact dimension guard")
	}
}

func TestIndexCatalogPublicationFencesDefinitionAndLifecycle(t *testing.T) {
	building := testIndexDescriptor()
	building.Lifecycle = IndexBuilding
	first, err := NewSnapshotWithIndexes(testConfig(t), testEndpoints(), 1, []IndexDescriptor{building})
	if err != nil {
		t.Fatal(err)
	}
	holder := NewCatalogHolder(first)

	publish := func(generation uint64, descriptor *IndexDescriptor) bool {
		t.Helper()
		var descriptors []IndexDescriptor
		if descriptor != nil {
			descriptors = []IndexDescriptor{*descriptor}
		}
		snapshot, buildErr := NewSnapshotWithIndexes(testConfig(t), testEndpoints(), generation, descriptors)
		if buildErr != nil {
			t.Fatalf("generation %d: %v", generation, buildErr)
		}
		return holder.PublishNewer(snapshot)
	}

	changed := building
	changed.Paths = []string{"/tenant_id", "/changed"}
	if publish(2, &changed) {
		t.Fatal("same incarnation changed its path definition")
	}
	changed = building
	changed.Flags |= IndexUnique
	if publish(2, &changed) {
		t.Fatal("same incarnation changed its certified flags")
	}
	ready := building
	ready.Lifecycle = IndexReady
	if !publish(2, &ready) {
		t.Fatal("Building -> Ready publication was refused")
	}
	regressed := ready
	regressed.Lifecycle = IndexCatchingUp
	if publish(3, &regressed) {
		t.Fatal("Ready -> CatchingUp lifecycle regression published")
	}
	if !publish(3, nil) {
		t.Fatal("newer snapshot could not remove an index")
	}

	readyBase, err := NewSnapshotWithIndexes(testConfig(t), testEndpoints(), 10, []IndexDescriptor{ready})
	if err != nil {
		t.Fatal(err)
	}
	holder = NewCatalogHolder(readyBase)
	rebuilt := ready
	rebuilt.Incarnation++
	rebuilt.Paths = []string{"/tenant_id", "/replacement"}
	if !publish(11, &rebuilt) {
		t.Fatal("generation jump did not admit a higher incarnation")
	}
	stale := rebuilt
	stale.Incarnation--
	if publish(12, &stale) {
		t.Fatal("older incarnation republished")
	}
}

func TestCompactIndexDirectory(t *testing.T) {
	if got := unsafe.Sizeof(plannerIndex{}); got != 32 {
		t.Fatalf("plannerIndex = %d bytes, want 32", got)
	}
	if got := unsafe.Sizeof(plannerStringRef{}); got != 8 {
		t.Fatalf("plannerStringRef = %d bytes, want 8", got)
	}
	without, err := NewSnapshotWithIndexes(testConfig(t), testEndpoints(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if without.plannerIndexSpans != nil || without.PlannerIndexMetadataBytes() != 0 {
		t.Fatalf("index-free spans/bytes = %v/%d, want nil/0", without.plannerIndexSpans, without.PlannerIndexMetadataBytes())
	}

	for _, count := range []int{1, 1_000, 100_000} {
		t.Run(fmt.Sprintf("indexes=%d", count), func(t *testing.T) {
			descriptors := makeIndexDescriptors(count)
			snapshot, err := NewSnapshotWithIndexes(testConfig(t), testEndpoints(), 1, descriptors)
			if err != nil {
				t.Fatalf("NewSnapshotWithIndexes: %v", err)
			}
			name := fmt.Sprintf("index_%06d", count/2)
			metadata, ok := snapshot.Index("messages", name)
			if !ok || metadata.Name != name || metadata.IndexID != uint64(count/2+1) {
				t.Fatalf("lookup %q = %+v,%v", name, metadata, ok)
			}
			nameBytes := 0
			for i := range count {
				nameBytes += len(fmt.Sprintf("index_%06d", i))
			}
			wantBytes := uint64(count*32 + count*8 + 8 + nameBytes + len("/tenant_id"))
			if got := snapshot.PlannerIndexMetadataBytes(); got != wantBytes {
				t.Fatalf("metadata bytes = %d, want exact %d (%0.2f/index)", got, wantBytes, float64(got)/float64(count))
			}
			if allocs := testing.AllocsPerRun(1000, func() {
				_, _ = snapshot.Index("messages", name)
			}); allocs != 0 {
				t.Fatalf("lookup allocations = %v, want 0", allocs)
			}
			if allocs := testing.AllocsPerRun(1000, func() {
				set := snapshot.Indexes("messages")
				_, _ = set.At(set.Len() / 2)
			}); allocs != 0 {
				t.Fatalf("enumeration allocations = %v, want 0", allocs)
			}
		})
	}
}

func makeIndexDescriptors(count int) []IndexDescriptor {
	descriptors := make([]IndexDescriptor, count)
	sharedPath := []string{"/tenant_id"}
	for i := range descriptors {
		descriptors[i] = IndexDescriptor{
			IndexID: uint64(i + 1), Incarnation: 1, Table: "messages",
			Name: fmt.Sprintf("index_%06d", i), Paths: sharedPath,
			Flags: IndexLocal | IndexOrdered, Lifecycle: IndexReady,
		}
	}
	return descriptors
}

func BenchmarkCompactIndexLookup(b *testing.B) {
	for _, count := range []int{1, 1_000, 100_000} {
		b.Run(fmt.Sprintf("indexes=%d", count), func(b *testing.B) {
			snapshot, err := NewSnapshotWithIndexes(testConfig(b), testEndpoints(), 1, makeIndexDescriptors(count))
			if err != nil {
				b.Fatal(err)
			}
			name := fmt.Sprintf("index_%06d", count/2)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, ok := snapshot.Index("messages", name); !ok {
					b.Fatal("lookup missed")
				}
			}
			b.ReportMetric(float64(snapshot.PlannerIndexMetadataBytes())/float64(count), "planner-B/index")
		})
	}
}

func TestPersistedIndexesAreDeterministicallyOrdered(t *testing.T) {
	descriptors := makeIndexDescriptors(3)
	descriptors[0], descriptors[2] = descriptors[2], descriptors[0]
	snapshot, err := NewSnapshotWithIndexes(testConfig(t), testEndpoints(), 1, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	persisted := toPersisted(snapshot)
	var names []string
	for _, index := range persisted.Indexes {
		names = append(names, index.Name)
	}
	if got := strings.Join(names, ","); got != "index_000000,index_000001,index_000002" {
		t.Fatalf("persisted order = %s", got)
	}
}
