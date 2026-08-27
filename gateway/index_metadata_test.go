package gateway

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
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
	rebuilt, err := NewSnapshotWithIndexes(snapshot.config, snapshot.endpoints, 12, snapshot.indexDescriptors())
	if err != nil {
		t.Fatalf("rebuild local index metadata for catalog transition: %v", err)
	}
	if got, ok := rebuilt.Index(metadata.Table, metadata.Name); !ok || got != metadata {
		t.Fatalf("catalog transition changed local index: %+v", got)
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

	// Locality is always explicit; omitting both local and global cannot smuggle
	// an unenforced unique descriptor into the catalog.
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
	changed = building
	changed.Name = "renamed"
	if publish(2, &changed) {
		t.Fatal("same incarnation changed its logical name")
	}
	changed = building
	changed.Paths = []string{"/tenant_id"}
	if publish(2, &changed) {
		t.Fatal("same incarnation changed its path count")
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
	renamedRebuild := rebuilt
	renamedRebuild.Incarnation++
	renamedRebuild.Name = "renamed"
	if publish(12, &renamedRebuild) {
		t.Fatal("higher incarnation changed its logical name")
	}
	stale := rebuilt
	stale.Incarnation--
	if publish(12, &stale) {
		t.Fatal("older incarnation republished")
	}
}

func TestSaveSnapshotFencesDurableIndexIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	descriptor := testIndexDescriptor()
	descriptor.Lifecycle = IndexReady
	first, err := NewSnapshotWithIndexes(
		testConfig(t), testEndpoints(), 1, []IndexDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveSnapshot(path, first); err != nil {
		t.Fatal(err)
	}

	changed := descriptor
	changed.Paths = []string{"/tenant_id", "/changed"}
	next, err := NewSnapshotWithIndexes(
		testConfig(t), testEndpoints(), 2, []IndexDescriptor{changed},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveSnapshot(path, next); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("SaveSnapshot changed incarnation identity err=%v, want ErrInvalidCatalog", err)
	}
	durable, err := LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if durable.Generation() != 1 {
		t.Fatalf("durable generation = %d, want 1", durable.Generation())
	}
}

func TestIndexIDHighWaterFencesReuseAfterRetirement(t *testing.T) {
	descriptor := testIndexDescriptor()
	descriptor.Lifecycle = IndexReady
	withIndex := func(generation uint64, descriptor *IndexDescriptor) *Snapshot {
		t.Helper()
		var indexes []IndexDescriptor
		if descriptor != nil {
			indexes = []IndexDescriptor{*descriptor}
		}
		snapshot, err := NewSnapshotWithIndexes(
			testConfig(t), testEndpoints(), generation, indexes,
		)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}

	first := withIndex(1, &descriptor)
	retired := withIndex(2, nil)
	reused := withIndex(3, &descriptor)
	holder := NewCatalogHolder(first)
	if !holder.PublishNewer(retired) {
		t.Fatal("index retirement was refused")
	}
	if holder.PublishNewer(reused) {
		t.Fatal("retired IndexID/incarnation was reused in memory")
	}
	holderPath := filepath.Join(t.TempDir(), "holder-catalog.json")
	if err := SaveSnapshot(holderPath, holder.Current()); err != nil {
		t.Fatal(err)
	}
	if err := SaveSnapshot(holderPath, reused); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("holder publication lost retired identity history: %v", err)
	}

	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveSnapshot(path, first); err != nil {
		t.Fatal(err)
	}
	if err := SaveSnapshot(path, retired); err != nil {
		t.Fatal(err)
	}
	if err := SaveSnapshot(path, reused); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("retired durable IndexID/incarnation reuse err=%v, want ErrInvalidCatalog", err)
	}
	reusedWithHigherIncarnation := descriptor
	reusedWithHigherIncarnation.Incarnation++
	if err := SaveSnapshot(path, withIndex(3, &reusedWithHigherIncarnation)); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("retired IndexID with higher incarnation err=%v, want ErrInvalidCatalog", err)
	}
	replacement := descriptor
	replacement.IndexID++
	replacement.Incarnation++
	if err := SaveSnapshot(path, withIndex(3, &replacement)); err != nil {
		t.Fatalf("fresh monotonic index id was refused: %v", err)
	}
}

func TestIndexIDHighWaterRetainsNoRetiredIdentityStrings(t *testing.T) {
	backing := strings.Repeat("x", 1<<20) + "by_tenant_created"
	descriptor := testIndexDescriptor()
	descriptor.Name = backing[len(backing)-len("by_tenant_created"):]
	snapshot, err := NewSnapshotWithIndexes(
		testConfig(t), testEndpoints(), 1, []IndexDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	holder := NewCatalogHolder(snapshot)
	retired, err := NewSnapshotWithIndexes(testConfig(t), testEndpoints(), 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !holder.PublishNewer(retired) {
		t.Fatal("index retirement was refused")
	}
	current := holder.Current()
	if current.indexIDHighWater != descriptor.IndexID {
		t.Fatalf("index id high-water = %d, want %d", current.indexIDHighWater, descriptor.IndexID)
	}
	if current.plannerIndexStrings != "" || len(current.plannerIndexes) != 0 {
		t.Fatal("retired index identity strings remain reachable from the active snapshot")
	}
	if len(current.shardGenerationHighWaters) != len(current.config.Distributions) {
		t.Fatalf("shard allocation high-waters = %d, want one per distribution", len(current.shardGenerationHighWaters))
	}
}

func TestIndexIDHighWaterSupportsSkippedCatalogGenerations(t *testing.T) {
	descriptor := testIndexDescriptor()
	descriptor.Lifecycle = IndexReady
	build := func(generation, indexID uint64) *Snapshot {
		t.Helper()
		candidate := descriptor
		candidate.IndexID = indexID
		snapshot, err := NewSnapshotWithIndexes(
			testConfig(t), testEndpoints(), generation, []IndexDescriptor{candidate},
		)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}

	holder := NewCatalogHolder(build(1, 41))
	skipped := build(2, 42)
	skipped.catalogLineagePresent = true
	skipped.indexIDHighWater = 100
	skipped.shardGenerationHighWaters = []distribution.ShardAllocationGeneration{2}
	if !holder.PublishNewer(skipped) {
		t.Fatal("generation jump with a monotonic persisted high-water was refused")
	}
	if holder.Current().indexIDHighWater != 100 {
		t.Fatalf("index high-water = %d, want skipped value 100", holder.Current().indexIDHighWater)
	}
	if holder.PublishNewer(build(3, 50)) {
		t.Fatal("id below the skipped high-water was admitted")
	}
	if !holder.PublishNewer(build(3, 101)) {
		t.Fatal("id above the skipped high-water was refused")
	}
	if next, ok := holder.Current().NextIndexID(); !ok || next != 102 {
		t.Fatalf("next index id = %d,%v, want 102,true", next, ok)
	}
}

func TestNextIndexIDUsesActiveOrPersistedHighWater(t *testing.T) {
	descriptor := testIndexDescriptor()
	descriptor.IndexID = 41
	fresh, err := NewSnapshotWithIndexes(
		testConfig(t), testEndpoints(), 1, []IndexDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	if next, ok := fresh.NextIndexID(); !ok || next != 42 {
		t.Fatalf("fresh next index id = %d,%v, want 42,true", next, ok)
	}
	if _, ok := (*Snapshot)(nil).NextIndexID(); ok {
		t.Fatal("nil snapshot returned an index id")
	}

	exhausted, err := NewSnapshotWithIndexes(testConfig(t), testEndpoints(), 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	exhausted.catalogLineagePresent = true
	exhausted.indexIDHighWater = ^uint64(0)
	if _, ok := exhausted.NextIndexID(); ok {
		t.Fatal("exhausted index id namespace wrapped")
	}
}

func TestCompactIndexDirectory(t *testing.T) {
	if got := unsafe.Sizeof(plannerIndex{}); got != 32 {
		t.Fatalf("plannerIndex = %d bytes, want 32", got)
	}
	if got := unsafe.Sizeof(plannerIndexNameRef{}); got != 8 {
		t.Fatalf("plannerIndexNameRef = %d bytes, want 8", got)
	}
	if got := unsafe.Sizeof(plannerStringRef{}); got != 8 {
		t.Fatalf("plannerStringRef = %d bytes, want 8", got)
	}
	if got := unsafe.Sizeof(plannerIndexLineageRef(0)); got != 4 {
		t.Fatalf("plannerIndexLineageRef = %d bytes, want 4", got)
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
			normalized, err := initialCatalogState(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if normalized.indexIDHighWater != uint64(count) ||
				len(normalized.shardGenerationHighWaters) != len(normalized.config.Distributions) {
				t.Fatalf("compact lineage = index high-water %d, shard water count %d",
					normalized.indexIDHighWater, len(normalized.shardGenerationHighWaters))
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

func TestIndexLineageDirectoryIsIDSortedIndependentlyOfNames(t *testing.T) {
	low, high := testIndexDescriptor(), testIndexDescriptor()
	low.IndexID, low.Name = 2, "z_by_tenant"
	high.IndexID, high.Name = 9, "a_by_tenant"
	snapshot, err := NewSnapshotWithIndexes(
		testConfig(t), testEndpoints(), 1, []IndexDescriptor{high, low},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.indexLineage) != 2 {
		t.Fatalf("index lineage refs = %d, want 2", len(snapshot.indexLineage))
	}
	first := snapshot.indexMetadataForLineage(snapshot.indexLineage[0])
	second := snapshot.indexMetadataForLineage(snapshot.indexLineage[1])
	if first.IndexID != 2 || second.IndexID != 9 {
		t.Fatalf("index lineage order = %d,%d, want 2,9", first.IndexID, second.IndexID)
	}
}

func TestPackedIndexPropertiesAndLineageTableOwnersRoundTrip(t *testing.T) {
	config := testConfig(t)
	config.Placements = append(config.Placements, distribution.TablePlacement{
		Table: "events", Distribution: "tenant_data", Columns: []string{"/tenant_id"},
	})
	lifecycles := [...]IndexLifecycle{
		IndexBuilding, IndexCatchingUp, IndexReady, IndexDraining,
	}
	flags := [...]IndexFlags{
		IndexLocal,
		IndexLocal | IndexUnique,
		IndexLocal | IndexCovering,
		IndexLocal | IndexOrdered,
	}
	descriptors := make([]IndexDescriptor, len(lifecycles))
	for i := range descriptors {
		paths := make([]string, i+1)
		paths[0] = "/tenant_id"
		for path := 1; path < len(paths); path++ {
			paths[path] = fmt.Sprintf("/field_%d", path)
		}
		table := "messages"
		if i&1 == 0 {
			table = "events"
		}
		descriptors[i] = IndexDescriptor{
			IndexID: uint64(100 - i), Incarnation: uint64(i + 1),
			Table: table, Name: fmt.Sprintf("packed_%d", i), Paths: paths,
			Flags: flags[i], Lifecycle: lifecycles[i],
		}
	}

	snapshot, err := NewSnapshotWithIndexes(config, testEndpoints(), 1, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	for i := range descriptors {
		want := &descriptors[i]
		got, ok := snapshot.Index(want.Table, want.Name)
		if !ok || got.IndexID != want.IndexID || got.Incarnation != want.Incarnation ||
			got.Table != want.Table || got.Name != want.Name ||
			got.PathCount != uint8(len(want.Paths)) || got.Flags != want.Flags ||
			got.Lifecycle != want.Lifecycle {
			t.Fatalf("packed descriptor %d = %+v,%v, want %+v", i, got, ok, *want)
		}
		for path := range want.Paths {
			if got.Paths[path] != want.Paths[path] {
				t.Fatalf("packed descriptor %d path %d = %q, want %q",
					i, path, got.Paths[path], want.Paths[path])
			}
		}
	}
	for i, ref := range snapshot.indexLineage {
		got := snapshot.indexMetadataForLineage(ref)
		wantID := uint64(97 + i)
		if got.IndexID != wantID {
			t.Fatalf("lineage ordinal %d id = %d, want %d", i, got.IndexID, wantID)
		}
		if want := descriptors[100-int(wantID)].Table; got.Table != want {
			t.Fatalf("lineage ordinal %d table = %q, want %q", i, got.Table, want)
		}
	}
}

func TestPackedIndexNameLengthBoundary(t *testing.T) {
	descriptor := testIndexDescriptor()
	descriptor.Name = strings.Repeat("n", math.MaxUint16)
	snapshot, err := NewSnapshotWithIndexes(
		testConfig(t), testEndpoints(), 1, []IndexDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	metadata, ok := snapshot.Index("messages", descriptor.Name)
	if !ok || metadata.Name != descriptor.Name {
		t.Fatalf("maximum-length packed name round trip = %d bytes,%v, want %d,true",
			len(metadata.Name), ok, len(descriptor.Name))
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
