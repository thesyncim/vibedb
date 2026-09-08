package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

type rf3DiagnosticResourceApply struct {
	resources sqldriver.ReplicatedApplyResourceStats
	err       error
	calls     int
}

func (apply *rf3DiagnosticResourceApply) ResourceStats() (sqldriver.ReplicatedApplyResourceStats, error) {
	apply.calls++
	return apply.resources, apply.err
}

func rf3DiagnosticTestGroup(id byte) raftmember.GroupKey {
	return raftmember.GroupKey{GroupID: [16]byte{id}}
}

func rf3DiagnosticTestStats(seed uint64) durable.Stats {
	return durable.Stats{
		ResidentBytes:                         seed + 101,
		CommitCapacityBytes:                   seed + 102,
		AutomaticCheckpoints:                  seed + 14,
		RetirementPressureCheckpoints:         seed + 15,
		DirtyBytes:                            seed + 13,
		SnapshotCapacity:                      seed + 103,
		ActiveSnapshots:                       seed + 104,
		OldestSnapshotGeneration:              seed + 105,
		OldestSnapshotAgeGenerations:          seed + 106,
		FreeScratchCapacityBytes:              seed + 107,
		FreeScratchExternalBytes:              seed + 108,
		FreeScratchLiveBytes:                  seed + 109,
		PrimaryOverlayFolds:                   seed,
		PrimaryOverlayMaterializationAttempts: seed + 1,
		PrimaryOverlayMaterializations:        seed + 2,
		PrimaryOverlayMaterializationFailures: seed + 3,
		PrimaryOverlayFoldNS: durable.StatsHistogram{
			Count: seed + 4, Sum: (seed + 4) * 10, Max: seed * 100,
			Buckets: [durable.StatsHistogramBuckets]uint64{seed, seed * 2},
		},
		PrimaryOverlayPressureFolds:     seed + 5,
		PrimaryOverlaySnapshotFolds:     seed + 6,
		PrimaryOverlayBarrierFolds:      seed + 7,
		PrimaryOverlayCheckpointFolds:   seed + 8,
		PrimaryOverlayArenaBytes:        seed + 9,
		PrimaryOverlayRetainedRecords:   seed + 10,
		PrimaryOverlayDirtyBuckets:      seed + 11,
		PrimaryOverlayReservedFoldBytes: seed + 12,
	}
}

func TestRF3DiagnosticResourceCollectionsPreserveMemoryBoundaries(t *testing.T) {
	first, second := &rf3DiagnosticResourceApply{resources: rf3DiagnosticTestResources(1)},
		&rf3DiagnosticResourceApply{resources: rf3DiagnosticTestResources(5)}
	groupOne, groupTwo := rf3DiagnosticTestGroup(1), rf3DiagnosticTestGroup(2)
	totals := aggregateRF3DiagnosticResources(
		map[raftmember.GroupKey]struct{}{groupTwo: {}, groupOne: {}},
		map[raftmember.GroupKey]rf3DiagnosticApply{groupTwo: second, groupOne: first}, false)
	if !totals.available || totals.collectionsTruncated || len(totals.collections) != 8 {
		t.Fatalf("collection cut availability=%t truncated=%t count=%d", totals.available,
			totals.collectionsTruncated, len(totals.collections))
	}
	want := []struct {
		group           raftmember.GroupKey
		kind            string
		relationOrdinal int
		seed            uint64
	}{
		{group: groupOne, kind: "system", relationOrdinal: -1, seed: 1},
		{group: groupOne, kind: "capture", relationOrdinal: -1, seed: 2},
		{group: groupOne, kind: "relation", relationOrdinal: 0, seed: 3},
		{group: groupOne, kind: "relation", relationOrdinal: 1, seed: 4},
		{group: groupTwo, kind: "system", relationOrdinal: -1, seed: 5},
		{group: groupTwo, kind: "capture", relationOrdinal: -1, seed: 6},
		{group: groupTwo, kind: "relation", relationOrdinal: 0, seed: 7},
		{group: groupTwo, kind: "relation", relationOrdinal: 1, seed: 8},
	}
	for index, expected := range want {
		got := totals.collections[index]
		if got.Group.GroupID != fmt.Sprintf("%032x", expected.group.GroupID) ||
			got.Collection != expected.kind || got.RelationOrdinal != expected.relationOrdinal ||
			got.ResidentBytes != expected.seed+101 || got.CommitCapacityBytes != expected.seed+102 ||
			got.SnapshotCapacity != expected.seed+103 || got.ActiveSnapshots != expected.seed+104 ||
			got.OldestSnapshotGeneration != expected.seed+105 ||
			got.OldestSnapshotAgeGenerations != expected.seed+106 ||
			got.FreeScratchCapacityBytes != expected.seed+107 ||
			got.FreeScratchExternalBytes != expected.seed+108 ||
			got.FreeScratchLiveBytes != expected.seed+109 {
			t.Fatalf("collection[%d]=%+v want group=%+v kind=%q ordinal=%d seed=%d", index, got,
				expected.group, expected.kind, expected.relationOrdinal, expected.seed)
		}
	}
}

func TestRF3DiagnosticSnapshotIncludesBoundedResourceMemoryCuts(t *testing.T) {
	group := rf3DiagnosticTestGroup(1)
	apply := &rf3DiagnosticResourceApply{resources: rf3DiagnosticTestResources(1)}
	totals := aggregateRF3DiagnosticResources(
		map[raftmember.GroupKey]struct{}{group: {}},
		map[raftmember.GroupKey]rf3DiagnosticApply{group: apply}, false)
	var snapshot rf3DiagnosticSnapshot
	applyRF3DiagnosticResourceTotals(&snapshot, totals)
	if !snapshot.ResourceStatsAvailable || !snapshot.ResourceCollectionsAvailable ||
		snapshot.ResourceCollectionsTruncated || len(snapshot.ResourceCollections) != 4 {
		t.Fatalf("resource memory cut availability=%t/%t truncated=%t count=%d",
			snapshot.ResourceStatsAvailable, snapshot.ResourceCollectionsAvailable,
			snapshot.ResourceCollectionsTruncated, len(snapshot.ResourceCollections))
	}
	collection := snapshot.ResourceCollections[2]
	if collection.Collection != "relation" || collection.RelationOrdinal != 0 ||
		collection.ResidentBytes != 104 || collection.CommitCapacityBytes != 105 ||
		collection.SnapshotCapacity != 106 || collection.ActiveSnapshots != 107 ||
		collection.OldestSnapshotGeneration != 108 ||
		collection.OldestSnapshotAgeGenerations != 109 ||
		collection.FreeScratchCapacityBytes != 110 ||
		collection.FreeScratchExternalBytes != 111 || collection.FreeScratchLiveBytes != 112 {
		t.Fatalf("relation collection cut=%+v", collection)
	}
}

func TestRF3DiagnosticFourGroupSnapshotFitsReaderBound(t *testing.T) {
	expected := make(map[raftmember.GroupKey]struct{}, 4)
	providers := make(map[raftmember.GroupKey]rf3DiagnosticApply, 4)
	for index := range [4]struct{}{} {
		group := rf3DiagnosticTestGroup(byte(index + 1))
		expected[group] = struct{}{}
		providers[group] = &rf3DiagnosticResourceApply{resources: rf3DiagnosticTestResources(uint64(index + 1))}
	}
	resources := aggregateRF3DiagnosticResources(expected, providers, false)
	snapshot := rf3DiagnosticSnapshot{
		ReadyWaveHistogram:              make([]uint64, raftstore.MaxPersistGroupBatches+1),
		ReadySeriesHistogram:            make([]uint64, raftstore.MaxReadySeries+1),
		ReadyDurableHistogram:           make([]uint64, raftstore.MaxReadySeries+1),
		RaftProposalQueueDepthHistogram: make([]uint64, raftservice.ProposalEntryHistogramBuckets),
		RaftProposalEntriesPerReady:     make([]uint64, raftservice.ProposalEntryHistogramBuckets),
		RaftProposalBytesPerReady:       make([]uint64, raftservice.ProposalBytesHistogramBuckets),
		RuntimeMemStatsAvailable:        true,
		RuntimeMemStats: rf3DiagnosticRuntimeMemStats{
			HeapAlloc: ^uint64(0), HeapInuse: ^uint64(0), HeapIdle: ^uint64(0),
			HeapReleased: ^uint64(0), Sys: ^uint64(0),
		},
	}
	applyRF3DiagnosticResourceTotals(&snapshot, resources)
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("encode diagnostic snapshot: %v", err)
	}
	if len(raw) > 64<<10 || !snapshot.ResourceStatsAvailable ||
		!snapshot.ResourceCollectionsAvailable || snapshot.ResourceCollectionsTruncated ||
		len(snapshot.ResourceCollections) != 16 {
		t.Fatalf("four-group diagnostic size=%d resource=%t/%t truncated=%t collections=%d",
			len(raw), snapshot.ResourceStatsAvailable, snapshot.ResourceCollectionsAvailable,
			snapshot.ResourceCollectionsTruncated, len(snapshot.ResourceCollections))
	}
}

func TestRF3DiagnosticResourceCollectionCutIsBounded(t *testing.T) {
	expected := make(map[raftmember.GroupKey]struct{}, rf3DiagnosticMaxResourceCollections)
	providers := make(map[raftmember.GroupKey]rf3DiagnosticApply, rf3DiagnosticMaxResourceCollections)
	for index := 1; index <= rf3DiagnosticMaxResourceCollections; index++ {
		group := rf3DiagnosticTestGroup(byte(index))
		expected[group] = struct{}{}
		providers[group] = &rf3DiagnosticResourceApply{resources: rf3DiagnosticTestResources(uint64(index))}
	}
	totals := aggregateRF3DiagnosticResources(expected, providers, false)
	if !totals.available || !totals.collectionsTruncated || len(totals.collections) != rf3DiagnosticMaxResourceCollections {
		t.Fatalf("bounded collection cut available=%t truncated=%t count=%d", totals.available,
			totals.collectionsTruncated, len(totals.collections))
	}
}

func rf3DiagnosticTestResources(first uint64) sqldriver.ReplicatedApplyResourceStats {
	return sqldriver.ReplicatedApplyResourceStats{
		System:        rf3DiagnosticTestStats(first),
		Capture:       rf3DiagnosticTestStats(first + 1),
		Relations:     [replication.MaxRelationsPerBundle]durable.Stats{rf3DiagnosticTestStats(first + 2), rf3DiagnosticTestStats(first + 3)},
		RelationCount: 2,
	}
}

func TestRF3DiagnosticResourceAggregationIncludesAllParticipants(t *testing.T) {
	first, second := &rf3DiagnosticResourceApply{resources: rf3DiagnosticTestResources(1)},
		&rf3DiagnosticResourceApply{resources: rf3DiagnosticTestResources(5)}
	expected := map[raftmember.GroupKey]struct{}{
		rf3DiagnosticTestGroup(1): {}, rf3DiagnosticTestGroup(2): {},
	}
	providers := map[raftmember.GroupKey]rf3DiagnosticApply{
		rf3DiagnosticTestGroup(1): first, rf3DiagnosticTestGroup(2): second,
	}
	beforeFirst, beforeSecond := first.resources, second.resources
	totals := aggregateRF3DiagnosticResources(expected, providers, false)
	if !totals.available || totals.covered != 2 || totals.failures != 0 {
		t.Fatalf("resource availability=%t covered=%d failures=%d", totals.available, totals.covered, totals.failures)
	}
	if first.calls != 1 || second.calls != 1 {
		t.Fatalf("resource calls=(%d,%d), want one per group", first.calls, second.calls)
	}
	if totals.primaryOverlayFolds != 36 || totals.primaryOverlayMaterializationAttempts != 44 ||
		totals.primaryOverlayMaterializations != 52 || totals.primaryOverlayMaterializationFailures != 60 {
		t.Fatalf("overlay counters=%d/%d/%d/%d", totals.primaryOverlayFolds,
			totals.primaryOverlayMaterializationAttempts, totals.primaryOverlayMaterializations,
			totals.primaryOverlayMaterializationFailures)
	}
	if totals.automaticCheckpoints != 148 || totals.retirementPressureCheckpoints != 156 ||
		totals.dirtyBytes != 140 {
		t.Fatalf("checkpoint/dirty counters=%d/%d/%d", totals.automaticCheckpoints,
			totals.retirementPressureCheckpoints, totals.dirtyBytes)
	}
	if totals.primaryOverlayFoldNS.Count != 68 || totals.primaryOverlayFoldNS.Sum != 680 ||
		totals.primaryOverlayFoldNS.Max != 800 || totals.primaryOverlayFoldNS.Buckets[0] != 36 ||
		totals.primaryOverlayFoldNS.Buckets[1] != 72 {
		t.Fatalf("fold histogram=%+v", totals.primaryOverlayFoldNS)
	}
	if totals.primaryOverlayPressureFolds != 76 || totals.primaryOverlaySnapshotFolds != 84 ||
		totals.primaryOverlayBarrierFolds != 92 || totals.primaryOverlayCheckpointFolds != 100 ||
		totals.primaryOverlayArenaBytes != 108 || totals.primaryOverlayRetainedRecords != 116 ||
		totals.primaryOverlayDirtyBuckets != 124 || totals.primaryOverlayReservedFoldBytes != 132 {
		t.Fatalf("overlay gauges/reasons=%d/%d/%d/%d/%d/%d/%d/%d",
			totals.primaryOverlayPressureFolds, totals.primaryOverlaySnapshotFolds,
			totals.primaryOverlayBarrierFolds, totals.primaryOverlayCheckpointFolds,
			totals.primaryOverlayArenaBytes, totals.primaryOverlayRetainedRecords,
			totals.primaryOverlayDirtyBuckets, totals.primaryOverlayReservedFoldBytes)
	}
	if !reflect.DeepEqual(first.resources, beforeFirst) || !reflect.DeepEqual(second.resources, beforeSecond) {
		t.Fatal("resource sampling mutated detached counters")
	}
}

func TestRF3DiagnosticResourceGroupDedupKeepsOneProvider(t *testing.T) {
	group := rf3DiagnosticTestGroup(1)
	first := &rf3DiagnosticResourceApply{resources: rf3DiagnosticTestResources(1)}
	second := &rf3DiagnosticResourceApply{resources: rf3DiagnosticTestResources(5)}
	expected := make(map[raftmember.GroupKey]struct{})
	providers := make(map[raftmember.GroupKey]rf3DiagnosticApply)
	addRF3DiagnosticResourceGroup(expected, group)
	addRF3DiagnosticResourceGroup(expected, group)
	addRF3DiagnosticResourceProvider(providers, group, first)
	addRF3DiagnosticResourceProvider(providers, group, second)
	totals := aggregateRF3DiagnosticResources(expected, providers, false)
	if len(expected) != 1 || len(providers) != 1 || totals.covered != 1 || totals.failures != 0 || !totals.available {
		t.Fatalf("deduplicated groups=%d providers=%d totals=%+v", len(expected), len(providers), totals)
	}
	if first.calls != 1 || second.calls != 0 || totals.primaryOverlayFolds != 10 {
		t.Fatalf("deduplicated provider calls=(%d,%d), folds=%d", first.calls, second.calls, totals.primaryOverlayFolds)
	}
}

func TestRF3DiagnosticCurrentGenerationMasksPreparedProvider(t *testing.T) {
	group := rf3DiagnosticTestGroup(1)
	prepared := &rf3DiagnosticResourceApply{resources: rf3DiagnosticTestResources(1)}
	current := &rf3DiagnosticResourceApply{resources: rf3DiagnosticTestResources(5)}
	expected := map[raftmember.GroupKey]struct{}{group: {}}
	providers := make(map[raftmember.GroupKey]rf3DiagnosticApply)
	addRF3DiagnosticResourceProvider(providers, group, prepared)
	setRF3DiagnosticResourceProvider(providers, group, current)
	totals := aggregateRF3DiagnosticResources(expected, providers, false)
	if !totals.available || current.calls != 1 || prepared.calls != 0 || totals.primaryOverlayFolds != 26 {
		t.Fatalf("current provider calls=(%d,%d), available=%t folds=%d", prepared.calls, current.calls,
			totals.available, totals.primaryOverlayFolds)
	}
	setRF3DiagnosticResourceProvider(providers, group, nil)
	totals = aggregateRF3DiagnosticResources(expected, providers, false)
	if totals.available || totals.covered != 0 || totals.failures != 1 {
		t.Fatalf("nil current provider available=%t covered=%d failures=%d", totals.available, totals.covered, totals.failures)
	}
}

func TestRF3DiagnosticResourceFailuresAreUnavailable(t *testing.T) {
	groups := []raftmember.GroupKey{
		rf3DiagnosticTestGroup(1), rf3DiagnosticTestGroup(2),
		rf3DiagnosticTestGroup(3), rf3DiagnosticTestGroup(4),
	}
	expected := make(map[raftmember.GroupKey]struct{}, len(groups))
	for _, group := range groups {
		expected[group] = struct{}{}
	}
	closed := &rf3DiagnosticResourceApply{err: sqldriver.ErrReplicatedApplyClosed}
	malformed := &rf3DiagnosticResourceApply{resources: sqldriver.ReplicatedApplyResourceStats{}}
	tooMany := &rf3DiagnosticResourceApply{resources: sqldriver.ReplicatedApplyResourceStats{
		RelationCount: uint16(len((sqldriver.ReplicatedApplyResourceStats{}).Relations)) + 1,
	}}
	providers := map[raftmember.GroupKey]rf3DiagnosticApply{
		groups[0]: &rf3DiagnosticResourceApply{resources: rf3DiagnosticTestResources(1)},
		groups[1]: closed,
		groups[2]: malformed,
		groups[3]: tooMany,
	}
	totals := aggregateRF3DiagnosticResources(expected, providers, true)
	if totals.available || totals.covered != 1 || totals.failures != 4 {
		t.Fatalf("failed resource totals available=%t covered=%d failures=%d", totals.available, totals.covered, totals.failures)
	}
	if closed.calls != 1 || malformed.calls != 1 || tooMany.calls != 1 {
		t.Fatalf("invalid resource calls=(%d,%d,%d)", closed.calls, malformed.calls, tooMany.calls)
	}
}

func TestRF3DiagnosticResourceLiveUnionOmitsRetiredPreparedGroups(t *testing.T) {
	live, retired := rf3DiagnosticTestGroup(1), rf3DiagnosticTestGroup(2)
	manifest := rf3Manifest{Groups: []rf3ManifestGroup{{Route: rf3ManifestGroupRoute{Group: live}}}}
	prepared := []preparedRF3Group{{manifest: rf3Manifest{Route: rf3ManifestGroupRoute{Group: retired}}}}
	totals := collectRF3DiagnosticResources(manifest, prepared, nil, nil)
	if len(totals.expected) != 1 {
		t.Fatalf("expected live group count=%d, want one", len(totals.expected))
	}
	if _, found := totals.expected[retired]; found {
		t.Fatal("retired prepared group reintroduced into live union")
	}
	if totals.available || totals.covered != 0 || totals.failures != 1 {
		t.Fatalf("live union totals available=%t covered=%d failures=%d", totals.available, totals.covered, totals.failures)
	}
}

func TestRF3DiagnosticInventorySnapshotFailsClosed(t *testing.T) {
	group := rf3DiagnosticTestGroup(1)
	inventory := &rf3AdoptedGroupInventory{failed: true}
	children := rf3NativeChildren{group: {Group: group}}
	inventory.nativeChildren.Store(&children)
	snapshot := snapshotRF3DiagnosticInventory(inventory)
	if snapshot.usable || len(snapshot.nativeGroups) != 1 || len(snapshot.providers) != 0 {
		t.Fatalf("failed inventory snapshot usable=%t native=%d providers=%d", snapshot.usable,
			len(snapshot.nativeGroups), len(snapshot.providers))
	}
	manifest := rf3Manifest{Groups: []rf3ManifestGroup{{Route: rf3ManifestGroupRoute{Group: group}}}}
	totals := collectRF3DiagnosticResources(manifest, nil, inventory, nil)
	if totals.available || totals.covered != 0 || totals.failures != 2 {
		t.Fatalf("failed inventory totals available=%t covered=%d failures=%d", totals.available, totals.covered, totals.failures)
	}
}

func TestRF3DiagnosticResourceOverflowFailsClosed(t *testing.T) {
	group := rf3DiagnosticTestGroup(1)
	stats := rf3DiagnosticTestResources(^uint64(0))
	stats.Capture.PrimaryOverlayFolds = 1
	apply := &rf3DiagnosticResourceApply{resources: stats}
	totals := aggregateRF3DiagnosticResources(
		map[raftmember.GroupKey]struct{}{group: {}},
		map[raftmember.GroupKey]rf3DiagnosticApply{group: apply}, false,
	)
	if totals.available || totals.failures != 1 || !totals.overflow {
		t.Fatalf("overflow totals available=%t failures=%d overflow=%t", totals.available, totals.failures, totals.overflow)
	}
}

func TestRF3DiagnosticResourceErrorIdentity(t *testing.T) {
	apply := &rf3DiagnosticResourceApply{err: sqldriver.ErrReplicatedApplyClosed}
	group := rf3DiagnosticTestGroup(1)
	totals := aggregateRF3DiagnosticResources(
		map[raftmember.GroupKey]struct{}{group: {}},
		map[raftmember.GroupKey]rf3DiagnosticApply{group: apply}, false,
	)
	if totals.available || totals.covered != 0 || totals.failures != 1 || !errors.Is(apply.err, sqldriver.ErrReplicatedApplyClosed) {
		t.Fatalf("closed apply totals available=%t covered=%d failures=%d err=%v", totals.available, totals.covered, totals.failures, apply.err)
	}
}

// The production adapter calls Collection.Stats for each participant. Exercise
// that exact storage sampler with a pending row overlay: a Snapshot-based
// substitute would fold it even though the logical rows remain unchanged.
type rf3DiagnosticCollectionApply struct{ collection *durable.Collection }

func (apply rf3DiagnosticCollectionApply) ResourceStats() (sqldriver.ReplicatedApplyResourceStats, error) {
	return sqldriver.ReplicatedApplyResourceStats{
		Relations:     [replication.MaxRelationsPerBundle]durable.Stats{apply.collection.Stats()},
		RelationCount: 1,
	}, nil
}

func TestRF3DiagnosticRealCollectionSamplingPreservesPendingOverlay(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "pending.vjc"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	options := durable.Options{Backend: durable.BackendPortable, ResidentBytes: 32 << 20,
		Durability: durable.DurabilityBufferedVisible}
	records := make([]durable.PrimaryBulkRecord, 1000)
	for index := range records {
		records[index] = durable.PrimaryBulkRecord{
			Key:   fmt.Sprintf("primary-key-%09d", index),
			Value: fmt.Appendf(nil, `{"id":%d,"group":%d,"name":"primary row %d"}`, index, index%997, index),
		}
	}
	if _, err := durable.CreateFromRecords(records, file, options); err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = collection.Close() })
	key := []byte(records[500].Key)
	if created, err := collection.Put(key, []byte(`{"hot":"first","id":500}`)); err != nil || created {
		t.Fatalf("buffered update: created=%t err=%v", created, err)
	}
	before := collection.Stats()
	if before.PrimaryOverlayDirtyBuckets == 0 || before.PrimaryOverlayRetainedRecords == 0 ||
		before.PrimaryOverlayArenaBytes == 0 || before.PrimaryOverlayReservedFoldBytes == 0 ||
		before.PublishedGeneration <= before.DurableGeneration {
		t.Fatalf("fixture has no pending overlay: %+v", before)
	}
	group := rf3DiagnosticTestGroup(1)
	totals := aggregateRF3DiagnosticResources(
		map[raftmember.GroupKey]struct{}{group: {}},
		map[raftmember.GroupKey]rf3DiagnosticApply{group: rf3DiagnosticCollectionApply{collection}}, false,
	)
	if !totals.available || totals.covered != 1 || totals.failures != 0 ||
		totals.primaryOverlayFolds != before.PrimaryOverlayFolds ||
		totals.primaryOverlayMaterializations != before.PrimaryOverlayMaterializations ||
		totals.primaryOverlayDirtyBuckets != before.PrimaryOverlayDirtyBuckets ||
		totals.primaryOverlayReservedFoldBytes != before.PrimaryOverlayReservedFoldBytes {
		t.Fatalf("real pending resource totals: %+v", totals)
	}
	// Compare every counter, including page reads, device bytes/commits, journal
	// syncs, materialization histograms, dirty debt and publication generations.
	if after := collection.Stats(); after != before {
		t.Fatalf("sampling changed storage state or I/O: before=%+v after=%+v", before, after)
	}
	if _, err := collection.Put(key, []byte(`{"hot":"other","id":500}`)); err != nil {
		t.Fatal(err)
	}
	if after := collection.Stats(); after.PrimaryOverlayRetainedRecords <= before.PrimaryOverlayRetainedRecords {
		t.Fatal("fixture did not advance the pending overlay after sampling")
	}
	if totals.primaryOverlayRetainedRecords != before.PrimaryOverlayRetainedRecords ||
		totals.primaryOverlayArenaBytes != before.PrimaryOverlayArenaBytes {
		t.Fatal("retained diagnostics changed after a later collection mutation")
	}
}
