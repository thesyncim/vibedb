package gateway

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
)

type catalogTransitionBenchmarkScale struct {
	placements int
	indexes    int
	shards     int
}

func BenchmarkCatalogTransitionScale(b *testing.B) {
	for _, dimension := range []struct {
		name  string
		scale func(int) catalogTransitionBenchmarkScale
	}{
		{"placements", func(n int) catalogTransitionBenchmarkScale {
			return catalogTransitionBenchmarkScale{placements: n, shards: 1}
		}},
		{"indexes", func(n int) catalogTransitionBenchmarkScale {
			return catalogTransitionBenchmarkScale{placements: 1, indexes: n, shards: 1}
		}},
		{"shards", func(n int) catalogTransitionBenchmarkScale {
			return catalogTransitionBenchmarkScale{placements: 1, shards: n}
		}},
	} {
		for _, count := range []int{1, 1_000, 100_000} {
			b.Run(fmt.Sprintf("advance/%s=%d", dimension.name, count), func(b *testing.B) {
				benchmarkCatalogTransition(b, dimension.scale(count), false)
			})
			b.Run(fmt.Sprintf("publish/%s=%d", dimension.name, count), func(b *testing.B) {
				benchmarkCatalogTransition(b, dimension.scale(count), true)
			})
		}
	}
}

func BenchmarkBuildManifestTransitionBatch(b *testing.B) {
	config, endpoints := globalIndexCatalog(b)
	current, err := NewSnapshotWithIndexes(
		config, endpoints, 5, []IndexDescriptor{testGlobalIndexDescriptor()},
	)
	if err != nil {
		b.Fatal(err)
	}
	base, _ := current.Manifest("tenant_data")
	index, _ := current.Manifest("message_email_index")
	manifests := []*distribution.Manifest{
		changedManifestLeader(b, index, "ep-index-b"),
		changedManifestLeader(b, base, "ep-b"),
	}
	b.Run("batch", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			next, err := BuildManifestTransitions(current, manifests, 6)
			if err != nil {
				b.Fatal(err)
			}
			runtime.KeepAlive(next)
		}
	})
	b.Run("independent", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			for _, manifest := range manifests {
				next, err := BuildManifestTransition(current, manifest, 6)
				if err != nil {
					b.Fatal(err)
				}
				runtime.KeepAlive(next)
			}
		}
	})
}

func benchmarkCatalogTransition(
	b *testing.B,
	scale catalogTransitionBenchmarkScale,
	publish bool,
) {
	b.Helper()
	currentRaw := benchmarkCatalogSnapshot(b, 1, scale)
	next := benchmarkCatalogSnapshot(b, 2, scale)
	current, err := initialCatalogState(currentRaw)
	if err != nil {
		b.Fatal(err)
	}
	probe, err := advanceCatalogState(current, next)
	if err != nil {
		b.Fatal(err)
	}
	if got := len(probe.shardGenerationHighWaters); got != 1 {
		b.Fatalf("shard high-water records = %d, want 1", got)
	}
	if got := catalogLineagePayloadBytes(probe); got != 16 {
		b.Fatalf("lineage payload = %d bytes, want 16", got)
	}
	wantTransitionBytes := uint64(16 + 4*scale.indexes + 8*scale.shards)
	if got := probe.CatalogTransitionMetadataBytes(); got != wantTransitionBytes {
		b.Fatalf("transition metadata = %d bytes, want %d", got, wantTransitionBytes)
	}

	records := scale.placements + scale.indexes + scale.shards
	b.ReportAllocs()
	b.ResetTimer()

	if !publish {
		for range b.N {
			published, err := advanceCatalogState(current, next)
			if err != nil {
				b.Fatal(err)
			}
			runtime.KeepAlive(published)
		}
		reportCatalogTransitionMetrics(b, probe, records)
		return
	}

	// PublishNewer mutates its holder. Resetting the already-normalized pointer
	// contributes one atomic store per operation but no catalog allocations.
	holder := &CatalogHolder{}
	for range b.N {
		holder.ptr.Store(current)
		if !holder.PublishNewer(next) {
			b.Fatal("PublishNewer refused a valid transition")
		}
	}
	reportCatalogTransitionMetrics(b, probe, records)
}

func reportCatalogTransitionMetrics(b *testing.B, snapshot *Snapshot, records int) {
	b.ReportMetric(float64(records), "records/op")
	b.ReportMetric(float64(catalogLineagePayloadBytes(snapshot)), "lineage-payload-B")
	b.ReportMetric(float64(snapshot.CatalogTransitionMetadataBytes()), "transition-metadata-B")
	b.ReportMetric(float64(len(snapshot.shardGenerationHighWaters)), "shard-water-records")
}

func catalogLineagePayloadBytes(snapshot *Snapshot) uintptr {
	if snapshot == nil {
		return 0
	}
	return unsafe.Sizeof(snapshot.indexIDHighWater) +
		uintptr(cap(snapshot.shardGenerationHighWaters))*
			unsafe.Sizeof(distribution.ShardAllocationGeneration(0))
}

func benchmarkCatalogSnapshot(
	b testing.TB,
	generation uint64,
	scale catalogTransitionBenchmarkScale,
) *Snapshot {
	b.Helper()
	manifest := benchmarkCatalogManifest(b, scale.shards)
	placements := make([]distribution.TablePlacement, scale.placements)
	for i := range placements {
		placements[i] = distribution.TablePlacement{
			Table:        fmt.Sprintf("table_%06d", i),
			Distribution: "bench",
			Columns:      []string{"/tenant_id"},
		}
	}
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{
			Name: "bench", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
		}},
		Placements: placements,
		Manifests:  []*distribution.Manifest{manifest},
	}
	indexes := make([]IndexDescriptor, scale.indexes)
	for i := range indexes {
		indexes[i] = IndexDescriptor{
			IndexID: uint64(i + 1), Incarnation: 1,
			Table: "table_000000", Name: fmt.Sprintf("index_%06d", i),
			Paths: []string{"/tenant_id"}, Flags: IndexLocal | IndexOrdered,
			Lifecycle: IndexReady,
		}
	}
	snapshot, err := NewSnapshotWithIndexes(
		config,
		map[distribution.EndpointID]string{"bench-ep": "127.0.0.1:1"},
		generation,
		indexes,
	)
	if err != nil {
		b.Fatalf("NewSnapshotWithIndexes: %v", err)
	}
	return snapshot
}

func benchmarkCatalogManifest(b testing.TB, count int) *distribution.Manifest {
	b.Helper()
	shards := make([]distribution.Shard, count)
	for i := range shards {
		var start distribution.KeyspacePoint
		binary.BigEndian.PutUint64(start[:], uint64(i))
		end := distribution.KeyspaceEnd{Max: i == count-1}
		if !end.Max {
			binary.BigEndian.PutUint64(end.Point[:], uint64(i+1))
		}
		shards[i] = distribution.Shard{
			ID:                   distribution.ShardID(fmt.Sprintf("shard_%06d", i)),
			AllocationGeneration: distribution.ShardAllocationGeneration(i + 1),
			Range:                distribution.KeyRange{Start: start, End: end},
			Leaders:              []distribution.EndpointID{"bench-ep"},
			Epoch:                1,
		}
	}
	manifest, err := distribution.NewManifest("bench", 1, shards)
	if err != nil {
		b.Fatalf("NewManifest: %v", err)
	}
	return manifest
}
