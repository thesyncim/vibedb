package durable

import (
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// BenchmarkFileMaterializationDurableUpdate compares matched single-writer,
// durable-per-operation updates. Both arms use explicit async visibility and
// Flush after every Put, so every required durability barrier remains inside
// the timer. Patch-only materialization reports its exact two-barrier protocol.
func BenchmarkFileMaterializationDurableUpdate(b *testing.B) {
	previous := store.SetZonePruning(true)
	defer store.SetZonePruning(previous)

	neutral := [2][]byte{
		[]byte(`{"zone":"fixed","payload":1.0}`),
		[]byte(`{"zone":"fixed","payload":1e0}`),
	}
	zoned := make([][]byte, 512)
	var previousZoneCode uint32
	for index := range zoned {
		number := fmt.Sprintf("%012d", int64(100_000_000_000)+int64(index)*1_000_000_000)
		code, ok := store.ZoneCodeNumber([]byte(number))
		if !ok || index != 0 && code <= previousZoneCode {
			b.Fatalf("zone benchmark code %q = %d after %d",
				number, code, previousZoneCode)
		}
		previousZoneCode = code
		zoned[index] = []byte(fmt.Sprintf(
			`{"zone":%s,"payload":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`,
			number,
		))
	}
	for _, shape := range []struct {
		name   string
		values [][]byte
	}{
		{name: "zone-neutral", values: neutral[:]},
		{name: "zone-changing", values: zoned},
	} {
		for _, mode := range []struct {
			name    string
			granule int
		}{
			{name: "cow"},
			{name: "materialized", granule: 512},
		} {
			b.Run(shape.name+"/"+mode.name, func(b *testing.B) {
				file, err := os.CreateTemp(b.TempDir(), "materialization-bench-*")
				if err != nil {
					b.Fatal(err)
				}
				defer file.Close()
				collection, err := Create(file, Options{
					Collection:                   store.Options{ChunkDocuments: 4},
					Durability:                   DurabilityAsyncVisible,
					MaterializationDamageGranule: mode.granule,
					DisableMutationCombining:     true,
					ResidentBytes:                64 << 20,
					Backend:                      BackendPortable,
				})
				if err != nil {
					b.Fatal(err)
				}
				defer collection.Close()
				if _, err := collection.Put("key", shape.values[0]); err != nil {
					b.Fatal(err)
				}
				if err := collection.Flush(); err != nil {
					b.Fatal(err)
				}
				base := collection.Stats()
				baseFile, err := file.Stat()
				if err != nil {
					b.Fatal(err)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; b.Loop(); iteration++ {
					value := shape.values[(iteration+1)%len(shape.values)]
					if _, err := collection.Put("key", value); err != nil {
						b.Fatal(err)
					}
					if err := collection.Flush(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				stats := collection.Stats()
				finalFile, err := file.Stat()
				if err != nil {
					b.Fatal(err)
				}
				updates := float64(b.N)
				if updates == 0 {
					return
				}
				b.ReportMetric(
					float64(stats.DeviceBytes-base.DeviceBytes)/updates,
					"devB/update",
				)
				b.ReportMetric(
					float64(stats.DeviceCommits-base.DeviceCommits)/updates,
					"devicePhases/update",
				)
				b.ReportMetric(
					float64(stats.MaterializationBarriers-
						base.MaterializationBarriers)/updates,
					"materializationBarriers/update",
				)
				b.ReportMetric(
					float64(stats.MaterializationJournalBytes-
						base.MaterializationJournalBytes)/updates,
					"journalB/update",
				)
				b.ReportMetric(
					float64(stats.MaterializationTargetBytes-
						base.MaterializationTargetBytes)/updates,
					"targetB/update",
				)
				b.ReportMetric(
					float64(finalFile.Size()-baseFile.Size())/updates,
					"growthB/update",
				)
				b.ReportMetric(
					float64(stats.MaterializationUpdates-
						base.MaterializationUpdates)/updates,
					"materialized/update",
				)
				b.ReportMetric(
					float64(stats.MaterializationFallbacks-
						base.MaterializationFallbacks)/updates,
					"fallback/update",
				)
			})
		}
	}
}
