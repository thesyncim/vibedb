package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkRecoveryJournalStorePut measures the store-level acknowledgement cost
// of a same-size in-place update on a buffered-visible primary collection with
// the recovery journal enabled. Each iteration applies the patch to the
// canonical frame and makes it durable through one bounded journal append plus
// one sync — no page copy-on-write, no root fence. It is the journal primitive's
// append+sync floor plus the frame apply and router bookkeeping the store adds.
//
// The two strengths isolate the sync floor:
//
//   - PowerSafe issues the F_FULLFSYNC-class barrier (the drive-cache drain,
//     milliseconds on an Apple internal SSD) — the same floor a power-safe
//     WAL pays.
//   - Filesystem issues the ordinary fdatasync-class barrier (tens of
//     microseconds), the ordinary-lane cost.
//
// The Journaled=false control run is the volatile buffered acknowledgement with
// no journal: the delta is exactly the durability the journal buys.
func BenchmarkRecoveryJournalStorePut(b *testing.B) {
	for _, cfg := range []struct {
		name     string
		journal  bool
		strength CheckpointStrength
	}{
		{"Journaled=false", false, CheckpointPowerSafe},
		{"Journaled=true/PowerSafe", true, CheckpointPowerSafe},
		{"Journaled=true/Filesystem", true, CheckpointFilesystem},
	} {
		b.Run(cfg.name, func(b *testing.B) {
			options := journalTestOptions(cfg.strength)
			options.RecoveryJournal = cfg.journal

			dir := b.TempDir()
			path := filepath.Join(dir, "bench.vibe")
			file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := CreateFromPrimary(seedPrimaryCollection(b), file, options); err != nil {
				b.Fatalf("CreateFromPrimary: %v", err)
			}
			_ = file.Close()
			file, err = os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				b.Fatal(err)
			}
			coll, err := Open(file, options)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}

			// A fixed-width value so every later update is a same-size in-place
			// patch that appends exactly one record. The first two touches
			// establish the frame and its first-touch tracking.
			const key = "bench-key"
			keyB := []byte(key)
			value := func(i int) []byte {
				return []byte(fmt.Sprintf(`{"n":"%016d"}`, i))
			}
			for i := 0; i < 3; i++ {
				if _, err := coll.Put(keyB, value(i)); err != nil {
					b.Fatalf("warmup put: %v", err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := coll.Put(keyB, value(i)); err != nil {
					b.Fatalf("put: %v", err)
				}
			}
			b.StopTimer()

			stats := coll.Stats()
			b.ReportMetric(float64(stats.JournalAcks), "journal-acks")
			b.ReportMetric(float64(stats.ChainAcks), "chain-acks")
			_ = coll.Close()
			_ = file.Close()
		})
	}
}

// BenchmarkRecoveryJournalStorePutConcurrent measures the buffered-journal ack
// path under 1 vs N concurrent goroutines, isolating the phase-1 group-commit
// amortization: each goroutine appends its own redo record under the writer, but
// concurrent callers share one journal sync. Per-op time should fall well below
// the single-goroutine sync floor as the group fans out. It reports the achieved
// group size (journal-acks / journal-syncs) so a run can see the amortization
// directly, at both sync strengths.
func BenchmarkRecoveryJournalStorePutConcurrent(b *testing.B) {
	for _, strength := range []struct {
		name     string
		strength CheckpointStrength
	}{
		{"Filesystem", CheckpointFilesystem},
		{"PowerSafe", CheckpointPowerSafe},
	} {
		for _, clients := range []int{1, 8} {
			b.Run(fmt.Sprintf("%s/clients=%d", strength.name, clients), func(b *testing.B) {
				options := journalTestOptions(strength.strength)
				options.RecoveryJournal = true

				dir := b.TempDir()
				path := filepath.Join(dir, "bench.vibe")
				file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := CreateFromPrimary(seedPrimaryCollection(b), file, options); err != nil {
					b.Fatalf("CreateFromPrimary: %v", err)
				}
				_ = file.Close()
				file, err = os.OpenFile(path, os.O_RDWR, 0o600)
				if err != nil {
					b.Fatal(err)
				}
				coll, err := Open(file, options)
				if err != nil {
					b.Fatalf("Open: %v", err)
				}

				// Each client owns a distinct key so their in-place patches never
				// contend on the same leaf byte range; the only thing they share is
				// the journal fence, which is exactly what this measures.
				value := func(i int) []byte {
					return []byte(fmt.Sprintf(`{"n":"%016d"}`, i))
				}
				for c := 0; c < clients; c++ {
					key := []byte(fmt.Sprintf("bench-key-%03d", c))
					for i := 0; i < 3; i++ {
						if _, err := coll.Put(key, value(i)); err != nil {
							b.Fatalf("warmup put: %v", err)
						}
					}
				}

				b.ReportAllocs()
				b.ResetTimer()
				var counter atomic.Int64
				var wg sync.WaitGroup
				per := b.N / clients
				extra := b.N % clients
				for c := 0; c < clients; c++ {
					n := per
					if c < extra {
						n++
					}
					wg.Add(1)
					go func(c, n int) {
						defer wg.Done()
						key := []byte(fmt.Sprintf("bench-key-%03d", c))
						for i := 0; i < n; i++ {
							if _, err := coll.Put(key, value(int(counter.Add(1)))); err != nil {
								b.Errorf("put: %v", err)
								return
							}
						}
					}(c, n)
				}
				wg.Wait()
				b.StopTimer()

				stats := coll.Stats()
				group := 0.0
				if stats.JournalSyncs > 0 {
					group = float64(stats.JournalAcks) / float64(stats.JournalSyncs)
				}
				b.ReportMetric(group, "acks/sync")
				b.ReportMetric(float64(stats.JournalLargestGroup), "largest-group")
				_ = coll.Close()
				_ = file.Close()
			})
		}
	}
}
