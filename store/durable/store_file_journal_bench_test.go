package durable

import (
	"fmt"
	"os"
	"path/filepath"
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
			value := func(i int) []byte {
				return []byte(fmt.Sprintf(`{"n":"%016d"}`, i))
			}
			for i := 0; i < 3; i++ {
				if _, err := coll.Put(key, value(i)); err != nil {
					b.Fatalf("warmup put: %v", err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := coll.Put(key, value(i)); err != nil {
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
