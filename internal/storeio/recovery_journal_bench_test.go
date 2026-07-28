package storeio

import (
	"os"
	"path/filepath"
	"testing"
)

// BenchmarkRecoveryJournalAppendSync measures the recovery journal's
// acknowledgement primitive: one bounded record append plus one sync on the
// journal file alone. It is the lower bound of a DurabilitySync in-place
// acknowledgement once the journal lane replaces the full copy-on-write chain
// and its two ordered barriers.
//
// The two strengths isolate the sync floor from everything else:
//
//   - PowerSafe issues the F_FULLFSYNC-class barrier (the DurabilitySync
//     default on Darwin). Its cost is dominated by the drive-cache drain, on
//     the order of milliseconds on an Apple internal SSD, and that floor is the
//     same one SQLite's power-safe lane pays.
//   - Filesystem issues the ordinary fdatasync-class barrier. It survives
//     process failure without the device-cache drain and exposes the
//     ordinary-lane cost, on the order of tens of microseconds.
//
// Device bytes per acknowledgement is reported as a custom metric: exactly one
// sector-padded record reaches the device, independent of the store's page
// geometry, versus the multi-kilobyte page-COW-plus-root a per-mutation
// publication writes.
func BenchmarkRecoveryJournalAppendSync(b *testing.B) {
	value := make([]byte, 64)
	key := []byte("benchmark-key")
	for _, strength := range []struct {
		name      string
		powerSafe bool
	}{
		{"PowerSafe", true},
		{"Filesystem", false},
	} {
		b.Run(strength.name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "bench.rjournal")
			file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer file.Close()
			// A generous capacity so recycles never enter the measured loop.
			rj, err := CreateRecoveryJournal(file, RecoveryJournalHeader{
				StoreID:        [16]byte{1},
				JournalID:      [16]byte{2},
				PageSize:       4096,
				SectorSize:     RecoveryJournalMinSectorSize,
				BaseGeneration: 1,
				Capacity:       uint64(b.N+8) * uint64(recoveryRecordPadded(RecoveryJournalMinSectorSize, len(key), len(value))),
			})
			if err != nil {
				b.Fatalf("create: %v", err)
			}
			padded := recoveryRecordPadded(RecoveryJournalMinSectorSize, len(key), len(value))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := rj.Append(recoveryRecordKindPut, uint64(i+2), key, value); err != nil {
					b.Fatalf("append: %v", err)
				}
				if err := rj.Sync(strength.powerSafe); err != nil {
					b.Fatalf("sync: %v", err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(padded), "device-bytes/ack")
		})
	}
}
