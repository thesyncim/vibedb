package storeio

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Compare the existing strict reservation protocol with the portable protocol
// on the same filesystem. Both use real file I/O and unchanged sync primitives.
func BenchmarkSealedRecoveryJournalReopen(b *testing.B) {
	for _, portable := range []bool{false, true} {
		name := "strict"
		if portable {
			name = "portable"
		}
		b.Run(name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "journal")
			file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
			if err != nil {
				b.Fatal(err)
			}
			const capacity = 1 << 20
			header := RecoveryJournalHeader{Format: RecoveryJournalFormat,
				StoreID: [16]byte{1}, JournalID: [16]byte{2}, PageSize: 4096,
				SectorSize: RecoveryJournalMinSectorSize, BaseGeneration: 1,
				Capacity: capacity, SealedCapacity: true, PortableCapacity: portable}
			journal, err := CreateRecoveryJournal(file, header)
			if err != nil {
				_ = file.Close()
				if !portable && errors.Is(err, ErrStrictAllocationUnsupported) {
					b.Skip(err)
				}
				b.Fatal(err)
			}
			if err := journal.Close(); err != nil {
				b.Fatal(err)
			}
			options := RecoveryJournalOpenOptions{SealedCapacityBytes: capacity, AllowPortableCapacity: portable}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				file, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					b.Fatal(err)
				}
				journal, err := OpenRecoveryJournalWithOptions(file, options)
				if err != nil {
					_ = file.Close()
					b.Fatal(err)
				}
				if err := journal.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSealedTxnMarkerReopen(b *testing.B) {
	for _, portable := range []bool{false, true} {
		name := "strict"
		if portable {
			name = "portable"
		}
		b.Run(name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "txn.vtm")
			options := TxnMarkerOptions{Capacity: 1 << 20, SealedCapacity: true, PortableCapacity: portable}
			marker, err := CreateTxnMarker(path, options)
			if err != nil {
				if !portable && errors.Is(err, ErrStrictAllocationUnsupported) {
					b.Skip(err)
				}
				b.Fatal(err)
			}
			if err := marker.Close(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				marker, _, err := OpenTxnMarker(path, options)
				if err != nil {
					b.Fatal(err)
				}
				if err := marker.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSealedRecoveryJournalAppendSync(b *testing.B) {
	for _, portable := range []bool{false, true} {
		name := "strict"
		if portable {
			name = "portable"
		}
		b.Run(name, func(b *testing.B) {
			file, err := os.CreateTemp(b.TempDir(), "journal")
			if err != nil {
				b.Fatal(err)
			}
			defer file.Close()
			key, value := []byte("benchmark-key"), make([]byte, 64)
			padded := recoveryRecordPadded(RecoveryJournalMinSectorSize, len(key), len(value))
			journal, err := CreateRecoveryJournal(file, RecoveryJournalHeader{
				StoreID: [16]byte{1}, JournalID: [16]byte{2}, PageSize: 4096,
				SectorSize: RecoveryJournalMinSectorSize, BaseGeneration: 1,
				Capacity: uint64(b.N+8) * uint64(padded), SealedCapacity: true, PortableCapacity: portable,
			})
			if err != nil {
				if !portable && errors.Is(err, ErrStrictAllocationUnsupported) {
					b.Skip(err)
				}
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if _, err := journal.Append(RecoveryRecordKindPut, uint64(i+2), key, value); err != nil {
					b.Fatal(err)
				}
				if err := journal.Sync(true); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			// Encoded journal bytes; filesystem metadata/COW writes are outside
			// this metric and must not be presented as device write traffic.
			b.ReportMetric(float64(padded), "journal-bytes/ack")
		})
	}
}
