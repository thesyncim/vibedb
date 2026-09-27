package storeio

import (
	"os"
	"path/filepath"
	"testing"
)

// BenchmarkTxnMarkerEntryCurrent compares the live cached-identity check with
// the prior descriptor-Stat implementation. Marker creation and all
// filesystem setup remain outside the timed loops.
func BenchmarkTxnMarkerEntryCurrent(b *testing.B) {
	marker, _ := createTestTxnMarker(b, 8*TxnMarkerMinSectorSize)
	defer marker.Close()
	if current, err := marker.EntryCurrent(); err != nil || !current {
		b.Fatalf("warm EntryCurrent = %t, %v", current, err)
	}
	b.Run("cached-identity", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			current, err := marker.EntryCurrent()
			if err != nil || !current {
				b.Fatalf("EntryCurrent = %t, %v", current, err)
			}
		}
	})
	b.Run("descriptor-stat-control", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			current, err := txnMarkerEntryCurrentWithDescriptorStat(marker)
			if err != nil || !current {
				b.Fatalf("descriptor-stat control = %t, %v", current, err)
			}
		}
	})
}

func txnMarkerEntryCurrentWithDescriptorStat(marker *TxnMarker) (bool, error) {
	if marker == nil || marker.file == nil || marker.root == nil {
		return false, ErrInvalidWrite
	}
	fileInfo, err := marker.file.Stat()
	if err != nil {
		return false, err
	}
	entryInfo, err := marker.root.Lstat(filepath.Base(marker.path))
	if err != nil {
		return false, err
	}
	return entryInfo.Mode().IsRegular() && os.SameFile(fileInfo, entryInfo), nil
}
