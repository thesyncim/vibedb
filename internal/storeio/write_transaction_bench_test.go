package storeio

import (
	"os"
	"testing"
)

func newWriteTransactionBenchCommitter(b *testing.B, pageSize int) *Committer {
	b.Helper()
	device := newMaterializationRecordingDevice(8, pageSize)
	file, err := os.CreateTemp(b.TempDir(), "write-transaction")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = file.Close() })
	committer, err := newCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 8, BufferSize: pageSize,
	}, CommitterOptions{
		QueueSlots: 4, MaxPagesPerBatch: 2, GroupLimit: 4,
	}, func(*os.File, DeviceOptions) (Device, error) { return device, nil })
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = committer.Close() })
	return committer
}

// BenchmarkWriteTransactionAdmission protects the ordinary COW transaction
// path from regressions introduced by optional transaction capabilities.
func BenchmarkWriteTransactionAdmission(b *testing.B) {
	pageSize := max(os.Getpagesize(), InlineSuperblockSize)
	options := WriteTransactionOptions{
		StoreID: testStoreID, Generation: 2,
		PageSize: uint32(pageSize),
		FileEnd:  uint64(8 * pageSize), NextLogicalID: 8,
	}
	b.Run("begin-abort", func(b *testing.B) {
		committer := newWriteTransactionBenchCommitter(b, pageSize)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			tx, err := BeginWriteTransaction(committer, nil, 0, options)
			if err != nil {
				b.Fatal(err)
			}
			if err := tx.Abort(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("allocate-abort", func(b *testing.B) {
		committer := newWriteTransactionBenchCommitter(b, pageSize)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			tx, err := BeginWriteTransaction(committer, nil, 1, options)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := tx.Allocate(
				PageChunkDirectory, uint32(pageSize), 0,
			); err != nil {
				b.Fatal(err)
			}
			if err := tx.Abort(); err != nil {
				b.Fatal(err)
			}
		}
	})
	template := make([]byte, pageSize)
	if _, err := InitPage(template, PageHeader{
		StoreID: testStoreID, Generation: 2, LogicalID: 8,
		PageSize: uint32(pageSize), Kind: PageChunkDirectory,
	}); err != nil {
		b.Fatal(err)
	}
	if _, err := SealPage(template); err != nil {
		b.Fatal(err)
	}
	b.Run("stage-abort", func(b *testing.B) {
		committer := newWriteTransactionBenchCommitter(b, pageSize)
		b.ReportAllocs()
		b.SetBytes(int64(pageSize))
		b.ResetTimer()
		for b.Loop() {
			tx, err := BeginWriteTransaction(committer, nil, 1, options)
			if err != nil {
				b.Fatal(err)
			}
			page, err := tx.Allocate(
				PageChunkDirectory, uint32(pageSize), 0,
			)
			if err != nil {
				b.Fatal(err)
			}
			copy(page.Bytes(), template)
			if err := page.Stage(); err != nil {
				b.Fatal(err)
			}
			if err := tx.Abort(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
