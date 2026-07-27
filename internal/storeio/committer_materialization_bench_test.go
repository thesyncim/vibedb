package storeio

import (
	"os"
	"testing"
)

func newMaterializationStageBenchmark(
	b *testing.B,
) (*Batch, materializationJournalFixture) {
	b.Helper()
	const bufferCount = 8
	pageSize := max(os.Getpagesize(), InlineSuperblockSize)
	device := newMaterializationRecordingDevice(bufferCount, pageSize)
	file, err := os.CreateTemp(b.TempDir(), "materialization-stage")
	if err != nil {
		b.Fatal(err)
	}
	committer, err := newCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: bufferCount,
		BufferSize: pageSize,
	}, CommitterOptions{
		QueueSlots: 4, MaxPagesPerBatch: 4, GroupLimit: 4,
		MaterializationDamageGranule: MaterializationJournalMinSectorSize,
	}, func(*os.File, DeviceOptions) (Device, error) {
		return device, nil
	})
	if err != nil {
		_ = file.Close()
		b.Fatal(err)
	}
	fixture := newMaterializationJournalFixture(b, 1006)
	batch, err := committer.BeginMaterialized(len(fixture.patches))
	if err != nil {
		_ = committer.Close()
		_ = file.Close()
		b.Fatal(err)
	}
	journal, err := batch.MaterializationJournalBuffer()
	if err == nil {
		_, err = EncodeMaterializationJournal(
			journal, fixture.header, fixture.targets, fixture.patches,
		)
	}
	if err == nil {
		err = batch.SealMaterializationJournal()
	}
	if err != nil {
		_ = batch.Abort()
		_ = committer.Close()
		_ = file.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = batch.Abort()
		_ = committer.Close()
		_ = file.Close()
	})
	return batch, fixture
}

func BenchmarkMaterializationTargetStaging(b *testing.B) {
	b.Run("strong-journal", func(b *testing.B) {
		batch, fixture := newMaterializationStageBenchmark(b)
		b.SetBytes(int64(len(fixture.after[0])))
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if err := batch.StageMaterializationTarget(
				0, fixture.after[0],
			); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("built-fast", func(b *testing.B) {
		batch, fixture := newMaterializationStageBenchmark(b)
		b.SetBytes(int64(len(fixture.after[0])))
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if err := batch.StageBuiltMaterializationTarget(
				0, fixture.targets[0], fixture.after[0],
			); err != nil {
				b.Fatal(err)
			}
		}
	})
}
