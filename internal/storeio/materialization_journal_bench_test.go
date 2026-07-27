package storeio

import (
	"crypto/sha256"
	"testing"
)

func BenchmarkMaterializationJournalEncodeTwoTinyPages(b *testing.B) {
	fixture := newMaterializationJournalFixture(b, 1000)
	slot := make([]byte, MaterializationJournalSize)
	b.ReportAllocs()
	b.SetBytes(MaterializationJournalSize)
	b.ReportMetric(float64(2048), "undo-B/op")
	b.ResetTimer()
	for range b.N {
		if _, err := EncodeMaterializationJournal(slot, fixture.header, fixture.targets, fixture.patches); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMaterializationJournalOpenTwoTinyPages(b *testing.B) {
	fixture := newMaterializationJournalFixture(b, 1001)
	slot := encodeMaterializationFixture(b, fixture)
	b.ReportAllocs()
	b.SetBytes(MaterializationJournalSize)
	b.ResetTimer()
	for range b.N {
		if _, err := OpenMaterializationJournal(slot); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMaterializationTargetBuildTinyPageUpdate(b *testing.B) {
	fixture := newMaterializationJournalFixture(b, 1004)
	b.SetBytes(int64(len(fixture.before[0])))
	b.ReportAllocs()
	var target MaterializationTarget
	for b.Loop() {
		var err error
		target, err = BuildMaterializationTarget(
			fixture.header, fixture.targets[0].Ref,
			fixture.before[0], fixture.after[0], fixture.patches, 0,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	materializationJournalBenchmarkChecksum =
		uint64(target.BeforeChecksum) | uint64(target.AfterPatchDigest[0])<<32
}

func BenchmarkMaterializationValidateAfterImageTinyPageUpdate(b *testing.B) {
	fixture := newMaterializationJournalFixture(b, 1005)
	view, err := OpenMaterializationJournal(encodeMaterializationFixture(b, fixture))
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(fixture.after[0])))
	b.ReportAllocs()
	for b.Loop() {
		if err := view.ValidateAfterImage(0, fixture.after[0]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMaterializationJournalRollbackTinyPageUpdate(b *testing.B) {
	fixture := newMaterializationJournalFixture(b, 1002)
	view, err := OpenMaterializationJournal(encodeMaterializationFixture(b, fixture))
	if err != nil {
		b.Fatal(err)
	}
	page := make([]byte, len(fixture.before[0]))
	b.ReportAllocs()
	b.SetBytes(int64(len(page)))
	b.ReportMetric(float64(len(fixture.patches[0].Data)+len(fixture.patches[1].Data)), "undo-B/op")
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		copy(page, fixture.after[0])
		b.StartTimer()
		if _, err := view.Rollback(0, page); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMaterializationJournalCommittedRootNoPageRead(b *testing.B) {
	fixture := newMaterializationJournalFixture(b, 1003)
	view, err := OpenMaterializationJournal(encodeMaterializationFixture(b, fixture))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := view.RecoverTarget(fixture.header.TargetGeneration, -1, nil)
		if err != nil || result != MaterializationRollbackNotNeeded {
			b.Fatalf("RecoverTarget=(%d,%v)", result, err)
		}
	}
}

func BenchmarkMaterializationAfterImageDigest64K(b *testing.B) {
	page := make([]byte, 64<<10)
	for index := range page {
		page[index] = byte(index*29 + 17)
	}
	b.Run("CRC32C", func(b *testing.B) {
		b.SetBytes(int64(len(page)))
		b.ReportAllocs()
		var checksum uint32
		for b.Loop() {
			checksum = PageChecksum(page)
		}
		materializationJournalBenchmarkChecksum = uint64(checksum)
	})
	b.Run("SHA256", func(b *testing.B) {
		b.SetBytes(int64(len(page)))
		b.ReportAllocs()
		var digest [sha256.Size]byte
		for b.Loop() {
			digest = sha256.Sum256(page)
		}
		materializationJournalBenchmarkChecksum =
			uint64(digest[0]) | uint64(digest[sha256.Size-1])<<8
	})
}

var materializationJournalBenchmarkChecksum uint64
