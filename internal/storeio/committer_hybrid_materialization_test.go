package storeio

import (
	"bytes"
	"errors"
	"os"
	"slices"
	"testing"
)

func TestCommitterHybridMaterializationMergesFullPagesAndPatches(t *testing.T) {
	pageSize := max(os.Getpagesize(), InlineSuperblockSize)
	device := newMaterializationRecordingDevice(10, pageSize)
	committer := newMaterializationTestCommitter(t, device, CommitterOptions{
		QueueSlots: 4, MaxPagesPerBatch: 4, GroupLimit: 4,
	})
	fixture := newCommitterMaterializationFixture(t, uint32(pageSize), 2, 1)
	batch, err := committer.beginHybridMaterialized(1, len(fixture.patches))
	if err != nil {
		t.Fatal(err)
	}
	abort := true
	defer func() {
		if abort {
			_ = batch.Abort()
		}
	}()
	journal, err := batch.MaterializationJournalBuffer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeMaterializationJournal(
		journal, fixture.header,
		[]MaterializationTarget{fixture.target}, fixture.patches,
	); err != nil {
		t.Fatal(err)
	}
	if err := batch.SealMaterializationJournal(); err != nil {
		t.Fatal(err)
	}
	if err := batch.StageMaterializationTarget(0, fixture.after); err != nil {
		t.Fatal(err)
	}

	fullOffset := fixture.ref.Offset - uint64(pageSize)
	full, err := batch.materializationPageBuffer(0)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := InitPage(full, PageHeader{
		StoreID: testStoreID, Generation: 2, LogicalID: 8,
		PageSize: uint32(pageSize), PayloadLength: 32,
		Kind: PageOverflow,
	})
	if err != nil {
		t.Fatal(err)
	}
	for rank := range payload {
		payload[rank] = byte(rank + 1)
	}
	if _, err := SealPage(full[:pageSize]); err != nil {
		t.Fatal(err)
	}
	if err := batch.stageMaterializationPage(
		0, int64(fullOffset), pageSize, PageOverflow,
	); err != nil {
		t.Fatal(err)
	}
	root := InlineSuperblock{
		StoreID: testStoreID, Generation: 2,
		FileEnd:  fixture.ref.Offset + uint64(fixture.ref.Length),
		PageSize: uint32(pageSize),
		State: StateRoot{
			StoreID: testStoreID, Generation: 2,
			PageSize: uint32(pageSize), MaxPageSize: uint32(pageSize), NextLogicalID: 9,
		},
	}
	if err := batch.SetInlineSuperblock(root); err != nil {
		t.Fatal(err)
	}
	if err := batch.Publish(2); err != nil {
		t.Fatal(err)
	}
	abort = false
	if err := committer.Wait(2); err != nil {
		t.Fatal(err)
	}

	calls := device.snapshot()
	if len(calls) != 3 ||
		calls[0].phase != "journal" ||
		calls[1].phase != "targets" ||
		calls[2].phase != "root" {
		t.Fatalf("hybrid phases = %+v", calls)
	}
	data := calls[1]
	if len(data.pages) != len(fixture.patches)+1 {
		t.Fatalf("hybrid data writes = %d, want %d", len(data.pages), len(fixture.patches)+1)
	}
	if data.pages[0].Offset != int64(fullOffset) ||
		!bytes.Equal(data.pageData[0], full[:pageSize]) {
		t.Fatalf("full page was not first in merged physical order: %+v", data.pages)
	}
	for rank := 1; rank < len(data.pages); rank++ {
		previous := data.pages[rank-1]
		if data.pages[rank].Offset <
			previous.Offset+int64(previous.Length) {
			t.Fatalf("hybrid data writes overlap or are unordered: %+v", data.pages)
		}
	}
	stats := committer.Stats()
	if stats.MaterializationTargetBytes !=
		uint64(len(fixture.patches)*int(fixture.header.SectorSize)) ||
		stats.MaterializationFullWriteBytes != uint64(pageSize) ||
		stats.MaterializationBarriers != 3 ||
		stats.DeviceBytes != uint64(MaterializationJournalSize+
			len(fixture.patches)*int(fixture.header.SectorSize)+2*pageSize) {
		t.Fatalf("hybrid materialization stats = %+v", stats)
	}
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitterHybridMaterializationRejectsUnjournaledOldPage(t *testing.T) {
	pageSize := max(os.Getpagesize(), InlineSuperblockSize)
	device := newMaterializationRecordingDevice(10, pageSize)
	committer := newMaterializationTestCommitter(t, device, CommitterOptions{
		QueueSlots: 4, MaxPagesPerBatch: 4, GroupLimit: 4,
	})
	fixture := newCommitterMaterializationFixture(t, uint32(pageSize), 2, 1)
	batch, err := committer.beginHybridMaterialized(1, len(fixture.patches))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = batch.Abort() }()
	journal, err := batch.MaterializationJournalBuffer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeMaterializationJournal(
		journal, fixture.header,
		[]MaterializationTarget{fixture.target}, fixture.patches,
	); err != nil {
		t.Fatal(err)
	}
	if err := batch.SealMaterializationJournal(); err != nil {
		t.Fatal(err)
	}
	if err := batch.StageMaterializationTarget(0, fixture.after); err != nil {
		t.Fatal(err)
	}
	full, err := batch.materializationPageBuffer(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := InitPage(full, PageHeader{
		StoreID: testStoreID,
		// A full write born before the publication generation is an
		// unjournaled canonical overwrite and must be rejected.
		Generation: 1, LogicalID: 8,
		PageSize: uint32(pageSize), Kind: PageOverflow,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := SealPage(full[:pageSize]); err != nil {
		t.Fatal(err)
	}
	if err := batch.stageMaterializationPage(
		0, int64(fixture.ref.Offset-uint64(pageSize)), pageSize,
		PageOverflow,
	); err != nil {
		t.Fatal(err)
	}
	if err := batch.SetInlineSuperblock(InlineSuperblock{
		StoreID: testStoreID, Generation: 2,
		FileEnd:  fixture.ref.Offset + uint64(fixture.ref.Length),
		PageSize: uint32(pageSize),
		State: StateRoot{
			StoreID: testStoreID, Generation: 2,
			PageSize: uint32(pageSize), MaxPageSize: uint32(pageSize), NextLogicalID: 9,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Publish(2); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("Publish(unjournaled old page) = %v, want %v", err, ErrInvalidWrite)
	}
	if len(device.snapshot()) != 0 {
		t.Fatal("invalid hybrid batch reached the device")
	}
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHybridWriteTransactionPublishesCOWAndCanonicalPatchTogether(t *testing.T) {
	pageSize := max(os.Getpagesize(), InlineSuperblockSize)
	device := newMaterializationRecordingDevice(12, pageSize)
	committer := newMaterializationTestCommitter(t, device, CommitterOptions{
		QueueSlots: 4, MaxPagesPerBatch: 5, GroupLimit: 4,
	})
	fixture := newCommitterMaterializationFixture(t, uint32(pageSize), 2, 1)
	tx, err := BeginHybridWriteTransaction(
		committer, nil, 2, len(fixture.patches),
		WriteTransactionOptions{
			StoreID: testStoreID, Generation: 2,
			PageSize:      uint32(pageSize),
			FileEnd:       fixture.ref.Offset + uint64(fixture.ref.Length),
			NextLogicalID: 8,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	abort := true
	defer func() {
		if abort {
			_ = tx.Abort()
		}
	}()
	sequence, err := tx.MaterializationSequence()
	if err != nil || sequence != 1 {
		t.Fatalf("hybrid sequence = %d, %v", sequence, err)
	}
	journal, err := tx.MaterializationJournalBuffer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeMaterializationJournal(
		journal, fixture.header,
		[]MaterializationTarget{fixture.target}, fixture.patches,
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.SealMaterializationJournal(); err != nil {
		t.Fatal(err)
	}
	page, err := tx.Allocate(PageOverflow, uint32(pageSize), 0)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := InitPage(page.Bytes(), PageHeader{
		StoreID: testStoreID, Generation: 2,
		LogicalID: page.Ref().LogicalID,
		PageSize:  uint32(pageSize), PayloadLength: 16,
		Kind: PageOverflow,
	})
	if err != nil {
		t.Fatal(err)
	}
	copy(payload, "hybrid-cow-page")
	if _, err := SealPage(page.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := page.Stage(); err != nil {
		t.Fatal(err)
	}
	if err := tx.StageBuiltMaterializationTarget(
		0, fixture.target, fixture.after,
	); err != nil {
		t.Fatal(err)
	}
	state := StateRoot{
		StoreID: testStoreID, Generation: 2,
		PageSize: uint32(pageSize), MaxPageSize: uint32(pageSize),
		NextLogicalID: tx.NextLogicalID(),
	}
	if err := tx.PublishInline(state, InlineFreeDelta{}); err != nil {
		t.Fatal(err)
	}
	abort = false
	if err := committer.Wait(2); err != nil {
		t.Fatal(err)
	}
	calls := device.snapshot()
	if len(calls) != 3 || len(calls[1].pages) != len(fixture.patches)+1 {
		t.Fatalf("hybrid transaction device calls = %+v", calls)
	}
	foundFull := false
	for rank, write := range calls[1].pages {
		if write.Offset != int64(page.Ref().Offset) {
			continue
		}
		foundFull = bytes.Equal(calls[1].pageData[rank], page.Bytes())
	}
	if !foundFull {
		t.Fatal("hybrid transaction did not publish its immutable page")
	}
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHybridWriteTransactionFailedPublishCannotConsumeCapacity(t *testing.T) {
	pageSize := uint32(max(os.Getpagesize(), InlineSuperblockSize))
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	device := newMaterializationRecordingDevice(12, int(pageSize))
	committer := newMaterializationTestCommitter(t, device, CommitterOptions{
		QueueSlots: 4, MaxPagesPerBatch: 4, GroupLimit: 2,
	})
	reusable := []FreeExtent{
		{Offset: layout.DataStart, Length: uint64(pageSize), RetiredGeneration: 1},
		{Offset: layout.DataStart + uint64(pageSize), Length: uint64(pageSize), RetiredGeneration: 1},
	}
	initialReusable := slices.Clone(reusable)
	tx, err := BeginHybridWriteTransaction(
		committer, nil, 2, 1,
		WriteTransactionOptions{
			StoreID: testStoreID, Generation: 2,
			PageSize:      pageSize,
			FileEnd:       layout.DataStart + 3*uint64(pageSize),
			NextLogicalID: 8,
			Reusable:      reusable,
			ReuseJournal:  make([]ReuseEdit, 0, 2),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	page, err := tx.Allocate(PageOverflow, pageSize, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := InitPage(page.Bytes(), PageHeader{
		StoreID: testStoreID, Generation: 2,
		LogicalID: page.Ref().LogicalID, PageSize: pageSize,
		Kind: PageOverflow,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := SealPage(page.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := page.Stage(); err != nil {
		t.Fatal(err)
	}
	invalidState := StateRoot{
		StoreID: testStoreID, Generation: 2, PageSize: pageSize,
		MaxPageSize:   pageSize,
		NextLogicalID: tx.NextLogicalID(),
		// The transaction passes its cheap identity checks, shrinks its
		// reserved buffers, then the root codec rejects this option bit.
		Options: 1 << 31,
	}
	if err := tx.PublishInline(
		invalidState, InlineFreeDelta{},
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("PublishInline(invalid state) = %v, want %v", err, ErrInvalidWrite)
	}
	wantNextID, wantFileEnd := tx.NextLogicalID(), tx.FileEnd()
	wantReusable := slices.Clone(reusable)
	wantEdits := slices.Clone(tx.ReuseEdits())
	if _, err := tx.Allocate(
		PageOverflow, pageSize, 0,
	); !errors.Is(err, ErrTooManyPages) {
		t.Fatalf("Allocate after failed publication = %v, want %v", err, ErrTooManyPages)
	}
	if tx.NextLogicalID() != wantNextID || tx.FileEnd() != wantFileEnd ||
		!slices.Equal(reusable, wantReusable) ||
		!slices.Equal(tx.ReuseEdits(), wantEdits) {
		t.Fatalf(
			"failed retry mutated transaction: id=%d/%d end=%d/%d reusable=%+v/%+v edits=%+v/%+v",
			tx.NextLogicalID(), wantNextID, tx.FileEnd(), wantFileEnd,
			reusable, wantReusable, tx.ReuseEdits(), wantEdits,
		)
	}
	if err := tx.Abort(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(reusable, initialReusable) {
		t.Fatalf("Abort reusable = %+v, want %+v", reusable, initialReusable)
	}
	if len(device.snapshot()) != 0 {
		t.Fatal("failed hybrid publication reached the device")
	}
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
}
