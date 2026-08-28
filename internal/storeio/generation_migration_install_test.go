package storeio

import (
	"fmt"
	"os"
	"testing"
)

func TestPublishStagedStateConditionalInstallsFsyncedPrimaryRoot(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "staged-install-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	layout, err := MutableStoreLayout(format0PageSize)
	if err != nil {
		t.Fatal(err)
	}
	committer, err := NewCommitter(
		file,
		DeviceOptions{
			Backend: BackendPortable, BufferCount: 32,
			BufferSize: GlobalTabletCatalogRootBytes,
		},
		CommitterOptions{QueueSlots: 4, MaxPagesPerBatch: 16, GroupLimit: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()
	records := []PrimaryGraphRecord{{Key: "key", Value: `{"v":1}`}}
	tx, err := BeginWriteTransaction(
		committer, nil, 8,
		WriteTransactionOptions{
			StoreID: testStoreID, Generation: 1, PageSize: format0PageSize,
			FileEnd: layout.DataStart, NextLogicalID: PrimaryFirstDynamicLogicalID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	initialRoot, err := BuildPrimaryGraph(tx, records)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := tx.ReserveUnrootedGeneration(32<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	current := StateRoot{
		StoreID: testStoreID, Generation: 1, PageSize: format0PageSize,
		DocumentCount: 1, NextLogicalID: tx.NextLogicalID(),
		MaxPageSize:      CommonPrimaryLeafMaxExtentBytes,
		MaxKeyBytes:      CommonPrimaryLeafMaxKeyBytes,
		InlineValueBytes: 1 << 20, MaxDocumentBytes: 16 << 20,
		PrimaryRoot: initialRoot,
	}
	if err := tx.PublishInline(current, InlineFreeDelta{}); err != nil {
		t.Fatal(err)
	}
	if err := committer.Wait(1); err != nil {
		t.Fatal(err)
	}

	writer, err := NewUnrootedGenerationWriter(
		file, reservation, testStoreID, 2, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := NewUnrootedPrimaryGraphSink(
		writer, testStoreID, 2, reservation.FirstLogicalID,
		reservation.Offset+reservation.Length, make([]byte, 512<<10),
	)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := NewPrimaryGraphStreamBuilder(sink, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.StageWindow(records, nil); err != nil {
		t.Fatal(err)
	}
	targetPrimary, err := stream.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatal(err)
	}

	install, err := BeginWriteTransaction(
		committer, nil, 1,
		WriteTransactionOptions{
			StoreID: testStoreID, Generation: 2, PageSize: format0PageSize,
			FileEnd:       reservation.Offset + reservation.Length,
			NextLogicalID: current.NextLogicalID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	target := current
	target.Generation = 2
	target.PrimaryRoot = targetPrimary
	descriptor, err := EncodePublicationDescriptor(make([]byte, 4096), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := PublishStagedStateConditional(
		install, current, target, InlineFreeDelta{}, descriptor,
	); err != nil {
		t.Fatal(err)
	}
	if err := committer.Wait(2); err != nil {
		t.Fatal(err)
	}
	first, second := make([]byte, format0PageSize), make([]byte, format0PageSize)
	if _, err := file.ReadAt(first, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReadAt(second, int64(format0PageSize)); err != nil {
		t.Fatal(err)
	}
	selected, _, err := SelectInlineSuperblock(first, second)
	if err != nil {
		t.Fatal(err)
	}
	if selected.State.Generation != 2 || selected.State.PrimaryRoot != targetPrimary {
		t.Fatalf("selected staged root = %+v", selected.State)
	}
	t.Logf("installed staged root %s at generation %d", fmt.Sprint(targetPrimary), 2)
}
