package storeio

import (
	"fmt"
	"os"
	"testing"
)

func TestBuildPrimaryGraphTemplateDiskSaving(t *testing.T) {
	const count = 8000
	records := make([]PrimaryGraphRecord, count)
	for at := range records {
		records[at] = PrimaryGraphRecord{
			Key: []byte(fmt.Sprintf("primary-key-%09d", at)),
			Value: []byte(fmt.Sprintf(
				`{"id":%d,"kind":"document","group":%d,"active":%t,`+
					`"tier":"standard","region":"eu-west-1","name":"row %d"}`,
				at, at%997, at%3 == 0, at)),
		}
	}
	build := func(policy ...PrimaryLeafClassPolicy) (uint64, PrimaryGraphBuildStats) {
		file, err := os.CreateTemp(t.TempDir(), "tc-disk-*")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		const maxPages = 4096
		committer, err := NewCommitter(file, DeviceOptions{
			Backend: BackendPortable, BufferCount: maxPages + 2,
			BufferSize: GlobalTabletCatalogRootBytes,
		}, CommitterOptions{QueueSlots: 2, MaxPagesPerBatch: maxPages, GroupLimit: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer committer.Close()
		layout, _ := MutableStoreLayout(format0PageSize)
		tx, err := BeginWriteTransaction(committer, nil, maxPages, WriteTransactionOptions{
			StoreID: format0StoreID, Generation: 1, PageSize: format0PageSize,
			FileEnd: layout.DataStart, NextLogicalID: PrimaryFirstDynamicLogicalID,
		})
		if err != nil {
			t.Fatal(err)
		}
		root, stats, err := BuildPrimaryGraphWithStats(tx, records, policy...)
		if err != nil {
			t.Fatal(err)
		}
		st := StateRoot{
			StoreID: format0StoreID, Generation: 1, PageSize: format0PageSize,
			MaxPageSize: GlobalTabletCatalogRootBytes, NextLogicalID: tx.NextLogicalID(),
			ChunkDocuments: 64, PrimaryRoot: root,
		}
		if err := tx.PublishInline(st, InlineFreeDelta{}); err != nil {
			t.Fatal(err)
		}
		if err := committer.Wait(1); err != nil {
			t.Fatal(err)
		}
		return tx.FileEnd(), stats
	}
	tcEnd, tcStats := build()
	rawEnd, rawStats := build(PrimaryLeafWide)
	t.Logf("adaptive(TC): fileEnd=%d leaves narrow=%d wide=%d template=%d",
		tcEnd, tcStats.LeavesByClass[1], tcStats.LeavesByClass[2], tcStats.LeavesByClass[3])
	t.Logf("raw(wide):    fileEnd=%d leaves narrow=%d wide=%d template=%d",
		rawEnd, rawStats.LeavesByClass[1], rawStats.LeavesByClass[2], rawStats.LeavesByClass[3])
	t.Logf("disk saving TC vs raw-wide = %.1f%% (%d -> %d bytes)",
		100*float64(rawEnd-tcEnd)/float64(rawEnd), rawEnd, tcEnd)
	if tcStats.LeavesByClass[CommonPrimaryLeafTemplate] == 0 {
		t.Fatal("template class was not selected for the redundant corpus")
	}
	if tcEnd >= rawEnd {
		t.Fatalf("template build not smaller: TC=%d raw=%d", tcEnd, rawEnd)
	}
}
