package durable

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

var primaryOpenTestStoreID = [16]byte{
	'p', 'r', 'i', 'm', 'a', 'r', 'y', '-', 'o', 'p', 'e', 'n', '-', '0', '0', '1',
}

func buildPrimaryOpenTestFile(
	t testing.TB,
) (*os.File, storeio.PageRef) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "primary-open-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	const (
		pageSize = uint32(4096)
		maxPages = 64
	)
	committer, err := storeio.NewCommitter(file, storeio.DeviceOptions{
		Backend: storeio.BackendPortable, BufferCount: 128,
		BufferSize: storeio.GlobalTabletCatalogRootBytes,
	}, storeio.CommitterOptions{
		QueueSlots: 2, MaxPagesPerBatch: maxPages, GroupLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	layout, err := storeio.MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := storeio.BeginWriteTransaction(
		committer, nil, maxPages,
		storeio.WriteTransactionOptions{
			StoreID: primaryOpenTestStoreID, Generation: 1, PageSize: pageSize,
			FileEnd:       layout.DataStart,
			NextLogicalID: storeio.PrimaryFirstDynamicLogicalID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	records := make([]storeio.PrimaryGraphRecord, 1_000)
	for at := range records {
		records[at] = storeio.PrimaryGraphRecord{
			Key:   []byte{byte(at >> 8), byte(at), 1},
			Value: []byte{byte(at), 1},
		}
	}
	primaryRoot, err := storeio.BuildPrimaryGraph(tx, records)
	if err != nil {
		_ = tx.Abort()
		t.Fatal(err)
	}
	root := storeio.StateRoot{
		StoreID: primaryOpenTestStoreID, Generation: 1, PageSize: pageSize,
		MaxPageSize:   storeio.GlobalTabletCatalogRootBytes,
		NextLogicalID: tx.NextLogicalID(), ChunkDocuments: 64,
		MaxKeyBytes: 256, InlineValueBytes: 512, MaxDocumentBytes: 4 << 20,
		PrimaryRoot: primaryRoot,
	}
	if err := tx.PublishInline(root, storeio.InlineFreeDelta{}); err != nil {
		t.Fatal(err)
	}
	if err := committer.Wait(root.Generation); err != nil {
		t.Fatal(err)
	}
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
	return file, primaryRoot
}

func readPrimaryOpenTestPage(
	t testing.TB, file *os.File, ref storeio.PageRef,
) []byte {
	t.Helper()
	page := make([]byte, ref.Length)
	if _, err := file.ReadAt(page, int64(ref.Offset)); err != nil {
		t.Fatal(err)
	}
	return page
}

func writePrimaryOpenTestPage(
	t testing.TB, file *os.File, ref storeio.PageRef, page []byte,
) {
	t.Helper()
	if _, err := file.WriteAt(page, int64(ref.Offset)); err != nil {
		t.Fatal(err)
	}
}

func resealPrimaryOpenTestTrailer(image []byte, trailer int) {
	checksum := storeio.PageChecksum(image[:trailer])
	binary.LittleEndian.PutUint32(image[trailer:trailer+4], checksum)
	binary.LittleEndian.PutUint32(image[trailer+4:trailer+8], ^checksum)
}

func TestOpenValidatesPrimaryCatalog(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		file, _ := buildPrimaryOpenTestFile(t)
		collection, err := Open(file, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := collection.Close(); err != nil {
			t.Fatal(err)
		}
	})
	for name, corrupt := range map[string]func(
		testing.TB, *os.File, storeio.PageRef,
	){
		"grafted StoreID": func(t testing.TB, file *os.File, ref storeio.PageRef) {
			info, err := file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			rootPage := readPrimaryOpenTestPage(t, file, ref)
			root, err := storeio.OpenGlobalTabletCatalogNode(
				rootPage, ref, storeio.GlobalTabletCatalogBounds{
					StoreID: primaryOpenTestStoreID, SelectedRootGeneration: 1,
					FileEnd:       uint64(info.Size()),
					NextLogicalID: storeio.PrimaryFirstDynamicLogicalID,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			ref = root.Route(nil).Ref
			page := readPrimaryOpenTestPage(t, file, ref)
			page[40] ^= 1
			if _, err := storeio.SealPage(page); err != nil {
				t.Fatal(err)
			}
			writePrimaryOpenTestPage(t, file, ref, page)
		},
		"wrong-kind child": func(t testing.TB, file *os.File, ref storeio.PageRef) {
			page := readPrimaryOpenTestPage(t, file, ref)
			page[storeio.PageHeaderSize+5] = byte(storeio.PageTabletRoute)
			if _, err := storeio.SealPage(page); err != nil {
				t.Fatal(err)
			}
			writePrimaryOpenTestPage(t, file, ref, page)
		},
		"out-of-band logical ID": func(t testing.TB, file *os.File, ref storeio.PageRef) {
			page := readPrimaryOpenTestPage(t, file, ref)
			payload := page[storeio.PageHeaderSize:]
			mapBytes := int(binary.LittleEndian.Uint32(payload[12:16]))
			mapStart := storeio.PageHeaderSize +
				storeio.GlobalTabletCatalogNodePayloadHeaderBytes
			// The one-child root has no fences or common bytes. Its embedded
			// map bucket follows the 64-byte header, 514-byte accelerator,
			// and one terminal offset.
			bucketAt := mapStart + storeio.TabletAnchorMapLabHeaderSize +
				storeio.TabletAnchorMapLabAcceleratorSlots*2 + 2
			binary.LittleEndian.PutUint32(
				page[bucketAt:bucketAt+4],
				storeio.GlobalTabletCatalogMaxLeafPages,
			)
			resealPrimaryOpenTestTrailer(
				page, mapStart+mapBytes-storeio.TabletAnchorMapLabTrailerSize,
			)
			if _, err := storeio.SealPage(page); err != nil {
				t.Fatal(err)
			}
			writePrimaryOpenTestPage(t, file, ref, page)
		},
		"truncated catalog": func(t testing.TB, file *os.File, ref storeio.PageRef) {
			if err := file.Truncate(int64(ref.Offset + uint64(ref.Length) - 1)); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			file, root := buildPrimaryOpenTestFile(t)
			corrupt(t, file, root)
			if collection, err := Open(file, Options{}); err == nil {
				_ = collection.Close()
				t.Fatal("Open accepted corrupt ordered primary catalog")
			}
		})
	}
}
