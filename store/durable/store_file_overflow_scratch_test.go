package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestPrimaryOverflowScratchGrowsWithObservedValue(t *testing.T) {
	for _, mode := range []DurabilityMode{DurabilitySync, DurabilityBufferedVisible} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			options := primaryBatchOverflowOptions(mode)
			options.MaxDocumentBytes = 1 << 20
			options.ResidentBytes = 32 << 20
			file, err := os.OpenFile(filepath.Join(t.TempDir(), "scratch.vibe"), os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			collection, err := Create(file, options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = collection.Close() }()
			checkCold := func() {
				t.Helper()
				if capacity := cap(collection.overflowValueScratch); capacity > storeio.CommonPrimaryLeafMaxExtentBytes {
					t.Fatalf("cold overflow scratch=%d, reserved unused document ceiling=%d", capacity, options.MaxDocumentBytes)
				}
			}
			checkCold()
			if err := collection.Close(); err != nil {
				t.Fatal(err)
			}
			collection, err = Open(file, options)
			if err != nil {
				t.Fatal(err)
			}
			checkCold()
			first := primaryBatchOverflowDocument("first", 'a', 128<<10)
			second := primaryBatchOverflowDocument("other", 'b', 128<<10)
			for _, document := range [][]byte{first, second, first} {
				if _, err := collection.Put([]byte("key"), document); err != nil {
					t.Fatal(err)
				}
				if err := collection.Flush(); err != nil {
					t.Fatal(err)
				}
				requirePrimaryBatchRaw(t, collection, "key", document)
			}
			if cap(collection.overflowValueScratch) < len(first) || cap(collection.overflowValueScratch) > options.MaxDocumentBytes {
				t.Fatalf("observed overflow scratch=%d for value=%d limit=%d", cap(collection.overflowValueScratch), len(first), options.MaxDocumentBytes)
			}
			storage := &collection.overflowValueScratch[:cap(collection.overflowValueScratch)][0]
			if _, err := collection.Put([]byte("key"), second); err != nil {
				t.Fatal(err)
			}
			if err := collection.Flush(); err != nil {
				t.Fatal(err)
			}
			if storage != &collection.overflowValueScratch[:cap(collection.overflowValueScratch)][0] {
				t.Fatal("same-size steady-state overflow replaced reusable scratch")
			}
			requirePrimaryBatchRaw(t, collection, "key", second)
		})
	}
}
