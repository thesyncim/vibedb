package durable

import (
	"fmt"
	"io"
	"math/bits"
	"os"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson/x/byteview"
)

// collectFileStoreBulkRecords enumerates every live document of an immutable
// heap State directly into the ordered-primary record plan. Key and value bytes
// remain borrowed from state; the caller retains its bulk snapshot through graph
// construction. Building the final descriptors in this pass avoids a separate
// (chunk,slot) list and a second source lookup pass.
func collectFileStoreBulkRecords(
	state *store.State,
	options normalizedFileStoreOptions,
) ([]storeio.PrimaryGraphRecord, error) {
	if state.Count < 0 ||
		uint64(state.Count) > uint64(^uint32(0))*uint64(store.MaxChunkDocuments) {
		return nil, store.ErrTooLarge
	}
	records := make([]storeio.PrimaryGraphRecord, state.Count)
	at := 0
	var collectErr error
	state.Chunks.Each(func(_ uint32, chunk *store.Chunk) bool {
		for live := chunk.Live; live != 0; live &= live - 1 {
			slot := uint8(bits.TrailingZeros64(live))
			key := chunk.Key(int(slot))
			raw := chunk.Docs.RawAt(int(chunk.Ord[slot]))
			if len(key) > options.MaxKeyBytes ||
				len(key) > storeio.CommonPrimaryLeafMaxKeyBytes {
				collectErr = ErrKeyTooLarge
				return false
			}
			if len(raw) > options.MaxDocumentBytes {
				collectErr = ErrDocumentTooLarge
				return false
			}
			if len(raw) == 0 || len(raw) > options.InlineValueBytes {
				collectErr = ErrPrimaryCutoverUnsupported
				return false
			}
			if at >= len(records) {
				collectErr = fmt.Errorf("vibedb: collection bulk source count invariant")
				return false
			}
			records[at] = storeio.PrimaryGraphRecord{
				Key: key, Value: byteview.String(raw),
			}
			at++
		}
		return true
	})
	if collectErr != nil {
		return nil, collectErr
	}
	if at != len(records) {
		return nil, fmt.Errorf("vibedb: collection bulk source count invariant")
	}
	return records, nil
}

// writeStorePageAt writes a fully framed page at a byte offset, verifying the
// platform accepted every byte. It is the shared one-shot page writer used by
// the canonical page-catalog builder and the ordered-primary bulk builder.
func writeStorePageAt(file *os.File, page []byte, offset uint64) error {
	n, err := file.WriteAt(page, int64(offset))
	if err == nil && n != len(page) {
		err = io.ErrShortWrite
	}
	return err
}
