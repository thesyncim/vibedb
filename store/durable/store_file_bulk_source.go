package durable

import (
	"fmt"
	"io"
	"math/bits"
	"os"

	"github.com/thesyncim/vibedb/store"
)

// fileStoreBulkRow identifies one live document in an in-memory heap State by
// its source chunk and stable slot. It is the row descriptor the ordered-primary
// bulk builder (CreateFromPrimary) walks; the overflow fields are retained for
// the descriptor shape but unused on the inline-only primary bulk path.
type fileStoreBulkRow struct {
	sourceChunk  uint32
	sourceSlot   uint8
	overflowBase int
	overflowN    int
}

// collectFileStoreBulkRows enumerates every live document of a heap State in
// chunk/slot order, enforcing the admission key and document bounds. It reads
// only the surviving in-memory heap store, so it is shared by the ordered
// primary bulk builder.
func collectFileStoreBulkRows(state *store.State, options normalizedFileStoreOptions) ([]fileStoreBulkRow, error) {
	if state.Count < 0 || uint64(state.Count) > uint64(^uint32(0))*uint64(options.Collection.ChunkDocuments) {
		return nil, store.ErrTooLarge
	}
	rows := make([]fileStoreBulkRow, 0, state.Count)
	var collectErr error
	state.Chunks.Each(func(chunkID uint32, chunk *store.Chunk) bool {
		for live := chunk.Live; live != 0; live &= live - 1 {
			slot := uint8(bits.TrailingZeros64(live))
			key := chunk.Key(int(slot))
			raw := chunk.Docs.RawAt(int(chunk.Ord[slot]))
			if len(key) > options.MaxKeyBytes {
				collectErr = ErrKeyTooLarge
				return false
			}
			if len(raw) > options.MaxDocumentBytes {
				collectErr = ErrDocumentTooLarge
				return false
			}
			rows = append(rows, fileStoreBulkRow{sourceChunk: chunkID, sourceSlot: slot, overflowBase: -1})
		}
		return true
	})
	if collectErr != nil {
		return nil, collectErr
	}
	if len(rows) != state.Count {
		return nil, fmt.Errorf("vibejson: collection bulk source count invariant")
	}
	return rows, nil
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
