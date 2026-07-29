package durable

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// Batched-commit durability for the ordered primary.
//
// The single-file page-kind crash sweep that this file once carried tore a
// commit image at every write boundary and reopened the bytes in isolation.
// That model belonged to the retired chunk store: the ordered primary recovers
// through a sibling redo journal, so an image copied without its journal is not
// a store the sweep can reopen, and the per-kind census it proved was stated in
// chunk/document/directory page kinds that no longer exist. Primary crash
// recovery is now covered directly by the primary crash matrices
// (TestFilePrimary*CrashBoundary/Matrix), the exhaustive sweep, and the
// recovery-journal suites. What remains here is the retirement-ordering and
// file-growth behaviour of the batched Update path, which is exercised through
// real Create/Open/Update cycles rather than image surgery.

// crashPage is one decoded physical page found by walking a store image.
type crashPage struct {
	offset uint64
	length uint32
	kind   storeio.PageKind
}

// walkImagePages decodes every physical page in a store image after the two
// superblock copies.
//
// It reads the file the way a forensic tool would rather than the way the store
// does: no root is consulted and no reference is followed, so a page the commit
// wrote and then failed to link is still found. That is deliberate. The point of
// this walk is to name what a commit put on disk, and a walk that started from
// the roots could only ever name what the roots already reach — which is the
// half of the question that is not in doubt.
//
// Extents are page-size aligned and every page records its own size, so the walk
// steps by the decoded size when a header is valid and by one page-size quantum
// when it is not. Free and never-written space simply fails to decode.
func walkImagePages(image []byte, pageSize, maxPageSize int) []crashPage {
	pages := make([]crashPage, 0, 64)
	for offset := testMutableDataStart(pageSize); offset < len(image); {
		end := min(offset+maxPageSize, len(image))
		header, _, err := storeio.OpenPage(image[offset:end])
		if err != nil {
			offset += pageSize
			continue
		}
		pages = append(pages, crashPage{
			offset: uint64(offset), length: header.PageSize, kind: header.Kind,
		})
		offset += int(header.PageSize)
	}
	return pages
}

// commitCrashOptions configures a collection whose ordinary commits touch every
// page kind the single-document and batched write paths can produce.
//
// The ordered primary stores values inline in its leaves; the incremental Put
// path does not spill to overflow pages the way the retired chunk store did, so
// InlineValueBytes (and the leaf page it must fit) is sized to admit every
// document this sweep writes. An index makes the exact-index tiles participate
// in the same torn commits.
func commitCrashOptions(batchDocuments int) Options {
	options := testFileStoreOptions()
	options.Collection.ChunkDocuments = 4
	options.ResidentBytes = 16 << 20
	options.BufferCount = 1024
	options.MaxRetiredExtents = 4096
	options.MaxBatchDocuments = batchDocuments
	if batchDocuments > 1 {
		// A wide batch's worst-case transaction is hundreds of pages, which no
		// fixed BufferCount from the single-document options can cover. Zero asks
		// the store to size the commit buffer from the batch width it was told.
		options.BufferCount = 0
	}
	options.InlineValueBytes = 32 << 10
	// The ordered primary does not yet support a transactional batch on an
	// indexed collection, and these retirement/growth assertions do not depend on
	// an index, so none is configured.
	return options
}

func commitCrashDocument(round, key int, padding int) []byte {
	return fmt.Appendf(nil, `{"round":%d,"key":%d,"status":%q,"score":%d.5,"padding":%q}`,
		round, key, [3]string{"active", "idle", "paused"}[(round+key)%3], key,
		strings.Repeat("x", padding))
}

func TestCollectionUpdateWritesItsRetirementsInTheSameCommit(t *testing.T) {
	options := commitCrashOptions(64)
	file, err := os.CreateTemp(t.TempDir(), "batch-retire-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	for round := range 8 {
		if err := collection.Update(func(b *WriteBatch) error {
			for i := range 6 {
				if err := b.Put([]byte(fmt.Sprintf("key-%d-%d", round, i)),
					commitCrashDocument(round, i, 100+i*300)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := collection.Flush(); err != nil {
			t.Fatal(err)
		}
		_ = freeSetFromFile(t, file.Name(), options.PageSize)
	}
	image, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	// State now lives in the fixed root. No publication may allocate the
	// standalone state-page shape that previously leaked when retirement
	// ordering was wrong.
	for _, page := range walkImagePages(image, options.PageSize, options.MaxPageSize) {
		if page.kind == storeio.PageStateRoot {
			t.Fatalf("inline-root collection allocated state page at %d", page.offset)
		}
	}
}

// Given the same content written by batched Updates in one session and across
// many close/reopen cycles, when the files are compared, then the multi-session
// file is no larger than the single-session one at all.
//
// TestFileStoreFreeSetSurvivesRestartsWithoutGrowingTheFile makes a bounded
// version of this claim for the single-document path. The batched path had no
// equivalent, and it was the one that did not hold: every batched commit
// abandoned its outgoing state root at Close, so a restart cost pages that were
// never written down.
//
// The bound here is zero rather than "the reclaimer's pending set", and that is
// the whole point of the assertion. A pending-set bound was tried first and is
// vacuous: across six sessions the reclaimer holds about 2 MiB at the Closes,
// while the bug costs 40 KiB, so the loose bound passed with the bug present.
// Once every retirement is durable in its own commit there is nothing left for a
// restart to lose — a reopen replays the whole free set, and the fenced tail
// becomes reusable again a couple of commits later — so the two files come out
// byte-identical in length. Measured: 561152 both ways with the ordering
// correct, 602112 against 561152 with it wrong.
func TestCollectionUpdateSurvivesRestartsWithoutGrowingTheFile(t *testing.T) {
	const (
		keys     = 32
		rounds   = 6
		sessions = 6
	)
	options := commitCrashOptions(64)
	write := func(collection *Collection, session int) {
		t.Helper()
		for round := range rounds {
			if err := collection.Update(func(b *WriteBatch) error {
				// The first round seeds all keys into one leaf, and a batch cannot
				// split a fresh leaf mid-fold, so the per-document padding is bounded to
				// keep the whole seed batch within a single leaf's fold budget. The
				// retirement/growth invariant this test asserts is size-independent.
				for key := range keys {
					if err := b.Put([]byte(fmt.Sprintf("key-%02d", key)),
						commitCrashDocument(session*rounds+round, key,
							40+(round*37+key*53)%110)); err != nil {
						return err
					}
				}
				for key := round % 3; key < keys; key += 3 {
					if err := b.Delete([]byte(fmt.Sprintf("key-%02d", key))); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}

	single, singleFile := openBatchCollection(t, options)
	for session := range sessions {
		write(single, session)
		// Checkpoint on the same per-session cadence the multi-session store uses
		// below, so the two differ only by the close/reopen between sessions. A
		// deferred lane's checkpoint layout depends on how often it folds, so an
		// unmatched cadence would compare compaction, not the restart cost.
		if err := single.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	singleEnd := single.Stats().FileEnd

	multiFile, err := os.CreateTemp(t.TempDir(), "batch-restart-multi-*")
	if err != nil {
		t.Fatal(err)
	}
	defer multiFile.Close()
	multi, err := Create(multiFile, options)
	if err != nil {
		t.Fatal(err)
	}
	heldBack := uint64(0)
	for session := range sessions {
		if session != 0 {
			if multi, err = Open(multiFile, options); err != nil {
				t.Fatal(err)
			}
		}
		write(multi, session)
		if err := multi.Flush(); err != nil {
			t.Fatal(err)
		}
		heldBack += multi.Stats().PendingRetiredBytes
		if err := multi.Close(); err != nil {
			t.Fatal(err)
		}
	}
	multi, err = Open(multiFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer multi.Close()
	multiEnd := multi.Stats().FileEnd
	if multiEnd > singleEnd {
		t.Fatalf("%d batched sessions ended at %d bytes against %d for one session of "+
			"the same writes, an excess of %d bytes (%d pages); a restart may cost "+
			"nothing once every retirement is durable in the commit that makes it, "+
			"and the reclaimer was holding %d bytes at those Closes, which is far "+
			"more than the excess and is why a pending-set bound would not have "+
			"noticed (single-session file %s)",
			sessions, multiEnd, singleEnd, multiEnd-singleEnd,
			(multiEnd-singleEnd)/uint64(options.PageSize), heldBack, singleFile.Name())
	}
}
