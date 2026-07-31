package storeio

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	superblockCopies        = 2
	maxSuperblockFileOffset = uint64(^uint64(0) >> 1)
	physicalPageQuantum     = uint32(4096)
	// MaxPhysicalPageSize bounds every contiguous extent admitted from durable
	// metadata. Larger documents use overflow chains; accepting multi-gigabyte
	// frame sizes would let a checksummed root turn Open into an allocation
	// denial.
	MaxPhysicalPageSize = uint32(64 << 20)
)

var (
	// ErrSuperblockCorrupt reports an invalid root header, checksum, extent, or
	// referenced root page.
	ErrSuperblockCorrupt = errors.New("vibejson: corrupt Store superblock")
	// ErrSuperblockNotFound reports that neither fixed root copy is valid.
	ErrSuperblockNotFound = errors.New("vibejson: no valid Store superblock")
	// ErrSuperblockConflict reports two individually valid root copies that do
	// not belong to one monotonic Store history.
	ErrSuperblockConflict = errors.New("vibejson: conflicting Store superblocks")
	// ErrRecoveryBufferTooSmall reports caller scratch that cannot hold one
	// referenced page or catalog. Recovery never silently selects an older root
	// merely because caller memory is undersized.
	ErrRecoveryBufferTooSmall = errors.New("vibejson: Store recovery buffer too small")

	pageChecksumTable = crc32.MakeTable(crc32.Castagnoli)
)

// PageChecksum returns the deterministic CRC32C used by attached-Store pages.
// SIMD builds may fold large pages with carry-less multiplication; all other
// builds use Go's hardware-dispatched implementation.
func PageChecksum(data []byte) uint32 { return pageChecksum(data) }

// superblockOffset returns the initial slot selected by generation. A Committer
// writing recoverable superblocks replaces it with the slot opposite the last
// successful physical commit.
func superblockOffset(generation uint64, pageSize uint32) (int64, error) {
	layout, err := MutableStoreLayout(pageSize)
	if generation == 0 || err != nil {
		return 0, fmt.Errorf("%w: generation=%d page-size=%d", ErrInvalidWrite, generation, pageSize)
	}
	return int64(layout.RootOffsets[(generation-1)&(superblockCopies-1)]), nil
}

func readStateRootRefs(
	file *os.File,
	root StateRoot,
	fileEnd uint64,
	scratch []byte,
) (bool, error) {
	refs := [...]PageRef{
		root.PrimaryRoot,
		root.ExactIndexRoot,
	}
	for _, ref := range refs {
		if ref == (PageRef{}) {
			continue
		}
		if uint64(ref.Length) > uint64(len(scratch)) {
			return false, nil
		}
		buf := scratch[:ref.Length]
		n, err := file.ReadAt(buf, int64(ref.Offset))
		if err != nil && !errors.Is(err, io.EOF) {
			return false, err
		}
		if n != len(buf) {
			return false, nil
		}
		header, _, openErr := OpenPage(buf)
		if openErr != nil || header.StoreID != root.StoreID || header.PageSize != ref.Length ||
			header.Kind != ref.Kind || header.LogicalID != ref.LogicalID || header.Generation != ref.Generation {
			return false, nil
		}
	}
	return validateRecoveredPageCatalog(file, root, fileEnd, scratch)
}

// validateRecoveredPageCatalog verifies the complete immutable catalog run
// before a recovery candidate may be selected. In particular, a valid head is
// not enough: every derived physical and logical segment reference, canonical
// byte, tail byte, and the exact root digest (including an all-zero value) must
// agree under the candidate's own high-water marks.
func validateRecoveredPageCatalog(
	file *os.File,
	root StateRoot,
	fileEnd uint64,
	scratch []byte,
) (bool, error) {
	_, err := openRecoveredPageCatalog(file, root, fileEnd, scratch)
	return err == nil, err
}

func openRecoveredPageCatalog(
	file *os.File,
	root StateRoot,
	fileEnd uint64,
	scratch []byte,
) (*CanonicalPageCatalog, error) {
	if root.PageCatalogBytes == 0 {
		if root.PageCatalogHead != (PageRef{}) ||
			root.PageCatalogDigest != ([PageCatalogDigestSize]byte{}) {
			return nil, fmt.Errorf(
				"%w: empty catalog has durable identity",
				ErrPageCatalogCorrupt,
			)
		}
		return OpenCanonicalPageCatalog(nil)
	}
	layout, err := MutableStoreLayout(root.PageSize)
	if err != nil {
		return nil, err
	}
	if uint64(len(scratch)) < uint64(root.PageSize) {
		return nil, fmt.Errorf(
			"%w: have=%d need=%d",
			ErrRecoveryBufferTooSmall, len(scratch), root.PageSize,
		)
	}
	reader := recoveryCatalogReader{reader: file}
	catalog, err := OpenPageCatalogChainAt(
		&reader,
		root.PageCatalogHead,
		PageCatalogBounds{
			StoreID:        root.StoreID,
			Generation:     root.Generation,
			PageSize:       root.PageSize,
			DataStart:      layout.DataStart,
			FileEnd:        fileEnd,
			NextLogicalID:  root.NextLogicalID,
			TotalBytes:     root.PageCatalogBytes,
			ExpectedDigest: root.PageCatalogDigest,
		},
		scratch[:root.PageSize],
	)
	if err != nil {
		if reader.err != nil {
			return nil, reader.err
		}
		// A malformed, torn, grafted, or short catalog disqualifies this root
		// just like any other checksum-valid but semantically invalid top-level
		// page. The alternate recovery root must still get its chance.
		if errors.Is(err, ErrPageCatalogCorrupt) {
			return nil, err
		}
		if errors.Is(err, ErrInvalidWrite) {
			return nil, fmt.Errorf(
				"%w: %w", ErrPageCatalogCorrupt, err,
			)
		}
		return nil, err
	}
	return catalog, nil
}

type recoveryCatalogReader struct {
	reader io.ReaderAt
	err    error
}

func (r *recoveryCatalogReader) ReadAt(
	dst []byte,
	offset int64,
) (int, error) {
	n, err := r.reader.ReadAt(dst, offset)
	if err != nil && !errors.Is(err, io.EOF) && r.err == nil {
		r.err = err
	}
	return n, err
}

func validPhysicalPageSize(pageSize uint32) bool {
	return pageSize >= physicalPageQuantum &&
		pageSize <= MaxPhysicalPageSize &&
		pageSize&(pageSize-1) == 0
}

func allZero(src []byte) bool {
	var combined byte
	for _, value := range src {
		combined |= value
	}
	return combined == 0
}
