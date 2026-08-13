package storeio

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// MutableInlineBootstrap is the immutable format geometry needed before Open
// can size recovery scratch or construct any reader/writer resources.
// PageCatalogHead/PageCatalogDigest/PageCatalogBytes are the exact catalog
// identity; a zero PageCatalogBytes denotes the canonical empty catalog.
type MutableInlineBootstrap struct {
	StoreID                      [16]byte
	FileEnd                      uint64
	PageSize                     uint32
	MaxPageSize                  uint32
	MaterializationDamageGranule uint32
	PhysicalCapacityBytes        uint64
	PageCatalogHead              PageRef
	PageCatalogDigest            [PageCatalogDigestSize]byte
	PageCatalogBytes             uint32
}

// DiscoverMutableInlinePageSize returns the allocation quantum carried by a
// mutable Store's fixed inline roots. It reads at most one 4 KiB record at
// offset zero plus one record at each power-of-two candidate offset. It never
// scans data pages.
//
// The first inline root always starts at offset zero. Its alternate starts at
// PageSize, while the encoded record itself is fixed at InlineSuperblockSize.
// If offset zero is torn, probing the finite uint32 power-of-two page-size
// domain finds the alternate without trusting caller configuration. Two valid
// roots that imply different page sizes fail closed.
func DiscoverMutableInlinePageSize(file *os.File) (uint32, error) {
	bootstrap, err := DiscoverMutableInlineBootstrap(file)
	return bootstrap.PageSize, err
}

// DiscoverMutableInlineBootstrap returns the immutable geometry carried by a
// mutable Store's fixed inline roots. Like page-size discovery, it probes only
// the finite root-slot locations and never scans data pages. Conflicting valid
// slots fail closed before their values can size scratch or select a catalog.
func DiscoverMutableInlineBootstrap(
	file *os.File,
) (MutableInlineBootstrap, error) {
	if file == nil {
		return MutableInlineBootstrap{},
			fmt.Errorf("%w: nil inline discovery file", ErrInvalidWrite)
	}
	info, err := file.Stat()
	if err != nil {
		return MutableInlineBootstrap{}, err
	}
	if info.Size() < InlineSuperblockSize {
		return MutableInlineBootstrap{}, ErrSuperblockNotFound
	}

	var record [InlineSuperblockSize]byte
	var discovered InlineSuperblock
	found := false
	consider := func(offset uint64, expected uint32) error {
		n, readErr := file.ReadAt(record[:], int64(offset))
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return readErr
		}
		if n != len(record) {
			return nil
		}
		root, decodeErr := DecodeInlineSuperblock(record[:])
		if decodeErr != nil || expected != 0 && root.PageSize != expected {
			return nil
		}
		if found &&
			(discovered.StoreID != root.StoreID ||
				discovered.PageSize != root.PageSize ||
				!sameImmutableInlineConfiguration(discovered, root)) {
			return ErrSuperblockConflict
		}
		if found && discovered.Generation == root.Generation &&
			discovered != root {
			return ErrSuperblockConflict
		}
		if !found || root.Generation > discovered.Generation {
			discovered = root
			found = true
		}
		return nil
	}

	if err := consider(0, 0); err != nil {
		return MutableInlineBootstrap{}, err
	}

	fileSize := uint64(info.Size())
	for candidate := uint64(physicalPageQuantum); candidate <= uint64(^uint32(0)) && candidate <= fileSize-uint64(InlineSuperblockSize); candidate <<= 1 {
		if err := consider(candidate, uint32(candidate)); err != nil {
			return MutableInlineBootstrap{}, err
		}
		if candidate > uint64(^uint32(0))>>1 {
			break
		}
	}
	if !found {
		return MutableInlineBootstrap{}, ErrSuperblockNotFound
	}
	return mutableInlineBootstrap(discovered), nil
}

func mutableInlineBootstrap(root InlineSuperblock) MutableInlineBootstrap {
	return MutableInlineBootstrap{
		StoreID:                      root.StoreID,
		FileEnd:                      root.FileEnd,
		PageSize:                     root.PageSize,
		MaxPageSize:                  root.State.MaxPageSize,
		MaterializationDamageGranule: root.State.MaterializationDamageGranule,
		PhysicalCapacityBytes:        root.State.PhysicalCapacityBytes,
		PageCatalogHead:              root.State.PageCatalogHead,
		PageCatalogDigest:            root.State.PageCatalogDigest,
		PageCatalogBytes:             root.State.PageCatalogBytes,
	}
}
