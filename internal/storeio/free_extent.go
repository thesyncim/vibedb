package storeio

import (
	"encoding/binary"
	"fmt"
)

// FreeExtent is one page-aligned physical range retired by a copy-on-write
// publication. RetiredGeneration is the last Store generation that may reach
// it; reuse additionally waits for every read lease and protected recovery
// root to advance beyond that generation.
//
// Coalescing two adjacent extents keeps the greater RetiredGeneration, never
// the lesser: the merged range is reachable from every generation either input
// was reachable from, so taking the minimum would advertise the older half for
// reuse while a recovery root could still dereference the newer one.
type FreeExtent struct {
	Offset            uint64
	Length            uint64
	RetiredGeneration uint64
}

func decodeFreeExtent(src []byte) FreeExtent {
	return FreeExtent{
		Offset:            binary.LittleEndian.Uint64(src[0:8]),
		Length:            binary.LittleEndian.Uint64(src[8:16]),
		RetiredGeneration: binary.LittleEndian.Uint64(src[16:24]),
	}
}

func validateFreeExtent(extent FreeExtent, pageSize uint32, fileEnd uint64) error {
	quantum := uint64(pageSize)
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		return err
	}
	if extent.Length == 0 || extent.Offset < layout.DataStart ||
		extent.Offset%quantum != 0 || extent.Length%quantum != 0 ||
		extent.Offset > maxSuperblockFileOffset || extent.Length > fileEnd || extent.Offset > fileEnd-extent.Length ||
		extent.RetiredGeneration == 0 {
		return fmt.Errorf("%w: invalid free extent", ErrInvalidWrite)
	}
	return nil
}
