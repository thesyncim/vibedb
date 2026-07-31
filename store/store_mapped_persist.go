package store

import (
	"encoding/binary"
	"unsafe"

	"github.com/thesyncim/vibejson"
)

// This file holds the few zero-copy helpers the memory-mapped document reader
// (store_mapped_docs.go, store_owned_documents.go, store_document_template.go)
// shares. They project posting entries and align record sections.

// A document record's storage class, recorded in its header: a classic tape, a
// 16-byte shape-taped value array, or an 8-byte narrow value array. The mapped
// reader dispatches on these to project each record without re-parsing.
const (
	persistDocClassic uint8 = iota
	persistDocWide
	persistDocNarrow
)

// persistNativeLittleEndian reports whether the host stores integers
// little-endian, so the bulk entry sections can be aliased (native) or must be
// decoded word by word (big-endian). Determined once at init.
var persistNativeLittleEndian = func() bool {
	x := uint16(1)
	return *(*byte)(unsafe.Pointer(&x)) == 1
}()

// persistAlign8 rounds n up to the next multiple of eight, the alignment the
// entry sections take so an aliased IndexEntry view meets its 4-byte load
// requirement and the record after it starts aligned.
func persistAlign8(n uint64) uint64 { return (n + 7) &^ 7 }

// openEntries returns a view of count 16-byte index entries starting at off in
// data. When the host is little-endian and the address is aligned the mapped
// bytes are aliased in place; otherwise each record is decoded word by word. The
// caller has bounded [off, off+count*16) within data.
func openEntries(data []byte, off, count uint64) []vibejson.IndexEntry {
	if count == 0 {
		return nil
	}
	if persistNativeLittleEndian {
		p := unsafe.Pointer(&data[off])
		if uintptr(p)%unsafe.Alignof(vibejson.IndexEntry{}) == 0 {
			return unsafe.Slice((*vibejson.IndexEntry)(p), int(count))
		}
	}
	out := make([]vibejson.IndexEntry, count)
	for i := range out {
		b := data[off+uint64(i)*16:]
		out[i] = vibejson.IndexEntry{
			Start: binary.LittleEndian.Uint32(b[0:4]),
			End:   binary.LittleEndian.Uint32(b[4:8]),
			Next:  binary.LittleEndian.Uint32(b[8:12]),
			Info:  binary.LittleEndian.Uint32(b[12:16]),
		}
	}
	return out
}
