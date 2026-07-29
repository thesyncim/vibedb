package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storemem"
	"github.com/thesyncim/vibejson"
)

// compactDocuments moves a completed builder's exact source bytes and
// pointer-free structural arrays into one owned external block. Chunks retain
// only packed row ordinals into one descriptor owner and one shared immutable
// shape table; no source, Index, shape-ref, or narrow-value object remains per
// document on the Go heap.
func (b *Builder) compactDocuments(state *State) error {
	if b.count == 0 {
		return nil
	}
	var templates []*DocumentTemplate
	var rowLayouts []storeOwnedRowLayout
	var dataBytes int
	var err error
	// Value-dictionary references are source offsets and compose with templates.
	// Wildcard postings still verify their classic remainder through Root, so
	// those builds retain classic nested tapes until that verifier is native.
	if !b.options.Postings {
		templates, rowLayouts, dataBytes, err = buildStoreDocumentTemplates(state.Chunks, b.count)
	} else {
		dataBytes, err = storeOwnedDocumentDataBytes(state.Chunks)
	}
	if err != nil {
		return err
	}
	layout := storeOwnedDocRefGeneral
	if storeCanUseOwnedDocRefs(state.Chunks, len(b.shapes)) {
		layout = storeOwnedDocRefWide
		if storeCanUseCompactDocRefs(state.Chunks, len(b.shapes)) {
			layout = storeOwnedDocRefCompact
		}
	}
	owned, err := newStoreOwnedDocuments(b.count, dataBytes, layout)
	if err != nil {
		return fmt.Errorf("vibejson: compact Builder documents: %w", err)
	}
	shapeIDs := make(map[*ShapeRecord]uint32, len(b.shapes))
	for id, rec := range b.shapes {
		shapeIDs[rec] = uint32(id)
	}

	data := owned.sourceBlock.Bytes()
	position, rowBase := 0, uint64(0)
	valid := true
	state.Chunks.Each(func(_ uint32, chunk *Chunk) bool {
		docs := &chunk.Docs
		narrow := 0
		for i := 0; i < docs.Len(); i++ {
			index := docs.DocAt(i)
			ref := docs.ShapeTapeRefAt(i)
			rowIndex := int(rowBase) + i
			narrowKind, narrowWidth := uint8(0), uint64(0)
			if len(rowLayouts) != 0 {
				narrowKind = rowLayouts[rowIndex].kind()
				if ref.Narrow {
					narrowWidth = storeOwnedNarrowStorageWidth(narrowKind)
				}
			} else {
				narrowKind, narrowWidth = storeOwnedNarrowStorage(docs, i, ref)
			}
			rootStart, rootEnd := ref.Start, ref.End
			templateID, templated := uint32(0), false
			if len(rowLayouts) != 0 {
				templateID, templated = rowLayouts[rowIndex].template()
			}
			start := position
			position += copy(data[position:], index.Src)
			kind, count, shapeID := uint8(persistDocClassic), len(index.Entries), storeMappedNoShape
			switch {
			case templated:
				kind = rowLayouts[rowIndex].kind()
				shapeID = templateID
				rootStart, rootEnd = index.Entries[0].Start, index.Entries[0].End
				position = int(storeMappedTapeOffset(uint64(position), kind))
				template := templates[templateID]
				width := int(storeOwnedTemplateSpanWidth(kind))
				for ordinal := range index.Entries {
					spanIndex := template.spanIndex[ordinal]
					if spanIndex == ^uint16(0) {
						continue
					}
					entry := &index.Entries[ordinal]
					off := position + int(spanIndex)*width
					switch kind {
					case storeOwnedDocTemplate8:
						data[off], data[off+1] = byte(entry.Start), byte(entry.End)
					case storeOwnedDocTemplateLength8:
						binary.LittleEndian.PutUint16(data[off:off+2], uint16(entry.Start))
						data[off+2] = byte(entry.End - entry.Start)
					default:
						binary.LittleEndian.PutUint32(data[off:off+storeOwnedTemplateSpanLen],
							entry.Start|entry.End<<16)
					}
				}
				position += int(template.spanCount) * width
				position = int(persistAlign8(uint64(position)))
			case ref.Rec == nil:
				position = int(storeMappedTapeOffset(uint64(position), kind))
				copyStoreOwnedEntries(data[position:], index.Entries)
				position += len(index.Entries) * int(unsafe.Sizeof(vibejson.IndexEntry{}))
			case ref.Narrow:
				kind, count = narrowKind, len(ref.Rec.Fields)
				id, ok := shapeIDs[ref.Rec]
				if !ok {
					valid = false
					return false
				}
				shapeID = id
				position = int(storeMappedTapeOffset(uint64(position), kind))
				width := int(narrowWidth)
				for ordinal := 0; ordinal < count; ordinal++ {
					value := docs.NarrowAt(i, ref, ordinal)
					if value.Info&vibejson.InfoCountMask != 0 || value.Info>>vibejson.InfoKindShift > uint32(^uint8(0)) {
						valid = false
						return false
					}
					off := position + ordinal*width
					switch kind {
					case storeOwnedDocNarrow8:
						data[off], data[off+1] = byte(value.Span), byte(value.Span>>16)
						data[off+2] = byte(value.Info >> vibejson.InfoKindShift)
					case storeOwnedDocNarrow9:
						start, end := value.Span&0xffff, value.Span>>16
						packed := start | (end-start)<<9 | (value.Info>>vibejson.InfoKindShift)<<17
						data[off], data[off+1], data[off+2] = byte(packed), byte(packed>>8), byte(packed>>16)
					case storeOwnedDocNarrowLength8:
						start, end := value.Span&0xffff, value.Span>>16
						binary.LittleEndian.PutUint16(data[off:off+2], uint16(start))
						data[off+2] = byte(end - start)
						data[off+3] = byte(value.Info >> vibejson.InfoKindShift)
					default:
						binary.LittleEndian.PutUint32(data[off:off+4], value.Span)
						data[off+4] = byte(value.Info >> vibejson.InfoKindShift)
					}
				}
				position += count * width
				narrow += count
			default:
				kind = persistDocWide
				id, ok := shapeIDs[ref.Rec]
				if !ok {
					valid = false
					return false
				}
				shapeID = id
				position = int(storeMappedTapeOffset(uint64(position), kind))
				copyStoreOwnedEntries(data[position:], index.Entries)
				position += len(index.Entries) * int(unsafe.Sizeof(vibejson.IndexEntry{}))
			}
			owned.setRef(rowBase+uint64(i), storeMappedDocRef{
				sourceOff: uint64(start), srcLen: uint32(len(index.Src)),
				entryCount: uint32(count), start: rootStart, end: rootEnd,
				shapeID: shapeID, kind: kind, enriched: ref.enriched,
			})
		}
		docs.docs = nil
		docs.srcChunk = nil
		docs.entryChunk = nil
		// The arena free lists go with the arenas they recycle: this segment's
		// documents now live in the owned block, and nothing will ever build
		// into it again.
		docs.srcPool.drop()
		docs.entryPool.drop()
		docs.scratch = nil
		docs.tapeRefs = nil
		docs.narrow = nil
		docs.shapes = ShapeCache{}
		docs.widened = nil
		docs.source = data
		docs.mappedDocs = owned
		docs.mappedShapes = b.shapes
		docs.mappedBase = rowBase
		docs.mappedCount = int(chunk.Count)
		docs.mappedNarrow = narrow
		rowBase += uint64(chunk.Count)
		return true
	})
	if !valid || position != len(data) || rowBase != uint64(b.count) {
		owned.release()
		return errors.New("vibejson: Builder compact document invariant")
	}
	state.mappedDocs = owned
	owned.templates = templates
	owned.shapes = b.shapes
	return nil
}

type storeOwnedDocRefLayout uint8

const (
	storeOwnedDocRefGeneral storeOwnedDocRefLayout = iota
	storeOwnedDocRefWide
	storeOwnedDocRefCompact
)

func newStoreOwnedDocuments(count, dataBytes int, layout storeOwnedDocRefLayout) (*storeMappedDocs, error) {
	var owned *storeMappedDocs
	var err error
	switch layout {
	case storeOwnedDocRefCompact:
		if count < 0 || count > math.MaxInt/storeCompactDocRefBytes {
			return nil, ErrCheckpointTooLarge
		}
		block, allocErr := storemem.Allocate(count * storeCompactDocRefBytes)
		if allocErr != nil {
			return nil, allocErr
		}
		owned = &storeMappedDocs{
			refData:   unsafe.Pointer(unsafe.SliceData(block.Bytes())),
			refStride: unsafe.Sizeof(storeCompactDocRef{}), block: block,
		}
		if count != 0 {
			owned.compactRefs = unsafe.Slice((*storeCompactDocRef)(unsafe.Pointer(unsafe.SliceData(block.Bytes()))), count)
		}
		runtime.SetFinalizer(owned, (*storeMappedDocs).release)
	case storeOwnedDocRefWide:
		if count < 0 || count > math.MaxInt/storeOwnedDocRefBytes {
			return nil, ErrCheckpointTooLarge
		}
		block, allocErr := storemem.Allocate(count * storeOwnedDocRefBytes)
		if allocErr != nil {
			return nil, allocErr
		}
		owned = &storeMappedDocs{
			refData:   unsafe.Pointer(unsafe.SliceData(block.Bytes())),
			refStride: unsafe.Sizeof(storeOwnedDocRef{}), block: block,
		}
		if count != 0 {
			owned.ownedRefs = unsafe.Slice((*storeOwnedDocRef)(unsafe.Pointer(unsafe.SliceData(block.Bytes()))), count)
		}
		runtime.SetFinalizer(owned, (*storeMappedDocs).release)
	default:
		owned, err = newStoreMappedDocs(count)
	}
	if err != nil {
		return nil, err
	}
	block, err := storemem.Allocate(dataBytes)
	if err != nil {
		owned.release()
		return nil, err
	}
	owned.sourceBlock = block
	return owned, nil
}

func storeCanUseCompactDocRefs(chunks storeChunkVector, shapeCount int) bool {
	if shapeCount >= int(storeOwnedNoShape) {
		return false
	}
	compact := true
	chunks.Each(func(_ uint32, chunk *Chunk) bool {
		for i := 0; i < chunk.Docs.Len(); i++ {
			index := chunk.Docs.DocAt(i)
			if uint64(len(index.Src)) > uint64(^uint32(0)) || len(index.Entries) > int(^uint16(0)) {
				compact = false
				return false
			}
		}
		return true
	})
	return compact
}

func storeCanUseOwnedDocRefs(chunks storeChunkVector, shapeCount int) bool {
	if shapeCount >= int(storeOwnedNoShape) {
		return false
	}
	compact := true
	chunks.Each(func(_ uint32, chunk *Chunk) bool {
		for i := 0; i < chunk.Docs.Len(); i++ {
			ref := chunk.Docs.ShapeTapeRefAt(i)
			if ref.Rec != nil && (ref.Start > ShapeNarrowMaxEnd || ref.End > ShapeNarrowMaxEnd) {
				compact = false
				return false
			}
		}
		return true
	})
	return compact
}

func storeOwnedDocumentDataBytes(chunks storeChunkVector) (int, error) {
	var total uint64
	valid := true
	chunks.Each(func(_ uint32, chunk *Chunk) bool {
		for i := 0; i < chunk.Docs.Len(); i++ {
			index := chunk.Docs.DocAt(i)
			ref := chunk.Docs.ShapeTapeRefAt(i)
			narrowKind, narrowWidth := storeOwnedNarrowStorage(&chunk.Docs, i, ref)
			var ok bool
			total, ok = storeOwnedDocumentEnd(total, index, ref, narrowKind, narrowWidth, false)
			if !ok {
				valid = false
				return false
			}
		}
		return true
	})
	if !valid {
		return 0, ErrCheckpointTooLarge
	}
	return int(total), nil
}

func copyStoreOwnedEntries(dst []byte, entries []vibejson.IndexEntry) {
	if len(entries) == 0 {
		return
	}
	out := unsafe.Slice((*vibejson.IndexEntry)(unsafe.Pointer(unsafe.SliceData(dst))), len(entries))
	copy(out, entries)
}
