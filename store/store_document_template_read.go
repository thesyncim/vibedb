package store

import (
	"encoding/binary"
	"runtime"
	"unsafe"

	"github.com/thesyncim/vibejson"
)

// Template reads decode one row's spans against an immutable structural
// tape. Batch paths retain compact spans; only the general Index API widens
// and caches a classic tape.

func (s *Segment) TemplateAt(i int) (*DocumentTemplate, bool) {
	if s.mappedDocs == nil {
		return nil, false
	}
	index := s.mappedBase + uint64(i)
	var kind uint8
	var shapeID uint32
	if s.mappedDocs.compactRefs != nil {
		r := &s.mappedDocs.compactRefs[index]
		kind, shapeID = r.kind, uint32(r.meta)
	} else if s.mappedDocs.ownedRefs != nil {
		r := &s.mappedDocs.ownedRefs[index]
		kind, shapeID = r.kind, uint32(r.shapeID)
	} else {
		r := &s.mappedDocs.refs[index]
		kind, shapeID = r.kind, r.shapeID
	}
	if !storeOwnedDocIsTemplate(kind) || int(shapeID) >= len(s.mappedDocs.templates) {
		runtime.KeepAlive(s.mappedDocs)
		return nil, false
	}
	template := s.mappedDocs.templates[shapeID]
	runtime.KeepAlive(s.mappedDocs)
	return template, true
}

func (s *Segment) TemplateSpan(i int, template *DocumentTemplate, ordinal int) uint32 {
	index := s.mappedBase + uint64(i)
	if ordinal == 0 {
		start, end := storeRootSpan(s.RawAt(i))
		return start | end<<16
	}
	var sourceOff uint64
	var srcLen uint32
	var kind uint8
	if s.mappedDocs.compactRefs != nil {
		r := &s.mappedDocs.compactRefs[index]
		sourceOff, srcLen, kind = r.sourceOff, r.srcLen, r.kind
	} else if s.mappedDocs.ownedRefs != nil {
		r := &s.mappedDocs.ownedRefs[index]
		sourceOff, srcLen, kind = r.sourceOff, r.srcLen, r.kind
	} else {
		r := &s.mappedDocs.refs[index]
		sourceOff, srcLen, kind = r.sourceOff, r.srcLen, r.kind
	}
	spanIndex := template.spanIndex[ordinal]
	if spanIndex == ^uint16(0) {
		return 0
	}
	width := storeOwnedTemplateSpanWidth(kind)
	off := storeMappedTapeOffset(sourceOff+uint64(srcLen), kind) + uint64(spanIndex)*width
	var span uint32
	switch kind {
	case storeOwnedDocTemplate8:
		span = uint32(s.source[off]) | uint32(s.source[off+1])<<16
	case storeOwnedDocTemplateLength8:
		start := uint32(binary.LittleEndian.Uint16(s.source[off : off+2]))
		span = start | (start+uint32(s.source[off+2]))<<16
	default:
		span = binary.LittleEndian.Uint32(s.source[off : off+storeOwnedTemplateSpanLen])
	}
	runtime.KeepAlive(s.mappedDocs)
	return span
}

func (s *Segment) storeTemplateKeySpan(i int, template *DocumentTemplate, ordinal int) (uint32, uint32) {
	valueSpan := s.TemplateSpan(i, template, ordinal+1)
	src := s.DocAt(i).Src
	j := int(valueSpan&0xffff) - 1
	for vibejson.IsJSONWhitespace(src[j]) {
		j--
	}
	j--
	for vibejson.IsJSONWhitespace(src[j]) {
		j--
	}
	end := uint32(j) + 1
	representative := &template.Index.Entries[ordinal]
	return end - (representative.End - representative.Start), end
}

func (s *Segment) synthStoreTemplate(i int, template *DocumentTemplate, dst []vibejson.IndexEntry) []vibejson.IndexEntry {
	dst = append(dst, template.Index.Entries...)
	base := len(dst) - len(template.Index.Entries)
	rootSpan := s.TemplateSpan(i, template, 0)
	dst[base].Start, dst[base].End = rootSpan&0xffff, rootSpan>>16
	for ordinal := 1; ordinal < len(template.Index.Entries); ordinal++ {
		if template.spanIndex[ordinal] == ^uint16(0) {
			continue
		}
		span := s.TemplateSpan(i, template, ordinal)
		dst[base+ordinal].Start = span & 0xffff
		dst[base+ordinal].End = span >> 16
	}
	for ordinal := range template.Index.Entries {
		entry := &dst[base+ordinal]
		if entry.Flags()&vibejson.TapeFlagKey == 0 {
			continue
		}
		entry.Start, entry.End = s.storeTemplateKeySpan(i, template, ordinal)
	}
	return dst
}

func (s *Segment) widenStoreTemplate(i int, template *DocumentTemplate) vibejson.Index {
	s.widenMu.Lock()
	defer s.widenMu.Unlock()
	if entries, ok := s.widened[i]; ok {
		return vibejson.Index{Src: s.DocAt(i).Src, Entries: entries}
	}
	entries := s.synthStoreTemplate(i, template, make([]vibejson.IndexEntry, 0, len(template.Index.Entries)))
	if s.widened == nil {
		s.widened = make(map[int][]vibejson.IndexEntry)
	}
	s.widened[i] = entries
	return vibejson.Index{Src: s.DocAt(i).Src, Entries: entries}
}

type storeTemplatePointerHint struct {
	template *DocumentTemplate
	ordinal  int
	ok       bool
	err      error
}

type storeTemplateFieldHint struct {
	template *DocumentTemplate
	ordinal  int32
}

func (h *storeTemplateFieldHint) lookup(template *DocumentTemplate, key vibejson.CompiledKey) int {
	if h.template == template {
		return int(h.ordinal)
	}
	node, ok := template.Index.Root().GetCompiled(key)
	ordinal := -1
	if ok {
		base := unsafe.Pointer(unsafe.SliceData(template.Index.Entries))
		delta := uintptr(unsafe.Pointer(node.Entry)) - uintptr(base)
		candidate := int(delta / unsafe.Sizeof(vibejson.IndexEntry{}))
		if candidate >= 0 && candidate < len(template.Index.Entries) {
			ordinal = candidate
		}
	}
	h.template, h.ordinal = template, int32(ordinal)
	return ordinal
}

func (h *storeTemplatePointerHint) resolve(template *DocumentTemplate, pointer vibejson.CompiledPointer) (int, bool, error) {
	if h.template == template {
		return h.ordinal, h.ok, h.err
	}
	node, ok, err := template.Index.PointerCompiled(pointer)
	ordinal := 0
	if ok {
		base := unsafe.Pointer(unsafe.SliceData(template.Index.Entries))
		delta := uintptr(unsafe.Pointer(node.Entry)) - uintptr(base)
		ordinal = int(delta / unsafe.Sizeof(vibejson.IndexEntry{}))
		if ordinal < 0 || ordinal >= len(template.Index.Entries) {
			ok = false
		}
	}
	h.template, h.ordinal, h.ok, h.err = template, ordinal, ok, err
	return ordinal, ok, err
}
