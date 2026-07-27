package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unsafe"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// TemplateColumnarLeaf is the template-columnar leaf codec. It stores one
// content-addressed template skeleton per document shape plus offset-indexed
// packed field slots, a per-leaf value dictionary, per-field zone vectors, and
// region checksums. The bulk builder selects it as a third primary-leaf class
// (CommonPrimaryLeafTemplate) when it saves at least the adoption-policy
// threshold over the raw promoted leaf; the wrapping into a PagePrimaryLeaf
// common page lives in common_primary_template_leaf.go. The functions here are
// the class-agnostic codec: encoding, splice reconstruction, field projection,
// zone pruning, region reseal, and fail-closed validation.
const (
	templateColumnarLeafVersion        = uint16(2)
	templateColumnarLeafHeader         = 64
	templateColumnarLeafSlots          = 256
	templateColumnarLeafEmptyRank      = byte(0xff)
	templateColumnarLeafRowBytes       = 28
	templateColumnarLeafTplBytes       = 56
	templateColumnarLeafHoleBytes      = 8
	templateColumnarLeafColBytes       = 28
	templateColumnarLeafDictBytes      = 8
	templateColumnarLeafMaxSpliceHoles = 64
)

var (
	templateColumnarLeafMagic = [8]byte{'T', 'C', 'L', 'E', 'A', 'F', '2', 0}

	ErrTemplateColumnarLeafCorrupt = errors.New("vibejson: corrupt template-columnar leaf lab")
	ErrTemplateColumnarLeafShape   = errors.New("vibejson: incompatible template-columnar leaf shape")
)

type TemplateColumnarLeafHole struct {
	SkeletonOffset uint32
	Kind           document.Kind
}

// A run is an invariant source span preceding one scalar hole. The final run
// follows the final hole. Keeping spans rather than materialising a skeleton is
// the important fused-extraction property: BuildIndex validates once, and the
// tape is consumed once to emit both the shape fingerprint and borrowed spans.
type templateColumnarLeafRun struct {
	Start uint32
	End   uint32
}

type TemplateColumnarLeafExtraction struct {
	// Skeleton is populated only by template admission. Extraction itself does
	// not copy it; this keeps the validation hot path genuinely fused.
	Skeleton []byte
	Holes    []TemplateColumnarLeafHole
	Fields   [][]byte
	Runs     []templateColumnarLeafRun
	Src      []byte
	ID       [32]byte
}

func ExtractTemplateColumnarLeaf(src []byte, storage []vibejson.IndexEntry) (TemplateColumnarLeafExtraction, error) {
	return ExtractTemplateColumnarLeafInto(src, storage, nil)
}

func ExtractTemplateColumnarLeafInto(src []byte, storage []vibejson.IndexEntry, dst *TemplateColumnarLeafExtraction) (TemplateColumnarLeafExtraction, error) {
	index, err := vibejson.BuildIndex(src, storage)
	if err != nil {
		return TemplateColumnarLeafExtraction{}, err
	}
	out := TemplateColumnarLeafExtraction{Src: src}
	if dst != nil {
		out.Holes = dst.Holes[:0]
		out.Fields = dst.Fields[:0]
		out.Runs = dst.Runs[:0]
	}
	cursor := uint32(0)
	// Four cheap independent lanes are a router, never authority. Admission
	// byte-compares every run and kind, so collisions cannot merge templates.
	state := [4]uint64{
		0x243f6a8885a308d3,
		0x13198a2e03707344,
		0xa4093822299f31d0,
		0x082efa98ec4e6c89,
	}
	for i := range index.Entries {
		e := &index.Entries[i]
		if e.Flags()&vibejson.TapeFlagKey != 0 {
			continue
		}
		switch e.Kind() {
		case document.String, document.Number, document.Bool, document.Null:
		default:
			continue
		}
		if e.Start < cursor || e.End < e.Start || int(e.End) > len(src) {
			return TemplateColumnarLeafExtraction{}, ErrTemplateColumnarLeafShape
		}
		run := templateColumnarLeafRun{Start: cursor, End: e.Start}
		out.Runs = append(out.Runs, run)
		out.Holes = append(out.Holes, TemplateColumnarLeafHole{
			SkeletonOffset: e.Start - cursor, Kind: e.Kind(),
		})
		out.Fields = append(out.Fields, src[e.Start:e.End:e.End])
		n := uint64(run.End-run.Start) | uint64(e.Kind())<<32 | uint64(len(out.Holes))<<40
		lane := (len(out.Holes) - 1) & 3
		state[lane] = (state[lane] ^ n) * 0x9e3779b185ebca87
		cursor = e.End
	}
	out.Runs = append(out.Runs, templateColumnarLeafRun{Start: cursor, End: uint32(len(src))})
	last := out.Runs[len(out.Runs)-1]
	state[0] ^= uint64(last.End-last.Start) << 17
	for i := range state {
		binary.LittleEndian.PutUint64(out.ID[i*8:], state[i])
	}
	return out, nil
}

func (e TemplateColumnarLeafExtraction) Append(dst []byte) ([]byte, bool) {
	if len(e.Runs) != len(e.Fields)+1 || len(e.Holes) != len(e.Fields) {
		return dst, false
	}
	for i, field := range e.Fields {
		r := e.Runs[i]
		if r.End < r.Start || int(r.End) > len(e.Src) {
			return dst, false
		}
		dst = append(dst, e.Src[r.Start:r.End]...)
		dst = append(dst, field...)
	}
	r := e.Runs[len(e.Fields)]
	if r.End < r.Start || int(r.End) > len(e.Src) {
		return dst, false
	}
	return append(dst, e.Src[r.Start:r.End]...), true
}

type TemplateColumnarLeafRow struct {
	Slot uint8
	Key  []byte
	JSON []byte
}

type templateColumnarLeafTemplate struct {
	id       [32]byte
	skeleton []byte
	holes    []TemplateColumnarLeafHole
	runs     []uint32 // skeleton run lengths; len(holes)+1
}

type templateColumnarLeafBuildRow struct {
	row       TemplateColumnarLeafRow
	template  uint16
	extracted TemplateColumnarLeafExtraction
	lengths   []byte
	values    []byte
}

type templateColumnarLeafBuildColumn struct {
	template uint16
	hole     uint16
	kind     document.Kind
	present  [32]byte
	min      []byte
	max      []byte
}

type templateColumnarLeafDictValue struct {
	value []byte
}

func TemplateColumnarLeafRawBytes(rows []TemplateColumnarLeafRow) int {
	if len(rows) == 0 || len(rows) >= CommonPrimaryLeafWideSlots {
		return 0
	}
	class := CommonPrimaryLeafNarrow
	if len(rows) > CommonPrimaryLeafNarrowLive {
		class = CommonPrimaryLeafWide
	}
	n := CommonPrimaryLeafStructuralBytes(class, len(rows), 64<<10)
	for _, row := range rows {
		n += len(row.Key) + len(row.JSON)
	}
	return n
}

type TemplateColumnarLeafPlan struct {
	UseTemplate   bool
	RawBytes      int
	TemplateBytes int
	SelectedBytes int
}

func PlanTemplateColumnarLeaf(rows []TemplateColumnarLeafRow) (TemplateColumnarLeafPlan, []byte, error) {
	raw := TemplateColumnarLeafRawBytes(rows)
	if raw == 0 {
		return TemplateColumnarLeafPlan{}, nil, fmt.Errorf("%w: raw geometry", ErrInvalidWrite)
	}
	image, err := EncodeTemplateColumnarLeaf(rows)
	if err != nil {
		return TemplateColumnarLeafPlan{}, nil, err
	}
	p := TemplateColumnarLeafPlan{RawBytes: raw, TemplateBytes: len(image), SelectedBytes: raw}
	if len(image) < raw {
		p.UseTemplate, p.SelectedBytes = true, len(image)
	}
	return p, image, nil
}

func EncodeTemplateColumnarLeaf(rows []TemplateColumnarLeafRow) ([]byte, error) {
	if len(rows) == 0 || len(rows) >= templateColumnarLeafSlots {
		return nil, fmt.Errorf("%w: row count", ErrInvalidWrite)
	}
	buildRows := make([]templateColumnarLeafBuildRow, len(rows))
	templates := make([]templateColumnarLeafTemplate, 0, 8)
	var occupied [4]uint64
	// Count exact scalar spellings first. The final admission decision includes
	// all directory and reference bytes, so singleton/short values stay inline.
	// dictOrder records first-appearance order so the emitted dictionary — and
	// therefore the whole image — is a deterministic function of the input rows,
	// never of Go map iteration order.
	counts := make(map[string]int)
	dictOrder := make([]string, 0)
	for rank, row := range rows {
		if len(row.Key) == 0 || len(row.JSON) == 0 || rank != 0 && bytes.Compare(rows[rank-1].Key, row.Key) >= 0 {
			return nil, fmt.Errorf("%w: key/value/order", ErrInvalidWrite)
		}
		bit := uint64(1) << uint(row.Slot&63)
		if occupied[row.Slot>>6]&bit != 0 {
			return nil, fmt.Errorf("%w: duplicate slot", ErrInvalidWrite)
		}
		occupied[row.Slot>>6] |= bit
		storage := make([]vibejson.IndexEntry, len(row.JSON)+2)
		x, err := ExtractTemplateColumnarLeaf(row.JSON, storage)
		if err != nil {
			return nil, err
		}
		for _, f := range x.Fields {
			if counts[string(f)] == 0 {
				dictOrder = append(dictOrder, string(f))
			}
			counts[string(f)]++
		}
		ti := -1
		for i := range templates {
			if templates[i].id == x.ID && templateColumnarLeafShapeEqual(templates[i], x) {
				ti = i
				break
			}
		}
		if ti < 0 {
			if len(templates) == math.MaxUint16 {
				return nil, fmt.Errorf("%w: templates", ErrInvalidWrite)
			}
			ti = len(templates)
			tpl := templateColumnarLeafTemplate{id: x.ID}
			tpl.holes = append(tpl.holes, x.Holes...)
			tpl.runs = make([]uint32, len(x.Runs))
			for i, r := range x.Runs {
				tpl.runs[i] = r.End - r.Start
				tpl.skeleton = append(tpl.skeleton, x.Src[r.Start:r.End]...)
				if i < len(tpl.holes) {
					tpl.holes[i].SkeletonOffset = uint32(len(tpl.skeleton))
				}
			}
			templates = append(templates, tpl)
		}
		buildRows[rank] = templateColumnarLeafBuildRow{row: row, template: uint16(ti), extracted: x}
	}

	dictIDs := make(map[string]uint32)
	dict := make([]templateColumnarLeafDictValue, 0)
	for _, value := range dictOrder {
		count := counts[value]
		// one copy + 8-byte directory + one varint id per occurrence must beat
		// inline bytes. This is measured raw-spelling economics, not a heuristic
		// cardinality label. dictOrder is first-appearance order, so dictionary
		// ids and byte layout are deterministic across identical inputs.
		if count >= 2 && count*len(value) > len(value)+8+count {
			dictIDs[value] = uint32(len(dict))
			dict = append(dict, templateColumnarLeafDictValue{value: []byte(value)})
		}
	}
	columns := make([]templateColumnarLeafBuildColumn, 0)
	columnBase := make([]uint16, len(templates))
	for ti := range templates {
		if len(columns)+len(templates[ti].holes) > math.MaxUint16 {
			return nil, fmt.Errorf("%w: columns", ErrInvalidWrite)
		}
		columnBase[ti] = uint16(len(columns))
		for hi, h := range templates[ti].holes {
			columns = append(columns, templateColumnarLeafBuildColumn{
				template: uint16(ti), hole: uint16(hi), kind: h.Kind,
			})
		}
	}
	for rank := range buildRows {
		br := &buildRows[rank]
		base := int(columnBase[br.template])
		for hi, f := range br.extracted.Fields {
			col := &columns[base+hi]
			col.present[rank>>3] |= 1 << uint(rank&7)
			if col.min == nil || bytes.Compare(f, col.min) < 0 {
				col.min = append(col.min[:0], f...)
			}
			if col.max == nil || bytes.Compare(f, col.max) > 0 {
				col.max = append(col.max[:0], f...)
			}
			if id, ok := dictIDs[string(f)]; ok {
				br.lengths = binary.AppendUvarint(br.lengths, uint64(len(f))<<1|1)
				br.values = binary.AppendUvarint(br.values, uint64(id))
			} else {
				br.lengths = binary.AppendUvarint(br.lengths, uint64(len(f))<<1)
				br.values = append(br.values, f...)
			}
		}
	}
	return encodeTemplateColumnarLeafImage(buildRows, templates, columnBase, columns, dict)
}

func templateColumnarLeafShapeEqual(t templateColumnarLeafTemplate, x TemplateColumnarLeafExtraction) bool {
	if len(t.holes) != len(x.Holes) || len(t.runs) != len(x.Runs) {
		return false
	}
	skel := 0
	for i, r := range x.Runs {
		if t.runs[i] != r.End-r.Start || !bytes.Equal(t.skeleton[skel:skel+int(t.runs[i])], x.Src[r.Start:r.End]) {
			return false
		}
		skel += int(t.runs[i])
		if i < len(t.holes) && t.holes[i].Kind != x.Holes[i].Kind {
			return false
		}
	}
	return true
}

func encodeTemplateColumnarLeafImage(rows []templateColumnarLeafBuildRow, templates []templateColumnarLeafTemplate, columnBase []uint16, columns []templateColumnarLeafBuildColumn, dict []templateColumnarLeafDictValue) ([]byte, error) {
	metadataBytes := templateColumnarLeafHeader + templateColumnarLeafSlots + len(rows) +
		len(rows)*templateColumnarLeafRowBytes + len(templates)*templateColumnarLeafTplBytes +
		len(columns)*templateColumnarLeafColBytes + len(dict)*templateColumnarLeafDictBytes
	for _, r := range rows {
		metadataBytes += len(r.row.Key)
	}
	for _, t := range templates {
		metadataBytes += len(t.holes) * templateColumnarLeafHoleBytes
	}
	for _, c := range columns {
		if len(c.min) > math.MaxUint16 || len(c.max) > math.MaxUint16 {
			return nil, fmt.Errorf("%w: zone", ErrInvalidWrite)
		}
		metadataBytes += 32 + len(c.min) + len(c.max)
	}
	dictBytes := 0
	for _, t := range templates {
		dictBytes += len(t.skeleton)
	}
	for _, d := range dict {
		dictBytes += len(d.value)
	}
	dataBytes := 0
	for _, r := range rows {
		dataBytes += len(r.lengths) + len(r.values)
	}
	total := metadataBytes + dictBytes + dataBytes + 16
	if total > math.MaxUint32 {
		return nil, fmt.Errorf("%w: image too large", ErrInvalidWrite)
	}
	image := make([]byte, total)
	copy(image[:8], templateColumnarLeafMagic[:])
	binary.LittleEndian.PutUint16(image[8:10], templateColumnarLeafVersion)
	binary.LittleEndian.PutUint16(image[10:12], templateColumnarLeafHeader)
	binary.LittleEndian.PutUint16(image[12:14], uint16(len(rows)))
	binary.LittleEndian.PutUint16(image[14:16], uint16(len(templates)))
	binary.LittleEndian.PutUint16(image[16:18], uint16(len(columns)))
	binary.LittleEndian.PutUint16(image[18:20], uint16(len(dict)))
	binary.LittleEndian.PutUint32(image[20:24], uint32(metadataBytes))
	binary.LittleEndian.PutUint32(image[24:28], uint32(metadataBytes+dictBytes))
	binary.LittleEndian.PutUint32(image[28:32], uint32(metadataBytes+dictBytes+dataBytes))
	binary.LittleEndian.PutUint32(image[32:36], uint32(total))
	cursor := templateColumnarLeafHeader
	for i := range templateColumnarLeafSlots {
		image[cursor+i] = templateColumnarLeafEmptyRank
	}
	for rank, r := range rows {
		image[cursor+int(r.row.Slot)] = byte(rank)
	}
	cursor += templateColumnarLeafSlots
	for _, r := range rows {
		image[cursor] = r.row.Slot
		cursor++
	}
	rowDir := cursor
	cursor += len(rows) * templateColumnarLeafRowBytes
	for rank, r := range rows {
		start := cursor
		cursor += copy(image[cursor:], r.row.Key)
		at := rowDir + rank*templateColumnarLeafRowBytes
		binary.LittleEndian.PutUint32(image[at:at+4], uint32(start))
		binary.LittleEndian.PutUint32(image[at+4:at+8], uint32(cursor))
		binary.LittleEndian.PutUint16(image[at+8:at+10], r.template)
		if len(r.row.JSON) > math.MaxUint16 {
			return nil, fmt.Errorf("%w: row too wide", ErrInvalidWrite)
		}
		binary.LittleEndian.PutUint16(image[at+10:at+12], uint16(len(r.row.JSON)))
	}
	tplDir := cursor
	cursor += len(templates) * templateColumnarLeafTplBytes
	holeDir := cursor
	holeOrdinal := 0
	for ti, t := range templates {
		at := tplDir + ti*templateColumnarLeafTplBytes
		copy(image[at:at+32], t.id[:])
		binary.LittleEndian.PutUint32(image[at+32:at+36], uint32(holeOrdinal))
		binary.LittleEndian.PutUint16(image[at+36:at+38], uint16(len(t.holes)))
		binary.LittleEndian.PutUint16(image[at+38:at+40], columnBase[ti])
		for hi, h := range t.holes {
			ha := holeDir + holeOrdinal*templateColumnarLeafHoleBytes
			binary.LittleEndian.PutUint32(image[ha:ha+4], h.SkeletonOffset)
			binary.LittleEndian.PutUint16(image[ha+4:ha+6], uint16(t.runs[hi]))
			image[ha+6] = byte(h.Kind)
			holeOrdinal++
		}
	}
	cursor += holeOrdinal * templateColumnarLeafHoleBytes
	colDir := cursor
	cursor += len(columns) * templateColumnarLeafColBytes
	for ci, c := range columns {
		at := colDir + ci*templateColumnarLeafColBytes
		binary.LittleEndian.PutUint16(image[at:at+2], c.template)
		binary.LittleEndian.PutUint16(image[at+2:at+4], c.hole)
		image[at+4] = byte(c.kind)
		binary.LittleEndian.PutUint32(image[at+8:at+12], uint32(cursor))
		cursor += copy(image[cursor:], c.present[:])
		binary.LittleEndian.PutUint32(image[at+12:at+16], uint32(cursor))
		binary.LittleEndian.PutUint16(image[at+16:at+18], uint16(len(c.min)))
		cursor += copy(image[cursor:], c.min)
		binary.LittleEndian.PutUint32(image[at+20:at+24], uint32(cursor))
		binary.LittleEndian.PutUint16(image[at+24:at+26], uint16(len(c.max)))
		cursor += copy(image[cursor:], c.max)
	}
	valueDir := cursor
	cursor += len(dict) * templateColumnarLeafDictBytes
	binary.LittleEndian.PutUint32(image[36:40], uint32(valueDir))
	if cursor != metadataBytes {
		panic("template-columnar v2 metadata size")
	}
	dc := metadataBytes
	for ti, t := range templates {
		at := tplDir + ti*templateColumnarLeafTplBytes
		binary.LittleEndian.PutUint32(image[at+40:at+44], uint32(dc))
		dc += copy(image[dc:], t.skeleton)
		binary.LittleEndian.PutUint32(image[at+44:at+48], uint32(dc))
		// Program bounds identify the hole records admitted for this template.
		binary.LittleEndian.PutUint32(image[at+48:at+52], uint32(binary.LittleEndian.Uint32(image[at+32:at+36])))
		binary.LittleEndian.PutUint32(image[at+52:at+56], uint32(len(t.holes)))
	}
	for i, d := range dict {
		at := valueDir + i*templateColumnarLeafDictBytes
		binary.LittleEndian.PutUint32(image[at:at+4], uint32(dc))
		dc += copy(image[dc:], d.value)
		binary.LittleEndian.PutUint32(image[at+4:at+8], uint32(dc))
	}
	dataCursor := metadataBytes + dictBytes
	for rank, r := range rows {
		at := rowDir + rank*templateColumnarLeafRowBytes
		binary.LittleEndian.PutUint32(image[at+12:at+16], uint32(dataCursor))
		dataCursor += copy(image[dataCursor:], r.lengths)
		binary.LittleEndian.PutUint32(image[at+16:at+20], uint32(dataCursor))
		binary.LittleEndian.PutUint32(image[at+20:at+24], uint32(dataCursor))
		dataCursor += copy(image[dataCursor:], r.values)
		binary.LittleEndian.PutUint32(image[at+24:at+28], uint32(dataCursor))
	}
	sealTemplateColumnarLeaf(image)
	return image, nil
}

type TemplateColumnarLeafView struct {
	image       []byte
	count       uint16
	templates   uint16
	columns     uint16
	dictCount   uint16
	metadataEnd uint32
	dictEnd     uint32
	dataEnd     uint32
	slotRanks   []byte
	rankSlots   []byte
	rowDir      []byte
	tplDir      []byte
	holeDir     []byte
	colDir      []byte
	valueDir    []byte
}

type TemplateColumnarLeafZone struct {
	Kind               document.Kind
	Min, Max, Presence []byte
}

func sealTemplateColumnarLeaf(image []byte) {
	me := int(binary.LittleEndian.Uint32(image[20:24]))
	de := int(binary.LittleEndian.Uint32(image[24:28]))
	xe := int(binary.LittleEndian.Uint32(image[28:32]))
	table := image[xe:]
	binary.LittleEndian.PutUint32(table[0:4], PageChecksum(image[:me]))
	binary.LittleEndian.PutUint32(table[4:8], PageChecksum(image[me:de]))
	binary.LittleEndian.PutUint32(table[8:12], PageChecksum(image[de:xe]))
	binary.LittleEndian.PutUint32(table[12:16], PageChecksum(table[:12]))
}

func OpenTemplateColumnarLeaf(src []byte) (TemplateColumnarLeafView, error) {
	v, ok := parseTemplateColumnarLeafDirectories(src)
	if !ok {
		return TemplateColumnarLeafView{}, ErrTemplateColumnarLeafCorrupt
	}
	t := src[v.dataEnd:]
	if PageChecksum(src[:v.metadataEnd]) != binary.LittleEndian.Uint32(t[0:4]) ||
		PageChecksum(src[v.metadataEnd:v.dictEnd]) != binary.LittleEndian.Uint32(t[4:8]) ||
		PageChecksum(src[v.dictEnd:v.dataEnd]) != binary.LittleEndian.Uint32(t[8:12]) ||
		PageChecksum(t[:12]) != binary.LittleEndian.Uint32(t[12:16]) || !v.validate() {
		return TemplateColumnarLeafView{}, ErrTemplateColumnarLeafCorrupt
	}
	return v, nil
}

func parseTemplateColumnarLeafDirectories(src []byte) (TemplateColumnarLeafView, bool) {
	if len(src) < templateColumnarLeafHeader+templateColumnarLeafSlots+16 ||
		!bytes.Equal(src[:8], templateColumnarLeafMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != templateColumnarLeafVersion {
		return TemplateColumnarLeafView{}, false
	}
	n := int(binary.LittleEndian.Uint16(src[12:14]))
	nt := int(binary.LittleEndian.Uint16(src[14:16]))
	nc := int(binary.LittleEndian.Uint16(src[16:18]))
	nd := int(binary.LittleEndian.Uint16(src[18:20]))
	me := int(binary.LittleEndian.Uint32(src[20:24]))
	de := int(binary.LittleEndian.Uint32(src[24:28]))
	xe := int(binary.LittleEndian.Uint32(src[28:32]))
	total := int(binary.LittleEndian.Uint32(src[32:36]))
	valueDirStart := int(binary.LittleEndian.Uint32(src[36:40]))
	if n == 0 || n >= 256 || nt == 0 || me > de || de > xe || xe+16 != total || total != len(src) {
		return TemplateColumnarLeafView{}, false
	}
	cursor := templateColumnarLeafHeader
	slotRanks := src[cursor : cursor+256]
	cursor += 256
	if cursor+n+n*templateColumnarLeafRowBytes > me {
		return TemplateColumnarLeafView{}, false
	}
	rankSlots := src[cursor : cursor+n]
	cursor += n
	rowDir := src[cursor : cursor+n*templateColumnarLeafRowBytes]
	cursor += len(rowDir)
	keyEnd := cursor
	for i := range n {
		e := int(binary.LittleEndian.Uint32(rowDir[i*templateColumnarLeafRowBytes+4:]))
		if e > keyEnd {
			keyEnd = e
		}
	}
	if keyEnd < cursor || keyEnd > me || keyEnd+nt*templateColumnarLeafTplBytes > me {
		return TemplateColumnarLeafView{}, false
	}
	cursor = keyEnd
	tplDir := src[cursor : cursor+nt*templateColumnarLeafTplBytes]
	cursor += len(tplDir)
	holes := 0
	for i := range nt {
		at := i * templateColumnarLeafTplBytes
		first := int(binary.LittleEndian.Uint32(tplDir[at+32:]))
		count := int(binary.LittleEndian.Uint16(tplDir[at+36:]))
		if first != holes {
			return TemplateColumnarLeafView{}, false
		}
		holes += count
	}
	if cursor+holes*templateColumnarLeafHoleBytes+nc*templateColumnarLeafColBytes > me {
		return TemplateColumnarLeafView{}, false
	}
	holeDir := src[cursor : cursor+holes*templateColumnarLeafHoleBytes]
	cursor += len(holeDir)
	colDir := src[cursor : cursor+nc*templateColumnarLeafColBytes]
	if valueDirStart < cursor+len(colDir) || valueDirStart+nd*8 > me {
		return TemplateColumnarLeafView{}, false
	}
	valueDir := src[valueDirStart : valueDirStart+nd*8]
	return TemplateColumnarLeafView{
		image: src, count: uint16(n), templates: uint16(nt), columns: uint16(nc), dictCount: uint16(nd),
		metadataEnd: uint32(me), dictEnd: uint32(de), dataEnd: uint32(xe),
		slotRanks: slotRanks, rankSlots: rankSlots, rowDir: rowDir, tplDir: tplDir,
		holeDir: holeDir, colDir: colDir, valueDir: valueDir,
	}, true
}

func (v TemplateColumnarLeafView) validate() bool {
	var seen [4]uint64
	var previous []byte
	for rank := 0; rank < int(v.count); rank++ {
		slot := v.rankSlots[rank]
		if v.slotRanks[slot] != byte(rank) {
			return false
		}
		bit := uint64(1) << uint(slot&63)
		if seen[slot>>6]&bit != 0 {
			return false
		}
		seen[slot>>6] |= bit
		key, ti, ok := v.row(rank)
		if !ok || int(ti) >= int(v.templates) || rank > 0 && bytes.Compare(previous, key) >= 0 {
			return false
		}
		previous = key
		if !v.validateRow(rank, ti) {
			return false
		}
	}
	for slot, rank := range v.slotRanks {
		if rank != 0xff && (int(rank) >= int(v.count) || v.rankSlots[rank] != byte(slot)) {
			return false
		}
	}
	for ti := 0; ti < int(v.templates); ti++ {
		at := ti * templateColumnarLeafTplBytes
		first := int(binary.LittleEndian.Uint32(v.tplDir[at+32:]))
		n := int(binary.LittleEndian.Uint16(v.tplDir[at+36:]))
		ss := int(binary.LittleEndian.Uint32(v.tplDir[at+40:]))
		se := int(binary.LittleEndian.Uint32(v.tplDir[at+44:]))
		pf := int(binary.LittleEndian.Uint32(v.tplDir[at+48:]))
		pn := int(binary.LittleEndian.Uint32(v.tplDir[at+52:]))
		if ss < int(v.metadataEnd) || se < ss || se > int(v.dictEnd) || pf != first || pn != n || first+n > len(v.holeDir)/8 {
			return false
		}
		var prior uint32
		for i := 0; i < n; i++ {
			ha := (first + i) * 8
			off := binary.LittleEndian.Uint32(v.holeDir[ha:])
			run := binary.LittleEndian.Uint16(v.holeDir[ha+4:])
			k := document.Kind(v.holeDir[ha+6])
			if off < prior || int(off) > se-ss || k < document.Null || k > document.String || i > 0 && run == 0 {
				return false
			}
			prior = off
		}
	}
	prior := int(v.metadataEnd)
	for i := 0; i < int(v.dictCount); i++ {
		a := int(binary.LittleEndian.Uint32(v.valueDir[i*8:]))
		b := int(binary.LittleEndian.Uint32(v.valueDir[i*8+4:]))
		if a < prior || b < a || b > int(v.dictEnd) {
			return false
		}
		prior = b
	}
	return true
}

func (v TemplateColumnarLeafView) validateRow(rank int, ti uint16) bool {
	at := rank * templateColumnarLeafRowBytes
	ls := int(binary.LittleEndian.Uint32(v.rowDir[at+12:]))
	le := int(binary.LittleEndian.Uint32(v.rowDir[at+16:]))
	ds := int(binary.LittleEndian.Uint32(v.rowDir[at+20:]))
	de := int(binary.LittleEndian.Uint32(v.rowDir[at+24:]))
	if ls < int(v.dictEnd) || le < ls || ds != le || de < ds || de > int(v.dataEnd) {
		return false
	}
	tat := int(ti) * templateColumnarLeafTplBytes
	n := int(binary.LittleEndian.Uint16(v.tplDir[tat+36:]))
	rawLen := int(binary.LittleEndian.Uint16(v.rowDir[at+10:]))
	skeletonStart := binary.LittleEndian.Uint32(v.tplDir[tat+40:])
	skeletonEnd := binary.LittleEndian.Uint32(v.tplDir[tat+44:])
	reconstructed := int(skeletonEnd - skeletonStart)
	lp, dp := ls, ds
	for range n {
		token, m := binary.Uvarint(v.image[lp:le])
		if m <= 0 {
			return false
		}
		lp += m
		reconstructed += int(token >> 1)
		if token&1 != 0 {
			id, z := binary.Uvarint(v.image[dp:de])
			if z <= 0 || id >= uint64(v.dictCount) {
				return false
			}
			dp += z
			a := binary.LittleEndian.Uint32(v.valueDir[id*8:])
			b := binary.LittleEndian.Uint32(v.valueDir[id*8+4:])
			if uint64(b-a) != token>>1 {
				return false
			}
		} else {
			if token>>1 > uint64(de-dp) {
				return false
			}
			dp += int(token >> 1)
		}
	}
	return lp == le && dp == de && reconstructed == rawLen
}

func (v TemplateColumnarLeafView) row(rank int) ([]byte, uint16, bool) {
	if rank < 0 || rank >= int(v.count) {
		return nil, 0, false
	}
	at := rank * templateColumnarLeafRowBytes
	a := int(binary.LittleEndian.Uint32(v.rowDir[at:]))
	b := int(binary.LittleEndian.Uint32(v.rowDir[at+4:]))
	ti := binary.LittleEndian.Uint16(v.rowDir[at+8:])
	if a < 0 || b <= a || b > int(v.metadataEnd) {
		return nil, 0, false
	}
	return v.image[a:b:b], ti, true
}

func (v TemplateColumnarLeafView) columnFor(t, h uint16) (int, bool) {
	if int(t) >= int(v.templates) {
		return 0, false
	}
	at := int(t) * templateColumnarLeafTplBytes
	n := binary.LittleEndian.Uint16(v.tplDir[at+36:])
	base := binary.LittleEndian.Uint16(v.tplDir[at+38:])
	if h >= n || int(base+h) >= int(v.columns) {
		return 0, false
	}
	return int(base + h), true
}

func (v TemplateColumnarLeafView) Zone(t, h uint16) (TemplateColumnarLeafZone, bool) {
	ci, ok := v.columnFor(t, h)
	if !ok {
		return TemplateColumnarLeafZone{}, false
	}
	at := ci * templateColumnarLeafColBytes
	ps := int(binary.LittleEndian.Uint32(v.colDir[at+8:]))
	ms := int(binary.LittleEndian.Uint32(v.colDir[at+12:]))
	ml := int(binary.LittleEndian.Uint16(v.colDir[at+16:]))
	xs := int(binary.LittleEndian.Uint32(v.colDir[at+20:]))
	xl := int(binary.LittleEndian.Uint16(v.colDir[at+24:]))
	if ps < 0 || ps+32 > int(v.metadataEnd) || ms < ps+32 || ms+ml > int(v.metadataEnd) || xs < ms+ml || xs+xl > int(v.metadataEnd) {
		return TemplateColumnarLeafZone{}, false
	}
	return TemplateColumnarLeafZone{Kind: document.Kind(v.colDir[at+4]), Presence: v.image[ps : ps+32 : ps+32], Min: v.image[ms : ms+ml : ms+ml], Max: v.image[xs : xs+xl : xs+xl]}, true
}

func (v TemplateColumnarLeafView) fieldRank(rank int, hole uint16) ([]byte, document.Kind, bool) {
	_, ti, ok := v.row(rank)
	if !ok {
		return nil, document.Invalid, false
	}
	ci, ok := v.columnFor(ti, hole)
	if !ok {
		return nil, document.Invalid, false
	}
	at := rank * templateColumnarLeafRowBytes
	lp := int(binary.LittleEndian.Uint32(v.rowDir[at+12:]))
	le := int(binary.LittleEndian.Uint32(v.rowDir[at+16:]))
	dp := int(binary.LittleEndian.Uint32(v.rowDir[at+20:]))
	de := int(binary.LittleEndian.Uint32(v.rowDir[at+24:]))
	for i := uint16(0); i <= hole; i++ {
		token, n := binary.Uvarint(v.image[lp:le])
		if n <= 0 {
			return nil, document.Invalid, false
		}
		lp += n
		if token&1 != 0 {
			id, z := binary.Uvarint(v.image[dp:de])
			if z <= 0 || id >= uint64(v.dictCount) {
				return nil, document.Invalid, false
			}
			dp += z
			if i == hole {
				a := binary.LittleEndian.Uint32(v.valueDir[id*8:])
				b := binary.LittleEndian.Uint32(v.valueDir[id*8+4:])
				return v.image[a:b:b], document.Kind(v.colDir[ci*28+4]), true
			}
		} else {
			nn := int(token >> 1)
			if nn > de-dp {
				return nil, document.Invalid, false
			}
			if i == hole {
				return v.image[dp : dp+nn : dp+nn], document.Kind(v.colDir[ci*28+4]), true
			}
			dp += nn
		}
	}
	return nil, document.Invalid, false
}

func (v TemplateColumnarLeafView) Field(slot uint8, hole uint16) ([]byte, document.Kind, bool) {
	rank := v.slotRanks[slot]
	if rank == 0xff || int(rank) >= int(v.count) {
		return nil, document.Invalid, false
	}
	return v.fieldRank(int(rank), hole)
}

// copyRun is the exact-copy fallback for long or terminal spans.
func templateColumnarLeafCopyRun(dst, src []byte) {
	copy(dst, src)
}

// copySpan may overwrite bytes after a short span inside the reserved
// destination row. The following program segment always replaces those bytes.
func templateColumnarLeafCopySpan(dst []byte, out int, src []byte, a, b, end int) {
	n := b - a
	if n == 0 {
		return
	}
	if n <= 8 && out+8 <= end && a+8 <= len(src) {
		*(*uint64)(unsafe.Pointer(&dst[out])) = *(*uint64)(unsafe.Pointer(&src[a]))
		return
	}
	if n <= 16 && out+16 <= end && a+16 <= len(src) {
		*(*[16]byte)(unsafe.Pointer(&dst[out])) = *(*[16]byte)(unsafe.Pointer(&src[a]))
		return
	}
	copy(dst[out:out+n], src[a:b])
}

func (v TemplateColumnarLeafView) AppendRaw(dst []byte, slot uint8, key []byte) ([]byte, bool) {
	rank := v.slotRanks[slot]
	if rank == 0xff || int(rank) >= int(v.count) {
		return dst, false
	}
	got, ti, ok := v.row(int(rank))
	if !ok || !bytes.Equal(got, key) {
		return dst, false
	}
	return v.appendRawDirect(dst, int(rank), ti), true
}

func (v TemplateColumnarLeafView) appendRawDirect(dst []byte, rank int, ti uint16) []byte {
	tat := int(ti) * templateColumnarLeafTplBytes
	first := int(binary.LittleEndian.Uint32(v.tplDir[tat+48:]))
	n := int(binary.LittleEndian.Uint32(v.tplDir[tat+52:]))
	ss := int(binary.LittleEndian.Uint32(v.tplDir[tat+40:]))
	se := int(binary.LittleEndian.Uint32(v.tplDir[tat+44:]))
	at := rank * templateColumnarLeafRowBytes
	lp := int(binary.LittleEndian.Uint32(v.rowDir[at+12:]))
	le := int(binary.LittleEndian.Uint32(v.rowDir[at+16:]))
	dp := int(binary.LittleEndian.Uint32(v.rowDir[at+20:]))
	total := int(binary.LittleEndian.Uint16(v.rowDir[at+10:]))
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	out, sk := start, ss
	for i := 0; i < n; i++ {
		token := uint64(v.image[lp])
		lp++
		if token >= 0x80 {
			var z int
			token, z = binary.Uvarint(v.image[lp-1 : le])
			lp += z - 1
		}
		var fa, fb int
		if token&1 != 0 {
			id := uint64(v.image[dp])
			dp++
			if id >= 0x80 {
				var q int
				id, q = binary.Uvarint(v.image[dp-1:])
				dp += q - 1
			}
			fa = int(binary.LittleEndian.Uint32(v.valueDir[id*8:]))
			fb = int(binary.LittleEndian.Uint32(v.valueDir[id*8+4:]))
		} else {
			fa, fb = dp, dp+int(token>>1)
			dp = fb
		}
		ha := (first + i) * 8
		run := int(binary.LittleEndian.Uint16(v.holeDir[ha+4:]))
		templateColumnarLeafCopySpan(dst, out, v.image, sk, sk+run, start+total)
		out += run
		sk += run
		templateColumnarLeafCopySpan(dst, out, v.image, fa, fb, start+total)
		out += fb - fa
	}
	templateColumnarLeafCopyRun(dst[out:], v.image[sk:se])
	return dst
}

// appendRawBatched resolves every value source first, then executes a
// branch-free alternating skeleton/value copy program. Coalescing merges
// adjacent source spans (including zero-width separators) before execution.
// It is kept as an independently measurable lab mechanism; AppendRaw uses the
// fastest qualified implementation for the representative shape.
func (v TemplateColumnarLeafView) appendRawBatched(dst []byte, rank int, ti uint16, coalesce bool) ([]byte, int, int) {
	tat := int(ti) * templateColumnarLeafTplBytes
	first := int(binary.LittleEndian.Uint32(v.tplDir[tat+48:]))
	n := int(binary.LittleEndian.Uint32(v.tplDir[tat+52:]))
	if n > templateColumnarLeafMaxSpliceHoles {
		return v.appendRawDirect(dst, rank, ti), 2*n + 1, 2*n + 1
	}
	ss := int(binary.LittleEndian.Uint32(v.tplDir[tat+40:]))
	se := int(binary.LittleEndian.Uint32(v.tplDir[tat+44:]))
	at := rank * templateColumnarLeafRowBytes
	lp := int(binary.LittleEndian.Uint32(v.rowDir[at+12:]))
	le := int(binary.LittleEndian.Uint32(v.rowDir[at+16:]))
	dp := int(binary.LittleEndian.Uint32(v.rowDir[at+20:]))
	total := int(binary.LittleEndian.Uint16(v.rowDir[at+10:]))

	var spans [templateColumnarLeafMaxSpliceHoles*2 + 1]uint64
	for i := 0; i < n; i++ {
		token, z := binary.Uvarint(v.image[lp:le])
		lp += z
		var a, b int
		if token&1 != 0 {
			id, q := binary.Uvarint(v.image[dp:])
			dp += q
			a = int(binary.LittleEndian.Uint32(v.valueDir[id*8:]))
			b = int(binary.LittleEndian.Uint32(v.valueDir[id*8+4:]))
		} else {
			a, b = dp, dp+int(token>>1)
			dp = b
		}
		spans[i*2+1] = uint64(uint32(a))<<32 | uint64(uint32(b))
	}
	sk := ss
	for i := 0; i < n; i++ {
		ha := (first + i) * 8
		run := int(binary.LittleEndian.Uint16(v.holeDir[ha+4:]))
		spans[i*2] = uint64(uint32(sk))<<32 | uint64(uint32(sk+run))
		sk += run
	}
	spans[n*2] = uint64(uint32(sk))<<32 | uint64(uint32(se))
	before, after := 0, 0
	for i := 0; i < 2*n+1; i++ {
		a, b := uint32(spans[i]>>32), uint32(spans[i])
		if a == b {
			continue
		}
		before++
		if coalesce && after > 0 && uint32(spans[after-1]) == a {
			spans[after-1] = spans[after-1]&0xffffffff00000000 | uint64(b)
			continue
		}
		spans[after] = spans[i]
		after++
	}
	if !coalesce {
		after = before
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	out := start
	for i := 0; i < after; i++ {
		a, b := int(uint32(spans[i]>>32)), int(uint32(spans[i]))
		templateColumnarLeafCopyRun(dst[out:out+b-a], v.image[a:b])
		out += b - a
	}
	return dst, before, after
}

// appendRawSkeletonFirst copies the compact skeleton once, expands its runs
// in-place from the back, then overlays the already-resolved values.
func (v TemplateColumnarLeafView) appendRawSkeletonFirst(dst []byte, rank int, ti uint16) []byte {
	tat := int(ti) * templateColumnarLeafTplBytes
	first := int(binary.LittleEndian.Uint32(v.tplDir[tat+48:]))
	n := int(binary.LittleEndian.Uint32(v.tplDir[tat+52:]))
	if n > templateColumnarLeafMaxSpliceHoles {
		return v.appendRawDirect(dst, rank, ti)
	}
	ss := int(binary.LittleEndian.Uint32(v.tplDir[tat+40:]))
	se := int(binary.LittleEndian.Uint32(v.tplDir[tat+44:]))
	at := rank * templateColumnarLeafRowBytes
	lp := int(binary.LittleEndian.Uint32(v.rowDir[at+12:]))
	le := int(binary.LittleEndian.Uint32(v.rowDir[at+16:]))
	dp := int(binary.LittleEndian.Uint32(v.rowDir[at+20:]))
	total := int(binary.LittleEndian.Uint16(v.rowDir[at+10:]))
	var values [templateColumnarLeafMaxSpliceHoles]uint64
	for i := 0; i < n; i++ {
		token, z := binary.Uvarint(v.image[lp:le])
		lp += z
		var a, b int
		if token&1 != 0 {
			id, q := binary.Uvarint(v.image[dp:])
			dp += q
			a = int(binary.LittleEndian.Uint32(v.valueDir[id*8:]))
			b = int(binary.LittleEndian.Uint32(v.valueDir[id*8+4:]))
		} else {
			a, b = dp, dp+int(token>>1)
			dp = b
		}
		values[i] = uint64(uint32(a))<<32 | uint64(uint32(b))
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	skeletonBytes := se - ss
	copy(dst[start:start+skeletonBytes], v.image[ss:se])
	srcEnd, dstEnd := skeletonBytes, total
	for i := n; i >= 0; i-- {
		var run int
		if i == n {
			if n == 0 {
				run = skeletonBytes
			} else {
				lastHole := (first + n - 1) * 8
				run = skeletonBytes - int(binary.LittleEndian.Uint32(v.holeDir[lastHole:]))
			}
		} else {
			run = int(binary.LittleEndian.Uint16(v.holeDir[(first+i)*8+4:]))
		}
		srcStart, dstStart := srcEnd-run, dstEnd-run
		if srcStart != dstStart {
			copy(dst[start+dstStart:start+dstEnd], dst[start+srcStart:start+srcEnd])
		}
		srcEnd, dstEnd = srcStart, dstStart
		if i > 0 {
			a, b := int(uint32(values[i-1]>>32)), int(uint32(values[i-1]))
			dstEnd -= b - a
		}
	}
	out := start
	for i := 0; i < n; i++ {
		run := int(binary.LittleEndian.Uint16(v.holeDir[(first+i)*8+4:]))
		out += run
		a, b := int(uint32(values[i]>>32)), int(uint32(values[i]))
		copy(dst[out:out+b-a], v.image[a:b])
		out += b - a
	}
	return dst
}

// AppendEqualRaw is the lab's predicated scan: it probes the addressed field
// first and runs the compiled splice only for survivors.
func (v TemplateColumnarLeafView) AppendEqualRaw(dst []byte, template, hole uint16, want []byte) ([]byte, int) {
	survivors := 0
	for rank := 0; rank < int(v.count); rank++ {
		key, rowTemplate, ok := v.row(rank)
		if !ok || rowTemplate != template {
			continue
		}
		field, _, ok := v.fieldRank(rank, hole)
		if !ok || !bytes.Equal(field, want) {
			continue
		}
		var appended bool
		dst, appended = v.AppendRaw(dst, v.rankSlots[rank], key)
		if !appended {
			return dst, survivors
		}
		survivors++
	}
	return dst, survivors
}

func PatchTemplateColumnarLeafFieldFixed(image []byte, slot uint8, hole uint16, replacement []byte) error {
	v, err := OpenTemplateColumnarLeaf(image)
	if err != nil {
		return err
	}
	return patchTemplateColumnarLeafFieldFixedAdmitted(v, slot, hole, replacement)
}

func patchTemplateColumnarLeafFieldFixedAdmitted(v TemplateColumnarLeafView, slot uint8, hole uint16, replacement []byte) error {
	rank := v.slotRanks[slot]
	if rank == 0xff {
		return ErrTemplateColumnarLeafShape
	}
	field, _, ok := v.fieldRank(int(rank), hole)
	if !ok || len(field) != len(replacement) {
		return ErrTemplateColumnarLeafShape
	}
	// Dictionary values are shared and cannot be patched through a row.
	fieldOffset := uintptr(unsafe.Pointer(&field[0])) - uintptr(unsafe.Pointer(&v.image[0]))
	if fieldOffset < uintptr(v.dictEnd) {
		return ErrTemplateColumnarLeafShape
	}
	zone, ok := v.Zone(func() uint16 { _, t, _ := v.row(int(rank)); return t }(), hole)
	if !ok || bytes.Compare(replacement, zone.Min) < 0 || bytes.Compare(replacement, zone.Max) > 0 || (!bytes.Equal(field, replacement) && (bytes.Equal(field, zone.Min) || bytes.Equal(field, zone.Max))) {
		return ErrTemplateColumnarLeafShape
	}
	copy(field, replacement)
	image := v.image
	t := image[v.dataEnd:]
	binary.LittleEndian.PutUint32(t[8:12], PageChecksum(image[v.dictEnd:v.dataEnd]))
	binary.LittleEndian.PutUint32(t[12:16], PageChecksum(t[:12]))
	return nil
}

func ResealTemplateColumnarLeaf(image []byte) error {
	if _, ok := parseTemplateColumnarLeafDirectories(image); !ok {
		return ErrTemplateColumnarLeafCorrupt
	}
	sealTemplateColumnarLeaf(image)
	return nil
}

// MetadataBytesPerDocument exposes the exact v2 row-directory plus length
// vector cost and the v1 offset-directory equivalent for qualification output.
func (v TemplateColumnarLeafView) MetadataBytesPerDocument() (before, after float64) {
	before = float64(int(v.columns)*4*(int(v.count)+1)) / float64(v.count)
	lengthBytes := 0
	for rank := 0; rank < int(v.count); rank++ {
		at := rank * templateColumnarLeafRowBytes
		lengthBytes += int(binary.LittleEndian.Uint32(v.rowDir[at+16:]) - binary.LittleEndian.Uint32(v.rowDir[at+12:]))
	}
	after = float64(lengthBytes) / float64(v.count)
	return
}
