package tin

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

// Snapshot wire format for persisting a complete tin.Index across process
// restarts. Every section is length-prefixed and bounds-checked before
// allocation, and the entire payload is protected by a trailing SHA-256
// digest so any truncation, corruption, or version mismatch fails closed.
//
// Layout (little-endian):
//
//	magic "tinI" (4) | version u8 (=1)
//	nDocs u32 | tokens u64 | minDocLen u32
//	dict: nDict u32 | [ hash u64 | spellLen u32 | spell bytes ]...
//	docs: nDocs u32 | [ docID u64 | length u32 | nTerms u32 | terms[nTerms] u64 ]...
//	post: nPost u32 | [ hash u64 | kind u8 (1=sealed, 0=open) | payload ]...
//	  sealed payload: byteLen u32 | MarshalSealed bytes
//	  open payload:   n u32 | ids[n] u64 | off[n+1] u32 | nPos u32 | pos[nPos] u32
//	sha256 (32 bytes over all preceding bytes)
const (
	snapshotWireMagic   = "tinI"
	snapshotWireVersion = 1
)

// MarshalSnapshot encodes ix into a self-contained, SHA-256-checksummed byte
// slice that UnmarshalSnapshot restores directly without re-tokenizing or
// re-scanning documents. Sealed posting lists are written in their compact
// bit-packed form via MarshalSealed.
func (ix *Index) MarshalSnapshot() ([]byte, error) {
	if ix == nil {
		return nil, fmt.Errorf("tin snapshot wire: nil index")
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.ensureSorted()

	out := make([]byte, 0, 256+len(ix.docs)*32+len(ix.post)*64)
	out = append(out, snapshotWireMagic...)
	out = append(out, snapshotWireVersion)
	out = binary.LittleEndian.AppendUint32(out, uint32(ix.nDocs))
	out = binary.LittleEndian.AppendUint64(out, ix.tokens)
	out = binary.LittleEndian.AppendUint32(out, ix.minDocLen)

	// Dictionary in deterministic spellIdx order.
	out = binary.LittleEndian.AppendUint32(out, uint32(len(ix.spellIdx)))
	for _, entry := range ix.spellIdx {
		out = binary.LittleEndian.AppendUint64(out, entry.hash)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(entry.spell)))
		out = append(out, entry.spell...)
	}

	// Documents in deterministic DocID order.
	docIDs := make([]DocID, 0, len(ix.docs))
	for id := range ix.docs {
		docIDs = append(docIDs, id)
	}
	sort.Slice(docIDs, func(i, j int) bool { return docIDs[i] < docIDs[j] })
	out = binary.LittleEndian.AppendUint32(out, uint32(len(docIDs)))
	for _, id := range docIDs {
		meta := ix.docs[id]
		out = binary.LittleEndian.AppendUint64(out, uint64(id))
		out = binary.LittleEndian.AppendUint32(out, meta.length)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(meta.terms)))
		for _, h := range meta.terms {
			out = binary.LittleEndian.AppendUint64(out, h)
		}
	}

	// Postings in deterministic hash order.
	hashes := make([]uint64, 0, len(ix.post))
	for h := range ix.post {
		hashes = append(hashes, h)
	}
	sort.Slice(hashes, func(i, j int) bool { return hashes[i] < hashes[j] })
	out = binary.LittleEndian.AppendUint32(out, uint32(len(hashes)))
	for _, h := range hashes {
		p := ix.post[h]
		out = binary.LittleEndian.AppendUint64(out, h)
		if p.sealed != nil {
			sealedBytes, err := MarshalSealed(p.sealed)
			if err != nil {
				return nil, fmt.Errorf("tin snapshot wire: marshal sealed term %x: %w", h, err)
			}
			out = append(out, 1)
			out = binary.LittleEndian.AppendUint32(out, uint32(len(sealedBytes)))
			out = append(out, sealedBytes...)
			continue
		}
		out = append(out, 0)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(p.ids)))
		for _, id := range p.ids {
			out = binary.LittleEndian.AppendUint64(out, uint64(id))
		}
		for _, off := range p.off {
			out = binary.LittleEndian.AppendUint32(out, off)
		}
		out = binary.LittleEndian.AppendUint32(out, uint32(len(p.pos)))
		for _, pos := range p.pos {
			out = binary.LittleEndian.AppendUint32(out, pos)
		}
	}

	sum := sha256.Sum256(out)
	out = append(out, sum[:]...)
	return out, nil
}

// UnmarshalSnapshot decodes a byte slice produced by MarshalSnapshot,
// verifying the trailing SHA-256 digest and every structural bound before
// allocating. Any mismatch, truncation, or corruption fails closed.
func UnmarshalSnapshot(b []byte) (*Index, error) {
	if len(b) < 4+1+sha256.Size {
		return nil, fmt.Errorf("tin snapshot wire: truncated")
	}
	payload := b[:len(b)-sha256.Size]
	wantSum := b[len(b)-sha256.Size:]
	gotSum := sha256.Sum256(payload)
	if string(gotSum[:]) != string(wantSum) {
		return nil, fmt.Errorf("tin snapshot wire: checksum mismatch")
	}
	r := &sealReader{b: payload}
	if string(r.bytes(4)) != snapshotWireMagic {
		return nil, fmt.Errorf("tin snapshot wire: bad magic")
	}
	if ver := r.byte(); ver != snapshotWireVersion {
		return nil, fmt.Errorf("tin snapshot wire: unsupported version %d", ver)
	}
	nDocs := int(r.u32())
	tokens := r.u64()
	minDocLen := r.u32()

	nDict := int(r.u32())
	if r.err != nil {
		return nil, r.err
	}
	if nDict > len(r.b)/12 {
		return nil, fmt.Errorf("tin snapshot wire: bad dict count %d", nDict)
	}
	ix := NewIndex()
	ix.spellIdx = make([]spellEntry, nDict)
	for i := 0; i < nDict; i++ {
		h := r.u64()
		slen := int(r.u32())
		if r.err != nil {
			return nil, r.err
		}
		if slen > len(r.b) {
			return nil, fmt.Errorf("tin snapshot wire: bad spell len %d", slen)
		}
		spell := string(r.bytes(slen))
		ix.dict[h] = spell
		ix.spellIdx[i] = spellEntry{spell: spell, hash: h}
		if len(spell) > ix.maxSpell {
			ix.maxSpell = len(spell)
		}
	}

	docCount := int(r.u32())
	if r.err != nil {
		return nil, r.err
	}
	if docCount != nDocs || docCount > len(r.b)/16 {
		return nil, fmt.Errorf("tin snapshot wire: bad doc count %d (header %d)", docCount, nDocs)
	}
	ix.docs = make(map[DocID]docMeta, docCount)
	for i := 0; i < docCount; i++ {
		id := DocID(r.u64())
		length := r.u32()
		nTerms := int(r.u32())
		if r.err != nil {
			return nil, r.err
		}
		if nTerms > len(r.b)/8 {
			return nil, fmt.Errorf("tin snapshot wire: bad doc terms count %d", nTerms)
		}
		var terms []uint64
		if nTerms > 0 {
			terms = make([]uint64, nTerms)
			for j := 0; j < nTerms; j++ {
				terms[j] = r.u64()
			}
		}
		if r.err != nil {
			return nil, r.err
		}
		ix.noteDocLength(id, length)
		ix.docs[id] = docMeta{length: length, terms: terms}
	}
	ix.nDocs = nDocs
	ix.tokens = tokens
	ix.minDocLen = minDocLen

	nPost := int(r.u32())
	if r.err != nil {
		return nil, r.err
	}
	if nPost > len(r.b)/9 {
		return nil, fmt.Errorf("tin snapshot wire: bad postings count %d", nPost)
	}
	ix.post = make(map[uint64]*postings, nPost)
	for i := 0; i < nPost; i++ {
		h := r.u64()
		kind := r.byte()
		if r.err != nil {
			return nil, r.err
		}
		switch kind {
		case 1:
			blen := int(r.u32())
			if r.err != nil || blen > len(r.b) {
				return nil, fmt.Errorf("tin snapshot wire: bad sealed length %d", blen)
			}
			sb := r.bytes(blen)
			s, err := UnmarshalSealed(sb)
			if err != nil {
				return nil, fmt.Errorf("tin snapshot wire: unmarshal sealed term %x: %w", h, err)
			}
			ix.post[h] = &postings{sealed: s}
		case 0:
			n := int(r.u32())
			if r.err != nil {
				return nil, r.err
			}
			// Need n*8 bytes for ids + (n+1)*4 bytes for off + 4 bytes for nPos.
			if n > len(r.b)/12 || len(r.b) < n*8+(n+1)*4+4 {
				return nil, fmt.Errorf("tin snapshot wire: bad open list count %d", n)
			}
			ids := make([]DocID, n)
			for j := 0; j < n; j++ {
				ids[j] = DocID(r.u64())
			}
			off := make([]uint32, n+1)
			for j := 0; j <= n; j++ {
				off[j] = r.u32()
			}
			nPos := int(r.u32())
			if r.err != nil || nPos > len(r.b)/4 {
				return nil, fmt.Errorf("tin snapshot wire: bad open pos count %d", nPos)
			}
			pos := make([]uint32, nPos)
			for j := 0; j < nPos; j++ {
				pos[j] = r.u32()
			}
			if r.err != nil {
				return nil, r.err
			}
			if off[0] != 0 || int(off[n]) != nPos {
				return nil, fmt.Errorf("tin snapshot wire: open offset accounting mismatch")
			}
			ix.post[h] = &postings{ids: ids, off: off, pos: pos}
		default:
			return nil, fmt.Errorf("tin snapshot wire: unknown posting kind %d", kind)
		}
	}
	if len(r.b) != 0 {
		return nil, fmt.Errorf("tin snapshot wire: %d trailing bytes", len(r.b))
	}
	ix.sorted = true
	return ix, nil
}
