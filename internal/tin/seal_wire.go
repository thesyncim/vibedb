package tin

import "fmt"

// Sealed wire format: the shippable unit for the distributed target.
// Immutable sealed term segments marshal to a versioned byte string a
// receiving node unmarshals straight into queryable sealedPostings — no
// re-indexing, no rescan. Integers are little-endian; slice lengths ride
// as u32 counts so unmarshal validates before allocating.
//
// The format is unreleased, so there is exactly one version and no
// legacy readers: rows ride as rows-1 in one byte (blocks hold 1..128
// rows), the per-document position bases pack at each block's own width,
// and the eight position checkpoints ride as u16, saturating at 0xFFFF
// exactly like the in-memory directory.
//
//	magic "tinS" (4) | version u8 (=1)
//	n u32 | occ u64 | nBlocks u32
//	first[nBlocks] u64
//	per block: rows-1 u8 | idW cntW posW baseW u8 |
//	  idOff cntOff posOff baseOff u32 | posCkpt[8] u16
//	idsLen u32 | ids[] u32 | cntsLen u32 | cnts[] u32 |
//	basesLen u32 | bases[] u32 | posLen u32 | pos[] u32
//
// Wire size is sealedBytes plus a small fixed header; marshal makes one
// allocation, unmarshal one per slice.
const (
	sealWireMagic   = "tinS"
	sealWireVersion = 1
)

// sealWireLen reports the exact marshaled size.
func sealWireLen(s *sealedPostings) int {
	const blkDir = 1 + 4 + 16 + sealedCkptPerBlock*2
	return 4 + 1 + 4 + 8 + 4 +
		8*len(s.first) +
		blkDir*len(s.blk) +
		4 + 4*len(s.ids) +
		4 + 4*len(s.cnts) +
		4 + 4*len(s.bases) +
		4 + 4*len(s.pos)
}

// MarshalSealed encodes one sealed list for shipment. It returns an
// error only for a structurally impossible list (nil or empty), never
// for ordinary sealed data.
func MarshalSealed(s *sealedPostings) ([]byte, error) {
	if s == nil || len(s.blk) == 0 {
		return nil, fmt.Errorf("tin seal wire: nothing to marshal")
	}
	out := make([]byte, 0, sealWireLen(s))
	out = append(out, sealWireMagic...)
	out = append(out, sealWireVersion)
	putU32 := func(v uint32) {
		out = append(out, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
	}
	putU16 := func(v uint16) {
		out = append(out, byte(v), byte(v>>8))
	}
	putU64 := func(v uint64) {
		putU32(uint32(v))
		putU32(uint32(v >> 32))
	}
	putU32(s.n)
	putU64(s.occ)
	putU32(uint32(len(s.blk)))
	for _, f := range s.first {
		putU64(f)
	}
	for i := range s.blk {
		bl := &s.blk[i]
		out = append(out, byte(bl.rows-1))
		out = append(out, bl.idW, bl.cntW, bl.posW, bl.baseW)
		putU32(bl.idOff)
		putU32(bl.cntOff)
		putU32(bl.posOff)
		putU32(bl.baseOff)
		for _, c := range bl.posCkpt {
			putU16(c)
		}
	}
	putStream := func(w []uint32) {
		putU32(uint32(len(w)))
		for _, v := range w {
			putU32(v)
		}
	}
	putStream(s.ids)
	putStream(s.cnts)
	putStream(s.bases)
	putStream(s.pos)
	return out, nil
}

// UnmarshalSealed decodes a shipped sealed list, validating structure
// before allocating: bad magic, version, widths, offsets, or row
// accounting all fail closed.
func UnmarshalSealed(b []byte) (*sealedPostings, error) {
	r := &sealReader{b: b}
	if string(r.bytes(4)) != sealWireMagic {
		return nil, fmt.Errorf("tin seal wire: bad magic")
	}
	if ver := r.byte(); ver != sealWireVersion {
		return nil, fmt.Errorf("tin seal wire: bad version")
	}
	s := &sealedPostings{}
	s.n = r.u32()
	s.occ = r.u64()
	nb := int(r.u32())
	if r.err != nil {
		return nil, r.err
	}
	// A block costs 8 first-bytes plus a 37-byte directory minimum,
	// so this bounds both makes by the bytes actually present.
	const minDir = 8 + 37
	if nb == 0 || nb > len(r.b)/minDir {
		return nil, fmt.Errorf("tin seal wire: bad block count %d", nb)
	}
	s.first = make([]uint64, nb)
	for i := range s.first {
		s.first[i] = r.u64()
	}
	s.blk = make([]sealedBlock, nb)
	rows := uint64(0)
	for i := range s.blk {
		bl := &s.blk[i]
		bl.rows = uint32(r.byte()) + 1
		bl.idW, bl.cntW, bl.posW, bl.baseW = r.byte(), r.byte(), r.byte(), r.byte()
		bl.idOff, bl.cntOff = r.u32(), r.u32()
		bl.posOff, bl.baseOff = r.u32(), r.u32()
		for j := range bl.posCkpt {
			bl.posCkpt[j] = r.u16()
		}
		if r.err != nil {
			return nil, r.err
		}
		if bl.rows == 0 || bl.rows > sealedBlockRows ||
			bl.idW == 0 || bl.idW > 32 || bl.cntW == 0 || bl.cntW > 32 ||
			bl.posW == 0 || bl.posW > 32 {
			return nil, fmt.Errorf("tin seal wire: bad block %d", i)
		}
		if bl.baseW == 0 || bl.baseW > 32 {
			return nil, fmt.Errorf("tin seal wire: bad block %d", i)
		}
		rows += uint64(bl.rows)
	}
	if rows != uint64(s.n) {
		return nil, fmt.Errorf("tin seal wire: row accounting %d != n %d", rows, s.n)
	}
	getStream := func(name string) []uint32 {
		n := int(r.u32())
		if r.err != nil {
			return nil
		}
		if n == 0 {
			// Preserve nil-ness: gapless lists leave streams nil
			// (packAppend never runs), and remarshal writes len 0
			// either way, so the wire stays byte-stable.
			return nil
		}
		// Bound the claim by the bytes actually remaining, before the
		// make: a corrupt length must fail closed, never over-allocate.
		if n > len(r.b)/4 {
			r.err = fmt.Errorf("tin seal wire: bad %s length %d", name, n)
			return nil
		}
		w := make([]uint32, n)
		for i := range w {
			w[i] = r.u32()
		}
		return w
	}
	s.ids = getStream("ids")
	s.cnts = getStream("cnts")
	s.bases = getStream("bases")
	s.pos = getStream("pos")
	if r.err != nil {
		return nil, r.err
	}
	for i := range s.blk {
		bl := &s.blk[i]
		baseEnd := uint64(bl.baseOff) + (uint64(bl.rows)*uint64(bl.baseW)+31)/32
		if baseEnd > uint64(len(s.bases)) {
			return nil, fmt.Errorf("tin seal wire: block %d bases escape stream", i)
		}
	}
	for i := range s.blk {
		bl := &s.blk[i]
		if uint64(bl.idOff) > uint64(len(s.ids)) || uint64(bl.cntOff) > uint64(len(s.cnts)) ||
			uint64(bl.posOff) > uint64(len(s.pos)) || uint64(bl.baseOff) > uint64(len(s.bases)) {
			return nil, fmt.Errorf("tin seal wire: block %d offsets escape streams", i)
		}
	}
	return s, nil
}

// sealReader is a bounds-checked little-endian cursor; the first fault
// sticks and fails the decode.
type sealReader struct {
	b   []byte
	err error
}

func (r *sealReader) bytes(n int) []byte {
	if r.err != nil {
		return nil
	}
	if len(r.b) < n {
		r.err = fmt.Errorf("tin seal wire: truncated")
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func (r *sealReader) byte() uint8 {
	if v := r.bytes(1); v != nil {
		return v[0]
	}
	return 0
}

func (r *sealReader) u16() uint16 {
	if v := r.bytes(2); v != nil {
		return uint16(v[0]) | uint16(v[1])<<8
	}
	return 0
}

func (r *sealReader) u32() uint32 {
	if v := r.bytes(4); v != nil {
		return uint32(v[0]) | uint32(v[1])<<8 | uint32(v[2])<<16 | uint32(v[3])<<24
	}
	return 0
}

func (r *sealReader) u64() uint64 {
	lo := uint64(r.u32())
	hi := uint64(r.u32())
	if r.err != nil {
		return 0
	}
	return lo | hi<<32
}
