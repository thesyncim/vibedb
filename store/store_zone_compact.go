package store

import (
	"encoding/binary"
)

// The persistable form of a chunk summary.
//
// store_zone.go's chunkZone is a 144-byte in-memory value embedded in a heap
// Chunk, where the only budget that matters is RAM per chunk. A durable
// backend has a second, much harder budget: the summary has to live somewhere
// on disk that can be read *without* reading the chunk it describes, or it
// buys nothing — a pruned chunk has to be a page never faulted, not a page
// read and then discarded.
//
// The place that satisfies that in store/durable's format is the chunk
// directory's radix leaf: it is the page a reader already has to open to learn
// where a chunk lives, and the page a writer already has to rewrite when a
// chunk moves. Putting the summary there costs zero extra reads on the probe
// path and zero extra writes on the commit path, and — the property that
// matters most — makes the summary and the chunk reference the *same* bytes
// under the same checksum in the same copy-on-write generation, so no crash
// can publish one without the other.
//
// What it costs is space. A 4096-byte page holds 4024 payload bytes; a full
// 64-lane leaf spends 32 on its own header and 2048 on its references, leaving
// 1944, or 30 bytes per lane. That is the whole budget, and this file is the
// summary that fits in it:
//
//   - three tracked paths instead of eight,
//   - a 16-bit path tag instead of a 64-bit hash,
//   - 24-bit value codes instead of 32-bit.
//
// Each of those is a precision loss, and each is a *sound* one, for the same
// reason the 29-bit number payload in store_zone.go is: they only merge
// distinct things onto one code or one entry, never reorder them and never
// narrow an interval. The soundness argument in store_zone.go's header applies
// here unchanged; what follows is only the part that differs.
//
// # 16-bit tags are sound because entries merge, not replace
//
// A tag collision between two top-level member names would be a false-negative
// generator if an entry meant "the statistics of path P". It does not: an
// entry means "the statistics of every path whose tag is T". observe() finds
// an entry by tag and merges into it, so a chunk holding both "price" and some
// unlucky "colour" with the same tag has one entry describing the union of
// both. A probe for either recovers bounds that cover it. The cost is a wider
// interval on one entry in roughly one chunk in 2^16 per path pair, which is a
// scan that could have been skipped — never a row that should have been
// returned and was not.
//
// # 24-bit codes are sound because they are stored as an interval
//
// A 32-bit zone code is stored as its top 24 bits. The minimum is rounded
// down (code>>8) and the maximum rounded up (code>>8, read back as
// max<<8|0xff), so the decoded interval always contains the exact one. Probes
// are compiled to full 32-bit codes by the same query-side code that serves
// the heap backend, and compared against the widened interval, so one
// ZoneProbe drives both backends and neither can drift from the other's idea
// of the order.
//
// The resolution left is 21 bits of payload: for numbers, sign, exponent, and
// nine mantissa bits; for strings, a little over two and a half bytes of
// prefix. That is enough to separate chunks of a clustered column and not
// enough to separate chunks of a shuffled one — which is a statement about
// what zone maps can do at all, not about the truncation.

// ZoneCompactPaths is how many distinct top-level member names one durable
// chunk summary tracks. The cap is the same first-come, never-evict policy
// store_zone.go documents at length, for the same soundness reason; only the
// number differs, and it differs because 30 bytes per chunk is what a full
// chunk-directory leaf has left over.
const ZoneCompactPaths = 3

// ZoneCompactBytes is the encoded width of one summary. It is a hard budget,
// not a measurement: exceeding it would not make the page bigger, it would
// make the page invalid.
const ZoneCompactBytes = 30

// Compact summary states. The zero value is deliberately ZoneCompactStale, so
// a zeroed or absent record — a lane in a leaf written before summaries
// existed, or one a bulk builder chose not to fill — decodes as "no statistics
// yet, recompute when convenient" rather than as a sound summary of nothing.
const (
	// ZoneCompactStale is a summary that does not cover its chunk's documents.
	// Every probe keeps the chunk; the next writer that has the chunk's rows in
	// hand recomputes it.
	ZoneCompactStale uint8 = iota
	// ZoneCompactOK is a summary that covers every live document in its chunk.
	ZoneCompactOK
	// ZoneCompactPoisoned is a chunk holding a document the fold path cannot
	// summarize — today only an escaped top-level member name. Recomputing
	// would meet the same document again, so the state is permanent for that
	// chunk's lineage and no further write-path work is spent on it.
	ZoneCompactPoisoned
)

// ZoneSummary is one durable chunk's summary in decoded form. It is a plain
// value: callers keep it in a struct field or on the stack, and it never
// allocates.
type ZoneSummary struct {
	tag  [ZoneCompactPaths]uint16
	min  [ZoneCompactPaths]uint32
	max  [ZoneCompactPaths]uint32
	flag [ZoneCompactPaths]uint8
	n    uint8
	// overflow records that the per-chunk path budget turned a path away, and
	// is what lets a *missing* entry mean "no document in this chunk carried
	// that path". With only three entries a heterogeneous corpus sets it far
	// more often than the heap's eight would, which is exactly why it is
	// tracked rather than assumed.
	overflow bool
	state    uint8
}

// zoneCompactTag derives the stored 16-bit tag from the full 64-bit path hash.
// The high bits are taken because ZonePathHash finishes with an avalanche, so
// every output bit already depends on every input byte; taking the top word
// keeps this identical to what a wider tag would have stored in its high half.
func zoneCompactTag(hash uint64) uint16 { return uint16(hash >> 48) }

// ZoneCompactTag is zoneCompactTag for callers outside this package that hold
// a [ZonePathHash] result — the durable backend's probe path.
func ZoneCompactTag(hash uint64) uint16 { return zoneCompactTag(hash) }

// Reset returns z to an empty, sound summary: no entries, nothing turned away,
// and every future fold accepted. It is what a brand-new chunk starts from,
// and what a rebuild starts from.
func (z *ZoneSummary) Reset() {
	*z = ZoneSummary{state: ZoneCompactOK}
}

// State reports whether the summary covers its chunk.
func (z *ZoneSummary) State() uint8 { return z.state }

// Sound reports whether any probe against this summary can prune.
func (z *ZoneSummary) Sound() bool { return z.state == ZoneCompactOK }

// Paths reports how many entries the summary carries. It exists for tests and
// stats reporting and is not on any execution path.
func (z *ZoneSummary) Paths() int { return int(z.n) }

// Encode writes the summary into its fixed 30-byte durable form.
//
// The layout packs state, overflow, the entry count, and all nine flag bits
// into one 16-bit word so the three ten-byte entries that follow are the only
// variable content. Trailing bytes are written zero and validated zero on
// decode, which is what makes a later field a schema edit rather than a
// layout change.
func (z *ZoneSummary) Encode(dst *[ZoneCompactBytes]byte) {
	status := uint16(z.state&3) | uint16(z.n&3)<<3
	if z.overflow {
		status |= 1 << 2
	}
	for i := 0; i < ZoneCompactPaths; i++ {
		status |= uint16(z.flag[i]&7) << (5 + 3*i)
	}
	binary.LittleEndian.PutUint16(dst[0:2], status)
	for i := 0; i < ZoneCompactPaths; i++ {
		binary.LittleEndian.PutUint16(dst[2+2*i:4+2*i], z.tag[i])
		zonePut24(dst[8+3*i:], z.min[i])
		zonePut24(dst[17+3*i:], z.max[i])
	}
	dst[26], dst[27], dst[28], dst[29] = 0, 0, 0, 0
}

// Decode reads a summary from its durable form. It never reports an error: the
// bytes are covered by the page checksum, and every field it could object to
// has a safe reading. An out-of-range entry count or a reserved bit that is
// set means the record was written by something this build does not
// understand, and the honest interpretation of that is "no statistics", which
// keeps every chunk rather than pruning on a record it cannot read.
func (z *ZoneSummary) Decode(src *[ZoneCompactBytes]byte) {
	status := binary.LittleEndian.Uint16(src[0:2])
	*z = ZoneSummary{
		state:    uint8(status & 3),
		overflow: status&(1<<2) != 0,
		n:        uint8(status >> 3 & 3),
	}
	if z.state > ZoneCompactPoisoned || z.n > ZoneCompactPaths ||
		status>>14 != 0 || src[26]|src[27]|src[28]|src[29] != 0 {
		*z = ZoneSummary{state: ZoneCompactStale}
		return
	}
	for i := 0; i < ZoneCompactPaths; i++ {
		z.flag[i] = uint8(status >> (5 + 3*i) & 7)
		z.tag[i] = binary.LittleEndian.Uint16(src[2+2*i : 4+2*i])
		z.min[i] = zoneGet24(src[8+3*i:])
		z.max[i] = zoneGet24(src[17+3*i:])
	}
}

func zonePut24(dst []byte, v uint32) {
	dst[0], dst[1], dst[2] = byte(v), byte(v>>8), byte(v>>16)
}

func zoneGet24(src []byte) uint32 {
	return uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16
}

// Poison marks the summary permanently unusable. The durable write path calls
// it when it is handed a chunk it cannot summarize for a reason recomputation
// would hit again.
func (z *ZoneSummary) Poison() { *z = ZoneSummary{state: ZoneCompactPoisoned} }

// Stale marks the summary as not covering its chunk, which asks the next
// writer holding the chunk's rows to recompute it.
func (z *ZoneSummary) Stale() { *z = ZoneSummary{state: ZoneCompactStale} }

// Fold folds one document's source into z, with priorDocs the number of live
// documents already summarized. It is store_zone.go's fold with this file's
// narrower entry table, and it makes exactly the same first-sighting deduction:
// an entry created after priorDocs documents have been folded records that
// those documents did not carry the path, which is sound only because the table
// never evicts.
func (z *ZoneSummary) Fold(src []byte, priorDocs int) {
	if z.state != ZoneCompactOK {
		return
	}
	var seen uint32
	i := zoneSkipSpace(src, 0)
	if i >= len(src) || src[i] != '{' {
		z.markAbsent(seen)
		return
	}
	i = zoneSkipSpace(src, i+1)
	if i < len(src) && src[i] == '}' {
		z.markAbsent(seen)
		return
	}
	for {
		if i >= len(src) || src[i] != '"' {
			z.state = ZoneCompactPoisoned
			return
		}
		keyStart := i + 1
		end, escaped, ok := zoneScanString(src, i)
		if !ok || escaped {
			z.state = ZoneCompactPoisoned
			return
		}
		name := src[keyStart : end-1]
		i = zoneSkipSpace(src, end)
		if i >= len(src) || src[i] != ':' {
			z.state = ZoneCompactPoisoned
			return
		}
		i = zoneSkipSpace(src, i+1)
		valueStart := i
		valueEnd, ok := zoneSkipValue(src, i)
		if !ok {
			z.state = ZoneCompactPoisoned
			return
		}
		if slot := z.observe(name, priorDocs); slot >= 0 {
			seen |= 1 << uint(slot)
			lo, hi, flag := zoneValueCodes(src[valueStart:valueEnd])
			z.flag[slot] |= flag
			if flag == zoneFlagValue {
				if lo24 := lo >> 8; z.min[slot] > lo24 {
					z.min[slot] = lo24
				}
				if hi24 := hi >> 8; z.max[slot] < hi24 {
					z.max[slot] = hi24
				}
			}
		}
		i = zoneSkipSpace(src, valueEnd)
		if i >= len(src) {
			z.state = ZoneCompactPoisoned
			return
		}
		switch src[i] {
		case ',':
			i = zoneSkipSpace(src, i+1)
		case '}':
			z.markAbsent(seen)
			return
		default:
			z.state = ZoneCompactPoisoned
			return
		}
	}
}

func (z *ZoneSummary) observe(name []byte, priorDocs int) int {
	tag := zoneCompactTag(zonePathHashBytes(name))
	for i := 0; i < int(z.n); i++ {
		if z.tag[i] == tag {
			return i
		}
	}
	if int(z.n) == ZoneCompactPaths {
		z.overflow = true
		return -1
	}
	slot := int(z.n)
	z.n++
	z.tag[slot] = tag
	z.min[slot] = zoneCompact24Max
	z.max[slot] = 0
	z.flag[slot] = 0
	if priorDocs > 0 {
		z.flag[slot] = zoneFlagAbsent
	}
	return slot
}

const zoneCompact24Max = uint32(1)<<24 - 1

func (z *ZoneSummary) markAbsent(seen uint32) {
	for i := 0; i < int(z.n); i++ {
		if seen&(1<<uint(i)) == 0 {
			z.flag[i] |= zoneFlagAbsent
		}
	}
}

// Keep reports whether a chunk whose summary is z may contain a row matching
// probe. It is store_zone.go's keep with the tag narrowed and the stored
// interval widened back to 32 bits before every comparison, so the two
// backends answer one compiled ZoneProbe by the same rules.
func (z *ZoneSummary) Keep(probe ZoneProbe) bool {
	if z.state != ZoneCompactOK || probe.Op == ZoneOpNone {
		return true
	}
	tag := zoneCompactTag(probe.Path)
	slot := -1
	for i := 0; i < int(z.n); i++ {
		if z.tag[i] == tag {
			slot = i
			break
		}
	}
	if slot < 0 {
		if z.overflow {
			return true
		}
		// No document in this chunk carried any path with this tag, so it
		// carried this path in none of them either. Every cell is absent, which
		// satisfies IS NULL and fails EXISTS and every comparison.
		return probe.Op == ZoneOpIsNull
	}
	flag := z.flag[slot]
	switch probe.Op {
	case ZoneOpIsNull:
		return flag&(zoneFlagAbsent|zoneFlagNull) != 0
	case ZoneOpExists:
		return flag&(zoneFlagNull|zoneFlagValue) != 0
	}
	if flag&zoneFlagValue == 0 {
		return false
	}
	// Widen the stored 24-bit interval back to the 32-bit code space the probe
	// was compiled in. The low byte of the minimum is unknown and taken as
	// zero; the low byte of the maximum is unknown and taken as 0xff.
	mn := z.min[slot] << 8
	mx := z.max[slot]<<8 | 0xff
	switch probe.Op {
	case ZoneOpEq:
		return probe.Lo <= mx && probe.Hi >= mn
	case ZoneOpLt, ZoneOpLe:
		return mn <= probe.Hi
	case ZoneOpGt, ZoneOpGe:
		return mx >= probe.Lo
	default:
		return true
	}
}
