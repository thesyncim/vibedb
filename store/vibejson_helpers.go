package store

import (
	"encoding/binary"
	"math/bits"

	"github.com/thesyncim/vibedb/internal/jsoncmp"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// FxHash-style mixing constants matching vibejson's compiled key hashes.
const (
	keyHashSeed = 0x9E3779B97F4A7C15
	keyHashMul  = 0xFF51AFD7ED558CCD
)

func keyHashInit(n int) uint64 {
	return keyHashSeed ^ uint64(n)*keyHashMul
}

func keyHashMix(h, w uint64) uint64 {
	return (h ^ w) * keyHashMul
}

func keyHashFinish(h uint64) uint32 {
	h ^= h >> 29
	h *= keyHashSeed
	return uint32(h >> 32)
}

func hashKeyContent(content []byte) uint32 {
	n := len(content)
	h := keyHashInit(n)
	switch {
	case n >= 8:
		for i := 8; i < n; i += 8 {
			h = keyHashMix(h, binary.LittleEndian.Uint64(content[i-8:i]))
		}
		h = keyHashMix(h, binary.LittleEndian.Uint64(content[n-8:n]))
	case n >= 4:
		lo := uint64(binary.LittleEndian.Uint32(content[:4]))
		hi := uint64(binary.LittleEndian.Uint32(content[n-4:]))
		h = keyHashMix(h, lo|hi<<(8*(uint(n)-4)))
	default:
		var w uint64
		if n > 0 {
			w = uint64(content[0]) |
				uint64(content[n>>1])<<(8*(uint(n)>>1)) |
				uint64(content[n-1])<<(8*(uint(n)-1))
		}
		h = keyHashMix(h, w)
	}
	return keyHashFinish(h)
}

func hashKey(key string) uint32 {
	return hashKeyContent(byteview.Bytes(key))
}

func enrichKeyHashes(index *vibejson.Index) {
	src := index.Src
	entries := index.Entries
	for i := range entries {
		e := &entries[i]
		if e.Flags()&document.TapeFlagKey != 0 {
			e.Next = hashKeyContent(src[e.Start+1 : e.End-1])
		} else if e.Kind() == document.Object {
			e.Info |= uint32(document.TapeFlagObjectKeysHashed) << document.InfoFlagsShift
		}
	}
}

func isJSONWhitespace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

func skipSpace(src []byte, pos int) int {
	for pos < len(src) && isJSONWhitespace(src[pos]) {
		pos++
	}
	return pos
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func bytesEqualString(b []byte, s string) bool {
	return byteview.String(b) == s
}

func rawJSONStringEqual(a []byte, aFlags uint8, b []byte, bFlags uint8) bool {
	return jsoncmp.RawJSONStringEqual(a, aFlags, b, bFlags)
}

func nodeFromEntries(src []byte, entries []vibejson.IndexEntry) vibejson.Node {
	if len(entries) == 0 {
		return vibejson.Node{}
	}
	return vibejson.Index{Src: src, Entries: entries}.Root()
}

func probeCapacity(count int) int {
	return 1 << bits.Len(uint(2*count-1))
}

// tapeInt64 parses a tape-classified plain integer (optional '-', then digits)
// and reports overflow.
func tapeInt64(src *byte, start, end uint32) (int64, bool) {
	buf := byteview.SliceRange(src, start, end)
	if len(buf) == 0 {
		return 0, false
	}
	i := 0
	negative := buf[0] == '-'
	if negative {
		i++
		if i >= len(buf) {
			return 0, false
		}
	}
	var value uint64
	for ; i < len(buf); i++ {
		d := uint64(buf[i] - '0')
		if value > (1<<63)/10 {
			return 0, false
		}
		next := value*10 + d
		if next < value {
			return 0, false
		}
		value = next
	}
	if negative {
		if value > 1<<63 {
			return 0, false
		}
		return -int64(value), true
	}
	if value > 1<<63-1 {
		return 0, false
	}
	return int64(value), true
}
