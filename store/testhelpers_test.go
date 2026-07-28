package store

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibejson"
)

// nodeKeyString decodes an object-key Node to an owned string.
func nodeKeyString(key vibejson.Node) string {
	if b, ok := key.StringBytes(); ok {
		return vibejson.OwnedBytesString(b)
	}
	out, _ := key.AppendText(nil)
	return vibejson.OwnedBytesString(out)
}

// buildEnrichedMachine builds a key-hash-enriched Index over the bitmap
// engine, for tests that need an enriched tape regardless of document size.
func buildEnrichedMachine(src []byte, storage []vibejson.IndexEntry) (vibejson.Index, bool) {
	entries, ok := vibejson.BuildIndexBitmap(src, storage)
	if !ok {
		return vibejson.Index{}, false
	}
	index := vibejson.Index{Src: src, Entries: entries}
	vibejson.EnrichKeyHashes(&index)
	return index, true
}

// keyHashCorpus is a corpus of key-shape edge cases: empty/duplicate/escaped
// keys, Unicode-equivalent escapes, and JSON-Pointer-special characters.
var keyHashCorpus = []string{
	`{}`,
	`{"a":1}`,
	`{"":1,"x":2}`,
	`{"a":1,"b":2,"c":3}`,
	`{"a":1,"ab":2,"abc":3,"abcd":4,"abcde":5,"abcdef":6,"abcdefg":7,"abcdefgh":8,"abcdefghi":9,"abcdefghijklmnop":10,"abcdefghijklmnopq":11}`,
	`{"aaaaaaaa":1,"aaaaaaab":2,"baaaaaaa":3,"ab":4,"ba":5}`,
	`{"a":1,"a":2}`,
	`{"dup":1,"other":{"dup":2},"dup":3}`,
	`{"k\n":1,"k\u000a":2,"kn":3}`,
	`{"\u0061bc":1,"abc":2}`,
	`{"abc":1,"\u0061bc":2}`,
	`{"héllo":1,"h\u00e9llo":2,"hello":3}`,
	`{"a/b":1,"a~b":2,"a~1b":3}`,
	`{"\ud83d\ude00":1,"😀":2}`,
	`{"outer":{"inner":{"deep":1,"deep":2},"list":[{"z":9},{"z":10}]},"outer":{"inner":3}}`,
	`{"x":[1,{"y":{"a":1,"b":[2,3]}},"s"],"x":{"y":4}}`,
}

// keyHashQuerySet derives a probe query set from an object's own keys: each
// key, each key with a trailing byte trimmed, and near-miss variants, plus
// the empty string and a definitely-absent sentinel.
func keyHashQuerySet(v vibejson.Node) []string {
	set := map[string]struct{}{
		"":                       {},
		"\x00 definitely absent": {},
	}
	iter, ok := v.ObjectIter()
	if ok {
		for {
			k, _, ok := iter.Next()
			if !ok {
				break
			}
			q := nodeKeyString(k)
			set[q] = struct{}{}
			set[q+"x"] = struct{}{}
			set["x"+q] = struct{}{}
			if len(q) > 0 {
				set[q[:len(q)-1]] = struct{}{}
			}
		}
	}
	queries := make([]string, 0, len(set))
	for q := range set {
		queries = append(queries, q)
	}
	sort.Strings(queries)
	return queries
}

// chunkReader/fixedChunkReader replays data in fixed-size reads.
type chunkReader struct {
	data  []byte
	chunk int
}

type fixedChunkReader = chunkReader

func (c *chunkReader) Read(p []byte) (int, error) {
	if len(c.data) == 0 {
		return 0, io.EOF
	}
	n := min(len(p), min(c.chunk, len(c.data)))
	copy(p, c.data[:n])
	c.data = c.data[n:]
	return n, nil
}

// tornReader replays data in pseudo-random-size reads, occasionally stopping
// short of io.EOF, to exercise readers against torn/short reads.
type tornReader struct {
	data  []byte
	state uint64
}

func (r *tornReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	r.state ^= r.state << 13
	r.state ^= r.state >> 7
	r.state ^= r.state << 17
	n := min(1+int(r.state%97), len(r.data))
	if n > len(p) {
		n = len(p)
	}
	copy(p, r.data[:n])
	r.data = r.data[n:]
	if len(r.data) == 0 && r.state&1 == 0 {
		return n, io.EOF
	}
	return n, nil
}

// sameRawValue reports whether a and b alias the same backing bytes.
func sameRawValue(a, b vibejson.RawValue) bool {
	return len(a.Src) == len(b.Src) && unsafe.SliceData(a.Src) == unsafe.SliceData(b.Src)
}

// escapePointerToken applies RFC 6901 tilde-escaping to a reference token.
func escapePointerToken(k string) string {
	out := make([]byte, 0, len(k))
	for i := 0; i < len(k); i++ {
		switch k[i] {
		case '~':
			out = append(out, '~', '0')
		case '/':
			out = append(out, '~', '1')
		default:
			out = append(out, k[i])
		}
	}
	return string(out)
}

// forceStackMovement perturbs the goroutine stack to help surface pointer bugs
// that only manifest after a stack move.
func forceStackMovement(depth int, acc int) int {
	if depth == 0 {
		var buf [64]byte
		for i := range buf {
			buf[i] = byte(acc + i)
		}
		s := 0
		for _, b := range buf {
			s += int(b)
		}
		return s
	}
	return forceStackMovement(depth-1, acc+depth) ^ depth
}

// keyHashWideDoc synthesizes a wide flat object exercising duplicate, escaped,
// and long-key spellings for key-hash enrichment tests.
func keyHashWideDoc(width int, padValue string) string {
	var sb strings.Builder
	sb.WriteString("{")
	for i := range width {
		if i > 0 {
			sb.WriteString(",")
		}
		switch {
		case i%13 == 12:
			fmt.Fprintf(&sb, `"member%04d":`, i-1) // duplicate of the previous key
		case i%5 == 4:
			fmt.Fprintf(&sb, `"m\tember%04d":`, i) // escaped spelling
		case i%3 == 2:
			fmt.Fprintf(&sb, `"very_long_member_key_%04d_with_tail":`, i)
		default:
			fmt.Fprintf(&sb, `"member%04d":`, i)
		}
		switch {
		case i%11 == 10:
			fmt.Fprintf(&sb, `[%d,{"nested":%d}]`, i, i)
		case padValue != "":
			fmt.Fprintf(&sb, `"%s%d"`, padValue, i)
		default:
			fmt.Fprintf(&sb, "%d", i)
		}
	}
	sb.WriteString("}")
	return sb.String()
}

// testIterations scales an iteration count down under `go test -short`.
func testIterations(full, short int) int {
	if testing.Short() {
		return short
	}
	return full
}
