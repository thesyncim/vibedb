package distribution

// This file loads testdata/tuple_v1_vectors.txt — the frozen tuple codec v1
// golden vectors — and asserts every one byte-for-byte against the live
// codec. See that file's header for the grammar and for why it must never be
// edited to make a failing assertion pass: a mismatch here means the
// implementation drifted from a frozen wire format or the fixture itself was
// authored inconsistently, either of which is a stop condition, not
// something this test (or the fixture) should be adjusted to hide.

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"testing"
)

// goldenRecord is one parsed NUMBER, STRING, or TUPLE line.
type goldenRecord struct {
	lineNo int
	kind   string // "NUMBER", "STRING", or "TUPLE"
	name   string
	group  string
	hex    string   // recorded hex, "" only for the empty-byte-string sentinel "-"
	elems  []string // TUPLE only: names of earlier NUMBER/STRING records
}

type goldenDistinct struct {
	lineNo         int
	groupA, groupB string
}

// splitFields extracts the first n whitespace-separated tokens from line and
// returns the trimmed remainder, which for NUMBER/STRING records is a
// Go-syntax quoted literal running to end of line (so it may itself contain
// spaces, quotes, and arbitrary escaped bytes).
func splitFields(line string, n int) ([]string, string) {
	out := make([]string, 0, n)
	rest := line
	for len(out) < n {
		rest = strings.TrimLeft(rest, " \t")
		if rest == "" {
			return out, ""
		}
		i := strings.IndexAny(rest, " \t")
		if i < 0 {
			out = append(out, rest)
			return out, ""
		}
		out = append(out, rest[:i])
		rest = rest[i+1:]
	}
	return out, strings.TrimSpace(rest)
}

// loadGoldenVectors parses testdata/tuple_v1_vectors.txt.
func loadGoldenVectors(t *testing.T) ([]goldenRecord, []goldenDistinct) {
	t.Helper()
	f, err := os.Open("testdata/tuple_v1_vectors.txt")
	if err != nil {
		t.Fatalf("open golden vectors: %v", err)
	}
	defer f.Close()

	var records []goldenRecord
	var distincts []goldenDistinct

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		kindTok := trimmed
		if i := strings.IndexAny(trimmed, " \t"); i >= 0 {
			kindTok = trimmed[:i]
		}
		switch kindTok {
		case "NUMBER", "STRING":
			head, quoted := splitFields(line, 4)
			if len(head) != 4 {
				t.Fatalf("line %d: %s record needs name, group, hex, and a quoted value", lineNo, head[0])
			}
			payload, err := strconv.Unquote(quoted)
			if err != nil {
				t.Fatalf("line %d: unquote %q: %v", lineNo, quoted, err)
			}
			records = append(records, goldenRecord{
				lineNo: lineNo, kind: head[0], name: head[1], group: head[2], hex: head[3],
			})
			// Stash the decoded literal in a side channel keyed by name via
			// a closure-free trick: encode it into the record's elems slice
			// position 0 for NUMBER/STRING (only TUPLE uses elems as names).
			records[len(records)-1].elems = []string{payload}
		case "TUPLE":
			head, rest2 := splitFields(line, 4)
			if len(head) != 4 {
				t.Fatalf("line %d: TUPLE record needs name, group, hex, and elements", lineNo)
			}
			var elems []string
			if rest2 != "" {
				elems = strings.Fields(rest2)
			}
			records = append(records, goldenRecord{
				lineNo: lineNo, kind: "TUPLE", name: head[1], group: head[2], hex: head[3], elems: elems,
			})
		case "DISTINCT":
			head, _ := splitFields(line, 3)
			if len(head) != 3 {
				t.Fatalf("line %d: DISTINCT record needs groupA and groupB", lineNo)
			}
			distincts = append(distincts, goldenDistinct{lineNo: lineNo, groupA: head[1], groupB: head[2]})
		default:
			t.Fatalf("line %d: unknown record type %q", lineNo, kindTok)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan golden vectors: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("no golden vectors parsed; fixture path or grammar changed?")
	}
	return records, distincts
}

// decodeGoldenHex decodes a fixture hex column; "-" is the empty byte string.
func decodeGoldenHex(t *testing.T, lineNo int, h string) []byte {
	t.Helper()
	if h == "-" {
		return []byte{}
	}
	b, err := hex.DecodeString(h)
	if err != nil {
		t.Fatalf("line %d: invalid hex %q: %v", lineNo, h, err)
	}
	return b
}

// TestTupleV1GoldenVectors is the byte-for-byte load-and-assert test required
// alongside the frozen fixture: every NUMBER/STRING/TUPLE record must
// reproduce its recorded bytes from the live V1 codec, every equivalence
// group's members must share identical bytes, and every DISTINCT pair must
// differ.
func TestTupleV1GoldenVectors(t *testing.T) {
	records, distincts := loadGoldenVectors(t)

	scalarsByName := make(map[string]Scalar)
	bytesByName := make(map[string][]byte)
	groupMembers := make(map[string][]string) // group -> names, in file order

	for _, r := range records {
		switch r.kind {
		case "NUMBER":
			spelling := r.elems[0]
			s, err := NewNumber(spelling)
			if err != nil {
				t.Fatalf("line %d: NewNumber(%q): %v", r.lineNo, spelling, err)
			}
			got, err := V1.AppendScalar(nil, s)
			if err != nil {
				t.Fatalf("line %d: AppendScalar(%s=%q): %v", r.lineNo, r.name, spelling, err)
			}
			want := decodeGoldenHex(t, r.lineNo, r.hex)
			if !bytes.Equal(got, want) {
				t.Fatalf("line %d: NUMBER %s %q = %x, fixture says %x", r.lineNo, r.name, spelling, got, want)
			}
			scalarsByName[r.name] = s
			bytesByName[r.name] = got

		case "STRING":
			raw := r.elems[0]
			s := NewString(raw)
			got, err := V1.AppendScalar(nil, s)
			if err != nil {
				t.Fatalf("line %d: AppendScalar(%s=%q): %v", r.lineNo, r.name, raw, err)
			}
			want := decodeGoldenHex(t, r.lineNo, r.hex)
			if !bytes.Equal(got, want) {
				t.Fatalf("line %d: STRING %s %q = %x, fixture says %x", r.lineNo, r.name, raw, got, want)
			}
			scalarsByName[r.name] = s
			bytesByName[r.name] = got

		case "TUPLE":
			values := make([]Scalar, len(r.elems))
			concat := make([]byte, 0, 64)
			for i, elem := range r.elems {
				s, ok := scalarsByName[elem]
				if !ok {
					t.Fatalf("line %d: TUPLE %s references unknown element %q (must be an earlier NUMBER/STRING name)", r.lineNo, r.name, elem)
				}
				values[i] = s
				concat = append(concat, bytesByName[elem]...)
			}
			got, err := V1.AppendTuple(nil, values)
			if err != nil {
				t.Fatalf("line %d: AppendTuple(%s): %v", r.lineNo, r.name, err)
			}
			want := decodeGoldenHex(t, r.lineNo, r.hex)
			if !bytes.Equal(got, want) {
				t.Fatalf("line %d: TUPLE %s = %x, fixture says %x", r.lineNo, r.name, got, want)
			}
			if !bytes.Equal(concat, want) {
				t.Fatalf("line %d: TUPLE %s: concatenating its elements' own recorded bytes gives %x, not the tuple's recorded %x",
					r.lineNo, r.name, concat, want)
			}
			bytesByName[r.name] = got

		default:
			t.Fatalf("line %d: unknown record kind %q", r.lineNo, r.kind)
		}

		if r.group != "-" && r.group != "" {
			groupMembers[r.group] = append(groupMembers[r.group], r.name)
		}
	}

	// Every member of an equivalence group must share identical bytes.
	for group, names := range groupMembers {
		first := bytesByName[names[0]]
		for _, name := range names[1:] {
			if !bytes.Equal(bytesByName[name], first) {
				t.Fatalf("group %q: %s = %x, %s = %x, want identical bytes",
					group, names[0], first, name, bytesByName[name])
			}
		}
	}

	// Every DISTINCT pair must differ, member-for-member across both groups.
	for _, d := range distincts {
		a, ok := groupMembers[d.groupA]
		if !ok || len(a) == 0 {
			t.Fatalf("line %d: DISTINCT references empty or unknown group %q", d.lineNo, d.groupA)
		}
		b, ok := groupMembers[d.groupB]
		if !ok || len(b) == 0 {
			t.Fatalf("line %d: DISTINCT references empty or unknown group %q", d.lineNo, d.groupB)
		}
		for _, na := range a {
			for _, nb := range b {
				if bytes.Equal(bytesByName[na], bytesByName[nb]) {
					t.Fatalf("line %d: DISTINCT %s vs %s: %s and %s encode identically (%x), want different bytes",
						d.lineNo, d.groupA, d.groupB, na, nb, bytesByName[na])
				}
			}
		}
	}

	t.Logf("checked %d records (%d equivalence groups, %d distinct pairs)", len(records), len(groupMembers), len(distincts))
}

// TestTupleV1GoldenVectorsFileIsClean is a light sanity check on the fixture
// itself: every hex column decodes, and record names are unique so a typo
// in a TUPLE element list fails loudly rather than silently resolving to the
// wrong scalar.
func TestTupleV1GoldenVectorsFileIsClean(t *testing.T) {
	records, _ := loadGoldenVectors(t)
	seen := make(map[string]int)
	for _, r := range records {
		if prior, ok := seen[r.name]; ok {
			t.Fatalf("duplicate record name %q at line %d (first defined at line %d)", r.name, r.lineNo, prior)
		}
		seen[r.name] = r.lineNo
		if r.hex != "-" {
			if _, err := hex.DecodeString(r.hex); err != nil {
				t.Fatalf("line %d: hex column %q does not decode: %v", r.lineNo, r.hex, err)
			}
		}
	}
}
