package storeio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

func TestCompactRankAffineMalformed(t *testing.T) {
	data := make([]byte, 18)
	data[0], data[1] = 3, 4
	binary.LittleEndian.PutUint64(data[2:], 10)
	binary.LittleEndian.PutUint64(data[10:], 2)
	raw, err := (compactStreamEncoding{kind: compactStreamRankAffine, count: 8, data: data, dict: [][]byte{[]byte("n:"), nil}}).appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openCompactStream(raw); err != nil {
		t.Fatal(err)
	}
	dataAt := compactStreamHeader + 4 + len("n:")
	mutations := map[string]func([]byte) []byte{
		"wrong-domain":      func(b []byte) []byte { binary.LittleEndian.PutUint32(b[4:], 2); return b },
		"zero-domain":       func(b []byte) []byte { binary.LittleEndian.PutUint32(b[4:], 0); return b },
		"unsupported-flags": func(b []byte) []byte { b[dataAt] = 7; return b },
		"zero-width":        func(b []byte) []byte { b[dataAt+1] = 0; return b },
		"narrow-width":      func(b []byte) []byte { b[dataAt+1] = 1; return b },
		"negative-base":     func(b []byte) []byte { binary.LittleEndian.PutUint64(b[dataAt+2:], 1<<63); return b },
		"endpoint-overflow": func(b []byte) []byte { binary.LittleEndian.PutUint64(b[dataAt+2:], math.MaxInt64); return b },
		"minimum-step":      func(b []byte) []byte { binary.LittleEndian.PutUint64(b[dataAt+10:], 1<<63); return b },
		"zero-step":         func(b []byte) []byte { clear(b[dataAt+10:]); return b },
		"digit-affix":       func(b []byte) []byte { b[compactStreamHeader+4] = '1'; return b },
		"bad-directory": func(b []byte) []byte {
			binary.LittleEndian.PutUint16(b[compactStreamHeader:], math.MaxUint16)
			return b
		},
		"trailing-byte": func(b []byte) []byte { return append(b, 0) },
		"truncated":     func(b []byte) []byte { return b[:len(b)-1] },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			actualRaw := mutate(bytes.Clone(raw))
			actualRaw[0] = compactStreamRankAffine
			actual, err := openCompactStream(actualRaw)
			// Self-delimiting streams allow a following stream. Exact enclosing
			// length and full-leaf count are contextual admission requirements.
			if err == nil && actual.encoded == len(actualRaw) && actual.matchesShapeRows(2, 8) {
				t.Fatal("actual-kind malformed descriptor/domain admitted")
			}
		})
	}
}

func TestCompactRankAffineWriterProvesEveryValue(t *testing.T) {
	for _, test := range []struct {
		name       string
		base, step int64
		fixed      bool
	}{
		{"ascending", 1000, 7, false}, {"descending", 10000, -7, false},
		{"fixed", 1000, 7, true}, {"exact-large", math.MaxInt64 - 10000, 7, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			const leafRows = 320
			ranks := make([]uint16, 128)
			values := make([][]byte, len(ranks))
			for i := range ranks {
				ranks[i] = uint16(i*2 + i/7)
				value := test.base + test.step*int64(ranks[i])
				if test.fixed {
					values[i] = fmt.Appendf(nil, `"n:%020d"`, value)
				} else {
					values[i] = fmt.Appendf(nil, `"n:%d"`, value)
				}
			}
			var scratch compactStreamScratch
			encoded := scratch.encodeShape(values, ranks, leafRows)
			if encoded.kind != compactStreamRankAffine {
				t.Fatalf("kind=%d", encoded.kind)
			}
			check := func(encoded compactStreamEncoding) {
				raw, err := encoded.appendBinary(nil)
				if err != nil {
					t.Fatal(err)
				}
				view, err := openCompactStream(raw)
				if err != nil {
					t.Fatal(err)
				}
				for i, rank := range ranks {
					got, ok := view.appendValue(nil, view.shapeCoordinate(int(rank), i))
					if !ok || !bytes.Equal(got, values[i]) {
						t.Fatalf("row %d got=%q want=%q", i, got, values[i])
					}
				}
			}
			check(encoded)
			// A late mismatch must decline and retain every exact spelling.
			values[len(values)-1] = []byte(`"n:8"`)
			encoded = scratch.encodeShape(values, ranks, leafRows)
			if encoded.kind == compactStreamRankAffine {
				t.Fatal("late mismatch admitted")
			}
			check(encoded)
		})
	}
}
