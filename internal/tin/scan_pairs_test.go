package tin

import (
	"strings"
	"testing"
)

// TestScanPairsAgreesWithScanString locks scanPairs to scanString over
// ASCII, folding, punctuation, digits, Latin-1, CJK, emoji, and malformed
// input. scanPairs exists only to dodge the emit closure's heap escape, so
// any tokenization drift is a correctness bug, not a dialect.
func TestScanPairsAgreesWithScanString(t *testing.T) {
	corpus := []string{
		"",
		"   ",
		"...,,,",
		"hello world",
		"Hello WORLD MiXeD",
		"a b c d e f g",
		"well-known co-op re-entry",
		"price $19.99 (sale!) 100%",
		"under_score trailing- 123 45abc abc45",
		"caf\u00e9 na\u00efve r\u00e9sum\u00e9 \u00c5ngstr\u00f6m",
		"\u00df \u00e6 \u0153 \u00f1",
		"\u4e2d\u6587\u6d4b\u8bd5 \u65e5\u672c\u8a9e \ud55c\uad6d\uc5b4",
		"emoji \U0001F600 test \U0001F50D query",
		"mixed caf\u00e9 \u4e2d\u6587 hello123",
		"\xff\xfe invalid \x80 bytes",
		"tab\tseparated\nnewline\rcarriage",
		strings.Repeat("ab ", 500),
		strings.Repeat("\u00e9", 100),
	}
	for _, text := range corpus {
		var want []tokPos
		scanString(text, func(h uint64, p uint32) {
			want = append(want, tokPos{hash: h, pos: p})
		})
		got := scanPairs(text, nil)
		if len(got) != len(want) {
			t.Fatalf("text %q: pairs %d, want %d", text, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("text %q pair %d: got %+v, want %+v", text, i, got[i], want[i])
			}
		}
		// Warmed reuse must append onto the same backing without drift.
		reused := scanPairs(text, got[:0])
		if len(reused) != len(want) {
			t.Fatalf("text %q: reused pairs %d, want %d", text, len(reused), len(want))
		}
		for i := range want {
			if reused[i] != want[i] {
				t.Fatalf("text %q reused pair %d: got %+v, want %+v", text, i, reused[i], want[i])
			}
		}
	}
}
