package tin

import "testing"

var scanBenchDocs = map[string]string{
	"short": "fuji apple juicy red pie",
	"mid": "fuji apple juicy red pie apple apple banana " +
		"lazy dog sleeps all day near the river bank security critical " +
		"peach blossom spring rain title alpha beta gamma disclaimer citrus " +
		"melon orange tang vintage luxury watches filler words padding out " +
		"this document body with common terms repeated common common filler",
	"long": "fuji apple juicy red pie apple apple banana " +
		"lazy dog sleeps all day near the river bank security critical " +
		"peach blossom spring rain title alpha beta gamma disclaimer citrus " +
		"melon orange tang vintage luxury watches filler words padding out " +
		"this document body with common terms repeated common common filler " +
		"fuji apple juicy red pie apple apple banana " +
		"lazy dog sleeps all day near the river bank security critical " +
		"peach blossom spring rain title alpha beta gamma disclaimer citrus " +
		"melon orange tang vintage luxury watches filler words padding out " +
		"this document body with common terms repeated common common filler " +
		"fuji apple juicy red pie apple apple banana " +
		"lazy dog sleeps all day near the river bank security critical " +
		"peach blossom spring rain title alpha beta gamma disclaimer citrus " +
		"melon orange tang vintage luxury watches filler words padding out " +
		"this document body with common terms repeated common common filler",
}

// BenchmarkScanPairs pins the transient tokenizer: bytes per second over
// representative document sizes, allocation-free on warm scratch.
func BenchmarkScanPairs(b *testing.B) {
	for name, doc := range scanBenchDocs {
		b.Run(name, func(b *testing.B) {
			var out []tokPos
			out = scanPairs(doc, out)
			b.SetBytes(int64(len(doc)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out = scanPairs(doc, out[:0])
			}
			_ = out
		})
	}
}
