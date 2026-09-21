package tin

// Folding: case folding for ASCII plus accent folding for Latin-1, shared by
// the scalar and vector paths. Bytes >= 0x80 pass the ASCII fold through
// untouched; the tokenizer decodes those UTF-8 sequences on the rune path.

const (
	// simdFoldThreshold gates the vector fold exactly like storeio's size
	// dispatch: short strings stay scalar, bulk documents go wide.
	simdFoldThreshold = 256
	// maxFoldDoc bounds the reused fold scratch against absurd inputs; longer
	// documents scan directly without folding first.
	maxFoldDoc = 1 << 26
)

// foldASCIIImpl is the fold implementation in force: the vector kernel once
// a per-arch enable file selects it (see fold_enable_*.go), the scalar
// spelling everywhere else.
var foldASCIIImpl = foldASCIIScalar

// foldASCII folds src's ASCII uppercase to lowercase into dst, which must
// have at least len(src) bytes. Non-ASCII bytes pass through untouched.
func foldASCII(dst []byte, src string) {
	foldASCIIImpl(dst, src)
}

// foldByte folds one ASCII byte to lowercase. Non-ASCII bytes pass through
// for the rune path in token.go.
func foldByte(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}

// foldASCIIScalar folds src into dst (which must have at least len(src)
// bytes). It is the scalar tail of the vector kernel and the whole fold on
// builds without one.
func foldASCIIScalar(dst []byte, src string) {
	for i := 0; i < len(src); i++ {
		b := src[i]
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		dst[i] = b
	}
}

// latinFold maps Latin-1 supplement rune values (U+00C0..U+00FF) to one ASCII
// fold byte. Zero means "no single-byte fold": the rune falls back to
// unicode.ToLower classification in token.go (×, ÷, and the like), which
// keeps them as token characters or separators by letter/digit property
// rather than by a lossy guess.
var latinFold = [256]byte{
	0xC0: 'a', 0xC1: 'a', 0xC2: 'a', 0xC3: 'a', 0xC4: 'a', 0xC5: 'a',
	0xC6: 'a', // Æ folds to a (single-byte fold keeps positions stable)
	0xC7: 'c',
	0xC8: 'e', 0xC9: 'e', 0xCA: 'e', 0xCB: 'e',
	0xCC: 'i', 0xCD: 'i', 0xCE: 'i', 0xCF: 'i',
	0xD0: 'd', // Ð
	0xD1: 'n',
	0xD2: 'o', 0xD3: 'o', 0xD4: 'o', 0xD5: 'o', 0xD6: 'o',
	0xD8: 'o',
	0xD9: 'u', 0xDA: 'u', 0xDB: 'u', 0xDC: 'u',
	0xDD: 'y',
	0xDE: 't', // Þ
	0xDF: 's', // ß folds to s (single-byte fold keeps positions stable)
	0xE0: 'a', 0xE1: 'a', 0xE2: 'a', 0xE3: 'a', 0xE4: 'a', 0xE5: 'a',
	0xE6: 'a', // æ
	0xE7: 'c',
	0xE8: 'e', 0xE9: 'e', 0xEA: 'e', 0xEB: 'e',
	0xEC: 'i', 0xED: 'i', 0xEE: 'i', 0xEF: 'i',
	0xF0: 'd', // ð
	0xF1: 'n',
	0xF2: 'o', 0xF3: 'o', 0xF4: 'o', 0xF5: 'o', 0xF6: 'o',
	0xF8: 'o',
	0xF9: 'u', 0xFA: 'u', 0xFB: 'u', 0xFC: 'u',
	0xFD: 'y', 0xFE: 't', // þ
	0xFF: 'y', // ÿ
}

// wordClass marks folded ASCII bytes that continue a token: letters, digits,
// and underscore. Everything else — including the hyphen, so "wi-fi" scans
// as two words per TIN's rule — separates tokens.
var wordClass = [256]bool{}

func init() {
	for c := byte('a'); c <= byte('z'); c++ {
		wordClass[c] = true
	}
	for c := byte('0'); c <= byte('9'); c++ {
		wordClass[c] = true
	}
	wordClass['_'] = true
}
