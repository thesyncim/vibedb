// Package pginput implements PostgreSQL's allocation-free textual input
// grammars for the small set of scalar domains shared by the SQL parser and
// executor.
package pginput

// Boolean mirrors PostgreSQL boolin/bool.c: after trimming SQL whitespace it
// accepts 1, 0, or a non-empty case-insensitive unique prefix of true/false,
// yes/no, and on/off. In particular, "o" is ambiguous and therefore invalid.
func Boolean(text string) (bool, bool) {
	text = trimSpace(text)
	if len(text) == 0 {
		return false, false
	}
	// Keep the branch shape source-parallel with PostgreSQL bool.c. Besides
	// avoiding six candidate checks for the common t/f cases, the dedicated
	// o branch expresses why a one-byte "o" is not a unique prefix.
	switch text[0] {
	case 't', 'T':
		if prefix(text, "true") {
			return true, true
		}
	case 'f', 'F':
		if prefix(text, "false") {
			return false, true
		}
	case 'y', 'Y':
		if prefix(text, "yes") {
			return true, true
		}
	case 'n', 'N':
		if prefix(text, "no") {
			return false, true
		}
	case 'o', 'O':
		if len(text) >= 2 {
			if prefix(text, "on") {
				return true, true
			}
			if prefix(text, "off") {
				return false, true
			}
		}
	case '1':
		if len(text) == 1 {
			return true, true
		}
	case '0':
		if len(text) == 1 {
			return false, true
		}
	}
	return false, false
}

func prefix(text, word string) bool {
	return len(text) <= len(word) && equalFoldASCII(text, word[:len(text)])
}

func trimSpace(text string) string {
	start, end := 0, len(text)
	for start < end && isSpace(text[start]) {
		start++
	}
	for end > start && isSpace(text[end-1]) {
		end--
	}
	return text[start:end]
}

func isSpace(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	default:
		return false
	}
}

func equalFoldASCII(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		a, b := left[i], right[i]
		if a >= 'A' && a <= 'Z' {
			a += 'a' - 'A'
		}
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		if a != b {
			return false
		}
	}
	return true
}
