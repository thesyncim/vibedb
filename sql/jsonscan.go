package sql

// The '@>' needle scanner.
//
// The right-hand side of containment is a JSON document, not a SQL literal:
// its strings are double-quoted, its escapes are backslash escapes, and its
// numbers are JSON numbers. Lexing it with SQL's rules would misread every one
// of those, so it is delimited structurally straight out of the statement text
// and kept verbatim.
//
// Delimiting is not validating. Whether the needle is a well-formed JSON
// document is settled when the statement is lowered, by the same indexer that
// will match it — one implementation of the JSON grammar rather than a second
// one here that could drift from it. What this must guarantee is only that the
// value is closed, so the parser resumes at the right byte and an unterminated
// needle is reported here rather than swallowing the rest of the statement.

// scanJSONValue reads one JSON value beginning at or after from and returns its
// exact source text together with the offsets it spans.
func (p *Parser) scanJSONValue(from int) (text string, start, end int, err error) {
	src := p.lx.src
	i := from
	for i < len(src) && isJSONSpace(src[i]) {
		i++
	}
	if i >= len(src) {
		return "", i, 0, p.errAt(i, "expected a JSON document after '@>'")
	}
	start = i
	switch src[i] {
	case '{', '[':
		end, err = p.scanJSONContainer(i)
	case '"':
		end, err = p.scanJSONString(i)
	default:
		end = scanJSONScalar(src, i)
		if end == start {
			err = p.errAt(start, "expected a JSON document after '@>'")
		}
	}
	if err != nil {
		return "", start, 0, err
	}
	return src[start:end], start, end, nil
}

// scanJSONContainer returns the offset just past a balanced object or array,
// honoring strings so a brace inside one does not disturb the balance.
func (p *Parser) scanJSONContainer(i int) (int, error) {
	src := p.lx.src
	start := i
	depth := 0
	for i < len(src) {
		switch src[i] {
		case '{', '[':
			depth++
			i++
			if depth > maxJSONDepth {
				return 0, p.errAt(i, "JSON document after '@>' nests too deeply")
			}
		case '}', ']':
			depth--
			i++
			if depth == 0 {
				return i, nil
			}
		case '"':
			end, err := p.scanJSONString(i)
			if err != nil {
				return 0, err
			}
			i = end
		default:
			i++
		}
	}
	return 0, p.errAt(start, "unterminated JSON document after '@>'")
}

// maxJSONDepth bounds needle nesting. The scan is iterative, so this is not a
// stack guard; it is a guard against a fuzz input that is nothing but a
// megabyte of '[' being reported as a valid needle and handed on to a lowering
// pass that would then have to reject it.
const maxJSONDepth = 1000

// scanJSONString returns the offset just past a JSON string, honoring backslash
// escapes so an escaped quote does not close it early.
func (p *Parser) scanJSONString(i int) (int, error) {
	src := p.lx.src
	start := i
	i++ // opening quote
	for i < len(src) {
		switch src[i] {
		case '\\':
			// A trailing backslash at end of input would step past the end;
			// the loop condition catches that on the next iteration because
			// i only ever grows.
			i += 2
		case '"':
			return i + 1, nil
		default:
			i++
		}
	}
	return 0, p.errAt(start, "unterminated JSON string after '@>'")
}

// scanJSONScalar returns the offset just past a JSON scalar run: everything up
// to the first byte that cannot continue a number or a bare keyword.
func scanJSONScalar(src string, i int) int {
	for i < len(src) {
		c := src[i]
		if isJSONSpace(c) || c == ',' || c == ')' || c == ']' || c == '}' {
			break
		}
		i++
	}
	return i
}

func isJSONSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}
