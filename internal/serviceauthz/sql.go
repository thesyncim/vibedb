package serviceauthz

// SQLCapability classifies the authority required by one SQL statement without
// allocating or normalizing it. The scanner skips whitespace and both SQL
// comment forms before reading the first ASCII keyword. Unterminated comments
// and non-ASCII/unknown leading tokens fail closed as schema authority on a
// writable lane; the SQL parser remains responsible for syntax validity.
func SQLCapability(sql string, writable bool) Capability {
	if !writable {
		return CapabilityDataRead
	}
	index, ok := sqlKeywordStart(sql)
	if !ok {
		return CapabilitySchema
	}
	for _, keyword := range [...]string{"create", "alter", "drop", "truncate", "rename"} {
		if asciiKeyword(sql[index:], keyword) {
			return CapabilitySchema
		}
	}
	return CapabilityDataWrite
}

func sqlKeywordStart(sql string) (int, bool) {
	index := 0
	for {
		for index < len(sql) && sql[index] <= ' ' {
			index++
		}
		if index == len(sql) {
			return 0, false
		}
		if index+1 >= len(sql) {
			break
		}
		switch {
		case sql[index] == '-' && sql[index+1] == '-':
			index += 2
			for index < len(sql) && sql[index] != '\n' && sql[index] != '\r' {
				index++
			}
		case sql[index] == '/' && sql[index+1] == '*':
			index += 2
			closed := false
			for index+1 < len(sql) {
				if sql[index] == '*' && sql[index+1] == '/' {
					index += 2
					closed = true
					break
				}
				index++
			}
			if !closed {
				return 0, false
			}
		default:
			goto keyword
		}
	}
keyword:
	if index >= len(sql) || !asciiLetter(sql[index]) {
		return 0, false
	}
	return index, true
}

func asciiKeyword(sql, keyword string) bool {
	if len(sql) < len(keyword) {
		return false
	}
	for index := range len(keyword) {
		value := sql[index]
		if value >= 'A' && value <= 'Z' {
			value += 'a' - 'A'
		}
		if value != keyword[index] {
			return false
		}
	}
	return len(sql) == len(keyword) || !asciiLetter(sql[len(keyword)])
}

func asciiLetter(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value == '_'
}
