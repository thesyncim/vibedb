package serviceauthz

import sqlast "github.com/thesyncim/vibedb/sql"

// SQLCapability classifies one SQL statement without allocating or trusting a
// caller-selected execution lane. Mixed statements and malformed lexical
// boundaries require every SQL capability and therefore fail closed before
// parsing or admission.
func SQLCapability(sql string) Capability {
	if !singleSQLStatement(sql) {
		return CapabilityDataRead | CapabilityDataWrite | CapabilitySchema
	}
	index, ok := sqlKeywordStart(sql)
	if !ok {
		if firstSQLTokenIsParenthesis(sql) {
			return CapabilityDataRead
		}
		return CapabilityDataRead | CapabilityDataWrite | CapabilitySchema
	}
	for _, keyword := range [...]string{"alter", "rename", "refresh"} {
		if asciiKeyword(sql[index:], keyword) {
			return CapabilitySchema
		}
	}
	kind := sqlast.KindOf(sql)
	switch kind {
	case sqlast.KindCreateTable, sqlast.KindCreateIndex, sqlast.KindCreateView,
		sqlast.KindDropTable, sqlast.KindDropIndex, sqlast.KindDropView, sqlast.KindTruncate:
		return CapabilitySchema
	case sqlast.KindInsert, sqlast.KindUpdate, sqlast.KindDelete,
		sqlast.KindSavepoint, sqlast.KindReleaseSavepoint, sqlast.KindRollbackToSavepoint:
		return CapabilityDataWrite
	default:
		for _, keyword := range [...]string{"select", "with", "explain", "values", "table"} {
			if asciiKeyword(sql[index:], keyword) {
				return CapabilityDataRead
			}
		}
		// KindOf deliberately maps unknown syntax to SELECT so parsing can issue
		// the useful error. Authorization cannot use that permissive routing
		// convention: unknown syntax needs every capability and fails closed.
		return CapabilityDataRead | CapabilityDataWrite | CapabilitySchema
	}
}

func firstSQLTokenIsParenthesis(sql string) bool {
	index := 0
	for {
		for index < len(sql) && sql[index] <= ' ' {
			index++
		}
		if index == len(sql) {
			return false
		}
		if index+1 < len(sql) && sql[index] == '-' && sql[index+1] == '-' {
			index += 2
			for index < len(sql) && sql[index] != '\n' && sql[index] != '\r' {
				index++
			}
			continue
		}
		if index+1 < len(sql) && sql[index] == '/' && sql[index+1] == '*' {
			index += 2
			for index+1 < len(sql) && (sql[index] != '*' || sql[index+1] != '/') {
				index++
			}
			if index+1 >= len(sql) {
				return false
			}
			index += 2
			continue
		}
		return sql[index] == '('
	}
}

func singleSQLStatement(sql string) bool {
	semicolon := false
	for index := 0; index < len(sql); {
		switch {
		case sql[index] <= ' ':
			index++
		case index+1 < len(sql) && sql[index] == '-' && sql[index+1] == '-':
			index += 2
			for index < len(sql) && sql[index] != '\n' && sql[index] != '\r' {
				index++
			}
		case index+1 < len(sql) && sql[index] == '/' && sql[index+1] == '*':
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
				return false
			}
		case sql[index] == '\'' || sql[index] == '"':
			if semicolon {
				return false
			}
			quote := sql[index]
			index++
			closed := false
			for index < len(sql) {
				if sql[index] != quote {
					index++
					continue
				}
				index++
				if index < len(sql) && sql[index] == quote {
					index++
					continue
				}
				closed = true
				break
			}
			if !closed {
				return false
			}
		case sql[index] == ';':
			if semicolon {
				return false
			}
			semicolon = true
			index++
		default:
			if semicolon {
				return false
			}
			index++
		}
	}
	return true
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
	return index, index < len(sql) && asciiLetter(sql[index])
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
