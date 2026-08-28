package pgwire

import (
	"fmt"
	"strings"
)

// Public is the single wire-protocol namespace. Lower only table positions,
// before the ordinary parser prepares a statement. String contents, field
// paths, aliases and error byte positions are preserved. Unknown namespaces
// remain unsupported; this is not general schema resolution.
func lowerPublicRelations(text string, check func() error) (string, bool, error) {
	s := newScanner(text, check)
	var lowered []byte
	expectTable := false
	ddl, indexDDL := false, false
	firstWord := true
	for s.pos < len(text) {
		s.skip()
		if s.cancelErr != nil {
			return "", false, s.cancelErr
		}
		if s.malformed || s.pos >= len(text) {
			break
		}
		start := s.pos
		word := ""
		quoted := false
		if text[s.pos] == '\'' || text[s.pos] == '"' {
			quote := text[s.pos]
			end, ok, err := skipQuotedCheckedCancelable(text, s.pos, quote, check)
			if err != nil {
				return "", false, err
			}
			if !ok {
				return "", false, nil
			}
			s.pos = end
			if quote == '\'' {
				expectTable = false
				continue
			}
			word = text[start+1 : end-1]
			quoted = true
		} else {
			word = s.word()
			if word == "" {
				s.pos++
				expectTable = false
				continue
			}
		}
		if expectTable && ((quoted && word == "public") || (!quoted && strings.EqualFold(word, "public"))) {
			end := s.pos
			if s.symbol('.') {
				if lowered == nil {
					lowered = []byte(text)
				}
				for i := start; i < end; i++ {
					lowered[i] = ' '
				}
				lowered[s.pos-1] = ' '
			}
		}
		if firstWord {
			ddl = !quoted && (strings.EqualFold(word, "CREATE") || strings.EqualFold(word, "DROP") || strings.EqualFold(word, "ALTER") || strings.EqualFold(word, "TRUNCATE"))
			firstWord = false
		}
		if ddl && !quoted && strings.EqualFold(word, "INDEX") {
			indexDDL = true
		}
		// Optional DDL guards do not consume the following object-name slot.
		if ddl && expectTable && !quoted && (strings.EqualFold(word, "IF") || strings.EqualFold(word, "NOT") || strings.EqualFold(word, "EXISTS")) {
			continue
		}
		expectTable = !quoted && (strings.EqualFold(word, "FROM") || strings.EqualFold(word, "JOIN") || strings.EqualFold(word, "UPDATE") || strings.EqualFold(word, "INTO") ||
			ddl && (strings.EqualFold(word, "TABLE") || strings.EqualFold(word, "TRUNCATE") || strings.EqualFold(word, "INDEX") || indexDDL && strings.EqualFold(word, "ON")))
	}
	if lowered == nil {
		return text, false, nil
	}
	return string(lowered), true, nil
}

func requirePublicSearchPath(v string) error {
	v = strings.TrimSpace(v)
	if v == "public" || v == "\"public\"" || v == "'public'" || v == "\"$user\", public" {
		return nil
	}
	return fmt.Errorf("only the public search_path is supported")
}
