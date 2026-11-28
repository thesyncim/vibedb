package pgwire

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

const (
	sqlstateCopyDataException = "22P04"
	sqlstateCopyInvalidSchema = "3F000"
)

type copyDirection uint8

const (
	copyFromStdin copyDirection = iota
	copyToStdout
)

type copyFormat uint8

const (
	copyFormatText copyFormat = iota
	copyFormatCSV
)

type copyCommand struct {
	dir          copyDirection
	table        string
	columns      []string
	query        string // non-empty for COPY (SELECT ...) TO STDOUT
	format       copyFormat
	delimiter    byte
	quote        byte
	escape       byte
	nullStr      string
	header       bool
	forceNull    []string // column names, or ["*"] for FORCE_NULL *
	forceNotNull []string // column names, or ["*"] for FORCE_NOT_NULL *
}

// parseCopyCommand parses PostgreSQL COPY statements:
//
//	COPY [schema.]table [(col1, col2, ...)] FROM STDIN [WITH] [(options...)]
//	COPY [schema.]table [(col1, col2, ...)] TO STDOUT [WITH] [(options...)]
//	COPY (query) TO STDOUT [WITH] [(options...)]
func parseCopyCommand(src string) (*copyCommand, error) {
	s := newScanner(src, nil)
	word := s.word()
	if !strings.EqualFold(word, "COPY") {
		return nil, newError(sqlstateSyntaxError, "expected COPY")
	}
	cmd := &copyCommand{
		format:    copyFormatText,
		delimiter: '\t',
		quote:     '"',
		escape:    '"',
		nullStr:   `\N`,
	}
	delimiterSet := false
	nullSet := false
	escapeSet := false

	s.skip()
	if s.pos < len(s.src) && s.src[s.pos] == '(' {
		// COPY (SELECT ...) TO STDOUT
		start := s.pos + 1
		depth := 0
		inSingle, inDouble := false, false
		for s.pos < len(s.src) {
			c := s.src[s.pos]
			if inSingle {
				if c == '\'' {
					if s.pos+1 < len(s.src) && s.src[s.pos+1] == '\'' {
						s.pos += 2
						continue
					}
					inSingle = false
				}
				s.pos++
				continue
			}
			if inDouble {
				if c == '"' {
					if s.pos+1 < len(s.src) && s.src[s.pos+1] == '"' {
						s.pos += 2
						continue
					}
					inDouble = false
				}
				s.pos++
				continue
			}
			switch c {
			case '\'':
				inSingle = true
			case '"':
				inDouble = true
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					cmd.query = strings.TrimSpace(s.src[start:s.pos])
					s.pos++
					goto afterTarget
				}
			}
			s.pos++
		}
		return nil, newError(sqlstateSyntaxError, "unterminated '(' in COPY query")
	} else {
		tbl, err := parseCopyIdent(&s)
		if err != nil {
			return nil, err
		}
		// Optional schema qualification: public.tbl -> tbl
		s.skip()
		if s.pos < len(s.src) && s.src[s.pos] == '.' {
			s.pos++
			if !strings.EqualFold(tbl, "public") {
				return nil, newError(sqlstateCopyInvalidSchema,
					fmt.Sprintf("schema %q does not exist", tbl))
			}
			tbl, err = parseCopyIdent(&s)
			if err != nil {
				return nil, err
			}
		}
		cmd.table = tbl
		s.skip()
		if s.pos < len(s.src) && s.src[s.pos] == '(' {
			s.pos++
			for {
				col, err := parseCopyIdent(&s)
				if err != nil {
					return nil, err
				}
				cmd.columns = append(cmd.columns, col)
				s.skip()
				if s.pos < len(s.src) && s.src[s.pos] == ',' {
					s.pos++
					continue
				}
				if s.pos < len(s.src) && s.src[s.pos] == ')' {
					s.pos++
					break
				}
				return nil, newError(sqlstateSyntaxError, "expected ',' or ')' in COPY column list")
			}
		}
	}

afterTarget:
	dirWord := s.word()
	switch {
	case strings.EqualFold(dirWord, "FROM"):
		if cmd.query != "" {
			return nil, newError(sqlstateSyntaxError, "COPY (query) only supports TO STDOUT")
		}
		cmd.dir = copyFromStdin
		s.skip()
		if s.pos < len(s.src) && s.src[s.pos] == '\'' {
			return nil, newError(sqlstateFeatureNotSupported,
				"COPY FROM file is not supported: use COPY ... FROM STDIN")
		}
		srcWord := s.word()
		if strings.EqualFold(srcWord, "PROGRAM") {
			return nil, newError(sqlstateFeatureNotSupported,
				"COPY FROM PROGRAM is not supported: use COPY ... FROM STDIN")
		}
		if !strings.EqualFold(srcWord, "STDIN") {
			return nil, newError(sqlstateSyntaxError, "expected STDIN after COPY ... FROM")
		}
	case strings.EqualFold(dirWord, "TO"):
		cmd.dir = copyToStdout
		s.skip()
		if s.pos < len(s.src) && s.src[s.pos] == '\'' {
			return nil, newError(sqlstateFeatureNotSupported,
				"COPY TO file is not supported: use COPY ... TO STDOUT")
		}
		dstWord := s.word()
		if strings.EqualFold(dstWord, "PROGRAM") {
			return nil, newError(sqlstateFeatureNotSupported,
				"COPY TO PROGRAM is not supported: use COPY ... TO STDOUT")
		}
		if !strings.EqualFold(dstWord, "STDOUT") {
			return nil, newError(sqlstateSyntaxError, "expected STDOUT after COPY ... TO")
		}
	default:
		return nil, newError(sqlstateSyntaxError, "expected FROM or TO in COPY")
	}

	// Optional WITH (...) or legacy unparenthesized options.
	s.skip()
	if s.pos < len(s.src) && s.src[s.pos] != ';' {
		if strings.EqualFold(s.peekWord(), "WITH") {
			_ = s.word()
			s.skip()
		}
		if s.pos < len(s.src) && s.src[s.pos] == '(' {
			s.pos++
			for {
				s.skip()
				if s.pos < len(s.src) && s.src[s.pos] == ')' {
					s.pos++
					break
				}
				if err := parseCopyOption(&s, cmd, &delimiterSet, &nullSet, &escapeSet); err != nil {
					return nil, err
				}
				s.skip()
				if s.pos < len(s.src) && s.src[s.pos] == ',' {
					s.pos++
					continue
				}
				if s.pos < len(s.src) && s.src[s.pos] == ')' {
					s.pos++
					break
				}
				return nil, newError(sqlstateSyntaxError, "expected ',' or ')' in COPY options")
			}
		} else {
			for {
				s.skip()
				if s.pos >= len(s.src) || s.src[s.pos] == ';' {
					break
				}
				if err := parseCopyOption(&s, cmd, &delimiterSet, &nullSet, &escapeSet); err != nil {
					return nil, err
				}
			}
		}
	}

	if cmd.format == copyFormatCSV {
		if !delimiterSet {
			cmd.delimiter = ','
		}
		if !nullSet {
			cmd.nullStr = ""
		}
	}
	if !escapeSet {
		cmd.escape = cmd.quote
	}
	if cmd.delimiter == '\n' || cmd.delimiter == '\r' {
		return nil, newError(sqlstateInvalidParameterValue,
			"COPY delimiter cannot be newline or carriage return")
	}
	if cmd.format == copyFormatText && cmd.delimiter == '\\' {
		return nil, newError(sqlstateInvalidParameterValue,
			"COPY delimiter cannot be backslash in text format")
	}
	if strings.ContainsAny(cmd.nullStr, "\r\n") {
		return nil, newError(sqlstateInvalidParameterValue,
			"COPY null representation cannot use newline or carriage return")
	}
	if strings.IndexByte(cmd.nullStr, cmd.delimiter) >= 0 {
		return nil, newError(sqlstateInvalidParameterValue,
			"COPY delimiter cannot appear in the NULL specification")
	}
	if cmd.format == copyFormatCSV {
		if cmd.quote == '\n' || cmd.quote == '\r' {
			return nil, newError(sqlstateInvalidParameterValue,
				"COPY quote cannot be newline or carriage return")
		}
		if cmd.escape == '\n' || cmd.escape == '\r' {
			return nil, newError(sqlstateInvalidParameterValue,
				"COPY escape cannot be newline or carriage return")
		}
		if cmd.delimiter == cmd.quote {
			return nil, newError(sqlstateInvalidParameterValue,
				"COPY delimiter and quote must be different")
		}
		if cmd.delimiter == cmd.escape {
			return nil, newError(sqlstateInvalidParameterValue,
				"COPY delimiter and escape must be different")
		}
	}
	if len(cmd.forceNull) > 0 || len(cmd.forceNotNull) > 0 {
		if cmd.format != copyFormatCSV {
			return nil, newError(sqlstateFeatureNotSupported,
				"COPY FORCE_NULL/FORCE_NOT_NULL is only supported in CSV mode")
		}
		if cmd.dir != copyFromStdin {
			return nil, newError(sqlstateFeatureNotSupported,
				"COPY FORCE_NULL/FORCE_NOT_NULL is only supported in COPY FROM")
		}
	}
	return cmd, nil
}

func parseCopyIdent(s *scanner) (string, error) {
	s.skip()
	if s.pos >= len(s.src) {
		return "", newError(sqlstateSyntaxError, "unexpected end of COPY statement")
	}
	if s.src[s.pos] == '"' {
		s.pos++
		var b strings.Builder
		for s.pos < len(s.src) {
			if s.src[s.pos] == '"' {
				if s.pos+1 < len(s.src) && s.src[s.pos+1] == '"' {
					b.WriteByte('"')
					s.pos += 2
					continue
				}
				s.pos++
				return b.String(), nil
			}
			b.WriteByte(s.src[s.pos])
			s.pos++
		}
		return "", newError(sqlstateSyntaxError, "unterminated quoted identifier in COPY")
	}
	w := s.word()
	if w == "" {
		return "", newError(sqlstateSyntaxError, "expected identifier in COPY")
	}
	return strings.ToLower(w), nil
}

func parseCopyStringLit(s *scanner) (string, error) {
	s.skip()
	if s.pos >= len(s.src) {
		return "", newError(sqlstateSyntaxError, "expected string literal in COPY option")
	}
	escape := false
	if (s.src[s.pos] == 'E' || s.src[s.pos] == 'e') && s.pos+1 < len(s.src) && s.src[s.pos+1] == '\'' {
		escape = true
		s.pos++
	}
	if s.src[s.pos] != '\'' {
		return "", newError(sqlstateSyntaxError, "expected single-quoted string in COPY option")
	}
	s.pos++
	var b strings.Builder
	for s.pos < len(s.src) {
		c := s.src[s.pos]
		if escape && c == '\\' && s.pos+1 < len(s.src) {
			s.pos++
			switch s.src[s.pos] {
			case 't':
				b.WriteByte('\t')
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case '\\':
				b.WriteByte('\\')
			case '\'':
				b.WriteByte('\'')
			default:
				b.WriteByte(s.src[s.pos])
			}
			s.pos++
			continue
		}
		if c == '\'' {
			if s.pos+1 < len(s.src) && s.src[s.pos+1] == '\'' {
				b.WriteByte('\'')
				s.pos += 2
				continue
			}
			s.pos++
			return b.String(), nil
		}
		b.WriteByte(c)
		s.pos++
	}
	return "", newError(sqlstateSyntaxError, "unterminated string literal in COPY option")
}

func parseCopyOption(s *scanner, cmd *copyCommand, delimiterSet, nullSet, escapeSet *bool) error {
	opt := strings.ToUpper(s.word())
	switch opt {
	case "FORMAT":
		val := strings.ToUpper(s.word())
		switch val {
		case "TEXT":
			cmd.format = copyFormatText
		case "CSV":
			cmd.format = copyFormatCSV
		case "BINARY":
			return newError(sqlstateFeatureNotSupported,
				"COPY FORMAT binary is not supported; use text or csv")
		default:
			return newError(sqlstateSyntaxError, fmt.Sprintf("unrecognized COPY format %q", val))
		}
	case "CSV":
		cmd.format = copyFormatCSV
	case "BINARY":
		return newError(sqlstateFeatureNotSupported,
			"COPY BINARY is not supported; use text or csv")
	case "DELIMITER":
		if strings.EqualFold(s.peekWord(), "AS") {
			_ = s.word()
		}
		val, err := parseCopyStringLit(s)
		if err != nil {
			return err
		}
		if len(val) != 1 {
			return newError(sqlstateInvalidParameterValue, "COPY delimiter must be a single one-byte character")
		}
		cmd.delimiter = val[0]
		*delimiterSet = true
	case "NULL":
		if strings.EqualFold(s.peekWord(), "AS") {
			_ = s.word()
		}
		val, err := parseCopyStringLit(s)
		if err != nil {
			return err
		}
		cmd.nullStr = val
		*nullSet = true
	case "QUOTE":
		if strings.EqualFold(s.peekWord(), "AS") {
			_ = s.word()
		}
		val, err := parseCopyStringLit(s)
		if err != nil {
			return err
		}
		if len(val) != 1 {
			return newError(sqlstateInvalidParameterValue, "COPY quote must be a single one-byte character")
		}
		cmd.quote = val[0]
	case "ESCAPE":
		if strings.EqualFold(s.peekWord(), "AS") {
			_ = s.word()
		}
		val, err := parseCopyStringLit(s)
		if err != nil {
			return err
		}
		if len(val) != 1 {
			return newError(sqlstateInvalidParameterValue, "COPY escape must be a single one-byte character")
		}
		cmd.escape = val[0]
		*escapeSet = true
	case "FORCE_NULL":
		cols, err := parseCopyColumnList(s)
		if err != nil {
			return err
		}
		cmd.forceNull = append(cmd.forceNull, cols...)
	case "FORCE_NOT_NULL":
		cols, err := parseCopyColumnList(s)
		if err != nil {
			return err
		}
		cmd.forceNotNull = append(cmd.forceNotNull, cols...)
	case "FORCE":
		if strings.EqualFold(s.peekWord(), "NOT") {
			_ = s.word()
			if !strings.EqualFold(s.word(), "NULL") {
				return newError(sqlstateSyntaxError, "expected NULL after FORCE NOT in COPY")
			}
			for {
				col, err := parseCopyIdent(s)
				if err != nil {
					return err
				}
				cmd.forceNotNull = append(cmd.forceNotNull, col)
				s.skip()
				if s.pos < len(s.src) && s.src[s.pos] == ',' {
					s.pos++
					continue
				}
				break
			}
		} else {
			return newError(sqlstateSyntaxError, "expected NOT NULL after FORCE in COPY")
		}
	case "HEADER":
		next := strings.ToUpper(s.peekWord())
		switch next {
		case "TRUE", "ON", "1":
			_ = s.word()
			cmd.header = true
		case "FALSE", "OFF", "0":
			_ = s.word()
			cmd.header = false
		default:
			cmd.header = true
		}
	case "ENCODING":
		_, err := parseCopyStringLit(s)
		if err != nil {
			return err
		}
	default:
		return newError(sqlstateSyntaxError, fmt.Sprintf("unrecognized COPY option %q", opt))
	}
	return nil
}

func parseCopyColumnList(s *scanner) ([]string, error) {
	s.skip()
	if s.pos < len(s.src) && s.src[s.pos] == '*' {
		s.pos++
		return []string{"*"}, nil
	}
	if s.pos >= len(s.src) || s.src[s.pos] != '(' {
		return nil, newError(sqlstateSyntaxError, "expected '(' or '*' in COPY column list option")
	}
	s.pos++
	var cols []string
	for {
		col, err := parseCopyIdent(s)
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		s.skip()
		if s.pos < len(s.src) && s.src[s.pos] == ',' {
			s.pos++
			continue
		}
		if s.pos < len(s.src) && s.src[s.pos] == ')' {
			s.pos++
			return cols, nil
		}
		return nil, newError(sqlstateSyntaxError, "expected ',' or ')' in COPY column list option")
	}
}

func resolveCopyForceFlags(cols []string, names []string) ([]bool, error) {
	if len(names) == 0 {
		return nil, nil
	}
	flags := make([]bool, len(cols))
	for _, name := range names {
		if name == "*" {
			for i := range flags {
				flags[i] = true
			}
			continue
		}
		matched := false
		for i, col := range cols {
			if strings.EqualFold(col, name) {
				flags[i] = true
				matched = true
				break
			}
		}
		if !matched {
			return nil, newError(sqlstateInvalidParameterValue,
				fmt.Sprintf("COPY column %q does not exist in target column list", name))
		}
	}
	return flags, nil
}

func (s *session) executeCopy(cmd *copyCommand) error {
	if s.sql == nil {
		return newError(sqlstateFeatureNotSupported,
			"COPY requires a writable SQL catalog session")
	}
	if cmd.dir == copyFromStdin {
		return s.executeCopyFromStdin(cmd)
	}
	return s.executeCopyToStdout(cmd)
}

func (s *session) resolveCopyColumns(cmd *copyCommand) ([]string, map[string]sqldriver.ColumnInfo, error) {
	ctx := s.operationContext()
	tables, err := s.sql.Tables(ctx)
	if err != nil {
		return nil, nil, asPGError(err)
	}
	var tableInfo *sqldriver.TableInfo
	for i := range tables {
		if strings.EqualFold(tables[i].Name, cmd.table) {
			tableInfo = &tables[i]
			break
		}
	}
	if tableInfo == nil {
		return nil, nil, newError(sqlstateUndefinedTable,
			fmt.Sprintf("relation %q does not exist", cmd.table))
	}
	colMap := make(map[string]sqldriver.ColumnInfo, len(tableInfo.Columns))
	for _, c := range tableInfo.Columns {
		name := strings.TrimPrefix(c.Path, "/")
		colMap[strings.ToLower(name)] = c
	}
	if len(cmd.columns) > 0 {
		return cmd.columns, colMap, nil
	}
	if len(tableInfo.Columns) == 0 {
		return nil, nil, newError(sqlstateSyntaxError,
			fmt.Sprintf("COPY on schemaless table %q requires an explicit column list", cmd.table))
	}
	cols := make([]string, len(tableInfo.Columns))
	for i, c := range tableInfo.Columns {
		cols[i] = strings.TrimPrefix(c.Path, "/")
	}
	return cols, colMap, nil
}

var (
	copyTrueBytes  = []byte{'t'}
	copyFalseBytes = []byte{'f'}
)

type copyField struct {
	data   []byte
	isNull bool
}

type copyLineParser struct {
	fields  []copyField
	scratch []byte
}

type copyValueSlot struct {
	i64     int64
	f64     float64
	boolean bool
	str     string
}

func (s *session) executeCopyFromStdin(cmd *copyCommand) error {
	cols, colMap, err := s.resolveCopyColumns(cmd)
	if err != nil {
		return err
	}
	forceNull, err := resolveCopyForceFlags(cols, cmd.forceNull)
	if err != nil {
		return err
	}
	forceNotNull, err := resolveCopyForceFlags(cols, cmd.forceNotNull)
	if err != nil {
		return err
	}
	colInfos := make([]sqldriver.ColumnInfo, len(cols))
	for i, colName := range cols {
		colInfos[i] = colMap[strings.ToLower(colName)]
	}
	ctx := s.operationContext()

	// Build prepared INSERT INTO table (c1, c2, ...) VALUES (?, ?, ...),
	// or whole-document INSERT INTO table VALUES (?) for COPY tbl ("$doc").
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(quoteCopyIdent(cmd.table))
	if len(cols) == 1 && cols[0] == "$doc" {
		sb.WriteString(" VALUES (?)")
	} else {
		sb.WriteString(" (")
		for i, c := range cols {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quoteCopyIdent(c))
		}
		sb.WriteString(") VALUES (")
		for i := range cols {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteByte('?')
		}
		sb.WriteByte(')')
	}

	stmt, err := s.sql.Prepare(ctx, sb.String())
	if err != nil {
		return asPGError(err)
	}
	defer stmt.Close()

	// Send CopyInResponse and flush so the client begins streaming CopyData.
	s.w.copyInResponse(formatText, len(cols))
	if err := s.w.flush(); err != nil {
		return err
	}

	// Wrap multi-row COPY in an implicit transaction when idle and backend supports tx.
	startedTx := false
	if s.sql.State() == sqldriver.SessionIdle && !autocommitWrites(s.sql) {
		if beginErr := s.sql.Begin(ctx, sqldriver.TxOptions{}); beginErr == nil {
			startedTx = true
		}
	}
	rollbackIfNeeded := func() {
		if startedTx {
			_ = s.sql.Rollback(ctx)
			startedTx = false
		}
	}

	var (
		pending   []byte
		rowCount  int64
		lineNum   int64
		eofMarker bool
		copyErr   error
		parser    = copyLineParser{fields: make([]copyField, 0, len(cols))}
		slots     = make([]copyValueSlot, len(cols))
		args      = make([]any, len(cols))
	)

	processLine := func(line []byte) error {
		lineNum++
		// Strip trailing \r for CRLF inputs.
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if cmd.format == copyFormatText && len(line) == 2 && line[0] == '\\' && line[1] == '.' {
			eofMarker = true
			return nil
		}
		if cmd.header && lineNum == 1 {
			return nil
		}
		if parseErr := parser.parseLine(line, cmd, forceNull, forceNotNull); parseErr != nil {
			return parseErr
		}
		if len(parser.fields) != len(cols) {
			return newError(sqlstateCopyDataException,
				fmt.Sprintf("COPY line %d: expected %d columns, got %d", lineNum, len(cols), len(parser.fields)))
		}
		for i, f := range parser.fields {
			if f.isNull {
				args[i] = nil
				continue
			}
			val, convErr := convertCopyFieldSlot(f.data, colInfos[i], &slots[i])
			if convErr != nil {
				return newError(sqlstateInvalidTextRepresentation,
					fmt.Sprintf("COPY line %d, column %s: %v", lineNum, cols[i], convErr))
			}
			args[i] = val
		}
		if _, execErr := stmt.Exec(ctx, args); execErr != nil {
			return asPGError(execErr)
		}
		rowCount++
		return nil
	}

	for {
		tag, body, readErr := s.r.next(maxMessageBody)
		if readErr != nil {
			rollbackIfNeeded()
			return readErr
		}
		switch tag {
		case msgCopyData:
			if copyErr != nil || eofMarker {
				continue
			}
			var buf []byte
			usingPending := len(pending) > 0
			if usingPending {
				pending = append(pending, body...)
				buf = pending
			} else {
				buf = body
			}
			consumed := 0
			for consumed < len(buf) {
				var nl int
				if cmd.format == copyFormatCSV {
					nl = scanCSVRecord(buf[consumed:], cmd.delimiter, cmd.quote, cmd.escape)
				} else {
					nl = bytes.IndexByte(buf[consumed:], '\n')
				}
				if nl < 0 {
					break
				}
				line := buf[consumed : consumed+nl]
				consumed += nl + 1
				if err := processLine(line); err != nil {
					copyErr = err
					break
				}
				if eofMarker {
					consumed = len(buf)
					break
				}
			}
			if copyErr != nil || eofMarker {
				pending = pending[:0]
			} else if usingPending {
				if consumed == len(pending) {
					pending = pending[:0]
				} else if consumed > 0 {
					rem := copy(pending, pending[consumed:])
					pending = pending[:rem]
				}
			} else if consumed < len(body) {
				pending = append(pending[:0], body[consumed:]...)
			}
		case msgCopyDone:
			if copyErr == nil && !eofMarker && len(pending) > 0 {
				copyErr = processLine(pending)
			}
			if copyErr != nil {
				rollbackIfNeeded()
				return copyErr
			}
			if startedTx {
				if commitErr := s.sql.Commit(ctx); commitErr != nil {
					return asPGError(commitErr)
				}
				startedTx = false
			}
			s.w.commandComplete(fmt.Sprintf("COPY %d", rowCount))
			return nil
		case msgCopyFail:
			rollbackIfNeeded()
			msg := string(bytes.TrimRight(body, "\x00"))
			if msg == "" {
				msg = "COPY from stdin failed"
			}
			return newError(sqlstateQueryCanceled, msg)
		case msgFlush, msgSync:
			// Ignore Flush/Sync while draining CopyIn until CopyDone/CopyFail.
			continue
		default:
			rollbackIfNeeded()
			return newError(sqlstateProtocolViolation,
				fmt.Sprintf("unexpected message %q during COPY FROM STDIN", tag))
		}
	}
}

func (s *session) executeCopyToStdout(cmd *copyCommand) error {
	ctx := s.operationContext()
	var selectSQL string
	if cmd.query != "" {
		selectSQL = cmd.query
	} else {
		var sb strings.Builder
		sb.WriteString("SELECT ")
		if len(cmd.columns) == 0 {
			sb.WriteByte('*')
		} else {
			for i, c := range cmd.columns {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(quoteCopyIdent(c))
			}
		}
		sb.WriteString(" FROM ")
		sb.WriteString(quoteCopyIdent(cmd.table))
		selectSQL = sb.String()
	}

	stmt, err := s.sql.Prepare(ctx, selectSQL)
	if err != nil {
		return asPGError(err)
	}
	defer stmt.Close()

	var rows BackendRows
	if err := stmt.QueryInto(ctx, nil, &rows); err != nil {
		return asPGError(err)
	}
	defer rows.Close()

	colNames := stmt.Columns()
	s.w.copyOutResponse(formatText, len(colNames))

	lineBuf := make([]byte, 0, 256)
	cellScratch := make([]byte, 0, 64)
	if cmd.header {
		lineBuf = appendCopyHeaderRow(lineBuf[:0], colNames, cmd)
		s.w.copyData(lineBuf)
	}

	var count int64
	for rows.Next() {
		lineBuf = lineBuf[:0]
		for i := range colNames {
			if i > 0 {
				lineBuf = append(lineBuf, cmd.delimiter)
			}
			lineBuf, cellScratch = appendCopyCell(lineBuf, cellScratch, rows.Cell(i), cmd)
		}
		lineBuf = append(lineBuf, '\n')
		s.w.copyData(lineBuf)
		count++
	}
	if err := rows.Close(); err != nil {
		return asPGError(err)
	}
	s.w.copyDone()
	s.w.commandComplete(fmt.Sprintf("COPY %d", count))
	return nil
}

func quoteCopyIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// scanCSVRecord returns the index of the first unquoted '\n' in buf,
// or -1 if buf does not yet contain a complete CSV record.
func scanCSVRecord(buf []byte, delim, quote, escape byte) int {
	inQuote := false
	atFieldStart := true
	for i := 0; i < len(buf); i++ {
		c := buf[i]
		if inQuote {
			if c == escape {
				if escape == quote {
					if i+1 < len(buf) {
						if buf[i+1] == quote {
							i++
							continue
						}
						inQuote = false
					} else {
						// Quote at end of chunk: wait for next chunk or CopyDone.
						return -1
					}
				} else {
					if i+1 < len(buf) {
						i++
						continue
					}
					return -1
				}
			} else if c == quote {
				inQuote = false
			}
			continue
		}
		if atFieldStart && c == quote {
			inQuote = true
			atFieldStart = false
			continue
		}
		if c == '\n' {
			return i
		}
		atFieldStart = (c == delim)
	}
	return -1
}

func (p *copyLineParser) parseLine(line []byte, cmd *copyCommand, forceNull, forceNotNull []bool) error {
	p.fields = p.fields[:0]
	p.scratch = p.scratch[:0]
	if cmd.format == copyFormatCSV {
		return p.parseCSVLine(line, cmd, forceNull, forceNotNull)
	}
	return p.parseTextLine(line, cmd)
}

func (p *copyLineParser) parseTextLine(line []byte, cmd *copyCommand) error {
	delim := cmd.delimiter
	nullStr := cmd.nullStr
	if bytes.IndexByte(line, '\\') < 0 {
		// Fast path: no backslashes anywhere in the line — zero unescaping needed.
		pos := 0
		for {
			idx := bytes.IndexByte(line[pos:], delim)
			var part []byte
			if idx < 0 {
				part = line[pos:]
			} else {
				part = line[pos : pos+idx]
			}
			if len(part) == len(nullStr) && string(part) == nullStr {
				p.fields = append(p.fields, copyField{isNull: true})
			} else {
				p.fields = append(p.fields, copyField{data: part})
			}
			if idx < 0 {
				return nil
			}
			pos += idx + 1
		}
	}
	// Backslash-aware scan: an escaped delimiter (e.g. \|) stays inside the field.
	start := 0
	for i := 0; i < len(line); i++ {
		c := line[i]
		if c == '\\' {
			if i+1 >= len(line) {
				return newError(sqlstateCopyDataException, "invalid TEXT line: trailing backslash at end of line")
			}
			i++
			continue
		}
		if c == delim {
			part := line[start:i]
			if len(part) == len(nullStr) && string(part) == nullStr {
				p.fields = append(p.fields, copyField{isNull: true})
			} else {
				p.fields = append(p.fields, copyField{data: p.unescapeText(part)})
			}
			start = i + 1
		}
	}
	part := line[start:]
	if len(part) == len(nullStr) && string(part) == nullStr {
		p.fields = append(p.fields, copyField{isNull: true})
	} else {
		p.fields = append(p.fields, copyField{data: p.unescapeText(part)})
	}
	return nil
}

func (p *copyLineParser) parseCSVLine(line []byte, cmd *copyCommand, forceNull, forceNotNull []bool) error {
	n := len(line)
	delim := cmd.delimiter
	quote := cmd.quote
	escape := cmd.escape
	nullStr := cmd.nullStr
	if n == 0 {
		// An empty CSV line represents a single empty unquoted field (e.g. NULL in a 1-column table).
		isNull := len(nullStr) == 0
		if forceNotNull != nil && len(forceNotNull) > 0 && forceNotNull[0] {
			isNull = false
		}
		p.fields = append(p.fields, copyField{isNull: isNull})
		return nil
	}
	i := 0
	for {
		colIdx := len(p.fields)
		var data []byte
		quoted := false

		if i < n && line[i] == quote {
			quoted = true
			i++ // skip opening quote
			start := i
			needScratch := false
			closed := false
			for i < n {
				c := line[i]
				if c == escape {
					if escape == quote {
						if i+1 < n && line[i+1] == quote {
							needScratch = true
							break
						}
						// Closing quote
						data = line[start:i]
						i++
						closed = true
						break
					}
					needScratch = true
					break
				}
				if c == quote {
					data = line[start:i]
					i++
					closed = true
					break
				}
				i++
			}
			if needScratch {
				sStart := len(p.scratch)
				p.scratch = append(p.scratch, line[start:i]...)
				for i < n {
					c := line[i]
					if c == escape {
						if escape == quote {
							if i+1 < n && line[i+1] == quote {
								p.scratch = append(p.scratch, quote)
								i += 2
								continue
							}
							i++
							closed = true
							break
						}
						if i+1 >= n {
							return newError(sqlstateCopyDataException, "invalid CSV line: unterminated escape sequence")
						}
						p.scratch = append(p.scratch, line[i+1])
						i += 2
						continue
					}
					if c == quote {
						i++
						closed = true
						break
					}
					p.scratch = append(p.scratch, c)
					i++
				}
				data = p.scratch[sStart:len(p.scratch):len(p.scratch)]
			}
			if !closed {
				return newError(sqlstateCopyDataException, "invalid CSV line: unterminated quoted field")
			}
			if i < n && line[i] != delim {
				return newError(sqlstateCopyDataException, "invalid CSV line: unexpected character after closing quote")
			}
		} else {
			start := i
			for i < n && line[i] != delim {
				if line[i] == quote {
					return newError(sqlstateCopyDataException, "invalid CSV line: unescaped quote in unquoted field")
				}
				i++
			}
			data = line[start:i]
		}

		var isNull bool
		if forceNotNull != nil && colIdx < len(forceNotNull) && forceNotNull[colIdx] {
			isNull = false
		} else if forceNull != nil && colIdx < len(forceNull) && forceNull[colIdx] {
			isNull = len(data) == len(nullStr) && string(data) == nullStr
		} else {
			isNull = !quoted && len(data) == len(nullStr) && string(data) == nullStr
		}
		p.fields = append(p.fields, copyField{data: data, isNull: isNull})

		if i >= n {
			return nil
		}
		// line[i] == delim
		i++
		if i == n {
			// Trailing delimiter produces one final empty unquoted field.
			colIdx = len(p.fields)
			var tailNull bool
			if forceNotNull != nil && colIdx < len(forceNotNull) && forceNotNull[colIdx] {
				tailNull = false
			} else {
				tailNull = len(nullStr) == 0
			}
			p.fields = append(p.fields, copyField{isNull: tailNull})
			return nil
		}
	}
}

func (p *copyLineParser) unescapeText(b []byte) []byte {
	first := bytes.IndexByte(b, '\\')
	if first < 0 {
		return b
	}
	start := len(p.scratch)
	p.scratch = append(p.scratch, b[:first]...)
	for i := first; i < len(b); i++ {
		if b[i] == '\\' && i+1 < len(b) {
			i++
			c := b[i]
			switch c {
			case 'b':
				p.scratch = append(p.scratch, '\b')
			case 'f':
				p.scratch = append(p.scratch, '\f')
			case 'n':
				p.scratch = append(p.scratch, '\n')
			case 'r':
				p.scratch = append(p.scratch, '\r')
			case 't':
				p.scratch = append(p.scratch, '\t')
			case 'v':
				p.scratch = append(p.scratch, '\v')
			case '\\':
				p.scratch = append(p.scratch, '\\')
			case '0', '1', '2', '3', '4', '5', '6', '7':
				val := c - '0'
				for k := 0; k < 2 && i+1 < len(b); k++ {
					next := b[i+1]
					if next < '0' || next > '7' {
						break
					}
					val = (val << 3) | (next - '0')
					i++
				}
				p.scratch = append(p.scratch, val)
			case 'x':
				if i+1 < len(b) {
					if d1, ok := copyHexDigit(b[i+1]); ok {
						i++
						val := d1
						if i+1 < len(b) {
							if d2, ok := copyHexDigit(b[i+1]); ok {
								i++
								val = (val << 4) | d2
							}
						}
						p.scratch = append(p.scratch, val)
						continue
					}
				}
				p.scratch = append(p.scratch, 'x')
			default:
				p.scratch = append(p.scratch, c)
			}
			continue
		}
		p.scratch = append(p.scratch, b[i])
	}
	return p.scratch[start:len(p.scratch):len(p.scratch)]
}

func copyHexDigit(b byte) (byte, bool) {
	switch {
	case b >= '0' && b <= '9':
		return b - '0', true
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10, true
	case b >= 'A' && b <= 'F':
		return b - 'A' + 10, true
	default:
		return 0, false
	}
}

func parseCopyBool(raw []byte) (bool, bool) {
	switch len(raw) {
	case 1:
		switch raw[0] | 0x20 {
		case 't', 'y':
			return true, true
		case 'f', 'n':
			return false, true
		}
		if raw[0] == '1' {
			return true, true
		}
		if raw[0] == '0' {
			return false, true
		}
	case 2:
		c0, c1 := raw[0]|0x20, raw[1]|0x20
		if c0 == 'o' && c1 == 'n' {
			return true, true
		}
		if c0 == 'n' && c1 == 'o' {
			return false, true
		}
	case 3:
		c0, c1, c2 := raw[0]|0x20, raw[1]|0x20, raw[2]|0x20
		if c0 == 'y' && c1 == 'e' && c2 == 's' {
			return true, true
		}
		if c0 == 'o' && c1 == 'f' && c2 == 'f' {
			return false, true
		}
	case 4:
		if raw[0]|0x20 == 't' && raw[1]|0x20 == 'r' && raw[2]|0x20 == 'u' && raw[3]|0x20 == 'e' {
			return true, true
		}
	case 5:
		if raw[0]|0x20 == 'f' && raw[1]|0x20 == 'a' && raw[2]|0x20 == 'l' && raw[3]|0x20 == 's' && raw[4]|0x20 == 'e' {
			return false, true
		}
	}
	return false, false
}

func isCopyIntBytes(raw []byte) bool {
	if len(raw) == 0 {
		return false
	}
	i := 0
	if raw[0] == '-' || raw[0] == '+' {
		i = 1
		if i == len(raw) {
			return false
		}
	}
	for ; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return false
		}
	}
	return true
}

func convertCopyFieldSlot(raw []byte, col sqldriver.ColumnInfo, slot *copyValueSlot) (any, error) {
	t := col.Types &^ sqlast.TypeNull
	switch t {
	case sqlast.TypeInteger:
		v, err := strconv.ParseInt(byteview.String(raw), 10, 64)
		if err != nil {
			return nil, err
		}
		slot.i64 = v
		return &slot.i64, nil
	case sqlast.TypeNumber, sqlast.TypeInteger | sqlast.TypeNumber:
		s := byteview.String(raw)
		if isCopyIntBytes(raw) {
			if v, err := strconv.ParseInt(s, 10, 64); err == nil {
				slot.i64 = v
				return &slot.i64, nil
			}
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("invalid input syntax for type numeric: %q", raw)
		}
		slot.f64 = v
		return &slot.f64, nil
	case sqlast.TypeBool:
		b, ok := parseCopyBool(raw)
		if !ok {
			return nil, fmt.Errorf("invalid input syntax for type boolean: %q", raw)
		}
		slot.boolean = b
		return &slot.boolean, nil
	case sqlast.TypeObject, sqlast.TypeArray, sqlast.TypeObject | sqlast.TypeArray:
		if err := vibejson.Validate(raw); err != nil {
			return nil, fmt.Errorf("invalid input syntax for type json")
		}
		return vibejson.RawValue{Src: raw}, nil
	default:
		slot.str = byteview.String(raw)
		return &slot.str, nil
	}
}

func appendCopyHeaderRow(dst []byte, colNames []string, cmd *copyCommand) []byte {
	for i, name := range colNames {
		if i > 0 {
			dst = append(dst, cmd.delimiter)
		}
		raw := byteview.Bytes(name)
		if cmd.format == copyFormatCSV {
			dst = appendCopyCSVField(dst, raw, cmd.delimiter, cmd.quote, cmd.escape, cmd.nullStr, false)
		} else {
			dst = appendCopyTextField(dst, raw, cmd.delimiter)
		}
	}
	return append(dst, '\n')
}

func appendCopyCell(dst, cellScratch []byte, cell query.Cell, cmd *copyCommand) ([]byte, []byte) {
	if cell.IsNull() {
		return append(dst, cmd.nullStr...), cellScratch
	}
	var val []byte
	switch cell.Type() {
	case query.TypeString:
		val, _ = cell.TextBytes()
	case query.TypeBool:
		if b, _ := cell.Bool(); b {
			val = copyTrueBytes
		} else {
			val = copyFalseBytes
		}
	default:
		if raw := cell.Payload(); raw != nil {
			val = raw
		} else {
			cellScratch = cell.AppendJSON(cellScratch[:0])
			val = cellScratch
		}
	}
	if cmd.format == copyFormatCSV {
		return appendCopyCSVField(dst, val, cmd.delimiter, cmd.quote, cmd.escape, cmd.nullStr, true), cellScratch
	}
	return appendCopyTextField(dst, val, cmd.delimiter), cellScratch
}

func appendCopyTextField(dst, src []byte, delimiter byte) []byte {
	start := 0
	for i := 0; i < len(src); i++ {
		c := src[i]
		var esc byte
		switch c {
		case '\\':
			esc = '\\'
		case '\b':
			esc = 'b'
		case '\f':
			esc = 'f'
		case '\n':
			esc = 'n'
		case '\r':
			esc = 'r'
		case '\t':
			esc = 't'
		case '\v':
			esc = 'v'
		default:
			if c == delimiter {
				esc = delimiter
			} else {
				continue
			}
		}
		dst = append(dst, src[start:i]...)
		dst = append(dst, '\\', esc)
		start = i + 1
	}
	return append(dst, src[start:]...)
}

func appendCopyCSVField(dst, src []byte, delimiter, quote, escape byte, nullStr string, checkNull bool) []byte {
	needsQuote := checkNull && len(src) == len(nullStr) && string(src) == nullStr
	if !needsQuote {
		for i := 0; i < len(src); i++ {
			c := src[i]
			if c == delimiter || c == quote || c == '\n' || c == '\r' || (escape != quote && c == escape) {
				needsQuote = true
				break
			}
		}
	}
	if !needsQuote {
		return append(dst, src...)
	}
	dst = append(dst, quote)
	start := 0
	for i := 0; i < len(src); i++ {
		c := src[i]
		if c == quote || c == escape {
			dst = append(dst, src[start:i]...)
			dst = append(dst, escape, c)
			start = i + 1
		}
	}
	dst = append(dst, src[start:]...)
	return append(dst, quote)
}
