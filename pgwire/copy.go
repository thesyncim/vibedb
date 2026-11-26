package pgwire

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

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
	dir       copyDirection
	table     string
	columns   []string
	query     string // non-empty for COPY (SELECT ...) TO STDOUT
	format    copyFormat
	delimiter byte
	quote     byte
	nullStr   string
	header    bool
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
		nullStr:   `\N`,
	}
	delimiterSet := false
	nullSet := false

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
				if err := parseCopyOption(&s, cmd, &delimiterSet, &nullSet); err != nil {
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
				if err := parseCopyOption(&s, cmd, &delimiterSet, &nullSet); err != nil {
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

func parseCopyOption(s *scanner, cmd *copyCommand, delimiterSet, nullSet *bool) error {
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

func (s *session) executeCopyFromStdin(cmd *copyCommand) error {
	cols, colMap, err := s.resolveCopyColumns(cmd)
	if err != nil {
		return err
	}
	ctx := s.operationContext()

	// Build prepared INSERT INTO table (c1, c2, ...) VALUES (?, ?, ...).
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(quoteCopyIdent(cmd.table))
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
		args      = make([]any, len(cols))
	)

	processLine := func(line []byte) error {
		lineNum++
		// Strip trailing \r for CRLF inputs.
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if cmd.format == copyFormatText && string(line) == `\.` {
			eofMarker = true
			return nil
		}
		if cmd.header && lineNum == 1 {
			return nil
		}
		if len(line) == 0 && cmd.format == copyFormatCSV {
			return nil
		}
		fields, nulls, parseErr := parseCopyLine(line, cmd)
		if parseErr != nil {
			return parseErr
		}
		if len(fields) != len(cols) {
			return newError(sqlstateCopyDataException,
				fmt.Sprintf("COPY line %d: expected %d columns, got %d", lineNum, len(cols), len(fields)))
		}
		for i, colName := range cols {
			if nulls[i] {
				args[i] = nil
				continue
			}
			val, convErr := convertCopyField(fields[i], colMap[strings.ToLower(colName)])
			if convErr != nil {
				return newError(sqlstateInvalidTextRepresentation,
					fmt.Sprintf("COPY line %d, column %s: %v", lineNum, colName, convErr))
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
			pending = append(pending, body...)
			for {
				nl := bytes.IndexByte(pending, '\n')
				if nl < 0 {
					break
				}
				line := pending[:nl]
				if err := processLine(line); err != nil {
					copyErr = err
					break
				}
				pending = pending[nl+1:]
				if eofMarker {
					pending = pending[:0]
					break
				}
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

	var lineBuf bytes.Buffer
	if cmd.header {
		formatCopyRow(&lineBuf, colNames, nil, cmd)
		s.w.copyData(lineBuf.Bytes())
	}

	var count int64
	vals := make([]string, len(colNames))
	nulls := make([]bool, len(colNames))
	for rows.Next() {
		for i := range colNames {
			cell := rows.Cell(i)
			if cell.IsNull() {
				nulls[i] = true
				vals[i] = ""
			} else {
				nulls[i] = false
				vals[i] = formatCopyCell(cell)
			}
		}
		lineBuf.Reset()
		formatCopyRow(&lineBuf, vals, nulls, cmd)
		s.w.copyData(lineBuf.Bytes())
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

func parseCopyLine(line []byte, cmd *copyCommand) ([]string, []bool, error) {
	if cmd.format == copyFormatCSV {
		r := csv.NewReader(bytes.NewReader(line))
		r.Comma = rune(cmd.delimiter)
		r.FieldsPerRecord = -1
		fields, err := r.Read()
		if err != nil && err != io.EOF {
			return nil, nil, newError(sqlstateCopyDataException, fmt.Sprintf("invalid CSV line: %v", err))
		}
		nulls := make([]bool, len(fields))
		for i, f := range fields {
			if f == cmd.nullStr {
				nulls[i] = true
			}
		}
		return fields, nulls, nil
	}
	// PostgreSQL text format: delimiter-separated with backslash escapes.
	rawParts := bytes.Split(line, []byte{cmd.delimiter})
	fields := make([]string, len(rawParts))
	nulls := make([]bool, len(rawParts))
	for i, part := range rawParts {
		if string(part) == cmd.nullStr {
			nulls[i] = true
			continue
		}
		fields[i] = unescapeCopyText(part)
	}
	return fields, nulls, nil
}

func unescapeCopyText(b []byte) string {
	if bytes.IndexByte(b, '\\') < 0 {
		return string(b)
	}
	var out strings.Builder
	out.Grow(len(b))
	for i := 0; i < len(b); i++ {
		if b[i] == '\\' && i+1 < len(b) {
			i++
			switch b[i] {
			case 'b':
				out.WriteByte('\b')
			case 'f':
				out.WriteByte('\f')
			case 'n':
				out.WriteByte('\n')
			case 'r':
				out.WriteByte('\r')
			case 't':
				out.WriteByte('\t')
			case 'v':
				out.WriteByte('\v')
			case '\\':
				out.WriteByte('\\')
			default:
				out.WriteByte(b[i])
			}
			continue
		}
		out.WriteByte(b[i])
	}
	return out.String()
}

func convertCopyField(raw string, col sqldriver.ColumnInfo) (any, error) {
	t := col.Types &^ sqlast.TypeNull
	switch t {
	case sqlast.TypeInteger:
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case sqlast.TypeNumber, sqlast.TypeInteger | sqlast.TypeNumber:
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return v, nil
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case sqlast.TypeBool:
		switch strings.ToLower(raw) {
		case "t", "true", "y", "yes", "on", "1":
			return true, nil
		case "f", "false", "n", "no", "off", "0":
			return false, nil
		default:
			return nil, fmt.Errorf("invalid input syntax for type boolean: %q", raw)
		}
	case sqlast.TypeObject, sqlast.TypeArray, sqlast.TypeObject | sqlast.TypeArray:
		if !json.Valid([]byte(raw)) {
			return nil, fmt.Errorf("invalid input syntax for type json")
		}
		return json.RawMessage(raw), nil
	default:
		return raw, nil
	}
}

func formatCopyCell(cell query.Cell) string {
	switch cell.Type() {
	case query.TypeString:
		raw := cell.AppendJSON(nil)
		var unquoted string
		if json.Unmarshal(raw, &unquoted) == nil {
			return unquoted
		}
		return string(raw)
	case query.TypeBool:
		b, _ := cell.Bool()
		if b {
			return "t"
		}
		return "f"
	default:
		return string(cell.AppendJSON(nil))
	}
}

func formatCopyRow(buf *bytes.Buffer, vals []string, nulls []bool, cmd *copyCommand) {
	if cmd.format == copyFormatCSV {
		w := csv.NewWriter(buf)
		w.Comma = rune(cmd.delimiter)
		out := make([]string, len(vals))
		for i, v := range vals {
			if nulls != nil && nulls[i] {
				out[i] = cmd.nullStr
			} else {
				out[i] = v
			}
		}
		_ = w.Write(out)
		w.Flush()
		return
	}
	for i, v := range vals {
		if i > 0 {
			buf.WriteByte(cmd.delimiter)
		}
		if nulls != nil && nulls[i] {
			buf.WriteString(cmd.nullStr)
			continue
		}
		for j := 0; j < len(v); j++ {
			switch v[j] {
			case '\\':
				buf.WriteString(`\\`)
			case '\n':
				buf.WriteString(`\n`)
			case '\r':
				buf.WriteString(`\r`)
			case '\t':
				buf.WriteString(`\t`)
			default:
				buf.WriteByte(v[j])
			}
		}
	}
	buf.WriteByte('\n')
}
