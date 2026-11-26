package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson/x/byteview"
)

// extractInsertSelectSource returns the exact SELECT query substring from an
// INSERT INTO tbl SELECT ... [ON CONFLICT ...] [RETURNING ...] statement.
func extractInsertSelectSource(text string, insert *sqlast.InsertStmt) (string, error) {
	if insert == nil || insert.Source == nil || insert.SourcePos <= 0 || insert.SourcePos >= len(text) {
		return "", ErrReplicatedSQLTransactionUnsupported
	}
	end := len(text)
	if insert.OnConflictPos > insert.SourcePos && insert.OnConflictPos < end {
		end = insert.OnConflictPos
	}
	if insert.Returning != nil {
		if retIdx := findTopLevelKeyword(strings.ToUpper(text[insert.SourcePos:end]), "RETURNING"); retIdx >= 0 {
			end = insert.SourcePos + retIdx
		}
	}
	source := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(text[insert.SourcePos:end]), ";"))
	if source == "" {
		return "", ErrReplicatedSQLTransactionUnsupported
	}
	return source, nil
}

// execInsertSelect evaluates the source SELECT via the distributed Executor and
// writes the resulting whole documents as an atomic VALUES batch.
func (p *postgresWriteStatement) execInsertSelect(
	ctx context.Context,
	params []shardservice.Param,
) (*Result, error) {
	s := p.session
	sourceRes, err := s.backend.Executor.Query(ctx, Query{
		SQL: p.insertSelectSQL, Params: params, ParamTypes: p.paramTypes, Class: ClassBatch,
	})
	if err != nil {
		return nil, err
	}
	if len(sourceRes.Rows) == 0 {
		return &Result{Kind: shardservice.ResponseCompletion, RowsAffected: 0}, nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(quoteSQLIdentifier(p.table))
	b.WriteString(" VALUES ")
	docParams := make([]shardservice.Param, 0, len(sourceRes.Rows))
	for i, row := range sourceRes.Rows {
		if len(row) != 1 || row[0].Null {
			return nil, ErrMalformedRequest
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(?)")
		docParams = append(docParams, shardservice.DocumentParam(string(row[0].Bytes)))
	}
	if p.insertOnConflictDoNothing {
		b.WriteString(" ON CONFLICT DO NOTHING")
	}
	if p.returning != nil {
		b.WriteString(` RETURNING "$doc"`)
	}
	return s.backend.Write(ctx, s.authority, Query{
		SQL: b.String(), Params: docParams, Class: ClassBatch,
	})
}

// execGeneralDeleteOrUpdate resolves matching documents for a non-primary-key
// DELETE or UPDATE (including unfiltered DELETE, range/secondary predicates,
// ORDER BY, and LIMIT) via the distributed Executor, then dispatches exact
// primary-key mutations through s.backend.Write.
func (p *postgresWriteStatement) execGeneralDeleteOrUpdate(
	ctx context.Context,
	params []shardservice.Param,
) (*Result, error) {
	s := p.session
	if s.backend.Executor == nil || s.backend.Executor.catalog == nil {
		return nil, ErrReplicatedSQLTransactionUnsupported
	}
	snap := s.backend.Executor.catalog.Current()
	if snap == nil {
		return nil, ErrNoCatalog
	}
	entry, ok := snap.replicatedTableAtBytes(byteview.Bytes(p.table))
	if !ok {
		return nil, ErrReplicatedSQLTransactionUnsupported
	}
	tableProfile, ok := snap.replicatedTableProfileAt(entry)
	if !ok || tableProfile.PrimaryKey == "" {
		return nil, ErrReplicatedSQLTransactionUnsupported
	}
	pkPath, err := pointerToSQLPath(tableProfile.PrimaryKey)
	if err != nil {
		return nil, ErrReplicatedSQLTransactionUnsupported
	}

	switch p.kind {
	case sqlast.KindDelete:
		selectSQL := buildSelectFromDelete(p.text, p.table)
		if selectSQL == "" {
			return nil, ErrReplicatedSQLTransactionUnsupported
		}
		matched, err := s.backend.Executor.Query(ctx, Query{
			SQL: selectSQL, Params: params, ParamTypes: p.paramTypes, Class: ClassBatch,
		})
		if err != nil {
			return nil, err
		}
		if len(matched.Rows) == 0 {
			return &Result{Kind: shardservice.ResponseCompletion, RowsAffected: 0}, nil
		}
		pkParams := make([]shardservice.Param, 0, len(matched.Rows))
		var b strings.Builder
		b.WriteString("DELETE FROM ")
		b.WriteString(quoteSQLIdentifier(p.table))
		b.WriteString(" WHERE ")
		b.WriteString(pkPath)
		b.WriteString(" IN (")
		for i, row := range matched.Rows {
			if len(row) != 1 || row[0].Null {
				return nil, ErrMalformedRequest
			}
			pkParam, err := extractPrimaryKeyParam(row[0].Bytes, tableProfile.PrimaryKey)
			if err != nil {
				return nil, err
			}
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteByte('?')
			pkParams = append(pkParams, pkParam)
		}
		b.WriteByte(')')
		if p.returning != nil {
			b.WriteString(` RETURNING "$doc"`)
		}
		return s.backend.Write(ctx, s.authority, Query{
			SQL: b.String(), Params: pkParams, Class: ClassBatch,
		})

	case sqlast.KindUpdate:
		setPrefix, whereTail, setParamCount, ok := splitUpdateForGeneralRewrite(p.text, p.table)
		if !ok || setParamCount > len(params) {
			return nil, ErrReplicatedSQLTransactionUnsupported
		}
		selectSQL := `SELECT "$doc" FROM ` + whereTail
		filterParams := params[setParamCount:]
		var filterParamTypes []shardservice.Param
		_ = filterParamTypes
		matched, err := s.backend.Executor.Query(ctx, Query{
			SQL: selectSQL, Params: filterParams, Class: ClassBatch,
		})
		if err != nil {
			return nil, err
		}
		if len(matched.Rows) == 0 {
			return &Result{Kind: shardservice.ResponseCompletion, RowsAffected: 0}, nil
		}
		total := &Result{Kind: shardservice.ResponseCompletion}
		updateSQL := setPrefix + " WHERE " + pkPath + " = ?"
		if p.returning != nil {
			updateSQL += ` RETURNING "$doc"`
		}
		setParams := params[:setParamCount]
		for _, row := range matched.Rows {
			if len(row) != 1 || row[0].Null {
				return nil, ErrMalformedRequest
			}
			pkParam, err := extractPrimaryKeyParam(row[0].Bytes, tableProfile.PrimaryKey)
			if err != nil {
				return nil, err
			}
			callParams := make([]shardservice.Param, 0, len(setParams)+1)
			callParams = append(callParams, setParams...)
			callParams = append(callParams, pkParam)
			res, err := s.backend.Write(ctx, s.authority, Query{
				SQL: updateSQL, Params: callParams, Class: ClassBatch,
			})
			if err != nil {
				return nil, err
			}
			if res != nil {
				total.RowsAffected += res.RowsAffected
				total.ReturningDocs = append(total.ReturningDocs, res.ReturningDocs...)
			}
		}
		return total, nil
	}
	return nil, ErrReplicatedSQLTransactionUnsupported
}

func pointerToSQLPath(pointer string) (string, error) {
	if !strings.HasPrefix(pointer, "/") || len(pointer) <= 1 {
		return "", fmt.Errorf("invalid pointer %q", pointer)
	}
	parts := strings.Split(pointer[1:], "/")
	var b strings.Builder
	for i, part := range parts {
		if i > 0 {
			b.WriteByte('.')
		}
		part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		b.WriteString(quoteSQLIdentifier(part))
	}
	return b.String(), nil
}

func extractPrimaryKeyParam(doc []byte, pointer string) (shardservice.Param, error) {
	if !strings.HasPrefix(pointer, "/") || len(pointer) <= 1 {
		return shardservice.Param{}, ErrMalformedRequest
	}
	dec := json.NewDecoder(strings.NewReader(string(doc)))
	dec.UseNumber()
	var current any
	if err := dec.Decode(&current); err != nil {
		return shardservice.Param{}, err
	}
	for _, seg := range strings.Split(pointer[1:], "/") {
		seg = strings.ReplaceAll(strings.ReplaceAll(seg, "~1", "/"), "~0", "~")
		obj, ok := current.(map[string]any)
		if !ok {
			return shardservice.Param{}, ErrMalformedRequest
		}
		next, exists := obj[seg]
		if !exists {
			return shardservice.Param{}, ErrMalformedRequest
		}
		current = next
	}
	switch v := current.(type) {
	case string:
		return shardservice.StringParam(v), nil
	case bool:
		return shardservice.BoolParam(v), nil
	case json.Number:
		return shardservice.NumberParam(v.String()), nil
	default:
		return shardservice.Param{}, ErrMalformedRequest
	}
}

func buildSelectFromDelete(text, table string) string {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(text), ";"))
	upper := strings.ToUpper(trimmed)
	fromIdx := findTopLevelKeyword(upper, "FROM")
	if fromIdx < 0 {
		return ""
	}
	tail := trimmed[fromIdx:]
	if retIdx := findTopLevelKeyword(strings.ToUpper(tail), "RETURNING"); retIdx >= 0 {
		tail = strings.TrimSpace(tail[:retIdx])
	}
	if tail == "" {
		return `SELECT "$doc" FROM ` + quoteSQLIdentifier(table)
	}
	return `SELECT "$doc" ` + tail
}

func splitUpdateForGeneralRewrite(text, table string) (setPrefix, whereTail string, setParams int, ok bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(text), ";"))
	if retIdx := findTopLevelKeyword(strings.ToUpper(trimmed), "RETURNING"); retIdx >= 0 {
		trimmed = strings.TrimSpace(trimmed[:retIdx])
	}
	upper := strings.ToUpper(trimmed)
	setIdx := findTopLevelKeyword(upper, "SET")
	if setIdx < 0 {
		return "", "", 0, false
	}
	// Extract target table + optional alias between UPDATE and SET.
	afterUpdate := strings.TrimSpace(trimmed[len("UPDATE"):setIdx])
	if afterUpdate == "" {
		afterUpdate = quoteSQLIdentifier(table)
	}
	afterSet := trimmed[setIdx:]
	upperAfterSet := strings.ToUpper(afterSet)
	tailIdx := -1
	for _, kw := range []string{"WHERE", "ORDER", "LIMIT"} {
		if idx := findTopLevelKeyword(upperAfterSet, kw); idx >= 0 && (tailIdx < 0 || idx < tailIdx) {
			tailIdx = idx
		}
	}
	if tailIdx < 0 {
		setPrefix = trimmed
		whereTail = afterUpdate
	} else {
		setPrefix = strings.TrimSpace(trimmed[:setIdx+tailIdx])
		whereTail = afterUpdate + " " + strings.TrimSpace(afterSet[tailIdx:])
	}
	setParams = countQuestionPlaceholders(setPrefix)
	return setPrefix, whereTail, setParams, true
}

func findTopLevelKeyword(upper, kw string) int {
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(upper); i++ {
		c := upper[i]
		if inSingle {
			if c == '\'' {
				if i+1 < len(upper) && upper[i+1] == '\'' {
					i++
				} else {
					inSingle = false
				}
			}
			continue
		}
		if inDouble {
			if c == '"' {
				if i+1 < len(upper) && upper[i+1] == '"' {
					i++
				} else {
					inDouble = false
				}
			}
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
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && i+len(kw) <= len(upper) && upper[i:i+len(kw)] == kw {
				beforeOK := i == 0 || !isIdentByte(upper[i-1])
				afterOK := i+len(kw) == len(upper) || !isIdentByte(upper[i+len(kw)])
				if beforeOK && afterOK {
					return i
				}
			}
		}
	}
	return -1
}

func countQuestionPlaceholders(s string) int {
	count := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inSingle {
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
				} else {
					inSingle = false
				}
			}
			continue
		}
		if inDouble {
			if c == '"' {
				if i+1 < len(s) && s[i+1] == '"' {
					i++
				} else {
					inDouble = false
				}
			}
			continue
		}
		switch c {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '?':
			count++
		}
	}
	return count
}

func isIdentByte(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '_'
}
