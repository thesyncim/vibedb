package pgwire

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/query"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type parseReuseStatement struct {
	BackendStatement
	reusable   bool
	closeCalls int
}

var parseReuseColumns = []string{"id"}

func (s *parseReuseStatement) ReusableForParse() bool { return s.reusable }
func (s *parseReuseStatement) ReturnsRows() bool      { return true }
func (s *parseReuseStatement) NumParams() int         { return 1 }
func (s *parseReuseStatement) ParamKind(int) sqldriver.ParamKind {
	return sqldriver.ParamScalar
}
func (s *parseReuseStatement) ParamPosition(int) int { return 0 }
func (s *parseReuseStatement) Columns() []string     { return parseReuseColumns }
func (s *parseReuseStatement) AppendSchema(dst []query.OutputColumn) []query.OutputColumn {
	return dst
}
func (s *parseReuseStatement) Close() error {
	s.closeCalls++
	return nil
}

type parseBenchmarkSession struct {
	BackendSession
	statement *parseReuseStatement
}

func (s *parseBenchmarkSession) Prepare(context.Context, string) (BackendStatement, error) {
	return s.statement, nil
}

func TestUnnamedParseReusePreservesOwnershipAndAccounting(t *testing.T) {
	runtime := &parseReuseStatement{reusable: true}
	stmt := &prepared{
		name: "", sql: "SELECT id FROM messages WHERE id = $1",
		runtime: runtime, paramOIDs: []int32{oidText},
		retainedBytes: 17, bindBytes: 23,
	}
	oldPortal := &portal{name: "p", stmt: stmt, retainedBytes: 11}
	s := &session{
		statements:         map[string]*prepared{"": stmt},
		portals:            map[string]*portal{"p": oldPortal},
		statementBytes:     stmt.retainedBytes,
		statementBindBytes: stmt.bindBytes,
		portalBytes:        oldPortal.retainedBytes,
	}
	m := &frontendMessage{
		query: stmt.sql, paramOIDs: []int32{oidText},
	}

	if !s.reuseUnnamedParse(stmt, m) {
		t.Fatal("exact reusable unnamed Parse missed")
	}
	if runtime.closeCalls != 0 || stmt.runtime != runtime {
		t.Fatal("reuse closed or replaced the backend statement")
	}
	if len(s.portals) != 0 || s.portalBytes != 0 {
		t.Fatal("reuse retained a portal from the replaced statement")
	}
	if stmt.bindBytes != 0 || s.statementBindBytes != 0 {
		t.Fatal("reuse retained the replaced statement's Bind charge")
	}
	if s.statementBytes != stmt.retainedBytes || s.statements[""] != stmt {
		t.Fatal("reuse changed prepared-statement ownership accounting")
	}
}

func TestUnnamedParseReuseRequiresExactOptedInContract(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		oids    []int32
		runtime BackendStatement
	}{
		{name: "changed SQL", query: "SELECT id FROM messages WHERE id = $2", oids: []int32{oidText}, runtime: &parseReuseStatement{reusable: true}},
		{name: "changed OIDs", query: "SELECT id FROM messages WHERE id = $1", oids: []int32{oidBool}, runtime: &parseReuseStatement{reusable: true}},
		{name: "backend declined", query: "SELECT id FROM messages WHERE id = $1", oids: []int32{oidText}, runtime: &parseReuseStatement{}},
		{name: "backend did not opt in", query: "SELECT id FROM messages WHERE id = $1", oids: []int32{oidText}, runtime: &struct{ BackendStatement }{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt := &prepared{
				name: "", sql: "SELECT id FROM messages WHERE id = $1",
				runtime: test.runtime, paramOIDs: []int32{oidText}, bindBytes: 7,
			}
			s := &session{
				statements:         map[string]*prepared{"": stmt},
				portals:            map[string]*portal{"": {stmt: stmt, retainedBytes: 5}},
				statementBindBytes: 7, portalBytes: 5,
			}
			m := &frontendMessage{query: test.query, paramOIDs: test.oids}
			if s.reuseUnnamedParse(stmt, m) {
				t.Fatal("non-reusable Parse contract hit")
			}
			if len(s.portals) != 1 || s.portalBytes != 5 || stmt.bindBytes != 7 || s.statementBindBytes != 7 {
				t.Fatal("cache miss mutated statement or portal accounting")
			}
		})
	}
}

func TestUnnamedParseReuseHonorsCancellation(t *testing.T) {
	stmt := &prepared{
		name: "", sql: "SELECT id FROM messages WHERE id = $1",
		runtime: &parseReuseStatement{reusable: true}, paramOIDs: []int32{oidText},
		bindBytes: 7,
	}
	s := &session{
		statements:         map[string]*prepared{"": stmt},
		portals:            map[string]*portal{"": {stmt: stmt, retainedBytes: 5}},
		statementBindBytes: 7, portalBytes: 5,
		cancelCheck: func() error { return errors.New("canceled") },
	}
	m := &frontendMessage{query: stmt.sql, paramOIDs: []int32{oidText}}
	if s.reuseUnnamedParse(stmt, m) {
		t.Fatal("canceled Parse reused the prior statement")
	}
	if len(s.portals) != 1 || stmt.bindBytes != 7 {
		t.Fatal("canceled reuse mutated the prior statement")
	}
}

func BenchmarkUnnamedParseFrontend(b *testing.B) {
	const sql = "SELECT id FROM messages WHERE id = $1 ORDER BY id LIMIT 256"
	b.Run("cold", func(b *testing.B) {
		runtime := &parseReuseStatement{}
		s := &session{sql: &parseBenchmarkSession{statement: runtime}}
		b.ReportAllocs()
		for b.Loop() {
			stmt, err := s.prepare("", sql, nil)
			if err != nil {
				b.Fatal(err)
			}
			stmt.release()
		}
	})

	b.Run("exact-reuse", func(b *testing.B) {
		stmt := &prepared{
			name: "", sql: sql,
			runtime: &parseReuseStatement{reusable: true}, paramOIDs: []int32{oidText},
		}
		s := &session{statements: map[string]*prepared{"": stmt}, portals: map[string]*portal{}}
		m := &frontendMessage{query: stmt.sql, paramOIDs: []int32{oidText}}
		b.ReportAllocs()
		for b.Loop() {
			if !s.reuseUnnamedParse(stmt, m) {
				b.Fatal("exact reusable unnamed Parse missed")
			}
		}
	})
}
