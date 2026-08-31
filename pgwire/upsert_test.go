package pgwire

import (
	"errors"
	"slices"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestPGWireUpsertBareConflictColumnAmbiguitySQLStateAndPosition(t *testing.T) {
	const source = `INSERT INTO wire_upsert_ambiguity (id, a) VALUES ('x', 1) ON CONFLICT DO UPDATE SET a = a + 1`
	_, err := sqlast.ParseStatement(source)
	var ambiguous *sqlast.AmbiguousColumnError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("bare conflict column error = %T %v, want *sql.AmbiguousColumnError", err, err)
	}

	pg := asPGErrorIn(err, source)
	wantPosition := strings.LastIndex(source, "a + 1") + 1
	if pg.code != sqlstateAmbiguousColumn || pg.position != wantPosition {
		t.Fatalf(
			"bare conflict column => code=%s position=%d, want %s/%d: %v",
			pg.code, pg.position, sqlstateAmbiguousColumn, wantPosition, err,
		)
	}
}

func TestPGWireUpsertValuesExtendedMixedReturning(t *testing.T) {
	c := connectSQLCatalog(t)
	requireWireOK(t, c.query(`
		CREATE TABLE wire_upsert_values (
			id STRING PRIMARY KEY,
			name STRING NOT NULL,
			visits INTEGER NOT NULL,
			note STRING NOT NULL
		)`))
	requireWireOK(t, c.query(`
		INSERT INTO wire_upsert_values VALUES
		({"id":"existing","name":"before","visits":1,"note":"preserved"})
	`))

	statement := `
		INSERT INTO wire_upsert_values VALUES ($1), ($2)
		ON CONFLICT DO UPDATE SET
			name = EXCLUDED.name,
			visits = EXCLUDED.visits
		RETURNING id, name, visits, note
	`
	messages := extendedSQL(c, statement, [][]byte{
		[]byte(`{"id":"existing","name":"updated","visits":9,"note":"ignored"}`),
		[]byte(`{"id":"fresh","name":"inserted","visits":2,"note":"candidate"}`),
	})
	requireWireOK(t, messages)
	assertReadyStatus(t, messages, statusIdle)
	if got := decodeParameterDescription(
		t, find(t, messages, msgParameterDesc).body,
	); !slices.Equal(got, []int32{oidJSON, oidJSON}) {
		t.Fatalf("upsert ParameterDescription = %v, want json/json", got)
	}
	if got := commandTagOf(t, messages); got != "INSERT 0 2" {
		t.Fatalf("mixed upsert tag = %q, want INSERT 0 2", got)
	}
	rows := rowsOf(t, messages)
	if len(rows) != 2 ||
		string(rows[0][0]) != `"existing"` ||
		string(rows[0][1]) != `"updated"` ||
		string(rows[0][2]) != `9` ||
		string(rows[0][3]) != `"preserved"` ||
		string(rows[1][0]) != `"fresh"` ||
		string(rows[1][1]) != `"inserted"` ||
		string(rows[1][2]) != `2` ||
		string(rows[1][3]) != `"candidate"` {
		t.Fatalf("mixed upsert RETURNING post-images = %q", rows)
	}
}

func TestPGWireUpsertValuesCanonicalDuplicateCardinalityAtomicRecovery(t *testing.T) {
	c := connectSQLCatalog(t)
	requireWireOK(t, c.query(`
		CREATE TABLE wire_upsert_cardinality (
			id NUMBER PRIMARY KEY,
			value STRING NOT NULL
		)`))
	requireWireOK(t, c.query(`
		INSERT INTO wire_upsert_cardinality VALUES ({"id":7,"value":"kept"})
	`))

	failure := extendedSQL(c, `
		INSERT INTO wire_upsert_cardinality VALUES ($1), ($2), ($3)
		ON CONFLICT DO UPDATE SET value = EXCLUDED.value
		RETURNING id, value
	`, [][]byte{
		[]byte(`{"id":8,"value":"must-not-publish"}`),
		[]byte(`{"id":1,"value":"first-spelling"}`),
		[]byte(`{"id":1.0,"value":"same-canonical-key"}`),
	})
	expectError(t, failure, sqlstateCardinalityViolation)
	assertReadyStatus(t, failure, statusIdle)
	if has(failure, msgCommandComplete) || has(failure, msgDataRow) {
		t.Fatalf("failed upsert emitted result completion: %s", tags(failure))
	}

	rows := rowsOf(t, c.query(
		`SELECT id, value FROM wire_upsert_cardinality ORDER BY id`,
	))
	if len(rows) != 1 || string(rows[0][0]) != `7` ||
		string(rows[0][1]) != `"kept"` {
		t.Fatalf("cardinality failure published candidates: %q", rows)
	}

	recovered := extendedSQL(c, `
		INSERT INTO wire_upsert_cardinality VALUES ($1)
		ON CONFLICT DO UPDATE SET value = EXCLUDED.value
		RETURNING id, value
	`, [][]byte{[]byte(`{"id":9,"value":"recovered"}`)})
	requireWireOK(t, recovered)
	assertReadyStatus(t, recovered, statusIdle)
	if got := commandTagOf(t, recovered); got != "INSERT 0 1" {
		t.Fatalf("recovery upsert tag = %q, want INSERT 0 1", got)
	}
	rows = rowsOf(t, recovered)
	if len(rows) != 1 || string(rows[0][0]) != `9` ||
		string(rows[0][1]) != `"recovered"` {
		t.Fatalf("recovery upsert RETURNING rows = %q", rows)
	}
}
