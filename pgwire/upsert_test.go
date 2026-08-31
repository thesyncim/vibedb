package pgwire

import (
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestPGWireUpsertBareConflictColumnsBindAgainstCatalog(t *testing.T) {
	c := connectSQLCatalog(t)
	requireWireOK(t, c.query(`
		CREATE TABLE wire_upsert_binding (
			id STRING PRIMARY KEY,
			a INTEGER NOT NULL
		)`))

	tests := []struct {
		name   string
		source string
		marker string
		code   string
	}{
		{
			name: "declared is ambiguous",
			source: `INSERT INTO wire_upsert_binding (id, a) VALUES ('x', 1) ` +
				`ON CONFLICT DO UPDATE SET a = a + 1`,
			marker: "a + 1",
			code:   sqlstateAmbiguousColumn,
		},
		{
			name: "missing is undefined",
			source: `INSERT INTO wire_upsert_binding (id, a) VALUES ('x', 1) ` +
				`ON CONFLICT DO UPDATE SET a = missing + 1`,
			marker: "missing",
			code:   sqlstateUndefinedColumn,
		},
		{
			name: "qualified missing starts at excluded",
			source: `INSERT INTO wire_upsert_binding (id, a) VALUES ('x', 1) ` +
				`ON CONFLICT DO UPDATE SET a = EXCLUDED.missing`,
			marker: "EXCLUDED.missing",
			code:   sqlstateUndefinedColumn,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			messages := c.query(test.source)
			fields := expectError(t, messages, test.code)
			assertReadyStatus(t, messages, statusIdle)
			wantPosition := strconv.Itoa(strings.LastIndex(
				test.source, test.marker,
			) + 1)
			if fields['P'] != wantPosition {
				t.Fatalf(
					"ErrorResponse position = %q, want %q at %q",
					fields['P'], wantPosition, test.marker,
				)
			}
		})
	}

	rows := rowsOf(t, c.query(`SELECT COUNT(*) FROM wire_upsert_binding`))
	if len(rows) != 1 || len(rows[0]) != 1 || string(rows[0][0]) != "0" {
		t.Fatalf("catalog binding errors published a candidate: %q", rows)
	}
}

func TestPGWireMutationTargetAliasErrorsKeepPostgreSQLClasses(t *testing.T) {
	c := connectSQLCatalog(t)
	requireWireOK(t, c.query(`
		CREATE TABLE wire_mutation_alias_error (
			id STRING PRIMARY KEY,
			a INTEGER NOT NULL
		)`))

	hidden := `UPDATE wire_mutation_alias_error AS target ` +
		`SET a = wire_mutation_alias_error.a + 1 WHERE target.id = 'x'`
	messages := c.query(hidden)
	fields := expectError(t, messages, sqlstateUndefinedTable)
	assertReadyStatus(t, messages, statusIdle)
	if want := strconv.Itoa(strings.LastIndex(
		hidden, "wire_mutation_alias_error.a",
	) + 1); fields['P'] != want {
		t.Fatalf("hidden target position = %q, want %q", fields['P'], want)
	}
	if !strings.Contains(fields['H'], `"target"`) {
		t.Fatalf("hidden target hint = %q, want alias guidance", fields['H'])
	}

	ambiguous := `INSERT INTO wire_mutation_alias_error AS excluded (id, a) ` +
		`VALUES ('x', 1) ON CONFLICT DO UPDATE SET a = excluded.a`
	messages = c.query(ambiguous)
	fields = expectError(t, messages, sqlstateAmbiguousAlias)
	assertReadyStatus(t, messages, statusIdle)
	if want := strconv.Itoa(strings.LastIndex(
		ambiguous, "excluded.a",
	) + 1); fields['P'] != want {
		t.Fatalf("ambiguous alias position = %q, want %q", fields['P'], want)
	}

	for _, source := range []string{
		`UPDATE wire_mutation_alias_error AS target SET target.a = 1`,
		`INSERT INTO wire_mutation_alias_error AS target (id, a) ` +
			`VALUES ('x', 1) ON CONFLICT DO UPDATE SET target.a = 1`,
		`INSERT INTO wire_mutation_alias_error AS target (id, a) ` +
			`VALUES ('x', 1) ON CONFLICT DO UPDATE SET EXCLUDED.a = 1`,
	} {
		messages = c.query(source)
		fields = expectError(t, messages, sqlstateUndefinedColumn)
		assertReadyStatus(t, messages, statusIdle)
		marker := "target.a"
		if strings.Contains(source, "SET EXCLUDED.a") {
			marker = "EXCLUDED.a"
		}
		if want := strconv.Itoa(strings.LastIndex(source, marker) + 1); fields['P'] != want {
			t.Fatalf("qualified SET target position = %q, want %q for %q",
				fields['P'], want, source)
		}
		if !strings.Contains(fields['H'], "cannot be qualified") {
			t.Fatalf("qualified SET target hint = %q", fields['H'])
		}
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
