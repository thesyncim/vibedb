package pgwire

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/conformance"
)

// TestPGWireCapabilityMatrix drives every pgwire manifest row through protocol
// messages, including implicit simple-query batches and explicit BEGIN/COMMIT.
// Every key-count and operation is an independent protocol/catalog subtest. It
// never calls the SQL driver directly.
func TestPGWireCapabilityMatrix(t *testing.T) {
	for _, capability := range conformance.CasesFor(conformance.PGWire) {
		capability := capability
		t.Run(capability.ID, func(t *testing.T) {
			for _, keys := range capability.Keys {
				keys := keys
				t.Run(string(keys), func(t *testing.T) {
					for _, operation := range capability.Operations {
						operation := operation
						t.Run(string(operation), func(t *testing.T) {
							c := connectSQLCatalog(t)
							pgCapabilityMust(t, c.query(`
								CREATE TABLE docs (
									id STRING PRIMARY KEY,
									grp STRING NOT NULL,
									n INTEGER NOT NULL
								)`))
							pgCapabilityMust(t, c.query(`
								INSERT INTO docs VALUES
								('{"id":"a","grp":"old","n":1}'),
								('{"id":"b","grp":"old","n":2}'),
								('{"id":"c","grp":"multi-delete","n":3}'),
								('{"id":"d","grp":"multi-delete","n":4}')`))
							if capability.Indexing == conformance.Indexed {
								pgCapabilityMust(t, c.query(`CREATE INDEX by_grp ON docs(grp)`))
							}

							applyPGWireCapability(
								t, c, capability.Transaction, keys, operation,
							)
							assertPGWireIndexOracle(t, c)
							assertPGWireRollback(t, c, capability.Transaction)
						})
					}
				})
			}
		})
	}
}

func applyPGWireCapability(
	t *testing.T, c *testClient, transaction conformance.Transaction,
	keys conformance.Keys, operation conformance.Operation,
) {
	t.Helper()
	if transaction == conformance.Explicit {
		pgCapabilityMust(t, c.query(`BEGIN`))
	}
	var statements string
	if keys == conformance.OneKey {
		switch operation {
		case conformance.Insert:
			statements = `INSERT INTO docs VALUES ('{"id":"wire-one","grp":"inserted","n":10}')`
		case conformance.Update:
			statements = `UPDATE docs SET "$doc" = '{"id":"a","grp":"updated","n":11}' WHERE id = 'a'`
		case conformance.Delete:
			statements = `DELETE FROM docs WHERE id = 'c'`
		case conformance.Mixed:
			statements = `
				INSERT INTO docs VALUES ('{"id":"wire-one","grp":"inserted","n":12}');
				UPDATE docs SET "$doc" = '{"id":"wire-one","grp":"updated","n":13}' WHERE id = 'wire-one';
				DELETE FROM docs WHERE id = 'wire-one';
				INSERT INTO docs VALUES ('{"id":"wire-one","grp":"mixed-final","n":14}')`
		default:
			t.Fatalf("unknown one-key pgwire operation %q", operation)
		}
	} else {
		switch operation {
		case conformance.Insert:
			statements = `
				INSERT INTO docs VALUES
					('{"id":"wire-a","grp":"inserted","n":20}'),
					('{"id":"wire-b","grp":"inserted","n":21}')`
		case conformance.Update:
			statements = `
				UPDATE docs SET "$doc" = '{"id":"a","grp":"updated","n":22}' WHERE id = 'a';
				UPDATE docs SET "$doc" = '{"id":"b","grp":"updated","n":23}' WHERE id = 'b'`
		case conformance.Delete:
			statements = `DELETE FROM docs WHERE grp = 'multi-delete'`
		case conformance.Mixed:
			statements = `
				INSERT INTO docs VALUES ('{"id":"wire-mixed","grp":"inserted","n":24}');
				UPDATE docs SET "$doc" = '{"id":"a","grp":"mixed-update","n":25}' WHERE id = 'a';
				DELETE FROM docs WHERE id = 'c'`
		default:
			t.Fatalf("unknown multi-key pgwire operation %q", operation)
		}
	}
	pgCapabilityMust(t, c.query(statements))
	if transaction == conformance.Explicit {
		pgCapabilityMust(t, c.query(`COMMIT`))
	}
}

func assertPGWireRollback(
	t *testing.T, c *testClient, transaction conformance.Transaction,
) {
	t.Helper()
	want := pgWireContent(t, c)
	if transaction == conformance.Explicit {
		assertReadyStatus(t, c.query(`BEGIN`), statusInTx)
		staged := c.query(
			`INSERT INTO docs VALUES ('{"id":"rolled","grp":"rollback","n":90}')`,
		)
		pgCapabilityMust(t, staged)
		assertReadyStatus(t, staged, statusInTx)
		// The first sibling row is valid and the second duplicates the BEGIN
		// snapshot. PostgreSQL semantics fail the transaction; neither sibling
		// may enter the overlay and the earlier valid statement must roll back.
		rejected := c.query(`
			INSERT INTO docs VALUES
			('{"id":"sibling-good","grp":"rollback","n":91}'),
			('{"id":"a","grp":"duplicate","n":92}')`)
		expectError(t, rejected, sqlstateUniqueViolation)
		assertReadyStatus(t, rejected, statusFailedT)
		failedRead := c.query(`SELECT id FROM docs WHERE id = 'rolled'`)
		expectError(t, failedRead, sqlstateFailedTransaction)
		assertReadyStatus(t, failedRead, statusFailedT)
		commit := c.query(`COMMIT`)
		if tag := commandTagOf(t, commit); tag != "ROLLBACK" {
			t.Fatalf("COMMIT after sibling rejection tag = %q, want ROLLBACK", tag)
		}
		assertReadyStatus(t, commit, statusIdle)
	} else {
		msgs := c.query(`
			INSERT INTO docs VALUES ('{"id":"rollback-good","grp":"rollback","n":90}');
			INSERT INTO docs VALUES ('{"id":"a","grp":"duplicate","n":91}')`)
		expectError(t, msgs, sqlstateUniqueViolation)
	}
	if got := pgWireContent(t, c); !pgCapabilityMapsEqual(got, want) {
		t.Fatalf("rollback content = %v, want %v", got, want)
	}
	assertPGWireIndexOracle(t, c)
	// A rejected implicit batch or failed explicit transaction must not poison
	// the next idle transaction.
	pgCapabilityMust(t, c.query(
		`INSERT INTO docs VALUES ('{"id":"after-rejection","grp":"usable","n":93}')`,
	))
	pgCapabilityMust(t, c.query(`DELETE FROM docs WHERE id = 'after-rejection'`))
	if got := pgWireContent(t, c); !pgCapabilityMapsEqual(got, want) {
		t.Fatalf("post-rejection usability content = %v, want %v", got, want)
	}
	assertPGWireIndexOracle(t, c)
}

func assertPGWireIndexOracle(t *testing.T, c *testClient) {
	t.Helper()
	content := pgWireContent(t, c)
	want := map[string][]string{"absent-sentinel": nil}
	for key, group := range content {
		want[group] = append(want[group], key)
	}
	for group, keys := range want {
		slices.Sort(keys)
		msgs := c.query(fmt.Sprintf(
			`SELECT id FROM docs WHERE grp = '%s' ORDER BY id`, group,
		))
		pgCapabilityMust(t, msgs)
		rows := rowsOf(t, msgs)
		got := make([]string, 0, len(rows))
		for _, row := range rows {
			var key string
			if err := json.Unmarshal(row[0], &key); err != nil {
				t.Fatal(err)
			}
			got = append(got, key)
		}
		if !slices.Equal(got, keys) {
			t.Fatalf("group=%q keys=%v want=%v", group, got, keys)
		}
	}
}

func pgWireContent(t *testing.T, c *testClient) map[string]string {
	t.Helper()
	msgs := c.query(`SELECT id, grp FROM docs ORDER BY id`)
	pgCapabilityMust(t, msgs)
	out := map[string]string{}
	for _, row := range rowsOf(t, msgs) {
		var key, group string
		if err := json.Unmarshal(row[0], &key); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(row[1], &group); err != nil {
			t.Fatal(err)
		}
		out[key] = group
	}
	return out
}

func pgCapabilityMust(t *testing.T, messages []backendMessage) {
	t.Helper()
	if has(messages, msgErrorResponse) {
		t.Fatalf("pgwire capability command failed: %s",
			formatError(find(t, messages, msgErrorResponse).body))
	}
}

func pgCapabilityMapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}
