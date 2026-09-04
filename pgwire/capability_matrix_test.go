package pgwire

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/conformance"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// TestPGWireCapabilityMatrix drives every pgwire manifest row through protocol
// messages, including implicit simple-query batches, explicit BEGIN/COMMIT,
// savepoints, and multi-table serialization failure. Every table-count,
// key-count, and operation is an independent protocol/catalog subtest. It
// never calls the SQL driver directly.
func TestPGWireCapabilityMatrix(t *testing.T) {
	for _, capability := range conformance.CasesFor(conformance.PGWire) {
		capability := capability
		t.Run(capability.ID, func(t *testing.T) {
			for _, tables := range capability.Tables {
				tables := tables
				t.Run(string(tables), func(t *testing.T) {
					for _, keys := range capability.Keys {
						keys := keys
						t.Run(string(keys), func(t *testing.T) {
							for _, operation := range capability.Operations {
								operation := operation
								t.Run(string(operation), func(t *testing.T) {
									multi := tables == conformance.MultipleTables
									if capability.Result == conformance.DocumentedError {
										runPGWireDocumentedError(t, capability, multi)
										return
									}
									if capability.Transaction == conformance.Savepoints {
										runPGWireSavepoint(t, multi)
										return
									}
									c := connectSeededPGWireCatalog(
										t, multi, capability.Indexing == conformance.Indexed,
									)
									applyPGWireCapability(
										t, c, capability.Transaction, keys, operation, multi,
									)
									for _, table := range pgCapabilityTables(multi) {
										assertPGWireIndexOracle(t, c, table)
									}
									assertPGWireRollback(t, c, capability.Transaction, multi)
								})
							}
						})
					}
				})
			}
		})
	}
}

func pgCapabilityTables(multi bool) []string {
	if multi {
		return []string{"docs", "extras"}
	}
	return []string{"docs"}
}

// connectSeededPGWireCatalog materializes the fixture, then reopens the catalog
// so each target journal is folded past the seed window and reminted at
// the conditional format word before multi-table commits prepare.
func connectSeededPGWireCatalog(t *testing.T, multi, indexed bool) *testClient {
	t.Helper()
	c, _ := connectSeededPGWireCatalogWithServer(t, multi, indexed, false)
	return c
}

func connectSeededPGWireCatalogWithServer(
	t *testing.T, multi, indexed, singleRowSeed bool,
) (*testClient, *Server) {
	t.Helper()
	catalogPath := filepath.Join(t.TempDir(), "catalog.vdb")
	seedDB, err := sqldriver.Open(catalogPath)
	if err != nil {
		t.Fatalf("open SQL catalog: %v", err)
	}
	seedServer, err := NewServer(seedDB, Options{Auth: Trust()})
	if err != nil {
		_ = seedDB.Close()
		t.Fatalf("NewServer: %v", err)
	}
	seedClient := dial(t, seedServer)
	seedClient.startup(map[string]string{"user": "tester", "database": "app"})
	for _, table := range pgCapabilityTables(multi) {
		pgCapabilityMust(t, seedClient.query(fmt.Sprintf(`
			CREATE TABLE %s (
				id STRING PRIMARY KEY,
				grp STRING NOT NULL,
				n INTEGER NOT NULL
			)`, table)))
		if singleRowSeed {
			pgCapabilityMust(t, seedClient.query(fmt.Sprintf(
				`INSERT INTO %s VALUES ('{"id":"a","grp":"old","n":1}')`, table,
			)))
		} else {
			pgCapabilityMust(t, seedClient.query(fmt.Sprintf(`
				INSERT INTO %s VALUES
				('{"id":"a","grp":"old","n":1}'),
				('{"id":"b","grp":"old","n":2}'),
				('{"id":"c","grp":"multi-delete","n":3}'),
				('{"id":"d","grp":"multi-delete","n":4}')`, table)))
		}
		if indexed {
			pgCapabilityMust(t, seedClient.query(fmt.Sprintf(
				`CREATE INDEX by_grp_%s ON %s(grp)`, table, table,
			)))
		}
	}
	if err := seedServer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := seedDB.Close(); err != nil {
		t.Fatal(err)
	}

	database, err := sqldriver.Open(catalogPath)
	if err != nil {
		t.Fatalf("reopen SQL catalog: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close SQL catalog: %v", err)
		}
	})
	server, err := NewServer(database, Options{Auth: Trust()})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close server: %v", err)
		}
	})
	c := dial(t, server)
	c.startup(map[string]string{"user": "tester", "database": "app"})
	return c, server
}

func runPGWireDocumentedError(t *testing.T, capability conformance.Case, multi bool) {
	t.Helper()
	if capability.Error != "SQLSTATE 40001" {
		t.Fatalf("unsupported documented-error %q", capability.Error)
	}
	c, srv := connectSeededPGWireCatalogWithServer(t, multi, false, true)
	want := pgWireAllContent(t, c, multi)

	blocker := dial(t, srv)
	blocker.startup(map[string]string{"user": "tester", "database": "app"})

	assertReadyStatus(t, requireQueryReady(t, c, `BEGIN`), statusInTx)
	for _, table := range pgCapabilityTables(multi) {
		pgCapabilityMust(t, c.query(fmt.Sprintf(
			`UPDATE %s SET "$doc" = '{"id":"a","grp":"loser","n":2}' WHERE id = 'a'`, table,
		)))
	}

	assertReadyStatus(t, requireQueryReady(t, blocker, `BEGIN`), statusInTx)
	pgCapabilityMust(t, blocker.query(
		`UPDATE docs SET "$doc" = '{"id":"a","grp":"winner","n":3}' WHERE id = 'a'`))
	assertReadyStatus(t, requireQueryReady(t, blocker, `COMMIT`), statusIdle)

	conflict := c.query(`COMMIT`)
	expectError(t, conflict, sqlstateSerializationFailure)
	assertReadyStatus(t, conflict, statusIdle)
	// Winner published only on docs; every other participant stays at the
	// pre-conflict cut — the loser's multi-table write set published nothing.
	want["docs"]["a"] = "winner"
	if got := pgWireAllContent(t, c, multi); !pgCapabilityAllEqual(got, want) {
		t.Fatalf("serialization failure content = %v, want %v", got, want)
	}
	for _, table := range pgCapabilityTables(multi) {
		assertPGWireIndexOracle(t, c, table)
	}
}

func runPGWireSavepoint(t *testing.T, multi bool) {
	t.Helper()
	c := connectSeededPGWireCatalog(t, multi, false)
	// Savepoint rows only need the seeded key a; trim extras for the one-row shape.
	assertReadyStatus(t, requireQueryReady(t, c, `BEGIN`), statusInTx)
	assertReadyStatus(t, requireQueryReady(t, c, `SAVEPOINT s1`), statusInTx)
	for _, table := range pgCapabilityTables(multi) {
		pgCapabilityMust(t, c.query(fmt.Sprintf(
			`UPDATE %s SET "$doc" = '{"id":"a","grp":"s1","n":2}' WHERE id = 'a'`, table,
		)))
	}
	assertReadyStatus(t, requireQueryReady(t, c, `SAVEPOINT s2`), statusInTx)
	for _, table := range pgCapabilityTables(multi) {
		pgCapabilityMust(t, c.query(fmt.Sprintf(`DELETE FROM %s WHERE id = 'a'`, table)))
		pgCapabilityMust(t, c.query(fmt.Sprintf(
			`INSERT INTO %s VALUES ('{"id":"a","grp":"s2","n":3}')`, table,
		)))
	}
	assertReadyStatus(t, requireQueryReady(t, c, `ROLLBACK TO s1`), statusInTx)
	for _, table := range pgCapabilityTables(multi) {
		if got := pgWireContent(t, c, table); got["a"] != "old" {
			t.Fatalf("%s after ROLLBACK TO s1 = %v, want old", table, got)
		}
	}
	assertReadyStatus(t, requireQueryReady(t, c, `RELEASE s1`), statusInTx)
	assertReadyStatus(t, requireQueryReady(t, c, `COMMIT`), statusIdle)
	for _, table := range pgCapabilityTables(multi) {
		if got := pgWireContent(t, c, table); got["a"] != "old" {
			t.Fatalf("%s after savepoint commit = %v, want old", table, got)
		}
		assertPGWireIndexOracle(t, c, table)
	}
}

func applyPGWireCapability(
	t *testing.T, c *testClient, transaction conformance.Transaction,
	keys conformance.Keys, operation conformance.Operation, multi bool,
) {
	t.Helper()
	if transaction == conformance.Explicit {
		pgCapabilityMust(t, c.query(`BEGIN`))
	}
	for _, table := range pgCapabilityTables(multi) {
		pgCapabilityMust(t, c.query(pgCapabilityStatements(table, keys, operation)))
	}
	if transaction == conformance.Explicit {
		pgCapabilityMust(t, c.query(`COMMIT`))
	}
}

func pgCapabilityStatements(
	table string, keys conformance.Keys, operation conformance.Operation,
) string {
	if keys == conformance.OneKey {
		switch operation {
		case conformance.Insert:
			return fmt.Sprintf(
				`INSERT INTO %s VALUES ('{"id":"wire-one","grp":"inserted","n":10}')`, table)
		case conformance.Update:
			return fmt.Sprintf(
				`UPDATE %s SET "$doc" = '{"id":"a","grp":"updated","n":11}' WHERE id = 'a'`, table)
		case conformance.Delete:
			return fmt.Sprintf(`DELETE FROM %s WHERE id = 'c'`, table)
		case conformance.Mixed:
			return fmt.Sprintf(`
				INSERT INTO %s VALUES ('{"id":"wire-one","grp":"inserted","n":12}');
				UPDATE %s SET "$doc" = '{"id":"wire-one","grp":"updated","n":13}' WHERE id = 'wire-one';
				DELETE FROM %s WHERE id = 'wire-one';
				INSERT INTO %s VALUES ('{"id":"wire-one","grp":"mixed-final","n":14}')`,
				table, table, table, table)
		}
	} else {
		switch operation {
		case conformance.Insert:
			return fmt.Sprintf(`
				INSERT INTO %s VALUES
					('{"id":"wire-a","grp":"inserted","n":20}'),
					('{"id":"wire-b","grp":"inserted","n":21}')`, table)
		case conformance.Update:
			return fmt.Sprintf(`
				UPDATE %s SET "$doc" = '{"id":"a","grp":"updated","n":22}' WHERE id = 'a';
				UPDATE %s SET "$doc" = '{"id":"b","grp":"updated","n":23}' WHERE id = 'b'`,
				table, table)
		case conformance.Delete:
			return fmt.Sprintf(`DELETE FROM %s WHERE grp = 'multi-delete'`, table)
		case conformance.Mixed:
			return fmt.Sprintf(`
				INSERT INTO %s VALUES ('{"id":"wire-mixed","grp":"inserted","n":24}');
				UPDATE %s SET "$doc" = '{"id":"a","grp":"mixed-update","n":25}' WHERE id = 'a';
				DELETE FROM %s WHERE id = 'c'`, table, table, table)
		}
	}
	return ""
}

func assertPGWireRollback(
	t *testing.T, c *testClient, transaction conformance.Transaction, multi bool,
) {
	t.Helper()
	want := pgWireAllContent(t, c, multi)
	if transaction == conformance.Explicit {
		assertReadyStatus(t, c.query(`BEGIN`), statusInTx)
		for _, table := range pgCapabilityTables(multi) {
			pgCapabilityMust(t, c.query(fmt.Sprintf(
				`INSERT INTO %s VALUES ('{"id":"rolled","grp":"rollback","n":90}')`, table,
			)))
		}
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
	if got := pgWireAllContent(t, c, multi); !pgCapabilityAllEqual(got, want) {
		t.Fatalf("rollback content = %v, want %v", got, want)
	}
	for _, table := range pgCapabilityTables(multi) {
		assertPGWireIndexOracle(t, c, table)
	}
	// A rejected implicit batch or failed explicit transaction must not poison
	// the next idle transaction.
	pgCapabilityMust(t, c.query(
		`INSERT INTO docs VALUES ('{"id":"after-rejection","grp":"usable","n":93}')`,
	))
	pgCapabilityMust(t, c.query(`DELETE FROM docs WHERE id = 'after-rejection'`))
	if got := pgWireAllContent(t, c, multi); !pgCapabilityAllEqual(got, want) {
		t.Fatalf("post-rejection usability content = %v, want %v", got, want)
	}
	for _, table := range pgCapabilityTables(multi) {
		assertPGWireIndexOracle(t, c, table)
	}
}

func assertPGWireIndexOracle(t *testing.T, c *testClient, table string) {
	t.Helper()
	content := pgWireContent(t, c, table)
	want := map[string][]string{"absent-sentinel": nil}
	for key, group := range content {
		want[group] = append(want[group], key)
	}
	for group, keys := range want {
		slices.Sort(keys)
		msgs := c.query(fmt.Sprintf(
			`SELECT id FROM %s WHERE grp = '%s' ORDER BY id`, table, group,
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
			t.Fatalf("%s group=%q keys=%v want=%v", table, group, got, keys)
		}
	}
}

func pgWireAllContent(t *testing.T, c *testClient, multi bool) map[string]map[string]string {
	t.Helper()
	out := make(map[string]map[string]string)
	for _, table := range pgCapabilityTables(multi) {
		out[table] = pgWireContent(t, c, table)
	}
	return out
}

func pgWireContent(t *testing.T, c *testClient, table string) map[string]string {
	t.Helper()
	msgs := c.query(fmt.Sprintf(`SELECT id, grp FROM %s ORDER BY id`, table))
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

func pgCapabilityAllEqual(a, b map[string]map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for table, content := range a {
		if !pgCapabilityMapsEqual(content, b[table]) {
			return false
		}
	}
	return true
}
