package driver

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/conformance"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// TestDatabaseSQLCapabilityMatrix executes every database/sql row in the
// shared public manifest against a pre-materialized table. That detail keeps
// initial temporary-file publication from masquerading as steady-state batch
// support. Tables, keys, and operations are real nested subtests, each with a
// fresh catalog and persistence lifecycle.
func TestDatabaseSQLCapabilityMatrix(t *testing.T) {
	for _, capability := range conformance.CasesFor(conformance.DatabaseSQL) {
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
									scenario := capability
									scenario.Tables = []conformance.Tables{tables}
									scenario.Keys = []conformance.Keys{keys}
									scenario.Operations = []conformance.Operation{operation}
									runDatabaseSQLCapability(t, scenario)
								})
							}
						})
					}
				})
			}
		})
	}
}

func runDatabaseSQLCapability(t *testing.T, capability conformance.Case) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "capability.vdb")
	db, err := sql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	multi := len(capability.Tables) == 1 && capability.Tables[0] == conformance.MultipleTables
	for _, table := range sqlCapabilityTables(multi) {
		if _, err := db.Exec(fmt.Sprintf(`
				CREATE TABLE %s (
					id STRING PRIMARY KEY,
					grp STRING NOT NULL,
					n INTEGER NOT NULL
				)`, table)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			fmt.Sprintf(`INSERT INTO %s VALUES (?), (?), (?), (?)`, table),
			sqlCapabilityDoc("a", "old", 1),
			sqlCapabilityDoc("b", "old", 2),
			sqlCapabilityDoc("c", "multi-delete", 3),
			sqlCapabilityDoc("d", "multi-delete", 4),
		); err != nil {
			t.Fatal(err)
		}
		if capability.Indexing == conformance.Indexed {
			if _, err := db.Exec(
				fmt.Sprintf(`CREATE INDEX by_grp_%s ON %s(grp)`, table, table),
			); err != nil {
				t.Fatal(err)
			}
		}
	}

	if capability.Result == conformance.DocumentedError {
		assertDatabaseSQLDocumentedError(t, db, capability)
	} else if capability.Transaction == conformance.Savepoints {
		applyDatabaseSQLSavepoint(t, db, multi)
	} else {
		applyDatabaseSQLCapability(t, db, capability, multi)
		assertDatabaseSQLRollback(t, db, capability.Transaction, multi)
	}
	for _, table := range sqlCapabilityTables(multi) {
		assertDatabaseSQLIndexOracle(t, db, table)
	}
	want := databaseSQLAllContent(t, db, multi)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = sql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := databaseSQLAllContent(t, db, multi); !sqlCapabilityAllEqual(got, want) {
		t.Fatalf("reopened content = %v, want %v", got, want)
	}
	for _, table := range sqlCapabilityTables(multi) {
		assertDatabaseSQLIndexOracle(t, db, table)
	}
}

func sqlCapabilityTables(multi bool) []string {
	if multi {
		return []string{"docs", "extras"}
	}
	return []string{"docs"}
}

func assertDatabaseSQLDocumentedError(
	t *testing.T, db *sql.DB, capability conformance.Case,
) {
	t.Helper()
	if len(capability.Operations) != 1 {
		t.Fatalf("documented-error case has operations %v, want one", capability.Operations)
	}
	want := databaseSQLContent(t, db, "docs")
	var err error
	switch capability.Operations[0] {
	case conformance.Update:
		_, err = db.Exec(
			`UPDATE docs SET "$doc" = ? WHERE grp = ?`,
			sqlCapabilityDoc("a", "replacement", 70), "old",
		)
		if !errors.Is(err, ErrUpdatePrimaryKey) {
			t.Fatalf("multi-key UPDATE error = %v, want ErrUpdatePrimaryKey", err)
		}
	case conformance.Mixed:
		_, err = db.Exec(`
			INSERT INTO docs VALUES ('{"id":"mixed-error","grp":"mixed","n":71}');
			DELETE FROM docs WHERE id = 'mixed-error'`)
		var parseErr *sqlast.ParseError
		if !errors.As(err, &parseErr) ||
			parseErr.Msg != "only one statement may be parsed at a time" {
			t.Fatalf("mixed Exec error = %v, want one-statement *sql.ParseError", err)
		}
	default:
		t.Fatalf("unsupported documented-error operation %q", capability.Operations[0])
	}
	if got := databaseSQLContent(t, db, "docs"); !sqlCapabilityMapsEqual(got, want) {
		t.Fatalf("documented error changed content = %v, want %v", got, want)
	}
	// A capability refusal must not poison the connection or collection.
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		sqlCapabilityDoc("after-error", "usable", 72)); err != nil {
		t.Fatalf("write after documented error: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM docs WHERE id = ?`, "after-error"); err != nil {
		t.Fatalf("cleanup after documented error: %v", err)
	}
	if got := databaseSQLContent(t, db, "docs"); !sqlCapabilityMapsEqual(got, want) {
		t.Fatalf("post-error usability probe changed content = %v, want %v", got, want)
	}
}

func applyDatabaseSQLCapability(
	t *testing.T, db *sql.DB, capability conformance.Case, multi bool,
) {
	t.Helper()
	if len(capability.Operations) != 1 || len(capability.Keys) != 1 {
		t.Fatalf("capability scenario is not one operation/key: %+v", capability)
	}
	operation := capability.Operations[0]
	keys := capability.Keys[0]
	if capability.Transaction == conformance.Autocommit {
		applyDatabaseSQLAutocommit(t, db, "docs", keys, operation)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	applyDatabaseSQLTx(t, tx, "docs", keys, operation)
	if multi {
		applyDatabaseSQLTx(t, tx, "extras", keys, operation)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func applyDatabaseSQLAutocommit(
	t *testing.T, db *sql.DB, table string,
	keys conformance.Keys, operation conformance.Operation,
) {
	t.Helper()
	if keys == conformance.OneKey {
		switch operation {
		case conformance.Insert:
			if _, err := db.Exec(
				fmt.Sprintf(`INSERT INTO %s VALUES (?)`, table),
				sqlCapabilityDoc("auto-one", "inserted", 10),
			); err != nil {
				t.Fatal(err)
			}
		case conformance.Update:
			if _, err := db.Exec(
				fmt.Sprintf(`UPDATE %s SET "$doc" = ? WHERE id = ?`, table),
				sqlCapabilityDoc("a", "updated", 11), "a",
			); err != nil {
				t.Fatal(err)
			}
		case conformance.Delete:
			if _, err := db.Exec(
				fmt.Sprintf(`DELETE FROM %s WHERE id = ?`, table), "c",
			); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unsupported autocommit one-key operation %q", operation)
		}
		return
	}
	switch operation {
	case conformance.Insert:
		if _, err := db.Exec(
			fmt.Sprintf(`INSERT INTO %s VALUES (?), (?)`, table),
			sqlCapabilityDoc("auto-a", "inserted", 20),
			sqlCapabilityDoc("auto-b", "inserted", 21),
		); err != nil {
			t.Fatal(err)
		}
	case conformance.Delete:
		if _, err := db.Exec(
			fmt.Sprintf(`DELETE FROM %s WHERE grp = ?`, table), "multi-delete",
		); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported autocommit multi-key operation %q", operation)
	}
}

func applyDatabaseSQLTx(
	t *testing.T, tx *sql.Tx, table string,
	keys conformance.Keys, operation conformance.Operation,
) {
	t.Helper()
	if keys == conformance.OneKey {
		switch operation {
		case conformance.Insert:
			if _, err := tx.Exec(
				fmt.Sprintf(`INSERT INTO %s VALUES (?)`, table),
				sqlCapabilityDoc("tx-one", "inserted", 30),
			); err != nil {
				t.Fatal(err)
			}
		case conformance.Update:
			if _, err := tx.Exec(
				fmt.Sprintf(`UPDATE %s SET "$doc" = ? WHERE id = ?`, table),
				sqlCapabilityDoc("a", "updated", 31), "a",
			); err != nil {
				t.Fatal(err)
			}
		case conformance.Delete:
			if _, err := tx.Exec(
				fmt.Sprintf(`DELETE FROM %s WHERE id = ?`, table), "c",
			); err != nil {
				t.Fatal(err)
			}
		case conformance.Mixed:
			if _, err := tx.Exec(
				fmt.Sprintf(`INSERT INTO %s VALUES (?)`, table),
				sqlCapabilityDoc("tx-one", "before", 32),
			); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(
				fmt.Sprintf(`UPDATE %s SET "$doc" = ? WHERE id = ?`, table),
				sqlCapabilityDoc("tx-one", "updated", 33), "tx-one",
			); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(
				fmt.Sprintf(`DELETE FROM %s WHERE id = ?`, table), "tx-one",
			); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(
				fmt.Sprintf(`INSERT INTO %s VALUES (?)`, table),
				sqlCapabilityDoc("tx-one", "mixed-final", 34),
			); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unsupported transaction one-key operation %q", operation)
		}
		return
	}
	switch operation {
	case conformance.Insert:
		if _, err := tx.Exec(
			fmt.Sprintf(`INSERT INTO %s VALUES (?), (?)`, table),
			sqlCapabilityDoc("tx-a", "inserted", 40),
			sqlCapabilityDoc("tx-b", "inserted", 41),
		); err != nil {
			t.Fatal(err)
		}
	case conformance.Update:
		for _, key := range []string{"a", "b"} {
			if _, err := tx.Exec(
				fmt.Sprintf(`UPDATE %s SET "$doc" = ? WHERE id = ?`, table),
				sqlCapabilityDoc(key, "updated", 42), key,
			); err != nil {
				t.Fatal(err)
			}
		}
	case conformance.Delete:
		if _, err := tx.Exec(
			fmt.Sprintf(`DELETE FROM %s WHERE grp = ?`, table), "multi-delete",
		); err != nil {
			t.Fatal(err)
		}
	case conformance.Mixed:
		if _, err := tx.Exec(
			fmt.Sprintf(`INSERT INTO %s VALUES (?)`, table),
			sqlCapabilityDoc("mixed", "inserted", 43),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(
			fmt.Sprintf(`UPDATE %s SET "$doc" = ? WHERE id = ?`, table),
			sqlCapabilityDoc("a", "mixed-update", 44), "a",
		); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(
			fmt.Sprintf(`DELETE FROM %s WHERE id = ?`, table), "c",
		); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported transaction multi-key operation %q", operation)
	}
}

func applyDatabaseSQLSavepoint(t *testing.T, db *sql.DB, multi bool) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`SAVEPOINT s1`); err != nil {
		t.Fatal(err)
	}
	for _, table := range sqlCapabilityTables(multi) {
		if _, err := tx.Exec(
			fmt.Sprintf(`UPDATE %s SET "$doc" = ? WHERE id = ?`, table),
			sqlCapabilityDoc("a", "s1", 50), "a",
		); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(`SAVEPOINT s2`); err != nil {
		t.Fatal(err)
	}
	for _, table := range sqlCapabilityTables(multi) {
		if _, err := tx.Exec(
			fmt.Sprintf(`DELETE FROM %s WHERE id = ?`, table), "a",
		); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(
			fmt.Sprintf(`INSERT INTO %s VALUES (?)`, table),
			sqlCapabilityDoc("a", "s2", 51),
		); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(`ROLLBACK TO s1`); err != nil {
		t.Fatal(err)
	}
	for _, table := range sqlCapabilityTables(multi) {
		var got string
		if err := tx.QueryRow(
			fmt.Sprintf(`SELECT grp FROM %s WHERE id = ?`, table), "a",
		).Scan(&got); err != nil || got != "old" {
			t.Fatalf("%s after ROLLBACK TO s1 grp = %q err=%v, want old", table, got, err)
		}
	}
	if _, err := tx.Exec(`RELEASE s1`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, table := range sqlCapabilityTables(multi) {
		var got string
		if err := db.QueryRow(
			fmt.Sprintf(`SELECT grp FROM %s WHERE id = ?`, table), "a",
		).Scan(&got); err != nil || got != "old" {
			t.Fatalf("%s after savepoint commit grp = %q err=%v, want old", table, got, err)
		}
	}
}

func assertDatabaseSQLRollback(
	t *testing.T, db *sql.DB, transaction conformance.Transaction, multi bool,
) {
	t.Helper()
	want := databaseSQLAllContent(t, db, multi)
	if transaction == conformance.Explicit {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		for _, table := range sqlCapabilityTables(multi) {
			if _, err := tx.Exec(
				fmt.Sprintf(`INSERT INTO %s VALUES (?)`, table),
				sqlCapabilityDoc("rolled", "rollback", 90),
			); err != nil {
				t.Fatal(err)
			}
		}
		// The first row of this sibling statement is valid, but the second
		// duplicates the BEGIN snapshot. The statement must reject without
		// adding sibling-good to the transaction overlay; the previously staged
		// rolled row remains visible only inside the transaction until rollback.
		if _, err := tx.Exec(
			`INSERT INTO docs VALUES (?), (?)`,
			sqlCapabilityDoc("sibling-good", "rollback", 91),
			sqlCapabilityDoc("a", "duplicate", 92),
		); !errors.Is(err, ErrDuplicatePrimaryKey) {
			t.Fatalf("duplicate sibling error = %v, want ErrDuplicatePrimaryKey", err)
		}
		var stagedGroup string
		if err := tx.QueryRow(`SELECT grp FROM docs WHERE id = ?`, "rolled").Scan(
			&stagedGroup,
		); err != nil || stagedGroup != "rollback" {
			t.Fatalf("valid staged row after sibling rejection = (%q, %v)", stagedGroup, err)
		}
		var siblingCount int64
		if err := tx.QueryRow(
			`SELECT COUNT(*) FROM docs WHERE id = ?`, "sibling-good",
		).Scan(&siblingCount); err != nil || siblingCount != 0 {
			t.Fatalf("rejected sibling count = (%d, %v), want zero", siblingCount, err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if got := databaseSQLAllContent(t, db, multi); !sqlCapabilityAllEqual(got, want) {
			t.Fatalf("rejected-sibling rollback content = %v, want %v", got, want)
		}
		for _, table := range sqlCapabilityTables(multi) {
			assertDatabaseSQLIndexOracle(t, db, table)
		}

		// A real COMMIT rejection is the second rollback proof. A competing
		// autocommit changes the same key after BEGIN; first-committer-wins must
		// reject the staged transaction and leave exactly the competing cut on
		// every participant.
		conflicted, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		for _, table := range sqlCapabilityTables(multi) {
			if _, err := conflicted.Exec(
				fmt.Sprintf(`UPDATE %s SET "$doc" = ? WHERE id = ?`, table),
				sqlCapabilityDoc("a", "rejected-commit", 93), "a",
			); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := db.Exec(
			`UPDATE docs SET "$doc" = ? WHERE id = ?`,
			sqlCapabilityDoc("a", "conflict-winner", 94), "a",
		); err != nil {
			t.Fatal(err)
		}
		winner := databaseSQLAllContent(t, db, multi)
		if err := conflicted.Commit(); !errors.Is(err, ErrTransactionConflict) {
			t.Fatalf("conflicted COMMIT = %v, want ErrTransactionConflict", err)
		}
		if got := databaseSQLAllContent(t, db, multi); !sqlCapabilityAllEqual(got, winner) {
			t.Fatalf("conflicted COMMIT content = %v, want winner %v", got, winner)
		}
		for _, table := range sqlCapabilityTables(multi) {
			assertDatabaseSQLIndexOracle(t, db, table)
		}
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
			sqlCapabilityDoc("after-rejection", "usable", 95)); err != nil {
			t.Fatalf("write after rejected COMMIT: %v", err)
		}
		if _, err := db.Exec(`DELETE FROM docs WHERE id = ?`, "after-rejection"); err != nil {
			t.Fatalf("cleanup after rejected COMMIT: %v", err)
		}
		return
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?), (?)`,
		sqlCapabilityDoc("rollback-good", "rollback", 90),
		sqlCapabilityDoc("a", "duplicate", 91),
	); err == nil {
		t.Fatal("duplicate multi-row INSERT succeeded")
	}
	if got := databaseSQLAllContent(t, db, multi); !sqlCapabilityAllEqual(got, want) {
		t.Fatalf("rollback content = %v, want %v", got, want)
	}
}

func assertDatabaseSQLIndexOracle(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	content := databaseSQLContent(t, db, table)
	want := map[string][]string{"absent-sentinel": nil}
	for key, group := range content {
		want[group] = append(want[group], key)
	}
	for group, keys := range want {
		slices.Sort(keys)
		rows, err := db.Query(
			fmt.Sprintf(`SELECT id FROM %s WHERE grp = ? ORDER BY id`, table), group,
		)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			got = append(got, key)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, keys) {
			t.Fatalf("%s group=%q keys=%v want=%v", table, group, got, keys)
		}
	}
}

func databaseSQLAllContent(t *testing.T, db *sql.DB, multi bool) map[string]map[string]string {
	t.Helper()
	out := make(map[string]map[string]string)
	for _, table := range sqlCapabilityTables(multi) {
		out[table] = databaseSQLContent(t, db, table)
	}
	return out
}

func databaseSQLContent(t *testing.T, db *sql.DB, table string) map[string]string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf(`SELECT id, grp FROM %s ORDER BY id`, table))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var key, group string
		if err := rows.Scan(&key, &group); err != nil {
			t.Fatal(err)
		}
		out[key] = group
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func sqlCapabilityDoc(id, group string, n int) string {
	return fmt.Sprintf(`{"id":%q,"grp":%q,"n":%d}`, id, group, n)
}

func sqlCapabilityMapsEqual(a, b map[string]string) bool {
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

func sqlCapabilityAllEqual(a, b map[string]map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for table, content := range a {
		if !sqlCapabilityMapsEqual(content, b[table]) {
			return false
		}
	}
	return true
}
