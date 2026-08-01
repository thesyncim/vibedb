package query

import "testing"

// The recursive dependency leak that introduced activeFrame exposed a general
// shared-CTE invariant: publication bytes and child relations must be released
// through the frame that admitted them, and the retained identity must die with
// the publication. Keep an ordinary chain here so the cold ownership sidecar
// cannot regress independently of WITH RECURSIVE.
func TestRecursiveSQLActiveFrameOrdinarySharedCTELifecycle(t *testing.T) {
	catalog := subqueryDatabase(t)
	statement, err := PrepareStatement(
		`WITH a AS MATERIALIZED (SELECT id FROM customers), ` +
			`b AS MATERIALIZED (SELECT id FROM a) ` +
			`SELECT id FROM b ORDER BY id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			statement.Release()
		}
	}()
	definitions := append([]*statementCTE(nil), statement.cteCatalog().defs...)
	if len(definitions) != 2 {
		t.Fatalf("ordinary shared CTE definitions = %d, want 2", len(definitions))
	}

	var execution Exec
	execution.Options.IntermediateBytes = -1
	source := FromDatabase(catalog, statement.Collection())
	for run := 0; run < 2; run++ {
		cursor, runErr := statement.RunInto(&execution, source, nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
		rows := 0
		for cursor.Next() {
			rows++
		}
		if rows == 0 {
			t.Fatal("ordinary shared CTE returned no rows")
		}
		for i, definition := range definitions {
			if definition.state != cteReady || definition.activeBytes <= 0 ||
				definition.activeFrame != &statement.nested.frame {
				t.Fatalf("live ordinary shared CTE %d frame/bytes/state = %p/%d/%d, want owner frame %p",
					i, definition.activeFrame, definition.activeBytes,
					definition.state, &statement.nested.frame)
			}
		}
		statement.discardRelations()
		if statement.nested.frame.intermediate.used != 0 {
			t.Fatalf("ordinary shared CTE run %d retained %d intermediate bytes",
				run, statement.nested.frame.intermediate.used)
		}
		for i, definition := range definitions {
			if definition.activeFrame != nil || definition.activeBytes != 0 ||
				definition.state != cteIdle || definition.spool.rows != 0 {
				t.Fatalf("discarded ordinary shared CTE %d retained frame/bytes/state/rows = %p/%d/%d/%d",
					i, definition.activeFrame, definition.activeBytes,
					definition.state, definition.spool.rows)
			}
		}
	}

	if _, err = statement.RunInto(&execution, source, nil); err != nil {
		t.Fatal(err)
	}
	statement.Release()
	released = true
	for i, definition := range definitions {
		if definition.activeFrame != nil || definition.activeBytes != 0 ||
			definition.state != cteIdle {
			t.Fatalf("released ordinary shared CTE %d retained frame/bytes/state = %p/%d/%d",
				i, definition.activeFrame, definition.activeBytes, definition.state)
		}
	}
	execution.Release()
}
