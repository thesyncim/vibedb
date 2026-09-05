package driver

import (
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/conformance"
)

func TestSQLWorkloadCompatibilityTrackedGapsRemainExplicit(t *testing.T) {
	db := openTestDB(t)
	for _, source := range []string{
		`CREATE TABLE compatibility_docs (id TEXT PRIMARY KEY, n INT, hidden BOOL, custom OBJECT, tags ARRAY)`,
		`CREATE TABLE compatibility_changes (id TEXT PRIMARY KEY, n INT)`,
		`INSERT INTO compatibility_docs VALUES ('{"id":"a","n":1,"hidden":false,"custom":{},"tags":["x"]}')`,
	} {
		if _, err := db.Exec(source); err != nil {
			t.Fatal(err)
		}
	}
	for _, gap := range conformance.SQLWorkloadGaps {
		t.Run(gap.ID, func(t *testing.T) {
			stmt, err := db.Prepare(gap.SQL)
			if stmt != nil {
				defer stmt.Close()
			}
			if err == nil {
				if strings.HasPrefix(gap.SQL, "SELECT") || strings.HasPrefix(gap.SQL, "WITH") {
					rows, queryErr := stmt.Query()
					err = queryErr
					if rows != nil {
						for rows.Next() {
						}
						if err == nil {
							err = rows.Err()
						}
						rows.Close()
					}
				} else {
					_, err = stmt.Exec()
				}
			}
			if err == nil {
				t.Fatalf("gap now executes: add result/atomicity coverage and close %s in the SQL workload tracker", gap.ID)
			}
		})
	}
}
