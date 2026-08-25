package serviceauthz

import "testing"

func TestSQLCapabilityRequiresCompleteParsedSemantics(t *testing.T) {
	read := []string{
		"SELECT * FROM docs",
		"WITH c AS (SELECT * FROM docs) SELECT * FROM c",
		"WITH RECURSIVE reachable(node) AS MATERIALIZED (" +
			"SELECT node FROM seeds WHERE node = ? UNION " +
			"SELECT e.dst AS node FROM reachable r JOIN edges e ON r.node = e.src WHERE e.enabled = ?" +
			") SELECT node FROM reachable WHERE node >= ?",
		"EXPLAIN SELECT * FROM docs",
		"VALUES (1)",
		"TABLE docs",
		"(SELECT * FROM docs)",
		"SELECT ';' AS marker FROM docs",
		"SELECT 1 /* ; DELETE */; -- one trailing terminator",
		`SELECT 'it''s UPDATE; DELETE' AS text`,
	}
	for _, sql := range read {
		if got := SQLCapability(sql); got != CapabilityDataRead {
			t.Fatalf("read %q=%x", sql, got)
		}
	}

	write := []string{
		"INSERT INTO docs VALUES (?)",
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		"DELETE FROM docs WHERE n = 1",
		"SAVEPOINT before_write",
		"RELEASE SAVEPOINT before_write",
		"ROLLBACK TO SAVEPOINT before_write",
	}
	for _, sql := range write {
		if got := SQLCapability(sql); got != CapabilityDataWrite {
			t.Fatalf("write %q=%x", sql, got)
		}
	}

	schema := []string{
		"CREATE TABLE docs (id TEXT)",
		"CREATE INDEX docs_by_id ON docs (id)",
		"CREATE VIEW visible_docs AS SELECT * FROM docs",
		"DROP TABLE docs",
		"DROP INDEX docs_by_id",
		"DROP VIEW visible_docs",
		"TRUNCATE TABLE docs",
	}
	for _, sql := range schema {
		if got := SQLCapability(sql); got != CapabilitySchema {
			t.Fatalf("schema %q=%x", sql, got)
		}
	}

	all := CapabilityDataRead | CapabilityDataWrite | CapabilitySchema
	failClosed := []string{
		"", "/* unterminated", "@invalid",
		"SELECT 1 GARBAGE",
		"SELECT 1; DELETE FROM docs",
		"SELECT 1;;",
		`SELECT "unterminated`,
		"EXPLAIN UPDATE docs SET n = 1",
		"WITH changed AS (DELETE FROM docs RETURNING id) SELECT id FROM changed",
		`WITH changed AS (UPDATE docs SET "$doc" = ? RETURNING id) SELECT id FROM changed`,
		"WITH changed AS (INSERT INTO docs VALUES (?) RETURNING id) SELECT id FROM changed",
		"SELECT * FROM docs UNKNOWN CLAUSE",
		"SELECT * FROM docs /* comment */ DELETE FROM docs",
		"ALTER TABLE docs ADD COLUMN n INT",
		"RENAME TABLE docs TO old_docs",
		"REFRESH MATERIALIZED VIEW visible_docs",
		"GRANT ALL ON docs",
		"VACUUM docs",
	}
	for _, sql := range failClosed {
		if got := SQLCapability(sql); got != all {
			t.Fatalf("unproven SQL %q=%x want=%x", sql, got, all)
		}
	}
}

func TestSQLCapabilityWarmPathAllocationFree(t *testing.T) {
	sql := "WITH c AS (SELECT id FROM docs WHERE n > 1) SELECT id FROM c ORDER BY id"
	for range 3 {
		if SQLCapability(sql) != CapabilityDataRead {
			t.Fatal("warm-up classification changed")
		}
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if SQLCapability(sql) != CapabilityDataRead {
			panic("classification changed")
		}
	}); allocations != 0 {
		t.Fatalf("SQLCapability allocations=%v", allocations)
	}
}
