package serviceauthz

import "testing"

func TestSQLCapabilitySkipsCommentsAndFailsClosed(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE docs (id TEXT)",
		" -- operator note\n ALTER TABLE docs ADD COLUMN n INT",
		"/* bounded comment */ DROP TABLE docs",
	} {
		if got := SQLCapability(sql); got != CapabilitySchema {
			t.Fatalf("SQLCapability(%q)=%x", sql, got)
		}
	}
	for _, sql := range []string{"/* unterminated", "@invalid"} {
		if got := SQLCapability(sql); got != CapabilityDataRead|CapabilityDataWrite|CapabilitySchema {
			t.Fatalf("invalid SQL %q=%x", sql, got)
		}
	}
	if got := SQLCapability("UPDATE docs SET n = 1"); got != CapabilityDataWrite {
		t.Fatalf("write=%x", got)
	}
	for _, sql := range []string{
		"SELECT * FROM docs", "WITH c AS (SELECT * FROM docs) SELECT * FROM c",
		"EXPLAIN SELECT * FROM docs", "VALUES (1)", "TABLE docs",
		"(SELECT * FROM docs)",
		"SELECT ';' FROM docs", "SELECT 1 /* ; DELETE */; -- one trailing terminator",
	} {
		if got := SQLCapability(sql); got != CapabilityDataRead {
			t.Fatalf("read %q=%x", sql, got)
		}
	}
	for _, sql := range []string{
		"SELECT 1; DELETE FROM docs", "SELECT 1;;", `SELECT "unterminated`,
		"GRANT ALL ON docs", "VACUUM docs",
	} {
		if got := SQLCapability(sql); got != CapabilityDataRead|CapabilityDataWrite|CapabilitySchema {
			t.Fatalf("mixed SQL %q=%x", sql, got)
		}
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if SQLCapability("/* comment */ CREATE TABLE docs (id TEXT)") != CapabilitySchema {
			panic("classification changed")
		}
	}); allocations != 0 {
		t.Fatalf("SQLCapability allocations=%v", allocations)
	}
}
