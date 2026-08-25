package serviceauthz

import "testing"

func TestSQLCapabilitySkipsCommentsAndFailsClosed(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE docs (id TEXT)",
		" -- operator note\n ALTER TABLE docs ADD COLUMN n INT",
		"/* bounded comment */ DROP TABLE docs",
		"/* unterminated",
		"@invalid",
	} {
		if got := SQLCapability(sql, true); got != CapabilitySchema {
			t.Fatalf("SQLCapability(%q)=%x", sql, got)
		}
	}
	if got := SQLCapability("UPDATE docs SET n = 1", true); got != CapabilityDataWrite {
		t.Fatalf("write=%x", got)
	}
	if got := SQLCapability("CREATE TABLE docs (id TEXT)", false); got != CapabilityDataRead {
		t.Fatalf("read lane=%x", got)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if SQLCapability("/* comment */ CREATE TABLE docs (id TEXT)", true) != CapabilitySchema {
			panic("classification changed")
		}
	}); allocations != 0 {
		t.Fatalf("SQLCapability allocations=%v", allocations)
	}
}
