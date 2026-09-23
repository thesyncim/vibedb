package pgwire

import (
	"path/filepath"
	"testing"
)

// TestFullStackTinRoundTripAcrossReopen drives the full-text lane through
// the same stack a real client crosses — pgwire wire protocol, the
// database/sql driver catalog, and the durable store — and proves the
// declaration and its answers survive a restart.
//
// CREATE INDEX ... USING tin publishes only the declaration; postings build
// lazily per generation on first query use. The reopen half therefore proves
// both mirrors round-tripped the method: the SQL catalog (which would
// otherwise rematerialize the declaration as an exact index) and the durable
// page catalog (which carries the tin section).
func TestFullStackTinRoundTripAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")

	c, stop := openFullStack(t, path)
	wireOK(t, c.query(`CREATE TABLE fts (`+
		`id STRING PRIMARY KEY, body STRING)`))
	wireOK(t, c.query(`CREATE INDEX fts_body_tin ON fts(body) USING tin`))
	seed := c.query(`INSERT INTO fts VALUES ` +
		`('{"id":"one","body":"luxury goods and vintage watches"}'),` +
		`('{"id":"two","body":"cheap goods everyday"}'),` +
		`('{"id":"three","body":"luxury watches"}') RETURNING id`)
	wireOK(t, seed)
	if tag := commandTagOf(t, seed); tag != "INSERT 0 3" {
		t.Fatalf("seed INSERT tag = %q, want %q", tag, "INSERT 0 3")
	}

	if got := wireKeys(t, mustSelect(t, c,
		`SELECT id FROM fts WHERE body ==> 'luxury' ORDER BY id`)); !equalStrings(
		got, []string{"one", "three"}) {
		t.Fatalf("wire ==> luxury = %v, want [one three]", got)
	}
	if got := wireKeys(t, mustSelect(t, c,
		`SELECT id FROM fts WHERE body ==> 'luxury AND watches' ORDER BY id`)); !equalStrings(
		got, []string{"one", "three"}) {
		t.Fatalf("wire ==> luxury AND watches = %v, want [one three]", got)
	}

	// A write after the declaration is searchable without redeclaring:
	// the next generation builds its own postings on first use.
	wireOK(t, c.query(`INSERT INTO fts VALUES `+
		`('{"id":"four","body":"luxury yachts"}')`))
	if got := wireKeys(t, mustSelect(t, c,
		`SELECT id FROM fts WHERE body ==> 'luxury' ORDER BY id`)); !equalStrings(
		got, []string{"four", "one", "three"}) {
		t.Fatalf("wire ==> luxury after write = %v, want [four one three]", got)
	}
	stop()

	c2, _ := openFullStack(t, path)
	if got := wireKeys(t, mustSelect(t, c2,
		`SELECT id FROM fts WHERE body ==> 'luxury' ORDER BY id`)); !equalStrings(
		got, []string{"four", "one", "three"}) {
		t.Fatalf("reopened wire ==> luxury = %v, want [four one three]", got)
	}
	if got := wireKeys(t, mustSelect(t, c2,
		`SELECT id FROM fts WHERE body ==> 'vintage' ORDER BY id`)); !equalStrings(
		got, []string{"one"}) {
		t.Fatalf("reopened wire ==> vintage = %v, want [one]", got)
	}
	// The reopened declaration still rejects what it must: UNIQUE was
	// never a tin shape, and a duplicate declaration is still a duplicate.
	if msgs := c2.query(
		`CREATE UNIQUE INDEX fts_body_unique ON fts(body) USING tin`,
	); !has(msgs, msgErrorResponse) {
		t.Fatal("reopened CREATE UNIQUE INDEX USING tin succeeded, want refusal")
	}
	if msgs := c2.query(
		`CREATE INDEX fts_body_tin ON fts(body) USING tin`,
	); !has(msgs, msgErrorResponse) {
		t.Fatal("reopened duplicate CREATE INDEX USING tin succeeded, want refusal")
	}
}
