package pgwire

import (
	"testing"
)

// TestSimpleCacheReusesRuntime proves repeated identical simple queries share
// one lowered runtime instead of preparing per query: the cached entry's
// runtime pointer is stable across queries, a DDL publish retires it, and
// DISCARD ALL clears the table.
func TestSimpleCacheReusesRuntime(t *testing.T) {
	srv := newTestServer(t, Options{})
	c := dial(t, srv)
	c.startup(map[string]string{"user": "tester", "database": "app"})

	const q = `SELECT id FROM users WHERE id = 1`
	cachedRuntime := func() BackendStatement {
		t.Helper()
		srv.mu.Lock()
		defer srv.mu.Unlock()
		if len(srv.sessions) != 1 {
			t.Fatalf("sessions = %d, want 1", len(srv.sessions))
		}
		for _, sess := range srv.sessions {
			entry, ok := sess.simpleCache[q]
			if !ok {
				t.Fatalf("no simple-cache entry for %q", q)
			}
			return entry.runtime
		}
		return nil
	}
	cachedSize := func() int {
		t.Helper()
		srv.mu.Lock()
		defer srv.mu.Unlock()
		for _, sess := range srv.sessions {
			return len(sess.simpleCache)
		}
		return -1
	}

	if got := rowsOf(t, c.query(q)); len(got) != 1 {
		t.Fatalf("rows = %d, want 1", len(got))
	}
	first := cachedRuntime()
	if got := rowsOf(t, c.query(q)); len(got) != 1 {
		t.Fatalf("rows = %d, want 1", len(got))
	}
	if second := cachedRuntime(); second != first {
		t.Fatalf("second identical query re-prepared instead of reusing the runtime")
	}

	// A DDL publish retires the entry: the next identical query lowers fresh
	// and still returns correct rows.
	c.query(`CREATE TABLE cache_probe (id STRING PRIMARY KEY)`)
	if got := rowsOf(t, c.query(q)); len(got) != 1 {
		t.Fatalf("rows = %d, want 1", len(got))
	}
	if third := cachedRuntime(); third == first {
		t.Fatalf("post-DDL query reused the pre-DDL runtime")
	}
	c.query(`DROP TABLE cache_probe`)

	// DISCARD ALL honors the cache like the named tables.
	c.query(`DISCARD ALL`)
	if size := cachedSize(); size != 0 {
		t.Fatalf("simple cache size after DISCARD ALL = %d, want 0", size)
	}
	if got := rowsOf(t, c.query(q)); len(got) != 1 {
		t.Fatalf("rows = %d, want 1", len(got))
	}
}
