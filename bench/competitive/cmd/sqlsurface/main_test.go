package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestMatchedSQLSurfacesSmoke(t *testing.T) {
	for _, test := range []struct{ engine, surface string }{
		{"vibedb", "database-sql"}, {"sqlite", "database-sql"}, {"vibedb", "pgwire"},
	} {
		t.Run(test.engine+"/"+test.surface, func(t *testing.T) {
			var out bytes.Buffer
			if err := run([]string{"-engine=" + test.engine, "-interface=" + test.surface,
				"-corpus=20", "-operations=40", "-max-rss-bytes=1073741824",
				"-max-physical-write-bytes=1073741824"}, &out); err != nil {
				t.Fatal(err)
			}
			text := out.String()
			for _, token := range []string{"durability", "exact-indexes", "p99.9-us", "max-us", test.engine + "\t" + test.surface + "\tpower-safe\t1"} {
				if !strings.Contains(text, token) {
					t.Fatalf("output omits %q:\n%s", token, text)
				}
			}
		})
	}
}

func TestSQLDocumentsAreExactSameSizeVibeJSON(t *testing.T) {
	for i := range 100 {
		first, second := sqlDocument(i, false), sqlDocument(i, true)
		if len(first) != len(second) || bytes.Equal(first, second) {
			t.Fatalf("document %d lengths=%d/%d equal=%t", i, len(first), len(second), bytes.Equal(first, second))
		}
	}
}
