package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestSnapshotPressureSmokeReportsMatchedPhases(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"-engine=vibedb", "-corpus=128", "-operations=256",
		"-checkpoint-mutations=32", "-exact-indexes=1", "-max-rss-bytes=1073741824",
		"-max-allocated-bytes=1073741824", "-max-physical-write-bytes=1073741824"}, &out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, token := range []string{"p99.9-us", "allocated-bytes", "physical-write-known",
		"vibedb\tcontrol\tbuffered-visible\t1", "vibedb\tpinned\tbuffered-visible\t1"} {
		if !strings.Contains(text, token) {
			t.Fatalf("output omits %q:\n%s", token, text)
		}
	}
}
