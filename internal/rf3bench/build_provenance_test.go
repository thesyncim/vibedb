package rf3bench

import (
	"os"
	"runtime/debug"
	"strings"
	"testing"
)

func TestBuildProvenanceFailsClosedOnAbsentDirtyAndContradictorySource(t *testing.T) {
	revision := strings.Repeat("a", 40)
	settings := func(revision, modified string) []debug.BuildSetting {
		return []debug.BuildSetting{{Key: "vcs.revision", Value: revision}, {Key: "vcs.modified", Value: modified}}
	}
	for _, test := range []struct {
		name, revision, modified   string
		settings                   []debug.BuildSetting
		wantRevision, wantModified string
	}{
		{"absent", "", "", nil, "unknown", "unknown"},
		{"clean stamped", revision, "false", nil, revision, "false"},
		{"dirty stamped", revision, "true", nil, revision, "true"},
		{"standard build", "", "", settings(revision, "false"), revision, "false"},
		{"matching", revision, "false", settings(revision, "false"), revision, "false"},
		{"revision conflict", revision, "false", settings(strings.Repeat("b", 40), "false"), "unknown", "unknown"},
		{"dirty conflict", revision, "false", settings(revision, "true"), "unknown", "unknown"},
		{"partial stamp", revision, "", nil, "unknown", "unknown"},
		{"partial settings", revision, "false", []debug.BuildSetting{{Key: "vcs.revision", Value: revision}}, "unknown", "unknown"},
		{"malformed revision", "HEAD", "false", nil, "unknown", "unknown"},
		{"malformed dirty", revision, "0", nil, "unknown", "unknown"},
		{"duplicate", "", "", append(settings(revision, "false"), debug.BuildSetting{Key: "vcs.modified", Value: "false"}), "unknown", "unknown"},
		{"duplicate empty", "", "", append([]debug.BuildSetting{{Key: "vcs.revision"}}, settings(revision, "false")...), "unknown", "unknown"},
		{"sha256", strings.Repeat("d", 64), "false", nil, strings.Repeat("d", 64), "false"},
	} {
		t.Run(test.name, func(t *testing.T) {
			revision, modified := resolveBuildProvenance(test.revision, test.modified, test.settings)
			if revision != test.wantRevision || modified != test.wantModified {
				t.Fatalf("got %q/%q, want %q/%q", revision, modified, test.wantRevision, test.wantModified)
			}
		})
	}
}

func TestBuildProvenanceLinkStamp(t *testing.T) {
	// Optional compile-path qualification checks the actual linker symbol,
	// not a runtime substitution. Expected values are assertions only.
	expected := os.Getenv("VIBEDB_RF3_EXPECT_BUILD_REVISION")
	if expected == "" {
		return
	}
	revision, modified := BuildProvenance()
	if revision != expected || modified != os.Getenv("VIBEDB_RF3_EXPECT_BUILD_MODIFIED") {
		t.Fatalf("linked provenance=%q/%q, expected=%q/%q", revision, modified, expected, os.Getenv("VIBEDB_RF3_EXPECT_BUILD_MODIFIED"))
	}
}
