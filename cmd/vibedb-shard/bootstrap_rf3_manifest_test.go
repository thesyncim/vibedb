package main

import (
	"errors"
	"strings"
	"testing"
)

const canonicalBootstrapRF3Manifest = `{
  "member_manifest": "/srv/vibedb/member.vibejson",
  "control_listener": "127.0.0.1:17700",
  "source_node": "0102030405060708090a0b0c0d0e0f10",
  "source_snapshot_address": "member-1.internal:17600",
  "repository_path": "/srv/vibedb/bootstrap-artifacts",
  "cursor_path": "/srv/vibedb/bootstrap.cursor",
  "journal_path": "/srv/vibedb/bootstrap-journal",
  "static_bootstrap_path": "/srv/vibedb/static-bootstrap.pb",
  "max_artifact_bytes": 1073741824
}`

const canonicalMultiGroupBootstrapRF3Manifest = `{
  "control_listener": "127.0.0.1:17700",
  "groups": [
    {
      "member_manifest": "/srv/vibedb/member.vibejson",
      "source_node": "0102030405060708090a0b0c0d0e0f10",
      "source_snapshot_address": "member-1.internal:17600",
      "repository_path": "/srv/vibedb/bootstrap-artifacts",
      "cursor_path": "/srv/vibedb/bootstrap.cursor",
      "journal_path": "/srv/vibedb/bootstrap-journal",
      "static_bootstrap_path": "/srv/vibedb/static-bootstrap.pb",
      "max_artifact_bytes": 1073741824
    },
    {
      "member_manifest": "/srv/vibedb/second/member.vibejson",
      "source_node": "1112131415161718191a1b1c1d1e1f20",
      "source_snapshot_address": "member-2.internal:17600",
      "repository_path": "/srv/vibedb/second/bootstrap-artifacts",
      "cursor_path": "/srv/vibedb/second/bootstrap.cursor",
      "journal_path": "/srv/vibedb/second/bootstrap-journal",
      "static_bootstrap_path": "/srv/vibedb/second/static-bootstrap.pb",
      "max_artifact_bytes": 536870912
    }
  ]
}`

func TestParseBootstrapRF3ManifestCanonical(t *testing.T) {
	manifest, err := parseBootstrapRF3Manifest([]byte(canonicalBootstrapRF3Manifest))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.MemberManifest != "/srv/vibedb/member.vibejson" ||
		manifest.ControlListener != "127.0.0.1:17700" ||
		manifest.SourceSnapshotAddress != "member-1.internal:17600" ||
		manifest.RepositoryPath != "/srv/vibedb/bootstrap-artifacts" ||
		manifest.CursorPath != "/srv/vibedb/bootstrap.cursor" ||
		manifest.JournalPath != "/srv/vibedb/bootstrap-journal" ||
		manifest.StaticBootstrapPath != "/srv/vibedb/static-bootstrap.pb" ||
		manifest.MaxArtifactBytes != 1<<30 || manifest.SourceNode[0] != 1 ||
		manifest.SourceNode[15] != 0x10 {
		t.Fatalf("manifest=%+v", manifest)
	}
	if _, err = parseBootstrapRF3Manifest([]byte(canonicalBootstrapRF3Manifest + " trailing")); !errors.Is(err, errInvalidBootstrapRF3Manifest) {
		t.Fatalf("trailing error=%v", err)
	}
}

func TestParseBootstrapRF3ManifestCanonicalMultiGroup(t *testing.T) {
	manifest, err := parseBootstrapRF3Manifest([]byte(canonicalMultiGroupBootstrapRF3Manifest))
	if err != nil {
		t.Fatal(err)
	}
	groups := manifest.groupBundles()
	if manifest.ControlListener != "127.0.0.1:17700" || len(manifest.Groups) != 2 || len(groups) != 2 {
		t.Fatalf("manifest=%+v", manifest)
	}
	if groups[0].MemberManifest != "/srv/vibedb/member.vibejson" ||
		groups[1].MemberManifest != "/srv/vibedb/second/member.vibejson" ||
		groups[1].SourceNode[0] != 0x11 || groups[1].SourceNode[15] != 0x20 ||
		groups[1].MaxArtifactBytes != 1<<29 {
		t.Fatalf("groups=%+v", groups)
	}
	projected := manifest.withGroup(groups[1])
	if projected.ControlListener != manifest.ControlListener ||
		projected.MemberManifest != groups[1].MemberManifest ||
		projected.SourceNode != groups[1].SourceNode || len(projected.Groups) != 0 {
		t.Fatalf("projected=%+v", projected)
	}
	legacy, err := parseBootstrapRF3Manifest([]byte(canonicalBootstrapRF3Manifest))
	legacyProjected := legacy.withGroup(legacy.groupBundles()[0])
	if err != nil || len(legacy.Groups) != 0 || len(legacy.groupBundles()) != 1 ||
		legacyProjected.MemberManifest != legacy.MemberManifest ||
		legacyProjected.ControlListener != legacy.ControlListener ||
		legacyProjected.SourceNode != legacy.SourceNode ||
		legacyProjected.MaxArtifactBytes != legacy.MaxArtifactBytes {
		t.Fatalf("legacy compatibility: manifest=%+v error=%v", legacy, err)
	}
}

func TestParseBootstrapRF3ManifestRejectsNoncanonicalMultiGroupInputs(t *testing.T) {
	secondStart := strings.Index(canonicalMultiGroupBootstrapRF3Manifest, "    {\n      \"member_manifest\": \"/srv/vibedb/second")
	if secondStart < 0 {
		t.Fatal("second group fixture not found")
	}
	second := canonicalMultiGroupBootstrapRF3Manifest[secondStart : len(canonicalMultiGroupBootstrapRF3Manifest)-4]
	tests := map[string]string{
		"empty groups": strings.Replace(canonicalMultiGroupBootstrapRF3Manifest,
			canonicalMultiGroupBootstrapRF3Manifest[strings.Index(canonicalMultiGroupBootstrapRF3Manifest, "[\n"):strings.LastIndex(canonicalMultiGroupBootstrapRF3Manifest, "]")+1], "[]", 1),
		"reordered top level": strings.Replace(canonicalMultiGroupBootstrapRF3Manifest,
			`"control_listener": "127.0.0.1:17700",`, `"groups": [],`, 1),
		"reordered group": strings.Replace(canonicalMultiGroupBootstrapRF3Manifest,
			`"member_manifest": "/srv/vibedb/member.vibejson",`,
			`"source_node": "0102030405060708090a0b0c0d0e0f10",`, 1),
		"extra group field": strings.Replace(canonicalMultiGroupBootstrapRF3Manifest,
			`"max_artifact_bytes": 536870912`, `"max_artifact_bytes": 536870912, "extra": true`, 1),
		"escaped group key": strings.Replace(canonicalMultiGroupBootstrapRF3Manifest,
			`"member_manifest"`, `"member_manif\u0065st"`, 1),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := parseBootstrapRF3Manifest([]byte(raw)); !errors.Is(err, errInvalidBootstrapRF3Manifest) {
				t.Fatalf("error=%v", err)
			}
		})
	}
	firstEnd := strings.Index(canonicalMultiGroupBootstrapRF3Manifest, "    },\n    {") + len("    }")
	first := canonicalMultiGroupBootstrapRF3Manifest[strings.Index(canonicalMultiGroupBootstrapRF3Manifest, "    {"):firstEnd]
	overLimit := "{\n  \"control_listener\": \"127.0.0.1:17700\",\n  \"groups\": [\n" +
		strings.Repeat(first+",\n", maxBootstrapRF3ManifestGroups) + second + "\n  ]\n}"
	if _, err := parseBootstrapRF3Manifest([]byte(overLimit)); !errors.Is(err, errInvalidBootstrapRF3Manifest) {
		t.Fatalf("over-limit error=%v", err)
	}
}

func TestParseBootstrapRF3ManifestRejectsNoncanonicalInputs(t *testing.T) {
	tests := map[string]string{
		"reordered": strings.Replace(canonicalBootstrapRF3Manifest,
			`"member_manifest": "/srv/vibedb/member.vibejson",`,
			`"max_artifact_bytes": 1073741824,`, 1),
		"zero artifact": strings.Replace(canonicalBootstrapRF3Manifest,
			`"max_artifact_bytes": 1073741824`, `"max_artifact_bytes": 0`, 1),
		"uppercase node": strings.Replace(canonicalBootstrapRF3Manifest, "0a0b", "0A0B", 1),
		"extra": strings.Replace(canonicalBootstrapRF3Manifest,
			`"max_artifact_bytes": 1073741824`,
			`"max_artifact_bytes": 1073741824, "extra": 1`, 1),
		"escaped key": strings.Replace(canonicalBootstrapRF3Manifest,
			`"member_manifest"`, `"member_manif\u0065st"`, 1),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := parseBootstrapRF3Manifest([]byte(raw)); !errors.Is(err, errInvalidBootstrapRF3Manifest) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestRunBootstrapRF3ArgumentExitClasses(t *testing.T) {
	tests := [][]string{
		{"vibedb-shard", "bootstrap-rf3"},
		{"vibedb-shard", "bootstrap-rf3", "-manifest"},
		{"vibedb-shard", "bootstrap-rf3", "-manifest", "missing", "extra"},
	}
	for _, args := range tests {
		if got := run(args); got != 2 {
			t.Fatalf("run(%q)=%d", args, got)
		}
	}
}
