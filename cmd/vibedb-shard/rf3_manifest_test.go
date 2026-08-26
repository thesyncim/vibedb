package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

const canonicalRF3Manifest = `{
  "wal": {
    "path": "/srv/vibedb/member.wal",
    "key_id": "production-key-1",
    "key_material_path": "/run/secrets/vibedb-wal-key",
    "max_file_bytes": 268435456,
    "max_record_bytes": 8388608,
    "max_records": 4096,
    "max_entries": 16384,
    "max_live_bytes": 134217728
  },
  "sql": {
    "path": "/srv/vibedb/member.vdb",
    "identity_path": "/srv/vibedb/member-sql-identity.json",
    "apply_identity_path": "/srv/vibedb/member-apply-identity.json"
  },
  "listeners": {
    "peer": "0.0.0.0:7400",
    "native": "0.0.0.0:7500",
    "snapshot": "0.0.0.0:7600",
    "control": "0.0.0.0:7700"
  },
  "tls": {
    "certificate": "/run/secrets/member-cert.pem",
    "key": "/run/secrets/member-key.pem",
    "roots": "/run/secrets/cluster-roots.pem",
    "identity_oid": "1.3.6.1.4.1.32473.1.1"
  },
  "authorization_policy": "/etc/vibedb/authorization.json",
  "replica_control": {
    "action_journal_path": "/srv/vibedb/replica-actions",
    "max_action_records": 4096,
    "source_data_root": "/srv/vibedb",
    "source_journal_path": "/srv/vibedb/source-exports",
    "max_source_records": 4096,
    "source_repository_path": "/srv/vibedb/source-artifacts",
    "max_source_artifacts": 8,
    "max_source_concurrent": 2,
    "max_source_artifact_bytes": 1073741824,
    "max_source_disk_bytes": 4294967296,
    "source_chunk_bytes": 1048576
  },
  "members": [
    {"member_id": 1, "node_id": "0102030405060708090a0b0c0d0e0f10", "peer_address": "member-1.internal:7400"},
    {"member_id": 2, "node_id": "1112131415161718191a1b1c1d1e1f20", "peer_address": "member-2.internal:7400"},
    {"member_id": 3, "node_id": "2122232425262728292a2b2c2d2e2f30", "peer_address": "member-3.internal:7400"}
  ]
}`

func TestLoadRF3ManifestCanonical(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rf3.json")
	if err := os.WriteFile(path, []byte(canonicalRF3Manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := loadRF3Manifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.WAL.Path != "/srv/vibedb/member.wal" ||
		manifest.WAL.KeyID != "production-key-1" ||
		manifest.WAL.KeyMaterialPath != "/run/secrets/vibedb-wal-key" {
		t.Fatalf("WAL identity = %+v", manifest.WAL)
	}
	if manifest.WAL.Options.MaxFileBytes != 268435456 ||
		manifest.WAL.Options.MaxRecordBytes != 8388608 ||
		manifest.WAL.Options.MaxRecords != 4096 ||
		manifest.WAL.Options.MaxEntries != 16384 ||
		manifest.WAL.Options.MaxLiveBytes != 134217728 {
		t.Fatalf("WAL options = %+v", manifest.WAL.Options)
	}
	if manifest.SQL.Path != "/srv/vibedb/member.vdb" ||
		manifest.SQL.IdentityPath != "/srv/vibedb/member-sql-identity.json" ||
		manifest.SQL.ApplyIdentityPath != "/srv/vibedb/member-apply-identity.json" {
		t.Fatalf("SQL artifacts = %+v", manifest.SQL)
	}
	if manifest.Listeners.Peer != "0.0.0.0:7400" || manifest.Listeners.Native != "0.0.0.0:7500" ||
		manifest.Listeners.Snapshot != "0.0.0.0:7600" ||
		manifest.Listeners.Control != "0.0.0.0:7700" {
		t.Fatalf("listeners = %+v", manifest.Listeners)
	}
	if manifest.TLS.Certificate != "/run/secrets/member-cert.pem" ||
		manifest.TLS.Key != "/run/secrets/member-key.pem" ||
		manifest.TLS.Roots != "/run/secrets/cluster-roots.pem" ||
		manifest.TLS.IdentityOID != "1.3.6.1.4.1.32473.1.1" {
		t.Fatalf("TLS = %+v", manifest.TLS)
	}
	if manifest.AuthorizationPolicy != "/etc/vibedb/authorization.json" {
		t.Fatalf("authorization policy = %q", manifest.AuthorizationPolicy)
	}
	control := manifest.ReplicaControl
	if control.ActionJournalPath != "/srv/vibedb/replica-actions" ||
		control.MaxActionRecords != 4096 || control.SourceDataRoot != "/srv/vibedb" ||
		control.SourceJournalPath != "/srv/vibedb/source-exports" ||
		control.MaxSourceRecords != 4096 ||
		control.SourceRepositoryPath != "/srv/vibedb/source-artifacts" ||
		control.MaxSourceArtifacts != 8 || control.MaxSourceConcurrent != 2 ||
		control.MaxSourceArtifactBytes != 1073741824 ||
		control.MaxSourceDiskBytes != 4294967296 || control.SourceChunkBytes != 1048576 {
		t.Fatalf("replica control = %+v", control)
	}
	var first rafttransport.NodeID
	for index := range first {
		first[index] = byte(index + 1)
	}
	if manifest.Members[0].MemberID != 1 || manifest.Members[0].NodeID != first ||
		manifest.Members[0].PeerAddress != "member-1.internal:7400" ||
		manifest.Members[2].MemberID != 3 {
		t.Fatalf("members = %+v", manifest.Members)
	}
	if manifest.EnrolledTarget != nil {
		t.Fatalf("unexpected enrolled target = %+v", manifest.EnrolledTarget)
	}
}

func TestParseRF3ManifestRetainsOneEnrolledTargetOutsideServingRF3(t *testing.T) {
	document := enrolledRF3Manifest()
	manifest, err := parseRF3Manifest([]byte(document))
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Members) != rf3ManifestMembers ||
		manifest.Members[0].MemberID != 1 || manifest.Members[2].MemberID != 3 {
		t.Fatalf("serving members = %+v", manifest.Members)
	}
	target := manifest.EnrolledTarget
	if target == nil || target.MemberID != 4 ||
		target.NodeID != (rafttransport.NodeID{
			0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
			0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40,
		}) ||
		target.StoreID != ([16]byte{
			0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48,
			0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50,
		}) || target.NodeIncarnation != 9 ||
		target.PeerAddress != "member-4.internal:7400" ||
		target.NativeAddress != "member-4.internal:7500" ||
		target.SnapshotAddress != "member-4.internal:7600" ||
		target.ControlAddress != "member-4.internal:7700" {
		t.Fatalf("enrolled target = %+v", target)
	}
}

func TestParseRF3ManifestRejectsNoncanonicalGrammar(t *testing.T) {
	replace := func(old, new string) string {
		t.Helper()
		if !strings.Contains(canonicalRF3Manifest, old) {
			t.Fatalf("fixture does not contain %q", old)
		}
		return strings.Replace(canonicalRF3Manifest, old, new, 1)
	}
	tests := []struct {
		name string
		data string
	}{
		{"top-level-not-object", `[]`},
		{"reordered-top-level", replace(`"wal": {`, `"sql": {`)},
		{"unknown-top-level", replace(`"members": [`, `"unknown": 1, "members": [`)},
		{"duplicate-top-level", replace(`"members": [`, `"authorization_policy": "/other", "members": [`)},
		{"missing-top-level", replace(`  "authorization_policy": "/etc/vibedb/authorization.json",`+"\n", ``)},
		{"escaped-top-level-key", replace(`"authorization_policy"`, `"authorization_\u0070olicy"`)},
		{"wal-not-object", replace(`"wal": {`, `"wal": 1, "discarded": {`)},
		{"reordered-wal", replace(`"path": "/srv/vibedb/member.wal",`+"\n    "+`"key_id": "production-key-1"`, `"key_id": "production-key-1",`+"\n    "+`"path": "/srv/vibedb/member.wal"`)},
		{"unknown-wal", replace(`"max_live_bytes": 134217728`, `"unknown": 1, "max_live_bytes": 134217728`)},
		{"duplicate-wal", replace(`"max_live_bytes": 134217728`, `"max_entries": 16384, "max_live_bytes": 134217728`)},
		{"missing-wal", replace(`    "key_id": "production-key-1",`+"\n", ``)},
		{"reordered-sql", replace(`"path": "/srv/vibedb/member.vdb",`+"\n    "+`"identity_path":`, `"identity_path": "/srv/vibedb/member-sql-identity.json",`+"\n    "+`"path":`)},
		{"reordered-listeners", replace(`"peer": "0.0.0.0:7400",`+"\n    "+`"native": "0.0.0.0:7500"`, `"native": "0.0.0.0:7500",`+"\n    "+`"peer": "0.0.0.0:7400"`)},
		{"missing-control-listener", replace(`,`+"\n    "+`"control": "0.0.0.0:7700"`, ``)},
		{"reordered-tls", replace(`"certificate": "/run/secrets/member-cert.pem",`+"\n    "+`"key": "/run/secrets/member-key.pem"`, `"key": "/run/secrets/member-key.pem",`+"\n    "+`"certificate": "/run/secrets/member-cert.pem"`)},
		{"escaped-value", replace(`/srv/vibedb/member.wal`, `/srv/vibedb/member\u002ewal`)},
		{"empty-value", replace(`/srv/vibedb/member.wal`, ``)},
		{"zero-bound", replace(`"max_records": 4096`, `"max_records": 0`)},
		{"negative-bound", replace(`"max_records": 4096`, `"max_records": -1`)},
		{"fractional-bound", replace(`"max_records": 4096`, `"max_records": 1.5`)},
		{"int64-overflow", replace(`"max_file_bytes": 268435456`, `"max_file_bytes": 9223372036854775808`)},
		{"int-overflow", replace(`"max_record_bytes": 8388608`, `"max_record_bytes": 18446744073709551615`)},
		{"relative-action-journal", replace(`/srv/vibedb/replica-actions`, `replica-actions`)},
		{"unclean-source-root", replace(`/srv/vibedb",`+"\n"+`    "source_journal_path`, `/srv/vibedb/../vibedb",`+"\n"+`    "source_journal_path`)},
		{"oversize-action-records", replace(`"max_action_records": 4096`, `"max_action_records": 4097`)},
		{"undersize-source-chunk", replace(`"source_chunk_bytes": 1048576`, `"source_chunk_bytes": 1024`)},
		{"source-disk-below-artifact", replace(`"max_source_disk_bytes": 4294967296`, `"max_source_disk_bytes": 1024`)},
		{"two-members", replace(`,`+"\n    "+`{"member_id": 3, "node_id": "2122232425262728292a2b2c2d2e2f30", "peer_address": "member-3.internal:7400"}`, ``)},
		{"four-members", replace(`{"member_id": 3, "node_id": "2122232425262728292a2b2c2d2e2f30", "peer_address": "member-3.internal:7400"}`, `{"member_id": 3, "node_id": "2122232425262728292a2b2c2d2e2f30", "peer_address": "member-3.internal:7400"},`+"\n    "+`{"member_id": 4, "node_id": "3132333435363738393a3b3c3d3e3f40", "peer_address": "member-4.internal:7400"}`)},
		{"reordered-member-fields", replace(`{"member_id": 1, "node_id": "0102030405060708090a0b0c0d0e0f10"`, `{"node_id": "0102030405060708090a0b0c0d0e0f10", "member_id": 1`)},
		{"zero-member", replace(`"member_id": 1`, `"member_id": 0`)},
		{"unordered-members", replace(`"member_id": 2`, `"member_id": 1`)},
		{"uppercase-node", replace(`0102030405060708090a0b0c0d0e0f10`, `0102030405060708090A0B0C0D0E0F10`)},
		{"escaped-node", replace(`0102030405060708090a0b0c0d0e0f10`, `0102030405060708090a0b0c0d0e0f\u0031\u0030`)},
		{"short-node", replace(`0102030405060708090a0b0c0d0e0f10`, `0102`)},
		{"zero-node", replace(`0102030405060708090a0b0c0d0e0f10`, `00000000000000000000000000000000`)},
		{"duplicate-node", replace(`1112131415161718191a1b1c1d1e1f20`, `0102030405060708090a0b0c0d0e0f10`)},
		{"duplicate-peer-address", replace(`member-2.internal:7400`, `member-1.internal:7400`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseRF3Manifest([]byte(tc.data)); !errors.Is(err, errInvalidRF3Manifest) {
				t.Fatalf("parse error = %v, want errInvalidRF3Manifest", err)
			}
		})
	}
}

func TestParseRF3ManifestRejectsInvalidEnrolledTarget(t *testing.T) {
	canonical := enrolledRF3Manifest()
	replace := func(old, new string) string {
		t.Helper()
		if !strings.Contains(canonical, old) {
			t.Fatalf("fixture does not contain %q", old)
		}
		return strings.Replace(canonical, old, new, 1)
	}
	tests := []struct {
		name string
		data string
	}{
		{"target-not-object", replace(`"enrolled_target": {`, `"enrolled_target": 1, "discarded": {`)},
		{"reordered-fields", replace(
			`"member_id": 4,`+"\n    "+`"node_id": "3132333435363738393a3b3c3d3e3f40"`,
			`"node_id": "3132333435363738393a3b3c3d3e3f40",`+"\n    "+`"member_id": 4`,
		)},
		{"unknown-field", replace(`"control_address":`, `"unknown": 1, "control_address":`)},
		{"duplicate-field", replace(`"control_address":`, `"snapshot_address": "member-4.internal:7600", "control_address":`)},
		{"zero-member", replace(`"member_id": 4`, `"member_id": 0`)},
		{"serving-member", replace(`"member_id": 4`, `"member_id": 2`)},
		{"serving-node", replace(`3132333435363738393a3b3c3d3e3f40`, `1112131415161718191a1b1c1d1e1f20`)},
		{"zero-store", replace(`4142434445464748494a4b4c4d4e4f50`, `00000000000000000000000000000000`)},
		{"zero-incarnation", replace(`"node_incarnation": 9`, `"node_incarnation": 0`)},
		{"serving-peer-address", replace(`member-4.internal:7400`, `member-2.internal:7400`)},
		{"native-repeats-serving-peer", replace(`member-4.internal:7500`, `member-2.internal:7400`)},
		{"repeated-target-address", replace(`member-4.internal:7700`, `member-4.internal:7600`)},
		{"empty-address", replace(`member-4.internal:7700`, ``)},
		{"second-target", strings.TrimSuffix(canonical, "\n}") + `, "enrolled_target": {}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseRF3Manifest([]byte(tc.data)); !errors.Is(err, errInvalidRF3Manifest) {
				t.Fatalf("parse error = %v, want errInvalidRF3Manifest", err)
			}
		})
	}
}

func enrolledRF3Manifest() string {
	return strings.TrimSuffix(canonicalRF3Manifest, "\n}") + `,
  "enrolled_target": {
    "member_id": 4,
    "node_id": "3132333435363738393a3b3c3d3e3f40",
    "store_id": "4142434445464748494a4b4c4d4e4f50",
    "node_incarnation": 9,
    "peer_address": "member-4.internal:7400",
    "native_address": "member-4.internal:7500",
    "snapshot_address": "member-4.internal:7600",
    "control_address": "member-4.internal:7700"
  }
}`
}

func TestLoadRF3ManifestEnforcesFileBound(t *testing.T) {
	directory := t.TempDir()
	if _, err := loadRF3Manifest(""); !errors.Is(err, errInvalidRF3Manifest) {
		t.Fatalf("empty path error = %v", err)
	}
	if _, err := loadRF3Manifest(filepath.Join(directory, "missing")); !errors.Is(err, errInvalidRF3Manifest) {
		t.Fatalf("missing path error = %v", err)
	}
	for name, data := range map[string][]byte{
		"empty":    nil,
		"oversize": make([]byte, maxRF3ManifestBytes+1),
	} {
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadRF3Manifest(path); !errors.Is(err, errInvalidRF3Manifest) {
			t.Fatalf("load %s error = %v", name, err)
		}
	}
}

func TestParseRF3ManifestEnforcesInputBoundAndDepth(t *testing.T) {
	if _, err := parseRF3Manifest(make([]byte, maxRF3ManifestBytes+1)); !errors.Is(err, errInvalidRF3Manifest) {
		t.Fatalf("oversize parse error = %v", err)
	}
	deep := `{"wal":{"path":{"a":{"b":{"c":1}}}}}`
	if _, err := parseRF3Manifest([]byte(deep)); !errors.Is(err, errInvalidRF3Manifest) {
		t.Fatalf("deep parse error = %v", err)
	}
}
