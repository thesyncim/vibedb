package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDevReadAuthorityIsExplicitRF3OnlyAndFixed(t *testing.T) {
	if newDevReadAuthority(false) != nil {
		t.Fatal("default development cluster unexpectedly enabled read authority")
	}
	config := newDevReadAuthority(true)
	if config == nil || !validDevReadAuthority(*config) {
		t.Fatalf("fixed RF3 read authority policy is invalid: %+v", config)
	}
	config.Voters[0] = 2
	if validDevReadAuthority(*config) {
		t.Fatal("changed voter roster accepted as the fixed policy")
	}
}

func TestDevReadAuthorityRejectsRF1(t *testing.T) {
	root := t.TempDir()
	if _, err := ensureDevCluster(devClusterOptions{
		root: root, replicas: devClusterRF1, readAuthority: true,
	}); !errors.Is(err, errDevCluster) {
		t.Fatalf("RF1 read authority request error = %v", err)
	}
}

func TestDevReadAuthorityRejectsStandaloneRF3(t *testing.T) {
	root := t.TempDir()
	if _, err := ensureDevCluster(devClusterOptions{
		root: root, replicas: devClusterRF3, readAuthority: true,
	}); !errors.Is(err, errDevCluster) {
		t.Fatalf("standalone RF3 read authority request error = %v", err)
	}
}

func TestDevPhysicalReadAuthorityManifestReconciliationSupportsMultipleTables(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			root := t.TempDir()
			nodeRoot := filepath.Join(root, "node-1")
			groupRoot := filepath.Join(nodeRoot, "group-1")
			if err := os.MkdirAll(groupRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			serveManifest := filepath.Join(nodeRoot, "serve-rf3.vibejson")
			groupManifest := filepath.Join(groupRoot, "serve-rf3.vibejson")
			groupSource := map[string]any{
				"wal":           map[string]any{},
				"sql":           map[string]any{},
				"route":         map[string]any{},
				"split_control": map[string]any{"child_registry": map[string]any{}},
				"members":       []any{},
			}
			groupRaw, err := json.Marshal(groupSource)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(groupManifest, groupRaw, 0o600); err != nil {
				t.Fatal(err)
			}
			source := map[string]any{
				"node_log":             map[string]any{},
				"listeners":            map[string]any{},
				"tls":                  map[string]any{},
				"authorization_policy": "policy",
				"replica_control":      map[string]any{},
				"split_control":        map[string]any{},
				"gateway":              map[string]any{},
				"groups":               []any{map[string]any{}},
			}
			if enabled {
				source["read_authority"] = map[string]any{"enabled": true}
			}
			nodeRaw, err := json.Marshal(source)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(serveManifest, nodeRaw, 0o600); err != nil {
				t.Fatal(err)
			}

			member := devClusterMember{GroupRoot: groupRoot, ServeManifest: serveManifest}
			if err := reconcileDevPhysicalNodeGroup(member, true); err != nil {
				t.Fatalf("reconcile enabled=%t: %v", enabled, err)
			}
			updated, err := os.ReadFile(serveManifest)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(updated, &fields); err != nil {
				t.Fatal(err)
			}
			var groups []json.RawMessage
			if err := json.Unmarshal(fields["groups"], &groups); err != nil || len(groups) != 2 {
				t.Fatalf("reconciled groups=%d err=%v", len(groups), err)
			}
			if enabled != (len(fields["read_authority"]) != 0) {
				t.Fatalf("read authority field presence=%t, want %t", len(fields["read_authority"]) != 0, enabled)
			}
		})
	}
}
