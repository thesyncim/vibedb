package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultResumeAcceptsOpaqueArtifactButRejectsEnabledSection(t *testing.T) {
	if err := validateDevReadAuthorityRaw([]byte("retained"), nil); err != nil {
		t.Fatalf("opaque default-off artifact rejected: %v", err)
	}
	if err := validateDevReadAuthorityRaw([]byte("retained"), newDevReadAuthority(true)); !errors.Is(err, errDevCluster) {
		t.Fatalf("opaque artifact accepted for enabled policy: %v", err)
	}
	encoded, err := json.Marshal(map[string]any{
		"read_authority": newDevReadAuthority(true),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := validateDevReadAuthorityRaw(encoded, nil); !errors.Is(err, errDevCluster) {
		t.Fatalf("enabled artifact without cluster policy error = %v", err)
	}
}

func TestPhysicalManifestRejectsMalformedEnabledSectionBeforeRewrite(t *testing.T) {
	root := t.TempDir()
	nodeRoot := filepath.Join(root, "node-1")
	groupRoot := filepath.Join(nodeRoot, "group-1")
	if err := os.MkdirAll(groupRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	groupRaw := []byte(`{"wal":{},"sql":{},"route":{},"split_control":{"child_registry":{}},"members":[]}`)
	if err := os.WriteFile(filepath.Join(groupRoot, "serve-rf3.vibejson"), groupRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	nodeRaw := []byte(`{"node_log":{},"listeners":{},"tls":{},"authorization_policy":"policy","replica_control":{},"split_control":{},"read_authority":{"enabled":true},"gateway":{},"groups":[]}`)
	nodePath := filepath.Join(nodeRoot, "serve-rf3.vibejson")
	if err := os.WriteFile(nodePath, nodeRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	member := devClusterMember{GroupRoot: groupRoot, ServeManifest: nodePath}
	if err := reconcileDevPhysicalNodeGroup(member, true, nil); !errors.Is(err, errDevCluster) {
		t.Fatalf("standard enabled physical manifest error = %v", err)
	}
	updated, err := os.ReadFile(nodePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(updated, nodeRaw) {
		t.Fatal("standard manifest rejection rewrote the physical manifest")
	}
}
