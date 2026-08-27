package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibejson"
)

func TestGatewaySchemaRolloutManifestRequiresCanonicalVibeJSON(t *testing.T) {
	manifest := gatewaySchemaRolloutManifest{Operation: string(make([]byte, 64)),
		TargetCatalog: "/catalog/target.vdb",
		Replicas: []gatewaySchemaRolloutReplicaConfig{{Node: string(make([]byte, 32)),
			Member: 1, ApplyContract: string(make([]byte, 64)), Bundle: "/bundle/one.vdb"}}}
	raw, err := vibejson.Marshal(&manifest)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = vibejson.AppendCanonicalize(nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "schema-rollout.vjson")
	if err = os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	opened, err := loadGatewaySchemaRolloutManifest(path)
	if err != nil || opened.Operation != manifest.Operation ||
		len(opened.Replicas) != len(manifest.Replicas) {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	noncanonical := append([]byte(" \n"), raw...)
	if err = os.WriteFile(path, noncanonical, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = loadGatewaySchemaRolloutManifest(path); err == nil {
		t.Fatal("noncanonical schema rollout manifest accepted")
	}
}

func TestSchemaRolloutCommandRequiresCompleteAuthenticatedConfiguration(t *testing.T) {
	if status := run([]string{"vibedb-gateway", "schema-rollout"}); status != 2 {
		t.Fatalf("schema-rollout status=%d", status)
	}
}
