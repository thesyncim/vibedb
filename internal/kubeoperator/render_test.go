package kubeoperator

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"testing"
)

func testConfig() Config {
	return Config{Namespace: "vibedb-test", Image: "registry.example/vibedb:test",
		ShardNodeIDs: [3]string{
			"11000000000000000000000000000000",
			"12000000000000000000000000000000",
			"13000000000000000000000000000000",
		},
		ManifestConfigMap: "rf3-manifests", TLSSecret: "rf3-tls",
		GatewayConfigMap: "gateway-config", GatewayTLSSecret: "gateway-tls",
		StorageClass: "fast-local", ShardStorage: "20Gi", GatewayStorage: "1Gi"}
}

func TestRenderRF3GoldenAndSafetyContract(t *testing.T) {
	var output bytes.Buffer
	if err := Render(&output, testConfig()); err != nil {
		t.Fatal(err)
	}
	raw := output.String()
	for _, required := range []string{
		"clusterIP: None", "publishNotReadyAddresses: true", "replicas: 3",
		"podManagementPolicy: Parallel", "maxUnavailable: 1",
		"terminationGracePeriodSeconds: 120", "volumeClaimTemplates:",
		"vibedb-shard-0.vibedb-shard-peer:7511=11000000000000000000000000000000",
		"type: ClusterIP", "replicas: 0", "replace-with-target-config",
		"app.kubernetes.io/component: serving",
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("render missing %q", required)
		}
	}
	for _, forbidden := range []string{"leader: true", "leader-election", "emptyDir:"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("render contains forbidden authority/storage shortcut %q", forbidden)
		}
	}
	if strings.Count(raw, "app.kubernetes.io/component: serving") != 4 {
		t.Fatal("serving shard selector is not isolated from replacement learners")
	}
	want := [sha256.Size]byte{0x6e, 0x4a, 0x0c, 0x17, 0x55, 0x63, 0xec, 0x4a,
		0x57, 0xcb, 0xa1, 0x98, 0x7a, 0xed, 0xa3, 0x79,
		0x0c, 0xe4, 0x10, 0xc5, 0x0c, 0x3a, 0x54, 0x28,
		0xfd, 0x84, 0x4e, 0x79, 0x8a, 0xf2, 0x67, 0x6c}
	if got := sha256.Sum256(output.Bytes()); got != want {
		t.Fatalf("golden digest = %x; update want only after reviewing the complete manifest", got)
	}
}

func TestRenderRejectsInvalidIdentityAndInjection(t *testing.T) {
	config := testConfig()
	config.ShardNodeIDs[1] = "ABC"
	if err := Render(&bytes.Buffer{}, config); err != ErrConfig {
		t.Fatalf("short node identity err=%v", err)
	}
	config = testConfig()
	config.Image = "image\nkind: Secret"
	if err := Render(&bytes.Buffer{}, config); err != ErrConfig {
		t.Fatalf("newline injection err=%v", err)
	}
	if err := Render(nil, testConfig()); err != ErrConfig {
		t.Fatalf("nil writer err=%v", err)
	}
	config = testConfig()
	config.ShardNodeIDs[1] = config.ShardNodeIDs[0]
	if err := Render(&bytes.Buffer{}, config); err != ErrConfig {
		t.Fatalf("duplicate node identity err=%v", err)
	}
	config = testConfig()
	config.ShardNodeIDs[1] = strings.Repeat("0", 32)
	if err := Render(&bytes.Buffer{}, config); err != ErrConfig {
		t.Fatalf("zero node identity err=%v", err)
	}
}
