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
		"type: ClusterIP", "replicas: 0", "REPLACE_WITH_TARGET_CONFIG",
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
	want := [sha256.Size]byte{0xd8, 0x80, 0x79, 0xb2, 0xa7, 0x7e, 0xc6, 0x91,
		0xa8, 0xe2, 0xe7, 0xdd, 0x78, 0x39, 0x82, 0x61,
		0x2e, 0x91, 0xe1, 0xad, 0x38, 0x02, 0x84, 0x30,
		0x19, 0xf2, 0x84, 0x21, 0x6c, 0x87, 0x2a, 0x4f}
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
}
