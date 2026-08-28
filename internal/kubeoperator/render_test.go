package kubeoperator

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"testing"
)

func testConfig() Config {
	return Config{Namespace: "vibedb-test", Image: "registry.example/vibedb:test",
		ShardNodeIDs: [9]string{
			"11000000000000000000000000000000",
			"12000000000000000000000000000000",
			"13000000000000000000000000000000",
			"21000000000000000000000000000000",
			"22000000000000000000000000000000",
			"23000000000000000000000000000000",
			"31000000000000000000000000000000",
			"32000000000000000000000000000000",
			"33000000000000000000000000000000",
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
		"vibedb-catalog-0.vibedb-catalog-peer:7511=11000000000000000000000000000000",
		"vibedb-ledger-1.vibedb-ledger-peer:7511=22000000000000000000000000000000",
		"vibedb-data-2.vibedb-data-peer:7511=33000000000000000000000000000000",
		"type: ClusterIP", "replicas: 0", "replace-with-target-config",
		"persistentVolumeClaimRetentionPolicy: {whenDeleted: Retain, whenScaled: Retain}",
		"whenUnsatisfiable: DoNotSchedule", "livenessProbe:",
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
	if err := ValidateRendered(output.Bytes()); err != nil {
		t.Fatalf("rendered contract: %v", err)
	}
	want := [sha256.Size]byte{0x53, 0xf9, 0x9e, 0xee, 0xeb, 0xe7, 0x9c, 0x52,
		0xc7, 0x9e, 0x3e, 0x97, 0xb5, 0xcd, 0xe1, 0x5e,
		0x16, 0xb8, 0xef, 0x94, 0xcd, 0xb2, 0x8f, 0x41,
		0x64, 0x99, 0x9a, 0x8a, 0x2f, 0xed, 0xcc, 0xdd}
	if got := sha256.Sum256(output.Bytes()); got != want {
		t.Fatalf("golden digest = %x; update want only after reviewing the complete manifest", got)
	}
}

func TestValidateRenderedRejectsLostDurabilityAndTopologyControls(t *testing.T) {
	var output bytes.Buffer
	if err := Render(&output, testConfig()); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct{ old, replacement string }{
		{"whenDeleted: Retain", "whenDeleted: Delete"},
		{"whenUnsatisfiable: DoNotSchedule", "whenUnsatisfiable: ScheduleAnyway"},
		{"  replicas: 3\n", "  replicas: 2\n"},
		{"livenessProbe:", "lostProbe:"},
		{"{name: bootstrap, mountPath: /bootstrap, readOnly: true}", "{name: bootstrap, mountPath: /lost-bootstrap, readOnly: true}"},
		{"- -catalog-route-seed=/var/lib/vibedb/catalog-route-seed.vibejson", "- -catalog-route-seed=/etc/vibedb/cluster.vibejson"},
		{"- -catalog=/var/lib/vibedb/catalog-genesis.vibejson", "- -catalog=/etc/vibedb/cluster.vibejson"},
		{"args: [prepare-gateway,", "args: [missing-init,"},
	} {
		raw := strings.Replace(output.String(), mutation.old, mutation.replacement, 1)
		if err := ValidateRendered([]byte(raw)); err != ErrManifest {
			t.Fatalf("mutation %q err=%v", mutation.old, err)
		}
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
