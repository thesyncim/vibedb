package kubeoperator

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibejson"
)

type failingBootstrapReader struct{}

func (failingBootstrapReader) Read([]byte) (int, error) { return 0, errors.New("entropy unavailable") }

func TestBootstrapReplicaControlBindsOnlyDataSplitSource(t *testing.T) {
	roles := [3]bootstrapRole{{name: "catalog"}, {name: "ledger"}, {
		name: "data", table: "documents", primary: "/id", digest: [32]byte{9},
		group: raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
			TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}},
	}}
	state := bootstrapState{GatewayNodeID: "10101010101010101010101010101010"}
	for i := range state.ShardNodeIDs {
		node := [16]byte{byte(9 - i)}
		state.ShardNodeIDs[i] = hex.EncodeToString(node[:])
	}
	raw, err := bootstrapReplicaControl(roles, state)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(`"split_template"`)) || bytes.Contains(raw, []byte(`"split_child_root"`)) {
		t.Fatal("legacy unscoped split authority in generated manifest")
	}
	var control bootstrapControl
	if err := vibejson.Unmarshal(raw, &control); err != nil {
		t.Fatal(err)
	}
	if len(control.SplitSources) != 1 {
		t.Fatalf("split sources=%d", len(control.SplitSources))
	}
	source, data := control.SplitSources[0], roles[2]
	if source.ClusterID != data.group.ClusterID || source.ClusterIncarnation != data.group.ClusterIncarnation ||
		source.TopologyRecoveryEpoch != data.group.TopologyRecoveryEpoch || source.ShardIncarnation != data.group.ShardIncarnation ||
		source.GroupID != data.group.GroupID || source.SchemaGeneration != 1 || source.RelationManifestDigest != data.digest ||
		source.Table != data.table || source.Template.ShardKey != data.primary || len(source.Replicas) != 3 {
		t.Fatalf("source did not bind exact data group/schema: %+v", source)
	}
	for i, replica := range source.Replicas {
		if replica.Node != state.ShardNodeIDs[8-i] || replica.ChildRoot != "/var/lib/vibedb/member/split-children" {
			t.Fatalf("replica %d is not canonical data-only authority: %+v", i, replica)
		}
	}
}

func TestBootstrapRoleManifestBindsServingProfile(t *testing.T) {
	role := bootstrapRole{distribution: "data", shard: "all", table: "documents", primary: "/id",
		group: raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
			TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}}}
	role.stores[0] = [16]byte{5}
	digest, limits, err := probeBootstrapRole(role)
	if err != nil || digest == ([32]byte{}) || limits.MaxKeyBytes == 0 {
		t.Fatalf("initial serving profile: %x %+v %v", digest, limits, err)
	}
	for i := byte(6); i < 9; i++ {
		role.stores[0] = [16]byte{i}
		got, gotLimits, err := probeBootstrapRole(role)
		if err != nil || got != digest || gotLimits != limits {
			t.Fatal("replica-local store identity changed the serving schema digest")
		}
	}
	role.shard = "other"
	changed, _, err := probeBootstrapRole(role)
	if err != nil || changed == digest {
		t.Fatal("catalog routing profile was omitted from the serving schema digest")
	}
}

func TestBootstrapCreatesCanonicalResumableRF3Authority(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	config := BootstrapConfig{
		Namespace: "vibedb-test", StateDirectory: root,
		ManifestConfigMap: "vibedb-rf3-manifests", TLSSecret: "vibedb-rf3-tls",
		GatewayConfigMap: "vibedb-gateway-config", GatewayTLSSecret: "vibedb-gateway-tls",
		Now: func() time.Time { return time.Unix(1_800_000_000, 0) },
	}
	var first bytes.Buffer
	result, err := Bootstrap(&first, config)
	if err != nil {
		t.Fatal(err)
	}
	if result.Bytes != first.Len() || result.GatewayNodeID == "" || result.ClientNodeID == "" ||
		result.GatewayNodeID == result.ClientNodeID || strings.Count(first.String(), "kind: Secret\n") != 3 ||
		strings.Count(first.String(), "kind: ConfigMap\n") != 2 ||
		!bytes.Contains(first.Bytes(), []byte("name: vibedb-qualification-client-tls")) {
		t.Fatalf("bootstrap result=%+v bytes=%d", result, first.Len())
	}
	seen := map[string]struct{}{result.GatewayNodeID: {}, result.ClientNodeID: {}}
	for _, node := range result.ShardNodeIDs {
		if len(node) != 32 {
			t.Fatalf("node=%q", node)
		}
		if _, duplicate := seen[node]; duplicate {
			t.Fatalf("duplicate node=%q", node)
		}
		seen[node] = struct{}{}
	}
	stateRaw, err := os.ReadFile(filepath.Join(root, bootstrapStateName))
	if err != nil {
		t.Fatal(err)
	}
	var state bootstrapState
	if err = vibejson.Unmarshal(stateRaw, &state); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{bootstrapStateName, bootstrapBundleName} {
		info, statErr := os.Stat(filepath.Join(root, name))
		if statErr != nil {
			t.Fatalf("private artifact %s: %v", name, statErr)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("private artifact %s mode=%v", name, info.Mode())
		}
	}
	canonical, err := vibejson.Marshal(&state)
	if err != nil || !bytes.Equal(canonical, stateRaw) || !validBootstrapState(state, config, first.Bytes()) {
		t.Fatalf("canonical=%t state-valid=%t err=%v", bytes.Equal(canonical, stateRaw), validBootstrapState(state, config, first.Bytes()), err)
	}
	var resumed bytes.Buffer
	again, err := Bootstrap(&resumed, config)
	if err != nil || again != result || !bytes.Equal(first.Bytes(), resumed.Bytes()) {
		t.Fatalf("resume result=%+v equal=%t err=%v", again, bytes.Equal(first.Bytes(), resumed.Bytes()), err)
	}
	loaded, err := LoadBootstrapState(root)
	if err != nil || loaded != result {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadBootstrapState(root); !errors.Is(err, ErrBootstrap) {
		t.Fatalf("loaded authority from public directory: %v", err)
	}
}

func TestBootstrapPropagatesEntropyFailure(t *testing.T) {
	config := BootstrapConfig{Namespace: "vibedb-test", StateDirectory: t.TempDir(),
		ManifestConfigMap: "vibedb-rf3-manifests", TLSSecret: "vibedb-rf3-tls",
		GatewayConfigMap: "vibedb-gateway-config", GatewayTLSSecret: "vibedb-gateway-tls"}
	if _, _, err := buildBootstrap(config, failingBootstrapReader{}); err == nil {
		t.Fatal("entropy failure was ignored")
	}
}

func TestBootstrapRecoversEveryPublicationCut(t *testing.T) {
	source := t.TempDir()
	if err := os.Chmod(source, 0o700); err != nil {
		t.Fatal(err)
	}
	config := BootstrapConfig{Namespace: "vibedb-test", StateDirectory: source,
		ManifestConfigMap: "vibedb-rf3-manifests", TLSSecret: "vibedb-rf3-tls",
		GatewayConfigMap: "vibedb-gateway-config", GatewayTLSSecret: "vibedb-gateway-tls"}
	var output bytes.Buffer
	if _, err := Bootstrap(&output, config); err != nil {
		t.Fatal(err)
	}
	state, err := os.ReadFile(filepath.Join(source, bootstrapStateName))
	if err != nil {
		t.Fatal(err)
	}
	bundle := output.Bytes()
	for _, cut := range []struct {
		name       string
		bundleName string
	}{
		{"both pending", bootstrapBundlePending},
		{"bundle committed", bootstrapBundleName},
	} {
		t.Run(cut.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Chmod(root, 0o700); err != nil {
				t.Fatal(err)
			}
			candidate := config
			candidate.StateDirectory = root
			if err := os.WriteFile(filepath.Join(root, bootstrapStatePending), state, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, cut.bundleName), bundle, 0o600); err != nil {
				t.Fatal(err)
			}
			var resumed bytes.Buffer
			if _, err := Bootstrap(&resumed, candidate); err != nil || !bytes.Equal(bundle, resumed.Bytes()) {
				t.Fatalf("resume equal=%t err=%v", bytes.Equal(bundle, resumed.Bytes()), err)
			}
		})
	}
	t.Run("state pending only", func(t *testing.T) {
		root := t.TempDir()
		if err := os.Chmod(root, 0o700); err != nil {
			t.Fatal(err)
		}
		candidate := config
		candidate.StateDirectory = root
		if err := os.WriteFile(filepath.Join(root, bootstrapStatePending), state, 0o600); err != nil {
			t.Fatal(err)
		}
		_, _, found, err := recoverBootstrap(candidate)
		if err != nil || found {
			t.Fatalf("found=%t err=%v", found, err)
		}
		entries, err := os.ReadDir(root)
		if err != nil || len(entries) != 0 {
			t.Fatalf("entries=%d err=%v", len(entries), err)
		}
	})
}

func TestBootstrapRejectsNonEmptyAndDriftedState(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "foreign"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := BootstrapConfig{Namespace: "vibedb-test", StateDirectory: root,
		ManifestConfigMap: "vibedb-rf3-manifests", TLSSecret: "vibedb-rf3-tls",
		GatewayConfigMap: "vibedb-gateway-config", GatewayTLSSecret: "vibedb-gateway-tls"}
	if _, err := Bootstrap(&bytes.Buffer{}, config); !errors.Is(err, ErrBootstrap) {
		t.Fatalf("nonempty state err=%v", err)
	}
}

func TestBootstrapRejectsUnsafeAuthorityDirectory(t *testing.T) {
	parent := t.TempDir()
	config := BootstrapConfig{Namespace: "vibedb-test",
		ManifestConfigMap: "vibedb-rf3-manifests", TLSSecret: "vibedb-rf3-tls",
		GatewayConfigMap: "vibedb-gateway-config", GatewayTLSSecret: "vibedb-gateway-tls"}
	t.Run("group readable", func(t *testing.T) {
		root := filepath.Join(parent, "unsafe")
		if err := os.Mkdir(root, 0o750); err != nil {
			t.Fatal(err)
		}
		config.StateDirectory = root
		if _, err := Bootstrap(&bytes.Buffer{}, config); !errors.Is(err, ErrBootstrap) {
			t.Fatalf("unsafe mode err=%v", err)
		}
	})
	t.Run("symlink", func(t *testing.T) {
		target := filepath.Join(parent, "target")
		if err := os.Mkdir(target, 0o700); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(parent, "link")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		config.StateDirectory = link
		if _, err := Bootstrap(&bytes.Buffer{}, config); !errors.Is(err, ErrBootstrap) {
			t.Fatalf("symlink err=%v", err)
		}
	})
}
