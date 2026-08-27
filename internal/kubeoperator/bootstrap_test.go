package kubeoperator

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibejson"
)

type failingBootstrapReader struct{}

func (failingBootstrapReader) Read([]byte) (int, error) { return 0, errors.New("entropy unavailable") }

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
