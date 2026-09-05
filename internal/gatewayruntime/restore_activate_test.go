package gatewayruntime

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibejson"
)

func TestGatewayRestoreAuthorityDirectoryRejectsPublicMode(t *testing.T) {
	root := filepath.Join(t.TempDir(), "authority")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ensureGatewayRestoreDirectory(root); !errors.Is(err, gateway.ErrRestoreActivation) ||
		!strings.Contains(err.Error(), "private restore authority directory") {
		t.Fatalf("public authority directory must fail with its exact stage: %v", err)
	}
	info, err := os.Stat(root)
	if err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("rejection must not silently repair existing permissions: info=%v err=%v", info, err)
	}
	private, err := os.MkdirTemp(t.TempDir(), "restore-authority-")
	if err != nil {
		t.Fatal(err)
	}
	if err = ensureGatewayRestoreDirectory(private); err != nil {
		t.Fatalf("MkdirTemp private fixture rejected: %v", err)
	}
}

func gatewayRestoreTestManifest(root string) gatewayRestoreManifest {
	path := func(name string) string { return filepath.Join(root, name) }
	return gatewayRestoreManifest{Format: 1, Operation: path("operation"), SchemaSet: path("schema-set.vibejson"),
		StagingRoot: path("staging"), ActivationRoot: path("activation"), TargetCatalog: path("catalog.vibejson"), Policy: path("policy.vibejson"),
		TLS: gatewayRestoreTLS{Certificate: path("cert.pem"), Key: path("key.pem"), Roots: path("roots.pem"), IdentityOID: "1.2.3.4"},
		Sessions: [2]gatewayRestoreSession{
			{ClientID: strings.Repeat("11", 16), RetryHome: strings.Repeat("21", 8), Journal: path("session-1")},
			{ClientID: strings.Repeat("12", 16), RetryHome: strings.Repeat("22", 8), Journal: path("session-2")},
		}, Groups: []gatewayRestoreGroup{{Ordinal: 0, Root: path("roots"), ControlAddresses: [3]string{"127.0.0.1:7711", "127.0.0.1:7712", "127.0.0.1:7713"}}},
		Repository: gatewayRestoreRepository{MaxArtifacts: 3, MaxArtifactBytes: 1 << 20, MaxDiskBytes: 4 << 20},
		TimeoutMS:  10000, AttemptTimeoutMS: 1000, SessionLeaseMS: 5000, Attempts: 3, MaxConnections: 8,
	}
}

func TestGatewayRestoreManifestCanonicalAndBounded(t *testing.T) {
	manifest := gatewayRestoreTestManifest(t.TempDir())
	raw, err := vibejson.Marshal(&manifest)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseGatewayRestoreManifest(raw)
	if err != nil || parsed.SchemaSet != manifest.SchemaSet || len(parsed.Groups) != 1 {
		t.Fatalf("parsed=%+v err=%v", parsed, err)
	}
	for _, malformed := range [][]byte{nil, append(append([]byte(nil), raw...), '\n'),
		bytes.Replace(raw, []byte(`"format":1`), []byte(`"format":1,"format":1`), 1),
		bytes.Repeat([]byte{' '}, maxGatewayRestoreManifestBytes+1)} {
		if _, err := parseGatewayRestoreManifest(malformed); err == nil {
			t.Fatal("malformed manifest accepted")
		}
	}
	for _, mutate := range []func(*gatewayRestoreManifest){
		func(m *gatewayRestoreManifest) { m.Groups[0].Ordinal = 1 },
		func(m *gatewayRestoreManifest) { m.Sessions[1].ClientID = m.Sessions[0].ClientID },
		func(m *gatewayRestoreManifest) { m.Groups[0].ControlAddresses[0] = "localhost:0" },
		func(m *gatewayRestoreManifest) { m.SchemaSet = "relative" },
		func(m *gatewayRestoreManifest) { m.ActivationRoot = m.StagingRoot },
		func(m *gatewayRestoreManifest) { m.SessionLeaseMS = m.AttemptTimeoutMS },
	} {
		candidate := gatewayRestoreTestManifest(t.TempDir())
		mutate(&candidate)
		encoded, err := vibejson.Marshal(&candidate)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = parseGatewayRestoreManifest(encoded); err == nil {
			t.Fatal("invalid identity/bound accepted")
		}
	}
}

func TestGatewayRestoreInputRejectsSymlinksAndOversize(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "input")
	if err := os.WriteFile(path, []byte("exact"), 0o600); err != nil {
		t.Fatal(err)
	}
	if raw, err := readGatewayRestoreInput(path, 5); err != nil || string(raw) != "exact" {
		t.Fatalf("raw=%q err=%v", raw, err)
	}
	if _, err := readGatewayRestoreInput(path, 4); err == nil {
		t.Fatal("oversized input accepted")
	}
	link := filepath.Join(root, "symlink")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readGatewayRestoreInput(link, 5); err == nil {
		t.Fatal("symlink input accepted")
	}
}

func TestGatewayRestoreCatalogRejectsIncompleteComposition(t *testing.T) {
	if catalog, pool, err := newGatewayRestoreCatalog(context.Background(), gatewayRestoreManifest{}, clusterrestore.Operation{}, nil, nil, nil, serviceauthz.Authority{}); err == nil || catalog != nil || pool != nil {
		t.Fatal("incomplete catalog composition accepted")
	}
	if _, err := (gatewayRestoreGroupInstaller{}).Install(context.Background(), clusterrestore.Operation{}, 0, bytes.NewReader(nil)); err == nil {
		t.Fatal("missing group installer accepted")
	}
	if status := runRestoreActivate(nil); status != 2 {
		t.Fatalf("missing manifest status=%d", status)
	}
}
