package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/clustercontrol"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibejson"
)

func writeTestScaleCA(t *testing.T, dir string) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ca := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test Scale CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(cryptorand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	caCert := filepath.Join(dir, "ca-cert.pem")
	caKey := filepath.Join(dir, "ca-key.pem")
	if err := os.WriteFile(caCert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caKey, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return caCert, caKey
}

func TestClusterInitOperatorAndInitNodeRoundTrip(t *testing.T) {
	root := t.TempDir()
	caCert, caKey := writeTestScaleCA(t, root)

	clusterID := "0102030405060708090a0b0c0d0e0f10"
	clusterInc := "100f0e0d0c0b0a090807060504030201"
	serverNode := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	// Seed an initial authorization policy with one existing principal.
	policyPath := filepath.Join(root, "authorization-policy.vibejson")
	initialPolicy := devPolicy{
		Generation: 1,
		Principals: []devPrincipal{
			{Node: serverNode, Capabilities: []string{"data_read", "data_write", "topology"}},
		},
	}
	rawPolicy, err := vibejson.Marshal(&initialPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(policyPath, rawPolicy, 0o600); err != nil {
		t.Fatal(err)
	}

	// 1. Generate operator credential + profile + update policy.
	opDir := filepath.Join(root, "operator")
	code := run([]string{
		"vibedb", "cluster", "init-operator",
		"--ca-cert", caCert,
		"--ca-key", caKey,
		"--cluster-id", clusterID,
		"--cluster-incarnation", clusterInc,
		"--address", "127.0.0.1:17400",
		"--server-node", serverNode,
		"--out-dir", opDir,
		"--policy", policyPath,
		"--update-policy",
	})
	if code != 0 {
		t.Fatalf("init-operator exited with %d", code)
	}

	profilePath := filepath.Join(opDir, "operator-profile.vibejson")
	profile, err := clustercontrol.LoadProfile(profilePath)
	if err != nil {
		t.Fatalf("LoadProfile: %v", err)
	}
	if profile.Address != "127.0.0.1:17400" || profile.ServerNode != serverNode {
		t.Fatalf("unexpected profile: %+v", profile)
	}
	tlsProfile, err := servicetls.LoadProfile(profile.Certificate, profile.Key, profile.Roots, profile.IdentityOID, time.Now)
	if err != nil {
		t.Fatalf("servicetls.LoadProfile(operator): %v", err)
	}
	gotClusterID := tlsProfile.LocalIdentity().TrustDomain.ClusterID
	if hex.EncodeToString(gotClusterID[:]) != clusterID {
		t.Fatalf("operator trust domain mismatch: %x", gotClusterID)
	}

	// 2. Generate empty node preparation manifest + public descriptor using --trust-domain-from.
	nodeOutDir := filepath.Join(root, "node4-inputs")
	nodeDataRoot := filepath.Join(root, "node4-data")
	code = run([]string{
		"vibedb", "cluster", "init-node",
		"--ca-cert", caCert,
		"--ca-key", caKey,
		"--trust-domain-from", profile.Certificate,
		"--root", nodeDataRoot,
		"--out-dir", nodeOutDir,
		"--policy", policyPath,
		"--update-policy",
		"--failure-domain", "rack-4",
		"--peer", "127.0.0.1:21001",
		"--native", "127.0.0.1:21002",
		"--snapshot", "127.0.0.1:21003",
		"--control", "127.0.0.1:21004",
		"--grant-node", serverNode,
	})
	if code != 0 {
		t.Fatalf("init-node exited with %d", code)
	}

	descPath := filepath.Join(nodeOutDir, "node-descriptor.vibejson")
	desc, err := clustercontrol.LoadNodeDescriptor(descPath)
	if err != nil {
		t.Fatalf("LoadNodeDescriptor: %v", err)
	}
	if desc.FailureDomain != "rack-4" || desc.Incarnation != 1 || desc.DataAddress != "127.0.0.1:21001" {
		t.Fatalf("unexpected node descriptor: %+v", desc)
	}
	nodeTLS, err := servicetls.LoadProfile(
		filepath.Join(nodeOutDir, "node-cert.pem"),
		filepath.Join(nodeOutDir, "node-key.pem"),
		caCert,
		devClusterOID,
		time.Now,
	)
	if err != nil {
		t.Fatalf("servicetls.LoadProfile(node): %v", err)
	}
	spkiDigest := nodeTLS.LocalServiceKeyDigest()
	wantPin := hex.EncodeToString(spkiDigest[:])
	if desc.ServiceKeyDigest != wantPin {
		t.Fatalf("service_key_digest=%s want=%s", desc.ServiceKeyDigest, wantPin)
	}

	// Verify prepare-node.vibejson is canonical and references the generated node-log.key.
	prepRaw, err := os.ReadFile(filepath.Join(nodeOutDir, "prepare-node.vibejson"))
	if err != nil {
		t.Fatalf("read prepare-node.vibejson: %v", err)
	}
	var prep scaleEmptyNodeManifest
	if err := vibejson.Unmarshal(prepRaw, &prep); err != nil {
		t.Fatalf("unmarshal prepare-node.vibejson: %v", err)
	}
	if prep.Root != nodeDataRoot || prep.Services.NodeIncarnation != 1 || len(prep.Groups) != 0 {
		t.Fatalf("unexpected prepare manifest: %+v", prep)
	}
	keyMat, err := os.ReadFile(prep.NodeLog.KeyMaterialPath)
	if err != nil || len(keyMat) != 32 {
		t.Fatalf("node-log.key len=%d err=%v", len(keyMat), err)
	}

	// Verify policy was bumped twice (generation 1 -> 3) and loads cleanly.
	updatedPolicyRaw, err := os.ReadFile(policyPath)
	if err != nil {
		t.Fatalf("read updated policy: %v", err)
	}
	loadedPolicy, err := serviceauthz.Load(updatedPolicyRaw)
	if err != nil {
		t.Fatalf("serviceauthz.Load: %v", err)
	}
	if loadedPolicy.Generation() != 3 {
		t.Fatalf("policy generation=%d want=3", loadedPolicy.Generation())
	}
	opID := tlsProfile.LocalIdentity().Node
	if loadedPolicy.Check(opID, serviceauthz.CapabilityMembership) != serviceauthz.DecisionAllow ||
		loadedPolicy.Check(opID, serviceauthz.CapabilityTopology) != serviceauthz.DecisionAllow {
		t.Fatal("operator principal missing membership/topology capabilities")
	}
	nodeID := nodeTLS.LocalIdentity().Node
	if loadedPolicy.Check(nodeID, serviceauthz.CapabilityDataRead) != serviceauthz.DecisionAllow ||
		loadedPolicy.Check(nodeID, serviceauthz.CapabilityDataWrite) != serviceauthz.DecisionAllow {
		t.Fatal("storage node principal missing data_read/data_write capabilities")
	}
}

func TestClusterInitRejectsInvalidArgs(t *testing.T) {
	if code := run([]string{"vibedb", "cluster", "init-operator"}); code != 2 {
		t.Fatalf("expected exit 2 on missing flags, got %d", code)
	}
	if code := run([]string{"vibedb", "cluster", "init-node"}); code != 2 {
		t.Fatalf("expected exit 2 on missing flags, got %d", code)
	}
}
