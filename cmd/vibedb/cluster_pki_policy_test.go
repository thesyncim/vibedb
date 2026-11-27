package main

import (
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/clustercontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibejson"
)

func writeTestPolicyFile(t *testing.T, dir string, seedNode string) string {
	t.Helper()
	policyPath := filepath.Join(dir, "authorization-policy.vibejson")
	raw, err := vibejson.Marshal(&devPolicy{
		Generation: 1,
		Principals: []devPrincipal{{
			Node:         seedNode,
			Capabilities: []string{"data_read", "data_write", "topology"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(policyPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return policyPath
}

func TestClusterRotateCertPreservesIdentityAndUpdatesDescriptor(t *testing.T) {
	dir := t.TempDir()
	caCert, caKey := writeTestScaleCA(t, dir)
	clusterID := "0102030405060708090a0b0c0d0e0f10"
	clusterInc := "1112131415161718191a1b1c1d1e1f20"
	serverNode := "2122232425262728292a2b2c2d2e2f30"
	policyPath := writeTestPolicyFile(t, dir, serverNode)

	nodeOutDir := filepath.Join(dir, "node4-inputs")
	nodeDataRoot := filepath.Join(dir, "node4-data")
	code := run([]string{
		"vibedb", "cluster", "init-node",
		"--ca-cert", caCert,
		"--ca-key", caKey,
		"--cluster-id", clusterID,
		"--cluster-incarnation", clusterInc,
		"--root", nodeDataRoot,
		"--out-dir", nodeOutDir,
		"--policy", policyPath,
		"--update-policy",
		"--failure-domain", "rack-4",
		"--peer", "127.0.0.1:28001",
		"--native", "127.0.0.1:28002",
		"--snapshot", "127.0.0.1:28003",
		"--control", "127.0.0.1:28004",
		"--grant-node", serverNode,
	})
	if code != 0 {
		t.Fatalf("init-node exited with %d", code)
	}

	certPath := filepath.Join(nodeOutDir, "node-cert.pem")
	keyPath := filepath.Join(nodeOutDir, "node-key.pem")
	descPath := filepath.Join(nodeOutDir, "node-descriptor.vibejson")

	beforeDesc, err := clustercontrol.LoadNodeDescriptor(descPath)
	if err != nil {
		t.Fatal(err)
	}
	beforePEM, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatal(err)
	}
	beforeBlock, _ := pem.Decode(beforePEM)
	beforeCert, err := x509.ParseCertificate(beforeBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	code = run([]string{
		"vibedb", "cluster", "rotate-cert",
		"--ca-cert", caCert,
		"--ca-key", caKey,
		"--cert", certPath,
		"--key", keyPath,
		"--node-descriptor", descPath,
		"--valid-for", "48h",
	})
	if code != 0 {
		t.Fatalf("rotate-cert exited with %d", code)
	}

	afterDesc, err := clustercontrol.LoadNodeDescriptor(descPath)
	if err != nil {
		t.Fatal(err)
	}
	afterPEM, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatal(err)
	}
	afterBlock, _ := pem.Decode(afterPEM)
	afterCert, err := x509.ParseCertificate(afterBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	// Serial and SPKI must rotate, while NodeID and TrustDomain stay identical,
	// and the rotated cert+key must load cleanly via servicetls.LoadProfile.
	if beforeCert.SerialNumber.Cmp(afterCert.SerialNumber) == 0 {
		t.Fatal("rotate-cert reused the same serial number")
	}
	if beforeDesc.ServiceKeyDigest == afterDesc.ServiceKeyDigest {
		t.Fatal("rotate-cert did not update node-descriptor SPKI digest")
	}
	if beforeDesc.NodeID != afterDesc.NodeID {
		t.Fatalf("rotate-cert changed node_id: %s -> %s", beforeDesc.NodeID, afterDesc.NodeID)
	}
	oid, err := parseScaleOID(devClusterOID)
	if err != nil {
		t.Fatal(err)
	}
	beforeID, err := rafttransport.ParsePeerIdentity(oid, beforeCert)
	if err != nil {
		t.Fatal(err)
	}
	afterID, err := rafttransport.ParsePeerIdentity(oid, afterCert)
	if err != nil {
		t.Fatal(err)
	}
	if beforeID != afterID {
		t.Fatalf("rotate-cert changed peer identity: %+v -> %+v", beforeID, afterID)
	}
	if _, err := servicetls.LoadProfile(certPath, keyPath, caCert, devClusterOID, time.Now); err != nil {
		t.Fatalf("servicetls.LoadProfile(rotated): %v", err)
	}
}

func TestClusterPolicyShowGrantRevoke(t *testing.T) {
	dir := t.TempDir()
	seedNode := "2122232425262728292a2b2c2d2e2f30"
	policyPath := writeTestPolicyFile(t, dir, seedNode)
	var principal rafttransport.NodeID
	principal[0] = 0x77
	principalHex := hex.EncodeToString(principal[:])

	// 1. Grant data_read + topology to new principal.
	if code := run([]string{
		"vibedb", "cluster", "policy", "grant",
		"--policy", policyPath,
		"--node-id", principalHex,
		"--capabilities", "data_read,topology",
	}); code != 0 {
		t.Fatalf("policy grant exited with %d", code)
	}
	raw, err := os.ReadFile(policyPath)
	if err != nil {
		t.Fatal(err)
	}
	pol, err := serviceauthz.Load(raw)
	if err != nil {
		t.Fatal(err)
	}
	if pol.Check(principal, serviceauthz.CapabilityDataRead) != serviceauthz.DecisionAllow ||
		pol.Check(principal, serviceauthz.CapabilityTopology) != serviceauthz.DecisionAllow ||
		pol.Check(principal, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow {
		t.Fatal("policy grant capabilities mismatch")
	}

	// 2. Show must succeed.
	if code := run([]string{"vibedb", "cluster", "policy", "show", "--policy", policyPath}); code != 0 {
		t.Fatalf("policy show exited with %d", code)
	}

	// 3. Revoke topology, keeping data_read.
	if code := run([]string{
		"vibedb", "cluster", "policy", "revoke",
		"--policy", policyPath,
		"--node-id", principalHex,
		"--capabilities", "topology",
	}); code != 0 {
		t.Fatalf("policy revoke exited with %d", code)
	}
	raw, _ = os.ReadFile(policyPath)
	pol, err = serviceauthz.Load(raw)
	if err != nil {
		t.Fatal(err)
	}
	if pol.Check(principal, serviceauthz.CapabilityDataRead) != serviceauthz.DecisionAllow ||
		pol.Check(principal, serviceauthz.CapabilityTopology) == serviceauthz.DecisionAllow {
		t.Fatal("policy revoke did not remove topology while retaining data_read")
	}

	// 4. Revoke --all removes principal completely.
	if code := run([]string{
		"vibedb", "cluster", "policy", "revoke",
		"--policy", policyPath,
		"--node-id", principalHex,
		"--all",
	}); code != 0 {
		t.Fatalf("policy revoke --all exited with %d", code)
	}
	raw, _ = os.ReadFile(policyPath)
	pol, err = serviceauthz.Load(raw)
	if err != nil {
		t.Fatal(err)
	}
	if pol.Check(principal, serviceauthz.CapabilityDataRead) == serviceauthz.DecisionAllow {
		t.Fatal("policy revoke --all did not remove principal")
	}
}
