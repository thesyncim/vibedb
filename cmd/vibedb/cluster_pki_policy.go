package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/thesyncim/vibedb/internal/clustercontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibejson"
)

func runClusterRotateCert(args []string) int {
	flags := flag.NewFlagSet("cluster rotate-cert", flag.ContinueOnError)
	var (
		caCertPath     string
		caKeyPath      string
		certPath       string
		keyPath        string
		identityOID    string
		validFor       time.Duration
		descriptorPath string
	)
	flags.StringVar(&caCertPath, "ca-cert", "", "path to cluster CA certificate PEM")
	flags.StringVar(&caKeyPath, "ca-key", "", "path to cluster CA private key PEM")
	flags.StringVar(&certPath, "cert", "", "path to existing leaf certificate PEM to rotate in place")
	flags.StringVar(&keyPath, "key", "", "path to existing leaf private key PEM to rotate in place")
	flags.StringVar(&identityOID, "identity-oid", devClusterOID, "VibeDB peer identity OID")
	flags.DurationVar(&validFor, "valid-for", 365*24*time.Hour, "validity duration for the rotated certificate")
	flags.StringVar(&descriptorPath, "node-descriptor", "", "optional public node-descriptor.vibejson to update with the new SPKI SHA-256 digest")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || caCertPath == "" || caKeyPath == "" || certPath == "" || keyPath == "" || validFor <= 0 {
		fmt.Fprintln(os.Stderr, "usage: vibedb cluster rotate-cert --ca-cert <pem> --ca-key <pem> --cert <pem> --key <pem> [--identity-oid <oid>] [--valid-for 8760h] [--node-descriptor <path>]")
		return 2
	}
	oid, err := parseScaleOID(identityOID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster rotate-cert: --identity-oid: %v\n", err)
		return 2
	}
	spkiDigest, identity, err := rotateLeafCertificate(caCertPath, caKeyPath, certPath, keyPath, oid, validFor)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster rotate-cert: %v\n", err)
		return 1
	}
	if descriptorPath != "" {
		if err := updateNodeDescriptorSPKI(descriptorPath, spkiDigest); err != nil {
			fmt.Fprintf(os.Stderr, "cluster rotate-cert: update node descriptor: %v\n", err)
			return 1
		}
	}
	fmt.Fprintf(os.Stdout, "node_id=%s spki_sha256=%s cert=%s key=%s\n",
		hex.EncodeToString(identity.Node[:]),
		hex.EncodeToString(spkiDigest[:]),
		certPath, keyPath)
	return 0
}

func rotateLeafCertificate(
	caCertPath, caKeyPath, certPath, keyPath string,
	oid []int,
	validity time.Duration,
) (replication.Digest, rafttransport.PeerIdentity, error) {
	oldCertPEM, err := os.ReadFile(certPath)
	if err != nil || len(oldCertPEM) == 0 || len(oldCertPEM) > 1<<20 {
		return replication.Digest{}, rafttransport.PeerIdentity{}, fmt.Errorf("read existing cert: %w", err)
	}
	oldBlock, _ := pem.Decode(oldCertPEM)
	if oldBlock == nil || oldBlock.Type != "CERTIFICATE" {
		return replication.Digest{}, rafttransport.PeerIdentity{}, errors.New("existing cert does not begin with a PEM CERTIFICATE block")
	}
	oldCert, err := x509.ParseCertificate(oldBlock.Bytes)
	if err != nil {
		return replication.Digest{}, rafttransport.PeerIdentity{}, fmt.Errorf("parse existing cert: %w", err)
	}
	identity, err := rafttransport.ParsePeerIdentity(oid, oldCert)
	if err != nil {
		return replication.Digest{}, rafttransport.PeerIdentity{}, fmt.Errorf("extract VibeDB identity from existing cert: %w", err)
	}

	caCertPEM, err := os.ReadFile(caCertPath)
	if err != nil || len(caCertPEM) == 0 || len(caCertPEM) > 1<<20 {
		return replication.Digest{}, rafttransport.PeerIdentity{}, fmt.Errorf("read ca-cert: %w", err)
	}
	caBlock, rest := pem.Decode(caCertPEM)
	if caBlock == nil || caBlock.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return replication.Digest{}, rafttransport.PeerIdentity{}, errors.New("ca-cert must contain exactly one PEM CERTIFICATE block")
	}
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil || !caCert.IsCA || !caCert.BasicConstraintsValid {
		return replication.Digest{}, rafttransport.PeerIdentity{}, errors.New("ca-cert is not a valid CA certificate")
	}
	caKeyPEM, err := os.ReadFile(caKeyPath)
	if err != nil || len(caKeyPEM) == 0 || len(caKeyPEM) > 1<<20 {
		return replication.Digest{}, rafttransport.PeerIdentity{}, fmt.Errorf("read ca-key: %w", err)
	}
	keyBlock, keyRest := pem.Decode(caKeyPEM)
	if keyBlock == nil || len(bytes.TrimSpace(keyRest)) != 0 {
		return replication.Digest{}, rafttransport.PeerIdentity{}, errors.New("ca-key must contain exactly one PEM key block")
	}
	caKey, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		parsed, parseErr := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
		if parseErr != nil {
			return replication.Digest{}, rafttransport.PeerIdentity{}, fmt.Errorf("parse ca-key: %w", parseErr)
		}
		var ok bool
		caKey, ok = parsed.(*ecdsa.PrivateKey)
		if !ok {
			return replication.Digest{}, rafttransport.PeerIdentity{}, errors.New("ca-key must be an ECDSA private key")
		}
	}
	caPub, ok := caCert.PublicKey.(*ecdsa.PublicKey)
	if !ok || !caKey.PublicKey.Equal(caPub) {
		return replication.Digest{}, rafttransport.PeerIdentity{}, errors.New("ca-key does not match ca-cert public key")
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		return replication.Digest{}, rafttransport.PeerIdentity{}, err
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 120)
	serial, err := cryptorand.Int(cryptorand.Reader, serialLimit)
	if err != nil {
		return replication.Digest{}, rafttransport.PeerIdentity{}, err
	}
	serial.Add(serial, big.NewInt(2))
	ext, err := rafttransport.PeerIdentityExtension(oid, identity)
	if err != nil {
		return replication.Digest{}, rafttransport.PeerIdentity{}, err
	}
	now := time.Now().UTC()
	tmpl := &x509.Certificate{
		SerialNumber:    serial,
		Subject:         oldCert.Subject,
		DNSNames:        append([]string(nil), oldCert.DNSNames...),
		IPAddresses:     append([]net.IP(nil), oldCert.IPAddresses...),
		NotBefore:       now.Add(-time.Minute),
		NotAfter:        now.Add(validity),
		KeyUsage:        x509.KeyUsageDigitalSignature,
		ExtKeyUsage:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		ExtraExtensions: []pkix.Extension{ext},
	}
	leafDER, err := x509.CreateCertificate(cryptorand.Reader, tmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return replication.Digest{}, rafttransport.PeerIdentity{}, err
	}
	// Verify the rotated certificate chains to the CA and preserves exact identity
	// before touching any file on disk.
	newCert, err := x509.ParseCertificate(leafDER)
	if err != nil {
		return replication.Digest{}, rafttransport.PeerIdentity{}, err
	}
	if err := newCert.CheckSignatureFrom(caCert); err != nil {
		return replication.Digest{}, rafttransport.PeerIdentity{}, fmt.Errorf("verify rotated cert signature: %w", err)
	}
	gotID, err := rafttransport.ParsePeerIdentity(oid, newCert)
	if err != nil || gotID != identity {
		return replication.Digest{}, rafttransport.PeerIdentity{}, errors.New("rotated cert identity verification failed")
	}

	leafPEM := append(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caBlock.Bytes})...,
	)
	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		return replication.Digest{}, rafttransport.PeerIdentity{}, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyDER})

	if err := writeAtomicSynced(keyPath, keyPEM, 0o600); err != nil {
		return replication.Digest{}, rafttransport.PeerIdentity{}, fmt.Errorf("write rotated key: %w", err)
	}
	if err := writeAtomicSynced(certPath, leafPEM, 0o600); err != nil {
		return replication.Digest{}, rafttransport.PeerIdentity{}, fmt.Errorf("write rotated cert: %w", err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&leafKey.PublicKey)
	if err != nil {
		return replication.Digest{}, rafttransport.PeerIdentity{}, err
	}
	return replication.Digest(sha256.Sum256(pubDER)), identity, nil
}

func updateNodeDescriptorSPKI(descriptorPath string, spki replication.Digest) error {
	desc, err := clustercontrol.LoadNodeDescriptor(descriptorPath)
	if err != nil {
		return err
	}
	desc.ServiceKeyDigest = hex.EncodeToString(spki[:])
	encoded, err := vibejson.Marshal(&desc)
	if err != nil {
		return err
	}
	if err := writeAtomicSynced(descriptorPath, encoded, 0o644); err != nil {
		return err
	}
	_, err = clustercontrol.LoadNodeDescriptor(descriptorPath)
	return err
}

func writeAtomicSynced(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".vibedb-atomic-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	if err := tmp.Chmod(perm); err != nil {
		cleanup()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

func runClusterPolicy(args []string) int {
	if len(args) == 0 {
		printClusterPolicyUsage()
		return 2
	}
	switch args[0] {
	case "show":
		return runClusterPolicyShow(args[1:])
	case "grant":
		return runClusterPolicyGrant(args[1:])
	case "revoke":
		return runClusterPolicyRevoke(args[1:])
	default:
		printClusterPolicyUsage()
		return 2
	}
}

func printClusterPolicyUsage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  vibedb cluster policy show --policy <path>")
	fmt.Fprintln(os.Stderr, "  vibedb cluster policy grant --policy <path> --node-id <hex> --capabilities <csv>")
	fmt.Fprintln(os.Stderr, "  vibedb cluster policy revoke --policy <path> --node-id <hex> (--capabilities <csv> | --all)")
}

func runClusterPolicyShow(args []string) int {
	flags := flag.NewFlagSet("cluster policy show", flag.ContinueOnError)
	var policyPath string
	flags.StringVar(&policyPath, "policy", "", "path to authorization-policy.vibejson")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || policyPath == "" {
		printClusterPolicyUsage()
		return 2
	}
	raw, err := os.ReadFile(policyPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster policy show: %v\n", err)
		return 1
	}
	if _, err := serviceauthz.Load(raw); err != nil {
		fmt.Fprintf(os.Stderr, "cluster policy show: invalid policy: %v\n", err)
		return 1
	}
	var policy devPolicy
	if err := vibejson.Unmarshal(raw, &policy); err != nil {
		fmt.Fprintf(os.Stderr, "cluster policy show: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stdout, "generation=%d principals=%d\n", policy.Generation, len(policy.Principals))
	for _, p := range policy.Principals {
		caps := append([]string(nil), p.Capabilities...)
		sort.Strings(caps)
		fmt.Fprintf(os.Stdout, "%s %s\n", p.Node, strings.Join(caps, ","))
	}
	return 0
}

func runClusterPolicyGrant(args []string) int {
	flags := flag.NewFlagSet("cluster policy grant", flag.ContinueOnError)
	var (
		policyPath string
		nodeIDHex  string
		capsList   scaleInitStringList
	)
	flags.StringVar(&policyPath, "policy", "", "path to authorization-policy.vibejson")
	flags.StringVar(&nodeIDHex, "node-id", "", "64-hex node_id principal")
	flags.Var(&capsList, "capabilities", "comma-separated capability names to grant")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || policyPath == "" || nodeIDHex == "" || len(capsList) == 0 {
		printClusterPolicyUsage()
		return 2
	}
	node, err := parseScaleNodeID(nodeIDHex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster policy grant: --node-id: %v\n", err)
		return 2
	}
	gen, err := mutatePolicyPrincipal(policyPath, node, capsList, nil, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster policy grant: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stdout, "generation=%d node_id=%s\n", gen, hex.EncodeToString(node[:]))
	return 0
}

func runClusterPolicyRevoke(args []string) int {
	flags := flag.NewFlagSet("cluster policy revoke", flag.ContinueOnError)
	var (
		policyPath string
		nodeIDHex  string
		capsList   scaleInitStringList
		revokeAll  bool
	)
	flags.StringVar(&policyPath, "policy", "", "path to authorization-policy.vibejson")
	flags.StringVar(&nodeIDHex, "node-id", "", "64-hex node_id principal")
	flags.Var(&capsList, "capabilities", "comma-separated capability names to revoke")
	flags.BoolVar(&revokeAll, "all", false, "remove the principal entirely from the policy")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || policyPath == "" || nodeIDHex == "" || (!revokeAll && len(capsList) == 0) {
		printClusterPolicyUsage()
		return 2
	}
	node, err := parseScaleNodeID(nodeIDHex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster policy revoke: --node-id: %v\n", err)
		return 2
	}
	gen, err := mutatePolicyPrincipal(policyPath, node, nil, capsList, revokeAll)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster policy revoke: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stdout, "generation=%d node_id=%s\n", gen, hex.EncodeToString(node[:]))
	return 0
}

func mutatePolicyPrincipal(
	policyPath string,
	node rafttransport.NodeID,
	grant []string,
	revoke []string,
	revokeAll bool,
) (uint64, error) {
	raw, err := os.ReadFile(policyPath)
	if err != nil {
		return 0, err
	}
	if _, err := serviceauthz.Load(raw); err != nil {
		return 0, fmt.Errorf("existing policy invalid: %w", err)
	}
	var policy devPolicy
	if err := vibejson.Unmarshal(raw, &policy); err != nil {
		return 0, err
	}
	nodeHex := hex.EncodeToString(node[:])
	idx := -1
	for i, existing := range policy.Principals {
		if existing.Node == nodeHex {
			idx = i
			break
		}
	}
	if revokeAll {
		if idx < 0 {
			return 0, fmt.Errorf("principal %s not found in %s", nodeHex, policyPath)
		}
		policy.Principals = append(policy.Principals[:idx], policy.Principals[idx+1:]...)
	} else if len(revoke) > 0 {
		if idx < 0 {
			return 0, fmt.Errorf("principal %s not found in %s", nodeHex, policyPath)
		}
		drop := make(map[string]struct{}, len(revoke))
		for _, c := range revoke {
			drop[c] = struct{}{}
		}
		var kept []string
		for _, c := range policy.Principals[idx].Capabilities {
			if _, removed := drop[c]; !removed {
				kept = append(kept, c)
			}
		}
		if len(kept) == 0 {
			policy.Principals = append(policy.Principals[:idx], policy.Principals[idx+1:]...)
		} else {
			sort.Strings(kept)
			policy.Principals[idx].Capabilities = kept
		}
	} else if len(grant) > 0 {
		capSet := make(map[string]struct{}, len(grant))
		if idx >= 0 {
			for _, c := range policy.Principals[idx].Capabilities {
				capSet[c] = struct{}{}
			}
		}
		for _, c := range grant {
			capSet[c] = struct{}{}
		}
		merged := make([]string, 0, len(capSet))
		for c := range capSet {
			merged = append(merged, c)
		}
		sort.Strings(merged)
		if idx >= 0 {
			policy.Principals[idx].Capabilities = merged
		} else {
			policy.Principals = append(policy.Principals, devPrincipal{
				Node:         nodeHex,
				Capabilities: merged,
			})
		}
	}
	sort.Slice(policy.Principals, func(i, j int) bool {
		return policy.Principals[i].Node < policy.Principals[j].Node
	})
	policy.Generation++
	updated, err := vibejson.Marshal(&policy)
	if err != nil {
		return 0, err
	}
	if _, err := serviceauthz.Load(updated); err != nil {
		return 0, fmt.Errorf("updated policy rejected by serviceauthz: %w", err)
	}
	if err := writeAtomicSynced(policyPath, updated, 0o600); err != nil {
		return 0, err
	}
	return policy.Generation, nil
}
