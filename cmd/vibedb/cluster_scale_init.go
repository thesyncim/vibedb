package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/thesyncim/vibedb/internal/clustercontrol"
	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibejson"
)

type scaleInitStringList []string

func (l *scaleInitStringList) String() string { return strings.Join(*l, ",") }
func (l *scaleInitStringList) Set(value string) error {
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			*l = append(*l, part)
		}
	}
	return nil
}

type scaleEmptyNodeManifest struct {
	Root     string                 `json:"root"`
	NodeLog  scaleEmptyNodeLog      `json:"node_log"`
	Services scaleEmptyNodeServices `json:"services"`
	Groups   []struct{}             `json:"groups"`
}

type scaleEmptyNodeLog struct {
	Format          uint16                     `json:"format"`
	Path            string                     `json:"path"`
	KeyID           string                     `json:"key_id"`
	WrappedKey      string                     `json:"wrapped_key"`
	KeyMaterialPath string                     `json:"key_material_path"`
	Options         raftstore.NodeStoreOptions `json:"options"`
}

type scaleEmptyNodeServices struct {
	NodeIncarnation      uint64                             `json:"node_incarnation"`
	Listeners            devPrepareListeners                `json:"listeners"`
	TLS                  devPrepareTLS                      `json:"tls"`
	AuthorizationPolicy  string                             `json:"authorization_policy"`
	GatewaySeeds         []nodecontrol.BootstrapGatewaySeed `json:"bootstrap_gateway_seeds"`
	CanonicalSourceSeeds []nodecontrol.BootstrapGatewaySeed `json:"canonical_source_seeds,omitempty"`
	ReplicaControl       scaleEmptyNodeReplicaControl       `json:"replica_control"`
	SplitControl         scaleEmptyNodeSplitControl         `json:"split_control"`
}

type scaleEmptyNodeReplicaControl struct {
	ActionJournalPath      string                  `json:"action_journal_path"`
	MaxActionRecords       int                     `json:"max_action_records"`
	SourceDataRoot         string                  `json:"source_data_root"`
	SourceJournalPath      string                  `json:"source_journal_path"`
	MaxSourceRecords       int                     `json:"max_source_records"`
	SourceRepositoryPath   string                  `json:"source_repository_path"`
	MaxSourceArtifacts     int                     `json:"max_source_artifacts"`
	MaxSourceConcurrent    int                     `json:"max_source_concurrent"`
	MaxSourceArtifactBytes int64                   `json:"max_source_artifact_bytes"`
	MaxSourceDiskBytes     int64                   `json:"max_source_disk_bytes"`
	SourceChunkBytes       int                     `json:"source_chunk_bytes"`
	Migration              scaleEmptyNodeMigration `json:"migration"`
}

type scaleEmptyNodeMigration struct {
	MaxActive      int                     `json:"max_active"`
	CPU            scaleEmptyNodeRateLimit `json:"cpu"`
	DiskRead       scaleEmptyNodeRateLimit `json:"disk_read"`
	DiskWrite      scaleEmptyNodeRateLimit `json:"disk_write"`
	NetworkSend    scaleEmptyNodeRateLimit `json:"network_send"`
	NetworkReceive scaleEmptyNodeRateLimit `json:"network_receive"`
}

type scaleEmptyNodeRateLimit struct {
	BytesPerSecond uint64 `json:"bytes_per_second"`
	BurstBytes     uint64 `json:"burst_bytes"`
}

type scaleEmptyNodeSplitControl struct {
	JournalPath   string                `json:"journal_path"`
	MaxRecords    int                   `json:"max_records"`
	MaxFileBytes  int64                 `json:"max_file_bytes"`
	Grants        []scaleEmptyNodeGrant `json:"grants"`
	MaxOperations int                   `json:"max_operations"`
}

type scaleEmptyNodeGrant struct {
	NodeID  string `json:"node_id"`
	Actions uint16 `json:"actions"`
}

// runClusterInitOperator mints an authenticated operator leaf certificate,
// private key, and canonical clustercontrol.Profile JSON file, optionally
// adding the new operator principal (topology + membership) to the cluster's
// authorization policy.
func runClusterInitOperator(arguments []string) int {
	flags := flag.NewFlagSet("cluster init-operator", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var (
		caCert, caKey, rootsPath             string
		trustDomainFrom                      string
		clusterIDHex, clusterIncarnationHex  string
		identityOIDText                      string
		nodeIDHex                            string
		address, serverNode                  string
		outDir, policyPath                   string
		validDays                            int
		updatePolicy                         bool
	)
	flags.StringVar(&caCert, "ca-cert", "", "path to cluster CA certificate PEM")
	flags.StringVar(&caKey, "ca-key", "", "path to cluster CA ECDSA private key PEM")
	flags.StringVar(&rootsPath, "roots", "", "path to cluster roots PEM (defaults to --ca-cert)")
	flags.StringVar(&trustDomainFrom, "trust-domain-from", "", "existing cluster leaf certificate PEM to copy trust domain from")
	flags.StringVar(&clusterIDHex, "cluster-id", "", "32-hex cluster ID (required unless --trust-domain-from is set)")
	flags.StringVar(&clusterIncarnationHex, "cluster-incarnation", "", "32-hex cluster incarnation (required unless --trust-domain-from is set)")
	flags.StringVar(&identityOIDText, "identity-oid", devClusterOID, "VibeDB identity extension OID")
	flags.StringVar(&nodeIDHex, "node-id", "", "optional 32-hex operator principal node ID (random when omitted)")
	flags.StringVar(&address, "address", "", "target frontend native client listener address (host:port)")
	flags.StringVar(&serverNode, "server-node", "", "32-hex gateway node ID of the target frontend")
	flags.StringVar(&outDir, "out-dir", "", "absolute output directory for operator-cert.pem, operator-key.pem, and operator-profile.vibejson")
	flags.StringVar(&policyPath, "policy", "", "optional path to authorization-policy.vibejson")
	flags.BoolVar(&updatePolicy, "update-policy", false, "add the operator principal (membership, topology) to --policy")
	flags.IntVar(&validDays, "valid-days", 30, "certificate validity period in days")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 {
		return 2
	}
	if caCert == "" || caKey == "" || address == "" || serverNode == "" || outDir == "" || validDays <= 0 {
		fmt.Fprintln(os.Stderr, "usage: vibedb cluster init-operator --ca-cert <pem> --ca-key <pem> (--trust-domain-from <cert.pem> | --cluster-id <hex> --cluster-incarnation <hex>) --address <host:port> --server-node <hex> --out-dir <abs-path> [--roots <pem>] [--policy <path> --update-policy]")
		return 2
	}
	if rootsPath == "" {
		rootsPath = caCert
	}
	if err := validateScaleAbsPath(outDir); err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-operator: --out-dir: %v\n", err)
		return 2
	}
	if err := validateScaleAbsPath(rootsPath); err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-operator: --roots: %v\n", err)
		return 2
	}
	oid, err := parseScaleOID(identityOIDText)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-operator: --identity-oid: %v\n", err)
		return 2
	}
	domain, err := resolveScaleTrustDomain(trustDomainFrom, clusterIDHex, clusterIncarnationHex, oid)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-operator: trust domain: %v\n", err)
		return 1
	}
	nodeID, err := resolveOrRandomScaleNodeID(nodeIDHex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-operator: --node-id: %v\n", err)
		return 2
	}
	if err := os.MkdirAll(outDir, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-operator: create out-dir: %v\n", err)
		return 1
	}
	certPath := filepath.Join(outDir, "operator-cert.pem")
	keyPath := filepath.Join(outDir, "operator-key.pem")
	if _, err := mintScaleLeafCertificate(caCert, caKey, certPath, keyPath, "VibeDB cluster operator", domain, nodeID, oid, time.Duration(validDays)*24*time.Hour); err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-operator: mint certificate: %v\n", err)
		return 1
	}
	profile := clustercontrol.Profile{
		Format:      clustercontrol.Format,
		Address:     address,
		ServerNode:  serverNode,
		Certificate: certPath,
		Key:         keyPath,
		Roots:       rootsPath,
		IdentityOID: identityOIDText,
	}
	if !profile.Valid() {
		fmt.Fprintln(os.Stderr, "cluster init-operator: generated profile failed validation (check --server-node and --address)")
		return 2
	}
	profileRaw, err := vibejson.Marshal(&profile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-operator: encode profile: %v\n", err)
		return 1
	}
	profilePath := filepath.Join(outDir, "operator-profile.vibejson")
	if err := writeDevExclusive(profilePath, profileRaw, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-operator: write profile: %v\n", err)
		return 1
	}
	if _, err := clustercontrol.LoadProfile(profilePath); err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-operator: verify profile: %v\n", err)
		return 1
	}
	if updatePolicy {
		if policyPath == "" {
			fmt.Fprintln(os.Stderr, "cluster init-operator: --update-policy requires --policy")
			return 2
		}
		if err := addScalePolicyPrincipal(policyPath, nodeID, []string{"membership", "topology"}); err != nil {
			fmt.Fprintf(os.Stderr, "cluster init-operator: update policy: %v\n", err)
			return 1
		}
	}
	fmt.Fprintf(os.Stdout, "operator_node=%s\nprofile=%s\ncertificate=%s\nkey=%s\n",
		hex.EncodeToString(nodeID[:]), profilePath, certPath, keyPath)
	return 0
}

// runClusterInitNode generates the complete input set required to prepare,
// start, and join a new empty physical node: signed TLS leaf credential,
// node-log key material, canonical prepare-node-rf3 manifest, and canonical
// public node descriptor for vibedb cluster join.
func runClusterInitNode(arguments []string) int {
	flags := flag.NewFlagSet("cluster init-node", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var (
		caCert, caKey, rootsPath            string
		trustDomainFrom                     string
		clusterIDHex, clusterIncarnationHex string
		identityOIDText                     string
		nodeIDHex                           string
		nodeRoot, outDir, policyPath        string
		failureDomain                       string
		peerAddr, nativeAddr                string
		snapshotAddr, controlAddr           string
		gatewayAddr                         string
		incarnation                         uint64
		migrationCapacity                   uint64
		maxReceives                         uint
		maxGroups                           uint
		validDays                           int
		updatePolicy                        bool
		grantNodes                          scaleInitStringList
		gatewaySeeds                        scaleInitStringList
		canonicalSourceSeeds                scaleInitStringList
	)
	flags.StringVar(&caCert, "ca-cert", "", "path to cluster CA certificate PEM")
	flags.StringVar(&caKey, "ca-key", "", "path to cluster CA ECDSA private key PEM")
	flags.StringVar(&rootsPath, "roots", "", "path to cluster roots PEM (defaults to --ca-cert)")
	flags.StringVar(&trustDomainFrom, "trust-domain-from", "", "existing cluster leaf certificate PEM to copy trust domain from")
	flags.StringVar(&clusterIDHex, "cluster-id", "", "32-hex cluster ID (required unless --trust-domain-from is set)")
	flags.StringVar(&clusterIncarnationHex, "cluster-incarnation", "", "32-hex cluster incarnation (required unless --trust-domain-from is set)")
	flags.StringVar(&identityOIDText, "identity-oid", devClusterOID, "VibeDB identity extension OID")
	flags.StringVar(&nodeIDHex, "node-id", "", "optional 32-hex physical node ID (random when omitted)")
	flags.Uint64Var(&incarnation, "incarnation", 1, "physical node incarnation (>= 1)")
	flags.StringVar(&nodeRoot, "root", "", "absolute future data root for vibedb-shard prepare-node-rf3")
	flags.StringVar(&outDir, "out-dir", "", "absolute output directory for node credentials, prepare-node.vibejson, and node-descriptor.vibejson")
	flags.StringVar(&policyPath, "policy", "", "absolute path to cluster authorization-policy.vibejson")
	flags.BoolVar(&updatePolicy, "update-policy", false, "add the storage node principal (data_read, data_write) to --policy")
	flags.StringVar(&failureDomain, "failure-domain", "", "failure domain label for the new physical node (e.g. rack-4)")
	flags.StringVar(&peerAddr, "peer", "", "Raft peer listener address (host:port)")
	flags.StringVar(&nativeAddr, "native", "", "RF3 native listener address (host:port)")
	flags.StringVar(&snapshotAddr, "snapshot", "", "snapshot transfer listener address (host:port)")
	flags.StringVar(&controlAddr, "control", "", "replica control listener address (host:port)")
	flags.StringVar(&gatewayAddr, "gateway", "", "optional gateway control listener address (host:port)")
	flags.Var(&grantNodes, "grant-node", "32-hex controller/frontend node ID granted split-control actions (repeatable or comma-separated)")
	flags.Var(&gatewaySeeds, "gateway-seed", "bootstrap gateway seed as <node_id>@<host:port>@<spki_sha256_hex> (repeatable)")
	flags.Var(&canonicalSourceSeeds, "canonical-source-seed", "canonical source gateway seed as <node_id>@<host:port>@<spki_sha256_hex> (repeatable)")
	flags.Uint64Var(&migrationCapacity, "migration-capacity", 64<<20, "public descriptor migration capacity bytes")
	flags.UintVar(&maxReceives, "max-receives", 4, "public descriptor max concurrent receives")
	flags.UintVar(&maxGroups, "max-groups", 64, "shared node log maximum group count")
	flags.IntVar(&validDays, "valid-days", 30, "certificate validity period in days")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 {
		return 2
	}
	if caCert == "" || caKey == "" || nodeRoot == "" || outDir == "" || policyPath == "" ||
		failureDomain == "" || peerAddr == "" || nativeAddr == "" || snapshotAddr == "" || controlAddr == "" ||
		incarnation == 0 || len(grantNodes) == 0 || validDays <= 0 || maxGroups == 0 {
		fmt.Fprintln(os.Stderr, "usage: vibedb cluster init-node --ca-cert <pem> --ca-key <pem> (--trust-domain-from <cert.pem> | --cluster-id <hex> --cluster-incarnation <hex>) --root <abs-path> --out-dir <abs-path> --policy <abs-path> --failure-domain <name> --peer <host:port> --native <host:port> --snapshot <host:port> --control <host:port> --grant-node <hex>[,<hex>...]")
		return 2
	}
	if rootsPath == "" {
		rootsPath = caCert
	}
	for label, p := range map[string]string{"--root": nodeRoot, "--out-dir": outDir, "--policy": policyPath, "--roots": rootsPath} {
		if err := validateScaleAbsPath(p); err != nil {
			fmt.Fprintf(os.Stderr, "cluster init-node: %s: %v\n", label, err)
			return 2
		}
	}
	if filepath.Clean(nodeRoot) == filepath.Clean(outDir) {
		fmt.Fprintln(os.Stderr, "cluster init-node: --out-dir must be distinct from --root (prepare-node-rf3 creates --root exclusively)")
		return 2
	}
	oid, err := parseScaleOID(identityOIDText)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-node: --identity-oid: %v\n", err)
		return 2
	}
	domain, err := resolveScaleTrustDomain(trustDomainFrom, clusterIDHex, clusterIncarnationHex, oid)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-node: trust domain: %v\n", err)
		return 1
	}
	nodeID, err := resolveOrRandomScaleNodeID(nodeIDHex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-node: --node-id: %v\n", err)
		return 2
	}
	grants := make([]scaleEmptyNodeGrant, 0, len(grantNodes))
	for _, rawID := range grantNodes {
		parsed, parseErr := parseScaleNodeID(rawID)
		if parseErr != nil {
			fmt.Fprintf(os.Stderr, "cluster init-node: --grant-node %q: %v\n", rawID, parseErr)
			return 2
		}
		grants = append(grants, scaleEmptyNodeGrant{NodeID: hex.EncodeToString(parsed[:]), Actions: ^uint16(0)})
	}
	gwSeeds, err := parseScaleGatewaySeeds(gatewaySeeds)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-node: --gateway-seed: %v\n", err)
		return 2
	}
	if gwSeeds == nil {
		gwSeeds = []nodecontrol.BootstrapGatewaySeed{}
	}
	srcSeeds, err := parseScaleGatewaySeeds(canonicalSourceSeeds)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-node: --canonical-source-seed: %v\n", err)
		return 2
	}
	if err := os.MkdirAll(outDir, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-node: create out-dir: %v\n", err)
		return 1
	}

	certPath := filepath.Join(outDir, "node-cert.pem")
	keyPath := filepath.Join(outDir, "node-key.pem")
	spkiDigest, err := mintScaleLeafCertificate(caCert, caKey, certPath, keyPath, "VibeDB physical node", domain, nodeID, oid, time.Duration(validDays)*24*time.Hour)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-node: mint certificate: %v\n", err)
		return 1
	}

	// 32-byte local WAL encryption key material (kept outside nodeRoot).
	keyMaterial := make([]byte, 32)
	if _, err := cryptorand.Read(keyMaterial); err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-node: generate node-log key: %v\n", err)
		return 1
	}
	keyMaterialPath := filepath.Join(outDir, "node-log.key")
	if err := writeDevExclusive(keyMaterialPath, keyMaterial, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-node: write node-log key: %v\n", err)
		return 1
	}
	wrappedKey := make([]byte, 32)
	if _, err := cryptorand.Read(wrappedKey); err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-node: generate wrapped key: %v\n", err)
		return 1
	}

	cleanRoot := filepath.Clean(nodeRoot)
	migCfg := migrationbudget.DefaultConfig()
	prepManifest := scaleEmptyNodeManifest{
		Root: cleanRoot,
		NodeLog: scaleEmptyNodeLog{
			Format:          1,
			Path:            filepath.Join(cleanRoot, "node-log"),
			KeyID:           "node-log-key-1",
			WrappedKey:      hex.EncodeToString(wrappedKey),
			KeyMaterialPath: keyMaterialPath,
			Options:         raftstore.NodeStoreOptions{MaxGroups: int(maxGroups)},
		},
		Services: scaleEmptyNodeServices{
			NodeIncarnation: incarnation,
			Listeners: devPrepareListeners{
				Peer: peerAddr, Native: nativeAddr, Snapshot: snapshotAddr, Control: controlAddr,
			},
			TLS: devPrepareTLS{
				Certificate: certPath, Key: keyPath, Roots: rootsPath, IdentityOID: identityOIDText,
			},
			AuthorizationPolicy:  policyPath,
			GatewaySeeds:         gwSeeds,
			CanonicalSourceSeeds: srcSeeds,
			ReplicaControl: scaleEmptyNodeReplicaControl{
				ActionJournalPath:      filepath.Join(cleanRoot, "replica-actions"),
				MaxActionRecords:       4096,
				SourceDataRoot:         cleanRoot,
				SourceJournalPath:      filepath.Join(cleanRoot, "source-exports"),
				MaxSourceRecords:       4096,
				SourceRepositoryPath:   filepath.Join(cleanRoot, "source-artifacts"),
				MaxSourceArtifacts:     8,
				MaxSourceConcurrent:    2,
				MaxSourceArtifactBytes: 1 << 30,
				MaxSourceDiskBytes:     4 << 30,
				SourceChunkBytes:       1 << 20,
				Migration: scaleEmptyNodeMigration{
					MaxActive:      migCfg.MaxActive,
					CPU:            scaleEmptyNodeRateLimit{BytesPerSecond: migCfg.CPU.BytesPerSecond, BurstBytes: migCfg.CPU.BurstBytes},
					DiskRead:       scaleEmptyNodeRateLimit{BytesPerSecond: migCfg.DiskRead.BytesPerSecond, BurstBytes: migCfg.DiskRead.BurstBytes},
					DiskWrite:      scaleEmptyNodeRateLimit{BytesPerSecond: migCfg.DiskWrite.BytesPerSecond, BurstBytes: migCfg.DiskWrite.BurstBytes},
					NetworkSend:    scaleEmptyNodeRateLimit{BytesPerSecond: migCfg.NetworkSend.BytesPerSecond, BurstBytes: migCfg.NetworkSend.BurstBytes},
					NetworkReceive: scaleEmptyNodeRateLimit{BytesPerSecond: migCfg.NetworkReceive.BytesPerSecond, BurstBytes: migCfg.NetworkReceive.BurstBytes},
				},
			},
			SplitControl: scaleEmptyNodeSplitControl{
				JournalPath:   filepath.Join(cleanRoot, "split-control.journal"),
				MaxRecords:    4096,
				MaxFileBytes:  64 << 20,
				Grants:        grants,
				MaxOperations: 8,
			},
		},
		Groups: []struct{}{},
	}
	prepRaw, err := vibejson.Marshal(&prepManifest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-node: encode prepare manifest: %v\n", err)
		return 1
	}
	prepPath := filepath.Join(outDir, "prepare-node.vibejson")
	if err := writeDevExclusive(prepPath, prepRaw, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-node: write prepare manifest: %v\n", err)
		return 1
	}

	roles := []string{"control", "storage"}
	if gatewayAddr != "" {
		roles = append(roles, "gateway")
	}
	sort.Strings(roles)
	descriptor := clustercontrol.NodeDescriptor{
		Format:            clustercontrol.Format,
		NodeID:            hex.EncodeToString(nodeID[:]),
		Incarnation:       incarnation,
		ServiceKeyDigest:  hex.EncodeToString(spkiDigest[:]),
		FailureDomain:     failureDomain,
		Roles:             roles,
		DataAddress:       peerAddr,
		NativeAddress:     nativeAddr,
		ControlAddress:    controlAddr,
		GatewayAddress:    gatewayAddr,
		MigrationCapacity: migrationCapacity,
		MaxReceives:       uint32(maxReceives),
	}
	descRaw, err := vibejson.Marshal(&descriptor)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-node: encode node descriptor: %v\n", err)
		return 1
	}
	descPath := filepath.Join(outDir, "node-descriptor.vibejson")
	if err := writeDevExclusive(descPath, descRaw, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-node: write node descriptor: %v\n", err)
		return 1
	}
	if _, err := clustercontrol.LoadNodeDescriptor(descPath); err != nil {
		fmt.Fprintf(os.Stderr, "cluster init-node: verify node descriptor: %v\n", err)
		return 1
	}
	if updatePolicy {
		if err := addScalePolicyPrincipal(policyPath, nodeID, []string{"data_read", "data_write"}); err != nil {
			fmt.Fprintf(os.Stderr, "cluster init-node: update policy: %v\n", err)
			return 1
		}
	}
	fmt.Fprintf(os.Stdout, "node=%s\nincarnation=%d\nservice_key_digest=%s\nprepare_manifest=%s\nnode_descriptor=%s\n",
		descriptor.NodeID, descriptor.Incarnation, descriptor.ServiceKeyDigest, prepPath, descPath)
	return 0
}

func validateScaleAbsPath(p string) error {
	if p == "" || !filepath.IsAbs(p) || filepath.Clean(p) != p {
		return fmt.Errorf("must be a clean absolute path: %q", p)
	}
	return nil
}

func parseScaleOID(text string) (asn1.ObjectIdentifier, error) {
	parts := strings.Split(text, ".")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid OID %q", text)
	}
	out := make(asn1.ObjectIdentifier, len(parts))
	for i, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid OID component %q", part)
		}
		out[i] = n
	}
	return out, nil
}

func parseScaleNodeID(hexText string) (rafttransport.NodeID, error) {
	var id rafttransport.NodeID
	decoded, err := hex.DecodeString(hexText)
	if err != nil || len(decoded) != len(id) || hexText != strings.ToLower(hexText) {
		return id, fmt.Errorf("must be 32 lowercase hex characters")
	}
	copy(id[:], decoded)
	if id == (rafttransport.NodeID{}) {
		return id, errors.New("cannot be all zeros")
	}
	return id, nil
}

func resolveOrRandomScaleNodeID(hexText string) (rafttransport.NodeID, error) {
	if hexText != "" {
		return parseScaleNodeID(hexText)
	}
	var id rafttransport.NodeID
	if _, err := cryptorand.Read(id[:]); err != nil {
		return id, err
	}
	if id == (rafttransport.NodeID{}) {
		id[0] = 1
	}
	return id, nil
}

func resolveScaleTrustDomain(fromCert, clusterIDHex, clusterIncarnationHex string, oid asn1.ObjectIdentifier) (rafttransport.TrustDomain, error) {
	if fromCert != "" {
		raw, err := os.ReadFile(fromCert)
		if err != nil || len(raw) == 0 || len(raw) > 1<<20 {
			return rafttransport.TrustDomain{}, fmt.Errorf("read %s: %w", fromCert, err)
		}
		block, _ := pem.Decode(raw)
		if block == nil || block.Type != "CERTIFICATE" {
			return rafttransport.TrustDomain{}, fmt.Errorf("%s: not a PEM certificate", fromCert)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return rafttransport.TrustDomain{}, err
		}
		identity, err := rafttransport.ParsePeerIdentity(oid, cert)
		if err != nil {
			return rafttransport.TrustDomain{}, err
		}
		return identity.TrustDomain, nil
	}
	cid, err := parseScaleNodeID(clusterIDHex)
	if err != nil {
		return rafttransport.TrustDomain{}, fmt.Errorf("cluster-id: %w", err)
	}
	cinc, err := parseScaleNodeID(clusterIncarnationHex)
	if err != nil {
		return rafttransport.TrustDomain{}, fmt.Errorf("cluster-incarnation: %w", err)
	}
	return rafttransport.TrustDomain{ClusterID: cid, ClusterIncarnation: cinc}, nil
}

func mintScaleLeafCertificate(
	caCertPath, caKeyPath, certOutPath, keyOutPath, commonName string,
	domain rafttransport.TrustDomain,
	node rafttransport.NodeID,
	oid asn1.ObjectIdentifier,
	validity time.Duration,
) (replication.Digest, error) {
	caCertPEM, err := os.ReadFile(caCertPath)
	if err != nil || len(caCertPEM) == 0 || len(caCertPEM) > 1<<20 {
		return replication.Digest{}, fmt.Errorf("read ca-cert: %w", err)
	}
	caBlock, rest := pem.Decode(caCertPEM)
	if caBlock == nil || caBlock.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return replication.Digest{}, errors.New("ca-cert must contain exactly one PEM CERTIFICATE block")
	}
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil || !caCert.IsCA || !caCert.BasicConstraintsValid {
		return replication.Digest{}, errors.New("ca-cert is not a valid CA certificate")
	}
	caKeyPEM, err := os.ReadFile(caKeyPath)
	if err != nil || len(caKeyPEM) == 0 || len(caKeyPEM) > 1<<20 {
		return replication.Digest{}, fmt.Errorf("read ca-key: %w", err)
	}
	keyBlock, keyRest := pem.Decode(caKeyPEM)
	if keyBlock == nil || len(bytes.TrimSpace(keyRest)) != 0 {
		return replication.Digest{}, errors.New("ca-key must contain exactly one PEM key block")
	}
	caKey, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		parsed, parseErr := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
		if parseErr != nil {
			return replication.Digest{}, fmt.Errorf("parse ca-key: %w", parseErr)
		}
		var ok bool
		caKey, ok = parsed.(*ecdsa.PrivateKey)
		if !ok {
			return replication.Digest{}, errors.New("ca-key must be an ECDSA private key")
		}
	}
	caPub, ok := caCert.PublicKey.(*ecdsa.PublicKey)
	if !ok || !caKey.PublicKey.Equal(caPub) {
		return replication.Digest{}, errors.New("ca-key does not match ca-cert public key")
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		return replication.Digest{}, err
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 120)
	serial, err := cryptorand.Int(cryptorand.Reader, serialLimit)
	if err != nil {
		return replication.Digest{}, err
	}
	serial.Add(serial, big.NewInt(2))
	ext, err := rafttransport.PeerIdentityExtension(oid, rafttransport.PeerIdentity{TrustDomain: domain, Node: node})
	if err != nil {
		return replication.Digest{}, err
	}
	now := time.Now().UTC()
	tmpl := &x509.Certificate{
		SerialNumber:    serial,
		Subject:         pkix.Name{CommonName: commonName},
		NotBefore:       now.Add(-time.Minute),
		NotAfter:        now.Add(validity),
		KeyUsage:        x509.KeyUsageDigitalSignature,
		ExtKeyUsage:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		ExtraExtensions: []pkix.Extension{ext},
	}
	leafDER, err := x509.CreateCertificate(cryptorand.Reader, tmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return replication.Digest{}, err
	}
	leafPEM := append(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caBlock.Bytes})...,
	)
	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		return replication.Digest{}, err
	}
	if err := writeDevExclusive(certOutPath, leafPEM, 0o600); err != nil {
		return replication.Digest{}, err
	}
	if err := writeDevExclusive(keyOutPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyDER}), 0o600); err != nil {
		return replication.Digest{}, err
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&leafKey.PublicKey)
	if err != nil {
		return replication.Digest{}, err
	}
	return replication.Digest(sha256.Sum256(pubDER)), nil
}

func parseScaleGatewaySeeds(items []string) ([]nodecontrol.BootstrapGatewaySeed, error) {
	if len(items) == 0 {
		return nil, nil
	}
	out := make([]nodecontrol.BootstrapGatewaySeed, 0, len(items))
	for _, raw := range items {
		parts := strings.Split(raw, "@")
		if len(parts) != 3 {
			return nil, fmt.Errorf("seed %q must be <node_id>@<host:port>@<spki_sha256_hex>", raw)
		}
		id, err := parseScaleNodeID(parts[0])
		if err != nil {
			return nil, fmt.Errorf("seed node_id: %w", err)
		}
		pinBytes, err := hex.DecodeString(parts[2])
		if err != nil || len(pinBytes) != sha256.Size || parts[2] != strings.ToLower(parts[2]) {
			return nil, fmt.Errorf("seed spki digest must be 64 lowercase hex chars")
		}
		var pin replication.Digest
		copy(pin[:], pinBytes)
		out = append(out, nodecontrol.BootstrapGatewaySeed{
			NodeID:         id,
			Incarnation:    1,
			ControlAddress: parts[1],
			SPKIPinDigest:  pin,
		})
	}
	return out, nil
}

func addScalePolicyPrincipal(policyPath string, node rafttransport.NodeID, capabilities []string) error {
	raw, err := os.ReadFile(policyPath)
	if err != nil {
		return err
	}
	var policy devPolicy
	if err := vibejson.Unmarshal(raw, &policy); err != nil {
		return err
	}
	nodeHex := hex.EncodeToString(node[:])
	for _, existing := range policy.Principals {
		if existing.Node == nodeHex {
			return fmt.Errorf("principal %s already exists in %s", nodeHex, policyPath)
		}
	}
	policy.Generation++
	policy.Principals = append(policy.Principals, devPrincipal{
		Node:         nodeHex,
		Capabilities: append([]string(nil), capabilities...),
	})
	sort.Slice(policy.Principals, func(i, j int) bool {
		return policy.Principals[i].Node < policy.Principals[j].Node
	})
	updated, err := vibejson.Marshal(&policy)
	if err != nil {
		return err
	}
	// Verify the updated policy loads cleanly through serviceauthz.
	if _, err := serviceauthz.Load(updated); err != nil {
		return err
	}
	tmp := policyPath + ".tmp"
	if err := os.WriteFile(tmp, updated, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, policyPath); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
