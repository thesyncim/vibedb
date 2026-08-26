package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

const (
	devClusterFormat = 1
	devClusterNodes  = 3
	devClusterOID    = "1.3.6.1.4.1.32473.1.1"
	devReadyTimeout  = 30 * time.Second
)

var errDevCluster = errors.New("vibedb: invalid local development cluster")

type devClusterManifest struct {
	Format              uint16             `json:"format"`
	Nodes               uint8              `json:"nodes"`
	ClientEndpoint      string             `json:"client_endpoint"`
	CatalogPath         string             `json:"catalog_path"`
	GatewayCertificate  string             `json:"gateway_certificate"`
	GatewayKey          string             `json:"gateway_key"`
	Roots               string             `json:"roots"`
	AuthorizationPolicy string             `json:"authorization_policy"`
	GatewayNode         string             `json:"gateway_node"`
	Members             []devClusterMember `json:"members"`
}

type devClusterMember struct {
	Member        uint64 `json:"member"`
	Node          string `json:"node"`
	Store         string `json:"store"`
	Peer          string `json:"peer"`
	Native        string `json:"native"`
	Snapshot      string `json:"snapshot"`
	Control       string `json:"control"`
	ServeManifest string `json:"serve_manifest"`
}

type devPrepareManifest struct {
	Root                  string                            `json:"root"`
	Distribution          string                            `json:"distribution"`
	Shard                 string                            `json:"shard"`
	ClusterID             string                            `json:"cluster_id"`
	ClusterIncarnation    string                            `json:"cluster_incarnation"`
	TopologyRecoveryEpoch uint64                            `json:"topology_recovery_epoch"`
	AllocationGeneration  uint64                            `json:"allocation_generation"`
	ShardIncarnation      string                            `json:"shard_incarnation"`
	GroupID               string                            `json:"group_id"`
	MemberID              uint64                            `json:"member_id"`
	StoreID               string                            `json:"store_id"`
	Table                 string                            `json:"table"`
	CreateTable           string                            `json:"create_table"`
	Authority             devPrepareAuthority               `json:"authority"`
	WAL                   devPrepareWAL                     `json:"wal"`
	Apply                 devPrepareApply                   `json:"apply"`
	Listeners             devPrepareListeners               `json:"listeners"`
	TLS                   devPrepareTLS                     `json:"tls"`
	AuthorizationPolicy   string                            `json:"authorization_policy"`
	Members               [devClusterNodes]devPrepareMember `json:"members"`
}
type devPrepareAuthority struct {
	ActivePolicyGeneration uint64 `json:"active_policy_generation"`
	ProtectionEpoch        uint64 `json:"protection_epoch"`
	OwnershipEpoch         uint64 `json:"ownership_epoch"`
	SchemaGeneration       uint64 `json:"schema_generation"`
	RoutingVersion         uint64 `json:"routing_version"`
	RouteGeneration        uint64 `json:"route_generation"`
}
type devPrepareWAL struct {
	KeyID           string `json:"key_id"`
	KeyMaterialPath string `json:"key_material_path"`
	WrappedKey      string `json:"wrapped_key"`
	MaxFileBytes    int64  `json:"max_file_bytes"`
	MaxRecordBytes  int    `json:"max_record_bytes"`
	MaxRecords      uint64 `json:"max_records"`
	MaxEntries      uint64 `json:"max_entries"`
	MaxLiveBytes    int64  `json:"max_live_bytes"`
}
type devPrepareApply struct {
	MaxSessions    uint64 `json:"max_sessions"`
	RetryWindow    uint16 `json:"retry_window"`
	MaxCollections int    `json:"max_collections"`
	MaxDocuments   int    `json:"max_documents"`
	MaxBytes       int    `json:"max_bytes"`
	ShardKey       string `json:"shard_key"`
}
type devPrepareListeners struct {
	Peer     string `json:"peer"`
	Native   string `json:"native"`
	Snapshot string `json:"snapshot"`
	Control  string `json:"control"`
}
type devPrepareTLS struct {
	Certificate string `json:"certificate"`
	Key         string `json:"key"`
	Roots       string `json:"roots"`
	IdentityOID string `json:"identity_oid"`
}
type devPrepareMember struct {
	MemberID    uint64 `json:"member_id"`
	NodeID      string `json:"node_id"`
	PeerAddress string `json:"peer_address"`
}
type devPolicy struct {
	Generation uint64         `json:"generation"`
	Principals []devPrincipal `json:"principals"`
}
type devPrincipal struct {
	Node         string   `json:"node"`
	Capabilities []string `json:"capabilities"`
}

type devClusterOptions struct {
	root, shardBinary, gatewayBinary string
	nodes                            int
}

func runClusterDev(args []string) int {
	fs := flag.NewFlagSet("cluster dev", flag.ContinueOnError)
	root := fs.String("root", "", "absolute durable cluster directory")
	nodes := fs.Int("nodes", devClusterNodes, "RF3 member count (must be 3)")
	shardBinary := fs.String("shard-binary", "", "vibedb-shard executable; defaults beside vibedb or PATH")
	gatewayBinary := fs.String("gateway-binary", "", "vibedb-gateway executable; defaults beside vibedb or PATH")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *root == "" || *nodes != devClusterNodes {
		usage()
		return 2
	}
	if !filepath.IsAbs(*root) || filepath.Clean(*root) != *root {
		fmt.Fprintf(os.Stderr, "cluster dev: %v\n", errDevCluster)
		return 2
	}
	abs, err := filepath.Abs(*root)
	if err != nil || filepath.Clean(abs) != abs || abs == string(filepath.Separator) {
		fmt.Fprintf(os.Stderr, "cluster dev: %v\n", errDevCluster)
		return 2
	}
	shard, err := resolveDevBinary(*shardBinary, "vibedb-shard")
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster dev: %v\n", err)
		return 1
	}
	gw, err := resolveDevBinary(*gatewayBinary, "vibedb-gateway")
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster dev: %v\n", err)
		return 1
	}
	manifest, err := ensureDevCluster(devClusterOptions{root: abs, nodes: *nodes, shardBinary: shard, gatewayBinary: gw})
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster dev: %v\n", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := serveDevCluster(ctx, manifest, shard, gw); err != nil {
		fmt.Fprintf(os.Stderr, "cluster dev: %v\n", err)
		return 1
	}
	return 0
}

func resolveDevBinary(explicit, name string) (string, error) {
	if explicit != "" {
		path, err := filepath.Abs(explicit)
		if err != nil {
			return "", err
		}
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
			return "", errors.Join(errDevCluster, err)
		}
		return path, nil
	}
	self, err := os.Executable()
	if err == nil {
		sibling := filepath.Join(filepath.Dir(self), name)
		if info, e := os.Stat(sibling); e == nil && info.Mode().IsRegular() && info.Mode()&0o111 != 0 {
			return sibling, nil
		}
	}
	return exec.LookPath(name)
}

func ensureDevCluster(options devClusterOptions) (devClusterManifest, error) {
	manifestPath := filepath.Join(options.root, "cluster.vibejson")
	if raw, err := readDevFile(manifestPath, 1<<20); err == nil {
		var m devClusterManifest
		if vibejson.Unmarshal(raw, &m) != nil {
			return m, errDevCluster
		}
		canonical, e := vibejson.Marshal(&m)
		if e != nil || !bytes.Equal(raw, canonical) || !validDevManifest(m, options.root) {
			return m, errDevCluster
		}
		return m, completeDevCluster(options, m)
	} else if !errors.Is(err, os.ErrNotExist) {
		return devClusterManifest{}, err
	}
	if entries, err := os.ReadDir(options.root); err == nil && len(entries) != 0 {
		return devClusterManifest{}, errDevCluster
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return devClusterManifest{}, err
	}
	if err := os.MkdirAll(options.root, 0o700); err != nil {
		return devClusterManifest{}, err
	}
	return initializeDevCluster(options, manifestPath)
}

func initializeDevCluster(options devClusterOptions, manifestPath string) (devClusterManifest, error) {
	var clusterID, clusterIncarnation, shardIncarnation, groupID [16]byte
	for _, dst := range [][]byte{clusterID[:], clusterIncarnation[:], shardIncarnation[:], groupID[:]} {
		if _, err := io.ReadFull(cryptorand.Reader, dst); err != nil {
			return devClusterManifest{}, err
		}
	}
	nodes := make([]rafttransport.NodeID, devClusterNodes+1)
	stores := make([][16]byte, devClusterNodes)
	for i := range nodes {
		if _, err := io.ReadFull(cryptorand.Reader, nodes[i][:]); err != nil {
			return devClusterManifest{}, err
		}
	}
	for i := range stores {
		if _, err := io.ReadFull(cryptorand.Reader, stores[i][:]); err != nil {
			return devClusterManifest{}, err
		}
	}
	ports, err := reserveDevPorts(1 + devClusterNodes*4)
	if err != nil {
		return devClusterManifest{}, err
	}
	credentials, roots, err := writeDevCredentials(options.root, rafttransport.TrustDomain{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation}, nodes)
	if err != nil {
		return devClusterManifest{}, err
	}
	policyPath := filepath.Join(options.root, "authorization-policy.vibejson")
	if err := writeDevPolicy(policyPath, nodes); err != nil {
		return devClusterManifest{}, err
	}
	keySource := filepath.Join(options.root, "wal-key-source")
	keyMaterial := make([]byte, 32)
	if _, err := io.ReadFull(cryptorand.Reader, keyMaterial); err != nil {
		return devClusterManifest{}, err
	}
	if err := writeDevExclusive(keySource, keyMaterial, 0o600); err != nil {
		return devClusterManifest{}, err
	}
	clear(keyMaterial)
	m := devClusterManifest{Format: devClusterFormat, Nodes: devClusterNodes, ClientEndpoint: ports[0], CatalogPath: filepath.Join(options.root, "catalog.vibejson"), GatewayCertificate: credentials[3][0], GatewayKey: credentials[3][1], Roots: roots, AuthorizationPolicy: policyPath, GatewayNode: hex.EncodeToString(nodes[3][:]), Members: make([]devClusterMember, devClusterNodes)}
	prepareMembers := [devClusterNodes]devPrepareMember{}
	for i := 0; i < devClusterNodes; i++ {
		base := 1 + i*4
		prepareMembers[i] = devPrepareMember{MemberID: uint64(i + 1), NodeID: hex.EncodeToString(nodes[i][:]), PeerAddress: ports[base]}
	}
	authority := devPrepareAuthority{ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1}
	for i := 0; i < devClusterNodes; i++ {
		memberRoot := filepath.Join(options.root, fmt.Sprintf("member-%d", i+1))
		base := 1 + i*4
		prep := devPrepareManifest{Root: memberRoot, Distribution: string(gateway.ReplicatedCatalogDistribution), Shard: string(gateway.ReplicatedCatalogShard), ClusterID: hex.EncodeToString(clusterID[:]), ClusterIncarnation: hex.EncodeToString(clusterIncarnation[:]), TopologyRecoveryEpoch: 1, AllocationGeneration: 1, ShardIncarnation: hex.EncodeToString(shardIncarnation[:]), GroupID: hex.EncodeToString(groupID[:]), MemberID: uint64(i + 1), StoreID: hex.EncodeToString(stores[i][:]), Table: gateway.ReplicatedCatalogTable, CreateTable: "CREATE TABLE controlplane (PRIMARY KEY (id))", Authority: authority, WAL: devPrepareWAL{KeyID: "dev-cluster-key", KeyMaterialPath: keySource, WrappedKey: "local-development-only", MaxFileBytes: raftstore.DefaultMaxFileBytes, MaxRecordBytes: raftstore.DefaultMaxRecordBytes, MaxRecords: raftstore.DefaultMaxRecords, MaxEntries: raftstore.DefaultMaxEntries, MaxLiveBytes: raftstore.DefaultMaxLiveBytes}, Apply: devPrepareApply{MaxSessions: 128, RetryWindow: 8, MaxCollections: 16, MaxDocuments: 4096, MaxBytes: 384 << 20, ShardKey: gateway.ReplicatedCatalogPrimaryKey}, Listeners: devPrepareListeners{Peer: ports[base], Native: ports[base+1], Snapshot: ports[base+2], Control: ports[base+3]}, TLS: devPrepareTLS{Certificate: credentials[i][0], Key: credentials[i][1], Roots: roots, IdentityOID: devClusterOID}, AuthorizationPolicy: policyPath, Members: prepareMembers}
		prepPath := filepath.Join(options.root, fmt.Sprintf("prepare-member-%d.vibejson", i+1))
		raw, e := vibejson.Marshal(&prep)
		if e != nil {
			return m, e
		}
		if e = writeDevExclusive(prepPath, raw, 0o600); e != nil {
			return m, e
		}
		m.Members[i] = devClusterMember{Member: uint64(i + 1), Node: prepareMembers[i].NodeID, Store: hex.EncodeToString(stores[i][:]), Peer: ports[base], Native: ports[base+1], Snapshot: ports[base+2], Control: ports[base+3], ServeManifest: filepath.Join(memberRoot, "serve-rf3.vibejson")}
	}
	raw, err := vibejson.Marshal(&m)
	if err != nil {
		return m, err
	}
	if err = writeDevExclusive(manifestPath, raw, 0o600); err != nil {
		return m, err
	}
	if err = syncDevDir(options.root); err != nil {
		return m, err
	}
	return m, completeDevCluster(options, m)
}

func completeDevCluster(options devClusterOptions, manifest devClusterManifest) error {
	for index, member := range manifest.Members {
		if _, err := os.Stat(member.ServeManifest); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		preparePath := filepath.Join(options.root, fmt.Sprintf("prepare-member-%d.vibejson", index+1))
		if err := runDevCommand(options.shardBinary, "prepare-rf3", "-manifest", preparePath); err != nil {
			return err
		}
	}
	if _, err := os.Stat(manifest.CatalogPath); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	raw, err := readDevFile(filepath.Join(options.root, "prepare-member-1.vibejson"), 1<<20)
	if err != nil {
		return err
	}
	var prepare devPrepareManifest
	if err := vibejson.Unmarshal(raw, &prepare); err != nil {
		return errors.Join(errDevCluster, err)
	}
	clusterID, err := decodeDev16(prepare.ClusterID)
	if err != nil {
		return err
	}
	clusterIncarnation, err := decodeDev16(prepare.ClusterIncarnation)
	if err != nil {
		return err
	}
	shardIncarnation, err := decodeDev16(prepare.ShardIncarnation)
	if err != nil {
		return err
	}
	groupID, err := decodeDev16(prepare.GroupID)
	if err != nil {
		return err
	}
	return writeDevCatalog(manifest, clusterID, clusterIncarnation, shardIncarnation, groupID)
}

func writeDevCatalog(m devClusterManifest, clusterID, clusterIncarnation, shardIncarnation, groupID [16]byte) error {
	leaders := make([]distribution.EndpointID, devClusterNodes)
	endpoints := make(map[distribution.EndpointID]string, devClusterNodes*3)
	replicas := make([]gateway.ReplicatedReplicaDescriptor, devClusterNodes)
	var digest [32]byte
	for i, member := range m.Members {
		leaders[i] = distribution.EndpointID(fmt.Sprintf("member-%d", i+1))
		native := distribution.EndpointID(fmt.Sprintf("member-%d-native", i+1))
		control := distribution.EndpointID(fmt.Sprintf("member-%d-control", i+1))
		endpoints[leaders[i]] = member.Peer
		endpoints[native] = member.Native
		endpoints[control] = member.Control
		node, _ := decodeDev16(member.Node)
		store, _ := decodeDev16(member.Store)
		replicas[i] = gateway.ReplicatedReplicaDescriptor{Member: member.Member, Node: rafttransport.NodeID(node), StoreID: store, NodeIncarnation: 1, Endpoint: leaders[i], NativeEndpoint: native, ControlEndpoint: control}
		var identity sqldriver.ReplicatedShardStoreIdentity
		raw, err := readDevFile(filepath.Join(filepath.Dir(member.ServeManifest), "sql-identity.vibejson"), 1<<20)
		if err != nil || identity.UnmarshalJSON(raw) != nil {
			return errDevCluster
		}
		if i == 0 {
			digest = identity.RelationManifestDigest
		} else if digest != identity.RelationManifestDigest {
			return errDevCluster
		}
	}
	manifest, err := distribution.NewManifest(gateway.ReplicatedCatalogDistribution, 1, []distribution.Shard{{ID: gateway.ReplicatedCatalogShard, AllocationGeneration: 1, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}, Leaders: leaders, Epoch: 1}})
	if err != nil {
		return err
	}
	group := raftmember.GroupKey{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: shardIncarnation, GroupID: groupID}
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(distribution.ClusterConfig{Distributions: []distribution.DistributionSpec{{Name: gateway.ReplicatedCatalogDistribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion}}, Placements: []distribution.TablePlacement{{Table: gateway.ReplicatedCatalogTable, Distribution: gateway.ReplicatedCatalogDistribution, Columns: []string{gateway.ReplicatedCatalogPrimaryKey}}}, Manifests: []*distribution.Manifest{manifest}}, endpoints, 1, nil, nil, []gateway.ReplicatedShardDescriptor{{Distribution: gateway.ReplicatedCatalogDistribution, Shard: gateway.ReplicatedCatalogShard, Group: group, AllocationGeneration: 1, Command: raftservice.CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1, RelationManifestDigest: digest, RoutingVersion: 1, RouteGeneration: 1}, Replicas: replicas}})
	if err != nil {
		return err
	}
	return gateway.SaveSnapshot(m.CatalogPath, snapshot)
}

type devChild struct {
	command     *exec.Cmd
	ready       chan error
	done        chan struct{}
	diagnostics *devDiagnostics
	waitMu      sync.Mutex
	waitErr     error
}

type devChildExit struct {
	name string
	err  error
}

func serveDevCluster(ctx context.Context, m devClusterManifest, shardBinary, gatewayBinary string) error {
	children := make([]*devChild, 0, devClusterNodes+1)
	exits := make(chan devChildExit, devClusterNodes+1)
	defer stopDevChildren(children)
	for _, member := range m.Members {
		child, err := startDevChild(shardBinary, []string{"serve-rf3", "-manifest", member.ServeManifest}, "vibedb-shard RF3 ready")
		if err != nil {
			return err
		}
		children = append(children, child)
		watchDevChildExit(exits, fmt.Sprintf("shard member %d", member.Member), child)
	}
	for _, child := range children {
		if err := waitDevReadyOrExit(ctx, child, exits); err != nil {
			return err
		}
	}
	args := []string{"serve", "-catalog", m.CatalogPath, "-catalog-relation", "1", "-catalog-session-journal", filepath.Join(filepath.Dir(m.CatalogPath), "gateway-session"), "-catalog-client-id", m.GatewayNode, "-catalog-retry-home", m.GatewayNode[:16], "-listen", m.ClientEndpoint, "-tls-certificate", m.GatewayCertificate, "-tls-key", m.GatewayKey, "-tls-roots", m.Roots, "-tls-identity-oid", devClusterOID, "-authorization-policy", m.AuthorizationPolicy}
	for _, member := range m.Members {
		args = append(args, "-shard-peer", member.Native+"="+member.Node)
	}
	gatewayChild, err := startDevChild(gatewayBinary, args, "vibedb-gateway serving")
	if err != nil {
		return err
	}
	children = append(children, gatewayChild)
	watchDevChildExit(exits, "gateway", gatewayChild)
	if err = waitDevReadyOrExit(ctx, gatewayChild, exits); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "VibeDB development cluster ready: %s\n", m.ClientEndpoint)
	select {
	case <-ctx.Done():
		return nil
	case exit := <-exits:
		return errors.Join(fmt.Errorf("%s exited", exit.name), exit.err)
	}
}

func watchDevChildExit(exits chan<- devChildExit, name string, child *devChild) {
	go func() {
		<-child.done
		exits <- devChildExit{name: name, err: child.waitError()}
	}()
}

func waitDevReadyOrExit(ctx context.Context, child *devChild, exits <-chan devChildExit) error {
	timer := time.NewTimer(devReadyTimeout)
	defer timer.Stop()
	select {
	case err := <-child.ready:
		return err
	case exit := <-exits:
		return errors.Join(fmt.Errorf("%s exited before cluster readiness", exit.name), exit.err)
	case <-timer.C:
		return errors.New("development cluster readiness timeout")
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
func startDevChild(binary string, args []string, marker string) (*devChild, error) {
	diagnostics := &devDiagnostics{maximum: 64 << 10}
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	command := exec.Command(binary, args...)
	command.Stdout = diagnostics
	command.Stderr = writer
	child := &devChild{command: command, ready: make(chan error, 1), done: make(chan struct{}), diagnostics: diagnostics}
	if err = command.Start(); err != nil {
		reader.Close()
		writer.Close()
		return nil, err
	}
	writer.Close()
	go func() {
		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 4096), 64<<10)
		found := false
		for scanner.Scan() {
			line := scanner.Text()
			diagnostics.Write([]byte(line + "\n"))
			if !found && strings.Contains(line, marker) {
				found = true
				child.ready <- nil
			}
		}
		if !found {
			child.ready <- errors.Join(errors.New(diagnostics.String()), scanner.Err())
		}
		reader.Close()
	}()
	go func() {
		err := command.Wait()
		child.waitMu.Lock()
		child.waitErr = err
		child.waitMu.Unlock()
		close(child.done)
	}()
	return child, nil
}

func (child *devChild) waitError() error {
	child.waitMu.Lock()
	defer child.waitMu.Unlock()
	return child.waitErr
}
func waitDevReady(ctx context.Context, child *devChild) error {
	timer := time.NewTimer(devReadyTimeout)
	defer timer.Stop()
	select {
	case err := <-child.ready:
		return err
	case <-timer.C:
		return errors.New("development cluster readiness timeout")
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
func stopDevChildren(children []*devChild) {
	for i := len(children) - 1; i >= 0; i-- {
		if children[i] != nil && children[i].command.Process != nil {
			_ = children[i].command.Process.Signal(syscall.SIGTERM)
		}
	}
	for i := len(children) - 1; i >= 0; i-- {
		select {
		case <-children[i].done:
		case <-time.After(10 * time.Second):
			_ = children[i].command.Process.Kill()
			<-children[i].done
		}
	}
}

type devDiagnostics struct {
	mu      sync.Mutex
	data    []byte
	maximum int
}

func (d *devDiagnostics) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if n := d.maximum - len(d.data); n > 0 {
		d.data = append(d.data, p[:min(n, len(p))]...)
	}
	return len(p), nil
}
func (d *devDiagnostics) String() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return string(bytes.Clone(d.data))
}

func reserveDevPorts(count int) ([]string, error) {
	listeners := make([]net.Listener, count)
	addresses := make([]string, count)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, open := range listeners {
				if open != nil {
					open.Close()
				}
			}
			return nil, err
		}
		listeners[i] = l
		addresses[i] = l.Addr().String()
	}
	for _, l := range listeners {
		if err := l.Close(); err != nil {
			return nil, err
		}
	}
	return addresses, nil
}
func writeDevCredentials(root string, domain rafttransport.TrustDomain, nodes []rafttransport.NodeID) ([][2]string, string, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		return nil, "", err
	}
	now := time.Now().UTC()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "VibeDB local development CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(30 * 24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	caDER, err := x509.CreateCertificate(cryptorand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, "", err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, "", err
	}
	roots := filepath.Join(root, "roots.pem")
	if err = writeDevExclusive(roots, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		return nil, "", err
	}
	result := make([][2]string, len(nodes))
	for i, node := range nodes {
		key, e := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
		if e != nil {
			return nil, "", e
		}
		extension, e := rafttransport.PeerIdentityExtension(devIdentityOID, rafttransport.PeerIdentity{TrustDomain: domain, Node: node})
		if e != nil {
			return nil, "", e
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: "VibeDB local node"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(30 * 24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, ExtraExtensions: []pkix.Extension{extension}}
		der, e := x509.CreateCertificate(cryptorand.Reader, leaf, caCert, &key.PublicKey, caKey)
		if e != nil {
			return nil, "", e
		}
		certPath := filepath.Join(root, fmt.Sprintf("node-%d-cert.pem", i+1))
		keyPath := filepath.Join(root, fmt.Sprintf("node-%d-key.pem", i+1))
		certPEM := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})...)
		keyDER, e := x509.MarshalECPrivateKey(key)
		if e != nil {
			return nil, "", e
		}
		if e = writeDevExclusive(certPath, certPEM, 0o600); e != nil {
			return nil, "", e
		}
		if e = writeDevExclusive(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); e != nil {
			return nil, "", e
		}
		result[i] = [2]string{certPath, keyPath}
	}
	return result, roots, nil
}

var devIdentityOID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}

func writeDevPolicy(path string, nodes []rafttransport.NodeID) error {
	encoded := make([]string, len(nodes))
	for i, n := range nodes {
		encoded[i] = hex.EncodeToString(n[:])
	}
	sort.Strings(encoded)
	caps := []string{"data_read", "data_write", "schema", "delegate", "membership", "topology", "transaction_recovery", "request_ledger", "execution_pin"}
	policy := devPolicy{Generation: 1, Principals: make([]devPrincipal, len(encoded))}
	for i, n := range encoded {
		policy.Principals[i] = devPrincipal{Node: n, Capabilities: caps}
	}
	raw, err := vibejson.Marshal(&policy)
	if err != nil {
		return err
	}
	return writeDevExclusive(path, raw, 0o600)
}
func validDevManifest(m devClusterManifest, root string) bool {
	if m.Format != devClusterFormat || m.Nodes != devClusterNodes || len(m.Members) != devClusterNodes ||
		!validDevLoopbackAddress(m.ClientEndpoint) {
		return false
	}
	if _, err := decodeDev16(m.GatewayNode); err != nil {
		return false
	}
	paths := []string{m.CatalogPath, m.GatewayCertificate, m.GatewayKey, m.Roots, m.AuthorizationPolicy}
	addresses := map[string]struct{}{m.ClientEndpoint: {}}
	nodes := map[string]struct{}{m.GatewayNode: {}}
	stores := make(map[string]struct{}, len(m.Members))
	for index, member := range m.Members {
		paths = append(paths, member.ServeManifest)
		if member.Member != uint64(index+1) {
			return false
		}
		if _, err := decodeDev16(member.Node); err != nil {
			return false
		}
		if _, err := decodeDev16(member.Store); err != nil {
			return false
		}
		if _, duplicate := nodes[member.Node]; duplicate {
			return false
		}
		nodes[member.Node] = struct{}{}
		if _, duplicate := stores[member.Store]; duplicate {
			return false
		}
		stores[member.Store] = struct{}{}
		for _, address := range [...]string{member.Peer, member.Native, member.Snapshot, member.Control} {
			if !validDevLoopbackAddress(address) {
				return false
			}
			if _, duplicate := addresses[address]; duplicate {
				return false
			}
			addresses[address] = struct{}{}
		}
	}
	for _, p := range paths {
		if !filepath.IsAbs(p) || p != filepath.Clean(p) || !strings.HasPrefix(p, root+string(filepath.Separator)) {
			return false
		}
	}
	return true
}

func validDevLoopbackAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		return false
	}
	value, err := strconv.Atoi(port)
	return err == nil && value > 0 && value <= 65535 && strconv.Itoa(value) == port
}
func decodeDev16(value string) ([16]byte, error) {
	var out [16]byte
	if len(value) != 32 {
		return out, errDevCluster
	}
	_, err := hex.Decode(out[:], []byte(value))
	if err == nil && out == ([16]byte{}) {
		err = errDevCluster
	}
	return out, err
}
func runDevCommand(binary string, args ...string) error {
	command := exec.Command(binary, args...)
	diagnostic := &devDiagnostics{maximum: 64 << 10}
	command.Stdout = diagnostic
	command.Stderr = diagnostic
	if err := command.Run(); err != nil {
		return errors.Join(err, errors.New(diagnostic.String()))
	}
	return nil
}
func writeDevExclusive(path string, data []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	_, werr := file.Write(data)
	serr := file.Sync()
	cerr := file.Close()
	return errors.Join(werr, serr, cerr)
}
func readDevFile(path string, max int) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > int64(max) {
		return nil, errors.Join(errDevCluster, err)
	}
	raw := make([]byte, info.Size())
	_, err = io.ReadFull(file, raw)
	return raw, err
}
func syncDevDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
