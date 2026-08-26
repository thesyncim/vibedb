//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"encoding/asn1"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	rf3CommandHelperEnvironment   = "VIBEDB_RF3_COMMAND_HELPER"
	rf3CommandManifestEnvironment = "VIBEDB_RF3_COMMAND_MANIFEST"
	rf3CommandPeerListenerFD      = 3
	rf3CommandControlListenerFD   = 4
	rf3CommandNativeListenerFD    = 5
	rf3CommandMembers             = 3
	rf3CommandDiagnosticBytes     = 64 << 10
)

var rf3CommandIdentityOID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}

func TestRF3CommandProcessDocuments(t *testing.T) {
	nodes := rf3CommandNodes()
	addresses := [rf3CommandMembers]string{
		"127.0.0.1:17401", "127.0.0.1:17402", "127.0.0.1:17403",
	}
	document := rf3CommandManifestDocument(
		"member.wal", "member.vdb", "sql-identity.json", "apply-identity.json",
		"wal-key", "127.0.0.1:17401", "127.0.0.1:17501", "127.0.0.1:17601", "127.0.0.1:17701",
		rf3testfixture.Credential{Certificate: "member-cert.pem", Key: "member-key.pem"},
		"roots.pem", "authorization-policy.vibejson",
		raftstore.Options{
			MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
			MaxRecords: 4096, MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes,
		},
		nodes, addresses,
	)
	manifest, err := parseRF3Manifest(document)
	if err != nil {
		t.Fatalf("generated manifest: %v\n%s", err, document)
	}
	if err := validateRF3Addresses(manifest); err != nil {
		t.Fatalf("generated manifest addresses: %v", err)
	}
	policy, err := serviceauthz.Load(rf3CommandPolicy(nodes))
	if err != nil {
		t.Fatalf("generated authorization policy: %v", err)
	}
	if policy.Generation() != rf3CommandAuthority().ActivePolicyGeneration ||
		len(policy.NodesWith(serviceauthz.CapabilityDelegate)) != rf3CommandMembers ||
		len(policy.NodesWith(serviceauthz.CapabilityTopology)) != rf3CommandMembers {
		t.Fatalf("generated authorization policy does not admit the exact RF3 roster")
	}
}

// TestServeRF3ShippedCompositionThreeProcesses is deliberately smaller than
// the RF3 fault matrix. It proves only the shipped composition boundary: three
// isolated command processes open prepared catalog members, publish both
// authenticated listeners, naturally elect one leader, answer authenticated
// native probes, and retire cleanly on SIGTERM.
func TestServeRF3ShippedCompositionThreeProcesses(t *testing.T) {
	if os.Getenv(rf3CommandHelperEnvironment) != "" {
		return
	}
	root := t.TempDir()
	peerListeners := make([]*net.TCPListener, rf3CommandMembers)
	nativeListeners := make([]*net.TCPListener, rf3CommandMembers)
	controlListeners := make([]*net.TCPListener, rf3CommandMembers)
	var peerAddresses, nativeAddresses, controlAddresses [rf3CommandMembers]string
	for index := 0; index < rf3CommandMembers; index++ {
		var err error
		peerListeners[index], err = net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		nativeListeners[index], err = net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		controlListeners[index], err = net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		peerAddresses[index] = peerListeners[index].Addr().String()
		nativeAddresses[index] = nativeListeners[index].Addr().String()
		controlAddresses[index] = controlListeners[index].Addr().String()
	}
	defer func() {
		for index := range peerListeners {
			if peerListeners[index] != nil {
				_ = peerListeners[index].Close()
			}
			if nativeListeners[index] != nil {
				_ = nativeListeners[index].Close()
			}
			if controlListeners[index] != nil {
				_ = controlListeners[index].Close()
			}
		}
	}()

	nodes := rf3CommandNodes()
	group := rf3CommandGroup()
	credentials, roots, err := rf3testfixture.WriteCredentials(
		root, rf3CommandIdentityOID,
		rafttransport.TrustDomain{
			ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		},
		nodes[:],
	)
	if err != nil {
		t.Fatal(err)
	}
	policyPath := filepath.Join(root, "authorization-policy.vibejson")
	if err := os.WriteFile(policyPath, rf3CommandPolicy(nodes), 0o600); err != nil {
		t.Fatal(err)
	}
	keyMaterial := make([]byte, 32)
	for index := range keyMaterial {
		keyMaterial[index] = byte(index + 1)
	}
	walOptions := raftstore.Options{
		MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
		MaxRecords: 4096, MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes,
	}
	authority := rf3CommandAuthority()
	manifestPaths := make([]string, rf3CommandMembers)
	for index := 0; index < rf3CommandMembers; index++ {
		memberRoot := filepath.Join(root, fmt.Sprintf("member-%d", index+1))
		if err := os.MkdirAll(memberRoot, 0o700); err != nil {
			t.Fatal(err)
		}
		identity := rf3CommandStoreIdentity(uint64(index + 1))
		key := raftstore.Key{ID: "rf3-command-key", Wrapped: []byte("explicit-test-wrapped-key")}
		copy(key.Material[:], keyMaterial)
		prepared, prepareErr := rf3testfixture.PrepareMember(rf3testfixture.MemberOptions{
			Root: memberRoot, Table: gateway.ReplicatedCatalogTable,
			CreateTable: `CREATE TABLE controlplane (PRIMARY KEY (id))`,
			Identity:    identity, Key: key, WAL: walOptions,
			Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
			Authority: authority,
			Apply: sqldriver.ReplicatedApplyOptions{
				MaxSessions: 32, RetryWindow: 8,
				TxnLimits: durable.TxnLimits{
					MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20,
				},
				Placement: sqldriver.ReplicatedPlacementProfile{
					Format:        sqldriver.ReplicatedPlacementProfileFormat,
					ShardKey:      gateway.ReplicatedCatalogPrimaryKey,
					TupleVersion:  distribution.CurrentTupleVersion,
					MapperVersion: distribution.NativeMapperVersion,
					Range:         distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
				},
			},
		})
		if errors.Is(prepareErr, storeio.ErrStrictAllocationUnsupported) ||
			errors.Is(prepareErr, raftstore.ErrPlatformUnsupported) {
			t.Skipf("RF3 strict durable allocation unsupported: %v", prepareErr)
		}
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		t.Cleanup(func() { _ = prepared.Close() })
		basePath := filepath.Join(memberRoot, "sql-identity.json")
		applyPath := filepath.Join(memberRoot, "apply-identity.json")
		keyPath := filepath.Join(memberRoot, "wal-key")
		writeRF3CommandIdentity(t, basePath, prepared.Base)
		writeRF3CommandIdentity(t, applyPath, prepared.ApplyIdentity)
		if err := os.WriteFile(keyPath, keyMaterial, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := prepared.Close(); err != nil {
			t.Fatal(err)
		}
		manifestPaths[index] = filepath.Join(memberRoot, "serve-rf3.json")
		document := rf3CommandManifestDocument(
			prepared.WALPath, prepared.SQLPath, basePath, applyPath, keyPath,
			peerAddresses[index], nativeAddresses[index], "127.0.0.1:"+strconv.Itoa(17601+index),
			controlAddresses[index], credentials[index], roots,
			policyPath, walOptions, nodes, peerAddresses,
		)
		if err := os.WriteFile(manifestPaths[index], document, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	children := make([]*rf3CommandChild, rf3CommandMembers)
	defer closeRF3CommandChildren(t, children)
	for index := 0; index < rf3CommandMembers; index++ {
		peerFile, err := peerListeners[index].File()
		if err != nil {
			t.Fatal(err)
		}
		nativeFile, err := nativeListeners[index].File()
		if err != nil {
			_ = peerFile.Close()
			t.Fatal(err)
		}
		controlFile, err := controlListeners[index].File()
		if err != nil {
			_ = peerFile.Close()
			_ = nativeFile.Close()
			t.Fatal(err)
		}
		diagnostic := &rf3CommandDiagnostic{maximum: rf3CommandDiagnosticBytes}
		command := exec.Command(
			executable, "-test.run=^TestServeRF3CommandProcessHelper$", "-test.v",
		)
		command.Env = append(os.Environ(),
			rf3CommandHelperEnvironment+"=1",
			rf3CommandManifestEnvironment+"="+manifestPaths[index],
		)
		command.ExtraFiles = []*os.File{peerFile, controlFile, nativeFile}
		command.Stdout, command.Stderr = diagnostic, diagnostic
		if err := command.Start(); err != nil {
			_ = peerFile.Close()
			_ = nativeFile.Close()
			_ = controlFile.Close()
			t.Fatal(err)
		}
		_ = peerFile.Close()
		_ = nativeFile.Close()
		_ = controlFile.Close()
		_ = peerListeners[index].Close()
		_ = nativeListeners[index].Close()
		_ = controlListeners[index].Close()
		peerListeners[index], nativeListeners[index], controlListeners[index] = nil, nil, nil
		child := &rf3CommandChild{
			member: uint64(index + 1), command: command, exited: make(chan struct{}),
			diagnostic: diagnostic,
		}
		children[index] = child
		go func() {
			err := command.Wait()
			child.mu.Lock()
			child.waitErr = err
			child.mu.Unlock()
			close(child.exited)
		}()
	}

	for _, child := range children {
		waitRF3CommandReady(t, child, 30*time.Second)
	}
	clientProfiles := make([]*rafttransport.PeerTLS, rf3CommandMembers)
	for index := range clientProfiles {
		clientProfiles[index], err = servicetls.LoadProfile(
			credentials[index].Certificate, credentials[index].Key, roots,
			"1.3.6.1.4.1.32473.1.1", time.Now,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	waitRF3CommandLeader(
		t, nativeAddresses, nodes, clientProfiles, group,
		rf3CommandStoreIdentity(1).AllocationGeneration, authority.ActivePolicyGeneration,
	)

	for _, child := range children {
		if err := child.command.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("signal member %d: %v", child.member, err)
		}
	}
	for _, child := range children {
		select {
		case <-child.exited:
			child.mu.Lock()
			waitErr := child.waitErr
			child.mu.Unlock()
			if waitErr != nil {
				t.Fatalf("member %d exit: %v\n%s", child.member, waitErr, child.diagnostic.String())
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("member %d did not shut down", child.member)
		}
	}
}

func TestServeRF3CommandProcessHelper(t *testing.T) {
	if os.Getenv(rf3CommandHelperEnvironment) != "1" {
		return
	}
	manifest, err := loadRF3Manifest(os.Getenv(rf3CommandManifestEnvironment))
	if err != nil {
		t.Fatal(err)
	}
	peer, err := inheritedRF3CommandListener(rf3CommandPeerListenerFD, "rf3-command-peer")
	if err != nil {
		t.Fatal(err)
	}
	control, err := inheritedRF3CommandListener(rf3CommandControlListenerFD, "rf3-command-control")
	if err != nil {
		_ = peer.Close()
		t.Fatal(err)
	}
	native, err := inheritedRF3CommandListener(rf3CommandNativeListenerFD, "rf3-command-native")
	if err != nil {
		_ = peer.Close()
		_ = control.Close()
		t.Fatal(err)
	}
	listeners := []net.Listener{peer, control, native}
	next := 0
	listen := func(network, address string) (net.Listener, error) {
		if network != "tcp" || next >= len(listeners) || listeners[next].Addr().String() != address {
			return nil, fmt.Errorf("unexpected inherited listener %s %s", network, address)
		}
		listener := listeners[next]
		next++
		return listener, nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := servePreparedRF3WithListen(ctx, manifest, listen); err != nil {
		t.Fatal(err)
	}
}

type rf3CommandChild struct {
	member     uint64
	command    *exec.Cmd
	exited     chan struct{}
	diagnostic *rf3CommandDiagnostic
	mu         sync.Mutex
	waitErr    error
}

type rf3CommandDiagnostic struct {
	mu      sync.Mutex
	data    []byte
	maximum int
}

func (diagnostic *rf3CommandDiagnostic) Write(data []byte) (int, error) {
	diagnostic.mu.Lock()
	defer diagnostic.mu.Unlock()
	if remaining := diagnostic.maximum - len(diagnostic.data); remaining > 0 {
		diagnostic.data = append(diagnostic.data, data[:min(len(data), remaining)]...)
	}
	return len(data), nil
}

func (diagnostic *rf3CommandDiagnostic) String() string {
	diagnostic.mu.Lock()
	defer diagnostic.mu.Unlock()
	return string(bytes.Clone(diagnostic.data))
}

func inheritedRF3CommandListener(descriptor uintptr, name string) (net.Listener, error) {
	file := os.NewFile(descriptor, name)
	if file == nil {
		return nil, errors.New("missing inherited RF3 command listener")
	}
	listener, err := net.FileListener(file)
	return listener, errors.Join(err, file.Close())
}

func waitRF3CommandReady(t testing.TB, child *rf3CommandChild, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(child.diagnostic.String(), "vibedb-shard RF3 ready") {
			return
		}
		select {
		case <-child.exited:
			t.Fatalf("member %d exited before readiness\n%s", child.member, child.diagnostic.String())
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("member %d readiness timeout\n%s", child.member, child.diagnostic.String())
}

func waitRF3CommandLeader(
	t testing.TB,
	addresses [rf3CommandMembers]string,
	nodes [rf3CommandMembers]rafttransport.NodeID,
	profiles []*rafttransport.PeerTLS,
	group raftmember.GroupKey,
	allocation, generation uint64,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var observed [rf3CommandMembers]string
	for ctx.Err() == nil {
		leader := uint64(0)
		consistent := true
		for index := 0; index < rf3CommandMembers; index++ {
			client := (index + 1) % rf3CommandMembers
			state, err := probeRF3CommandMember(
				ctx, addresses[index], nodes[index], profiles[client], nodes[client],
				group, allocation, generation,
			)
			if err != nil {
				observed[index] = err.Error()
				consistent = false
				break
			}
			observed[index] = fmt.Sprintf(
				"term=%d leader=%d commit=%d applied=%d",
				state.Fence.Term, state.LeaderID, state.Commit, state.Applied,
			)
			if state.LeaderID == 0 || leader != 0 && state.LeaderID != leader {
				consistent = false
				break
			}
			leader = state.LeaderID
		}
		if consistent && leader != 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("natural RF3 leader unavailable: %v; observed=%q", ctx.Err(), observed)
}

func probeRF3CommandMember(
	ctx context.Context,
	address string,
	serverNode rafttransport.NodeID,
	profile *rafttransport.PeerTLS,
	authorityNode rafttransport.NodeID,
	group raftmember.GroupKey,
	allocation, generation uint64,
) (shardservice.ReplicatedMemberState, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(probeCtx, "tcp", address)
	if err != nil {
		return shardservice.ReplicatedMemberState{}, err
	}
	deadline := func() time.Time { return time.Now().Add(3 * time.Second) }
	connection, err := profile.Client(
		probeCtx, raw, serverNode, rafttransport.TrafficShardNative, deadline,
	)
	if err != nil {
		_ = raw.Close()
		return shardservice.ReplicatedMemberState{}, err
	}
	defer connection.Close()
	response, err := shardservice.RoundTripReplicated(
		probeCtx, connection, &shardservice.ReplicatedRequest{
			Operation:  shardservice.ReplicatedProbe,
			Authority:  serviceauthz.Authority{Node: authorityNode, Generation: generation},
			Capability: serviceauthz.CapabilityTopology,
			Fence: shardservice.ReplicatedFence{
				Group: group, AllocationGeneration: allocation,
			},
		},
	)
	if err != nil {
		return shardservice.ReplicatedMemberState{}, err
	}
	if response == nil || response.Kind != shardservice.ReplicatedHandshake || !response.HasState {
		return shardservice.ReplicatedMemberState{}, fmt.Errorf("unexpected probe response %+v", response)
	}
	return response.State, nil
}

func closeRF3CommandChildren(t testing.TB, children []*rf3CommandChild) {
	t.Helper()
	for _, child := range children {
		if child == nil {
			continue
		}
		select {
		case <-child.exited:
			continue
		default:
		}
		_ = child.command.Process.Signal(syscall.SIGTERM)
		select {
		case <-child.exited:
		case <-time.After(10 * time.Second):
			_ = child.command.Process.Kill()
			<-child.exited
			t.Errorf("member %d required forced cleanup\n%s", child.member, child.diagnostic.String())
		}
	}
}

func writeRF3CommandIdentity(t testing.TB, path string, identity interface{ MarshalJSON() ([]byte, error) }) {
	t.Helper()
	data, err := identity.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func rf3CommandManifestDocument(
	walPath, sqlPath, basePath, applyPath, keyPath, peerAddress, nativeAddress, snapshotAddress, controlAddress string,
	credential rf3testfixture.Credential,
	roots, policyPath string,
	options raftstore.Options,
	nodes [rf3CommandMembers]rafttransport.NodeID,
	peerAddresses [rf3CommandMembers]string,
) []byte {
	dataRoot := filepath.Dir(sqlPath)
	if absolute, err := filepath.Abs(dataRoot); err == nil {
		dataRoot = absolute
	}
	return []byte(fmt.Sprintf(`{"wal":{"path":%q,"key_id":"rf3-command-key","key_material_path":%q,"max_file_bytes":%d,"max_record_bytes":%d,"max_records":%d,"max_entries":%d,"max_live_bytes":%d},"sql":{"path":%q,"identity_path":%q,"apply_identity_path":%q},"listeners":{"peer":%q,"native":%q,"snapshot":%q,"control":%q},"tls":{"certificate":%q,"key":%q,"roots":%q,"identity_oid":"1.3.6.1.4.1.32473.1.1"},"authorization_policy":%q,"replica_control":{"action_journal_path":%q,"max_action_records":4096,"source_data_root":%q,"source_journal_path":%q,"max_source_records":4096,"source_repository_path":%q,"max_source_artifacts":8,"max_source_concurrent":2,"max_source_artifact_bytes":1073741824,"max_source_disk_bytes":4294967296,"source_chunk_bytes":1048576},"members":[{"member_id":1,"node_id":"%x","peer_address":%q},{"member_id":2,"node_id":"%x","peer_address":%q},{"member_id":3,"node_id":"%x","peer_address":%q}]}`,
		walPath, keyPath,
		options.MaxFileBytes, options.MaxRecordBytes, options.MaxRecords,
		options.MaxEntries, options.MaxLiveBytes,
		sqlPath, basePath, applyPath, peerAddress, nativeAddress, snapshotAddress, controlAddress,
		credential.Certificate, credential.Key, roots, policyPath,
		filepath.Join(dataRoot, "replica-actions"), dataRoot,
		filepath.Join(dataRoot, "source-exports"), filepath.Join(dataRoot, "source-artifacts"),
		nodes[0], peerAddresses[0], nodes[1], peerAddresses[1], nodes[2], peerAddresses[2],
	))
}

func rf3CommandPolicy(nodes [rf3CommandMembers]rafttransport.NodeID) []byte {
	return []byte(fmt.Sprintf(
		`{"generation":5,"principals":[{"node":"%x","capabilities":["delegate","membership","topology"]},{"node":"%x","capabilities":["delegate","membership","topology"]},{"node":"%x","capabilities":["delegate","membership","topology"]}]}`,
		nodes[0], nodes[1], nodes[2],
	))
}

func rf3CommandNodes() (nodes [rf3CommandMembers]rafttransport.NodeID) {
	for member := range nodes {
		for index := range nodes[member] {
			nodes[member][index] = byte((member+1)*17 + index)
		}
	}
	return nodes
}

func rf3CommandStoreIdentity(member uint64) raftstore.Identity {
	identity := raftstore.Identity{
		Distribution:         string(gateway.ReplicatedCatalogDistribution),
		Shard:                string(gateway.ReplicatedCatalogShard),
		AllocationGeneration: 23, MemberID: member,
	}
	for index := range identity.ClusterID {
		identity.ClusterID[index] = byte(index + 1)
		identity.ClusterIncarnation[index] = byte(index + 21)
		identity.ShardIncarnation[index] = byte(index + 141)
		identity.GroupID[index] = byte(index + 161)
		identity.StoreID[index] = byte(index+181) ^ byte(member)
	}
	return identity
}

func rf3CommandGroup() raftmember.GroupKey {
	identity := rf3CommandStoreIdentity(1)
	return raftmember.GroupKey{
		ClusterID: identity.ClusterID, ClusterIncarnation: identity.ClusterIncarnation,
		TopologyRecoveryEpoch: 3, ShardIncarnation: identity.ShardIncarnation,
		GroupID: identity.GroupID,
	}
}

func rf3CommandAuthority() sqldriver.ReplicatedAuthorityProfile {
	return sqldriver.ReplicatedAuthorityProfile{
		ActivePolicyGeneration: 5, ProtectionEpoch: 7, OwnershipEpoch: 11,
		SchemaGeneration: 13, RoutingVersion: 17, RouteGeneration: 19,
	}
}
