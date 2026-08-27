//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
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
	"github.com/thesyncim/vibedb/internal/storeio"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	rf3ColdTargetHelperEnvironment    = "VIBEDB_RF3_COLD_TARGET_HELPER"
	rf3ColdTargetBootstrapEnvironment = "VIBEDB_RF3_COLD_TARGET_BOOTSTRAP"
)

// rf3ColdTargetOptions is the complete cold target boundary needed by the
// shipped RF3 process test. The stable voters remain the member manifest's
// transport roster; Target is retained separately and does not serve until a
// certified learner snapshot has been installed.
type rf3ColdTargetOptions struct {
	Root                  string
	Group                 raftmember.GroupKey
	Authority             sqldriver.ReplicatedAuthorityProfile
	WAL                   raftstore.Options
	Apply                 sqldriver.ReplicatedApplyOptions
	Key                   raftstore.Key
	Credential            rf3testfixture.Credential
	Roots                 string
	AuthorizationPolicy   string
	ServingNodes          [rf3CommandMembers]rafttransport.NodeID
	ServingPeerAddresses  [rf3CommandMembers]string
	Target                rf3ManifestEnrolledTarget
	Listeners             rf3ManifestListeners
	SourceNode            rafttransport.NodeID
	SourceSnapshotAddress string
	MaxArtifactBytes      uint64
}

// rf3ColdTargetProcess owns the prepared no-WAL target and its child process.
// Paths are exposed so the end-to-end test can assert the cold and installed
// durability boundaries without duplicating preparation details.
type rf3ColdTargetProcess struct {
	WALPath               string
	SQLPath               string
	MemberManifestPath    string
	BootstrapManifestPath string
	StaticBootstrapPath   string

	member     uint64
	command    *exec.Cmd
	exited     chan struct{}
	diagnostic *rf3CommandDiagnostic
	mu         sync.Mutex
	waitErr    error
}

func prepareRF3ColdTarget(t testing.TB, options rf3ColdTargetOptions) *rf3ColdTargetProcess {
	t.Helper()
	if options.Root == "" || options.Target.MemberID == 0 || options.Target.NodeID == (rafttransport.NodeID{}) ||
		options.Target.StoreID == ([16]byte{}) || options.Target.NodeIncarnation == 0 ||
		options.SourceNode == (rafttransport.NodeID{}) || options.Key.ID != "rf3-command-key" {
		t.Fatal("invalid RF3 cold target options")
	}
	if options.MaxArtifactBytes == 0 {
		options.MaxArtifactBytes = 1 << 30
	}
	if err := os.MkdirAll(options.Root, 0o700); err != nil {
		t.Fatal(err)
	}

	identity := rf3CommandStoreIdentity(options.Target.MemberID)
	identity.ClusterID = options.Group.ClusterID
	identity.ClusterIncarnation = options.Group.ClusterIncarnation
	identity.ShardIncarnation = options.Group.ShardIncarnation
	identity.GroupID = options.Group.GroupID
	identity.StoreID = options.Target.StoreID
	staticBootstrap := rf3testfixture.InitialBootstrap([]uint64{1, 2, 3})
	staticBootstrap.TopologyRecoveryEpoch = options.Group.TopologyRecoveryEpoch
	// The transient preparation WAL must contain its local member. Its only job
	// is binding SQL identities, so prepare it as the learner beside the stable
	// RF3; the separately retained static bootstrap remains the original voter
	// cut required by the certified learner installer.
	preparationBootstrap := raftstore.Bootstrap{
		TopologyRecoveryEpoch: staticBootstrap.TopologyRecoveryEpoch,
		Snapshot:              proto.Clone(staticBootstrap.Snapshot).(*pb.Snapshot),
	}
	preparationBootstrap.Snapshot.Metadata.ConfState.Learners = []uint64{options.Target.MemberID}
	prepared, err := rf3testfixture.PrepareMember(rf3testfixture.MemberOptions{
		Root: options.Root, Table: gateway.ReplicatedCatalogTable,
		CreateTable: `CREATE TABLE controlplane (PRIMARY KEY (id))`,
		Identity:    identity, Key: options.Key, WAL: options.WAL,
		Bootstrap: preparationBootstrap, Authority: options.Authority, Apply: options.Apply,
	})
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) ||
		errors.Is(err, raftstore.ErrPlatformUnsupported) {
		t.Skipf("RF3 strict durable allocation unsupported: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	prepareRF3CommandSplitRuntime(t, options.Root, staticBootstrap)
	basePath := filepath.Join(options.Root, "sql-identity.json")
	applyPath := filepath.Join(options.Root, "apply-identity.json")
	keyPath := filepath.Join(options.Root, "wal-key")
	writeRF3CommandIdentity(t, basePath, prepared.Base)
	writeRF3CommandIdentity(t, applyPath, prepared.ApplyIdentity)
	if err = os.WriteFile(keyPath, options.Key.Material[:], 0o600); err != nil {
		_ = prepared.Close()
		t.Fatal(err)
	}
	if err = prepared.Close(); err != nil {
		t.Fatal(err)
	}
	// PrepareMember is used only to bind the exact SQL and retained identities.
	// Removing its transient WAL is what makes bootstrapPreparedRF3 enter the
	// non-serving cold-target path in the child process.
	if err = os.Remove(prepared.WALPath); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(prepared.WALPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cold target WAL still exists: %v", err)
	}

	staticPath := filepath.Join(options.Root, "static-bootstrap.pb")
	staticRaw, err := proto.MarshalOptions{Deterministic: true}.Marshal(staticBootstrap.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(staticPath, staticRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	memberPath := filepath.Join(options.Root, "serve-rf3.json")
	document := rf3CommandManifestDocument(
		prepared.WALPath, prepared.SQLPath, basePath, applyPath, keyPath,
		options.Listeners.Peer, options.Listeners.Native, options.Listeners.Snapshot,
		options.Listeners.Control, options.Credential, options.Roots,
		options.AuthorizationPolicy, options.WAL, options.ServingNodes,
		options.ServingPeerAddresses,
		walIdentityFromBinding(prepared.Base.Binding), prepared.Base.Binding.TopologyRecoveryEpoch,
	)
	document = rf3CommandEnrollTarget(
		document, options.Target.NodeID, options.Target.StoreID,
		options.Target.NodeIncarnation, options.Target.PeerAddress,
		options.Target.NativeAddress, options.Target.SnapshotAddress,
		options.Target.ControlAddress,
	)
	if err = os.WriteFile(memberPath, document, 0o600); err != nil {
		t.Fatal(err)
	}
	bootstrapPath := filepath.Join(options.Root, "bootstrap-rf3.json")
	bootstrapDocument := []byte(fmt.Sprintf(
		`{"member_manifest":%q,"control_listener":%q,"source_node":"%x","source_snapshot_address":%q,"repository_path":%q,"cursor_path":%q,"journal_path":%q,"static_bootstrap_path":%q,"max_artifact_bytes":%d}`,
		memberPath, options.Listeners.Control, options.SourceNode,
		options.SourceSnapshotAddress, filepath.Join(options.Root, "target-artifacts"),
		filepath.Join(options.Root, "snapshot-cursor"),
		filepath.Join(options.Root, "bootstrap-journal"), staticPath,
		options.MaxArtifactBytes,
	))
	if err = os.WriteFile(bootstrapPath, bootstrapDocument, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = loadBootstrapRF3Manifest(bootstrapPath); err != nil {
		t.Fatalf("generated cold target bootstrap manifest: %v", err)
	}

	return &rf3ColdTargetProcess{
		WALPath: prepared.WALPath, SQLPath: prepared.SQLPath,
		MemberManifestPath: memberPath, BootstrapManifestPath: bootstrapPath,
		StaticBootstrapPath: staticPath, member: options.Target.MemberID,
	}
}

func (process *rf3ColdTargetProcess) Start(t testing.TB) {
	t.Helper()
	if process == nil || process.command != nil || process.BootstrapManifestPath == "" {
		t.Fatal("invalid RF3 cold target process start")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := &rf3CommandDiagnostic{maximum: rf3CommandDiagnosticBytes}
	command := exec.Command(executable, "-test.run=^TestBootstrapRF3CommandProcessHelper$", "-test.v")
	command.Env = append(os.Environ(),
		rf3ColdTargetHelperEnvironment+"=1",
		rf3ColdTargetBootstrapEnvironment+"="+process.BootstrapManifestPath,
	)
	command.Stdout, command.Stderr = diagnostic, diagnostic
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	process.command, process.exited, process.diagnostic = command, make(chan struct{}), diagnostic
	go func() {
		waitErr := command.Wait()
		process.mu.Lock()
		process.waitErr = waitErr
		process.mu.Unlock()
		close(process.exited)
	}()
}

func (process *rf3ColdTargetProcess) WaitColdReady(t testing.TB) {
	t.Helper()
	process.waitReady(t, "vibedb-shard RF3 cold bootstrap ready", 30*time.Second)
}

func (process *rf3ColdTargetProcess) WaitServingReady(t testing.TB) {
	t.Helper()
	process.waitReady(t, "vibedb-shard RF3 ready", 30*time.Second)
}

func (process *rf3ColdTargetProcess) Restart(t testing.TB) {
	t.Helper()
	process.Close(t)
	process.mu.Lock()
	process.command, process.exited, process.diagnostic = nil, nil, nil
	process.waitErr = nil
	process.mu.Unlock()
	process.Start(t)
}

func (process *rf3ColdTargetProcess) waitReady(t testing.TB, marker string, timeout time.Duration) {
	t.Helper()
	if process == nil || process.command == nil {
		t.Fatal("RF3 cold target process was not started")
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(process.diagnostic.String(), marker) {
			return
		}
		select {
		case <-process.exited:
			t.Fatalf("member %d exited before %q\n%s", process.member, marker, process.diagnostic.String())
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("member %d readiness timeout waiting for %q\n%s", process.member, marker, process.diagnostic.String())
}

func (process *rf3ColdTargetProcess) Close(t testing.TB) {
	t.Helper()
	if process == nil || process.command == nil {
		return
	}
	select {
	case <-process.exited:
	default:
		_ = process.command.Process.Signal(syscall.SIGTERM)
		select {
		case <-process.exited:
		case <-time.After(10 * time.Second):
			_ = process.command.Process.Kill()
			<-process.exited
			t.Errorf("member %d required forced cleanup\n%s", process.member, process.diagnostic.String())
		}
	}
	process.mu.Lock()
	waitErr := process.waitErr
	process.mu.Unlock()
	if waitErr != nil {
		t.Errorf("member %d exit: %v\n%s", process.member, waitErr, process.diagnostic.String())
	}
}

func TestBootstrapRF3CommandProcessHelper(t *testing.T) {
	if os.Getenv(rf3ColdTargetHelperEnvironment) != "1" {
		return
	}
	bootstrap, err := loadBootstrapRF3Manifest(os.Getenv(rf3ColdTargetBootstrapEnvironment))
	if err != nil {
		t.Fatal(err)
	}
	member, err := loadRF3Manifest(bootstrap.MemberManifest)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err = bootstrapPreparedRF3(ctx, bootstrap, member); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareRF3ColdTargetProcessHarness(t *testing.T) {
	if os.Getenv(rf3ColdTargetHelperEnvironment) != "" || os.Getenv(rf3CommandHelperEnvironment) != "" {
		return
	}
	root := t.TempDir()
	nodes := rf3CommandNodes()
	group := rf3CommandGroup()
	targetNode := rafttransport.NodeID{0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78,
		0x79, 0x7a, 0x7b, 0x7c, 0x7d, 0x7e, 0x7f, 0x80}
	credentials, roots, err := rf3testfixture.WriteCredentials(
		root, rf3CommandIdentityOID,
		rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation},
		append(append([]rafttransport.NodeID(nil), nodes[:]...), targetNode),
	)
	if err != nil {
		t.Fatal(err)
	}
	policyPath := filepath.Join(root, "authorization-policy.vibejson")
	if err = os.WriteFile(policyPath, rf3CommandPolicyWithTarget(nodes, targetNode), 0o600); err != nil {
		t.Fatal(err)
	}
	addresses := make([]string, 7)
	for index := range addresses {
		listener, listenErr := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
		if listenErr != nil {
			t.Fatal(listenErr)
		}
		addresses[index] = listener.Addr().String()
		_ = listener.Close()
	}
	var peers [rf3CommandMembers]string
	copy(peers[:], addresses[:3])
	target := rf3ManifestEnrolledTarget{
		MemberID: 4, NodeID: targetNode,
		StoreID: [16]byte{0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88,
			0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f, 0x90},
		NodeIncarnation: 9, PeerAddress: addresses[3], NativeAddress: addresses[4],
		SnapshotAddress: addresses[5], ControlAddress: addresses[6],
	}
	key := raftstore.Key{ID: "rf3-command-key", Wrapped: []byte("explicit-test-wrapped-key")}
	for index := range key.Material {
		key.Material[index] = byte(index + 1)
	}
	wal := raftstore.Options{
		MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
		MaxRecords: 4096, MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes,
	}
	apply := sqldriver.ReplicatedApplyOptions{
		MaxSessions: 32, RetryWindow: 8,
		TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20},
		Placement: sqldriver.ReplicatedPlacementProfile{
			Format:        sqldriver.ReplicatedPlacementProfileFormat,
			ShardKey:      gateway.ReplicatedCatalogPrimaryKey,
			TupleVersion:  distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			Range:         distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
	}
	process := prepareRF3ColdTarget(t, rf3ColdTargetOptions{
		Root: filepath.Join(root, "target"), Group: group, Authority: rf3CommandAuthority(),
		WAL: wal, Apply: apply, Key: key, Credential: credentials[3], Roots: roots,
		AuthorizationPolicy: policyPath, ServingNodes: nodes, ServingPeerAddresses: peers,
		Target: target, Listeners: rf3ManifestListeners{
			Peer: target.PeerAddress, Native: target.NativeAddress,
			Snapshot: target.SnapshotAddress, Control: target.ControlAddress,
		}, SourceNode: nodes[0], SourceSnapshotAddress: "127.0.0.1:29999",
	})
	if _, err = os.Stat(process.WALPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("prepared cold target WAL = %v", err)
	}
	process.Start(t)
	defer process.Close(t)
	process.WaitColdReady(t)
}
