//go:build darwin || linux

package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibedb/internal/storeio"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

// The operator contacts both source and learner; it cannot borrow either
// serving identity because authenticated control streams reject self-peers.
func rf3EnrolledGroupOperatorCredentials(t testing.TB, root string,
	nodes [rf3CommandMembers]rafttransport.NodeID, target rafttransport.NodeID,
) ([]rf3testfixture.Credential, string, string, *rafttransport.PeerTLS) {
	t.Helper()
	// Keep this actor after the fixture's 0xd1 learner in canonical policy order.
	operator := rafttransport.NodeID{0xe1, 1}
	credentials, roots, err := rf3testfixture.WriteCredentials(root, rf3CommandIdentityOID,
		rafttransport.TrustDomain{ClusterID: rf3CommandGroup().ClusterID, ClusterIncarnation: rf3CommandGroup().ClusterIncarnation},
		append(append([]rafttransport.NodeID(nil), nodes[:]...), target, operator))
	if err != nil {
		t.Fatal(err)
	}
	policyPath := filepath.Join(root, "policy.vibejson")
	if err := os.WriteFile(policyPath, rf3CommandPolicyWithTarget(nodes, target, operator), 0o600); err != nil {
		t.Fatal(err)
	}
	credential := credentials[rf3CommandMembers+1]
	profile, err := servicetls.LoadProfile(credential.Certificate, credential.Key, roots,
		rf3testfixture.ProcessIdentityOID, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if profile.LocalIdentity().Node != operator {
		t.Fatal("enrollment operator credential names a different actor")
	}
	return credentials, roots, policyPath, profile
}

func TestRF3EnrolledGroupOperatorAuthentication(t *testing.T) {
	nodes, target := rf3CommandNodes(), rafttransport.NodeID{0xd1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	credentials, roots, policyPath, operator := rf3EnrolledGroupOperatorCredentials(t, t.TempDir(), nodes, target)
	policy, err := serviceauthz.LoadFile(policyPath)
	if err != nil {
		t.Fatal(err)
	}
	if policy.Generation() != rf3CommandAuthority().ActivePolicyGeneration ||
		policy.Check(operator.LocalIdentity().Node, serviceauthz.CapabilityTopology) != serviceauthz.DecisionAllow ||
		policy.Check(rafttransport.NodeID{0xff}, serviceauthz.CapabilityTopology) == serviceauthz.DecisionAllow {
		t.Fatal("enrollment policy does not bind the explicit operator")
	}
	for _, peer := range []struct {
		name       string
		node       rafttransport.NodeID
		credential int
	}{{"source", nodes[0], 0}, {"learner", target, rf3CommandMembers}} {
		t.Run(peer.name, func(t *testing.T) {
			credential := credentials[peer.credential]
			profile, err := servicetls.LoadProfile(credential.Certificate, credential.Key, roots,
				rf3testfixture.ProcessIdentityOID, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			if profile.LocalIdentity().Node != peer.node || operator.LocalIdentity().Node == peer.node {
				t.Fatal("operator aliases a serving identity")
			}
			for _, self := range []bool{false, true} {
				caller := operator
				if self {
					caller = profile
				}
				left, right := net.Pipe()
				deadline := func() time.Time { return time.Now().Add(3 * time.Second) }
				result := make(chan error, 1)
				go func() {
					connection, err := profile.Server(t.Context(), right, rafttransport.TrafficShardControl, deadline)
					if err == nil && (connection.PeerIdentity().Node != caller.LocalIdentity().Node ||
						policy.Check(connection.PeerIdentity().Node, serviceauthz.CapabilityTopology) != serviceauthz.DecisionAllow) {
						err = errors.New("control peer lacks the exact operator identity or capability")
					}
					result <- err
				}()
				connection, clientErr := caller.Client(t.Context(), left, peer.node, rafttransport.TrafficShardControl, deadline)
				serverErr := <-result
				_ = left.Close()
				_ = right.Close()
				if self {
					if !errors.Is(clientErr, rafttransport.ErrPeerAuthentication) && !errors.Is(serverErr, rafttransport.ErrPeerAuthentication) {
						t.Fatalf("self-peer accepted: client=%v server=%v", clientErr, serverErr)
					}
				} else if clientErr != nil || serverErr != nil || connection.PeerIdentity().Node != peer.node {
					t.Fatalf("operator control authentication: client=%v server=%v", clientErr, serverErr)
				}
			}
		})
	}
}

func TestServeRF3ProcessRoutesTwoEnrolledGroups(t *testing.T) {
	if os.Getenv(rf3CommandHelperEnvironment) != "" {
		return
	}
	root := t.TempDir()
	listeners := reserveRF3ProcessListeners(t)
	defer closeRF3ProcessListeners(listeners)
	addresses := rf3testfixture.ProcessListeners{
		Peer: listeners[0].Addr().String(), Control: listeners[1].Addr().String(),
		Native: listeners[2].Addr().String(), Snapshot: listeners[3].Addr().String(),
	}
	nodes := rf3CommandNodes()
	targetNode := rafttransport.NodeID{0xd1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	credentials, roots, policyPath, profile := rf3EnrolledGroupOperatorCredentials(t, root, nodes, targetNode)
	peerAddresses := [3]string{addresses.Peer, rf3CommandUnusedAddress(t), rf3CommandUnusedAddress(t)}
	walOptions := raftstore.Options{MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
		MaxRecords: 4096, MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes}
	authority := rf3CommandAuthority()
	apply := sqldriver.ReplicatedApplyOptions{MaxSessions: 32, RetryWindow: 8,
		TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20},
		Placement: sqldriver.ReplicatedPlacementProfile{Format: sqldriver.ReplicatedPlacementProfileFormat,
			ShardKey: gateway.ReplicatedCatalogPrimaryKey, TupleVersion: distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}}
	identity1, identity2 := rf3EnrolledGroupIdentities()
	target1 := rf3testfixture.ProcessTarget{MemberID: 4, NodeID: targetNode, StoreID: [16]byte{0xe1}, NodeIncarnation: 9,
		Listeners: rf3testfixture.ProcessListeners{Peer: rf3CommandUnusedAddress(t), Native: rf3CommandUnusedAddress(t), Snapshot: rf3CommandUnusedAddress(t), Control: rf3CommandUnusedAddress(t)}}
	target2 := target1
	target2.StoreID[0]++
	prepare := func(name string, identity raftstore.Identity, target rf3testfixture.ProcessTarget) rf3testfixture.PreparedProcessMember {
		key := raftstore.Key{ID: "multi-group-key", Wrapped: []byte("test-wrapped")}
		for i := range key.Material {
			key.Material[i] = byte(i + 1)
		}
		bootstrap := rf3testfixture.InitialBootstrap([]uint64{1, 2, 3})
		bootstrap.Snapshot.Metadata.ConfState.Learners = []uint64{4}
		prepared, prepareErr := rf3testfixture.PrepareProcessMember(rf3testfixture.ProcessMemberOptions{
			Root: filepath.Join(root, name), ControlRoot: filepath.Join(root, "control"),
			Table: gateway.ReplicatedCatalogTable, CreateTable: `CREATE TABLE controlplane (PRIMARY KEY (id))`,
			Identity: identity, Key: key, WAL: walOptions, Bootstrap: bootstrap,
			Authority: authority, Apply: apply, Listeners: addresses, Credential: credentials[0], Roots: roots,
			AuthorizationPolicy: policyPath, Nodes: nodes, PeerAddresses: peerAddresses, Target: &target,
		})
		if info, statErr := os.Stat(filepath.Join(root, name, "split-runtime")); statErr != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("private split-runtime namespace: info=%v err=%v", info, statErr)
		}
		if errors.Is(prepareErr, storeio.ErrStrictAllocationUnsupported) || errors.Is(prepareErr, raftstore.ErrPlatformUnsupported) {
			t.Skipf("RF3 strict durable allocation unsupported: %v", prepareErr)
		}
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		return prepared
	}
	first := prepare("first", identity1, target1)
	second := prepare("second", identity2, target2)
	manifestPath := filepath.Join(root, "multi-group-serve.vibejson")
	if err := os.WriteFile(manifestPath, combineRF3ProcessGroups(t, first.ManifestPath, second.ManifestPath), 0o600); err != nil {
		t.Fatal(err)
	}
	targetKey := raftstore.Key{ID: "rf3-command-key", Wrapped: []byte("test-wrapped")}
	for index := range targetKey.Material {
		targetKey.Material[index] = byte(index + 1)
	}
	groupFor := func(identity raftstore.Identity) raftmember.GroupKey {
		return raftmember.GroupKey{ClusterID: identity.ClusterID, ClusterIncarnation: identity.ClusterIncarnation,
			TopologyRecoveryEpoch: rf3CommandGroup().TopologyRecoveryEpoch,
			ShardIncarnation:      identity.ShardIncarnation, GroupID: identity.GroupID}
	}
	cold1 := prepareRF3ColdTarget(t, rf3ColdTargetOptions{Root: filepath.Join(root, "target-first"),
		Group: groupFor(identity1), Authority: authority, WAL: walOptions, Apply: apply, Key: targetKey,
		Distribution: identity1.Distribution, Shard: identity1.Shard, AllocationGeneration: identity1.AllocationGeneration,
		Credential: credentials[3], Roots: roots, AuthorizationPolicy: policyPath,
		ServingNodes: nodes, ServingPeerAddresses: peerAddresses,
		Target: rf3ManifestEnrolledTarget{MemberID: target1.MemberID, NodeID: target1.NodeID,
			StoreID: target1.StoreID, NodeIncarnation: target1.NodeIncarnation,
			PeerAddress: target1.Listeners.Peer, NativeAddress: target1.Listeners.Native,
			SnapshotAddress: target1.Listeners.Snapshot, ControlAddress: target1.Listeners.Control},
		Listeners: rf3ManifestListeners{Peer: target1.Listeners.Peer, Native: target1.Listeners.Native,
			Snapshot: target1.Listeners.Snapshot, Control: target1.Listeners.Control},
		SourceNode: nodes[0], SourceSnapshotAddress: addresses.Snapshot, MaxArtifactBytes: 1 << 30})
	cold2 := prepareRF3ColdTarget(t, rf3ColdTargetOptions{Root: filepath.Join(root, "target-second"),
		Group: groupFor(identity2), Authority: authority, WAL: walOptions, Apply: apply, Key: targetKey,
		Distribution: identity2.Distribution, Shard: identity2.Shard, AllocationGeneration: identity2.AllocationGeneration,
		Credential: credentials[3], Roots: roots, AuthorizationPolicy: policyPath,
		ServingNodes: nodes, ServingPeerAddresses: peerAddresses,
		Target: rf3ManifestEnrolledTarget{MemberID: target2.MemberID, NodeID: target2.NodeID,
			StoreID: target2.StoreID, NodeIncarnation: target2.NodeIncarnation,
			PeerAddress: target2.Listeners.Peer, NativeAddress: target2.Listeners.Native,
			SnapshotAddress: target2.Listeners.Snapshot, ControlAddress: target2.Listeners.Control},
		Listeners: rf3ManifestListeners{Peer: target2.Listeners.Peer, Native: target2.Listeners.Native,
			Snapshot: target2.Listeners.Snapshot, Control: target2.Listeners.Control},
		SourceNode: nodes[0], SourceSnapshotAddress: addresses.Snapshot, MaxArtifactBytes: 1 << 30})
	multiBootstrapPath := filepath.Join(root, "target-bootstrap-groups.vibejson")
	for _, cold := range []*rf3ColdTargetProcess{cold1, cold2} {
		member, err := loadRF3Manifest(cold.MemberManifestPath)
		if err != nil {
			t.Fatal(err)
		}
		base, exactApply, err := loadRF3RetainedIdentities(member)
		if err != nil {
			t.Fatal(err)
		}
		// Cold targets already have an activated apply participant. The
		// multigroup opener must present its exact retained identity, just as
		// singleton bootstrap does; no default-profile reopen is permissible.
		wrongApply := exactApply
		wrongApply.MaxSessions++
		if database, err := sqldriver.OpenReplicatedShardStoreWithApply(member.SQL.Path, base, wrongApply); !errors.Is(err, sqldriver.ErrReplicatedApplyMismatch) {
			if database != nil {
				_ = database.Close()
			}
			t.Fatalf("cold target accepted foreign apply identity: %v", err)
		}
		database, err := sqldriver.OpenReplicatedShardStoreWithApply(member.SQL.Path, base, exactApply)
		if err != nil {
			t.Fatalf("cold target exact apply reopen: %v", err)
		}
		if err = database.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(multiBootstrapPath, combineBootstrapRF3ProcessGroups(t,
		cold1.BootstrapManifestPath, cold2.BootstrapManifestPath), 0o600); err != nil {
		t.Fatal(err)
	}
	coldTarget := &rf3ColdTargetProcess{BootstrapManifestPath: multiBootstrapPath, member: 4}
	defer coldTarget.Close(t)

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := &rf3CommandDiagnostic{maximum: rf3CommandDiagnosticBytes}
	command := exec.Command(executable, "-test.run=^TestServeRF3CommandProcessHelper$", "-test.v")
	command.Env = append(os.Environ(), rf3CommandHelperEnvironment+"=1", rf3CommandManifestEnvironment+"="+manifestPath)
	for _, listener := range listeners {
		file, fileErr := listener.File()
		if fileErr != nil {
			t.Fatal(fileErr)
		}
		defer file.Close()
		command.ExtraFiles = append(command.ExtraFiles, file)
	}
	command.Stdout, command.Stderr = diagnostic, diagnostic
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	for index := range listeners {
		_ = command.ExtraFiles[index].Close()
		_ = listeners[index].Close()
		listeners[index] = nil
	}
	child := &rf3CommandChild{member: 1, command: command, exited: make(chan struct{}), diagnostic: diagnostic}
	go func() {
		waitErr := command.Wait()
		child.mu.Lock()
		child.waitErr = waitErr
		child.mu.Unlock()
		close(child.exited)
	}()
	defer closeRF3CommandChildren(t, []*rf3CommandChild{child})
	waitRF3CommandReady(t, child, 30*time.Second)
	coldTarget.Start(t)
	coldTarget.WaitColdReady(t)

	deadline := func() time.Time { return time.Now().Add(10 * time.Second) }
	client, err := snapshottransfer.NewSourceControlClient(snapshottransfer.SourceControlClientOptions{
		Opener:       rf3CommandControlOpener{profile: profile, addresses: map[rafttransport.NodeID]string{nodes[0]: addresses.Control}, deadline: deadline},
		ReadDeadline: deadline, WriteDeadline: deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	groups := []struct {
		identity raftstore.Identity
		target   rf3testfixture.ProcessTarget
		op       byte
	}{{identity1, target1, 0xa1}, {identity2, target2, 0xa2}}
	requests := make([]snapshottransfer.SourceControlRequest, 0, len(groups))
	descriptors := make([]snapshottransfer.Descriptor, 0, len(groups))
	latencies := make([]time.Duration, 0, len(groups)*2)
	var networkBytes uint64
	for _, item := range groups {
		group := raftmember.GroupKey{ClusterID: item.identity.ClusterID, ClusterIncarnation: item.identity.ClusterIncarnation,
			TopologyRecoveryEpoch: rf3CommandGroup().TopologyRecoveryEpoch, ShardIncarnation: item.identity.ShardIncarnation, GroupID: item.identity.GroupID}
		request := snapshottransfer.SourceControlRequest{Operation: [32]byte{item.op}, Step: [32]byte{item.op + 1}, Group: group,
			SourceMember: 1, TargetMember: 4, TargetStore: item.target.StoreID, TargetIncarnation: item.target.NodeIncarnation,
			ReplicaSetVersion: 1, SourceNode: nodes[0]}
		started := time.Now()
		descriptor, prepareErr := client.PrepareReplicaMoveSnapshot(t.Context(), request)
		latencies = append(latencies, time.Since(started))
		if prepareErr != nil {
			t.Fatalf("group %x prepare: %v; process=%s", group.GroupID, prepareErr, diagnostic.String())
		}
		if descriptor.Group != group || descriptor.TargetStore != item.target.StoreID {
			t.Fatalf("misrouted descriptor=%+v", descriptor)
		}
		requests = append(requests, request)
		descriptors = append(descriptors, descriptor)
		networkBytes += descriptor.ArtifactBytes
	}
	bootstrapClient, err := snapshottransfer.NewBootstrapControlClient(snapshottransfer.BootstrapControlClientOptions{
		Opener: rf3CommandControlOpener{profile: profile,
			addresses: map[rafttransport.NodeID]string{targetNode: target1.Listeners.Control}, deadline: deadline},
		ReadDeadline: deadline, WriteDeadline: deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	bootstrapStarted := time.Now()
	if _, err = bootstrapClient.Execute(t.Context(), targetNode, snapshottransfer.BootstrapRequest{
		Operation: requests[0].Operation, Step: requests[0].Step, Descriptor: descriptors[0]}); err != nil {
		t.Fatalf("first learner bootstrap: %v", err)
	}
	// Crash after one group owns an installed learner WAL while the other is
	// still cold. Reopen must exclude the completed group from cold ownership,
	// finish the remaining group on the same address, then atomically transition
	// the process to ordinary multi-group serving.
	if err = coldTarget.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-coldTarget.exited:
	case <-time.After(10 * time.Second):
		t.Fatal("cold target did not exit after SIGKILL")
	}
	coldTarget.mu.Lock()
	coldTarget.command, coldTarget.exited, coldTarget.diagnostic = nil, nil, nil
	coldTarget.waitErr = nil
	coldTarget.mu.Unlock()
	coldTarget.Start(t)
	coldTarget.WaitColdReady(t)
	if _, err = bootstrapClient.Execute(t.Context(), targetNode, snapshottransfer.BootstrapRequest{
		Operation: requests[1].Operation, Step: requests[1].Step, Descriptor: descriptors[1]}); err != nil {
		t.Fatalf("second learner bootstrap after SIGKILL: %v", err)
	}
	if elapsed := time.Since(bootstrapStarted); elapsed > 20*time.Second {
		t.Fatalf("multi-group learner bootstrap latency=%s", elapsed)
	}
	coldTarget.WaitServingReady(t)
	baseRoot := filepath.Join(root, "first")
	for _, identity := range []raftstore.Identity{identity1, identity2} {
		group := raftmember.GroupKey{ClusterID: identity.ClusterID, ClusterIncarnation: identity.ClusterIncarnation,
			TopologyRecoveryEpoch: rf3CommandGroup().TopologyRecoveryEpoch, ShardIncarnation: identity.ShardIncarnation, GroupID: identity.GroupID}
		if info, statErr := os.Stat(rf3SnapshotGroupPath(filepath.Join(baseRoot, "source-artifacts"), group, true)); statErr != nil || !info.IsDir() {
			t.Fatalf("group %x repository isolation: %v", group.GroupID, statErr)
		}
	}
	if networkBytes == 0 || networkBytes > 64<<20 {
		t.Fatalf("bounded snapshot bytes=%d", networkBytes)
	}
	if bytesOnDisk := rf3ProcessTreeBytes(t, filepath.Join(baseRoot, "source-artifacts")); bytesOnDisk > 64<<20 {
		t.Fatalf("bounded source storage bytes=%d", bytesOnDisk)
	}
	if rss := rf3ProcessRSSBytes(t, command.Process.Pid); rss > 1<<30 {
		t.Fatalf("bounded source RSS bytes=%d", rss)
	}

	// SIGKILL after both durable Complete records, then reopen the exact
	// manifest. Exact retries must settle from the group-scoped journals and
	// return byte-identical descriptors without exporting a second artifact.
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-child.exited:
	case <-time.After(10 * time.Second):
		t.Fatal("multi-group source did not exit after SIGKILL")
	}
	child.mu.Lock()
	child.waitErr = nil // SIGKILL is the injected fault, not an unexpected exit.
	child.mu.Unlock()
	rf3CommandRestartChild(t, child, executable, manifestPath, [4]string{
		addresses.Peer, addresses.Control, addresses.Native, addresses.Snapshot,
	})
	for index, request := range requests {
		started := time.Now()
		descriptor, retryErr := client.PrepareReplicaMoveSnapshot(t.Context(), request)
		latencies = append(latencies, time.Since(started))
		if retryErr != nil || descriptor != descriptors[index] {
			t.Fatalf("post-SIGKILL group %d descriptor=%+v want=%+v err=%v",
				index, descriptor, descriptors[index], retryErr)
		}
	}
	var maximum time.Duration
	for _, latency := range latencies {
		if latency > maximum {
			maximum = latency
		}
	}
	if maximum > 10*time.Second {
		t.Fatalf("bounded control p99/max=%s samples=%v", maximum, latencies)
	}
}

// Independent Raft groups must also identify independent logical shard
// allocations; changing only GroupID aliases the retained split registry key.
func rf3EnrolledGroupIdentities() (raftstore.Identity, raftstore.Identity) {
	first := rf3CommandStoreIdentity(1)
	second := first
	second.GroupID[0] ^= 0x40
	second.ShardIncarnation[0] ^= 0x20
	second.StoreID[0] ^= 0x10
	second.Shard += "-second"
	return first, second
}

func reserveRF3ProcessListeners(t testing.TB) []*net.TCPListener {
	t.Helper()
	listeners := make([]*net.TCPListener, 4)
	for index := range listeners {
		listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			closeRF3ProcessListeners(listeners)
			t.Fatal(err)
		}
		listeners[index] = listener
	}
	return listeners
}

func closeRF3ProcessListeners(listeners []*net.TCPListener) {
	for _, listener := range listeners {
		if listener != nil {
			_ = listener.Close()
		}
	}
}

func combineRF3ProcessGroups(t testing.TB, paths ...string) []byte {
	t.Helper()
	documents := make([][]byte, 0, len(paths))
	for _, path := range paths {
		document, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		documents = append(documents, document)
	}
	result, err := rf3testfixture.CombineProcessManifests(documents...)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestCombineRF3ProcessGroupsPreflightWithNestedMemberRoster(t *testing.T) {
	// Both input manifests contain a child_registry.members before their
	// top-level members. This must be structural composition, not byte slicing.
	options := rf3testfixture.ProcessMemberOptions{
		Root: "/srv/vibedb/first", ControlRoot: "/srv/vibedb/control",
		Table: gateway.ReplicatedCatalogTable, CreateTable: `CREATE TABLE controlplane (PRIMARY KEY (id))`,
		Identity: rf3CommandStoreIdentity(1), Key: raftstore.Key{ID: "fixture-key"},
		WAL: raftstore.Options{MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
			MaxRecords: 4096, MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes},
		Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
		Apply: sqldriver.ReplicatedApplyOptions{MaxSessions: 32, RetryWindow: 8,
			TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20},
			Placement: sqldriver.ReplicatedPlacementProfile{Format: sqldriver.ReplicatedPlacementProfileFormat,
				ShardKey: gateway.ReplicatedCatalogPrimaryKey, TupleVersion: distribution.CurrentTupleVersion,
				MapperVersion: distribution.NativeMapperVersion, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}},
		Listeners: rf3testfixture.ProcessListeners{Peer: "127.0.0.1:21001", Native: "127.0.0.1:22001",
			Snapshot: "127.0.0.1:23001", Control: "127.0.0.1:24001"},
		Credential: rf3testfixture.Credential{Certificate: "/cert", Key: "/key"},
		Roots:      "/roots", AuthorizationPolicy: "/policy", Nodes: rf3CommandNodes(),
		PeerAddresses: [3]string{"127.0.0.1:21001", "127.0.0.1:21002", "127.0.0.1:21003"},
		Target: &rf3testfixture.ProcessTarget{MemberID: 4, NodeID: rafttransport.NodeID{0xd1},
			StoreID: [16]byte{0xe1}, NodeIncarnation: 9,
			Listeners: rf3testfixture.ProcessListeners{Peer: "127.0.0.1:21004", Native: "127.0.0.1:22004",
				Snapshot: "127.0.0.1:23004", Control: "127.0.0.1:24004"}},
	}
	first := rf3testfixture.ProcessMemberManifest(options)
	options.Root = "/srv/vibedb/second"
	options.Identity.GroupID[0] ^= 0x40
	options.Identity.ShardIncarnation[0] ^= 0x20
	options.Identity.StoreID[0] ^= 0x10
	options.Target.StoreID[0]++
	second := rf3testfixture.ProcessMemberManifest(options)
	paths := []string{filepath.Join(t.TempDir(), "first"), filepath.Join(t.TempDir(), "second")}
	for index, raw := range [][]byte{first, second} {
		if err := os.WriteFile(paths[index], raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifest, err := parseRF3Manifest(combineRF3ProcessGroups(t, paths...))
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Groups) != 2 {
		t.Fatalf("incomplete composite: groups=%d", len(manifest.Groups))
	}
	for index, group := range manifest.Groups {
		if group.ChildRegistry.MemberCount != 3 || group.ChildRegistry.Root != filepath.Join(group.Route.MemberRoot, "split-children") {
			t.Fatalf("group %d lost exact child registry: %+v", index, group.ChildRegistry)
		}
		if group.EnrolledTarget == nil || group.EnrolledTarget.MemberID != 4 || group.MemberCount != 3 {
			t.Fatalf("group %d lost enrolled target or voter roster: %+v", index, group)
		}
	}
}

func combineBootstrapRF3ProcessGroups(t testing.TB, paths ...string) []byte {
	t.Helper()
	if len(paths) < 2 {
		t.Fatal("multi-group bootstrap requires at least two groups")
	}
	groups := make([]bootstrapRF3ManifestGroup, len(paths))
	var control string
	for index, path := range paths {
		manifest, err := loadBootstrapRF3Manifest(path)
		if err != nil {
			t.Fatal(err)
		}
		bundles := manifest.groupBundles()
		if len(bundles) != 1 || index != 0 && manifest.ControlListener != control {
			t.Fatal("incompatible bootstrap group listeners")
		}
		control, groups[index] = manifest.ControlListener, bundles[0]
	}
	raw := fmt.Appendf(nil, `{"control_listener":%q,"groups":[`, control)
	for index, group := range groups {
		if index != 0 {
			raw = append(raw, ',')
		}
		raw = fmt.Appendf(raw,
			`{"member_manifest":%q,"source_node":"%x","source_snapshot_address":%q,"repository_path":%q,"cursor_path":%q,"journal_path":%q,"static_bootstrap_path":%q,"max_artifact_bytes":%d}`,
			group.MemberManifest, group.SourceNode, group.SourceSnapshotAddress,
			group.RepositoryPath, group.CursorPath, group.JournalPath,
			group.StaticBootstrapPath, group.MaxArtifactBytes)
	}
	return append(raw, ']', '}')
}

func rf3ProcessTreeBytes(t testing.TB, root string) uint64 {
	t.Helper()
	var total uint64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += uint64(info.Size())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return total
}

func rf3ProcessRSSBytes(t testing.TB, pid int) uint64 {
	t.Helper()
	if runtime.GOOS != "linux" {
		return 0
	}
	raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "status"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[2] != "kB" {
			t.Fatalf("invalid VmRSS line %q", line)
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return kb << 10
	}
	t.Fatal("VmRSS missing")
	return 0
}
