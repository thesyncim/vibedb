//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
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
	rf3CommandSnapshotListenerFD  = 6
	rf3CommandDiagnosticBytes     = 64 << 10
)

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
		nodes, addresses, rf3CommandStoreIdentity(1), rf3CommandGroup().TopologyRecoveryEpoch,
	)
	manifest, err := parseRF3Manifest(document)
	if err != nil {
		t.Fatalf("generated manifest: %v\n%s", err, document)
	}
	if err := validateRF3Addresses(manifest); err != nil {
		t.Fatalf("generated manifest addresses: %v", err)
	}
	// This fixture serves the catalog table. Parsing alone cannot detect a
	// stale split-child template that the serving command will reject at open.
	base := sqldriver.ReplicatedShardStoreIdentity{UserTable: gateway.ReplicatedCatalogTable}
	apply := sqldriver.ReplicatedApplyIdentity{
		MaxSessions: 32, RetryWindow: 8,
		TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20},
		Placement: sqldriver.ReplicatedPlacementProfile{
			Format:        sqldriver.ReplicatedPlacementProfileFormat,
			ShardKey:      gateway.ReplicatedCatalogPrimaryKey,
			TupleVersion:  distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
		},
	}
	if !rf3SplitChildTemplateMatchesRetained(manifest.SplitControl.ChildRegistry, base, apply) {
		t.Fatal("generated split-child template differs from the catalog apply profile")
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
	target := rafttransport.NodeID{0x71}
	enrolled, err := serviceauthz.Load(rf3CommandPolicyWithTarget(nodes, target))
	if err != nil {
		t.Fatalf("generated enrolled-target authorization policy: %v", err)
	}
	want := serviceauthz.CapabilityDataRead | serviceauthz.CapabilityDataWrite |
		serviceauthz.CapabilityDelegate | serviceauthz.CapabilityMembership |
		serviceauthz.CapabilityTopology | serviceauthz.CapabilityTransactionRecovery |
		serviceauthz.CapabilityRequestLedger | serviceauthz.CapabilityExecutionPin
	if enrolled.Check(target, want) != serviceauthz.DecisionAllow ||
		enrolled.Check(target, serviceauthz.CapabilitySchema) == serviceauthz.DecisionAllow {
		t.Fatal("generated target policy differs from the enrolled fixture capabilities")
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
	snapshotListeners := make([]*net.TCPListener, rf3CommandMembers)
	controlListeners := make([]*net.TCPListener, rf3CommandMembers)
	var peerAddresses, nativeAddresses, snapshotAddresses, controlAddresses [rf3CommandMembers]string
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
		snapshotListeners[index], err = net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		controlListeners[index], err = net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		peerAddresses[index] = peerListeners[index].Addr().String()
		nativeAddresses[index] = nativeListeners[index].Addr().String()
		snapshotAddresses[index] = snapshotListeners[index].Addr().String()
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
			if snapshotListeners[index] != nil {
				_ = snapshotListeners[index].Close()
			}
			if controlListeners[index] != nil {
				_ = controlListeners[index].Close()
			}
		}
	}()

	nodes := rf3CommandNodes()
	group := rf3CommandGroup()
	targetNode := rafttransport.NodeID{0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78,
		0x79, 0x7a, 0x7b, 0x7c, 0x7d, 0x7e, 0x7f, 0x80}
	clientNode, gatewayNode := rf3CompositionClientNodes()
	credentialNodes := append(append([]rafttransport.NodeID(nil), nodes[:]...), targetNode, clientNode, gatewayNode)
	credentials, roots, err := rf3testfixture.WriteCredentials(
		root, rf3CommandIdentityOID,
		rafttransport.TrustDomain{
			ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		},
		credentialNodes,
	)
	if err != nil {
		t.Fatal(err)
	}
	policyPath := filepath.Join(root, "authorization-policy.vibejson")
	if err := os.WriteFile(policyPath, rf3CommandPolicyWithTarget(nodes, targetNode, clientNode, gatewayNode), 0o600); err != nil {
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
	applyOptions := sqldriver.ReplicatedApplyOptions{
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
	}
	targetStore := [16]byte{0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88,
		0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f, 0x90}
	const targetIncarnation = uint64(9)
	targetListeners := rf3ManifestListeners{
		Peer: rf3CommandUnusedAddress(t), Native: rf3CommandUnusedAddress(t),
		Snapshot: rf3CommandUnusedAddress(t), Control: rf3CommandUnusedAddress(t),
	}
	target := rf3ManifestEnrolledTarget{
		MemberID: 4, NodeID: targetNode, StoreID: targetStore,
		NodeIncarnation: targetIncarnation, PeerAddress: targetListeners.Peer,
		NativeAddress: targetListeners.Native, SnapshotAddress: targetListeners.Snapshot,
		ControlAddress: targetListeners.Control,
	}
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
			Apply:     applyOptions,
		})
		if errors.Is(prepareErr, storeio.ErrStrictAllocationUnsupported) ||
			errors.Is(prepareErr, raftstore.ErrPlatformUnsupported) {
			t.Skipf("RF3 strict durable allocation unsupported: %v", prepareErr)
		}
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		prepareRF3CommandSplitRuntime(t, memberRoot, rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}))
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
			peerAddresses[index], nativeAddresses[index], snapshotAddresses[index],
			controlAddresses[index], credentials[index], roots,
			policyPath, walOptions, nodes, peerAddresses,
			walIdentityFromBinding(prepared.Base.Binding), prepared.Base.Binding.TopologyRecoveryEpoch,
		)
		document = rf3CommandEnrollTarget(
			document, targetNode, targetStore, targetIncarnation,
			target.PeerAddress, target.NativeAddress,
			target.SnapshotAddress, target.ControlAddress,
		)
		if err := os.WriteFile(manifestPaths[index], document, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	targetKey := raftstore.Key{ID: "rf3-command-key", Wrapped: []byte("explicit-test-wrapped-key")}
	copy(targetKey.Material[:], keyMaterial)
	targetProcess := prepareRF3ColdTarget(t, rf3ColdTargetOptions{
		StaticBootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
		Root:            filepath.Join(root, "member-4"), Group: group, Authority: authority,
		WAL: walOptions, Apply: applyOptions, Key: targetKey,
		Credential: credentials[3], Roots: roots, AuthorizationPolicy: policyPath,
		ServingNodes: nodes, ServingPeerAddresses: peerAddresses,
		Target: target, Listeners: targetListeners,
		SourceNode: nodes[0], SourceSnapshotAddress: snapshotAddresses[0],
		MaxArtifactBytes: 1 << 30,
	})
	defer targetProcess.Close(t)

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
		snapshotFile, err := snapshotListeners[index].File()
		if err != nil {
			_ = peerFile.Close()
			_ = nativeFile.Close()
			_ = controlFile.Close()
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
		command.ExtraFiles = []*os.File{peerFile, controlFile, nativeFile, snapshotFile}
		command.Stdout, command.Stderr = diagnostic, diagnostic
		if err := command.Start(); err != nil {
			_ = peerFile.Close()
			_ = nativeFile.Close()
			_ = controlFile.Close()
			_ = snapshotFile.Close()
			t.Fatal(err)
		}
		_ = peerFile.Close()
		_ = nativeFile.Close()
		_ = controlFile.Close()
		_ = snapshotFile.Close()
		_ = peerListeners[index].Close()
		_ = nativeListeners[index].Close()
		_ = controlListeners[index].Close()
		_ = snapshotListeners[index].Close()
		peerListeners[index], nativeListeners[index], snapshotListeners[index], controlListeners[index] = nil, nil, nil, nil
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
	clientProfile, err := servicetls.LoadProfile(
		credentials[4].Certificate, credentials[4].Key, roots,
		"1.3.6.1.4.1.32473.1.1", time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	targetProcess.Start(t)
	targetProcess.WaitColdReady(t)
	networkDeadline := func() time.Time { return time.Now().Add(10 * time.Second) }
	controlAddressesByNode := make(map[rafttransport.NodeID]string, rf3CommandMembers+1)
	for index, node := range nodes {
		controlAddressesByNode[node] = controlAddresses[index]
	}
	controlAddressesByNode[targetNode] = target.ControlAddress
	opener := rf3CommandControlOpener{
		profile: clientProfile, addresses: controlAddressesByNode, deadline: networkDeadline,
	}
	grantClient, err := shardservice.NewMembershipGrantControlClient(
		opener, networkDeadline, networkDeadline,
	)
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv(gatewayHotShardLiveChildEnvironment) == "1" {
		runGatewayHotShardLiveChild(t, gatewayHotShardLiveFixture{
			root: root, executable: executable, children: children, target: targetProcess,
			nodes: nodes, group: group, credentials: credentials, roots: roots,
			policyPath: policyPath, peerAddresses: peerAddresses, nativeAddresses: nativeAddresses,
			controlAddresses: controlAddresses, targetNode: targetNode, targetStore: targetStore,
			targetIncarnation: targetIncarnation, targetListeners: targetListeners,
			clientNode: clientNode, gatewayNode: gatewayNode, clientProfile: clientProfile, authority: authority, grantClient: grantClient,
		})
		return
	}
	var initialRoster [3]membershipgrant.RosterMember
	for index, node := range nodes {
		initialRoster[index] = membershipgrant.RosterMember{
			Member: uint64(index + 1), Node: [16]byte(node),
		}
	}
	grant := membershipgrant.Grant{
		Group: group, TransitionID: [16]byte{0x41}, MetadataEpoch: 2,
		CatalogGeneration: 1, InitialReplicaSetVersion: 1,
		InitialVoters:           [3]uint64{1, 2, 3},
		InitialRosterDigest:     membershipgrant.CertifiedRosterDigest(group, 1, initialRoster),
		InitialDescriptorDigest: sha256.Sum256([]byte("rf3-process-catalog-descriptor")),
		SourceMember:            1, TargetMember: 4, TargetNode: [16]byte(targetNode),
	}
	for _, node := range append(append([]rafttransport.NodeID(nil), nodes[:]...), targetNode) {
		if err = grantClient.InstallMembershipGrant(t.Context(), node, grant); err != nil {
			t.Fatalf("install grant on %x: %v", node, err)
		}
	}
	authorityIdentity := serviceauthz.Authority{
		Node: clientNode, Generation: authority.ActivePolicyGeneration,
	}
	servingAddresses := append([]string(nil), nativeAddresses[:]...)
	servingNodes := append([]rafttransport.NodeID(nil), nodes[:]...)
	observationClient, err := replicacontrol.NewClient(replicacontrol.ClientOptions{
		Opener: opener, ReadDeadline: networkDeadline, WriteDeadline: networkDeadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	learnerState := rf3CommandApplyMembership(
		t, servingAddresses, servingNodes, clientProfile, observationClient, authorityIdentity, group,
		rf3CommandStoreIdentity(1).AllocationGeneration,
		shardservice.ReplicatedMembershipRequest{
			Kind: raftservice.MembershipAddLearner, TransitionID: grant.TransitionID,
			MetadataEpoch: grant.MetadataEpoch, CatalogGeneration: grant.CatalogGeneration,
			SourceMember: grant.SourceMember, TargetMember: grant.TargetMember,
		},
	)
	sourceClient, err := snapshottransfer.NewSourceControlClient(
		snapshottransfer.SourceControlClientOptions{
			Opener: opener, ReadDeadline: networkDeadline, WriteDeadline: networkDeadline,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshotRequest := snapshottransfer.SourceControlRequest{
		Operation: [32]byte{0x51}, Step: [32]byte{0x52}, Group: group,
		SourceMember: 1, TargetMember: 4, TargetStore: targetStore,
		TargetIncarnation: targetIncarnation,
		ReplicaSetVersion: learnerState.Fence.Command.ReplicaSetVersion,
		SourceNode:        nodes[0],
	}
	descriptor, err := sourceClient.PrepareReplicaMoveSnapshot(t.Context(), snapshotRequest)
	if err != nil {
		t.Fatalf("prepare source snapshot: %v", err)
	}
	bootstrapClient, err := snapshottransfer.NewBootstrapControlClient(
		snapshottransfer.BootstrapControlClientOptions{
			Opener: opener, ReadDeadline: networkDeadline, WriteDeadline: networkDeadline,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = bootstrapClient.Execute(t.Context(), targetNode,
		snapshottransfer.BootstrapRequest{
			Operation: snapshotRequest.Operation, Step: snapshotRequest.Step,
			Descriptor: descriptor,
		}); err != nil {
		t.Fatalf("bootstrap target: %v", err)
	}
	// BootstrapComplete is the only authority for source reclamation. Exercise
	// the shipped release protocol twice so both physical cleanup and the
	// durable idempotent terminal observation are covered by the process test.
	if err = sourceClient.ReleaseReplicaMoveSnapshot(
		t.Context(), snapshotRequest, descriptor,
	); err != nil {
		t.Fatalf("release source snapshot: %v", err)
	}
	if err = sourceClient.ReleaseReplicaMoveSnapshot(
		t.Context(), snapshotRequest, descriptor,
	); err != nil {
		t.Fatalf("retry released source snapshot: %v", err)
	}
	rf3CommandAssertArtifactRepositoryEmpty(
		t, filepath.Join(root, "member-1", "source-artifacts"),
	)
	targetProcess.WaitServingReady(t)
	targetProcess.Restart(t)
	targetProcess.WaitServingReady(t)
	if _, err = probeRF3CommandMember(
		t.Context(), target.NativeAddress, targetNode, clientProfile, clientNode,
		group, rf3CommandStoreIdentity(1).AllocationGeneration,
		authority.ActivePolicyGeneration,
	); err == nil {
		t.Fatal("learner target served native traffic")
	}
	observationRequest := replicacontrol.Request{
		Operation: [32]byte{0x61}, Step: [32]byte{0x62}, Group: group,
		TargetMember: 4, ExpectedReplicaSetVersion: learnerState.Fence.Command.ReplicaSetVersion,
	}
	learnerLeader, learnerLeaderState := rf3CommandFindLeader(
		t, servingAddresses, servingNodes, clientProfile, clientNode, group,
		rf3CommandStoreIdentity(1).AllocationGeneration,
		authority.ActivePolicyGeneration,
	)
	leaderObservation, err := observationClient.Observe(
		t.Context(), servingNodes[learnerLeader], observationRequest,
	)
	if err != nil {
		t.Fatalf("observe learner progress: %v", err)
	}
	targetObservation, err := observationClient.Observe(
		t.Context(), targetNode, observationRequest,
	)
	if err != nil || !leaderObservation.ProgressFound ||
		!leaderObservation.Progress.Learner || !leaderObservation.Progress.RecentActive ||
		leaderObservation.Progress.PendingSnapshot != 0 ||
		leaderObservation.Progress.Match < learnerLeaderState.Commit ||
		targetObservation.Status.Applied < learnerLeaderState.Commit ||
		targetObservation.State.SnapshotBaseDigest == ([sha256.Size]byte{}) {
		t.Fatalf("learner did not expose a caught-up installed cut: leader=%+v target=%+v err=%v",
			leaderObservation, targetObservation, err)
	}
	promotedState := rf3CommandApplyMembership(
		t, servingAddresses, servingNodes, clientProfile, observationClient, authorityIdentity, group,
		rf3CommandStoreIdentity(1).AllocationGeneration,
		shardservice.ReplicatedMembershipRequest{
			Kind: raftservice.MembershipPromoteVoter, TransitionID: grant.TransitionID,
			MetadataEpoch: grant.MetadataEpoch, CatalogGeneration: grant.CatalogGeneration,
			SourceMember: grant.SourceMember, TargetMember: grant.TargetMember,
		},
	)
	if _, err = probeRF3CommandMember(
		t.Context(), target.NativeAddress, targetNode, clientProfile, clientNode,
		group, rf3CommandStoreIdentity(1).AllocationGeneration,
		authority.ActivePolicyGeneration,
	); err == nil {
		t.Fatal("RF4 target served native traffic")
	}
	observationRequest.ExpectedReplicaSetVersion = promotedState.Fence.Command.ReplicaSetVersion
	targetObservation, err = observationClient.Observe(t.Context(), targetNode, observationRequest)
	if err != nil {
		t.Fatalf("observe promoted target: %v", err)
	}
	binding := targetObservation.State.Binding
	ownership, err := replicatedstate.AppendOwnershipTransition(nil, replicatedstate.OwnershipTransition{
		From: binding, ExpectedReplicaSetVersion: targetObservation.State.ReplicaSetVersion,
		SourceMember: 1, TargetMember: 4,
		ToOwnershipEpoch:  binding.OwnershipEpoch + 1,
		ToRoutingVersion:  binding.RoutingVersion + 1,
		ToRouteGeneration: binding.RouteGeneration + 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionClient, err := replicaaction.NewClient(replicaaction.ClientOptions{
		Opener: opener, ReadDeadline: networkDeadline, WriteDeadline: networkDeadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	leader, leaderState := rf3CommandFindLeader(
		t, servingAddresses, servingNodes, clientProfile, authorityIdentity.Node, group,
		rf3CommandStoreIdentity(1).AllocationGeneration, authorityIdentity.Generation,
	)
	rf3CommandExecuteAction(t, actionClient, servingNodes[leader], replicaaction.Request{
		Operation: [32]byte{0x63}, Step: [32]byte{0x64}, Kind: replicaaction.OwnershipTransition,
		Fence: rf3CommandServingFence(leaderState.Fence), SourceMember: 1, TargetMember: 4,
		Command: ownership,
	})
	leader, leaderState = rf3CommandFindLeader(
		t, servingAddresses, servingNodes, clientProfile, authorityIdentity.Node, group,
		rf3CommandStoreIdentity(1).AllocationGeneration, authorityIdentity.Generation,
	)
	if leaderState.Fence.Command.OwnershipEpoch != binding.OwnershipEpoch+1 ||
		leaderState.Fence.Command.RoutingVersion != binding.RoutingVersion+1 ||
		leaderState.Fence.Command.RouteGeneration != binding.RouteGeneration+1 {
		t.Fatalf("ownership transition did not settle: %+v", leaderState.Fence.Command)
	}
	sourceState, err := probeRF3CommandMember(
		t.Context(), nativeAddresses[0], nodes[0], clientProfile, authorityIdentity.Node,
		group, rf3CommandStoreIdentity(1).AllocationGeneration, authorityIdentity.Generation,
	)
	if err != nil {
		t.Fatalf("capture retiring source fence: %v", err)
	}
	removeRequest := shardservice.ReplicatedMembershipRequest{
		Kind: raftservice.MembershipRemoveVoter, TransitionID: grant.TransitionID,
		MetadataEpoch: grant.MetadataEpoch, CatalogGeneration: grant.CatalogGeneration,
		SourceMember: grant.SourceMember, TargetMember: grant.TargetMember,
	}
	if leaderState.LeaderID == grant.SourceMember {
		beforeTerm := leaderState.Fence.Term
		_ = rf3CommandApplyMembership(
			t, servingAddresses, servingNodes, clientProfile, observationClient, authorityIdentity, group,
			rf3CommandStoreIdentity(1).AllocationGeneration,
			shardservice.ReplicatedMembershipRequest{
				Kind: raftservice.MembershipTransferLeader, TransitionID: grant.TransitionID,
				MetadataEpoch: grant.MetadataEpoch, CatalogGeneration: grant.CatalogGeneration,
				SourceMember: grant.SourceMember, TargetMember: grant.TargetMember,
			},
		)
		leaderState = rf3CommandWaitLeaderWitness(
			t, servingAddresses, servingNodes, clientProfile, authorityIdentity.Node,
			group, rf3CommandStoreIdentity(1).AllocationGeneration,
			authorityIdentity.Generation, grant.TargetMember, beforeTerm,
		)
		removeRequest.ExpectedReplicaSetVersion = leaderState.Fence.Command.ReplicaSetVersion
		removeRequest.TransferTerm = leaderState.Fence.Term
		removeObservationRequest := replicacontrol.Request{
			Operation: sha256.Sum256(removeRequest.TransitionID[:]), Step: [32]byte{0x6d, byte(removeRequest.Kind)},
			Group: group, TargetMember: removeRequest.TargetMember, ExpectedReplicaSetVersion: removeRequest.ExpectedReplicaSetVersion,
		}
		beforeRemoval, err := observationClient.Observe(t.Context(), targetNode, removeObservationRequest)
		if err != nil {
			t.Fatalf("observe target leader before removal: %v", err)
		}
		response := rf3CommandRoundTrip(t, target.NativeAddress, targetNode, clientProfile,
			&shardservice.ReplicatedRequest{
				Operation: shardservice.ReplicatedMembership, Authority: authorityIdentity,
				Capability: serviceauthz.CapabilityMembership,
				Fence: shardservice.ReplicatedFence{Group: group,
					AllocationGeneration: rf3CommandStoreIdentity(1).AllocationGeneration,
					Command:              leaderState.Fence.Command, MemberID: 4, StoreID: targetStore,
					NodeIncarnation: targetIncarnation, Term: leaderState.Fence.Term},
				Membership: removeRequest,
			})
		if response.Kind != shardservice.ReplicatedMembershipAccepted {
			t.Fatalf("target-leader removal response: %+v", response)
		}
		settlementContext, cancelSettlement := context.WithTimeout(t.Context(), 30*time.Second)
		_, settleErr := rf3AwaitMembershipSettlement(settlementContext, beforeRemoval, removeRequest,
			rf3MembershipNetworkObserver(observationClient, target.NativeAddress, targetNode, clientProfile,
				authorityIdentity, rf3CommandStoreIdentity(1).AllocationGeneration, removeObservationRequest))
		cancelSettlement()
		if settleErr != nil {
			t.Fatalf("accepted target-leader removal did not settle: %v", settleErr)
		}
	} else {
		removeRequest.TransferTerm = leaderState.Fence.Term
		_ = rf3CommandApplyMembership(
			t, servingAddresses, servingNodes, clientProfile, observationClient, authorityIdentity, group,
			rf3CommandStoreIdentity(1).AllocationGeneration, removeRequest,
		)
	}
	finalAddresses := []string{nativeAddresses[1], nativeAddresses[2], target.NativeAddress}
	finalNodes := []rafttransport.NodeID{nodes[1], nodes[2], targetNode}
	_, finalState := rf3CommandFindLeader(
		t, finalAddresses, finalNodes, clientProfile, authorityIdentity.Node, group,
		rf3CommandStoreIdentity(1).AllocationGeneration, authorityIdentity.Generation,
	)
	if finalState.Fence.Command.ReplicaSetVersion <= promotedState.Fence.Command.ReplicaSetVersion {
		t.Fatalf("final RF3 did not advance membership: %+v", finalState)
	}
	finalReplicaSetVersion := finalState.Fence.Command.ReplicaSetVersion
	rf3CommandExecuteAction(t, actionClient, nodes[0], replicaaction.Request{
		Operation: [32]byte{0x65}, Step: [32]byte{0x66}, Kind: replicaaction.SourceRetirement,
		Fence: raftservice.ServingFence{Group: group,
			AllocationGeneration: rf3CommandStoreIdentity(1).AllocationGeneration,
			Command:              finalState.Fence.Command, MemberID: 1,
			StoreID:         rf3CommandStoreIdentity(1).StoreID,
			NodeIncarnation: sourceState.Fence.NodeIncarnation, Term: finalState.Fence.Term},
		SourceMember: 1, TargetMember: 4,
	})
	if _, err = probeRF3CommandMember(
		t.Context(), nativeAddresses[0], nodes[0], clientProfile, authorityIdentity.Node,
		group, rf3CommandStoreIdentity(1).AllocationGeneration, authorityIdentity.Generation,
	); err == nil {
		t.Fatal("retired source continued serving")
	}

	// Reopen the final RF3 one member at a time. Each stop leaves an actual
	// two-voter quorum, and each restarted process must reopen the dynamically
	// changed ConfState before the next member is stopped. This catches the
	// fixed-roster/restart regression that a single target restart cannot.
	for _, index := range []int{1, 2} {
		rf3CommandRestartChild(t, children[index], executable, manifestPaths[index], [4]string{
			peerAddresses[index], controlAddresses[index],
			nativeAddresses[index], snapshotAddresses[index],
		})
		_, finalState = rf3CommandFindLeader(
			t, finalAddresses, finalNodes, clientProfile, authorityIdentity.Node, group,
			rf3CommandStoreIdentity(1).AllocationGeneration, authorityIdentity.Generation,
		)
		if finalState.Fence.Command.ReplicaSetVersion != finalReplicaSetVersion {
			t.Fatalf("member %d reopened wrong final replica-set cut: %+v",
				index+1, finalState.Fence.Command)
		}
	}
	targetProcess.Restart(t)
	targetProcess.WaitServingReady(t)
	_, finalState = rf3CommandFindLeader(
		t, finalAddresses, finalNodes, clientProfile, authorityIdentity.Node, group,
		rf3CommandStoreIdentity(1).AllocationGeneration, authorityIdentity.Generation,
	)
	if finalState.Fence.Command.ReplicaSetVersion != finalReplicaSetVersion {
		t.Fatalf("target reopened wrong final replica-set cut: %+v", finalState.Fence.Command)
	}

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
	stopMemoryDiagnostics := startWALRetentionMemoryDiagnostics(t)
	defer stopMemoryDiagnostics()
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
	snapshot, err := inheritedRF3CommandListener(rf3CommandSnapshotListenerFD, "rf3-command-snapshot")
	if err != nil {
		_ = peer.Close()
		_ = control.Close()
		_ = native.Close()
		t.Fatal(err)
	}
	defer snapshot.Close()
	listeners := map[string]net.Listener{
		peer.Addr().String(): peer, control.Addr().String(): control,
		native.Addr().String(): native, snapshot.Addr().String(): snapshot,
	}
	listen := func(network, address string) (net.Listener, error) {
		listener, ok := listeners[address]
		if network != "tcp" || !ok {
			return nil, fmt.Errorf("unexpected inherited listener %s %s", network, address)
		}
		delete(listeners, address)
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

func rf3CommandAssertArtifactRepositoryEmpty(t testing.TB, path string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if len(name) > 2 && name[1] == '-' && strings.Contains("scptd", name[:1]) {
			t.Fatalf("released source repository retained %q", name)
		}
	}
}

func rf3CommandRestartChild(
	t testing.TB,
	child *rf3CommandChild,
	executable, manifest string,
	addresses [4]string,
) {
	t.Helper()
	if child == nil || child.command == nil || executable == "" || manifest == "" {
		t.Fatal("invalid RF3 command child restart")
	}
	select {
	case <-child.exited:
	default:
		if err := child.command.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("signal member %d for rolling restart: %v", child.member, err)
		}
		select {
		case <-child.exited:
		case <-time.After(10 * time.Second):
			_ = child.command.Process.Kill()
			<-child.exited
			t.Fatalf("member %d required forced rolling-restart cleanup", child.member)
		}
	}
	child.mu.Lock()
	waitErr := child.waitErr
	child.mu.Unlock()
	if waitErr != nil {
		t.Fatalf("member %d rolling-restart exit: %v\n%s",
			child.member, waitErr, child.diagnostic.String())
	}

	// The child helper consumes inherited descriptors in peer, control,
	// native, snapshot order. Preserve that contract when rebinding the exact
	// addresses during a rolling restart.
	listeners := make([]*net.TCPListener, len(addresses))
	files := make([]*os.File, len(addresses))
	defer func() {
		for _, listener := range listeners {
			if listener != nil {
				_ = listener.Close()
			}
		}
		for _, file := range files {
			if file != nil {
				_ = file.Close()
			}
		}
	}()
	for index, address := range addresses {
		resolved, err := net.ResolveTCPAddr("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		listeners[index], err = net.ListenTCP("tcp", resolved)
		if err != nil {
			t.Fatalf("reserve member %d rolling listener %q: %v", child.member, address, err)
		}
		files[index], err = listeners[index].File()
		if err != nil {
			t.Fatal(err)
		}
	}
	diagnostic := &rf3CommandDiagnostic{maximum: rf3CommandDiagnosticBytes}
	command := exec.Command(
		executable, "-test.run=^TestServeRF3CommandProcessHelper$", "-test.v",
	)
	command.Env = append(os.Environ(),
		rf3CommandHelperEnvironment+"=1", rf3CommandManifestEnvironment+"="+manifest,
	)
	command.ExtraFiles = files
	command.Stdout, command.Stderr = diagnostic, diagnostic
	if err := command.Start(); err != nil {
		t.Fatalf("restart member %d: %v", child.member, err)
	}
	for index := range files {
		_ = files[index].Close()
		files[index] = nil
		_ = listeners[index].Close()
		listeners[index] = nil
	}
	child.mu.Lock()
	child.command, child.exited, child.diagnostic = command, make(chan struct{}), diagnostic
	child.waitErr = nil
	child.mu.Unlock()
	go func() {
		err := command.Wait()
		child.mu.Lock()
		child.waitErr = err
		child.mu.Unlock()
		close(child.exited)
	}()
	waitRF3CommandReady(t, child, 30*time.Second)
}

type rf3CommandControlOpener struct {
	profile   *rafttransport.PeerTLS
	addresses map[rafttransport.NodeID]string
	deadline  rafttransport.DeadlineFunc
}

func (opener rf3CommandControlOpener) OpenShardControl(
	ctx context.Context, node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	address, found := opener.addresses[node]
	if opener.profile == nil || opener.deadline == nil || !found {
		return nil, errors.New("invalid RF3 command control endpoint")
	}
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	connection, err := opener.profile.Client(
		ctx, raw, node, rafttransport.TrafficShardControl, opener.deadline,
	)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	return connection, nil
}

func rf3CommandFindLeader(
	t testing.TB,
	addresses []string,
	nodes []rafttransport.NodeID,
	profile *rafttransport.PeerTLS,
	authorityNode rafttransport.NodeID,
	group raftmember.GroupKey,
	allocation, generation uint64,
) (int, shardservice.ReplicatedMemberState) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		for index := range addresses {
			state, err := probeRF3CommandMember(
				ctx, addresses[index], nodes[index], profile, authorityNode,
				group, allocation, generation,
			)
			if err == nil && state.Fence.MemberID == state.LeaderID && state.Fence.Term != 0 {
				return index, state
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("RF3 leader unavailable: %v", ctx.Err())
	return 0, shardservice.ReplicatedMemberState{}
}

func rf3CommandRoundTrip(
	t testing.TB,
	address string,
	serverNode rafttransport.NodeID,
	profile *rafttransport.PeerTLS,
	request *shardservice.ReplicatedRequest,
) *shardservice.ReplicatedResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	deadline := func() time.Time { return time.Now().Add(10 * time.Second) }
	connection, err := profile.Client(
		ctx, raw, serverNode, rafttransport.TrafficShardNative, deadline,
	)
	if err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	defer connection.Close()
	response, err := shardservice.RoundTripReplicated(ctx, connection, request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func rf3CommandUnusedAddress(t testing.TB) string {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func rf3CommandServingFence(fence shardservice.ReplicatedFence) raftservice.ServingFence {
	return raftservice.ServingFence{
		Group: fence.Group, AllocationGeneration: fence.AllocationGeneration,
		Command: fence.Command, MemberID: fence.MemberID, StoreID: fence.StoreID,
		NodeIncarnation: fence.NodeIncarnation, Term: fence.Term,
	}
}

func rf3CommandExecuteAction(
	t testing.TB,
	client *replicaaction.Client,
	node rafttransport.NodeID,
	request replicaaction.Request,
) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = client.Execute(ctx, node, request)
		cancel()
		if err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("replica action did not settle: %v", err)
}

func rf3CommandWaitLeaderWitness(
	t testing.TB,
	addresses []string,
	nodes []rafttransport.NodeID,
	profile *rafttransport.PeerTLS,
	authorityNode rafttransport.NodeID,
	group raftmember.GroupKey,
	allocation, generation, leaderMember, afterTerm uint64,
) shardservice.ReplicatedMemberState {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		for index := range addresses {
			state, err := probeRF3CommandMember(
				ctx, addresses[index], nodes[index], profile, authorityNode,
				group, allocation, generation,
			)
			if err == nil && state.LeaderID == leaderMember && state.Fence.Term > afterTerm {
				return state
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("leader %d witness unavailable after term %d", leaderMember, afterTerm)
	return shardservice.ReplicatedMemberState{}
}

func rf3CommandApplyMembership(
	t testing.TB,
	addresses []string,
	nodes []rafttransport.NodeID,
	profile *rafttransport.PeerTLS,
	observer *replicacontrol.Client,
	authority serviceauthz.Authority,
	group raftmember.GroupKey,
	allocation uint64,
	request shardservice.ReplicatedMembershipRequest,
) shardservice.ReplicatedMemberState {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	for time.Now().Before(deadline) {
		leader, state := rf3CommandFindLeader(
			t, addresses, nodes, profile, authority.Node, group, allocation,
			authority.Generation,
		)
		request.ExpectedReplicaSetVersion = state.Fence.Command.ReplicaSetVersion
		observationRequest := replicacontrol.Request{
			Operation: sha256.Sum256(request.TransitionID[:]), Step: [32]byte{0x6d, byte(request.Kind)},
			Group: group, TargetMember: request.TargetMember,
			ExpectedReplicaSetVersion: request.ExpectedReplicaSetVersion,
		}
		before, err := observer.Observe(ctx, nodes[leader], observationRequest)
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		response := rf3CommandRoundTrip(t, addresses[leader], nodes[leader], profile,
			&shardservice.ReplicatedRequest{
				Operation: shardservice.ReplicatedMembership, Authority: authority,
				Capability: serviceauthz.CapabilityMembership, Fence: state.Fence,
				Membership: request,
			})
		switch response.Kind {
		case shardservice.ReplicatedMembershipAccepted:
			if !response.HasState {
				t.Fatal("membership admission omitted member identity")
			}
			// Accepted is admission, not quorum/apply settlement. Once accepted,
			// never submit this mutation again; observe its exact resulting cut.
			settled, err := rf3AwaitMembershipSettlement(ctx, before, request,
				rf3MembershipNetworkObserver(observer, addresses[leader], nodes[leader], profile, authority, allocation, observationRequest))
			if err != nil {
				t.Fatalf("accepted membership did not settle exact cut: %v", err)
			}
			return settled
		case shardservice.ReplicatedNotLeader, shardservice.ReplicatedOutcomeUnknown:
			time.Sleep(10 * time.Millisecond)
			continue
		case shardservice.ReplicatedRefusal:
			if response.Refusal == shardservice.ReplicatedRefusalMembershipNotCaughtUp {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			t.Fatalf("membership refused: %+v", response)
		default:
			t.Fatalf("membership refused: %+v", response)
		}
	}
	t.Fatal("membership action did not settle")
	return shardservice.ReplicatedMemberState{}
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

// Raw member fixtures bypass prepare-rf3, so retain its required runtime
// namespace explicitly before handing the artifacts to the serving command.
func prepareRF3CommandSplitRuntime(t testing.TB, memberRoot string, bootstrap raftstore.Bootstrap) {
	t.Helper()
	if err := rf3testfixture.PrepareSplitRuntime(memberRoot, bootstrap); err != nil {
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
	identity raftstore.Identity,
	topologyRecoveryEpoch uint64,
) []byte {
	dataRoot := filepath.Dir(sqlPath)
	if absolute, err := filepath.Abs(dataRoot); err == nil {
		dataRoot = absolute
	}
	paths := []*string{&walPath, &sqlPath, &basePath, &applyPath, &keyPath}
	for _, path := range paths {
		if !filepath.IsAbs(*path) {
			*path = filepath.Join(dataRoot, *path)
		}
	}
	return []byte(fmt.Sprintf(`{"wal":{"path":%q,"key_id":"rf3-command-key","key_material_path":%q,"max_file_bytes":%d,"max_record_bytes":%d,"max_records":%d,"max_entries":%d,"max_live_bytes":%d},"sql":{"path":%q,"identity_path":%q,"apply_identity_path":%q},"route":{"cluster_id":"%x","cluster_incarnation":"%x","topology_recovery_epoch":%d,"shard_incarnation":"%x","group_id":"%x","distribution":%q,"shard":%q,"allocation_generation":%d,"member_id":%d,"store_id":"%x","member_root":%q,"split_runtime_root":%q,"membership_grant_path":%q},"listeners":{"peer":%q,"native":%q,"snapshot":%q,"control":%q},"tls":{"certificate":%q,"key":%q,"roots":%q,"identity_oid":"1.3.6.1.4.1.32473.1.1"},"authorization_policy":%q,"replica_control":{"action_journal_path":%q,"max_action_records":4096,"source_data_root":%q,"source_journal_path":%q,"max_source_records":4096,"source_repository_path":%q,"max_source_artifacts":8,"max_source_concurrent":2,"max_source_artifact_bytes":1073741824,"max_source_disk_bytes":4294967296,"source_chunk_bytes":1048576},"split_control":{"journal_path":%q,"max_records":4096,"max_file_bytes":67108864,"grants":[{"node_id":"%x","actions":65535},{"node_id":"%x","actions":65535},{"node_id":"%x","actions":65535}],"child_registry":{"root":%q,"max_operations":8,"stage_checkpoint_bytes":33554432,"table":"controlplane","create_table":"CREATE TABLE controlplane (PRIMARY KEY (id))","wal":{"key_id":"rf3-command-key","key_material_path":%q,"max_file_bytes":%d,"max_record_bytes":%d,"max_records":%d,"max_entries":%d,"max_live_bytes":%d},"apply":{"max_sessions":32,"retry_window":8,"max_collections":16,"max_documents":1024,"max_bytes":402653184,"request_ledger_capacity_bytes":0,"request_ledger_cleanup_reserve_bytes":0,"request_ledger_range_start":"","request_ledger_range_end":"","request_ledger_range_identity":"","format":0,"shard_key":"/id","tuple_version":1,"mapper_version":1},"static_bootstrap_path":%q,"replica_set_version":1,"members":[{"member_id":1,"node_id":"%x","peer_address":%q},{"member_id":2,"node_id":"%x","peer_address":%q},{"member_id":3,"node_id":"%x","peer_address":%q}]}},"members":[{"member_id":1,"node_id":"%x","peer_address":%q},{"member_id":2,"node_id":"%x","peer_address":%q},{"member_id":3,"node_id":"%x","peer_address":%q}]}`,
		walPath, keyPath,
		options.MaxFileBytes, options.MaxRecordBytes, options.MaxRecords,
		options.MaxEntries, options.MaxLiveBytes,
		sqlPath, basePath, applyPath,
		identity.ClusterID, identity.ClusterIncarnation, topologyRecoveryEpoch,
		identity.ShardIncarnation, identity.GroupID, identity.Distribution, identity.Shard,
		identity.AllocationGeneration, identity.MemberID, identity.StoreID,
		dataRoot, filepath.Join(dataRoot, "split-runtime"), filepath.Join(dataRoot, "membership-grant"),
		peerAddress, nativeAddress, snapshotAddress, controlAddress,
		credential.Certificate, credential.Key, roots, policyPath,
		filepath.Join(dataRoot, "replica-actions"), dataRoot,
		filepath.Join(dataRoot, "source-exports"), filepath.Join(dataRoot, "source-artifacts"),
		filepath.Join(dataRoot, "split-control.journal"), nodes[0], nodes[1], nodes[2],
		filepath.Join(dataRoot, "split-children"), keyPath,
		options.MaxFileBytes, options.MaxRecordBytes, options.MaxRecords,
		options.MaxEntries, options.MaxLiveBytes,
		filepath.Join(dataRoot, "split-children", "static-bootstrap.pb"),
		nodes[0], peerAddresses[0], nodes[1], peerAddresses[1], nodes[2], peerAddresses[2],
		nodes[0], peerAddresses[0], nodes[1], peerAddresses[1], nodes[2], peerAddresses[2],
	))
}

func rf3CommandPolicyWithTarget(
	nodes [rf3CommandMembers]rafttransport.NodeID,
	target rafttransport.NodeID,
	clients ...rafttransport.NodeID,
) []byte {
	policy := []byte(fmt.Sprintf(
		`{"generation":5,"principals":[{"node":"%x","capabilities":["delegate","membership","topology"]},{"node":"%x","capabilities":["delegate","membership","topology"]},{"node":"%x","capabilities":["delegate","membership","topology"]},{"node":"%x","capabilities":["data_read","data_write","delegate","membership","topology","transaction_recovery","request_ledger","execution_pin"]}]}`,
		nodes[0], nodes[1], nodes[2], target,
	))
	policy = policy[:len(policy)-2]
	for _, client := range clients {
		policy = fmt.Appendf(policy, `,{"node":"%x","capabilities":["data_read","data_write","delegate","membership","topology","transaction_recovery","request_ledger","execution_pin"]}`, client)
	}
	return append(policy, ']', '}')
}

func rf3CompositionClientNodes() (client, gateway rafttransport.NodeID) {
	return rafttransport.NodeID{0xa1, 1}, rafttransport.NodeID{0xb1, 1}
}

func rf3CommandEnrollTarget(
	document []byte,
	node rafttransport.NodeID,
	store [16]byte,
	incarnation uint64,
	peer, native, snapshot, control string,
) []byte {
	if len(document) == 0 || document[len(document)-1] != '}' {
		panic("invalid RF3 command document")
	}
	document = document[:len(document)-1]
	return fmt.Appendf(document,
		`,"enrolled_target":{"member_id":4,"node_id":"%x","store_id":"%x","node_incarnation":%d,"peer_address":%q,"native_address":%q,"snapshot_address":%q,"control_address":%q}}`,
		node, store, incarnation, peer, native, snapshot, control,
	)
}
