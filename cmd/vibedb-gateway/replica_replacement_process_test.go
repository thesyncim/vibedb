//go:build darwin || linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/asn1"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/storeio"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

// TestGatewayAutomaticReplicaReplacementProcesses is Linux qualification, not
// a unit simulation. Three serve-rf3 processes and one cold bootstrap-rf3
// process are driven by the shipped gateway catalog, health, grant, and move
// controllers. Darwin retains compile coverage but skips because its durable
// allocation contract cannot provide the Linux power-loss boundary.
func TestGatewayAutomaticReplicaReplacementProcesses(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("external replica replacement requires Linux durable allocation semantics")
	}
	if os.Getenv("VIBEDB_REPLICA_REPLACEMENT_E2E") != "1" {
		t.Skip("set VIBEDB_REPLICA_REPLACEMENT_E2E=1 for external replica replacement qualification")
	}
	if testing.Short() {
		t.Skip("external four-process RF3 qualification")
	}
	evidencePath := os.Getenv("VIBEDB_REPLICA_REPLACEMENT_EVIDENCE")
	if evidencePath == "" {
		t.Fatal("VIBEDB_REPLICA_REPLACEMENT_EVIDENCE is required for qualification")
	}
	qualificationStarted := time.Now()
	phase, finalGeneration := "setup", uint64(0)
	defer func() {
		result := "pass"
		if t.Failed() {
			result = "fail"
		}
		raw := fmt.Appendf(nil,
			"schema\tvibedb.replica-replacement-process\t1\nresult\t%s\nphase\t%s\nelapsed_millis\t%d\nfinal_catalog_generation\t%d\n",
			result, phase, time.Since(qualificationStarted).Milliseconds(), finalGeneration)
		if len(raw) > 64<<10 {
			t.Errorf("replica replacement evidence exceeded 64 KiB")
			return
		}
		if err := os.WriteFile(evidencePath, raw, 0o600); err != nil {
			t.Errorf("write replica replacement evidence: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	root := t.TempDir()
	topology, err := rf3testfixture.ReserveProcessCluster()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = topology.Close() })
	listeners := topology.Members()
	gatewayReservation, err := rf3testfixture.ReserveLoopbackAddresses(2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gatewayReservation.Close() })
	gatewayNative, gatewayControl := gatewayReservation.Addresses[0], gatewayReservation.Addresses[1]

	nodes := replicaProcessNodes()
	identities := replicaProcessIdentities()
	group := replicaProcessGroup(identities[0])
	credentials, roots, err := rf3testfixture.WriteCredentials(
		root, asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1},
		rafttransport.TrustDomain{ClusterID: group.ClusterID,
			ClusterIncarnation: group.ClusterIncarnation}, nodes[:],
	)
	if err != nil {
		t.Fatal(err)
	}
	policyPath := filepath.Join(root, "authorization-policy.vibejson")
	if err = os.WriteFile(policyPath, replicaProcessPolicy(nodes), 0o600); err != nil {
		t.Fatal(err)
	}
	wal := raftstore.Options{MaxFileBytes: 256 << 20,
		MaxRecordBytes: raftstore.DefaultMaxRecordBytes, MaxRecords: 4096,
		MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes}
	authority := sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: 5,
		ProtectionEpoch: 7, OwnershipEpoch: 11, SchemaGeneration: 13,
		RoutingVersion: 17, RouteGeneration: 19}
	apply := sqldriver.ReplicatedApplyOptions{MaxSessions: 64, RetryWindow: 16,
		TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024,
			MaxBytes: 384 << 20}, Placement: sqldriver.ReplicatedPlacementProfile{
			Format:        sqldriver.ReplicatedPlacementProfileFormat,
			ShardKey:      gateway.ReplicatedCatalogPrimaryKey,
			TupleVersion:  distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			Range:         distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		}}
	key := raftstore.Key{ID: "replica-process-key", Wrapped: []byte("test-wrapped-key")}
	for index := range key.Material {
		key.Material[index] = byte(index + 1)
	}

	// Learn the deterministic logical relation digest before creating the
	// catalog document. The probe is never served and is removed immediately.
	probe, err := rf3testfixture.PrepareMember(rf3testfixture.MemberOptions{
		Root: filepath.Join(root, "relation-probe"), Table: gateway.ReplicatedCatalogTable,
		CreateTable: "CREATE TABLE controlplane (PRIMARY KEY (id))",
		Identity:    identities[0], Key: key, WAL: wal,
		Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
		Authority: authority, Apply: apply,
	})
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) ||
		errors.Is(err, raftstore.ErrPlatformUnsupported) {
		t.Skipf("external RF3 durable allocation unsupported: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	relationDigest := probe.Base.RelationManifestDigest
	if err = probe.Close(); err != nil {
		t.Fatal(err)
	}

	snapshot := replicaProcessCatalog(t, nodes, identities, listeners, group,
		authority, relationDigest)
	catalogPath := filepath.Join(root, "catalog.vibejson")
	if err = gateway.SaveSnapshot(catalogPath, snapshot); err != nil {
		t.Fatal(err)
	}
	seed := replicaProcessCatalogSeed(t, snapshot)
	var servingNodes [3]rafttransport.NodeID
	var servingPeers [3]string
	copy(servingNodes[:], nodes[:3])
	for index := range servingPeers {
		servingPeers[index] = listeners[index].Peer
	}
	target := rf3testfixture.ProcessTarget{MemberID: 4, NodeID: nodes[3],
		StoreID: identities[3].StoreID, NodeIncarnation: 44, Listeners: listeners[3]}
	prepared := make([]rf3testfixture.PreparedProcessMember, 3)
	for index := range prepared {
		prepared[index], err = rf3testfixture.PrepareProcessMember(
			rf3testfixture.ProcessMemberOptions{
				Root:        filepath.Join(root, fmt.Sprintf("member-%d", index+1)),
				Table:       gateway.ReplicatedCatalogTable,
				CreateTable: "CREATE TABLE controlplane (PRIMARY KEY (id))",
				Identity:    identities[index], Key: key, WAL: wal,
				Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
				Authority: authority, Apply: apply, Listeners: listeners[index],
				Credential: credentials[index], Roots: roots,
				AuthorizationPolicy: policyPath, Nodes: servingNodes,
				PeerAddresses: servingPeers, Target: &target, SeedDocuments: seed,
			})
		if err != nil {
			t.Fatal(err)
		}
	}
	cold, err := rf3testfixture.PrepareColdProcessTarget(
		rf3testfixture.ProcessMemberOptions{
			Root: filepath.Join(root, "member-4"), Table: gateway.ReplicatedCatalogTable,
			CreateTable: "CREATE TABLE controlplane (PRIMARY KEY (id))",
			Identity:    identities[3], Key: key, WAL: wal,
			Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
			Authority: authority, Apply: apply, Listeners: listeners[3],
			Credential: credentials[3], Roots: roots, AuthorizationPolicy: policyPath,
			Nodes: servingNodes, PeerAddresses: servingPeers, Target: &target,
			SeedDocuments: seed,
		}, nodes[0], listeners[0].Snapshot, 1<<30,
	)
	if err != nil {
		t.Fatal(err)
	}
	replicaManifestPath := filepath.Join(root, "replica-control.vibejson")
	if err = os.WriteFile(replicaManifestPath, replicaProcessControlManifest(t,
		nodes, identities, listeners, credentials[4], roots, policyPath,
		gatewayControl, snapshot.Generation()), 0o600); err != nil {
		t.Fatal(err)
	}
	ackPath := filepath.Join(root, "durable-ack-key")
	if err = os.WriteFile(ackPath, []byte(strings.Repeat("3a", 32)), 0o600); err != nil {
		t.Fatal(err)
	}
	shardBinary := filepath.Join(root, "vibedb-shard")
	gatewayBinary := filepath.Join(root, "vibedb-gateway")
	replicaProcessBuild(t, ctx, shardBinary, "./cmd/vibedb-shard")
	replicaProcessBuild(t, ctx, gatewayBinary, "./cmd/vibedb-gateway")
	if err = errors.Join(topology.ReleaseListeners(), gatewayReservation.Close()); err != nil {
		t.Fatal(err)
	}

	voters := [3]*rf3testfixture.ExternalProcess{}
	for index := range voters {
		voters[index] = &rf3testfixture.ExternalProcess{Binary: shardBinary,
			Args: []string{"serve-rf3", "-manifest", prepared[index].ManifestPath}}
		if err = voters[index].Start(); err != nil {
			t.Fatal(err)
		}
		defer replicaProcessStop(t, voters[index])
	}
	for _, voter := range voters {
		if err = voter.WaitReady(ctx, "vibedb-shard RF3 ready"); err != nil {
			t.Fatalf("voter readiness: %v\n%s", err, voter.Diagnostics())
		}
	}
	phase = "voters_ready"
	coldTarget := &rf3testfixture.ExternalProcess{Binary: shardBinary,
		Args: []string{"bootstrap-rf3", "-manifest", cold.BootstrapManifestPath}}
	if err = coldTarget.Start(); err != nil {
		t.Fatal(err)
	}
	defer replicaProcessStop(t, coldTarget)
	if err = coldTarget.WaitReady(ctx, "vibedb-shard RF3 cold bootstrap ready"); err != nil {
		t.Fatalf("cold target readiness: %v\n%s", err, coldTarget.Diagnostics())
	}
	phase = "cold_target_ready"
	gatewayProcess := replicaProcessGateway(gatewayBinary, catalogPath, gatewayNative,
		replicaManifestPath, credentials[4], roots, policyPath, ackPath,
		filepath.Join(root, "gateway-session"), listeners, nodes)
	if err = gatewayProcess.Start(); err != nil {
		t.Fatal(err)
	}
	defer replicaProcessStop(t, gatewayProcess)
	if err = gatewayProcess.WaitReady(ctx, "vibedb-gateway serving catalog generation"); err != nil {
		t.Fatalf("gateway readiness: %v\n%s", err, gatewayProcess.Diagnostics())
	}
	phase = "gateway_ready"

	started := time.Now()
	if err = voters[0].Kill(ctx); err != nil {
		t.Fatal(err)
	}
	// Restart the controller once while the failure window or move journal is
	// live. No process-local queue may be needed for eventual completion.
	time.Sleep(150 * time.Millisecond)
	if err = gatewayProcess.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if err = gatewayProcess.Start(); err != nil {
		t.Fatal(err)
	}
	if err = gatewayProcess.WaitReady(ctx, "vibedb-gateway serving catalog generation"); err != nil {
		t.Fatalf("restarted gateway readiness: %v\n%s", err, gatewayProcess.Diagnostics())
	}
	phase = "controller_restarted"

	// The old member is reopened after the final roster can be observed. This
	// gives the durable retirement action a live endpoint for certified cleanup,
	// while its stale WAL still cannot recover serving authority.
	restartedSource := false
	sourceReady := false
	for {
		if err = ctx.Err(); err != nil {
			t.Fatalf("replacement timeout: %v\ngateway:\n%s\ntarget:\n%s",
				err, gatewayProcess.Diagnostics(), coldTarget.Diagnostics())
		}
		current, loadErr := gateway.LoadSnapshot(catalogPath)
		if loadErr == nil {
			descriptors := current.ReplicatedShardDescriptors()
			if len(descriptors) == 1 && replicaProcessRosterContains(descriptors[0], 4) &&
				!replicaProcessRosterContains(descriptors[0], 1) {
				if !restartedSource {
					if err = voters[0].Start(); err != nil {
						t.Fatal(err)
					}
					restartedSource = true
				}
				if !sourceReady && strings.Contains(voters[0].Diagnostics(), "vibedb-shard RF3 ready") {
					sourceReady = true
				}
				if strings.Contains(coldTarget.Diagnostics(), "vibedb-shard RF3 ready") &&
					sourceReady && strings.Contains(gatewayProcess.Diagnostics(), "completed 1") &&
					replicaProcessAllArtifactsEmpty(root) &&
					time.Since(started) < 90*time.Second {
					break
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !restartedSource {
		t.Fatal("retired source was not reopened for cleanup")
	}
	phase = "replacement_complete"
	if elapsed := time.Since(started); elapsed > 90*time.Second {
		t.Fatalf("replacement latency %s exceeded bound", elapsed)
	}
	if !replicaProcessTreeBounded(t, root, "gateway-session", 32, 64<<20) ||
		!replicaProcessTreeBounded(t, filepath.Join(root, "member-1"),
			"replica-actions", 4097, 64<<20) {
		t.Fatal("controller state exceeded its process qualification bound")
	}
	if err = gatewayProcess.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if err = gatewayProcess.Start(); err != nil {
		t.Fatal(err)
	}
	if err = gatewayProcess.WaitReady(ctx, "vibedb-gateway serving catalog generation"); err != nil {
		t.Fatalf("terminal controller reopen: %v\n%s", err, gatewayProcess.Diagnostics())
	}
	final, err := gateway.LoadSnapshot(catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	descriptors := final.ReplicatedShardDescriptors()
	if len(descriptors) != 1 || !replicaProcessRosterContains(descriptors[0], 4) ||
		replicaProcessRosterContains(descriptors[0], 1) {
		t.Fatalf("retired source rejoined after controller restart: %+v", descriptors)
	}
	finalGeneration = final.Generation()
	phase = "terminal_reopen_verified"
}

func replicaProcessNodes() (nodes [5]rafttransport.NodeID) {
	for member := range nodes {
		for index := range nodes[member] {
			nodes[member][index] = byte((member+1)*23 + index)
		}
	}
	return nodes
}

func replicaProcessIdentities() (identities [4]raftstore.Identity) {
	for member := range identities {
		identities[member] = raftstore.Identity{Distribution: string(gateway.ReplicatedCatalogDistribution),
			Shard: string(gateway.ReplicatedCatalogShard), AllocationGeneration: 23,
			MemberID: uint64(member + 1)}
		for index := range identities[member].ClusterID {
			identities[member].ClusterID[index] = byte(index + 1)
			identities[member].ClusterIncarnation[index] = byte(index + 21)
			identities[member].ShardIncarnation[index] = byte(index + 141)
			identities[member].GroupID[index] = byte(index + 161)
			identities[member].StoreID[index] = byte(index+181) ^ byte(member+1)
		}
	}
	return identities
}

func replicaProcessGroup(identity raftstore.Identity) raftmember.GroupKey {
	return raftmember.GroupKey{ClusterID: identity.ClusterID,
		ClusterIncarnation: identity.ClusterIncarnation, TopologyRecoveryEpoch: 3,
		ShardIncarnation: identity.ShardIncarnation, GroupID: identity.GroupID}
}

func replicaProcessPolicy(nodes [5]rafttransport.NodeID) []byte {
	var raw []byte
	raw = append(raw, `{"generation":5,"principals":[`...)
	for index, node := range nodes {
		if index != 0 {
			raw = append(raw, ',')
		}
		raw = fmt.Appendf(raw, `{"node":"%x","capabilities":["data_read","data_write","schema","delegate","membership","topology","transaction_recovery","request_ledger","execution_pin"]}`, node)
	}
	return append(raw, ']', '}')
}

func replicaProcessCatalog(t testing.TB, nodes [5]rafttransport.NodeID,
	identities [4]raftstore.Identity, listeners [4]rf3testfixture.ProcessListeners,
	group raftmember.GroupKey, authority sqldriver.ReplicatedAuthorityProfile,
	relationDigest [32]byte,
) *gateway.Snapshot {
	t.Helper()
	leaders := []distribution.EndpointID{"member-1-peer", "member-2-peer", "member-3-peer"}
	manifest, err := distribution.NewManifest(gateway.ReplicatedCatalogDistribution,
		distribution.RoutingVersion(authority.RoutingVersion), []distribution.Shard{{
			ID:                   gateway.ReplicatedCatalogShard,
			AllocationGeneration: distribution.ShardAllocationGeneration(identities[0].AllocationGeneration),
			Range:                distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
			Leaders:              leaders, Epoch: distribution.OwnershipEpoch(authority.OwnershipEpoch),
		}})
	if err != nil {
		t.Fatal(err)
	}
	endpoints := make(map[distribution.EndpointID]string, 12)
	replicas := make([]gateway.ReplicatedReplicaDescriptor, 3)
	for index := range replicas {
		peer := distribution.EndpointID(fmt.Sprintf("member-%d-peer", index+1))
		native := distribution.EndpointID(fmt.Sprintf("member-%d-native", index+1))
		control := distribution.EndpointID(fmt.Sprintf("member-%d-control", index+1))
		endpoints[peer], endpoints[native], endpoints[control] = listeners[index].Peer,
			listeners[index].Native, listeners[index].Control
		replicas[index] = gateway.ReplicatedReplicaDescriptor{Member: uint64(index + 1),
			Node: nodes[index], StoreID: identities[index].StoreID,
			NodeIncarnation: uint64(41 + index), Endpoint: peer,
			NativeEndpoint: native, ControlEndpoint: control}
	}
	targetPeer, targetNative, targetControl := distribution.EndpointID("member-4-peer"),
		distribution.EndpointID("member-4-native"), distribution.EndpointID("member-4-control")
	endpoints[targetPeer], endpoints[targetNative], endpoints[targetControl] = listeners[3].Peer,
		listeners[3].Native, listeners[3].Control
	descriptor := gateway.ReplicatedShardDescriptor{
		Distribution: gateway.ReplicatedCatalogDistribution, Shard: gateway.ReplicatedCatalogShard,
		Group: group, AllocationGeneration: distribution.ShardAllocationGeneration(identities[0].AllocationGeneration),
		Command: raftservice.CommandFence{ReplicaSetVersion: 1,
			ActivePolicyGeneration: authority.ActivePolicyGeneration,
			ProtectionEpoch:        authority.ProtectionEpoch, OwnershipEpoch: authority.OwnershipEpoch,
			SchemaGeneration: authority.SchemaGeneration, RelationManifestDigest: relationDigest,
			RoutingVersion: authority.RoutingVersion, RouteGeneration: authority.RouteGeneration},
		RangeIdentity: replication.Digest{0x71}, LineageDigest: replication.Digest{0x72},
		ForwardingRuleDigest: replication.Digest{0x73}, Replicas: replicas,
		EnrolledTarget: &gateway.ReplicatedReplicaDescriptor{Member: 4, Node: nodes[3],
			StoreID: identities[3].StoreID, NodeIncarnation: 44, Endpoint: targetPeer,
			NativeEndpoint: targetNative, ControlEndpoint: targetControl},
	}
	config := distribution.ClusterConfig{Distributions: []distribution.DistributionSpec{{
		Name: gateway.ReplicatedCatalogDistribution, Arity: 1,
		MapperVersion: distribution.NativeMapperVersion}}, Placements: []distribution.TablePlacement{{
		Table: gateway.ReplicatedCatalogTable, Distribution: gateway.ReplicatedCatalogDistribution,
		Columns: []string{gateway.ReplicatedCatalogPrimaryKey}}}, Manifests: []*distribution.Manifest{manifest}}
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(config, endpoints, 1, nil, nil,
		[]gateway.ReplicatedShardDescriptor{descriptor})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func replicaProcessCatalogSeed(t testing.TB, snapshot *gateway.Snapshot) [][]byte {
	t.Helper()
	payload, err := gateway.AppendSnapshotDocument(nil, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	head := append([]byte(`{"id":"catalog/head","payload":`), payload...)
	head = append(head, '}')
	witnessPayload, err := vibejson.Marshal(&struct {
		Generation uint64   `json:"generation"`
		HeadBytes  uint64   `json:"head_bytes"`
		HeadDigest [32]byte `json:"head_digest"`
	}{Generation: snapshot.Generation(), HeadBytes: uint64(len(head)), HeadDigest: sha256.Sum256(head)})
	if err != nil {
		t.Fatal(err)
	}
	witness := append([]byte(`{"id":"catalog/witness","payload":`), witnessPayload...)
	witness = append(witness, '}')
	return [][]byte{head, witness}
}

func replicaProcessControlManifest(t testing.TB, nodes [5]rafttransport.NodeID,
	identities [4]raftstore.Identity, listeners [4]rf3testfixture.ProcessListeners,
	credential rf3testfixture.Credential, roots, policy, gatewayControl string,
	generation uint64,
) []byte {
	t.Helper()
	manifest := persistedGatewayReplicaControlManifest{Generation: generation,
		LocalGateway: persistedGatewayControlEndpoint{Node: fmt.Sprintf("%x", nodes[4]),
			Incarnation: 1, ControlAddress: gatewayControl},
		TLS: persistedGatewayReplicaTLS{Certificate: credential.Certificate, Key: credential.Key,
			Roots: roots, IdentityOID: rf3testfixture.ProcessIdentityOID,
			AuthorizationPolicy: policy},
		Bounds: persistedGatewayReplicaBounds{MaxConnections: 32, MaxHandshakes: 8,
			MaxConcurrentDrains: 4, ControllerInterval: 20, ReadTimeout: 2000,
			WriteTimeout: 5000},
		GatewayEndpoints: []persistedGatewayControlEndpoint{{Node: fmt.Sprintf("%x", nodes[4]),
			Incarnation: 1, ControlAddress: gatewayControl}},
		Candidates: []persistedGatewayReplacementCandidate{{Member: 4,
			Node: fmt.Sprintf("%x", nodes[3]), Store: fmt.Sprintf("%x", identities[3].StoreID),
			NodeIncarnation: 44, Endpoint: "member-4-peer", Load: 0}},
	}
	for index := 0; index < 4; index++ {
		manifest.ShardEndpoints = append(manifest.ShardEndpoints,
			persistedGatewayShardControlEndpoint{Node: fmt.Sprintf("%x", nodes[index]),
				ControlAddress: listeners[index].Control})
	}
	slices.SortFunc(manifest.ShardEndpoints, func(left, right persistedGatewayShardControlEndpoint) int {
		return strings.Compare(left.Node, right.Node)
	})
	raw, err := vibejson.Marshal(&manifest)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func replicaProcessBuild(t testing.TB, ctx context.Context, output, pkg string) {
	t.Helper()
	command := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", output, pkg)
	command.Dir = replicaProcessModuleRoot(t)
	if raw, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", pkg, err, raw)
	}
}

func replicaProcessModuleRoot(t testing.TB) string {
	t.Helper()
	command := exec.Command("go", "env", "GOMOD")
	raw, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(strings.TrimSpace(string(raw)))
}

func replicaProcessGateway(binary, catalog, listen, controlManifest string,
	credential rf3testfixture.Credential, roots, policy, ack, journal string,
	listeners [4]rf3testfixture.ProcessListeners, nodes [5]rafttransport.NodeID,
) *rf3testfixture.ExternalProcess {
	args := []string{"serve", "-catalog", catalog, "-catalog-relation", "1",
		"-catalog-session-journal", journal, "-durable-ack-key", ack,
		"-catalog-client-id", strings.Repeat("1a", 16),
		"-catalog-retry-home", strings.Repeat("2b", 8),
		"-catalog-attempts", "16", "-catalog-attempt-timeout", "2s",
		"-catalog-session-lease", "1h", "-controller-interval", "20ms",
		"-listen", listen, "-tls-certificate", credential.Certificate,
		"-tls-key", credential.Key, "-tls-roots", roots,
		"-tls-identity-oid", rf3testfixture.ProcessIdentityOID,
		"-authorization-policy", policy, "-replica-control-manifest", controlManifest}
	for index := 0; index < 4; index++ {
		args = append(args, "-shard-peer", listeners[index].Native+"="+fmt.Sprintf("%x", nodes[index]))
	}
	return &rf3testfixture.ExternalProcess{Binary: binary, Args: args}
}

func replicaProcessRosterContains(descriptor gateway.ReplicatedShardDescriptor, member uint64) bool {
	for _, replica := range descriptor.Replicas {
		if replica.Member == member {
			return true
		}
	}
	return false
}

func replicaProcessArtifactsEmpty(path string) bool {
	entries, err := os.ReadDir(path)
	return errors.Is(err, os.ErrNotExist) || err == nil && len(entries) == 0
}

func replicaProcessAllArtifactsEmpty(root string) bool {
	for member := 1; member <= 3; member++ {
		if !replicaProcessArtifactsEmpty(filepath.Join(root,
			fmt.Sprintf("member-%d", member), "source-artifacts")) {
			return false
		}
	}
	return true
}

func replicaProcessTreeBounded(t testing.TB, root, prefix string, maxFiles int,
	maxBytes int64,
) bool {
	t.Helper()
	files, bytes := 0, int64(0)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		withinDirectory := strings.Contains(path,
			string(filepath.Separator)+prefix+string(filepath.Separator))
		if entry.IsDir() || !strings.HasPrefix(filepath.Base(path), prefix) && !withinDirectory {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		files++
		bytes += info.Size()
		return nil
	})
	if err != nil {
		t.Errorf("measure %s/%s: %v", root, prefix, err)
		return false
	}
	return files <= maxFiles && bytes <= maxBytes
}

func replicaProcessStop(t testing.TB, process *rf3testfixture.ExternalProcess) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := process.Stop(ctx); err != nil && !strings.Contains(err.Error(), "signal: killed") {
		t.Errorf("process cleanup: %v\n%s", err, process.Diagnostics())
	}
}
