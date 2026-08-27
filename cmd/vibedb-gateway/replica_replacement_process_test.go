//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/asn1"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rebalanceexec"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
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
	evidenceRoot := os.Getenv("VIBEDB_REPLICA_REPLACEMENT_EVIDENCE")
	if evidenceRoot == "" {
		t.Fatal("VIBEDB_REPLICA_REPLACEMENT_EVIDENCE is required for qualification")
	}
	if err := os.MkdirAll(evidenceRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	qualificationStarted := time.Now()
	phase, finalGeneration := "setup", uint64(0)
	var measurements replicaProcessMeasurements
	defer func() {
		result := "pass"
		if t.Failed() {
			result = "fail"
		}
		raw := fmt.Appendf(nil,
			"schema\tvibedb.replica-replacement-process\t2\nresult\t%s\nphase\t%s\nelapsed_millis\t%d\nfinal_catalog_generation\t%d\n",
			result, phase, time.Since(qualificationStarted).Milliseconds(), finalGeneration)
		raw = measurements.appendTSV(raw)
		if len(raw) > 64<<10 {
			t.Errorf("replica replacement evidence exceeded 64 KiB")
			return
		}
		evidencePath := filepath.Join(evidenceRoot, fmt.Sprintf("run-%d-%d.tsv", os.Getpid(), qualificationStarted.UnixNano()))
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
	secondaryIdentities := replicaProcessSecondaryIdentities(identities)
	groups := [2]raftmember.GroupKey{
		replicaProcessGroup(identities[0]), replicaProcessGroup(secondaryIdentities[0]),
	}
	credentials, roots, err := rf3testfixture.WriteCredentials(
		root, asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1},
		rafttransport.TrustDomain{ClusterID: groups[0].ClusterID,
			ClusterIncarnation: groups[0].ClusterIncarnation}, nodes[:],
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

	// Learn the exact serving machine digest before creating the
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
	relationDigest, err := probe.Apply.RangeSplitRelationManifestDigest()
	if err != nil {
		t.Fatal(err)
	}
	if err = probe.Close(); err != nil {
		t.Fatal(err)
	}

	snapshot := replicaProcessCatalog(t, nodes, identities, secondaryIdentities, listeners, groups,
		authority, relationDigest)
	catalogPath := filepath.Join(root, "catalog.vibejson")
	if err = gateway.SaveSnapshot(catalogPath, snapshot); err != nil {
		t.Fatal(err)
	}
	seed := replicaProcessCatalogSeed(t, snapshot)
	for _, document := range seed {
		measurements.logicalBytes += uint64(len(document))
	}
	var servingNodes [3]rafttransport.NodeID
	var servingPeers [3]string
	copy(servingNodes[:], nodes[:3])
	for index := range servingPeers {
		servingPeers[index] = listeners[index].Peer
	}
	target := rf3testfixture.ProcessTarget{MemberID: 4, NodeID: nodes[3],
		StoreID: identities[3].StoreID, NodeIncarnation: 44, Listeners: listeners[3]}
	prepared := make([]rf3testfixture.PreparedProcessMember, 3)
	secondaryPrepared := make([]rf3testfixture.PreparedProcessMember, 3)
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
		secondaryPrepared[index], err = rf3testfixture.PrepareProcessMember(
			rf3testfixture.ProcessMemberOptions{
				Root:        filepath.Join(root, fmt.Sprintf("member-%d-secondary", index+1)),
				Table:       gateway.ReplicatedCatalogTable,
				CreateTable: "CREATE TABLE controlplane (PRIMARY KEY (id))",
				Identity:    secondaryIdentities[index], Key: key, WAL: wal,
				Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
				Authority: authority, Apply: apply, Listeners: listeners[index],
				Credential: credentials[index], Roots: roots,
				AuthorizationPolicy: policyPath, Nodes: servingNodes,
				PeerAddresses: servingPeers, Target: &target, SeedDocuments: seed,
			})
		if err != nil {
			t.Fatal(err)
		}
		combinedPath := filepath.Join(root, fmt.Sprintf("member-%d-groups.vibejson", index+1))
		if err = os.WriteFile(combinedPath, replicaProcessCombineServingGroups(t,
			prepared[index].ManifestPath, secondaryPrepared[index].ManifestPath), 0o600); err != nil {
			t.Fatal(err)
		}
		prepared[index].ManifestPath = combinedPath
	}
	firstAbandonment := replicaProcessSeedAbandonment(t, root, groups[0], identities[0], target,
		nodes[0], 0xa1)
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
		}, nodes[1], listeners[1].Snapshot, 1<<30,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondaryCold, err := rf3testfixture.PrepareColdProcessTarget(
		rf3testfixture.ProcessMemberOptions{
			Root: filepath.Join(root, "member-4-secondary"), Table: gateway.ReplicatedCatalogTable,
			CreateTable: "CREATE TABLE controlplane (PRIMARY KEY (id))",
			Identity:    secondaryIdentities[3], Key: key, WAL: wal,
			Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
			Authority: authority, Apply: apply, Listeners: listeners[3],
			Credential: credentials[3], Roots: roots, AuthorizationPolicy: policyPath,
			Nodes: servingNodes, PeerAddresses: servingPeers, Target: &target,
			SeedDocuments: seed,
		}, nodes[1], listeners[1].Snapshot, 1<<30,
	)
	if err != nil {
		t.Fatal(err)
	}
	combinedBootstrapPath := filepath.Join(root, "member-4-bootstrap-groups.vibejson")
	if err = os.WriteFile(combinedBootstrapPath, replicaProcessCombineBootstrapGroups(t,
		cold.BootstrapManifestPath, secondaryCold.BootstrapManifestPath), 0o600); err != nil {
		t.Fatal(err)
	}
	cold.BootstrapManifestPath = combinedBootstrapPath
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
		if index == 0 {
			voters[index].Env = append(os.Environ(), "VIBEDB_REPLICA_REPLACEMENT_E2E=1",
				"VIBEDB_QUALIFICATION_ABANDON_CRASH=after_rename")
		}
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
	profile, err := servicetls.LoadProfile(credentials[4].Certificate, credentials[4].Key,
		roots, rf3testfixture.ProcessIdentityOID, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	catalogAuthority, closeCatalogAuthority := replicaProcessCatalogAuthority(t, profile, snapshot,
		filepath.Join(root, "abandonment-catalog-session"))
	defer closeCatalogAuthority()
	replicaProcessPublishAbandonment(t, ctx, catalogAuthority, snapshot.Generation(), firstAbandonment)
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

	measurements.initialStorageBytes = replicaProcessAllocatedBytes(root, "")
	measurements.initialWALBytes = replicaProcessAllocatedBytes(root, ".wal")
	metricStop := make(chan struct{})
	metricDone := make(chan struct{})
	go replicaProcessSampleMetrics(metricStop, metricDone, &measurements, root,
		[]*rf3testfixture.ExternalProcess{voters[0], voters[1], voters[2], coldTarget, gatewayProcess})
	defer func() {
		select {
		case <-metricDone:
		default:
			close(metricStop)
			<-metricDone
		}
	}()
	measurements.abandonmentControlBytes += snapshottransfer.SourceControlRequestBytes +
		snapshottransfer.AbandonmentWitnessBytes + snapshottransfer.SourceControlResponseBytes
	measurements.abandonRenameMillis = replicaProcessSettleAbandonmentCrash(t, ctx, "after_rename",
		root, voters[0], gatewayProcess, catalogAuthority, firstAbandonment.Operation)

	// Re-arm the second durable cut only while both source and gateway are
	// stopped, so no process-local cursor or open repository can participate.
	if err = gatewayProcess.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if err = voters[0].Kill(ctx); err != nil {
		t.Fatal(err)
	}
	secondAbandonment := replicaProcessSeedAbandonment(t, root, groups[0], identities[0], target,
		nodes[0], 0xa2)
	voters[0].Env = append(os.Environ(), "VIBEDB_REPLICA_REPLACEMENT_E2E=1",
		"VIBEDB_QUALIFICATION_ABANDON_CRASH=after_unlink")
	if err = voters[0].Start(); err != nil {
		t.Fatal(err)
	}
	if err = voters[0].WaitReady(ctx, "vibedb-shard RF3 ready"); err != nil {
		t.Fatal(err)
	}
	replicaProcessPublishAbandonment(t, ctx, catalogAuthority, snapshot.Generation(), secondAbandonment)
	if err = gatewayProcess.Start(); err != nil {
		t.Fatal(err)
	}
	if err = gatewayProcess.WaitReady(ctx, "vibedb-gateway serving catalog generation"); err != nil {
		t.Fatal(err)
	}
	measurements.abandonmentControlBytes += snapshottransfer.SourceControlRequestBytes +
		snapshottransfer.AbandonmentWitnessBytes + snapshottransfer.SourceControlResponseBytes
	measurements.abandonUnlinkMillis = replicaProcessSettleAbandonmentCrash(t, ctx, "after_unlink",
		root, voters[0], gatewayProcess, catalogAuthority, secondAbandonment.Operation)
	measurements.abandonmentRetainedBytes = replicaProcessRetainedAbandonmentBytes(root)
	phase = "abandonment_crash_reopen_verified"
	baselineOperations, err := catalogAuthority.ReadOperationIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
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
	measurements.controllerRestartMillis = uint64(time.Since(started).Milliseconds())

	// The old member is reopened after the final roster can be observed. This
	// gives the durable retirement action a live endpoint for certified cleanup,
	// while its stale WAL still cannot recover serving authority.
	restartedSource := false
	sourceReady := false
	var sourceRestarted time.Time
	groupCompletion := make(map[raftmember.GroupKey]time.Duration, 2)
	for {
		if err = ctx.Err(); err != nil {
			t.Fatalf("replacement timeout: %v\ngateway:\n%s\ntarget:\n%s",
				err, gatewayProcess.Diagnostics(), coldTarget.Diagnostics())
		}
		current, loadErr := catalogAuthority.Read(ctx)
		if measurements.admissionMillis == 0 {
			operations, operationErr := catalogAuthority.ReadOperationIDs(ctx)
			if operationErr == nil && len(operations) >= len(baselineOperations)+2 {
				measurements.admissionMillis = uint64(max(time.Since(started).Milliseconds(), 1))
			}
		}
		if measurements.failoverMillis == 0 &&
			strings.Contains(gatewayProcess.Diagnostics(), "revision controller published") {
			measurements.failoverMillis = uint64(time.Since(started).Milliseconds())
		}
		if loadErr == nil {
			descriptors := current.ReplicatedShardDescriptors()
			for _, descriptor := range descriptors {
				if replicaProcessRosterContains(descriptor, 4) &&
					!replicaProcessRosterContains(descriptor, 1) {
					if _, found := groupCompletion[descriptor.Group]; !found {
						groupCompletion[descriptor.Group] = time.Since(started)
					}
				}
			}
			if replicaProcessAllRostersReplaced(descriptors, 2, 1, 4) {
				if !restartedSource {
					if err = voters[0].Start(); err != nil {
						t.Fatal(err)
					}
					restartedSource = true
					sourceRestarted = time.Now()
				}
				if !sourceReady && strings.Contains(voters[0].Diagnostics(), "vibedb-shard RF3 ready") {
					sourceReady = true
				}
				if strings.Contains(coldTarget.Diagnostics(), "vibedb-shard RF3 ready") &&
					sourceReady && strings.Contains(gatewayProcess.Diagnostics(), "completed 2") &&
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
	if measurements.admissionMillis == 0 {
		t.Fatal("two-group move set was never atomically discoverable")
	}
	phase = "replacement_complete"
	measurements.replacementMillis = uint64(time.Since(started).Milliseconds())
	measurements.groupP50Millis, measurements.groupP99Millis, measurements.groupMaxMillis =
		replicaProcessGroupLatencyQuantiles(groupCompletion)
	if !sourceRestarted.IsZero() {
		measurements.cleanupMillis = uint64(time.Since(sourceRestarted).Milliseconds())
	}
	if elapsed := time.Since(started); elapsed > 90*time.Second {
		t.Fatalf("replacement latency %s exceeded bound", elapsed)
	}
	if !replicaProcessTreeBounded(t, root, "gateway-session", 32, 64<<20) ||
		!replicaProcessTreeBounded(t, filepath.Join(root, "member-1"),
			"replica-actions", 4097, 64<<20) ||
		!replicaProcessTreeBounded(t, filepath.Join(root, "member-1-secondary"),
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
	final, err := catalogAuthority.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	descriptors := final.ReplicatedShardDescriptors()
	if !replicaProcessAllRostersReplaced(descriptors, 2, 1, 4) {
		t.Fatalf("retired source rejoined after controller restart: %+v", descriptors)
	}
	finalGeneration = final.Generation()
	phase = "terminal_reopen_verified"
	close(metricStop)
	<-metricDone
	measurements.finalStorageBytes = replicaProcessAllocatedBytes(root, "")
	measurements.finalWALBytes = replicaProcessAllocatedBytes(root, ".wal")
	if err := measurements.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicaProcessCatalogCarriesTwoPhysicalNodeGroups(t *testing.T) {
	nodes := replicaProcessNodes()
	primary := replicaProcessIdentities()
	secondary := replicaProcessSecondaryIdentities(primary)
	var listeners [4]rf3testfixture.ProcessListeners
	for index := range listeners {
		listeners[index] = rf3testfixture.ProcessListeners{
			Peer:     fmt.Sprintf("127.0.0.1:%d", 10000+index*4),
			Native:   fmt.Sprintf("127.0.0.1:%d", 10001+index*4),
			Control:  fmt.Sprintf("127.0.0.1:%d", 10002+index*4),
			Snapshot: fmt.Sprintf("127.0.0.1:%d", 10003+index*4),
		}
	}
	authority := sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: 5,
		ProtectionEpoch: 7, OwnershipEpoch: 11, SchemaGeneration: 13,
		RoutingVersion: 17, RouteGeneration: 19}
	snapshot := replicaProcessCatalog(t, nodes, primary, secondary, listeners,
		[2]raftmember.GroupKey{replicaProcessGroup(primary[0]), replicaProcessGroup(secondary[0])},
		authority, [32]byte{1})
	descriptors := snapshot.ReplicatedShardDescriptors()
	if len(descriptors) != 2 || descriptors[0].Group == descriptors[1].Group {
		t.Fatalf("descriptors=%+v", descriptors)
	}
	for _, descriptor := range descriptors {
		if descriptor.EnrolledTarget == nil || descriptor.EnrolledTarget.StoreID != primary[3].StoreID {
			t.Fatalf("physical target identity diverged across groups: %+v", descriptor)
		}
	}
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

func replicaProcessSecondaryIdentities(primary [4]raftstore.Identity) (secondary [4]raftstore.Identity) {
	for index := range primary {
		secondary[index] = primary[index]
		secondary[index].Distribution = "replica-secondary"
		secondary[index].Shard = "all"
		secondary[index].GroupID[0] ^= 0x40
		secondary[index].ShardIncarnation[0] ^= 0x20
		// StoreID names the physical store and therefore remains stable across
		// every group hosted by the same process.
	}
	return secondary
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
	identities, secondaryIdentities [4]raftstore.Identity,
	listeners [4]rf3testfixture.ProcessListeners, groups [2]raftmember.GroupKey,
	authority sqldriver.ReplicatedAuthorityProfile,
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
	secondaryManifest, err := distribution.NewManifest("replica-secondary",
		distribution.RoutingVersion(authority.RoutingVersion), []distribution.Shard{{
			ID: "all", AllocationGeneration: distribution.ShardAllocationGeneration(secondaryIdentities[0].AllocationGeneration),
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
			Leaders: leaders, Epoch: distribution.OwnershipEpoch(authority.OwnershipEpoch),
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
		Group: groups[0], AllocationGeneration: distribution.ShardAllocationGeneration(identities[0].AllocationGeneration),
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
	secondaryDescriptor := descriptor
	secondaryDescriptor.Distribution = "replica-secondary"
	secondaryDescriptor.Shard = "all"
	secondaryDescriptor.Group = groups[1]
	secondaryDescriptor.AllocationGeneration = distribution.ShardAllocationGeneration(secondaryIdentities[0].AllocationGeneration)
	secondaryDescriptor.RangeIdentity = replication.Digest{0x81}
	secondaryDescriptor.LineageDigest = replication.Digest{0x82}
	secondaryDescriptor.ForwardingRuleDigest = replication.Digest{0x83}
	for index := range secondaryDescriptor.Replicas {
		secondaryDescriptor.Replicas[index].StoreID = secondaryIdentities[index].StoreID
	}
	secondaryDescriptor.EnrolledTarget.StoreID = secondaryIdentities[3].StoreID
	config := distribution.ClusterConfig{Distributions: []distribution.DistributionSpec{{
		Name: gateway.ReplicatedCatalogDistribution, Arity: 1,
		MapperVersion: distribution.NativeMapperVersion}, {Name: "replica-secondary", Arity: 1,
		MapperVersion: distribution.NativeMapperVersion}}, Placements: []distribution.TablePlacement{{
		Table: gateway.ReplicatedCatalogTable, Distribution: gateway.ReplicatedCatalogDistribution,
		Columns: []string{gateway.ReplicatedCatalogPrimaryKey}}}, Manifests: []*distribution.Manifest{manifest, secondaryManifest}}
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(config, endpoints, 1, nil, nil,
		[]gateway.ReplicatedShardDescriptor{descriptor, secondaryDescriptor})
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
				ControlAddress:       listeners[index].Control,
				SplitSnapshotAddress: listeners[index].Snapshot})
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
	args := []string{"serve", "-catalog", catalog,
		"-catalog-route-seed", journal + ".catalog-route-seed", "-catalog-relation", "1",
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

func replicaProcessAllRostersReplaced(descriptors []gateway.ReplicatedShardDescriptor,
	want int, retired, target uint64,
) bool {
	if len(descriptors) != want {
		return false
	}
	for _, descriptor := range descriptors {
		if !replicaProcessRosterContains(descriptor, target) ||
			replicaProcessRosterContains(descriptor, retired) ||
			descriptor.Command.ReplicaSetVersion <= 1 {
			return false
		}
	}
	return true
}

func replicaProcessGroupLatencyQuantiles(completed map[raftmember.GroupKey]time.Duration) (
	p50, p99, maximum uint64,
) {
	if len(completed) != 2 {
		return 0, 0, 0
	}
	latencies := make([]time.Duration, 0, len(completed))
	for _, latency := range completed {
		latencies = append(latencies, latency)
	}
	slices.Sort(latencies)
	milliseconds := func(value time.Duration) uint64 {
		return uint64(max(value.Milliseconds(), 1))
	}
	return milliseconds(latencies[0]), milliseconds(latencies[len(latencies)-1]),
		milliseconds(latencies[len(latencies)-1])
}

func replicaProcessCombineServingGroups(t testing.TB, paths ...string) []byte {
	t.Helper()
	groups := make([][]byte, 0, len(paths))
	var common []byte
	for index, path := range paths {
		document, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		listener := bytes.Index(document, []byte(`"listeners":`))
		members := bytes.Index(document, []byte(`,"members":`))
		if listener < 2 || members <= listener || document[listener-1] != ',' ||
			len(document) == 0 || document[len(document)-1] != '}' {
			t.Fatalf("invalid singleton serving manifest %q", path)
		}
		if index == 0 {
			common = append([]byte(nil), document[listener:members]...)
		}
		group := make([]byte, 0, len(document))
		group = append(group, '{')
		group = append(group, document[1:listener-1]...)
		group = append(group, ',')
		group = append(group, document[members+1:len(document)-1]...)
		groups = append(groups, append(group, '}'))
	}
	result := append([]byte{'{'}, common...)
	result = append(result, `,"groups":[`...)
	for index, group := range groups {
		if index != 0 {
			result = append(result, ',')
		}
		result = append(result, group...)
	}
	return append(result, ']', '}')
}

func replicaProcessCombineBootstrapGroups(t testing.TB, paths ...string) []byte {
	t.Helper()
	groups := make([][]byte, 0, len(paths))
	var control []byte
	for index, path := range paths {
		document, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		controlStart := bytes.Index(document, []byte(`,"control_listener":`))
		sourceStart := bytes.Index(document, []byte(`,"source_node":`))
		if controlStart < 1 || sourceStart <= controlStart || len(document) == 0 ||
			document[len(document)-1] != '}' {
			t.Fatalf("invalid singleton bootstrap manifest %q", path)
		}
		candidate := document[controlStart+1 : sourceStart]
		if index == 0 {
			control = append([]byte(nil), candidate...)
		} else if !bytes.Equal(control, candidate) {
			t.Fatal("multi-group bootstrap manifests do not share one control listener")
		}
		group := append([]byte{'{'}, document[1:controlStart]...)
		group = append(group, document[sourceStart:]...)
		groups = append(groups, group)
	}
	result := append([]byte{'{'}, control...)
	result = append(result, `,"groups":[`...)
	for index, group := range groups {
		if index != 0 {
			result = append(result, ',')
		}
		result = append(result, group...)
	}
	return append(result, ']', '}')
}

func replicaProcessCatalogAuthority(t *testing.T, profile *rafttransport.PeerTLS,
	snapshot *gateway.Snapshot, journalPath string,
) (*gateway.ReplicatedCatalogAuthority, func()) {
	t.Helper()
	client, err := gateway.NewAuthenticatedReplicatedClient(gateway.AuthenticatedReplicatedClientOptions{
		TLS: profile, Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}, HandshakeDeadline: func() time.Time { return time.Now().Add(2 * time.Second) },
		MaxConnections: 16, MaxPerEndpoint: 8, MaxIdlePerEndpoint: 4, MaxHandshakes: 4,
		MaxWaiters: 32, MaxIdleAge: time.Minute, MaxLifetime: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := gateway.NewReplicatedExecutor(client, 8, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, ok := snapshot.ResolveReplicatedRoute(gateway.ReplicatedCatalogDistribution,
		gateway.ReplicatedCatalogShard, replicas[:0])
	if !ok {
		t.Fatal("resolve catalog route")
	}
	binding, err := gateway.NativeSessionJournalBinding(route, string(route.Distribution),
		string(route.Shard), []byte{1}, 1, serviceauthz.CapabilityTopology)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := gateway.OpenNativeSessionJournal(gateway.NativeSessionJournalOptions{
		Path: journalPath, ClientID: replication.ID128{0xc1}, RetryHome: replication.RetryHome{0xd1},
		MaxCommandBytes: replication.MaxCommandBytes, Binding: binding})
	if err != nil {
		t.Fatal(err)
	}
	session, err := gateway.NewNativeSession(gateway.NativeSessionOptions{Executor: executor,
		Route: route, Distribution: string(route.Distribution), Shard: string(route.Shard),
		Tenant: []byte{1}, ClientID: replication.ID128{0xc1}, RetryHome: replication.RetryHome{0xd1},
		Resolver: gateway.BaseRelationResolver{Relation: 1}, Journal: journal,
		ProposalCapability: serviceauthz.CapabilityTopology})
	if err != nil {
		t.Fatal(err)
	}
	identity := serviceauthz.Authority{Node: profile.LocalIdentity().Node, Generation: 5}
	authenticated, err := serviceauthz.WithAuthority(t.Context(), identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = session.Open(authenticated, time.Now().Add(time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	authority, err := gateway.NewReplicatedCatalogAuthority(gateway.ReplicatedCatalogAuthorityOptions{
		Executor: executor, Route: route, Relation: 1, Holder: gateway.NewCatalogHolder(snapshot),
		Session: session, Authority: identity})
	if err != nil {
		t.Fatal(err)
	}
	return authority, func() { _ = client.Close() }
}

func replicaProcessArtifactsEmpty(path string) bool {
	entries, err := os.ReadDir(path)
	return errors.Is(err, os.ErrNotExist) || err == nil && len(entries) == 0
}

func replicaProcessAllArtifactsEmpty(root string) bool {
	for member := 1; member <= 3; member++ {
		for _, suffix := range []string{"", "-secondary"} {
			if !replicaProcessArtifactsEmpty(filepath.Join(root,
				fmt.Sprintf("member-%d%s", member, suffix), "source-artifacts")) {
				return false
			}
		}
	}
	return true
}

func replicaProcessSeedAbandonment(t testing.TB, root string, group raftmember.GroupKey,
	identity raftstore.Identity, target rf3testfixture.ProcessTarget,
	source rafttransport.NodeID, operationByte byte,
) snapshottransfer.ArtifactAbandonmentWitness {
	t.Helper()
	payload := make([]byte, 2<<20)
	for index := range payload {
		payload[index] = byte(index) ^ operationByte
	}
	descriptor := snapshottransfer.Descriptor{Group: group, SourceMember: identity.MemberID,
		TargetMember: target.MemberID, TargetStore: target.StoreID,
		TargetIncarnation: target.NodeIncarnation, SchemaGeneration: 13,
		ReplicaSetVersion: 1, SnapshotIndex: 1, SnapshotTerm: 1,
		Lineage: sha256.Sum256([]byte{operationByte, 0x71}), ArtifactHash: sha256.Sum256(payload),
		ArtifactBytes: uint64(len(payload)), ChunkBytes: 1 << 20}
	request := snapshottransfer.SourceControlRequest{Operation: [32]byte{operationByte},
		Step: [32]byte{operationByte, 0x52}, Group: group, SourceMember: identity.MemberID,
		TargetMember: target.MemberID, TargetStore: target.StoreID,
		TargetIncarnation: target.NodeIncarnation, ReplicaSetVersion: 1, SourceNode: source}
	memberRoot := filepath.Join(root, "member-1")
	repository, err := snapshottransfer.OpenRepository(filepath.Join(memberRoot, "source-artifacts"),
		snapshottransfer.Limits{MaxArtifacts: 8, MaxArtifactBytes: 1 << 30, MaxDiskBytes: 4 << 30})
	if err != nil {
		t.Fatal(err)
	}
	chunk := payload[:1<<20]
	if _, _, err = repository.Append(descriptor, 0, chunk, sha256.Sum256(chunk)); err != nil {
		_ = repository.Close()
		t.Fatal(err)
	}
	if err = repository.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err := snapshottransfer.OpenSourceFileJournal(filepath.Join(memberRoot, "source-exports"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err = journal.PublishSourceExport(context.Background(), 0, snapshottransfer.SourceControlRecord{
		Request: request, Revision: 1, State: snapshottransfer.SourceControlRunning,
	}); err != nil {
		_ = journal.Close()
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	return snapshottransfer.ArtifactAbandonmentWitness{Operation: request.Operation, Step: request.Step,
		Artifact: descriptor.ArtifactHash, TargetStore: descriptor.TargetStore,
		TargetIncarnation: descriptor.TargetIncarnation, SchemaGeneration: descriptor.SchemaGeneration,
		ReplicaSetVersion: descriptor.ReplicaSetVersion, Owner: source, Descriptor: descriptor}
}

func replicaProcessPublishAbandonment(t testing.TB, ctx context.Context,
	authority *gateway.ReplicatedCatalogAuthority, generation uint64,
	witness snapshottransfer.ArtifactAbandonmentWitness,
) {
	t.Helper()
	intent := []byte("{}")
	record := gateway.ReplicatedOperationRecord{ID: witness.Operation,
		Kind: gateway.ReplicatedOperationMove, State: gateway.ReplicatedOperationPlanned,
		Revision: 1, CatalogGeneration: generation, Cursor: [8]uint64{1},
		Proof:        sha256.Sum256([]byte{witness.Operation[0], 0x63}),
		IntentDigest: sha256.Sum256(intent), Intent: intent}
	if err := authority.SubmitOperation(ctx, record); err != nil {
		t.Fatal(err)
	}
	if _, err := (rebalanceexec.CatalogAbandonmentAuthority{Journal: authority}).Publish(
		ctx, record.Revision, witness); err != nil {
		t.Fatal(err)
	}
}

func replicaProcessRetainedAbandonmentBytes(root string) uint64 {
	var total uint64
	entries, _ := os.ReadDir(filepath.Join(root, "member-1", "source-artifacts"))
	for _, entry := range entries {
		name := entry.Name()
		if len(name) < 2 || name[1] != '-' || !strings.Contains("scptda", name[:1]) {
			continue
		}
		if info, err := entry.Info(); err == nil && info.Size() > 0 {
			total += uint64(info.Size())
		}
	}
	return total
}

func replicaProcessSettleAbandonmentCrash(t testing.TB, ctx context.Context, phase string,
	root string, source, gatewayProcess *rf3testfixture.ExternalProcess,
	authority *gateway.ReplicatedCatalogAuthority, operation [32]byte,
) uint64 {
	t.Helper()
	started := time.Now()
	for source.PID() != 0 {
		if err := ctx.Err(); err != nil {
			t.Fatalf("%s source crash timeout: %v", phase, err)
		}
		if time.Since(started) > 15*time.Second {
			t.Fatalf("%s source did not crash", phase)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := gatewayProcess.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	source.Env = os.Environ()
	if err := source.Start(); err != nil {
		t.Fatal(err)
	}
	if err := source.WaitReady(ctx, "vibedb-shard RF3 ready"); err != nil {
		t.Fatalf("%s source reopen: %v\n%s", phase, err, source.Diagnostics())
	}
	if err := gatewayProcess.Start(); err != nil {
		t.Fatal(err)
	}
	if err := gatewayProcess.WaitReady(ctx, "vibedb-gateway serving catalog generation"); err != nil {
		t.Fatalf("%s gateway reopen: %v\n%s", phase, err, gatewayProcess.Diagnostics())
	}
	for {
		_, readErr := authority.ReadOperation(ctx, operation)
		if errors.Is(readErr, gateway.ErrReplicatedOperationMissing) &&
			replicaProcessRetainedAbandonmentBytes(root) == 0 {
			break
		}
		if err := ctx.Err(); err != nil || time.Since(started) > 15*time.Second {
			t.Fatalf("%s cleanup convergence err=%v retained=%d gateway=%s",
				phase, readErr, replicaProcessRetainedAbandonmentBytes(root), gatewayProcess.Diagnostics())
		}
		time.Sleep(10 * time.Millisecond)
	}
	return uint64(time.Since(started).Milliseconds())
}

type replicaProcessMeasurements struct {
	mu sync.Mutex

	failoverMillis, admissionMillis                   uint64
	replacementMillis, cleanupMillis                  uint64
	groupP50Millis, groupP99Millis, groupMaxMillis    uint64
	controllerRestartMillis                           uint64
	logicalBytes                                      uint64
	initialStorageBytes, finalStorageBytes            uint64
	initialWALBytes, finalWALBytes                    uint64
	maxSnapshotPayloadBytes, maxRSSBytes              uint64
	abandonRenameMillis, abandonUnlinkMillis          uint64
	abandonmentControlBytes, abandonmentRetainedBytes uint64
}

func TestReplicaProcessMeasurementsEvidenceBounds(t *testing.T) {
	measurements := replicaProcessMeasurements{
		failoverMillis: 10, admissionMillis: 9, controllerRestartMillis: 11, replacementMillis: 20,
		cleanupMillis: 5, groupP50Millis: 17, groupP99Millis: 19, groupMaxMillis: 19,
		logicalBytes: 100, initialStorageBytes: 1_000,
		finalStorageBytes: 2_000, initialWALBytes: 500, finalWALBytes: 700,
		maxSnapshotPayloadBytes: 300, maxRSSBytes: 400,
		abandonRenameMillis: 12, abandonUnlinkMillis: 13,
		abandonmentControlBytes: 1968,
	}
	if err := measurements.validate(); err != nil {
		t.Fatal(err)
	}
	raw := measurements.appendTSV(nil)
	for _, exact := range [][]byte{
		[]byte("metric\tfailover_millis\t10\n"),
		[]byte("metric\tmove_set_admission_millis\t9\n"),
		[]byte("metric\tgroup_replacement_p99_millis\t19\n"),
		[]byte("metric\tstorage_growth_bytes\t1000\n"),
		[]byte("metric\twal_growth_bytes\t200\n"),
		[]byte("metric\tstorage_amplification_milli\t10000\n"),
	} {
		if !strings.Contains(string(raw), string(exact)) {
			t.Fatalf("missing evidence row %q in %q", exact, raw)
		}
	}
	measurements.failoverMillis = 30_001
	if err := measurements.validate(); err == nil {
		t.Fatal("measurement accepted failover above the hard bound")
	}
}

func (measurements *replicaProcessMeasurements) appendTSV(raw []byte) []byte {
	measurements.mu.Lock()
	defer measurements.mu.Unlock()
	storageGrowth := positiveDifference(measurements.finalStorageBytes,
		measurements.initialStorageBytes)
	walGrowth := positiveDifference(measurements.finalWALBytes, measurements.initialWALBytes)
	amplification := uint64(0)
	if measurements.logicalBytes != 0 && storageGrowth <= ^uint64(0)/1000 {
		amplification = storageGrowth * 1000 / measurements.logicalBytes
	}
	metrics := [...]struct {
		name  string
		value uint64
	}{
		{"failover_millis", measurements.failoverMillis},
		{"move_set_admission_millis", measurements.admissionMillis},
		{"controller_restart_millis", measurements.controllerRestartMillis},
		{"replacement_millis", measurements.replacementMillis},
		{"group_replacement_p50_millis", measurements.groupP50Millis},
		{"group_replacement_p99_millis", measurements.groupP99Millis},
		{"group_replacement_max_millis", measurements.groupMaxMillis},
		{"cleanup_millis", measurements.cleanupMillis},
		{"snapshot_network_payload_bytes", measurements.maxSnapshotPayloadBytes},
		{"max_rss_bytes", measurements.maxRSSBytes},
		{"storage_growth_bytes", storageGrowth},
		{"wal_growth_bytes", walGrowth},
		{"storage_amplification_milli", amplification},
		{"logical_catalog_bytes", measurements.logicalBytes},
		{"abandonment_rename_restart_millis", measurements.abandonRenameMillis},
		{"abandonment_unlink_restart_millis", measurements.abandonUnlinkMillis},
		{"abandonment_control_payload_bytes", measurements.abandonmentControlBytes},
		{"abandonment_retained_bytes", measurements.abandonmentRetainedBytes},
	}
	for _, metric := range metrics {
		raw = fmt.Appendf(raw, "metric\t%s\t%d\n", metric.name, metric.value)
	}
	return raw
}

func (measurements *replicaProcessMeasurements) validate() error {
	measurements.mu.Lock()
	defer measurements.mu.Unlock()
	storageGrowth := positiveDifference(measurements.finalStorageBytes,
		measurements.initialStorageBytes)
	walGrowth := positiveDifference(measurements.finalWALBytes, measurements.initialWALBytes)
	if measurements.failoverMillis == 0 || measurements.failoverMillis > 30_000 ||
		measurements.admissionMillis == 0 || measurements.admissionMillis > 30_000 ||
		measurements.controllerRestartMillis == 0 || measurements.controllerRestartMillis > 15_000 ||
		measurements.replacementMillis == 0 || measurements.replacementMillis > 90_000 ||
		measurements.groupP50Millis == 0 || measurements.groupP50Millis > 90_000 ||
		measurements.groupP99Millis < measurements.groupP50Millis ||
		measurements.groupMaxMillis < measurements.groupP99Millis || measurements.groupMaxMillis > 90_000 ||
		measurements.cleanupMillis == 0 || measurements.cleanupMillis > 60_000 ||
		measurements.maxSnapshotPayloadBytes == 0 || measurements.maxSnapshotPayloadBytes > 1<<30 ||
		measurements.maxRSSBytes == 0 || measurements.maxRSSBytes > 8<<30 ||
		measurements.abandonRenameMillis == 0 || measurements.abandonRenameMillis > 15_000 ||
		measurements.abandonUnlinkMillis == 0 || measurements.abandonUnlinkMillis > 15_000 ||
		measurements.abandonmentControlBytes == 0 || measurements.abandonmentControlBytes > 4096 ||
		measurements.abandonmentRetainedBytes != 0 ||
		storageGrowth > 2<<30 || walGrowth > 512<<20 || measurements.logicalBytes == 0 {
		return fmt.Errorf("replica process performance bounds: failover=%dms admission=%dms restart=%dms replacement=%dms cleanup=%dms snapshot=%d rss=%d storage-growth=%d wal-growth=%d logical=%d",
			measurements.failoverMillis, measurements.admissionMillis, measurements.controllerRestartMillis,
			measurements.replacementMillis, measurements.cleanupMillis,
			measurements.maxSnapshotPayloadBytes, measurements.maxRSSBytes,
			storageGrowth, walGrowth, measurements.logicalBytes)
	}
	return nil
}

func positiveDifference(after, before uint64) uint64 {
	if after <= before {
		return 0
	}
	return after - before
}

func replicaProcessSampleMetrics(stop <-chan struct{}, done chan<- struct{},
	measurements *replicaProcessMeasurements, root string,
	processes []*rf3testfixture.ExternalProcess,
) {
	defer close(done)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		rss := uint64(0)
		for _, process := range processes {
			rss += replicaProcessRSS(process.PID())
		}
		snapshot := replicaProcessSnapshotPayloadBytes(root)
		measurements.mu.Lock()
		measurements.maxRSSBytes = max(measurements.maxRSSBytes, rss)
		measurements.maxSnapshotPayloadBytes = max(measurements.maxSnapshotPayloadBytes, snapshot)
		measurements.mu.Unlock()
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
	}
}

func replicaProcessRSS(pid int) uint64 {
	if pid <= 0 {
		return 0
	}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "VmRSS:" && fields[2] == "kB" {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil && value <= ^uint64(0)/1024 {
				return value * 1024
			}
		}
	}
	return 0
}

func replicaProcessSnapshotPayloadBytes(root string) uint64 {
	total := uint64(0)
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.Contains(path, "source-artifacts") &&
			!strings.Contains(path, "target-artifacts") {
			return nil
		}
		info, err := entry.Info()
		if err == nil && info.Size() > 0 {
			total += uint64(info.Size())
		}
		return nil
	})
	return total
}

func replicaProcessAllocatedBytes(root, suffix string) uint64 {
	total := uint64(0)
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || suffix != "" && !strings.HasSuffix(path, suffix) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Blocks > 0 {
			total += uint64(stat.Blocks) * 512
		}
		return nil
	})
	return total
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
