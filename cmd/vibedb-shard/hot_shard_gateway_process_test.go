//go:build darwin || linux

package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	vibejson "github.com/thesyncim/vibejson"
)

const gatewayHotShardLiveChildEnvironment = "VIBEDB_GATEWAY_HOT_SHARD_LIVE_CHILD"

// TestGatewayHotShardLiveRF3NetworkPartition is the external parent. Its child
// owns the existing prepared-RF3 fixture and in turn launches three serving
// members, one cold target, and the actual vibedb-gateway executable.
func TestGatewayHotShardLiveRF3NetworkPartition(t *testing.T) {
	if os.Getenv(gatewayHotShardLiveChildEnvironment) != "" ||
		os.Getenv(rf3CommandHelperEnvironment) != "" {
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable,
		"-test.run=^TestServeRF3ShippedCompositionThreeProcesses$", "-test.count=1", "-test.v")
	command.Env = append(os.Environ(), gatewayHotShardLiveChildEnvironment+"=1")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live hot-shard RF3 child: %v\n%s", err, output)
	}
	if strings.Contains(string(output), "--- SKIP: TestServeRF3ShippedCompositionThreeProcesses") {
		t.Skip("strict RF3 physical allocation is unavailable on this host")
	}
	t.Log(strings.TrimSpace(string(output)))
}

type gatewayHotShardLiveFixture struct {
	root              string
	executable        string
	children          []*rf3CommandChild
	target            *rf3ColdTargetProcess
	nodes             [rf3CommandMembers]rafttransport.NodeID
	group             raftmember.GroupKey
	credentials       []rf3testfixture.Credential
	roots             string
	policyPath        string
	peerAddresses     [rf3CommandMembers]string
	nativeAddresses   [rf3CommandMembers]string
	snapshotAddresses [rf3CommandMembers]string
	controlAddresses  [rf3CommandMembers]string
	targetNode        rafttransport.NodeID
	targetStore       [16]byte
	targetIncarnation uint64
	targetListeners   rf3ManifestListeners
	targetReservation *rf3testfixture.ReservedAddresses
	clientProfile     *rafttransport.PeerTLS
	clientNode        rafttransport.NodeID
	gatewayNode       rafttransport.NodeID
	authority         sqldriver.ReplicatedAuthorityProfile
	grantClient       *shardservice.MembershipGrantControlClient
}

func runGatewayHotShardLiveChild(t testing.TB, fixture gatewayHotShardLiveFixture) {
	t.Helper()
	states := make([]shardservice.ReplicatedMemberState, rf3CommandMembers)
	for index := range states {
		state, err := probeRF3CommandMember(t.Context(), fixture.nativeAddresses[index],
			fixture.nodes[index], fixture.clientProfile, fixture.clientNode, fixture.group,
			rf3CommandStoreIdentity(1).AllocationGeneration,
			fixture.authority.ActivePolicyGeneration)
		if err != nil {
			t.Fatalf("probe member %d: %v", index+1, err)
		}
		states[index] = state
	}

	links := make([]*gatewayHotShardNetworkLink, rf3CommandMembers+1)
	for index := range fixture.controlAddresses {
		links[index] = newGatewayHotShardNetworkLink(t, fixture.controlAddresses[index])
		defer links[index].close()
	}
	links[rf3CommandMembers] = newGatewayHotShardNetworkLink(t, fixture.targetListeners.Control)
	defer links[rf3CommandMembers].close()

	snapshot := gatewayHotShardLiveSnapshot(t, fixture, states, links)
	bootstrapPath := filepath.Join(fixture.root, "gateway-bootstrap.vibejson")
	if err := gateway.SaveSnapshot(bootstrapPath, snapshot); err != nil {
		t.Fatal(err)
	}
	authority, closeAuthority := gatewayHotShardLiveAuthority(t, fixture, snapshot)
	defer closeAuthority()
	if err := authority.Publish(t.Context(), 0, snapshot); err != nil {
		t.Fatalf("seed replicated catalog: %v", err)
	}
	grant, err := gateway.BuildReplicaReplacementMembershipGrant(
		snapshot, fixture.group, [16]byte{0x41}, 2, 1, 4,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = authority.PublishMembershipGrant(t.Context(), grant); err != nil {
		gatewayHotShardSeedFailureDiagnostics(t, fixture)
		t.Fatalf("seed certified membership grant: %v", err)
	}
	for _, node := range append(append([]rafttransport.NodeID(nil), fixture.nodes[:]...), fixture.targetNode) {
		if err = fixture.grantClient.InstallMembershipGrant(t.Context(), node, grant); err != nil {
			t.Fatalf("install certified grant on %x: %v", node, err)
		}
	}

	gatewayAddresses := rf3CommandUnusedAddresses(t, 2)
	gatewayAddress, gatewayControlAddress := gatewayAddresses[0], gatewayAddresses[1]
	capacityPath := gatewayHotShardLiveCapacity(t, fixture.root)
	manifestPath := gatewayHotShardLiveManifest(
		t, fixture, links, gatewayControlAddress,
	)
	ackKeyPath := filepath.Join(fixture.root, "durable-ack-key")
	if err = os.WriteFile(ackKeyPath,
		[]byte("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"), 0o600); err != nil {
		t.Fatal(err)
	}
	gatewayBinary := filepath.Join(fixture.root, "vibedb-gateway")
	repository := gatewayHotShardRepositoryRoot(t)
	build := exec.Command("go", "build", "-o", gatewayBinary, "./cmd/vibedb-gateway")
	build.Dir = repository
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build vibedb-gateway: %v\n%s", buildErr, output)
	}
	diagnostic := &rf3CommandDiagnostic{maximum: rf3CommandDiagnosticBytes}
	arguments := []string{"serve", "-catalog", bootstrapPath, "-catalog-relation", "1",
		"-catalog-route-seed", filepath.Join(fixture.root, "gateway-route-seed.vibejson"),
		"-catalog-attempts", "8", "-catalog-attempt-timeout", "500ms",
		"-catalog-session-journal", filepath.Join(fixture.root, "gateway-session"),
		"-durable-ack-key", ackKeyPath,
		"-catalog-client-id", "102132435465768798a9bacbdcedfe0f",
		"-catalog-retry-home", "1122334455667788", "-catalog-session-lease", "1h",
		"-controller-interval", "50ms", "-hot-shard-capacity", capacityPath,
		"-hot-shard-interval", "10s", "-replica-control-manifest", manifestPath,
		"-listen", gatewayAddress, "-tls-certificate", fixture.credentials[5].Certificate,
		"-tls-key", fixture.credentials[5].Key, "-tls-roots", fixture.roots,
		"-tls-identity-oid", "1.3.6.1.4.1.32473.1.1",
		"-authorization-policy", fixture.policyPath, "-tls-handshake-timeout", "2s",
		"-max-shard-connections-per-pool", "32", "-max-shard-handshakes-per-pool", "8"}
	for index := range fixture.nodes {
		arguments = append(arguments, "-shard-peer",
			fixture.nativeAddresses[index]+"="+fmt.Sprintf("%x", fixture.nodes[index]))
	}
	arguments = append(arguments, "-shard-peer",
		fixture.targetListeners.Native+"="+fmt.Sprintf("%x", fixture.targetNode))
	command := exec.Command(gatewayBinary, arguments...)
	command.Stdout, command.Stderr = diagnostic, diagnostic
	// Every fixture-owned proxy and gateway address is now fixed. Release the
	// cold target's future serving listeners before the controller can begin
	// bootstrap, without leaving them available for an earlier proxy bind.
	if err = fixture.targetReservation.Close(); err != nil {
		t.Fatal(err)
	}
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	gatewayExited := make(chan error, 1)
	go func(command *exec.Cmd, exited chan error) { exited <- command.Wait() }(command, gatewayExited)
	restarts := 0
	var exitedPeakRSS uint64
	restartQuiescedGateway := func() {
		t.Helper()
		select {
		case exitErr := <-gatewayExited:
			if restarts >= 2 || !strings.Contains(diagnostic.String(), gateway.ErrReplicatedCatalogRouteRestartRequired.Error()) {
				t.Fatalf("unexpected gateway exit: %v\n%s", exitErr, diagnostic.String())
			}
			usage, ok := command.ProcessState.SysUsage().(*syscall.Rusage)
			if !ok || usage.Maxrss <= 0 {
				t.Fatal("missing exited gateway peak RSS")
			}
			peak := uint64(usage.Maxrss)
			if runtime.GOOS == "linux" {
				peak <<= 10 // Linux reports KiB; Darwin reports bytes.
			}
			exitedPeakRSS = max(exitedPeakRSS, peak)
			// Catalog self-placement changes deliberately quiesce and destroy
			// the old bound session before promoting the certified route seed.
			// Model the deployment supervisor by restarting the same executable
			// with the same durable files, never by reseeding the catalog.
			restarts++
			diagnostic = &rf3CommandDiagnostic{maximum: rf3CommandDiagnosticBytes}
			command = exec.Command(gatewayBinary, arguments...)
			command.Stdout, command.Stderr = diagnostic, diagnostic
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			gatewayExited = make(chan error, 1)
			go func(command *exec.Cmd, exited chan error) { exited <- command.Wait() }(command, gatewayExited)
		default:
		}
	}
	defer func() {
		if command.Process != nil {
			_ = command.Process.Signal(syscall.SIGTERM)
		}
		select {
		case <-gatewayExited:
		case <-time.After(10 * time.Second):
			if command.Process != nil {
				_ = command.Process.Kill()
			}
		}
	}()

	connection := gatewayHotShardDialGateway(t, fixture.clientProfile, fixture.gatewayNode,
		gatewayAddress, diagnostic, gatewayExited)
	defer connection.Close()
	baselineRSS := gatewayHotShardProcessRSS(t, command.Process.Pid)
	latencies := gatewayHotShardDriveReads(t, connection, 950)
	sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
	p99 := latencies[(len(latencies)*99+99)/100-1]
	if p99 > 250*time.Millisecond {
		t.Fatalf("live foreground p99=%s exceeds 250ms", p99)
	}

	maximumOperations := 0
	maximumDemand := uint64(0)
	var peakPressure []byte
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if record, pressureErr := authority.ReadPressureRecord(t.Context()); pressureErr == nil {
			if view, viewErr := hotshard.OpenView(record.Payload); viewErr == nil {
				for _, report := range view.Reports {
					if demand := report.Demand[autosplit.ResourceRequests]; demand > maximumDemand {
						maximumDemand = demand
						peakPressure = append(peakPressure[:0], record.Payload...)
					}
				}
			}
		}
		ids, readErr := authority.ReadOperationIDs(t.Context())
		if readErr == nil {
			maximumOperations = max(maximumOperations, len(ids))
			if len(ids) == 1 {
				break
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	if maximumOperations != 1 {
		record, pressureErr := authority.ReadPressureRecord(t.Context())
		t.Fatalf("automatic hot move was not admitted; max operations=%d max requests/window=%d peak=%s pressure=%s err=%v\n%s",
			maximumOperations, maximumDemand, peakPressure, record.Payload, pressureErr, diagnostic.String())
	}

	beforeRejected := links[0].rejected.Load()
	links[0].partition()
	time.Sleep(time.Second)
	ids, err := authority.ReadOperationIDs(t.Context())
	if err != nil || len(ids) != 1 {
		t.Fatalf("partition lost/duplicated operation ids=%x err=%v", ids, err)
	}
	if links[0].rejected.Load() == beforeRejected {
		t.Fatal("gateway shard-control partition rejected no real TCP attempt")
	}
	links[0].heal()

	// Admission and final publication are distinct bounded phases. The forced
	// partition above deliberately consumes time after admission, so reusing
	// its deadline would shorten the controller's final convergence budget.
	deadline = time.Now().Add(30 * time.Second)
	var final *gateway.Snapshot
	for time.Now().Before(deadline) {
		restartQuiescedGateway()
		candidate, readErr := authority.Read(t.Context())
		if readErr == nil && candidate.Generation() >= 3 {
			if currentIDs, idsErr := authority.ReadOperationIDs(t.Context()); idsErr == nil && len(currentIDs) == 0 {
				final = candidate
				break
			}
		}
		if currentIDs, idsErr := authority.ReadOperationIDs(t.Context()); idsErr == nil {
			maximumOperations = max(maximumOperations, len(currentIDs))
		}
		time.Sleep(50 * time.Millisecond)
	}
	if final == nil {
		for _, id := range ids {
			record, readErr := authority.ReadOperation(t.Context(), id)
			t.Logf("hot move diagnostic: state=%d revision=%d catalog=%d cursor=%v err=%v", record.State, record.Revision, record.CatalogGeneration, record.Cursor, readErr)
		}
		t.Fatalf("automatic replica move did not publish final catalog\n%s", diagnostic.String())
	}
	var workspace [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, found := final.ResolveReplicatedRoute(gateway.ReplicatedCatalogDistribution,
		gateway.ReplicatedCatalogShard, workspace[:0])
	if !found || len(route.Replicas) != gateway.ServingReplicaCount {
		t.Fatalf("final route retained source: %+v", route)
	}
	targetFound := false
	for _, replica := range route.Replicas {
		if replica.Member == 1 {
			t.Fatalf("final route retained source: %+v", route)
		}
		targetFound = targetFound || replica.Member == 4
	}
	if !targetFound || maximumOperations > 1 {
		t.Fatalf("final route=%+v max_operations=%d", route, maximumOperations)
	}
	if restarts != 2 {
		t.Fatalf("catalog self-move handoffs=%d, want exactly two durable route-seed restarts", restarts)
	}
	finalConnection := gatewayHotShardDialGateway(t, fixture.clientProfile, fixture.gatewayNode,
		gatewayAddress, diagnostic, gatewayExited)
	defer finalConnection.Close()
	gatewayHotShardDriveReads(t, finalConnection, 1)
	record, err := authority.ReadPressureRecord(t.Context())
	if err != nil || len(record.Payload) == 0 || len(record.Payload) > hotshard.MaxStaticCapacityBytes {
		t.Fatalf("bounded pressure record bytes=%d err=%v", len(record.Payload), err)
	}

	// Supervisor restarts must not erase the old processes' memory high-water
	// marks from the existing resource gate.
	finalRSS := max(exitedPeakRSS, gatewayHotShardProcessRSS(t, command.Process.Pid))
	const maximumRSSGrowth = uint64(128 << 20)
	if finalRSS > baselineRSS && finalRSS-baselineRSS > maximumRSSGrowth {
		t.Fatalf("gateway RSS growth=%d exceeds %d", finalRSS-baselineRSS, maximumRSSGrowth)
	}
	var accepted, rejected, bytes, peak uint64
	for _, link := range links {
		accepted += link.accepted.Load()
		rejected += link.rejected.Load()
		bytes += link.bytes.Load()
		peak = max(peak, link.peak.Load())
	}
	if accepted+rejected > 4096 || bytes > 64<<20 || peak > 8 {
		t.Fatalf("control amplification connections=%d rejected=%d bytes=%d peak=%d",
			accepted, rejected, bytes, peak)
	}
	t.Logf("hot-move live qualification: initial_leader=%d foreground_reads=%d p99=%s max_operations=%d pressure_bytes=%d control_connections=%d rejected=%d control_bytes=%d peak_connections=%d rss_growth=%d",
		states[0].LeaderID, len(latencies), p99, maximumOperations, len(record.Payload), accepted, rejected, bytes, peak,
		max(finalRSS, baselineRSS)-baselineRSS)
}

// Failure-only diagnostics retain the original bounded child output and one
// authenticated probe per voter. They never retry or alter the seed operation.
func gatewayHotShardSeedFailureDiagnostics(t testing.TB, fixture gatewayHotShardLiveFixture) {
	t.Helper()
	for index, child := range fixture.children {
		if child == nil || index >= len(fixture.nodes) {
			continue
		}
		exited := false
		select {
		case <-child.exited:
			exited = true
		default:
		}
		child.mu.Lock()
		waitErr := child.waitErr
		child.mu.Unlock()
		t.Logf("seed grant member=%d exited=%t process_error=%v\n%s", child.member, exited, waitErr, child.diagnostic.String())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		state, err := probeRF3CommandMember(ctx, fixture.nativeAddresses[index], fixture.nodes[index],
			fixture.clientProfile, fixture.clientNode, fixture.group,
			rf3CommandStoreIdentity(1).AllocationGeneration, fixture.authority.ActivePolicyGeneration)
		cancel()
		t.Logf("seed grant member=%d native_probe_error=%v state=%+v", child.member, err, state)
	}
}

func gatewayHotShardLiveSnapshot(
	t testing.TB, fixture gatewayHotShardLiveFixture,
	states []shardservice.ReplicatedMemberState, links []*gatewayHotShardNetworkLink,
) *gateway.Snapshot {
	t.Helper()
	var base sqldriver.ReplicatedShardStoreIdentity
	if err := loadRF3IdentityFile(filepath.Join(fixture.root, "member-1", "sql-identity.json"), &base); err != nil {
		t.Fatal(err)
	}
	logical, err := sqldriver.ReplicatedRelationManifestDigest(base)
	if err != nil {
		t.Fatal(err)
	}
	return gatewayHotShardSnapshotForLogical(t, fixture, states, links, logical)
}

// Keep catalog endpoint qualification independent of sealed SQL allocation so
// every host can exercise the exact serving fixture's transport-role mapping.
func gatewayHotShardSnapshotForLogical(t testing.TB, fixture gatewayHotShardLiveFixture,
	states []shardservice.ReplicatedMemberState, links []*gatewayHotShardNetworkLink, logical [32]byte,
) *gateway.Snapshot {
	t.Helper()
	command := states[0].Fence.Command
	for index := 1; index < len(states); index++ {
		if states[index].Fence.Command != command {
			t.Fatal("RF3 members disagree on command fence")
		}
	}
	endpointNames := []distribution.EndpointID{"member-1", "member-2", "member-3"}
	manifest, err := distribution.NewManifest(gateway.ReplicatedCatalogDistribution,
		distribution.RoutingVersion(command.RoutingVersion), []distribution.Shard{{
			ID: gateway.ReplicatedCatalogShard,
			AllocationGeneration: distribution.ShardAllocationGeneration(
				rf3CommandStoreIdentity(1).AllocationGeneration),
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
			Leaders: endpointNames, Epoch: distribution.OwnershipEpoch(command.OwnershipEpoch),
		}})
	if err != nil {
		t.Fatal(err)
	}
	endpoints := make(map[distribution.EndpointID]string, 12)
	replicas := make([]gateway.ReplicatedReplicaDescriptor, rf3CommandMembers)
	for index := range replicas {
		endpoint, native, control := endpointNames[index],
			distribution.EndpointID(fmt.Sprintf("member-%d-native", index+1)),
			distribution.EndpointID(fmt.Sprintf("member-%d-control", index+1))
		endpoints[endpoint] = fixture.peerAddresses[index]
		endpoints[native] = fixture.nativeAddresses[index]
		endpoints[control] = links[index].address()
		replicas[index] = gateway.ReplicatedReplicaDescriptor{Member: uint64(index + 1),
			Node: fixture.nodes[index], StoreID: states[index].Fence.StoreID,
			NodeIncarnation: states[index].Fence.NodeIncarnation,
			Endpoint:        endpoint, NativeEndpoint: native, ControlEndpoint: control}
	}
	endpoints["target"] = fixture.targetListeners.Peer
	endpoints["target-native"] = fixture.targetListeners.Native
	endpoints["target-control"] = links[rf3CommandMembers].address()
	target := gateway.ReplicatedReplicaDescriptor{Member: 4, Node: fixture.targetNode,
		StoreID: fixture.targetStore, NodeIncarnation: fixture.targetIncarnation,
		Endpoint: "target", NativeEndpoint: "target-native", ControlEndpoint: "target-control"}
	descriptor := gateway.ReplicatedShardDescriptor{
		LogicalSchemaDigest: replication.Digest(logical),
		Distribution:        gateway.ReplicatedCatalogDistribution, Shard: gateway.ReplicatedCatalogShard,
		Group: fixture.group,
		AllocationGeneration: distribution.ShardAllocationGeneration(
			rf3CommandStoreIdentity(1).AllocationGeneration),
		Command: command, RangeIdentity: replication.Digest{0x71},
		LineageDigest: replication.Digest{0x72}, ForwardingRuleDigest: replication.Digest{0x73},
		RequestLedgerRanges: []gateway.DurableRequestLedgerRangeDescriptor{{Identity: replication.Digest{0x91}}},
		Replicas:            replicas, EnrolledTarget: &target,
	}
	profile := gateway.ReplicatedTableProfile{Table: gateway.ReplicatedCatalogTable,
		Relation: 1, PrimaryKey: gateway.ReplicatedCatalogPrimaryKey,
		SchemaGeneration:    command.SchemaGeneration,
		LogicalSchemaDigest: replication.Digest(logical),
		MaxKeyBytes:         256, MaxDocumentBytes: 4 << 20}
	snapshot, err := gateway.NewSnapshotWithReplicatedTableMetadata(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: gateway.ReplicatedCatalogDistribution,
			Arity: 1, MapperVersion: distribution.NativeMapperVersion}},
		Placements: []distribution.TablePlacement{{Table: gateway.ReplicatedCatalogTable,
			Distribution: gateway.ReplicatedCatalogDistribution,
			Columns:      []string{gateway.ReplicatedCatalogPrimaryKey}}},
		Manifests: []*distribution.Manifest{manifest},
	}, endpoints, 1, nil, nil, []gateway.ReplicatedShardDescriptor{descriptor},
		[]gateway.ReplicatedTableProfile{profile})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func gatewayHotShardLiveAuthority(
	t testing.TB, fixture gatewayHotShardLiveFixture, snapshot *gateway.Snapshot,
) (*gateway.ReplicatedCatalogAuthority, func()) {
	t.Helper()
	pool, err := gateway.NewAuthenticatedReplicatedClient(gatewayHotShardSeedClientOptions(fixture.clientProfile))
	if err != nil {
		t.Fatal(err)
	}
	executor, err := gateway.NewReplicatedExecutor(pool, 8, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, ok := snapshot.ResolveReplicatedRoute(gateway.ReplicatedCatalogDistribution,
		gateway.ReplicatedCatalogShard, replicas[:0])
	if !ok {
		t.Fatal("resolve catalog route")
	}
	clientID := replication.ID128{0xa1}
	retryHome := replication.RetryHome{0xb1}
	binding, err := gateway.NativeSessionJournalBinding(route,
		string(gateway.ReplicatedCatalogDistribution), string(gateway.ReplicatedCatalogShard),
		[]byte{1}, 1, serviceauthz.CapabilityTopology)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := gateway.OpenNativeSessionJournal(gateway.NativeSessionJournalOptions{
		Path: filepath.Join(fixture.root, "seed-session"), ClientID: clientID,
		RetryHome: retryHome, MaxCommandBytes: replication.MaxCommandBytes, Binding: binding,
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := gateway.NewNativeSession(gateway.NativeSessionOptions{
		Executor: executor, Route: route, Distribution: string(gateway.ReplicatedCatalogDistribution),
		Shard: string(gateway.ReplicatedCatalogShard), Tenant: []byte{1}, ClientID: clientID,
		RetryHome: retryHome, Resolver: gateway.BaseRelationResolver{Relation: 1}, Journal: journal,
		ProposalCapability: serviceauthz.CapabilityTopology, MaxRelationBatches: 1,
		MaxMutations: 4, InitialCommandBytes: 4 << 10, MaxCommandBytes: replication.MaxCommandBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	authorityIdentity := serviceauthz.Authority{Node: fixture.clientNode,
		Generation: fixture.authority.ActivePolicyGeneration}
	ctx, err := serviceauthz.WithAuthority(t.Context(), authorityIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = session.Open(ctx, time.Now().Add(time.Hour).UnixNano()); err != nil {
		t.Fatalf("open seed native session: %v", err)
	}
	authority, err := gateway.NewReplicatedCatalogAuthority(gateway.ReplicatedCatalogAuthorityOptions{
		Executor: executor, Route: route, Relation: 1, Holder: gateway.NewCatalogHolder(nil),
		Session: session, Authority: authorityIdentity,
	})
	if err != nil {
		t.Fatal(err)
	}
	return authority, func() { _ = pool.Close() }
}

func gatewayHotShardSeedClientOptions(profile *rafttransport.PeerTLS) gateway.AuthenticatedReplicatedClientOptions {
	return gateway.AuthenticatedReplicatedClientOptions{
		TLS: profile,
		Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}, HandshakeDeadline: func() time.Time { return time.Now().Add(2 * time.Second) },
		MaxConnections: 16, MaxPerEndpoint: 8, MaxIdlePerEndpoint: 4,
		MaxHandshakes: 4, MaxWaiters: 16,
		MaxIdleAge: time.Minute, MaxLifetime: 5 * time.Minute,
	}
}

func gatewayHotShardLiveCapacity(t testing.TB, root string) string {
	t.Helper()
	capacity := autosplit.CapacityVector{}
	targetCapacity := autosplit.CapacityVector{}
	for resource := range autosplit.ResourceCount {
		capacity[resource] = 1_000
		// Moving the only allocation between equal empty nodes provides no
		// relief. Provision the cold destination with twice the source capacity:
		// 950 requests move from 95% source pressure to 47.5% target pressure,
		// below the unchanged 85% destination ceiling.
		targetCapacity[resource] = 2_000
	}
	config := hotshard.StaticCapacityConfig{Format: hotshard.StaticCapacityFormat,
		RecorderLanes: 8, WindowCapacity: capacity, NodeCapacity: capacity,
		MigrationCapacity: 1_000, ShardMigrationBytes: 1, MaxReceives: 1,
		Nodes: []hotshard.StaticCapacityNode{
			{Endpoint: "member-1", FailureDomain: 1}, {Endpoint: "member-2", FailureDomain: 2},
			{Endpoint: "member-3", FailureDomain: 3}, {Endpoint: "target", FailureDomain: 4, Capacity: &targetCapacity},
		}}
	raw, err := hotshard.AppendStaticCapacityConfig(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "hot-shard-capacity.vibejson")
	if err = os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

type gatewayHotShardPersistedManifest struct {
	Generation       uint64                                    `json:"generation"`
	LocalGateway     gatewayHotShardPersistedGatewayEndpoint   `json:"local_gateway"`
	TLS              gatewayHotShardPersistedTLS               `json:"tls"`
	Bounds           gatewayHotShardPersistedBounds            `json:"bounds"`
	ShardEndpoints   []gatewayHotShardPersistedShardEndpoint   `json:"shard_endpoints"`
	GatewayEndpoints []gatewayHotShardPersistedGatewayEndpoint `json:"gateway_endpoints"`
	Candidates       []gatewayHotShardPersistedCandidate       `json:"candidates"`
	// This replacement-only fixture has no split sources, but the strict
	// canonical manifest still requires the explicit null field.
	SplitSources []struct{} `json:"split_sources"`
}

type gatewayHotShardPersistedTLS struct {
	Certificate         string `json:"certificate"`
	Key                 string `json:"key"`
	Roots               string `json:"roots"`
	IdentityOID         string `json:"identity_oid"`
	AuthorizationPolicy string `json:"authorization_policy"`
}

type gatewayHotShardPersistedBounds struct {
	MaxConnections      uint32 `json:"max_connections"`
	MaxHandshakes       uint32 `json:"max_handshakes"`
	MaxConcurrentDrains uint32 `json:"max_concurrent_drains"`
	ControllerInterval  uint64 `json:"controller_interval_millis"`
	ReadTimeout         uint64 `json:"read_timeout_millis"`
	WriteTimeout        uint64 `json:"write_timeout_millis"`
}

type gatewayHotShardPersistedShardEndpoint struct {
	Node                 string `json:"node"`
	ControlAddress       string `json:"control_address"`
	SplitSnapshotAddress string `json:"split_snapshot_address"`
}

type gatewayHotShardPersistedGatewayEndpoint struct {
	Node           string `json:"node"`
	Incarnation    uint64 `json:"incarnation"`
	ControlAddress string `json:"control_address"`
}

type gatewayHotShardPersistedCandidate struct {
	Member          uint64 `json:"member"`
	Node            string `json:"node"`
	Store           string `json:"store"`
	NodeIncarnation uint64 `json:"node_incarnation"`
	Endpoint        string `json:"endpoint"`
	Load            uint64 `json:"load"`
}

func gatewayHotShardLiveManifest(t testing.TB, fixture gatewayHotShardLiveFixture,
	links []*gatewayHotShardNetworkLink, gatewayControlAddress string) string {
	t.Helper()
	local := gatewayHotShardPersistedGatewayEndpoint{Node: fmt.Sprintf("%x", fixture.gatewayNode),
		Incarnation: fixture.targetIncarnation, ControlAddress: gatewayControlAddress}
	document := gatewayHotShardPersistedManifest{Generation: 1, LocalGateway: local,
		TLS: gatewayHotShardPersistedTLS{Certificate: fixture.credentials[5].Certificate,
			Key: fixture.credentials[5].Key, Roots: fixture.roots,
			IdentityOID: "1.3.6.1.4.1.32473.1.1", AuthorizationPolicy: fixture.policyPath},
		Bounds: gatewayHotShardPersistedBounds{MaxConnections: 8, MaxHandshakes: 4,
			MaxConcurrentDrains: 2, ControllerInterval: 50, ReadTimeout: 250, WriteTimeout: 250},
		GatewayEndpoints: []gatewayHotShardPersistedGatewayEndpoint{local},
		Candidates: []gatewayHotShardPersistedCandidate{{Member: 4,
			Node: fmt.Sprintf("%x", fixture.targetNode), Store: fmt.Sprintf("%x", fixture.targetStore),
			NodeIncarnation: fixture.targetIncarnation, Endpoint: "target"}}}
	for index, node := range append(append([]rafttransport.NodeID(nil), fixture.nodes[:]...), fixture.targetNode) {
		snapshotAddress := fixture.targetListeners.Snapshot
		if index < len(fixture.snapshotAddresses) {
			snapshotAddress = fixture.snapshotAddresses[index]
		}
		document.ShardEndpoints = append(document.ShardEndpoints,
			gatewayHotShardPersistedShardEndpoint{Node: fmt.Sprintf("%x", node),
				ControlAddress: links[index].address(), SplitSnapshotAddress: snapshotAddress})
	}
	raw, err := vibejson.Marshal(&document)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(fixture.root, "gateway-replica-control.vibejson")
	if err = os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func gatewayHotShardDialGateway(t testing.TB, profile *rafttransport.PeerTLS,
	node rafttransport.NodeID, address string, diagnostic *rf3CommandDiagnostic,
	exited <-chan error) net.Conn {
	t.Helper()
	deadline := func() time.Time { return time.Now().Add(2 * time.Second) }
	client, err := servicetls.NewClient(servicetls.ClientOptions{TLS: profile,
		Class:     rafttransport.TrafficGatewayClient,
		Endpoints: []servicetls.Endpoint{{Address: address, Node: node}},
		Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}, HandshakeDeadline: deadline, MaxConnections: 1, MaxHandshakes: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	until := time.Now().Add(30 * time.Second)
	for time.Now().Before(until) {
		connection, dialErr := client.Dial(t.Context(), address)
		if dialErr == nil {
			return connection
		}
		select {
		case exitErr := <-exited:
			t.Fatalf("gateway exited before ready: %v\n%s", exitErr, diagnostic.String())
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("gateway did not become ready\n%s", diagnostic.String())
	return nil
}

func gatewayHotShardDriveReads(t testing.TB, connection net.Conn, count int) []time.Duration {
	t.Helper()
	key, ok := orderedkey.AppendString(nil, []byte("hot-shard-missing"), orderedkey.Ascending)
	if !ok {
		t.Fatal("ordered key")
	}
	request := []byte(fmt.Sprintf(`{"op":"get","table":"controlplane","key":"%s","consistency":"linearizable"}`+"\n",
		base64.RawURLEncoding.EncodeToString(key)))
	reader := bufio.NewReader(connection)
	latencies := make([]time.Duration, count)
	for index := range latencies {
		if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		if _, err := connection.Write(request); err != nil {
			t.Fatalf("foreground write %d: %v", index, err)
		}
		response, err := reader.ReadBytes('\n')
		latencies[index] = time.Since(started)
		if err != nil || !strings.Contains(string(response), `"ok":true`) {
			t.Fatalf("foreground read %d response=%s err=%v", index, response, err)
		}
	}
	return latencies
}

func gatewayHotShardRepositoryRoot(t testing.TB) string {
	t.Helper()
	command := exec.Command("git", "rev-parse", "--show-toplevel")
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(output))
}

func gatewayHotShardProcessRSS(t testing.TB, pid int) uint64 {
	t.Helper()
	output, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		t.Fatal(err)
	}
	kib, err := strconv.ParseUint(strings.TrimSpace(string(output)), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return kib << 10
}

type gatewayHotShardNetworkLink struct {
	listener                                *net.TCPListener
	target                                  string
	enabled                                 atomic.Bool
	accepted, rejected, bytes, active, peak atomic.Uint64
	mu                                      sync.Mutex
	connections                             map[net.Conn]net.Conn
	closed                                  chan struct{}
	wg                                      sync.WaitGroup
}

func newGatewayHotShardNetworkLink(t testing.TB, target string) *gatewayHotShardNetworkLink {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	link := &gatewayHotShardNetworkLink{listener: listener, target: target,
		connections: make(map[net.Conn]net.Conn), closed: make(chan struct{})}
	link.enabled.Store(true)
	link.wg.Add(1)
	go link.accept()
	return link
}

func (link *gatewayHotShardNetworkLink) address() string { return link.listener.Addr().String() }

func (link *gatewayHotShardNetworkLink) accept() {
	defer link.wg.Done()
	for {
		incoming, err := link.listener.Accept()
		if err != nil {
			select {
			case <-link.closed:
				return
			default:
				continue
			}
		}
		if !link.enabled.Load() {
			link.rejected.Add(1)
			_ = incoming.Close()
			continue
		}
		outgoing, err := net.DialTimeout("tcp", link.target, time.Second)
		if err != nil {
			link.rejected.Add(1)
			_ = incoming.Close()
			continue
		}
		link.accepted.Add(1)
		active := link.active.Add(1)
		for peak := link.peak.Load(); active > peak && !link.peak.CompareAndSwap(peak, active); peak = link.peak.Load() {
		}
		link.mu.Lock()
		link.connections[incoming] = outgoing
		link.mu.Unlock()
		link.wg.Add(1)
		go link.relay(incoming, outgoing)
	}
}

func (link *gatewayHotShardNetworkLink) relay(incoming, outgoing net.Conn) {
	defer link.wg.Done()
	done := make(chan uint64, 2)
	copyLane := func(destination, source net.Conn) {
		count, _ := io.Copy(destination, source)
		done <- uint64(max(count, 0))
	}
	go copyLane(outgoing, incoming)
	go copyLane(incoming, outgoing)
	link.bytes.Add(<-done)
	_ = incoming.Close()
	_ = outgoing.Close()
	link.bytes.Add(<-done)
	link.active.Add(^uint64(0))
	link.mu.Lock()
	delete(link.connections, incoming)
	link.mu.Unlock()
}

func (link *gatewayHotShardNetworkLink) partition() {
	link.enabled.Store(false)
	link.mu.Lock()
	for incoming, outgoing := range link.connections {
		link.rejected.Add(1)
		_ = incoming.Close()
		_ = outgoing.Close()
	}
	link.mu.Unlock()
}

func (link *gatewayHotShardNetworkLink) heal() { link.enabled.Store(true) }

func (link *gatewayHotShardNetworkLink) close() {
	select {
	case <-link.closed:
		return
	default:
		close(link.closed)
	}
	_ = link.listener.Close()
	link.partition()
	link.wg.Wait()
}
