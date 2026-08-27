//go:build linux

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/asn1"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
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
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	vibejson "github.com/thesyncim/vibejson"
)

const durableRF3ExternalEnvironment = "VIBEDB_DURABLE_RF3_PROCESS_E2E"

const (
	durableRF3ExternalVoters = 3
	// Shards plus gateway A, gateway B, the stable user principal, and an
	// independent observation principal all carry distinct certificate IDs.
	durableRF3ExternalNodes = 7
)

// TestGatewayDurableRF3ExternalProcessRecovery is the black-box deployment
// qualification for the shipped durable SQL path. Three shard OS processes
// each host four independent multi-Raft groups: catalog RF3, request-ledger
// RF3, and two data RF3 groups. All shard, gateway, and client traffic crosses
// the native mTLS transport.
//
// A first gateway loses all but one byte of a committed terminal response and
// is killed. A replacement gateway with a different certificate principal,
// catalog-session identity, retry home, and journal recovers the exact result
// after the elected ledger voter is SIGKILLed. The ACK response is lost at the
// same deterministic external boundary, its GC-complete tombstone is read
// authoritatively, the next ledger leader replays the exact ACK, and both
// gateways subsequently retire their principal-specific pin journals.
func TestGatewayDurableRF3ExternalProcessRecovery(t *testing.T) {
	if os.Getenv(durableRF3ExternalEnvironment) != "1" {
		t.Skip("set VIBEDB_DURABLE_RF3_PROCESS_E2E=1 for mandatory external RF3 qualification")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("required external RF3 qualification needs Linux /proc and strict allocation")
	}
	if testing.Short() {
		t.Fatal("required external RF3 qualification cannot run in short mode")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	fixture := newDurableRF3ExternalFixture(t, ctx)
	defer fixture.close(t)
	fixture.startShards(t)
	fixture.startGateway(t, fixture.gatewayA)
	fixture.initializeObservers(t)

	baselineStorage := replicaProcessAllocatedBytes(fixture.root, "")
	baselineWALs := fixture.captureWALAllocatedBytes(t)
	baselineWAL := durableRF3ExternalAllocatedTotal(baselineWALs)
	baselineSnapshot := replicaProcessSnapshotPayloadBytes(fixture.root)
	baselineRSS := fixture.liveRSS()
	if baselineStorage == 0 || baselineStorage > 2<<30 || baselineWAL == 0 || baselineRSS == 0 {
		t.Fatalf("invalid baseline storage=%d wal=%d rss=%d", baselineStorage, baselineWAL, baselineRSS)
	}
	measurements := &durableRF3ExternalMeasurements{peakRSS: baselineRSS}
	fixture.measurements = measurements
	measurementStop := make(chan struct{})
	measurementDone := make(chan struct{})
	go fixture.sample(measurementStop, measurementDone, measurements)
	defer func() {
		select {
		case <-measurementDone:
		default:
			close(measurementStop)
			<-measurementDone
		}
	}()

	clientA := fixture.dialGateway(t, fixture.gatewayANode, fixture.gatewayAAddress)
	reference, openLatency := clientA.openIssuer(t)
	latencies := []time.Duration{openLatency}

	// SIGSTOP is an honest process partition: sockets remain established but
	// the selected voter cannot tick, receive, or serve. Because every shard
	// process hosts every role, explicitly require a two-voter leader for all
	// four RF3 groups before issuing a foreground read.
	partitionLeader, _ := fixture.waitRouteLeader(t, durableRF3DataAGroup, -1, 30*time.Second)
	partitioned := durableRF3ExternalLeaderMember(t, partitionLeader)
	partitionStarted := time.Now()
	if err := syscall.Kill(fixture.shards[partitioned].PID(), syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	partitionActive := true
	defer func() {
		if partitionActive {
			_ = syscall.Kill(fixture.shards[partitioned].PID(), syscall.SIGCONT)
		}
	}()
	fixture.waitAllRoleLeaders(t, partitioned, 30*time.Second)
	partitionFailover := time.Since(partitionStarted)
	if partitionFailover > 15*time.Second {
		t.Fatalf("partition failover=%s exceeds 15s", partitionFailover)
	}
	queryLatency := clientA.assertPoint(t, "orders_a", "seed-a")
	latencies = append(latencies, queryLatency)

	// Keep the selected process partitioned through the actual two-group
	// durable write, terminal publication, lost response, and independent
	// readback. This exercises the shipped path on two-voter quorums for every
	// role rather than proving only adjacent read availability.
	terminalRequest := hotMutationRequest(t, reference, 1, []serveStatement{
		{SQL: `INSERT INTO orders_a VALUES (?)`, Params: []serveParam{{Kind: "document", Text: `{"id":"terminal-a","kind":"terminal","email":"terminal-a@example.test","value":101}`}}},
		{SQL: `INSERT INTO orders_b VALUES (?)`, Params: []serveParam{{Kind: "document", Text: `{"id":"terminal-b","kind":"terminal","email":"terminal-b@example.test","value":202}`}}},
	})
	terminalLossLatency := clientA.loseResponseAfterFirstByte(t, terminalRequest)
	latencies = append(latencies, terminalLossLatency)
	fixture.assertPinJournalRetired(t, fixture.gatewayAJournal)
	// The byte proves the server emitted a response, while these independent
	// linearizable reads prove the discarded response followed durable apply.
	proofA := fixture.dialGateway(t, fixture.gatewayANode, fixture.gatewayAAddress)
	latencies = append(latencies, proofA.assertPoint(t, "orders_a", "terminal-a"))
	latencies = append(latencies, proofA.assertPoint(t, "orders_b", "terminal-b"))
	proofA.close()
	if err := syscall.Kill(fixture.shards[partitioned].PID(), syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	partitionActive = false
	fixture.waitMemberCaughtUpAllRoles(t, partitioned, 45*time.Second)
	fixture.waitAllRoleLeaders(t, -1, 30*time.Second)
	if err := fixture.gatewayA.Kill(ctx); err != nil {
		t.Fatal(err)
	}

	terminalLedgerLeader, _ := fixture.waitRouteLeader(t, durableRF3LedgerGroup, -1, 30*time.Second)
	terminalKilled := durableRF3ExternalLeaderMember(t, terminalLedgerLeader)
	terminalFailoverStarted := time.Now()
	fixture.killShard(t, terminalKilled)
	fixture.waitAllRoleLeaders(t, terminalKilled, 30*time.Second)
	terminalFailover := time.Since(terminalFailoverStarted)
	if terminalFailover > 15*time.Second {
		t.Fatalf("terminal leader failover=%s exceeds 15s", terminalFailover)
	}
	gatewayReplacementStarted := time.Now()
	fixture.startGateway(t, fixture.gatewayB)
	fixture.assertRouteSeedFilesDistinct(t)
	clientB := fixture.dialGateway(t, fixture.gatewayBNode, fixture.gatewayBAddress)
	recoveredRaw, recoveredLatency := clientB.roundTrip(t, terminalRequest)
	latencies = append(latencies, recoveredLatency)
	gatewayReplacement := time.Since(gatewayReplacementStarted)
	if gatewayReplacement > 15*time.Second {
		t.Fatalf("gateway replacement recovery=%s exceeds 15s", gatewayReplacement)
	}
	recovered := durableRF3ExternalExecResponse(t, recoveredRaw)
	if !recovered.Committed || recovered.RowsAffected != 2 || recovered.ShardsFanned != 2 ||
		recovered.Error != "" {
		t.Fatalf("replacement terminal response=%s", recoveredRaw)
	}
	replayedRaw, replayLatency := clientB.roundTrip(t, terminalRequest)
	latencies = append(latencies, replayLatency)
	if !bytes.Equal(recoveredRaw, replayedRaw) {
		t.Fatalf("exact terminal replay drifted\nfirst=%s\nsecond=%s", recoveredRaw, replayedRaw)
	}
	ackRequest := durableRF3ExternalAckRequest(t, recoveredRaw)
	fixture.restartShard(t, terminalKilled)
	fixture.waitMemberCaughtUpAllRoles(t, terminalKilled, 45*time.Second)
	fixture.waitAllRoleLeaders(t, -1, 30*time.Second)

	userAuthority := serviceauthz.Authority{Node: fixture.nodes[fixture.userNode], Generation: 5}
	requestKey, err := fixture.catalogAuthority.ValidateIssuerRequestKey(
		ctx, userAuthority, authenticatedIssuerTenantResolver{}, ackRequest.Identity.Reference,
		requestledger.RequestID(ackRequest.Identity.RequestID), ackRequest.Identity.IssuerSequence,
	)
	if err != nil {
		t.Fatal(err)
	}
	home := fixture.requestHome(t, requestKey)

	// Heal the complete RF3 before the distinct ACK-loss fault. Reading only
	// one response byte makes the ACK unusable to the caller while proving the
	// shipped gateway emitted it after collection.
	ackWire := sessionProtocolAckRequest(t, ackRequest)
	ackLossLatency := clientB.loseResponseAfterFirstByte(t, ackWire)
	latencies = append(latencies, ackLossLatency)
	ackBeforeKill := fixture.waitAckComplete(t, home, requestKey, 30*time.Second)
	durableRF3ExternalAssertAckRecord(t, ackBeforeKill, requestKey, ackRequest)
	fixture.assertPinJournalRetired(t, fixture.gatewayBJournal)

	ackLedgerLeader, _ := fixture.waitRouteLeader(t, durableRF3LedgerGroup, -1, 30*time.Second)
	ackKilled := durableRF3ExternalLeaderMember(t, ackLedgerLeader)
	ackFailoverStarted := time.Now()
	fixture.killShard(t, ackKilled)
	fixture.waitAllRoleLeaders(t, ackKilled, 30*time.Second)
	ackFailover := time.Since(ackFailoverStarted)
	if ackFailover > 15*time.Second {
		t.Fatalf("ACK leader failover=%s exceeds 15s", ackFailover)
	}
	clientB = fixture.dialGateway(t, fixture.gatewayBNode, fixture.gatewayBAddress)
	ackRetryRaw, ackRetryLatency := clientB.roundTrip(t, ackWire)
	latencies = append(latencies, ackRetryLatency)
	durableRF3ExternalAssertAckResponse(t, ackRetryRaw, ackRequest)
	fixture.restartShard(t, ackKilled)
	fixture.waitMemberCaughtUpAllRoles(t, ackKilled, 45*time.Second)
	fixture.waitAllRoleLeaders(t, -1, 30*time.Second)
	ackAfterKill := fixture.waitAckComplete(t, home, requestKey, 30*time.Second)
	durableRF3ExternalAssertAckRecord(t, ackAfterKill, requestKey, ackRequest)
	if ackAfterKill.AckDigest != ackBeforeKill.AckDigest ||
		ackAfterKill.ReclaimedBytes != ackAfterKill.PriorEncodedBytes {
		t.Fatalf("ACK tombstone drifted across failover before=%+v after=%+v", ackBeforeKill, ackAfterKill)
	}

	// Stop gateway B, reopen gateway A's original catalog journal, and replay
	// the ACK to prove the replicated tombstone survives process replacement
	// while both principal-specific local pin journals remain retired.
	fixture.stopGateway(t, fixture.gatewayB)
	fixture.startGateway(t, fixture.gatewayA)
	clientA = fixture.dialGateway(t, fixture.gatewayANode, fixture.gatewayAAddress)
	ackARaw, ackALatency := clientA.roundTrip(t, ackWire)
	latencies = append(latencies, ackALatency)
	durableRF3ExternalAssertAckResponse(t, ackARaw, ackRequest)
	if !bytes.Equal(ackARaw, ackRetryRaw) {
		t.Fatalf("exact ACK replay drifted across gateways\nB=%s\nA=%s", ackRetryRaw, ackARaw)
	}
	fixture.assertPinJournalRetired(t, fixture.gatewayAJournal)
	fixture.assertPinJournalRetired(t, fixture.gatewayBJournal)
	acknowledgedRaw, acknowledgedLatency := clientA.roundTrip(t, terminalRequest)
	latencies = append(latencies, acknowledgedLatency)
	acknowledged := durableRF3ExternalExecResponse(t, acknowledgedRaw)
	if acknowledged.Error == "" || !strings.Contains(acknowledged.Error, gateway.ErrDurableRequestAcknowledged.Error()) ||
		acknowledged.Committed || strings.Contains(string(acknowledgedRaw), `"ack_token"`) {
		t.Fatalf("acknowledged exact exec replay=%s", acknowledgedRaw)
	}

	// Restart every shard process one at a time. Since a process hosts all four
	// roles, each kill must leave and then restore an independently probed RF3
	// quorum for catalog, ledger, and both data groups.
	maxVoterFailover := time.Duration(0)
	for member := 0; member < durableRF3ExternalVoters; member++ {
		failoverStarted := time.Now()
		fixture.killShard(t, member)
		fixture.waitAllRoleLeaders(t, member, 30*time.Second)
		failover := time.Since(failoverStarted)
		maxVoterFailover = max(maxVoterFailover, failover)
		if failover > 15*time.Second {
			t.Fatalf("rolling voter %d failover=%s exceeds 15s", member+1, failover)
		}
		fixture.restartShard(t, member)
		fixture.waitMemberCaughtUpAllRoles(t, member, 45*time.Second)
	}
	fixture.waitAllRoleLeaders(t, -1, 30*time.Second)
	postRestartAck, postRestartLatency := clientA.roundTrip(t, ackWire)
	latencies = append(latencies, postRestartLatency)
	durableRF3ExternalAssertAckResponse(t, postRestartAck, ackRequest)
	if !bytes.Equal(postRestartAck, ackRetryRaw) {
		t.Fatalf("exact ACK replay drifted after every voter restart\nwant=%s\ngot=%s",
			ackRetryRaw, postRestartAck)
	}
	postRestartRecord := fixture.waitAckComplete(t, home, requestKey, 30*time.Second)
	durableRF3ExternalAssertAckRecord(t, postRestartRecord, requestKey, ackRequest)
	if postRestartRecord.AckDigest != ackBeforeKill.AckDigest {
		t.Fatalf("ACK digest changed after every voter restart: %x != %x",
			postRestartRecord.AckDigest, ackBeforeKill.AckDigest)
	}

	for range 8 {
		latencies = append(latencies, clientA.assertPoint(t, "orders_a", "terminal-a"))
		latencies = append(latencies, clientA.assertPoint(t, "orders_b", "terminal-b"))
	}
	clientA.close()
	clientB.close()
	close(measurementStop)
	<-measurementDone

	slices.Sort(latencies)
	p99 := latencies[(len(latencies)*99+99)/100-1]
	if p99 > 5*time.Second {
		t.Fatalf("external durable RF3 foreground p99=%s exceeds 5s", p99)
	}
	finalStorage := replicaProcessAllocatedBytes(fixture.root, "")
	finalWALs := fixture.captureWALAllocatedBytes(t)
	finalWAL := durableRF3ExternalAllocatedTotal(finalWALs)
	finalSnapshot := replicaProcessSnapshotPayloadBytes(fixture.root)
	storageGrowth := positiveDifference(finalStorage, baselineStorage)
	walGrowth := positiveDifference(finalWAL, baselineWAL)
	for path, baseline := range baselineWALs {
		final, found := finalWALs[path]
		if !found {
			t.Fatalf("RF3 WAL disappeared: %q", path)
		}
		if growth := positiveDifference(final, baseline); growth > 16<<20 {
			t.Fatalf("RF3 WAL %q grew %d bytes, exceeds 16MiB", path, growth)
		}
	}
	snapshotGrowth := positiveDifference(finalSnapshot, baselineSnapshot)
	measurements.mu.Lock()
	peakRSS, clientBytes := measurements.peakRSS, measurements.clientBytes
	measurements.mu.Unlock()
	rssGrowth := positiveDifference(peakRSS, baselineRSS)
	if storageGrowth > 128<<20 || walGrowth > 64<<20 || rssGrowth > 384<<20 ||
		snapshotGrowth != 0 || clientBytes > 2<<20 {
		t.Fatalf("external durable RF3 bounds rss=%d storage=%d wal=%d public_client_wire=%d snapshot_payload=%d",
			rssGrowth, storageGrowth, walGrowth, clientBytes, snapshotGrowth)
	}
	for _, journal := range []string{fixture.gatewayAJournal, fixture.gatewayBJournal} {
		if !replicaProcessTreeBounded(t, fixture.root, filepath.Base(journal), 6, 8<<20) {
			t.Fatalf("unbounded gateway journal tree %q", journal)
		}
	}
	// The public wire counter is exact for every test-owned gateway request and
	// response. No shipped external Raft byte counter exists, so the gate names
	// that boundary honestly and separately proves zero snapshot payload growth.
	t.Logf("durable external RF3: roles=4 shard_processes=3 gateway_replacement=true gateway_principals_distinct=true route_seeds_distinct=true mtls=true shard_sigstop=true shard_sigkill=true all_shards_restarted=true terminal_response_lost=true ack_response_lost=true exact_terminal_replay=true exact_ack_replay=true acknowledged_replay_refused=true no_acknowledged_loss=true partition_failover=%s terminal_failover=%s ack_failover=%s gateway_replacement_recovery=%s max_voter_failover=%s p99=%s rss_growth=%d storage_growth=%d wal_growth=%d public_client_wire_bytes=%d snapshot_payload_bytes=%d ack_gc_complete=true pin_journals_retired=true",
		partitionFailover, terminalFailover, ackFailover, gatewayReplacement, maxVoterFailover,
		p99, rssGrowth, storageGrowth, walGrowth, clientBytes, snapshotGrowth)
}

type durableRF3ExternalFixture struct {
	ctx  context.Context
	root string

	nodes       [durableRF3ExternalNodes]rafttransport.NodeID
	credentials []rf3testfixture.Credential
	roots       string
	policy      string
	listeners   [4]rf3testfixture.ProcessListeners
	identities  [durableRF3ExternalGroups][4]raftstore.Identity
	routes      [durableRF3ExternalGroups]gateway.ReplicatedRoute
	capability  [durableRF3ExternalGroups]serviceauthz.Capability

	shards            [durableRF3ExternalVoters]*rf3testfixture.ExternalProcess
	gatewayA          *rf3testfixture.ExternalProcess
	gatewayB          *rf3testfixture.ExternalProcess
	gatewayANode      int
	gatewayBNode      int
	userNode          int
	observerNode      int
	gatewayAAddress   string
	gatewayBAddress   string
	gatewayAJournal   string
	gatewayBJournal   string
	gatewayARouteSeed string
	gatewayBRouteSeed string
	catalogPath       string

	userProfile      *rafttransport.PeerTLS
	observerProfile  *rafttransport.PeerTLS
	probeClient      *gateway.AuthenticatedReplicatedClient
	catalogAuthority *gateway.ReplicatedCatalogAuthority
	ledger           *gateway.DurableRequestLedgerRF3
	topology         *gateway.DurableRequestLedgerTopologyHolder
	snapshot         *gateway.Snapshot
	walPaths         [durableRF3ExternalGroups][durableRF3ExternalVoters]string
	catalogClose     func()

	measurements *durableRF3ExternalMeasurements
	seeded       bool
}

type durableRF3ExternalMeasurements struct {
	mu          sync.Mutex
	peakRSS     uint64
	clientBytes uint64
}

func newDurableRF3ExternalFixture(
	t *testing.T,
	ctx context.Context,
) *durableRF3ExternalFixture {
	t.Helper()
	fixture := &durableRF3ExternalFixture{ctx: ctx, root: t.TempDir(),
		gatewayANode: 3, gatewayBNode: 4, userNode: 5, observerNode: 6}
	cluster, err := rf3testfixture.ReserveProcessCluster()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cluster.Close() })
	fixture.listeners = cluster.Members()
	gatewayAddresses, err := rf3testfixture.ReserveLoopbackAddresses(2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gatewayAddresses.Close() })
	fixture.gatewayAAddress, fixture.gatewayBAddress = gatewayAddresses.Addresses[0], gatewayAddresses.Addresses[1]

	for node := range fixture.nodes {
		for index := range fixture.nodes[node] {
			fixture.nodes[node][index] = byte((node+1)*29 + index)
		}
	}
	fixture.identities = durableRF3ExternalIdentities()
	fixture.credentials, fixture.roots, err = rf3testfixture.WriteCredentials(
		fixture.root, asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1},
		rafttransport.TrustDomain{ClusterID: fixture.identities[0][0].ClusterID,
			ClusterIncarnation: fixture.identities[0][0].ClusterIncarnation}, fixture.nodes[:],
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture.policy = filepath.Join(fixture.root, "authorization-policy.vibejson")
	if err = os.WriteFile(fixture.policy, durableRF3ExternalPolicy(t, fixture.nodes[:]), 0o600); err != nil {
		t.Fatal(err)
	}

	profiles := durableRF3ExternalMemberProfiles()
	authority := profiles[durableRF3CatalogGroup].Authority
	wal := durableRF3ExternalWALOptions()
	key := raftstore.Key{ID: "durable-rf3-process-key", Wrapped: []byte("external-wrapped-key")}
	for index := range key.Material {
		key.Material[index] = byte(index + 1)
	}
	ledgerIdentity := profiles[durableRF3LedgerGroup].Apply.RequestLedgerRangeIdentity
	var prepared [durableRF3ExternalGroups][durableRF3ExternalVoters]rf3testfixture.PreparedProcessMember
	var memberNodes [3]rafttransport.NodeID
	var peerAddresses [3]string
	copy(memberNodes[:], fixture.nodes[:durableRF3ExternalVoters])
	for member := range peerAddresses {
		peerAddresses[member] = fixture.listeners[member].Peer
	}
	for group := 0; group < durableRF3ExternalGroups; group++ {
		profile := profiles[group]
		for member := 0; member < durableRF3ExternalVoters; member++ {
			prepared[group][member], err = rf3testfixture.PrepareProcessMember(
				rf3testfixture.ProcessMemberOptions{
					Root:        filepath.Join(fixture.root, fmt.Sprintf("role-%d-member-%d", group, member+1)),
					ControlRoot: filepath.Join(fixture.root, fmt.Sprintf("node-%d-control", member+1)),
					Table:       profile.Table, CreateTable: profile.CreateTable, Identity: fixture.identities[group][member],
					Key: key, WAL: wal, Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
					Authority: profile.Authority, Apply: profile.Apply, Listeners: fixture.listeners[member],
					Credential: fixture.credentials[member], Roots: fixture.roots,
					AuthorizationPolicy: fixture.policy, Nodes: memberNodes,
					PeerAddresses: peerAddresses, SeedDocuments: profile.SeedDocuments,
					SchemaStatements: profile.SchemaStatements, GlobalIndexes: profile.GlobalIndexes,
				},
			)
			if errors.Is(err, storeio.ErrStrictAllocationUnsupported) ||
				errors.Is(err, raftstore.ErrPlatformUnsupported) {
				t.Fatalf("required strict RF3 allocation unsupported: %v", err)
			}
			if err != nil {
				t.Fatal(err)
			}
			if member > 0 && prepared[group][member].RelationManifestDigest !=
				prepared[group][0].RelationManifestDigest {
				t.Fatalf("role %s relation digest drifted", durableRF3ExternalRoleNames[group])
			}
		}
		fixture.routes[group] = durableRF3ExternalRoute(
			fixture.identities[group], fixture.listeners, fixture.nodes,
			authority, replication.Digest(prepared[group][0].RelationManifestDigest),
		)
		fixture.routes[group].LogicalSchemaDigest = replication.Digest(prepared[group][0].LogicalSchemaDigest)
	}
	fixture.routes[durableRF3LedgerGroup].RangeIdentity = replication.Digest(ledgerIdentity)
	fixture.capability = [durableRF3ExternalGroups]serviceauthz.Capability{
		serviceauthz.CapabilityTopology, serviceauthz.CapabilityRequestLedger,
		serviceauthz.CapabilityDataWrite, serviceauthz.CapabilityDataWrite,
	}

	ackKey := gateway.DurableRequestAckDerivationKey{}
	for index := range ackKey {
		ackKey[index] = 0x3a
	}
	built, err := rf3testfixture.NewDurableCatalog(rf3testfixture.DurableCatalogOptions{
		Generation: 1, AckKey: ackKey, Groups: []rf3testfixture.DurableCatalogGroup{
			{Route: fixture.routes[durableRF3CatalogGroup], Table: profiles[durableRF3CatalogGroup].Table,
				PrimaryKey: "/id", Relation: 1, MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20},
			{Route: fixture.routes[durableRF3LedgerGroup], Table: profiles[durableRF3LedgerGroup].Table,
				PrimaryKey: "/home", LedgerRanges: []gateway.DurableRequestLedgerRangeDescriptor{{
					Identity: replication.Digest(ledgerIdentity),
				}}},
			{Route: fixture.routes[durableRF3DataAGroup], Table: profiles[durableRF3DataAGroup].Table,
				PrimaryKey: "/id", Relation: 1, MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20,
				AdditionalTables: []rf3testfixture.DurableCatalogTable{{Table: "orders_b_email",
					PrimaryKey: "/email", Relation: 2, MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20}}},
			{Route: fixture.routes[durableRF3DataBGroup], Table: profiles[durableRF3DataBGroup].Table,
				PrimaryKey: "/id", Relation: 1, MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20,
				AdditionalTables: []rf3testfixture.DurableCatalogTable{{Table: "orders_a_email",
					PrimaryKey: "/email", Relation: 2, MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20}}},
		},
		Indexes: []gateway.IndexDescriptor{
			{IndexID: 31, Incarnation: 1, Table: "orders_a", Name: "by_kind_a",
				Paths: []string{"/kind"}, Flags: gateway.IndexLocal | gateway.IndexOrdered,
				Lifecycle: gateway.IndexReady},
			{IndexID: 32, Incarnation: 1, Table: "orders_b", Name: "by_kind_b",
				Paths: []string{"/kind"}, Flags: gateway.IndexLocal | gateway.IndexOrdered,
				Lifecycle: gateway.IndexReady},
			{IndexID: 41, Incarnation: 1, Table: "orders_a", Name: "by_email_a",
				Relation: "orders_a_email", Paths: []string{"/email"},
				LocatorPaths: []string{"/id"}, PrimaryPath: "/id",
				Flags:     gateway.IndexGlobal | gateway.IndexUnique | gateway.IndexOrdered,
				Lifecycle: gateway.IndexReady},
			{IndexID: 42, Incarnation: 1, Table: "orders_b", Name: "by_email_b",
				Relation: "orders_b_email", Paths: []string{"/email"},
				LocatorPaths: []string{"/id"}, PrimaryPath: "/id",
				Flags:     gateway.IndexGlobal | gateway.IndexUnique | gateway.IndexOrdered,
				Lifecycle: gateway.IndexReady},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	catalogPath := filepath.Join(fixture.root, "immutable-genesis.vibejson")
	if err = gateway.SaveSnapshot(catalogPath, built.Snapshot); err != nil {
		t.Fatal(err)
	}
	fixture.catalogPath = catalogPath
	ackPath := filepath.Join(fixture.root, "durable-ack-key")
	if err = os.WriteFile(ackPath, []byte(strings.Repeat("3a", 32)), 0o600); err != nil {
		t.Fatal(err)
	}

	shardBinary, gatewayBinary := durableRF3ExternalBinaryPaths(t, fixture.root)
	for member := 0; member < durableRF3ExternalVoters; member++ {
		documents := make([][]byte, durableRF3ExternalGroups)
		for group := range documents {
			documents[group], err = os.ReadFile(prepared[group][member].ManifestPath)
			if err != nil {
				t.Fatal(err)
			}
			fixture.walPaths[group][member] = prepared[group][member].WALPath
		}
		bundle, bundleErr := rf3testfixture.CombineProcessManifests(documents...)
		if bundleErr != nil {
			t.Fatalf("combine member %d process groups: %v", member+1, bundleErr)
		}
		manifestPath := filepath.Join(fixture.root, fmt.Sprintf("member-%d-rf3.vibejson", member+1))
		if err = os.WriteFile(manifestPath, bundle, 0o600); err != nil {
			t.Fatal(err)
		}
		fixture.shards[member] = &rf3testfixture.ExternalProcess{Binary: shardBinary,
			Args: []string{"serve-rf3", "-manifest", manifestPath}}
	}
	replicaProcessBuild(t, ctx, shardBinary, "./cmd/vibedb-shard")
	replicaProcessBuild(t, ctx, gatewayBinary, "./cmd/vibedb-gateway")
	t.Logf("external RF3 reserved_wal_bytes=%d build_artifact_allocated_bytes=%d (outside persistent data root)",
		int64(durableRF3ExternalGroups*durableRF3ExternalVoters)*wal.MaxFileBytes,
		replicaProcessAllocatedBytes(filepath.Dir(shardBinary), ""))
	if err = cluster.ReleaseListeners(); err != nil {
		t.Fatal(err)
	}
	if err = gatewayAddresses.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.gatewayAJournal = filepath.Join(fixture.root, "gateway-a-session")
	fixture.gatewayBJournal = filepath.Join(fixture.root, "gateway-b-session")
	fixture.gatewayARouteSeed = filepath.Join(fixture.root, "gateway-a-catalog-route-seed.vibejson")
	fixture.gatewayBRouteSeed = filepath.Join(fixture.root, "gateway-b-catalog-route-seed.vibejson")
	if filepath.Clean(fixture.gatewayARouteSeed) == filepath.Clean(fixture.gatewayBRouteSeed) {
		t.Fatal("gateway route-seed paths alias")
	}
	for _, routeSeed := range []string{fixture.gatewayARouteSeed, fixture.gatewayBRouteSeed} {
		if err = gateway.ValidateReplicatedCatalogRouteSeedSeparation(catalogPath, routeSeed); err != nil {
			t.Fatalf("catalog genesis/route-seed separation: %v", err)
		}
	}
	fixture.gatewayA = durableRF3ExternalGatewayProcess(gatewayBinary, catalogPath,
		fixture.gatewayARouteSeed,
		fixture.gatewayAAddress, fixture.credentials[fixture.gatewayANode], fixture.roots,
		fixture.policy, ackPath, fixture.gatewayAJournal, "1a", "2b", fixture.listeners,
		fixture.nodes, true)
	fixture.gatewayB = durableRF3ExternalGatewayProcess(gatewayBinary, catalogPath,
		fixture.gatewayBRouteSeed,
		fixture.gatewayBAddress, fixture.credentials[fixture.gatewayBNode], fixture.roots,
		fixture.policy, ackPath, fixture.gatewayBJournal, "3c", "4d", fixture.listeners,
		fixture.nodes, true)
	fixture.userProfile, err = servicetls.LoadProfile(fixture.credentials[fixture.userNode].Certificate,
		fixture.credentials[fixture.userNode].Key, fixture.roots, rf3testfixture.ProcessIdentityOID, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	fixture.observerProfile, err = servicetls.LoadProfile(
		fixture.credentials[fixture.observerNode].Certificate,
		fixture.credentials[fixture.observerNode].Key, fixture.roots,
		rf3testfixture.ProcessIdentityOID, time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture.probeClient = durableRF3ExternalReplicatedClient(t, fixture.observerProfile)
	fixture.measurements = &durableRF3ExternalMeasurements{}
	fixture.snapshot = built.Snapshot
	return fixture
}

func durableRF3ExternalIdentities() (identities [durableRF3ExternalGroups][4]raftstore.Identity) {
	base := replicaProcessIdentities()
	names := [...][2]string{
		{string(gateway.ReplicatedCatalogDistribution), string(gateway.ReplicatedCatalogShard)},
		{"request-ledger", "all"}, {"orders-a-data", "all"}, {"orders-b-data", "all"},
	}
	for group := range identities {
		for member := range base {
			identities[group][member] = base[member]
			identities[group][member].Distribution = names[group][0]
			identities[group][member].Shard = names[group][1]
			for index := range identities[group][member].GroupID {
				identities[group][member].GroupID[index] ^= byte(group*43 + 1)
				identities[group][member].ShardIncarnation[index] ^= byte(group*61 + 1)
			}
			// StoreID is a physical-process identity and intentionally remains
			// byte-identical across all four groups hosted by one member.
		}
	}
	return identities
}

type durableRF3ExternalPolicyDocument struct {
	Generation uint64                              `json:"generation"`
	Principals []durableRF3ExternalPolicyPrincipal `json:"principals"`
}

type durableRF3ExternalPolicyPrincipal struct {
	Node         string   `json:"node"`
	Capabilities []string `json:"capabilities"`
}

func durableRF3ExternalPolicy(t testing.TB, nodes []rafttransport.NodeID) []byte {
	t.Helper()
	capabilities := []string{"data_read", "data_write", "schema", "delegate", "membership",
		"topology", "transaction_recovery", "request_ledger", "execution_pin"}
	document := durableRF3ExternalPolicyDocument{Generation: 5,
		Principals: make([]durableRF3ExternalPolicyPrincipal, len(nodes))}
	for index, node := range nodes {
		document.Principals[index] = durableRF3ExternalPolicyPrincipal{
			Node: fmt.Sprintf("%x", node), Capabilities: append([]string(nil), capabilities...),
		}
	}
	raw, err := vibejson.Marshal(&document)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func durableRF3ExternalRoute(
	identities [4]raftstore.Identity,
	listeners [4]rf3testfixture.ProcessListeners,
	nodes [durableRF3ExternalNodes]rafttransport.NodeID,
	authority sqldriver.ReplicatedAuthorityProfile,
	digest replication.Digest,
) gateway.ReplicatedRoute {
	route := gateway.ReplicatedRoute{
		Distribution: distribution.DistributionName(identities[0].Distribution),
		Shard:        distribution.ShardID(identities[0].Shard),
		Group: raftmember.GroupKey{ClusterID: identities[0].ClusterID,
			ClusterIncarnation: identities[0].ClusterIncarnation, TopologyRecoveryEpoch: 3,
			ShardIncarnation: identities[0].ShardIncarnation, GroupID: identities[0].GroupID},
		AllocationGeneration: identities[0].AllocationGeneration,
		Command: raftservice.CommandFence{ReplicaSetVersion: 1,
			ActivePolicyGeneration: authority.ActivePolicyGeneration,
			ProtectionEpoch:        authority.ProtectionEpoch, OwnershipEpoch: authority.OwnershipEpoch,
			SchemaGeneration: authority.SchemaGeneration, RelationManifestDigest: digest,
			RoutingVersion: authority.RoutingVersion, RouteGeneration: authority.RouteGeneration},
		RangeIdentity:        replication.Digest{0x71, identities[0].GroupID[0]},
		LineageDigest:        replication.Digest{0x72, identities[0].GroupID[0]},
		ForwardingRuleDigest: replication.Digest{0x73, identities[0].GroupID[0]},
	}
	for member := 0; member < durableRF3ExternalVoters; member++ {
		route.Replicas = append(route.Replicas, gateway.ReplicatedEndpoint{
			Member: uint64(member + 1), Node: nodes[member], StoreID: identities[member].StoreID,
			NodeIncarnation: 1, Endpoint: listeners[member].Peer,
			DataAddress: listeners[member].Peer, NativeEndpoint: listeners[member].Native,
			Address: listeners[member].Native, ControlEndpoint: listeners[member].Control,
			ControlAddress: listeners[member].Control,
		})
	}
	return route
}

func durableRF3ExternalGatewayProcess(
	binary, catalog, routeSeed, listen string,
	credential rf3testfixture.Credential,
	roots, policy, ack, journal, clientByte, retryByte string,
	listeners [4]rf3testfixture.ProcessListeners,
	nodes [durableRF3ExternalNodes]rafttransport.NodeID,
	bootstrap bool,
) *rf3testfixture.ExternalProcess {
	args := []string{"serve", "-catalog", catalog, "-catalog-route-seed", routeSeed,
		"-catalog-relation", "1",
		"-catalog-session-journal", journal, "-durable-ack-key", ack,
		"-catalog-client-id", strings.Repeat(clientByte, 16),
		"-catalog-retry-home", strings.Repeat(retryByte, 8),
		"-catalog-attempts", "16", "-catalog-attempt-timeout", "2s",
		"-catalog-session-lease", "1h", "-controller-interval", "50ms",
		"-max-client-connections", "32", "-max-client-handshakes", "8",
		"-max-shard-connections-per-pool", "64", "-max-shard-handshakes-per-pool", "8",
		"-listen", listen, "-tls-certificate", credential.Certificate,
		"-tls-key", credential.Key, "-tls-roots", roots,
		"-tls-identity-oid", rf3testfixture.ProcessIdentityOID,
		"-authorization-policy", policy}
	if bootstrap {
		args = append(args, "-catalog-bootstrap-if-missing")
	}
	for member := 0; member < durableRF3ExternalVoters; member++ {
		args = append(args, "-shard-peer", listeners[member].Native+"="+fmt.Sprintf("%x", nodes[member]))
	}
	return &rf3testfixture.ExternalProcess{Binary: binary, Args: args}
}

func durableRF3ExternalReplicatedClient(
	t testing.TB,
	profile *rafttransport.PeerTLS,
) *gateway.AuthenticatedReplicatedClient {
	t.Helper()
	client, err := gateway.NewAuthenticatedReplicatedClient(gateway.AuthenticatedReplicatedClientOptions{
		TLS: profile, Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}, HandshakeDeadline: func() time.Time { return time.Now().Add(2 * time.Second) },
		MaxConnections: 32, MaxPerEndpoint: 8, MaxIdlePerEndpoint: 4,
		MaxHandshakes: 4, MaxWaiters: 32, MaxIdleAge: time.Minute, MaxLifetime: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func mustDurableRF3ExternalExecutor(
	t testing.TB,
	client gateway.ReplicatedRoundTripper,
) *gateway.ReplicatedExecutor {
	t.Helper()
	executor, err := gateway.NewReplicatedExecutorWithOptions(client, gateway.ReplicatedExecutorOptions{
		MaxAttempts: 16, AttemptTimeout: 2 * time.Second, LeaderHintCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	return executor
}

func (fixture *durableRF3ExternalFixture) initializeObservers(t *testing.T) {
	t.Helper()
	// These readers use a third authenticated service principal and exact
	// shipped RF3 codecs; neither reaches into a child process store.
	fixture.catalogAuthority, fixture.catalogClose = hotMutationCatalogAuthority(t, fixture.observerProfile,
		fixture.snapshot, filepath.Join(fixture.root, "observer-catalog-session"))
	observerAuthority := serviceauthz.Authority{Node: fixture.nodes[fixture.observerNode], Generation: 5}
	ledgerRF3, err := gateway.NewReplicatedRequestLedgerRF3(gateway.ReplicatedRequestLedgerRF3Options{
		Executor: mustDurableRF3ExternalExecutor(t, fixture.probeClient), Service: observerAuthority,
		ServiceTenant: durableRequestServiceTenant[:],
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.ledger, err = gateway.NewDurableRequestLedgerRF3(ledgerRF3)
	if err != nil {
		t.Fatal(err)
	}
	fixture.topology, err = gateway.NewCatalogDurableRequestLedgerTopologyHolder(
		gateway.NewCatalogHolder(fixture.snapshot),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func (fixture *durableRF3ExternalFixture) startShards(t testing.TB) {
	t.Helper()
	for _, process := range fixture.shards {
		if err := process.Start(); err != nil {
			t.Fatal(err)
		}
	}
	for member, process := range fixture.shards {
		if err := process.WaitReady(fixture.ctx, "vibedb-shard RF3 ready"); err != nil {
			t.Fatalf("member %d readiness: %v\n%s", member+1, err, process.Diagnostics())
		}
	}
}

func (fixture *durableRF3ExternalFixture) startGateway(
	t *testing.T,
	process *rf3testfixture.ExternalProcess,
) {
	t.Helper()
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	if err := process.WaitReady(fixture.ctx, "vibedb-gateway serving catalog generation"); err != nil {
		t.Fatalf("gateway readiness: %v\n%s", err, process.Diagnostics())
	}
	if !fixture.seeded {
		// Bundle activation requires unmaterialized base/index tables. Seeding
		// through the shipped transaction path also initializes both remote
		// global indexes instead of leaving the initial rows unindexed.
		node, address := fixture.gatewayANode, fixture.gatewayAAddress
		if process == fixture.gatewayB {
			node, address = fixture.gatewayBNode, fixture.gatewayBAddress
		}
		client := fixture.dialGateway(t, node, address)
		defer client.close()
		reference, _ := client.openIssuerInstallation(t, 0x92)
		request := hotMutationRequest(t, reference, 1, []serveStatement{
			{SQL: `INSERT INTO orders_a VALUES (?)`, Params: []serveParam{{Kind: "document", Text: `{"id":"seed-a","kind":"seed","email":"seed-a@example.test","value":1}`}}},
			{SQL: `INSERT INTO orders_b VALUES (?)`, Params: []serveParam{{Kind: "document", Text: `{"id":"seed-b","kind":"seed","email":"seed-b@example.test","value":2}`}}},
		})
		response, _ := client.roundTrip(t, request)
		durableRF3ExternalAssertCommitted(t, response, 2, 2)
		client.ackTerminal(t, response)
		fixture.seeded = true
	}
}

func (fixture *durableRF3ExternalFixture) stopGateway(
	t testing.TB,
	process *rf3testfixture.ExternalProcess,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := process.Stop(ctx); err != nil {
		t.Fatalf("stop gateway: %v\n%s", err, process.Diagnostics())
	}
}

func (fixture *durableRF3ExternalFixture) killShard(t testing.TB, member int) {
	t.Helper()
	if member < 0 || member >= len(fixture.shards) {
		t.Fatal("invalid shard member")
	}
	if err := fixture.shards[member].Kill(fixture.ctx); err != nil {
		t.Fatal(err)
	}
}

func (fixture *durableRF3ExternalFixture) restartShard(t testing.TB, member int) {
	t.Helper()
	if err := fixture.shards[member].Start(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.shards[member].WaitReady(fixture.ctx, "vibedb-shard RF3 ready"); err != nil {
		t.Fatalf("restart member %d: %v\n%s", member+1, err, fixture.shards[member].Diagnostics())
	}
}

func (fixture *durableRF3ExternalFixture) close(t testing.TB) {
	t.Helper()
	if fixture.catalogClose != nil {
		fixture.catalogClose()
	}
	if fixture.probeClient != nil {
		_ = fixture.probeClient.Close()
	}
	replicaProcessStop(t, fixture.gatewayA)
	replicaProcessStop(t, fixture.gatewayB)
	for _, process := range fixture.shards {
		replicaProcessStop(t, process)
	}
}

func (fixture *durableRF3ExternalFixture) assertRouteSeedFilesDistinct(t testing.TB) {
	t.Helper()
	genesis, err := os.Lstat(fixture.catalogPath)
	if err != nil || !genesis.Mode().IsRegular() {
		t.Fatalf("immutable catalog genesis info=%v err=%v", genesis, err)
	}
	var routeSeedInfo [2]os.FileInfo
	for index, path := range []string{fixture.gatewayARouteSeed, fixture.gatewayBRouteSeed} {
		if err = gateway.ValidateReplicatedCatalogRouteSeedSeparation(fixture.catalogPath, path); err != nil {
			t.Fatalf("gateway %d route-seed separation: %v", index+1, err)
		}
		routeSeedInfo[index], err = os.Lstat(path)
		if err != nil || !routeSeedInfo[index].Mode().IsRegular() || os.SameFile(genesis, routeSeedInfo[index]) {
			t.Fatalf("gateway %d route-seed info=%v err=%v", index+1, routeSeedInfo[index], err)
		}
		state, loadErr := gateway.LoadReplicatedCatalogRouteSeed(path)
		active, found := state.Active()
		var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
		var route gateway.ReplicatedRoute
		resolved := false
		if loadErr == nil && found && active != nil {
			route, resolved = active.ResolveReplicatedRoute(
				gateway.ReplicatedCatalogDistribution, gateway.ReplicatedCatalogShard, replicas[:0],
			)
		}
		if loadErr != nil || !found || active == nil || !resolved ||
			route.Group != fixture.routes[durableRF3CatalogGroup].Group {
			t.Fatalf("gateway %d route-seed active=%v found=%v resolved=%v err=%v",
				index+1, active, found, resolved, loadErr)
		}
	}
	if os.SameFile(routeSeedInfo[0], routeSeedInfo[1]) {
		t.Fatal("gateway route-seed files alias")
	}
}

func (fixture *durableRF3ExternalFixture) tryProbe(
	group, member int,
) (shardservice.ReplicatedMemberState, error) {
	if group < 0 || group >= durableRF3ExternalGroups || member < 0 || member >= durableRF3ExternalVoters {
		return shardservice.ReplicatedMemberState{}, errors.New("external RF3: invalid probe")
	}
	ctx, cancel := context.WithTimeout(fixture.ctx, time.Second)
	defer cancel()
	route := fixture.routes[group]
	response, err := fixture.probeClient.DoReplicated(ctx, route.Replicas[member],
		rf3FixtureProbeRequest(route, serviceauthz.Authority{
			Node: fixture.nodes[fixture.observerNode], Generation: 5,
		}, fixture.capability[group]),
	)
	if err != nil {
		return shardservice.ReplicatedMemberState{}, err
	}
	if response == nil || response.Kind != shardservice.ReplicatedHandshake ||
		response.State.Fence.Group != route.Group || response.State.LeaderID == 0 {
		return shardservice.ReplicatedMemberState{}, errors.New("external RF3: incomplete probe")
	}
	return response.State, nil
}

func durableRF3ExternalLeaderMember(t testing.TB, leader uint64) int {
	t.Helper()
	if leader == 0 || leader > durableRF3ExternalVoters {
		t.Fatalf("invalid RF3 leader member %d", leader)
	}
	return int(leader - 1)
}

func (fixture *durableRF3ExternalFixture) waitRouteLeader(
	t testing.TB,
	group, excluded int,
	timeout time.Duration,
) (uint64, uint64) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastStates [durableRF3ExternalVoters]shardservice.ReplicatedMemberState
	var lastErrors [durableRF3ExternalVoters]error
	for time.Now().Before(deadline) {
		leader := uint64(0)
		applied := uint64(0)
		observed := 0
		consistent := true
		for member := 0; member < durableRF3ExternalVoters; member++ {
			if member == excluded {
				continue
			}
			state, err := fixture.tryProbe(group, member)
			lastStates[member], lastErrors[member] = state, err
			if err != nil || leader != 0 && state.LeaderID != leader ||
				excluded >= 0 && state.LeaderID == uint64(excluded+1) {
				consistent = false
				break
			}
			leader = state.LeaderID
			applied = max(applied, state.Applied)
			observed++
		}
		if consistent && observed >= 2 && leader != 0 {
			return leader, applied
		}
		time.Sleep(20 * time.Millisecond)
	}
	for member, state := range lastStates {
		if member != excluded {
			t.Logf("role %s probe member=%d leader=%d term=%d applied=%d error=%v",
				durableRF3ExternalRoleNames[group], member+1, state.LeaderID, state.Term, state.Applied, lastErrors[member])
		}
	}
	t.Fatalf("role %s has no consistent RF3 leader excluding member %d",
		durableRF3ExternalRoleNames[group], excluded+1)
	return 0, 0
}

func (fixture *durableRF3ExternalFixture) waitAllRoleLeaders(
	t testing.TB,
	excluded int,
	timeout time.Duration,
) {
	t.Helper()
	for group := 0; group < durableRF3ExternalGroups; group++ {
		fixture.waitRouteLeader(t, group, excluded, timeout)
	}
}

func (fixture *durableRF3ExternalFixture) waitMemberCaughtUpAllRoles(
	t testing.TB,
	member int,
	timeout time.Duration,
) {
	t.Helper()
	for group := 0; group < durableRF3ExternalGroups; group++ {
		_, required := fixture.waitRouteLeader(t, group, member, timeout)
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			state, err := fixture.tryProbe(group, member)
			if err == nil && state.Applied >= required && state.LeaderID != 0 {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		state, err := fixture.tryProbe(group, member)
		if err != nil || state.Applied < required {
			t.Fatalf("member %d role %s did not catch up through %d: state=%+v err=%v",
				member+1, durableRF3ExternalRoleNames[group], required, state, err)
		}
	}
}

func (fixture *durableRF3ExternalFixture) requestHome(
	t testing.TB,
	key requestledger.RequestKey,
) gateway.DurableRequestLedgerHome {
	t.Helper()
	point, err := requestledger.Home(key)
	if err != nil {
		t.Fatal(err)
	}
	home, _, found := fixture.topology.Lookup(point)
	if !found || home.ReplicatedRoute().Group != fixture.routes[durableRF3LedgerGroup].Group {
		t.Fatalf("request home=%+v found=%v", home, found)
	}
	return home
}

func (fixture *durableRF3ExternalFixture) waitAckComplete(
	t testing.TB,
	home gateway.DurableRequestLedgerHome,
	key requestledger.RequestKey,
	timeout time.Duration,
) requestledger.AckRecord {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		row, err := fixture.ledger.ReadRow(fixture.ctx, home, gateway.DurableRequestLifecycleRead{
			Key: key, Kind: replicatedstate.RequestLedgerReadAck, MinimumApplied: 1,
		})
		if err == nil && row.Found && row.Kind == replicatedstate.RequestLedgerReadAck &&
			row.Ack.GCPhase == requestledger.AckGCComplete &&
			row.Ack.PriorEncodedBytes != 0 && row.Ack.ReclaimedBytes == row.Ack.PriorEncodedBytes &&
			row.Ack.TerminalResultBytes != 0 && row.Ack.AckDigest != (requestledger.Digest{}) {
			return row.Ack
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("authoritative ACK never reached GC-complete: %v", lastErr)
	return requestledger.AckRecord{}
}

func (fixture *durableRF3ExternalFixture) assertPinJournalRetired(
	t testing.TB,
	journal string,
) {
	t.Helper()
	directory := filepath.Clean(journal) + ".durable-pins"
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(directory)
		if errors.Is(err, os.ErrNotExist) || err == nil && len(entries) == 0 {
			return
		}
		if err != nil {
			t.Fatalf("read execution-pin journal %q: %v", directory, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	entries, err := os.ReadDir(directory)
	t.Fatalf("execution-pin journal %q retained entries=%v err=%v", directory, entries, err)
}

func (fixture *durableRF3ExternalFixture) captureWALAllocatedBytes(
	t testing.TB,
) map[string]uint64 {
	t.Helper()
	want := make(map[string]struct{}, durableRF3ExternalGroups*durableRF3ExternalVoters)
	for group := 0; group < durableRF3ExternalGroups; group++ {
		for member := 0; member < durableRF3ExternalVoters; member++ {
			path := filepath.Clean(fixture.walPaths[group][member])
			if path == "." {
				t.Fatalf("role %s member %d has no WAL path",
					durableRF3ExternalRoleNames[group], member+1)
			}
			if _, duplicate := want[path]; duplicate {
				t.Fatalf("RF3 WAL path aliases another role/member: %q", path)
			}
			want[path] = struct{}{}
		}
	}
	got := make(map[string]uint64, len(want))
	err := filepath.WalkDir(fixture.root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".wal") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Blocks <= 0 {
			return fmt.Errorf("RF3 WAL %q has no allocated-block evidence", path)
		}
		got[filepath.Clean(path)] = uint64(stat.Blocks) * 512
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("external RF3 WAL set=%d, want exactly %d", len(got), len(want))
	}
	for path := range want {
		if _, found := got[path]; !found {
			t.Fatalf("external RF3 WAL %q missing from allocated set", path)
		}
	}
	return got
}

func durableRF3ExternalAllocatedTotal(values map[string]uint64) uint64 {
	total := uint64(0)
	for _, value := range values {
		total += value
	}
	return total
}

func (fixture *durableRF3ExternalFixture) liveRSS() uint64 {
	total := uint64(0)
	for _, process := range fixture.shards {
		total += replicaProcessRSS(process.PID())
	}
	total += replicaProcessRSS(fixture.gatewayA.PID())
	total += replicaProcessRSS(fixture.gatewayB.PID())
	return total
}

func (fixture *durableRF3ExternalFixture) sample(
	stop <-chan struct{},
	done chan<- struct{},
	measurements *durableRF3ExternalMeasurements,
) {
	defer close(done)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		rss := fixture.liveRSS()
		measurements.mu.Lock()
		measurements.peakRSS = max(measurements.peakRSS, rss)
		measurements.mu.Unlock()
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
	}
}

type durableRF3ExternalWireClient struct {
	connection net.Conn
	reader     *bufio.Reader
	transport  *servicetls.Client
	metrics    *durableRF3ExternalMeasurements
}

func (fixture *durableRF3ExternalFixture) dialGateway(
	t testing.TB,
	serverNode int,
	address string,
) *durableRF3ExternalWireClient {
	t.Helper()
	transport, err := servicetls.NewClient(servicetls.ClientOptions{
		TLS: fixture.userProfile, Class: rafttransport.TrafficGatewayClient,
		Endpoints: []servicetls.Endpoint{{Address: address, Node: fixture.nodes[serverNode]}},
		Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}, HandshakeDeadline: func() time.Time { return time.Now().Add(2 * time.Second) },
		MaxConnections: 1, MaxHandshakes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		connection, dialErr := transport.Dial(fixture.ctx, address)
		if dialErr == nil {
			return &durableRF3ExternalWireClient{connection: connection,
				reader: bufio.NewReader(connection), transport: transport, metrics: fixture.measurements}
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = transport.Close()
	t.Fatal("gateway mTLS dial timeout")
	return nil
}

func (client *durableRF3ExternalWireClient) recordBytes(bytes uint64) {
	if client == nil || client.metrics == nil {
		return
	}
	client.metrics.mu.Lock()
	client.metrics.clientBytes += bytes
	client.metrics.mu.Unlock()
}

func (client *durableRF3ExternalWireClient) roundTrip(
	t testing.TB,
	request []byte,
) ([]byte, time.Duration) {
	t.Helper()
	if client == nil || client.connection == nil || len(request) == 0 {
		t.Fatal("invalid external gateway round trip")
	}
	if err := client.connection.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	wire := append(append([]byte(nil), request...), '\n')
	started := time.Now()
	written, err := client.connection.Write(wire)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.reader.ReadBytes('\n')
	latency := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	client.recordBytes(uint64(written + len(response)))
	return response, latency
}

func (client *durableRF3ExternalWireClient) loseResponseAfterFirstByte(
	t testing.TB,
	request []byte,
) time.Duration {
	t.Helper()
	if err := client.connection.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	wire := append(append([]byte(nil), request...), '\n')
	started := time.Now()
	written, err := client.connection.Write(wire)
	if err != nil {
		t.Fatal(err)
	}
	var first [1]byte
	read, err := client.reader.Read(first[:])
	latency := time.Since(started)
	if err != nil || read != 1 || first[0] != '{' {
		t.Fatalf("partial response read=%d first=%q err=%v", read, first[:read], err)
	}
	client.recordBytes(uint64(written + read))
	client.close()
	return latency
}

func (client *durableRF3ExternalWireClient) openIssuer(
	t *testing.T,
) (gateway.ReplicatedIssuerReference, time.Duration) {
	t.Helper()
	response, latency := client.roundTrip(t, sessionProtocolIssuerOpenRequest(t,
		gateway.ReplicatedIssuerOpen{Installation: replication.ID128{0x81}, Epoch: 1},
	))
	var decoded struct {
		OK             bool   `json:"ok"`
		InstallationID string `json:"installation_id"`
		IssuerEpoch    uint64 `json:"issuer_epoch"`
		LaneOrdinal    uint16 `json:"lane_ordinal"`
		GrantDigest    string `json:"grant_digest"`
		Error          string `json:"error"`
	}
	if err := vibejson.Unmarshal(response, &decoded); err != nil || !decoded.OK || decoded.Error != "" {
		t.Fatalf("issuer_open response=%s err=%v", response, err)
	}
	var reference gateway.ReplicatedIssuerReference
	if decodeFixedHex(decoded.InstallationID, reference.Installation[:]) != nil ||
		decodeFixedHex(decoded.GrantDigest, reference.GrantDigest[:]) != nil ||
		decoded.IssuerEpoch == 0 {
		t.Fatalf("issuer_open reference=%+v", decoded)
	}
	reference.Epoch, reference.LaneOrdinal = decoded.IssuerEpoch, decoded.LaneOrdinal
	return reference, latency
}

func (client *durableRF3ExternalWireClient) assertPoint(
	t testing.TB,
	table, identifier string,
) time.Duration {
	t.Helper()
	raw, err := vibejson.Marshal(&serveRequest{Op: "query",
		SQL: "SELECT id, value FROM " + table + " WHERE id = ?", Class: "interactive",
		Params: []serveParam{{Kind: "string", Text: identifier}},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, latency := client.roundTrip(t, raw)
	document, err := vibejson.Parse(response)
	if err != nil {
		t.Fatalf("point response=%s err=%v", response, err)
	}
	if failure, found := document.Get("error"); found {
		message, _ := failure.Text()
		if message != "" {
			t.Fatalf("point response=%s", response)
		}
	}
	rows, found := document.Get("rows")
	if !found {
		t.Fatalf("point response has no rows: %s", response)
	}
	rowValues, ok := rows.Array()
	if !ok || len(rowValues) != 1 {
		t.Fatalf("point rows=%s", response)
	}
	cells, ok := rowValues[0].Array()
	if !ok || len(cells) != 2 {
		t.Fatalf("point cells=%s", response)
	}
	got, ok := cells[0].Text()
	if !ok || got != identifier {
		t.Fatalf("point id=%q want=%q response=%s", got, identifier, response)
	}
	return latency
}

func (client *durableRF3ExternalWireClient) close() {
	if client == nil {
		return
	}
	if client.connection != nil {
		_ = client.connection.Close()
	}
	if client.transport != nil {
		_ = client.transport.Close()
	}
	client.connection, client.transport = nil, nil
}

func durableRF3ExternalAckRequest(
	t testing.TB,
	raw []byte,
) durableExecBatchAckWireRequest {
	t.Helper()
	response := durableRF3ExternalExecResponse(t, raw)
	var request durableExecBatchAckWireRequest
	if !response.Committed || response.OutcomeUnknown || response.Error != "" ||
		decodeFixedHex(response.RequestID, request.Identity.RequestID[:]) != nil ||
		decodeFixedHex(response.RequestDigest, request.Identity.RequestDigest[:]) != nil ||
		decodeFixedHex(response.InstallationID, request.Identity.Reference.Installation[:]) != nil ||
		decodeFixedHex(response.GrantDigest, request.Identity.Reference.GrantDigest[:]) != nil ||
		decodeFixedHex(response.ResultDigest, request.ResultDigest[:]) != nil ||
		decodeFixedHex(response.AckToken, request.AckToken[:]) != nil {
		t.Fatalf("invalid durable terminal response=%s", raw)
	}
	request.Identity.Reference.Epoch = response.IssuerEpoch
	request.Identity.Reference.LaneOrdinal = response.LaneOrdinal
	request.Identity.IssuerSequence = response.IssuerSequence
	request.TerminalRevision = response.TerminalRevision
	if !validDurableExecBatchAckRequest(&request) {
		t.Fatalf("invalid durable ACK handle=%+v", request)
	}
	return request
}

func durableRF3ExternalAssertAckResponse(
	t testing.TB,
	raw []byte,
	want durableExecBatchAckWireRequest,
) {
	t.Helper()
	response := durableRF3ExternalExecResponse(t, raw)
	var echoed durableExecBatchAckWireRequest
	if !response.OK || response.Op != "ack_exec_batch" || response.Error != "" || response.Applied == 0 ||
		decodeFixedHex(response.RequestID, echoed.Identity.RequestID[:]) != nil ||
		decodeFixedHex(response.RequestDigest, echoed.Identity.RequestDigest[:]) != nil ||
		decodeFixedHex(response.InstallationID, echoed.Identity.Reference.Installation[:]) != nil ||
		decodeFixedHex(response.GrantDigest, echoed.Identity.Reference.GrantDigest[:]) != nil ||
		decodeFixedHex(response.ResultDigest, echoed.ResultDigest[:]) != nil ||
		decodeFixedHex(response.AckToken, echoed.AckToken[:]) != nil {
		t.Fatalf("invalid durable ACK response=%s", raw)
	}
	echoed.Identity.Reference.Epoch = response.IssuerEpoch
	echoed.Identity.Reference.LaneOrdinal = response.LaneOrdinal
	echoed.Identity.IssuerSequence = response.IssuerSequence
	echoed.TerminalRevision = response.TerminalRevision
	if echoed != want {
		t.Fatalf("ACK echo=%+v want=%+v", echoed, want)
	}
}

func durableRF3ExternalAssertAckRecord(
	t testing.TB,
	ack requestledger.AckRecord,
	key requestledger.RequestKey,
	want durableExecBatchAckWireRequest,
) {
	t.Helper()
	keyDigest, err := requestledger.KeyDigest(key)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Key != key || ack.KeyDigest != keyDigest ||
		ack.RequestDigest != requestledger.Digest(want.Identity.RequestDigest) ||
		ack.ResultDigest != requestledger.Digest(want.ResultDigest) ||
		ack.AckTokenDigest != requestledger.AckTokenDigest(want.AckToken) ||
		ack.TerminalRevision != want.TerminalRevision ||
		ack.GCPhase != requestledger.AckGCComplete || ack.PriorEncodedBytes == 0 ||
		ack.ReclaimedBytes != ack.PriorEncodedBytes || ack.TerminalResultBytes == 0 {
		t.Fatalf("authoritative ACK tombstone does not bind exact request: %+v", ack)
	}
}
