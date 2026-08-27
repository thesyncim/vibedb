//go:build linux

package main

import (
	"bufio"
	"context"
	"encoding/asn1"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

// TestGatewayHotShardMutationProcesses proves that the shipped durable
// exec_batch path, rather than a test-only pressure injection, can move a live
// RF3 shard. It uses two data groups, a distinct request-ledger/catalog group,
// and an enrolled cold target: ten shard processes plus the real gateway.
func TestGatewayHotShardMutationProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("external ten-process RF3 mutation qualification")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	root := t.TempDir()
	nodes := replicaProcessNodes()
	clusters := make([]*rf3testfixture.ProcessCluster, 3)
	for index := range clusters {
		var err error
		clusters[index], err = rf3testfixture.ReserveProcessCluster()
		if err != nil {
			t.Fatal(err)
		}
		defer clusters[index].Close()
	}
	gatewayAddresses, err := rf3testfixture.ReserveLoopbackAddresses(2)
	if err != nil {
		t.Fatal(err)
	}
	defer gatewayAddresses.Close()
	listeners := [3][4]rf3testfixture.ProcessListeners{}
	for group := range listeners {
		listeners[group] = clusters[group].Members()
	}

	clusterIdentity := hotMutationIdentities(0, "messages-data", "messages-all")
	credentials, roots, err := rf3testfixture.WriteCredentials(root,
		asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1},
		rafttransport.TrustDomain{ClusterID: clusterIdentity[0].ClusterID,
			ClusterIncarnation: clusterIdentity[0].ClusterIncarnation}, nodes[:])
	if err != nil {
		t.Fatal(err)
	}
	policyPath := filepath.Join(root, "authorization-policy.vibejson")
	if err = os.WriteFile(policyPath, replicaProcessPolicy(nodes), 0o600); err != nil {
		t.Fatal(err)
	}
	authorityProfile := sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: 5,
		ProtectionEpoch: 7, OwnershipEpoch: 11, SchemaGeneration: 13,
		RoutingVersion: 17, RouteGeneration: 19}
	wal := raftstore.Options{MaxFileBytes: 256 << 20,
		MaxRecordBytes: raftstore.DefaultMaxRecordBytes, MaxRecords: 4096,
		MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes}
	key := raftstore.Key{ID: "hot-mutation-key", Wrapped: []byte("test-wrapped-key")}
	for index := range key.Material {
		key.Material[index] = byte(index + 1)
	}
	apply := sqldriver.ReplicatedApplyOptions{MaxSessions: 64, RetryWindow: 16,
		TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024,
			MaxBytes: 384 << 20}, Placement: sqldriver.ReplicatedPlacementProfile{
			Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: "id",
			TupleVersion:  distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			Range:         distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		}}

	identities := [3][4]raftstore.Identity{
		hotMutationIdentities(0, "messages-data", "messages-all"),
		hotMutationIdentities(1, "logs-data", "logs-all"),
		hotMutationIdentities(2, string(gateway.ReplicatedCatalogDistribution),
			string(gateway.ReplicatedCatalogShard)),
	}
	tables := [3]string{"messages", "logs", gateway.ReplicatedCatalogTable}
	creates := [3]string{"CREATE TABLE messages (PRIMARY KEY (id))",
		"CREATE TABLE logs (PRIMARY KEY (id))",
		"CREATE TABLE controlplane (PRIMARY KEY (id))"}
	digests := [3]replication.Digest{}
	for group := range identities {
		probe, probeErr := rf3testfixture.PrepareMember(rf3testfixture.MemberOptions{
			Root: filepath.Join(root, fmt.Sprintf("probe-%d", group)), Table: tables[group],
			CreateTable: creates[group], Identity: identities[group][0], Key: key, WAL: wal,
			Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
			Authority: authorityProfile, Apply: apply,
		})
		if errors.Is(probeErr, raftstore.ErrPlatformUnsupported) {
			t.Skipf("strict durable allocation unsupported: %v", probeErr)
		}
		if probeErr != nil {
			t.Fatal(probeErr)
		}
		digests[group] = replication.Digest(probe.Base.RelationManifestDigest)
		if err = probe.Close(); err != nil {
			t.Fatal(err)
		}
	}

	routes := [3]gateway.ReplicatedRoute{}
	for group := range routes {
		routes[group] = hotMutationRoute(identities[group], listeners[group], nodes,
			authorityProfile, digests[group])
	}
	target := gateway.ReplicatedEndpoint{Member: 4, Node: nodes[3],
		StoreID: identities[0][3].StoreID, NodeIncarnation: 44,
		Endpoint: listeners[0][3].Peer, DataAddress: listeners[0][3].Peer,
		NativeEndpoint: listeners[0][3].Native, Address: listeners[0][3].Native,
		ControlEndpoint: listeners[0][3].Control, ControlAddress: listeners[0][3].Control}
	ackKey := gateway.DurableRequestAckDerivationKey{}
	for index := range ackKey {
		ackKey[index] = 0x3a
	}
	built, err := rf3testfixture.NewDurableCatalog(rf3testfixture.DurableCatalogOptions{
		Generation: 1, AckKey: ackKey, Groups: []rf3testfixture.DurableCatalogGroup{
			{Route: routes[0], Table: tables[0], PrimaryKey: "/id", Relation: 1,
				MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20, EnrolledTarget: &target},
			{Route: routes[1], Table: tables[1], PrimaryKey: "/id", Relation: 1,
				MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20},
			{Route: routes[2], Table: "request_ledger", PrimaryKey: "/id",
				LedgerRanges: []gateway.DurableRequestLedgerRangeDescriptor{{Identity: replication.Digest{0x91}}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	catalogPath := filepath.Join(root, "catalog.vibejson")
	if err = gateway.SaveSnapshot(catalogPath, built.Snapshot); err != nil {
		t.Fatal(err)
	}
	seed := replicaProcessCatalogSeed(t, built.Snapshot)
	prepared := [3][3]rf3testfixture.PreparedProcessMember{}
	var cold rf3testfixture.PreparedColdTarget
	for group := range prepared {
		var memberNodes [3]rafttransport.NodeID
		var peers [3]string
		copy(memberNodes[:], nodes[:3])
		for member := range peers {
			peers[member] = listeners[group][member].Peer
		}
		processTarget := &rf3testfixture.ProcessTarget{MemberID: 4, NodeID: nodes[3],
			StoreID: identities[0][3].StoreID, NodeIncarnation: 44, Listeners: listeners[0][3]}
		for member := range prepared[group] {
			options := rf3testfixture.ProcessMemberOptions{
				Root:  filepath.Join(root, fmt.Sprintf("group-%d-member-%d", group, member+1)),
				Table: tables[group], CreateTable: creates[group], Identity: identities[group][member],
				Key: key, WAL: wal, Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
				Authority: authorityProfile, Apply: apply, Listeners: listeners[group][member],
				Credential: credentials[member], Roots: roots, AuthorizationPolicy: policyPath,
				Nodes: memberNodes, PeerAddresses: peers,
			}
			if group == 0 {
				options.Target = processTarget
			}
			if group == 2 {
				options.SeedDocuments = seed
			}
			prepared[group][member], err = rf3testfixture.PrepareProcessMember(options)
			if err != nil {
				t.Fatal(err)
			}
		}
		if group == 0 {
			cold, err = rf3testfixture.PrepareColdProcessTarget(rf3testfixture.ProcessMemberOptions{
				Root: filepath.Join(root, "group-0-member-4"), Table: tables[0],
				CreateTable: creates[0], Identity: identities[0][3], Key: key, WAL: wal,
				Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
				Authority: authorityProfile, Apply: apply, Listeners: listeners[0][3],
				Credential: credentials[3], Roots: roots, AuthorizationPolicy: policyPath,
				Nodes: memberNodes, PeerAddresses: peers, Target: processTarget,
			}, nodes[0], listeners[0][0].Snapshot, 1<<30)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	shardBinary, gatewayBinary := filepath.Join(root, "vibedb-shard"), filepath.Join(root, "vibedb-gateway")
	replicaProcessBuild(t, ctx, shardBinary, "./cmd/vibedb-shard")
	replicaProcessBuild(t, ctx, gatewayBinary, "./cmd/vibedb-gateway")
	for _, cluster := range clusters {
		if err = cluster.ReleaseListeners(); err != nil {
			t.Fatal(err)
		}
	}
	if err = gatewayAddresses.Close(); err != nil {
		t.Fatal(err)
	}
	processes := make([]*rf3testfixture.ExternalProcess, 0, 10)
	for group := range prepared {
		for member := range prepared[group] {
			process := &rf3testfixture.ExternalProcess{Binary: shardBinary,
				Args: []string{"serve-rf3", "-manifest", prepared[group][member].ManifestPath}}
			if err = process.Start(); err != nil {
				t.Fatal(err)
			}
			defer replicaProcessStop(t, process)
			processes = append(processes, process)
		}
	}
	for _, process := range processes {
		if err = process.WaitReady(ctx, "vibedb-shard RF3 ready"); err != nil {
			t.Fatalf("voter readiness: %v\n%s", err, process.Diagnostics())
		}
	}
	coldProcess := &rf3testfixture.ExternalProcess{Binary: shardBinary,
		Args: []string{"bootstrap-rf3", "-manifest", cold.BootstrapManifestPath}}
	if err = coldProcess.Start(); err != nil {
		t.Fatal(err)
	}
	defer replicaProcessStop(t, coldProcess)
	if err = coldProcess.WaitReady(ctx, "vibedb-shard RF3 cold bootstrap ready"); err != nil {
		t.Fatalf("cold target readiness: %v\n%s", err, coldProcess.Diagnostics())
	}

	profile, err := servicetls.LoadProfile(credentials[4].Certificate, credentials[4].Key,
		roots, rf3testfixture.ProcessIdentityOID, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	catalogAuthority, closeAuthority := hotMutationCatalogAuthority(t, profile,
		built.Snapshot, filepath.Join(root, "grant-session"))
	defer closeAuthority()
	grant, err := gateway.BuildReplicaReplacementMembershipGrant(built.Snapshot,
		routes[0].Group, [16]byte{0x41}, 2, 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err = catalogAuthority.PublishMembershipGrant(ctx, grant); err != nil {
		t.Fatalf("publish membership grant: %v", err)
	}

	capacityPath := hotMutationCapacity(t, root)
	controlPath := filepath.Join(root, "replica-control.vibejson")
	if err = os.WriteFile(controlPath, hotMutationControlManifest(t, nodes, identities[0],
		listeners[0], credentials[4], roots, policyPath, gatewayAddresses.Addresses[1]), 0o600); err != nil {
		t.Fatal(err)
	}
	ackPath := filepath.Join(root, "durable-ack-key")
	if err = os.WriteFile(ackPath, []byte(strings.Repeat("3a", 32)), 0o600); err != nil {
		t.Fatal(err)
	}
	gatewayProcess := hotMutationGatewayProcess(gatewayBinary, catalogPath,
		gatewayAddresses.Addresses[0], controlPath, capacityPath, credentials[4], roots,
		policyPath, ackPath, filepath.Join(root, "gateway-session"), listeners, nodes)
	if err = gatewayProcess.Start(); err != nil {
		t.Fatal(err)
	}
	defer replicaProcessStop(t, gatewayProcess)
	if err = gatewayProcess.WaitReady(ctx, "vibedb-gateway serving catalog generation"); err != nil {
		t.Fatalf("gateway readiness: %v\n%s", err, gatewayProcess.Diagnostics())
	}
	baselineRSS := replicaProcessRSS(gatewayProcess.PID())
	connection := hotMutationDialGateway(t, profile, nodes[4], gatewayAddresses.Addresses[0])
	defer connection.Close()
	client := &hotMutationWireClient{connection: connection, reader: bufio.NewReader(connection)}
	reference := client.openIssuer(t)

	var latencies []time.Duration
	request := hotMutationRequest(t, reference, 1, []serveStatement{{
		SQL: `INSERT INTO messages VALUES (?)`, Params: []serveParam{{Kind: "document", Text: `{"id":"m-0","value":1}`}},
	}})
	latencies = append(latencies, client.execute(t, request), client.execute(t, request))
	latencies = append(latencies, client.execute(t, hotMutationRequest(t, reference, 2, []serveStatement{
		{SQL: `DELETE FROM messages WHERE id = ?`, Params: []serveParam{{Kind: "string", Text: "m-0"}}},
		{SQL: `INSERT INTO logs VALUES (?)`, Params: []serveParam{{Kind: "document", Text: `{"id":"l-1","value":2}`}}},
	})))
	latencies = append(latencies, client.execute(t, hotMutationRequest(t, reference, 3, []serveStatement{{
		SQL: `INSERT INTO messages VALUES (?)`, Params: []serveParam{{Kind: "document", Text: `{"id":"m-0","value":3}`}},
	}})))
	for sequence := uint64(4); sequence <= 7; sequence++ {
		statement := serveStatement{SQL: `DELETE FROM messages WHERE id = ?`,
			Params: []serveParam{{Kind: "string", Text: "m-0"}}}
		if sequence%2 != 0 {
			statement = serveStatement{SQL: `INSERT INTO messages VALUES (?)`,
				Params: []serveParam{{Kind: "document", Text: fmt.Sprintf(`{"id":"m-0","value":%d}`, sequence)}}}
		}
		latencies = append(latencies, client.execute(t, hotMutationRequest(t, reference, sequence,
			[]serveStatement{statement})))
	}
	sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
	p99 := latencies[(len(latencies)*99+99)/100-1]
	if p99 > 5*time.Second {
		t.Fatalf("foreground mutation p99=%s exceeds 5s", p99)
	}

	deadline := time.Now().Add(60 * time.Second)
	maximumOperations := 0
	for time.Now().Before(deadline) {
		ids, readErr := catalogAuthority.ReadOperationIDs(ctx)
		if readErr == nil {
			maximumOperations = max(maximumOperations, len(ids))
			if len(ids) == 1 {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if maximumOperations != 1 {
		t.Fatalf("write pressure admitted operations=%d want=1\n%s", maximumOperations,
			gatewayProcess.Diagnostics())
	}
	record, err := catalogAuthority.ReadPressureRecord(ctx)
	if err != nil || len(record.Payload) == 0 || len(record.Payload) > hotshard.MaxStaticCapacityBytes {
		t.Fatalf("pressure record bytes=%d err=%v", len(record.Payload), err)
	}
	view, err := hotshard.OpenView(record.Payload)
	if err != nil {
		t.Fatal(err)
	}
	var hotDemand, coldDemand uint64
	for _, report := range view.Reports {
		switch report.Recommendation.Source.Distribution {
		case routes[0].Distribution:
			hotDemand = report.Demand[autosplit.ResourceRequests]
		case routes[1].Distribution:
			coldDemand = report.Demand[autosplit.ResourceRequests]
		}
	}
	// Seven created admissions hit messages. The exact retry does not count;
	// the cross-shard admission contributes once to each participant.
	if hotDemand != 7 || coldDemand != 1 {
		t.Fatalf("participant pressure hot=%d cold=%d want=7,1", hotDemand, coldDemand)
	}
	var final *gateway.Snapshot
	for time.Now().Before(deadline) {
		candidate, readErr := catalogAuthority.Read(ctx)
		if readErr == nil && candidate.Generation() >= 2 {
			var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
			route, found := candidate.ResolveReplicatedRoute(routes[0].Distribution,
				routes[0].Shard, replicas[:0])
			if found && hotMutationRosterContains(route, 4) && !hotMutationRosterContains(route, 1) {
				final = candidate
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if final == nil || maximumOperations > 1 {
		t.Fatalf("automatic write-driven move incomplete operations=%d\n%s",
			maximumOperations, gatewayProcess.Diagnostics())
	}
	finalRSS := replicaProcessRSS(gatewayProcess.PID())
	if finalRSS > baselineRSS && finalRSS-baselineRSS > 128<<20 {
		t.Fatalf("gateway RSS growth=%d exceeds 128MiB", finalRSS-baselineRSS)
	}
	if client.requests > 16 || client.bytes > 1<<20 ||
		!replicaProcessTreeBounded(t, root, "gateway-session", 64, 64<<20) {
		t.Fatalf("foreground/state amplification requests=%d bytes=%d", client.requests, client.bytes)
	}
	t.Logf("write-driven hot move: p99=%s operations=%d pressure_bytes=%d foreground_requests=%d foreground_bytes=%d rss_growth=%d",
		p99, maximumOperations, len(record.Payload), client.requests, client.bytes,
		max(finalRSS, baselineRSS)-baselineRSS)
}

func hotMutationIdentities(ordinal byte, distributionName, shard string) (identities [4]raftstore.Identity) {
	identities = replicaProcessIdentities()
	for member := range identities {
		identities[member].Distribution, identities[member].Shard = distributionName, shard
		for index := range identities[member].ShardIncarnation {
			identities[member].ShardIncarnation[index] ^= ordinal*37 + 1
			identities[member].GroupID[index] ^= ordinal*53 + 1
			identities[member].StoreID[index] ^= ordinal*29 + 1
		}
	}
	return identities
}

func hotMutationRoute(identities [4]raftstore.Identity,
	listeners [4]rf3testfixture.ProcessListeners, nodes [5]rafttransport.NodeID,
	authority sqldriver.ReplicatedAuthorityProfile, digest replication.Digest,
) gateway.ReplicatedRoute {
	route := gateway.ReplicatedRoute{Distribution: distribution.DistributionName(identities[0].Distribution),
		Shard: distribution.ShardID(identities[0].Shard), Group: replicaProcessGroup(identities[0]),
		AllocationGeneration: identities[0].AllocationGeneration,
		Command: raftservice.CommandFence{ReplicaSetVersion: 1,
			ActivePolicyGeneration: authority.ActivePolicyGeneration, ProtectionEpoch: authority.ProtectionEpoch,
			OwnershipEpoch: authority.OwnershipEpoch, SchemaGeneration: authority.SchemaGeneration,
			RelationManifestDigest: digest, RoutingVersion: authority.RoutingVersion,
			RouteGeneration: authority.RouteGeneration},
		RangeIdentity:        replication.Digest{0x71, byte(identities[0].GroupID[0])},
		LineageDigest:        replication.Digest{0x72, byte(identities[0].GroupID[0])},
		ForwardingRuleDigest: replication.Digest{0x73, byte(identities[0].GroupID[0])}}
	for member := 0; member < 3; member++ {
		route.Replicas = append(route.Replicas, gateway.ReplicatedEndpoint{Member: uint64(member + 1),
			Node: nodes[member], StoreID: identities[member].StoreID, NodeIncarnation: uint64(41 + member),
			Endpoint: listeners[member].Peer, DataAddress: listeners[member].Peer,
			NativeEndpoint: listeners[member].Native, Address: listeners[member].Native,
			ControlEndpoint: listeners[member].Control, ControlAddress: listeners[member].Control})
	}
	return route
}

func hotMutationCatalogAuthority(t *testing.T, profile *rafttransport.PeerTLS,
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
		Path: journalPath, ClientID: replication.ID128{0xa1}, RetryHome: replication.RetryHome{0xb1},
		MaxCommandBytes: replication.MaxCommandBytes, Binding: binding})
	if err != nil {
		t.Fatal(err)
	}
	session, err := gateway.NewNativeSession(gateway.NativeSessionOptions{Executor: executor,
		Route: route, Distribution: string(route.Distribution), Shard: string(route.Shard),
		Tenant: []byte{1}, ClientID: replication.ID128{0xa1}, RetryHome: replication.RetryHome{0xb1},
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

func hotMutationCapacity(t *testing.T, root string) string {
	t.Helper()
	capacity := autosplit.CapacityVector{}
	for resource := range autosplit.ResourceCount {
		capacity[resource] = 2
	}
	config := hotshard.StaticCapacityConfig{Format: hotshard.StaticCapacityFormat,
		RecorderLanes: 8, WindowCapacity: capacity, NodeCapacity: capacity,
		MigrationCapacity: 1024, ShardMigrationBytes: 1, MaxReceives: 1}
	for group := 0; group < 3; group++ {
		for member := 1; member <= 3; member++ {
			config.Nodes = append(config.Nodes, hotshard.StaticCapacityNode{
				Endpoint:      distribution.EndpointID(fmt.Sprintf("fixture-g%d-m%d-data", group, member)),
				FailureDomain: uint32(member)})
		}
	}
	config.Nodes = append(config.Nodes, hotshard.StaticCapacityNode{
		Endpoint: "fixture-g0-target-data", FailureDomain: 4})
	raw, err := hotshard.AppendStaticCapacityConfig(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "hot-capacity.vibejson")
	if err = os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func hotMutationControlManifest(t *testing.T, nodes [5]rafttransport.NodeID,
	identities [4]raftstore.Identity, listeners [4]rf3testfixture.ProcessListeners,
	credential rf3testfixture.Credential, roots, policy, gatewayControl string,
) []byte {
	t.Helper()
	manifest := persistedGatewayReplicaControlManifest{Generation: 1,
		LocalGateway: persistedGatewayControlEndpoint{Node: fmt.Sprintf("%x", nodes[4]),
			Incarnation: 1, ControlAddress: gatewayControl},
		TLS: persistedGatewayReplicaTLS{Certificate: credential.Certificate, Key: credential.Key,
			Roots: roots, IdentityOID: rf3testfixture.ProcessIdentityOID, AuthorizationPolicy: policy},
		Bounds: persistedGatewayReplicaBounds{MaxConnections: 32, MaxHandshakes: 8,
			MaxConcurrentDrains: 4, ControllerInterval: 20, ReadTimeout: 2000, WriteTimeout: 5000},
		GatewayEndpoints: []persistedGatewayControlEndpoint{{Node: fmt.Sprintf("%x", nodes[4]),
			Incarnation: 1, ControlAddress: gatewayControl}},
		Candidates: []persistedGatewayReplacementCandidate{{Member: 4, Node: fmt.Sprintf("%x", nodes[3]),
			Store: fmt.Sprintf("%x", identities[3].StoreID), NodeIncarnation: 44,
			Endpoint: "fixture-g0-target-data"}}}
	for member := 0; member < 4; member++ {
		manifest.ShardEndpoints = append(manifest.ShardEndpoints, persistedGatewayShardControlEndpoint{
			Node: fmt.Sprintf("%x", nodes[member]), ControlAddress: listeners[member].Control})
	}
	raw, err := vibejson.Marshal(&manifest)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func hotMutationGatewayProcess(binary, catalog, listen, control, capacity string,
	credential rf3testfixture.Credential, roots, policy, ack, journal string,
	listeners [3][4]rf3testfixture.ProcessListeners, nodes [5]rafttransport.NodeID,
) *rf3testfixture.ExternalProcess {
	args := []string{"serve", "-catalog", catalog, "-catalog-relation", "1",
		"-catalog-session-journal", journal, "-durable-ack-key", ack,
		"-catalog-client-id", strings.Repeat("1a", 16), "-catalog-retry-home", strings.Repeat("2b", 8),
		"-catalog-attempts", "16", "-catalog-attempt-timeout", "2s", "-catalog-session-lease", "1h",
		"-controller-interval", "20ms", "-hot-shard-capacity", capacity, "-hot-shard-interval", "10s",
		"-listen", listen, "-tls-certificate", credential.Certificate, "-tls-key", credential.Key,
		"-tls-roots", roots, "-tls-identity-oid", rf3testfixture.ProcessIdentityOID,
		"-authorization-policy", policy, "-replica-control-manifest", control}
	for group := range listeners {
		for member := 0; member < 3; member++ {
			args = append(args, "-shard-peer", listeners[group][member].Native+"="+fmt.Sprintf("%x", nodes[member]))
		}
	}
	args = append(args, "-shard-peer", listeners[0][3].Native+"="+fmt.Sprintf("%x", nodes[3]))
	return &rf3testfixture.ExternalProcess{Binary: binary, Args: args}
}

type hotMutationWireClient struct {
	connection net.Conn
	reader     *bufio.Reader
	requests   uint64
	bytes      uint64
}

func hotMutationDialGateway(t *testing.T, profile *rafttransport.PeerTLS,
	node rafttransport.NodeID, address string,
) net.Conn {
	t.Helper()
	client, err := servicetls.NewClient(servicetls.ClientOptions{TLS: profile,
		Class:     rafttransport.TrafficGatewayClient,
		Endpoints: []servicetls.Endpoint{{Address: address, Node: node}},
		Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}, HandshakeDeadline: func() time.Time { return time.Now().Add(2 * time.Second) },
		MaxConnections: 1, MaxHandshakes: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		connection, dialErr := client.Dial(t.Context(), address)
		if dialErr == nil {
			return connection
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("gateway dial timeout")
	return nil
}

func (client *hotMutationWireClient) roundTrip(t *testing.T, request []byte) ([]byte, time.Duration) {
	t.Helper()
	if err := client.connection.SetDeadline(time.Now().Add(8 * time.Second)); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	request = append(request, '\n')
	written, err := client.connection.Write(request)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.reader.ReadBytes('\n')
	latency := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	client.requests++
	client.bytes += uint64(written + len(response))
	return response, latency
}

func (client *hotMutationWireClient) openIssuer(t *testing.T) gateway.ReplicatedIssuerReference {
	t.Helper()
	open := gateway.ReplicatedIssuerOpen{Installation: replication.ID128{0x81}, Epoch: 1}
	response, _ := client.roundTrip(t, sessionProtocolIssuerOpenRequest(t, open))
	var decoded struct {
		OK             bool   `json:"ok"`
		InstallationID string `json:"installation_id"`
		IssuerEpoch    uint64 `json:"issuer_epoch"`
		LaneOrdinal    uint16 `json:"lane_ordinal"`
		GrantDigest    string `json:"grant_digest"`
		Error          string `json:"error"`
	}
	if json.Unmarshal(response, &decoded) != nil || !decoded.OK || decoded.Error != "" {
		t.Fatalf("issuer_open response=%s", response)
	}
	installation, err := hex.DecodeString(decoded.InstallationID)
	if err != nil || len(installation) != 16 {
		t.Fatalf("issuer installation=%q", decoded.InstallationID)
	}
	grant, err := hex.DecodeString(decoded.GrantDigest)
	if err != nil || len(grant) != 32 {
		t.Fatalf("issuer grant=%q", decoded.GrantDigest)
	}
	var reference gateway.ReplicatedIssuerReference
	copy(reference.Installation[:], installation)
	copy(reference.GrantDigest[:], grant)
	reference.Epoch, reference.LaneOrdinal = decoded.IssuerEpoch, decoded.LaneOrdinal
	return reference
}

func (client *hotMutationWireClient) execute(t *testing.T, request []byte) time.Duration {
	t.Helper()
	response, latency := client.roundTrip(t, request)
	if !strings.Contains(string(response), `"committed":true`) ||
		strings.Contains(string(response), `"error"`) {
		t.Fatalf("exec_batch response=%s", response)
	}
	return latency
}

func hotMutationRequest(t *testing.T, reference gateway.ReplicatedIssuerReference,
	sequence uint64, statements []serveStatement,
) []byte {
	t.Helper()
	var requestID replication.ID128
	binary.LittleEndian.PutUint64(requestID[:8], sequence)
	requestID[15] = 0xa5
	raw, err := vibejson.Marshal(&serveRequest{Op: "exec_batch", RequestID: hex.EncodeToString(requestID[:]),
		InstallationID: hex.EncodeToString(reference.Installation[:]), IssuerEpoch: reference.Epoch,
		LaneOrdinal: reference.LaneOrdinal, GrantDigest: hex.EncodeToString(reference.GrantDigest[:]),
		IssuerSequence: sequence, Class: "interactive", Statements: statements})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func hotMutationRosterContains(route gateway.ReplicatedRoute, member uint64) bool {
	for _, replica := range route.Replicas {
		if replica.Member == member {
			return true
		}
	}
	return false
}
