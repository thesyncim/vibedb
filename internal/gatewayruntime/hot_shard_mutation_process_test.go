//go:build linux

package gatewayruntime

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	"github.com/thesyncim/vibedb/internal/topologyscheduler"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

// TestGatewayHotShardMutationProcesses proves that the shipped durable
// exec_batch path, rather than a test-only pressure injection, can move a live
// RF3 shard. It uses two data groups, a distinct request-ledger/catalog group,
// and an enrolled cold target: ten shard processes plus the real gateway.
func TestGatewayHotShardMutationProcesses(t *testing.T) {
	if os.Getenv("VIBEDB_HOT_SHARD_MUTATION_E2E") != "1" {
		t.Skip("set VIBEDB_HOT_SHARD_MUTATION_E2E=1 for external mutation qualification")
	}
	if testing.Short() {
		t.Skip("external ten-process RF3 mutation qualification")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	root := t.TempDir()
	nodes := replicaProcessNodes()
	groupNodes := [3][5]rafttransport.NodeID{nodes, nodes, nodes}
	allNodes := append([]rafttransport.NodeID(nil), nodes[:]...)
	credentialOffset := [3]int{0, 5, 8}
	for group := 1; group < len(groupNodes); group++ {
		for member := 0; member < 3; member++ {
			groupNodes[group][member][0] ^= byte(group * 0x40)
			allNodes = append(allNodes, groupNodes[group][member])
		}
	}
	userNode := nodes[4]
	userNode[15] ^= 0x80
	allNodes = append(allNodes, userNode)
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
			ClusterIncarnation: clusterIdentity[0].ClusterIncarnation}, allNodes)
	if err != nil {
		t.Fatal(err)
	}
	userCredential := credentials[len(credentials)-1]
	userProfile, err := servicetls.LoadProfile(userCredential.Certificate, userCredential.Key,
		roots, rf3testfixture.ProcessIdentityOID, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	policyPath := filepath.Join(root, "authorization-policy.vibejson")
	policyNodes := append([]rafttransport.NodeID(nil), allNodes...)
	sort.Slice(policyNodes, func(i, j int) bool { return bytes.Compare(policyNodes[i][:], policyNodes[j][:]) < 0 })
	if err = os.WriteFile(policyPath, durableRF3ExternalPolicy(t, policyNodes), 0o600); err != nil {
		t.Fatal(err)
	}
	authorityProfile := sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: 5,
		ProtectionEpoch: 7, OwnershipEpoch: 11, SchemaGeneration: 13,
		RoutingVersion: 17, RouteGeneration: 1}
	wal := rf3testfixture.DurableGatewayWALOptions()
	// The catalog retains both request continuations and every split tail-step
	// witness. Provision its fixed log for that workload before any process
	// starts; data-shard and final growth limits remain unchanged.
	catalogWAL := wal
	catalogWAL.MaxFileBytes += 56 << 20
	catalogWAL.MaxLiveBytes += 56 << 20
	key := raftstore.Key{ID: "hot-mutation-key", Wrapped: []byte("test-wrapped-key")}
	for index := range key.Material {
		key.Material[index] = byte(index + 1)
	}
	apply := sqldriver.ReplicatedApplyOptions{MaxSessions: 64, RetryWindow: 16,
		TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024,
			MaxBytes: 384 << 20}, Placement: sqldriver.ReplicatedPlacementProfile{
			Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: "/id",
			TupleVersion:  distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			Range:         distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		}}
	groupApply := [3]sqldriver.ReplicatedApplyOptions{apply, apply, apply}
	groupApply[2].RequestLedgerCapacityBytes = 64 << 20
	groupApply[2].RequestLedgerCleanupReserveBytes = 8 << 20
	groupApply[2].RequestLedgerRangeIdentity = replication.Digest{0x91}

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
	schemas := [3][]string{
		{`CREATE INDEX by_kind ON messages (kind)`},
		{`CREATE TABLE messages_email (PRIMARY KEY (key))`},
		nil,
	}
	globalIndexes := [3][]sqldriver.ReplicatedGlobalIndexRelation{
		nil,
		{{Relation: 2, Table: "messages_email", IndexID: 41, Incarnation: 7,
			LocatorCount: 1, Unique: true,
			KeyEncoding: sqldriver.ReplicatedRelationKeyCanonicalTuple, KeyArity: 1,
			TupleVersion:  distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			BucketBits:    distribution.DefaultVirtualBucketBits}},
		nil,
	}
	digests := [3]replication.Digest{}
	logicalDigests := [3]replication.Digest{}
	for group := range identities {
		probe, probeErr := rf3testfixture.PrepareMember(rf3testfixture.MemberOptions{
			Root: t.TempDir(), Table: tables[group],
			CreateTable: creates[group], Identity: identities[group][0], Key: key, WAL: wal,
			Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
			Authority: authorityProfile, Apply: groupApply[group], SchemaStatements: schemas[group],
			GlobalIndexes: globalIndexes[group],
		})
		if errors.Is(probeErr, raftstore.ErrPlatformUnsupported) {
			t.Skipf("strict durable allocation unsupported: %v", probeErr)
		}
		if probeErr != nil {
			t.Fatal(probeErr)
		}
		machineDigest, digestErr := probe.Apply.RangeSplitRelationManifestDigest()
		if digestErr != nil {
			t.Fatal(digestErr)
		}
		logicalDigest, digestErr := sqldriver.ReplicatedRelationManifestDigest(probe.Base)
		if digestErr != nil {
			t.Fatal(digestErr)
		}
		digests[group] = replication.Digest(machineDigest)
		logicalDigests[group] = replication.Digest(logicalDigest)
		if err = probe.Close(); err != nil {
			t.Fatal(err)
		}
	}

	routes := [3]gateway.ReplicatedRoute{}
	for group := range routes {
		routes[group] = hotMutationRoute(identities[group], listeners[group], groupNodes[group],
			authorityProfile, digests[group])
		routes[group].LogicalSchemaDigest = logicalDigests[group]
	}
	target := gateway.ReplicatedEndpoint{Member: 4, Node: nodes[3],
		StoreID: identities[0][3].StoreID, NodeIncarnation: 1,
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
				MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20,
				AdditionalTables: []rf3testfixture.DurableCatalogTable{{
					Table: "messages_email", PrimaryKey: "/email", Relation: 2,
					MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20,
				}}},
			{Route: routes[2], Table: "request_ledger", PrimaryKey: "/id",
				LedgerRanges: []gateway.DurableRequestLedgerRangeDescriptor{{Identity: replication.Digest{0x91}}}},
		},
		Indexes: []gateway.IndexDescriptor{
			{IndexID: 40, Incarnation: 1, Table: "messages", Name: "by_kind",
				Paths: []string{"/kind"}, Flags: gateway.IndexLocal | gateway.IndexOrdered,
				Lifecycle: gateway.IndexReady},
			{IndexID: 41, Incarnation: 7, Table: "messages", Name: "by_email",
				Relation: "messages_email", Paths: []string{"/email"},
				LocatorPaths: []string{"/id"}, PrimaryPath: "/id",
				Flags:     gateway.IndexGlobal | gateway.IndexUnique | gateway.IndexOrdered,
				Lifecycle: gateway.IndexReady},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	catalogPath := filepath.Join(root, "catalog.vibejson")
	if err = gateway.SaveSnapshot(catalogPath, built.Snapshot); err != nil {
		t.Fatal(err)
	}
	prepared := [3][3]rf3testfixture.PreparedProcessMember{}
	var cold rf3testfixture.PreparedColdTarget
	for group := range prepared {
		var memberNodes [3]rafttransport.NodeID
		var peers [3]string
		copy(memberNodes[:], groupNodes[group][:3])
		for member := range peers {
			peers[member] = listeners[group][member].Peer
		}
		processTarget := &rf3testfixture.ProcessTarget{MemberID: 4, NodeID: nodes[3],
			StoreID: identities[0][3].StoreID, NodeIncarnation: 1, Listeners: listeners[0][3]}
		for member := range prepared[group] {
			options := rf3testfixture.ProcessMemberOptions{
				Root:  filepath.Join(root, fmt.Sprintf("group-%d-member-%d", group, member+1)),
				Table: tables[group], CreateTable: creates[group], Identity: identities[group][member],
				Key: key, WAL: wal, Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
				Authority: authorityProfile, Apply: groupApply[group], Listeners: listeners[group][member],
				Credential: credentials[credentialOffset[group]+member], Roots: roots, AuthorizationPolicy: policyPath,
				Nodes: memberNodes, PeerAddresses: peers, SchemaStatements: schemas[group],
				GlobalIndexes: globalIndexes[group],
			}
			if group == 0 {
				options.Target = processTarget
			}
			if group == 2 {
				options.WAL = catalogWAL
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
				SchemaStatements: schemas[0],
			}, nodes[1], listeners[0][1].Snapshot, 1<<30)
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
	defer func() {
		if t.Failed() {
			t.Logf("cold target diagnostics:\n%s", coldProcess.Diagnostics())
			for index, process := range processes {
				t.Logf("voter %d diagnostics:\n%s", index, process.Diagnostics())
			}
		}
	}()
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
	if err = catalogAuthority.Publish(ctx, 0, built.Snapshot); err != nil {
		t.Fatalf("publish immutable catalog genesis through RF3: %v", err)
	}
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
	if err = os.WriteFile(controlPath, hotMutationControlManifest(t, root, groupNodes, identities[0],
		listeners, credentials[4], roots, policyPath, gatewayAddresses.Addresses[1], built.Snapshot, apply), 0o600); err != nil {
		t.Fatal(err)
	}
	ackPath := filepath.Join(root, "durable-ack-key")
	if err = os.WriteFile(ackPath, []byte(strings.Repeat("3a", 32)), 0o600); err != nil {
		t.Fatal(err)
	}
	gatewayProcess := hotMutationGatewayProcess(gatewayBinary, catalogPath,
		gatewayAddresses.Addresses[0], controlPath, capacityPath, credentials[4], roots,
		policyPath, ackPath, filepath.Join(root, "gateway-session"), listeners, groupNodes)
	if err = gatewayProcess.Start(); err != nil {
		t.Fatal(err)
	}
	defer replicaProcessStop(t, gatewayProcess)
	defer func() {
		if t.Failed() {
			t.Logf("gateway diagnostics:\n%s", gatewayProcess.Diagnostics())
		}
	}()
	if err = gatewayProcess.WaitReady(ctx, "vibedb-gateway serving catalog generation"); err != nil {
		t.Fatalf("gateway readiness: %v\n%s", err, gatewayProcess.Diagnostics())
	}
	baselineRSS := replicaProcessRSS(gatewayProcess.PID())
	baselineStorage := replicaProcessAllocatedBytes(root, "")
	baselineSnapshotNetwork := replicaProcessSnapshotPayloadBytes(root)
	connection := hotMutationDialGateway(t, userProfile, nodes[4], gatewayAddresses.Addresses[0])
	defer connection.Close()
	client := &hotMutationWireClient{connection: connection, reader: bufio.NewReader(connection)}
	client.verifier = newHotMutationVerifier(t, userProfile, built.Snapshot, catalogAuthority, routes[0], routes[1])
	reference := client.openIssuer(t)

	var latencies []time.Duration
	request := hotMutationRequest(t, reference, 1, []serveStatement{{
		SQL: `INSERT INTO messages VALUES (?)`, Params: []serveParam{{Kind: "document", Text: `{"id":"m-0","kind":"initial","email":"initial@example.com","value":1}`}},
	}})
	latencies = append(latencies, client.execute(t, request), client.execute(t, request))
	hotMutationAssertMessage(t, client, "initial", "initial@example.com", 1)

	// Submit an index-changing replacement, kill the exact remote-index RF3
	// leader while apply may be in flight, and sever the client response path.
	// The byte-identical retry must resolve the outcome through the shipped
	// request ledger without duplicating base, local, or global maintenance.
	leader := hotMutationLeader(t, profile, routes[1])
	faultRequest := hotMutationRequest(t, reference, 2, []serveStatement{{
		SQL: `UPDATE messages SET "$doc" = ? WHERE id = ?`, Params: []serveParam{
			{Kind: "document", Text: `{"id":"m-0","kind":"changed","email":"changed@example.com","value":2}`},
			{Kind: "string", Text: "m-0"},
		},
	}})
	client.partitionAfterWrite(t, faultRequest, func() {
		time.Sleep(10 * time.Millisecond)
		fault := processes[3+int(leader)-1]
		if killErr := fault.Kill(ctx); killErr != nil {
			t.Fatalf("kill global-index leader %d: %v", leader, killErr)
		}
	})
	client.reconnect(t, userProfile, nodes[4], gatewayAddresses.Addresses[0])
	latencies = append(latencies, client.execute(t, faultRequest))
	hotMutationAssertMessage(t, client, "changed", "changed@example.com", 2)
	hotMutationAssertSelectorEmpty(t, client, "kind", "initial")
	hotMutationAssertSelectorEmpty(t, client, "email", "initial@example.com")

	latencies = append(latencies, client.execute(t, hotMutationRequest(t, reference, 3, []serveStatement{
		{SQL: `DELETE FROM messages WHERE id = ?`, Params: []serveParam{{Kind: "string", Text: "m-0"}}},
		{SQL: `INSERT INTO logs VALUES (?)`, Params: []serveParam{{Kind: "document", Text: `{"id":"l-1","value":3}`}}},
	})))
	hotMutationAssertSelectorEmpty(t, client, "id", "m-0")
	hotMutationAssertSelectorEmpty(t, client, "kind", "changed")
	hotMutationAssertSelectorEmpty(t, client, "email", "changed@example.com")
	latencies = append(latencies, client.execute(t, hotMutationRequest(t, reference, 4, []serveStatement{{
		SQL: `INSERT INTO messages VALUES (?)`, Params: []serveParam{{Kind: "document", Text: `{"id":"m-0","kind":"steady","email":"steady@example.com","value":4}`}},
	}})))
	for sequence := uint64(5); sequence <= 6; sequence++ {
		statement := serveStatement{SQL: `DELETE FROM messages WHERE id = ?`,
			Params: []serveParam{{Kind: "string", Text: "m-0"}}}
		if sequence%2 == 0 {
			statement = serveStatement{SQL: `INSERT INTO messages VALUES (?)`,
				Params: []serveParam{{Kind: "document", Text: fmt.Sprintf(`{"id":"m-0","kind":"steady","email":"steady@example.com","value":%d}`, sequence)}}}
		}
		latencies = append(latencies, client.execute(t, hotMutationRequest(t, reference, sequence,
			[]serveStatement{statement})))
	}
	latencies = append(latencies, client.execute(t, hotMutationRequest(t, reference, 7, []serveStatement{{
		SQL: `UPDATE messages SET "$doc" = ? WHERE id = ?`, Params: []serveParam{
			{Kind: "document", Text: `{"id":"m-0","kind":"final","email":"final@example.com","value":7}`},
			{Kind: "string", Text: "m-0"},
		},
	}})))
	hotMutationAssertMessage(t, client, "final", "final@example.com", 7)
	hotMutationAssertSelectorEmpty(t, client, "kind", "steady")
	hotMutationAssertSelectorEmpty(t, client, "email", "steady@example.com")

	sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
	p99 := latencies[(len(latencies)*99+99)/100-1]
	if p99 > 5*time.Second {
		t.Fatalf("foreground mutation p99=%s exceeds 5s", p99)
	}
	t.Logf("foreground mutations and exact retries passed: p99=%s", p99)

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
		pressure, pressureErr := catalogAuthority.ReadPressureRecord(ctx)
		t.Logf("pressure before admission: %s err=%v", pressure.Payload, pressureErr)
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
	// Seven created admissions hit the base group and the remote global-index
	// group. Exact retries do not count, and the cross-table batch still emits
	// one sample for the already-created index/log participant.
	if hotDemand != 7 || coldDemand != 7 {
		t.Fatalf("participant pressure base=%d remote-index=%d want=7,7", hotDemand, coldDemand)
	}
	var final *gateway.Snapshot
	for time.Now().Before(deadline) {
		candidate, readErr := catalogAuthority.Read(ctx)
		if readErr == nil && candidate.Generation() >= 3 {
			var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
			route, found := candidate.ResolveReplicatedRoute(routes[0].Distribution,
				routes[0].Shard, replicas[:0])
			if found && hotMutationRosterContains(route, 4) && !hotMutationRosterContains(route, 1) {
				ids, operationErr := catalogAuthority.ReadOperationIDs(ctx)
				if operationErr == nil {
					maximumOperations = max(maximumOperations, len(ids))
					if len(ids) == 0 {
						final = candidate
						break
					}
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if final == nil || maximumOperations > 1 {
		if ids, readErr := catalogAuthority.ReadOperationIDs(ctx); readErr == nil {
			for _, id := range ids {
				operation, operationErr := catalogAuthority.ReadOperation(ctx, id)
				t.Logf("incomplete move state=%d revision=%d catalog=%d cursor=%v execution-revision=%d settled=%t err=%v",
					operation.State, operation.Revision, operation.CatalogGeneration, operation.Cursor,
					operation.ExecutionRevision, operation.ExecutionSettled, operationErr)
			}
		}
		t.Fatalf("automatic write-driven move incomplete operations=%d\n%s",
			maximumOperations, gatewayProcess.Diagnostics())
	}
	terminalOperations, err := catalogAuthority.ReadOperationIDs(ctx)
	if err != nil || len(terminalOperations) != 0 {
		t.Fatalf("topology operation amplification=%d err=%v", len(terminalOperations), err)
	}
	t.Logf("replacement completed and operation collected at catalog generation %d", final.Generation())

	// Reopen both the killed remote-index voter and the complete shipped
	// gateway. Reads then traverse newly opened retained identities and prove
	// the base/local/global state stayed atomically aligned across the fault.
	faultProcess := processes[3+int(leader)-1]
	if err = faultProcess.Start(); err != nil {
		t.Fatalf("restart global-index voter %d: %v", leader, err)
	}
	if err = faultProcess.WaitReady(ctx, "vibedb-shard RF3 ready"); err != nil {
		t.Fatalf("reopened global-index voter: %v\n%s", err, faultProcess.Diagnostics())
	}
	_ = client.connection.Close()
	restartCtx, restartCancel := context.WithTimeout(ctx, 15*time.Second)
	if err = gatewayProcess.Stop(restartCtx); err != nil {
		restartCancel()
		t.Fatalf("stop gateway for reopen: %v", err)
	}
	restartCancel()
	hotMutationRefreshSplitVoters(t, ctx, root, final, groupNodes[0], listeners[0], prepared[0], cold, processes[:3], coldProcess)
	// Split roots are an explicit operator inventory. Refresh it to the
	// certified replacement roster before reopening the gateway for the
	// split phase; the retired source must not authorize child placement.
	if err = os.WriteFile(controlPath, hotMutationControlManifest(t, root, groupNodes, identities[0],
		listeners, credentials[4], roots, policyPath, gatewayAddresses.Addresses[1], final, apply), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = gatewayProcess.Start(); err != nil {
		t.Fatalf("restart gateway: %v", err)
	}
	if err = gatewayProcess.WaitReady(ctx, fmt.Sprintf(
		"vibedb-gateway serving catalog generation %d", final.Generation())); err != nil {
		t.Fatalf("reopened gateway: %v\n%s", err, gatewayProcess.Diagnostics())
	}
	client.reconnect(t, userProfile, nodes[4], gatewayAddresses.Addresses[0])
	reopenedRoute, ok := final.ResolveReplicatedRoute(routes[0].Distribution, routes[0].Shard,
		make([]gateway.ReplicatedEndpoint, 0, gateway.ServingReplicaCount))
	if !ok || hotMutationLeader(t, profile, reopenedRoute) == 0 {
		t.Fatal("reopened source has no elected leader")
	}
	hotMutationAssertMessage(t, client, "final", "final@example.com", 7)
	hotMutationAssertSelectorEmpty(t, client, "kind", "steady")
	hotMutationAssertSelectorEmpty(t, client, "email", "steady@example.com")

	// Admit a real binary range split through the same prepared-child RPC and
	// replicated operation journal used by the shipped hot-shard controller.
	// The pressure planner is tested independently; this phase fixes the exact
	// topology cut so the external fault gate is deterministic.
	splitPlan := hotMutationAdmitExactSplit(
		t, ctx, controlPath, nodes[4], catalogAuthority, final, routes[0],
	)
	if splitPlan == nil {
		t.Fatal("exact split admission returned no plan")
	}
	t.Log("split admitted on the reopened replacement roster")
	hotMutationWaitSplitRevision(t, ctx, catalogAuthority, [32]byte(splitPlan.OperationID()), 4)
	t.Log("split capture/artifact execution started")
	var splitLatencies []time.Duration
	for sequence := uint64(8); sequence <= 9; sequence++ {
		splitLatencies = append(splitLatencies, client.execute(t, hotMutationRequest(t, reference, sequence,
			[]serveStatement{{SQL: `UPDATE messages SET "$doc" = ? WHERE id = ?`, Params: []serveParam{
				{Kind: "document", Text: fmt.Sprintf(`{"id":"m-0","kind":"splitting","email":"split@example.com","value":%d}`, sequence)},
				{Kind: "string", Text: "m-0"},
			}}})))
	}
	servingRoute, found := final.ResolveReplicatedRoute(
		routes[0].Distribution, routes[0].Shard, make([]gateway.ReplicatedEndpoint, 0, gateway.ServingReplicaCount),
	)
	if !found {
		t.Fatal("published source route missing before split fault")
	}
	splitLeader := hotMutationLeader(t, profile, servingRoute)
	var splitLeaderProcess *rf3testfixture.ExternalProcess
	switch splitLeader {
	case 1, 2, 3:
		splitLeaderProcess = processes[int(splitLeader)-1]
	case 4:
		splitLeaderProcess = coldProcess
	default:
		t.Fatalf("split source leader=%d", splitLeader)
	}
	partitionMember := uint64(0)
	var partitionProcess *rf3testfixture.ExternalProcess
	for _, replica := range servingRoute.Replicas {
		if replica.Member == splitLeader {
			continue
		}
		partitionMember = replica.Member
		if replica.Member == 4 {
			partitionProcess = coldProcess
		} else {
			partitionProcess = processes[int(replica.Member)-1]
		}
		break
	}
	if partitionProcess == nil || partitionProcess.PID() == 0 {
		t.Fatal("no live source follower available for split partition")
	}
	if signalErr := syscall.Kill(partitionProcess.PID(), syscall.SIGSTOP); signalErr != nil {
		t.Fatalf("partition split follower %d: %v", partitionMember, signalErr)
	}
	if err = splitLeaderProcess.Kill(ctx); err != nil {
		t.Fatalf("kill split source leader %d: %v", splitLeader, err)
	}
	if err = splitLeaderProcess.Start(); err != nil {
		t.Fatalf("restart split source leader %d: %v", splitLeader, err)
	}
	time.Sleep(100 * time.Millisecond)
	if signalErr := syscall.Kill(partitionProcess.PID(), syscall.SIGCONT); signalErr != nil {
		t.Fatalf("heal split follower %d: %v", partitionMember, signalErr)
	}
	if err = splitLeaderProcess.WaitReady(ctx, "vibedb-shard RF3"); err != nil {
		t.Fatalf("restarted split source leader %d: %v\n%s", splitLeader, err, splitLeaderProcess.Diagnostics())
	}
	for sequence := uint64(10); sequence <= 11; sequence++ {
		splitLatencies = append(splitLatencies, client.execute(t, hotMutationRequest(t, reference, sequence,
			[]serveStatement{{SQL: `UPDATE messages SET "$doc" = ? WHERE id = ?`, Params: []serveParam{
				{Kind: "document", Text: fmt.Sprintf(`{"id":"m-0","kind":"splitting","email":"split@example.com","value":%d}`, sequence)},
				{Kind: "string", Text: "m-0"},
			}}})))
	}
	splitCatalog := hotMutationWaitSplitComplete(
		t, ctx, catalogAuthority, final.Generation()+1, [32]byte(splitPlan.OperationID()), routes[0], gatewayProcess,
	)
	hotMutationAssertStaleParentRefused(t, profile, servingRoute, splitCatalog)
	for _, descriptor := range splitCatalog.ReplicatedShardDescriptors() {
		if descriptor.Distribution != routes[0].Distribution || descriptor.Shard == routes[0].Shard {
			continue
		}
		childRoute, ok := splitCatalog.ResolveReplicatedRoute(
			descriptor.Distribution, descriptor.Shard,
			make([]gateway.ReplicatedEndpoint, 0, gateway.ServingReplicaCount),
		)
		if !ok || hotMutationLeader(t, profile, childRoute) == 0 {
			t.Fatalf("split child %q did not serve RF3", descriptor.Shard)
		}
	}
	sort.Slice(splitLatencies, func(left, right int) bool { return splitLatencies[left] < splitLatencies[right] })
	splitP99 := splitLatencies[(len(splitLatencies)*99+99)/100-1]
	if splitP99 > 5*time.Second {
		t.Fatalf("split-under-write p99=%s exceeds 5s", splitP99)
	}

	finalRSS := replicaProcessRSS(gatewayProcess.PID())
	finalStorage := replicaProcessAllocatedBytes(root, "")
	finalSnapshotNetwork := replicaProcessSnapshotPayloadBytes(root)
	storageGrowth := uint64(0)
	if finalStorage > baselineStorage {
		storageGrowth = finalStorage - baselineStorage
	}
	if finalRSS > baselineRSS && finalRSS-baselineRSS > 128<<20 {
		t.Fatalf("gateway RSS growth=%d exceeds 128MiB", finalRSS-baselineRSS)
	}
	snapshotNetworkGrowth := uint64(0)
	if finalSnapshotNetwork > baselineSnapshotNetwork {
		snapshotNetworkGrowth = finalSnapshotNetwork - baselineSnapshotNetwork
	}
	// The cold replacement and the three independent child replicas each
	// allocate one strictly reserved WAL and one 32 MiB checkpoint. These
	// admitted fixed costs alone exceed the old move-only 512 MiB budget.
	// Bound all additional growth separately, including artifacts/journals.
	plannedStorageGrowth := uint64(gateway.ServingReplicaCount+1) * (uint64(wal.MaxFileBytes) + 32<<20)
	if client.requests > 48 || client.bytes > 1<<20 || storageGrowth > plannedStorageGrowth+64<<20 ||
		snapshotNetworkGrowth > 1<<30 ||
		!replicaProcessTreeBounded(t, root, "gateway-session", 64, 64<<20) {
		t.Fatalf("foreground/state amplification requests=%d bytes=%d storage_growth=%d planned_storage_growth=%d snapshot_network_growth=%d",
			client.requests, client.bytes, storageGrowth, plannedStorageGrowth, snapshotNetworkGrowth)
	}
	hotMutationVerifyFinalStores(t, root, splitCatalog, splitPlan, client, processes, coldProcess, gatewayProcess)
	t.Logf("write-driven hot move: split=true atomic_relation_index_visibility=true leader_kill=true source_partition=true response_partition=true reopen=true p99=%s split_p99=%s operations=%d pressure_bytes=%d foreground_requests=%d foreground_bytes=%d rss_growth=%d storage_growth=%d planned_storage_growth=%d snapshot_network_growth=%d",
		p99, splitP99, maximumOperations, len(record.Payload), client.requests, client.bytes,
		max(finalRSS, baselineRSS)-baselineRSS, storageGrowth, plannedStorageGrowth, snapshotNetworkGrowth)
}

// The split phase provisions new children on the replacement roster. Preserve
// each source's durable WAL and SQL identities, and replace only the explicit
// transport/child enrollment and the unused index-one child bootstrap.
func hotMutationRefreshSplitVoters(t *testing.T, ctx context.Context, root string, catalog *gateway.Snapshot,
	nodes [5]rafttransport.NodeID, listeners [4]rf3testfixture.ProcessListeners,
	prepared [3]rf3testfixture.PreparedProcessMember, cold rf3testfixture.PreparedColdTarget,
	processes []*rf3testfixture.ExternalProcess, coldProcess *rf3testfixture.ExternalProcess) {
	t.Helper()
	var roster []gateway.ReplicatedReplicaDescriptor
	for _, descriptor := range catalog.ReplicatedShardDescriptors() {
		if descriptor.Distribution == "messages-data" {
			roster = descriptor.Replicas
		}
	}
	sort.Slice(roster, func(i, j int) bool { return roster[i].Member < roster[j].Member })
	if len(roster) != 3 || roster[0].Member != 2 || roster[1].Member != 3 || roster[2].Member != 4 {
		t.Fatalf("uncertified split voter roster: %+v", roster)
	}
	memberList := func(members []uint64) []byte {
		raw := []byte(`"members":[`)
		for index, member := range members {
			if index != 0 {
				raw = append(raw, ',')
			}
			raw = fmt.Appendf(raw, `{"member_id":%d,"node_id":"%x","peer_address":%q}`, member, nodes[member-1], listeners[member-1].Peer)
		}
		return append(raw, ']')
	}
	oldMembers, newMembers := memberList([]uint64{1, 2, 3}), memberList([]uint64{2, 3, 4})
	serving := []*rf3testfixture.ExternalProcess{processes[1], processes[2], coldProcess}
	for _, process := range serving {
		replicaProcessStop(t, process)
	}
	for _, replica := range roster {
		member := replica.Member
		manifestPath := cold.ManifestPath
		if member != 4 {
			manifestPath = prepared[member-1].ManifestPath
		}
		raw, err := os.ReadFile(manifestPath)
		if err != nil || bytes.Count(raw, oldMembers) != 2 {
			t.Fatalf("split enrollment %s: %v", manifestPath, err)
		}
		// These inventories are bound to the complete startup manifest. The
		// operator is provisioning the first split, so only provably empty
		// inventories may be archived before enrolling the replacement roster.
		var manifest struct {
			Route struct {
				MembershipGrantPath string `json:"membership_grant_path"`
			} `json:"route"`
			ReplicaControl struct {
				SourceDataRoot string `json:"source_data_root"`
			} `json:"replica_control"`
		}
		if err := json.Unmarshal(raw, &manifest); err != nil {
			t.Fatal(err)
		}
		// The completed move's grant names the retired member, which is no
		// longer enrolled by this new operator configuration. Preserve it as
		// evidence; the final catalog and WAL now own the stable voter cut.
		grantRaw, err := os.ReadFile(manifest.Route.MembershipGrantPath)
		if err != nil || len(grantRaw) != membershipgrant.CanonicalGrantBytes+32 {
			t.Fatalf("completed grant: %v", err)
		}
		completedGrant, err := membershipgrant.OpenCanonical(grantRaw[:membershipgrant.CanonicalGrantBytes])
		grantDigest := completedGrant.Digest()
		if err != nil || completedGrant.SourceMember != 1 || completedGrant.TargetMember != 4 ||
			completedGrant.CatalogGeneration+2 != catalog.Generation() ||
			!bytes.Equal(grantRaw[membershipgrant.CanonicalGrantBytes:], grantDigest[:]) {
			t.Fatalf("completed replacement grant does not match split enrollment: %v", err)
		}
		if err := os.Rename(manifest.Route.MembershipGrantPath, manifest.Route.MembershipGrantPath+".before-split-enrollment"); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		for _, inventory := range []struct {
			name, magic  string
			digestOffset int
		}{
			{"adopted-groups.state", "VDBLIVEG", 8},
			{"child-preparations.state", "VDBCHADM", 16},
		} {
			path := filepath.Join(manifest.ReplicaControl.SourceDataRoot, inventory.name)
			state, err := os.ReadFile(path)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil || len(state) < 96 || string(state[:8]) != inventory.magic ||
				!bytes.Equal(state[inventory.digestOffset:inventory.digestOffset+32], digest[:]) ||
				sha256.Sum256(state[:len(state)-32]) != [32]byte(state[len(state)-32:]) ||
				!bytes.Equal(state[64:len(state)-32], make([]byte, len(state)-96)) {
				t.Fatalf("cannot reprovision nonempty or invalid split inventory %s: %v", path, err)
			}
			if err := os.Rename(path, path+".before-split-enrollment"); err != nil {
				t.Fatal(err)
			}
		}
		raw = bytes.ReplaceAll(raw, oldMembers, newMembers)
		// The fixture's default action grants cover shard peers only. The
		// shipped gateway is a distinct authenticated principal and must be
		// explicitly enrolled to drive this split's exact action grants.
		grantEnd := []byte(`],"child_registry":`)
		if bytes.Count(raw, grantEnd) != 1 {
			t.Fatal("split action grant boundary missing")
		}
		raw = bytes.Replace(raw, grantEnd, fmt.Appendf(nil, `,{"node_id":"%x","actions":65535},{"node_id":"%x","actions":65535}],"child_registry":`, nodes[3], nodes[4]), 1)
		if at := bytes.Index(raw, []byte(`,"enrolled_target":`)); at >= 0 {
			raw = append(raw[:at], '}')
		}
		if err := os.WriteFile(manifestPath, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := rf3testfixture.PrepareSplitRuntime(filepath.Join(root, fmt.Sprintf("group-0-member-%d", member)),
			rf3testfixture.InitialBootstrap([]uint64{2, 3, 4})); err != nil {
			t.Fatal(err)
		}
	}
	coldProcess.Args = []string{"serve-rf3", "-manifest", cold.ManifestPath}
	for _, process := range serving {
		if err := process.Start(); err != nil {
			t.Fatal(err)
		}
	}
	for _, process := range serving {
		if err := process.WaitReady(ctx, "vibedb-shard RF3 ready"); err != nil {
			t.Fatalf("reopen split voter: %v\n%s", err, process.Diagnostics())
		}
	}
}

func hotMutationAssertStaleParentRefused(
	t *testing.T, profile *rafttransport.PeerTLS, route gateway.ReplicatedRoute, current *gateway.Snapshot,
) {
	t.Helper()
	client, err := gateway.NewAuthenticatedReplicatedClient(gateway.AuthenticatedReplicatedClientOptions{
		TLS: profile, Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}, HandshakeDeadline: func() time.Time { return time.Now().Add(2 * time.Second) },
		MaxConnections: 8, MaxPerEndpoint: 4, MaxIdlePerEndpoint: 2, MaxHandshakes: 2,
		MaxWaiters: 8, MaxIdleAge: time.Minute, MaxLifetime: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	currentRoute, ok := current.ResolveReplicatedRoute(route.Distribution, route.Shard, make([]gateway.ReplicatedEndpoint, 0, gateway.ServingReplicaCount))
	if !ok {
		t.Fatal("retained child route missing")
	}
	authority := serviceauthz.Authority{Node: profile.LocalIdentity().Node, Generation: currentRoute.Command.ActivePolicyGeneration}
	ctx, err := serviceauthz.WithAuthority(t.Context(), authority)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, endpoint := range currentRoute.Replicas {
			probe, probeErr := client.ProbeReplicated(ctx, currentRoute, endpoint, serviceauthz.CapabilityDataRead)
			if probeErr != nil || probe == nil || !probe.HasState {
				continue
			}
			endpoint.NodeIncarnation = probe.State.Fence.NodeIncarnation
			fence := probe.State.Fence
			fence.Command = route.Command
			response, requestErr := client.DoReplicated(ctx, endpoint, &shardservice.ReplicatedRequest{
				Operation: shardservice.ReplicatedReadLeader, Capability: serviceauthz.CapabilityDataRead, Authority: authority,
				Fence: fence, Relation: 1, Key: []byte("m-0"), MinimumApplied: 1, MaxValueBytes: 1 << 20,
			})
			if requestErr != nil || response == nil || response.Kind == shardservice.ReplicatedNotLeader {
				continue
			}
			if response.Kind == shardservice.ReplicatedRefusal &&
				response.Refusal == shardservice.ReplicatedRefusalStaleFence {
				return
			}
			t.Fatalf("stale parent served old route: %+v", response)
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("stale parent produced no explicit stale-fence refusal")
}

func hotMutationWaitSplitRevision(
	t *testing.T, ctx context.Context, authority *gateway.ReplicatedCatalogAuthority,
	operation [32]byte, revision uint64,
) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last gateway.ReplicatedOperationRecord
	var lastErr error
	for time.Now().Before(deadline) {
		record, err := authority.ReadOperation(ctx, operation)
		last, lastErr = record, err
		if err == nil && record.Revision >= revision && record.Cursor[0] >= uint64(splitcontroller.ActionBuildArtifacts) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("split operation did not reach capture/artifact execution: state=%d revision=%d cursor=%v settled=%t error=%v", last.State, last.Revision, last.Cursor, last.ExecutionSettled, lastErr)
}

func hotMutationWaitSplitComplete(
	t *testing.T, ctx context.Context, authority *gateway.ReplicatedCatalogAuthority,
	generation uint64, operation [32]byte, source gateway.ReplicatedRoute,
	gatewayProcess *rf3testfixture.ExternalProcess,
) *gateway.Snapshot {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var lastGeneration uint64
	var lastRecord gateway.ReplicatedOperationRecord
	var lastReadErr, lastOperationErr error
	progressReports := 0
	lastDiagnostic := time.Now()
	for time.Now().Before(deadline) {
		if time.Since(lastDiagnostic) >= 5*time.Second {
			diagnostic := gatewayProcess.Diagnostics()
			if len(diagnostic) > 2048 {
				diagnostic = diagnostic[len(diagnostic)-2048:]
			}
			t.Logf("split controller tail: %s", diagnostic)
			lastDiagnostic = time.Now()
		}
		snapshot, err := authority.Read(ctx)
		record, operationErr := authority.ReadOperation(ctx, operation)
		lastReadErr, lastOperationErr = err, operationErr
		if snapshot != nil {
			lastGeneration = snapshot.Generation()
		}
		if operationErr == nil {
			if progressReports < 64 && (lastRecord.State != record.State || lastRecord.Cursor != record.Cursor) {
				t.Logf("split progress: generation=%d state=%d revision=%d cursor=%v",
					lastGeneration, record.State, record.Revision, record.Cursor)
				progressReports++
			}
			lastRecord = record
		}
		if err == nil && snapshot.Generation() >= generation &&
			errors.Is(operationErr, gateway.ErrReplicatedOperationMissing) {
			var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
			retained, found := snapshot.ResolveReplicatedRoute(
				source.Distribution, source.Shard, replicas[:0],
			)
			if !found || retained.Command.OwnershipEpoch <= source.Command.OwnershipEpoch ||
				retained.Command.RoutingVersion <= source.Command.RoutingVersion ||
				retained.Command.RouteGeneration <= source.Command.RouteGeneration {
				t.Fatalf("retained child did not replace stale parent authority: %+v", retained)
			}
			return snapshot
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("split did not publish, prune, retire, and collect its operation: generation=%d state=%d revision=%d cursor=%v read_error=%v operation_error=%v",
		lastGeneration, lastRecord.State, lastRecord.Revision, lastRecord.Cursor, lastReadErr, lastOperationErr)
	return nil
}

func hotMutationAdmitExactSplit(
	t *testing.T,
	ctx context.Context,
	controlPath string,
	local rafttransport.NodeID,
	authority *gateway.ReplicatedCatalogAuthority,
	catalog *gateway.Snapshot,
	route gateway.ReplicatedRoute,
) *splitcontroller.Plan {
	t.Helper()
	manifest, err := loadGatewayReplicaControlManifest(controlPath, local)
	if err != nil {
		t.Fatal(err)
	}
	factory, err := newGatewayHotSplitFactory(manifest, catalog)
	if err != nil {
		t.Fatal(err)
	}
	current, ok := catalog.Manifest(route.Distribution)
	if !ok {
		t.Fatal("split source manifest missing")
	}
	var source distribution.Shard
	for index := 0; index < current.ShardCount(); index++ {
		candidate, found := current.ShardInfo(index)
		if found && candidate.ID == route.Shard {
			source = candidate
			break
		}
	}
	if source.ID == "" {
		t.Fatal("split source shard missing")
	}
	operation := [32]byte{0x91, 0x53, 0x50, 0x4c, 0x49, 0x54}
	work := hotshard.SplitWork{Group: route.Group, Candidate: topologyscheduler.SplitCandidate{
		CatalogGeneration: catalog.Generation(), MigrationBytes: 1,
		Recommendation: autosplit.Recommendation{
			Source: autosplit.SourceIdentity{
				Distribution: route.Distribution, Shard: route.Shard,
				AllocationGeneration: distribution.ShardAllocationGeneration(route.AllocationGeneration), Range: source.Range,
				BucketBits:     distribution.DefaultVirtualBucketBits,
				RoutingVersion: current.Version(), OwnershipEpoch: source.Epoch,
			},
			WindowSequence: 1, Kind: autosplit.RecommendationBinarySplit,
			Boundaries: [2]distribution.KeyspacePoint{{0x80}}, BoundaryCount: 1,
			CandidateBin: 32, CurrentPressurePPM: 1_100_000,
			PredictedPressurePPM: 600_000, BenefitPPM: 500_000,
		},
	}}
	plan, err := factory.BuildHotSplitPlan(ctx, catalog, operation, work)
	if err != nil {
		t.Fatalf("prepare exact split: %v", err)
	}
	if _, err = splitcontroller.AdmitReplicatedPlan(ctx, authority, catalog, plan); err != nil {
		t.Fatalf("admit exact split: %v", err)
	}
	return plan
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
			Node: nodes[member], StoreID: identities[member].StoreID, NodeIncarnation: 1,
			Endpoint: listeners[member].Peer, DataAddress: listeners[member].Peer,
			NativeEndpoint: listeners[member].Native, Address: listeners[member].Native,
			ControlEndpoint: listeners[member].Control, ControlAddress: listeners[member].Control})
	}
	return route
}

func hotMutationCatalogAuthority(t *testing.T, profile *rafttransport.PeerTLS,
	snapshot *gateway.Snapshot, journalPath string, policyGeneration ...uint64,
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
		CatalogBootstrap: snapshot,
		Route:            route, Distribution: string(route.Distribution), Shard: string(route.Shard),
		Tenant: []byte{1}, ClientID: replication.ID128{0xa1}, RetryHome: replication.RetryHome{0xb1},
		Resolver: gateway.BaseRelationResolver{Relation: 1}, Journal: journal,
		ProposalCapability: serviceauthz.CapabilityTopology})
	if err != nil {
		t.Fatal(err)
	}
	generation := uint64(5)
	if len(policyGeneration) == 1 && policyGeneration[0] != 0 {
		generation = policyGeneration[0]
	} else if len(policyGeneration) != 0 {
		t.Fatal("invalid catalog authority policy generation")
	}
	identity := serviceauthz.Authority{Node: profile.LocalIdentity().Node, Generation: generation}
	authenticated, err := serviceauthz.WithAuthority(t.Context(), identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = session.Open(authenticated, time.Now().Add(time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	authority, err := gateway.NewReplicatedCatalogAuthority(gateway.ReplicatedCatalogAuthorityOptions{
		Executor: executor, Route: route, Relation: 1, Holder: gateway.NewCatalogHolder(nil),
		Session: session, Authority: identity})
	if err != nil {
		t.Fatal(err)
	}
	return authority, func() { _ = client.Close() }
}

func hotMutationLeader(t *testing.T, profile *rafttransport.PeerTLS,
	route gateway.ReplicatedRoute,
) uint64 {
	t.Helper()
	client, err := gateway.NewAuthenticatedReplicatedClient(gateway.AuthenticatedReplicatedClientOptions{
		TLS: profile, Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}, HandshakeDeadline: func() time.Time { return time.Now().Add(2 * time.Second) },
		MaxConnections: 8, MaxPerEndpoint: 4, MaxIdlePerEndpoint: 2, MaxHandshakes: 2,
		MaxWaiters: 8, MaxIdleAge: time.Minute, MaxLifetime: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	authority := serviceauthz.Authority{Node: profile.LocalIdentity().Node,
		Generation: route.Command.ActivePolicyGeneration}
	probeCtx, err := serviceauthz.WithAuthority(t.Context(), authority)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	var lastProbeError error
	var lastProbe *shardservice.ReplicatedResponse
	for time.Now().Before(deadline) {
		for _, endpoint := range route.Replicas {
			response, probeErr := client.ProbeReplicated(probeCtx, route, endpoint, serviceauthz.CapabilityDataWrite)
			lastProbe, lastProbeError = response, probeErr
			if probeErr == nil && response != nil &&
				response.Kind == shardservice.ReplicatedHandshake &&
				response.State.Fence.Group == route.Group &&
				response.State.LeaderID != 0 {
				for _, voter := range route.Replicas {
					if voter.Member == response.State.LeaderID {
						return voter.Member
					}
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("discover %s/%s leader timeout: %v response=%+v", route.Distribution, route.Shard, lastProbeError, lastProbe)
	return 0
}

func hotMutationCapacity(t *testing.T, root string) string {
	t.Helper()
	capacity := autosplit.CapacityVector{}
	for resource := range autosplit.ResourceCount {
		capacity[resource] = 64 << 20
	}
	capacity[autosplit.ResourceRequests] = 2
	spare := capacity
	spare[autosplit.ResourceRequests] = 64
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
		Endpoint: "fixture-g0-target-data", FailureDomain: 4, Capacity: &spare})
	sort.Slice(config.Nodes, func(i, j int) bool { return config.Nodes[i].Endpoint < config.Nodes[j].Endpoint })
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

func hotMutationControlManifest(t *testing.T, root string, groupNodes [3][5]rafttransport.NodeID,
	identities [4]raftstore.Identity, groupListeners [3][4]rf3testfixture.ProcessListeners,
	credential rf3testfixture.Credential, roots, policy, gatewayControl string,
	catalog *gateway.Snapshot,
	apply sqldriver.ReplicatedApplyOptions,
) []byte {
	t.Helper()
	nodes, listeners := groupNodes[0], groupListeners[0]
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
			Store: fmt.Sprintf("%x", identities[3].StoreID), NodeIncarnation: 1,
			Endpoint: "fixture-g0-target-data"}}}
	for member := 0; member < 4; member++ {
		manifest.ShardEndpoints = append(manifest.ShardEndpoints, persistedGatewayShardControlEndpoint{
			Node: fmt.Sprintf("%x", nodes[member]), ControlAddress: listeners[member].Control,
			SplitSnapshotAddress: listeners[member].Snapshot})
	}
	for group := 1; group < len(groupNodes); group++ {
		for member := 0; member < 3; member++ {
			manifest.ShardEndpoints = append(manifest.ShardEndpoints, persistedGatewayShardControlEndpoint{
				Node: fmt.Sprintf("%x", groupNodes[group][member]), ControlAddress: groupListeners[group][member].Control,
				SplitSnapshotAddress: groupListeners[group][member].Snapshot})
		}
	}
	sort.Slice(manifest.ShardEndpoints, func(i, j int) bool { return manifest.ShardEndpoints[i].Node < manifest.ShardEndpoints[j].Node })
	for _, source := range catalog.ReplicatedShardDescriptors() {
		if source.Group.GroupID != identities[0].GroupID {
			continue
		}
		for _, profile := range catalog.ReplicatedTableProfiles() {
			placement, ok := catalog.Placement(profile.Table)
			if !ok || placement.Distribution != source.Distribution || profile.Relation != 1 {
				continue
			}
			entry := persistedGatewaySplitSource{ClusterID: source.Group.ClusterID, ClusterIncarnation: source.Group.ClusterIncarnation,
				TopologyRecoveryEpoch: source.Group.TopologyRecoveryEpoch, ShardIncarnation: source.Group.ShardIncarnation, GroupID: source.Group.GroupID,
				SchemaGeneration: profile.SchemaGeneration, RelationManifestDigest: source.Command.RelationManifestDigest,
				Table: profile.Table, Template: gatewaySplitTemplateFixture()}
			identityRaw, err := os.ReadFile(filepath.Join(root, fmt.Sprintf("group-0-member-%d", source.Replicas[0].Member), "sql-identity.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := entry.SQL.UnmarshalJSON(identityRaw); err != nil {
				t.Fatal(err)
			}
			entry.LocalIndexes = []persistedGatewaySplitIndex{{Name: "by_kind", Paths: []string{"/kind"}}}
			entry.Template.MaxSessions, entry.Template.RetryWindow, entry.Template.TxnLimits = apply.MaxSessions, apply.RetryWindow, apply.TxnLimits
			entry.Template.Format, entry.Template.ShardKey = apply.Placement.Format, placement.Columns[0]
			entry.Placement = persistGatewaySplitPlacement(apply.Placement)
			entry.Template.TupleVersion, entry.Template.MapperVersion = uint16(apply.Placement.TupleVersion), uint16(apply.Placement.MapperVersion)
			for _, replica := range source.Replicas {
				entry.Replicas = append(entry.Replicas, persistedGatewaySplitReplica{
					Node: fmt.Sprintf("%x", replica.Node), ChildRoot: filepath.Join(root, fmt.Sprintf("group-0-member-%d", replica.Member), "split-children")})
			}
			sort.Slice(entry.Replicas, func(i, j int) bool { return entry.Replicas[i].Node < entry.Replicas[j].Node })
			manifest.SplitSources = append(manifest.SplitSources, entry)
		}
	}
	raw, err := vibejson.Marshal(&manifest)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func hotMutationGatewayProcess(binary, catalog, listen, control, capacity string,
	credential rf3testfixture.Credential, roots, policy, ack, journal string,
	listeners [3][4]rf3testfixture.ProcessListeners, nodes [3][5]rafttransport.NodeID,
) *rf3testfixture.ExternalProcess {
	args := []string{"serve", "-catalog", catalog,
		"-catalog-route-seed", journal + ".catalog-route-seed", "-catalog-relation", "1",
		"-catalog-session-journal", journal, "-durable-ack-key", ack,
		"-catalog-client-id", strings.Repeat("1a", 16), "-catalog-retry-home", strings.Repeat("2b", 8),
		"-catalog-attempts", "16", "-catalog-attempt-timeout", "2s", "-catalog-session-lease", "1h",
		// Accumulate the complete seven-request fault workload before the first
		// placement cut. The separate split phase below runs during writes.
		"-controller-interval", "20ms", "-hot-shard-capacity", capacity, "-hot-shard-interval", "30s",
		"-listen", listen, "-tls-certificate", credential.Certificate, "-tls-key", credential.Key,
		"-tls-roots", roots, "-tls-identity-oid", rf3testfixture.ProcessIdentityOID,
		"-authorization-policy", policy, "-replica-control-manifest", control}
	for group := range listeners {
		for member := 0; member < 3; member++ {
			args = append(args, "-shard-peer", listeners[group][member].Native+"="+fmt.Sprintf("%x", nodes[group][member]))
		}
	}
	args = append(args, "-shard-peer", listeners[0][3].Native+"="+fmt.Sprintf("%x", nodes[0][3]))
	return &rf3testfixture.ExternalProcess{Binary: binary, Args: args}
}

type hotMutationWireClient struct {
	connection net.Conn
	reader     *bufio.Reader
	requests   uint64
	bytes      uint64
	verifier   *hotMutationVerifier
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
	var lastErr error
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		connection, dialErr := client.Dial(t.Context(), address)
		if dialErr == nil {
			return connection
		}
		lastErr = dialErr
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("gateway dial %s timeout: %v", address, lastErr)
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
	started := time.Now()
	response, latency := client.roundTrip(t, request)
	// A leader handoff may explicitly leave this durable request unresolved.
	// Resolve it with one exact public retry; include both attempts in the
	// existing latency, request-count, and byte bounds.
	if strings.Contains(string(response), gateway.ErrDurableRequestUnresolved.Error()) {
		response, _ = client.roundTrip(t, request)
		latency = time.Since(started)
	}
	if !strings.Contains(string(response), `"committed":true`) ||
		strings.Contains(string(response), `"error"`) {
		t.Fatalf("exec_batch response=%s", response)
	}
	return latency
}

func (client *hotMutationWireClient) partitionAfterWrite(t *testing.T, request []byte,
	afterWrite func(),
) {
	t.Helper()
	if client == nil || client.connection == nil || afterWrite == nil {
		t.Fatal("invalid response-partition fixture")
	}
	if err := client.connection.SetDeadline(time.Now().Add(8 * time.Second)); err != nil {
		t.Fatal(err)
	}
	wire := append(append([]byte(nil), request...), '\n')
	written, err := client.connection.Write(wire)
	if err != nil {
		t.Fatal(err)
	}
	client.requests++
	client.bytes += uint64(written)
	afterWrite()
	if err = client.connection.Close(); err != nil {
		t.Fatal(err)
	}
}

func (client *hotMutationWireClient) reconnect(t *testing.T, profile *rafttransport.PeerTLS,
	node rafttransport.NodeID, address string,
) {
	t.Helper()
	connection := hotMutationDialGateway(t, profile, node, address)
	client.connection, client.reader = connection, bufio.NewReader(connection)
}

func hotMutationAssertMessage(t *testing.T, client *hotMutationWireClient,
	kind, email string, value int,
) {
	t.Helper()
	document, found := client.getMessage(t)
	if !found || document.ID != "m-0" || document.Kind != kind || document.Email != email || document.Value != value {
		t.Fatalf("native base visibility found=%t document=%+v want m-0,%s,%s,%d", found, document, kind, email, value)
	}
	client.verifier.assertEmail(t, email, true)
}

func hotMutationAssertSelectorEmpty(t *testing.T, client *hotMutationWireClient,
	field, value string,
) {
	t.Helper()
	if field == "email" {
		client.verifier.assertEmail(t, value, false)
		return
	}
	if field != "id" && field != "kind" {
		t.Fatalf("invalid visibility selector %q", field)
	}
	document, found := client.getMessage(t)
	if found && (field == "id" && document.ID == value || field == "kind" && document.Kind == value) {
		t.Fatalf("stale native %s=%q document=%+v", field, value, document)
	}
}

func hotMutationRosterContains(route gateway.ReplicatedRoute, member uint64) bool {
	for _, replica := range route.Replicas {
		if replica.Member == member {
			return true
		}
	}
	return false
}
