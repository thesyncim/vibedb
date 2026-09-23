package gatewayruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestDropTablePostDurableCommitReceiverFailuresKeepOldGatesFencedAndIFExistsRetryCompletesBarrier(t *testing.T) {
	ctx := context.Background()
	var (
		durablePending bool
		oldGatesFenced = true
		releaseCalls   int
		refreshCalls   int
	)
	firstReceiverLoss := errors.New("first receiver response lost after durable retirement")
	finalReceiverLoss := errors.New("final receiver response lost after durable retirement")

	retire := func(context.Context) (bool, error) {
		durablePending = true
		return true, nil
	}
	refresh := func(context.Context) error {
		refreshCalls++
		switch refreshCalls {
		case 1:
			return firstReceiverLoss
		case 2:
			return finalReceiverLoss
		default:
			return nil
		}
	}
	release := func(context.Context) error {
		releaseCalls++
		oldGatesFenced = false
		return nil
	}

	if err := completeGatewayTableDropPostCommit(ctx, retire, refresh, release); !errors.Is(err, firstReceiverLoss) {
		t.Fatalf("first postcommit receiver loss=%v, want %v", err, firstReceiverLoss)
	}
	if !durablePending {
		t.Fatal("durable retirement witness was not retained after first receiver loss")
	}
	if !oldGatesFenced || releaseCalls != 0 {
		t.Fatalf("first receiver loss released old gates: fenced=%v releaseCalls=%d", oldGatesFenced, releaseCalls)
	}

	// This is the exact pending-witness IF EXISTS/recovery branch: it retries
	// the publication barrier without invoking retirement or releasing gates.
	if err := retryGatewayTableDropAfterCommit(ctx, durablePending, refresh); !errors.Is(err, finalReceiverLoss) {
		t.Fatalf("final postcommit receiver loss=%v, want %v", err, finalReceiverLoss)
	}
	if !oldGatesFenced || releaseCalls != 0 {
		t.Fatalf("final receiver loss released old gates: fenced=%v releaseCalls=%d", oldGatesFenced, releaseCalls)
	}
	if err := retryGatewayTableDropAfterCommit(ctx, durablePending, refresh); err != nil {
		t.Fatalf("recovered IF EXISTS/recovery publication: %v", err)
	}
	if !oldGatesFenced || releaseCalls != 0 || refreshCalls != 3 {
		t.Fatalf("recovered barrier state fenced=%v releaseCalls=%d refreshCalls=%d", oldGatesFenced, releaseCalls, refreshCalls)
	}
}

// dropTableRuntimeClient is a small authenticated native service boundary for
// this integration test. Catalog commands use the real session and catalog
// authority protocol; the table route uses the real route-gate state machine.
// Only the transport and storage below are in-memory so the test can inject a
// response loss at the postcommit refresh boundary without bypassing
// DropTable itself.
type dropTableRuntimeClient struct {
	mu             sync.Mutex
	authority      serviceauthz.Authority
	catalogRoute   gateway.ReplicatedRoute
	dataRoute      gateway.ReplicatedRoute
	catalogRows    map[string][]byte
	catalogApplied uint64
	dataApplied    uint64
	dataGate       *routegate.Machine
}

func (client *dropTableRuntimeClient) DoReplicated(
	ctx context.Context, endpoint gateway.ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if ctx == nil || request == nil {
		return nil, gateway.ErrReplicatedRoute
	}
	if request.Authority != client.authority {
		return nil, gateway.ErrReplicatedUnauthorized
	}
	switch request.Fence.Group {
	case client.catalogRoute.Group:
		return client.doCatalog(endpoint, request)
	case client.dataRoute.Group:
		return client.doData(endpoint, request)
	default:
		return nil, gateway.ErrReplicatedRoute
	}
}

func dropTableRuntimeState(
	route gateway.ReplicatedRoute, endpoint gateway.ReplicatedEndpoint, applied uint64,
) shardservice.ReplicatedMemberState {
	return shardservice.ReplicatedMemberState{
		Fence: shardservice.ReplicatedFence{
			Group: route.Group, AllocationGeneration: route.AllocationGeneration,
			Command: route.Command, MemberID: endpoint.Member, StoreID: endpoint.StoreID,
			NodeIncarnation: endpoint.NodeIncarnation, Term: 1,
		},
		LeaderID: route.Replicas[0].Member, Commit: applied, Applied: applied,
		CheckpointApplied: applied,
	}
}

func (client *dropTableRuntimeClient) doCatalog(
	endpoint gateway.ReplicatedEndpoint, request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	state := dropTableRuntimeState(client.catalogRoute, endpoint, client.catalogApplied)
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake,
			HasState: true, State: state}, nil
	}
	if request.Operation == shardservice.ReplicatedReadLeader ||
		request.Operation == shardservice.ReplicatedReadFollower {
		value, found := client.catalogRows[string(request.Key)]
		response := &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedReadMissing,
			HasState: true, State: state, ReadApplied: client.catalogApplied}
		if found {
			response.Kind = shardservice.ReplicatedReadFound
			response.Value = append([]byte(nil), value...)
		}
		return response, nil
	}
	response, err := client.applyCommandLocked(client.catalogRoute, endpoint, request, false)
	if err != nil {
		return nil, fmt.Errorf("drop-table fixture catalog operation=%d: %w", request.Operation, err)
	}
	if err := shardservice.ValidateReplicatedResponse(response); err != nil {
		return nil, fmt.Errorf("drop-table fixture catalog operation=%d invalid response: %w", request.Operation, err)
	}
	return response, nil
}

func (client *dropTableRuntimeClient) doData(
	endpoint gateway.ReplicatedEndpoint, request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	state := dropTableRuntimeState(client.dataRoute, endpoint, client.dataApplied)
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake,
			HasState: true, State: state}, nil
	}
	if request.Operation == shardservice.ReplicatedRouteGateRead {
		status := client.dataGate.Status()
		value, err := shardservice.AppendReplicatedRouteGateReadValue(nil, status)
		if err != nil {
			return nil, err
		}
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRouteGateReadResult,
			HasState: true, State: state, ReadApplied: client.dataApplied, Value: value}, nil
	}
	response, err := client.applyCommandLocked(client.dataRoute, endpoint, request, true)
	if err != nil {
		return nil, fmt.Errorf("drop-table fixture data operation=%d: %w", request.Operation, err)
	}
	if err := shardservice.ValidateReplicatedResponse(response); err != nil {
		return nil, fmt.Errorf("drop-table fixture data operation=%d invalid response: %w", request.Operation, err)
	}
	return response, nil
}

func (client *dropTableRuntimeClient) applyCommandLocked(
	route gateway.ReplicatedRoute, endpoint gateway.ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest, data bool,
) (*shardservice.ReplicatedResponse, error) {
	view, err := replication.OpenCommand(request.Command)
	if err != nil {
		return nil, fmt.Errorf("drop-table test command operation=%d data=%t bytes=%d: %w", request.Operation, data, len(request.Command), err)
	}
	if view.Kind() == replication.CommandSessionRelease {
		// A released session still consumes its admitted log position. Return
		// the same deterministic terminal refusal as the real state machine so
		// NativeSession can settle and destroy its exact journal.
		if data {
			client.dataApplied++
		} else {
			client.catalogApplied++
		}
		applied := client.catalogApplied
		if data {
			applied = client.dataApplied
		}
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalDeterministic,
			HasState: true, State: dropTableRuntimeState(route, endpoint, applied),
			RequestDigest: sha256.Sum256(request.Command),
			Outcome:       raftserve.Outcome{Code: raftserve.OutcomeSessionReleased, AppliedIndex: applied},
		}, nil
	}
	if data && view.Kind() != replication.CommandRouteGate &&
		view.Kind() != replication.CommandSessionOpen &&
		view.Kind() != replication.CommandSessionRetire {
		return nil, gateway.ErrReplicatedRoute
	}
	if !data && view.Kind() == replication.CommandRouteGate {
		return nil, gateway.ErrReplicatedRoute
	}
	var (
		resultCode   = replicatedstate.ResultApplied
		resultFormat = replicatedstate.ResultFormatMutation
		result       []byte
		clientEpoch  = view.ClientEpoch
	)
	if data {
		switch view.Kind() {
		case replication.CommandSessionOpen:
			client.dataApplied++
			clientEpoch = client.dataApplied
			resultCode = replicatedstate.ResultSessionOpened
		case replication.CommandSessionRetire:
			client.dataApplied++
			resultCode = replicatedstate.ResultSessionRetired
		case replication.CommandRouteGate:
			gateCommand, openErr := view.OpenRouteGate()
			if openErr != nil {
				return nil, openErr
			}
			outcome := client.dataGate.Apply(gateCommand)
			result, err = routegate.AppendOutcome(nil, outcome)
			if err != nil {
				return nil, err
			}
			resultCode = replicatedstate.ResultRouteGate
			resultFormat = replicatedstate.ResultFormatRouteGate
			client.dataApplied++
		default:
			return nil, gateway.ErrReplicatedRoute
		}
	} else {
		switch view.Kind() {
		case replication.CommandSessionOpen:
			client.catalogApplied++
			clientEpoch = client.catalogApplied
			resultCode = replicatedstate.ResultSessionOpened
		case replication.CommandSessionRetire:
			client.catalogApplied++
			resultCode = replicatedstate.ResultSessionRetired
		case replication.CommandMutationBatch:
			if resultCode, err = client.applyCatalogMutationsLocked(view); err != nil {
				return nil, err
			}
			result, err = replicatedstate.AppendMutationCompletionResult(nil, resultCode, 0)
			if err != nil {
				return nil, err
			}
			client.catalogApplied++
		default:
			return nil, gateway.ErrReplicatedRoute
		}
	}
	completion, err := dropTableRuntimeCompletion(view, clientEpoch,
		func() uint64 {
			if data {
				return client.dataApplied
			}
			return client.catalogApplied
		}(), resultCode, resultFormat, result)
	if err != nil {
		return nil, err
	}
	applied := client.catalogApplied
	if data {
		applied = client.dataApplied
	}
	state := dropTableRuntimeState(route, endpoint, applied)
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedCompletion,
		HasState: true, State: state, RequestDigest: sha256.Sum256(request.Command),
		Completion: completion, Outcome: raftserve.Outcome{Code: raftserve.OutcomeCompletion,
			AppliedIndex: applied, CompletionAppliedSequence: applied,
			CompletionBytes: len(completion)}}, nil
}

func (client *dropTableRuntimeClient) applyCatalogMutationsLocked(
	view replication.CommandView,
) (uint32, error) {
	type update struct{ key, value string }
	updates := make([]update, 0, view.MutationCount())
	batches := view.RelationBatches()
	for batches.Next() {
		mutations := batches.Batch().Mutations()
		for mutations.Next() {
			mutation := mutations.Mutation()
			key := string(mutation.Key)
			prior, found := client.catalogRows[key]
			switch mutation.Kind {
			case replication.MutationPutDigestEqual:
				if !found || uint64(len(prior)) != mutation.ExpectedValueLength ||
					sha256.Sum256(prior) != mutation.ExpectedValueDigest {
					return replicatedstate.ResultIndexConflict, nil
				}
				updates = append(updates, update{key: key, value: string(mutation.Value)})
			case replication.MutationPutAbsentOrEqual:
				if found && !bytes.Equal(prior, mutation.Value) {
					return replicatedstate.ResultIndexConflict, nil
				}
				if !found {
					updates = append(updates, update{key: key, value: string(mutation.Value)})
				}
			case replication.MutationPut:
				updates = append(updates, update{key: key, value: string(mutation.Value)})
			case replication.MutationDeleteDigestEqual:
				if !found || uint64(len(prior)) != mutation.ExpectedValueLength ||
					sha256.Sum256(prior) != mutation.ExpectedValueDigest {
					return replicatedstate.ResultIndexConflict, nil
				}
				updates = append(updates, update{key: key})
			case replication.MutationDelete:
				updates = append(updates, update{key: key})
			default:
				return 0, fmt.Errorf("unsupported catalog test mutation kind %d", mutation.Kind)
			}
		}
	}
	for _, value := range updates {
		if value.value == "" {
			delete(client.catalogRows, value.key)
		} else {
			client.catalogRows[value.key] = []byte(value.value)
		}
	}
	return replicatedstate.ResultApplied, nil
}

func dropTableRuntimeCompletion(
	view replication.CommandView, clientEpoch, applied uint64,
	resultCode uint32, resultFormat uint16, result []byte,
) ([]byte, error) {
	digest := replication.CompletionResultDigest(resultCode, resultFormat, result)
	return replication.AppendCompletionBytes(nil, replication.CompletionBytes{
		ClusterID: view.ClusterID, ClusterIncarnation: view.ClusterIncarnation,
		TopologyRecoveryEpoch: view.TopologyRecoveryEpoch,
		Distribution:          view.Distribution, Shard: view.Shard,
		AllocationGeneration: view.AllocationGeneration,
		ShardIncarnation:     view.ShardIncarnation, GroupID: view.GroupID,
		ReplicaSetVersion:      view.ReplicaSetVersion,
		ActivePolicyGeneration: view.ActivePolicyGeneration,
		ProtectionEpoch:        view.ProtectionEpoch, RoutingVersion: view.RoutingVersion,
		RouteGeneration: view.RouteGeneration, Tenant: view.Tenant,
		ClientID: view.ClientID, ClientEpoch: clientEpoch,
		ClientSequence: view.ClientSequence, Fingerprint: view.Fingerprint,
		RetryHome: view.RetryHome, AppliedSequence: applied,
		ResultCode: resultCode, ResultFormat: resultFormat,
		Storage: replication.CompletionInline, ResultLength: uint64(len(result)),
		ResultDigest: digest, InlineResult: result,
	})
}

type dropTableRuntimeFixture struct {
	authority *gateway.ReplicatedCatalogAuthority
	runtime   *gatewaySchemaDDLRuntime
	executor  *gateway.ReplicatedExecutor
	client    *dropTableRuntimeClient
	dataRoute gateway.ReplicatedRoute
}

func newDropTableRuntimeFixture(t *testing.T, refresh func(context.Context) error) dropTableRuntimeFixture {
	t.Helper()
	group := func(seed byte) raftmember.GroupKey {
		var result raftmember.GroupKey
		result.TopologyRecoveryEpoch = 1
		for index := range result.ClusterID {
			result.ClusterID[index] = seed + byte(index)
			result.ClusterIncarnation[index] = seed + 0x20 + byte(index)
			result.ShardIncarnation[index] = seed + 0x40 + byte(index)
			result.GroupID[index] = seed + 0x60 + byte(index)
		}
		return result
	}
	dataGroup := group(2)
	ledgerGroup := group(3)
	manifest, err := distribution.NewManifest("drop_data", 1, []distribution.Shard{{
		ID: "all", AllocationGeneration: 1,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"data-1", "data-2", "data-3"}, Epoch: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	ledgerManifest, err := distribution.NewManifest("ledger_data", 1, []distribution.Shard{{
		ID: "all", AllocationGeneration: 1,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"data-1", "data-2", "data-3"}, Epoch: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	endpoints := make(map[distribution.EndpointID]string, 9)
	replicas := make([]gateway.ReplicatedReplicaDescriptor, gateway.ServingReplicaCount)
	for index := range replicas {
		member := index + 1
		dataEndpoint := distribution.EndpointID(fmt.Sprintf("data-%d", member))
		nativeEndpoint := distribution.EndpointID(fmt.Sprintf("native-%d", member))
		controlEndpoint := distribution.EndpointID(fmt.Sprintf("control-%d", member))
		endpoints[dataEndpoint] = fmt.Sprintf("127.0.0.1:%d", 7001+index)
		endpoints[nativeEndpoint] = fmt.Sprintf("127.0.0.1:%d", 7101+index)
		endpoints[controlEndpoint] = fmt.Sprintf("127.0.0.1:%d", 7201+index)
		replicas[index] = gateway.ReplicatedReplicaDescriptor{
			Member: uint64(member), Node: rafttransport.NodeID{byte(member)},
			StoreID: [16]byte{byte(0x10 + member)}, NodeIncarnation: 1,
			Endpoint: dataEndpoint, NativeEndpoint: nativeEndpoint,
			ControlEndpoint: controlEndpoint,
		}
	}
	digest := [32]byte{7}
	logical := replication.Digest{8}
	descriptor := gateway.ReplicatedShardDescriptor{
		Distribution: "drop_data", Shard: "all", Group: dataGroup,
		AllocationGeneration: 1, RangeIdentity: replication.Digest{9},
		LineageDigest: replication.Digest{10}, ForwardingRuleDigest: replication.Digest{11},
		Command: raftservice.CommandFence{ReplicaSetVersion: 1,
			ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1,
			SchemaGeneration: 1, RelationManifestDigest: digest,
			RoutingVersion: 1, RouteGeneration: 1},
		LogicalSchemaDigest: logical, Replicas: replicas,
	}
	ledgerDescriptor := descriptor
	ledgerDescriptor.Distribution = "ledger_data"
	ledgerDescriptor.Group = ledgerGroup
	ledgerDescriptor.RequestLedgerRanges = []gateway.DurableRequestLedgerRangeDescriptor{{
		Identity: replication.Digest{12},
	}}
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{
			{Name: "drop_data", Arity: 1, MapperVersion: distribution.NativeMapperVersion},
			{Name: "ledger_data", Arity: 1, MapperVersion: distribution.NativeMapperVersion},
		},
		Placements: []distribution.TablePlacement{{Table: "drop_messages", Distribution: "drop_data", Columns: []string{"/id"}}},
		Manifests:  []*distribution.Manifest{manifest, ledgerManifest},
	}
	profile := gateway.ReplicatedTableProfile{Table: "drop_messages", Relation: 1,
		PrimaryKey: "/id", SchemaGeneration: 1, LogicalSchemaDigest: logical,
		MaxKeyBytes: 256, MaxDocumentBytes: 1 << 20}
	current, err := gateway.NewSnapshotWithReplicatedTableMetadata(config, endpoints, 1,
		nil, nil, []gateway.ReplicatedShardDescriptor{descriptor, ledgerDescriptor},
		[]gateway.ReplicatedTableProfile{profile}, []gateway.ReplicatedTableDeclaration{{
			Table: "drop_messages", CreateTable: "CREATE TABLE drop_messages (id TEXT PRIMARY KEY)",
		}})
	if err != nil {
		t.Fatal(err)
	}
	var scratch [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	dataRoute, ok := current.ResolveReplicatedRoute("drop_data", "all", scratch[:0])
	if !ok {
		t.Fatal("resolve drop data route")
	}
	catalogRoute := dataRoute
	catalogRoute.Distribution = gateway.ReplicatedCatalogDistribution
	catalogRoute.Shard = gateway.ReplicatedCatalogShard
	// Keep the catalog service group distinct from the table's data group. The
	// authenticated native request carries only its group fence, so using the
	// same group here would route catalog topology reads through the data
	// handler and make the fixture unable to distinguish a route-gate read.
	catalogRoute.Group = group(1)
	authorityIdentity := serviceauthz.Authority{Generation: 9}
	authorityIdentity.Node[0] = 0x71
	client := &dropTableRuntimeClient{authority: authorityIdentity,
		catalogRoute: catalogRoute, dataRoute: dataRoute,
		catalogRows: make(map[string][]byte), dataApplied: 1, catalogApplied: 1}
	client.dataGate, ok = routegate.NewMachine(1, 64)
	if !ok {
		t.Fatal("construct route gate")
	}
	records := make([]gateway.NodeRecord, len(replicas))
	for index, replica := range replicas {
		records[index] = gateway.NodeRecord{
			NodeID: replica.Node, Incarnation: replica.NodeIncarnation,
			ServiceKeyDigest: replication.Digest{byte(0x30 + index)},
			DataEndpoint:     replica.Endpoint, NativeEndpoint: replica.NativeEndpoint,
			ControlEndpoint: replica.ControlEndpoint,
			DataAddress:     endpoints[replica.Endpoint], NativeAddress: endpoints[replica.NativeEndpoint],
			ControlAddress: endpoints[replica.ControlEndpoint], FailureDomain: fmt.Sprintf("zone-%d", index),
			Roles:     gateway.NodeRoleStorage | gateway.NodeRoleCatalog | gateway.NodeRoleControl,
			Lifecycle: gateway.NodeActive, Revision: 1, CatalogGeneration: 1,
		}
	}
	genesis, err := gateway.BuildReplicatedCatalogGenesisMutations(current, records)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range genesis {
		client.catalogRows[string(mutation.Key)] = append([]byte(nil), mutation.Value...)
	}
	executor, err := gateway.NewReplicatedExecutor(client, 4, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	session, err := gateway.NewNativeSession(gateway.NativeSessionOptions{
		Executor: executor, Route: catalogRoute,
		Distribution: string(catalogRoute.Distribution), Shard: string(catalogRoute.Shard),
		Tenant: []byte("control-plane"), ClientID: replication.ID128{0x71},
		Resolver:           gateway.BaseRelationResolver{Relation: 1},
		ProposalCapability: serviceauthz.CapabilityTopology, MaxRelationBatches: 1,
		MaxMutations: 8, InitialCommandBytes: 4 << 10, MaxCommandBytes: replication.MaxCommandBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(context.Background(), authorityIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = session.Open(ctx, time.Now().Add(time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	authority, err := gateway.NewReplicatedCatalogAuthority(gateway.ReplicatedCatalogAuthorityOptions{
		Executor: executor, Route: catalogRoute, Relation: 1,
		Holder: gateway.NewCatalogHolder(current), Session: session, Authority: authorityIdentity,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := newGatewaySchemaDDLRuntime(authority, executor,
		&gatewayShardControlOpener{}, func() time.Time { return time.Now().Add(time.Second) },
		func() time.Time { return time.Now().Add(time.Second) }, t.TempDir(), authorityIdentity, refresh)
	if err != nil {
		t.Fatal(err)
	}
	return dropTableRuntimeFixture{authority: authority, runtime: runtime,
		executor: executor, client: client, dataRoute: dataRoute}
}

func TestDropTableRuntimePostcommitFailuresFenceActualRouteAndRecoverIFExists(t *testing.T) {
	firstReceiverLoss := errors.New("first receiver response lost after durable retirement")
	finalReceiverLoss := errors.New("final receiver response lost after durable retirement")
	refreshCalls := 0
	fixture := newDropTableRuntimeFixture(t, func(context.Context) error {
		refreshCalls++
		switch refreshCalls {
		case 1:
			return firstReceiverLoss
		case 2:
			return finalReceiverLoss
		default:
			return nil
		}
	})
	ctx, err := serviceauthz.WithAuthority(context.Background(), fixture.client.authority)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.runtime.DropTable(ctx, "drop_messages", false); !errors.Is(err, firstReceiverLoss) {
		t.Fatalf("first actual DropTable receiver loss=%v, want %v", err, firstReceiverLoss)
	}
	current, err := fixture.authority.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := current.Placement("drop_messages"); found {
		t.Fatal("durably retired table remains in catalog placement")
	}
	if table, _, pending := current.PendingProvisionedTableRetirement(); !pending || table != "drop_messages" {
		t.Fatalf("durable retirement witness=%q,%v", table, pending)
	}
	observed, err := fixture.executor.ReadRouteGate(ctx, fixture.dataRoute, 1)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Status.Drain.State != routegate.DrainActive {
		t.Fatalf("old route gate after first postcommit failure=%v, want active", observed.Status.Drain.State)
	}
	if err := fixture.runtime.DropTable(ctx, "drop_messages", true); !errors.Is(err, finalReceiverLoss) {
		t.Fatalf("IF EXISTS final receiver loss=%v, want %v", err, finalReceiverLoss)
	}
	observed, err = fixture.executor.ReadRouteGate(ctx, fixture.dataRoute, 1)
	if err != nil || observed.Status.Drain.State != routegate.DrainActive {
		t.Fatalf("old route gate after final failure=%+v err=%v, want active", observed.Status, err)
	}
	if err := fixture.runtime.DropTable(ctx, "drop_messages", true); err != nil {
		t.Fatalf("recovered IF EXISTS publication barrier: %v", err)
	}
	if refreshCalls != 3 {
		t.Fatalf("refresh calls=%d, want first/final failure plus one successful retry", refreshCalls)
	}
	if err := fixture.authority.ConfirmProvisionedTableRetirement(ctx, "drop_messages"); err != nil {
		t.Fatalf("confirm durable inventory cleanup: %v", err)
	}
	current, err = fixture.authority.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, pending := current.PendingProvisionedTableRetirement(); pending {
		t.Fatal("confirmed table retirement retained pending witness")
	}
	observed, err = fixture.executor.ReadRouteGate(ctx, fixture.dataRoute, 1)
	if err != nil || observed.Status.Drain.State != routegate.DrainActive {
		t.Fatalf("route gate after witness confirmation=%+v err=%v, old route must remain fenced", observed.Status, err)
	}
}
