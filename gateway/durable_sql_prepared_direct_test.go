package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibejson"
)

func TestDurableSQLPreparedUpdateReplaysPersistedMutation(t *testing.T) {
	snapshot, planner := replicatedSQLTransactionFixture(t, true)
	old := []byte(`{"id":"message-1","n":41}`)
	reader, data := attachReplicatedSQLIndexedReadClient(t, snapshot, old)
	executor := &DurableSQLRequestExecutor{planner: planner, data: data, singleFast: true}
	tenant := []byte("prepared-direct")
	key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{1}, Request: requestledger.RequestID{2}, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)), IssuerEpoch: 1, IssuerLane: requestledger.IssuerLane{3}, IssuerSequence: 1}
	ctx, err := serviceauthz.WithAuthority(t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	queries := []Query{{SQL: `UPDATE messages SET n=n+1 WHERE id=?`, Class: ClassInteractive, Params: []shardservice.Param{shardservice.StringParam("message-1")}}}
	plan, err := executor.PrepareDirect(ctx, key, tenant, queries)
	if err != nil || reader.reads != 1 {
		t.Fatalf("prepare reads=%d err=%v", reader.reads, err)
	}
	mutation := plan.Target.Batches[0].Mutations[0]
	if mutation.Kind != replication.MutationPutDigestEqual || string(mutation.Value) != `{"id":"message-1","n":42}` || mutation.ExpectedValueDigest != replication.Digest(sha256.Sum256(old)) {
		t.Fatalf("mutation=%+v", mutation)
	}
	raw, err := vibejson.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	var recovered DurableSQLDirectPlan
	if err = vibejson.Unmarshal(raw, &recovered); err != nil {
		t.Fatal(err)
	}
	command := func(p *DurableSQLDirectPlan) []byte {
		encoded, _, err := appendReplicatedDirectMutationCommand(nil, ReplicatedDirectMutation{Key: p.Key, RequestDigest: p.RequestDigest, Tenant: tenant, Target: p.Target})
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	if !bytes.Equal(command(plan), command(&recovered)) {
		t.Fatal("journal round trip changed native command")
	}
	reader.value = []byte(`{"id":"message-1","n":99}`)
	proposer := &directSQLProposalClient{t: t, route: plan.Target.Route, applied: 1}
	executor.data, err = NewReplicatedExecutor(proposer, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result, err := executor.ExecutePreparedDirect(ctx, key, tenant, queries, &recovered, false)
		if err != nil || !result.Direct || result.Result.RowsAffected != 1 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	}
	if reader.reads != 1 || proposer.proposals != 2 {
		t.Fatal("execution reevaluated SQL")
	}
	queries[0].SQL = `UPDATE messages SET n=n+2 WHERE id=?`
	if _, err = executor.ExecutePreparedDirect(ctx, key, tenant, queries, &recovered, false); err == nil || proposer.proposals != 2 {
		t.Fatal("changed caller input accepted")
	}
}

func TestDurableSQLPreparedDirectRecoversLostReplyOnAdvancedFence(t *testing.T) {
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	initialRoute := routeFromTestDescriptor(descriptor, endpoints)
	initialState := shardservice.ReplicatedMemberState{
		Fence: shardservice.ReplicatedFence{
			Group: initialRoute.Group, AllocationGeneration: initialRoute.AllocationGeneration,
			Command: initialRoute.Command, MemberID: 2,
			StoreID:         initialRoute.Replicas[1].StoreID,
			NodeIncarnation: initialRoute.Replicas[1].NodeIncarnation, Term: 7,
		},
		LeaderID: 2, Commit: 1, Applied: 1, CheckpointApplied: 1,
	}
	initialRoute, client, _ := newRouteSessionMachineForRoute(t, initialRoute, initialState, false)
	descriptor.Command.RelationManifestDigest = initialRoute.Command.RelationManifestDigest
	oldSnapshot, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 1, nil, nil, []ReplicatedShardDescriptor{descriptor},
		[]ReplicatedTableProfile{profile},
	)
	if err != nil {
		t.Fatal(err)
	}
	// Model an unrelated shard publication: the global catalog generation is
	// already newer, but this retained shard still carries its original fence.
	intermediateSnapshot, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 2, nil, nil, []ReplicatedShardDescriptor{descriptor},
		[]ReplicatedTableProfile{profile},
	)
	if err != nil {
		t.Fatal(err)
	}

	currentConfig := cloneConfig(config)
	currentEndpoints := make(map[distribution.EndpointID]string, len(endpoints))
	for endpoint, address := range endpoints {
		currentEndpoints[endpoint] = address
	}
	shard, ok := currentConfig.Manifests[0].ShardInfo(0)
	if !ok {
		t.Fatal("test manifest has no shard")
	}
	shard.Epoch++
	currentManifest, err := distribution.NewManifest(
		currentConfig.Manifests[0].Distribution(), currentConfig.Manifests[0].Version()+1,
		[]distribution.Shard{shard},
	)
	if err != nil {
		t.Fatal(err)
	}
	currentConfig.Manifests[0] = currentManifest
	currentDescriptor := descriptor
	currentDescriptor.Command.OwnershipEpoch++
	currentDescriptor.Command.RoutingVersion++
	currentDescriptor.Command.RouteGeneration++
	currentSnapshot, err := NewSnapshotWithReplicatedTableMetadata(
		currentConfig, currentEndpoints, 3, nil, nil,
		[]ReplicatedShardDescriptor{currentDescriptor}, []ReplicatedTableProfile{profile},
	)
	if err != nil {
		t.Fatal(err)
	}

	var refreshed bool
	var refreshFloors []uint64
	delayedClient := &staleFenceOnceDirectRecoveryClient{inner: client}
	planner := NewExecutor(nil, NewCatalogHolder(intermediateSnapshot), Options{Refresh: func(_ context.Context, floor uint64) (*Snapshot, error) {
		refreshFloors = append(refreshFloors, floor)
		switch floor {
		case intermediateSnapshot.Generation():
			if !refreshed {
				return nil, ErrStaleGeneration
			}
			return currentSnapshot, nil
		case currentSnapshot.Generation():
			// The fixture deliberately keeps the exact latest target route on the
			// second lookup; the server's one stale-fence refusal is transient.
			return nil, ErrStaleGeneration
		default:
			t.Fatalf("catalog refresh floor=%d, want %d or %d", floor,
				intermediateSnapshot.Generation(), currentSnapshot.Generation())
		}
		return nil, ErrStaleGeneration
	}})
	data, err := NewReplicatedExecutor(delayedClient, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	executor := &DurableSQLRequestExecutor{planner: planner, data: data, singleFast: true}
	tenant := []byte("prepared-fence-recovery")
	key := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{0x31},
		Request: requestledger.RequestID{0x41}, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		IssuerEpoch: 5, IssuerLane: requestledger.IssuerLane{0x51}, IssuerSequence: 1,
	}
	ctx, err := serviceauthz.WithAuthority(t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	queries := []Query{{SQL: `INSERT INTO messages (id,n) VALUES ('lost-route-row',1)`, Class: ClassInteractive}}
	targets, handled, err := planner.planReplicatedSQLTransactionWithData(
		ctx, oldSnapshot, queries, planner.profileFor(ClassInteractive), nil,
	)
	if err != nil || !handled || len(targets) != 1 || targets[0].Route.Command != initialRoute.Command {
		t.Fatalf("prepared route targets=%+v handled=%v err=%v", targets, handled, err)
	}
	plan := &DurableSQLDirectPlan{
		Key: key, RequestDigest: replicatedSQLTransactionRequestDigest(queries),
		CatalogGeneration: oldSnapshot.Generation(), Target: targets[0],
	}
	client.hideDirect = true
	client.onDirectLost = func() error {
		batch := plan.Target.Batches[0]
		stored, readErr := client.machine.PointReadInto(
			batch.Relation, batch.Mutations[0].Key, client.state.Applied,
			replication.MaxMutationValueBytes, nil,
		)
		if readErr != nil || !stored.Found {
			t.Fatalf("first committed row relation=%d key=%q applied=%d value=%q found=%v err=%v",
				batch.Relation, batch.Mutations[0].Key, client.state.Applied, stored.Value, stored.Found, readErr)
		}
		advanced := advanceRouteSessionOwnershipSameRoster(t, client, initialRoute)
		refreshed = true
		delayedClient.rejectNextProposal = true
		if advanced.Command != currentDescriptor.Command {
			t.Fatalf("applied machine fence=%+v catalog fence=%+v", advanced.Command, currentDescriptor.Command)
		}
		return nil
	}
	result, err := executor.ExecutePreparedDirect(ctx, key, tenant, queries, plan, false)
	firstCompletion, firstCompletionErr := replication.OpenCompletion(client.firstDirect)
	retryCompletion, retryCompletionErr := replication.OpenCompletion(client.retryDirect)
	sameOutcome := firstCompletionErr == nil && retryCompletionErr == nil &&
		firstCompletion.ClientID == retryCompletion.ClientID &&
		firstCompletion.ClientEpoch == retryCompletion.ClientEpoch &&
		firstCompletion.ClientSequence == retryCompletion.ClientSequence &&
		firstCompletion.Fingerprint == retryCompletion.Fingerprint &&
		firstCompletion.AppliedSequence == retryCompletion.AppliedSequence &&
		firstCompletion.ResultCode == retryCompletion.ResultCode &&
		firstCompletion.ResultDigest == retryCompletion.ResultDigest &&
		firstCompletion.ResultLength == retryCompletion.ResultLength &&
		bytes.Equal(firstCompletion.InlineResult, retryCompletion.InlineResult)
	if err != nil || !result.Direct || result.Result == nil || result.Result.RowsAffected != 1 || refreshed != true || len(client.retryDirect) == 0 ||
		!sameOutcome || len(refreshFloors) != 2 ||
		refreshFloors[0] != intermediateSnapshot.Generation() || refreshFloors[1] != currentSnapshot.Generation() ||
		planner.catalog.Current().Generation() != currentSnapshot.Generation() {
		t.Fatalf("prepared recovery result=%+v generation=%d applied=%d refreshed=%v same_outcome=%v refresh_floors=%v retry_len=%d first_open=%v retry_open=%v err=%v",
			result, planner.catalog.Current().Generation(), client.state.Applied, refreshed,
			sameOutcome, refreshFloors, len(client.retryDirect), firstCompletionErr, retryCompletionErr, err)
	}
	batch := plan.Target.Batches[0]
	stored, err := client.machine.PointReadInto(
		batch.Relation, batch.Mutations[0].Key, client.state.Applied, replication.MaxMutationValueBytes, nil,
	)
	if err != nil || !stored.Found || !bytes.Equal(stored.Value, []byte(`{"id":"lost-route-row","n":1}`)) {
		t.Fatalf("recovered row relation=%d key=%q applied=%d value=%q found=%v err=%v",
			batch.Relation, batch.Mutations[0].Key, client.state.Applied, stored.Value, stored.Found, err)
	}
}

type staleFenceOnceDirectRecoveryClient struct {
	inner              *routeSessionMachineClient
	rejectNextProposal bool
}

func (client *staleFenceOnceDirectRecoveryClient) DoReplicated(
	ctx context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedPropose && client.rejectNextProposal {
		client.rejectNextProposal = false
		state := client.inner.state
		state.Fence.MemberID = endpoint.Member
		state.Fence.StoreID = endpoint.StoreID
		state.Fence.NodeIncarnation = endpoint.NodeIncarnation
		state.Fence.Command.OwnershipEpoch++
		state.Fence.Command.RoutingVersion++
		state.Fence.Command.RouteGeneration++
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedRefusal, HasState: true, State: state,
			Refusal: shardservice.ReplicatedRefusalStaleFence,
		}, nil
	}
	return client.inner.DoReplicated(ctx, endpoint, request)
}

func TestDirectMutationRecoveryRouteRequiresSameLogicalAllocation(t *testing.T) {
	_, endpoints, descriptor, _ := testReplicatedTableInput(t)
	old := routeFromTestDescriptor(descriptor, endpoints)
	advanced := old
	advanced.Command.OwnershipEpoch++
	advanced.Command.RoutingVersion++
	advanced.Command.RouteGeneration++
	if !directMutationRouteAdvanceAllowed(old, advanced) {
		t.Fatal("monotone fence advance on the same logical allocation was rejected")
	}
	tests := []struct {
		name   string
		mutate func(*ReplicatedRoute)
	}{
		{"group", func(route *ReplicatedRoute) { route.Group.GroupID[0]++ }},
		{"allocation", func(route *ReplicatedRoute) { route.AllocationGeneration++ }},
		{"distribution", func(route *ReplicatedRoute) { route.Distribution += "-other" }},
		{"shard", func(route *ReplicatedRoute) { route.Shard += "-other" }},
		{"range", func(route *ReplicatedRoute) { route.RangeIdentity[0]++ }},
		{"lineage", func(route *ReplicatedRoute) { route.LineageDigest[0]++ }},
		{"forwarding", func(route *ReplicatedRoute) { route.ForwardingRuleDigest[0]++ }},
		{"logical-schema", func(route *ReplicatedRoute) { route.LogicalSchemaDigest[0]++ }},
		{"relation-schema", func(route *ReplicatedRoute) { route.Command.RelationManifestDigest[0]++ }},
		{"active-policy", func(route *ReplicatedRoute) { route.Command.ActivePolicyGeneration++ }},
		{"protection", func(route *ReplicatedRoute) { route.Command.ProtectionEpoch++ }},
		{"schema-generation", func(route *ReplicatedRoute) { route.Command.SchemaGeneration++ }},
		{"replica-set-regression", func(route *ReplicatedRoute) { route.Command.ReplicaSetVersion-- }},
		{"ownership-regression", func(route *ReplicatedRoute) { route.Command.OwnershipEpoch -= 2 }},
		{"routing-regression", func(route *ReplicatedRoute) { route.Command.RoutingVersion -= 2 }},
		{"route-generation-regression", func(route *ReplicatedRoute) { route.Command.RouteGeneration -= 2 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := advanced
			test.mutate(&current)
			if directMutationRouteAdvanceAllowed(old, current) {
				t.Fatal("recovery accepted a changed allocation or non-monotone fence")
			}
		})
	}
}

func TestRetryableDirectOutcomeRecoveryRequiresTypedTransientUnknown(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"unknown stale fence", errors.Join(raftservice.ErrOutcomeUnknown, raftservice.ErrServingFence), true},
		{"unknown no leader", errors.Join(raftservice.ErrOutcomeUnknown, ErrReplicatedLeader), true},
		{"unknown invalid route", errors.Join(raftservice.ErrOutcomeUnknown, ErrReplicatedRoute), false},
		{"definite stale fence", raftservice.ErrServingFence, false},
		{"unknown canceled", errors.Join(raftservice.ErrOutcomeUnknown, ErrReplicatedLeader, context.Canceled), false},
		{"unknown deadline", errors.Join(raftservice.ErrOutcomeUnknown, raftservice.ErrServingFence, context.DeadlineExceeded), false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := retryableDirectOutcomeRecovery(test.err); got != test.want {
				t.Fatalf("retryableDirectOutcomeRecovery(%v)=%v, want %v", test.err, got, test.want)
			}
		})
	}
}

type unavailableDirectRecoveryClient struct {
	probes    int
	proposals int
}

func (client *unavailableDirectRecoveryClient) DoReplicated(
	_ context.Context,
	_ ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedProbe {
		client.probes++
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalUnavailable,
		}, nil
	}
	client.proposals++
	return nil, errors.New("unexpected proposal without a discovered leader")
}

func TestDurableSQLDirectRecoveryKeepsUnknownWhenAuthorityIsUnavailable(t *testing.T) {
	for _, waitForDeadline := range []bool{false, true} {
		name := "authority unavailable"
		if waitForDeadline {
			name = "authority respects recovery deadline"
		}
		t.Run(name, func(t *testing.T) {
			snapshot, planner := replicatedSQLTransactionFixture(t, true)
			client := &unavailableDirectRecoveryClient{}
			data, err := NewReplicatedExecutor(client, 1, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			refreshes := 0
			planner.refresh = func(ctx context.Context, _ uint64) (*Snapshot, error) {
				refreshes++
				if waitForDeadline {
					<-ctx.Done()
					return nil, context.Cause(ctx)
				}
				return nil, errors.New("catalog authority unavailable")
			}
			executor := &DurableSQLRequestExecutor{planner: planner, data: data, singleFast: true}
			tenant := []byte("direct-route-recovery-deadline")
			key := requestledger.RequestKey{
				Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{0x61},
				Request: requestledger.RequestID{0x62}, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
				IssuerEpoch: 1, IssuerLane: requestledger.IssuerLane{0x63}, IssuerSequence: 1,
			}
			queries := []Query{{SQL: `INSERT INTO messages (id,n) VALUES ('retained-route-row',1)`, Class: ClassInteractive}}
			targets, handled, err := planner.planReplicatedSQLTransactionWithData(
				t.Context(), snapshot, queries, planner.profileFor(ClassInteractive), nil,
			)
			if err != nil || !handled || len(targets) != 1 {
				t.Fatalf("plan=%+v handled=%v err=%v", targets, handled, err)
			}
			digest := replicatedSQLTransactionRequestDigest(queries)
			ledgerKey, err := NewDurableRequestLedgerKey(key, digest)
			if err != nil {
				t.Fatal(err)
			}
			request := ReplicatedDirectMutation{
				Key: key, RequestDigest: replication.Digest(digest), Tenant: tenant, Target: targets[0],
			}
			mutationDigest, err := replication.TransactionMutationDigest(request.Target.Batches)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 125*time.Millisecond)
			defer cancel()
			ctx, err = serviceauthz.WithAuthority(ctx, serviceauthz.Authority{Node: [16]byte{7}, Generation: 1})
			if err != nil {
				t.Fatal(err)
			}
			_, recovered, direct, err := executor.retryDirectOutcome(ctx, request, snapshot.Generation())
			recoveredMutationDigest, digestErr := replication.TransactionMutationDigest(recovered.Target.Batches)
			if !errors.Is(err, raftservice.ErrOutcomeUnknown) || !errors.Is(err, context.DeadlineExceeded) ||
				refreshes == 0 || client.proposals != 0 || direct.ID != (distributedtxn.ID{}) ||
				recovered.Key != request.Key || recovered.RequestDigest != request.RequestDigest ||
				recoveredMutationDigest != mutationDigest || digestErr != nil || ledgerKey.Digest != request.RequestDigest {
				t.Fatalf("recovery error=%v refreshes=%d probes=%d proposals=%d direct=%+v", err, refreshes, client.probes, client.proposals, direct)
			}
		})
	}
}

func routeFromTestDescriptor(
	descriptor ReplicatedShardDescriptor,
	addresses map[distribution.EndpointID]string,
) ReplicatedRoute {
	route := ReplicatedRoute{
		Distribution: descriptor.Distribution, Shard: descriptor.Shard,
		Group: descriptor.Group, AllocationGeneration: uint64(descriptor.AllocationGeneration),
		Command: descriptor.Command, LogicalSchemaDigest: descriptor.LogicalSchemaDigest,
		RangeIdentity: descriptor.RangeIdentity, LineageDigest: descriptor.LineageDigest,
		ForwardingRuleDigest: descriptor.ForwardingRuleDigest,
		Replicas:             make([]ReplicatedEndpoint, 0, len(descriptor.Replicas)),
	}
	for _, replica := range descriptor.Replicas {
		route.Replicas = append(route.Replicas, ReplicatedEndpoint{
			Member: replica.Member, Node: replica.Node, StoreID: replica.StoreID,
			NodeIncarnation: replica.NodeIncarnation, Endpoint: string(replica.Endpoint),
			DataAddress: addresses[replica.Endpoint], NativeEndpoint: string(replica.NativeEndpoint),
			Address: addresses[replica.NativeEndpoint], ControlEndpoint: string(replica.ControlEndpoint),
			ControlAddress: addresses[replica.ControlEndpoint],
		})
	}
	return route
}

func TestDurableSQLPreparedUpdateRequiresExactPreimageGuard(t *testing.T) {
	query := []Query{{SQL: `UPDATE messages SET n=n+1 WHERE id='missing'`}}
	targets := []ReplicatedTransactionTarget{{Batches: []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPutPresent, Key: []byte("missing"), Value: []byte(`{}`)}}}}}}
	if preparedDirectEligible(query, targets) {
		t.Fatal("a missing-row placeholder is not a guarded update")
	}
	snapshot, planner := replicatedSQLTransactionFixture(t, true)
	reader, data := attachReplicatedSQLIndexedReadClient(t, snapshot, nil)
	executor := &DurableSQLRequestExecutor{planner: planner, data: data, singleFast: true}
	tenant := []byte("prepared-missing")
	key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{1}, Request: requestledger.RequestID{2}, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)), IssuerEpoch: 1, IssuerLane: requestledger.IssuerLane{3}, IssuerSequence: 1}
	plan, err := executor.PrepareDirect(t.Context(), key, tenant, query)
	if plan != nil || !errors.Is(err, ErrDurableSQLDirectIneligible) || !errors.Is(err, ErrDurableSQLNotAdmitted) || reader.reads != 2 {
		t.Fatalf("missing preimage plan=%+v reads=%d err=%v", plan, reader.reads, err)
	}
}
