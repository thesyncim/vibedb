package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type membershipTransitionSQLClient struct {
	delegate        *replicatedSQLIndexedReadClient
	transition      bool
	observedCommand raftservice.CommandFence
	probes          int
	probeErr        error
}

func (client *membershipTransitionSQLClient) ProbeReplicated(
	ctx context.Context,
	route ReplicatedRoute,
	endpoint ReplicatedEndpoint,
	capability serviceauthz.Capability,
) (*shardservice.ReplicatedResponse, error) {
	client.probes++
	if client.probeErr != nil {
		return nil, client.probeErr
	}
	response, err := client.delegate.DoReplicated(ctx, endpoint, &shardservice.ReplicatedRequest{
		Operation: shardservice.ReplicatedProbe, Capability: capability,
		Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration,
			Command: route.Command},
	})
	if err != nil || !client.transition {
		if err == nil && response != nil && response.HasState && client.observedCommand.Valid() {
			response.State.Fence.Group = route.Group
			response.State.Fence.AllocationGeneration = route.AllocationGeneration
			response.State.Fence.MemberID = endpoint.Member
			response.State.Fence.StoreID = endpoint.StoreID
			response.State.Fence.NodeIncarnation = endpoint.NodeIncarnation
			response.State.Fence.Command = client.observedCommand
		}
		return response, err
	}
	response.State.Fence.Command.ReplicaSetVersion++
	return response, nil
}

func (client *membershipTransitionSQLClient) DoReplicated(
	ctx context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	response, err := client.delegate.DoReplicated(ctx, endpoint, request)
	if err == nil && response != nil && response.HasState && client.observedCommand.Valid() {
		response.State.Fence.MemberID = endpoint.Member
		response.State.Fence.StoreID = endpoint.StoreID
		response.State.Fence.NodeIncarnation = endpoint.NodeIncarnation
		response.State.Fence.Command = client.observedCommand
	}
	return response, err
}

func membershipTransitionCatalogSnapshots(t *testing.T) (*Snapshot, *Snapshot) {
	t.Helper()
	plain, _ := replicatedSQLTransactionFixture(t, true)
	if plain == nil || plain.statistics == nil {
		t.Fatal("fixture snapshot has no statistics catalog")
	}
	descriptors := plain.replicatedDescriptors()
	found := false
	for index := range descriptors {
		if descriptors[index].Distribution == "data" && descriptors[index].Shard == "all" {
			descriptors[index].RequestLedgerRanges = []DurableRequestLedgerRangeDescriptor{{
				Identity: replication.Digest{0x91},
			}}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("SQL fixture has no data route for the request-ledger home")
	}
	stale, err := NewSnapshotWithReplicatedTableMetadata(
		plain.config, plain.endpoints, plain.Generation(), plain.indexDescriptors(),
		plain.statistics.Descriptors(), descriptors, plain.replicatedTableProfiles(),
		plain.ReplicatedTableDeclarations(),
	)
	if err != nil {
		t.Fatalf("stale catalog with request-ledger topology: %v", err)
	}
	freshDescriptors := stale.replicatedDescriptors()
	found = false
	for index := range freshDescriptors {
		if freshDescriptors[index].Distribution == "data" && freshDescriptors[index].Shard == "all" {
			freshDescriptors[index].Command.ReplicaSetVersion++
			// A ReplicaSetVersion advance must carry an actual roster cut. Keep
			// the endpoint address stable so this fixture exercises the planner's
			// refresh/replan path without growing a second transport directory,
			// while replacing the authenticated identity at one ordinal models a
			// real membership transition.
			freshDescriptors[index].Replicas[2].Member = 4
			freshDescriptors[index].Replicas[2].Node = [16]byte{4}
			freshDescriptors[index].Replicas[2].StoreID = [16]byte{14}
			freshDescriptors[index].Replicas[2].NodeIncarnation = 24
			found = true
			break
		}
	}
	if !found {
		t.Fatal("stale catalog lost data route")
	}
	fresh, err := NewSnapshotWithReplicatedTableMetadata(
		stale.config, stale.endpoints, stale.Generation()+1, stale.indexDescriptors(),
		stale.statistics.Descriptors(), freshDescriptors, stale.replicatedTableProfiles(),
		stale.ReplicatedTableDeclarations(),
	)
	if err != nil {
		t.Fatalf("fresh catalog with advanced membership: %v", err)
	}
	return stale, fresh
}

func membershipTransitionSQLFixture(
	t *testing.T,
) (*DurableSQLRequestExecutor, *membershipTransitionSQLClient, *CatalogHolder, *Snapshot, *typedServiceLedger, *typedServicePinStop, *DurableRequestLedgerTopologyHolder, requestledger.RequestKey, []byte, []Query) {
	t.Helper()
	stale, fresh := membershipTransitionCatalogSnapshots(t)
	holder := NewCatalogHolder(stale)
	planner := NewExecutor(nil, holder, Options{})
	reader, _ := attachReplicatedSQLIndexedReadClient(
		t, stale, []byte(`{"id":"message-1","n":1}`),
	)
	var workspace [ServingReplicaCount]ReplicatedEndpoint
	freshRoute, ok := fresh.ResolveReplicatedRoute("data", "all", workspace[:0])
	if !ok {
		t.Fatal("fresh catalog lost data route")
	}
	client := &membershipTransitionSQLClient{
		delegate: reader, transition: true, observedCommand: freshRoute.Command,
	}
	replicated, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	planner.refresh = func(ctx context.Context, staleGeneration uint64) (*Snapshot, error) {
		if staleGeneration != stale.Generation() {
			return nil, fmt.Errorf("unexpected stale generation %d", staleGeneration)
		}
		holder.leaseMu.Lock()
		oldLeases := holder.activeLeases[staleGeneration]
		holder.leaseMu.Unlock()
		if oldLeases != 0 {
			return nil, fmt.Errorf("stale planning lease still held: %d", oldLeases)
		}
		client.transition = false
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// The authority has already certified this membership cut before the
		// planner observes it. Install that immutable result through the same
		// test boundary used by catalog-authority refresh tests; the generic
		// catalog publisher intentionally rejects an uncertified roster change.
		installCatalogRefreshTestSnapshot(t, holder, fresh)
		return fresh, nil
	}

	topology, err := NewCatalogDurableRequestLedgerTopologyHolder(holder)
	if err != nil {
		t.Fatal(err)
	}
	ledger := new(typedServiceLedger)
	pins := new(typedServicePinStop)
	service, err := newDurableRequestService(topology, ledger, typedServiceRunnerStop{}, pins)
	if err != nil {
		t.Fatal(err)
	}
	tenant := []byte("membership-transition-tenant")
	key := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{0x41},
		Request:      requestledger.RequestID{0x42},
		TenantDigest: requestledger.Digest(sha256.Sum256(tenant)), IssuerEpoch: 7,
		IssuerLane: requestledger.IssuerLane{0x43}, IssuerSequence: 1,
	}
	queries := []Query{{
		SQL: `UPDATE messages SET n = n + 1 WHERE id = ?`, Class: ClassInteractive,
		Params: []shardservice.Param{shardservice.StringParam("message-1")},
	}}
	executor := &DurableSQLRequestExecutor{
		planner: planner, data: replicated, requests: service,
		recoveryPulses: 3, planningLeaseSpan: 64,
	}
	return executor, client, holder, fresh, ledger, pins, topology, key, tenant, queries
}

func TestDurableSQLPlanningContinuesAcrossMembershipOnlyProgress(t *testing.T) {
	executor, client, holder, fresh, ledger, pins, topology, key, tenant, queries := membershipTransitionSQLFixture(t)
	_, err := executor.Execute(t.Context(), key, tenant, queries)
	if !errors.Is(err, errTypedServicePin) {
		t.Fatalf("membership progress interrupted logical admission: %v", err)
	}
	if holder.Current() == nil || holder.Current().Generation() != fresh.Generation()-1 || topology.Current() == nil || topology.Current().Generation != fresh.Generation()-1 ||
		!client.transition || client.probes == 0 || ledger.applies != 1 || pins.called != 1 {
		t.Fatalf("refresh/replan state holder=%p fresh=%p topology=%+v transition=%t probes=%d applies=%d pins=%d",
			holder.Current(), fresh, topology.Current(), client.transition, client.probes, ledger.applies, pins.called)
	}
}

func TestDurableSQLPrepareDirectKeepsLogicalCatalogDuringMembershipProgress(t *testing.T) {
	executor, client, _, fresh, ledger, pins, _, key, tenant, queries := membershipTransitionSQLFixture(t)
	plan, err := (&DurableSQLRequestExecutor{planner: executor.planner, data: executor.data, singleFast: true}).PrepareDirect(
		t.Context(), key, tenant, queries,
	)
	if err != nil || plan == nil || plan.CatalogGeneration != fresh.Generation()-1 {
		t.Fatalf("membership progress interrupted direct preimage: plan=%+v err=%v", plan, err)
	}
	if !client.transition || client.probes == 0 || executor.planner.catalog.Current() == nil ||
		executor.planner.catalog.Current().Generation() != fresh.Generation()-1 || ledger.applies != 0 || pins.called != 0 {
		t.Fatalf("direct refresh leaked ledger/admission: transition=%t probes=%d applies=%d pins=%d",
			client.transition, client.probes, ledger.applies, pins.called)
	}
}

type membershipReplayErrorLedger struct {
	typedServiceLedger
	err error
}

func (ledger *membershipReplayErrorLedger) ReadRow(
	context.Context,
	DurableRequestLedgerHome,
	DurableRequestLifecycleRead,
) (DurableRequestLifecycleRow, error) {
	ledger.reads++
	return DurableRequestLifecycleRow{}, ledger.err
}

func TestDurableSQLPlanningFailurePreservesReplayError(t *testing.T) {
	executor, client, holder, _, _, pins, _, key, tenant, queries := membershipTransitionSQLFixture(t)
	client.probeErr = ErrReplicatedRoute
	const replayErrText = "retained replay unavailable"
	ledger := &membershipReplayErrorLedger{err: errors.New(replayErrText)}
	topology, err := NewCatalogDurableRequestLedgerTopologyHolder(holder)
	if err != nil {
		t.Fatal(err)
	}
	service, err := newDurableRequestService(topology, ledger, typedServiceRunnerStop{}, pins)
	if err != nil {
		t.Fatal(err)
	}
	executor.requests = service
	_, err = executor.Execute(t.Context(), key, tenant, queries)
	if !errors.Is(err, ledger.err) || !errors.Is(err, ErrDurableSQLNotAdmitted) ||
		client.transition == false || ledger.applies != 0 || pins.called != 0 || holder.Current().Generation() != 7 {
		t.Fatalf("replay error was hidden or refreshed: err=%v transition=%t reads=%d applies=%d pins=%d generation=%d",
			err, client.transition, ledger.reads, ledger.applies, pins.called, holder.Current().Generation())
	}
	if ledger.reads != 1 {
		t.Fatalf("replay error caused repeated ledger reads: %d", ledger.reads)
	}
}

func TestDurableSQLMembershipProgressRetainsTerminalReplayAuthority(t *testing.T) {
	executor, client, holder, _, _, pins, _, _, _, queries := membershipTransitionSQLFixture(t)
	targets := durableFaultTargets(t)
	requestDigest := replication.Digest(replicatedSQLTransactionRequestDigest(queries))
	base := durableFaultRequestWith(t, targets, replication.ID128{0x91}, requestDigest, 7)
	topology := durableFaultTopology(t, targets)
	current := topology.Current()
	current.Generation = 7
	if err := topology.Publish(*current); err != nil {
		t.Fatal(err)
	}
	resultRaw, err := AppendDurableRequestResult(nil, DurableRequestResult{
		Committed: true, AffectedRows: 4, Transaction: base.Program.Identity.ID,
		CatalogGeneration:       base.Program.Identity.CatalogGeneration,
		ShardsFanned:            uint64(len(base.Program.Targets)),
		TransitionTag:           base.Program.Contract.CommitTransitionTag,
		TerminalStateDigest:     base.Program.Contract.CommitTerminalStateDigest,
		TerminalContractDigest:  base.Program.Contract.TerminalContractDigest,
		RetirementWitnessDigest: base.Program.Contract.RetirementWitnessDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger := &typedServiceLedger{
		head: requestledger.HeadRecord{
			Key: base.Key.RequestKey, RequestDigest: requestledger.Digest(base.Key.Digest),
			PlanRoot: requestledger.Digest{2}, Revision: 9, Phase: requestledger.PhaseTerminal,
		},
		terminal: requestledger.TerminalRecord{
			Revision: 9, Result: resultRaw, ResultDigest: requestledger.ResultDigest(resultRaw),
			RequestDigest: requestledger.Digest(base.Key.Digest), PlanRoot: requestledger.Digest{2},
			CatalogGeneration:      base.Program.Identity.CatalogGeneration,
			TerminalContractDigest: requestledger.Digest(base.Program.Contract.TerminalContractDigest),
			AckToken:               requestledger.AckToken{1},
		},
	}
	ledger.terminal.KeyDigest, err = requestledger.KeyDigest(base.Key.RequestKey)
	if err != nil {
		t.Fatal(err)
	}
	service, err := newDurableRequestService(topology, ledger, typedServiceRunnerStop{}, pins)
	if err != nil {
		t.Fatal(err)
	}
	executor.requests = service
	result, err := executor.Execute(t.Context(), base.Key.RequestKey, []byte("tenant-fault"), queries)
	if err != nil || result.Result == nil || result.Result.RowsAffected != 4 || result.Key.Digest != requestDigest {
		t.Fatalf("retained terminal was not authoritative: result=%+v err=%v", result, err)
	}
	if client.transition == false || holder.Current().Generation() != 7 || ledger.applies != 0 || pins.called != 0 {
		t.Fatalf("replay-found path refreshed/admitted: transition=%t generation=%d applies=%d pins=%d",
			client.transition, holder.Current().Generation(), ledger.applies, pins.called)
	}
}
