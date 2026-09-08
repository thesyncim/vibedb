package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// baselineMembershipTransitionClient deliberately uses only APIs that
// predate the membership-refresh implementation. It models a certified
// catalog cut becoming available while an indexed computed UPDATE is still
// holding the predecessor route, so this file can be copied to the pre-fix
// base and used as a causal regression (the old source fails before Begin).
type baselineMembershipTransitionClient struct {
	delegate   *replicatedSQLIndexedReadClient
	transition bool
	fresh      raftservice.CommandFence
}

func (client *baselineMembershipTransitionClient) ProbeReplicated(
	ctx context.Context,
	route ReplicatedRoute,
	endpoint ReplicatedEndpoint,
	capability serviceauthz.Capability,
) (*shardservice.ReplicatedResponse, error) {
	response, err := client.delegate.DoReplicated(ctx, endpoint, &shardservice.ReplicatedRequest{
		Operation: shardservice.ReplicatedProbe, Capability: capability,
		Fence: shardservice.ReplicatedFence{
			Group: route.Group, AllocationGeneration: route.AllocationGeneration,
		},
	})
	if err != nil || response == nil || !response.HasState {
		return response, err
	}
	if client.transition {
		response.State.Fence.Command.ReplicaSetVersion = route.Command.ReplicaSetVersion + 1
		return response, nil
	}
	response.State.Fence.Group = route.Group
	response.State.Fence.AllocationGeneration = route.AllocationGeneration
	response.State.Fence.MemberID = endpoint.Member
	response.State.Fence.StoreID = endpoint.StoreID
	response.State.Fence.NodeIncarnation = endpoint.NodeIncarnation
	response.State.Fence.Command = client.fresh
	return response, nil
}

func (client *baselineMembershipTransitionClient) DoReplicated(
	ctx context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	response, err := client.delegate.DoReplicated(ctx, endpoint, request)
	if err != nil || response == nil || !response.HasState || client.transition {
		return response, err
	}
	response.State.Fence.MemberID = endpoint.Member
	response.State.Fence.StoreID = endpoint.StoreID
	response.State.Fence.NodeIncarnation = endpoint.NodeIncarnation
	response.State.Fence.Command = client.fresh
	return response, nil
}

func baselineMembershipTransitionSnapshots(t *testing.T) (*Snapshot, *Snapshot) {
	t.Helper()
	plain, _ := replicatedSQLTransactionFixture(t, true)
	descriptors := plain.replicatedDescriptors()
	for index := range descriptors {
		if descriptors[index].Distribution == "data" && descriptors[index].Shard == "all" {
			descriptors[index].RequestLedgerRanges = []DurableRequestLedgerRangeDescriptor{{
				Identity: replication.Digest{0x91},
			}}
			break
		}
	}
	stale, err := NewSnapshotWithReplicatedTableMetadata(
		plain.config, plain.endpoints, plain.Generation(), plain.indexDescriptors(),
		plain.statistics.Descriptors(), descriptors, plain.replicatedTableProfiles(),
		plain.ReplicatedTableDeclarations(),
	)
	if err != nil {
		t.Fatalf("stale catalog: %v", err)
	}
	freshDescriptors := stale.replicatedDescriptors()
	for index := range freshDescriptors {
		if freshDescriptors[index].Distribution == "data" && freshDescriptors[index].Shard == "all" {
			freshDescriptors[index].Command.ReplicaSetVersion++
			freshDescriptors[index].Replicas[2].Member = 4
			freshDescriptors[index].Replicas[2].Node = [16]byte{4}
			freshDescriptors[index].Replicas[2].StoreID = [16]byte{14}
			freshDescriptors[index].Replicas[2].NodeIncarnation = 24
			break
		}
	}
	fresh, err := NewSnapshotWithReplicatedTableMetadata(
		stale.config, stale.endpoints, stale.Generation()+1, stale.indexDescriptors(),
		stale.statistics.Descriptors(), freshDescriptors, stale.replicatedTableProfiles(),
		stale.ReplicatedTableDeclarations(),
	)
	if err != nil {
		t.Fatalf("fresh catalog: %v", err)
	}
	return stale, fresh
}

func TestDurableSQLMembershipTransitionBaselineExecutesCertifiedRefresh(t *testing.T) {
	stale, fresh := baselineMembershipTransitionSnapshots(t)
	catalog := NewCatalogHolder(stale)
	planner := NewExecutor(nil, catalog, Options{})
	reader, _ := attachReplicatedSQLIndexedReadClient(
		t, stale, []byte(`{"id":"message-1","n":1}`),
	)
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	freshRoute, ok := fresh.ResolveReplicatedRoute("data", "all", replicas[:0])
	if !ok {
		t.Fatal("fresh route")
	}
	client := &baselineMembershipTransitionClient{
		delegate: reader, transition: true, fresh: freshRoute.Command,
	}
	data, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	topology, err := NewCatalogDurableRequestLedgerTopologyHolder(catalog)
	if err != nil {
		t.Fatal(err)
	}
	ledger := new(typedServiceLedger)
	pins := new(typedServicePinStop)
	service, err := newDurableRequestService(topology, ledger, typedServiceRunnerStop{}, pins)
	if err != nil {
		t.Fatal(err)
	}
	planner.refresh = func(ctx context.Context, generation uint64) (*Snapshot, error) {
		if generation != stale.Generation() {
			t.Fatalf("refresh generation=%d", generation)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		client.transition = false
		// This helper models the certified authority publication boundary. The
		// generic catalog publisher must still reject an uncertified roster cut.
		installCatalogRefreshTestSnapshot(t, catalog, fresh)
		return fresh, nil
	}
	executor := &DurableSQLRequestExecutor{
		planner: planner, data: data, requests: service,
		recoveryPulses: 3, planningLeaseSpan: 64,
	}
	tenant := []byte("membership-transition-baseline")
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
	_, err = executor.Execute(t.Context(), key, tenant, queries)
	if !errors.Is(err, errTypedServicePin) {
		t.Fatalf("certified refresh did not reach admission: %v", err)
	}
	if ledger.applies != 1 || pins.called != 1 || catalog.Current().Generation() != fresh.Generation() {
		t.Fatalf("applies=%d pins=%d catalog_generation=%d", ledger.applies, pins.called, catalog.Current().Generation())
	}
}
