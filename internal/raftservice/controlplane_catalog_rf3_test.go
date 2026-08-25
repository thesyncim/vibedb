package raftservice_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

// TestReplicatedCatalogAuthorityRF3QuorumReplayAndControllerRestart proves the
// control-plane relation through the same three-process RF3 serving path as
// ordinary data. The process harness is intentionally reused without another
// metadata consensus or an in-memory apply substitute.
func TestReplicatedCatalogAuthorityRF3QuorumReplayAndControllerRestart(t *testing.T) {
	if os.Getenv(processHelperEnvironment) != "" {
		return
	}
	cluster, err := startProcessRF3Cluster(t)
	if errors.Is(err, errProcessStrictAllocation) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cluster.close(t)
		cluster.logDiagnostics(t)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leader := cluster.elect(t, ctx, 1)
	route := cluster.route()
	client := newFaultProcessClient(t, cluster)
	session := newProcessNativeSession(t, route, client, 0xa1)
	if _, err = session.Open(ctx, 2_000_000_000_000_000_000); err != nil {
		t.Fatalf("open catalog session: %v", err)
	}
	first := processControlPlaneSnapshot(t, cluster, 1)
	authority, err := gateway.NewReplicatedCatalogAuthority(gateway.ReplicatedCatalogAuthorityOptions{
		Executor: client.executor, Route: route, Relation: 1,
		Holder: gateway.NewCatalogHolder(nil), Session: session,
	})
	if err != nil {
		t.Fatal(err)
	}

	client.resetAttempts()
	if err = authority.Publish(ctx, 0, first); err != nil {
		t.Fatalf("publish initial catalog: %v", err)
	}
	attempts, _ := client.snapshot()
	if len(attempts) != 1 {
		t.Fatalf("catalog proposal attempts = %d, want 1", len(attempts))
	}
	// Replaying the byte-identical proposal is a retained-result lookup. It must
	// not apply the put-if-absent command a second time.
	replayed, err := client.executor.Propose(ctx, route, attempts[0])
	if err != nil {
		t.Fatalf("replay catalog proposal: %v", err)
	}
	replayedAgain, err := client.executor.Propose(ctx, route, attempts[0])
	if err != nil || !bytes.Equal(replayed.Completion, replayedAgain.Completion) ||
		replayed.Outcome.AppliedIndex != replayedAgain.Outcome.AppliedIndex {
		t.Fatalf("idempotent replay changed result: first=%+v second=%+v err=%v",
			replayed.Outcome, replayedAgain.Outcome, err)
	}
	for member := uint64(1); member <= processVoters; member++ {
		cluster.waitCommittedApplied(t, ctx, member, replayed.Outcome.AppliedIndex, 0)
	}

	// Reconstruct all controller-local state from the RF3 relation. A fresh
	// session and holder have no catalog bytes or pending command in memory.
	restartedSession := newProcessNativeSession(t, route, client, 0xa2)
	if _, err = restartedSession.Open(ctx, 2_000_000_000_000_000_000); err != nil {
		t.Fatalf("open restarted catalog session: %v", err)
	}
	restarted, err := gateway.NewReplicatedCatalogAuthority(gateway.ReplicatedCatalogAuthorityOptions{
		Executor: client.executor, Route: route, Relation: 1,
		Holder: gateway.NewCatalogHolder(nil), Session: restartedSession,
	})
	if err != nil {
		t.Fatal(err)
	}
	read, err := restarted.Read(ctx)
	if err != nil || read.Generation() != 1 {
		t.Fatalf("catalog after controller restart = %v, err=%v", read, err)
	}

	// The same authority is the split controller's replicated journal. Its
	// operation survives reconstruction through a linearizable relation read.
	var journal splitcontroller.ReplicatedOperationJournal = restarted
	record := gateway.ReplicatedOperationRecord{
		ID: [32]byte{0xc1}, Kind: gateway.ReplicatedOperationSplit,
		State: gateway.ReplicatedOperationPlanned, Revision: 1,
		CatalogGeneration: 1, Cursor: [8]uint64{1}, Proof: [32]byte{0xd1},
	}
	if err = journal.PublishOperation(ctx, 0, record); err != nil {
		t.Fatalf("publish split operation: %v", err)
	}
	loaded, err := journal.ReadOperation(ctx, record.ID)
	if err != nil || loaded != record {
		t.Fatalf("read split operation = %+v, err=%v", loaded, err)
	}

	second := processControlPlaneSnapshot(t, cluster, 2)
	if err = restarted.Publish(ctx, 1, second); err != nil {
		t.Fatalf("catalog CAS generation 1->2: %v", err)
	}
	if err = restarted.Publish(ctx, 1, second); !errors.Is(err, gateway.ErrCatalogGenerationMismatch) {
		t.Fatalf("stale catalog generation = %v", err)
	}

	// Leader loss cannot expose the local holder as authority. The read travels
	// through the replacement leader and returns the committed generation.
	cluster.killAndElect(t, leader)
	afterLoss, err := restarted.Read(ctx)
	if err != nil || afterLoss.Generation() != 2 {
		t.Fatalf("catalog after leader loss = %v, err=%v", afterLoss, err)
	}
}

func processControlPlaneSnapshot(
	t testing.TB, cluster *processRF3Cluster, generation uint64,
) *gateway.Snapshot {
	t.Helper()
	const (
		distributionName distribution.DistributionName = "orders"
		shardID          distribution.ShardID          = "0000-ffff"
	)
	endpointIDs := [processVoters]distribution.EndpointID{
		"rf3-member-1", "rf3-member-2", "rf3-member-3",
	}
	nativeEndpointIDs := [processVoters]distribution.EndpointID{
		"rf3-native-1", "rf3-native-2", "rf3-native-3",
	}
	sqlAddresses := [processVoters]string{"127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"}
	manifest, err := distribution.NewManifest(distributionName, 17, []distribution.Shard{{
		ID: shardID, AllocationGeneration: 7,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: endpointIDs[:], Epoch: 11,
	}})
	if err != nil {
		t.Fatal(err)
	}
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{
			Name: distributionName, Arity: 1,
			MapperVersion: distribution.NativeMapperVersion,
		}},
		Placements: []distribution.TablePlacement{{
			Table: "docs", Distribution: distributionName, Columns: []string{"/id"},
		}},
		Manifests: []*distribution.Manifest{manifest},
	}
	endpoints := make(map[distribution.EndpointID]string, processVoters)
	replicas := make([]gateway.ReplicatedReplicaDescriptor, processVoters)
	for index := 0; index < processVoters; index++ {
		endpoints[endpointIDs[index]] = sqlAddresses[index]
		endpoints[nativeEndpointIDs[index]] = cluster.nativeAddresses[index]
		replicas[index] = gateway.ReplicatedReplicaDescriptor{
			Member: uint64(index + 1), Node: processNode(uint64(index + 1)),
			StoreID:         processStoreIdentity(uint64(index + 1)).StoreID,
			NodeIncarnation: 1, Endpoint: endpointIDs[index],
			NativeEndpoint: nativeEndpointIDs[index],
		}
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(
		config, endpoints, generation, nil, nil, []gateway.ReplicatedShardDescriptor{{
			Distribution: distributionName, Shard: shardID,
			Group: processGroup(), AllocationGeneration: 7,
			Command: cluster.commandFence, Replicas: replicas,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
