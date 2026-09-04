package gatewayruntime

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

type rejectingSplitPreparationJournal struct {
	splitcontroller.ReplicatedOperationJournal
	submits int
}

func (*rejectingSplitPreparationJournal) ReadOperation(context.Context, [32]byte) (gateway.ReplicatedOperationRecord, error) {
	return gateway.ReplicatedOperationRecord{}, gateway.ErrReplicatedOperationMissing
}
func (j *rejectingSplitPreparationJournal) SubmitOperation(context.Context, gateway.ReplicatedOperationRecord) error {
	j.submits++
	return gateway.ErrReplicatedCatalogConflict
}

func TestGatewayHotSplitCompetingAdmissionCreatesNoChildFiles(t *testing.T) {
	catalog, source, profile, work := gatewayHotSplitFactoryFixture(t)
	entry := gatewaySplitSourceFixture(t, source, profile)
	manifest := gatewayReplicaControlManifest{SplitSources: []gatewaySplitSource{entry}}
	for i, replica := range source.Replicas {
		manifest.Shards = append(manifest.Shards, gateway.ReplicatedEndpoint{Node: replica.Node})
		manifest.SplitSnapshots = append(manifest.SplitSnapshots, "127.0.0.1:"+strconv.Itoa(9301+i))
	}
	factory, err := newGatewayHotSplitFactory(manifest, catalog)
	if err != nil {
		t.Fatal(err)
	}
	journal := &rejectingSplitPreparationJournal{}
	admission := hotshard.Admission{ID: [32]byte{1}, CatalogGeneration: catalog.Generation(), SplitCount: 1}
	admission.Splits[0] = work
	err = (hotshard.OperationSink{Catalog: catalog, Splits: factory, Journal: journal}).SubmitHotShardAdmission(t.Context(), admission)
	if !errors.Is(err, gateway.ErrReplicatedCatalogConflict) || journal.submits != 1 {
		t.Fatalf("admission result %v submits=%d", err, journal.submits)
	}
	for _, replica := range entry.Replicas {
		files, err := os.ReadDir(replica.Root)
		if err != nil || len(files) != 0 {
			t.Fatalf("unadmitted child created files under %s: %v", replica.Root, err)
		}
	}
}

type retryingCommittedPreparationClient struct {
	mu    sync.Mutex
	calls map[rafttransport.NodeID]int
}

func (c *retryingCommittedPreparationClient) Prepare(_ context.Context, node rafttransport.NodeID, p splitcontroller.ChildPreparation) (splitcontroller.ChildPrepareReceipt, error) {
	c.mu.Lock()
	c.calls[node]++
	attempt := c.calls[node]
	c.mu.Unlock()
	if attempt == 1 {
		return splitcontroller.ChildPrepareReceipt{}, splitcontroller.ErrRuntimeStoreOutcomeUnknown
	}
	return splitcontroller.NewChildPrepareReceipt(p, p.ReplicaTarget())
}
func TestGatewayCommittedPreparationSettlesExactRetriedReceipts(t *testing.T) {
	catalog, source, profile, work := gatewayHotSplitFactoryFixture(t)
	entry := gatewaySplitSourceFixture(t, source, profile)
	manifest := gatewayReplicaControlManifest{SplitSources: []gatewaySplitSource{entry}}
	for i, replica := range source.Replicas {
		manifest.Shards = append(manifest.Shards, gateway.ReplicatedEndpoint{Node: replica.Node})
		manifest.SplitSnapshots = append(manifest.SplitSnapshots, "127.0.0.1:"+strconv.Itoa(9301+i))
	}
	factory, err := newGatewayHotSplitFactory(manifest, catalog)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := factory.BuildHotSplitPlan(t.Context(), catalog, [32]byte{1}, work)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := splitcontroller.AppendPlanIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := splitcontroller.OpenPlanIntent(raw, catalog)
	if err != nil {
		t.Fatal(err)
	}
	client := &retryingCommittedPreparationClient{calls: make(map[rafttransport.NodeID]int)}
	if err := (gatewayCommittedChildPreparer{client: client}).PrepareCommittedPlan(t.Context(), reopened); err != nil {
		t.Fatal(err)
	}
	if len(client.calls) != gateway.ServingReplicaCount {
		t.Fatal("wrong preparation fanout")
	}
	for _, attempts := range client.calls {
		if attempts != 2 {
			t.Fatalf("retry attempts=%d", attempts)
		}
	}
}
