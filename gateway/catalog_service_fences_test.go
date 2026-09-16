package gateway

import (
	"context"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestCatalogServiceFencesIncludeDurableSQLScopes(t *testing.T) {
	authority, _, snapshot := newCatalogAuthorityFixture(t)
	fences, generation, err := authority.RefreshCatalogServiceFences(context.Background())
	if err != nil || generation != snapshot.Generation() {
		t.Fatalf("refresh fences generation=%d err=%v", generation, err)
	}
	cached, cachedGeneration, err := authority.CatalogServiceFences(context.Background())
	if err != nil || cachedGeneration != generation || len(cached) != len(fences) {
		t.Fatalf("cached fences generation=%d len=%d err=%v", cachedGeneration, len(cached), err)
	}
	descriptor := snapshot.ReplicatedShardDescriptors()[0]
	var relation [16]byte
	copy(relation[:], descriptor.Command.RelationManifestDigest[:16])
	for _, want := range []struct {
		action    serviceauthz.ServiceAction
		operation serviceauthz.ServiceOperation
	}{
		{serviceauthz.ServiceActionGatewayExecutionPin, serviceauthz.ServiceOperationExecutionPin},
		{serviceauthz.ServiceActionGatewayTransactionRecovery, serviceauthz.ServiceOperationTransactionRecovery},
	} {
		count := 0
		for _, fence := range fences {
			if fence.Action != want.action {
				continue
			}
			count++
			if fence.Operation != want.operation || fence.Group != descriptor.Group || fence.Relation != relation || fence.IntentID != descriptor.Command.RelationManifestDigest || fence.FenceDigest != descriptor.Command.RelationManifestDigest {
				t.Fatal("durable SQL scope differs from the committed group manifest")
			}
		}
		if count != 1 {
			t.Fatalf("action=%v exact scopes=%d", want.action, count)
		}
	}
}

func TestCatalogServiceFencesExcludeUnprovenSQLRoles(t *testing.T) {
	for _, test := range []struct {
		name           string
		ledger, placed bool
	}{
		{"unplaced-nonledger", false, false}, {"placed-data", false, true}, {"unplaced-ledger", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			authority, _, snapshot := newCatalogAuthorityFixture(t)
			config := cloneConfig(snapshot.config)
			if !test.placed {
				config.Placements = nil
			}
			descriptors := snapshot.ReplicatedShardDescriptors()
			// Keep the data group distinct from the fixture catalog route.
			descriptors[0].Group.GroupID[0] ^= 0x40
			if !test.ledger {
				descriptors[0].RequestLedgerRanges = nil
			}
			scoped, err := NewSnapshotWithReplicatedMetadata(config, snapshot.endpoints, snapshot.Generation(), nil, nil, descriptors)
			if err != nil {
				t.Fatal(err)
			}
			authority.holder = NewCatalogHolder(scoped)
			fences, _, err := authority.CatalogServiceFences(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			pin, recovery, topologyRead, topologyWrite := false, false, false, false
			for _, fence := range fences {
				if fence.Group != descriptors[0].Group {
					continue
				}
				if fence.Action == serviceauthz.ServiceActionGatewayCatalogRead {
					topologyRead = true
				}
				if fence.Action == serviceauthz.ServiceActionGatewayCatalogWrite {
					topologyWrite = true
				}
				if fence.Action == serviceauthz.ServiceActionGatewayExecutionPin {
					pin = true
				}
				if fence.Action == serviceauthz.ServiceActionGatewayTransactionRecovery {
					recovery = true
				}
			}
			if topologyRead != test.placed || topologyWrite != test.placed {
				t.Fatalf("topology read=%t write=%t placed=%t", topologyRead, topologyWrite, test.placed)
			}
			if pin != test.ledger || recovery != (test.ledger || test.placed) {
				t.Fatalf("pin=%t recovery=%t", pin, recovery)
			}
		})
	}
}

// Keep the directory resource bound tied to the real catalog expansion path.
// Each placed replicated group owns catalog read/write and recovery scopes;
// this deliberately crosses the former action-shaped 32-entry ceiling.
func TestCatalogServiceFencesSupportCatalogBeyondThirtyTwoResources(t *testing.T) {
	authority, _, snapshot := newCatalogAuthorityFixture(t)
	config := cloneConfig(snapshot.config)
	base := snapshot.ReplicatedShardDescriptors()[0]
	baseManifest := config.Manifests[0]
	shards := make([]distribution.Shard, 0, baseManifest.ShardCount())
	for index := 0; index < baseManifest.ShardCount(); index++ {
		shard, ok := baseManifest.ShardInfo(index)
		if !ok {
			t.Fatalf("missing fixture shard %d", index)
		}
		shards = append(shards, shard)
	}
	descriptors := []ReplicatedShardDescriptor{base}
	for index := 0; index < 11; index++ {
		name := distribution.DistributionName(fmt.Sprintf("fence_growth_%02d", index))
		manifest, err := distribution.NewManifest(name, baseManifest.Version(), shards)
		if err != nil {
			t.Fatal(err)
		}
		config.Manifests = append(config.Manifests, manifest)
		config.Distributions = append(config.Distributions, distribution.DistributionSpec{Name: name, Arity: 1, MapperVersion: 1})
		config.Placements = append(config.Placements, distribution.TablePlacement{Table: fmt.Sprintf("fence_growth_%02d", index), Distribution: name, Columns: []string{"/tenant_id"}})
		descriptor := base
		descriptor.Distribution = name
		descriptor.Group.GroupID[0] = byte(index + 1)
		descriptor.Group.ShardIncarnation[0] = byte(index + 1)
		descriptor.Command.RelationManifestDigest[0] = byte(index + 1)
		descriptor.RangeIdentity[0] = byte(index + 1)
		descriptor.LineageDigest[0] = byte(index + 1)
		descriptor.ForwardingRuleDigest[0] = byte(index + 1)
		// The placement alone grants catalog read/write and transaction recovery;
		// avoid inventing overlapping ledger ranges in this multi-table fixture.
		descriptor.RequestLedgerRanges = nil
		descriptors = append(descriptors, descriptor)
	}
	grown, err := NewSnapshotWithReplicatedMetadata(config, snapshot.endpoints, snapshot.Generation()+1, nil, nil, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	authority.holder = NewCatalogHolder(grown)
	fences, generation, err := authority.CatalogServiceFences(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if generation != grown.Generation() || len(fences) <= 32 {
		t.Fatalf("catalog generation=%d fences=%d; expected real catalog fence set beyond legacy bound", generation, len(fences))
	}
	for _, fence := range fences {
		if fence.Action == 0 || fence.Operation == 0 || fence.Group == (raftmember.GroupKey{}) || fence.FenceDigest == ([32]byte{}) {
			t.Fatalf("catalog builder emitted incomplete fence: %+v", fence)
		}
	}
}
