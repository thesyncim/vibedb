package gateway

import (
	"context"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"testing"
)

func TestCatalogServiceFencesIncludeDurableSQLScopes(t *testing.T) {
	authority, _, snapshot := newCatalogAuthorityFixture(t)
	fences, generation, err := authority.CatalogServiceFences(context.Background())
	if err != nil || generation != snapshot.Generation() {
		t.Fatalf("fences generation=%d err=%v", generation, err)
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
			pin, recovery := false, false
			for _, fence := range fences {
				if fence.Action == serviceauthz.ServiceActionGatewayExecutionPin {
					pin = true
				}
				if fence.Action == serviceauthz.ServiceActionGatewayTransactionRecovery {
					recovery = true
				}
			}
			if pin != test.ledger || recovery != (test.ledger || test.placed) {
				t.Fatalf("pin=%t recovery=%t", pin, recovery)
			}
		})
	}
}
