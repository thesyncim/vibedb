package gateway

import (
	"context"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
)

// Resolve from an independent catalog inventory, not by copying the caller's
// logical identity into a synthetic route. The latter hides borrowed-name drift.
type cloneLifetimeCatalog []ReplicatedRoute

func (catalog cloneLifetimeCatalog) ResolveDurableRequestTarget(_ context.Context, target DurableRequestLogicalTarget) (ReplicatedRoute, error) {
	for _, route := range catalog {
		if route.Distribution == target.Distribution && route.Shard == target.Shard {
			return route, nil
		}
	}
	return ReplicatedRoute{}, ErrDurableRequestConflict
}

func cloneLifetimeRecipe(t *testing.T) (*durableRequestRecipeStreamReader, DurableRequestLogicalProgram, cloneLifetimeCatalog) {
	t.Helper()
	key, program := durableLogicalStreamFixture(t, 2, 3)
	program.Identity.CoordinatorOrdinal = 0
	program.Targets[0].Distribution, program.Targets[0].Shard = "orders_a", "shard_a"
	program.Targets[1].Distribution, program.Targets[1].Shard = "orders_b", "shard_b"
	var err error
	program, err = SealDurableRequestLogicalProgram(program)
	if err != nil {
		t.Fatal(err)
	}
	measurement, pages := durableLogicalStreamBuild(t, key, program)
	reader, err := openDurableRequestRecipeStream(key, measurement.descriptor(), pages)
	if err != nil {
		t.Fatal(err)
	}
	base, _, _ := testReplicatedRouteCommand(t)
	catalog := make(cloneLifetimeCatalog, len(program.Targets))
	for index, target := range program.Targets {
		catalog[index], err = (distributedRunnerResolver{base: base}).ResolveDurableRequestTarget(t.Context(), target)
		if err != nil || !durableRequestRouteMatchesTarget(catalog[index], target) {
			t.Fatalf("catalog participant %d: %v", index, err)
		}
	}
	return reader, program, catalog
}

func TestDurableRequestOwnedClonesSurviveRecipeFrameReuse(t *testing.T) {
	reader, program, catalog := cloneLifetimeRecipe(t)
	if !reader.Next() {
		t.Fatal(reader.Err())
	}
	borrowed := reader.Current()
	owned := cloneDurableLogicalTarget(borrowed)
	borrowedRoute := catalog[0]
	borrowedRoute.Distribution, borrowedRoute.Shard = borrowed.Distribution, borrowed.Shard
	ownedRoute := cloneDurableRequestRoute(borrowedRoute)
	frame := &reader.frame[0]
	if !reader.Next() || frame != &reader.frame[0] || borrowed.Distribution != program.Targets[1].Distribution || borrowed.Shard != program.Targets[1].Shard {
		t.Fatalf("fixture must actually overwrite borrowed names in the same recipe frame: %v", reader.Err())
	}
	if !reflect.DeepEqual(owned, program.Targets[0]) {
		t.Errorf("owned participant drifted after Next: names=%s/%s group=%x", owned.Distribution, owned.Shard, owned.Group.GroupID)
	}
	if !reflect.DeepEqual(ownedRoute, catalog[0]) {
		t.Errorf("owned route drifted after Next: names=%s/%s group=%x", ownedRoute.Distribution, ownedRoute.Shard, ownedRoute.Group.GroupID)
	}
	for pass := 0; pass < 2; pass++ {
		if err := reader.Reset(); err != nil {
			t.Fatal(err)
		}
		for reader.Next() {
		}
		if reader.Err() != nil || !reader.Complete() || !reflect.DeepEqual(owned, program.Targets[0]) || !reflect.DeepEqual(ownedRoute, catalog[0]) {
			t.Fatalf("owned clones changed after reset/replay %d: %v", pass, reader.Err())
		}
	}
}

func TestDurableRequestMeasuredCoordinatorResolvesAfterRecipeReuse(t *testing.T) {
	reader, program, catalog := cloneLifetimeRecipe(t)
	progress := durableDistributedProgress{
		runner: &DurableRequestDistributedRunner{resolver: catalog},
		execution: DurableRequestTypedExecutionContext{
			Recipe:  DurableRequestRecipe{Identity: reader.Identity, TargetCount: reader.TargetCount},
			Targets: reader,
		},
	}
	descriptor, coordinator, route, err := progress.measureManifest(t.Context())
	if err != nil || descriptor.TargetCount != 2 {
		t.Fatalf("measure manifest: %v", err)
	}
	// Construct exact bytes from the authoritative catalog, so a drifting
	// coordinator cannot manufacture a self-consistent but wrong target.
	command := replicatedTransactionCommandHeader(catalog[0], program.Tenant, program.Identity.RetryHome, replication.ID128(program.Identity.ID), 1, 1)
	command.Kind, command.Batches = replication.CommandMutationBatch, program.Targets[0].Batches
	command.Fingerprint = nativeCommandFingerprint(command)
	raw, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := DurableRequestLifecycleRunner{resolver: catalog}
	resolved, err := lifecycle.resolveWave(t.Context(), DurableRequestWave{LogicalTarget: coordinator, Command: raw})
	if err != nil || !reflect.DeepEqual(resolved, catalog[0]) || !reflect.DeepEqual(route, catalog[0]) {
		t.Fatalf("measured coordinator no longer resolves to its exact original route: coordinator=%s/%s err=%v", coordinator.Distribution, coordinator.Shard, err)
	}
	// A genuine name/group substitution remains rejected after taking ownership.
	coordinator.Distribution, coordinator.Shard = distribution.DistributionName("orders_b"), distribution.ShardID("shard_b")
	if _, err := lifecycle.resolveWave(t.Context(), DurableRequestWave{LogicalTarget: coordinator, Command: raw}); err == nil {
		t.Fatal("foreign participant names accepted with original group authority")
	}
}
