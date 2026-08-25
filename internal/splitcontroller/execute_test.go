package splitcontroller

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

func TestAppendSourceSealBuildsExactAllocationFreeBinaryTransition(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = 2
	artifacts := testArtifactSet(t, plan, state)
	tail, err := plan.partitioner.InitialTailCursor(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 0, replicatedstate.MaxOwnershipTransitionBytes)
	encoded, err := plan.AppendSourceSeal(buffer[:0], state, tail, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	view, err := replicatedstate.OpenOwnershipTransition(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if view.ExpectedReplicaSetVersion != state.ReplicaSetVersion ||
		view.SourceMember != 1 || view.TargetMember != 2 ||
		view.ToOwnershipEpoch != state.Binding.OwnershipEpoch+1 ||
		view.ToRoutingVersion != state.Binding.RoutingVersion+1 ||
		view.ToRouteGeneration != state.Binding.RouteGeneration+1 {
		t.Fatalf("seal view = %+v", view)
	}
	if !raceDetectorEnabled {
		if allocations := testing.AllocsPerRun(1_000, func() {
			var appendErr error
			encoded, appendErr = plan.AppendSourceSeal(buffer[:0], state, tail, 1, 2)
			if appendErr != nil {
				panic(appendErr)
			}
		}); allocations != 0 {
			t.Fatalf("warm seal encode allocations = %v, want 0", allocations)
		}
	}

	prefix := []byte("unchanged")
	stale := state
	stale.Binding.RouteGeneration--
	got, err := plan.AppendSourceSeal(prefix, stale, tail, 1, 2)
	if !errors.Is(err, ErrTopologyConflict) || !bytes.Equal(got, prefix) {
		t.Fatalf("stale seal = %q, %v", got, err)
	}
	got, err = plan.AppendSourceSeal(prefix, state, tail, 1, 1)
	if !errors.Is(err, ErrTopologyConflict) || !bytes.Equal(got, prefix) {
		t.Fatalf("same-member seal = %q, %v", got, err)
	}
	advanced := state
	advanced.Applied++
	advanced.LastEntryDigest = [32]byte{1}
	got, err = plan.AppendSourceSeal(prefix, advanced, tail, 1, 2)
	if !errors.Is(err, ErrTopologyConflict) || !bytes.Equal(got, prefix) {
		t.Fatalf("tail-behind seal = %q, %v", got, err)
	}
	withSession := state
	withSession.SessionCount = 1
	withSession.SessionSlotCount = 1
	got, err = plan.AppendSourceSeal(prefix, withSession, tail, 1, 2)
	if !errors.Is(err, ErrSessionTransferRequired) || !bytes.Equal(got, prefix) {
		t.Fatalf("session-bearing seal = %q, %v", got, err)
	}
}

func TestPublishBeforePruneCrashMatrixNeverLosesOrDoubleRoutesRows(t *testing.T) {
	plan, current, _, _ := testPlan(t)
	target := plan.targetSnapshotForTest(t)
	currentManifest, _ := current.Manifest(plan.source.Distribution)
	targetManifest, _ := target.Manifest(plan.source.Distribution)
	points := []distribution.KeyspacePoint{{0x00}, {0x40}, {0x7f}, {0x80}, {0xc0}, {0xff}}
	rightShard, ok := targetManifest.ResolvePoint(distribution.KeyspacePoint{0x80})
	if !ok || rightShard == plan.source.Shard {
		t.Fatalf("right child route=%q ok=%t", rightShard, ok)
	}
	type physicalRows map[distribution.ShardID]map[distribution.KeyspacePoint]bool
	prepared := physicalRows{
		plan.source.Shard: {},
		rightShard:        {},
	}
	for _, point := range points {
		prepared[plan.source.Shard][point] = true
		if point[0] >= 0x80 {
			prepared[rightShard][point] = true
		}
	}
	cloneRows := func(source physicalRows) physicalRows {
		cloned := make(physicalRows, len(source))
		for shard, rows := range source {
			cloned[shard] = make(map[distribution.KeyspacePoint]bool, len(rows))
			for point, present := range rows {
				cloned[shard][point] = present
			}
		}
		return cloned
	}
	partiallyPruned := cloneRows(prepared)
	delete(partiallyPruned[plan.source.Shard], distribution.KeyspacePoint{0x80})
	pruned := cloneRows(prepared)
	for _, point := range points {
		if point[0] >= 0x80 {
			delete(pruned[plan.source.Shard], point)
		}
	}
	for _, phase := range []struct {
		name     string
		manifest *distribution.Manifest
		rows     physicalRows
	}{
		{"prepared_crash", currentManifest, prepared},
		{"published_crash", targetManifest, prepared},
		{"drained_crash", targetManifest, prepared},
		{"partial_prune_crash", targetManifest, partiallyPruned},
		{"prune_complete_crash", targetManifest, pruned},
	} {
		t.Run(phase.name, func(t *testing.T) {
			for _, point := range points {
				authorities := 0
				var routed distribution.ShardID
				for ordinal := 0; ordinal < phase.manifest.ShardCount(); ordinal++ {
					metadata, metadataOK := phase.manifest.ShardMetadataAt(ordinal)
					if metadataOK && metadata.Range.Contains(point) {
						authorities++
						routed = metadata.ID
					}
				}
				if authorities != 1 || !phase.rows[routed][point] {
					t.Fatalf("point=%x authorities=%d routed=%q present=%t",
						point, authorities, routed, phase.rows[routed][point])
				}
			}
		})
	}
}

func TestBuildCatalogTransitionRefusesMissingProofs(t *testing.T) {
	plan, current, _, _ := testPlan(t)
	if _, err := plan.BuildCatalogTransition(
		current, testSourceState(plan),
		rangesplit.CutoverCertificate{},
	); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("missing proof error = %v", err)
	}
}
