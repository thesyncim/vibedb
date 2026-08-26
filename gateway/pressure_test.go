package gateway

import (
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/shardservice"
)

type recordingPressureObserver struct {
	calls int
	last  PressureObservation
}

func (observer *recordingPressureObserver) ObservePressure(observation PressureObservation) {
	observer.calls++
	observer.last = observation
}

func TestExecutorPressureObservationIsExactAndSynchronous(t *testing.T) {
	observer := &recordingPressureObserver{}
	executor := &Executor{pressure: observer}
	source := autosplit.SourceIdentity{Distribution: "data", Shard: "shard", AllocationGeneration: 3,
		Range:      distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		BucketBits: 20, RoutingVersion: 7, OwnershipEpoch: 9}
	scopes := []distributedtxn.IntentScope{{Start: 17, End: 18}}
	executor.observePressureCall(shardCall{pressureSource: source,
		req: &shardservice.ShardRequest{ExecutionMode: shardservice.ExecutionReadWrite, AccessScopes: scopes}})
	if observer.calls != 1 || observer.last.Source != source || !observer.last.Write ||
		len(observer.last.AccessScopes) != 1 || observer.last.AccessScopes[0] != scopes[0] {
		t.Fatalf("observation=%+v calls=%d", observer.last, observer.calls)
	}
	executor.observePressureCall(shardCall{req: &shardservice.ShardRequest{}})
	if observer.calls != 1 {
		t.Fatal("unfenced shard call entered pressure intake")
	}
}

func TestPressureSourceUsesPinnedManifestOrdinalAndFullFence(t *testing.T) {
	manifest, err := distribution.NewManifest("data", 7, []distribution.Shard{
		{ID: "left", AllocationGeneration: 3,
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Point: distribution.KeyspacePoint{distribution.KeyspaceWidth - 1: 0x80}}},
			Leaders: []distribution.EndpointID{"left-node"}, Epoch: 9},
		{ID: "right", AllocationGeneration: 4,
			Range:   distribution.KeyRange{Start: distribution.KeyspacePoint{distribution.KeyspaceWidth - 1: 0x80}, End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"right-node"}, Epoch: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	target, ok := manifest.ResolvePointTarget(distribution.KeyspacePoint{distribution.KeyspaceWidth - 1: 0x90})
	if !ok || target.ManifestOrdinal != 1 {
		t.Fatalf("target=%+v ok=%v", target, ok)
	}
	source := pressureSourceForTarget(manifest, 20, target)
	if source.Shard != "right" || source.Range != (distribution.KeyRange{
		Start: distribution.KeyspacePoint{distribution.KeyspaceWidth - 1: 0x80}, End: distribution.KeyspaceEnd{Max: true},
	}) {
		t.Fatalf("source=%+v", source)
	}
	target.OwnershipEpoch++
	if got := pressureSourceForTarget(manifest, 20, target); got != (autosplit.SourceIdentity{}) {
		t.Fatalf("stale target produced source=%+v", got)
	}
}
