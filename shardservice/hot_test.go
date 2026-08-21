package shardservice

import (
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
)

func TestNewServerFencesHotRecorderToExactOwnership(t *testing.T) {
	db := openDB(t, t.TempDir()+"/hot.vdb")
	source := hotTestSource()
	source.RoutingVersion++
	recorder, err := autosplit.NewRecorder(source, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewServer(db, testOwner(), Options{HotRecorder: recorder}); err == nil {
		t.Fatal("NewServer accepted a stale hot-recorder routing version")
	}

	// Recorder validation happens before the physical store claim; a corrected
	// identity can immediately claim and serve the same database.
	recorder, err = autosplit.NewRecorder(hotTestSource(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(db, testOwner(), Options{HotRecorder: recorder})
	if err != nil {
		t.Fatalf("NewServer after rejected recorder: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
}

func TestShardHotRecorderAttributesExactVirtualBucket(t *testing.T) {
	source := hotTestSource()
	recorder, err := autosplit.NewRecorder(source, 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	server, _ := newServer(t, Options{HotRecorder: recorder})
	conn := dial(t, server)
	exec(t, conn, ownedRequest(ddlDocs))
	if _, err := recorder.Rotate(2); err != nil {
		t.Fatal(err)
	}

	scope := []distributedtxn.IntentScope{{Start: 17, End: 18}}
	insert := ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("hot"), StringParam("bucket"), NumberParam("7"),
	)
	insert.BucketBits, insert.AccessScopes = source.BucketBits, scope
	exec(t, conn, insert)
	read := ownedRequest(`SELECT id, name, n FROM docs WHERE id = ?`, StringParam("hot"))
	read.BucketBits, read.AccessScopes = source.BucketBits, scope
	exec(t, conn, read)

	window, err := recorder.Rotate(3)
	if err != nil {
		t.Fatal(err)
	}
	pulse := window.Pulse()
	if pulse.Sequence != 2 || pulse.Samples != 2 ||
		pulse.Total[autosplit.ResourceRequests] != 2 ||
		pulse.Total[autosplit.ResourceLatencyDebt] == 0 ||
		pulse.Total[autosplit.ResourceIO] == 0 || pulse.Bounded != 2 || pulse.Unbounded != 0 {
		t.Fatalf("hot request pulse = %+v", pulse)
	}

	capacities := autosplit.CapacitySet{Source: source, WindowSequence: pulse.Sequence}
	for resource := range autosplit.ResourceCount {
		if pulse.Total[resource] == 0 {
			continue
		}
		capacities.Current[resource] = 1
		capacities.Left[resource] = 1
		capacities.Right[resource] = 1
		capacities.Isolated[resource] = pulse.Total[resource] * 2
	}
	recommendation := autosplit.Recommend(window, capacities, autosplit.Policy{
		TriggerPressurePPM: 1, MaxChildPressurePPM: 750_000,
		CelebritySharePPM: 1,
	})
	want, ok := distribution.VirtualBucketRange(17, source.BucketBits)
	if !ok {
		t.Fatal("test virtual bucket is invalid")
	}
	if recommendation.Kind != autosplit.RecommendationIsolateBucket ||
		recommendation.HotBucketStart != want.Start {
		t.Fatalf("hot bucket recommendation = %+v, want start %x", recommendation, want.Start)
	}
}

func hotTestSource() autosplit.SourceIdentity {
	owner := testOwner()
	var end distribution.KeyspacePoint
	end[0] = 0x80
	return autosplit.SourceIdentity{
		Distribution: owner.Distribution, Shard: owner.Shard,
		AllocationGeneration: owner.AllocationGeneration,
		Range:                distribution.KeyRange{End: distribution.KeyspaceEnd{Point: end}},
		BucketBits:           sourceBucketBits,
		RoutingVersion:       owner.RoutingVersion, OwnershipEpoch: owner.Epoch,
	}
}

const sourceBucketBits uint8 = 8
