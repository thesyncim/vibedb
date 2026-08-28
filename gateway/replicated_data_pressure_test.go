package gateway

import (
	"context"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
)

type nativePointPressureObserver struct {
	mu           sync.Mutex
	observations []PressureObservation
}

func (observer *nativePointPressureObserver) ObservePressure(observation PressureObservation) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.observations = append(observer.observations, observation)
}

func TestReplicatedDataReaderBatchesPreserveEveryPointPlacement(t *testing.T) {
	for _, kind := range []string{"batch", "scatter", "sql"} {
		t.Run(kind, func(t *testing.T) {
			fixture, _ := sameGroupSQLReadFixture(t)
			if kind != "batch" {
				fixture = newScatterCatalogFixture(t, 3, 5)
			}
			reader := newScatterReader(t, fixture, &scatterReadClient{}, nil, 3)
			observer := &nativePointPressureObserver{}
			if !reader.InstallPressureObserver(observer) {
				t.Fatal("install pressure observer")
			}
			var err error
			switch kind {
			case "batch":
				var result ReplicatedTableBatchReadResult
				result, err = reader.ReadBatch(t.Context(), fixture.request)
				result.Release()
			case "scatter":
				var result ReplicatedTableScatterReadResult
				result, err = reader.ReadScatterBatch(t.Context(), fixture.request)
				result.Release()
			case "sql":
				var result ReplicatedTableScatterReadResult
				result, err = reader.ReadSQLBatch(t.Context(), scatterSQLReadRequest(fixture))
				result.Release()
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(observer.observations) != len(fixture.routes) {
				t.Fatalf("samples=%d want=%d", len(observer.observations), len(fixture.routes))
			}
			for _, resolved := range fixture.routes {
				source := replicatedDataPressureSource(fixture.snapshot, resolved.Route)
				matches := 0
				for _, sample := range observer.observations {
					if sample.Source == source && sample.Point == resolved.Point && sample.HasPoint &&
						!sample.Write && len(sample.AccessScopes) == 0 {
						matches++
					}
				}
				if matches != 1 {
					t.Fatalf("exact point %x source=%+v matches=%d", resolved.Point, source, matches)
				}
			}
		})
	}
}

func TestReplicatedDataReaderNativeReadPublishesBoundedPressure(t *testing.T) {
	client := &publicPointReadClient{value: []byte(`{"id":"customer-17"}`),
		wantRelation: 1, wantMaxValue: 4 << 20, wantMinimum: 1}
	reader, _, key, resolved := testReplicatedDataReader(t, client)
	observer := &recordingPressureObserver{}
	if !reader.InstallPressureObserver(observer) || reader.InstallPressureObserver(observer) {
		t.Fatal("native pressure observer startup binding was not single-assignment")
	}
	result, err := reader.Read(context.Background(), ReplicatedTableReadRequest{
		Table: []byte("messages"), Key: key, Consistency: ReplicatedDataReadLinearizable,
	})
	result.Release()
	if err != nil {
		t.Fatal(err)
	}
	want := replicatedDataPressureSource(reader.catalog.Current(), resolved.Route)
	if observer.calls != 1 || observer.last.Source != want || observer.last.Write ||
		len(observer.last.AccessScopes) != 0 || !observer.last.HasPoint || observer.last.Point != resolved.Point {
		t.Fatalf("native pressure calls=%d observation=%+v want_source=%+v",
			observer.calls, observer.last, want)
	}
}

func BenchmarkReplicatedDataReaderPressureIntake(b *testing.B) {
	reader := &ReplicatedDataReader{pressure: &recordingPressureObserver{}}
	source := autosplitSourceForBenchmark()
	points := []distribution.KeyspacePoint{{0x80}}
	b.ReportAllocs()
	for b.Loop() {
		reader.observeReplicatedPressure(source, points)
	}
}

func autosplitSourceForBenchmark() (source autosplit.SourceIdentity) {
	source.Distribution = "data"
	source.Shard = "all"
	source.AllocationGeneration = 1
	source.BucketBits = 20
	source.RoutingVersion = 1
	source.OwnershipEpoch = 1
	source.Range.End.Max = true
	return source
}
