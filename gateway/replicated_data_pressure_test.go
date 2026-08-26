package gateway

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
)

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
		len(observer.last.AccessScopes) != 0 {
		t.Fatalf("native pressure calls=%d observation=%+v want_source=%+v",
			observer.calls, observer.last, want)
	}
}

func BenchmarkReplicatedDataReaderPressureIntake(b *testing.B) {
	reader := &ReplicatedDataReader{pressure: &recordingPressureObserver{}}
	source := autosplitSourceForBenchmark()
	b.ReportAllocs()
	for b.Loop() {
		reader.observeReplicatedPressure(source, 1, false)
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
