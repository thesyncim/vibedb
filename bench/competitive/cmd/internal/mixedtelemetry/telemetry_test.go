package mixedtelemetry

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestWriteParseRoundTripAmidDiagnostics(t *testing.T) {
	want := Record{
		Engine: "vibedb", Clients: 8, Available: true,
		RuntimeTotalAllocBytes: 11, RuntimeMallocs: 12,
		ScalarPatchAttempts: 21, ScalarPatchAccepts: 20,
		PublishGroups: 5, PublishGroupMaxBefore: 2, PublishGroupMax: 8,
		JournalAcks: 30, JournalSyncs: 6,
		JournalGroupMaxBefore: 3, JournalGroupMax: 7,
		JournalDeltaRecords: 9, JournalDeltaBytes: 4096,
		JournalDeltaFallbacks: 1, DeviceBytes: 8192,
		Histograms: map[string]Histogram{
			"stripe-wait-ns": {
				Count: 2, Sum: 30, MaxBefore: 9, Max: 20,
				Buckets: []uint64{0, 1, 1},
			},
		},
	}
	var encoded bytes.Buffer
	encoded.WriteString("ordinary diagnostic before\n")
	if err := Write(&encoded, want); err != nil {
		t.Fatal(err)
	}
	encoded.WriteString("ordinary diagnostic after\n")
	got, err := Parse(encoded.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	want.Schema = Schema
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

func TestParseRejectsMissingDuplicateAndUnknownSchema(t *testing.T) {
	if _, err := Parse([]byte("diagnostic only\n")); err == nil {
		t.Fatal("missing record was accepted")
	}
	var encoded bytes.Buffer
	if err := Write(&encoded, Record{Engine: "vibedb"}); err != nil {
		t.Fatal(err)
	}
	one := bytes.Clone(encoded.Bytes())
	encoded.Write(one)
	if _, err := Parse(encoded.Bytes()); err == nil {
		t.Fatal("duplicate records were accepted")
	}
	unknown := strings.Replace(encoded.String(), `"schema":1`, `"schema":99`, 1)
	unknown = strings.SplitAfter(unknown, "\n")[0]
	if _, err := Parse([]byte(unknown)); err == nil {
		t.Fatal("unknown schema was accepted")
	}
}

func TestMetricsOmitDurableCountersWhenUnavailable(t *testing.T) {
	without := (Record{ScalarPatchAttempts: 99}).Metrics()
	if len(without) != 2 || without[0].Scope != "runtime" || without[1].Scope != "runtime" {
		t.Fatalf("metrics without durable stats = %+v", without)
	}
	with := (Record{Available: true, ScalarPatchAttempts: 99}).Metrics()
	found := false
	for _, metric := range with {
		if metric.Name == "scalar-patch-attempts" && metric.Value == 99 {
			found = true
		}
	}
	if !found {
		t.Fatalf("durable metrics omitted scalar patch attempts: %+v", with)
	}
}
