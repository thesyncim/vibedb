package mixedtelemetry

import (
	"bytes"
	"io"
	"math"
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
		JournalDeltaFallbacks: 1, DurabilityPayloadKnown: true, DurabilityPayloadBytes: 8192,
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

func TestWriteParsePreservesExactUint64AndCanonicalBytes(t *testing.T) {
	record := Record{
		Engine: "vibedb", RuntimeTotalAllocBytes: math.MaxUint64,
		Histograms: map[string]Histogram{
			"z": {Count: math.MaxUint64, Buckets: []uint64{0, math.MaxUint64}},
			"a": {Sum: math.MaxUint64},
		},
	}
	var first, second bytes.Buffer
	if err := Write(&first, record); err != nil {
		t.Fatal(err)
	}
	if err := Write(&second, record); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatalf("noncanonical output:\n%s\n%s", first.Bytes(), second.Bytes())
	}
	got, err := Parse(first.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got.RuntimeTotalAllocBytes != math.MaxUint64 ||
		got.Histograms["z"].Count != math.MaxUint64 ||
		got.Histograms["z"].Buckets[1] != math.MaxUint64 {
		t.Fatalf("uint64 precision changed: %+v", got)
	}
}

func TestParseRejectsNoncanonicalSchemaAndBounds(t *testing.T) {
	tests := [][]byte{
		[]byte(Prefix + `{"schema":1,"Engine":"vibedb"}` + "\n"),
		[]byte(Prefix + `{"schema":1,"unknown":0}` + "\n"),
		[]byte(Prefix + `{"schema":1,"histograms":{"x":{"buckets":[[[[0]]]]}}}` + "\n"),
	}
	for _, input := range tests {
		if _, err := Parse(input); err == nil {
			t.Fatalf("invalid telemetry accepted: %.160q", input)
		}
	}
	oversized := append([]byte(Prefix), bytes.Repeat([]byte{' '}, maxTelemetryJSONBytes+1)...)
	oversized = append(oversized, '\n')
	if _, err := Parse(oversized); err == nil {
		t.Fatal("oversized telemetry accepted")
	}
}

func TestWriteParseAcceptsExactMaximumPayload(t *testing.T) {
	record := Record{Schema: Schema}
	base, err := recordEncoder.AppendJSON(nil, &record)
	if err != nil {
		t.Fatal(err)
	}
	if len(base) >= maxTelemetryJSONBytes {
		t.Fatalf("base telemetry payload = %d, limit %d", len(base), maxTelemetryJSONBytes)
	}
	record.Engine = strings.Repeat("x", maxTelemetryJSONBytes-len(base))
	exact, err := recordEncoder.AppendJSON(nil, &record)
	if err != nil {
		t.Fatal(err)
	}
	if len(exact) != maxTelemetryJSONBytes {
		t.Fatalf("generated payload = %d bytes, want %d", len(exact), maxTelemetryJSONBytes)
	}

	var wire bytes.Buffer
	if err := Write(&wire, record); err != nil {
		t.Fatalf("Write exact maximum payload: %v", err)
	}
	if got := len(wire.Bytes()) - len(prefixBytes) - 1; got != maxTelemetryJSONBytes {
		t.Fatalf("wire payload = %d bytes, want %d", got, maxTelemetryJSONBytes)
	}
	decoded, err := Parse(wire.Bytes())
	if err != nil {
		t.Fatalf("Parse exact maximum payload: %v", err)
	}
	if decoded.Engine != record.Engine {
		t.Fatalf("decoded maximum Engine length = %d, want %d", len(decoded.Engine), len(record.Engine))
	}
}

func TestWriteRejectsOversizeAndShortSink(t *testing.T) {
	huge := Record{Engine: strings.Repeat("x", maxTelemetryJSONBytes)}
	if err := Write(io.Discard, huge); err == nil {
		t.Fatal("oversized record was written")
	}
	if err := Write(shortWriter{}, Record{Engine: "vibedb"}); err != io.ErrShortWrite {
		t.Fatalf("short sink error = %v, want %v", err, io.ErrShortWrite)
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

func TestMetricsOmitDurableCountersWhenUnavailable(t *testing.T) {
	without := (Record{ScalarPatchAttempts: 99}).Metrics()
	if len(without) != 4 || without[0].Scope != "runtime" || without[1].Scope != "runtime" ||
		without[2] != (Metric{Scope: "durability", Name: "payload-known"}) ||
		without[3] != (Metric{Scope: "durability", Name: "payload-bytes"}) {
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
