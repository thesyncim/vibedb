package rf3bench

import (
	"bytes"
	"errors"
	"testing"
)

func TestWriteTSVCanonicalRawDistributionAndCounters(t *testing.T) {
	report := Report{
		Config: Config{Clients: 2, Operations: 3, Warmup: 1, Seed: 7,
			ElapsedNS: 1000, Workload: WorkloadMixed},
		Metadata: []Metadata{{Key: []byte("commit"), Value: []byte("abc")}},
		Samples: []Sample{
			{Ordinal: 1, Client: 0, ClientSequence: 1, Operation: OperationWrite,
				LatencyNS: 30, Applied: 4, Commit: 4, Checkpoint: 3, PayloadBytes: 20},
			{Ordinal: 2, Client: 1, ClientSequence: 1, Operation: OperationRead,
				LatencyNS: 10, Applied: 4, Commit: 4, Checkpoint: 4, Found: true, PayloadBytes: 20},
			{Ordinal: 3, Client: 0, ClientSequence: 2, Operation: OperationWrite,
				LatencyNS: 20, Applied: 5, Commit: 5, Checkpoint: 4, PayloadBytes: 20},
		},
		Counters: []Counter{{Scope: []byte("network"), Name: []byte("sent_bytes"), Before: 10, After: 42}},
	}
	var first, second bytes.Buffer
	if err := WriteTSV(&first, report); err != nil {
		t.Fatal(err)
	}
	if err := WriteTSV(&second, report); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatal("same report produced different bytes")
	}
	for _, token := range [][]byte{
		[]byte("schema\tvibedb.rf3.evidence\t1\n"),
		[]byte("meta\tdurability\tpower-safe\n"),
		[]byte("raw\t2\t1\t1\tread\t10\t0\t4\t4\t4\ttrue\t20\n"),
		[]byte("summary\twrite\t2\t20\t30\t30\t30\t30\n"),
		[]byte("counter\tnetwork\tsent_bytes\t10\t42\t32\n"),
	} {
		if !bytes.Contains(first.Bytes(), token) {
			t.Fatalf("output omits %q:\n%s", token, first.Bytes())
		}
	}
}

func TestReportRejectsNoncanonicalAndRegressingEvidence(t *testing.T) {
	base := Report{Config: Config{Clients: 1, Operations: 1, ElapsedNS: 1, Workload: WorkloadRead},
		Samples: []Sample{{Ordinal: 1, ClientSequence: 1, LatencyNS: 1,
			Operation: OperationRead, Applied: 1, Commit: 1}}}
	for name, mutate := range map[string]func(*Report){
		"sample ordinal": func(report *Report) { report.Samples[0].Ordinal = 2 },
		"metadata order": func(report *Report) { report.Metadata = []Metadata{{Key: []byte("z")}, {Key: []byte("a")}} },
		"counter regression": func(report *Report) {
			report.Counters = []Counter{{Scope: []byte("storage"), Name: []byte("bytes"), Before: 2, After: 1}}
		},
		"control byte":      func(report *Report) { report.Metadata = []Metadata{{Key: []byte("host"), Value: []byte("bad\nvalue")}} },
		"reserved metadata": func(report *Report) { report.Metadata = []Metadata{{Key: []byte("topology"), Value: []byte("other")}} },
	} {
		t.Run(name, func(t *testing.T) {
			report := base
			report.Samples = append([]Sample(nil), base.Samples...)
			mutate(&report)
			if err := WriteTSV(&bytes.Buffer{}, report); !errors.Is(err, ErrInvalidReport) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
