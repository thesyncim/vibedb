package rf3bench

import (
	"bytes"
	"errors"
	"testing"
)

func TestWriteChaosTSVCanonicalRawOutcomes(t *testing.T) {
	firstDigest := [32]byte{1, 2, 3}
	report := ChaosReport{TimeoutNS: 30,
		Metadata: []Metadata{
			{Key: []byte("binary_sha256"), Value: []byte("abc")},
			{Key: []byte("test_name"), Value: []byte("TestServeRF3ShippedFaultHarness")},
		},
		Runs: []ChaosRun{
			{Ordinal: 1, ElapsedNS: 10, OutputBytes: 100, OutputSHA256: firstDigest,
				ExitCode: 0, ExactRun: true, Passed: true},
			{Ordinal: 2, ElapsedNS: 20, OutputBytes: 200, OutputSHA256: [32]byte{4},
				ExitCode: 1, ExactRun: true},
		},
	}
	var first, second bytes.Buffer
	if err := WriteChaosTSV(&first, report); err != nil {
		t.Fatal(err)
	}
	if err := WriteChaosTSV(&second, report); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatal("same chaos report produced different bytes")
	}
	for _, token := range [][]byte{
		[]byte("schema\tvibedb.rf3.chaos-evidence\t1\n"),
		[]byte("meta\tfault_harness\texternal-process\n"),
		[]byte("raw\t1\t10\t0\tfalse\ttrue\ttrue\t100\t010203"),
		[]byte("summary\tpassed\t1\tfailed\t1\n"),
	} {
		if !bytes.Contains(first.Bytes(), token) {
			t.Fatalf("output omits %q:\n%s", token, first.Bytes())
		}
	}
}

func TestChaosReportRejectsFalsePassingClaims(t *testing.T) {
	base := ChaosReport{TimeoutNS: 1, Runs: []ChaosRun{{Ordinal: 1, ElapsedNS: 1,
		ExitCode: 0, ExactRun: true, Passed: true}}}
	for name, mutate := range map[string]func(*ChaosReport){
		"missing timeout":     func(report *ChaosReport) { report.TimeoutNS = 0 },
		"wrong ordinal":       func(report *ChaosReport) { report.Runs[0].Ordinal = 2 },
		"passing timeout":     func(report *ChaosReport) { report.Runs[0].TimedOut = true },
		"passing nonzero":     func(report *ChaosReport) { report.Runs[0].ExitCode = 1 },
		"passing absent test": func(report *ChaosReport) { report.Runs[0].ExactRun = false },
		"reserved metadata": func(report *ChaosReport) {
			report.Metadata = []Metadata{{Key: []byte("fault_harness"), Value: []byte("in-process")}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			report := base
			report.Runs = append([]ChaosRun(nil), base.Runs...)
			mutate(&report)
			if err := WriteChaosTSV(&bytes.Buffer{}, report); !errors.Is(err, ErrInvalidReport) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
