package rf3bench

import (
	"bytes"
	"errors"
	"testing"
)

func TestWriteChaosTSVCanonicalRawOutcomes(t *testing.T) {
	firstDigest := [32]byte{1, 2, 3}
	qualification := validQualification()
	report := ChaosReport{TimeoutNS: 30,
		Metadata: []Metadata{
			{Key: []byte("binary_sha256"), Value: []byte("abc")},
			{Key: []byte("test_name"), Value: []byte("TestServeRF3ShippedFaultHarness")},
		},
		Runs: []ChaosRun{
			{Ordinal: 1, ElapsedNS: 10, OutputBytes: 100, OutputSHA256: firstDigest,
				ExitCode: 0, ExactRun: true, QualificationExact: true, Qualification: qualification, Passed: true},
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
		[]byte("schema\tvibedb.rf3.chaos-evidence\t2\n"),
		[]byte("meta\tfault_harness\texternal-process\n"),
		[]byte("raw\t1\t10\t0\tfalse\ttrue\ttrue\ttrue\t100\t010203"),
		[]byte("summary\tpassed\t1\tfailed\t1\n"),
	} {
		if !bytes.Contains(first.Bytes(), token) {
			t.Fatalf("output omits %q:\n%s", token, first.Bytes())
		}
	}
}

func TestChaosReportRejectsFalsePassingClaims(t *testing.T) {
	base := ChaosReport{TimeoutNS: 1, Runs: []ChaosRun{{Ordinal: 1, ElapsedNS: 1,
		ExitCode: 0, ExactRun: true, QualificationExact: true, Qualification: validQualification(), Passed: true}}}
	for name, mutate := range map[string]func(*ChaosReport){
		"missing timeout":               func(report *ChaosReport) { report.TimeoutNS = 0 },
		"wrong ordinal":                 func(report *ChaosReport) { report.Runs[0].Ordinal = 2 },
		"passing timeout":               func(report *ChaosReport) { report.Runs[0].TimedOut = true },
		"passing nonzero":               func(report *ChaosReport) { report.Runs[0].ExitCode = 1 },
		"passing absent test":           func(report *ChaosReport) { report.Runs[0].ExactRun = false },
		"passing absent qualification":  func(report *ChaosReport) { report.Runs[0].QualificationExact = false },
		"passing invalid qualification": func(report *ChaosReport) { report.Runs[0].Qualification.WaiterWaves = 0 },
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

func TestQualificationTSVRoundTripAndRejectsDrift(t *testing.T) {
	want := validQualification()
	var encoded bytes.Buffer
	if err := WriteQualificationTSV(&encoded, want); err != nil {
		t.Fatal(err)
	}
	got, err := ParseQualificationTSV(encoded.Bytes())
	if err != nil || got != want {
		t.Fatalf("qualification = %+v, %v", got, err)
	}
	for _, malformed := range [][]byte{
		bytes.Replace(encoded.Bytes(), []byte("\t1\n"), []byte("\t01\n"), 1),
		append(bytes.Clone(encoded.Bytes()), '\n'),
		[]byte("schema\tvibedb.rf3.qualification\t1\n"),
	} {
		if _, err := ParseQualificationTSV(malformed); err == nil {
			t.Fatalf("accepted malformed qualification %q", malformed)
		}
	}
}

func validQualification() Qualification {
	return Qualification{
		KillBeforeRequestCuts: 1, KillAdmissionResponseCuts: 1, KillAfterApplyResponseCuts: 1,
		AsymmetricPartitionLoops: RequiredAsymmetricPartitionLoops, AsymmetricRejectedConnections: 2,
		WaiterWaves: RequiredWaiterWaves, WaiterCalls: RequiredWaiterWaves * RequiredWaiterCallsPerWave,
		WaiterCompletions: 4, WaiterRefusals: RequiredWaiterWaves*RequiredWaiterCallsPerWave - 4,
		WaiterReuseCompletions: RequiredWaiterWaves,
		WALBaselineBytes:       1024, WALFinalBytes: 2048, WALGrowthBytes: 1024, WALGrowthBoundBytes: WALGrowthBoundBytes,
		WaiterRSSBaselineBytes: 4096, WaiterRSSPeakBytes: 8192, WaiterRSSGrowthBytes: 4096,
		WaiterRSSGrowthBoundBytes: WaiterRSSGrowthBoundBytes, LostResponseApplied: 3, AckOutcome: 9,
	}
}
