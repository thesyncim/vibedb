package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rf3bench"
)

func TestExecuteFaultRunRequiresExactPassNotSkip(t *testing.T) {
	t.Setenv("VIBEDB_RF3CHAOS_HELPER", "pass")
	passed := executeFaultRun(os.Args[0], 1, 10*time.Second)
	if !passed.run.Passed || !passed.run.ExactRun || passed.run.ExitCode != 0 ||
		passed.run.OutputBytes == 0 || passed.run.OutputSHA256 == ([32]byte{}) {
		t.Fatalf("passing helper outcome = %+v", passed.run)
	}

	t.Setenv("VIBEDB_RF3CHAOS_HELPER", "skip")
	skipped := executeFaultRun(os.Args[0], 2, 10*time.Second)
	if skipped.run.Passed || !skipped.run.ExactRun || skipped.run.ExitCode != 0 {
		t.Fatalf("skipped helper outcome = %+v", skipped.run)
	}

	t.Setenv("VIBEDB_RF3CHAOS_HELPER", "noqual")
	missing := executeFaultRun(os.Args[0], 3, 10*time.Second)
	if missing.run.Passed || !missing.run.ExactRun || missing.run.QualificationExact || missing.run.ExitCode != 0 {
		t.Fatalf("missing-qualification helper outcome = %+v", missing.run)
	}
}

// This deliberately matches the shipped harness name. executeFaultRun starts a
// fresh copy of this test binary, and the Go test driver's own verbose markers
// prove that exact-test detection cannot confuse a successful process with a
// skipped qualification.
func TestServeRF3ShippedFaultHarness(t *testing.T) {
	switch os.Getenv("VIBEDB_RF3CHAOS_HELPER") {
	case "pass":
		writeHelperQualification(t)
		return
	case "skip":
		t.Skip("qualification unavailable")
	case "noqual":
		return
	}
}

func writeHelperQualification(t *testing.T) {
	t.Helper()
	path := os.Getenv(rf3bench.QualificationPathEnvironment)
	if path == "" {
		t.Fatal("qualification result path missing")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	q := rf3bench.Qualification{
		KillBeforeRequestCuts: 1, KillAdmissionResponseCuts: 1, KillAfterApplyResponseCuts: 1,
		AsymmetricPartitionLoops: rf3bench.RequiredAsymmetricPartitionLoops, AsymmetricRejectedConnections: 1,
		WaiterWaves:       rf3bench.RequiredWaiterWaves,
		WaiterCalls:       rf3bench.RequiredWaiterWaves * rf3bench.RequiredWaiterCallsPerWave,
		WaiterCompletions: 1, WaiterRefusals: rf3bench.RequiredWaiterWaves*rf3bench.RequiredWaiterCallsPerWave - 1,
		WaiterReuseCompletions: rf3bench.RequiredWaiterWaves,
		WALBaselineBytes:       1, WALFinalBytes: 2, WALGrowthBytes: 1, WALGrowthBoundBytes: rf3bench.WALGrowthBoundBytes,
		WaiterRSSBaselineBytes: 1, WaiterRSSPeakBytes: 2, WaiterRSSGrowthBytes: 1, WaiterRSSGrowthBoundBytes: rf3bench.WaiterRSSGrowthBoundBytes,
		LostResponseApplied: 1, AckOutcome: 1,
	}
	if err = rf3bench.WriteQualificationTSV(file, q); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(fmt.Errorf("close qualification: %w", err))
	}
}
