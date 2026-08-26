package main

import (
	"os"
	"testing"
	"time"
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
}

// This deliberately matches the shipped harness name. executeFaultRun starts a
// fresh copy of this test binary, and the Go test driver's own verbose markers
// prove that exact-test detection cannot confuse a successful process with a
// skipped qualification.
func TestServeRF3ShippedFaultHarness(t *testing.T) {
	switch os.Getenv("VIBEDB_RF3CHAOS_HELPER") {
	case "pass":
		return
	case "skip":
		t.Skip("qualification unavailable")
	}
}
