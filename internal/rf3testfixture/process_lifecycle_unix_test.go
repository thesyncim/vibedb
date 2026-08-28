//go:build darwin || linux

package rf3testfixture

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestExternalProcessReadinessRestartAndBoundedCleanup(t *testing.T) {
	process := &ExternalProcess{Binary: "/bin/sh", Args: []string{"-c",
		"echo fixture-ready; trap 'exit 0' TERM; while :; do sleep 1; done"}, Env: os.Environ()}
	for attempt := 0; attempt < 2; attempt++ {
		if err := process.Start(); err != nil {
			t.Fatal(err)
		}
		ready, cancel := context.WithTimeout(t.Context(), time.Second)
		if err := process.WaitReady(ready, "fixture-ready"); err != nil {
			cancel()
			t.Fatalf("attempt %d readiness: %v diagnostics=%q", attempt, err, process.Diagnostics())
		}
		cancel()
		stop, stopCancel := context.WithTimeout(t.Context(), 3*time.Second)
		if err := process.Stop(stop); err != nil {
			stopCancel()
			t.Fatalf("attempt %d stop: %v diagnostics=%q", attempt, err, process.Diagnostics())
		}
		stopCancel()
	}
	reservation, err := ReserveLoopbackAddresses(4)
	if err != nil || len(reservation.Addresses) != 4 {
		t.Fatalf("reservation=%+v err=%v", reservation, err)
	}
	if err = reservation.Close(); err != nil {
		t.Fatal(err)
	}
}
