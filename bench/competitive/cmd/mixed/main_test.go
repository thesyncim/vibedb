package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestOutputHeaderRequiresResolvedDurabilityAndCheckpoint(t *testing.T) {
	var out bytes.Buffer
	printHeader(&out)
	fields := strings.Fields(out.String())
	contains := func(want string) bool {
		for _, field := range fields {
			if field == want {
				return true
			}
		}
		return false
	}
	if !contains("durability") {
		t.Fatalf("header omits resolved durability column: %q", out.String())
	}
	if !contains("checkpoint") {
		t.Fatalf("header omits checkpoint interval column: %q", out.String())
	}
	if !contains("forced-cp") {
		t.Fatalf("header omits forced checkpoint column: %q", out.String())
	}
	if contains("sync") {
		t.Fatalf("header retained ambiguous sync column: %q", out.String())
	}
}

func TestMutationCount(t *testing.T) {
	tests := []struct {
		operation int
		want      int
	}{
		{operation: opRead, want: 0},
		{operation: opUpdate, want: 1},
		{operation: opReadModifyWrite, want: 1},
		{operation: opChurn, want: 2},
		{operation: opScan, want: 0},
	}
	for _, test := range tests {
		if got := mutationCount(test.operation); got != test.want {
			t.Fatalf("mutationCount(%d) = %d, want %d", test.operation, got, test.want)
		}
	}
}

func TestCheckpointScheduleCountsStateChanges(t *testing.T) {
	schedule := checkpointSchedule{every: 3}
	if schedule.Add(mutationCount(opUpdate)) {
		t.Fatal("checkpoint due after one mutation")
	}
	if !schedule.Add(mutationCount(opChurn)) {
		t.Fatal("checkpoint not due after update plus two-state-change churn")
	}
	if schedule.Pending() != 3 {
		t.Fatalf("pending mutations = %d, want 3", schedule.Pending())
	}
	schedule.Mark()
	if schedule.Pending() != 0 {
		t.Fatalf("pending mutations after checkpoint = %d, want 0", schedule.Pending())
	}
}

func TestZeroCheckpointIntervalMeansFinalOnly(t *testing.T) {
	schedule := checkpointSchedule{}
	if schedule.Add(10) {
		t.Fatal("zero interval requested a periodic checkpoint")
	}
	if schedule.Pending() != 10 {
		t.Fatalf("pending mutations = %d, want 10", schedule.Pending())
	}
}
