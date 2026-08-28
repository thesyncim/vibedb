//go:build darwin || linux

package main

import (
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/shardservice"
)

// Keep the shipped ten-minute cadence. Small authenticated record bounds make
// all three processes hit real WAL pressure in seconds, without a production
// fault hook, shortened maintenance timer, or storage reset.
func TestServeRF3WALPressureBeforeMaintenance(t *testing.T) {
	if os.Getenv(rf3CommandHelperEnvironment) != "" {
		return
	}
	if runtime.GOOS != "linux" {
		t.Skip("external WAL pressure qualification requires Linux physical allocation")
	}
	if rf3WALGenerationIntervalTicks != rf3DefaultWALGenerationIntervalTicks {
		t.Skip("pressure qualification requires the production maintenance cadence")
	}
	fixture := newRF3FaultFixtureWithWALRecords(t, 32)
	defer fixture.close(t)
	started := time.Now()
	fixture.startAll(t)
	previous := walRetentionWALGenerations(t, fixture.walPaths)
	leader, states := fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
	epoch, lastApplied := fixture.openSession(t, leader, states[leader])
	const id = "wal-pressure-retained"
	value := []byte(`{"id":"wal-pressure-retained","value":"survives pressure"}`)
	for sequence := uint64(2); sequence < 66; sequence++ {
		leader, states = fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
		command := walRetentionMutationCommand(t, fixture, states[leader], epoch, sequence, id, value)
		response := fixture.propose(t, leader, states[leader], command)
		if response.Kind != shardservice.ReplicatedCompletion {
			t.Fatalf("pressure proposal %d: %+v", sequence, response)
		}
		lastApplied = response.Outcome.AppliedIndex
	}
	fixture.waitAllApplied(t, lastApplied, 30*time.Second)
	walRetentionWaitGenerationReplacement(t, fixture.walPaths, previous, 10*time.Second)
	if elapsed := time.Since(started); elapsed >= 10*time.Minute {
		t.Fatalf("pressure test overlapped periodic maintenance: %s", elapsed)
	}
	fixture.kill(t, leader)
	fixture.waitLeader(t, rf3FaultOtherMembers(leader), 30*time.Second)
	fixture.restart(t, leader)
	fixture.waitCaughtUp(t, leader, lastApplied, 30*time.Second)
	leader, states = fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
	walRetentionWaitValue(t, fixture, leader, states[leader], id, value, 30*time.Second)
}
