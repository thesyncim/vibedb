//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
)

const (
	walRetentionQualificationEnvironment = "VIBEDB_WAL_RETENTION_E2E"
	walRetentionEvidenceEnvironment      = "VIBEDB_WAL_RETENTION_EVIDENCE"
	walRetentionCycles                   = 3
	walRetentionKeysPerCycle             = 24
	walRetentionDocumentBytes            = 64 << 10
	walRetentionMaximumGrowthBytes       = 1 << 20
	walRetentionMaximumRatioPermille     = 250
	walRetentionMaximumRSSGrowthBytes    = 128 << 20
	walRetentionMaximumFDGrowth          = 24
	walRetentionP99Bound                 = 5 * time.Second
	walRetentionMaxBound                 = 15 * time.Second
)

var walRetentionEvidenceRun atomic.Uint32

// init is inherited only by the test binary. Production binaries retain the
// ten-minute cadence; a qualification child explicitly opts into eight ticks
// so three complete maintenance and crash cycles fit in a bounded CI job.
func init() {
	if os.Getenv(walRetentionQualificationEnvironment) == "1" {
		rf3WALGenerationIntervalTicks = 8
	}
}

// TestServeRF3WALRetentionCrashQualification drives the shipped, authenticated
// three-process command rather than an in-memory Store. Each cycle creates a
// new checkpoint, observes all three logical WAL inodes being replaced by
// compacted generations, SIGKILLs one process, catches it up, and verifies an
// acknowledged value through a linearizable read. Physical allocation, live
// data, RSS, file descriptors, waiter reuse, and latency are hard gates.
func TestServeRF3WALRetentionCrashQualification(t *testing.T) {
	if os.Getenv(rf3CommandHelperEnvironment) != "" {
		return
	}
	if runtime.GOOS != "linux" {
		t.Skip("external WAL-retention qualification requires Linux /proc and physical block accounting")
	}
	if os.Getenv(walRetentionQualificationEnvironment) != "1" {
		t.Skip("set VIBEDB_WAL_RETENTION_E2E=1; mandatory Linux CI rejects this skip")
	}

	fixture := newRF3FaultFixture(t)
	defer fixture.close(t)
	fixture.startAll(t)
	leader, states := fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
	epoch, lastApplied := fixture.openSession(t, leader, states[leader])
	sequence := uint64(2)
	firstCommand := []byte(nil)
	latencies := make([]time.Duration, 0, walRetentionCycles*walRetentionKeysPerCycle)
	initialInodes := walRetentionWALInodes(t, fixture.walPaths)
	previousInodes := initialInodes
	baselineAllocated := rf3FaultWALAllocatedBytes(t, fixture.walPaths)
	baselineRSS := rf3FaultProcessRSSBytes(t, fixture.children)
	baselineFDs := walRetentionProcessFDs(t, fixture.children)
	peakRSS, peakFDs := baselineRSS, baselineFDs

	var finalValue []byte
	for cycle := 0; cycle < walRetentionCycles; cycle++ {
		for key := 0; key < walRetentionKeysPerCycle; key++ {
			leader, states = fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
			id := fmt.Sprintf("retained-%02d", key)
			value := walRetentionDocument(id, cycle, walRetentionDocumentBytes)
			command := walRetentionMutationCommand(t, fixture, states[leader], epoch, sequence, id, value)
			if firstCommand == nil {
				firstCommand = append([]byte(nil), command...)
			}
			started := time.Now()
			response := fixture.propose(t, leader, states[leader], command)
			elapsed := time.Since(started)
			if response.Kind != shardservice.ReplicatedCompletion {
				t.Fatalf("cycle %d key %d completion = %+v", cycle+1, key+1, response)
			}
			if elapsed > walRetentionMaxBound {
				t.Fatalf("cycle %d key %d latency %s exceeds %s", cycle+1, key+1, elapsed, walRetentionMaxBound)
			}
			latencies = append(latencies, elapsed)
			lastApplied = response.Outcome.AppliedIndex
			sequence++
			if key == walRetentionKeysPerCycle-1 {
				finalValue = value
			}
		}

		fixture.waitAllApplied(t, lastApplied, 30*time.Second)
		currentInodes := walRetentionWaitGenerationReplacement(t, fixture.walPaths, previousInodes, 45*time.Second)
		previousInodes = currentInodes

		victim := cycle % rf3CommandMembers
		fixture.kill(t, victim)
		live := rf3FaultOtherMembers(victim)
		fixture.waitLeader(t, live, 30*time.Second)
		fixture.restart(t, victim)
		fixture.waitCaughtUp(t, victim, lastApplied, 30*time.Second)
		leader, states = fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
		walRetentionWaitValue(t, fixture, leader, states[leader],
			fmt.Sprintf("retained-%02d", walRetentionKeysPerCycle-1), finalValue, 30*time.Second)

		// A duplicate wave exercises the bounded settlement registry after every
		// process replacement; the next fresh command proves capacity is reusable.
		walRetentionDuplicateWave(t, fixture, leader, states[leader],
			walRetentionMutationCommand(t, fixture, states[leader], epoch, sequence,
				fmt.Sprintf("reuse-%d", cycle+1), walRetentionDocument(fmt.Sprintf("reuse-%d", cycle+1), cycle, 4096)))
		sequence++
		states[leader] = fixture.probe(t, leader)
		freshID := fmt.Sprintf("capacity-%d", cycle+1)
		fresh := fixture.propose(t, leader, states[leader], walRetentionMutationCommand(
			t, fixture, states[leader], epoch, sequence, freshID, walRetentionDocument(freshID, cycle, 4096)))
		if fresh.Kind != shardservice.ReplicatedCompletion {
			t.Fatalf("cycle %d did not return waiter capacity: %+v", cycle+1, fresh)
		}
		lastApplied = fresh.Outcome.AppliedIndex
		sequence++
		fixture.waitAllApplied(t, lastApplied, 30*time.Second)
		if rss := rf3FaultProcessRSSBytes(t, fixture.children); rss > peakRSS {
			peakRSS = rss
		}
		if fds := walRetentionProcessFDs(t, fixture.children); fds > peakFDs {
			peakFDs = fds
		}
	}

	// All three process lifetimes have changed. The first acknowledged command
	// must remain retired, never re-execute or regress to outcome-unknown.
	leader, states = fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
	retired := fixture.propose(t, leader, states[leader], firstCommand)
	if retired.Kind != shardservice.ReplicatedRefusal ||
		retired.Refusal != shardservice.ReplicatedRefusalDeterministic ||
		retired.Outcome.Code != raftserve.OutcomeRetryRetired {
		t.Fatalf("acknowledged command was not durably retired after crash loops: %+v", retired)
	}

	finalAllocated := rf3FaultWALAllocatedBytes(t, fixture.walPaths)
	if finalAllocated < baselineAllocated {
		t.Fatalf("WAL allocation regressed below reserved baseline: before=%d after=%d", baselineAllocated, finalAllocated)
	}
	growth := uint64(finalAllocated - baselineAllocated)
	if growth > walRetentionMaximumGrowthBytes {
		t.Fatalf("retained WAL allocation grew %d bytes, bound %d", growth, walRetentionMaximumGrowthBytes)
	}
	// The final key set is identical on every replica. This denominator excludes
	// overwritten historical bytes, so the ratio catches retained churn rather
	// than rewarding the test for issuing more writes.
	liveBytes := uint64(walRetentionKeysPerCycle * walRetentionDocumentBytes * rf3CommandMembers)
	ratioPermille := uint64(0)
	if liveBytes != 0 {
		ratioPermille = (growth*1000 + liveBytes - 1) / liveBytes
	}
	if ratioPermille > walRetentionMaximumRatioPermille {
		t.Fatalf("retained WAL/live ratio %d permille exceeds %d; growth=%d live=%d",
			ratioPermille, walRetentionMaximumRatioPermille, growth, liveBytes)
	}
	if peakRSS-baselineRSS > walRetentionMaximumRSSGrowthBytes {
		t.Fatalf("RSS grew %d bytes, bound %d", peakRSS-baselineRSS, walRetentionMaximumRSSGrowthBytes)
	}
	if peakFDs > baselineFDs+walRetentionMaximumFDGrowth {
		t.Fatalf("file descriptors grew from %d to %d, bound +%d", baselineFDs, peakFDs, walRetentionMaximumFDGrowth)
	}
	slices.Sort(latencies)
	p99 := latencies[(99*len(latencies)+99)/100-1]
	maximum := latencies[len(latencies)-1]
	if p99 > walRetentionP99Bound || maximum > walRetentionMaxBound {
		t.Fatalf("foreground latency p99=%s max=%s bounds=%s/%s", p99, maximum, walRetentionP99Bound, walRetentionMaxBound)
	}
	walRetentionWriteEvidence(t, walRetentionEvidence{
		Cycles: walRetentionCycles, Writes: len(latencies), Restarts: walRetentionCycles,
		GenerationChanges: walRetentionCycles * rf3CommandMembers,
		LiveBytes:         liveBytes, WALBaselineBytes: uint64(baselineAllocated), WALFinalBytes: uint64(finalAllocated),
		WALGrowthBytes: growth, WALRatioPermille: ratioPermille,
		RSSGrowthBytes: peakRSS - baselineRSS, FDGrowth: peakFDs - baselineFDs,
		P99NS: uint64(p99), MaxNS: uint64(maximum), RetiredOutcome: uint64(retired.Outcome.Code),
	})
}

func walRetentionMutationCommand(t testing.TB, fixture *rf3FaultFixture, state shardservice.ReplicatedMemberState,
	epoch, sequence uint64, id string, value []byte) []byte {
	t.Helper()
	mutation := replication.Mutation{Kind: replication.MutationPut, Key: rf3FaultKey(t, id), Value: value}
	command := fixture.command(state, epoch, sequence, sha256.Sum256(value), []replication.Mutation{mutation})
	if sequence > 1 {
		command.AckThrough = sequence - 1
	}
	raw, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func walRetentionDocument(id string, cycle, size int) []byte {
	prefix := []byte(fmt.Sprintf(`{"id":%q,"cycle":%d,"payload":"`, id, cycle))
	result := make([]byte, 0, size)
	result = append(result, prefix...)
	for len(result) < size-2 {
		result = append(result, byte('a'+cycle%26))
	}
	result = append(result, '"', '}')
	return result
}

func walRetentionWaitValue(t testing.TB, fixture *rf3FaultFixture, member int,
	state shardservice.ReplicatedMemberState, id string, want []byte, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		request := fixture.readRequest(member, state, rf3FaultKey(t, id))
		response, err := fixture.roundTrip(t, member, request)
		if err == nil && response.Kind == shardservice.ReplicatedReadFound && bytes.Equal(response.Value, want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("member %d did not return acknowledged value %q", member+1, id)
}

func walRetentionDuplicateWave(t testing.TB, fixture *rf3FaultFixture, member int,
	state shardservice.ReplicatedMemberState, command []byte) {
	t.Helper()
	const callers = 32
	type result struct {
		response *shardservice.ReplicatedResponse
		err      error
	}
	results := make(chan result, callers)
	for range callers {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			response, err := fixture.roundTripContext(ctx, member, fixture.proposalRequest(member, state, command))
			results <- result{response: response, err: err}
		}()
	}
	completed := 0
	for range callers {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.response.Kind == shardservice.ReplicatedCompletion {
			completed++
			continue
		}
		if result.response.Kind != shardservice.ReplicatedRefusal ||
			result.response.Refusal != shardservice.ReplicatedRefusalAdmissionBound {
			t.Fatalf("duplicate waiter result = %+v", result.response)
		}
	}
	if completed == 0 {
		t.Fatal("duplicate waiter wave had no completion")
	}
}

func walRetentionWALInodes(t testing.TB, paths [rf3CommandMembers]string) [rf3CommandMembers]uint64 {
	t.Helper()
	var result [rf3CommandMembers]uint64
	for index, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Ino == 0 {
			t.Fatalf("WAL inode unavailable for %q", path)
		}
		result[index] = stat.Ino
	}
	return result
}

func walRetentionWaitGenerationReplacement(t testing.TB, paths [rf3CommandMembers]string,
	previous [rf3CommandMembers]uint64, timeout time.Duration) [rf3CommandMembers]uint64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		current := walRetentionWALInodes(t, paths)
		changed := true
		for index := range current {
			changed = changed && current[index] != previous[index]
		}
		if changed {
			return current
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("all WAL generations did not replace before timeout; previous=%v current=%v",
		previous, walRetentionWALInodes(t, paths))
	return [rf3CommandMembers]uint64{}
}

func walRetentionProcessFDs(t testing.TB, children [rf3CommandMembers]*rf3CommandChild) uint64 {
	t.Helper()
	var total uint64
	for member, child := range children {
		if child == nil || child.command == nil || child.command.Process == nil {
			t.Fatalf("member %d has no process for FD evidence", member+1)
		}
		entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", child.command.Process.Pid))
		if err != nil {
			t.Fatalf("read member %d descriptors: %v", member+1, err)
		}
		total += uint64(len(entries))
	}
	return total
}

type walRetentionEvidence struct {
	Cycles, Writes, Restarts, GenerationChanges                                  int
	LiveBytes, WALBaselineBytes, WALFinalBytes, WALGrowthBytes, WALRatioPermille uint64
	RSSGrowthBytes, FDGrowth, P99NS, MaxNS, RetiredOutcome                       uint64
}

func (e walRetentionEvidence) validate() error {
	if e.Cycles != walRetentionCycles || e.Writes != walRetentionCycles*walRetentionKeysPerCycle ||
		e.Restarts != walRetentionCycles || e.GenerationChanges != walRetentionCycles*rf3CommandMembers ||
		e.LiveBytes == 0 || e.WALBaselineBytes == 0 || e.WALFinalBytes < e.WALBaselineBytes ||
		e.WALGrowthBytes != e.WALFinalBytes-e.WALBaselineBytes ||
		e.WALGrowthBytes > walRetentionMaximumGrowthBytes || e.WALRatioPermille > walRetentionMaximumRatioPermille ||
		e.RSSGrowthBytes > walRetentionMaximumRSSGrowthBytes || e.FDGrowth > walRetentionMaximumFDGrowth ||
		e.P99NS == 0 || e.P99NS > uint64(walRetentionP99Bound) || e.MaxNS < e.P99NS ||
		e.MaxNS > uint64(walRetentionMaxBound) || e.RetiredOutcome != uint64(raftserve.OutcomeRetryRetired) {
		return errors.New("invalid WAL-retention evidence")
	}
	return nil
}

func walRetentionWriteEvidence(t testing.TB, evidence walRetentionEvidence) {
	t.Helper()
	if err := evidence.validate(); err != nil {
		t.Fatal(err)
	}
	directory := os.Getenv(walRetentionEvidenceEnvironment)
	if directory == "" {
		return
	}
	if !filepath.IsAbs(directory) {
		t.Fatalf("WAL-retention evidence directory must be absolute: %q", directory)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, fmt.Sprintf("run-%d.tsv", walRetentionEvidenceRun.Add(1)))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	fields := []struct {
		name  string
		value uint64
	}{
		{"cycles", uint64(evidence.Cycles)}, {"writes", uint64(evidence.Writes)},
		{"restarts", uint64(evidence.Restarts)}, {"generation_changes", uint64(evidence.GenerationChanges)},
		{"live_bytes", evidence.LiveBytes}, {"wal_baseline_bytes", evidence.WALBaselineBytes},
		{"wal_final_bytes", evidence.WALFinalBytes}, {"wal_growth_bytes", evidence.WALGrowthBytes},
		{"wal_live_ratio_permille", evidence.WALRatioPermille}, {"rss_growth_bytes", evidence.RSSGrowthBytes},
		{"fd_growth", evidence.FDGrowth}, {"p99_ns", evidence.P99NS}, {"max_ns", evidence.MaxNS},
		{"retired_outcome", evidence.RetiredOutcome},
	}
	write := func(parts ...string) {
		line := []byte(nil)
		for index, part := range parts {
			if index != 0 {
				line = append(line, '\t')
			}
			line = append(line, part...)
		}
		line = append(line, '\n')
		if _, writeErr := file.Write(line); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	write("schema", "vibedb.wal-retention-process", "1")
	write("result", "pass")
	for _, field := range fields {
		write("metric", field.name, strconv.FormatUint(field.value, 10))
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALRetentionEvidenceRejectsInvalidBounds(t *testing.T) {
	valid := walRetentionEvidence{Cycles: walRetentionCycles, Writes: walRetentionCycles * walRetentionKeysPerCycle,
		Restarts: walRetentionCycles, GenerationChanges: walRetentionCycles * rf3CommandMembers,
		LiveBytes: 1, WALBaselineBytes: 1, WALFinalBytes: 1, P99NS: 1, MaxNS: 1,
		RetiredOutcome: uint64(raftserve.OutcomeRetryRetired)}
	if err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.WALGrowthBytes = walRetentionMaximumGrowthBytes + 1
	invalid.WALFinalBytes = invalid.WALBaselineBytes + invalid.WALGrowthBytes
	if err := invalid.validate(); err == nil {
		t.Fatal("accepted excessive WAL growth")
	}
	invalid = valid
	invalid.WALRatioPermille = walRetentionMaximumRatioPermille + 1
	if err := invalid.validate(); err == nil {
		t.Fatal("accepted excessive retained/live ratio")
	}
	invalid = valid
	invalid.Restarts--
	if err := invalid.validate(); err == nil {
		t.Fatal("accepted missing crash loop")
	}
}
