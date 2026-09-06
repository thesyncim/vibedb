//go:build darwin || linux

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibejson"
)

const nodeSpaceEnvironment = "VIBEDB_NODE_SPACE_E2E"

func init() {
	if os.Getenv(nodeSpaceEnvironment) == "1" {
		// Test binaries only: exercise repeated maintenance with production log
		// geometry. Both the base and candidate use this identical cadence.
		rf3WALGenerationIntervalTicks = 40
	}
}

type nodeSpaceSample struct {
	Kind               string
	StartNS, ElapsedNS int64
}

type nodeSpaceEvidence struct {
	Workers, WritesPerWorker, DocumentBytes      int
	ElapsedNS, BaselineAllocated, FinalAllocated int64
	RemovedSegments                              [3]int
	Samples                                      [][]nodeSpaceSample
	Diagnostics                                  [3]string
	Checkpoints                                  [3]uint64
	Passed                                       bool
}

// TestServeRF3NodeSpaceQualification is opt-in because it writes hundreds of
// MiB through three real server processes. Copy this test unchanged to the
// comparison base; it does not call candidate-only APIs. Keep all operation
// samples, including stalls, and check acknowledged values after each crash.
func TestServeRF3NodeSpaceQualification(t *testing.T) {
	if os.Getenv(rf3CommandHelperEnvironment) != "" {
		return
	}
	if runtime.GOOS != "linux" || os.Getenv(nodeSpaceEnvironment) != "1" {
		t.Skip("set VIBEDB_NODE_SPACE_E2E=1 on Linux with strict allocation")
	}
	workers, writes, documentBytes := 2, 2048, 64<<10
	if value := os.Getenv("VIBEDB_NODE_SPACE_WRITES"); value != "" {
		var err error
		writes, err = strconv.Atoi(value)
		if err != nil || writes < 32 || writes > 65536 {
			t.Fatal("invalid write count")
		}
	}
	fixture := newRF3FaultFixtureWithStorage(t, 4096, true)
	defer fixture.close(t)
	initialSegments := [3][]string{}
	for i, root := range fixture.walPaths {
		var err error
		initialSegments[i], err = filepath.Glob(filepath.Join(root, "log", "segment-*.dat"))
		if err != nil {
			t.Fatal(err)
		}
		if len(initialSegments[i]) == 0 {
			t.Fatalf("no initial segments under %s", root)
		}
	}
	evidence := nodeSpaceEvidence{Workers: workers, WritesPerWorker: writes, DocumentBytes: documentBytes,
		BaselineAllocated: fixture.allocatedLogBytes(t), Samples: make([][]nodeSpaceSample, workers)}
	defer func() {
		evidence.Passed = !t.Failed()
		if path := os.Getenv("VIBEDB_NODE_SPACE_EVIDENCE"); path != "" {
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				t.Error(err)
				return
			}
			compressed := gzip.NewWriter(file)
			if err := json.NewEncoder(compressed).Encode(evidence); err != nil {
				t.Error(err)
			}
			if err := compressed.Close(); err != nil {
				t.Error(err)
			}
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	fixture.startAll(t)
	leader, states := fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
	state := states[leader]
	type finalValue struct {
		id      string
		value   []byte
		applied uint64
	}
	finals := make([]finalValue, workers)
	var ready, done sync.WaitGroup
	ready.Add(workers)
	done.Add(workers)
	start := make(chan time.Time, workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer done.Done()
			announced := false
			defer func() {
				if !announced {
					ready.Done()
				}
			}()
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			connection, err := fixture.dialNative(ctx, leader)
			if err != nil {
				t.Error(err)
				return
			}
			defer connection.Close()
			sequence, epoch := uint64(1), uint64(0)
			put := func(id string, value []byte, opening bool) (*shardservice.ReplicatedResponse, error) {
				command := fixture.command(state, epoch, sequence, sha256.Sum256(value), nil)
				command.ClientID[15] = byte(worker + 1)
				if opening {
					command.Kind, command.Batches = replication.CommandSessionOpen, nil
					command.NextDeadlineUnixNano = 2_000_000_000_000_000_000
				} else {
					command.AckThrough = sequence - 1
					command.Batches[0].Mutations = []replication.Mutation{{Kind: replication.MutationPut, Key: rf3FaultKey(t, id), Value: value}}
				}
				raw, err := replication.AppendCommand(nil, command)
				if err != nil {
					return nil, err
				}
				response, err := shardservice.RoundTripReplicated(ctx, connection, fixture.proposalRequest(leader, state, raw))
				if err != nil {
					return nil, err
				}
				if response.Kind != shardservice.ReplicatedCompletion {
					return nil, fmt.Errorf("write %d: %+v", sequence, response)
				}
				if !opening {
					completion, err := replication.OpenCompletion(response.Completion)
					if err != nil || completion.ResultCode != replicatedstate.ResultApplied {
						return nil, fmt.Errorf("mutation %d result: %+v %v", sequence, completion, err)
					}
				}
				sequence++
				return response, nil
			}
			response, err := put("", []byte("space-session"), true)
			if err != nil {
				t.Error(err)
				return
			}
			completion, err := replication.OpenCompletion(response.Completion)
			if err != nil || completion.ResultCode != replicatedstate.ResultSessionOpened {
				t.Errorf("session: %+v %v", completion, err)
				return
			}
			epoch = completion.ClientEpoch
			for key := 0; key < 32; key++ {
				id := fmt.Sprintf("update-%d-%d", worker, key)
				if _, err := put(id, walRetentionDocument(id, 0, documentBytes), false); err != nil {
					t.Error(err)
					return
				}
			}
			ready.Done()
			announced = true
			started := <-start
			samples := make([]nodeSpaceSample, 0, writes+writes/2)
			defer func() { evidence.Samples[worker] = samples }()
			for i := 0; i < writes; i++ {
				id, kind := fmt.Sprintf("update-%d-%d", worker, i%32), "update"
				if i%4 == 0 {
					id, kind = fmt.Sprintf("insert-%d-%d", worker, i), "insert"
				}
				value := walRetentionDocument(id, i+1, documentBytes)
				before := time.Now()
				response, err := put(id, value, false)
				samples = append(samples, nodeSpaceSample{kind, before.Sub(started).Nanoseconds(), time.Since(before).Nanoseconds()})
				if err != nil {
					t.Error(err)
					return
				}
				finals[worker] = finalValue{id, value, response.Outcome.AppliedIndex}
				if i%2 == 0 {
					request := fixture.readRequest(leader, state, rf3FaultKey(t, id))
					request.MinimumApplied = response.Outcome.AppliedIndex
					before = time.Now()
					read, err := shardservice.RoundTripReplicated(ctx, connection, request)
					samples = append(samples, nodeSpaceSample{"read", before.Sub(started).Nanoseconds(), time.Since(before).Nanoseconds()})
					want, canonicalErr := vibejson.AppendCanonicalize(nil, value)
					if err != nil || canonicalErr != nil || read.Kind != shardservice.ReplicatedReadFound || !bytes.Equal(read.Value, want) {
						t.Errorf("read worker=%d operation=%d: %v %v", worker, i, err, canonicalErr)
						return
					}
				}
			}
		}(worker)
	}
	ready.Wait()
	started := time.Now()
	for range workers {
		start <- started
	}
	done.Wait()
	evidence.ElapsedNS = time.Since(started).Nanoseconds()
	if t.Failed() {
		return
	}
	var lastApplied uint64
	for _, final := range finals {
		lastApplied = max(lastApplied, final.applied)
	}
	fixture.waitAllApplied(t, lastApplied, 30*time.Second)
	// Detach every server before the physical footprint cut. This avoids races
	// with unlink and counts all reserves, catalogs and certificate files.
	closeRF3CommandChildren(t, fixture.children[:])
	for i := range fixture.children {
		evidence.Diagnostics[i] = fixture.children[i].diagnostic.String()
		fixture.children[i] = nil
	}
	evidence.FinalAllocated = fixture.allocatedLogBytes(t)
	for i, paths := range initialSegments {
		for _, path := range paths {
			if _, err := os.Stat(path); os.IsNotExist(err) {
				evidence.RemovedSegments[i]++
			} else if err != nil {
				t.Fatal(err)
			}
		}
		manifest, err := loadRF3Manifest(fixture.manifestPaths[i])
		if err != nil {
			t.Fatal(err)
		}
		owner, err := openRF3NodeOwner(manifest, fixture.profiles[i])
		if err != nil {
			t.Fatal(err)
		}
		checkpoint, err := owner.store.Group(1).Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		evidence.Checkpoints[i] = checkpoint.GetMetadata().GetIndex()
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for member := range 3 {
		fixture.restart(t, member)
	}
	leader, states = fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
	for _, final := range finals {
		walRetentionWaitValue(t, fixture, leader, states[leader], final.id, final.value, 30*time.Second)
	}
	// A crash after reclamation must preserve the same acknowledged values.
	for victim := range 3 {
		fixture.kill(t, victim)
		fixture.waitLeader(t, rf3FaultOtherMembers(victim), 30*time.Second)
		fixture.restart(t, victim)
		fixture.waitCaughtUp(t, victim, lastApplied, 30*time.Second)
		leader, states = fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
		for _, final := range finals {
			walRetentionWaitValue(t, fixture, leader, states[leader], final.id, final.value, 30*time.Second)
		}
	}
	if os.Getenv("VIBEDB_NODE_SPACE_EXPECT_RECLAIM") == "1" {
		for i := range 3 {
			if evidence.RemovedSegments[i] == 0 || evidence.Checkpoints[i] <= 1 {
				t.Errorf("member %d did not reclaim history: removed=%d checkpoint=%d", i+1, evidence.RemovedSegments[i], evidence.Checkpoints[i])
			}
		}
	}
	t.Logf("writes=%d elapsed=%s allocated=%d->%d initial segments removed=%v checkpoints=%v", workers*writes, time.Duration(evidence.ElapsedNS), evidence.BaselineAllocated, evidence.FinalAllocated, evidence.RemovedSegments, evidence.Checkpoints)
}
