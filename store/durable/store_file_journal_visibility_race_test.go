package durable

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestFileBufferedJournalConcurrentPointReadVisibility is the store-level
// regression for the buffered-journal lane's lost-key symptom: N writers on
// disjoint key shards, each verifying its own acknowledged mutation with a
// fresh point read, while the store's own checkpoint cadence (a shared
// mutation-count schedule driving Flush, exactly like the mixed harness's
// coordinator) folds and recycles concurrently.
//
// Every acknowledged Put must be visible to an immediately following point
// read of the same key: the shards are disjoint, so no other goroutine can
// have deleted it. A miss is the bug. The failure diagnostic distinguishes a
// transient read-skew (an immediate re-read finds the key) from a durable
// lost update (the miss persists), which is the fact that decides where the
// root cause lives.
func TestFileBufferedJournalConcurrentPointReadVisibility(t *testing.T) {
	const (
		corpus          = 4096
		workers         = 8
		opsPerWorker    = 1500
		checkpointEvery = 64
	)
	built, keys, values := buildFilePrimaryCorpus(t, corpus)
	options := Options{
		Backend:            BackendPortable,
		ResidentBytes:      64 << 20,
		Durability:         DurabilityBufferedVisible,
		CheckpointStrength: CheckpointPowerSafe,
		RecoveryJournal:    true,
	}
	file := createPrimaryPointFile(t, built, options, "journal-visibility.vibe")
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	ops := opsPerWorker
	if testing.Short() {
		ops = 300
	}

	// The harness's checkpoint coordinator: every worker counts its mutations
	// into a shared schedule and the worker that crosses the boundary runs the
	// checkpoint, so folds interleave with the other workers' mutations and
	// verification reads exactly as in the mixed run.
	var mutations atomic.Uint64
	var checkpointMu sync.Mutex
	var checkpointed uint64
	countMutation := func() error {
		total := mutations.Add(1)
		if total%checkpointEvery != 0 {
			return nil
		}
		checkpointMu.Lock()
		defer checkpointMu.Unlock()
		if total <= checkpointed {
			return nil
		}
		checkpointed = total
		return collection.Flush()
	}

	var stop atomic.Bool
	var group sync.WaitGroup
	errs := make([]error, workers)
	for worker := range workers {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			base := worker * (corpus / workers)
			span := corpus / workers
			buffer := make([]byte, 0, 512)
			verify := func(at int, want []byte, after string) error {
				out, ok, readErr := collection.AppendRaw(buffer[:0], []byte(keys[at]))
				buffer = out
				if readErr != nil {
					return fmt.Errorf("verify read %q after %s: %w", keys[at], after, readErr)
				}
				if !ok {
					// The decisive diagnostic: does an immediate retry see it?
					again, okAgain, againErr := collection.AppendRaw(buffer[:0], []byte(keys[at]))
					buffer = again
					return fmt.Errorf(
						"LOST KEY %q after acknowledged %s (worker %d): retry found=%v err=%v",
						keys[at], after, worker, okAgain, againErr,
					)
				}
				if want != nil && !bytes.Equal(out, want) {
					again, okAgain, againErr := collection.AppendRaw(buffer[:0], []byte(keys[at]))
					match := okAgain && bytes.Equal(again, want)
					buffer = again
					return fmt.Errorf(
						"STALE VALUE %q after acknowledged %s (worker %d): retry found=%v match=%v err=%v",
						keys[at], after, worker, okAgain, match, againErr,
					)
				}
				return nil
			}
			for op := 0; op < ops && !stop.Load(); op++ {
				at := base + (op*7)%span
				switch op % 3 {
				case 0, 1: // update in place / COW
					doc := fmt.Appendf(nil,
						`{"id":%d,"group":%d,"name":"primary row %d"}`,
						at, (at+op)%997, at,
					)
					if _, putErr := collection.Put([]byte(keys[at]), doc); putErr != nil {
						errs[worker] = fmt.Errorf("put %q: %w", keys[at], putErr)
						stop.Store(true)
						return
					}
					if err := countMutation(); err != nil {
						errs[worker] = fmt.Errorf("checkpoint: %w", err)
						stop.Store(true)
						return
					}
					if err := verify(at, doc, "Put"); err != nil {
						errs[worker] = err
						stop.Store(true)
						return
					}
				case 2: // churn: delete then restore
					deleted, delErr := collection.Delete([]byte(keys[at]))
					if delErr != nil {
						errs[worker] = fmt.Errorf("delete %q: %w", keys[at], delErr)
						stop.Store(true)
						return
					}
					if !deleted {
						errs[worker] = fmt.Errorf(
							"LOST KEY %q: Delete reported not found (worker %d)",
							keys[at], worker,
						)
						stop.Store(true)
						return
					}
					if err := countMutation(); err != nil {
						errs[worker] = fmt.Errorf("checkpoint: %w", err)
						stop.Store(true)
						return
					}
					if _, putErr := collection.Put([]byte(keys[at]), values[at]); putErr != nil {
						errs[worker] = fmt.Errorf("restore %q: %w", keys[at], putErr)
						stop.Store(true)
						return
					}
					if err := countMutation(); err != nil {
						errs[worker] = fmt.Errorf("checkpoint: %w", err)
						stop.Store(true)
						return
					}
					if err := verify(at, values[at], "Delete+restore Put"); err != nil {
						errs[worker] = err
						stop.Store(true)
						return
					}
				}
			}
		}(worker)
	}
	group.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}
