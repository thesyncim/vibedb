package durable

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	vibejson "github.com/thesyncim/vibejson"
)

// TestFilePrimaryReadEpochAllocatesZero pins the complete epoch-protected
// convenience read — enterReadEpoch, resolve, Exit — at zero allocations, the
// same bar the resolver-only pin already holds.
func TestFilePrimaryReadEpochAllocatesZero(t *testing.T) {
	built, keys, _ := buildFilePrimaryCorpus(t, 2_000)
	options := Options{Backend: BackendPortable, ResidentBytes: 64 << 20}
	file := createPrimaryPointFile(t, built, options, "epoch-alloc.vibe")
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	buffer := make([]byte, 0, 512)
	target := keys[len(keys)/2]
	if _, ok, err := collection.AppendRaw(buffer[:0], target); err != nil || !ok {
		t.Fatalf("warm AppendRaw = %v,%v", ok, err)
	}
	allocs := testing.AllocsPerRun(1_000, func() {
		out, ok, err := collection.AppendRaw(buffer[:0], target)
		if err != nil || !ok {
			panic("epoch AppendRaw failed")
		}
		buffer = out
	})
	if allocs != 0 {
		t.Fatalf("epoch AppendRaw allocations = %v, want 0", allocs)
	}
}

// TestFilePrimaryReadEpochStress is the epoch-read concurrency gate: reader
// goroutines hammer the lock-free point-read entry and snapshot scans while
// the single writer mutates, splits, checkpoints, and recycles volatile pages.
// Every read must return either a valid committed value or a clean miss;
// under -race this also proves the epoch entry protocol publishes and
// re-validates in the documented order.
func TestFilePrimaryReadEpochStress(t *testing.T) {
	const (
		seed    = 2_000
		readers = 4
		scans   = 2
	)
	built, keys, _ := buildFilePrimaryCorpus(t, seed)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 64 << 20,
		Durability:               DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(t, built, options, "epoch-stress.vibe")
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	duration := 2 * time.Second
	if testing.Short() {
		duration = 300 * time.Millisecond
	}
	deadline := time.Now().Add(duration)
	var stop atomic.Bool
	var readsDone, scansDone, writesDone atomic.Uint64
	var group sync.WaitGroup
	fail := func(format string, args ...any) {
		t.Errorf(format, args...)
		stop.Store(true)
	}

	for reader := range readers {
		group.Add(1)
		go func(offset int) {
			defer group.Done()
			buffer := make([]byte, 0, 512)
			for at := offset; !stop.Load(); at += readers + 1 {
				key := keys[at%seed]
				out, ok, err := collection.AppendRaw(buffer[:0], key)
				if err != nil {
					fail("AppendRaw(%q): %v", key, err)
					return
				}
				// The writer deletes and re-inserts seeded keys, so a miss is
				// legal; a hit must be a complete committed document.
				if ok {
					if validateErr := vibejson.Validate(out); validateErr != nil {
						fail("AppendRaw(%q) returned torn value %q: %v",
							key, out, validateErr)
						return
					}
				}
				buffer = out
				readsDone.Add(1)
			}
		}(reader)
	}

	for range scans {
		group.Add(1)
		go func() {
			defer group.Done()
			var scratch []byte
			for !stop.Load() {
				snapshot, err := collection.Snapshot()
				if err != nil {
					fail("Snapshot: %v", err)
					return
				}
				rows := uint64(0)
				scratch, err = snapshot.RangeRawBuffer(
					scratch,
					func(key, value []byte) error {
						rows++
						if len(key) == 0 || len(value) == 0 {
							return fmt.Errorf(
								"empty row %q/%q", key, value,
							)
						}
						return nil
					},
				)
				expected := snapshot.Len()
				_ = snapshot.Close()
				if err != nil {
					fail("scan: %v", err)
					return
				}
				if rows != expected {
					fail("scan rows = %d, snapshot Len = %d", rows, expected)
					return
				}
				scansDone.Add(1)
			}
		}()
	}

	// The single writer alternates same-size in-place-eligible rewrites,
	// growing COW rewrites, delete/re-insert churn, fresh inserts that force
	// leaf splits, and explicit checkpoints, so every publication lane runs
	// against the concurrent readers.
	writerErr := func() error {
		round := 0
		for !stop.Load() && time.Now().Before(deadline) {
			round++
			key := keys[round%seed]
			same := fmt.Appendf(nil,
				`{"id":%d,"group":%d,"name":"primary row %d"}`,
				round%seed, round%997, round%seed,
			)
			if _, err := collection.Put(key, same); err != nil {
				return fmt.Errorf("in-place put %q: %w", key, err)
			}
			grown := fmt.Appendf(nil,
				`{"id":%d,"round":%d,"pad":"%032d"}`,
				round%seed, round, round,
			)
			if _, err := collection.Put(key, grown); err != nil {
				return fmt.Errorf("cow put %q: %w", key, err)
			}
			if round%7 == 0 {
				if _, err := collection.Delete(key); err != nil {
					return fmt.Errorf("delete %q: %w", key, err)
				}
				if _, err := collection.Put(key, same); err != nil {
					return fmt.Errorf("reinsert %q: %w", key, err)
				}
			}
			fresh := fmt.Sprintf("primary-key-%09d", seed+round)
			if _, err := collection.Put(fresh, grown); err != nil {
				return fmt.Errorf("insert %q: %w", fresh, err)
			}
			if round%97 == 0 {
				if err := collection.Flush(); err != nil {
					return fmt.Errorf("checkpoint: %w", err)
				}
			}
			writesDone.Add(1)
		}
		return nil
	}()
	stop.Store(true)
	group.Wait()
	if writerErr != nil {
		t.Fatal(writerErr)
	}
	if readsDone.Load() == 0 || scansDone.Load() == 0 || writesDone.Load() == 0 {
		t.Fatalf(
			"stress made no progress: reads=%d scans=%d writes=%d",
			readsDone.Load(), scansDone.Load(), writesDone.Load(),
		)
	}
	t.Logf(
		"reads=%d scans=%d writes=%d inplace=%d/%d fallbacks=%d",
		readsDone.Load(), scansDone.Load(), writesDone.Load(),
		collection.bufferedInplaceUpdates.Load(),
		collection.bufferedInplaceAttempts.Load(),
		collection.bufferedInplaceFallbacks.Load(),
	)
}
