package txnclock

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestClockHistoryIsHardBounded(t *testing.T) {
	var clock Clock
	clock.Arm()
	old := clock.Begin()
	for i := 0; i <= HistoryKeys; i++ {
		clock.RecordKeys([]string{fmt.Sprintf("key-%d", i)})
		if len(clock.Writes) > HistoryKeys {
			t.Fatalf(
				"conflict history grew to %d keys, limit %d",
				len(clock.Writes), HistoryKeys,
			)
		}
	}
	if _, overflow, conflict := clock.Conflict(old, []string{"untouched"}); !conflict || !overflow {
		t.Fatalf(
			"pre-overflow transaction conflict = (overflow %v, conflict %v), want true, true",
			overflow, conflict,
		)
	}

	afterOverflow := clock.Begin()
	clock.RecordKeys([]string{"later"})
	key, overflow, conflict := clock.Conflict(
		afterOverflow, []string{"later"},
	)
	if !conflict || overflow || key != "later" {
		t.Fatalf(
			"post-overflow exact conflict = (%q, overflow %v, conflict %v)",
			key, overflow, conflict,
		)
	}
	clock.Finish(old)
	if len(clock.Writes) != 1 {
		t.Fatalf("history after doomed transaction finished = %d, want 1", len(clock.Writes))
	}
	clock.Finish(afterOverflow)
	if clock.Writes != nil || clock.Active != nil {
		t.Fatalf(
			"last transaction retained history: writes %d active %d",
			len(clock.Writes), len(clock.Active),
		)
	}
}

func TestClockRevisionWindows(t *testing.T) {
	var clock Clock
	clock.Arm()

	first := clock.Begin()
	clock.RecordKeys([]string{"a"})
	if key, overflow, conflict := clock.Conflict(first, []string{"a"}); !conflict || overflow || key != "a" {
		t.Fatalf("first writer conflict = (%q, overflow %v, conflict %v)", key, overflow, conflict)
	}

	second := clock.Begin()
	if _, overflow, conflict := clock.Conflict(second, []string{"a"}); conflict || overflow {
		t.Fatalf("second begin saw stale conflict on a: overflow %v conflict %v", overflow, conflict)
	}
	clock.RecordKeys([]string{"b"})
	if key, overflow, conflict := clock.Conflict(second, []string{"b"}); !conflict || overflow || key != "b" {
		t.Fatalf("second writer conflict = (%q, overflow %v, conflict %v)", key, overflow, conflict)
	}
	if _, overflow, conflict := clock.Conflict(second, []string{"a"}); conflict || overflow {
		t.Fatalf("second begin conflicted on a written before it began")
	}

	clock.Finish(first)
	if _, ok := clock.Writes["a"]; ok {
		t.Fatalf("finish retained key a after the only observer finished")
	}
	if _, ok := clock.Writes["b"]; !ok {
		t.Fatalf("finish dropped key b still observable by second")
	}
	clock.Finish(second)
}

func TestClockRevisionExhaustionFailsClosedWithoutWrapping(t *testing.T) {
	var clock Clock
	clock.Arm()
	clock.revision = maxRevision - 1

	beforeMaximum := clock.Begin()
	clock.RecordKeys([]string{"at-maximum"})
	if clock.revision != maxRevision || clock.revisionStopped {
		t.Fatalf("first maximum write stopped clock: revision %d stopped %v", clock.revision, clock.revisionStopped)
	}
	if key, overflow, conflict := clock.Conflict(beforeMaximum, []string{"at-maximum"}); !conflict || overflow || key != "at-maximum" {
		t.Fatalf("maximum revision conflict = (%q, overflow %v, conflict %v)", key, overflow, conflict)
	}

	// Reusing MaxUint64 remains exact for transactions whose tokens are lower.
	clock.RecordKeys([]string{"also-maximum"})
	if clock.revisionStopped {
		t.Fatal("clock stopped without an active maximum-revision transaction")
	}
	if key, overflow, conflict := clock.Conflict(beforeMaximum, []string{"also-maximum"}); !conflict || overflow || key != "also-maximum" {
		t.Fatalf("reused maximum conflict = (%q, overflow %v, conflict %v)", key, overflow, conflict)
	}

	atMaximum := clock.Begin()
	clock.RecordKeys([]string{"unrepresentable"})
	if clock.revision != maxRevision || !clock.revisionStopped {
		t.Fatalf("exhausted clock = revision %d stopped %v", clock.revision, clock.revisionStopped)
	}
	if clock.Writes != nil {
		t.Fatalf("exhausted clock retained %d ambiguous writes", len(clock.Writes))
	}
	for name, begin := range map[string]uint64{
		"older":   beforeMaximum,
		"maximum": atMaximum,
		"future":  clock.Begin(),
	} {
		if key, overflow, conflict := clock.Conflict(begin, []string{"untouched"}); !conflict || !overflow || key != "" {
			t.Fatalf("%s validation after exhaustion = (%q, overflow %v, conflict %v)", name, key, overflow, conflict)
		}
	}

	clock.Finish(beforeMaximum)
	clock.Finish(atMaximum)
	clock.Finish(maxRevision)
	if _, overflow, conflict := clock.Conflict(clock.Begin(), nil); !conflict || !overflow {
		t.Fatalf("quiescence reopened exhausted clock: overflow %v conflict %v", overflow, conflict)
	}
}

func TestClockMaximumRevisionWithoutReadersRemainsUsable(t *testing.T) {
	var clock Clock
	clock.Arm()
	clock.revision = maxRevision
	clock.RecordKeys([]string{"no-reader"})
	if clock.revisionStopped || clock.revision != maxRevision {
		t.Fatalf("reader-free maximum write = revision %d stopped %v", clock.revision, clock.revisionStopped)
	}
	if clock.Writes != nil {
		t.Fatalf("reader-free maximum write retained history: %v", clock.Writes)
	}
}

func TestClockActiveCountExhaustionRetainsHistory(t *testing.T) {
	var clock Clock
	clock.Arm()
	clock.Active = map[uint64]uint32{0: maxActiveCount - 1}

	if begin := clock.Begin(); begin != 0 {
		t.Fatalf("begin revision = %d, want 0", begin)
	}
	if count := clock.Active[0]; count != maxActiveCount {
		t.Fatalf("active saturation count = %d, want %d", count, uint64(maxActiveCount))
	}
	for i := 0; i < 3; i++ {
		clock.Finish(0)
	}
	if count := clock.Active[0]; count != maxActiveCount {
		t.Fatalf("saturated active count after Finish = %d, want %d", count, uint64(maxActiveCount))
	}

	clock.RecordKeys([]string{"protected"})
	if key, overflow, conflict := clock.Conflict(0, []string{"protected"}); !conflict || overflow || key != "protected" {
		t.Fatalf("saturated holder conflict = (%q, overflow %v, conflict %v)", key, overflow, conflict)
	}
}

func TestClockActiveMaximumExactCountCanFinish(t *testing.T) {
	var clock Clock
	clock.Active = map[uint64]uint32{0: maxActiveCount - 2}
	clock.Begin()
	if count := clock.Active[0]; count != maxActiveCount-1 {
		t.Fatalf("representable active count = %d, want %d", count, uint64(maxActiveCount-1))
	}
	clock.Finish(0)
	if count := clock.Active[0]; count != maxActiveCount-2 {
		t.Fatalf("active count after Finish = %d, want %d", count, uint64(maxActiveCount-2))
	}
}

func TestClockArmedCountExhaustionStaysArmed(t *testing.T) {
	var clock Clock
	clock.armed.Store(maxActiveCount - 1)
	clock.Arm()
	if clock.Armed() != maxActiveCount {
		t.Fatalf("armed saturation count = %d, want %d", clock.Armed(), uint64(maxActiveCount))
	}
	for i := 0; i < 3; i++ {
		clock.Disarm()
	}
	if clock.Armed() != maxActiveCount {
		t.Fatalf("Disarm reopened saturated armed gate: %d", clock.Armed())
	}

	begin := clock.Begin()
	clock.RecordKeys([]string{"still-published"})
	if key, overflow, conflict := clock.Conflict(begin, []string{"still-published"}); !conflict || overflow || key != "still-published" {
		t.Fatalf("saturated armed publication = (%q, overflow %v, conflict %v)", key, overflow, conflict)
	}
}

func TestClockUnarmedRecordIsNoop(t *testing.T) {
	var clock Clock
	begin := clock.Begin()
	clock.RecordKeys([]string{"a"})
	if clock.Writes != nil {
		t.Fatalf("unarmed RecordKeys retained history")
	}
	if _, _, conflict := clock.Conflict(begin, []string{"a"}); conflict {
		t.Fatalf("unarmed write became visible to Conflict")
	}
	clock.Finish(begin)
}

func TestClockArmDisarmQuiescence(t *testing.T) {
	var clock Clock
	clock.Arm()
	begin := clock.Begin()
	clock.RecordKeys([]string{"a"})
	clock.Disarm()
	if clock.Armed() != 0 {
		t.Fatalf("armed count after Disarm = %d, want 0", clock.Armed())
	}
	clock.RecordKeys([]string{"b"})
	if _, ok := clock.Writes["b"]; ok {
		t.Fatalf("RecordKeys after Disarm quiescence retained b")
	}
	if key, overflow, conflict := clock.Conflict(begin, []string{"a"}); !conflict || overflow || key != "a" {
		t.Fatalf("pre-disarm write lost: (%q, overflow %v, conflict %v)", key, overflow, conflict)
	}
	clock.Finish(begin)
	clock.Disarm() // zero-count no-op
	if clock.Armed() != 0 {
		t.Fatalf("Disarm at zero wrapped armed count to %d", clock.Armed())
	}
}

func TestClockArmingInvariantRace(t *testing.T) {
	// Stated invariant: every publication not visible in a transaction's begin
	// snapshot and committed before its commit validation is observable to that
	// validation. Under -race, hammer point writers against Begin/Conflict edges
	// while the clock is armed; after Disarm quiescence, RecordKeys must not
	// extend history.
	const (
		goroutines   = 8
		iterations   = 128
		keysPerWrite = 4
	)
	var (
		clock   Clock
		mu      sync.Mutex
		holders atomic.Int32
		misses  atomic.Uint64
		seen    atomic.Uint64
	)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				mu.Lock()
				if holders.Add(1) == 1 {
					clock.Arm()
				}
				begin := clock.Begin()
				mu.Unlock()

				pointKey := fmt.Sprintf("point-%d-%d", id, i)
				txKeys := make([]string, keysPerWrite)
				for k := 0; k < keysPerWrite; k++ {
					txKeys[k] = fmt.Sprintf("tx-%d-%d-%d", id, i, k)
				}

				var pointDone sync.WaitGroup
				pointDone.Add(1)
				go func() {
					defer pointDone.Done()
					mu.Lock()
					defer mu.Unlock()
					if clock.Armed() == 0 {
						return
					}
					clock.RecordKeys([]string{pointKey})
				}()

				mu.Lock()
				clock.RecordKeys(txKeys)
				checkKeys := append(append([]string{}, txKeys...), pointKey)
				_, _, conflict := clock.Conflict(begin, checkKeys)
				if !conflict {
					// Self-recorded keys must always conflict. A concurrent point
					// write may still be in flight; only count a miss when the
					// transaction's own recorded set is invisible.
					if _, _, self := clock.Conflict(begin, txKeys); !self {
						misses.Add(1)
					}
				} else {
					seen.Add(1)
				}
				clock.Finish(begin)
				if holders.Add(-1) == 0 {
					clock.Disarm()
				}
				mu.Unlock()
				pointDone.Wait()
			}
		}(g)
	}
	wg.Wait()

	mu.Lock()
	if clock.Armed() != 0 {
		t.Fatalf("armed count after quiescence = %d, want 0", clock.Armed())
	}
	before := len(clock.Writes)
	clock.RecordKeys([]string{"after-quiescence"})
	if len(clock.Writes) != before {
		t.Fatalf("RecordKeys after Disarm quiescence grew history from %d to %d", before, len(clock.Writes))
	}
	mu.Unlock()

	if misses.Load() != 0 {
		t.Fatalf("armed clock lost %d self-recorded write sets", misses.Load())
	}
	if seen.Load() == 0 {
		t.Fatal("race test observed no conflicts")
	}
}

func BenchmarkClockRecordKeysUnarmed(b *testing.B) {
	var clock Clock
	keys := []string{"key"}
	b.ReportAllocs()
	for b.Loop() {
		clock.RecordKeys(keys)
	}
}

func BenchmarkClockConflictNoWrites(b *testing.B) {
	var clock Clock
	begin := clock.Begin()
	keys := []string{"key"}
	b.ReportAllocs()
	for b.Loop() {
		clock.Conflict(begin, keys)
	}
}

func TestClockChangedSinceDoesNotDependOnExactHistory(t *testing.T) {
	var clock Clock
	clock.Arm()
	begin := clock.Begin()
	if clock.ChangedSince(begin) {
		t.Fatal("new clock changed before a publication")
	}
	for i := 0; i < HistoryKeys+1; i++ {
		clock.RecordKeys([]string{fmt.Sprintf("key-%d", i)})
	}
	if !clock.ChangedSince(begin) {
		t.Fatal("coarse dependency missed publications after exact-history overflow")
	}
	after := clock.Begin()
	if clock.ChangedSince(after) {
		t.Fatal("new begin token was older than the current revision")
	}
}
