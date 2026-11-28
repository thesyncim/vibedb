package query

import (
	"fmt"
	"math"
	"testing"

	"github.com/thesyncim/vibejson/x/keyhash"
)

func TestFileGroupIndexBasicAndEdgeCases(t *testing.T) {
	var idx fileGroupIndex
	if idx.len() != 0 {
		t.Fatalf("zero-value len = %d, want 0", idx.len())
	}
	// Lookups and reset on zero-value index must be safe no-ops.
	if _, ok := idx.lookupBytes([]byte("missing"), nil); ok {
		t.Fatal("lookupBytes on empty index returned ok=true")
	}
	if _, ok := idx.lookupString("missing", nil); ok {
		t.Fatal("lookupString on empty index returned ok=true")
	}
	idx.reset()
	if idx.len() != 0 || len(idx.slots) != 0 {
		t.Fatalf("reset on zero-value mutated state: len=%d slots=%d", idx.len(), len(idx.slots))
	}

	// Empty key and groupID == 0 must work identically to any other entry.
	groups := []fileGroup{
		{key: ""},
		{key: "alpha"},
		{key: "beta"},
	}
	for id, g := range groups {
		h := keyhash.String(g.key)
		if h != keyhash.Bytes([]byte(g.key)) {
			t.Fatalf("keyhash mismatch between String and Bytes for %q", g.key)
		}
		idx.insertHash(h, id)
	}
	if idx.len() != len(groups) {
		t.Fatalf("len = %d, want %d", idx.len(), len(groups))
	}
	for id, g := range groups {
		got, ok := idx.lookupString(g.key, groups)
		if !ok || got != id {
			t.Fatalf("lookupString(%q) = (%d, %v), want (%d, true)", g.key, got, ok, id)
		}
		got, ok = idx.lookupBytes([]byte(g.key), groups)
		if !ok || got != id {
			t.Fatalf("lookupBytes(%q) = (%d, %v), want (%d, true)", g.key, got, ok, id)
		}
	}
	if _, ok := idx.lookupBytes(nil, groups); !ok {
		t.Fatal("lookupBytes(nil) should match empty string key")
	}
	if _, ok := idx.lookupString("gamma", groups); ok {
		t.Fatal("lookupString(gamma) unexpectedly found")
	}

	// O(1) reset retains capacity and clears logical entries.
	capBefore := len(idx.slots)
	idx.reset()
	if idx.len() != 0 {
		t.Fatalf("len after reset = %d, want 0", idx.len())
	}
	if len(idx.slots) != capBefore {
		t.Fatalf("slots len changed across reset: %d -> %d", capBefore, len(idx.slots))
	}
	for _, g := range groups {
		if _, ok := idx.lookupString(g.key, groups); ok {
			t.Fatalf("stale key %q still visible after reset", g.key)
		}
	}

	idx.release()
	if idx.len() != 0 || idx.slots != nil || idx.gen != 0 || idx.limit != 0 {
		t.Fatalf("release did not zero index: %+v", idx)
	}
}

func TestFileGroupIndexHashCollisionStress(t *testing.T) {
	for _, forcedHash := range []uint32{0, 1, 0xdeadbeef, math.MaxUint32} {
		t.Run(fmt.Sprintf("hash=%#x", forcedHash), func(t *testing.T) {
			var idx fileGroupIndex
			const n = 250
			groups := make([]fileGroup, n)
			for i := range n {
				groups[i] = fileGroup{key: fmt.Sprintf("collide-%04d", i)}
				if _, ok := idx.lookupBytesHash([]byte(groups[i].key), forcedHash, groups[:i]); ok {
					t.Fatalf("key %d unexpectedly found before insert", i)
				}
				idx.insertHash(forcedHash, i)
				// Verify every previously inserted key survives growth/rehash under 100% collisions.
				for j := 0; j <= i; j++ {
					got, ok := idx.lookupStringHash(groups[j].key, forcedHash, groups[:i+1])
					if !ok || got != j {
						t.Fatalf("after insert %d: lookupStringHash(%q) = (%d, %v), want (%d, true)",
							i, groups[j].key, got, ok, j)
					}
					got, ok = idx.lookupBytesHash([]byte(groups[j].key), forcedHash, groups[:i+1])
					if !ok || got != j {
						t.Fatalf("after insert %d: lookupBytesHash(%q) = (%d, %v), want (%d, true)",
							i, groups[j].key, got, ok, j)
					}
				}
			}
			// Absent key with the exact same colliding hash must miss cleanly.
			if _, ok := idx.lookupBytesHash([]byte("absent-collider"), forcedHash, groups); ok {
				t.Fatal("absent key with colliding hash returned ok=true")
			}
			if _, ok := idx.lookupStringHash("absent-collider", forcedHash, groups); ok {
				t.Fatal("absent string with colliding hash returned ok=true")
			}

			idx.reset()
			for i := range n {
				if _, ok := idx.lookupStringHash(groups[i].key, forcedHash, groups); ok {
					t.Fatalf("colliding key %d visible after reset", i)
				}
			}
		})
	}
}

func TestFileGroupIndexGenerationWraparound(t *testing.T) {
	var idx fileGroupIndex
	groupsGen1 := []fileGroup{{key: "gen1-a"}, {key: "gen1-b"}}
	for id, g := range groupsGen1 {
		idx.insertHash(keyhash.String(g.key), id)
	}
	if idx.gen != 1 {
		t.Fatalf("initial gen = %d, want 1", idx.gen)
	}

	// Advance generation to math.MaxUint32 without clearing the old gen=1 slots.
	idx.count = 0
	idx.gen = math.MaxUint32

	groupsMax := []fileGroup{{key: "max-a"}, {key: "max-b"}}
	for id, g := range groupsMax {
		idx.insertHash(keyhash.String(g.key), id)
	}
	for id, g := range groupsMax {
		if got, ok := idx.lookupString(g.key, groupsMax); !ok || got != id {
			t.Fatalf("at gen=MaxUint32: lookup(%q) = (%d, %v), want (%d, true)", g.key, got, ok, id)
		}
	}

	// Wraparound reset: gen overflows uint32 -> 0 -> clears slots and sets gen = 1.
	idx.reset()
	if idx.gen != 1 {
		t.Fatalf("gen after wraparound reset = %d, want 1", idx.gen)
	}
	if idx.len() != 0 {
		t.Fatalf("len after wraparound reset = %d, want 0", idx.len())
	}
	// Neither the old gen=1 entries nor the gen=MaxUint32 entries may resurrect!
	for i, s := range idx.slots {
		if s.gen != 0 {
			t.Fatalf("slot %d retained stale gen=%d after wraparound clear", i, s.gen)
		}
	}
	for _, g := range groupsGen1 {
		if _, ok := idx.lookupString(g.key, groupsGen1); ok {
			t.Fatalf("stale gen=1 key %q resurrected after wraparound", g.key)
		}
	}
	for _, g := range groupsMax {
		if _, ok := idx.lookupString(g.key, groupsMax); ok {
			t.Fatalf("stale gen=MaxUint32 key %q visible after wraparound", g.key)
		}
	}

	// Fresh inserts after wraparound must work normally.
	fresh := []fileGroup{{key: "post-wrap"}}
	idx.insertHash(keyhash.String(fresh[0].key), 0)
	if got, ok := idx.lookupString("post-wrap", fresh); !ok || got != 0 {
		t.Fatalf("post-wrap lookup = (%d, %v), want (0, true)", got, ok)
	}

	// Growing directly from gen=MaxUint32 must discard stale gen=1 slots and
	// preserve every live gen=MaxUint32 entry rehashed at gen=1.
	idx.count = 0
	idx.gen = math.MaxUint32
	growGroups := make([]fileGroup, fileGroupIndexInitCap+10)
	for i := range growGroups {
		growGroups[i] = fileGroup{key: fmt.Sprintf("max-grow-%03d", i)}
		h := keyhash.String(growGroups[i].key)
		_, pos, ok := idx.lookupStringSlot(growGroups[i].key, h, growGroups[:i])
		if ok {
			t.Fatalf("unexpected hit for %q before insert", growGroups[i].key)
		}
		idx.insertAt(pos, h, i)
	}
	if idx.gen != 1 {
		t.Fatalf("gen after grow from MaxUint32 = %d, want 1", idx.gen)
	}
	if idx.len() != len(growGroups) {
		t.Fatalf("len after grow from MaxUint32 = %d, want %d", idx.len(), len(growGroups))
	}
	for i, g := range growGroups {
		if got, ok := idx.lookupString(g.key, growGroups); !ok || got != i {
			t.Fatalf("post-grow lookup(%q) = (%d, %v), want (%d, true)", g.key, got, ok, i)
		}
	}
}

func TestFileGroupIndexGrowthAndRehash(t *testing.T) {
	var idx fileGroupIndex
	const total = 10_000
	groups := make([]fileGroup, total)
	for i := range total {
		groups[i] = fileGroup{key: fmt.Sprintf("key-%06d", i)}
		h := keyhash.String(groups[i].key)
		if _, ok := idx.lookupStringHash(groups[i].key, h, groups[:i]); ok {
			t.Fatalf("unexpected hit for %q before insert", groups[i].key)
		}
		idx.insertHash(h, i)

		n := len(idx.slots)
		if n == 0 || (n&(n-1)) != 0 {
			t.Fatalf("slots len %d is not a power of two", n)
		}
		if uint64(idx.len())*10 > uint64(n)*7 {
			t.Fatalf("load factor exceeded 70%%: count=%d slots=%d", idx.len(), n)
		}
	}
	for i, g := range groups {
		got, ok := idx.lookupString(g.key, groups)
		if !ok || got != i {
			t.Fatalf("lookupString(%q) = (%d, %v), want (%d, true)", g.key, got, ok, i)
		}
		got, ok = idx.lookupBytes([]byte(g.key), groups)
		if !ok || got != i {
			t.Fatalf("lookupBytes(%q) = (%d, %v), want (%d, true)", g.key, got, ok, i)
		}
	}

	// Reset, insert a smaller set, then grow past the old capacity and verify
	// stale entries from the previous generation were not carried into newSlots.
	idx.reset()
	const second = 12_000
	groups2 := make([]fileGroup, second)
	for i := range second {
		groups2[i] = fileGroup{key: fmt.Sprintf("round2-%06d", i)}
		idx.insertHash(keyhash.String(groups2[i].key), i)
	}
	if idx.len() != second {
		t.Fatalf("round2 len = %d, want %d", idx.len(), second)
	}
	live := 0
	for _, s := range idx.slots {
		if s.gen == idx.gen {
			live++
		}
	}
	if live != second {
		t.Fatalf("live slots after grow = %d, want %d", live, second)
	}
	for i, g := range groups2 {
		if got, ok := idx.lookupString(g.key, groups2); !ok || got != i {
			t.Fatalf("round2 lookup(%q) = (%d, %v), want (%d, true)", g.key, got, ok, i)
		}
	}
}

func TestFileGroupIndexWarmZeroAlloc(t *testing.T) {
	const n = 256
	groups := make([]fileGroup, n)
	keyBytes := make([][]byte, n)
	for i := range n {
		k := fmt.Sprintf("warm-group-key-%04d", i)
		groups[i] = fileGroup{key: k}
		keyBytes[i] = []byte(k)
	}

	var idx fileGroupIndex
	// Warm up backing slice to final capacity.
	for i := range n {
		idx.insertHash(keyhash.Bytes(keyBytes[i]), i)
	}

	allocs := testing.AllocsPerRun(200, func() {
		idx.reset()
		for i := range n {
			h := keyhash.Bytes(keyBytes[i])
			_, pos, ok := idx.lookupBytesSlot(keyBytes[i], h, groups)
			if ok {
				t.Fatalf("unexpected hit on fresh batch for %d", i)
			}
			idx.insertAt(pos, h, i)
		}
		for i := range n {
			if got, ok := idx.lookupBytes(keyBytes[i], groups); !ok || got != i {
				t.Fatalf("lookupBytes(%d) = (%d, %v)", i, got, ok)
			}
			if got, ok := idx.lookupString(groups[i].key, groups); !ok || got != i {
				t.Fatalf("lookupString(%d) = (%d, %v)", i, got, ok)
			}
		}
	})
	if allocs != 0 {
		t.Fatalf("warm fileGroupIndex operations allocated %.1f allocs/run, want 0", allocs)
	}
}

func TestFileGroupParallelDurable(t *testing.T) {
	const numDocs = 4000
	snapshot := durableGroupedCorpus(t, numDocs)

	// Reference counts and sums for each cardinality column.
	type agg struct {
		count int64
		sum   int64
	}
	wantLow := make(map[string]agg, 16)
	wantMed := make(map[string]agg, 512)
	wantHigh := make(map[string]agg, 4000)
	for i := range numDocs {
		amt := int64((i * 17) % 1000)
		kl := fmt.Sprintf("grp-%02d", i%16)
		km := fmt.Sprintf("grp-%04d", i%512)
		kh := fmt.Sprintf("grp-%05d", i%10000)
		al := wantLow[kl]
		al.count++
		al.sum += amt
		wantLow[kl] = al
		am := wantMed[km]
		am.count++
		am.sum += amt
		wantMed[km] = am
		ah := wantHigh[kh]
		ah.count++
		ah.sum += amt
		wantHigh[kh] = ah
	}

	verify := func(t *testing.T, e *Exec, col string, want map[string]agg) {
		t.Helper()
		q := Select(Path(col), Count(), Sum("amount")).GroupBy(col)
		// Run twice on the same Exec to exercise warm O(1) resets across runs.
		for run := range 2 {
			if err := q.RunInto(e, FromFile(snapshot)); err != nil {
				t.Fatalf("col=%s run=%d: RunInto: %v", col, run, err)
			}
			if e.Result.RowCount != len(want) {
				t.Fatalf("col=%s run=%d: got %d groups, want %d", col, run, e.Result.RowCount, len(want))
			}
			keyCol := e.Result.Columns[0].Cells
			cntCol := e.Result.Columns[1].Cells
			sumCol := e.Result.Columns[2].Cells
			seen := make(map[string]bool, len(want))
			for r := range e.Result.RowCount {
				k, ok := keyCol[r].Text()
				if !ok {
					t.Fatalf("col=%s run=%d row=%d: expected text group key", col, run, r)
				}
				if seen[k] {
					t.Fatalf("col=%s run=%d: duplicate group %q", col, run, k)
				}
				seen[k] = true
				w, ok := want[k]
				if !ok {
					t.Fatalf("col=%s run=%d: unexpected group %q", col, run, k)
				}
				gotCnt := string(cntCol[r].JSON())
				gotSum := string(sumCol[r].JSON())
				if gotCnt != fmt.Sprintf("%d", w.count) || gotSum != fmt.Sprintf("%d", w.sum) {
					t.Fatalf("col=%s run=%d key=%q: got (count=%s, sum=%s), want (%d, %d)",
						col, run, k, gotCnt, gotSum, w.count, w.sum)
				}
			}
		}
	}

	t.Run("parallel_no_spill", func(t *testing.T) {
		e := Exec{Options: ExecOptions{
			Workers:   4,
			BatchRows: 64,
		}}
		defer e.Release()
		verify(t, &e, "low", wantLow)
		verify(t, &e, "med", wantMed)
		verify(t, &e, "high", wantHigh)

		// Composite two-column GROUP BY exercises multi-field encoded byte keys.
		qComposite := Select(Path("low"), Path("med"), Count(), Sum("amount")).GroupBy("low", "med")
		if err := qComposite.RunInto(&e, FromFile(snapshot)); err != nil {
			t.Fatalf("composite GroupBy RunInto: %v", err)
		}
		// Since i%16 == (i%512)%16, every med value pairs with exactly one low value.
		if e.Result.RowCount != len(wantMed) {
			t.Fatalf("composite GroupBy rows = %d, want %d", e.Result.RowCount, len(wantMed))
		}
	})

	t.Run("parallel_with_spill", func(t *testing.T) {
		e := Exec{Options: ExecOptions{
			Workers:     4,
			BatchRows:   64,
			MemoryBytes: 64 << 10,
		}}
		defer e.Release()
		verify(t, &e, "med", wantMed)
		if e.Stats.SpillRuns == 0 {
			t.Fatal("expected spill runs with 64 KiB MemoryBytes on med cardinality")
		}
		verify(t, &e, "high", wantHigh)
		if e.Stats.SpillRuns == 0 {
			t.Fatal("expected spill runs with 64 KiB MemoryBytes on high cardinality")
		}
	})
}
