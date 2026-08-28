package durable

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

func newCanonicalBatch(t testing.TB, opaque bool) (*Collection, *WriteBatch) {
	t.Helper()
	options := testBatchOptions(16)
	options.OpaqueValues = opaque
	options.MaxDocumentBytes = 64 << 10
	options.MaxBatchBytes = 1 << 20
	normalized, err := options.normalized()
	if err != nil {
		t.Fatal(err)
	}
	c := &Collection{options: normalized}
	return c, &WriteBatch{collection: c, position: make(map[string]int), active: true}
}

func canonicalBatchOracle(t testing.TB, raw []byte) []byte {
	t.Helper()
	want, err := vibejson.AppendCanonicalize(nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	return want
}

func TestPrimaryBatchCanonicalOracle(t *testing.T) {
	inputs := []string{
		` { "z": [3, {"b":true,"a":null}], "a":1 } `,
		`{"z":1,"a":2,"a":3,"\u0061":4}`,
		`{"n":-0.00e+02,"s":"\/\u000a\u0000<>&\u00e9\ud83d\ude00"}`,
		"{\"z\":\"\u2028\u2029\",\"\u2028\":\"x\"}",
		"{\"\u2028\":1}",
		`{"a":"\u2028\u2029","z":1}`,
		`[1e+2,-0,true,null,{"z":2,"a":1}]`,
	}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			c, batch := newCanonicalBatch(t, false)
			raw := []byte(input)
			before := bytes.Clone(raw)
			want := canonicalBatchOracle(t, raw)
			if err := batch.Put([]byte("k"), raw); err != nil {
				t.Fatal(err)
			}
			if err := c.canonicalizePrimaryBatchValues(batch); err != nil {
				t.Fatal(err)
			}
			if got := batch.value(batch.entries[0]); !bytes.Equal(got, want) {
				t.Fatalf("canonical = %q, want %q", got, want)
			}
			if !bytes.Equal(raw, before) {
				t.Fatal("modified caller input")
			}
			if batch.logicalBytes() != int64(1+len(want)) {
				t.Fatal("logical byte charge differs")
			}
			batch.canonical = false // prove revalidation, not merely the cached flag
			if err := c.canonicalizePrimaryBatchValues(batch); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(batch.value(batch.entries[0]), want) {
				t.Fatal("not idempotent")
			}
		})
	}
}

func TestPrimaryBatchCanonicalInterleavedSlots(t *testing.T) {
	c, batch := newCanonicalBatch(t, false)
	put := func(key, value string) {
		t.Helper()
		if err := batch.Put([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	put("a", `{"z":1,"a":0}`)
	put("b", `{"z":2,"a":0}`)
	put("c", `{"z":3,"a":0}`)
	put("a", ` { "z": [1,2,3], "a": "replacement" } `)
	if err := batch.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	put("b", "{\"z\":\"\u2028\u2029\",\"a\":2}")
	put("c", ` { "z":3, "a":0 } `)
	put("dead", `{"z":999}`)
	if err := batch.Delete([]byte("dead")); err != nil {
		t.Fatal(err)
	}
	// Deliberately reverse physical slot order and leave an orphaned prefix.
	// The normalizer must not rely on insertion order matching arena offsets.
	wants := make([][]byte, len(batch.entries))
	var arena []byte
	arena = append(arena, []byte("orphaned-value")...)
	for i := len(batch.entries) - 1; i >= 0; i-- {
		entry := &batch.entries[i]
		old := batch.value(*entry)
		if !entry.remove {
			wants[i] = canonicalBatchOracle(t, old)
		}
		entry.valueOffset = len(arena)
		arena = append(arena, old...)
	}
	batch.values = arena
	if err := c.canonicalizePrimaryBatchValues(batch); err != nil {
		t.Fatal(err)
	}
	var logical int64
	for i, entry := range batch.entries {
		if !bytes.Equal(batch.value(entry), wants[i]) {
			t.Fatalf("slot %d corrupted", i)
		}
		logical += int64(entry.keyLength + len(wants[i]))
	}
	if batch.logicalBytes() != logical {
		t.Fatal("holes charged as live bytes")
	}
	if len(batch.values) <= int(logical)-len(batch.keys) {
		t.Fatal("fixture did not retain holes")
	}
}

func TestPrimaryBatchCanonicalExpansionLimitsAndRecovery(t *testing.T) {
	raw := []byte("{\"a\":\"\u2028\u2029\"}")
	want := canonicalBatchOracle(t, raw)
	if len(want) <= len(raw) {
		t.Fatal("expected legal expansion")
	}
	for _, test := range []string{"document", "batch", "recovery", "opaque"} {
		t.Run(test, func(t *testing.T) {
			c, batch := newCanonicalBatch(t, test == "opaque")
			if test == "document" {
				c.options.MaxDocumentBytes = len(raw)
			}
			c.options.MaxBatchBytes = len(raw) + 1
			if test == "recovery" {
				if err := batch.appendRecovery([]byte("k"), raw, false); err != nil {
					t.Fatal(err)
				}
			} else if err := batch.Put([]byte("k"), raw); err != nil {
				t.Fatal(err)
			}
			err := c.canonicalizePrimaryBatchValues(batch)
			switch test {
			case "document":
				if !errors.Is(err, ErrDocumentTooLarge) {
					t.Fatalf("got %v", err)
				}
			case "batch":
				if !errors.Is(err, ErrBatchTooLarge) {
					t.Fatalf("got %v", err)
				}
			case "recovery":
				if err != nil || !bytes.Equal(batch.value(batch.entries[0]), want) {
					t.Fatalf("recovery: %v", err)
				}
			case "opaque":
				if err != nil || !bytes.Equal(batch.value(batch.entries[0]), raw) {
					t.Fatalf("opaque: %v", err)
				}
			}
		})
	}
}

func TestPrimaryBatchCanonicalSingleBatchInlineOverflow(t *testing.T) {
	for _, size := range []int{8, 3 << 10} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			options := primaryBatchOverflowOptions(DurabilitySync)
			c, file := openBatchCollection(t, options)
			raw := fmt.Appendf(nil, ` { "tag":"same", "pad":%q, "a":"%s" } `, strings.Repeat("x", size), "\u2028\u2029")
			want := canonicalBatchOracle(t, raw)
			if _, err := c.Put([]byte("single"), raw); err != nil {
				t.Fatal(err)
			}
			if err := c.Update(func(batch *WriteBatch) error { return batch.Put([]byte("batch"), raw) }); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"single", "batch"} {
				requirePrimaryBatchRaw(t, c, key, want)
			}
			if got := primaryExactTestKeys(t, c, "tag", primaryExactTestNeedle(t, `"same"`)); len(got) != 2 {
				t.Fatalf("index = %v", got)
			}
			before := c.Generation()
			err := c.Update(func(batch *WriteBatch) error {
				if err := batch.Put([]byte("batch"), []byte(`{"tag":"changed","a":1}`)); err != nil {
					return err
				}
				return batch.Put([]byte("bad"), []byte(`{"tag":`))
			})
			if err == nil {
				t.Fatal("malformed later document accepted")
			}
			if c.Generation() != before {
				t.Fatal("failed validation published a generation")
			}
			requirePrimaryBatchRaw(t, c, "batch", want)
			if got := primaryExactTestKeys(t, c, "tag", primaryExactTestNeedle(t, `"changed"`)); len(got) != 0 {
				t.Fatalf("failed index mutation = %v", got)
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(file, options)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			for _, key := range []string{"single", "batch"} {
				requirePrimaryBatchRaw(t, reopened, key, want)
			}
		})
	}
}

func TestPrimaryBatchCanonicalSchemaFailure(t *testing.T) {
	options := testBatchOptions(2)
	options.Collection.Schema = testDurableStoreSchema(t)
	c, _ := openBatchCollection(t, options)
	err := c.Update(func(batch *WriteBatch) error {
		if err := batch.Put([]byte("valid"), []byte(`{"profile":{"name":"Ada"},"id":1}`)); err != nil {
			return err
		}
		return batch.Put([]byte("invalid"), []byte(`{"profile":{},"id":2}`))
	})
	if !errors.Is(err, store.ErrSchemaViolation) {
		t.Fatalf("schema = %v", err)
	}
	requirePrimaryBatchMissing(t, c, "valid")
	requirePrimaryBatchMissing(t, c, "invalid")
}

func TestPrimaryBatchCanonicalTransactionActualBytes(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		t.Run(fmt.Sprint(checkpoint), func(t *testing.T) {
			_, members, log := newCheckpointGroupTestResources(t, "a", "b")
			var group *CheckpointGroup
			if checkpoint {
				var err error
				group, err = NewCheckpointGroup(log, members, CheckpointGroupOptions{CheckpointEvery: 64})
				if err != nil {
					t.Fatal(err)
				}
				defer group.Close()
			}
			raw := []byte("{\"z\":\"\u2028\u2029\",\"a\":1}")
			want := canonicalBatchOracle(t, raw)
			write := func(batch *DatabaseBatch) error {
				for _, member := range members {
					b, err := batch.Collection(member.Name)
					if err != nil {
						return err
					}
					if err := b.Put([]byte("k"), raw); err != nil {
						return err
					}
				}
				return nil
			}
			limits := TxnLimits{MaxCollections: 2, MaxDocuments: 2, MaxBytes: int64(2 * (1 + len(raw)))}
			update := func() error {
				if group != nil {
					return group.Update(1, members, limits, write)
				}
				return UpdateCollections(log, members, limits, write)
			}
			beforeJournals := make([][]byte, len(members))
			for i, member := range members {
				beforeJournals[i] = journalBytes(t, member.Collection)
			}
			if err := update(); !errors.Is(err, ErrTxnTooLarge) {
				t.Fatalf("expanded aggregate admission = %v", err)
			}
			for i, member := range members {
				requirePrimaryBatchMissing(t, member.Collection, "k")
				if !bytes.Equal(journalBytes(t, member.Collection), beforeJournals[i]) {
					t.Fatal("rejected bytes staged a journal")
				}
			}
			limits.MaxBytes = int64(2 * (1 + len(want)))
			if err := update(); err != nil {
				t.Fatal(err)
			}
			for _, member := range members {
				requirePrimaryBatchRaw(t, member.Collection, "k", want)
			}
		})
	}
}

func TestPrimaryBatchCanonicalSteadyStateAllocs(t *testing.T) {
	for _, opaque := range []bool{false, true} {
		c, batch := newCanonicalBatch(t, opaque)
		if err := batch.Put([]byte("k"), []byte(`{"a":1,"z":"value"}`)); err != nil {
			t.Fatal(err)
		}
		if err := c.canonicalizePrimaryBatchValues(batch); err != nil {
			t.Fatal(err)
		}
		allocs := testing.AllocsPerRun(100, func() {
			batch.canonical = false
			if err := c.canonicalizePrimaryBatchValues(batch); err != nil {
				panic(err)
			}
		})
		if allocs != 0 {
			t.Fatalf("opaque=%t: %g allocations", opaque, allocs)
		}
	}
}

func BenchmarkPrimaryBatchCanonical(b *testing.B) {
	for _, name := range []string{"canonical", "rewrite", "unicode-expand", "opaque"} {
		b.Run(name, func(b *testing.B) {
			c, batch := newCanonicalBatch(b, name == "opaque")
			raw := []byte(`{"a":1,"z":"value"}`)
			if name == "rewrite" {
				raw = []byte(` { "z":"value", "a":1 } `)
			}
			if name == "unicode-expand" {
				raw = []byte("{\"a\":\"\u2028\u2029\"}")
			}
			key := []byte("k")
			for i := 0; i < 2; i++ {
				batch.reset()
				if err := batch.Put(key, raw); err != nil {
					b.Fatal(err)
				}
				if err := c.canonicalizePrimaryBatchValues(batch); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch.reset()
				if err := batch.Put(key, raw); err != nil {
					b.Fatal(err)
				}
				if err := c.canonicalizePrimaryBatchValues(batch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func FuzzPrimaryBatchCanonical(f *testing.F) {
	for _, seed := range []string{`{"z":1,"a":2}`, "{\"a\":\"\u2028\u2029\"}", `{"a":1,"a":2}`, ` { "b":[null,true], "a":"\u0000" } `} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) == 0 || len(raw) > 16<<10 {
			t.Skip()
		}
		want, err := vibejson.AppendCanonicalize(nil, raw)
		if err != nil {
			return
		}
		c, batch := newCanonicalBatch(t, false)
		if err := batch.Put([]byte("k"), raw); err != nil {
			t.Fatal(err)
		}
		if err := c.canonicalizePrimaryBatchValues(batch); err != nil {
			t.Fatal(err)
		}
		if got := batch.value(batch.entries[0]); !bytes.Equal(got, want) {
			t.Fatalf("got %q want %q", got, want)
		}
		if len(want) > 2*len(raw) {
			t.Fatal("canonical output exceeded proven expansion bound")
		}
	})
}
