package durable

import (
	"bytes"
	"fmt"
	"testing"

	vibejson "github.com/thesyncim/vibejson"
)

// TestFilePrimaryIndexedInsertScale pins the bounded insert path: append,
// update, and delete batches publish exact-index overlay diff records for
// changed (term, tile) pairs only, so no batch or split falls back to a full
// structural rebuild however large the index grows. It then verifies every
// live row through both indexes — including rows cuckoo placement displaced
// across splits — and proves deleted and replaced terms left no stale
// postings behind.
func TestFilePrimaryIndexedInsertScale(t *testing.T) {
	indexes := exactPackIntegrationIndexes()
	options := journalTestOptions(CheckpointPowerSafe)
	options.RecoveryJournal = false
	options.Indexes = indexes
	coll, file, _ := openPrimaryBatchStore(t, options)
	defer func() { _ = coll.Close(); _ = file.Close() }()

	const rows = 640
	const batch = 64
	keys, documents := exactOverlapCorpus(t, rows, rows, 256)

	rebuildsBefore := coll.primaryExactStructuralRebuilds.Load()
	live := make(map[string][]byte, rows)

	// Append in small batches so leaves fill, split, and displace rows.
	for start := 0; start < rows; start += batch {
		end := start + batch
		if err := coll.Update(func(b *WriteBatch) error {
			for at := start; at < end; at++ {
				if err := b.Put([]byte(keys[at]), documents[at]); err != nil {
					return err
				}
				live[keys[at]] = documents[at]
			}
			return nil
		}); err != nil {
			t.Fatalf("append batch %d: %v", start/batch, err)
		}
	}

	// Rewrite the indexed fields of every 11th row: new terms must appear
	// and the replaced terms must retract.
	for at := 0; at < rows; at += 11 {
		key := keys[at]
		document := appendScaleVariantDocument(t, at)
		if err := coll.Update(func(b *WriteBatch) error {
			return b.Put([]byte(key), document)
		}); err != nil {
			t.Fatalf("update %s: %v", key, err)
		}
		live[key] = document
	}

	// Delete every 7th row of the first half, plus one absent key.
	for at := 0; at < rows/2; at += 7 {
		key := keys[at]
		if err := coll.Update(func(b *WriteBatch) error {
			return b.Delete([]byte(key))
		}); err != nil {
			t.Fatalf("delete %s: %v", key, err)
		}
		delete(live, key)
	}
	if err := coll.Update(func(b *WriteBatch) error {
		return b.Delete([]byte("overlap-999999999"))
	}); err != nil {
		t.Fatalf("absent delete: %v", err)
	}

	if got := coll.primaryExactStructuralRebuilds.Load() - rebuildsBefore; got != 0 {
		t.Fatalf("steady indexed batches took %d full structural rebuilds", got)
	}

	snapshot, err := coll.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	assertMasks := func(definition string, needles []vibejson.Index, key string, want bool) {
		t.Helper()
		masks, err := snapshot.AppendIndexMasks(nil, definition, needles...)
		if err != nil {
			t.Fatal(err)
		}
		matched := false
		if err := snapshot.RangeMasksRaw(masks, func(k, _ []byte) error {
			matched = matched || string(k) == key
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if matched != want {
			t.Fatalf("index %s key %s matched=%v want=%v", definition, key, matched, want)
		}
	}

	// Every live row must read back and resolve through both indexes under
	// its current terms.
	for key, want := range live {
		got, found, err := snapshot.AppendRaw(nil, []byte(key))
		if err != nil || !found || !bytes.Equal(got, want) {
			t.Fatalf("row %s found=%v err=%v", key, found, err)
		}
		for _, definition := range indexes {
			needles := exactPackIntegrationNeedles(t, want, definition)
			assertMasks(definition.Name, needles, key, true)
		}
	}

	// Deleted rows must be gone from every posting that once named them.
	for at := 0; at < rows/2; at += 7 {
		key := keys[at]
		old := documents[at]
		if _, found, err := snapshot.AppendRaw(nil, []byte(key)); err != nil || found {
			t.Fatalf("deleted row %s found=%v err=%v", key, found, err)
		}
		for _, definition := range indexes {
			needles := exactPackIntegrationNeedles(t, old, definition)
			assertMasks(definition.Name, needles, key, false)
		}
	}

	// Rewritten rows must no longer resolve under their replaced terms.
	for at := 0; at < rows; at += 11 {
		key := keys[at]
		if _, ok := live[key]; !ok {
			continue
		}
		old := documents[at]
		for _, definition := range indexes {
			needles := exactPackIntegrationNeedles(t, old, definition)
			assertMasks(definition.Name, needles, key, false)
		}
	}
}

// appendScaleVariantDocument renders a replacement document whose indexed
// tenant/shared/a/b values differ from every corpus row.
func appendScaleVariantDocument(tb testing.TB, row int) []byte {
	tb.Helper()
	document := fmt.Appendf(nil,
		`{"tenant":"t-variant","shared":"variant-%09d","a":"a-variant","b":"b-variant","id":%d}`,
		row, row,
	)
	canonical, err := vibejson.AppendCanonicalize(nil, document)
	if err != nil {
		tb.Fatal(err)
	}
	return canonical
}
