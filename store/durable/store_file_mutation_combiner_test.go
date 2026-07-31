package durable

import (
	"fmt"
	"sync"
	"testing"
)

func TestPrimaryMutationCombinerGroupsSyncWrites(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	collection, _ := openJournalGroupCollection(t, options)

	const writers = 12
	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	done.Add(writers)
	for writer := 0; writer < writers; writer++ {
		go func(writer int) {
			defer done.Done()
			start.Wait()
			key := []byte(fmt.Sprintf("combine-%02d", writer))
			value := []byte(fmt.Sprintf(`{"writer":%d}`, writer))
			created, err := collection.Put(key, value)
			if err != nil {
				t.Errorf("put %q: %v", key, err)
				return
			}
			if !created {
				t.Errorf("put %q reported created=false", key)
			}
		}(writer)
	}
	start.Done()
	done.Wait()

	stats := collection.Stats()
	if stats.JournalAcks >= writers {
		t.Fatalf("journal acknowledgements=%d, want fewer than %d", stats.JournalAcks, writers)
	}

	for writer := 0; writer < writers; writer++ {
		key := []byte(fmt.Sprintf("combine-%02d", writer))
		value, found, err := collection.AppendRaw(nil, key)
		if err != nil {
			t.Fatalf("read %q: %v", key, err)
		}
		if !found || string(value) != fmt.Sprintf(`{"writer":%d}`, writer) {
			t.Fatalf("read %q = %q,%v", key, value, found)
		}
	}
}

func TestPrimaryMutationCombinerPreservesDeleteResults(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	collection, _ := openJournalGroupCollection(t, options)
	const rows = 8
	for row := 0; row < rows; row++ {
		key := []byte(fmt.Sprintf("delete-%02d", row))
		if created, err := collection.Put(key, []byte(`{"v":1}`)); err != nil || !created {
			t.Fatalf("seed put %q: created=%v err=%v", key, created, err)
		}
	}

	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	done.Add(rows)
	for row := 0; row < rows; row++ {
		go func(row int) {
			defer done.Done()
			start.Wait()
			deleted, err := collection.Delete(
				[]byte(fmt.Sprintf("delete-%02d", row)),
			)
			if err != nil {
				t.Errorf("delete row %d: %v", row, err)
				return
			}
			if !deleted {
				t.Errorf("delete row %d reported deleted=false", row)
			}
		}(row)
	}
	start.Done()
	done.Wait()

	for row := 0; row < rows; row++ {
		key := []byte(fmt.Sprintf("delete-%02d", row))
		if _, found, err := collection.AppendRaw(nil, key); err != nil {
			t.Fatalf("read deleted %q: %v", key, err)
		} else if found {
			t.Fatalf("deleted key %q is still present", key)
		}
	}
}
