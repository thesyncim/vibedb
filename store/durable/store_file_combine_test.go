package durable

import (
	"errors"
	"strings"
	"testing"
	"time"
)

type combinedMutationResult struct {
	changed bool
	err     error
}

func waitForCombinedQueue(t *testing.T, collection *Collection, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		collection.combiner.mu.Lock()
		got := len(collection.combiner.queue)
		collection.combiner.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("automatic mutation queue = %d, want %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func awaitCombinedResult(
	t *testing.T, result <-chan combinedMutationResult,
) combinedMutationResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("automatic mutation did not complete")
		return combinedMutationResult{}
	}
}

func TestFileStoreAutomaticCombinerPreservesSameKeyResults(t *testing.T) {
	options := testBatchOptions(8)
	options.Durability = DurabilitySync
	collection, _ := openBatchCollection(t, options)
	initialGeneration := collection.Generation()

	collection.writer.Lock()
	putOne := make(chan combinedMutationResult, 1)
	go func() {
		changed, err := collection.Put(
			"same", []byte(`{"pad":"`+strings.Repeat("x", 32<<10)+`"}`),
		)
		putOne <- combinedMutationResult{changed: changed, err: err}
	}()
	waitForCombinedQueue(t, collection, 1)
	deleted := make(chan combinedMutationResult, 1)
	go func() {
		changed, err := collection.Delete("same")
		deleted <- combinedMutationResult{changed: changed, err: err}
	}()
	waitForCombinedQueue(t, collection, 2)
	putTwo := make(chan combinedMutationResult, 1)
	go func() {
		changed, err := collection.Put("same", []byte(`{"v":2}`))
		putTwo <- combinedMutationResult{changed: changed, err: err}
	}()
	waitForCombinedQueue(t, collection, 3)
	collection.writer.Unlock()

	for name, result := range map[string]combinedMutationResult{
		"first put":  awaitCombinedResult(t, putOne),
		"delete":     awaitCombinedResult(t, deleted),
		"second put": awaitCombinedResult(t, putTwo),
	} {
		if result.err != nil || !result.changed {
			t.Fatalf("%s = (%v,%v), want (true,nil)", name, result.changed, result.err)
		}
	}
	if got := collection.Generation(); got != initialGeneration+1 {
		t.Fatalf("combined generation = %d, want %d", got, initialGeneration+1)
	}
	if capacity := cap(collection.batch.values); capacity > 1024 {
		t.Fatalf(
			"automatic batch copied superseded 32 KiB value: arena capacity %d",
			capacity,
		)
	}
	raw, ok, err := collection.AppendRaw(nil, "same")
	if err != nil || !ok || string(raw) != `{"v":2}` {
		t.Fatalf("combined final value = (%q,%v,%v)", raw, ok, err)
	}
	stats := collection.Stats()
	if stats.AutomaticMutationGroups != 1 ||
		stats.AutomaticMutationRequests != 3 ||
		stats.LargestAutomaticMutationGroup != 3 {
		t.Fatalf("automatic mutation stats = %+v", stats)
	}
}

func TestFileStoreAutomaticCombinerIsolatesInvalidInput(t *testing.T) {
	collection, _ := openBatchCollection(t, testBatchOptions(8))
	collection.writer.Lock()
	if _, err := collection.Put("bad", []byte(`{"broken"`)); err == nil {
		collection.writer.Unlock()
		t.Fatal("invalid queued Put succeeded")
	}
	valid := make(chan combinedMutationResult, 1)
	go func() {
		changed, err := collection.Put("good", []byte(`{"ok":true}`))
		valid <- combinedMutationResult{changed: changed, err: err}
	}()
	waitForCombinedQueue(t, collection, 1)
	collection.writer.Unlock()
	result := awaitCombinedResult(t, valid)
	if result.err != nil || !result.changed {
		t.Fatalf("valid neighbour = (%v,%v), want (true,nil)", result.changed, result.err)
	}
	if _, ok, err := collection.AppendRaw(nil, "bad"); err != nil || ok {
		t.Fatalf("invalid key became visible = (%v,%v)", ok, err)
	}
}

func TestFileStoreAutomaticCombinerLeavesLoneMutationDirect(t *testing.T) {
	collection, _ := openBatchCollection(t, testBatchOptions(8))
	if created, err := collection.Put("lone", []byte(`{"v":1}`)); err != nil || !created {
		t.Fatalf("lone Put = (%v,%v)", created, err)
	}
	if deleted, err := collection.Delete("lone"); err != nil || !deleted {
		t.Fatalf("lone Delete = (%v,%v)", deleted, err)
	}
	stats := collection.Stats()
	if stats.AutomaticMutationGroups != 0 ||
		stats.AutomaticMutationRequests != 0 {
		t.Fatalf("lone mutations entered automatic combiner: %+v", stats)
	}
	if _, err := collection.Delete(
		string(make([]byte, collection.options.MaxKeyBytes+1)),
	); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("oversized direct Delete = %v, want ErrKeyTooLarge", err)
	}
}
