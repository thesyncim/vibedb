package durable

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestPrimaryMutationGenerationExhaustionIsSideEffectFree(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	same, _ := concurrentPrimaryTestTargets(t, fixture)
	putKey := []byte(fixture.keys[same[0]])
	deleteKey := []byte(fixture.keys[same[1]])
	putBefore, putFound := collectionDoc(t, collection, string(putKey))
	deleteBefore, deleteFound := collectionDoc(t, collection, string(deleteKey))
	if !putFound || !deleteFound {
		t.Fatal("generation-exhaustion keys are not present")
	}
	putAfter := append([]byte(nil), putBefore...)
	marker := bytes.Index(putAfter, []byte(`"group":`))
	if marker < 0 {
		t.Fatalf("scalar field missing from %q", putAfter)
	}
	marker += len(`"group":`)
	if marker >= len(putAfter) || putAfter[marker] < '0' || putAfter[marker] > '9' {
		t.Fatalf("scalar field is not numeric in %q", putAfter)
	}
	if putAfter[marker] == '9' {
		putAfter[marker] = '8'
	} else {
		putAfter[marker]++
	}

	journal := collection.journal
	if journal == nil {
		t.Fatal("fixture did not root its recovery journal")
	}
	faultJournal := storeio.NewFaultJournal(journal)
	journalCursor := journal.Cursor()
	mainBefore, err := os.ReadFile(fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	journalPath := RecoveryJournalPath(fixture.path)
	journalBefore, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}

	collection.writer.Lock()
	originalState := collection.state.Load()
	originalVisible := collection.visibleState.Load()
	originalDurable := collection.durableState.Load()
	originalCut := collection.logicalCut.Load()
	router := collection.primaryRouter.Load()
	originalRouterGeneration := router.Generation()
	originalValidatorGeneration := collection.pageValidator.generation.Load()
	exhausted := *originalState
	exhausted.root.Generation = fileLogicalCutGenerationMask
	collection.state.Store(&exhausted)
	collection.visibleState.Store(&exhausted)
	collection.durableState.Store(&exhausted)
	exhaustedCut, ok := packFileLogicalCut(
		fileLogicalCutGenerationMask, 0,
	)
	if !ok {
		collection.writer.Unlock()
		t.Fatal("maximum generation did not fit logical cut")
	}
	collection.logicalCut.Store(exhaustedCut)
	router.AdvanceGeneration(fileLogicalCutGenerationMask)
	collection.pageValidator.advanceGeneration(fileLogicalCutGenerationMask)
	collection.writer.Unlock()
	t.Cleanup(func() {
		collection.writer.Lock()
		collection.state.Store(originalState)
		collection.visibleState.Store(originalVisible)
		collection.durableState.Store(originalDurable)
		collection.logicalCut.Store(originalCut)
		router.AdvanceGeneration(originalRouterGeneration)
		collection.pageValidator.advanceGeneration(originalValidatorGeneration)
		collection.writer.Unlock()
	})

	assertUnchanged := func(operation string) {
		t.Helper()
		if got := collection.Generation(); got != fileLogicalCutGenerationMask {
			t.Fatalf("%s visible generation = %d, want maximum", operation, got)
		}
		if got, found := collectionDoc(t, collection, string(putKey)); !found || got != putBefore {
			t.Fatalf("%s changed put key = %q,%v", operation, got, found)
		}
		if got, found := collectionDoc(t, collection, string(deleteKey)); !found || got != deleteBefore {
			t.Fatalf("%s changed delete key = %q,%v", operation, got, found)
		}
		if got := journal.Cursor(); got != journalCursor {
			t.Fatalf("%s journal cursor = %d, want %d", operation, got, journalCursor)
		}
		if got := faultJournal.Appends(); got != 0 {
			t.Fatalf("%s issued %d journal appends, want 0", operation, got)
		}
		if got := faultJournal.Syncs(); got != 0 {
			t.Fatalf("%s issued %d journal syncs, want 0", operation, got)
		}
		mainAfter, err := os.ReadFile(fixture.path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(mainAfter, mainBefore) {
			t.Fatalf("%s changed primary file bytes", operation)
		}
		journalAfter, err := os.ReadFile(journalPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(journalAfter, journalBefore) {
			t.Fatalf("%s changed recovery journal bytes", operation)
		}
	}

	if created, err := collection.Put(putKey, putAfter); created ||
		!errors.Is(err, storeio.ErrGenerationOrder) {
		t.Fatalf("unified scalar Put = %v,%v, want false,generation-order", created, err)
	}
	assertUnchanged("unified scalar Put")

	if deleted, err := collection.Delete(deleteKey); deleted ||
		!errors.Is(err, storeio.ErrGenerationOrder) {
		t.Fatalf("unified Delete = %v,%v, want false,generation-order", deleted, err)
	}
	assertUnchanged("unified Delete")

	err = collection.Update(func(batch *WriteBatch) error {
		return batch.Put(putKey, putAfter)
	})
	if !errors.Is(err, storeio.ErrGenerationOrder) {
		t.Fatalf("transactional COW Update = %v, want generation-order", err)
	}
	assertUnchanged("transactional COW Update")
}
