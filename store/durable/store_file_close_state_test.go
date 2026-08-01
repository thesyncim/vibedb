package durable

import (
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestFileCloseResumesAfterPinnedPageCacheWithoutRefinalizing(t *testing.T) {
	getFault, restore := installJournalFaultSeam(t)
	defer restore()
	file, err := os.CreateTemp(t.TempDir(), "file-close-phase-pinned-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, testFileStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put([]byte("baseline"), []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	fault := getFault()
	fault.Program(storeio.JournalFaultPlan{
		Phase:       storeio.JournalFaultENOSPCAppend,
		AppendIndex: fault.Appends(),
	})
	if _, err := collection.Put([]byte("rejected"), []byte(`{"n":2}`)); err == nil {
		t.Fatal("faulted mutation succeeded")
	}
	persistErr := collection.PersistenceError()
	if persistErr == nil {
		t.Fatal("faulted mutation did not poison persistence")
	}
	state := collection.state.Load()
	page, err := collection.cache.Acquire(state.root.PrimaryRoot)
	if err != nil {
		t.Fatal(err)
	}
	first := collection.Close()
	if !errors.Is(first, persistErr) || !errors.Is(first, storeio.ErrPageCachePinned) {
		page.Release()
		t.Fatalf("first Close = %v, want terminal %v and retryable %v",
			first, persistErr, storeio.ErrPageCachePinned)
	}
	if collection.CloseCompleted() || collection.closePhase != closePhaseCommitter {
		page.Release()
		t.Fatalf("blocked Close state = completed %v phase %d, want phase %d",
			collection.CloseCompleted(), collection.closePhase, closePhaseCommitter)
	}
	page.Release()
	second := collection.Close()
	if !errors.Is(second, persistErr) || errors.Is(second, storeio.ErrPageCachePinned) {
		t.Fatalf("resumed Close = %v, want only terminal %v", second, persistErr)
	}
	if !collection.CloseCompleted() || collection.closePhase != closePhaseUnlocked {
		t.Fatalf("completed Close state = completed %v phase %d, want phase %d",
			collection.CloseCompleted(), collection.closePhase, closePhaseUnlocked)
	}
	if repeated := collection.Close(); repeated != second {
		t.Fatalf("repeated Close = %v, want cached exact %v", repeated, second)
	}
}

func TestFileCloseRetriesOnlyWriterUnlockAfterResourcesConsumed(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-close-phase-unlock-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, testFileStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected writer unlock failure")
	previous := unlockCollectionWriter
	calls := 0
	unlockCollectionWriter = func(file *os.File) error {
		calls++
		if calls == 1 {
			return injected
		}
		return previous(file)
	}
	defer func() { unlockCollectionWriter = previous }()

	first := collection.Close()
	if !errors.Is(first, injected) {
		t.Fatalf("first Close = %v, want %v", first, injected)
	}
	if collection.CloseCompleted() || collection.closePhase != closePhaseBlocks ||
		!collection.writerLocked {
		t.Fatalf("failed unlock state = completed %v phase %d locked %v",
			collection.CloseCompleted(), collection.closePhase, collection.writerLocked)
	}
	if second := collection.Close(); second != nil {
		t.Fatalf("retry Close = %v", second)
	}
	if calls != 2 || !collection.CloseCompleted() || collection.writerLocked ||
		collection.closePhase != closePhaseUnlocked {
		t.Fatalf("retry state = calls %d completed %v phase %d locked %v",
			calls, collection.CloseCompleted(), collection.closePhase, collection.writerLocked)
	}
}

func TestFileCloseCachesConsumedDescriptorErrorAsTerminal(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-close-phase-terminal-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, testFileStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	consumed, err := os.CreateTemp(t.TempDir(), "already-closed-*")
	if err != nil {
		t.Fatal(err)
	}
	if err := consumed.Close(); err != nil {
		t.Fatal(err)
	}
	collection.readFile = consumed
	first := collection.Close()
	if first == nil {
		t.Fatal("Close hid consumed descriptor error")
	}
	if !collection.CloseCompleted() || collection.closePhase != closePhaseUnlocked {
		t.Fatalf("terminal descriptor state = completed %v phase %d",
			collection.CloseCompleted(), collection.closePhase)
	}
	if second := collection.Close(); second != first {
		t.Fatalf("repeated Close = %v, want cached exact %v", second, first)
	}
}
