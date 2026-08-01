package driver

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/query"
)

// durableBuildGateContext pauses the second direct Err call. Its nil Done
// channel deliberately leaves the driver's lock and publication checkpoints on
// their non-cancellable path. The durable collection makes its first Err call
// before claiming the online-build flag and its second after claiming that flag,
// so the pause is inside a live build after createIndexContext has captured the
// collection and released database.mu.
type durableBuildGateContext struct {
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
	errCalls    atomic.Uint32
}

func newDurableBuildGateContext() *durableBuildGateContext {
	return &durableBuildGateContext{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (*durableBuildGateContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*durableBuildGateContext) Done() <-chan struct{}       { return nil }
func (*durableBuildGateContext) Value(any) any               { return nil }

func (c *durableBuildGateContext) Err() error {
	if c.errCalls.Add(1) == 2 {
		c.enterOnce.Do(func() {
			close(c.entered)
			<-c.release
		})
	}
	return nil
}

func (c *durableBuildGateContext) unblock() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func TestCreateIndexRejectsReplacedStorageIncarnation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := database.close(); closeErr != nil {
			t.Errorf("close database: %v", closeErr)
		}
	}()
	prepareIncarnationTable(t, database)

	database.mu.RLock()
	oldTable := database.tables["docs"]
	oldSnapshot, err := oldTable.collection.Snapshot()
	database.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := oldSnapshot.Close(); closeErr != nil {
			t.Errorf("close old snapshot: %v", closeErr)
		}
	}()

	statement, err := query.PrepareDML(`CREATE INDEX by_zone ON docs (zone)`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	gate := newDurableBuildGateContext()
	defer gate.unblock()
	createDone := make(chan error, 1)
	go func() {
		_, createErr := database.createIndexContext(gate, statement)
		createDone <- createErr
	}()

	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("CREATE INDEX did not reach the durable build after releasing database.mu")
	}

	database.mu.Lock()
	err = database.truncateTableStorageLockedContext(context.Background(), "docs")
	currentTable := database.tables["docs"]
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if currentTable == oldTable {
		t.Fatal("TRUNCATE did not publish a replacement storage incarnation")
	}

	gate.unblock()
	select {
	case err = <-createDone:
	case <-time.After(15 * time.Second):
		t.Fatal("CREATE INDEX did not finish after the durable build was released")
	}
	if !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("CREATE INDEX after incarnation replacement = %v, want %v", err, ErrTransactionConflict)
	}

	snapshot, err := currentTable.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := snapshot.Close(); closeErr != nil {
			t.Errorf("close current snapshot: %v", closeErr)
		}
	}()
	for _, index := range snapshot.AppendIndexes(nil) {
		if index.Name == "by_zone" {
			t.Fatal("replacement incarnation unexpectedly contains the stale CREATE INDEX result")
		}
	}
}
