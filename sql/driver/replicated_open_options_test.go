//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package driver

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
	"golang.org/x/sys/unix"
)

func TestReplicatedOpeningOptionsAreExplicitAndBounded(t *testing.T) {
	ctx := context.Background()
	deadline := time.Now().Add(time.Second)
	want := ReplicatedOpenOptions{WriterLockContext: ctx, WriterLockDeadline: deadline}
	got, err := replicatedOpeningOptions([]ReplicatedOpenOptions{want})
	if err != nil || got != want {
		t.Fatalf("policy lost original deadline/context: %+v %v", got, err)
	}
	for _, options := range [][]ReplicatedOpenOptions{
		{want, want}, {{WriterLockContext: ctx}}, {{WriterLockDeadline: deadline}},
	} {
		if _, err := replicatedOpeningOptions(options); !errors.Is(err, ErrReplicatedApplyMismatch) {
			t.Fatalf("invalid policy admitted: %v", err)
		}
	}
	if got, err := replicatedOpeningOptions(nil); err != nil || got != (ReplicatedOpenOptions{}) {
		t.Fatalf("default options: %+v %v", got, err)
	}
}

// Exercise the production catalog->complete-catalog->collection path with an
// ordinary physical SQL table, without the Linux-only sealed RF3 allocator.
func TestReplicatedOpeningPolicyReachesSQLCollectionWithoutReplayRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	session, err := db.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := testRuntimeExec(session, `CREATE TABLE docs (PRIMARY KEY (id))`, nil); err != nil {
		t.Fatal(err)
	}
	if err := testRuntimeExec(session, `INSERT INTO docs VALUES (?)`, []any{[]byte(`{"id":"kept","value":9}`)}); err != nil {
		t.Fatal(err)
	}
	collectionPath := db.connector.db.tablePath("docs")
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := os.OpenFile(collectionPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if err := unix.Flock(int(owner.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer unix.Flock(int(owner.Fd()), unix.LOCK_UN)
	if core, err := openDatabase(path); !errors.Is(err, storeio.ErrWriterLocked) {
		if core != nil {
			_ = core.close()
		}
		t.Fatalf("default SQL open must remain immediate: %v", err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	policy := shardStoreOpenPolicy{openOptions: ReplicatedOpenOptions{
		WriterLockContext: ctx, WriterLockDeadline: time.Now().Add(time.Second),
	}}
	type result struct {
		core *database
		err  error
	}
	done := make(chan result, 1)
	go func() { core, err := openDatabaseWithShardStorePolicy(path, nil, policy); done <- result{core, err} }()
	select {
	case got := <-done:
		if got.core != nil {
			_ = got.core.close()
		}
		t.Fatalf("catalog opener did not wait at collection lock: %v", got.err)
	case <-time.After(15 * time.Millisecond):
	}
	cause := errors.New("startup interrupted while acquiring collection")
	cancel(cause)
	got := <-done
	if got.core != nil || !errors.Is(got.err, cause) {
		t.Fatalf("canceled SQL open: %+v", got)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("canceled open changed catalog: %v", err)
	}
	if err := unix.Flock(int(owner.Fd()), unix.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	// Expired contention budget must not become a two-second recovery deadline.
	policy.openOptions = ReplicatedOpenOptions{WriterLockContext: context.Background(), WriterLockDeadline: time.Now().Add(-time.Second)}
	core, err := openDatabaseWithShardStorePolicy(path, nil, policy)
	if err != nil {
		t.Fatalf("uncontended recovery after wait budget: %v", err)
	}
	if err := core.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	read, err := reopened.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	selectKept := runtimePrepare(t, read, `SELECT value FROM docs WHERE id = ?`)
	defer selectKept.Close()
	cursor, err := selectKept.Query(context.Background(), []any{"kept"})
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		_ = cursor.Close()
		t.Fatal("acknowledged row disappeared during canceled startup")
	}
	if value, ok := cursor.Cell(0).Int64(); !ok || value != 9 {
		_ = cursor.Close()
		t.Fatalf("recovered value=%d present=%v", value, ok)
	}
	if cursor.Next() {
		_ = cursor.Close()
		t.Fatal("duplicate recovered row")
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := testRuntimeExec(read, `INSERT INTO docs VALUES (?)`, []any{[]byte(`{"id":"after","value":10}`)}); err != nil {
		t.Fatalf("canceled startup poisoned subsequent recovery: %v", err)
	}
}
