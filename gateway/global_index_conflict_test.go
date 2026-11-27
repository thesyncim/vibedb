package gateway

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/shardservice"
)

func TestInsertOnConflictWithGlobalIndexBranchAwareMaintenance(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	snapshot, err := NewSnapshotWithIndexes(
		config, endpoints, 1, []IndexDescriptor{testGlobalIndexDescriptor()},
	)
	if err != nil {
		t.Fatal(err)
	}

	// 1. ON CONFLICT DO NOTHING prepares cleanly with writeInsertConflict=true
	// and defers global index mutations until captureIndexedMutation.
	doNothing, err := snapshot.Prepare(context.Background(),
		`INSERT INTO messages (tenant_id, id, email) VALUES (?, ?, ?) ON CONFLICT (id) DO NOTHING`)
	if err != nil || !doNothing.writeInsertConflict || len(doNothing.writeGlobalIndexes) != 1 {
		t.Fatalf("prepare ON CONFLICT DO NOTHING = %v, conflict=%v programs=%d",
			err, doNothing != nil && doNothing.writeInsertConflict, len(doNothing.writeGlobalIndexes))
	}
	boundNothing, err := doNothing.BindWrite([]any{
		[]byte("tenant-7"), []byte("message-9"), []byte("new@example.com"),
	})
	if err != nil || len(boundNothing.globalIndexes) != 0 {
		t.Fatalf("BindWrite(DO NOTHING) before capture = %v, globalIndexes=%d, want 0", err, len(boundNothing.globalIndexes))
	}

	program := doNothing.writeGlobalIndexes[0].program
	oldDoc := []byte(`{"tenant_id":"tenant-7","id":"message-9","email":"old@example.com"}`)
	var ws GlobalIndexWorkspace
	oldRoute, err := program.RouteDocument(oldDoc, &ws)
	if err != nil {
		t.Fatal(err)
	}

	// Case A: Conflict hit (DO NOTHING) -> before == after == oldDoc -> ZERO global index mutations!
	if err := doNothing.bindGlobalIndexCapture(boundNothing, oldRoute.BaseTarget, [][]shardservice.Cell{{
		{Bytes: oldRoute.BasePrimaryKey},
		{Bytes: oldDoc},
		{Bytes: oldDoc},
	}}); err != nil {
		t.Fatalf("bindGlobalIndexCapture(DO NOTHING conflict): %v", err)
	}
	if len(boundNothing.globalIndexes) != 0 {
		t.Fatalf("DO NOTHING conflict emitted %d global index mutations, want 0 (no ghost entries)", len(boundNothing.globalIndexes))
	}
	if len(boundNothing.expectedDigests) != 1 || boundNothing.expectedDigests[0] != sha256.Sum256(oldDoc) {
		t.Fatalf("DO NOTHING conflict missing before-image precondition digest")
	}

	// Case B: Absent row (insert branch) -> before == NULL, after == newDoc -> zero-digest absent precondition + Put!
	newDoc := []byte(`{"tenant_id":"tenant-7","id":"message-10","email":"fresh@example.com"}`)
	var wsNew GlobalIndexWorkspace
	newRoute, err := program.RouteDocument(newDoc, &wsNew)
	if err != nil {
		t.Fatal(err)
	}
	boundFresh, err := doNothing.BindWrite([]any{
		[]byte("tenant-7"), []byte("message-10"), []byte("fresh@example.com"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := doNothing.bindGlobalIndexCapture(boundFresh, newRoute.BaseTarget, [][]shardservice.Cell{{
		{Bytes: newRoute.BasePrimaryKey},
		{Null: true},
		{Bytes: newDoc},
	}}); err != nil {
		t.Fatalf("bindGlobalIndexCapture(DO NOTHING insert): %v", err)
	}
	if len(boundFresh.globalIndexes) != 1 || boundFresh.globalIndexes[0].kind != shardservice.MutationGlobalIndexPut {
		t.Fatalf("DO NOTHING insert globalIndexes = %+v, want 1 Put", boundFresh.globalIndexes)
	}
	if len(boundFresh.expectedDigests) != 1 || boundFresh.expectedDigests[0] != ([sha256.Size]byte{}) {
		t.Fatalf("DO NOTHING insert expected absent precondition digest [0..0], got %x", boundFresh.expectedDigests)
	}
	if len(boundFresh.postimageDigests) != 1 || boundFresh.postimageDigests[0] != sha256.Sum256(newDoc) {
		t.Fatalf("DO NOTHING insert postimage digest mismatch")
	}

	// 2. ON CONFLICT DO UPDATE changing indexed email -> Delete(old) + Put(new)!
	doUpdate, err := snapshot.Prepare(context.Background(),
		`INSERT INTO messages (tenant_id, id, email) VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email`)
	if err != nil || !doUpdate.writeInsertConflict {
		t.Fatalf("prepare ON CONFLICT DO UPDATE = %v", err)
	}
	boundUpd, err := doUpdate.BindWrite([]any{
		[]byte("tenant-7"), []byte("message-9"), []byte("updated@example.com"),
	})
	if err != nil {
		t.Fatal(err)
	}
	updatedDoc := []byte(`{"tenant_id":"tenant-7","id":"message-9","email":"updated@example.com"}`)
	if err := doUpdate.bindGlobalIndexCapture(boundUpd, oldRoute.BaseTarget, [][]shardservice.Cell{{
		{Bytes: oldRoute.BasePrimaryKey},
		{Bytes: oldDoc},
		{Bytes: updatedDoc},
	}}); err != nil {
		t.Fatalf("bindGlobalIndexCapture(DO UPDATE): %v", err)
	}
	if len(boundUpd.globalIndexes) != 2 ||
		boundUpd.globalIndexes[0].kind != shardservice.MutationGlobalIndexDelete ||
		boundUpd.globalIndexes[1].kind != shardservice.MutationGlobalIndexPut {
		t.Fatalf("DO UPDATE globalIndexes = %+v, want Delete+Put", boundUpd.globalIndexes)
	}
}
