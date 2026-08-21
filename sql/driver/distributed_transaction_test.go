package driver

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
)

func driverTransactionID(seed byte) distributedtxn.ID {
	var id distributedtxn.ID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func TestCommitDistributedParticipantPublishesDataAndStateAtomically(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "shard.vdb")
	binding := ShardStoreBinding{
		Distribution: distribution.DistributionName("tenant_data"),
		Shard:        distribution.ShardID("-80"), AllocationGeneration: 1,
	}
	db, err := InitializeShardStore(path, binding)
	if err != nil {
		t.Fatalf("InitializeShardStore: %v", err)
	}
	journal, err := db.OpenDistributedTransactionJournal()
	if err != nil {
		t.Fatalf("OpenDistributedTransactionJournal: %v", err)
	}
	_ = journal.Close()
	session, err := db.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	create, err := session.Prepare(ctx, `CREATE TABLE docs (id STRING PRIMARY KEY, n INTEGER NOT NULL)`)
	if err != nil {
		t.Fatalf("prepare CREATE: %v", err)
	}
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	_ = create.Close()
	insert, err := session.Prepare(ctx, `INSERT INTO docs (id, n) VALUES (?, ?)`)
	if err != nil {
		t.Fatalf("prepare INSERT: %v", err)
	}
	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	result, err := insert.Exec(ctx, []any{"a", int64(1)})
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	id := driverTransactionID(1)
	rows, err := session.CommitDistributedParticipant(ctx, id, 1, result.RowsAffected)
	if err != nil || rows != 1 {
		t.Fatalf("CommitDistributedParticipant = %d,%v, want 1,nil", rows, err)
	}
	_ = insert.Close()
	_ = session.Close()
	revision, rows, found, err := db.DistributedParticipantStatus(id)
	if err != nil || !found || revision != 2 || rows != 1 {
		t.Fatalf("status = revision %d rows %d found %v err %v", revision, rows, found, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err = OpenShardStore(path, binding)
	if err != nil {
		t.Fatalf("OpenShardStore: %v", err)
	}
	defer db.Close()
	revision, rows, found, err = db.DistributedParticipantStatus(id)
	if err != nil || !found || revision != 2 || rows != 1 {
		t.Fatalf("reopened status = revision %d rows %d found %v err %v", revision, rows, found, err)
	}
}
