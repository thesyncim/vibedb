package driver

import (
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestAutocommitFilteredDeleteStopsAtBatchBound(t *testing.T) {
	connection := directTestConn(t).(*conn)
	directExec(t, connection,
		`CREATE TABLE docs (id STRING PRIMARY KEY, selected BOOL NOT NULL)`, nil)
	table := connection.db.tables["docs"]
	limits, err := tableMutationLimits(table)
	if err != nil {
		t.Fatal(err)
	}
	count := limits.MaxBatchDocuments + 1
	insert, err := connection.Prepare(`INSERT INTO docs VALUES (?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer insert.Close()
	for i := 0; i < count; i++ {
		document := fmt.Sprintf(`{"id":"%04d","selected":true}`, i)
		if _, err := insert.Exec([]sqldriver.Value{document}); err != nil {
			t.Fatal(err)
		}
	}

	remove, err := connection.Prepare(
		`DELETE FROM docs WHERE selected = TRUE`)
	if err != nil {
		t.Fatal(err)
	}
	defer remove.Close()
	if _, err := remove.Exec(nil); !errors.Is(err, durable.ErrBatchTooLarge) {
		t.Fatalf("oversized filtered DELETE = %v, want ErrBatchTooLarge", err)
	}

	snapshot, err := table.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	remaining := 0
	if err := snapshot.RangeRaw(func(_, _ []byte) error {
		remaining++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if remaining != count {
		t.Fatalf("failed DELETE left %d documents, want %d", remaining, count)
	}
}

func TestTransactionFilteredDeleteStopsAtCapturedBounds(t *testing.T) {
	documents := make([][]byte, 5)
	for i := range documents {
		documents[i] = []byte(fmt.Sprintf(
			`{"id":"%04d","selected":true}`, i))
	}
	_, transaction, _ := beginRawDocsTransaction(t, documents...)
	state := transaction.tables["docs"]
	state.limits.MaxBatchDocuments = len(documents) - 1

	remove, err := query.PrepareDML(
		`DELETE FROM docs WHERE selected = TRUE`)
	if err != nil {
		t.Fatal(err)
	}
	defer remove.Release()
	if _, err := transaction.execMutation(remove, nil); !errors.Is(err, ErrTransactionTooLarge) ||
		!errors.Is(err, durable.ErrBatchTooLarge) {
		t.Fatalf(
			"oversized transaction DELETE = %v, want ErrTransactionTooLarge and ErrBatchTooLarge",
			err,
		)
	}
	assertEmptyTransactionPending(t, state)
}

func TestTransactionFilteredDeleteStopsAtByteBound(t *testing.T) {
	documents := [][]byte{
		[]byte(`{"id":"first","selected":true}`),
		[]byte(`{"id":"second","selected":true}`),
	}
	_, transaction, _ := beginRawDocsTransaction(t, documents...)
	state := transaction.tables["docs"]
	firstKey, err := primaryScalarKey("first")
	if err != nil {
		t.Fatal(err)
	}
	state.limits.MaxBatchBytes = len(firstKey)

	remove, err := query.PrepareDML(
		`DELETE FROM docs WHERE selected = TRUE`)
	if err != nil {
		t.Fatal(err)
	}
	defer remove.Release()
	if _, err := transaction.execMutation(remove, nil); !errors.Is(err, ErrTransactionTooLarge) ||
		!errors.Is(err, durable.ErrBatchTooLarge) {
		t.Fatalf(
			"byte-bounded transaction DELETE = %v, want ErrTransactionTooLarge and ErrBatchTooLarge",
			err,
		)
	}
	assertEmptyTransactionPending(t, state)
}
