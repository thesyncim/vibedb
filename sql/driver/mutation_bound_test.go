package driver

import (
	"context"
	stdsql "database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"strings"
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

func TestTransactionDeclaredColumnUpdateStopsAtProspectiveByteBound(t *testing.T) {
	connection := directTestConn(t).(*conn)
	directExec(t, connection,
		`CREATE TABLE docs (`+
			`id STRING PRIMARY KEY, state STRING NOT NULL, selected BOOL NOT NULL)`, nil)
	beforeA := []byte(`{"id":"a","state":"old","selected":true}`)
	beforeB := []byte(`{"id":"b","state":"old","selected":true}`)
	directExec(t, connection, `INSERT INTO docs VALUES (?), (?)`,
		[]sqldriver.NamedValue{
			{Ordinal: 1, Value: string(beforeA)},
			{Ordinal: 2, Value: string(beforeB)},
		})
	transaction, err := connection.beginTx(
		context.Background(), sqldriver.TxOptions{
			Isolation: sqldriver.IsolationLevel(stdsql.LevelRepeatableRead),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	connection.tx = transaction
	defer transaction.Rollback()
	state := transaction.tables["docs"]
	keyA, err := primaryScalarKey("a")
	if err != nil {
		t.Fatal(err)
	}
	afterA := []byte(`{"id":"a","selected":true,"state":"after"}`)
	state.limits.MaxBatchBytes = len(keyA) + len(afterA)
	update, err := query.PrepareDML(
		`UPDATE docs SET state = ? WHERE selected = TRUE`)
	if err != nil {
		t.Fatal(err)
	}
	defer update.Release()
	_, err = transaction.execMutation(update, []any{"after"})
	if !errors.Is(err, ErrTransactionTooLarge) ||
		!errors.Is(err, durable.ErrBatchTooLarge) ||
		!strings.Contains(err.Error(), "would stage") {
		t.Fatalf(
			"byte-bounded transaction column UPDATE = %v, want early ErrTransactionTooLarge and ErrBatchTooLarge",
			err,
		)
	}
	assertEmptyTransactionPending(t, state)
	for id, want := range map[string][]byte{
		"a": []byte(`{"id":"a","selected":true,"state":"old"}`),
		"b": []byte(`{"id":"b","selected":true,"state":"old"}`),
	} {
		key, err := primaryScalarKey(id)
		if err != nil {
			t.Fatal(err)
		}
		got, found, err := state.appendRaw(nil, key)
		if err != nil || !found || string(got) != string(want) {
			t.Fatalf("row %q after refused UPDATE = %s, found=%v, err=%v; want %s", id, got, found, err, want)
		}
	}
}
