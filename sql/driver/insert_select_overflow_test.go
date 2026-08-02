package driver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

const insertSelectOverflowPadBytes = 4 << 10

type insertSelectOverflowRow struct {
	id  string
	tag string
	pad string
}

func TestInsertSelectOverflowAutocommitReturningExactIndexAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "insert-select-overflow-auto.vdb")
	want := []insertSelectOverflowRow{
		insertSelectOverflowTestRow("auto-a", "hot", 'A'),
		insertSelectOverflowTestRow("auto-b", "hot", 'B'),
	}
	database, session := openInsertSelectOverflowRuntime(t, path)
	createInsertSelectOverflowTables(t, session, "overflow_auto_source", "overflow_auto_target")
	execInsertSelectOverflowSQL(t, session,
		`CREATE INDEX overflow_auto_tag_exact ON overflow_auto_target (tag)`, nil)
	insertInsertSelectOverflowSourceRows(t, session, "overflow_auto_source", want)

	insert := prepareInsertSelectOverflowSQL(t, session,
		`INSERT INTO overflow_auto_target `+
			`SELECT * FROM overflow_auto_source ORDER BY id `+
			`RETURNING id, tag, pad`)
	cursor, err := insert.Query(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireInsertSelectOverflowRows(t, cursor, want)
	if err := insert.Close(); err != nil {
		t.Fatal(err)
	}
	requireInsertSelectOverflowExactRows(
		t, session, "overflow_auto_target", "hot", want, true,
	)

	closeInsertSelectOverflowRuntime(t, database, session)
	database, session = reopenInsertSelectOverflowRuntime(t, path)
	defer closeInsertSelectOverflowRuntime(t, database, session)
	requireInsertSelectOverflowExactRows(
		t, session, "overflow_auto_target", "hot", want, true,
	)
}

func TestInsertSelectOverflowTransactionReturningReplaceDeleteAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "insert-select-overflow-tx.vdb")
	inserted := []insertSelectOverflowRow{
		insertSelectOverflowTestRow("tx-a", "old", 'A'),
		insertSelectOverflowTestRow("tx-b", "old", 'B'),
	}
	database, session := openInsertSelectOverflowRuntime(t, path)
	createInsertSelectOverflowTables(t, session, "overflow_tx_source", "overflow_tx_target")
	execInsertSelectOverflowSQL(t, session,
		`CREATE INDEX overflow_tx_tag_exact ON overflow_tx_target (tag)`, nil)
	insertInsertSelectOverflowSourceRows(t, session, "overflow_tx_source", inserted)

	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	insert := prepareInsertSelectOverflowSQL(t, session,
		`INSERT INTO overflow_tx_target `+
			`SELECT * FROM overflow_tx_source ORDER BY id `+
			`RETURNING id, tag, pad`)
	cursor, err := insert.Query(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireInsertSelectOverflowRows(t, cursor, inserted)
	if err := insert.Close(); err != nil {
		t.Fatal(err)
	}
	if err := session.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	requireInsertSelectOverflowExactRows(
		t, session, "overflow_tx_target", "old", inserted, true,
	)

	replacement := insertSelectOverflowTestRow("tx-a", "new", 'R')
	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	update := prepareInsertSelectOverflowSQL(t, session,
		`UPDATE overflow_tx_target SET "$doc" = ? WHERE id = 'tx-a' `+
			`RETURNING id, tag, pad`)
	cursor, err = update.Query(ctx, []any{insertSelectOverflowDocument(replacement)})
	if err != nil {
		t.Fatal(err)
	}
	requireInsertSelectOverflowRows(t, cursor, []insertSelectOverflowRow{replacement})
	if err := update.Close(); err != nil {
		t.Fatal(err)
	}
	deleteStatement := prepareInsertSelectOverflowSQL(t, session,
		`DELETE FROM overflow_tx_target WHERE id = 'tx-b' RETURNING id, tag, pad`)
	cursor, err = deleteStatement.Query(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireInsertSelectOverflowRows(t, cursor, inserted[1:])
	if err := deleteStatement.Close(); err != nil {
		t.Fatal(err)
	}
	if err := session.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	requireInsertSelectOverflowExactRows(
		t, session, "overflow_tx_target", "old", nil, true,
	)
	requireInsertSelectOverflowExactRows(
		t, session, "overflow_tx_target", "new",
		[]insertSelectOverflowRow{replacement}, true,
	)

	closeInsertSelectOverflowRuntime(t, database, session)
	database, session = reopenInsertSelectOverflowRuntime(t, path)
	defer closeInsertSelectOverflowRuntime(t, database, session)
	requireInsertSelectOverflowExactRows(
		t, session, "overflow_tx_target", "old", nil, true,
	)
	requireInsertSelectOverflowExactRows(
		t, session, "overflow_tx_target", "new",
		[]insertSelectOverflowRow{replacement}, true,
	)
}

func TestInsertSelectOverflowTargetSourceUsesStatementSnapshot(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "insert-select-overflow-self.vdb")
	database, session := openInsertSelectOverflowRuntime(t, path)
	defer closeInsertSelectOverflowRuntime(t, database, session)
	execInsertSelectOverflowSQL(t, session,
		`CREATE TABLE overflow_self (`+
			`id STRING PRIMARY KEY, tag STRING, pad STRING, payload JSON)`, nil)

	want := []insertSelectOverflowRow{
		insertSelectOverflowTestRow("copy-a", "copied", 'A'),
		insertSelectOverflowTestRow("copy-b", "copied", 'B'),
	}
	insertOuter := prepareInsertSelectOverflowSQL(t, session,
		`INSERT INTO overflow_self VALUES (?)`)
	for i, row := range want {
		outer := fmt.Sprintf(`{"id":%q,"payload":%s}`,
			fmt.Sprintf("seed-%d", i), insertSelectOverflowDocument(row))
		requireInsertSelectOverflowDocumentBounds(t, []byte(outer))
		result, err := insertOuter.Exec(ctx, []any{outer})
		if err != nil || result.RowsAffected != 1 {
			t.Fatalf("seed self row %d = %+v, %v", i, result, err)
		}
	}
	if err := insertOuter.Close(); err != nil {
		t.Fatal(err)
	}

	copyStatement := prepareInsertSelectOverflowSQL(t, session,
		`INSERT INTO overflow_self SELECT payload FROM overflow_self ORDER BY id `+
			`RETURNING id, tag, pad`)
	cursor, err := copyStatement.Query(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireInsertSelectOverflowRows(t, cursor, want)
	if err := copyStatement.Close(); err != nil {
		t.Fatal(err)
	}

	count := prepareInsertSelectOverflowSQL(t, session,
		`SELECT count(*) FROM overflow_self`)
	cursor, err = count.Query(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("self-source count returned no row")
	}
	got, ok := cursor.Cell(0).Int64()
	if !ok || got != 4 || cursor.Next() {
		t.Fatalf("self-source count = %d/%t, want exactly 4", got, ok)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := count.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInsertSelectOverflowFailuresPublishNothing(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "insert-select-overflow-failures.vdb")
	database, session := openInsertSelectOverflowRuntime(t, path)
	defer closeInsertSelectOverflowRuntime(t, database, session)
	for _, statement := range []string{
		`CREATE TABLE overflow_failure_source (id STRING PRIMARY KEY)`,
		`CREATE TABLE overflow_schema_target (` +
			`id STRING PRIMARY KEY, required STRING NOT NULL, pad STRING)`,
		`CREATE TABLE overflow_cancel_target (` +
			`id STRING PRIMARY KEY, required STRING, pad STRING)`,
		`CREATE TABLE overflow_budget_target (` +
			`id STRING PRIMARY KEY, required STRING, pad STRING)`,
	} {
		execInsertSelectOverflowSQL(t, session, statement, nil)
	}
	good := insertSelectOverflowRawDocument("a-good", true, 'G')
	bad := insertSelectOverflowRawDocument("z-bad", false, 'B')
	requireInsertSelectOverflowDocumentBounds(t, []byte(good))
	requireInsertSelectOverflowDocumentBounds(t, []byte(bad))
	insert := prepareInsertSelectOverflowSQL(t, session,
		`INSERT INTO overflow_failure_source VALUES (?)`)
	for _, document := range []string{good, bad} {
		result, err := insert.Exec(ctx, []any{document})
		if err != nil || result.RowsAffected != 1 {
			t.Fatalf("seed failure source = %+v, %v", result, err)
		}
	}
	if err := insert.Close(); err != nil {
		t.Fatal(err)
	}

	schemaFailure := prepareInsertSelectOverflowSQL(t, session,
		`INSERT INTO overflow_schema_target `+
			`SELECT * FROM overflow_failure_source ORDER BY id`)
	if _, err := schemaFailure.Exec(ctx, nil); !errors.Is(err, store.ErrSchemaViolation) {
		t.Fatalf("schema failure = %T %v, want ErrSchemaViolation", err, err)
	}
	if err := schemaFailure.Close(); err != nil {
		t.Fatal(err)
	}
	requireInsertSelectOverflowCount(t, session, "overflow_schema_target", 0)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	cancelStatement := prepareInsertSelectOverflowSQL(t, session,
		`INSERT INTO overflow_cancel_target `+
			`SELECT * FROM overflow_failure_source ORDER BY id`)
	if _, err := cancelStatement.Exec(canceled, nil); !errors.Is(err, context.Canceled) &&
		!errors.Is(err, query.ErrCanceled) {
		t.Fatalf("canceled overflow insert = %T %v, want cancellation", err, err)
	}
	if err := cancelStatement.Close(); err != nil {
		t.Fatal(err)
	}
	requireInsertSelectOverflowCount(t, session, "overflow_cancel_target", 0)

	if err := session.SetIntermediateLimit(1); err != nil {
		t.Fatal(err)
	}
	budgetStatement := prepareInsertSelectOverflowSQL(t, session,
		`INSERT INTO overflow_budget_target `+
			`SELECT * FROM overflow_failure_source ORDER BY id`)
	if _, err := budgetStatement.Exec(ctx, nil); !errors.Is(err, query.ErrIntermediateBudget) {
		t.Fatalf("budget failure = %T %v, want ErrIntermediateBudget", err, err)
	}
	if err := budgetStatement.Close(); err != nil {
		t.Fatal(err)
	}
	if err := session.SetIntermediateLimit(-1); err != nil {
		t.Fatal(err)
	}
	requireInsertSelectOverflowCount(t, session, "overflow_budget_target", 0)
}

func insertSelectOverflowTestRow(id, tag string, fill byte) insertSelectOverflowRow {
	return insertSelectOverflowRow{
		id: id, tag: tag,
		pad: strings.Repeat(string(fill), insertSelectOverflowPadBytes),
	}
}

func insertSelectOverflowDocument(row insertSelectOverflowRow) string {
	return fmt.Sprintf(`{"id":%q,"tag":%q,"pad":%q}`, row.id, row.tag, row.pad)
}

func insertSelectOverflowRawDocument(id string, required bool, fill byte) string {
	if required {
		return fmt.Sprintf(`{"id":%q,"required":"yes","pad":%q}`,
			id, strings.Repeat(string(fill), insertSelectOverflowPadBytes))
	}
	return fmt.Sprintf(`{"id":%q,"pad":%q}`,
		id, strings.Repeat(string(fill), insertSelectOverflowPadBytes))
}

func requireInsertSelectOverflowDocumentBounds(t testing.TB, document []byte) {
	t.Helper()
	options, err := durable.NormalizeOptions(durable.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(document) <= options.InlineValueBytes {
		t.Fatalf("fixture bytes = %d, want > InlineValueBytes %d",
			len(document), options.InlineValueBytes)
	}
	if len(document) > options.MaxDocumentBytes {
		t.Fatalf("fixture bytes = %d, want <= MaxDocumentBytes %d",
			len(document), options.MaxDocumentBytes)
	}
}

func openInsertSelectOverflowRuntime(t testing.TB, path string) (*Database, *Session) {
	t.Helper()
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	return database, session
}

func reopenInsertSelectOverflowRuntime(t testing.TB, path string) (*Database, *Session) {
	t.Helper()
	return openInsertSelectOverflowRuntime(t, path)
}

func closeInsertSelectOverflowRuntime(t testing.TB, database *Database, session *Session) {
	t.Helper()
	if session != nil {
		if err := session.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if database != nil {
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func prepareInsertSelectOverflowSQL(t testing.TB, session *Session, statement string) *Prepared {
	t.Helper()
	prepared, err := session.Prepare(context.Background(), statement)
	if err != nil {
		t.Fatalf("prepare %q: %v", statement, err)
	}
	return prepared
}

func execInsertSelectOverflowSQL(
	t testing.TB,
	session *Session,
	statement string,
	args []any,
) {
	t.Helper()
	prepared := prepareInsertSelectOverflowSQL(t, session, statement)
	result, err := prepared.Exec(context.Background(), args)
	if closeErr := prepared.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("exec %q: %v", statement, err)
	}
	_ = result
}

func createInsertSelectOverflowTables(
	t testing.TB,
	session *Session,
	source string,
	target string,
) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE ` + source + ` (` +
			`id STRING PRIMARY KEY, tag STRING NOT NULL, pad STRING NOT NULL)`,
		`CREATE TABLE ` + target + ` (` +
			`id STRING PRIMARY KEY, tag STRING NOT NULL, pad STRING NOT NULL)`,
	} {
		execInsertSelectOverflowSQL(t, session, statement, nil)
	}
}

func insertInsertSelectOverflowSourceRows(
	t testing.TB,
	session *Session,
	table string,
	rows []insertSelectOverflowRow,
) {
	t.Helper()
	prepared := prepareInsertSelectOverflowSQL(t, session,
		`INSERT INTO `+table+` VALUES (?)`)
	for _, row := range rows {
		document := insertSelectOverflowDocument(row)
		requireInsertSelectOverflowDocumentBounds(t, []byte(document))
		result, err := prepared.Exec(context.Background(), []any{document})
		if err != nil || result.RowsAffected != 1 {
			t.Fatalf("insert source %q = %+v, %v", row.id, result, err)
		}
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
}

func requireInsertSelectOverflowRows(
	t testing.TB,
	cursor *Cursor,
	want []insertSelectOverflowRow,
) {
	t.Helper()
	got := make([]insertSelectOverflowRow, 0, len(want))
	for cursor.Next() {
		id, idOK := cursor.Cell(0).Text()
		tag, tagOK := cursor.Cell(1).Text()
		pad, padOK := cursor.Cell(2).Text()
		if !idOK || !tagOK || !padOK {
			t.Fatalf("RETURNING row has non-string cells: %s, %s, %s",
				cursor.Cell(0).String(), cursor.Cell(1).String(), cursor.Cell(2).String())
		}
		got = append(got, insertSelectOverflowRow{id: id, tag: tag, pad: pad})
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(got, func(a, b insertSelectOverflowRow) int {
		return strings.Compare(a.id, b.id)
	})
	want = slices.Clone(want)
	slices.SortFunc(want, func(a, b insertSelectOverflowRow) int {
		return strings.Compare(a.id, b.id)
	})
	if len(got) != len(want) {
		t.Fatalf("rows = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].id != want[i].id || got[i].tag != want[i].tag ||
			!bytes.Equal([]byte(got[i].pad), []byte(want[i].pad)) {
			t.Fatalf("row %d = (%q,%q,%d bytes), want (%q,%q,%d bytes)",
				i, got[i].id, got[i].tag, len(got[i].pad),
				want[i].id, want[i].tag, len(want[i].pad))
		}
	}
}

func requireInsertSelectOverflowExactRows(
	t testing.TB,
	session *Session,
	table string,
	tag string,
	want []insertSelectOverflowRow,
	requireIndex bool,
) {
	t.Helper()
	prepared := prepareInsertSelectOverflowSQL(t, session,
		`SELECT id, tag, pad FROM `+table+` WHERE tag = ? ORDER BY id`)
	cursor, err := prepared.Query(context.Background(), []any{tag})
	if err != nil {
		t.Fatal(err)
	}
	requireInsertSelectOverflowRows(t, cursor, want)
	if requireIndex {
		stats := session.conn.exec.Stats
		if !stats.IndexBounded || stats.IndexLookups == 0 {
			t.Fatalf("exact query did not use durable index: %+v", stats)
		}
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
}

func requireInsertSelectOverflowCount(
	t testing.TB,
	session *Session,
	table string,
	want int64,
) {
	t.Helper()
	prepared := prepareInsertSelectOverflowSQL(t, session,
		`SELECT count(*) FROM `+table)
	cursor, err := prepared.Query(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("count returned no row")
	}
	got, ok := cursor.Cell(0).Int64()
	if !ok || got != want || cursor.Next() {
		t.Fatalf("%s count = %d/%t, want %d", table, got, ok, want)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
}
