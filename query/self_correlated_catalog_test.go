package query

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

const selfCorrelatedCatalogLeaf = `
	SELECT o.id
	FROM self_catalog_docs AS o
	WHERE EXISTS (
		SELECT 1
		FROM self_catalog_docs AS i
		WHERE i.parent = o.id AND i.active = TRUE
	)`

func TestSelfCorrelatedCatalogCapabilitiesPropagateThroughWrappers(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		joins int
	}{
		{"direct", selfCorrelatedCatalogLeaf, 1},
		{
			"derived",
			`SELECT d.id FROM (` + selfCorrelatedCatalogLeaf + `) AS d`,
			1,
		},
		{
			"cte",
			`WITH matched AS (` + selfCorrelatedCatalogLeaf + `) ` +
				`SELECT id FROM matched`,
			1,
		},
		{
			"window",
			`SELECT o.id, ROW_NUMBER() OVER (ORDER BY o.id) AS ordinal ` +
				`FROM self_catalog_docs AS o WHERE EXISTS (` +
				`SELECT 1 FROM self_catalog_docs AS i ` +
				`WHERE i.parent = o.id AND i.active = TRUE)`,
			1,
		},
		{
			"set",
			selfCorrelatedCatalogLeaf + ` UNION ALL ` + selfCorrelatedCatalogLeaf,
			2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement, err := PrepareStatement(test.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			if !statement.RequiresCatalog() ||
				!statement.UsesDirectCatalogExecution() ||
				statement.NumJoins() != test.joins {
				t.Fatalf("capabilities = catalog:%t direct:%t joins:%d, want true/true/%d",
					statement.RequiresCatalog(),
					statement.UsesDirectCatalogExecution(),
					statement.NumJoins(), test.joins)
			}
		})
	}
}

func TestSameTableUncorrelatedSubqueryKeepsSingleSourceFastPath(t *testing.T) {
	statement, err := PrepareStatement(`
		SELECT o.id
		FROM self_catalog_docs AS o
		WHERE EXISTS (
			SELECT 1 FROM self_catalog_docs AS i WHERE i.active = TRUE
		)`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.RequiresCatalog() || statement.UsesDirectCatalogExecution() ||
		statement.NumJoins() != 0 {
		t.Fatalf("uncorrelated capabilities = catalog:%t direct:%t joins:%d",
			statement.RequiresCatalog(), statement.UsesDirectCatalogExecution(),
			statement.NumJoins())
	}
}

func TestCatalogCapabilitiesKeepNestedLegacyFanOutOnHeapAdapter(t *testing.T) {
	statement, err := PrepareStatement(`
		WITH joined(doc) AS MATERIALIZED (
			SELECT s.*
			FROM memory_source AS s
			JOIN memory_gate AS g ON s.group_id = g.id
			WHERE g.enabled = TRUE
		), combined(doc) AS MATERIALIZED (
			SELECT doc FROM joined
			UNION ALL
			SELECT s.* FROM memory_source AS s WHERE id = 'absent'
		)
		SELECT d.doc FROM (SELECT doc FROM combined) AS d`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if !statement.RequiresCatalog() || statement.NumJoins() != 1 ||
		statement.UsesDirectCatalogExecution() {
		t.Fatalf("capabilities = catalog:%t direct:%t joins:%d, want true/false/1",
			statement.RequiresCatalog(), statement.UsesDirectCatalogExecution(),
			statement.NumJoins())
	}
}

func TestSelfCorrelatedEmptyDurableCatalogIsPresentAndEmpty(t *testing.T) {
	catalog, err := durable.SnapshotCollections([]durable.NamedCollection{
		{Name: "self_catalog_docs"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	statement, err := PrepareStatement(selfCorrelatedCatalogLeaf)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var exec Exec
	defer exec.Release()
	exec.Result.fileData = append(exec.Result.fileData, "stale"...)
	exec.Options.MemoryBytes = 1
	_, err = statement.RunInto(
		&exec, FromFileDatabase(catalog, "self_catalog_docs"), nil,
	)
	if err == nil || !strings.Contains(err.Error(), "MemoryBytes") {
		t.Fatalf("empty catalog 1-byte MemoryBytes = %v", err)
	}
	if len(exec.Result.fileData) != 0 {
		t.Fatalf("invalid empty-catalog run retained %d file bytes", len(exec.Result.fileData))
	}
	exec.Options = ExecOptions{Workers: 3}
	cursor, err := statement.RunInto(
		&exec, FromFileDatabase(catalog, "self_catalog_docs"), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Next() || exec.Result.RowCount != 0 {
		t.Fatalf("empty catalog published %d rows", exec.Result.RowCount)
	}
	if exec.Stats.Workers != 3 || exec.Stats.RowsTotal != 0 ||
		exec.Stats.RowsScanned != 0 {
		t.Fatalf("empty catalog stats = %+v, want workers=3 and no rows", exec.Stats)
	}
	count, err := PrepareStatement(`
		SELECT COUNT(*) FROM self_catalog_docs AS o
		WHERE EXISTS (
			SELECT 1 FROM self_catalog_docs AS i
			WHERE i.parent = o.id AND i.active = TRUE
		)`)
	if err != nil {
		t.Fatal(err)
	}
	defer count.Release()
	countCursor, err := count.RunInto(
		&exec, FromFileDatabase(catalog, "self_catalog_docs"), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !countCursor.Next() {
		t.Fatal("aggregate over empty catalog omitted its single row")
	}
	if value, ok := countCursor.Cell(0).Int64(); !ok || value != 0 {
		t.Fatalf("COUNT(*) over empty correlated catalog = %d/%t, want 0/true", value, ok)
	}
	if countCursor.Next() {
		t.Fatal("aggregate over empty catalog published more than one row")
	}
	_, err = statement.RunInto(
		&exec, FromFileDatabase(catalog, "absent"), nil,
	)
	if err == nil || !strings.Contains(err.Error(), `names collection "absent"`) {
		t.Fatalf("absent durable source = %v", err)
	}
}

func TestCorrelatedExistsCatalogedEmptyDurableInner(t *testing.T) {
	database, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	outer, err := database.CreateCollection("deferred_outer", durableJoinOptions())
	if err != nil {
		t.Fatal(err)
	}
	outerDocs := []string{
		`{"id":"scalar","k":"x"}`,
		`{"id":"null","k":null}`,
		`{"id":"missing"}`,
	}
	for i, document := range outerDocs {
		if _, err := outer.Put(
			[]byte(fmt.Sprintf("outer-%d", i)), []byte(document),
		); err != nil {
			t.Fatal(err)
		}
	}
	catalog, err := durable.SnapshotCollections([]durable.NamedCollection{
		{Name: "deferred_outer", Collection: outer},
		{Name: "deferred_inner"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()

	const leaf = `EXISTS (SELECT 1 FROM deferred_inner AS i WHERE i.k = o.k)`
	for _, test := range []struct {
		name string
		pred string
		want []string
	}{
		{name: "exists", pred: leaf},
		{name: "not_exists", pred: "NOT " + leaf, want: []string{"missing", "null", "scalar"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			statement, err := PrepareStatement(
				`SELECT o.id FROM deferred_outer AS o WHERE ` + test.pred +
					` ORDER BY o.id`,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			var exec Exec
			defer exec.Release()
			got := correlatedStatementIDs(
				t, statement, FromFileDatabase(catalog, "deferred_outer"), &exec,
			)
			if !slices.Equal(got, test.want) {
				t.Fatalf("rows = %v, want %v", got, test.want)
			}
			if exec.Stats.JoinMemberships != 1 || exec.Stats.JoinKeys != 0 ||
				exec.Stats.JoinLookups != 0 || exec.Stats.JoinProbes != 0 {
				t.Fatalf("empty inner adaptive stats = %+v", exec.Stats)
			}
		})
	}

	missingCatalog, err := durable.SnapshotCollections([]durable.NamedCollection{
		{Name: "deferred_outer", Collection: outer},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer missingCatalog.Close()
	statement, err := PrepareStatement(
		`SELECT o.id FROM deferred_outer AS o WHERE ` + leaf,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var exec Exec
	defer exec.Release()
	_, err = statement.RunInto(
		&exec, FromFileDatabase(missingCatalog, "deferred_outer"), nil,
	)
	if err == nil || !strings.Contains(err.Error(), `collection "deferred_inner"`) {
		t.Fatalf("absent durable inner = %v", err)
	}
}
