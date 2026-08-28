package pgwire

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestGoLandDiscoveryCapturedQueries(t *testing.T) {
	c := connectIntrospectionCatalog(t)
	for _, shape := range discoveryShapes {
		if shape.Name == "RetrieveDateStyle" {
			continue
		}
		t.Run(shape.Name, func(t *testing.T) {
			q := strings.ReplaceAll(shape.SQL, ":schema_id", "2200")
			q = strings.ReplaceAll(q, ":[*schema_ids]", "2200")
			q = strings.ReplaceAll(q, ":[*f_names]", "'users', 'orders'")
			q = strings.ReplaceAll(q, ":state", "NULL")
			q = strings.NewReplacer(":schema_name", "'public'", ":table_name", "'users'", ":table_pattern", "'%'", ":column_pattern", "'%'").Replace(q)
			msgs := c.query(q)
			if has(msgs, msgErrorResponse) {
				t.Fatalf("%s: %s", q, formatError(find(t, msgs, msgErrorResponse).body))
			}
			cols := decodeRowDescription(t, find(t, msgs, msgRowDescription).body)
			if len(cols) != len(shape.Columns) {
				t.Fatalf("columns=%d want %d", len(cols), len(shape.Columns))
			}
			if strings.Contains(shape.SQL, ":[*f_names]") {
				empty := strings.ReplaceAll(q, "'users', 'orders'", "NULL")
				msgs := c.query(empty)
				if has(msgs, msgErrorResponse) || len(rowsOf(t, msgs)) != 0 {
					t.Fatalf("empty fragment %s: %s", shape.Name, tags(msgs))
				}
			}
		})
	}
}

func discoveryTestShape(t *testing.T, name string) *discoveryShape {
	t.Helper()
	for i := range discoveryShapes {
		if discoveryShapes[i].Name == name {
			return &discoveryShapes[i]
		}
	}
	t.Fatalf("missing shape %q", name)
	return nil
}

func TestDiscoveryCatalogValuesAndFilters(t *testing.T) {
	c := connectIntrospectionCatalog(t)
	shape := discoveryTestShape(t, "RetrieveColumns")
	q := strings.ReplaceAll(shape.SQL, ":schema_id", "2200")
	msgs := c.query(q)
	if has(msgs, msgErrorResponse) {
		t.Fatal(formatError(find(t, msgs, msgErrorResponse).body))
	}
	cols := decodeRowDescription(t, find(t, msgs, msgRowDescription).body)
	index := map[string]int{}
	for i, c := range cols {
		index[c.name] = i
	}
	rows := rowsOf(t, msgs)
	if len(rows) != 9 {
		t.Fatalf("columns=%d want 9", len(rows))
	}
	mandatory, optional, docs := 0, 0, 0
	for _, r := range rows {
		if string(r[index["type_id"]]) != "114" || string(r[index["type_spec"]]) != "json" {
			t.Fatalf("untruthful projection type: %q", r)
		}
		switch string(r[index["column_name"]]) {
		case "$doc":
			docs++
		case "tier", "n":
			if string(r[index["mandatory"]]) != "f" {
				t.Fatalf("nullable column: %q", r)
			}
			optional++
		case "id":
			if string(r[index["mandatory"]]) != "t" {
				t.Fatalf("primary key: %q", r)
			}
			mandatory++
		}
	}
	if mandatory != 2 || optional != 2 || docs != 2 {
		t.Fatalf("key/nullable/document counts %d/%d/%d", mandatory, optional, docs)
	}
	if got := rowsOf(t, c.query(strings.ReplaceAll(shape.SQL, ":schema_id", "9999"))); len(got) != 0 {
		t.Fatalf("unknown schema: %q", got)
	}

	for _, test := range []struct {
		f    discoveryFilter
		name string
		want bool
	}{
		{discoveryFilter{hasSchemaName: true, schemaName: ""}, "users", false},
		{discoveryFilter{hasSchemaName: true, schemaName: "private"}, "users", false},
		{discoveryFilter{hasSchemaName: true, schemaName: "pub%"}, "users", true},
		{discoveryFilter{hasTableName: true, tableName: ""}, "users", false},
		{discoveryFilter{hasTablePattern: true, tablePattern: "u_er%"}, "users", true},
		{discoveryFilter{hasTablePattern: true, tablePattern: `u\_er%`}, "users", false},
		{discoveryFilter{names: []string{"orders"}}, "users", false},
	} {
		if got := test.f.includes(test.name); got != test.want {
			t.Errorf("filter %+v = %v", test.f, got)
		}
	}
}

func TestDiscoveryPreparedSnapshotAndRebind(t *testing.T) {
	c := connectIntrospectionCatalog(t)
	q := strings.ReplaceAll(discoveryTestShape(t, "RetrieveTables").SQL, ":schema_id", "$1")
	c.send(msgParse, parseMsg("metadata", q))
	c.send(msgBind, bindMsg("snapshot", "metadata", nil, [][]byte{[]byte("2200")}, nil))
	c.send(msgExecute, executeMsg("snapshot", 1))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	if !has(msgs, msgPortalSuspended) || len(rowsOf(t, msgs)) != 1 {
		t.Fatalf("first page: %s", tags(msgs))
	}
	if msgs := c.query(`CREATE TABLE later (id STRING PRIMARY KEY)`); has(msgs, msgErrorResponse) {
		t.Fatal(formatError(find(t, msgs, msgErrorResponse).body))
	}
	c.send(msgExecute, executeMsg("snapshot", 0))
	c.send(msgSync, nil)
	msgs = c.until(msgReadyForQuery)
	if len(rowsOf(t, msgs)) != 1 {
		t.Fatalf("snapshot changed: %s", tags(msgs))
	}
	c.send(msgBind, bindMsg("fresh", "metadata", nil, [][]byte{[]byte("2200")}, nil))
	c.send(msgExecute, executeMsg("fresh", 0))
	c.send(msgSync, nil)
	msgs = c.until(msgReadyForQuery)
	if len(rowsOf(t, msgs)) != 3 {
		t.Fatalf("rebind did not refresh: %s", tags(msgs))
	}
}

func TestDiscoveryResultAndIdentityBounds(t *testing.T) {
	s := session{server: &Server{opts: Options{MaxResultRows: 1, MaxResultBytes: 4096}}}
	a := catalogAnswer{tables: []sqldriver.TableInfo{{Name: "a", PrimaryKey: "/id"}, {Name: "b", PrimaryKey: "/id"}}, oidMap: map[string]uint32{"a": 16384, "b": 16385}}
	for _, name := range []string{"RetrieveTables", "JDBC all tables"} {
		shape := discoveryTestShape(t, name)
		var err error
		if strings.HasPrefix(name, "JDBC") {
			_, err = s.answerJDBCDiscovery(shape, discoveryFilter{}, &a, nil)
		} else {
			_, err = s.answerDiscovery(shape, discoveryFilter{}, &a)
		}
		if err == nil || asPGError(err).code != sqlstateProgramLimitExceeded {
			t.Fatalf("%s result budget: %v", name, err)
		}
	}
	server := Server{}
	id := server.discoveryOID("stable")
	if id == 0 || server.discoveryOID("stable") != id {
		t.Fatal("unstable identity")
	}
	server.catalogNameBytes = 4 << 20
	if server.discoveryOID("another") != 0 || server.discoveryOID("stable") != id {
		t.Fatal("identity byte budget")
	}
	col := sqldriver.ColumnInfo{Path: "/nullable", Required: true, Types: sqlast.TypeString | sqlast.TypeNull}
	a.tables = []sqldriver.TableInfo{{Name: "nullable", PrimaryKey: "/id", Columns: []sqldriver.ColumnInfo{col}}}
	s.server.opts.MaxResultRows = 10
	r, err := s.answerJDBCDiscovery(discoveryTestShape(t, "JDBC columns documents"), discoveryFilter{}, &a, nil)
	if err != nil || *r.rows[0][4] != "f" {
		t.Fatalf("required nullable column: %v %+v", err, r)
	}
}

func TestPublicNamespaceReadWriteAndTransaction(t *testing.T) {
	c := connectSQLCatalog(t)
	for _, q := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`,
		`SET search_path TO public`,
		`BEGIN`,
		`INSERT INTO public.docs (id,value) VALUES ('a','before')`,
		`UPDATE "public".docs SET "$doc" = '{"id":"a","value":"after"}' WHERE id = 'a'`,
		`COMMIT`,
	} {
		if msgs := c.query(q); has(msgs, msgErrorResponse) {
			t.Fatalf("%s: %s", q, formatError(find(t, msgs, msgErrorResponse).body))
		}
	}
	rows := rowsOf(t, c.query(`SELECT d.value FROM public.docs d WHERE d.id = 'a'`))
	if len(rows) != 1 || string(rows[0][0]) != `"after"` {
		t.Fatalf("read after write: %q", rows)
	}
	for _, q := range []string{`SELECT id,docs."$doc" FROM docs`, `SELECT id,"$doc" FROM public.docs`, `SELECT d.id,d."$doc" FROM public.docs d`} {
		msgs := c.query(q)
		if has(msgs, msgErrorResponse) {
			t.Fatalf("%s: %s", q, formatError(find(t, msgs, msgErrorResponse).body))
		}
		cols := decodeRowDescription(t, find(t, msgs, msgRowDescription).body)
		rows := rowsOf(t, msgs)
		if len(rows) != 1 || string(rows[0][0]) != `"a"` || string(rows[0][1]) != `{"id":"a","value":"after"}` || cols[1].name != "$doc" {
			t.Fatalf("%s: %q %+v", q, rows, cols)
		}
	}
	if tag := commandTagOf(t, extendedSQL(c, `DELETE FROM public.docs WHERE id=$1`, [][]byte{[]byte("a")})); tag != "DELETE 1" {
		t.Fatal(tag)
	}
	expectError(t, c.query(`SELECT * FROM private.docs`), sqlstateSyntaxError)
	for _, q := range []string{`SELECT 'FROM public.docs'`, `SELECT x.public.value FROM docs x`, `SELECT * FROM "Public".docs`} {
		if _, changed, err := lowerPublicRelations(q, nil); changed || err != nil {
			t.Fatalf("rewrote non-public relation: %s", q)
		}
	}
}

func TestGoLandSearchPathBinaryAndJDBCIsolation(t *testing.T) {
	c := connectIntrospectionCatalog(t)
	c.send(msgParse, parseMsg("path", `select current_database() as a, current_schemas(false) as b`))
	c.send(msgBind, bindMsg("", "path", nil, nil, []int16{formatBinary}))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	rows := rowsOf(t, c.until(msgReadyForQuery))
	if len(rows) != 1 || len(rows[0][1]) != 30 {
		t.Fatalf("array row: %q", rows)
	}
	a := rows[0][1]
	if binary.BigEndian.Uint32(a) != 1 || binary.BigEndian.Uint32(a[8:]) != oidText || string(a[24:]) != "public" {
		t.Fatalf("array encoding: %v", a)
	}
	for _, level := range []string{"READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"} {
		if msgs := c.query("BEGIN ISOLATION LEVEL " + level); has(msgs, msgErrorResponse) {
			t.Fatal(formatError(find(t, msgs, msgErrorResponse).body))
		}
		rows := rowsOf(t, c.query("SHOW TRANSACTION ISOLATION LEVEL"))
		if len(rows) != 1 || string(rows[0][0]) != strings.ToLower(level) {
			t.Fatalf("isolation %s: %q", level, rows)
		}
		c.query("ROLLBACK")
	}
	rows = rowsOf(t, c.query("SHOW TRANSACTION ISOLATION LEVEL"))
	if len(rows) != 1 || string(rows[0][0]) != "read committed" {
		t.Fatalf("default isolation: %q", rows)
	}
	rows = rowsOf(t, c.query("SELECT id FROM users; SHOW TRANSACTION ISOLATION LEVEL"))
	if len(rows) != 1 || string(rows[0][0]) != "read committed" {
		t.Fatalf("implicit isolation after explicit rollback: %q", rows)
	}
}

func TestDiscoveryMatcherRejectsSemanticChanges(t *testing.T) {
	for _, shape := range discoveryShapes {
		if shape.Name == "RetrieveDateStyle" {
			continue
		}
		q := strings.ReplaceAll(strings.ReplaceAll(shape.SQL, ":schema_id", "2200"), ":[*f_names]", "'users'")
		for _, suffix := range []string{" LIMIT 1", "; SELECT 1", " UNION SELECT 1"} {
			tokens, err := discoveryTokens(q+"\n"+suffix, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := matchDiscovery(tokens, shape.tokens); ok {
				t.Fatalf("accepted suffix for %s", shape.Name)
			}
		}
	}
}

func TestGoLandPreparedSchemaDiscovery(t *testing.T) {
	c := connectIntrospectionCatalog(t)
	for _, shape := range discoveryShapes {
		if strings.HasPrefix(shape.Name, "JDBC ") || !strings.Contains(shape.SQL, ":schema_id") {
			continue
		}
		q := shape.SQL
		var args [][]byte
		for strings.Contains(q, ":schema_id") {
			args = append(args, []byte("2200"))
			q = strings.Replace(q, ":schema_id", fmt.Sprintf("$%d", len(args)), 1)
		}
		for strings.Contains(q, ":[*f_names]") {
			args = append(args, []byte("users"))
			q = strings.Replace(q, ":[*f_names]", fmt.Sprintf("$%d", len(args)), 1)
		}
		for strings.Contains(q, ":state") {
			args = append(args, nil)
			q = strings.Replace(q, ":state", fmt.Sprintf("$%d", len(args)), 1)
		}
		msgs := extendedSQL(c, q, args)
		if has(msgs, msgErrorResponse) {
			t.Errorf("%s: %s", shape.Name, formatError(find(t, msgs, msgErrorResponse).body))
		}
	}
}
