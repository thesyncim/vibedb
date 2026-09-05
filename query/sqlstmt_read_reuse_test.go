package query

import (
	"fmt"
	"strings"
	"testing"
	"unsafe"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestStatementResetReadBindingsForReuseRebindsLikeFresh(t *testing.T) {
	const source = `SELECT id, score FROM docs WHERE id = ? ORDER BY id`
	statement, err := PrepareStatement(source)
	if err != nil {
		t.Fatalf("PrepareStatement: %v", err)
	}
	defer statement.Release()

	retained, ok := statement.ResetReadBindingsForReuse()
	if !ok {
		t.Fatal("direct point-read statement declined")
	}
	if retained < int64(unsafe.Sizeof(*statement)) {
		t.Fatalf("retained bytes = %d, Statement object = %d", retained, unsafe.Sizeof(*statement))
	}
	if statement.c.plan == nil || statement.c.result == nil || statement.q.built != nil ||
		statement.c.plan.where != nil || statement.c.plan.columns != nil ||
		statement.c.result.plan != nil || statement.c.result.err != nil ||
		statement.args != nil || statement.stack != nil || statement.having.active {
		t.Fatal("binding/compiler state survived reset")
	}
	if statement.tree == nil || statement.params != 1 || len(statement.names) != 2 {
		t.Fatal("immutable preparation metadata did not survive reset")
	}

	segment := mustSegment(t,
		`{"id":1,"score":10}`,
		`{"id":2,"score":20}`,
		`{"id":3,"score":30}`,
	)
	for _, id := range []int64{2, 1, 3, 2} {
		var reusedExec Exec
		reusedCursor, err := statement.RunInto(&reusedExec, FromSegment(segment), []any{id})
		if err != nil {
			t.Fatalf("reused bind %d: %v", id, err)
		}
		reused := cursorKey(statement, reusedCursor)

		fresh, err := PrepareStatement(source)
		if err != nil {
			t.Fatalf("fresh PrepareStatement(%d): %v", id, err)
		}
		var freshExec Exec
		freshCursor, err := fresh.RunInto(&freshExec, FromSegment(segment), []any{id})
		if err != nil {
			fresh.Release()
			t.Fatalf("fresh bind %d: %v", id, err)
		}
		want := cursorKey(fresh, freshCursor)
		fresh.Release()
		if reused != want {
			t.Fatalf("bind %d reused result %q, fresh result %q", id, reused, want)
		}
		freshExec.Release()
		reusedExec.Release()
		if _, ok := statement.ResetReadBindingsForReuse(); !ok {
			t.Fatalf("reset after bind %d declined", id)
		}
	}
}

func TestStatementReadReuseAlternatesNullTypesLimitsAndErrors(t *testing.T) {
	const source = `SELECT id, score FROM docs WHERE id = ? ORDER BY id LIMIT ?`
	segment := mustSegment(t, `{"id":1,"score":10}`, `{"id":2,"score":20}`, `{"id":"2","score":200}`)
	for _, typed := range []bool{false, true} {
		t.Run(fmt.Sprint(typed), func(t *testing.T) {
			prepare := func() *Statement {
				tree, err := sqlast.ParseStatement(source)
				if err != nil {
					t.Fatal(err)
				}
				var hints []ParameterType
				if typed {
					hints = []ParameterType{ParameterTypeText, ParameterTypeUnspecified}
				}
				statement, err := PrepareParsedStatementWithParameterTypes(source, tree.Select, hints)
				if err != nil {
					t.Fatal(err)
				}
				return statement
			}
			statement := prepare()
			defer statement.Release()
			text, limit := "2", int64(1)
			floatValue := 2.0
			boolValue := true
			cases := [][]any{
				{int64(2), int64(1)}, {nil, int64(2)}, {"2", int64(1)},
				{true, int64(1)}, {int64(1), "bad"}, {"2", int64(0)}, {int64(1), int64(2)},
				{&text, &limit}, {&floatValue, &limit}, {&boolValue, &limit},
			}
			for _, args := range cases {
				fresh := prepare()
				var reusedExec, freshExec Exec
				got, gotErr := statement.RunInto(&reusedExec, FromSegment(segment), args)
				want, wantErr := fresh.RunInto(&freshExec, FromSegment(segment), args)
				if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
					t.Fatalf("args=%v reused error=%v fresh error=%v", args, gotErr, wantErr)
				}
				if gotErr == nil && cursorKey(statement, got) != cursorKey(fresh, want) {
					t.Fatalf("args=%v reused result differs", args)
				}
				fresh.Release()
				freshExec.Release()
				reusedExec.Release()
				if _, ok := statement.ResetReadBindingsForReuse(); !ok {
					t.Fatalf("args=%v reset declined", args)
				}
				if strings.Join(statement.Columns(), ",") != "id,score" {
					t.Fatal("reset changed output metadata")
				}
				if typed && statement.ParameterType(0) != ParameterTypeText {
					t.Fatal("reset changed declared parameter type")
				}
			}
		})
	}
}

func TestStatementReadReuseScrubsArenaTails(t *testing.T) {
	statement, err := PrepareStatement(`SELECT id FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if _, ok := statement.ResetReadBindingsForReuse(); !ok {
		t.Fatal("direct statement declined")
	}
	if err := statement.bind([]any{"small"}); err != nil {
		t.Fatal(err)
	}

	// Keep one live any arena chunk and put a second chunk header in the
	// directory's unused capacity. Both the object in the live tail and the
	// chunk referenced only by the directory tail must be severed by reset.
	poison := new(string)
	*poison = strings.Repeat("poison", 128)
	statement.c.boxes.chunks = make([][]any, 1, 2)
	statement.c.boxes.chunks[0] = make([]any, 1, 4)
	statement.c.boxes.chunks[0][0] = poison
	statement.c.boxes.chunks[0][:4][3] = poison
	directory := statement.c.boxes.chunks[:2]
	directory[1] = []any{poison}
	statement.c.boxes.chunks = directory[:1:2]
	if _, ok := statement.ResetReadBindingsForReuse(); !ok {
		t.Fatal("reset declined")
	}
	if got := statement.c.boxes.chunks[0][0]; got != nil {
		t.Fatalf("live arena tail retained %T", got)
	}
	if got := statement.c.boxes.chunks[0][:cap(statement.c.boxes.chunks[0])][3]; got != nil {
		t.Fatal("unused arena capacity retained a value")
	}
	if got := statement.c.boxes.chunks[:cap(statement.c.boxes.chunks)][1]; got != nil {
		t.Fatal("arena directory tail retained a chunk")
	}
}

func TestStatementReadReuseCountsCompilerStorage(t *testing.T) {
	statement, err := PrepareStatement(`SELECT id FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	base, ok := statement.ResetReadBindingsForReuse()
	if !ok {
		t.Fatal("direct statement declined")
	}

	// Install known capacities in one retained plain slice and one retained
	// arena. The difference between two resets must charge both the arena
	// directory headers and every chunk's full backing capacity.
	baseHeaders := cap(statement.c.headers)
	statement.c.headers = make([]string, 0, baseHeaders+3)
	statement.c.boxes.chunks = make([][]any, 2, 4)
	statement.c.boxes.chunks[0] = make([]any, 0, 3)
	statement.c.boxes.chunks[1] = make([]any, 0, 5)
	got, ok := statement.ResetReadBindingsForReuse()
	if !ok {
		t.Fatal("second reset declined")
	}
	wantDelta := int64(3*unsafe.Sizeof(string(""))) +
		int64(4*unsafe.Sizeof([]any{})) +
		int64(8*unsafe.Sizeof(any(nil)))
	if got-base != wantDelta {
		t.Fatalf("compiler retained-byte delta = %d, want %d", got-base, wantDelta)
	}
}

func TestStatementReadReuseSmallLargeSmallRecovery(t *testing.T) {
	const source = `SELECT id FROM docs WHERE id = ?`
	statement, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if _, ok := statement.ResetReadBindingsForReuse(); !ok {
		t.Fatal("direct statement declined")
	}
	segment := mustSegment(t, `{"id":"small"}`)

	run := func(value any) string {
		var exec Exec
		cursor, err := statement.RunInto(&exec, FromSegment(segment), []any{value})
		if err != nil {
			exec.Release()
			t.Fatalf("bind %v: %v", value, err)
		}
		got := cursorKey(statement, cursor)
		exec.Release()
		if _, ok := statement.ResetReadBindingsForReuse(); !ok {
			t.Fatalf("reset after %v declined", value)
		}
		return got
	}

	if got := run("small"); got != "|id\n4:\"small\"|\n" {
		t.Fatalf("small result = %q", got)
	}
	long := strings.Repeat("x", 128<<10)
	if got := run(long); got != "|id\n" {
		t.Fatalf("large result = %q", got)
	}
	if statement.c.plan != nil || statement.c.result != nil {
		t.Fatal("oversized compiler storage was retained")
	}
	if got := run("small"); got != "|id\n4:\"small\"|\n" {
		t.Fatalf("recovered small result = %q", got)
	}
}

func TestStatementReadReuseRebasesRetiredPathArenas(t *testing.T) {
	paths := make([]string, 32)
	for i := range paths {
		paths[i] = fmt.Sprintf("field_%03d_%s", i, strings.Repeat("x", 24))
	}
	statement, err := PrepareStatement("SELECT " + strings.Join(paths, ",") + " FROM docs WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	inArena := func(text string) bool {
		if len(text) == 0 {
			return true
		}
		start := uintptr(unsafe.Pointer(unsafe.SliceData(statement.specBuf)))
		at := uintptr(unsafe.Pointer(unsafe.StringData(text)))
		return at >= start && at-start <= uintptr(len(statement.specBuf)) && uintptr(len(text)) <= uintptr(len(statement.specBuf))-(at-start)
	}
	retired := false
	for _, name := range statement.names {
		retired = retired || !inArena(name)
	}
	if !retired {
		t.Fatal("fixture did not retain a previous append backing array")
	}
	if _, ok := statement.ResetReadBindingsForReuse(); !ok {
		t.Fatal("direct statement declined")
	}
	for _, name := range statement.names {
		if !inArena(name) {
			t.Fatalf("header %q retains an uncharged arena", name)
		}
	}
	for _, spec := range statement.specs {
		if !inArena(spec.text) {
			t.Fatalf("path %q retains an uncharged arena", spec.text)
		}
	}
	if allocs := testing.AllocsPerRun(20, func() {
		if _, ok := statement.ResetReadBindingsForReuse(); !ok {
			t.Fatal("warm reset declined")
		}
	}); allocs != 0 {
		t.Fatalf("metadata rebasing allocations=%v", allocs)
	}
}

func TestStatementResetReadBindingsForReuseDropsLongBinding(t *testing.T) {
	statement, err := PrepareStatement(`SELECT id FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatalf("PrepareStatement: %v", err)
	}
	defer statement.Release()
	if _, ok := statement.ResetReadBindingsForReuse(); !ok {
		t.Fatal("direct point-read statement declined")
	}
	long := make([]byte, 128<<10)
	for i := range long {
		long[i] = 'x'
	}
	if err := statement.bind([]any{string(long)}); err != nil {
		t.Fatalf("bind(long): %v", err)
	}
	if statement.args != nil {
		t.Fatal("bind retained caller argument vector")
	}
	if _, ok := statement.ResetReadBindingsForReuse(); !ok {
		t.Fatal("reset after long bind declined")
	}
	if statement.q.built != nil || statement.c.plan != nil || statement.c.result != nil ||
		statement.args != nil {
		t.Fatal("long binding remains reachable after reset")
	}
}

func TestStatementResetReadBindingsForReuseCountsOwnedMetadata(t *testing.T) {
	ones, err := PrepareStatement(`SELECT id FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatalf("PrepareStatement(one): %v", err)
	}
	defer ones.Release()
	many, err := PrepareStatement(`SELECT id, score, bucket FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatalf("PrepareStatement(many): %v", err)
	}
	defer many.Release()
	oneBytes, ok := ones.ResetReadBindingsForReuse()
	if !ok {
		t.Fatal("one-column statement declined")
	}
	manyBytes, ok := many.ResetReadBindingsForReuse()
	if !ok {
		t.Fatal("multi-column statement declined")
	}
	if manyBytes <= oneBytes {
		t.Fatalf("metadata capacities did not affect retained bytes: one=%d many=%d", oneBytes, manyBytes)
	}
}

func TestStatementResetReadBindingsForReuseDeclinesUnsupportedShapes(t *testing.T) {
	cases := []string{
		`SELECT id FROM docs`,
		`SELECT COUNT(id) FROM docs WHERE id = ?`,
		`SELECT id FROM docs WHERE id <> ?`,
		`SELECT id FROM docs WHERE id = ? ORDER BY id DESC`,
		`SELECT id + 1 FROM docs WHERE id = ?`,
		`SELECT id FROM docs WHERE id IN (?)`,
		`SELECT id FROM docs WHERE id = ? OFFSET 1`,
	}
	for _, source := range cases {
		t.Run(source, func(t *testing.T) {
			statement, err := PrepareStatement(source)
			if err != nil {
				t.Fatalf("PrepareStatement: %v", err)
			}
			defer statement.Release()
			if retained, ok := statement.ResetReadBindingsForReuse(); ok || retained != 0 {
				t.Fatalf("unsupported shape retained bytes = (%d, %v)", retained, ok)
			}
		})
	}
}
