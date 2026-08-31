package query

import (
	"errors"
	"slices"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func proveCorrelatedValue(
	t testing.TB,
	source string,
) (*Statement, *statementDecorrelatedExists) {
	t.Helper()
	tree, err := sqlast.Parse(source)
	if err != nil {
		t.Fatalf("parse correlated predicate: %v", err)
	}
	statement := &Statement{text: source, tree: tree, params: tree.Params}
	if err := statement.prepareDecorrelatedExists(); err != nil {
		t.Fatalf("prove correlated predicate: %v", err)
	}
	if statement.nested == nil || len(statement.nested.decorrelated) != 1 {
		t.Fatalf("proofs = %+v, want exactly one", statement.nested)
	}
	return statement, &statement.nested.decorrelated[0]
}

func TestCorrelatedValueProofClassifiesLegacyAndGroupedOperators(t *testing.T) {
	const child = `SELECT i.value FROM inner_docs AS i WHERE ` +
		`i.tenant = o.tenant AND o.bucket = i.bucket AND i.active = TRUE`
	tests := []struct {
		name      string
		predicate string
		kind      correlatedMarkKind
		op        Op
		mark      bool
		probe     string
		project   string
	}{
		{
			name: "single key exists keeps legacy semi join",
			predicate: `EXISTS (SELECT 1 FROM inner_docs i ` +
				`WHERE i.tenant = o.tenant AND i.active = TRUE)`,
			kind: correlatedMarkExists,
		},
		{
			name: "single key not exists keeps legacy anti join",
			predicate: `NOT EXISTS (SELECT 1 FROM inner_docs i ` +
				`WHERE i.tenant = o.tenant AND i.active = TRUE)`,
			kind: correlatedMarkNotExists,
		},
		{
			name:      "composite exists",
			predicate: `EXISTS (` + child + `)`,
			kind:      correlatedMarkExists,
			mark:      true,
		},
		{
			name:      "composite direct not exists",
			predicate: `NOT EXISTS (` + child + `)`,
			kind:      correlatedMarkNotExists,
			mark:      true,
		},
		{
			name:      "in",
			predicate: `o.wanted IN (` + child + `)`,
			kind:      correlatedMarkIn,
			mark:      true,
			probe:     "wanted",
			project:   "value",
		},
		{
			name:      "authored not in",
			predicate: `o.wanted NOT IN (` + child + `)`,
			kind:      correlatedMarkNotIn,
			mark:      true,
			probe:     "wanted",
			project:   "value",
		},
		{
			name:      "direct not maps in to not in",
			predicate: `NOT (o.wanted IN (` + child + `))`,
			kind:      correlatedMarkNotIn,
			mark:      true,
			probe:     "wanted",
			project:   "value",
		},
		{
			name:      "direct not maps not in to in",
			predicate: `NOT (o.wanted NOT IN (` + child + `))`,
			kind:      correlatedMarkIn,
			mark:      true,
			probe:     "wanted",
			project:   "value",
		},
		{
			name:      "scalar",
			predicate: `o.wanted < (` + child + `)`,
			kind:      correlatedMarkScalar,
			op:        Lt,
			mark:      true,
			probe:     "wanted",
			project:   "value",
		},
		{
			name:      "direct not scalar inverts authored operator",
			predicate: `NOT (o.wanted <= (` + child + `))`,
			kind:      correlatedMarkScalar,
			op:        Gt,
			mark:      true,
			probe:     "wanted",
			project:   "value",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := `SELECT o.id FROM outer_docs AS o WHERE ` + test.predicate
			statement, proof := proveCorrelatedValue(t, source)
			defer statement.Release()
			wantAnti := test.kind == correlatedMarkNotExists
			if proof.kind != test.kind || proof.op != test.op ||
				proof.mark != test.mark || proof.anti != wantAnti {
				t.Fatalf("proof kind/op/mark/anti = %d/%d/%t/%t, want %d/%d/%t/%t",
					proof.kind, proof.op, proof.mark, proof.anti,
					test.kind, test.op, test.mark, wantAnti)
			}
			wantKeys := []correlatedMarkKey{{outer: "tenant", inner: "tenant"}}
			if test.mark && strings.Contains(test.predicate, "bucket") {
				wantKeys = append(wantKeys,
					correlatedMarkKey{outer: "bucket", inner: "bucket"})
			}
			if !sameCorrelatedMarkKeyPaths(proof.markKeys, wantKeys) {
				t.Fatalf("mark keys = %+v, want %+v", proof.markKeys, wantKeys)
			}

			statement.c.prepare(&statement.q)
			if err := statement.buildDecorrelatedExists(nil); err != nil {
				t.Fatal(err)
			}
			if test.mark {
				if len(statement.q.joins) != 0 || len(statement.q.marks) != 1 {
					t.Fatalf("lowered joins/marks = %d/%d, want 0/1",
						len(statement.q.joins), len(statement.q.marks))
				}
				mark := statement.q.marks[0]
				if mark.kind != test.kind || mark.op != test.op ||
					mark.probe != test.probe || mark.project != test.project ||
					!slices.Equal(mark.keys, proof.markKeys) || !mark.hasWhere {
					t.Fatalf("lowered mark = %+v, want kind=%d op=%d probe=%q project=%q keys=%+v where",
						mark, test.kind, test.op, test.probe, test.project, wantKeys)
				}
			} else if len(statement.q.joins) != 1 || len(statement.q.marks) != 0 ||
				statement.q.joins[0].anti != wantAnti ||
				statement.q.joins[0].origin != joinOriginDecorrelatedExists {
				t.Fatalf("legacy lowering joins=%+v marks=%+v",
					statement.q.joins, statement.q.marks)
			}
		})
	}
}

func sameCorrelatedMarkKeyPaths(left, right []correlatedMarkKey) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].outer != right[i].outer || left[i].inner != right[i].inner {
			return false
		}
	}
	return true
}

func TestCorrelatedValueDirectNotScalarComplements(t *testing.T) {
	tests := []struct {
		authored string
		want     Op
	}{
		{"=", Ne}, {"!=", Eq}, {"<>", Eq}, {"<", Ge},
		{"<=", Gt}, {">", Le}, {">=", Lt},
	}
	for _, test := range tests {
		source := `SELECT o.id FROM outer_docs o WHERE NOT (o.value ` +
			test.authored + ` (SELECT i.value FROM inner_docs i WHERE i.k = o.k))`
		statement, proof := proveCorrelatedValue(t, source)
		if proof.kind != correlatedMarkScalar || proof.op != test.want {
			statement.Release()
			t.Fatalf("NOT scalar %s lowered op = %d, want %d",
				test.authored, proof.op, test.want)
		}
		statement.Release()
	}
}

func TestCorrelatedValueProofRebasesPlaceholdersWithoutMutatingAST(t *testing.T) {
	const source = `SELECT o.id FROM outer_docs o WHERE o.enabled = ? AND ` +
		`o.wanted IN (SELECT i.value FROM inner_docs i WHERE ` +
		`i.tenant = o.tenant AND i.active = ?) AND o.tail = ?`
	tree, err := sqlast.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	leaf := tree.Where.Kids[1]
	child := leaf.Subquery
	key := child.Where.Kids[0]
	residual := child.Where.Kids[1]
	if residual.Value.Kind != sqlast.OperandParam || residual.Value.Ordinal != 0 ||
		child.ParamBase != 1 {
		t.Fatalf("parsed child placeholder = %+v base=%d", residual.Value, child.ParamBase)
	}
	originalWhere, originalKey, originalResidual := child.Where, key, residual
	statement := &Statement{text: source, tree: tree, params: tree.Params}
	defer statement.Release()
	if err := statement.prepareDecorrelatedExists(); err != nil {
		t.Fatal(err)
	}
	proof := &statement.nested.decorrelated[0]
	if proof.local == residual || proof.local.Value.Ordinal != 1 {
		t.Fatalf("private residual clone = %p/%+v, authored=%p",
			proof.local, proof.local.Value, residual)
	}
	statement.c.prepare(&statement.q)
	if err := statement.buildDecorrelatedExists([]any{true, false, true}); err != nil {
		t.Fatal(err)
	}
	if child.Where != originalWhere || child.Where.Kids[0] != originalKey ||
		child.Where.Kids[1] != originalResidual || residual.Value.Ordinal != 0 ||
		key.RightPath != child.Correlation.References[0].Path {
		t.Fatal("proof/lowering mutated the parser-owned tree or correlation identity")
	}
	if len(statement.q.marks) != 1 || !statement.q.marks[0].hasWhere {
		t.Fatalf("parameterized mark = %+v", statement.q.marks)
	}
}

func TestCorrelatedValueWarmParameterizedLoweringAllocatesZero(t *testing.T) {
	const source = `SELECT o.id FROM outer_docs o WHERE o.enabled = ? AND ` +
		`o.wanted NOT IN (SELECT i.value FROM inner_docs i WHERE ` +
		`i.tenant = o.tenant AND i.bucket = o.bucket AND i.active = ?)`
	statement, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	enabled, active := true, true
	args := []any{&enabled, &active}
	for range 2 {
		if err := statement.lower(args); err != nil {
			t.Fatal(err)
		}
	}
	if got := testing.AllocsPerRun(200, func() {
		if err := statement.lower(args); err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("warmed grouped correlated lowering allocated %.2f times", got)
	}
	if len(statement.q.marks) != 1 || statement.q.marks[0].kind != correlatedMarkNotIn ||
		len(statement.q.marks[0].keys) != 2 {
		t.Fatalf("final warmed mark = %+v", statement.q.marks)
	}
}

func TestCorrelatedValueReplacesOnlyProvedAuthoredConjuncts(t *testing.T) {
	const source = `SELECT o.id FROM outer_docs o WHERE o.live = TRUE AND ` +
		`EXISTS (SELECT 1 FROM allow_docs a WHERE a.k = o.k) AND ` +
		`o.wanted IN (SELECT i.value FROM inner_docs i WHERE ` +
		`i.tenant = o.tenant AND i.bucket = o.bucket) AND ` +
		`NOT (o.score >= (SELECT s.value FROM scalar_docs s WHERE ` +
		`s.tenant = o.tenant AND s.bucket = o.bucket))`
	tree, err := sqlast.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	statement := &Statement{text: source, tree: tree, params: tree.Params}
	defer statement.Release()
	if err := statement.prepareSubqueries(0); err != nil {
		t.Fatal(err)
	}
	if statement.nested == nil || len(statement.nested.decorrelated) != 3 ||
		len(statement.nested.subqueries) != 0 {
		t.Fatalf("cold proofs/subqueries = %+v", statement.nested)
	}
	statement.c.prepare(&statement.q)
	if err := statement.buildDecorrelatedExists(nil); err != nil {
		t.Fatal(err)
	}
	if err := statement.buildWhere(nil); err != nil {
		t.Fatal(err)
	}
	if len(statement.q.joins) != 1 || len(statement.q.marks) != 2 ||
		!statement.q.hasWhere || statement.q.where.kind != predCmp {
		t.Fatalf("lowered joins/marks/where = %d/%d/%+v",
			len(statement.q.joins), len(statement.q.marks), statement.q.where)
	}
	if statement.q.marks[0].kind != correlatedMarkIn ||
		statement.q.marks[1].kind != correlatedMarkScalar ||
		statement.q.marks[1].op != Lt {
		t.Fatalf("mark order/modes = %+v", statement.q.marks)
	}
	if statement.decorrelatedExistsFor(tree.Where.Kids[0]) != nil ||
		statement.decorrelatedExistsFor(tree.Where.Kids[1]) == nil ||
		statement.decorrelatedExistsFor(tree.Where.Kids[2]) == nil ||
		statement.decorrelatedExistsFor(tree.Where.Kids[3]) == nil {
		t.Fatal("proof lookup did not preserve exact authored conjunct identity")
	}
}

func TestCorrelatedValueProofRefusalsAreTypedAndPositioned(t *testing.T) {
	tests := []struct {
		name   string
		source string
		at     string
	}{
		{
			"outer OR placement",
			`SELECT o.id FROM outer_docs o WHERE o.live = TRUE OR o.v IN (` +
				`SELECT i.v FROM inner_docs i WHERE i.k = o.k)`,
			"o.v IN",
		},
		{
			"double NOT placement",
			`SELECT o.id FROM outer_docs o WHERE NOT NOT o.v IN (` +
				`SELECT i.v FROM inner_docs i WHERE i.k = o.k)`,
			"o.v IN",
		},
		{
			"correlation under child OR",
			`SELECT o.id FROM outer_docs o WHERE o.v IN (` +
				`SELECT i.v FROM inner_docs i WHERE i.k = o.k OR i.active = TRUE)`,
			"OR i.active",
		},
		{
			"non equality correlation",
			`SELECT o.id FROM outer_docs o WHERE o.v IN (` +
				`SELECT i.v FROM inner_docs i WHERE i.k > o.k)`,
			"i.k >",
		},
		{
			"UTF-8 byte position",
			`/* préfix ✓ */ SELECT o.id FROM outer_docs o WHERE o.v IN (` +
				`SELECT i.v FROM inner_docs i WHERE i.k > o.k)`,
			"i.k >",
		},
		{
			"captured projection is not a key",
			`SELECT o.id FROM outer_docs o WHERE EXISTS (` +
				`SELECT o.secret FROM inner_docs i WHERE i.k = o.k)`,
			"o.secret",
		},
		{
			"local path residual",
			`SELECT o.id FROM outer_docs o WHERE EXISTS (` +
				`SELECT 1 FROM inner_docs i WHERE i.k = o.k AND i.a = i.b)`,
			"i.a =",
		},
		{
			"multiple value projections",
			`SELECT o.id FROM outer_docs o WHERE o.v IN (` +
				`SELECT i.v, i.other FROM inner_docs i WHERE i.k = o.k)`,
			"i.other",
		},
		{
			"computed value projection",
			`SELECT o.id FROM outer_docs o WHERE o.v = (` +
				`SELECT i.v + 1 FROM inner_docs i WHERE i.k = o.k)`,
			"i.v +",
		},
		{
			"aggregate projection",
			`SELECT o.id FROM outer_docs o WHERE EXISTS (` +
				`SELECT COUNT(*) FROM inner_docs i WHERE i.k = o.k)`,
			"COUNT",
		},
		{
			"child join",
			`SELECT o.id FROM outer_docs o WHERE EXISTS (` +
				`SELECT i.v FROM inner_docs i JOIN tags t ON i.tag = t.id ` +
				`WHERE i.k = o.k)`,
			"tags",
		},
		{
			"child CTE",
			`SELECT o.id FROM outer_docs o WHERE EXISTS (` +
				`WITH seed AS (SELECT i.v FROM inner_docs i) ` +
				`SELECT s.v FROM seed s WHERE s.v = o.v)`,
			"WITH",
		},
		{
			"child set expression",
			`SELECT o.id FROM outer_docs o WHERE EXISTS ((` +
				`SELECT i.v FROM inner_docs i WHERE i.k = o.k) UNION ALL ` +
				`SELECT a.v FROM archive_docs a)`,
			"(SELECT i.v",
		},
		{
			"child distinct",
			`SELECT o.id FROM outer_docs o WHERE o.v IN (` +
				`SELECT DISTINCT i.v FROM inner_docs i WHERE i.k = o.k)`,
			"i.v FROM",
		},
		{
			"child grouping",
			`SELECT o.id FROM outer_docs o WHERE EXISTS (` +
				`SELECT i.v FROM inner_docs i WHERE i.k = o.k GROUP BY i.v)`,
			"i.v)",
		},
		{
			"child ordering",
			`SELECT o.id FROM outer_docs o WHERE o.v IN (` +
				`SELECT i.v FROM inner_docs i WHERE i.k = o.k ORDER BY i.v)`,
			"i.v)",
		},
		{
			"child window",
			`SELECT o.id FROM outer_docs o WHERE EXISTS (` +
				`SELECT ROW_NUMBER() OVER (ORDER BY i.v) FROM inner_docs i ` +
				`WHERE i.k = o.k)`,
			"ROW_NUMBER",
		},
		{
			"child limit",
			`SELECT o.id FROM outer_docs o WHERE o.v IN (` +
				`SELECT i.v FROM inner_docs i WHERE i.k = o.k LIMIT 1)`,
			"1)",
		},
		{
			"child offset",
			`SELECT o.id FROM outer_docs o WHERE o.v IN (` +
				`SELECT i.v FROM inner_docs i WHERE i.k = o.k OFFSET 2)`,
			"2)",
		},
		{
			"nested predicate subquery",
			`SELECT o.id FROM outer_docs o WHERE EXISTS (` +
				`SELECT i.v FROM inner_docs i WHERE i.k = o.k AND EXISTS (` +
				`SELECT x.v FROM deep_docs x WHERE x.v = o.v))`,
			"EXISTS (SELECT x",
		},
		{
			"outer is a join pipeline",
			`SELECT o.id FROM outer_docs o JOIN peers p ON o.id = p.id WHERE EXISTS (` +
				`SELECT i.v FROM inner_docs i WHERE i.k = o.k)`,
			"EXISTS",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree, err := sqlast.Parse(test.source)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			statement := &Statement{text: test.source, tree: tree, params: tree.Params}
			err = statement.prepareSubqueries(0)
			statement.Release()
			var unsupported *sqlast.FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("error = %T %v, want positioned 0A000", err, err)
			}
			want := strings.Index(test.source, test.at)
			if unsupported.Pos != want {
				t.Fatalf("position = %d, want %d at %q: %v",
					unsupported.Pos, want, test.at, err)
			}
		})
	}
}
