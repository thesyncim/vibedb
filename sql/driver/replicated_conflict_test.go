package driver

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestReplicatedConflictPreservesCurrentColumnsAndCandidateBranch(t *testing.T) {
	validator, key, current := employeeValidator(t)
	candidate := []byte(`{"active":false,"city":null,"id":"employee-0001","name":"Candidate","score":3,"team":"Another"}`)
	for _, tc := range []struct {
		set  string
		args []any
		want string
	}{
		{`score=EXCLUDED.score,city=?`, []any{"Porto"}, `{"active":true,"city":"Porto","id":"employee-0001","name":"Alex","score":3,"team":"Platform"}`},
		{`score=9007199254740993,city=EXCLUDED.city`, nil, `{"active":true,"city":null,"id":"employee-0001","name":"Alex","score":9007199254740993,"team":"Platform"}`},
	} {
		t.Run(tc.set, func(t *testing.T) {
			statement, err := sqlast.ParseStatement(`INSERT INTO employees (id) VALUES ('unused') ON CONFLICT DO UPDATE SET ` + tc.set)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := EncodeReplicatedConflictValue(candidate, statement.Insert.OnConflictUpdate, tc.args, nil)
			if err != nil {
				t.Fatal(err)
			}
			row, program, ok := replication.OpenConflictValue(payload)
			if !ok {
				t.Fatal("bad encoded payload")
			}
			value, code := validator.MaterializeConflict(key, row, program, current, true)
			if code != replicatedstate.MutationValidationAccept || string(value) != tc.want {
				t.Fatalf("value=%s code=%v", value, code)
			}
			value, code = validator.MaterializeConflict(key, row, program, nil, false)
			if code != replicatedstate.MutationValidationAccept || !bytes.Equal(value, candidate) {
				t.Fatalf("insert evaluated conflict action: %s %v", value, code)
			}
		})
	}
	statement, _ := sqlast.ParseStatement(`INSERT INTO employees (id) VALUES ('unused') ON CONFLICT DO UPDATE SET score=NULL`)
	payload, err := EncodeReplicatedConflictValue(candidate, statement.Insert.OnConflictUpdate, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	row, program, _ := replication.OpenConflictValue(payload)
	value, code := validator.MaterializeConflict(key, row, program, nil, false)
	if code != replicatedstate.MutationValidationAccept || !bytes.Equal(value, candidate) {
		t.Fatal("unused NULL assignment affected the insert")
	}
	value, code = validator.MaterializeConflict(key, row, program, current, true)
	if code != replicatedstate.MutationValidationAccept || validator.ValidatePut(key, value) != replicatedstate.MutationValidationInvalid {
		t.Fatalf("postimage bypassed schema: %s %v", value, code)
	}
	if _, code = validator.MaterializeConflict(key, []byte(`{"id":"employee-0001"}`), program, current, true); code != replicatedstate.MutationValidationInvalid {
		t.Fatal("invalid candidate skipped on conflict")
	}
	for _, set := range []string{`missing=1`, `score=EXCLUDED.missing`} {
		statement, _ := sqlast.ParseStatement(`INSERT INTO employees (id) VALUES ('unused') ON CONFLICT DO UPDATE SET ` + set)
		payload, err := EncodeReplicatedConflictValue(candidate, statement.Insert.OnConflictUpdate, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		row, program, _ := replication.OpenConflictValue(payload)
		if _, code = validator.MaterializeConflict(key, row, program, nil, false); code != replicatedstate.MutationValidationInvalid {
			t.Fatalf("undeclared column accepted on insert: %s", set)
		}
	}
}

func TestReplicatedConflictBoundsMaterializedCurrentRow(t *testing.T) {
	validator, key, current := employeeValidator(t)
	large := strings.Repeat("x", 3000)
	current = bytes.Replace(current, []byte(`"Alex"`), []byte(`"`+large+`"`), 1)
	statement, err := sqlast.ParseStatement(`INSERT INTO employees (id) VALUES ('unused') ON CONFLICT DO UPDATE SET score=1`)
	if err != nil {
		t.Fatal(err)
	}
	candidate := []byte(`{"active":false,"city":null,"id":"employee-0001","name":"Candidate","score":3,"team":"Another"}`)
	payload, err := EncodeReplicatedConflictValue(candidate, statement.Insert.OnConflictUpdate, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	row, program, _ := replication.OpenConflictValue(payload)
	value, code := validator.MaterializeConflict(key, row, program, current, true)
	if code != replicatedstate.MutationValidationAccept || len(value) <= len(payload) || !bytes.Contains(value, []byte(large)) || validator.ValidatePut(key, value) != replicatedstate.MutationValidationAccept {
		t.Fatalf("large current row lost or incorrectly bounded: size=%d code=%v", len(value), code)
	}
	statement, err = sqlast.ParseStatement(`INSERT INTO employees (id) VALUES ('unused') ON CONFLICT DO UPDATE SET team=?`)
	if err != nil {
		t.Fatal(err)
	}
	payload, err = EncodeReplicatedConflictValue(candidate, statement.Insert.OnConflictUpdate, []any{strings.Repeat("y", 2000)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	row, program, _ = replication.OpenConflictValue(payload)
	if _, code = validator.MaterializeConflict(key, row, program, current, true); code != replicatedstate.MutationValidationTargetBound {
		t.Fatalf("expanded conflict row escaped document bound: %v", code)
	}
}

func FuzzReplicatedConflictProgram(f *testing.F) {
	statement, err := sqlast.ParseStatement(`INSERT INTO employees (id) VALUES ('unused') ON CONFLICT DO UPDATE SET city=EXCLUDED.city,score=1`)
	if err != nil {
		f.Fatal(err)
	}
	value, err := EncodeReplicatedConflictValue([]byte(`{"id":"a"}`), statement.Insert.OnConflictUpdate, nil, nil)
	if err != nil {
		f.Fatal(err)
	}
	_, program, _ := replication.OpenConflictValue(value)
	f.Add(program)
	computed, _ := sqlast.ParseStatement(`INSERT INTO employees VALUES (?) ON CONFLICT DO UPDATE SET score=CASE WHEN employees.active THEN employees.score+? ELSE EXCLUDED.score END`)
	encoded, err := EncodeReplicatedConflictValue([]byte(`{"id":"a"}`), computed.Insert.OnConflictUpdate, []any{nil, int64(2)}, nil)
	if err != nil {
		f.Fatal(err)
	}
	_, computedProgram, _ := replication.OpenConflictValue(encoded)
	f.Add(computedProgram)
	f.Add([]byte{0, 0})
	schema := declaredEmployeeSchema(f)
	validator, key, current := employeeValidator(f)
	f.Fuzz(func(t *testing.T, raw []byte) {
		assignments, err := decodeReplicatedConflictAssignments(raw, schema)
		if err != nil {
			return
		}
		if len(assignments) == 0 || len(assignments) > replicatedConflictAssignmentLimit {
			t.Fatal("unbounded program")
		}
		action := &sqlast.InsertConflictUpdate{Assignments: assignments}
		if !ReplicatedConflictAssignments(action) {
			t.Fatal("accepted invalid assignment")
		}
		template, bindings, _ := openConflictProgram(raw)
		_, params, err := decodeConflictTemplate(template)
		if err != nil {
			t.Fatal(err)
		}
		args := make([]any, len(params))
		if err = decodeConflictBindings(bindings, args); err != nil {
			t.Fatal(err)
		}
		if _, err := EncodeReplicatedConflictValue([]byte(`{"id":"a"}`), action, args, params); err != nil {
			t.Fatalf("decoded program cannot encode: %v", err)
		}
		validator.MaterializeConflict(key, current, raw, current, true)
	})
}

func TestReplicatedConflictExpressionsShareLocalSemantics(t *testing.T) {
	v, key, current := employeeValidator(t)
	candidate := []byte(`{"active":false,"city":null,"id":"employee-0001","name":"Candidate","score":3,"team":"Another"}`)
	cases := []struct {
		set  string
		args []any
	}{
		{`score=employees.score+EXCLUDED.score,name=employees.name||':'||EXCLUDED.name`, nil},
		{`score=EXCLUDED.score,city=CAST(employees.score AS TEXT)`, nil},
		{`score=9007199254740993+EXCLUDED.score`, nil},
		{`score=COALESCE(NULL,employees.score,1/0)`, nil},
		{`score=GREATEST(employees.score,EXCLUDED.score),city=NULLIF(employees.city,?)`, []any{"Lisbon"}},
		{`score=CASE WHEN employees.active AND (EXCLUDED.city IS NULL OR employees.score<0) THEN employees.score+? ELSE 1/0 END`, []any{int64(7)}},
		{`score=CASE employees.city WHEN ? THEN -EXCLUDED.score ELSE employees.score END`, []any{"Lisbon"}},
		{`score=CASE WHEN employees.score>=90 AND employees.score<=100 AND (employees.city='Lisbon' OR employees.city='Porto') THEN employees.score%5 ELSE 1 END`, nil},
		{`score=CASE WHEN employees.city IS NOT DISTINCT FROM ? THEN employees.score/2 ELSE employees.score*2 END`, []any{"Lisbon"}},
		{`score=CASE WHEN NOT (employees.active IS FALSE) THEN LEAST(employees.score,EXCLUDED.score) ELSE 0 END`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.set, func(t *testing.T) {
			parsed, err := sqlast.ParseStatement(`INSERT INTO employees (id) VALUES ('unused') ON CONFLICT DO UPDATE SET ` + tc.set)
			if err != nil {
				t.Fatal(err)
			}
			local, err := query.PrepareParsedDML("", parsed)
			if err != nil {
				t.Fatal(err)
			}
			defer local.Release()
			var exec query.Exec
			defer exec.Release()
			want, err := materializeConflictColumnAssignments(local, &exec, current, candidate, parsed.Insert.OnConflictUpdate.Assignments, tc.args, v.maxDocumentBytes)
			if err != nil {
				t.Fatal(err)
			}
			want, err = canonicalMutationCapturePostimage(want, v.maxDocumentBytes)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := EncodeReplicatedConflictValue(candidate, parsed.Insert.OnConflictUpdate, tc.args, nil)
			if err != nil {
				t.Fatal(err)
			}
			row, program, _ := replication.OpenConflictValue(payload)
			got, code := v.MaterializeConflict(key, row, program, current, true)
			if code != replicatedstate.MutationValidationAccept || !bytes.Equal(got, want) {
				t.Fatalf("replica=%s code=%v local=%s", got, code, want)
			}
			got, code = v.MaterializeConflict(key, row, program, nil, false)
			if code != replicatedstate.MutationValidationAccept || !bytes.Equal(got, row) {
				t.Fatalf("insert=%s code=%v", got, code)
			}
		})
	}
}

func TestReplicatedConflictTemplateRebindAndConcurrentAudit(t *testing.T) {
	v, key, current := employeeValidator(t)
	parsed, err := sqlast.ParseStatement(`INSERT INTO employees VALUES (?) ON CONFLICT DO UPDATE SET score=employees.score+?,city=?`)
	if err != nil {
		t.Fatal(err)
	}
	var template []byte
	var prepared *query.DMLStatement
	for i := 0; i < 8; i++ {
		payload, err := EncodeReplicatedConflictValue(current, parsed.Insert.OnConflictUpdate, []any{"candidate excluded from program", int64(i), "Porto"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		row, program, _ := replication.OpenConflictValue(payload)
		next, _, err := openConflictProgram(program)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			template = bytes.Clone(next)
		} else if !bytes.Equal(template, next) {
			t.Fatal("binding changed template")
		}
		got, code := v.MaterializeConflict(key, row, program, current, true)
		if code != replicatedstate.MutationValidationAccept || !bytes.Contains(got, []byte(`"city":"Porto"`)) {
			t.Fatalf("value=%s code=%v", got, code)
		}
		if i == 0 {
			prepared = v.conflict.statement
		} else if prepared != v.conflict.statement {
			t.Fatal("recompiled template")
		}
		for _, arg := range v.conflict.args {
			if arg != nil {
				t.Fatal("retained binding")
			}
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload, err := EncodeReplicatedConflictValue(current, parsed.Insert.OnConflictUpdate, []any{nil, int64(1), "Porto"}, nil)
			if err != nil {
				t.Error(err)
				return
			}
			row, program, _ := replication.OpenConflictValue(payload)
			for j := 0; j < 10; j++ {
				if _, code := v.MaterializeConflict(key, row, program, current, true); code != replicatedstate.MutationValidationAccept {
					t.Errorf("audit=%v", code)
				}
			}
		}()
	}
	wg.Wait()
}

func TestReplicatedConflictExpressionProgramValidationAndLaziness(t *testing.T) {
	v, key, current := employeeValidator(t)
	for _, set := range []string{`score=employees.score/0`, `score=CASE WHEN employees.active THEN employees.score/0 ELSE 1 END`} {
		parsed, err := sqlast.ParseStatement(`INSERT INTO employees VALUES (?) ON CONFLICT DO UPDATE SET ` + set)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := EncodeReplicatedConflictValue(current, parsed.Insert.OnConflictUpdate, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		row, program, _ := replication.OpenConflictValue(payload)
		if _, code := v.MaterializeConflict(key, row, program, nil, false); code != replicatedstate.MutationValidationAccept {
			t.Fatalf("unused branch=%v", code)
		}
		if _, code := v.MaterializeConflict(key, row, program, current, true); code != replicatedstate.MutationValidationInvalid {
			t.Fatalf("division accepted=%v", code)
		}
	}
	for _, set := range []string{`score=employees.missing+1`, `score=EXCLUDED.missing+1`} {
		parsed, _ := sqlast.ParseStatement(`INSERT INTO employees VALUES (?) ON CONFLICT DO UPDATE SET ` + set)
		payload, err := EncodeReplicatedConflictValue(current, parsed.Insert.OnConflictUpdate, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		row, program, _ := replication.OpenConflictValue(payload)
		if _, code := v.MaterializeConflict(key, row, program, nil, false); code != replicatedstate.MutationValidationInvalid {
			t.Fatal("undeclared computed column accepted")
		}
	}
	parsed, _ := sqlast.ParseStatement(`INSERT INTO employees VALUES (?) ON CONFLICT DO UPDATE SET score=employees.score+?`)
	payload, err := EncodeReplicatedConflictValue(current, parsed.Insert.OnConflictUpdate, []any{nil, int64(1)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	row, program, _ := replication.OpenConflictValue(payload)
	for n := 0; n < len(program); n++ {
		if _, code := v.MaterializeConflict(key, row, program[:n], current, true); code != replicatedstate.MutationValidationInvalid {
			t.Fatalf("accepted truncation %d", n)
		}
	}
	if _, code := v.MaterializeConflict(key, row, append(bytes.Clone(program), 0), current, true); code != replicatedstate.MutationValidationInvalid {
		t.Fatal("accepted trailing bytes")
	}
	malformed := bytes.Clone(program)
	binary.LittleEndian.PutUint32(malformed, 0xffffffff)
	if _, code := v.MaterializeConflict(key, row, malformed, current, true); code != replicatedstate.MutationValidationInvalid {
		t.Fatal("accepted overflow")
	}
	expression := &sqlast.ScalarExpr{Kind: sqlast.ScalarNull}
	for i := 0; i < replicatedConflictDepthLimit+1; i++ {
		expression = &sqlast.ScalarExpr{Kind: sqlast.ScalarUnary, Op: sqlast.ScalarPositive, Left: expression}
	}
	action := &sqlast.InsertConflictUpdate{Assignments: []sqlast.UpdateAssignment{{Column: "score", Value: sqlast.Operand{Kind: sqlast.OperandExpression}, Expr: expression}}}
	if _, err := EncodeReplicatedConflictValue(current, action, nil, nil); err == nil {
		t.Fatal("accepted over-depth expression")
	}
}

func TestReplicatedConflictTemplateGolden(t *testing.T) {
	parsed, err := sqlast.ParseStatement(`INSERT INTO employees VALUES (?) ON CONFLICT DO UPDATE SET score=employees.score+?`)
	if err != nil {
		t.Fatal(err)
	}
	template, ordinals, err := encodeConflictTemplate(parsed.Insert.OnConflictUpdate, nil)
	if err != nil {
		t.Fatal(err)
	}
	const want = "01000100050073636f72650104000000050073636f72650104000000"
	if hex.EncodeToString(template) != want || len(ordinals) != 1 || ordinals[1] != 0 {
		t.Fatalf("template=%x ordinals=%v", template, ordinals)
	}
}

func BenchmarkReplicatedConflictMaterialization(b *testing.B) {
	for _, set := range []string{`score=EXCLUDED.score`, `score=employees.score+?`} {
		b.Run(set, func(b *testing.B) {
			v, key, current := employeeValidator(b)
			parsed, err := sqlast.ParseStatement(`INSERT INTO employees VALUES (?) ON CONFLICT DO UPDATE SET ` + set)
			if err != nil {
				b.Fatal(err)
			}
			value, err := EncodeReplicatedConflictValue(current, parsed.Insert.OnConflictUpdate, []any{nil, int64(1)}, nil)
			if err != nil {
				b.Fatal(err)
			}
			row, program, _ := replication.OpenConflictValue(value)
			if _, code := v.MaterializeConflict(key, row, program, current, true); code != replicatedstate.MutationValidationAccept {
				b.Fatal(code)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, code := v.MaterializeConflict(key, row, program, current, true); code != replicatedstate.MutationValidationAccept {
					b.Fatal(code)
				}
			}
		})
	}
}

func TestReplicatedConflictPreservesParameterTypes(t *testing.T) {
	v, key, current := employeeValidator(t)
	parsed, err := sqlast.ParseStatement(`INSERT INTO employees VALUES (?) ON CONFLICT DO UPDATE SET active=CASE WHEN employees.active THEN ? ELSE NULL END`)
	if err != nil {
		t.Fatal(err)
	}
	types := []query.ParameterType{query.ParameterTypeUnspecified, query.ParameterTypeBool}
	value, err := EncodeReplicatedConflictValue(current, parsed.Insert.OnConflictUpdate, []any{nil, false}, types)
	if err != nil {
		t.Fatal(err)
	}
	row, program, _ := replication.OpenConflictValue(value)
	template, _, _ := openConflictProgram(program)
	_, decoded, err := decodeConflictTemplate(template)
	if err != nil || len(decoded) != 1 || decoded[0] != query.ParameterTypeBool {
		t.Fatalf("types=%v err=%v", decoded, err)
	}
	got, code := v.MaterializeConflict(key, row, program, current, true)
	if code != replicatedstate.MutationValidationAccept || !bytes.Contains(got, []byte(`"active":false`)) {
		t.Fatalf("typed result=%s code=%v", got, code)
	}
}
