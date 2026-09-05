package driver

import (
	"bytes"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
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
			payload, err := EncodeReplicatedConflictValue(candidate, statement.Insert.OnConflictUpdate, tc.args)
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
	payload, err := EncodeReplicatedConflictValue(candidate, statement.Insert.OnConflictUpdate, nil)
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
		payload, err := EncodeReplicatedConflictValue(candidate, statement.Insert.OnConflictUpdate, nil)
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
	payload, err := EncodeReplicatedConflictValue(candidate, statement.Insert.OnConflictUpdate, nil)
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
	payload, err = EncodeReplicatedConflictValue(candidate, statement.Insert.OnConflictUpdate, []any{strings.Repeat("y", 2000)})
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
	value, err := EncodeReplicatedConflictValue([]byte(`{"id":"a"}`), statement.Insert.OnConflictUpdate, nil)
	if err != nil {
		f.Fatal(err)
	}
	_, program, _ := replication.OpenConflictValue(value)
	f.Add(program)
	f.Add([]byte{0, 0})
	schema := declaredEmployeeSchema(f)
	f.Fuzz(func(t *testing.T, raw []byte) {
		assignments, err := decodeReplicatedConflictAssignments(raw, schema)
		if err != nil {
			return
		}
		if len(assignments) == 0 || len(assignments) > replicatedConflictAssignmentLimit {
			t.Fatal("unbounded program")
		}
		action := &sqlast.InsertConflictUpdate{Assignments: assignments}
		if !DirectReplicatedConflictAssignments(action) {
			t.Fatal("accepted non-direct instruction")
		}
		if _, err := EncodeReplicatedConflictValue([]byte(`{"id":"a"}`), action, nil); err != nil {
			t.Fatalf("decoded program cannot encode: %v", err)
		}
	})
}
