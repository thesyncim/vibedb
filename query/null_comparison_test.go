package query

import "testing"

func TestNullSafeComparisonWarmExecution(t *testing.T) {
	statement, err := PrepareStatement(`SELECT CASE WHEN n IS NOT DISTINCT FROM ? THEN TRUE ELSE FALSE END FROM docs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromSegment(mustSegment(t, `{"id":1}`, `{"id":2,"n":null}`, `{"id":3,"n":0}`))
	args := []any{nil}
	var exec Exec
	defer exec.Release()
	exec.Options.Workers = 1
	run := func() {
		cursor, err := statement.RunInto(&exec, source, args)
		if err != nil {
			panic(err)
		}
		ordinal := 0
		for cursor.Next() {
			want := "true"
			if ordinal == 2 {
				want = "false"
			}
			if string(cursor.Cell(0).JSON()) != want {
				panic("incorrect null-safe result")
			}
			ordinal++
		}
		if ordinal != 3 {
			panic("missing null-safe rows")
		}
	}
	run()
	run()
	if allocations := testing.AllocsPerRun(100, run); allocations != 0 {
		t.Fatalf("warm allocations=%g", allocations)
	}
}
