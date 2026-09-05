package query

import (
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// Put each duplicate case first in its own covered range. An unsupported row
// elsewhere must not hide a wrong answer by forcing the entire query to fall
// back before it reaches the duplicate.
func TestFileProjectionDuplicateReview(t *testing.T) {
	cases := []struct {
		name, fields, path string
		projected          bool
	}{
		{"scalar", `"value":1,"value":2`, "/value", true},
		{"escaped-scalar-key", `"value":1,"\u0076alue":3`, "/value", true},
		{"container-then-scalar", `"value":{"a":1,"b":2},"value":3`, "/value", true},
		{"scalar-then-container", `"value":3,"value":{"a":1,"b":2}`, "/value", false},
		{"ancestor", `"parent":{"x":1},"parent":{"x":2}`, "/parent/x", true},
		{"ancestor-removes-field", `"parent":{"x":1},"parent":{}`, "/parent/x", false},
		{"ancestor-changes-type", `"parent":{"x":1},"parent":4`, "/parent/x", false},
		{"ancestor-restores-type", `"parent":4,"parent":{"x":1}`, "/parent/x", true},
		{"nested-duplicates", `"parent":{"x":1,"x":2},"parent":{"x":3,"x":4}`, "/parent/x", true},
		{"escaped-slash", `"a/b":1,"a\/b":2`, "/a~1b", true},
		{"scalar-then-null", `"value":3,"value":null`, "/value", true},
		{"null-then-scalar", `"value":null,"value":3`, "/value", true},
	}
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for row, test := range cases {
		key := fmt.Sprintf("%04d", row)
		if err := builder.Append(key, fmt.Appendf(nil, `{"id":%q,%s}`, key, test.fields)); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "duplicate-projection-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := durable.CreateFromPrimary(built, file, durable.Options{}); err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Open(file, durable.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	reference := projectedReviewHeap(t, snapshot)
	var execution Exec
	defer execution.Release()
	for row, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			lower, upper := fmt.Sprintf("%04d", row), fmt.Sprintf("%04d", row+1)
			statement := Select(Path(test.path)).Where(And(Cmp("/id", Ge, lower), Cmp("/id", Lt, upper))).OrderBy("/id", Asc).Limit(1)
			want, err := statement.Run(FromSegment(reference))
			if err != nil {
				t.Fatal(err)
			}
			span := NewFileRangeSource([]byte(lower), []byte(upper), false)
			span.BindPrimaryOrder("/id")
			span.BindPrimaryPredicate("/id")
			if err := statement.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
				t.Fatal(err)
			}
			projectedReviewEqual(t, execution.Result, want)
			if test.projected && execution.Stats.ProjectedRows != 1 {
				t.Fatalf("scalar duplicate fell back instead of resolving the last value: %+v", execution.Stats)
			}
		})
	}
}
