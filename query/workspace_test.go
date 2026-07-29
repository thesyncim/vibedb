package query

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson/document"
)

func TestCellCompactTaggedLayout(t *testing.T) {
	if got := unsafe.Sizeof(Cell{}); got > 56 {
		t.Fatalf("Cell occupies %d bytes, want at most 56", got)
	}

	cases := []struct {
		name string
		cell Cell
		json string
	}{
		{"integer", cellFromScalar(scalar{kind: kindNumber, num: []byte("9007199254740993"), isInt: true, ival: 9007199254740993}), "9007199254740993"},
		{"float", cellFromScalar(scalar{kind: kindNumber, num: []byte("1.25")}), "1.25"},
		{"true", cellFromScalar(scalar{kind: kindBool, bval: true}), "true"},
		{"false", cellFromScalar(scalar{kind: kindBool}), "false"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got := test.cell.AppendJSON(make([]byte, 0, len(test.json)))
			if !bytes.Equal(got, []byte(test.json)) {
				t.Fatalf("AppendJSON = %q, want %q", got, test.json)
			}
		})
	}
	if got, ok := cases[0].cell.Int64(); !ok || got != 9007199254740993 {
		t.Fatalf("large integer = (%d,%v)", got, ok)
	}
	if got, ok := cases[1].cell.Float64(); !ok || got != 1.25 {
		t.Fatalf("float = (%g,%v)", got, ok)
	}

	computed := []struct {
		cell Cell
		want string
	}{
		{Cell{kind: KindNumber, flag: cellInteger, word: uint64(9007199254740993)}, "9007199254740993"},
		{Cell{kind: KindNumber, word: math.Float64bits(12.5)}, "12.5"},
	}
	buf := make([]byte, 0, 32)
	for _, test := range computed {
		if got := string(test.cell.AppendJSON(buf[:0])); got != test.want {
			t.Fatalf("computed AppendJSON = %q, want %q", got, test.want)
		}
		if got := string(test.cell.JSON()); got != test.want {
			t.Fatalf("computed JSON = %q, want %q", got, test.want)
		}
	}
}

func TestCellAppendJSONAllocs(t *testing.T) {
	cell := Cell{kind: KindNumber, word: math.Float64bits(12.5)}
	buf := make([]byte, 0, 32)
	allocs := testing.AllocsPerRun(100, func() {
		buf = cell.AppendJSON(buf[:0])
	})
	if allocs != 0 {
		t.Fatalf("computed AppendJSON allocated %.2f times, want 0", allocs)
	}
}

// TestRunIntoSteadyAllocs closes the whole-query allocation contract over the
// executor's materially different paths. The warm run may grow caller-owned
// buffers; an identical pass must then reuse every byte of scan, predicate,
// containment, sort, group, aggregate, decoded-text, and result storage.
func TestRunIntoSteadyAllocs(t *testing.T) {
	docs := make([][]byte, 512)
	for i := range docs {
		docs[i] = fmt.Appendf(nil,
			`{"id":%d,"bucket":%d,"name":"item-\u%04x","score":%d,"tags":["x","g%d"],"obj":{"x":%d,"live":true}}`,
			i, i%17, 'a'+i%26, i*3, i%7, i%3)
	}
	set := &store.Segment{
		Options:    document.IndexOptions{HashKeys: true},
		ShapeTapes: true,
		Postings:   true,
		ValueDict:  true,
	}
	for _, doc := range docs {
		if _, err := set.Append(doc); err != nil {
			t.Fatal(err)
		}
	}

	queries := map[string]*Query{
		"projection": Select(Path("id"), Path("name"), Path("obj")),
		"order":      Select(Path("id"), Path("name")).OrderBy("name", Asc).OrderBy("id", Desc).Limit(100),
		"posting":    Select(Path("id"), Path("name")).Where(Cmp("bucket", Eq, 3)),
		"contains":   Select(Path("id")).Where(Contains("obj", `{"x":1}`)),
		"aggregate":  Select(Count(), Sum("score"), Avg("score"), Min("score"), Max("score")).Where(Exists("score")),
		"group":      Select(Path("bucket"), Count(), Sum("score")).GroupBy("bucket").OrderBy("bucket", Desc),
		"text-group": Select(Path("name"), Count()).GroupBy("name").OrderBy("name", Asc).Limit(20),
	}

	decoded := decodeDocs(t, docs)
	for name, q := range queries {
		t.Run(name, func(t *testing.T) {
			var e Exec
			if err := q.RunInto(&e, FromSegment(set)); err != nil {
				t.Fatal(err)
			}
			if diff := compareResults(e.Result, referenceRun(t, q, decoded)); diff != "" {
				t.Fatal(diff)
			}

			allocs := testing.AllocsPerRun(25, func() {
				if err := q.RunInto(&e, FromSegment(set)); err != nil {
					panic(err)
				}
			})
			if allocs != 0 {
				t.Fatalf("warmed RunInto allocated %.2f times, want 0", allocs)
			}
		})
	}
}

func TestRunIntoTextBytes(t *testing.T) {
	set := buildSegment(t, [][]byte{[]byte(`{"s":"clean"}`), []byte(`{"s":"es\u0063aped"}`)}, storageMode{"", true, true})
	q := Select(Path("s"))
	var e Exec
	if err := q.RunInto(&e, FromSegment(set)); err != nil {
		t.Fatal(err)
	}
	want := []string{"clean", "escaped"}
	for i, cell := range e.Result.Columns[0].Cells {
		got, ok := cell.TextBytes()
		if !ok || string(got) != want[i] {
			t.Fatalf("row %d TextBytes = %q, %v; want %q", i, got, ok, want[i])
		}
		text, ok := cell.Text()
		if !ok || text != want[i] {
			t.Fatalf("row %d Text = %q, %v; want %q", i, text, ok, want[i])
		}
	}
}
