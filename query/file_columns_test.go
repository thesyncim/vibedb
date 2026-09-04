package query

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

func fileColumnTestPlan(t testing.TB) *plan {
	t.Helper()
	p, err := Select(Path("a"), Path("b"), Path("c"), Sum("a"), Sum("b")).GroupBy("a", "b", "c").Where(Cmp("c", Ne, false)).compiled()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func checkFileColumns(t testing.TB, p *plan, w *fileWorkerWorkspace, docs ...string) bool {
	t.Helper()
	var batch fileBatch
	var segment store.Segment
	for _, doc := range docs {
		batch.data = append(batch.data, doc...)
		batch.ends = append(batch.ends, len(batch.data))
		if _, err := segment.Append([]byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	var reference Workspace
	defer reference.Release()
	reference.ctx.s, reference.ctx.rows = &segment, len(docs)
	if err := reference.ctx.extract(p, nil, &reference); err != nil {
		t.Fatal(err)
	}
	handled, err := w.extractFileColumns(p, batch)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		return false
	}
	if !reflect.DeepEqual(w.ctx.values, reference.ctx.values) {
		t.Fatalf("values differ for %q:\n got %#v\nwant %#v", docs, w.ctx.values, reference.ctx.values)
	}
	if !reflect.DeepEqual(w.ctx.nums, reference.ctx.nums) {
		t.Fatalf("numbers differ for %q:\n got %#v\nwant %#v", docs, w.ctx.nums, reference.ctx.nums)
	}
	return true
}

func TestFileColumnsDifferential(t *testing.T) {
	p := fileColumnTestPlan(t)
	var w fileWorkerWorkspace
	defer w.Release()
	docs := []string{
		`{"a":1,"b":2,"c":true}`,
		`{"a":1,"a":null,"b":"x","b":3,"c":"line\n\u1234"}`,
		`{"a":2,"a":{},"b":1,"b":false,"c":[{"c":2},"\"}"]}`,
		`{"a":9007199254740993,"b":9223372036854775808,"c":-9223372036854775808}`,
		`{"a":1e1000,"b":-0.00100,"c":-0}`,
		`{"ignored":{"a":99},"c":"\\\"escaped"}`,
		`{"a":[],"b":{"b":2},"c":null}`,
		`{}`, `[]`, `[1,{"a":2}]`, `42`, `"text"`, `true`, `null`,
		`{"a":"` + strings.Repeat(`\u1234`, 4096) + `","b":"tail\n","c":"head\t"}`,
	}
	for _, q := range []*Query{
		Select(Sum("a"), Sum("b")),
		Select(Path("a"), Sum("a")).GroupBy("a"),
		Select(Path("a"), Path("b"), Sum("a"), Sum("b")).GroupBy("a", "b"),
	} {
		tiny, err := q.compiled()
		if err != nil {
			t.Fatal(err)
		}
		if !checkFileColumns(t, tiny, &w, docs...) {
			t.Fatal("tiny directory fell back")
		}
	}
	for range 3 {
		if !checkFileColumns(t, p, &w, docs...) {
			t.Fatal("ordinary fields fell back")
		}
		if !checkFileColumns(t, p, &w, `{}`) {
			t.Fatal("empty object fell back")
		}
		for _, doc := range []string{`{"a":1,"\u0061":2}`, `{"\u0062":4}`, `{"irrelevant\n":4}`} {
			if checkFileColumns(t, p, &w, docs[0], doc) {
				t.Fatal("escaped key did not fall back")
			}
		}
	}
	for _, path := range []string{"a.b", "/a", ""} {
		q, err := Select(Path(path)).compiled()
		if err != nil {
			t.Fatal(err)
		}
		if handled, err := w.extractFileColumns(q, fileBatch{}); handled || err != nil {
			t.Fatalf("path %q: handled=%v err=%v", path, handled, err)
		}
	}
}

func TestFileColumnsCancellation(t *testing.T) {
	p := fileColumnTestPlan(t)
	var cancel CancelFlag
	cancel.Cancel()
	w := fileWorkerWorkspace{Workspace: Workspace{cancel: &cancel}}
	defer w.Release()
	if handled, err := w.extractFileColumns(p, fileBatch{data: []byte(`{}`), ends: []int{2}}); !handled || !errors.Is(err, ErrCanceled) {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
}

func TestFileColumnsWideDirectory(t *testing.T) {
	var w fileWorkerWorkspace
	defer w.Release()
	for _, width := range []int{1, 16, 129, 2} {
		cols := make([]Column, width)
		var doc strings.Builder
		doc.WriteByte('{')
		for i := range width {
			cols[i] = Path(fmt.Sprintf("field-%04d", i))
			if i > 0 {
				doc.WriteByte(',')
			}
			fmt.Fprintf(&doc, `"field-%04d":%d`, width-i-1, i)
		}
		doc.WriteByte('}')
		p, err := Select(cols...).compiled()
		if err != nil {
			t.Fatal(err)
		}
		if !checkFileColumns(t, p, &w, doc.String(), `{}`) {
			t.Fatalf("width %d fell back", width)
		}
	}
}

func TestFileColumnsDecodedTextAdmission(t *testing.T) {
	p := fileColumnTestPlan(t)
	doc := []byte(`{"a":"one\ntwo","c":"three\tfour"}`)
	batch := fileBatch{data: doc, ends: []int{len(doc)}}
	var w fileWorkerWorkspace
	defer w.Release()
	w.heapWorkBudget.begin(1)
	if handled, err := w.extractFileColumns(p, batch); !handled || err == nil {
		t.Fatalf("decoded text exceeded budget: handled=%v err=%v", handled, err)
	}
	// An escaped field in the final row must fall back before any phase
	// admits decoded text, including text gathered from preceding rows.
	w.heapWorkBudget.begin(1)
	batch.data = append(batch.data, `{"\u0061":"tail\n"}`...)
	batch.ends = append(batch.ends, len(batch.data))
	if handled, err := w.extractFileColumns(p, batch); handled || err != nil {
		t.Fatalf("escaped key: handled=%v err=%v", handled, err)
	}
	if used := w.heapWorkBudget.used.Load(); used != 0 {
		t.Fatalf("fallback charged %d decoded-text bytes", used)
	}
}

func FuzzFileColumnsDifferential(f *testing.F) {
	for _, doc := range []string{`{}`, `{"a":1,"b":2,"c":true}`, `{"a":null,"a":3}`, `{"c":"a\nb"}`, `[]`, `{"a":{"a":[1,2]}}`} {
		f.Add(doc)
	}
	f.Fuzz(func(t *testing.T, doc string) {
		if !vibejson.Valid([]byte(doc)) {
			t.Skip()
		}
		var w fileWorkerWorkspace
		defer w.Release()
		checkFileColumns(t, fileColumnTestPlan(t), &w, doc)
	})
}
