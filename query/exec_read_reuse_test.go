package query

import (
	"reflect"
	"testing"

	"github.com/thesyncim/vibejson"
)

func TestExecReadReuseScrubsCapacityAndBoundsRetention(t *testing.T) {
	s := &fileSmallScan{}
	s.row = s.appendRow
	s.work.raws = make([][]vibejson.RawValue, 1, 3)
	s.work.ctx.values = make([][]scalar, 1, 3)
	for i := range 3 {
		s.work.raws[:3][i] = make([]vibejson.RawValue, 1, 4)
		s.work.ctx.values[:3][i] = make([]scalar, 1, 4)
		for j := range 4 {
			s.work.raws[:3][i][:4][j].Src = []byte("borrowed")
			s.work.ctx.values[:3][i][:4][j] = scalar{raw: []byte("borrowed"), sval: "borrowed"}
		}
	}
	s.work.text = []byte("decoded secret")
	s.work.lateText = []byte("late secret")
	s.batch.data = []byte("borrowed batch")
	s.p = &plan{}
	s.work.cancel = &CancelFlag{}
	e := Exec{file: fileWorkspace{small: s}}
	s.e = &e
	want := smallReadReuseBytes(s)
	e.ResetReadForReuse(want)
	if e.file.small != s || e.ReadReuseCapacityBytes() != want {
		t.Fatal("bounded reset dropped or miscounted scratch")
	}
	if s.p != nil || s.e != nil || s.batch.data != nil || s.work.cancel != nil {
		t.Fatal("reset retained request state")
	}
	for _, col := range s.work.raws[:cap(s.work.raws)] {
		for _, value := range col[:cap(col)] {
			if value.Src != nil {
				t.Fatal("raw column retained a borrowed tail")
			}
		}
	}
	for _, col := range s.work.ctx.values[:cap(s.work.ctx.values)] {
		for _, value := range col[:cap(col)] {
			if !reflect.DeepEqual(value, scalar{}) {
				t.Fatal("scalar column retained a borrowed tail")
			}
		}
	}
	for _, buf := range [][]byte{s.work.text, s.work.lateText} {
		for _, b := range buf[:cap(buf)] {
			if b != 0 {
				t.Fatal("decoded text retained secret bytes")
			}
		}
	}
	e.ResetReadForReuse(want - 1)
	if e.file.small != nil || e.ReadReuseCapacityBytes() != 0 {
		t.Fatal("oversized scratch retained")
	}
}

func TestExecReadReuseRawFallbackAndChangingBindings(t *testing.T) {
	const sql = `SELECT id, score FROM docs WHERE id = ?`
	statement, err := PrepareStatement(sql)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var exec Exec
	defer exec.Release()
	for _, doc := range []string{
		`{"id":1,"score":"first"}`, `{"id":2,"score":"miss"}`,
		`{"id":1,"sc\u006fre":"escaped key"}`, `null`,
		`{"id":1,"score":"escaped\nvalue"}`, `{"id":1,"score":9}`,
	} {
		var raw ValidatedRawSource
		raw.Bind([]byte(doc))
		got, err := statement.RunInto(&exec, FromValidatedRaw(&raw), []any{int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		actual := cursorKey(statement, got)
		fresh, err := PrepareStatement(sql)
		if err != nil {
			t.Fatal(err)
		}
		var reference Exec
		want, err := fresh.RunInto(&reference, FromSegment(mustSegment(t, doc)), []any{int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		if expected := cursorKey(fresh, want); actual != expected {
			t.Fatalf("%s: got %q, want %q", doc, actual, expected)
		}
		reference.Release()
		fresh.Release()
		exec.ResetReadForReuse(128 << 10)
		if _, ok := statement.ResetReadBindingsForReuse(); !ok {
			t.Fatal("statement reset declined")
		}
	}
}
