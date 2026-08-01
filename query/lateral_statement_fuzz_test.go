package query

import (
	"bytes"
	"fmt"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson"
)

func FuzzLateralComparisonReversal(f *testing.F) {
	for _, op := range []sqlast.CmpOp{
		sqlast.OpEq, sqlast.OpNe, sqlast.OpLt, sqlast.OpLe,
		sqlast.OpGt, sqlast.OpGe,
	} {
		f.Add(uint8(op), int64(-1), int64(2))
	}
	f.Fuzz(func(t *testing.T, raw uint8, left, right int64) {
		op := sqlast.CmpOp(raw % 6)
		got := acceptSign(compareScalar(
			joinNumberScalar([]byte(fmt.Sprint(right))),
			joinNumberScalar([]byte(fmt.Sprint(left))),
		), Op(lateralReverseComparison(op)))
		want := acceptSign(compareScalar(
			joinNumberScalar([]byte(fmt.Sprint(left))),
			joinNumberScalar([]byte(fmt.Sprint(right))),
		), Op(op))
		if got != want {
			t.Fatalf("reverse(%v, %d, %d) = %v, want %v", op, left, right, got, want)
		}
	})
}

func FuzzLateralDecodedJSONStringAccounting(f *testing.F) {
	for _, seed := range []string{
		`"plain"`, `"\u0061"`, `"\ud83d\ude00"`, `"a\n\t\\\"z"`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, src string) {
		raw := vibejson.RawValue{Src: []byte(src)}
		want, ok, err := raw.AppendText(nil)
		if err != nil || !ok {
			t.Skip()
		}
		size, err := lateralDecodedJSONStringBytes([]byte(src), nil)
		if err != nil {
			t.Fatal(err)
		}
		got, err := lateralAppendDecodedJSONString(
			make([]byte, 0, size), []byte(src), nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != size || !bytes.Equal(got, want) {
			t.Fatalf("decoded %q = %q/%d, want %q/%d", src, got, size, want, len(want))
		}
	})
}

func FuzzLateralOuterGateExactComparison(f *testing.F) {
	for _, op := range []sqlast.CmpOp{
		sqlast.OpEq, sqlast.OpNe, sqlast.OpLt, sqlast.OpLe,
		sqlast.OpGt, sqlast.OpGe,
	} {
		f.Add(uint8(op), int64(-10), int64(2), uint8(0), false)
		f.Add(uint8(op), int64(1), int64(1), uint8(1), true)
	}
	f.Fuzz(func(
		t *testing.T,
		raw uint8,
		left, right int64,
		nulls uint8,
		negated bool,
	) {
		op := sqlast.CmpOp(raw % 6)
		leftScalar := joinNumberScalar([]byte(fmt.Sprint(left)))
		rightScalar := joinNumberScalar([]byte(fmt.Sprint(right)))
		if nulls&1 != 0 {
			leftScalar = scalar{kind: kindNull}
		}
		if nulls&2 != 0 {
			rightScalar = scalar{kind: kindNull, raw: nullBytes}
		}
		lateral := statementLateral{
			slots: []lateralBindingSlot{{value: leftScalar}, {value: rightScalar}},
		}
		gate := lateralGateExpr{
			kind: sqlast.ExprCompare, op: op, left: 0, right: 1,
			negated: negated,
		}
		got, err := lateral.evalGate(new(statementRelationJoin), &gate, nil)
		if err != nil {
			t.Fatal(err)
		}
		want := triUnknown
		if leftScalar.kind != kindNull && rightScalar.kind != kindNull {
			want = boolTri(acceptSign(compareScalar(leftScalar, rightScalar), Op(op)))
		}
		if negated {
			want = notTri(want)
		}
		if got != want {
			t.Fatalf("gate(%d %v %d, nulls=%d, not=%t) = %d, want %d",
				left, op, right, nulls, negated, got, want)
		}
	})
}
