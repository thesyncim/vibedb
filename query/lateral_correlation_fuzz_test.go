package query

import (
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func FuzzLateralInheritedFrameResolution(f *testing.F) {
	f.Add(uint8(1), int8(0), "id", false)
	f.Add(uint8(3), int8(2), "nested.value", true)
	f.Fuzz(func(t *testing.T, rawDepth uint8, rawSource int8, key string, mismatch bool) {
		depth := int(rawDepth%8) + 1
		source := int(rawSource)
		segments := []sqlast.Segment{{Key: key}}
		parent := &statementLateral{
			spec: &sqlast.LateralSpec{Bindings: []sqlast.LateralBinding{{
				Depth: depth, Source: source, Segments: segments,
			}}},
			bindingUse:   []bool{false},
			slots:        []lateralBindingSlot{{value: scalar{kind: kindNumber, num: []byte("1")}}},
			bindingReady: true,
		}
		child := sqlast.LateralBinding{
			Depth: depth + 1, Source: source, Segments: segments,
		}
		if mismatch {
			child.Source++
		}
		resolved, err := (&lateralPrepareFrame{apply: parent}).resolve("SELECT", &child)
		if mismatch {
			if err == nil {
				t.Fatal("mismatched lexical binding resolved")
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		got, err := resolved.scalar()
		if err != nil || got.kind != kindNumber || string(got.num) != "1" ||
			!parent.bindingUse[0] {
			t.Fatalf("resolved inherited scalar/use = %+v/%v/%t",
				got, err, parent.bindingUse[0])
		}
	})
}
