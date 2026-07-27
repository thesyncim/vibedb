package query

import (
	"testing"
	"unsafe"

	"github.com/thesyncim/vibejson"
)

// Given the struct sizes the spill accounting charges, when the layouts
// change, then the constants must be updated with them.
//
// file_execute.go keeps these as constants rather than unsafe.Sizeof so the
// package carries no unsafe scope of its own. That trade only holds while
// something checks them, which is this: a field added to scalar or fileRow
// would otherwise silently under-budget the spill threshold, and the threshold
// is what keeps a bounded execution inside its configured memory target.
func TestSpillAccountingStructSizes(t *testing.T) {
	if got, want := int64(unsafe.Sizeof(scalar{})), int64(scalarStructBytes); got != want {
		t.Fatalf("unsafe.Sizeof(scalar{}) = %d, but scalarStructBytes is %d; "+
			"update the constant in file_execute.go", got, want)
	}
	if got, want := int64(unsafe.Sizeof(fileRow{})), int64(fileRowStructBytes); got != want {
		t.Fatalf("unsafe.Sizeof(fileRow{}) = %d, but fileRowStructBytes is %d; "+
			"update the constant in file_execute.go", got, want)
	}
}

// classifyValue classifies one JSON document's root the way the executor does,
// returning the scalar the spill path would be handed.
func classifyValue(t testing.TB, src string, scratch *[]byte) scalar {
	t.Helper()
	raw, ok, err := vibejson.GetRaw([]byte(src), "")
	if err != nil || !ok {
		t.Fatalf("GetRaw(%s) = (%v,%v)", src, ok, err)
	}
	return classifyRawInto(raw, scratch)
}

// detach packs s exactly the way a projected row does: scalarOwnBytes sizes an
// arena, ownScalarInto fills it. Running both is the point — the packing pass
// hands out views into an arena that must never be grown, so a sizing pass that
// disagreed with it by even one byte would repoint scalars already returned for
// earlier columns of the same row. Nothing else in the package can catch that,
// because the overrun stays silent whenever the arena happens to have spare
// capacity.
func detach(t testing.TB, s scalar) scalar {
	t.Helper()
	need := scalarOwnBytes(s)
	arena := make([]byte, 0, max(need, 1))
	arena, out := ownScalarInto(arena, s)
	if len(arena) != need {
		t.Fatalf("packed %d bytes into an arena sized for %d", len(arena), need)
	}
	return out
}

// Given each classified scalar shape, when it is detached, then every field
// keeps its content and none still points into the source.
//
// Detaching copies raw once and re-derives num and sval where they aliased it,
// which is what removes the duplicate copies of a number's digits and an
// unescaped string's content. This checks those aliasing tests against real
// classifier output rather than against hand-built scalars, including the
// escaped forms where sval genuinely needs its own copy.
func TestOwnScalarPreservesContentAndDetaches(t *testing.T) {
	sources := []string{
		`42`,               // number: num and raw are one slice
		`-1.5e3`,           // number with a fractional spelling
		`9007199254740993`, // integer past float64's mantissa
		`"plain"`,          // unescaped string: sval points inside raw
		`"esc\"aped"`,      // escaped string: sval lives in the arena
		`"éè"`,             // escaped string that shrinks substantially
		`""`,               // empty string, the len(sval)+2 boundary
		`true`,
		`null`,
		`{"a":[1,2]}`, // container: raw only
		`[1,2,3]`,
	}
	for _, src := range sources {
		t.Run(src, func(t *testing.T) {
			var scratch []byte
			before := classifyValue(t, src, &scratch)
			// Capture the field contents before detaching, so the comparison
			// reads the original storage rather than anything ownScalar wrote.
			wantRaw, wantNum, wantSval := string(before.raw), string(before.num), before.sval

			after := detach(t, before)

			if after.kind != before.kind || after.isInt != before.isInt ||
				after.ival != before.ival || after.bval != before.bval {
				t.Fatalf("scalar tag changed: %+v -> %+v", before, after)
			}
			if string(after.raw) != wantRaw {
				t.Fatalf("raw = %q, want %q", after.raw, wantRaw)
			}
			if string(after.num) != wantNum {
				t.Fatalf("num = %q, want %q", after.num, wantNum)
			}
			if after.sval != wantSval {
				t.Fatalf("sval = %q, want %q", after.sval, wantSval)
			}

			// Detached: nothing may still point into the classified source.
			if len(after.raw) != 0 && len(before.raw) != 0 && &after.raw[0] == &before.raw[0] {
				t.Fatal("raw still points into the source document")
			}
			if len(after.num) != 0 && len(before.num) != 0 && &after.num[0] == &before.num[0] {
				t.Fatal("num still points into the source document")
			}

			// A number's digits must ride the single raw copy rather than a
			// second span of the arena.
			if after.kind == kindNumber && len(after.num) != 0 && &after.num[0] != &after.raw[0] {
				t.Fatal("a number's num was copied separately from its raw bytes")
			}

			// The charge must cover the struct and every byte actually held.
			if got, floor := scalarBytes(after), int64(scalarStructBytes)+int64(len(after.raw)); got < floor {
				t.Fatalf("scalarBytes = %d, below the struct plus its raw bytes (%d)", got, floor)
			}
		})
	}
}

// Given an unescaped string and an escaped one, when both are detached, then
// only the escaped one pays for a second copy — the saving the aliasing test
// exists to capture.
func TestOwnScalarChargesOneCopyForUnescapedStrings(t *testing.T) {
	var scratch []byte
	plain := detach(t, classifyValue(t, `"aaaaaaaa"`, &scratch))
	escaped := detach(t, classifyValue(t, `"aaaaaaa\n"`, &scratch))

	if got, want := scalarBytes(plain), int64(scalarStructBytes)+int64(len(plain.raw)); got != want {
		t.Fatalf("unescaped string charged %d, want %d: its content should ride the raw copy", got, want)
	}
	if got, floor := scalarBytes(escaped), int64(scalarStructBytes)+int64(len(escaped.raw)); got <= floor {
		t.Fatalf("escaped string charged %d, want more than %d: its decoded form is a separate copy",
			got, floor)
	}
}
