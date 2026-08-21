package distribution

import (
	"errors"
	"testing"
)

func TestDocumentPointProgramMatchesNativeTuple(t *testing.T) {
	program, err := CompileDocumentPointProgram(
		[]string{"/tenant", "/region", "/sequence"}, DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"tenant":"acme","region":"eu\u002dwest","sequence":5.00}`)
	var workspace DocumentPointWorkspace
	got, err := program.Point(document, &workspace)
	if err != nil {
		t.Fatal(err)
	}
	number, err := NewNumber("5.00")
	if err != nil {
		t.Fatal(err)
	}
	want, err := NewNativeMapperWithBucketBits(3, DefaultVirtualBucketBits).PointFor([]Scalar{
		NewString("acme"), NewString("eu-west"), number,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("point = %x, want %x", got, want)
	}
	if program.Arity() != 3 {
		t.Fatalf("arity = %d", program.Arity())
	}
}

func TestDocumentPointProgramRejectsIncompleteOrNonscalarKeys(t *testing.T) {
	program, err := CompileDocumentPointProgram(
		[]string{"/tenant", "/region"}, DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, document := range [][]byte{
		[]byte(`{"tenant":"acme"}`),
		[]byte(`{"tenant":"acme","region":null}`),
		[]byte(`{"tenant":"acme","region":["west"]}`),
		[]byte(`{"tenant":"acme","region":`),
	} {
		var workspace DocumentPointWorkspace
		if _, err := program.Point(document, &workspace); !errors.Is(err, ErrDocumentPoint) {
			t.Fatalf("Point(%q) error = %v", document, err)
		}
	}
}

func TestDocumentPointProgramAllocatesZeroWhenWarm(t *testing.T) {
	program, err := CompileDocumentPointProgram(
		[]string{"/tenant", "/region", "/sequence"}, DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"tenant":"acme","region":"eu\u002dwest","sequence":5.00,"payload":"kept out of placement"}`)
	var workspace DocumentPointWorkspace
	if _, err := program.Point(document, &workspace); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1_000, func() {
		if _, err := program.Point(document, &workspace); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warm document point allocations = %v, want 0", allocs)
	}
}

func BenchmarkDocumentPointProgram(b *testing.B) {
	program, err := CompileDocumentPointProgram(
		[]string{"/tenant", "/region", "/sequence"}, DefaultVirtualBucketBits,
	)
	if err != nil {
		b.Fatal(err)
	}
	document := []byte(`{"tenant":"acme","region":"eu-west","sequence":5.00,"payload":"split once"}`)
	var workspace DocumentPointWorkspace
	if _, err := program.Point(document, &workspace); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(document)))
	b.ResetTimer()
	for range b.N {
		if _, err := program.Point(document, &workspace); err != nil {
			b.Fatal(err)
		}
	}
}
