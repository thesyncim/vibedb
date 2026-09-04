package distribution

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibejson"
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

func TestDocumentPointProgramDigestBindsOrderedPlacementIdentity(t *testing.T) {
	first, err := CompileDocumentPointProgram(
		[]string{"/tenant", "/sequence"}, DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	again, err := CompileDocumentPointProgram(
		[]string{"/tenant", "/sequence"}, DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	reordered, err := CompileDocumentPointProgram(
		[]string{"/sequence", "/tenant"}, DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	widerBuckets, err := CompileDocumentPointProgram(
		[]string{"/tenant", "/sequence"}, DefaultVirtualBucketBits+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest() == ([32]byte{}) || first.Digest() != again.Digest() ||
		first.Digest() == reordered.Digest() || first.Digest() == widerBuckets.Digest() {
		t.Fatalf(
			"placement digests = %x / %x / %x / %x",
			first.Digest(), again.Digest(), reordered.Digest(), widerBuckets.Digest(),
		)
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

func TestDocumentPointProgramReleasesBorrowedScalarsOnEveryExit(t *testing.T) {
	program, err := CompileDocumentPointProgram(
		[]string{"/tenant", "/sequence"}, DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	var workspace DocumentPointWorkspace
	assertReleased := func(stage string) {
		t.Helper()
		for ordinal := range workspace.scalars {
			if workspace.scalars[ordinal] != (Scalar{}) {
				t.Fatalf("%s retained borrowed scalar %d: %+v", stage, ordinal, workspace.scalars[ordinal])
			}
		}
	}
	success := []byte(`{"tenant":"borrowed-success","sequence":7}`)
	if _, err := program.Point(success, &workspace); err != nil {
		t.Fatal(err)
	}
	assertReleased("success")
	lateError := []byte(`{"tenant":"borrowed-before-error","sequence":{}}`)
	if _, err := program.Point(lateError, &workspace); !errors.Is(err, ErrDocumentPoint) {
		t.Fatalf("late scalar error=%v", err)
	}
	assertReleased("late error")
}

func TestDocumentPointProgramReusesIndexAndValidatesGrowth(t *testing.T) {
	program, err := CompileDocumentPointProgram([]string{"/tenant", "/region"}, DefaultVirtualBucketBits)
	if err != nil {
		t.Fatal(err)
	}
	want, err := program.mapper.PointFor([]Scalar{NewString("acme"), NewString("eu-west")})
	if err != nil {
		t.Fatal(err)
	}
	var workspace DocumentPointWorkspace
	for _, source := range []string{
		`{"tenant":"acme","region":"eu-west"}`,
		`{"tenant":"old","region":"eu\u002dwest","payload":[1,{"nested":true}],"tenant":"acme"}`,
		`{"tenant":"acme","region":"eu-west"}`,
	} {
		got, err := program.Point([]byte(source), &workspace)
		if err != nil || got != want {
			t.Fatalf("Point(%s) = %x, %v; want %x", source, got, err, want)
		}
	}
	retained := &workspace.entries[0]
	// BuildIndex consumes capacity, so reuse must also work with zero length.
	workspace.entries = workspace.entries[:0]
	if _, err := program.Point([]byte(`{"tenant":"acme","region":"eu-west"}`), &workspace); err != nil {
		t.Fatal(err)
	}
	if &workspace.entries[:cap(workspace.entries)][0] != retained {
		t.Fatal("warm call replaced sufficient index storage")
	}
	for _, source := range []string{
		`{"tenant":"acme","region":"eu-west"} garbage`,
		`{"tenant":"acme","region":"eu-west","payload":[` + strings.Repeat("0,", 100) + `]}`,
		`{"tenant":"acme","region":"eu-west","payload":` + strings.Repeat("[", vibejson.DefaultMaxDepth+1) + `0` + strings.Repeat("]", vibejson.DefaultMaxDepth+1) + `}`,
	} {
		for _, cold := range []bool{false, true} {
			if cold {
				workspace = DocumentPointWorkspace{}
			}
			if _, err := program.Point([]byte(source), &workspace); !errors.Is(err, ErrDocumentPoint) {
				t.Fatalf("Point invalid input (cold=%v) error = %v", cold, err)
			}
		}
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
