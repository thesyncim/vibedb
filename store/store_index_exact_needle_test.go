package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibejson/document"
)

// AppendIndexRawKeys no longer counts a needle's entries before indexing it:
// the one-entry storage it builds into is itself the scalar test, and
// ErrIndexFull is what a non-scalar reports. That folds two verdicts —
// "not a scalar" and "not valid JSON at all" — into one error, so the
// rejecting path re-validates to tell them apart. These tests pin both halves
// of that split, which is the only place the change is observable.

func indexedScalarCollection(t *testing.T) *Collection {
	t.Helper()
	collection := &Collection{Options: Options{ChunkDocuments: 4}}
	if _, err := collection.Put("a", []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	info, err := collection.CreateIndex(IndexDefinition{Name: "v", Paths: []string{"/v"}})
	if err != nil {
		t.Fatal(err)
	}
	for info.State != IndexReady {
		if info, err = collection.BackfillIndex("v", 2); err != nil {
			t.Fatal(err)
		}
	}
	return collection
}

// TestIndexRawKeysScalarVerdicts holds the acceptance boundary exactly where
// the entry count used to draw it: a needle whose tape is one entry wide gets
// past the width gate, and everything wider is refused there.
//
// The empty containers are the interesting rows. Their tape is one entry, so
// the width gate admits them exactly as the counting form did, and the
// container refusal they end in comes from the probe below it. Keeping them
// here pins that the refusal still comes from that layer and not from a width
// gate that quietly moved.
func TestIndexRawKeysScalarVerdicts(t *testing.T) {
	collection := indexedScalarCollection(t)
	for _, needle := range []string{`1`, `"a"`, `null`, `true`, `1.5e3`} {
		if _, err := collection.IndexRawKeys("v", []byte(needle)); err != nil {
			t.Errorf("one-entry needle %s rejected: %v", needle, err)
		}
	}
	for _, needle := range []string{`{}`, `[]`, `[1]`, `[1,2]`, `{"a":1}`, `[[]]`, `[{}]`} {
		if _, err := collection.IndexRawKeys("v", []byte(needle)); !errors.Is(err, ErrIndexScalar) {
			t.Errorf("container needle %s error = %v, want %v", needle, err, ErrIndexScalar)
		}
	}
}

// TestIndexRawKeysMalformedNeedleReportsSyntax is the non-vacuous test for the
// re-validation fallback. Each needle here both overflows the one-entry tape
// and is malformed, so the builder reports ErrIndexFull before it can
// diagnose anything: without the fallback these would all surface as
// ErrIndexScalar and a caller would be told its syntax error was a shape
// error.
func TestIndexRawKeysMalformedNeedleReportsSyntax(t *testing.T) {
	collection := indexedScalarCollection(t)
	for _, needle := range []string{`[1,2`, `{"a":1`, `[1,]`, `{"a"1}`, `[1 2]`, `["x`} {
		_, err := collection.IndexRawKeys("v", []byte(needle))
		switch {
		case err == nil:
			t.Errorf("malformed needle %s accepted", needle)
		case errors.Is(err, ErrIndexScalar):
			t.Errorf("malformed needle %s reported as a shape error, want a syntax error", needle)
		case errors.Is(err, document.ErrIndexFull):
			t.Errorf("malformed needle %s leaked the storage shortfall: %v", needle, err)
		}
	}
}

// TestIndexRawKeysMalformedScalarReportsSyntax covers the complementary case:
// input that is malformed without ever outgrowing one entry never reaches the
// fallback, and must still report its syntax error unchanged.
func TestIndexRawKeysMalformedScalarReportsSyntax(t *testing.T) {
	collection := indexedScalarCollection(t)
	for _, needle := range []string{`tru`, `01`, `"unterminated`, `-`, ``, `  `} {
		_, err := collection.IndexRawKeys("v", []byte(needle))
		if err == nil {
			t.Errorf("malformed needle %q accepted", needle)
			continue
		}
		if errors.Is(err, ErrIndexScalar) || errors.Is(err, document.ErrIndexFull) {
			t.Errorf("malformed needle %q error = %v, want a syntax error", needle, err)
		}
	}
}

// TestIndexRawKeysWideNeedleDoesNotOverrun guards the sizing itself: the
// per-needle storage is a single stack entry with its capacity clipped, so a
// wide needle must be refused rather than write past it. A needle far wider
// than the arity keeps that boundary honest under -race and the bounds
// checker rather than by inspection.
func TestIndexRawKeysWideNeedleDoesNotOverrun(t *testing.T) {
	collection := indexedScalarCollection(t)
	needle := "[" + strings.Repeat("1,", 4095) + "1]"
	if _, err := collection.IndexRawKeys("v", []byte(needle)); !errors.Is(err, ErrIndexScalar) {
		t.Fatalf("wide needle error = %v, want %v", err, ErrIndexScalar)
	}
}
