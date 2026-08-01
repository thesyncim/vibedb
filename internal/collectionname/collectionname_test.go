package collectionname

import (
	"strings"
	"testing"
)

func TestPortableCollectionNameRoundTrip(t *testing.T) {
	for _, name := range []string{
		"orders", "Orders", "a/b", `a\b`, "CON", "name. ",
		"caf\u00e9", "cafe\u0301", strings.Repeat("x", MaxNameBytes),
	} {
		filename, ok := Encode(name)
		if !ok {
			t.Fatalf("Encode(%q) rejected a valid name", name)
		}
		if got, ok := Decode(filename); !ok || got != name {
			t.Fatalf("Decode(Encode(%q)) = (%q,%v)", name, got, ok)
		}
		if got := len(filename + JournalSuffix); got > 255 {
			t.Fatalf("paired filename for %q is %d bytes", name, got)
		}
	}
}

func TestCollectionNameRejectsNonCanonicalOrUnrepresentableInput(t *testing.T) {
	for _, name := range []string{
		"", string([]byte{0xff}), strings.Repeat("x", MaxNameBytes+1),
	} {
		if _, ok := Encode(name); ok {
			t.Fatalf("Encode(%q) succeeded", name)
		}
	}
	for _, filename := range []string{
		"orders.vjc", "c-.vjc", "c-0.vjc", "c-6F.vjc", "c-zz.vjc",
	} {
		if _, ok := Decode(filename); ok {
			t.Fatalf("Decode(%q) succeeded", filename)
		}
	}
}

func TestCaseAliasesAreRecognizedForCatalogRejection(t *testing.T) {
	for _, filename := range []string{
		"C-6f.vjc", "c-6F.vjc", "c-6f.VJC",
	} {
		if !PrimaryCaseAlias(filename) {
			t.Fatalf("PrimaryCaseAlias(%q) = false", filename)
		}
	}
	for _, filename := range []string{
		"C-6f.vjc.rjournal", "c-6F.vjc.rjournal",
		"c-6f.vjc.RJOURNAL",
	} {
		if !JournalCaseAlias(filename) {
			t.Fatalf("JournalCaseAlias(%q) = false", filename)
		}
	}
	if PrimaryCaseAlias("unrelated.VJC") || JournalCaseAlias("notes.RJOURNAL") {
		t.Fatal("an unrelated filename was classified as a canonical case alias")
	}
}
