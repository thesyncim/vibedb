package pginput

import (
	"testing"
)

func TestBooleanMatchesPostgreSQLUniquePrefixGrammar(t *testing.T) {
	for _, test := range []struct {
		text  string
		value bool
		ok    bool
	}{
		{"t", true, true}, {"tr", true, true}, {"tru", true, true}, {"TRUE", true, true},
		{"f", false, true}, {"fa", false, true}, {"fal", false, true}, {"fals", false, true},
		{"y", true, true}, {"ye", true, true}, {"YES", true, true},
		{"n", false, true}, {"no", false, true},
		{"on", true, true}, {"of", false, true}, {"OFF", false, true},
		{"1", true, true}, {"0", false, true},
		{"  Tr  ", true, true}, {"\tOF\n", false, true},
		{"", false, false}, {" \f ", false, false}, {"o", false, false},
		{"truth", false, false}, {"2", false, false}, {"yesplease", false, false},
		{"t rue", false, false}, {"\u00a0true", false, false},
	} {
		value, ok := Boolean(test.text)
		if value != test.value || ok != test.ok {
			t.Fatalf("Boolean(%q) = %v/%v, want %v/%v",
				test.text, value, ok, test.value, test.ok)
		}
	}
}

func TestBooleanIsAllocationFree(t *testing.T) {
	if allocs := testing.AllocsPerRun(1000, func() {
		value, ok := Boolean("  FaLs  ")
		if !ok || value {
			t.Fatal("unexpected boolean parse")
		}
	}); allocs != 0 {
		t.Fatalf("Boolean allocated %.2f/run", allocs)
	}
}
