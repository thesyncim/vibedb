package competitive

import "testing"

func TestMatchesCountryValidatedExactAndBounded(t *testing.T) {
	src := []byte(`{"country":"PT","nested":{"ignored":true},"n":18446744073709551615}`)
	got, err := matchesCountryValidated(src, []byte(`"PT"`))
	if err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Fatal("typed country did not match")
	}
	tooDeep := []byte(`{"country":"PT","x":[[[[[[[[[0]]]]]]]]]}`)
	if _, err := matchesCountryValidated(tooDeep, []byte(`"PT"`)); err == nil {
		t.Fatal("validated pointer accepted nesting beyond its bound")
	}
}

func TestMatchesCountryValidatedAllocs(t *testing.T) {
	src := []byte(`{"country":"PT","id":18446744073709551615}`)
	needle := []byte(`"PT"`)
	allocs := testing.AllocsPerRun(1000, func() {
		got, err := matchesCountryValidated(src, needle)
		if err != nil || !got {
			panic("typed country mismatch")
		}
	})
	if allocs != 0 {
		t.Fatalf("validated country filter allocations = %v, want 0", allocs)
	}
}
