package raftsim

import "testing"

func TestRNGGoldenAndBounds(t *testing.T) {
	r := NewRNG(0)
	want := [...]uint64{
		0xe220a8397b1dcdaf,
		0x6e789e6aa1b965f4,
		0x06c45d188009454f,
		0xf88bb8a8724c81ec,
	}
	for i, expected := range want {
		if got := r.Uint64(); got != expected {
			t.Fatalf("word %d = %#x, want %#x", i, got, expected)
		}
	}
	if _, ok := r.Choose(0); ok {
		t.Fatal("Choose(0) succeeded")
	}
	for n := uint64(1); n < 100; n++ {
		for i := 0; i < 100; i++ {
			v, ok := r.Choose(n)
			if !ok || v >= n {
				t.Fatalf("Choose(%d) = %d, %v", n, v, ok)
			}
		}
	}
}

func TestRNGSameSeedIsExact(t *testing.T) {
	a, b := NewRNG(0xfeedbeef), NewRNG(0xfeedbeef)
	for i := 0; i < 10_000; i++ {
		if av, bv := a.Uint64(), b.Uint64(); av != bv {
			t.Fatalf("word %d differs: %#x != %#x", i, av, bv)
		}
	}
}

func TestRNGChooseGoldenPinsRejectionConsumption(t *testing.T) {
	// For this n, seed 3's first word is in the incomplete tail and must be
	// rejected. The exact choice and following word pin both reduction and
	// stream consumption.
	r := NewRNG(3)
	got, ok := r.Choose(1<<63 + 1)
	if !ok || got != 0x33466f8a7b81a988 {
		t.Fatalf("Choose = %#x, %v", got, ok)
	}
	if next := r.Uint64(); next != 0x9cebe8a6d050dd01 {
		t.Fatalf("word after rejection = %#x", next)
	}
}
