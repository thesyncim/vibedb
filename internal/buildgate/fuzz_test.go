package buildgate

import (
	"bytes"
	"testing"
)

func FuzzOpenPrefaceCanonical(f *testing.F) {
	canonical, err := AppendPreface(nil, testProfile())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(canonical)
	f.Add([]byte(nil))
	f.Add(canonical[:PrefaceBytes-1])
	f.Fuzz(func(t *testing.T, raw []byte) {
		profile, err := OpenPreface(raw)
		if err != nil {
			return
		}
		if !profile.Valid() || !Compatible(profile, profile) {
			t.Fatalf("opened invalid profile: %#v", profile)
		}
		reencoded, err := AppendPreface(nil, profile)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(reencoded, raw) {
			t.Fatalf("accepted noncanonical preface\n got %x\nwant %x", raw, reencoded)
		}
	})
}

func FuzzCompatibilitySymmetric(f *testing.F) {
	f.Add(uint64(1), uint64(3), uint64(1), uint64(2), true, true)
	f.Add(uint64(1), uint64(1), uint64(2), uint64(2), false, true)
	f.Fuzz(func(t *testing.T, leftProvided, rightProvided, leftRequired, rightRequired uint64,
		sameWire, sameDisk bool,
	) {
		leftRequired &= leftProvided
		rightRequired &= rightProvided
		left := Profile{
			WireGrammar: testGrammar(1),
			DiskGrammar: testGrammar(33),
			Provided:    CapabilitySet{leftProvided},
			Required:    CapabilitySet{leftRequired},
		}
		right := Profile{
			WireGrammar: left.WireGrammar,
			DiskGrammar: left.DiskGrammar,
			Provided:    CapabilitySet{rightProvided},
			Required:    CapabilitySet{rightRequired},
		}
		if !sameWire {
			right.WireGrammar = testGrammar(2)
		}
		if !sameDisk {
			right.DiskGrammar = testGrammar(34)
		}
		leftRight := Compatible(left, right)
		rightLeft := Compatible(right, left)
		if leftRight != rightLeft {
			t.Fatalf("asymmetric compatibility: %v/%v", leftRight, rightLeft)
		}
		if leftRight {
			leftAgreed, leftErr := CheckCompatibility(left, right)
			rightAgreed, rightErr := CheckCompatibility(right, left)
			if leftErr != nil || rightErr != nil || leftAgreed != rightAgreed {
				t.Fatalf("asymmetric agreement: %#v/%v %#v/%v", leftAgreed, leftErr, rightAgreed, rightErr)
			}
		}
	})
}
