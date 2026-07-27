package storeio

import (
	"bytes"
	"testing"
)

func TestDocumentLocatorExactRoundTripAndSpace(t *testing.T) {
	for _, test := range []PageRef{
		{
			Offset: 2 * uint64(physicalPageQuantum), LogicalID: 2, Generation: 1,
			Length: physicalPageQuantum, Kind: PageDocument,
		},
		{
			Offset: (1<<43 - 1) * uint64(physicalPageQuantum), LogicalID: 1 << 40,
			Generation: 1<<48 - 1, Length: 16 * physicalPageQuantum,
			Kind: PageDocument,
		},
		{
			Offset: 991 * uint64(physicalPageQuantum), LogicalID: 81, Generation: 1 << 37,
			Length: 8 * physicalPageQuantum, Kind: PageDocumentGroup,
		},
	} {
		image, ok := EncodeDocumentLocator(nil, test)
		if !ok || len(image) != DocumentLocatorBytes {
			t.Fatalf("encode %+v = (%d,%v)", test, len(image), ok)
		}
		got, ok := DecodeDocumentLocator(image)
		if !ok || got.Offset != test.Offset ||
			got.Generation != test.Generation || got.Length != test.Length ||
			got.Grouped != (test.Kind == PageDocumentGroup) {
			t.Fatalf("round trip %+v = (%+v,%v)", test, got, ok)
		}
		expanded, ok := got.PageRef(test.LogicalID)
		if !ok || expanded != test {
			t.Fatalf("expanded %+v = (%+v,%v)", got, expanded, ok)
		}
	}

	const rowsPerBlock = 64
	if got := float64(DocumentLocatorBytes) / rowsPerBlock; got != 0.1875 {
		t.Fatalf("block-map bytes/current key = %.4f, want 0.1875", got)
	}
}

func TestDocumentLocatorRejectsExtendedAndInvalidReferences(t *testing.T) {
	valid := PageRef{
		Offset: 2 * uint64(physicalPageQuantum), LogicalID: 2, Generation: 1,
		Length: physicalPageQuantum, Kind: PageDocument,
	}
	tests := []PageRef{
		{},
		func() PageRef { ref := valid; ref.Offset = uint64(physicalPageQuantum); return ref }(),
		func() PageRef { ref := valid; ref.Offset++; return ref }(),
		func() PageRef { ref := valid; ref.Offset = 1 << 43 * uint64(physicalPageQuantum); return ref }(),
		func() PageRef { ref := valid; ref.LogicalID = StateRootLogicalID; return ref }(),
		func() PageRef { ref := valid; ref.Generation = 0; return ref }(),
		func() PageRef { ref := valid; ref.Generation = 1 << 48; return ref }(),
		func() PageRef { ref := valid; ref.Length = 17 * physicalPageQuantum; return ref }(),
		func() PageRef { ref := valid; ref.Kind = PageOverflow; return ref }(),
		func() PageRef { ref := valid; ref.Flags = 1; return ref }(),
		func() PageRef { ref := valid; ref.Aux = 1; return ref }(),
		func() PageRef {
			ref := valid
			ref.Kind, ref.Length = PageDocumentGroup, 3*physicalPageQuantum
			return ref
		}(),
	}
	prefix := []byte{1, 2, 3}
	for _, test := range tests {
		dst, ok := EncodeDocumentLocator(bytes.Clone(prefix), test)
		if ok || !bytes.Equal(dst, prefix) {
			t.Fatalf("invalid encode %+v = (%x,%v)", test, dst, ok)
		}
	}

	if _, ok := DecodeDocumentLocator(nil); ok {
		t.Fatal("empty locator decoded")
	}
	zero := make([]byte, DocumentLocatorBytes)
	if _, ok := DecodeDocumentLocator(zero); ok {
		t.Fatal("zero locator decoded")
	}
	if _, ok := (DocumentLocator{}).PageRef(StateRootLogicalID); ok {
		t.Fatal("invalid reconstructed logical identity")
	}
}

func TestDocumentLocatorZeroAllocation(t *testing.T) {
	ref := PageRef{
		Offset: 17 * uint64(physicalPageQuantum), LogicalID: 99, Generation: 71,
		Length: 3 * physicalPageQuantum, Kind: PageDocument,
	}
	var image [DocumentLocatorBytes]byte
	if allocs := testing.AllocsPerRun(1000, func() {
		dst, ok := EncodeDocumentLocator(image[:0], ref)
		if !ok {
			panic("encode")
		}
		got, ok := DecodeDocumentLocator(dst)
		if !ok || got.Offset != ref.Offset || got.Generation != ref.Generation {
			panic("decode")
		}
	}); allocs != 0 {
		t.Fatalf("locator allocations = %.2f, want 0", allocs)
	}
}

func BenchmarkDocumentLocator(b *testing.B) {
	ref := PageRef{
		Offset: 17 * uint64(physicalPageQuantum), LogicalID: 99, Generation: 71,
		Length: 3 * physicalPageQuantum, Kind: PageDocument,
	}
	var image [DocumentLocatorBytes]byte
	b.ReportAllocs()
	for b.Loop() {
		dst, ok := EncodeDocumentLocator(image[:0], ref)
		if !ok {
			b.Fatal("encode")
		}
		if _, ok := DecodeDocumentLocator(dst); !ok {
			b.Fatal("decode")
		}
	}
}
