package storeio

import (
	"bytes"
	"errors"
	"testing"
)

const coldLeafExtentRunLabTestBase = uint64(64 << 10)

func TestColdLeafExtentRunLabClassGeometry(t *testing.T) {
	tests := []struct {
		leaf, class, run int
	}{
		{1, 4 << 10, 260 << 10},
		{4 << 10, 4 << 10, 260 << 10},
		{4<<10 + 1, 8 << 10, 516 << 10},
		{8 << 10, 8 << 10, 516 << 10},
		{16 << 10, 16 << 10, 1028 << 10},
		{32 << 10, 32 << 10, 2052 << 10},
		{64 << 10, 64 << 10, 4100 << 10},
	}
	for _, test := range tests {
		if got := ColdLeafExtentRunLabClass(test.leaf); got != test.class {
			t.Errorf("class(%d) = %d, want %d", test.leaf, got, test.class)
		}
		if got := ColdLeafExtentRunLabBytes(test.leaf); got != test.run {
			t.Errorf("run(%d) = %d, want %d", test.leaf, got, test.run)
		}
	}
	for _, invalid := range []int{-1, 0, ColdLeafExtentRunLabMaximumLeafBytes + 1} {
		if ColdLeafExtentRunLabClass(invalid) != 0 ||
			ColdLeafExtentRunLabBytes(invalid) != 0 {
			t.Errorf("invalid leaf size %d admitted", invalid)
		}
	}
}

func TestColdLeafExtentRunLabRoundTripAndDirectReferences(t *testing.T) {
	records := coldLeafExtentRunLabTestRecords(73, 4568)
	run := make([]byte, ColdLeafExtentRunLabBytes(len(records[0].Leaf)))
	refs := make([]ColdLeafExtentRunLabRef, len(records))
	gotRefs, err := EncodeColdLeafExtentRunLab(
		run, coldLeafExtentRunLabTestBase, 7, records, refs,
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenColdLeafExtentRunLab(run, coldLeafExtentRunLabTestBase)
	if err != nil {
		t.Fatal(err)
	}
	if view.Generation() != 7 || view.Len() != len(records) ||
		view.PhysicalBytes() != len(run) {
		t.Fatalf("view identity = generation %d count %d bytes %d",
			view.Generation(), view.Len(), view.PhysicalBytes())
	}

	iterator := view.Iterator()
	for rank, record := range records {
		ref, leaf, ok := iterator.Next()
		if !ok || ref != gotRefs[rank] || !bytes.Equal(leaf, record.Leaf) {
			t.Fatalf("rank %d iterator mismatch", rank)
		}
		if ref.Offset%ColdLeafExtentRunLabLeafAlignment != 0 {
			t.Fatalf("rank %d reference is not eight-byte aligned", rank)
		}
		resolved, ok := ResolveColdLeafExtentRunLab(
			run, coldLeafExtentRunLabTestBase, ref,
		)
		if !ok || !bytes.Equal(resolved, record.Leaf) {
			t.Fatalf("rank %d direct resolution mismatch", rank)
		}
		window, inner, ok := ColdLeafExtentRunLabReadWindow(
			run, coldLeafExtentRunLabTestBase, ref,
			ColdLeafExtentRunLabIOQuantum,
		)
		if !ok || int(inner)+len(record.Leaf) > len(window) ||
			!bytes.Equal(window[inner:int(inner)+len(record.Leaf)], record.Leaf) {
			t.Fatalf("rank %d direct-I/O window mismatch", rank)
		}
		if (ref.Offset-uint64(inner))%ColdLeafExtentRunLabIOQuantum != 0 ||
			len(window)%ColdLeafExtentRunLabIOQuantum != 0 {
			t.Fatalf("rank %d window is not direct-I/O aligned", rank)
		}
	}
	if _, _, ok := iterator.Next(); ok {
		t.Fatal("iterator returned an extra record")
	}
}

func TestColdLeafExtentRunLabEscapeRetainsExactImage(t *testing.T) {
	records := coldLeafExtentRunLabTestRecords(64, 4829)
	run := make([]byte, ColdLeafExtentRunLabBytes(len(records[0].Leaf)))
	refs := make([]ColdLeafExtentRunLabRef, len(records))
	refs, err := EncodeColdLeafExtentRunLab(
		run, coldLeafExtentRunLabTestBase, 9, records, refs,
	)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 8<<10)
	target := len(refs) / 2
	hot, checksum, ok := EscapeColdLeafExtentRunLab(
		dst, run, coldLeafExtentRunLabTestBase, refs[target],
	)
	if !ok || len(hot) != 8<<10 ||
		!bytes.Equal(hot[:len(records[target].Leaf)], records[target].Leaf) ||
		!allZero(hot[len(records[target].Leaf):]) ||
		PageChecksum(hot) != checksum {
		t.Fatal("cold-to-hot escape mismatch")
	}
}

func TestColdLeafExtentRunLabRejectsCorruptionAndInvalidUse(t *testing.T) {
	records := coldLeafExtentRunLabTestRecords(4, 1024)
	run := make([]byte, ColdLeafExtentRunLabBytes(len(records[0].Leaf)))
	refs := make([]ColdLeafExtentRunLabRef, len(records))
	refs, err := EncodeColdLeafExtentRunLab(
		run, coldLeafExtentRunLabTestBase, 1, records, refs,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []int{0, 24, ColdLeafExtentRunLabHeaderSize, len(run) - 1} {
		corrupt := append([]byte(nil), run...)
		corrupt[at] ^= 0x80
		if _, err := OpenColdLeafExtentRunLab(
			corrupt, coldLeafExtentRunLabTestBase,
		); !errors.Is(err, ErrColdLeafExtentRunLabCorrupt) {
			t.Fatalf("corruption at %d: %v", at, err)
		}
	}
	bad := refs[0]
	bad.Offset = coldLeafExtentRunLabTestBase - 1
	if _, ok := ResolveColdLeafExtentRunLab(
		run, coldLeafExtentRunLabTestBase, bad,
	); ok {
		t.Fatal("underflowing point reference admitted")
	}
	bad = refs[0]
	bad.Offset++
	if _, ok := ResolveColdLeafExtentRunLab(
		run, coldLeafExtentRunLabTestBase, bad,
	); ok {
		t.Fatal("unaligned point reference admitted")
	}
	if _, _, ok := ColdLeafExtentRunLabReadWindow(
		run, coldLeafExtentRunLabTestBase, refs[0], 3000,
	); ok {
		t.Fatal("non-power-of-two I/O quantum admitted")
	}

	tooMany := coldLeafExtentRunLabTestRecords(65, 4<<10)
	smallRun := make([]byte, ColdLeafExtentRunLabBytes(4<<10))
	if _, err := EncodeColdLeafExtentRunLab(
		smallRun, coldLeafExtentRunLabTestBase, 1, tooMany,
		make([]ColdLeafExtentRunLabRef, len(tooMany)),
	); !errors.Is(err, ErrColdLeafExtentRunLabFull) {
		t.Fatalf("overfull run error = %v", err)
	}
}

func TestColdLeafExtentRunLabTornImagesFailClosed(t *testing.T) {
	records := coldLeafExtentRunLabTestRecords(64, 4829)
	run := make([]byte, ColdLeafExtentRunLabBytes(len(records[0].Leaf)))
	if _, err := EncodeColdLeafExtentRunLab(
		run, coldLeafExtentRunLabTestBase, 1, records,
		make([]ColdLeafExtentRunLabRef, len(records)),
	); err != nil {
		t.Fatal(err)
	}
	for _, cut := range []int{
		1,
		ColdLeafExtentRunLabHeaderSize,
		len(run) / 3,
		len(run) - ColdLeafExtentRunLabTrailerSize,
		len(run) - 1,
	} {
		torn := make([]byte, len(run))
		copy(torn, run[:cut])
		if _, err := OpenColdLeafExtentRunLab(
			torn, coldLeafExtentRunLabTestBase,
		); !errors.Is(err, ErrColdLeafExtentRunLabCorrupt) {
			t.Fatalf("torn image at %d: %v", cut, err)
		}
	}
}

func TestColdLeafExtentRunLabZeroAllocationHotPaths(t *testing.T) {
	records := coldLeafExtentRunLabTestRecords(64, 4829)
	run := make([]byte, ColdLeafExtentRunLabBytes(len(records[0].Leaf)))
	refs := make([]ColdLeafExtentRunLabRef, len(records))
	refs, err := EncodeColdLeafExtentRunLab(
		run, coldLeafExtentRunLabTestBase, 1, records, refs,
	)
	if err != nil {
		t.Fatal(err)
	}
	ref := refs[len(refs)/2]
	dst := make([]byte, 8<<10)
	if got := testing.AllocsPerRun(1000, func() {
		leaf, ok := ResolveColdLeafExtentRunLab(
			run, coldLeafExtentRunLabTestBase, ref,
		)
		if !ok || len(leaf) == 0 {
			panic("resolve")
		}
	}); got != 0 {
		t.Fatalf("Resolve allocations = %v", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		window, inner, ok := ColdLeafExtentRunLabReadWindow(
			run, coldLeafExtentRunLabTestBase, ref,
			ColdLeafExtentRunLabIOQuantum,
		)
		if !ok || int(inner) >= len(window) {
			panic("window")
		}
	}); got != 0 {
		t.Fatalf("ReadWindow allocations = %v", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		hot, _, ok := EscapeColdLeafExtentRunLab(
			dst, run, coldLeafExtentRunLabTestBase, ref,
		)
		if !ok || len(hot) == 0 {
			panic("escape")
		}
	}); got != 0 {
		t.Fatalf("Escape allocations = %v", got)
	}
}

func FuzzOpenColdLeafExtentRunLab(f *testing.F) {
	records := coldLeafExtentRunLabTestRecords(2, 1024)
	run := make([]byte, ColdLeafExtentRunLabIOQuantum)
	if _, err := EncodeColdLeafExtentRunLab(
		run, coldLeafExtentRunLabTestBase, 1, records,
		make([]ColdLeafExtentRunLabRef, len(records)),
	); err != nil {
		f.Fatal(err)
	}
	f.Add(run)
	f.Fuzz(func(t *testing.T, candidate []byte) {
		_, _ = OpenColdLeafExtentRunLab(
			candidate, coldLeafExtentRunLabTestBase,
		)
	})
}

func coldLeafExtentRunLabTestRecords(
	count int,
	leafBytes int,
) []ColdLeafExtentRunLabRecord {
	records := make([]ColdLeafExtentRunLabRecord, count)
	for rank := range records {
		leaf := make([]byte, leafBytes)
		for index := range leaf {
			leaf[index] = byte(rank*31 + index)
		}
		records[rank] = ColdLeafExtentRunLabRecord{
			BucketID: uint32(rank + 1),
			Leaf:     leaf,
		}
	}
	return records
}
