package storeio

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"testing"
)

func TestPageChecksumMatchesStandardLibrary(t *testing.T) {
	table := crc32.MakeTable(crc32.Castagnoli)
	data := make([]byte, (128<<10)+31)
	for i := range data {
		data[i] = byte(i*131 + i>>3)
	}
	for alignment := range 16 {
		for _, size := range []int{0, 1, 2, 3, 4, 7, 8, 15, 16, 31, 32, 119, 120, 127, 128, 255, 256, 257, 511, 512, 1023, 1024, 1025, 4095, 4096, 4097, 64 << 10, 128 << 10} {
			input := data[alignment : alignment+size]
			if got, want := PageChecksum(input), crc32.Checksum(input, table); got != want {
				t.Fatalf("alignment=%d size=%d: checksum=%08x, want %08x", alignment, size, got, want)
			}
		}
	}
}

const testSuperblockPageSize = uint32(4096)

var testStoreID = [16]byte{0x51, 0x7a, 0x93, 0x11, 0x2c, 0x44, 0x58, 0x61, 0x70, 0x8d, 0xa2, 0xb5, 0xc9, 0xd0, 0xe4, 0xff}

func testMutableStoreDataStart(pageSize uint32) uint64 {
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		panic(err)
	}
	return layout.DataStart
}

func testSuperblock(generation, stateOffset uint64, state []byte) Superblock {
	return Superblock{
		StoreID:       testStoreID,
		Generation:    generation,
		StateOffset:   stateOffset,
		StateLength:   uint32(len(state)),
		StateChecksum: PageChecksum(state),
		FileEnd:       stateOffset + uint64(testSuperblockPageSize),
		PageSize:      testSuperblockPageSize,
	}
}

func encodeTestSuperblock(t *testing.T, root Superblock) [SuperblockSize]byte {
	t.Helper()
	var encoded [SuperblockSize]byte
	if _, err := EncodeSuperblock(encoded[:], root); err != nil {
		t.Fatal(err)
	}
	return encoded
}

func resealTestSuperblock(encoded []byte) {
	checksum := PageChecksum(encoded[:120])
	binary.LittleEndian.PutUint32(encoded[120:124], checksum)
	binary.LittleEndian.PutUint32(encoded[124:128], ^checksum)
}

func TestSuperblockCodecAndCorruption(t *testing.T) {
	state := []byte("state-root-one")
	root := testSuperblock(7, 4*uint64(testSuperblockPageSize), state)
	encoded := encodeTestSuperblock(t, root)
	decoded, err := DecodeSuperblock(encoded[:])
	if err != nil {
		t.Fatal(err)
	}
	if decoded != root {
		t.Fatalf("decoded = %+v, want %+v", decoded, root)
	}

	for i := range encoded {
		corrupt := encoded
		corrupt[i] ^= 1
		if _, err := DecodeSuperblock(corrupt[:]); !errors.Is(err, ErrSuperblockCorrupt) {
			t.Fatalf("byte %d corruption = %v, want %v", i, err, ErrSuperblockCorrupt)
		}
	}

	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"unknown flags", func(b []byte) { binary.LittleEndian.PutUint32(b[16:20], 1) }},
		{"generation complement", func(b []byte) { b[32] ^= 1 }},
		{"state checksum complement", func(b []byte) { b[56] ^= 1 }},
		{"reserved bytes", func(b []byte) { b[60] = 1 }},
		{"empty state root", func(b []byte) { binary.LittleEndian.PutUint32(b[48:52], 0) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			corrupt := encoded
			test.mutate(corrupt[:])
			resealTestSuperblock(corrupt[:])
			if _, err := DecodeSuperblock(corrupt[:]); !errors.Is(err, ErrSuperblockCorrupt) {
				t.Fatalf("DecodeSuperblock = %v, want %v", err, ErrSuperblockCorrupt)
			}
		})
	}
}

func TestSuperblockValidationAndSlotSelection(t *testing.T) {
	state := []byte("state")
	valid := testSuperblock(1, 4*uint64(testSuperblockPageSize), state)
	for _, test := range []struct {
		name   string
		mutate func(*Superblock)
	}{
		{"generation", func(root *Superblock) { root.Generation = 0 }},
		{"store id", func(root *Superblock) { root.StoreID = [16]byte{} }},
		{"page size", func(root *Superblock) { root.PageSize = 6000 }},
		{"flags", func(root *Superblock) { root.Flags = 1 }},
		{"file end", func(root *Superblock) { root.FileEnd-- }},
		{"state alignment", func(root *Superblock) { root.StateOffset++ }},
		{"state length", func(root *Superblock) { root.StateLength = root.PageSize + 1 }},
		{"missing state", func(root *Superblock) { root.StateLength = 0 }},
		{"empty free offset", func(root *Superblock) { root.FreeOffset = uint64(root.PageSize) * 3 }},
		{"empty free checksum", func(root *Superblock) { root.FreeChecksum = 1 }},
		{"free alignment", func(root *Superblock) { root.FreeOffset = uint64(root.PageSize)*3 + 1; root.FreeLength = 1 }},
		{"overlapping roots", func(root *Superblock) { root.FreeOffset = root.StateOffset; root.FreeLength = 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := valid
			test.mutate(&root)
			var dst [SuperblockSize]byte
			if _, err := EncodeSuperblock(dst[:], root); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("EncodeSuperblock = %v, want %v", err, ErrInvalidWrite)
			}
		})
	}

	for generation, want := range map[uint64]int64{1: 0, 2: 4096, 3: 0, 4: 4096} {
		got, err := SuperblockOffset(generation, testSuperblockPageSize)
		if err != nil || got != want {
			t.Fatalf("generation %d offset = %d, %v; want %d", generation, got, err, want)
		}
	}
	if _, err := SuperblockOffset(0, testSuperblockPageSize); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("zero generation = %v, want %v", err, ErrInvalidWrite)
	}
}

func TestSelectSuperblockNewestFallbackAndConflict(t *testing.T) {
	state1 := []byte("state-one")
	state2 := []byte("state-two")
	root1 := testSuperblock(1, 4*uint64(testSuperblockPageSize), state1)
	root2 := testSuperblock(2, 5*uint64(testSuperblockPageSize), state2)
	first := encodeTestSuperblock(t, root1)
	second := encodeTestSuperblock(t, root2)

	got, slot, err := SelectSuperblock(first[:], second[:])
	if err != nil || got != root2 || slot != 1 {
		t.Fatalf("newest = %+v slot %d err %v", got, slot, err)
	}
	second[7] ^= 1
	got, slot, err = SelectSuperblock(first[:], second[:])
	if err != nil || got != root1 || slot != 0 {
		t.Fatalf("fallback = %+v slot %d err %v", got, slot, err)
	}
	first[0] ^= 1
	if _, _, err := SelectSuperblock(first[:], second[:]); !errors.Is(err, ErrSuperblockNotFound) {
		t.Fatalf("both corrupt = %v, want %v", err, ErrSuperblockNotFound)
	}

	first = encodeTestSuperblock(t, root1)
	root2.StoreID[0] ^= 1
	second = encodeTestSuperblock(t, root2)
	if _, _, err := SelectSuperblock(first[:], second[:]); !errors.Is(err, ErrSuperblockConflict) {
		t.Fatalf("foreign root = %v, want %v", err, ErrSuperblockConflict)
	}

	repositioned := encodeTestSuperblock(t, testSuperblock(2, 5*uint64(testSuperblockPageSize), state2))
	var corrupt [SuperblockSize]byte
	got, slot, err = SelectSuperblock(repositioned[:], corrupt[:])
	if err != nil || got.Generation != 2 || slot != 0 {
		t.Fatalf("physical-slot root = %+v slot %d err %v", got, slot, err)
	}
}

func TestRecoverSuperblockValidatesReferencedState(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "superblock-recovery")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	state1 := []byte("durable-state-one")
	state2 := []byte("durable-state-two")
	root1 := testSuperblock(1, 4*uint64(testSuperblockPageSize), state1)
	root2 := testSuperblock(2, 5*uint64(testSuperblockPageSize), state2)
	first := encodeTestSuperblock(t, root1)
	second := encodeTestSuperblock(t, root2)
	if err := file.Truncate(int64(root2.FileEnd)); err != nil {
		t.Fatal(err)
	}
	writeAtTest(t, file, first[:], 0)
	writeAtTest(t, file, second[:], int64(testSuperblockPageSize))
	writeAtTest(t, file, state1, int64(root1.StateOffset))
	writeAtTest(t, file, state2, int64(root2.StateOffset))

	scratch := make([]byte, testSuperblockPageSize)
	got, slot, err := RecoverSuperblock(file, testSuperblockPageSize, scratch)
	if err != nil || got != root2 || slot != 1 {
		t.Fatalf("recover newest = %+v slot %d err %v", got, slot, err)
	}

	corruptState := append([]byte(nil), state2...)
	corruptState[0] ^= 1
	writeAtTest(t, file, corruptState, int64(root2.StateOffset))
	got, slot, err = RecoverSuperblock(file, testSuperblockPageSize, scratch)
	if err != nil || got != root1 || slot != 0 {
		t.Fatalf("state fallback = %+v slot %d err %v", got, slot, err)
	}
	writeAtTest(t, file, state2, int64(root2.StateOffset))

	torn := second
	torn[64] ^= 1
	writeAtTest(t, file, torn[:], int64(testSuperblockPageSize))
	got, slot, err = RecoverSuperblock(file, testSuperblockPageSize, scratch)
	if err != nil || got != root1 || slot != 0 {
		t.Fatalf("header fallback = %+v slot %d err %v", got, slot, err)
	}
	writeAtTest(t, file, second[:], int64(testSuperblockPageSize))

	if _, _, err := RecoverSuperblock(file, testSuperblockPageSize, scratch[:len(scratch)-1]); !errors.Is(err, ErrRecoveryBufferTooSmall) {
		t.Fatalf("short scratch = %v, want %v", err, ErrRecoveryBufferTooSmall)
	}
	if err := file.Truncate(int64(root2.StateOffset)); err != nil {
		t.Fatal(err)
	}
	got, slot, err = RecoverSuperblock(file, testSuperblockPageSize, scratch)
	if err != nil || got != root1 || slot != 0 {
		t.Fatalf("truncated newest = %+v slot %d err %v", got, slot, err)
	}
}

func TestRecoverSuperblockTornAlternateRoot(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "superblock-torn-root")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	state2 := []byte("generation-two")
	state3 := []byte("generation-three")
	state4 := []byte("generation-four")
	root2 := testSuperblock(2, 4*uint64(testSuperblockPageSize), state2)
	root3 := testSuperblock(3, 5*uint64(testSuperblockPageSize), state3)
	root4 := testSuperblock(4, 6*uint64(testSuperblockPageSize), state4)
	encoded2 := encodeTestSuperblock(t, root2)
	encoded3 := encodeTestSuperblock(t, root3)
	encoded4 := encodeTestSuperblock(t, root4)
	if err := file.Truncate(int64(root4.FileEnd)); err != nil {
		t.Fatal(err)
	}
	writeAtTest(t, file, encoded3[:], 0)
	writeAtTest(t, file, state2, int64(root2.StateOffset))
	writeAtTest(t, file, state3, int64(root3.StateOffset))
	writeAtTest(t, file, state4, int64(root4.StateOffset))
	scratch := make([]byte, testSuperblockPageSize)

	for cut := 0; cut <= SuperblockSize; cut++ {
		torn := encoded2
		copy(torn[:cut], encoded4[:cut])
		writeAtTest(t, file, torn[:], int64(testSuperblockPageSize))
		got, slot, recoverErr := RecoverSuperblock(file, testSuperblockPageSize, scratch)
		want, wantSlot := root3, 0
		if cut == SuperblockSize {
			want, wantSlot = root4, 1
		}
		if recoverErr != nil || got != want || slot != wantSlot {
			t.Fatalf("cut %d = %+v slot %d err %v; want generation %d slot %d", cut, got, slot, recoverErr, want.Generation, wantSlot)
		}
	}
}

func TestRecoverSuperblockValidatesFreeRoot(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "superblock-free-root")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	state1 := []byte("state-one")
	state2 := []byte("state-two")
	free2 := []byte("free-tree-two")
	root1 := testSuperblock(1, 4*uint64(testSuperblockPageSize), state1)
	root2 := testSuperblock(2, 5*uint64(testSuperblockPageSize), state2)
	root2.FreeOffset = 6 * uint64(testSuperblockPageSize)
	root2.FreeLength = uint32(len(free2))
	root2.FreeChecksum = PageChecksum(free2)
	root2.FileEnd = 7 * uint64(testSuperblockPageSize)
	first := encodeTestSuperblock(t, root1)
	second := encodeTestSuperblock(t, root2)
	if err := file.Truncate(int64(root2.FileEnd)); err != nil {
		t.Fatal(err)
	}
	writeAtTest(t, file, first[:], 0)
	writeAtTest(t, file, second[:], int64(testSuperblockPageSize))
	writeAtTest(t, file, state1, int64(root1.StateOffset))
	writeAtTest(t, file, state2, int64(root2.StateOffset))
	writeAtTest(t, file, free2, int64(root2.FreeOffset))
	scratch := make([]byte, testSuperblockPageSize)

	got, slot, recoverErr := RecoverSuperblock(file, testSuperblockPageSize, scratch)
	if recoverErr != nil || got != root2 || slot != 1 {
		t.Fatalf("recover free root = %+v slot %d err %v", got, slot, recoverErr)
	}
	free2[0] ^= 1
	writeAtTest(t, file, free2, int64(root2.FreeOffset))
	got, slot, recoverErr = RecoverSuperblock(file, testSuperblockPageSize, scratch)
	if recoverErr != nil || got != root1 || slot != 0 {
		t.Fatalf("corrupt free fallback = %+v slot %d err %v", got, slot, recoverErr)
	}
}

func TestCommitterPublishesEncodedSuperblock(t *testing.T) {
	committer, file, pageSize := newPortableCommitter(t, 3, 1)
	defer committer.Close()
	batch, err := committer.Begin(1)
	if err != nil {
		t.Fatal(err)
	}
	page, err := batch.PageBuffer(0)
	if err != nil {
		t.Fatal(err)
	}
	page = page[:pageSize]
	clear(page)
	copy(page, "committed state root")
	stateOffset := testMutableStoreDataStart(uint32(pageSize))
	if err := batch.SetPage(0, int64(stateOffset), pageSize); err != nil {
		t.Fatal(err)
	}
	root := testSuperblock(1, stateOffset, page)
	root.PageSize = uint32(pageSize)
	root.FileEnd = stateOffset + uint64(pageSize)
	if err := batch.SetSuperblock(root); err != nil {
		t.Fatal(err)
	}
	if err := batch.Publish(2); !errors.Is(err, ErrGenerationOrder) {
		t.Fatalf("mismatched Publish = %v, want %v", err, ErrGenerationOrder)
	}
	if err := batch.Publish(1); err != nil {
		t.Fatal(err)
	}
	if err := committer.Wait(1); err != nil {
		t.Fatal(err)
	}
	scratch := make([]byte, pageSize)
	got, slot, err := RecoverSuperblock(file, uint32(pageSize), scratch)
	if err != nil || got != root || slot != 0 {
		t.Fatalf("recover committed = %+v slot %d err %v", got, slot, err)
	}
}

func writeAtTest(t *testing.T, file *os.File, data []byte, offset int64) {
	t.Helper()
	n, err := file.WriteAt(data, offset)
	if err != nil || n != len(data) {
		t.Fatalf("WriteAt(%d) = %d, %v", offset, n, err)
	}
}
