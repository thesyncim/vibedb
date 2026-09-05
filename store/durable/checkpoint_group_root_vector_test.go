package durable

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func exactRootVectorTestMember(t *testing.T, rank byte, generation uint64) RootVectorMember {
	t.Helper()
	storeID := [16]byte{rank, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70}
	journalID := [16]byte{rank, 0xa0, 0xb0, 0xc0, 0xd0, 0xe0, 0xf0, 0x01}
	layout, err := storeio.MutableStoreLayout(4096)
	if err != nil {
		t.Fatal(err)
	}
	state := storeio.StateRoot{
		StoreID:       storeID,
		Generation:    generation,
		PageSize:      4096,
		MaxPageSize:   64 << 10,
		NextLogicalID: 1,
		JournalID:     journalID,
	}
	root := storeio.InlineSuperblock{
		StoreID:    storeID,
		Generation: generation,
		FileEnd:    layout.DataStart,
		PageSize:   4096,
		State:      state,
	}
	return RootVectorMember{
		NameDigest: sha256.Sum256([]byte{rank}),
		StoreID:    storeID,
		JournalID:  journalID,
		Root:       root,
	}
}

func exactRootVectorTestVector(t *testing.T, generation uint64) RootVector {
	t.Helper()
	return RootVector{
		Cut: RootVectorCut{
			Applied:     9,
			Term:        4,
			EntryDigest: [32]byte{0x11},
			Lineage:     [32]byte{0x22},
			GroupID:     [32]byte{0x33},
		},
		Members: []RootVectorMember{
			exactRootVectorTestMember(t, 1, generation),
			exactRootVectorTestMember(t, 2, generation+1),
		},
	}
}

func TestExactRootVectorCheckpointPublishesConsecutiveBanks(t *testing.T) {
	path := filepath.Join(t.TempDir(), ExactRootVectorFilename)
	checkpoint, err := OpenExactRootVectorCheckpoint(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	vector := exactRootVectorTestVector(t, 4)
	if err := checkpoint.Publish(vector); err != nil {
		t.Fatal(err)
	}
	selected, floors, err := checkpoint.Read()
	if err != nil {
		t.Fatal(err)
	}
	if selected.Sequence != 2 || len(floors) != 2 {
		t.Fatalf("initial selected sequence=%d floors=%d", selected.Sequence, len(floors))
	}
	firstBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpoint.Publish(exactRootVectorTestVector(t, 8)); err != nil {
		t.Fatal(err)
	}
	secondBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(firstBytes, secondBytes) {
		t.Fatal("changed vector did not advance sidecar")
	}
	bankBytes := checkpoint.bankBytes
	left, leftErr := storeio.DecodeRootVectorBank(secondBytes[:bankBytes])
	right, rightErr := storeio.DecodeRootVectorBank(secondBytes[bankBytes:])
	if leftErr != nil || rightErr != nil {
		t.Fatalf("published banks left=%v right=%v", leftErr, rightErr)
	}
	if left.Sequence != 3 || right.Sequence != 4 {
		t.Fatalf("published sequences=%d,%d want 3,4", left.Sequence, right.Sequence)
	}
	if _, _, err := storeio.SelectRootVectorBanks(
		secondBytes[:bankBytes], secondBytes[bankBytes:],
	); err != nil {
		t.Fatal(err)
	}
	if err := checkpoint.Close(); err != nil {
		t.Fatal(err)
	}

	// A torn newest bank leaves the older complete bank selectable.
	secondBytes[bankBytes+storeio.RootVectorBankHeaderBytes+
		2*storeio.RootVectorMemberBytes] ^= 0x40
	if err := os.WriteFile(path, secondBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenExactRootVectorCheckpoint(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	selected, floors, err = reopened.Read()
	if err != nil {
		t.Fatal(err)
	}
	if selected.Sequence != 3 || selected.Members[0].Root.Generation != 8 {
		t.Fatalf("torn fallback sequence=%d generation=%d", selected.Sequence, selected.Members[0].Root.Generation)
	}
	if floors[0].Generation != 8 || floors[1].Generation != 9 {
		t.Fatalf("torn fallback floors=%+v", floors)
	}
}
