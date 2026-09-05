package storeio

import (
	"bytes"
	"crypto/sha256"
	"slices"
	"testing"
)

func rootVectorTestMember(t *testing.T, rank byte, generation uint64) RootVectorMember {
	t.Helper()
	storeID := [16]byte{rank, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77}
	journalID := [16]byte{rank, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x01}
	root := testInlineSuperblock(generation)
	root.StoreID = storeID
	root.State.StoreID = storeID
	root.State.Generation = generation
	root.State.JournalID = journalID
	return RootVectorMember{
		NameDigest: sha256.Sum256([]byte{rank}),
		StoreID:    storeID,
		JournalID:  journalID,
		Root:       root,
	}
}

func rootVectorTestFixture(t *testing.T, generationA, generationB uint64) RootVector {
	t.Helper()
	groupID := [32]byte{0x41}
	lineage := [32]byte{0x52}
	return RootVector{
		Sequence: 1,
		Cut: RootVectorCut{
			Applied:     7,
			Term:        3,
			EntryDigest: [32]byte{0x63},
			Lineage:     lineage,
			GroupID:     groupID,
		},
		Members: []RootVectorMember{
			rootVectorTestMember(t, 1, generationA),
			rootVectorTestMember(t, 2, generationB),
		},
	}
}

func TestRootVectorBankRoundTripAndTornFallback(t *testing.T) {
	vector := rootVectorTestFixture(t, 4, 5)
	bankBytes, err := RootVectorBankBytes(len(vector.Members))
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, bankBytes)
	if _, err := EncodeRootVectorBank(first, vector); err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRootVectorBank(first)
	if err != nil {
		t.Fatal(err)
	}
	if !rootVectorSameContent(vector, decoded) {
		t.Fatal("decoded vector changed content")
	}
	newer := vector
	newer.Sequence = 2
	newer.Members = append([]RootVectorMember(nil), vector.Members...)
	newer.Members[0].Root.Generation = 9
	newer.Members[0].Root.State.Generation = 9
	second := make([]byte, bankBytes)
	if _, err := EncodeRootVectorBank(second, newer); err != nil {
		t.Fatal(err)
	}
	second[RootVectorBankHeaderBytes+RootVectorMemberBytes] ^= 0x40
	selected, slot, err := SelectRootVectorBanks(first, second)
	if err != nil {
		t.Fatal(err)
	}
	if slot != 0 || !rootVectorSameContent(selected, vector) {
		t.Fatalf("torn newest selected slot=%d vector=%+v", slot, selected)
	}
}

func TestRootVectorBothBanksAdvanceAndRetainFloors(t *testing.T) {
	old := rootVectorTestFixture(t, 2, 3)
	old.Sequence = 3
	newer := rootVectorTestFixture(t, 8, 9)
	newer.Sequence = 4
	bankBytes, err := RootVectorBankBytes(len(old.Members))
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, bankBytes)
	second := make([]byte, bankBytes)
	if _, err := EncodeRootVectorBank(first, old); err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeRootVectorBank(second, newer); err != nil {
		t.Fatal(err)
	}
	selected, slot, err := SelectRootVectorBanks(first, second)
	if err != nil {
		t.Fatal(err)
	}
	if slot != 1 || selected.Members[0].Root.Generation != 8 {
		t.Fatalf("selected slot=%d generation=%d", slot, selected.Members[0].Root.Generation)
	}
	floors, err := RootVectorMemberFloors(first, second)
	if err != nil {
		t.Fatal(err)
	}
	if len(floors) != 2 || floors[0].Generation != 2 || floors[1].Generation != 3 {
		t.Fatalf("floors=%+v", floors)
	}
	// A pair at the same sequence must be byte-equivalent in all root images.
	mismatched := old
	mismatched.Members = append([]RootVectorMember(nil), old.Members...)
	mismatched.Members[0].Root.Generation++
	mismatched.Members[0].Root.State.Generation++
	second = bytes.Clone(first)
	if _, err := EncodeRootVectorBank(second, mismatched); err != nil {
		t.Fatal(err)
	}
	if _, _, err := SelectRootVectorBanks(first, second); err == nil {
		t.Fatal("accepted mismatched same-sequence banks")
	}
}

func TestRootVectorKeepsLeafOverflowAndFreeMetadata(t *testing.T) {
	member := rootVectorTestMember(t, 7, 12)
	layout, err := MutableStoreLayout(4096)
	if err != nil {
		t.Fatal(err)
	}
	fileEnd := layout.DataStart + GlobalTabletCatalogRootBytes + 4096
	member.Root = testInlineFreeSuperblock(12, fileEnd/4096)
	member.Root.StoreID = member.StoreID
	member.Root.FileEnd = fileEnd
	member.Root.State.StoreID = member.StoreID
	member.Root.State.Generation = 12
	member.Root.State.PageSize = 4096
	member.Root.State.JournalID = member.JournalID
	member.Root.State.MaxPageSize = GlobalTabletCatalogRootBytes
	member.Root.State.NextLogicalID = PrimaryFirstDynamicLogicalID
	member.Root.State.DocumentCount = 1
	member.Root.State.MaxKeyBytes = 128
	member.Root.State.InlineValueBytes = 512
	member.Root.State.MaxDocumentBytes = 8192
	member.Root.State.PrimaryRoot = PageRef{
		Offset:     layout.DataStart,
		LogicalID:  PrimaryCatalogRootLogicalID,
		Generation: 12,
		Length:     GlobalTabletCatalogRootBytes,
		Kind:       PagePrimaryCatalog,
	}
	member.Root.FreeDelta = NewInlineFreeDelta(PageRef{}, PageRef{})
	if err := member.Root.FreeDelta.Append([]FreeDelta{{
		Op: FreeOpSet,
		Extent: FreeExtent{
			Offset:            layout.DataStart + GlobalTabletCatalogRootBytes,
			Length:            4096,
			RetiredGeneration: 11,
		},
	}}, 4096, fileEnd); err != nil {
		t.Fatal(err)
	}
	vector := RootVector{
		Sequence: 1,
		Cut: RootVectorCut{
			Applied:     12,
			Term:        5,
			EntryDigest: [32]byte{0x69},
			Lineage:     [32]byte{0x7a},
			GroupID:     [32]byte{0x7b},
		},
		Members: []RootVectorMember{member},
	}
	bankBytes, err := RootVectorBankBytes(1)
	if err != nil {
		t.Fatal(err)
	}
	encoded := make([]byte, bankBytes)
	if _, err := EncodeRootVectorBank(encoded, vector); err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRootVectorBank(encoded)
	if err != nil {
		t.Fatal(err)
	}
	got := decoded.Members[0].Root
	if got.State.PrimaryRoot != member.Root.State.PrimaryRoot ||
		got.State.MaxDocumentBytes != 8192 || got.FreeDelta.Len() != 1 {
		t.Fatalf("complete root metadata lost: primary=%+v maxdoc=%d free=%d",
			got.State.PrimaryRoot, got.State.MaxDocumentBytes, got.FreeDelta.Len())
	}
	delta, ok := got.FreeDelta.Latest(layout.DataStart + GlobalTabletCatalogRootBytes)
	if !ok || delta.Op != FreeOpSet {
		t.Fatal("free metadata lookup changed")
	}
}

func TestRootVectorMaximumMemberBankKeepsFinalRoot(t *testing.T) {
	vector := rootVectorTestFixture(t, 1, 2)
	vector.Members = make([]RootVectorMember, RootVectorMaxMembers)
	for index := range vector.Members {
		vector.Members[index] = rootVectorTestMember(t, byte(index+1), uint64(index+1))
	}
	slices.SortFunc(vector.Members, compareRootVectorMembers)
	bankBytes, err := RootVectorBankBytes(len(vector.Members))
	if err != nil {
		t.Fatal(err)
	}
	if bankBytes*2 != 499712 {
		t.Fatalf("maximum vector bytes = %d, want 499712", bankBytes*2)
	}
	encoded := make([]byte, bankBytes)
	if _, err := EncodeRootVectorBank(encoded, vector); err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRootVectorBank(encoded)
	if err != nil {
		t.Fatal(err)
	}
	last := len(vector.Members) - 1
	if decoded.Members[last].Root.Generation != vector.Members[last].Root.Generation ||
		decoded.Members[last].Root.State != vector.Members[last].Root.State {
		t.Fatal("maximum-member checksum damaged final complete root")
	}
}
