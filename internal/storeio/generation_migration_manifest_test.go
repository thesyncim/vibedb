package storeio

import (
	"bytes"
	"errors"
	"testing"
)

func TestGenerationMigrationManifestCanonicalAndCrashRejecting(t *testing.T) {
	m := GenerationMigrationManifest{
		StoreID: [16]byte{1}, MigrationID: [16]byte{2},
		Phase:            GenerationMigrationCatchingUp,
		SourceGeneration: 41, TargetGeneration: 42,
		CapturedSequence: 99, AppliedSequence: 87,
		SourceFileEnd: 1 << 20, TargetFileEnd: 512 << 10,
		ReservedOffset: 2 << 20, ReservedBytes: 8 << 20,
		FirstLogicalID: 100, LogicalIDCount: 1000,
		SourcePrimaryRoot: PageRef{
			Offset: 64 << 10, LogicalID: GlobalTabletCatalogRootLogicalID,
			Generation: 41, Length: GlobalTabletCatalogRootBytes,
			Kind: PagePrimaryCatalog,
		},
		SourceCatalogHead:    PageRef{Offset: 128 << 10, LogicalID: 8, Generation: 41, Length: 4096, Kind: PageCatalogSegment},
		SourceExactIndexRoot: PageRef{Offset: 192 << 10, LogicalID: 9, Generation: 41, Length: 4096, Kind: PagePrimaryExactRoot},
		SourceCatalogBytes:   PageCatalogCanonicalHeaderSize,
		SourceIndexCount:     2,
		Cursor:               []byte("row-0000042"),
	}
	first, err := EncodeGenerationMigrationManifest(
		make([]byte, GenerationMigrationManifestBytes), m,
	)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenGenerationMigrationManifest(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := EncodeGenerationMigrationManifest(
		make([]byte, GenerationMigrationManifestBytes), opened,
	)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("canonical re-encode: %v equal=%v", err, bytes.Equal(first, second))
	}
	for _, cut := range []int{0, 1, generationMigrationHeaderBytes, len(first) - 1} {
		if _, err := OpenGenerationMigrationManifest(first[:cut]); !errors.Is(err, ErrGenerationMigrationManifestCorrupt) {
			t.Fatalf("cut %d error = %v", cut, err)
		}
	}
	corrupt := bytes.Clone(first)
	corrupt[generationMigrationHeaderBytes] ^= 1
	if _, err := OpenGenerationMigrationManifest(corrupt); !errors.Is(err, ErrGenerationMigrationManifestCorrupt) {
		t.Fatalf("corrupt cursor error = %v", err)
	}
	next := m
	next.Phase = GenerationMigrationReady
	next.AppliedSequence = next.CapturedSequence
	next.TargetFileEnd++
	next.TargetPrimaryRoot = PageRef{
		Offset: m.ReservedOffset, LogicalID: GlobalTabletCatalogRootLogicalID,
		Generation: 42, Length: GlobalTabletCatalogRootBytes,
		Kind: PagePrimaryCatalog,
	}
	if err := ValidateGenerationMigrationAdvance(m, next); err != nil {
		t.Fatalf("valid advance: %v", err)
	}
	for _, invalid := range []GenerationMigrationManifest{
		func() GenerationMigrationManifest { n := next; n.Phase = GenerationMigrationCopying; return n }(),
		func() GenerationMigrationManifest { n := next; n.AppliedSequence--; return n }(),
		func() GenerationMigrationManifest { n := next; n.MigrationID[0]++; return n }(),
	} {
		if err := ValidateGenerationMigrationAdvance(m, invalid); err == nil {
			t.Fatalf("invalid advance accepted: %+v", invalid)
		}
	}
}
