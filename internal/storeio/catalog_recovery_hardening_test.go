package storeio

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

type exactCatalogRecoveryFixture struct {
	file     *os.File
	layout   MutableStoreFileLayout
	pageSize uint32
	catalog  *CanonicalPageCatalog
	pages    []PageCatalogChainPage
	root     InlineSuperblock
	extraRef PageRef
}

type protectedFreeLogWriter struct {
	t        *testing.T
	file     *os.File
	cache    *PageCache
	pageSize uint32
	fileEnd  uint64
	next     uint64
	logical  uint64
}

func newProtectedFreeLogWriter(
	t *testing.T,
	pageSize uint32,
) *protectedFreeLogWriter {
	t.Helper()
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "protected-free-log-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	fileEnd := uint64(256) * uint64(pageSize)
	if err := file.Truncate(int64(fileEnd)); err != nil {
		t.Fatal(err)
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: int(pageSize), MaxPageSize: int(pageSize),
		ResidentBytes: 8 * int64(pageSize),
		StoreID:       testStoreID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return &protectedFreeLogWriter{
		t: t, file: file, cache: cache, pageSize: pageSize,
		fileEnd: fileEnd,
		next:    layout.DataStart/uint64(pageSize) + 4,
		logical: 2,
	}
}

func (w *protectedFreeLogWriter) extent(
	page, pages, generation uint64,
) FreeExtent {
	return FreeExtent{
		Offset:            page * uint64(w.pageSize),
		Length:            pages * uint64(w.pageSize),
		RetiredGeneration: generation,
	}
}

func (w *protectedFreeLogWriter) write(
	kind PageKind,
	encode func([]byte, FreeLogHeader) error,
) PageRef {
	w.t.Helper()
	page := make([]byte, w.pageSize)
	header := FreeLogHeader{
		StoreID: testStoreID, Generation: 3,
		LogicalID: w.logical, PageSize: w.pageSize,
	}
	if err := encode(page, header); err != nil {
		w.t.Fatal(err)
	}
	ref := PageRef{
		Offset:    w.next * uint64(w.pageSize),
		LogicalID: w.logical, Generation: 3,
		Length: w.pageSize, Kind: kind,
	}
	if _, err := w.file.WriteAt(page, int64(ref.Offset)); err != nil {
		w.t.Fatal(err)
	}
	w.next++
	w.logical++
	return ref
}

func (w *protectedFreeLogWriter) image(
	extents []FreeExtent,
) PageRef {
	return w.write(
		PageFreeImage,
		func(page []byte, header FreeLogHeader) error {
			_, err := EncodeFreeImagePage(
				page, header, extents, w.fileEnd, w.logical+1,
			)
			return err
		},
	)
}

func (w *protectedFreeLogWriter) index(
	ref PageRef,
	extents []FreeExtent,
) PageRef {
	largest := uint64(0)
	for _, extent := range extents {
		largest = max(largest, extent.Length)
	}
	segment := FreeSegment{
		Ref: ref, FirstOffset: extents[0].Offset,
		LargestFree: largest, Count: uint32(len(extents)),
	}
	return w.write(
		PageFreeIndex,
		func(page []byte, header FreeLogHeader) error {
			_, err := EncodeFreeIndexPage(
				page, header, []FreeSegment{segment}, PageRef{},
				w.fileEnd, w.logical+1,
			)
			return err
		},
	)
}

func (w *protectedFreeLogWriter) bounds(
	protected FreeExtent,
) FreeLogBounds {
	return FreeLogBounds{
		FileEnd: w.fileEnd, NextLogicalID: w.logical + 1,
		ProtectedExtent: protected,
	}
}

func newExactCatalogRecoveryFixture(
	t *testing.T,
	catalog *CanonicalPageCatalog,
	pageSize uint32,
) *exactCatalogRecoveryFixture {
	t.Helper()
	const catalogGeneration = uint64(1)
	pages, catalogBounds := encodePageCatalogTestChainAtPageSize(
		t, catalog, testStoreID, catalogGeneration, pageSize,
	)
	if len(pages) < 2 {
		t.Fatalf("catalog uses %d %d-byte page(s), want multiple", len(pages), pageSize)
	}
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "exact-catalog-recovery-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })

	extraRef := PageRef{
		Offset:     catalogBounds.FileEnd,
		LogicalID:  catalogBounds.NextLogicalID,
		Generation: 8,
		Length:     pageSize,
		Kind:       PageFloat64Catalog,
	}
	fileEnd := extraRef.Offset + uint64(pageSize)
	if err := file.Truncate(int64(fileEnd)); err != nil {
		t.Fatal(err)
	}
	for _, page := range pages {
		if _, err := file.WriteAt(page.Page, int64(page.Ref.Offset)); err != nil {
			t.Fatal(err)
		}
	}
	state := StateRoot{
		StoreID:           testStoreID,
		Generation:        8,
		PageSize:          pageSize,
		MaxPageSize:       pageSize,
		NextLogicalID:     extraRef.LogicalID + 1,
		ChunkDocuments:    64,
		Options:           StateOptionFloat64Columns | StateOptionSchema,
		IndexCatalogHash:  1,
		PageCatalogHead:   pages[0].Ref,
		PageCatalogDigest: catalog.Digest(),
		PageCatalogBytes:  uint32(catalog.CanonicalSize()),
	}
	return &exactCatalogRecoveryFixture{
		file: file, layout: layout, pageSize: pageSize,
		catalog: catalog, pages: pages, extraRef: extraRef,
		root: InlineSuperblock{
			StoreID: testStoreID, Generation: 8,
			FileEnd: fileEnd, PageSize: pageSize, State: state,
		},
	}
}

func (f *exactCatalogRecoveryFixture) writeRoot(
	t *testing.T,
	slot int,
	root InlineSuperblock,
	corrupt bool,
) {
	t.Helper()
	root.Generation = root.State.Generation
	page := make([]byte, f.pageSize)
	if _, err := EncodeInlineSuperblock(page, root); err != nil {
		t.Fatal(err)
	}
	if corrupt {
		page[0] ^= 0x80
	}
	if _, err := f.file.WriteAt(
		page, int64(f.layout.RootOffsets[slot]),
	); err != nil {
		t.Fatal(err)
	}
}

func (f *exactCatalogRecoveryFixture) roots(
	t *testing.T,
	mutateNewest func(*InlineSuperblock),
) {
	t.Helper()
	older := f.root
	older.Generation = 7
	older.State.Generation = 7
	newest := f.root
	if mutateNewest != nil {
		mutateNewest(&newest)
	}
	f.writeRoot(t, 0, older, false)
	f.writeRoot(t, 1, newest, false)
}

func (f *exactCatalogRecoveryFixture) recover() (
	MutableInlineRecovery,
	error,
) {
	return RecoverMutableInlineStateRoot(
		f.file, f.pageSize, 0, make([]byte, f.pageSize),
	)
}

func TestMutableRecoveryAuthenticatesCompleteCatalogRunAtEveryPageSize(
	t *testing.T,
) {
	definition := PageCatalogDefinition{
		Float64Paths: []string{"/score"},
		Schema:       &PageCatalogSchema{Root: PageCatalogSchemaObject},
	}
	for i := range 2_400 {
		definition.Schema.Fields = append(
			definition.Schema.Fields,
			PageCatalogSchemaField{
				Path: fmt.Sprintf(
					"/wide/%04d/%s", i, strings.Repeat("x", 64),
				),
				Types: PageCatalogSchemaString,
			},
		)
	}
	catalog, err := BuildCanonicalPageCatalog(definition)
	if err != nil {
		t.Fatal(err)
	}

	for _, pageSize := range []uint32{4 << 10, 16 << 10, 64 << 10} {
		t.Run(fmt.Sprintf("%dKiB", pageSize>>10), func(t *testing.T) {
			t.Run("valid-complete-run", func(t *testing.T) {
				fixture := newExactCatalogRecoveryFixture(t, catalog, pageSize)
				fixture.roots(t, nil)
				result, recoverErr := fixture.recover()
				if recoverErr != nil || result.Root.Generation != 8 ||
					result.FallbackGeneration != 7 ||
					result.Catalog == nil ||
					!result.Catalog.Equal(catalog) {
					t.Fatalf("recovery = (%+v, %v)", result, recoverErr)
				}
			})

			t.Run("checksummed-corrupt-tail", func(t *testing.T) {
				fixture := newExactCatalogRecoveryFixture(t, catalog, pageSize)
				fixture.roots(t, nil)
				last := append([]byte(nil), fixture.pages[len(fixture.pages)-1].Page...)
				encodePageRef(last[PageHeaderSize+32:], fixture.pages[0].Ref)
				if _, err := SealPage(last); err != nil {
					t.Fatal(err)
				}
				if _, err := fixture.file.WriteAt(
					last,
					int64(fixture.pages[len(fixture.pages)-1].Ref.Offset),
				); err != nil {
					t.Fatal(err)
				}
				if _, recoverErr := fixture.recover(); !errors.Is(
					recoverErr, ErrSuperblockNotFound,
				) || !errors.Is(recoverErr, ErrPageCatalogCorrupt) {
					t.Fatalf("corrupt tail recovery = %v", recoverErr)
				}
			})

			t.Run("checksummed-graft", func(t *testing.T) {
				fixture := newExactCatalogRecoveryFixture(t, catalog, pageSize)
				fixture.roots(t, nil)
				otherStore := testStoreID
				otherStore[0] ^= 0xff
				graft, _ := encodePageCatalogTestChainAtPageSize(
					t, catalog, otherStore, 1, pageSize,
				)
				if _, err := fixture.file.WriteAt(
					graft[1].Page, int64(fixture.pages[1].Ref.Offset),
				); err != nil {
					t.Fatal(err)
				}
				if _, recoverErr := fixture.recover(); !errors.Is(
					recoverErr, ErrSuperblockNotFound,
				) || !errors.Is(recoverErr, ErrPageCatalogCorrupt) {
					t.Fatalf("grafted tail recovery = %v", recoverErr)
				}
			})

			t.Run("all-zero-root-digest-is-not-optional", func(t *testing.T) {
				fixture := newExactCatalogRecoveryFixture(t, catalog, pageSize)
				fixture.root.State.PageCatalogDigest =
					[PageCatalogDigestSize]byte{}
				fixture.roots(t, nil)
				if _, recoverErr := fixture.recover(); !errors.Is(
					recoverErr, ErrSuperblockNotFound,
				) || !errors.Is(recoverErr, ErrPageCatalogCorrupt) {
					t.Fatalf("zero digest recovery = %v", recoverErr)
				}
			})

			t.Run("typed-newest-falls-back-after-catalog-validation", func(t *testing.T) {
				fixture := newExactCatalogRecoveryFixture(t, catalog, pageSize)
				malformed := make([]byte, pageSize)
				if _, err := InitPage(malformed, PageHeader{
					StoreID: testStoreID, Generation: 8,
					LogicalID: fixture.extraRef.LogicalID,
					PageSize:  pageSize, Kind: PageFloat64Catalog,
				}); err != nil {
					t.Fatal(err)
				}
				if _, err := SealPage(malformed); err != nil {
					t.Fatal(err)
				}
				if _, err := fixture.file.WriteAt(
					malformed, int64(fixture.extraRef.Offset),
				); err != nil {
					t.Fatal(err)
				}
				fixture.roots(t, func(root *InlineSuperblock) {
					root.State.Float64ScanHead = fixture.extraRef
				})
				result, recoverErr := fixture.recover()
				if recoverErr != nil || result.Root.Generation != 7 ||
					result.RootSlot != 0 ||
					result.FallbackGeneration != 7 {
					t.Fatalf("fallback recovery = (%+v, %v)", result, recoverErr)
				}
			})

			t.Run("inline-free-overlap-is-unencodable", func(t *testing.T) {
				fixture := newExactCatalogRecoveryFixture(t, catalog, pageSize)
				catalogExtent, ok := StateRootPageCatalogExtent(
					fixture.root.State,
				)
				if !ok {
					t.Fatal("missing catalog extent")
				}
				catalogExtent.RetiredGeneration = 7
				delta := NewInlineFreeDelta(PageRef{}, PageRef{})
				if err := delta.Append(
					[]FreeDelta{{Op: FreeOpSet, Extent: catalogExtent}},
					pageSize, fixture.root.FileEnd,
				); err != nil {
					t.Fatal(err)
				}
				fixture.root.FreeDelta = delta
				if _, err := EncodeInlineSuperblock(
					make([]byte, pageSize), fixture.root,
				); !errors.Is(err, ErrInvalidWrite) {
					t.Fatalf("catalog/free overlap = %v", err)
				}
			})
		})
	}
}

func TestFreeLogReplayRejectsCatalogRunFromEverySource(t *testing.T) {
	protected := freeLogTestExtent(20, 2, 0)
	boundsWithProtection := func(w *freeLogTestWriter) FreeLogBounds {
		bounds := w.bounds()
		bounds.ProtectedExtent = protected
		return bounds
	}

	t.Run("external-image", func(t *testing.T) {
		w := newFreeLogTestWriter(t)
		image := []FreeExtent{
			freeLogTestExtent(10, 1, 1),
			freeLogTestExtent(20, 2, 1),
		}
		page := w.image(image)
		index := w.index([]FreeSegment{segmentOf(page, image)}, PageRef{})
		inline := NewInlineFreeDelta(PageRef{}, index)
		got, _, err := ReplayInlineFreeLog(
			w.cache, &inline, boundsWithProtection(w),
			nil, 16, 1,
		)
		if !errors.Is(err, ErrFreeLogCorrupt) || len(got) != 0 {
			t.Fatalf("image overlap = (%+v, %v)", got, err)
		}
	})

	t.Run("external-delta", func(t *testing.T) {
		w := newFreeLogTestWriter(t)
		image := []FreeExtent{freeLogTestExtent(40, 1, 1)}
		page := w.image(image)
		index := w.index([]FreeSegment{segmentOf(page, image)}, PageRef{})
		external := w.delta(
			[]FreeDelta{{
				Op:     FreeOpSet,
				Extent: freeLogTestExtent(20, 1, 2),
			}},
			PageRef{}, index,
		)
		inline := NewInlineFreeDelta(external, index)
		got, _, err := ReplayInlineFreeLog(
			w.cache, &inline, boundsWithProtection(w),
			nil, 16, 1,
		)
		if !errors.Is(err, ErrFreeLogCorrupt) || len(got) != 0 {
			t.Fatalf("external delta overlap = (%+v, %v)", got, err)
		}
	})

	t.Run("inline-delta", func(t *testing.T) {
		w := newFreeLogTestWriter(t)
		inline := NewInlineFreeDelta(PageRef{}, PageRef{})
		if err := inline.Append(
			[]FreeDelta{{
				Op:     FreeOpSet,
				Extent: freeLogTestExtent(20, 1, 3),
			}},
			testSuperblockPageSize, freeLogTestFileEnd,
		); err != nil {
			t.Fatal(err)
		}
		got, _, err := ReplayInlineFreeLog(
			w.cache, &inline, boundsWithProtection(w),
			nil, 16, 1,
		)
		if !errors.Is(err, ErrFreeLogCorrupt) || len(got) != 0 {
			t.Fatalf("inline delta overlap = (%+v, %v)", got, err)
		}
	})

	t.Run("existing-destination", func(t *testing.T) {
		w := newFreeLogTestWriter(t)
		dst := []FreeExtent{freeLogTestExtent(20, 1, 1)}
		got, _, err := ReplayInlineFreeLog(
			w.cache, &InlineFreeDelta{}, boundsWithProtection(w),
			dst, len(dst), 1,
		)
		if !errors.Is(err, ErrFreeLogCorrupt) ||
			len(got) != len(dst) {
			t.Fatalf("destination overlap = (%+v, %v)", got, err)
		}
	})
}

func TestFreeLogReplayCatalogFenceAtEveryPageSize(t *testing.T) {
	for _, pageSize := range []uint32{4 << 10, 16 << 10, 64 << 10} {
		t.Run(fmt.Sprintf("%dKiB", pageSize>>10), func(t *testing.T) {
			t.Run("external-image", func(t *testing.T) {
				w := newProtectedFreeLogWriter(t, pageSize)
				protected := w.extent(20, 2, 0)
				image := []FreeExtent{
					w.extent(10, 1, 1),
					w.extent(20, 2, 1),
				}
				imageRef := w.image(image)
				indexRef := w.index(imageRef, image)
				inline := NewInlineFreeDelta(PageRef{}, indexRef)
				got, _, err := ReplayInlineFreeLog(
					w.cache, &inline, w.bounds(protected),
					nil, 16, 1,
				)
				if !errors.Is(err, ErrFreeLogCorrupt) ||
					len(got) != 0 {
					t.Fatalf("external image overlap = (%+v, %v)", got, err)
				}
			})

			t.Run("inline-delta", func(t *testing.T) {
				w := newProtectedFreeLogWriter(t, pageSize)
				protected := w.extent(20, 2, 0)
				inline := NewInlineFreeDelta(PageRef{}, PageRef{})
				if err := inline.Append(
					[]FreeDelta{{
						Op:     FreeOpSet,
						Extent: w.extent(20, 1, 3),
					}},
					pageSize, w.fileEnd,
				); err != nil {
					t.Fatal(err)
				}
				got, _, err := ReplayInlineFreeLog(
					w.cache, &inline, w.bounds(protected),
					nil, 16, 1,
				)
				if !errors.Is(err, ErrFreeLogCorrupt) ||
					len(got) != 0 {
					t.Fatalf("inline delta overlap = (%+v, %v)", got, err)
				}
			})
		})
	}
}

func TestMaterializationCannotTargetCatalogRun(t *testing.T) {
	const pageSize = uint32(4096)
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	header := MaterializationJournalHeader{
		StoreID: testStoreID, Sequence: 1, TargetGeneration: 9,
		PageSize: pageSize, SectorSize: MaterializationJournalMinSectorSize,
	}
	ref := PageRef{
		Offset: layout.DataStart, LogicalID: 2, Generation: 8,
		Length: pageSize, Kind: PageCatalogSegment,
	}
	before := makeMutableRecoveryPage(t, testStoreID, ref, 0x20)
	after := append([]byte(nil), before...)
	after[PageHeaderSize] ^= 1
	if _, err := SealPage(after); err != nil {
		t.Fatal(err)
	}
	patches := []MaterializationPatch{
		{Offset: 0, Data: before[:MaterializationJournalMinSectorSize]},
		{
			Offset: pageSize - MaterializationJournalMinSectorSize,
			Data:   before[pageSize-MaterializationJournalMinSectorSize:],
		},
	}
	if _, err := BuildMaterializationTarget(
		header, ref, before, after, patches, 0,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("direct catalog materialization = %v", err)
	}

	root := InlineSuperblock{
		StoreID: testStoreID, Generation: 8,
		PageSize: pageSize,
		FileEnd:  layout.DataStart + 3*uint64(pageSize),
		State: StateRoot{
			StoreID: testStoreID, Generation: 8,
			PageSize: pageSize, MaxPageSize: pageSize,
			NextLogicalID: 20, ChunkDocuments: 64,
			Options: StateOptionSchema, IndexCatalogHash: 1,
			PageCatalogHead: PageRef{
				Offset: layout.DataStart, LogicalID: 10,
				Generation: 1, Length: pageSize,
				Kind: PageCatalogSegment,
			},
			PageCatalogBytes: PageCatalogCanonicalHeaderSize,
		},
	}
	for _, test := range []struct {
		name string
		ref  PageRef
	}{
		{
			name: "physical-overlap",
			ref: PageRef{
				Offset: layout.DataStart, LogicalID: 2,
				Generation: 8, Length: pageSize, Kind: PageDocument,
			},
		},
		{
			name: "logical-overlap",
			ref: PageRef{
				Offset:    layout.DataStart + uint64(pageSize),
				LogicalID: 10, Generation: 8,
				Length: pageSize, Kind: PageDocument,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := makeMutableRecoveryPage(
				t, testStoreID, test.ref, 0x30,
			)
			after := append([]byte(nil), before...)
			after[PageHeaderSize+1] ^= 1
			if _, err := SealPage(after); err != nil {
				t.Fatal(err)
			}
			patches := []MaterializationPatch{
				{
					Offset: 0,
					Data:   before[:MaterializationJournalMinSectorSize],
				},
				{
					Offset: pageSize - MaterializationJournalMinSectorSize,
					Data:   before[pageSize-MaterializationJournalMinSectorSize:],
				},
			}
			target, err := BuildMaterializationTarget(
				header, test.ref, before, after, patches, 0,
			)
			if err != nil {
				t.Fatal(err)
			}
			encoded := make([]byte, MaterializationJournalSize)
			if _, err := EncodeMaterializationJournal(
				encoded, header,
				[]MaterializationTarget{target}, patches,
			); err != nil {
				t.Fatal(err)
			}
			journal, err := OpenMaterializationJournal(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateJournalTargetsForCandidate(
				journal, root, layout, root.FileEnd,
			); !errors.Is(err, ErrMaterializationJournalConflict) {
				t.Fatalf("catalog overlap = %v", err)
			}
		})
	}
}

func TestRecoveryCatalogRejectsChecksummedCanonicalByteCorruption(
	t *testing.T,
) {
	definition := PageCatalogDefinition{
		Schema: &PageCatalogSchema{Root: PageCatalogSchemaObject},
	}
	for i := range 900 {
		definition.Schema.Fields = append(
			definition.Schema.Fields,
			PageCatalogSchemaField{
				Path: fmt.Sprintf(
					"/canonical/%04d/%s", i, strings.Repeat("x", 32),
				),
				Types: PageCatalogSchemaString,
			},
		)
	}
	catalog, err := BuildCanonicalPageCatalog(definition)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newExactCatalogRecoveryFixture(t, catalog, 4096)
	fixture.roots(t, nil)
	first := append([]byte(nil), fixture.pages[0].Page...)
	first[PageHeaderSize+PageCatalogSegmentPayloadHeaderSize] ^= 1
	if _, err := SealPage(first); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.file.WriteAt(
		first, int64(fixture.pages[0].Ref.Offset),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.recover(); !errors.Is(
		err, ErrSuperblockNotFound,
	) || !errors.Is(err, ErrPageCatalogCorrupt) {
		t.Fatalf("canonical corruption recovery = %v", err)
	}
}
