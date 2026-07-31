package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
)

type pageCatalogExtentReader struct {
	base int64
	data []byte
}

func (r pageCatalogExtentReader) ReadAt(dst []byte, offset int64) (int, error) {
	if offset < r.base {
		return 0, io.EOF
	}
	start := offset - r.base
	if start >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(dst, r.data[start:])
	if n != len(dst) {
		return n, io.EOF
	}
	return n, nil
}

func TestOpenPageCatalogChainAtAllocationQuanta(t *testing.T) {
	definition := PageCatalogDefinition{
		Indexes: []PageCatalogIndex{
			{Name: "by_tenant", Paths: []string{"/tenant"}},
			{Name: "tenant_alias", Paths: []string{"/tenant"}},
			{Name: "by_tenant_status", Paths: []string{"/tenant", "/status"}},
		},
		Schema: &PageCatalogSchema{Root: PageCatalogSchemaObject},
	}
	for i := range 2_000 {
		definition.Schema.Fields = append(
			definition.Schema.Fields,
			PageCatalogSchemaField{
				Path: fmt.Sprintf(
					"/wide/%04d/%s", i, strings.Repeat("x", 64),
				),
				Types:    PageCatalogSchemaString,
				Required: i&1 != 0,
			},
		)
	}
	catalog, err := BuildCanonicalPageCatalog(definition)
	if err != nil {
		t.Fatal(err)
	}

	for _, pageSize := range []uint32{4 << 10, 16 << 10, 64 << 10} {
		t.Run(fmt.Sprintf("%dKiB", pageSize>>10), func(t *testing.T) {
			pages, bounds := encodePageCatalogTestChainAtPageSize(
				t, catalog, testStoreID, 17, pageSize,
			)
			if len(pages) < 2 {
				t.Fatalf(
					"%d-byte catalog unexpectedly used %d page",
					catalog.CanonicalSize(), len(pages),
				)
			}
			reader := pageCatalogTestExtent(pages, bounds)
			scratch := make([]byte, pageSize)
			opened, openErr := OpenPageCatalogChainAt(
				reader, pages[0].Ref, bounds, scratch,
			)
			if openErr != nil || !opened.Equal(catalog) {
				t.Fatalf(
					"OpenPageCatalogChainAt equal=%v error=%v",
					openErr == nil && opened.Equal(catalog), openErr,
				)
			}
			clear(scratch)
			for i := range reader.data {
				reader.data[i] ^= byte(i*31 + 7)
			}
			if !opened.Equal(catalog) {
				t.Fatal("opened catalog aliases scratch or ReaderAt storage")
			}
		})
	}
}

func TestOpenPageCatalogChainAtRejectsNonContiguousAndGraftedChains(
	t *testing.T,
) {
	catalog := pageCatalogStreamingTestCatalog(t)
	pages, bounds := encodePageCatalogTestChain(
		t, catalog, testStoreID, 23,
	)
	head := pages[0].Ref

	open := func(
		t *testing.T,
		reader pageCatalogExtentReader,
		candidateHead PageRef,
		candidateBounds PageCatalogBounds,
		scratchBytes int,
	) error {
		t.Helper()
		_, err := OpenPageCatalogChainAt(
			reader, candidateHead, candidateBounds,
			make([]byte, scratchBytes),
		)
		return err
	}

	t.Run("encoded-next-is-not-derived-reference", func(t *testing.T) {
		reader := pageCatalogTestExtent(pages, bounds)
		first := reader.data[:bounds.PageSize]
		nextOffset := PageHeaderSize + 32
		binary.LittleEndian.PutUint64(
			first[nextOffset:nextOffset+8],
			head.Offset+2*uint64(bounds.PageSize),
		)
		if _, err := SealPage(first); err != nil {
			t.Fatal(err)
		}
		if err := open(
			t, reader, head, bounds, int(bounds.PageSize),
		); !errors.Is(err, ErrPageCatalogCorrupt) {
			t.Fatalf("non-contiguous Next = %v", err)
		}
	})

	t.Run("page-identity-does-not-match-derived-reference", func(t *testing.T) {
		reader := pageCatalogTestExtent(pages, bounds)
		second := reader.data[bounds.PageSize : 2*bounds.PageSize]
		binary.LittleEndian.PutUint64(
			second[32:40], pages[1].Ref.LogicalID+1,
		)
		if _, err := SealPage(second); err != nil {
			t.Fatal(err)
		}
		if err := open(
			t, reader, head, bounds, int(bounds.PageSize),
		); !errors.Is(err, ErrPageCatalogCorrupt) {
			t.Fatalf("logical graft = %v", err)
		}
	})

	t.Run("non-zero-tail", func(t *testing.T) {
		reader := pageCatalogTestExtent(pages, bounds)
		lastStart := (len(pages) - 1) * int(bounds.PageSize)
		last := reader.data[lastStart : lastStart+int(bounds.PageSize)]
		encodePageRef(last[PageHeaderSize+32:], head)
		if _, err := SealPage(last); err != nil {
			t.Fatal(err)
		}
		if err := open(
			t, reader, head, bounds, int(bounds.PageSize),
		); !errors.Is(err, ErrPageCatalogCorrupt) {
			t.Fatalf("non-zero tail = %v", err)
		}
	})

	t.Run("reordered-physical-pages", func(t *testing.T) {
		reader := pageCatalogTestExtent(pages, bounds)
		first := slices.Clone(reader.data[:bounds.PageSize])
		copy(
			reader.data[:bounds.PageSize],
			reader.data[bounds.PageSize:2*bounds.PageSize],
		)
		copy(reader.data[bounds.PageSize:2*bounds.PageSize], first)
		if err := open(
			t, reader, head, bounds, int(bounds.PageSize),
		); !errors.Is(err, ErrPageCatalogCorrupt) {
			t.Fatalf("reordered pages = %v", err)
		}
	})

	t.Run("wrong-root-total", func(t *testing.T) {
		reader := pageCatalogTestExtent(pages, bounds)
		wrong := bounds
		wrong.TotalBytes--
		if err := open(
			t, reader, head, wrong, int(bounds.PageSize),
		); !errors.Is(err, ErrPageCatalogCorrupt) {
			t.Fatalf("wrong total = %v", err)
		}
	})

	t.Run("wrong-mandatory-digest", func(t *testing.T) {
		reader := pageCatalogTestExtent(pages, bounds)
		wrong := bounds
		wrong.ExpectedDigest[0] ^= 1
		if err := open(
			t, reader, head, wrong, int(bounds.PageSize),
		); !errors.Is(err, ErrPageCatalogCorrupt) {
			t.Fatalf("wrong digest = %v", err)
		}
	})

	t.Run("head-and-extent-bounds", func(t *testing.T) {
		reader := pageCatalogTestExtent(pages, bounds)
		for _, mutate := range []func(*PageRef, *PageCatalogBounds){
			func(ref *PageRef, _ *PageCatalogBounds) {
				ref.Offset += uint64(bounds.PageSize)
			},
			func(ref *PageRef, _ *PageCatalogBounds) { ref.LogicalID++ },
			func(ref *PageRef, _ *PageCatalogBounds) { ref.Generation++ },
			func(_ *PageRef, b *PageCatalogBounds) {
				b.FileEnd -= uint64(bounds.PageSize)
			},
			func(_ *PageRef, b *PageCatalogBounds) {
				b.FileEnd = maxSuperblockFileOffset + 1
			},
			func(_ *PageRef, b *PageCatalogBounds) { b.NextLogicalID-- },
		} {
			candidateHead, candidateBounds := head, bounds
			mutate(&candidateHead, &candidateBounds)
			if err := open(
				t, reader, candidateHead, candidateBounds,
				int(bounds.PageSize),
			); !errors.Is(err, ErrPageCatalogCorrupt) {
				t.Fatalf(
					"head=%+v bounds=%+v error=%v",
					candidateHead, candidateBounds, err,
				)
			}
		}
	})

	t.Run("short-reader-and-scratch", func(t *testing.T) {
		reader := pageCatalogTestExtent(pages, bounds)
		reader.data = reader.data[:len(reader.data)-1]
		if err := open(
			t, reader, head, bounds, int(bounds.PageSize),
		); !errors.Is(err, ErrPageCatalogCorrupt) {
			t.Fatalf("short ReaderAt = %v", err)
		}
		reader = pageCatalogTestExtent(pages, bounds)
		if err := open(
			t, reader, head, bounds, int(bounds.PageSize)-1,
		); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf("short scratch = %v", err)
		}
	})
}

func TestPageCatalogAlwaysRequiresExactDigest(t *testing.T) {
	catalog, err := BuildCanonicalPageCatalog(PageCatalogDefinition{
		Schema: &PageCatalogSchema{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pages, bounds := encodePageCatalogTestChain(
		t, catalog, testStoreID, 31,
	)
	bounds.ExpectedDigest = [PageCatalogDigestSize]byte{}
	if _, err := OpenPageCatalogSegment(
		pages[0].Page, bounds,
	); !errors.Is(err, ErrPageCatalogCorrupt) {
		t.Fatalf("all-zero expected digest = %v", err)
	}
	bounds.TotalBytes = uint32(catalog.CanonicalSize())
	reader := pageCatalogTestExtent(pages, bounds)
	if _, err := OpenPageCatalogChainAt(
		reader, pages[0].Ref, bounds,
		make([]byte, bounds.PageSize),
	); !errors.Is(err, ErrPageCatalogCorrupt) {
		t.Fatalf("streaming all-zero expected digest = %v", err)
	}
}

func pageCatalogStreamingTestCatalog(
	t testing.TB,
) *CanonicalPageCatalog {
	t.Helper()
	definition := PageCatalogDefinition{
		Schema: &PageCatalogSchema{Root: PageCatalogSchemaObject},
	}
	for i := range 900 {
		definition.Schema.Fields = append(
			definition.Schema.Fields,
			PageCatalogSchemaField{
				Path:  fmt.Sprintf("/records/%04d/value", i),
				Types: PageCatalogSchemaNumber,
			},
		)
	}
	catalog, err := BuildCanonicalPageCatalog(definition)
	if err != nil {
		t.Fatal(err)
	}
	if catalog.SegmentCount() < 3 {
		t.Fatalf("streaming fixture uses %d segments", catalog.SegmentCount())
	}
	return catalog
}

func pageCatalogTestExtent(
	pages []PageCatalogChainPage,
	bounds PageCatalogBounds,
) pageCatalogExtentReader {
	pageSize := int(bounds.PageSize)
	data := make([]byte, len(pages)*pageSize)
	for i, page := range pages {
		copy(data[i*pageSize:(i+1)*pageSize], page.Page)
	}
	return pageCatalogExtentReader{
		base: int64(bounds.DataStart),
		data: data,
	}
}
