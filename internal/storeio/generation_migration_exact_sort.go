package storeio

import (
	"bytes"
	"fmt"
	"slices"
)

// GenerationMigrationExactRunRegion is a contiguous sequence of fixed-size
// authenticated run pages. Contiguity lets merge passes rediscover every run
// boundary by scanning headers, without retaining one descriptor per run.
type GenerationMigrationExactRunRegion struct {
	First PageRef
	Pages uint64
	Runs  uint64
}

func (r GenerationMigrationExactRunRegion) RefAt(page uint64) (PageRef, bool) {
	if r.First == (PageRef{}) || r.First.Kind != PageMigrationExactRun ||
		page >= r.Pages || page > (^uint64(0)-r.First.Offset)/uint64(r.First.Length) ||
		page > ^uint64(0)-r.First.LogicalID {
		return PageRef{}, false
	}
	ref := r.First
	ref.Offset += page * uint64(ref.Length)
	ref.LogicalID += page
	return ref, true
}

// GenerationMigrationExactRunBuilder bounds the unsorted ingestion window by
// both descriptors and canonical-key bytes. A full window is sorted and
// coalesced into one same-file run before its memory is reused.
type GenerationMigrationExactRunBuilder struct {
	sink      PrimaryGraphBuildSink
	pageBytes uint32
	records   []GenerationMigrationExactRunRecord
	keyArena  []byte
	region    GenerationMigrationExactRunRegion
	nextRunID uint64
}

func NewGenerationMigrationExactRunBuilder(sink PrimaryGraphBuildSink, pageBytes uint32, recordWindow, keyWindowBytes int) (*GenerationMigrationExactRunBuilder, error) {
	if sink == nil || !validPhysicalPageSize(pageBytes) || pageBytes > uint32(sink.MaxBuildPageBytes()) || recordWindow < 2 || keyWindowBytes < IndexTermMaxKeyBytes {
		return nil, fmt.Errorf("%w: exact run builder", ErrInvalidWrite)
	}
	return &GenerationMigrationExactRunBuilder{sink: sink, pageBytes: pageBytes, records: make([]GenerationMigrationExactRunRecord, 0, recordWindow), keyArena: make([]byte, 0, keyWindowBytes), nextRunID: 1}, nil
}

func (b *GenerationMigrationExactRunBuilder) Add(indexID uint32, key []byte, tileID uint32, mask uint64) error {
	if b == nil || mask == 0 || len(key) == 0 || len(key) > IndexTermMaxKeyBytes || !ValidIndexTermKey(key) || generationMigrationExactRunHeaderBytes+generationMigrationExactRunRecordBytes+len(key) > int(b.pageBytes)-PageHeaderSize-PageTrailerSize {
		return fmt.Errorf("%w: exact run record", ErrInvalidWrite)
	}
	if len(b.records) == cap(b.records) || len(key) > cap(b.keyArena)-len(b.keyArena) {
		if err := b.flush(); err != nil {
			return err
		}
	}
	if len(b.records) == cap(b.records) || len(key) > cap(b.keyArena)-len(b.keyArena) {
		return fmt.Errorf("%w: exact run window", ErrInvalidWrite)
	}
	start := len(b.keyArena)
	b.keyArena = append(b.keyArena, key...)
	b.records = append(b.records, GenerationMigrationExactRunRecord{IndexID: indexID, Key: b.keyArena[start:len(b.keyArena):len(b.keyArena)], TileID: tileID, Mask: mask})
	return nil
}

func (b *GenerationMigrationExactRunBuilder) Finish() (GenerationMigrationExactRunRegion, error) {
	if b == nil {
		return GenerationMigrationExactRunRegion{}, ErrInvalidWrite
	}
	if err := b.flush(); err != nil {
		return GenerationMigrationExactRunRegion{}, err
	}
	if b.region.Pages == 0 {
		return GenerationMigrationExactRunRegion{}, nil
	}
	return b.region, nil
}

func (b *GenerationMigrationExactRunBuilder) flush() error {
	if len(b.records) == 0 {
		return nil
	}
	slices.SortFunc(b.records, compareGenerationMigrationExactRunRecord)
	out := 0
	for index := range b.records {
		if out != 0 && compareGenerationMigrationExactRunRecord(b.records[out-1], b.records[index]) == 0 {
			b.records[out-1].Mask |= b.records[index].Mask
			continue
		}
		b.records[out] = b.records[index]
		out++
	}
	b.records = b.records[:out]
	for first, ordinal := 0, uint32(0); first < len(b.records); ordinal++ {
		payloadBytes := generationMigrationExactRunHeaderBytes
		last := first
		for last < len(b.records) {
			next := generationMigrationExactRunRecordBytes + len(b.records[last].Key)
			if payloadBytes+next > int(b.pageBytes)-PageHeaderSize-PageTrailerSize {
				break
			}
			payloadBytes += next
			last++
		}
		if last == first {
			return fmt.Errorf("%w: exact run page progress", ErrInvalidWrite)
		}
		page, err := b.sink.AllocatePage(PageMigrationExactRun, b.pageBytes, 0)
		if err != nil {
			return err
		}
		ref := page.Ref()
		if b.region.Pages == 0 {
			b.region.First = ref
		} else {
			want := b.region.First
			if b.region.Pages > (^uint64(0)-want.Offset)/uint64(want.Length) ||
				b.region.Pages > ^uint64(0)-want.LogicalID {
				return fmt.Errorf("%w: exact run region overflow", ErrInvalidWrite)
			}
			want.Offset += b.region.Pages * uint64(want.Length)
			want.LogicalID += b.region.Pages
			if ref != want {
				return fmt.Errorf("%w: non-contiguous exact run", ErrInvalidWrite)
			}
		}
		if _, err := EncodeGenerationMigrationExactRunPage(page.Bytes(), b.sink.StoreIdentity(), b.sink.BuildGeneration(), ref.LogicalID, b.nextRunID, ordinal, last == len(b.records), b.records[first:last]); err != nil {
			return err
		}
		if err := page.Stage(); err != nil {
			return err
		}
		b.region.Pages++
		first = last
	}
	b.region.Runs++
	b.nextRunID++
	b.records = b.records[:0]
	b.keyArena = b.keyArena[:0]
	return nil
}

func compareGenerationMigrationExactRunRecord(a, b GenerationMigrationExactRunRecord) int {
	if a.IndexID < b.IndexID {
		return -1
	}
	if a.IndexID > b.IndexID {
		return 1
	}
	if cmp := bytes.Compare(a.Key, b.Key); cmp != 0 {
		return cmp
	}
	if a.TileID < b.TileID {
		return -1
	}
	if a.TileID > b.TileID {
		return 1
	}
	return 0
}
