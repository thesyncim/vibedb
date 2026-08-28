package storeio

import "fmt"

const GenerationMigrationExactMergeMaxFanIn = 64

type GenerationMigrationExactRunReader func(PageRef, []byte) error

type generationMigrationExactRunSpan struct {
	first PageRef
	pages uint64
	runID uint64
}

// MergeGenerationMigrationExactRuns performs fixed-fan-in merge passes. Run
// descriptors are rediscovered from authenticated page headers one group at a
// time, so memory is O(fanIn*pageBytes), independent of source cardinality and
// initial run count. Output pages append through the same unrooted sink.
func MergeGenerationMigrationExactRuns(
	sink PrimaryGraphBuildSink,
	read GenerationMigrationExactRunReader,
	input GenerationMigrationExactRunRegion,
	pageBytes uint32,
	fanIn int,
) (GenerationMigrationExactRunRegion, error) {
	if sink == nil || read == nil || input.First.Kind != PageMigrationExactRun ||
		input.Pages == 0 || input.Runs == 0 || input.First.Length != pageBytes ||
		fanIn < 2 || fanIn > GenerationMigrationExactMergeMaxFanIn {
		return GenerationMigrationExactRunRegion{}, fmt.Errorf("%w: exact run merge", ErrInvalidWrite)
	}
	if input.Runs == 1 {
		return input, nil
	}
	pageScratch := make([]byte, int(pageBytes)*(fanIn+1))
	spans := make([]generationMigrationExactRunSpan, fanIn)
	cursors := make([]generationMigrationExactRunCursor, fanIn)
	writer := generationMigrationExactOrderedRunWriter{
		sink: sink, pageBytes: pageBytes,
		records:  make([]GenerationMigrationExactRunRecord, 0, int(pageBytes)/generationMigrationExactRunRecordBytes),
		keyArena: make([]byte, 0, int(pageBytes)),
	}
	for input.Runs > 1 {
		var output GenerationMigrationExactRunRegion
		pageAt := uint64(0)
		for pageAt < input.Pages {
			spanCount := 0
			for spanCount < fanIn && pageAt < input.Pages {
				span, next, err := scanGenerationMigrationExactRunSpan(read, input, pageAt, pageScratch[:pageBytes], sink.StoreIdentity(), sink.BuildGeneration())
				if err != nil {
					return GenerationMigrationExactRunRegion{}, err
				}
				spans[spanCount] = span
				spanCount++
				pageAt = next
			}
			writer.runID, writer.region = output.Runs+1, &output
			writer.records, writer.keyArena = writer.records[:0], writer.keyArena[:0]
			writer.pending, writer.hasPending, writer.ordinal = GenerationMigrationExactRunRecord{}, false, 0
			for index := 0; index < spanCount; index++ {
				cursors[index] = generationMigrationExactRunCursor{read: read, span: spans[index], page: pageScratch[(index+1)*int(pageBytes) : (index+2)*int(pageBytes)]}
				if err := cursors[index].start(sink.StoreIdentity(), sink.BuildGeneration()); err != nil {
					return GenerationMigrationExactRunRegion{}, err
				}
			}
			for {
				selected := -1
				for index := 0; index < spanCount; index++ {
					if !cursors[index].valid {
						continue
					}
					if selected < 0 || compareGenerationMigrationExactRunRecord(cursors[index].current, cursors[selected].current) < 0 {
						selected = index
					}
				}
				if selected < 0 {
					break
				}
				if err := writer.add(cursors[selected].current); err != nil {
					return GenerationMigrationExactRunRegion{}, err
				}
				if err := cursors[selected].advance(sink.StoreIdentity(), sink.BuildGeneration()); err != nil {
					return GenerationMigrationExactRunRegion{}, err
				}
			}
			if err := writer.finish(); err != nil {
				return GenerationMigrationExactRunRegion{}, err
			}
		}
		if output.Runs == 0 || output.Runs >= input.Runs {
			return GenerationMigrationExactRunRegion{}, fmt.Errorf("%w: exact merge progress", ErrInvalidWrite)
		}
		input = output
	}
	return input, nil
}

func scanGenerationMigrationExactRunSpan(read GenerationMigrationExactRunReader, region GenerationMigrationExactRunRegion, at uint64, scratch []byte, storeID [16]byte, generation uint64) (generationMigrationExactRunSpan, uint64, error) {
	first, ok := region.RefAt(at)
	if !ok {
		return generationMigrationExactRunSpan{}, at, ErrGenerationMigrationManifestCorrupt
	}
	span := generationMigrationExactRunSpan{first: first}
	for ordinal := uint32(0); ; ordinal++ {
		ref, ok := region.RefAt(at)
		if !ok || ref.Length != uint32(len(scratch)) {
			return generationMigrationExactRunSpan{}, at, ErrGenerationMigrationManifestCorrupt
		}
		if err := read(ref, scratch); err != nil {
			return generationMigrationExactRunSpan{}, at, err
		}
		view, err := OpenGenerationMigrationExactRunPage(scratch, ref, storeID, generation)
		if err != nil {
			return generationMigrationExactRunSpan{}, at, err
		}
		if ordinal == 0 {
			span.runID = view.RunID()
		}
		if view.RunID() != span.runID || view.PageOrdinal() != ordinal {
			return generationMigrationExactRunSpan{}, at, ErrGenerationMigrationManifestCorrupt
		}
		span.pages++
		at++
		if view.Last() {
			return span, at, nil
		}
	}
}

type generationMigrationExactRunCursor struct {
	read     GenerationMigrationExactRunReader
	span     generationMigrationExactRunSpan
	page     []byte
	pageAt   uint64
	view     GenerationMigrationExactRunPageView
	iterator GenerationMigrationExactRunIterator
	current  GenerationMigrationExactRunRecord
	valid    bool
}

func (c *generationMigrationExactRunCursor) start(storeID [16]byte, generation uint64) error {
	c.pageAt = 0
	if err := c.load(storeID, generation); err != nil {
		return err
	}
	return c.advance(storeID, generation)
}

func (c *generationMigrationExactRunCursor) load(storeID [16]byte, generation uint64) error {
	if c.pageAt >= c.span.pages {
		return ErrGenerationMigrationManifestCorrupt
	}
	ref := c.span.first
	ref.Offset += c.pageAt * uint64(ref.Length)
	ref.LogicalID += c.pageAt
	if err := c.read(ref, c.page); err != nil {
		return err
	}
	view, err := OpenGenerationMigrationExactRunPage(c.page, ref, storeID, generation)
	if err != nil || view.RunID() != c.span.runID || view.PageOrdinal() != uint32(c.pageAt) || view.Last() != (c.pageAt+1 == c.span.pages) {
		if err != nil {
			return err
		}
		return ErrGenerationMigrationManifestCorrupt
	}
	c.view = view
	c.iterator = view.Iterator()
	return nil
}

func (c *generationMigrationExactRunCursor) advance(storeID [16]byte, generation uint64) error {
	if record, ok := c.iterator.Next(); ok {
		c.current, c.valid = record, true
		return nil
	}
	if c.pageAt+1 == c.span.pages {
		c.valid = false
		return nil
	}
	c.pageAt++
	if err := c.load(storeID, generation); err != nil {
		return err
	}
	record, ok := c.iterator.Next()
	if !ok {
		return ErrGenerationMigrationManifestCorrupt
	}
	c.current, c.valid = record, true
	return nil
}

type generationMigrationExactOrderedRunWriter struct {
	sink       PrimaryGraphBuildSink
	pageBytes  uint32
	runID      uint64
	region     *GenerationMigrationExactRunRegion
	records    []GenerationMigrationExactRunRecord
	keyArena   []byte
	pending    GenerationMigrationExactRunRecord
	pendingKey [IndexTermMaxKeyBytes]byte
	hasPending bool
	ordinal    uint32
}

func (w *generationMigrationExactOrderedRunWriter) add(record GenerationMigrationExactRunRecord) error {
	if !w.hasPending {
		return w.setPending(record)
	}
	cmp := compareGenerationMigrationExactRunRecord(w.pending, record)
	if cmp > 0 {
		return ErrGenerationMigrationManifestCorrupt
	}
	if cmp == 0 {
		w.pending.Mask |= record.Mask
		return nil
	}
	if err := w.appendPending(); err != nil {
		return err
	}
	return w.setPending(record)
}

func (w *generationMigrationExactOrderedRunWriter) setPending(record GenerationMigrationExactRunRecord) error {
	if len(record.Key) == 0 || len(record.Key) > len(w.pendingKey) {
		return ErrGenerationMigrationManifestCorrupt
	}
	copy(w.pendingKey[:], record.Key)
	record.Key = w.pendingKey[:len(record.Key):len(record.Key)]
	w.pending, w.hasPending = record, true
	return nil
}

func (w *generationMigrationExactOrderedRunWriter) appendPending() error {
	recordBytes := generationMigrationExactRunRecordBytes + len(w.pending.Key)
	used := generationMigrationExactRunHeaderBytes
	for index := range w.records {
		used += generationMigrationExactRunRecordBytes + len(w.records[index].Key)
	}
	limit := int(w.pageBytes) - PageHeaderSize - PageTrailerSize
	if len(w.records) != 0 && used+recordBytes > limit {
		if err := w.flush(false); err != nil {
			return err
		}
	}
	start := len(w.keyArena)
	w.keyArena = append(w.keyArena, w.pending.Key...)
	w.pending.Key = w.keyArena[start:len(w.keyArena):len(w.keyArena)]
	w.records = append(w.records, w.pending)
	w.hasPending = false
	return nil
}

func (w *generationMigrationExactOrderedRunWriter) finish() error {
	if w.hasPending {
		if err := w.appendPending(); err != nil {
			return err
		}
	}
	if len(w.records) == 0 {
		return ErrGenerationMigrationManifestCorrupt
	}
	if err := w.flush(true); err != nil {
		return err
	}
	w.region.Runs++
	return nil
}

func (w *generationMigrationExactOrderedRunWriter) flush(last bool) error {
	page, err := w.sink.AllocatePage(PageMigrationExactRun, w.pageBytes, 0)
	if err != nil {
		return err
	}
	ref := page.Ref()
	if w.region.Pages == 0 {
		w.region.First = ref
	} else {
		want := w.region.First
		want.Offset += w.region.Pages * uint64(want.Length)
		want.LogicalID += w.region.Pages
		if ref != want {
			return fmt.Errorf("%w: non-contiguous merge output", ErrInvalidWrite)
		}
	}
	if _, err := EncodeGenerationMigrationExactRunPage(page.Bytes(), w.sink.StoreIdentity(), w.sink.BuildGeneration(), ref.LogicalID, w.runID, w.ordinal, last, w.records); err != nil {
		return err
	}
	if err := page.Stage(); err != nil {
		return err
	}
	w.region.Pages++
	w.ordinal++
	w.records = w.records[:0]
	w.keyArena = w.keyArena[:0]
	return nil
}
