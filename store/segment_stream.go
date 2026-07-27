package store

// This file implements bulk stream ingestion: ReadFrom fuses the streaming
// Reader's document framing with the Segment's arena-resident indexing. Bytes
// are read in large blocks straight into the source arena's spare capacity,
// each document is framed and indexed where it landed, and committing a
// document only extends the arena over storage already written in place — no
// intermediate buffer, no per-document copy, and on the hot path a single
// pass that validates, indexes, and locates the document's end together.
//
// The current source chunk plays both roles at once — committed arena on the
// left, read buffer on the right:
//
//	0             len(srcChunk)    pos              bufEnd            cap
//	|  committed documents  | ws  |  read-ahead      |  unfilled       |
//	                               ^ next document starts here
//
// The committed length only ever grows over bytes already framed and
// indexed, so a failed document needs no rollback: its bytes sit in the
// uncommitted read-ahead and are simply never committed. When a document
// straddles the chunk's end, only its uncommitted prefix is copied into the
// next chunk (fill); committed documents never move.

import (
	"fmt"
	"io"
	"unsafe"

	"github.com/thesyncim/vibejson"
)

// segmentStream carries ReadFrom's progress. Read-ahead lives in the current
// source chunk's spare capacity, between the committed length and bufEnd, so
// a failed document needs no rollback: its bytes were never committed.
type segmentStream struct {
	s      *Segment
	r      io.Reader
	bufEnd int   // filled bytes within the current chunk's capacity
	total  int64 // bytes read from r so far
	eof    bool  // no more input will arrive
	// readErr is a non-EOF read error, surfaced once the bytes that arrived
	// with or before it have been consumed; io.Reader delivers data and
	// error together and the data comes first.
	readErr error

	// Commit statistics for this call steer the two adaptive policies below:
	// srcChunkMax sizes chunk rolls from the mean committed document, and
	// extendWalk judges entry headroom from the mean entry count.
	docs       int64 // documents committed by this call
	docBytes   int64 // their total source bytes
	docEntries int64 // their total index entries
}

// record notes one committed document in the call's running statistics.
func (d *segmentStream) record(bytes, entries int) {
	d.docs++
	d.docBytes += int64(bytes)
	d.docEntries += int64(entries)
}

// offset translates a chunk position into this call's input offset.
func (d *segmentStream) offset(p int) int64 {
	return d.total - int64(d.bufEnd-p)
}

// fill reads more input into the current chunk's spare capacity, first
// rolling to a fresh chunk when none remains. A roll moves only the
// uncommitted bytes at or after *keep — at most one partial document — so
// committed documents never move; *keep is rewritten to the bytes' new
// position. The partial's length doubles into the new capacity, so a
// document larger than the chunk bound still ingests with amortized-linear
// copying; the bound itself adapts to the stream's document sizes
// (srcChunkMax). fill reports false when no new bytes will ever arrive.
func (d *segmentStream) fill(keep *int) bool {
	if d.eof {
		return false
	}
	s := d.s
	if d.bufEnd == cap(s.srcChunk) {
		partial := d.bufEnd - *keep
		chunk := make([]byte, 0, segmentChunkCap(cap(s.srcChunk), 2*partial, segmentMinSrcChunk, d.srcChunkMax()))
		chunk = append(chunk, s.srcChunk[*keep:d.bufEnd]...)
		s.srcChunk = chunk[:0]
		d.bufEnd = partial
		*keep = 0
	}
	for {
		n, err := d.r.Read(s.srcChunk[d.bufEnd:cap(s.srcChunk)])
		d.bufEnd += n
		d.total += int64(n)
		switch {
		case err == io.EOF:
			d.eof = true
			return n > 0
		case err != nil:
			d.eof = true
			d.readErr = err
			return n > 0
		case n > 0:
			return true
		}
	}
}

// ReadFrom appends every JSON document in r to the segment, in order, until the
// input ends. Documents are framed exactly like the streaming Reader's
// values: NDJSON, other whitespace separation, and direct concatenation all
// work. Bytes are read in large blocks straight into the segment's source arena
// and every document is validated and indexed in place under the segment's
// Options, exactly as Append would, with no intermediate buffering: a
// document straddling an arena chunk boundary costs one copy of its partial
// prefix, and everything else lands in place. ReadFrom implements
// io.ReaderFrom: it returns the number of bytes read from r; the number of
// documents appended is the change in Len.
//
// A failure — an invalid document, a read error, or input ending
// mid-document — keeps every fully ingested document, discards the failed
// one, and leaves the segment valid for further Append or ReadFrom calls; bytes
// read past the failure are discarded with it. The error carries the
// document's byte offset within this call's input. Like the Reader, a read
// error surfaces only after the documents that arrived before it.
func (s *Segment) ReadFrom(r io.Reader) (int64, error) {
	d := segmentStream{s: s, r: r, bufEnd: len(s.srcChunk)}
	pos := len(s.srcChunk)
	for {
		// Skip inter-document whitespace, refilling as needed, to the next
		// document's first significant byte.
		pos = vibejson.SkipSpace(s.srcChunk[:d.bufEnd], pos)
		if pos == d.bufEnd {
			if !d.fill(&pos) {
				return d.total, d.readErr
			}
			continue
		}
		if err := s.readDoc(&d, &pos); err != nil {
			return d.total, err
		}
	}
}

// segmentPrefixWindow bounds readDoc's first walk while more input can still
// land in the current chunk. A document that outgrows the window has an
// unknown extent: the first walk stops there rather than risk full-document
// work thrown away at the buffered edge. It is only a probe, not a verdict —
// when the walk stops at the window short of buffered bytes, readDoc lifts the
// cap and re-walks the whole extent, and when it stops at the buffered edge
// short of the document, readDoc buffers more and re-walks (bounded by
// segmentWalkRefillLimit). extendWalk skips the cap entirely once the buffered
// bytes plausibly hold the whole document, so the common large-document case
// walks its full extent on the first pass.
const segmentPrefixWindow = vibejson.ValidBitmapMinBytes

// segmentWalkRefillLimit bounds how many refills readDoc will spend buffering
// one document for the one-pass walk before conceding to readDocSlow. A
// document that fits within one arena chunk completes within a couple of
// refills (the straddling document at a chunk's end needs one roll); the bound
// preserves the slow lane's single-structural-scan guarantee for documents
// that span many reads — torn streams, and documents larger than the source
// chunk — by handing them over before the per-read re-walks add up.
const segmentWalkRefillLimit = 4

// segmentMaxStreamSrcChunk caps srcChunkMax's adaptive raise. It bounds both
// the retention granularity of a stream's source arena and the copying a
// single roll can perform, while keeping the roll's abandoned tail — at most
// one mean-sized document per chunk — a small fraction of the whole.
const segmentMaxStreamSrcChunk = 8 << 20

// srcChunkMax returns the size bound for the next source-chunk roll. The
// static bound serves mixed and small-document streams unchanged; a stream
// of large documents raises it toward eight times its mean committed
// document, so one chunk holds several documents and each roll abandons at
// most one document's worth of tail instead of nearly a whole chunk.
func (d *segmentStream) srcChunkMax() int {
	bound := int64(segmentMaxSrcChunk)
	if d.docs > 0 {
		if t := 8 * (d.docBytes / d.docs); t > bound {
			bound = min(t, segmentMaxStreamSrcChunk)
		}
	}
	return int(bound)
}

// extendWalk reports whether readDoc's first walk may skip the
// segmentPrefixWindow cap and run over every buffered byte at once. Skipping
// pays off exactly when the buffered bytes plausibly hold the whole document:
// once the chunk is full or the input has ended, no more bytes can land in
// place, so a document that will ever complete in this buffer already has, and
// walking it in one pass saves both the capped probe and the slow lane's
// separate framing scan. While the buffer can still grow, the cap stays on —
// the uncapped walk would routinely reach the buffered edge mid-document and
// throw that work away, and readDoc's refill loop reaches the same fully
// buffered documents by lifting the cap after the cheap probe truncates. The
// entry-headroom test keeps streams of entry-dense documents on the probe for
// the same reason, using twice the stream's mean entry count as the bar for
// the entry chunk's free tail.
func (d *segmentStream) extendWalk() bool {
	if !d.eof && d.bufEnd < cap(d.s.srcChunk) {
		return false
	}
	if d.docs == 0 {
		return true
	}
	free := cap(d.s.entryChunk) - len(d.s.entryChunk)
	return int64(free) >= 2*(d.docEntries/d.docs)
}

// readDoc ingests the document whose first significant byte is at *pos,
// advancing *pos past it on success. The hot path is one pass: the fast tape
// walker validates, indexes, and locates the document's end directly in the
// buffered arena bytes, and the commit extends the source and entry chunks
// over that storage.
//
// A walk that stops short of the root's end because it reached the buffered
// edge (prefixTruncated) is not yet a failure: when more input can still
// arrive, readDoc buffers the rest and re-walks the whole extent in one pass,
// so a large document that fits within one arena chunk indexes on the hot
// path instead of falling to the two-scan slow lane. The first refill also
// lifts the prefix-window cap (a fully buffered document past the cap needs no
// more bytes, only a wider walk). The refill is bounded: a document that
// spans more than segmentWalkRefillLimit reads — a torn stream, or a document
// larger than the source chunk — declines to readDocSlow, whose resumable
// framer scans it once across every refill rather than re-walking it per read.
// A walk that declines for a reason more input cannot repair (prefixDeclined:
// exhausted entry storage, deep nesting, an oversized window) goes straight to
// readDocSlow, which settles every such case.
func (s *Segment) readDoc(d *segmentStream, pos *int) error {
	start := *pos
	refills := 0
	full := false // walk the whole buffered extent, past the prefix-window cap
	for {
		windowEnd := d.bufEnd
		if !full && windowEnd-start > segmentPrefixWindow && !d.extendWalk() {
			windowEnd = start + segmentPrefixWindow
		}
		index, end, status := s.buildDocPrefix(start, windowEnd)
		if status != prefixComplete {
			if status == prefixTruncated {
				if windowEnd < d.bufEnd {
					// The prefix-window cap, not the buffered edge, stopped the
					// walk, so the rest of the document may already be buffered:
					// widen to the whole extent and re-walk before refilling.
					full = true
					continue
				}
				// The walk reached the buffered edge. More input may complete
				// the document, so buffer it and re-walk in one pass — bounded,
				// past which the resumable framer takes over. A failed fill
				// sets eof; readDocSlow then reports the truncation exactly.
				if !d.eof && refills < segmentWalkRefillLimit && d.fill(&start) {
					refills++
					continue
				}
			}
			return s.readDocSlow(d, start, pos)
		}
		// A root number ending exactly at the filled edge may continue in
		// unread input, so it commits only once a byte follows it or the
		// input ends. Every other root ends on its closing or final byte.
		// start sits past the committed length, so the byte is read through
		// the filled extent.
		if c := s.srcChunk[start:d.bufEnd][0]; c == '-' || vibejson.IsDigit(c) {
			if end == d.bufEnd && !d.eof {
				// Read the confirming byte and re-walk: a fill can roll the
				// chunk, so the built index must not outlive it. A failed
				// fill sets eof and the re-walk commits the same extent.
				d.fill(&start)
				continue
			}
			if end == windowEnd && windowEnd < d.bufEnd {
				// The window cap cut the number short of buffered bytes.
				return s.readDocSlow(d, start, pos)
			}
		}
		// The build's full entry count feeds the headroom statistics: a dedup
		// compaction commits fewer entries, but the next build still needs
		// classic-tape room in the chunk tail before it can compact.
		built := len(index.Entries)
		index, ref := s.shapeTapeCompact(index)
		s.entryChunk = s.entryChunk[:len(s.entryChunk)+len(index.Entries)]
		s.srcChunk = s.srcChunk[:end]
		s.commitDoc(index, ref)
		d.record(end-start, built)
		*pos = end
		return nil
	}
}

// readDocSlow ingests one document through the resumable structural framer:
// it locates the document's exact extent across refills, then hands that
// extent to buildDoc, which behaves exactly as it does under Append — same
// builder routing (including the stage-1 bitmap engine for large documents),
// same diagnostic errors, same spill handling, same atomicity. Framing is
// structure-only and skips string interiors with the vector scanner, so a
// document spanning K refills is scanned once, not K times.
func (s *Segment) readDocSlow(d *segmentStream, start int, pos *int) error {
	var fr vibejson.ValueFrame
	fr.Init(s.srcChunk[start:d.bufEnd][0])
	framed := false
	for {
		framed = fr.Scan(s.srcChunk[:d.bufEnd], start, d.bufEnd)
		if framed || !d.fill(&start) {
			break
		}
	}
	// When the input ends mid-document the framer has consumed every
	// buffered byte: a root number legitimately ends there, and anything
	// else is truncation, which buildDoc rejects with its exact diagnosis.
	end := start + fr.Framed
	index, ref, err := s.buildDoc(s.srcChunk[start:end:end])
	if err != nil {
		if !framed && d.readErr != nil {
			// The stream broke mid-document; the read failure, not the
			// truncated bytes it left behind, is the cause worth reporting.
			err = d.readErr
		}
		return fmt.Errorf("vibejson: invalid document at input offset %d: %w", d.offset(start), err)
	}
	built := len(index.Entries)
	if ref.Rec != nil {
		built = 2*built + 1 // the classic count, for the headroom statistics
	}
	s.srcChunk = s.srcChunk[:end]
	s.commitDoc(index, ref)
	d.record(end-start, built)
	*pos = end
	return nil
}

// prefixStatus is buildDocPrefix's three-way verdict, steering readDoc's
// refill decision. The fast walk still never diagnoses — the slow lane owns
// every error — but the verdict distinguishes a walk that merely ran out of
// buffered bytes from one that cannot be helped by more of them.
type prefixStatus uint8

const (
	// prefixComplete: the walk indexed a whole document within the window.
	prefixComplete prefixStatus = iota
	// prefixTruncated: the walk reached the window's end short of the root's
	// close. More input may complete it, so readDoc buffers more and re-walks.
	// A syntax error the fast walker cannot separate from truncation lands
	// here too; the bounded refill spends at most a few extra reads before the
	// slow lane reports the error exactly, so conflating the two costs only
	// off-hot-path work, never correctness.
	prefixTruncated
	// prefixDeclined: the walk failed for a reason more input cannot repair —
	// exhausted entry storage, nesting past the fast walker's fixed stack, or
	// a window wider than uint32 offsets reach. readDoc hands these straight
	// to the slow lane, which spills or diagnoses as Append would.
	prefixDeclined
)

// buildDocPrefix walks one document beginning at start against the buffered
// window ending at windowEnd, building its index into the entry chunk's free
// tail without committing anything, so a discarded result leaves no trace. On
// prefixComplete it returns the built index and the document's exclusive end
// in chunk coordinates. Decline still carries no cause — the slow path settles
// every case itself — but the status separates a walk short of buffered bytes
// (prefixTruncated) from one more bytes cannot help (prefixDeclined). The
// walker is the same one BuildIndexOptions runs first on documents this size,
// stopped at the root value's end instead of demanding end of input, so
// accepted documents index byte-identically to Append.
func (s *Segment) buildDocPrefix(start, windowEnd int) (vibejson.Index, int, prefixStatus) {
	if uint64(windowEnd-start) > uint64(^uint32(0)) {
		// Entry offsets are uint32, exactly as buildIndexOptions enforces; a
		// window past their reach cannot be walked. The slow lane frames the
		// document's true extent and reports its own error.
		return vibejson.Index{}, 0, prefixDeclined
	}
	if cap(s.entryChunk) == 0 {
		s.entryChunk = make([]vibejson.IndexEntry, 0, segmentMinEntryChunk)
	}
	used := len(s.entryChunk)
	free := s.entryChunk[used:]
	window := s.srcChunk[start:windowEnd:windowEnd]
	maxDepth := s.Options.MaxDepth
	if maxDepth <= 0 {
		maxDepth = vibejson.DefaultMaxDepth
	}
	b := vibejson.TapeBuilder{
		Src:      window,
		Base:     vibejson.ByteSourceOf(window).PointerAt(0),
		Entries:  free[:0],
		Parent:   vibejson.NoTapeParent,
		MaxDepth: maxDepth,
	}
	switch b.WalkFast() {
	case vibejson.TapeParseOK:
		// fall through to commit the built prefix below
	case vibejson.TapeParseFull:
		// Entry storage, not source bytes, ran out; refilling cannot help.
		return vibejson.Index{}, 0, prefixDeclined
	default: // tapeParseInvalid: truncated at the window, or a syntax error
		return vibejson.Index{}, 0, prefixTruncated
	}
	n := len(b.Entries)
	if n == 0 || unsafe.SliceData(b.Entries) != unsafe.SliceData(free) {
		// The walker builds by reslicing the storage it was handed, and a
		// complete document always emits at least one entry. If either
		// invariant ever broke, committing would expose garbage, so fail
		// closed into the slow path, which owns its storage end to end.
		return vibejson.Index{}, 0, prefixDeclined
	}
	end := b.I
	index := vibejson.Index{Src: window[:end:end], Entries: free[:n:n]}
	if s.Options.HashKeys {
		vibejson.EnrichKeyHashes(&index)
	}
	return index, start + end, prefixComplete
}
