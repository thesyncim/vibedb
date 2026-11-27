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

	"github.com/thesyncim/vibejson"
)

const segmentPrefixWindow = 1 << 16

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
		pos = skipSpace(s.srcChunk[:d.bufEnd], pos)
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

// readDoc ingests the document whose first significant byte is at *pos,
// advancing *pos past it on success using the resumable structural framer and
// s.buildDoc (which writes directly into the entry arena's free tail).
func (s *Segment) readDoc(d *segmentStream, pos *int) error {
	return s.readDocSlow(d, *pos, pos)
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
		return fmt.Errorf("vibedb: invalid document at input offset %d: %w", d.offset(start), err)
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
