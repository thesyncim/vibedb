package seglog

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
)

// The sealed index has three independently addressable regions:
//
//	header | compact group-run directory | optional summaries |
//	fixed route descriptors | route payloads
//
// Open reads only the first three regions. A point lookup computes its route
// descriptor ordinal from (index-first)/blockEntries and reads exactly that
// descriptor and payload. Descriptors are fixed-size so a hot group does not
// require a descriptor-directory read or an entry-sized heap index.
const (
	sealedIndexHeaderBytes     = 64
	sealedRouteDescriptorBytes = 40
	sealedRouteTagBytes        = 16
	sealedDefaultBlockEntries  = 256
	sealedMaxBlockRouteBytes   = 16 << 10
	sealedMaxSegmentBytes      = uint64(^uint32(0))
)

type sealedRetryState struct {
	ID       WaveID
	Digest   [32]byte
	Sequence uint64
}

// appendSealedDirectory writes each live retry identity once, in first-group
// reference order. Group summaries carry the canonical one-based ordinal.
func appendSealedDirectory(dst []byte, runs []sealedGroupRun) ([]byte, uint32, uint32, error) {
	if uint64(len(runs)) > uint64(^uint32(0)) {
		return nil, 0, 0, ErrBounds
	}
	start := len(dst)
	ordinals := make(map[WaveID]uint32, len(runs))
	states := make([]sealedRetryState, 0, len(runs))
	for i := range runs {
		s := &runs[i].Summary
		if s.LatestWaveID == (WaveID{}) {
			if s.LatestWaveDigest != ([32]byte{}) || s.LatestWaveSequence != 0 || s.RetryOrdinal != 0 {
				return nil, 0, 0, ErrCorrupt
			}
			continue
		}
		if s.LatestWaveDigest == ([32]byte{}) || s.LatestWaveSequence == 0 {
			return nil, 0, 0, ErrCorrupt
		}
		ordinal := ordinals[s.LatestWaveID]
		if ordinal == 0 {
			if uint64(len(states)) >= uint64(^uint32(0)) {
				return nil, 0, 0, ErrBounds
			}
			ordinal = uint32(len(states) + 1)
			ordinals[s.LatestWaveID] = ordinal
			states = append(states, sealedRetryState{ID: s.LatestWaveID, Digest: s.LatestWaveDigest, Sequence: s.LatestWaveSequence})
		} else {
			state := states[ordinal-1]
			if state.Digest != s.LatestWaveDigest || state.Sequence != s.LatestWaveSequence {
				return nil, 0, 0, ErrCorrupt
			}
		}
		s.RetryOrdinal = ordinal
	}
	for i := range states {
		dst = append(dst, states[i].ID[:]...)
		dst = append(dst, states[i].Digest[:]...)
		dst = appendUvarint(dst, states[i].Sequence)
	}
	retryBytes := len(dst) - start
	if uint64(retryBytes) > uint64(^uint32(0)) {
		return nil, 0, 0, ErrBounds
	}
	var err error
	dst, err = appendRunDirectory(dst, runs)
	return dst, uint32(retryBytes), uint32(len(states)), err
}

func decodeSealedDirectory(src []byte, header sealedIndexHeader) ([]sealedGroupRun, error) {
	if uint64(header.RetryBytes) > uint64(len(src)) || header.DirectoryBytes != uint32(len(src)) {
		return nil, ErrCorrupt
	}
	if uint64(header.RetryCount) > uint64(header.RetryBytes)/49 {
		return nil, ErrCorrupt
	}
	retries := make([]sealedRetryState, 0, header.RetryCount)
	cursor := canonicalCursor{data: src[:header.RetryBytes]}
	for range header.RetryCount {
		id, err := cursor.take(16)
		if err != nil {
			return nil, ErrCorrupt
		}
		state := sealedRetryState{}
		copy(state.ID[:], id)
		digest, err := cursor.take(32)
		if err != nil {
			return nil, ErrCorrupt
		}
		copy(state.Digest[:], digest)
		state.Sequence, err = cursor.uvarint()
		if err != nil || state.ID == (WaveID{}) || state.Digest == ([32]byte{}) || state.Sequence == 0 {
			return nil, ErrCorrupt
		}
		for i := range retries {
			if retries[i].ID == state.ID {
				return nil, ErrCorrupt
			}
		}
		retries = append(retries, state)
	}
	if !cursor.empty() {
		return nil, ErrCorrupt
	}
	runs, err := decodeRunDirectory(src[header.RetryBytes:], uint64(header.Runs))
	if err != nil {
		return nil, err
	}
	nextOrdinal := uint32(1)
	seen := make([]bool, len(retries))
	for i := range runs {
		ordinal := runs[i].Summary.RetryOrdinal
		if ordinal == 0 {
			continue
		}
		if ordinal > uint32(len(retries)) {
			return nil, ErrCorrupt
		}
		if !seen[ordinal-1] {
			if ordinal != nextOrdinal {
				return nil, ErrCorrupt
			}
			seen[ordinal-1] = true
			nextOrdinal++
		}
		state := retries[ordinal-1]
		runs[i].Summary.LatestWaveID = state.ID
		runs[i].Summary.LatestWaveDigest = state.Digest
		runs[i].Summary.LatestWaveSequence = state.Sequence
	}
	if nextOrdinal != uint32(len(retries))+1 {
		return nil, ErrCorrupt
	}
	return runs, nil
}

var sealedIndexMagic = [8]byte{'V', 'D', 'B', 'S', 'I', 'D', 'X', 0}
var sealedRouteDomain = []byte{'v', 'i', 'b', 'e', 'd', 'b', '/', 's', 'e', 'g', 'l', 'o', 'g', '/', 's', 'e', 'a', 'l', 'e', 'd', '/', 'r', 'o', 'u', 't', 'e', 0}
var sealedTopDomain = []byte{'v', 'i', 'b', 'e', 'd', 'b', '/', 's', 'e', 'g', 'l', 'o', 'g', '/', 's', 'e', 'a', 'l', 'e', 'd', '/', 't', 'o', 'p', 0}

type sealedIndexHeader struct {
	TotalBytes, Runs, DirectoryBytes      uint32
	DescriptorOffset, DescriptorBytes     uint32
	RoutePayloadOffset, RoutePayloadBytes uint32
	DataBytes                             uint32
	LastSequence                          uint64
	RetryBytes, RetryCount                uint32
}

func marshalSealedIndexHeader(h sealedIndexHeader) ([sealedIndexHeaderBytes]byte, error) {
	var b [sealedIndexHeaderBytes]byte
	total, directory, descriptorOffset, descriptorBytes := uint64(h.TotalBytes), uint64(h.DirectoryBytes), uint64(h.DescriptorOffset), uint64(h.DescriptorBytes)
	routeOffset, routeBytes := uint64(h.RoutePayloadOffset), uint64(h.RoutePayloadBytes)
	if total < sealedIndexHeaderBytes || directory > total-sealedIndexHeaderBytes || uint64(h.RetryBytes) > directory || (h.RetryCount == 0) != (h.RetryBytes == 0) || descriptorOffset != sealedIndexHeaderBytes+directory || descriptorBytes%sealedRouteDescriptorBytes != 0 || routeOffset != descriptorOffset+descriptorBytes || routeOffset > total || routeBytes != total-routeOffset || uint64(h.DataBytes) > sealedMaxSegmentBytes {
		return b, ErrCorrupt
	}
	copy(b[:8], sealedIndexMagic[:])
	binary.LittleEndian.PutUint16(b[8:10], canonicalFormatMarker)
	binary.LittleEndian.PutUint16(b[10:12], sealedIndexHeaderBytes)
	binary.LittleEndian.PutUint32(b[12:16], h.TotalBytes)
	binary.LittleEndian.PutUint32(b[16:20], h.Runs)
	binary.LittleEndian.PutUint32(b[20:24], h.DirectoryBytes)
	binary.LittleEndian.PutUint32(b[24:28], h.DescriptorOffset)
	binary.LittleEndian.PutUint32(b[28:32], h.DescriptorBytes)
	binary.LittleEndian.PutUint32(b[32:36], h.RoutePayloadOffset)
	binary.LittleEndian.PutUint32(b[36:40], h.RoutePayloadBytes)
	binary.LittleEndian.PutUint32(b[40:44], h.DataBytes)
	binary.LittleEndian.PutUint64(b[44:52], h.LastSequence)
	binary.LittleEndian.PutUint32(b[52:56], h.RetryBytes)
	binary.LittleEndian.PutUint32(b[56:60], h.RetryCount)
	putCRC(b[:])
	return b, nil
}

func unmarshalSealedIndexHeader(b []byte) (sealedIndexHeader, error) {
	if len(b) != sealedIndexHeaderBytes || string(b[:8]) != string(sealedIndexMagic[:]) || binary.LittleEndian.Uint16(b[8:10]) != canonicalFormatMarker || binary.LittleEndian.Uint16(b[10:12]) != sealedIndexHeaderBytes || !validCRC(b) {
		return sealedIndexHeader{}, ErrCorrupt
	}
	h := sealedIndexHeader{TotalBytes: binary.LittleEndian.Uint32(b[12:16]), Runs: binary.LittleEndian.Uint32(b[16:20]), DirectoryBytes: binary.LittleEndian.Uint32(b[20:24]), DescriptorOffset: binary.LittleEndian.Uint32(b[24:28]), DescriptorBytes: binary.LittleEndian.Uint32(b[28:32]), RoutePayloadOffset: binary.LittleEndian.Uint32(b[32:36]), RoutePayloadBytes: binary.LittleEndian.Uint32(b[36:40]), DataBytes: binary.LittleEndian.Uint32(b[40:44]), LastSequence: binary.LittleEndian.Uint64(b[44:52]), RetryBytes: binary.LittleEndian.Uint32(b[52:56]), RetryCount: binary.LittleEndian.Uint32(b[56:60])}
	if h.RetryBytes > h.DirectoryBytes || (h.RetryCount == 0) != (h.RetryBytes == 0) {
		return sealedIndexHeader{}, ErrCorrupt
	}
	if _, err := marshalSealedIndexHeader(h); err != nil {
		return sealedIndexHeader{}, err
	}
	return h, nil
}

func sealedTopMAC(key [32]byte, logID [16]byte, segmentID, generation uint64, header, directory []byte) [32]byte {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(sealedTopDomain)
	var context [32]byte
	copy(context[:16], logID[:])
	binary.LittleEndian.PutUint64(context[16:24], segmentID)
	binary.LittleEndian.PutUint64(context[24:32], generation)
	_, _ = mac.Write(context[:])
	_, _ = mac.Write(header)
	_, _ = mac.Write(directory)
	var result [32]byte
	_ = mac.Sum(result[:0])
	return result
}

func segmentSealedMetadataMAC(key [32]byte, header segmentHeader, top []byte, footer segmentFooter) [32]byte {
	footer.Auth = [32]byte{}
	encodedFooter := marshalSegmentFooter(footer)
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(sealedTopDomain)
	_, _ = mac.Write(marshalSegmentHeader(header))
	_, _ = mac.Write(top)
	_, _ = mac.Write(encodedFooter)
	var result [32]byte
	_ = mac.Sum(result[:0])
	return result
}

type sealedRunSummary struct {
	LastIndex, LastTerm         uint64
	Hard                        HardState
	TruncateIndex, TruncateTerm uint64
	Checkpoint                  Checkpoint
	NodeIncarnation, ReadyID    uint64
	ReadyDigest                 [16]byte
	ReadyWaveID                 WaveID
	LatestWaveID                WaveID
	LatestWaveDigest            [32]byte
	LatestWaveSequence          uint64
	RetryOrdinal                uint32
}

type sealedGroupRun struct {
	GroupID, First, Last            uint64
	DescriptorOrdinal, ExtentOffset uint64
	ExtentBytes                     uint64
	DescriptorCount, BlockEntries   uint32
	Inline                          routeEntry
	Summary                         sealedRunSummary
}

type routeEntry struct {
	Term, ExtentOffset, ExtentBytes uint64
	ExtentID                        uint64
	DataOffset, DataBytes           uint64
	Type                            uint16
	BatchID                         WaveID
}

type routeDescriptor struct {
	PayloadOffset, ExtentOffset uint32
	PayloadBytes, Entries       uint32
	ExtentBytes                 uint32
	Tag                         [sealedRouteTagBytes]byte
}

type routeAuthWorkspace struct {
	mac     hash.Hash
	context [36]byte
	sum     [32]byte
}

func newRouteAuthWorkspace(key [32]byte) routeAuthWorkspace {
	return routeAuthWorkspace{mac: hmac.New(sha256.New, key[:])}
}

const (
	summaryHard = 1 << iota
	summaryTruncate
	summaryCheckpoint
	summaryReady
	runInline
	summaryTail
	summaryWave
)

func appendRunDirectory(dst []byte, runs []sealedGroupRun) ([]byte, error) {
	previousGroup := uint64(0)
	for i := range runs {
		run := &runs[i]
		inline := run.DescriptorCount == 0
		controlOnly := run.First == 0 && run.Last == 0 && run.DescriptorCount == 0 && run.ExtentBytes == 0
		if run.GroupID == 0 || run.GroupID <= previousGroup || !controlOnly && (run.First == 0 || run.Last < run.First || run.BlockEntries == 0 || inline && (run.First != run.Last || !validRouteEntry(run.Inline) || run.Inline.ExtentOffset < run.ExtentOffset)) {
			return nil, ErrCorrupt
		}
		dst = appendUvarint(dst, run.GroupID-previousGroup)
		dst = appendUvarint(dst, run.First)
		dst = appendUvarint(dst, run.Last-run.First)
		dst = appendUvarint(dst, run.DescriptorOrdinal)
		dst = appendUvarint(dst, uint64(run.DescriptorCount))
		dst = appendUvarint(dst, uint64(run.BlockEntries))
		dst = appendUvarint(dst, run.ExtentOffset)
		dst = appendUvarint(dst, run.ExtentBytes)
		flags := byte(0)
		if run.Summary.Hard != (HardState{}) {
			flags |= summaryHard
		}
		if run.Summary.TruncateIndex != 0 {
			flags |= summaryTruncate
		}
		if run.Summary.Checkpoint != (Checkpoint{}) {
			flags |= summaryCheckpoint
		}
		if run.Summary.NodeIncarnation != 0 {
			flags |= summaryReady
		}
		if run.Summary.LatestWaveID != (WaveID{}) {
			flags |= summaryWave
		}
		if inline && !controlOnly {
			flags |= runInline
		}
		flags |= summaryTail
		dst = append(dst, flags)
		dst = appendUvarint(dst, run.Summary.LastIndex)
		dst = appendUvarint(dst, run.Summary.LastTerm)
		if flags&runInline != 0 {
			dst = appendUvarint(dst, run.Inline.Term)
			dst = appendUvarint(dst, uint64(run.Inline.Type))
			dst = appendUvarint(dst, run.Inline.ExtentOffset-run.ExtentOffset)
			dst = appendUvarint(dst, run.Inline.ExtentBytes)
			dst = appendUvarint(dst, run.Inline.ExtentID)
			dst = appendUvarint(dst, run.Inline.DataOffset)
			dst = appendUvarint(dst, run.Inline.DataBytes)
			dst = append(dst, run.Inline.BatchID[:]...)
		}
		if flags&summaryHard != 0 {
			dst = appendUvarint(dst, run.Summary.Hard.Term)
			dst = appendUvarint(dst, run.Summary.Hard.Vote)
			dst = appendUvarint(dst, run.Summary.Hard.Commit)
		}
		if flags&summaryTruncate != 0 {
			dst = appendUvarint(dst, run.Summary.TruncateIndex)
			dst = appendUvarint(dst, run.Summary.TruncateTerm)
		}
		if flags&summaryCheckpoint != 0 {
			dst = append(dst, run.Summary.Checkpoint.ID[:]...)
			dst = appendUvarint(dst, run.Summary.Checkpoint.Index)
			dst = appendUvarint(dst, run.Summary.Checkpoint.Term)
		}
		if flags&summaryReady != 0 {
			dst = appendUvarint(dst, run.Summary.NodeIncarnation)
			dst = appendUvarint(dst, run.Summary.ReadyID)
			dst = append(dst, run.Summary.ReadyDigest[:]...)
			dst = append(dst, run.Summary.ReadyWaveID[:]...)
		}
		if flags&summaryWave != 0 {
			if run.Summary.RetryOrdinal == 0 {
				return nil, ErrCorrupt
			}
			dst = appendUvarint(dst, uint64(run.Summary.RetryOrdinal))
		}
		previousGroup = run.GroupID
	}
	return dst, nil
}

func decodeRunDirectory(src []byte, count uint64) ([]sealedGroupRun, error) {
	if count > uint64(len(src))/9+1 {
		return nil, ErrCorrupt
	}
	runs := make([]sealedGroupRun, 0, count)
	cursor := canonicalCursor{data: src}
	previousGroup := uint64(0)
	for range count {
		delta, err := cursor.uvarint()
		if err != nil || delta == 0 || previousGroup > ^uint64(0)-delta {
			return nil, ErrCorrupt
		}
		run := sealedGroupRun{GroupID: previousGroup + delta}
		run.First, err = cursor.uvarint()
		if err != nil {
			return nil, ErrCorrupt
		}
		span, err := cursor.uvarint()
		if err != nil || run.First > ^uint64(0)-span {
			return nil, ErrCorrupt
		}
		run.Last = run.First + span
		run.DescriptorOrdinal, err = cursor.uvarint()
		if err != nil {
			return nil, ErrCorrupt
		}
		descriptorCount, err := cursor.uvarint()
		if err != nil || descriptorCount > uint64(^uint32(0)) {
			return nil, ErrCorrupt
		}
		run.DescriptorCount = uint32(descriptorCount)
		blockEntries, err := cursor.uvarint()
		if err != nil || blockEntries > uint64(^uint32(0)) {
			return nil, ErrCorrupt
		}
		run.BlockEntries = uint32(blockEntries)
		run.ExtentOffset, err = cursor.uvarint()
		if err != nil {
			return nil, ErrCorrupt
		}
		run.ExtentBytes, err = cursor.uvarint()
		if err != nil {
			return nil, ErrCorrupt
		}
		flags, err := cursor.byte()
		if err != nil || flags & ^byte(summaryHard|summaryTruncate|summaryCheckpoint|summaryReady|runInline|summaryTail|summaryWave) != 0 || flags&summaryTail == 0 {
			return nil, ErrCorrupt
		}
		run.Summary.LastIndex, err = cursor.uvarint()
		if err == nil {
			run.Summary.LastTerm, err = cursor.uvarint()
		}
		if flags&runInline != 0 {
			run.Inline.Term, err = cursor.uvarint()
			var entryType, offsetDelta uint64
			if err == nil {
				entryType, err = cursor.uvarint()
			}
			if entryType > uint64(eventBlobEntryConfV2) {
				err = ErrCorrupt
			}
			run.Inline.Type = uint16(entryType)
			if err == nil {
				offsetDelta, err = cursor.uvarint()
			}
			if offsetDelta > ^uint64(0)-run.ExtentOffset {
				err = ErrCorrupt
			} else {
				run.Inline.ExtentOffset = run.ExtentOffset + offsetDelta
			}
			if err == nil {
				run.Inline.ExtentBytes, err = cursor.uvarint()
			}
			if err == nil {
				run.Inline.ExtentID, err = cursor.uvarint()
			}
			if err == nil {
				run.Inline.DataOffset, err = cursor.uvarint()
			}
			if err == nil {
				run.Inline.DataBytes, err = cursor.uvarint()
			}
			if err == nil {
				var batch []byte
				batch, err = cursor.take(16)
				copy(run.Inline.BatchID[:], batch)
			}
		}
		if flags&summaryHard != 0 {
			run.Summary.Hard.Term, err = cursor.uvarint()
			if err == nil {
				run.Summary.Hard.Vote, err = cursor.uvarint()
			}
			if err == nil {
				run.Summary.Hard.Commit, err = cursor.uvarint()
			}
		}
		if err == nil && flags&summaryTruncate != 0 {
			run.Summary.TruncateIndex, err = cursor.uvarint()
			if err == nil {
				run.Summary.TruncateTerm, err = cursor.uvarint()
			}
		}
		if err == nil && flags&summaryCheckpoint != 0 {
			var value []byte
			value, err = cursor.take(16)
			if err == nil {
				copy(run.Summary.Checkpoint.ID[:], value)
				run.Summary.Checkpoint.Index, err = cursor.uvarint()
			}
			if err == nil {
				run.Summary.Checkpoint.Term, err = cursor.uvarint()
			}
		}
		if err == nil && flags&summaryReady != 0 {
			run.Summary.NodeIncarnation, err = cursor.uvarint()
			if err == nil {
				run.Summary.ReadyID, err = cursor.uvarint()
			}
			var value []byte
			if err == nil {
				value, err = cursor.take(16)
				copy(run.Summary.ReadyDigest[:], value)
			}
			if err == nil {
				value, err = cursor.take(16)
				copy(run.Summary.ReadyWaveID[:], value)
			}
		}
		if err == nil && flags&summaryWave != 0 {
			ordinal, ordinalErr := cursor.uvarint()
			if ordinalErr != nil || ordinal == 0 || ordinal > uint64(^uint32(0)) {
				err = ErrCorrupt
			} else {
				run.Summary.RetryOrdinal = uint32(ordinal)
			}
		}
		wantDescriptors := uint64(0)
		if run.First != 0 {
			if run.BlockEntries == 0 {
				return nil, ErrCorrupt
			}
			wantDescriptors = (run.Last-run.First)/uint64(run.BlockEntries) + 1
		} else if run.Last != 0 || run.DescriptorCount != 0 || run.ExtentBytes != 0 {
			return nil, ErrCorrupt
		}
		if flags&runInline != 0 {
			wantDescriptors = 0
		}
		if err != nil || uint64(run.DescriptorCount) != wantDescriptors || flags&runInline != 0 && (run.First != run.Last || !validRouteEntry(run.Inline)) {
			return nil, ErrCorrupt
		}
		runs = append(runs, run)
		previousGroup = run.GroupID
	}
	if !cursor.empty() {
		return nil, ErrCorrupt
	}
	return runs, nil
}

func validRouteEntry(entry routeEntry) bool {
	return entry.Term != 0 && entry.ExtentOffset <= sealedMaxSegmentBytes && entry.ExtentBytes <= sealedMaxSegmentBytes-entry.ExtentOffset && entry.DataOffset <= entry.ExtentBytes && entry.DataBytes <= entry.ExtentBytes-entry.DataOffset && entry.Type <= 2
}

// appendRoutePayload uses implicit contiguous indexes. Terms are signed deltas;
// a physical extent is described once and following entries only carry their
// within-extent slice. The descriptor MAC authenticates these canonical bytes.
func appendRoutePayload(dst []byte, entries []routeEntry) ([]byte, error) {
	start := len(dst)
	previousTerm, previousExtent, previousExtentBytes, previousDataEnd := uint64(0), uint64(0), uint64(0), uint64(0)
	previousExtentID := uint64(0)
	var previousBatch WaveID
	for i := range entries {
		entry := entries[i]
		if !validRouteEntry(entry) {
			return nil, ErrCorrupt
		}
		newExtent := i == 0 || entry.ExtentOffset != previousExtent
		flags := byte(entry.Type & 0x0f)
		encodedTerm, deltaOK := signedDelta(previousTerm, entry.Term)
		if !deltaOK {
			flags |= 0x40
		}
		if newExtent {
			flags |= 0x80
		}
		dst = append(dst, flags)
		if deltaOK {
			dst = appendUvarint(dst, encodedTerm)
		} else {
			dst = appendUvarint(dst, entry.Term)
		}
		if newExtent {
			if i != 0 && (entry.ExtentOffset <= previousExtent || previousExtentBytes > entry.ExtentOffset-previousExtent) {
				return nil, ErrCorrupt
			}
			dst = appendUvarint(dst, entry.ExtentOffset-previousExtent)
			dst = appendUvarint(dst, entry.ExtentBytes)
			dst = appendUvarint(dst, entry.ExtentID)
			dst = append(dst, entry.BatchID[:]...)
			previousExtent = entry.ExtentOffset
			previousExtentBytes, previousDataEnd = entry.ExtentBytes, 0
			previousBatch, previousExtentID = entry.BatchID, entry.ExtentID
		} else if entry.ExtentBytes != previousExtentBytes || entry.ExtentID != previousExtentID || entry.BatchID != previousBatch || entry.DataOffset < previousDataEnd {
			return nil, ErrCorrupt
		}
		dst = appendUvarint(dst, entry.DataOffset)
		dst = appendUvarint(dst, entry.DataBytes)
		previousDataEnd = entry.DataOffset + entry.DataBytes
		previousTerm = entry.Term
	}
	if len(dst)-start > sealedMaxBlockRouteBytes {
		return nil, ErrBounds
	}
	return dst, nil
}

func decodeRoutePayload(src []byte, count uint32, dst []routeEntry) ([]routeEntry, error) {
	if count == 0 || uint64(count) > uint64(len(src))/4+1 || cap(dst) < int(count) {
		return nil, ErrCorrupt
	}
	dst = dst[:count]
	cursor := canonicalCursor{data: src}
	previousTerm, previousExtent, extentBytes, previousDataEnd := uint64(0), uint64(0), uint64(0), uint64(0)
	extentID := uint64(0)
	var batchID WaveID
	for i := range dst {
		flags, err := cursor.byte()
		if err != nil || flags&0x30 != 0 || uint16(flags&0x0f) > 2 {
			return nil, ErrCorrupt
		}
		encodedDelta, err := cursor.uvarint()
		if err != nil {
			return nil, ErrCorrupt
		}
		term, ok := applySignedDelta(previousTerm, encodedDelta)
		if flags&0x40 != 0 {
			if _, canonical := signedDelta(previousTerm, encodedDelta); canonical {
				return nil, ErrCorrupt
			}
			term, ok = encodedDelta, encodedDelta != 0
		}
		if !ok || term == 0 {
			return nil, ErrCorrupt
		}
		if flags&0x80 != 0 {
			delta, readErr := cursor.uvarint()
			if readErr != nil || i != 0 && (delta == 0 || delta < extentBytes) || previousExtent > ^uint64(0)-delta {
				return nil, ErrCorrupt
			}
			previousExtent += delta
			extentBytes, err = cursor.uvarint()
			if err != nil || previousExtent > sealedMaxSegmentBytes || extentBytes > sealedMaxSegmentBytes-previousExtent {
				return nil, ErrCorrupt
			}
			extentID, err = cursor.uvarint()
			if err != nil {
				return nil, ErrCorrupt
			}
			previousDataEnd = 0
			batch, takeErr := cursor.take(16)
			if takeErr != nil {
				return nil, ErrCorrupt
			}
			copy(batchID[:], batch)
		} else if i == 0 {
			return nil, ErrCorrupt
		}
		dataOffset, err := cursor.uvarint()
		if err != nil {
			return nil, ErrCorrupt
		}
		dataBytes, err := cursor.uvarint()
		if err != nil || dataOffset < previousDataEnd || dataOffset > extentBytes || dataBytes > extentBytes-dataOffset {
			return nil, ErrCorrupt
		}
		dst[i] = routeEntry{Term: term, Type: uint16(flags & 0x0f), ExtentOffset: previousExtent, ExtentBytes: extentBytes, ExtentID: extentID, DataOffset: dataOffset, DataBytes: dataBytes, BatchID: batchID}
		previousTerm = term
		previousDataEnd = dataOffset + dataBytes
	}
	if !cursor.empty() {
		return nil, ErrCorrupt
	}
	return dst, nil
}

func signedDelta(previous, current uint64) (uint64, bool) {
	if current >= previous {
		delta := current - previous
		return delta << 1, delta <= ^uint64(0)>>1
	}
	delta := previous - current
	return delta<<1 | 1, delta <= ^uint64(0)>>1
}

func applySignedDelta(previous, encoded uint64) (uint64, bool) {
	delta := encoded >> 1
	if encoded&1 == 0 {
		return previous + delta, previous <= ^uint64(0)-delta
	}
	return previous - delta, previous >= delta
}

func marshalRouteDescriptor(dst []byte, descriptor routeDescriptor, key [32]byte, logID [16]byte, segmentID, groupID uint64, ordinal uint32, payload []byte) ([]byte, error) {
	if len(dst) < sealedRouteDescriptorBytes || descriptor.Entries == 0 || descriptor.PayloadBytes != uint32(len(payload)) || descriptor.PayloadBytes > sealedMaxBlockRouteBytes || uint64(descriptor.PayloadOffset)+uint64(descriptor.PayloadBytes) > sealedMaxSegmentBytes || uint64(descriptor.ExtentOffset)+uint64(descriptor.ExtentBytes) > sealedMaxSegmentBytes {
		return nil, ErrCorrupt
	}
	b := dst[:sealedRouteDescriptorBytes]
	clear(b)
	binary.LittleEndian.PutUint32(b[0:4], descriptor.PayloadOffset)
	binary.LittleEndian.PutUint32(b[4:8], descriptor.PayloadBytes)
	binary.LittleEndian.PutUint32(b[8:12], descriptor.Entries)
	binary.LittleEndian.PutUint32(b[12:16], descriptor.ExtentOffset)
	binary.LittleEndian.PutUint32(b[16:20], descriptor.ExtentBytes)
	workspace := newRouteAuthWorkspace(key)
	tag := workspace.tag(logID, segmentID, groupID, ordinal, b[:24], payload)
	copy(b[24:40], tag[:])
	return b, nil
}

func (w *routeAuthWorkspace) unmarshalRouteDescriptor(src, payload []byte, logID [16]byte, segmentID, groupID uint64, ordinal uint32) (routeDescriptor, error) {
	if len(src) != sealedRouteDescriptorBytes {
		return routeDescriptor{}, ErrCorrupt
	}
	d := routeDescriptor{PayloadOffset: binary.LittleEndian.Uint32(src[0:4]), PayloadBytes: binary.LittleEndian.Uint32(src[4:8]), Entries: binary.LittleEndian.Uint32(src[8:12]), ExtentOffset: binary.LittleEndian.Uint32(src[12:16]), ExtentBytes: binary.LittleEndian.Uint32(src[16:20])}
	copy(d.Tag[:], src[24:40])
	if !allZero(src[20:24]) || d.Entries == 0 || d.PayloadBytes != uint32(len(payload)) || d.PayloadBytes > sealedMaxBlockRouteBytes || uint64(d.PayloadOffset)+uint64(d.PayloadBytes) > sealedMaxSegmentBytes || uint64(d.ExtentOffset)+uint64(d.ExtentBytes) > sealedMaxSegmentBytes {
		return routeDescriptor{}, ErrCorrupt
	}
	want := w.tag(logID, segmentID, groupID, ordinal, src[:24], payload)
	if !hmac.Equal(d.Tag[:], want[:]) {
		return routeDescriptor{}, fmt.Errorf("%w: route block authentication", ErrCorrupt)
	}
	return d, nil
}

func (w *routeAuthWorkspace) tag(logID [16]byte, segmentID, groupID uint64, ordinal uint32, descriptor, payload []byte) [sealedRouteTagBytes]byte {
	w.mac.Reset()
	_, _ = w.mac.Write(sealedRouteDomain[:])
	copy(w.context[:16], logID[:])
	binary.LittleEndian.PutUint64(w.context[16:24], segmentID)
	binary.LittleEndian.PutUint64(w.context[24:32], groupID)
	binary.LittleEndian.PutUint32(w.context[32:36], ordinal)
	_, _ = w.mac.Write(w.context[:])
	_, _ = w.mac.Write(descriptor)
	_, _ = w.mac.Write(payload)
	_ = w.mac.Sum(w.sum[:0])
	var tag [sealedRouteTagBytes]byte
	copy(tag[:], w.sum[:sealedRouteTagBytes])
	return tag
}
