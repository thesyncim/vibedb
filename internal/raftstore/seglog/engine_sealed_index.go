package seglog

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
)

type pendingRouteBlock struct {
	groupID                   uint64
	ordinal                   uint32
	extentOffset, extentBytes uint64
	payload                   []byte
	entries                   uint32
}

func readUnpublishedSealed(path string, logID [16]byte, previousID uint64, previousHash, key [32]byte) (SegmentMeta, segmentFooter, error) {
	file, err := os.Open(path)
	if err != nil {
		return SegmentMeta{}, segmentFooter{}, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil || stat.Size() < segmentHeaderBytes+sealedIndexHeaderBytes+segmentFooterBytes {
		return SegmentMeta{}, segmentFooter{}, ErrCorrupt
	}
	var headerBytes [segmentHeaderBytes]byte
	var footerBytes [segmentFooterBytes]byte
	if readFullAt(file, headerBytes[:], 0) != nil || readFullAt(file, footerBytes[:], stat.Size()-segmentFooterBytes) != nil {
		return SegmentMeta{}, segmentFooter{}, ErrCorrupt
	}
	header, err := unmarshalSegmentHeader(headerBytes[:])
	if err != nil {
		return SegmentMeta{}, segmentFooter{}, err
	}
	footer, err := unmarshalSegmentFooter(footerBytes[:])
	if err != nil {
		return SegmentMeta{}, segmentFooter{}, err
	}
	want := SegmentMeta{ID: header.ID, Generation: header.Generation, Bytes: uint64(stat.Size()), Records: footer.Records, IndexOffset: footer.IndexOffset, IndexBytes: footer.IndexBytes, PreviousHash: header.PreviousHash, Hash: footer.Hash, State: SegmentSealed}
	if _, _, _, _, err = readSealedSealedMetadata(file, want, logID, previousID, previousHash, key); err != nil {
		return SegmentMeta{}, segmentFooter{}, err
	}
	return want, footer, nil
}

func readSealedSealedMetadata(file *os.File, want SegmentMeta, logID [16]byte, previousID uint64, previousHash, key [32]byte) (segmentFooter, sealedIndexHeader, []sealedGroupRun, uint64, error) {
	stat, err := file.Stat()
	if err != nil {
		return segmentFooter{}, sealedIndexHeader{}, nil, 0, err
	}
	if stat.Size() < segmentHeaderBytes+sealedIndexHeaderBytes+segmentFooterBytes || uint64(stat.Size()) != want.Bytes {
		return segmentFooter{}, sealedIndexHeader{}, nil, 0, ErrCorrupt
	}
	var headerBytes [segmentHeaderBytes]byte
	if _, err = file.ReadAt(headerBytes[:], 0); err != nil {
		return segmentFooter{}, sealedIndexHeader{}, nil, 0, err
	}
	segment, err := unmarshalSegmentHeader(headerBytes[:])
	if err != nil || segment.ID != want.ID || segment.Generation != want.Generation || segment.LogID != logID || segment.PreviousID != previousID || segment.PreviousHash != previousHash {
		return segmentFooter{}, sealedIndexHeader{}, nil, 0, ErrCorrupt
	}
	var footerBytes [segmentFooterBytes]byte
	if _, err = file.ReadAt(footerBytes[:], stat.Size()-segmentFooterBytes); err != nil {
		return segmentFooter{}, sealedIndexHeader{}, nil, 0, err
	}
	footer, err := unmarshalSegmentFooter(footerBytes[:])
	if err != nil || footer.ID != segment.ID || footer.Generation != segment.Generation || footer.IndexOffset != footer.DataBytes || footer.IndexBytes != want.IndexBytes || footer.IndexOffset+footer.IndexBytes+segmentFooterBytes != uint64(stat.Size()) || footer.Hash != want.Hash {
		return segmentFooter{}, sealedIndexHeader{}, nil, 0, ErrCorrupt
	}
	var indexHeaderBytes [sealedIndexHeaderBytes]byte
	if _, err = file.ReadAt(indexHeaderBytes[:], int64(footer.IndexOffset)); err != nil {
		return segmentFooter{}, sealedIndexHeader{}, nil, 0, err
	}
	indexHeader, err := unmarshalSealedIndexHeader(indexHeaderBytes[:])
	if err != nil || uint64(indexHeader.TotalBytes) != footer.IndexBytes || uint64(indexHeader.DataBytes) != footer.DataBytes || uint64(indexHeader.DirectoryBytes) > footer.IndexBytes-sealedIndexHeaderBytes {
		return segmentFooter{}, sealedIndexHeader{}, nil, 0, ErrCorrupt
	}
	directory := make([]byte, indexHeader.DirectoryBytes)
	if len(directory) != 0 {
		if _, err = file.ReadAt(directory, int64(footer.IndexOffset+sealedIndexHeaderBytes)); err != nil {
			return segmentFooter{}, sealedIndexHeader{}, nil, 0, err
		}
	}
	top := make([]byte, sealedIndexHeaderBytes+len(directory))
	copy(top, indexHeaderBytes[:])
	copy(top[sealedIndexHeaderBytes:], directory)
	if footer.Auth != segmentSealedMetadataMAC(key, segment, top, footer) {
		return segmentFooter{}, sealedIndexHeader{}, nil, 0, fmt.Errorf("%w: sealed top authentication", ErrCorrupt)
	}
	runs, err := decodeRunDirectory(directory, uint64(indexHeader.Runs))
	if err != nil {
		return segmentFooter{}, sealedIndexHeader{}, nil, 0, err
	}
	totalDescriptors := uint64(indexHeader.DescriptorBytes / sealedRouteDescriptorBytes)
	for i := range runs {
		run := &runs[i]
		baseOrdinal := run.DescriptorOrdinal
		if baseOrdinal > totalDescriptors || uint64(run.DescriptorCount) > totalDescriptors-baseOrdinal {
			return segmentFooter{}, sealedIndexHeader{}, nil, 0, ErrCorrupt
		}
		if footer.IndexOffset > sealedMaxSegmentBytes || uint64(indexHeader.DescriptorOffset) > sealedMaxSegmentBytes-footer.IndexOffset || baseOrdinal > (sealedMaxSegmentBytes-footer.IndexOffset-uint64(indexHeader.DescriptorOffset))/sealedRouteDescriptorBytes {
			return segmentFooter{}, sealedIndexHeader{}, nil, 0, ErrCorrupt
		}
		if run.First != 0 && (run.ExtentOffset < segmentHeaderBytes || run.ExtentOffset > footer.DataBytes || run.ExtentBytes > footer.DataBytes-run.ExtentOffset) {
			return segmentFooter{}, sealedIndexHeader{}, nil, 0, ErrCorrupt
		}
		if (run.Summary.LastIndex == 0) != (run.Summary.LastTerm == 0) || (run.Summary.TruncateIndex == 0) != (run.Summary.TruncateTerm == 0) {
			return segmentFooter{}, sealedIndexHeader{}, nil, 0, ErrCorrupt
		}
	}
	return footer, indexHeader, runs, uint64(len(indexHeaderBytes)) + uint64(len(directory)) + segmentHeaderBytes + segmentFooterBytes, nil
}

func readFullAt(file *os.File, dst []byte, offset int64) error {
	n, err := file.ReadAt(dst, offset)
	if err != nil && err != io.EOF {
		return err
	}
	if n != len(dst) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (e *Engine) applySealedRun(group *engineGroup, segment SegmentMeta, header sealedIndexHeader, run sealedGroupRun) error {
	summary := run.Summary
	if summary.LastIndex < group.Hard.Commit || (summary.LastIndex == 0) != (summary.LastTerm == 0) || summary.Hard.Term < group.Hard.Term || summary.Hard.Commit < group.Hard.Commit || summary.Hard.Commit > summary.LastIndex || summary.Hard.Term == group.Hard.Term && group.Hard.Vote != 0 && summary.Hard.Vote != group.Hard.Vote || summary.TruncateIndex < group.TruncateIndex || summary.Checkpoint.Index < group.Checkpoint.Index {
		return ErrCorrupt
	}
	group.clipSealedSuffix(summary.LastIndex)
	if run.First != 0 {
		if run.Last != summary.LastIndex || run.First > run.Last || run.DescriptorOrdinal > uint64(header.DescriptorBytes/sealedRouteDescriptorBytes) || uint64(run.DescriptorCount) > uint64(header.DescriptorBytes/sealedRouteDescriptorBytes)-run.DescriptorOrdinal {
			return ErrCorrupt
		}
		group.clipSealedSuffix(run.First - 1)
		descriptorBase := segment.IndexOffset + uint64(header.DescriptorOffset) + run.DescriptorOrdinal*sealedRouteDescriptorBytes
		group.sealed = append(group.sealed, sealedRunRef{SegmentID: segment.ID, GroupID: run.GroupID, First: run.First, Last: run.Last, RouteFirst: run.First, RouteLast: run.Last, DescriptorBase: descriptorBase, DescriptorCount: run.DescriptorCount, BlockEntries: run.BlockEntries, ExtentOffset: run.ExtentOffset, ExtentBytes: run.ExtentBytes, Inline: run.Inline})
	}
	if summary.TruncateIndex != 0 {
		group.clipSealedThrough(summary.TruncateIndex)
		group.TruncateIndex, group.TruncateTerm = summary.TruncateIndex, summary.TruncateTerm
	}
	if summary.Checkpoint.Index != 0 {
		group.clipSealedThrough(summary.Checkpoint.Index)
		group.Checkpoint = summary.Checkpoint
		group.TruncateIndex, group.TruncateTerm = summary.Checkpoint.Index, summary.Checkpoint.Term
	}
	if summary.Hard != (HardState{}) {
		group.Hard = summary.Hard
	}
	if summary.NodeIncarnation != 0 {
		if summary.NodeIncarnation < group.NodeIncarnation || summary.NodeIncarnation == group.NodeIncarnation && summary.ReadyID < group.ReadyID {
			return ErrCorrupt
		}
		group.NodeIncarnation, group.ReadyID, group.ReadyDigest, group.ReadyWaveID = summary.NodeIncarnation, summary.ReadyID, summary.ReadyDigest, summary.ReadyWaveID
	}
	group.lastIndex, group.lastTerm = summary.LastIndex, summary.LastTerm
	return nil
}

func verifySealedRuns(file *os.File, segment SegmentMeta, header sealedIndexHeader, runs []sealedGroupRun, verifier *Engine, key [32]byte) error {
	router, err := newLazyRouteReader(file, key, verifier.log.manifest.LogID, segment.ID, 0, true)
	if err != nil {
		return err
	}
	for i := range runs {
		run := runs[i]
		group := verifier.groups[run.GroupID]
		if group == nil {
			return ErrCorrupt
		}
		last := durableLast(group)
		lastTerm := uint64(0)
		if last != 0 {
			lastTerm, err = termAt(group, nil, last)
			if err != nil {
				return err
			}
		}
		wantSummary := sealedRunSummary{LastIndex: last, LastTerm: lastTerm, Hard: group.Hard, TruncateIndex: group.TruncateIndex, TruncateTerm: group.TruncateTerm, Checkpoint: group.Checkpoint, NodeIncarnation: group.NodeIncarnation, ReadyID: group.ReadyID, ReadyDigest: group.ReadyDigest, ReadyWaveID: group.ReadyWaveID}
		if run.Summary != wantSummary {
			return ErrCorrupt
		}
		first := sort.Search(len(group.Entries), func(j int) bool { return group.Entries[j].SegmentID >= segment.ID })
		active := group.Entries[first:]
		if len(active) == 0 {
			if run.First != 0 {
				return ErrCorrupt
			}
			continue
		}
		if run.First != active[0].Index || run.Last != active[len(active)-1].Index {
			return ErrCorrupt
		}
		ref := sealedRunRef{SegmentID: segment.ID, GroupID: run.GroupID, First: run.First, Last: run.Last, RouteFirst: run.First, RouteLast: run.Last, DescriptorBase: segment.IndexOffset + uint64(header.DescriptorOffset) + run.DescriptorOrdinal*sealedRouteDescriptorBytes, DescriptorCount: run.DescriptorCount, BlockEntries: run.BlockEntries, ExtentOffset: run.ExtentOffset, ExtentBytes: run.ExtentBytes, Inline: run.Inline}
		for j := range active {
			route, routeErr := router.Point(ref, active[j].Index)
			if routeErr != nil {
				return routeErr
			}
			want := routeEntry{Term: active[j].Term, Type: uint16(active[j].Type), ExtentOffset: active[j].Offset, ExtentBytes: active[j].Bytes, ExtentID: active[j].ExtentID, DataOffset: active[j].DataOffset, DataBytes: active[j].DataBytes, BatchID: active[j].BatchID}
			if route != want {
				return ErrCorrupt
			}
		}
	}
	return nil
}

// marshalEngineSealedIndex runs only on the rotation/control path. It snapshots
// final group semantics and only the active segment's still-live entry routes;
// overwritten locations never enter the sealed index.
func (e *Engine) marshalEngineSealedIndex(dataBytes uint64) ([]byte, int, error) {
	if dataBytes > sealedMaxSegmentBytes {
		return nil, 0, ErrBounds
	}
	touched := make(map[uint64]struct{})
	for i := range e.log.events {
		if e.log.events[i].GroupID != engineWaveGroup {
			touched[e.log.events[i].GroupID] = struct{}{}
		}
	}
	groups := make([]uint64, 0, len(touched))
	for groupID := range touched {
		groups = append(groups, groupID)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i] < groups[j] })
	runs := make([]sealedGroupRun, 0, len(groups))
	blocks := make([]pendingRouteBlock, 0)
	descriptorBase := uint64(0)
	for _, groupID := range groups {
		group := e.groups[groupID]
		if group == nil {
			return nil, 0, ErrCorrupt
		}
		summary, exact := e.sealSummaryOverride[groupID]
		if !exact {
			lastIndex := durableLast(group)
			lastTerm := uint64(0)
			if lastIndex != 0 {
				var termErr error
				lastTerm, termErr = termAt(group, nil, lastIndex)
				if termErr != nil {
					return nil, 0, termErr
				}
			}
			summary = sealedRunSummary{LastIndex: lastIndex, LastTerm: lastTerm, Hard: group.Hard, TruncateIndex: group.TruncateIndex, TruncateTerm: group.TruncateTerm, Checkpoint: group.Checkpoint, NodeIncarnation: group.NodeIncarnation, ReadyID: group.ReadyID, ReadyDigest: group.ReadyDigest, ReadyWaveID: group.ReadyWaveID}
		}
		run := sealedGroupRun{GroupID: groupID, BlockEntries: sealedDefaultBlockEntries, DescriptorOrdinal: descriptorBase, Summary: summary}
		first := sort.Search(len(group.Entries), func(i int) bool { return group.Entries[i].SegmentID >= e.log.manifest.ActiveID })
		for i := first; i < len(group.Entries); i++ {
			if group.Entries[i].SegmentID != e.log.manifest.ActiveID {
				return nil, 0, ErrCorrupt
			}
		}
		active := group.Entries[first:]
		if len(active) == 0 {
			run.BlockEntries = 0
			runs = append(runs, run)
			continue
		}
		run.First, run.Last = active[0].Index, active[len(active)-1].Index
		if run.Last-run.First+1 != uint64(len(active)) {
			return nil, 0, ErrCorrupt
		}
		routes := make([]routeEntry, len(active))
		minExtent, maxExtent := uint64(math.MaxUint64), uint64(0)
		for i := range active {
			location := active[i]
			routes[i] = routeEntry{Term: location.Term, Type: uint16(location.Type), ExtentOffset: location.Offset, ExtentBytes: location.Bytes, ExtentID: location.ExtentID, DataOffset: location.DataOffset, DataBytes: location.DataBytes, BatchID: location.BatchID}
			if !validRouteEntry(routes[i]) {
				return nil, 0, ErrCorrupt
			}
			if location.Offset < minExtent {
				minExtent = location.Offset
			}
			if end := routes[i].ExtentOffset + routes[i].ExtentBytes; end > maxExtent {
				maxExtent = end
			}
		}
		run.ExtentOffset, run.ExtentBytes = minExtent, maxExtent-minExtent
		if len(routes) == 1 {
			run.Inline = routes[0]
			runs = append(runs, run)
			continue
		}
		for first := 0; first < len(routes); first += sealedDefaultBlockEntries {
			last := min(first+sealedDefaultBlockEntries, len(routes))
			payload, err := appendRoutePayload(nil, routes[first:last])
			if err != nil {
				return nil, 0, err
			}
			blockMin, blockMax := routes[first].ExtentOffset, routes[first].ExtentOffset+routes[first].ExtentBytes
			for i := first + 1; i < last; i++ {
				if routes[i].ExtentOffset < blockMin {
					blockMin = routes[i].ExtentOffset
				}
				if end := routes[i].ExtentOffset + routes[i].ExtentBytes; end > blockMax {
					blockMax = end
				}
			}
			blocks = append(blocks, pendingRouteBlock{groupID: groupID, ordinal: uint32((first) / sealedDefaultBlockEntries), extentOffset: blockMin, extentBytes: blockMax - blockMin, payload: payload, entries: uint32(last - first)})
			run.DescriptorCount++
		}
		descriptorBase += uint64(run.DescriptorCount)
		runs = append(runs, run)
	}
	directory, err := appendRunDirectory(nil, runs)
	if err != nil {
		return nil, 0, err
	}
	descriptorBytes := uint64(len(blocks) * sealedRouteDescriptorBytes)
	routeBytes := uint64(0)
	for i := range blocks {
		routeBytes += uint64(len(blocks[i].payload))
	}
	total := uint64(sealedIndexHeaderBytes+len(directory)) + descriptorBytes + routeBytes
	if total > sealedMaxSegmentBytes || dataBytes+total+segmentFooterBytes > sealedMaxSegmentBytes {
		return nil, 0, ErrBounds
	}
	header := sealedIndexHeader{TotalBytes: uint32(total), Runs: uint32(len(runs)), DirectoryBytes: uint32(len(directory)), DescriptorOffset: uint32(sealedIndexHeaderBytes + len(directory)), DescriptorBytes: uint32(descriptorBytes), RoutePayloadOffset: uint32(uint64(sealedIndexHeaderBytes+len(directory)) + descriptorBytes), RoutePayloadBytes: uint32(routeBytes), DataBytes: uint32(dataBytes), LastSequence: e.sequence}
	headerBytes, err := marshalSealedIndexHeader(header)
	if err != nil {
		return nil, 0, err
	}
	index := make([]byte, total)
	copy(index, headerBytes[:])
	copy(index[sealedIndexHeaderBytes:], directory)
	descriptorCursor, routeCursor := uint64(header.DescriptorOffset), uint64(header.RoutePayloadOffset)
	for i := range blocks {
		block := &blocks[i]
		payloadOffset := dataBytes + routeCursor
		descriptor := routeDescriptor{PayloadOffset: uint32(payloadOffset), PayloadBytes: uint32(len(block.payload)), Entries: block.entries, ExtentOffset: uint32(block.extentOffset), ExtentBytes: uint32(block.extentBytes)}
		if _, err = marshalRouteDescriptor(index[descriptorCursor:descriptorCursor+sealedRouteDescriptorBytes], descriptor, e.authKey, e.log.manifest.LogID, e.log.manifest.ActiveID, block.groupID, block.ordinal, block.payload); err != nil {
			return nil, 0, err
		}
		copy(index[routeCursor:], block.payload)
		descriptorCursor += sealedRouteDescriptorBytes
		routeCursor += uint64(len(block.payload))
	}
	return index, sealedIndexHeaderBytes + len(directory), nil
}
