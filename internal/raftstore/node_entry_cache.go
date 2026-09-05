package raftstore

import (
	"math"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	// The physical node owns one 8 MiB payload arena, independent of group count.
	// Large entries and ring misses retain the ordinary authenticated log path.
	nodeEntryCacheSlotBytes = 4 << 10
	nodeEntryCacheSlots     = 2048
)

// cacheEntryPayloadLocked stages a private copy while coordinateMu is held.
// Slot generations make eviction independent of per-group window lifetimes.
// Staged references are not attached to a serving cut until sync and namespace
// proof succeed. Evicting an older slot early causes only a storage fallback.
func (s *NodeStore) cacheEntryPayloadLocked(entry *nodeTermCoordinate, data []byte) {
	if len(s.entryCacheArena) == 0 || len(data) > nodeEntryCacheSlotBytes || s.entryCacheGeneration == math.MaxUint64 {
		return
	}
	s.entryCacheGeneration++
	generation := s.entryCacheGeneration
	slot := generation % nodeEntryCacheSlots
	copy(s.entryCacheArena[slot*nodeEntryCacheSlotBytes:], data)
	s.entryCacheGenerations[slot] = generation
	entry.payloadGeneration = generation
	entry.payloadBytes = uint32(len(data))
}

func cachedEntryWireBytes(entry nodeTermCoordinate) uint64 {
	// The storage projection always materializes Type, Term and Index; data is
	// omitted only when empty, matching readEntry's protobuf representation.
	size := 3 + protowire.SizeVarint(uint64(entry.kind)) + protowire.SizeVarint(entry.term) + protowire.SizeVarint(entry.index)
	if entry.payloadBytes != 0 {
		size += 1 + protowire.SizeVarint(uint64(entry.payloadBytes)) + int(entry.payloadBytes)
	}
	return uint64(size)
}

func (v *GroupView) cachedEntries(lo, hi, maxSize uint64) ([]*pb.Entry, bool, error) {
	s := v.store
	if err := s.coordinateReadError(); err != nil {
		return nil, true, err
	}
	s.coordinateMu.RLock()
	defer s.coordinateMu.RUnlock()
	cut, found := s.coordinates[v.group]
	if !found {
		return nil, false, nil
	}
	if lo < cut.first {
		return nil, true, raft.ErrCompacted
	}
	if hi < lo || hi > cut.last+1 {
		return nil, true, raft.ErrUnavailable
	}
	count := 0
	size := uint64(0)
	for index := lo; index < hi; index++ {
		entry := cut.terms[index%nodeRecentTerms]
		if entry.index != index || entry.payloadGeneration == 0 || s.entryCacheGenerations[entry.payloadGeneration%nodeEntryCacheSlots] != entry.payloadGeneration {
			return nil, false, nil
		}
		bytes := cachedEntryWireBytes(entry)
		if count > 0 && (size > math.MaxUint64-bytes || size+bytes > maxSize) {
			break
		}
		size += bytes
		count++
	}
	// No allocation until the whole selected prefix is proven resident. Payloads
	// and scalar pointers are detached before the publication lock is released.
	result := make([]*pb.Entry, 0, count)
	for index := lo; index < lo+uint64(count); index++ {
		cached := cut.terms[index%nodeRecentTerms]
		offset := (cached.payloadGeneration % nodeEntryCacheSlots) * nodeEntryCacheSlotBytes
		data := append([]byte(nil), s.entryCacheArena[offset:offset+uint64(cached.payloadBytes)]...)
		term, entryIndex, kind := cached.term, cached.index, cached.kind
		result = append(result, &pb.Entry{Term: &term, Index: &entryIndex, Type: &kind, Data: data})
	}
	return result, true, nil
}

// stageEntryPayloadsLocked requires the device mutex. Encryption is complete,
// but the plaintext arena will be reused by seglog for framing. Preserve only
// eligible payloads now; publishCoordinatesLocked exposes them after durability.
func (s *NodeStore) stageEntryPayloadsLocked(count int) {
	if s.coordinates == nil {
		return
	}
	s.coordinateMu.Lock()
	defer s.coordinateMu.Unlock()
	offset := uint64(0)
	for batchIndex := 0; batchIndex < count; batchIndex++ {
		clear(s.pendingEntryPayloads[batchIndex][:])
		batch := &s.waveBatches[batchIndex]
		_, warmed := s.coordinates[batch.GroupID]
		for index, entry := range batch.Entries {
			if warmed && index >= len(batch.Entries)-nodeRecentTerms && offset <= uint64(len(s.plainArena)) && entry.DataBytes <= uint64(len(s.plainArena))-offset {
				prepared := nodeTermCoordinate{index: entry.Index, term: entry.Term, kind: entry.Type}
				s.cacheEntryPayloadLocked(&prepared, s.plainArena[offset:offset+entry.DataBytes])
				s.pendingEntryPayloads[batchIndex][entry.Index%nodeRecentTerms] = prepared
			}
			offset += entry.DataBytes
		}
	}
}
