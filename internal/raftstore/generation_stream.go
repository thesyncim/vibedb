package raftstore

import (
	"bufio"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const generationReplayReadBufferBytes = 64 << 10

// generationScratchChunk names one authenticated retained-entry record in an
// unpublished target file. The slice is a tiny index over chunks, never over
// entries: chunk bytes and entry count are both independently bounded.
type generationScratchChunk struct {
	offset    int64
	total     int
	first     uint64
	last      uint64
	sequence  uint64
	previous  [sha256.Size]byte
	digest    [sha256.Size]byte
	footprint int64
}

// generationScratch is an overwriteable, disk-backed Raft log projection. It
// keeps only the current suffix above the future checkpoint. Source record
// ciphertext/plaintext, pending logical entries, retained encoding, and entry
// descriptors are all independently bounded by sealed format limits.
type generationScratch struct {
	store             *Store
	incarnation       uint64
	initialEnd        int64
	writeOffset       int64
	sequence          uint64
	previous          [sha256.Size]byte
	chunks            []generationScratchChunk
	pending           []*pb.Entry
	pendingPlainBytes int
	pendingFootprint  int64
	hard              *pb.HardState
	first             uint64
	last              uint64
	baseTerm          uint64
	baseKnown         bool
	lastTerm          uint64
	liveBytes         int64
	sourceFirst       uint64
	sourceLast        uint64
}

func newGenerationScratch(
	store *Store,
	baseIndex uint64,
	baseTerm uint64,
	baseKnown bool,
	incarnation uint64,
) (*generationScratch, error) {
	if store == nil || baseIndex == 0 || baseIndex == math.MaxUint64 ||
		incarnation == 0 || baseKnown && (baseTerm == 0 || baseTerm == math.MaxUint64) {
		return nil, ErrGenerationSource
	}
	return &generationScratch{
		store: store, incarnation: incarnation,
		initialEnd: store.current.walEnd, writeOffset: store.current.walEnd,
		sequence: store.current.recordSequence, previous: store.current.chainDigest,
		first: baseIndex + 1, last: baseIndex,
		baseTerm: baseTerm, baseKnown: baseKnown, lastTerm: baseTerm,
	}, nil
}

func validateGenerationReadyEntries(entries []*pb.Entry, options normalizedOptions) error {
	if len(entries) > MaxReadyEntries || uint64(len(entries)) > options.maxEntries {
		return fmt.Errorf("%w: Ready entry count", ErrBounds)
	}
	var previousIndex uint64
	var previousTerm uint64
	var batchBytes int64
	for ordinal, entry := range entries {
		if entry == nil || entry.GetIndex() == 0 || entry.GetIndex() == math.MaxUint64 ||
			entry.GetTerm() == 0 || entry.GetTerm() == math.MaxUint64 ||
			len(entry.ProtoReflect().GetUnknown()) != 0 ||
			entry.GetType() < pb.EntryNormal || entry.GetType() > pb.EntryConfChangeV2 ||
			(ordinal != 0 && (entry.GetIndex() != previousIndex+1 ||
				entry.GetTerm() < previousTerm)) ||
			len(entry.GetData()) > raftmodel.MaxProposalBytes ||
			batchBytes > int64(raftmodel.MaxUncommittedEntriesSize)-int64(len(entry.GetData())) {
			return fmt.Errorf("%w: malformed entry ordinal %d", ErrInvalid, ordinal)
		}
		previousIndex = entry.GetIndex()
		previousTerm = entry.GetTerm()
		batchBytes += int64(len(entry.GetData()))
	}
	return nil
}

func (scratch *generationScratch) applyEntries(
	entries []*pb.Entry,
	committed uint64,
) error {
	if scratch == nil || scratch.store == nil || scratch.first == 0 ||
		scratch.last == math.MaxUint64 || scratch.first-1 > scratch.last ||
		(scratch.last >= scratch.first && !scratch.baseKnown) {
		return fmt.Errorf("%w: invalid generation scratch image", ErrCorrupt)
	}
	options := scratch.store.options
	if len(entries) == 0 {
		return nil
	}
	start := entries[0].GetIndex()
	if start < scratch.first || start > scratch.last+1 {
		return fmt.Errorf("%w: entries start %d outside [%d,%d]",
			ErrInvalid, start, scratch.first, scratch.last+1)
	}
	boundaryTerm, err := scratch.term(start - 1)
	if err != nil || entries[0].GetTerm() < boundaryTerm {
		return fmt.Errorf("%w: entry term decreases across retained boundary", ErrInvalid)
	}
	overlapLast := entries[len(entries)-1].GetIndex()
	if overlapLast > scratch.last {
		overlapLast = scratch.last
	}
	if start <= overlapLast {
		if err := scratch.compareOverlap(start, overlapLast, entries, committed); err != nil {
			return err
		}
	}
	removedBytes, err := scratch.footprintFrom(start)
	if err != nil {
		return err
	}
	newBytes := entriesFootprint(entries)
	nextLive := scratch.liveBytes - removedBytes + newBytes
	newCount := start - scratch.first + uint64(len(entries))
	if nextLive < 0 || nextLive > options.maxLiveBytes || newCount > options.maxEntries {
		return fmt.Errorf("%w: generation scratch live geometry", ErrBounds)
	}
	if err := scratch.truncate(start); err != nil {
		return err
	}
	if err := scratch.append(entries); err != nil {
		return err
	}
	scratch.liveBytes = nextLive
	scratch.last = entries[len(entries)-1].GetIndex()
	scratch.lastTerm = entries[len(entries)-1].GetTerm()
	return nil
}

// project applies one already-validated historical Ready to the logical suffix
// above the future checkpoint. The term at the checkpoint is historical state:
// it can be absent or lower than the final snapshot term until a later Ready
// regrows or overwrites that index.
func (scratch *generationScratch) project(
	entries []*pb.Entry,
	committed uint64,
) error {
	if len(entries) == 0 {
		return nil
	}
	baseIndex := scratch.first - 1
	start := entries[0].GetIndex()
	last := entries[len(entries)-1].GetIndex()
	if start > baseIndex {
		return scratch.applyEntries(entries, committed)
	}
	overlapLast := last
	if overlapLast > scratch.last {
		overlapLast = scratch.last
	}
	if start <= overlapLast {
		if err := scratch.compareOverlap(start, overlapLast, entries, committed); err != nil {
			return err
		}
	}
	baseKnown := last >= baseIndex
	var baseTerm uint64
	if baseKnown {
		baseTerm = entries[baseIndex-start].GetTerm()
	}
	scratch.reset(baseTerm, baseKnown)
	if last == baseIndex {
		return nil
	}
	if last < baseIndex {
		return nil
	}
	return scratch.applyEntries(entries[baseIndex+1-start:], committed)
}

func (scratch *generationScratch) reset(baseTerm uint64, baseKnown bool) {
	clear(scratch.chunks)
	scratch.chunks = scratch.chunks[:0]
	scratch.clearPending()
	scratch.writeOffset = scratch.initialEnd
	scratch.sequence = scratch.store.current.recordSequence
	scratch.previous = scratch.store.current.chainDigest
	scratch.last = scratch.first - 1
	scratch.baseTerm = baseTerm
	scratch.baseKnown = baseKnown
	scratch.lastTerm = baseTerm
	scratch.liveBytes = 0
}

func (scratch *generationScratch) compareOverlap(
	first uint64,
	last uint64,
	replacements []*pb.Entry,
	committed uint64,
) error {
	for _, chunk := range scratch.chunks {
		if chunk.last < first {
			continue
		}
		if chunk.first > last {
			break
		}
		entries, err := scratch.readChunk(chunk)
		if err != nil {
			return err
		}
		for _, existing := range entries {
			index := existing.GetIndex()
			if index < first || index > last {
				continue
			}
			replacement := replacements[index-first]
			equal := entriesSemanticallyEqual(existing, replacement)
			if existing.GetTerm() == replacement.GetTerm() && !equal {
				return fmt.Errorf("%w: entry %d changes bytes within term %d",
					ErrCorrupt, index, replacement.GetTerm())
			}
			if index <= committed && !equal {
				return fmt.Errorf("%w: entry %d overwrites committed prefix", ErrInvalid, index)
			}
		}
	}
	for _, existing := range scratch.pending {
		index := existing.GetIndex()
		if index < first || index > last {
			continue
		}
		replacement := replacements[index-first]
		equal := entriesSemanticallyEqual(existing, replacement)
		if existing.GetTerm() == replacement.GetTerm() && !equal {
			return fmt.Errorf("%w: entry %d changes bytes within term %d",
				ErrCorrupt, index, replacement.GetTerm())
		}
		if index <= committed && !equal {
			return fmt.Errorf("%w: entry %d overwrites committed prefix", ErrInvalid, index)
		}
	}
	return nil
}

func (scratch *generationScratch) term(index uint64) (uint64, error) {
	if index == scratch.first-1 {
		if !scratch.baseKnown {
			return 0, ErrInvalid
		}
		return scratch.baseTerm, nil
	}
	if index < scratch.first || index > scratch.last {
		return 0, ErrInvalid
	}
	if index == scratch.last {
		return scratch.lastTerm, nil
	}
	for _, chunk := range scratch.chunks {
		if index < chunk.first || index > chunk.last {
			continue
		}
		entries, err := scratch.readChunk(chunk)
		if err != nil {
			return 0, err
		}
		return entries[index-chunk.first].GetTerm(), nil
	}
	if len(scratch.pending) != 0 {
		first := scratch.pending[0].GetIndex()
		last := scratch.pending[len(scratch.pending)-1].GetIndex()
		if index >= first && index <= last {
			return scratch.pending[index-first].GetTerm(), nil
		}
	}
	return 0, ErrCorrupt
}

func (scratch *generationScratch) footprintFrom(index uint64) (int64, error) {
	if index == scratch.last+1 {
		return 0, nil
	}
	var result int64
	for _, chunk := range scratch.chunks {
		if chunk.last < index {
			continue
		}
		if chunk.first >= index {
			result += chunk.footprint
			continue
		}
		entries, err := scratch.readChunk(chunk)
		if err != nil {
			return 0, err
		}
		result += entriesFootprint(entries[index-chunk.first:])
	}
	if len(scratch.pending) != 0 {
		first := scratch.pending[0].GetIndex()
		last := scratch.pending[len(scratch.pending)-1].GetIndex()
		switch {
		case index <= first:
			result += scratch.pendingFootprint
		case index <= last:
			result += entriesFootprint(scratch.pending[index-first:])
		}
	}
	return result, nil
}

func (scratch *generationScratch) truncate(index uint64) error {
	if index == scratch.last+1 {
		return nil
	}
	for position, chunk := range scratch.chunks {
		if index < chunk.first || index > chunk.last {
			continue
		}
		if index == chunk.first {
			scratch.clearPending()
			scratch.writeOffset = chunk.offset
			scratch.sequence = chunk.sequence - 1
			scratch.previous = chunk.previous
			clear(scratch.chunks[position:])
			scratch.chunks = scratch.chunks[:position]
			return nil
		}
		entries, err := scratch.readChunk(chunk)
		if err != nil {
			return err
		}
		keep := entries[:index-chunk.first]
		scratch.clearPending()
		clear(scratch.chunks[position+1:])
		scratch.chunks = scratch.chunks[:position]
		scratch.writeOffset = chunk.offset
		scratch.sequence = chunk.sequence - 1
		scratch.previous = chunk.previous
		if err := scratch.append(keep); err != nil {
			return err
		}
		return nil
	}
	if len(scratch.pending) != 0 {
		first := scratch.pending[0].GetIndex()
		last := scratch.pending[len(scratch.pending)-1].GetIndex()
		if index >= first && index <= last {
			keep := int(index - first)
			clear(scratch.pending[keep:])
			scratch.pending = scratch.pending[:keep]
			scratch.recountPending()
			return nil
		}
	}
	return ErrCorrupt
}

func (scratch *generationScratch) append(entries []*pb.Entry) error {
	for _, entry := range entries {
		if !scratch.pendingCanAppend(entry) && len(scratch.pending) != 0 {
			if err := scratch.flushPending(); err != nil {
				return err
			}
		}
		if !scratch.pendingCanAppend(entry) {
			return ErrBounds
		}
		cloned := cloneEntry(entry)
		scratch.pending = append(scratch.pending, cloned)
		entryBytes := 32 + len(cloned.GetData())
		if scratch.pendingPlainBytes == 0 {
			scratch.pendingPlainBytes = retainedPayloadHeaderBytes
		}
		scratch.pendingPlainBytes += entryBytes
		scratch.pendingFootprint += int64(entryBytes)
	}
	return nil
}

func (scratch *generationScratch) pendingCanAppend(entry *pb.Entry) bool {
	if entry == nil || len(scratch.pending) >= MaxReadyEntries {
		return false
	}
	plainBytes := scratch.pendingPlainBytes
	if plainBytes == 0 {
		plainBytes = retainedPayloadHeaderBytes
	}
	plainBytes += 32 + len(entry.GetData())
	if generationRecordBytes(plainBytes, len(scratch.store.header.keyID)) >
		scratch.store.options.maxRecordBytes {
		return false
	}
	return len(scratch.pending) == 0 || plainBytes <= DefaultGenerationRetainedChunkBytes
}

func (scratch *generationScratch) flushPending() error {
	if len(scratch.pending) == 0 {
		return nil
	}
	payload, err := marshalRetainedEntries(scratch.pending)
	if err != nil {
		return err
	}
	sequence := scratch.sequence + 1
	record, digest, _, err := marshalRecord(
		recordKindRetainedEntries, 0, sequence, scratch.incarnation, 0,
		scratch.previous, payload, scratch.store.header, scratch.store.options,
	)
	if err != nil {
		return err
	}
	end, ok := addInt64(scratch.writeOffset, len(record))
	if !ok || end > scratch.store.options.maxFileBytes {
		return ErrFull
	}
	if err := writeExactAt(
		scratch.store.options.ops, scratch.store.file, record, scratch.writeOffset,
	); err != nil {
		return persistenceError("write generation replay scratch", false, err)
	}
	scratch.chunks = append(scratch.chunks, generationScratchChunk{
		offset: scratch.writeOffset, total: len(record),
		first:    scratch.pending[0].GetIndex(),
		last:     scratch.pending[len(scratch.pending)-1].GetIndex(),
		sequence: sequence, previous: scratch.previous, digest: digest,
		footprint: scratch.pendingFootprint,
	})
	scratch.writeOffset = end
	scratch.sequence = sequence
	scratch.previous = digest
	scratch.clearPending()
	return nil
}

func (scratch *generationScratch) clearPending() {
	clear(scratch.pending)
	scratch.pending = scratch.pending[:0]
	scratch.pendingPlainBytes = 0
	scratch.pendingFootprint = 0
}

func (scratch *generationScratch) recountPending() {
	scratch.pendingPlainBytes = 0
	scratch.pendingFootprint = 0
	if len(scratch.pending) == 0 {
		return
	}
	scratch.pendingPlainBytes = retainedPayloadHeaderBytes
	for _, entry := range scratch.pending {
		entryBytes := 32 + len(entry.GetData())
		scratch.pendingPlainBytes += entryBytes
		scratch.pendingFootprint += int64(entryBytes)
	}
}

func (scratch *generationScratch) readChunk(
	chunk generationScratchChunk,
) ([]*pb.Entry, error) {
	data := make([]byte, chunk.total)
	n, err := scratch.store.file.ReadAt(data, chunk.offset)
	if err != nil || n != len(data) {
		return nil, fmt.Errorf("%w: read generation scratch chunk: %v", ErrCorrupt, err)
	}
	record, err := unmarshalRecord(data, scratch.store.header, scratch.store.options)
	if err != nil {
		return nil, err
	}
	if record.envelope.kind != recordKindRetainedEntries ||
		record.envelope.sequence != chunk.sequence ||
		record.envelope.incarnation != scratch.incarnation ||
		record.envelope.previous != chunk.previous || record.digest != chunk.digest {
		return nil, fmt.Errorf("%w: generation scratch identity", ErrCorrupt)
	}
	entries, err := unmarshalRetainedEntriesView(record.payload, scratch.store.options)
	if err != nil || len(entries) == 0 || entries[0].GetIndex() != chunk.first ||
		entries[len(entries)-1].GetIndex() != chunk.last {
		return nil, errors.Join(ErrCorrupt, err)
	}
	return entries, nil
}

// replaySourceIntoGeneration validates the exact captured current-slot cut and
// reconstructs its logical image in the target WAL's private scratch stream.
func (builder *GenerationBuilder) replaySourceIntoGeneration(
	stage *Store,
) (*generationScratch, headerState, error) {
	if builder.source == nil {
		return nil, headerState{}, ErrClosed
	}
	header := cloneGenerationHeader(builder.header)
	checkpointIndex := builder.input.Snapshot.GetMetadata().GetIndex()
	checkpointTerm := builder.input.Snapshot.GetMetadata().GetTerm()
	offset := int64(HeaderBytes)
	sourceLength := builder.current.walEnd - offset
	if sourceLength <= 0 {
		return nil, headerState{}, ErrGenerationSource
	}
	sourceReader := bufio.NewReaderSize(
		io.NewSectionReader(builder.source, offset, sourceLength),
		generationReplayReadBufferBytes,
	)
	var prefix [recordPrefixBytes]byte
	var recordData []byte
	decodeWorkspace := newRecordDecodeWorkspace(header)
	defer func() {
		clear(prefix[:])
		clear(recordData[:cap(recordData)])
		clear(decodeWorkspace.aad[:cap(decodeWorkspace.aad)])
		clear(decodeWorkspace.plaintext[:cap(decodeWorkspace.plaintext)])
		clear(decodeWorkspace.tagContext[:])
		clear(decodeWorkspace.crypto.sequence[:])
		clear(decodeWorkspace.crypto.sum[:])
	}()
	previousDigest := header.headerDigest
	var scratch *generationScratch
	var sourceHard *pb.HardState
	var sourceFirst uint64
	var sourceLast uint64
	var sourceLastTerm uint64
	var lastReady retryKey
	var lastReadyDigest [sha256.Size]byte
	retainedHash := newRetainedSuffixHash(header.reference.index, header.reference.term)
	var retainedIncarnation uint64
	var retainedCount uint64
	var retainedBytes uint64
	var sourceGeneration generationSeal
	sourceGenerationSeen := false
	for sequence := uint64(1); sequence <= builder.current.recordSequence; sequence++ {
		if _, err := io.ReadFull(sourceReader, prefix[:]); err != nil {
			return nil, headerState{}, fmt.Errorf("%w: read source record %d prefix: %v",
				ErrGenerationSource, sequence, err)
		}
		envelope, err := inspectRecordPrefixWithWorkspace(
			prefix[:], header, builder.options, &decodeWorkspace,
		)
		if err != nil {
			return nil, headerState{}, fmt.Errorf("source record %d: %w", sequence, err)
		}
		end, ok := addInt64(offset, envelope.total)
		if !ok || end > builder.current.walEnd {
			return nil, headerState{}, fmt.Errorf("%w: source record %d exceeds captured cut",
				ErrGenerationSource, sequence)
		}
		if cap(recordData) < envelope.total {
			clear(recordData[:cap(recordData)])
			recordData = make([]byte, envelope.total)
		} else {
			recordData = recordData[:envelope.total]
		}
		copy(recordData[:recordPrefixBytes], prefix[:])
		n, readErr := io.ReadFull(sourceReader, recordData[recordPrefixBytes:])
		if readErr != nil || n != len(recordData)-recordPrefixBytes {
			return nil, headerState{}, fmt.Errorf("%w: read source record %d: %v",
				ErrGenerationSource, sequence, readErr)
		}
		record, err := unmarshalInspectedRecord(
			recordData, envelope, header, &decodeWorkspace,
		)
		if err != nil {
			return nil, headerState{}, fmt.Errorf("source record %d: %w", sequence, err)
		}
		if record.envelope.sequence != sequence || record.envelope.previous != previousDigest {
			return nil, headerState{}, fmt.Errorf("%w: source record %d chain gap",
				ErrGenerationSource, sequence)
		}
		switch {
		case sequence == 1:
			if record.envelope.kind != recordKindBootstrap ||
				uint64(len(record.payload)) != header.reference.size ||
				sha256.Sum256(record.payload) != header.reference.digest {
				return nil, headerState{}, fmt.Errorf("%w: source bootstrap reference",
					ErrGenerationSource)
			}
			bootstrap, snapshotBytes, decodeErr := unmarshalBootstrap(
				record.payload, header.identity.MemberID,
			)
			if decodeErr != nil {
				return nil, headerState{}, decodeErr
			}
			if bootstrap.Snapshot.GetMetadata().GetIndex() != header.reference.index ||
				bootstrap.Snapshot.GetMetadata().GetTerm() != header.reference.term {
				return nil, headerState{}, fmt.Errorf("%w: source bootstrap metadata",
					ErrGenerationSource)
			}
			header.topologyRecoveryEpoch = bootstrap.TopologyRecoveryEpoch
			header.snapshot = bootstrap.Snapshot
			header.snapshotBytes = snapshotBytes
			image := bootstrapImage(bootstrap.Snapshot)
			sourceHard = cloneHardState(image.hard)
			sourceFirst = image.first
			sourceLast = image.last
			sourceLastTerm = image.baseTerm
			baseKnown := image.last == checkpointIndex
			var historicalBaseTerm uint64
			if baseKnown {
				historicalBaseTerm = image.baseTerm
			}
			scratch, err = newGenerationScratch(
				stage, checkpointIndex, historicalBaseTerm, baseKnown,
				builder.current.currentIncarnation,
			)
			if err != nil {
				return nil, headerState{}, err
			}

		case record.envelope.kind == recordKindRetainedEntries && scratch != nil:
			if builder.parentBinding == ([sha256.Size]byte{}) || sourceGenerationSeen ||
				lastReady != (retryKey{}) {
				return nil, headerState{}, fmt.Errorf("%w: retained source record order",
					ErrGenerationSource)
			}
			if retainedIncarnation == 0 {
				retainedIncarnation = record.envelope.incarnation
			} else if retainedIncarnation != record.envelope.incarnation {
				return nil, headerState{}, fmt.Errorf("%w: retained source incarnation",
					ErrGenerationSource)
			}
			entries, decodeErr := unmarshalRetainedEntriesView(record.payload, builder.options)
			if decodeErr != nil || len(entries) == 0 ||
				entries[0].GetIndex() != sourceLast+1 {
				return nil, headerState{}, errors.Join(ErrGenerationSource, decodeErr)
			}
			if validateErr := validateGenerationReadyEntries(entries, builder.options); validateErr != nil {
				return nil, headerState{}, errors.Join(ErrGenerationSource, validateErr)
			}
			if entries[0].GetTerm() < sourceLastTerm {
				return nil, headerState{}, fmt.Errorf("%w: retained source term regression",
					ErrGenerationSource)
			}
			if projectErr := scratch.project(entries, sourceHard.GetCommit()); projectErr != nil {
				return nil, headerState{}, errors.Join(ErrGenerationSource, projectErr)
			}
			for _, entry := range entries {
				footprint := uint64(32 + len(entry.GetData()))
				if retainedCount == math.MaxUint64 ||
					retainedBytes > math.MaxUint64-footprint {
					return nil, headerState{}, ErrBounds
				}
				retainedHash.add(entry)
				retainedCount++
				retainedBytes += footprint
			}
			sourceLast = entries[len(entries)-1].GetIndex()
			sourceLastTerm = entries[len(entries)-1].GetTerm()

		case record.envelope.kind == recordKindGenerationSeal && scratch != nil:
			if builder.parentBinding == ([sha256.Size]byte{}) || sourceGenerationSeen ||
				lastReady != (retryKey{}) {
				return nil, headerState{}, fmt.Errorf("%w: source generation seal order",
					ErrGenerationSource)
			}
			seal, decodeErr := unmarshalGenerationSeal(record.payload)
			if decodeErr != nil {
				return nil, headerState{}, decodeErr
			}
			if seal.bindingDigest != builder.parentBinding ||
				seal.familyID != builder.familyID || seal.generation == math.MaxUint64 ||
				seal.generation+1 != builder.generation ||
				seal.identityDigest != generationIdentityDigest(header.identity) ||
				seal.topologyRecoveryEpoch != header.topologyRecoveryEpoch ||
				seal.baseIndex != header.reference.index || seal.baseTerm != header.reference.term ||
				seal.baseDigest != header.reference.digest ||
				seal.confDigest != generationConfDigest(header.snapshot.GetMetadata().GetConfState()) ||
				seal.suffixFirst != sourceFirst || seal.suffixLast != sourceLast ||
				seal.suffixCount != retainedCount || seal.suffixBytes != retainedBytes ||
				seal.suffixDigest != retainedHash.finish() ||
				record.envelope.incarnation != seal.sourceCurrentIncarnation ||
				(retainedIncarnation != 0 && retainedIncarnation != seal.sourceCurrentIncarnation) ||
				seal.hard.GetTerm() < sourceLastTerm ||
				seal.hard.GetCommit() < header.reference.index ||
				seal.hard.GetCommit() > sourceLast {
				return nil, headerState{}, fmt.Errorf("%w: source generation seal binding",
					ErrGenerationSource)
			}
			sourceHard = cloneHardState(seal.hard)
			sourceGeneration = seal
			sourceGenerationSeen = true

		case record.envelope.kind == recordKindReady && scratch != nil:
			if builder.parentBinding != ([sha256.Size]byte{}) && !sourceGenerationSeen {
				return nil, headerState{}, fmt.Errorf("%w: Ready precedes source generation seal",
					ErrGenerationSource)
			}
			if builder.current.currentIncarnation == 0 ||
				record.envelope.incarnation > builder.current.currentIncarnation ||
				(sourceGenerationSeen &&
					record.envelope.incarnation <= sourceGeneration.sourceCurrentIncarnation) ||
				(lastReady.incarnation != 0 &&
					(record.envelope.incarnation < lastReady.incarnation ||
						(record.envelope.incarnation == lastReady.incarnation &&
							record.envelope.readyID <= lastReady.readyID))) {
				return nil, headerState{}, fmt.Errorf("%w: source Ready identity regression",
					ErrGenerationSource)
			}
			payload, decodeErr := unmarshalReadyPayloadView(record.payload, builder.options)
			if decodeErr != nil {
				return nil, headerState{}, decodeErr
			}
			if bool(record.envelope.flags&1 != 0) != payload.mustSync {
				return nil, headerState{}, fmt.Errorf("%w: source Ready sync flag",
					ErrGenerationSource)
			}
			if payload.hard != nil && len(payload.hard.ProtoReflect().GetUnknown()) != 0 {
				return nil, headerState{}, fmt.Errorf("%w: source HardState unknown fields",
					ErrGenerationSource)
			}
			if validateErr := validateGenerationReadyEntries(
				payload.entries, builder.options,
			); validateErr != nil {
				return nil, headerState{}, fmt.Errorf("%w: source Ready %d: %w",
					ErrGenerationSource, sequence, validateErr)
			}
			nextLast := sourceLast
			nextLastTerm := sourceLastTerm
			if len(payload.entries) != 0 {
				start := payload.entries[0].GetIndex()
				if start < sourceFirst || start > sourceLast+1 {
					return nil, headerState{}, fmt.Errorf(
						"%w: source entries start %d outside [%d,%d]",
						ErrGenerationSource, start, sourceFirst, sourceLast+1,
					)
				}
				nextLast = payload.entries[len(payload.entries)-1].GetIndex()
				nextLastTerm = payload.entries[len(payload.entries)-1].GetTerm()
				if sourceHard.GetCommit() > nextLast {
					return nil, headerState{}, fmt.Errorf(
						"%w: source Ready truncates committed index %d to %d",
						ErrGenerationSource, sourceHard.GetCommit(), nextLast,
					)
				}
				if projectErr := scratch.project(
					payload.entries, sourceHard.GetCommit(),
				); projectErr != nil {
					return nil, headerState{}, fmt.Errorf("%w: source Ready %d: %w",
						ErrGenerationSource, sequence, projectErr)
				}
			}
			nextHard := sourceHard
			if !isEmptyHardState(payload.hard) {
				candidate := payload.hard
				if candidate.GetTerm() == math.MaxUint64 ||
					candidate.GetTerm() < sourceHard.GetTerm() ||
					candidate.GetCommit() < sourceHard.GetCommit() ||
					(candidate.GetTerm() == sourceHard.GetTerm() &&
						sourceHard.GetVote() != 0 && candidate.GetVote() != sourceHard.GetVote()) ||
					candidate.GetCommit() < sourceFirst-1 || candidate.GetCommit() > nextLast ||
					(candidate.GetTerm() == 0 && candidate.GetVote() != 0) ||
					(candidate.GetVote() != 0 && raft.IsLocalMsgTarget(candidate.GetVote())) {
					return nil, headerState{}, fmt.Errorf(
						"%w: source HardState regression", ErrGenerationSource,
					)
				}
				nextHard = candidate
			}
			if nextHard.GetTerm() < nextLastTerm || nextHard.GetCommit() > nextLast {
				return nil, headerState{}, fmt.Errorf(
					"%w: source HardState does not cover durable log", ErrGenerationSource,
				)
			}
			sourceHard = nextHard
			sourceLast = nextLast
			sourceLastTerm = nextLastTerm
			lastReady = retryKey{
				incarnation: record.envelope.incarnation, readyID: record.envelope.readyID,
			}
			lastReadyDigest = sha256.Sum256(record.payload)

		default:
			return nil, headerState{}, fmt.Errorf("%w: invalid source record order",
				ErrGenerationSource)
		}
		previousDigest = record.digest
		offset = end
	}
	if scratch == nil || sourceHard == nil ||
		(builder.parentBinding != ([sha256.Size]byte{})) != sourceGenerationSeen ||
		offset != builder.current.walEnd ||
		previousDigest != builder.current.chainDigest ||
		sourceFirst != builder.current.first || sourceLast != builder.current.last ||
		!proto.Equal(sourceHard, builder.current.hard) ||
		!scratch.baseKnown || scratch.baseTerm != checkpointTerm ||
		scratch.last != builder.current.last ||
		header.topologyRecoveryEpoch != builder.current.topologyRecoveryEpoch {
		return nil, headerState{}, fmt.Errorf("%w: captured current does not bind source replay",
			ErrGenerationSource)
	}
	if builder.current.retryPresent {
		if lastReady != builder.current.retry || lastReadyDigest != builder.current.retryDigest {
			return nil, headerState{}, fmt.Errorf("%w: source retry binding",
				ErrGenerationSource)
		}
	} else if lastReady != (retryKey{}) &&
		builder.current.currentIncarnation <= lastReady.incarnation {
		return nil, headerState{}, fmt.Errorf("%w: source incarnation fence",
			ErrGenerationSource)
	}
	scratch.hard = sourceHard
	scratch.sourceFirst = sourceFirst
	scratch.sourceLast = sourceLast
	return scratch, header, nil
}

func (builder *GenerationBuilder) finishGenerationScratch(
	stage *Store,
	scratch *generationScratch,
	sourceHeader headerState,
) error {
	if err := scratch.flushPending(); err != nil {
		return err
	}
	baseIndex := builder.input.Snapshot.GetMetadata().GetIndex()
	baseTerm := builder.input.Snapshot.GetMetadata().GetTerm()
	term, err := scratch.term(baseIndex)
	if err != nil || term != baseTerm {
		return ErrGenerationSource
	}
	bootstrapPayload, _, err := marshalBootstrap(Bootstrap{
		TopologyRecoveryEpoch: sourceHeader.topologyRecoveryEpoch,
		Snapshot:              builder.input.Snapshot,
	}, sourceHeader.identity.MemberID)
	if err != nil {
		return err
	}
	wantRetainedBytes, err := scratch.footprintFrom(baseIndex + 1)
	if err != nil {
		return err
	}

	offset := scratch.initialEnd
	sequence := stage.current.recordSequence
	previous := stage.current.chainDigest
	retainedHash := newRetainedSuffixHash(baseIndex, baseTerm)
	var retainedCount uint64
	var retainedBytes uint64
	for _, chunk := range scratch.chunks {
		if chunk.first <= baseIndex || chunk.offset != offset ||
			chunk.sequence != sequence+1 || chunk.previous != previous {
			return fmt.Errorf("%w: generation scratch chain", ErrCorrupt)
		}
		entries, err := scratch.readChunk(chunk)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			footprint := uint64(32 + len(entry.GetData()))
			if retainedCount == math.MaxUint64 ||
				retainedBytes > math.MaxUint64-footprint {
				return ErrBounds
			}
			retainedHash.add(entry)
			retainedCount++
			retainedBytes += footprint
		}
		end, ok := addInt64(offset, chunk.total)
		if !ok || end > stage.options.maxFileBytes {
			return ErrFull
		}
		offset = end
		sequence = chunk.sequence
		previous = chunk.digest
	}
	if retainedCount != scratch.last-baseIndex ||
		retainedBytes != uint64(wantRetainedBytes) ||
		offset != scratch.writeOffset || sequence != scratch.sequence ||
		previous != scratch.previous {
		return fmt.Errorf("%w: retained suffix geometry", ErrCorrupt)
	}
	seal := generationSeal{
		familyID: builder.familyID, generation: builder.generation,
		parentBindingDigest: builder.parentBinding,
		identityDigest:      generationIdentityDigest(sourceHeader.identity),
		sourceFileID:        sourceHeader.fileID, sourceHeaderDigest: sourceHeader.headerDigest,
		sourceCurrentGeneration:  builder.current.generation,
		sourceWALEnd:             uint64(builder.current.walEnd),
		sourceRecordSequence:     builder.current.recordSequence,
		sourceChainDigest:        builder.current.chainDigest,
		sourceCurrentIncarnation: builder.current.currentIncarnation,
		topologyRecoveryEpoch:    sourceHeader.topologyRecoveryEpoch,
		baseIndex:                baseIndex, baseTerm: baseTerm,
		baseDigest:          sha256.Sum256(bootstrapPayload),
		confDigest:          generationConfDigest(builder.input.Snapshot.GetMetadata().GetConfState()),
		retentionCommitment: builder.input.RetentionCommitment,
		hard:                cloneHardState(scratch.hard), suffixFirst: baseIndex + 1,
		suffixLast: scratch.last, suffixCount: retainedCount,
		suffixBytes: retainedBytes, suffixDigest: retainedHash.finish(),
		sourceFirst: scratch.sourceFirst, sourceLast: scratch.sourceLast,
	}
	seal.bindingDigest = generationBindingDigest(seal)
	if err := validateGenerationSealStatic(seal); err != nil {
		return err
	}
	sealPayload, err := marshalGenerationSeal(seal)
	if err != nil {
		return err
	}
	sequence++
	sealRecord, sealDigest, _, err := marshalRecord(
		recordKindGenerationSeal, 0, sequence, builder.current.currentIncarnation,
		0, previous, sealPayload, stage.header, stage.options,
	)
	if err != nil {
		return err
	}
	end, ok := addInt64(offset, len(sealRecord))
	if !ok || end > stage.options.maxFileBytes || sequence > stage.options.maxRecords {
		return ErrFull
	}
	if err := writeExactAt(stage.options.ops, stage.file, sealRecord, offset); err != nil {
		return persistenceError("write WAL generation seal", false, err)
	}
	if err := stage.options.ops.sync(stage.file); err != nil {
		return persistenceError("sync WAL generation records", false, err)
	}
	stage.syncCount++
	next := stage.current
	next.activeSlot = 1
	next.generation = 2
	next.walEnd = end
	next.recordSequence = sequence
	next.chainDigest = sealDigest
	next.currentIncarnation = builder.current.currentIncarnation
	next.hard = cloneHardState(scratch.hard)
	next.first = baseIndex + 1
	next.last = scratch.last
	next.retryPresent = false
	next.retry = retryKey{}
	next.retryDigest = [sha256.Size]byte{}
	currentBytes, _, err := marshalCurrentSlot(next, next.activeSlot, stage.header)
	if err != nil {
		return err
	}
	if err := writeExactAt(
		stage.options.ops, stage.file, currentBytes,
		int64(StaticHeaderBytes+next.activeSlot*CurrentSlotBytes),
	); err != nil {
		return persistenceError("write WAL generation current slot", true, err)
	}
	if err := stage.options.ops.sync(stage.file); err != nil {
		return persistenceError("sync WAL generation current slot", true, err)
	}
	stage.syncCount++
	stage.current = next
	stage.generation = generationRecovery{present: true, seal: seal}
	builder.seal = seal
	builder.loaded = true
	return nil
}
