package raftstore

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func recoverCurrent(file *os.File, header headerState, options normalizedOptions) (currentState, bool, error) {
	var decoded [CurrentSlotCount]slotDecode
	var decodeErrors [CurrentSlotCount]error
	for slot := 0; slot < CurrentSlotCount; slot++ {
		data := make([]byte, CurrentSlotBytes)
		if _, err := file.ReadAt(data, int64(StaticHeaderBytes+slot*CurrentSlotBytes)); err != nil {
			return currentState{}, false, fmt.Errorf("%w: read current slot %d: %v", ErrCorrupt, slot, err)
		}
		decoded[slot], decodeErrors[slot] = unmarshalCurrentSlot(data, slot, header)
	}
	valid := make([]int, 0, CurrentSlotCount)
	for slot := 0; slot < CurrentSlotCount; slot++ {
		if decodeErrors[slot] == nil && !decoded[slot].absent && !decoded[slot].torn {
			valid = append(valid, slot)
		}
	}
	if len(valid) == 0 {
		return currentState{}, false, errors.Join(ErrCorrupt, decodeErrors[0], decodeErrors[1], errors.New("no authenticated current slot"))
	}
	if len(valid) == 2 {
		left, right := decoded[valid[0]].state, decoded[valid[1]].state
		var selected currentState
		if left.generation > right.generation {
			selected = left
		} else {
			selected = right
		}
		lower := left.generation
		if right.generation < lower {
			lower = right.generation
		}
		if selected.generation != lower+1 {
			return currentState{}, false, fmt.Errorf("%w: current generations %d and %d are not adjacent", ErrCorrupt, left.generation, right.generation)
		}
		if err := validateSelectedCurrent(selected, header, options); err != nil {
			return currentState{}, false, err
		}
		return selected, false, nil
	}
	selectedSlot := valid[0]
	selected := decoded[selectedSlot].state
	other := 1 - selectedSlot
	recoveredTorn := false
	switch {
	case decodeErrors[other] != nil:
		return currentState{}, false, errors.Join(ErrCorrupt, fmt.Errorf("inactive current slot %d: %w", other, decodeErrors[other]))
	case decoded[other].torn:
		// A current-slot write can tear at any byte boundary over the older
		// contents of the inactive slot. The remaining authenticated slot is
		// therefore authoritative. This deliberately cannot distinguish that
		// local crash from corruption of a once-newer slot; the format has no
		// external anti-rollback witness.
		recoveredTorn = true
	case decoded[other].absent && selected.generation == 1 && selected.activeSlot == 0:
		// Canonical newly created image.
	case decoded[other].absent:
		// A torn sector/page update may read back as all zeroes. The other
		// authenticated slot remains the only selected state.
		recoveredTorn = true
	default:
		return currentState{}, false, fmt.Errorf("%w: missing inactive current slot after generation %d", ErrCorrupt, selected.generation)
	}
	if err := validateSelectedCurrent(selected, header, options); err != nil {
		return currentState{}, false, err
	}
	return selected, recoveredTorn, nil
}

func validateSelectedCurrent(current currentState, header headerState, options normalizedOptions) error {
	if current.generation == 0 || current.activeSlot < 0 || current.activeSlot >= CurrentSlotCount ||
		current.generation%CurrentSlotCount != uint64(current.activeSlot+1)%CurrentSlotCount ||
		current.walEnd < HeaderBytes+recordDamageGranule || current.walEnd > options.maxFileBytes || current.walEnd%recordDamageGranule != 0 ||
		current.recordSequence == 0 || current.recordSequence > options.maxRecords || current.hard == nil ||
		current.first != header.reference.index+1 || current.last < header.reference.index ||
		current.snapshotID != header.reference.id || current.snapshotIndex != header.reference.index ||
		current.snapshotTerm != header.reference.term || current.snapshotSize != header.reference.size || current.snapshotChunks != 1 ||
		current.snapshotDigest != header.reference.digest || current.hard.GetCommit() < header.reference.index || current.hard.GetCommit() > current.last {
		return fmt.Errorf("%w: selected current slot has impossible state", ErrCorrupt)
	}
	if current.retryPresent {
		if current.retry.incarnation == 0 || current.retry.readyID == 0 || current.currentIncarnation != current.retry.incarnation {
			return fmt.Errorf("%w: current retry identity", ErrCorrupt)
		}
	} else if current.retry != (retryKey{}) || current.retryDigest != ([32]byte{}) {
		return fmt.Errorf("%w: absent current retry has data", ErrCorrupt)
	}
	return nil
}

type generationRecovery struct {
	present bool
	seal    generationSeal
}

func recoverRecords(file *os.File, header *headerState, current currentState, options normalizedOptions) (logImage, generationRecovery, error) {
	if header == nil {
		return logImage{}, generationRecovery{}, fmt.Errorf("%w: nil header", ErrCorrupt)
	}
	offset := int64(HeaderBytes)
	previousDigest := header.headerDigest
	var image logImage
	var lastReady retryKey
	var lastReadyDigest [32]byte
	var generation generationRecovery
	retainedHash := newRetainedSuffixHash(header.reference.index, header.reference.term)
	var retainedIncarnation uint64
	var retainedCount uint64
	var retainedBytes uint64
	for sequence := uint64(1); sequence <= current.recordSequence; sequence++ {
		prefix := make([]byte, recordPrefixBytes)
		if _, err := file.ReadAt(prefix, offset); err != nil {
			return logImage{}, generationRecovery{}, fmt.Errorf("%w: read record %d prefix: %v", ErrCorrupt, sequence, err)
		}
		envelope, err := inspectRecordPrefix(prefix, *header, options)
		if err != nil {
			return logImage{}, generationRecovery{}, fmt.Errorf("record %d: %w", sequence, err)
		}
		end, ok := addInt64(offset, envelope.total)
		if !ok || end > current.walEnd {
			return logImage{}, generationRecovery{}, fmt.Errorf("%w: record %d exceeds selected WAL end", ErrCorrupt, sequence)
		}
		data := make([]byte, envelope.total)
		if _, err := file.ReadAt(data, offset); err != nil && !errors.Is(err, io.EOF) {
			return logImage{}, generationRecovery{}, fmt.Errorf("%w: read record %d: %v", ErrCorrupt, sequence, err)
		}
		record, err := unmarshalRecord(data, *header, options)
		if err != nil {
			return logImage{}, generationRecovery{}, fmt.Errorf("record %d: %w", sequence, err)
		}
		if record.envelope.sequence != sequence || record.envelope.previous != previousDigest {
			return logImage{}, generationRecovery{}, fmt.Errorf("%w: record %d chain or sequence gap", ErrCorrupt, sequence)
		}
		switch {
		case sequence == 1:
			if record.envelope.kind != recordKindBootstrap || uint64(len(record.payload)) != header.reference.size || sha256.Sum256(record.payload) != header.reference.digest {
				return logImage{}, generationRecovery{}, fmt.Errorf("%w: bootstrap record reference", ErrCorrupt)
			}
			bootstrap, snapshotBytes, decodeErr := unmarshalBootstrap(record.payload, header.identity.MemberID)
			if decodeErr != nil {
				return logImage{}, generationRecovery{}, decodeErr
			}
			if bootstrap.Snapshot.GetMetadata().GetIndex() != header.reference.index || bootstrap.Snapshot.GetMetadata().GetTerm() != header.reference.term {
				return logImage{}, generationRecovery{}, fmt.Errorf("%w: bootstrap metadata differs from header", ErrCorrupt)
			}
			header.topologyRecoveryEpoch = bootstrap.TopologyRecoveryEpoch
			header.snapshot = bootstrap.Snapshot
			header.snapshotBytes = snapshotBytes
			image = bootstrapImage(bootstrap.Snapshot)

		case record.envelope.kind == recordKindRetainedEntries:
			if generation.present || lastReady != (retryKey{}) {
				return logImage{}, generationRecovery{}, fmt.Errorf("%w: retained entries after generation seal or Ready", ErrCorrupt)
			}
			if retainedIncarnation == 0 {
				retainedIncarnation = record.envelope.incarnation
			} else if retainedIncarnation != record.envelope.incarnation {
				return logImage{}, generationRecovery{}, fmt.Errorf("%w: retained-entry incarnation changed", ErrCorrupt)
			}
			entries, decodeErr := unmarshalRetainedEntries(record.payload, options)
			if decodeErr != nil {
				return logImage{}, generationRecovery{}, decodeErr
			}
			// Retained records precede the seal that authenticates the source
			// HardState. Use only an entry-derived provisional term and the already
			// authenticated snapshot-base commit while validating/appending the
			// suffix. No incomplete generation is ever returned, and the seal below
			// must replace this provisional state with its bound HardState.
			provisional := &pb.HardState{
				Term:   uint64Pointer(entries[len(entries)-1].GetTerm()),
				Commit: uint64Pointer(header.reference.index),
			}
			payload := readyPayload{hard: provisional, entries: entries}
			if applyErr := applyReadyPayload(&image, payload, options); applyErr != nil {
				return logImage{}, generationRecovery{}, fmt.Errorf("%w: recover retained entries: %w", ErrCorrupt, applyErr)
			}
			for _, entry := range entries {
				if retainedCount == math.MaxUint64 || retainedBytes > math.MaxUint64-uint64(32+len(entry.GetData())) {
					return logImage{}, generationRecovery{}, fmt.Errorf("%w: retained suffix overflow", ErrCorrupt)
				}
				retainedHash.add(entry)
				retainedCount++
				retainedBytes += uint64(32 + len(entry.GetData()))
			}

		case record.envelope.kind == recordKindGenerationSeal:
			if generation.present || lastReady != (retryKey{}) {
				return logImage{}, generationRecovery{}, fmt.Errorf("%w: repeated or late generation seal", ErrCorrupt)
			}
			seal, decodeErr := unmarshalGenerationSeal(record.payload)
			if decodeErr != nil {
				return logImage{}, generationRecovery{}, decodeErr
			}
			if record.envelope.incarnation != seal.sourceCurrentIncarnation ||
				(retainedIncarnation != 0 && retainedIncarnation != seal.sourceCurrentIncarnation) {
				return logImage{}, generationRecovery{}, fmt.Errorf("%w: generation record incarnation", ErrCorrupt)
			}
			if err := validateRecoveredGenerationSeal(
				seal, *header, current, image, retainedCount, retainedBytes,
				retainedHash.finish(), options,
			); err != nil {
				return logImage{}, generationRecovery{}, err
			}
			delta, planErr := planReadyPayload(&image, readyPayload{hard: seal.hard}, options)
			if planErr != nil {
				return logImage{}, generationRecovery{}, fmt.Errorf("%w: generation HardState: %w", ErrCorrupt, planErr)
			}
			commitImageDelta(&image, delta)
			generation = generationRecovery{present: true, seal: seal}

		case record.envelope.kind == recordKindReady:
			if retainedIncarnation != 0 && !generation.present {
				return logImage{}, generationRecovery{}, fmt.Errorf("%w: Ready precedes generation seal", ErrCorrupt)
			}
			if current.currentIncarnation == 0 || record.envelope.incarnation > current.currentIncarnation ||
				(lastReady.incarnation != 0 && (record.envelope.incarnation < lastReady.incarnation ||
					(record.envelope.incarnation == lastReady.incarnation && record.envelope.readyID <= lastReady.readyID))) {
				return logImage{}, generationRecovery{}, fmt.Errorf("%w: Ready identity regression", ErrCorrupt)
			}
			if generation.present && record.envelope.incarnation <= generation.seal.sourceCurrentIncarnation {
				return logImage{}, generationRecovery{}, fmt.Errorf("%w: generation Ready reused source incarnation", ErrCorrupt)
			}
			payload, decodeErr := unmarshalReadyPayload(record.payload, options)
			if decodeErr != nil {
				return logImage{}, generationRecovery{}, decodeErr
			}
			if bool(record.envelope.flags&1 != 0) != payload.mustSync {
				return logImage{}, generationRecovery{}, fmt.Errorf("%w: Ready sync flag mismatch", ErrCorrupt)
			}
			if applyErr := applyReadyPayload(&image, payload, options); applyErr != nil {
				return logImage{}, generationRecovery{}, fmt.Errorf("%w: recover Ready %d: %v", ErrCorrupt, sequence, applyErr)
			}
			lastReady = retryKey{incarnation: record.envelope.incarnation, readyID: record.envelope.readyID}
			lastReadyDigest = sha256.Sum256(record.payload)

		default:
			return logImage{}, generationRecovery{}, fmt.Errorf("%w: invalid record order", ErrCorrupt)
		}
		previousDigest = record.digest
		offset = end
	}
	if retainedIncarnation != 0 && !generation.present {
		return logImage{}, generationRecovery{}, fmt.Errorf("%w: retained suffix lacks generation seal", ErrCorrupt)
	}
	if offset != current.walEnd || previousDigest != current.chainDigest || header.snapshot == nil ||
		image.first != current.first || image.last != current.last || !proto.Equal(image.hard, current.hard) ||
		header.topologyRecoveryEpoch != current.topologyRecoveryEpoch {
		return logImage{}, generationRecovery{}, fmt.Errorf("%w: selected current slot does not bind recovered image", ErrCorrupt)
	}
	if current.retryPresent {
		if lastReady != current.retry || lastReadyDigest != current.retryDigest {
			return logImage{}, generationRecovery{}, fmt.Errorf("%w: current retry does not bind final Ready", ErrCorrupt)
		}
	} else if lastReady != (retryKey{}) && current.currentIncarnation <= lastReady.incarnation {
		return logImage{}, generationRecovery{}, fmt.Errorf("%w: current incarnation does not fence recovered Ready", ErrCorrupt)
	}
	return image, generation, nil
}

func validateRecoveredGenerationSeal(
	seal generationSeal,
	header headerState,
	current currentState,
	image logImage,
	retainedCount uint64,
	retainedBytes uint64,
	retainedDigest [sha256.Size]byte,
	options normalizedOptions,
) error {
	if seal.identityDigest != generationIdentityDigest(header.identity) ||
		seal.topologyRecoveryEpoch != header.topologyRecoveryEpoch ||
		seal.baseIndex != header.reference.index || seal.baseTerm != header.reference.term ||
		seal.baseDigest != header.reference.digest ||
		seal.confDigest != generationConfDigest(header.snapshot.GetMetadata().GetConfState()) ||
		seal.sourceWALEnd > uint64(options.maxFileBytes) ||
		seal.sourceRecordSequence > options.maxRecords ||
		seal.sourceFirst > seal.suffixFirst || seal.sourceLast != seal.suffixLast ||
		seal.suffixFirst != image.first || seal.suffixLast != image.last ||
		seal.suffixCount != retainedCount || seal.suffixBytes != retainedBytes ||
		seal.suffixBytes != uint64(image.liveBytes) || seal.suffixDigest != retainedDigest ||
		current.currentIncarnation < seal.sourceCurrentIncarnation {
		return fmt.Errorf("%w: generation seal does not bind recovered image", ErrCorrupt)
	}
	return nil
}
