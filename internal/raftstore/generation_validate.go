package raftstore

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/thesyncim/vibedb/internal/storeio"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// readGenerationCandidate streams one immutable freshly-built candidate. It
// intentionally rejects later Ready records: the deterministic candidate name
// is idempotent only for the exact offline image, not an independently advanced
// descendant. Peak heap is one authenticated retained record.
func (builder *GenerationBuilder) readGenerationCandidate() (
	generationSeal,
	*pb.Snapshot,
	error,
) {
	_, parentPath, base, root, directoryInfo, err := openNamespace(builder.candidatePath)
	if err != nil {
		return generationSeal{}, nil, err
	}
	defer root.Close()
	if parentPath != builder.parentPath || builder.directoryInfo == nil ||
		!os.SameFile(builder.directoryInfo, directoryInfo) || base != builder.candidateBase {
		return generationSeal{}, nil, ErrNamespaceChanged
	}
	entryInfo, err := root.Lstat(base)
	if err != nil || !entryInfo.Mode().IsRegular() {
		return generationSeal{}, nil, errors.Join(ErrGenerationCandidate, err)
	}
	file, err := root.OpenFile(base, os.O_RDWR, 0)
	if err != nil {
		return generationSeal{}, nil, err
	}
	locked := false
	defer func() {
		if locked {
			_ = storeio.UnlockWriter(file)
		}
		_ = file.Close()
	}()
	if err := storeio.LockWriter(file); err != nil {
		return generationSeal{}, nil, errors.Join(ErrLocked, err)
	}
	locked = true
	if err := proveNamedFile(
		root, parentPath, directoryInfo, base, file, builder.options.maxFileBytes,
	); err != nil {
		return generationSeal{}, nil, err
	}
	staticBytes := make([]byte, StaticHeaderBytes)
	if _, err := file.ReadAt(staticBytes, 0); err != nil {
		return generationSeal{}, nil, fmt.Errorf("%w: read candidate static header: %v",
			ErrCorrupt, err)
	}
	header, _, err := unmarshalStaticHeader(
		staticBytes, builder.header.identity, builder.key, builder.options,
	)
	if err != nil {
		return generationSeal{}, nil, err
	}
	current, recoveredTorn, err := recoverCurrent(file, header, builder.options)
	if err != nil {
		return generationSeal{}, nil, err
	}
	if recoveredTorn {
		return generationSeal{}, nil, ErrGenerationCandidate
	}
	seal, err := streamFreshGenerationRecords(file, &header, current, builder.options)
	if err != nil {
		return generationSeal{}, nil, err
	}
	if header.topologyRecoveryEpoch != builder.header.topologyRecoveryEpoch {
		return generationSeal{}, nil, ErrIdentityMismatch
	}
	if err := proveNamedFile(
		root, parentPath, directoryInfo, base, file, builder.options.maxFileBytes,
	); err != nil {
		return generationSeal{}, nil, err
	}
	return seal, cloneSnapshot(header.snapshot), nil
}

func streamFreshGenerationRecords(
	file *os.File,
	header *headerState,
	current currentState,
	options normalizedOptions,
) (generationSeal, error) {
	if file == nil || header == nil {
		return generationSeal{}, ErrCorrupt
	}
	offset := int64(HeaderBytes)
	previousDigest := header.headerDigest
	retainedHash := newRetainedSuffixHash(header.reference.index, header.reference.term)
	retainedLast := header.reference.index
	retainedTerm := header.reference.term
	var retainedIncarnation uint64
	var retainedCount uint64
	var retainedBytes uint64
	var recovered generationSeal
	for sequence := uint64(1); sequence <= current.recordSequence; sequence++ {
		prefix := make([]byte, recordPrefixBytes)
		if _, err := file.ReadAt(prefix, offset); err != nil {
			return generationSeal{}, fmt.Errorf("%w: read candidate record %d prefix: %v",
				ErrCorrupt, sequence, err)
		}
		envelope, err := inspectRecordPrefix(prefix, *header, options)
		if err != nil {
			return generationSeal{}, fmt.Errorf("candidate record %d: %w", sequence, err)
		}
		end, ok := addInt64(offset, envelope.total)
		if !ok || end > current.walEnd {
			return generationSeal{}, fmt.Errorf("%w: candidate record %d exceeds current",
				ErrCorrupt, sequence)
		}
		data := make([]byte, envelope.total)
		n, readErr := file.ReadAt(data, offset)
		if readErr != nil && !errors.Is(readErr, io.EOF) || n != len(data) {
			return generationSeal{}, fmt.Errorf("%w: read candidate record %d: %v",
				ErrCorrupt, sequence, readErr)
		}
		record, err := unmarshalRecord(data, *header, options)
		if err != nil {
			return generationSeal{}, fmt.Errorf("candidate record %d: %w", sequence, err)
		}
		if record.envelope.sequence != sequence || record.envelope.previous != previousDigest {
			return generationSeal{}, fmt.Errorf("%w: candidate record %d chain gap",
				ErrCorrupt, sequence)
		}
		switch {
		case sequence == 1:
			if record.envelope.kind != recordKindBootstrap ||
				uint64(len(record.payload)) != header.reference.size ||
				sha256.Sum256(record.payload) != header.reference.digest {
				return generationSeal{}, fmt.Errorf("%w: candidate bootstrap reference", ErrCorrupt)
			}
			bootstrap, snapshotBytes, decodeErr := unmarshalBootstrap(
				record.payload, header.identity.MemberID,
			)
			if decodeErr != nil {
				return generationSeal{}, decodeErr
			}
			if bootstrap.Snapshot.GetMetadata().GetIndex() != header.reference.index ||
				bootstrap.Snapshot.GetMetadata().GetTerm() != header.reference.term {
				return generationSeal{}, fmt.Errorf("%w: candidate bootstrap metadata", ErrCorrupt)
			}
			header.topologyRecoveryEpoch = bootstrap.TopologyRecoveryEpoch
			header.snapshot = bootstrap.Snapshot
			header.snapshotBytes = snapshotBytes

		case record.envelope.kind == recordKindRetainedEntries && recovered.hard == nil:
			if retainedIncarnation == 0 {
				retainedIncarnation = record.envelope.incarnation
			} else if retainedIncarnation != record.envelope.incarnation {
				return generationSeal{}, fmt.Errorf("%w: candidate retained incarnation", ErrCorrupt)
			}
			entries, decodeErr := unmarshalRetainedEntriesView(record.payload, options)
			if decodeErr != nil {
				return generationSeal{}, decodeErr
			}
			for _, entry := range entries {
				footprint := uint64(32 + len(entry.GetData()))
				if entry.GetIndex() != retainedLast+1 || entry.GetTerm() < retainedTerm ||
					retainedCount == math.MaxUint64 ||
					retainedCount >= options.maxEntries ||
					footprint > uint64(options.maxLiveBytes) ||
					retainedBytes > math.MaxUint64-footprint ||
					retainedBytes > uint64(options.maxLiveBytes)-footprint {
					return generationSeal{}, fmt.Errorf("%w: candidate retained sequence", ErrCorrupt)
				}
				retainedLast = entry.GetIndex()
				retainedTerm = entry.GetTerm()
				retainedCount++
				retainedBytes += footprint
				retainedHash.add(entry)
			}

		case record.envelope.kind == recordKindGenerationSeal && recovered.hard == nil:
			if sequence != current.recordSequence {
				return generationSeal{}, fmt.Errorf("%w: candidate seal is not terminal", ErrCorrupt)
			}
			seal, decodeErr := unmarshalGenerationSeal(record.payload)
			if decodeErr != nil {
				return generationSeal{}, decodeErr
			}
			if record.envelope.incarnation != seal.sourceCurrentIncarnation ||
				(retainedIncarnation != 0 && retainedIncarnation != seal.sourceCurrentIncarnation) ||
				seal.identityDigest != generationIdentityDigest(header.identity) ||
				seal.topologyRecoveryEpoch != header.topologyRecoveryEpoch ||
				seal.baseIndex != header.reference.index || seal.baseTerm != header.reference.term ||
				seal.baseDigest != header.reference.digest ||
				seal.confDigest != generationConfDigest(header.snapshot.GetMetadata().GetConfState()) ||
				seal.suffixFirst != header.reference.index+1 || seal.suffixLast != retainedLast ||
				seal.suffixCount != retainedCount || seal.suffixBytes != retainedBytes ||
				seal.suffixDigest != retainedHash.finish() ||
				seal.hard.GetTerm() < retainedTerm || seal.hard.GetCommit() < seal.baseIndex ||
				seal.hard.GetCommit() > retainedLast {
				return generationSeal{}, fmt.Errorf("%w: candidate generation seal binding", ErrCorrupt)
			}
			recovered = seal

		default:
			return generationSeal{}, fmt.Errorf("%w: candidate is not a fresh generation", ErrCorrupt)
		}
		previousDigest = record.digest
		offset = end
	}
	if recovered.hard == nil || offset != current.walEnd ||
		previousDigest != current.chainDigest || header.snapshot == nil ||
		current.first != recovered.suffixFirst || current.last != recovered.suffixLast ||
		current.currentIncarnation != recovered.sourceCurrentIncarnation ||
		current.retryPresent || current.retry != (retryKey{}) ||
		current.retryDigest != ([sha256.Size]byte{}) ||
		!proto.Equal(current.hard, recovered.hard) ||
		current.snapshotID != header.reference.id ||
		current.snapshotIndex != header.reference.index ||
		current.snapshotTerm != header.reference.term ||
		current.snapshotSize != header.reference.size ||
		current.snapshotDigest != header.reference.digest ||
		current.topologyRecoveryEpoch != header.topologyRecoveryEpoch {
		return generationSeal{}, fmt.Errorf("%w: candidate current does not bind generation", ErrCorrupt)
	}
	return recovered, nil
}

func (builder *GenerationBuilder) candidateSealMatches(
	seal generationSeal,
	snapshot *pb.Snapshot,
) bool {
	if builder == nil || seal.hard == nil || snapshot == nil ||
		!proto.Equal(snapshot, builder.input.Snapshot) {
		return false
	}
	bootstrapPayload, _, err := marshalBootstrap(Bootstrap{
		TopologyRecoveryEpoch: builder.header.topologyRecoveryEpoch,
		Snapshot:              builder.input.Snapshot,
	}, builder.header.identity.MemberID)
	if err != nil {
		return false
	}
	baseIndex := builder.input.Snapshot.GetMetadata().GetIndex()
	baseTerm := builder.input.Snapshot.GetMetadata().GetTerm()
	return seal.familyID == builder.familyID && seal.generation == FirstWALGeneration &&
		seal.identityDigest == generationIdentityDigest(builder.header.identity) &&
		seal.sourceFileID == builder.header.fileID &&
		seal.sourceHeaderDigest == builder.header.headerDigest &&
		seal.sourceCurrentGeneration == builder.current.generation &&
		seal.sourceWALEnd == uint64(builder.current.walEnd) &&
		seal.sourceRecordSequence == builder.current.recordSequence &&
		seal.sourceChainDigest == builder.current.chainDigest &&
		seal.sourceCurrentIncarnation == builder.current.currentIncarnation &&
		seal.topologyRecoveryEpoch == builder.header.topologyRecoveryEpoch &&
		seal.baseIndex == baseIndex && seal.baseTerm == baseTerm &&
		seal.baseDigest == sha256.Sum256(bootstrapPayload) &&
		seal.confDigest == generationConfDigest(
			builder.input.Snapshot.GetMetadata().GetConfState(),
		) && seal.retentionCommitment == builder.input.RetentionCommitment &&
		proto.Equal(seal.hard, builder.current.hard) &&
		seal.suffixFirst == baseIndex+1 && seal.suffixLast == builder.current.last &&
		seal.suffixCount == builder.current.last-baseIndex &&
		seal.sourceFirst == builder.current.first && seal.sourceLast == builder.current.last
}
