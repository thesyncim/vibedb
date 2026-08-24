package raftstore

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	// FirstWALGeneration is the deterministic first compacted sibling of a
	// legacy single-file WAL. This safe point deliberately does not publish a
	// family manifest or make the sibling authoritative.
	FirstWALGeneration uint64 = 1

	// DefaultGenerationRetainedChunkBytes bounds ordinary builder workspace and
	// write size. A single legal entry may be larger, but remains bounded by the
	// fixed 16 MiB proposal limit and the WAL's sealed record limit.
	DefaultGenerationRetainedChunkBytes = 4 << 20
)

var (
	ErrGenerationCandidate = errors.New("raftstore: invalid WAL generation candidate")
	ErrGenerationConflict  = errors.New("raftstore: WAL generation name is occupied by another image")
	ErrGenerationSource    = errors.New("raftstore: WAL generation source is not a canonical live cut")
	// ErrGenerationActivationPending is a forward-compatibility fence. This
	// candidate-only safe point never creates a family manifest, but it refuses
	// to serve any path once a newer binary has durably selected a generation.
	ErrGenerationActivationPending = errors.New("raftstore: WAL generation activation requires settlement")
	errGenerationContended         = errors.New("raftstore: WAL generation publication contended")
)

func linkGenerationName(root *os.Root, oldName, newName string) error {
	return root.Link(oldName, newName)
}

// GenerationInput is the compact SQL/Raft checkpoint bridge accepted by the
// offline builder. RetentionCommitment must come from a currently validated
// checkpoint retention witness. The builder seals it but cannot independently
// revalidate its SQL owner; activation must perform that validation again.
type GenerationInput struct {
	Snapshot            *pb.Snapshot
	RetentionCommitment [sha256.Size]byte
}

// GenerationInfo is detached fixed-width evidence recovered from one compacted
// generation. Path is cold control-plane metadata; every authority coordinate
// and digest remains binary and comparable.
type GenerationInfo struct {
	Path                     string
	FamilyID                 [16]byte
	Generation               uint64
	SourceFileID             [16]byte
	SourceCutDigest          [sha256.Size]byte
	SnapshotBaseDigest       [sha256.Size]byte
	RetentionCommitment      [sha256.Size]byte
	BaseIndex                uint64
	BaseTerm                 uint64
	LastIndex                uint64
	HardTerm                 uint64
	HardVote                 uint64
	HardCommit               uint64
	SourceCurrentIncarnation uint64
	RetainedEntries          uint64
	RetainedBytes            uint64
}

// GenerationCandidate is a fully synced and strictly reopened sibling. It is
// not serving or deletion authority: Open(path, ...) still selects the legacy
// path and no family manifest is created by this safe point.
type GenerationCandidate struct {
	Path string
	Info GenerationInfo
}

// GenerationBuilder owns an immutable duplicate descriptor for one selected
// source cut. PrepareGeneration holds the live Store lock only long enough to
// capture and fence that descriptor; WAL replay, suffix selection, encoding,
// preallocation, and candidate validation happen later through Build.
//
// A builder is single-threaded. Close is idempotent and releases an unconsumed
// source descriptor. Build consumes the descriptor automatically.
type GenerationBuilder struct {
	logicalPath   string
	parentPath    string
	base          string
	candidatePath string
	candidateBase string
	familyID      [16]byte

	directoryInfo os.FileInfo
	sourceInfo    os.FileInfo
	source        *os.File
	header        headerState
	current       currentState
	options       normalizedOptions
	key           Key
	input         GenerationInput
	link          func(*os.Root, string, string) error

	loaded bool
	closed bool
	seal   generationSeal
}

// PrepareGeneration captures an immutable source current-slot cut and duplicate
// read descriptor without replaying or copying the live log. The source must be
// a healthy, begun legacy WAL with no unsettled mutation. key must open that
// exact WAL; its wrapped metadata may be omitted and is filled from the source.
func (store *Store) PrepareGeneration(input GenerationInput, key Key) (*GenerationBuilder, error) {
	if store == nil {
		return nil, ErrClosed
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if err := store.checkLocked(); err != nil {
		return nil, err
	}
	if store.recoveredTornSlot || store.generation.present || !store.begun ||
		store.current.currentIncarnation == 0 {
		return nil, ErrGenerationSource
	}
	if input.RetentionCommitment == ([sha256.Size]byte{}) {
		return nil, fmt.Errorf("%w: zero retention commitment", ErrGenerationSource)
	}
	if err := validateSnapshotBase(input.Snapshot, store.header.identity.MemberID); err != nil {
		return nil, errors.Join(ErrGenerationSource, err)
	}
	baseIndex := input.Snapshot.GetMetadata().GetIndex()
	if baseIndex < store.header.reference.index || baseIndex > store.image.last ||
		baseIndex > store.current.hard.GetCommit() {
		return nil, fmt.Errorf("%w: checkpoint index %d outside durable committed range [%d,%d]",
			ErrGenerationSource, baseIndex, store.header.reference.index, store.current.hard.GetCommit())
	}
	baseTerm, err := termFromImage(store.image, baseIndex)
	if err != nil || baseTerm != input.Snapshot.GetMetadata().GetTerm() {
		return nil, fmt.Errorf("%w: checkpoint term mismatch", ErrGenerationSource)
	}
	if err := validateGenerationKey(key, store.header); err != nil {
		return nil, err
	}
	if key.Wrapped == nil {
		key.Wrapped = slices.Clone(store.header.wrapped)
	} else {
		key.Wrapped = slices.Clone(key.Wrapped)
	}
	key.ID = strings.Clone(key.ID)
	reader, err := store.root.OpenFile(store.base, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	if err := proveNamedFile(
		store.root, store.parentPath, store.directoryInfo, store.base, reader,
		store.options.maxFileBytes,
	); err != nil {
		_ = reader.Close()
		return nil, err
	}
	sourceInfo, err := reader.Stat()
	if err != nil {
		_ = reader.Close()
		return nil, err
	}
	header := cloneGenerationHeader(store.header)
	current := cloneGenerationCurrent(store.current)
	familyID := generationFamilyID(store.base, header.identity)
	candidateBase := generationCandidateBase(familyID, FirstWALGeneration)
	return &GenerationBuilder{
		logicalPath: store.path, parentPath: store.parentPath, base: store.base,
		candidatePath: filepath.Join(store.parentPath, candidateBase), candidateBase: candidateBase,
		familyID: familyID, directoryInfo: store.directoryInfo, sourceInfo: sourceInfo,
		source: reader, header: header, current: current, options: store.options,
		link: linkGenerationName,
		key:  key, input: GenerationInput{
			Snapshot: cloneSnapshot(input.Snapshot), RetentionCommitment: input.RetentionCommitment,
		},
	}, nil
}

// CandidatePath returns the deterministic sibling name without creating or
// publishing it.
func (builder *GenerationBuilder) CandidatePath() string {
	if builder == nil {
		return ""
	}
	return builder.candidatePath
}

// Build replays the captured source cut from its immutable descriptor, writes
// one offline compacted image, syncs it completely, publishes only the
// deterministic non-authoritative sibling name, and strictly reopens it.
// Existing exact candidates make this operation idempotent. A different or
// corrupt occupant is never overwritten.
func (builder *GenerationBuilder) Build() (
	candidate GenerationCandidate,
	resultErr error,
) {
	if builder == nil || builder.closed {
		return GenerationCandidate{}, ErrClosed
	}
	lease, err := builder.acquireBuildLease()
	if err != nil {
		return GenerationCandidate{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, lease.Close()) }()
	if err := builder.reclaimAbandonedStage(); err != nil {
		return GenerationCandidate{}, err
	}
	if candidate, found, err := builder.validateExistingCandidate(); found || err != nil {
		return candidate, err
	}
	stage, err := builder.createStage()
	if err != nil {
		return GenerationCandidate{}, err
	}
	stageOpen := true
	stageInfo := stage.fileInfo
	cleanupStage := func() error {
		var cleanupErr error
		if stageOpen {
			cleanupErr = stage.Close()
			stageOpen = false
		}
		cleanupErr = errors.Join(
			cleanupErr,
			builder.removeStage(builder.stageBase(), stageInfo),
		)
		return cleanupErr
	}
	defer func() {
		if cleanupErr := cleanupStage(); cleanupErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf(
				"reclaim WAL generation stage: %w", cleanupErr,
			))
		}
	}()
	scratch, sourceHeader, err := builder.replaySourceIntoGeneration(stage)
	if err != nil {
		return GenerationCandidate{}, err
	}
	if err := builder.finishGenerationScratch(stage, scratch, sourceHeader); err != nil {
		return GenerationCandidate{}, err
	}
	publicationErr := builder.publishStage(stage)
	if publicationErr != nil && !errors.Is(publicationErr, errGenerationContended) {
		return GenerationCandidate{}, publicationErr
	}
	if err := cleanupStage(); err != nil {
		return GenerationCandidate{}, err
	}
	if publicationErr != nil {
		candidate, found, validationErr := builder.validateExistingCandidate()
		if validationErr != nil || !found {
			return GenerationCandidate{}, errors.Join(
				ErrGenerationConflict, publicationErr, validationErr,
			)
		}
		return candidate, builder.releaseSource()
	}
	candidate, err = builder.ValidateCandidate()
	if err != nil {
		return GenerationCandidate{}, err
	}
	return candidate, builder.releaseSource()
}

func (builder *GenerationBuilder) releaseSource() error {
	if builder == nil || builder.source == nil {
		return nil
	}
	err := builder.source.Close()
	builder.source = nil
	return err
}

// ValidateCandidate strictly reopens the deterministic sibling and compares
// its complete recovered generation seal with the source cut captured by this
// builder. It performs no publication, activation, or deletion.
func (builder *GenerationBuilder) ValidateCandidate() (GenerationCandidate, error) {
	if builder == nil || builder.closed {
		return GenerationCandidate{}, ErrClosed
	}
	seal, snapshot, err := builder.readGenerationCandidate()
	if err != nil {
		return GenerationCandidate{}, errors.Join(ErrGenerationCandidate, err)
	}
	if builder.loaded && !equalGenerationSeal(seal, builder.seal) ||
		!builder.candidateSealMatches(seal, snapshot) {
		return GenerationCandidate{}, ErrGenerationCandidate
	}
	builder.seal = seal
	builder.loaded = true
	info := generationInfo(builder.candidatePath, seal)
	return GenerationCandidate{Path: builder.candidatePath, Info: info}, builder.releaseSource()
}

// Close releases an unconsumed source descriptor and clears retained builder
// workspace. It never removes a published candidate.
func (builder *GenerationBuilder) Close() error {
	if builder == nil || builder.closed {
		return nil
	}
	builder.closed = true
	var err error
	if builder.source != nil {
		err = builder.source.Close()
		builder.source = nil
	}
	clear(builder.key.Material[:])
	clear(builder.key.Wrapped)
	builder.key = Key{}
	clear(builder.header.dataKey[:])
	clear(builder.header.nonceKey[:])
	clear(builder.header.wrapped)
	builder.header = headerState{}
	builder.current = currentState{}
	builder.input = GenerationInput{}
	builder.seal = generationSeal{}
	return err
}

// GenerationInfo returns the intrinsic recovered seal for an open generation.
// A legacy WAL returns ErrGenerationCandidate.
func (store *Store) GenerationInfo() (GenerationInfo, error) {
	if store == nil {
		return GenerationInfo{}, ErrClosed
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if err := store.checkLocked(); err != nil {
		return GenerationInfo{}, err
	}
	if !store.generation.present {
		return GenerationInfo{}, ErrGenerationCandidate
	}
	return generationInfo(store.path, store.generation.seal), nil
}

func (builder *GenerationBuilder) validateExistingCandidate() (GenerationCandidate, bool, error) {
	_, err := os.Lstat(builder.candidatePath)
	switch {
	case err == nil:
		candidate, validationErr := builder.ValidateCandidate()
		if validationErr != nil {
			return GenerationCandidate{}, true, errors.Join(ErrGenerationConflict, validationErr)
		}
		// This also settles a prior publish whose directory sync outcome was
		// unknown. Build reports success only after the candidate name is durable
		// in the same pinned parent captured with the source cut.
		if syncErr := builder.syncParentDirectory(); syncErr != nil {
			return GenerationCandidate{}, true, persistenceError(
				"sync existing WAL generation candidate directory", true, syncErr,
			)
		}
		return candidate, true, nil
	case errors.Is(err, os.ErrNotExist):
		return GenerationCandidate{}, false, nil
	default:
		return GenerationCandidate{}, true, err
	}
}

func (builder *GenerationBuilder) publishStage(stage *Store) error {
	if stage == nil || stage.file == nil || stage.fileInfo == nil {
		return ErrClosed
	}
	_, parentPath, _, root, directoryInfo, err := openNamespace(builder.logicalPath)
	if err != nil {
		return err
	}
	defer root.Close()
	if parentPath != builder.parentPath || !os.SameFile(builder.directoryInfo, directoryInfo) {
		return ErrNamespaceChanged
	}
	liveSource, err := root.Lstat(builder.base)
	if err != nil || !liveSource.Mode().IsRegular() || !os.SameFile(liveSource, builder.sourceInfo) {
		return fmt.Errorf("%w: generation source leaf changed", ErrNamespaceChanged)
	}
	stageBase := builder.stageBase()
	if err := proveNamedFile(
		root, builder.parentPath, builder.directoryInfo, stageBase, stage.file,
		builder.options.maxFileBytes,
	); err != nil {
		return err
	}
	link := builder.link
	if link == nil {
		link = linkGenerationName
	}
	if err := link(root, stageBase, builder.candidateBase); err != nil {
		candidateInfo, statErr := root.Lstat(builder.candidateBase)
		stageInfo, fileErr := stage.file.Stat()
		switch {
		case statErr == nil && fileErr == nil && candidateInfo.Mode().IsRegular() &&
			os.SameFile(candidateInfo, stageInfo):
			if syncErr := syncPinnedDirectory(root); syncErr != nil {
				return persistenceError(
					"sync settled WAL generation candidate directory", true, syncErr,
				)
			}
			return nil
		case errors.Is(statErr, os.ErrNotExist) && fileErr == nil:
			return persistenceError("publish WAL generation candidate", false, err)
		case statErr == nil && fileErr == nil:
			return errors.Join(errGenerationContended, err)
		default:
			return persistenceError(
				"settle WAL generation candidate publication", true,
				errors.Join(err, statErr, fileErr),
			)
		}
	}
	if err := proveNamedFile(
		root, builder.parentPath, builder.directoryInfo, builder.candidateBase, stage.file,
		builder.options.maxFileBytes,
	); err != nil {
		return persistenceError("prove WAL generation candidate", true, err)
	}
	if err := syncPinnedDirectory(root); err != nil {
		return persistenceError("sync WAL generation candidate directory", true, err)
	}
	return nil
}

func (builder *GenerationBuilder) syncParentDirectory() error {
	root, err := os.OpenRoot(builder.parentPath)
	if err != nil {
		return err
	}
	defer root.Close()
	info, err := root.Stat(".")
	if err != nil {
		return err
	}
	if builder.directoryInfo == nil || !info.IsDir() ||
		!os.SameFile(builder.directoryInfo, info) {
		return ErrNamespaceChanged
	}
	return syncPinnedDirectory(root)
}

func (builder *GenerationBuilder) removeStage(base string, expected os.FileInfo) error {
	if builder == nil || base == "" || expected == nil {
		return ErrNamespaceChanged
	}
	root, err := os.OpenRoot(builder.parentPath)
	if err != nil {
		return err
	}
	defer root.Close()
	directory, err := root.Stat(".")
	if err != nil {
		return err
	}
	if builder.directoryInfo == nil || !directory.IsDir() ||
		!os.SameFile(builder.directoryInfo, directory) {
		return ErrNamespaceChanged
	}
	entry, err := root.Lstat(base)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !entry.Mode().IsRegular() || !os.SameFile(entry, expected) {
		return ErrNamespaceChanged
	}
	if err := root.Remove(base); err != nil {
		return err
	}
	return syncPinnedDirectory(root)
}

func generationRecordBytes(plainBytes, keyBytes int) int {
	return alignRecordLength(recordPrefixBytes + keyBytes + plainBytes + 16 + recordChecksumBytes)
}

func generationInfo(path string, seal generationSeal) GenerationInfo {
	return GenerationInfo{
		Path: path, FamilyID: seal.familyID, Generation: seal.generation,
		SourceFileID: seal.sourceFileID, SourceCutDigest: seal.sourceChainDigest,
		SnapshotBaseDigest: seal.baseDigest, RetentionCommitment: seal.retentionCommitment,
		BaseIndex: seal.baseIndex, BaseTerm: seal.baseTerm, LastIndex: seal.suffixLast,
		HardTerm: seal.hard.GetTerm(), HardVote: seal.hard.GetVote(), HardCommit: seal.hard.GetCommit(),
		SourceCurrentIncarnation: seal.sourceCurrentIncarnation,
		RetainedEntries:          seal.suffixCount, RetainedBytes: seal.suffixBytes,
	}
}

func equalGenerationSeal(left, right generationSeal) bool {
	leftHard, rightHard := left.hard, right.hard
	left.hard, right.hard = nil, nil
	return left == right && proto.Equal(leftHard, rightHard)
}

func termFromImage(image logImage, index uint64) (uint64, error) {
	base := image.first - 1
	switch {
	case index == base:
		return image.baseTerm, nil
	case index < base || index > image.last:
		return 0, ErrInvalid
	default:
		return image.entries[index-image.first].GetTerm(), nil
	}
}

func cloneGenerationHeader(header headerState) headerState {
	result := header
	result.identity.Distribution = string([]byte(header.identity.Distribution))
	result.identity.Shard = string([]byte(header.identity.Shard))
	result.keyID = string([]byte(header.keyID))
	result.wrapped = slices.Clone(header.wrapped)
	result.snapshot = cloneSnapshot(header.snapshot)
	result.snapshotBytes = slices.Clone(header.snapshotBytes)
	return result
}

func cloneGenerationCurrent(current currentState) currentState {
	result := current
	result.hard = cloneHardState(current.hard)
	return result
}

func validateGenerationKey(key Key, header headerState) error {
	if err := validateKey(key, false); err != nil {
		return err
	}
	if key.ID != header.keyID || (key.Wrapped != nil && !bytes.Equal(key.Wrapped, header.wrapped)) {
		return ErrKeyMismatch
	}
	crypto, err := makeFileCrypto(key, header.fileID)
	if err != nil {
		return err
	}
	if subtle.ConstantTimeCompare(crypto.dataKey[:], header.dataKey[:]) != 1 ||
		subtle.ConstantTimeCompare(crypto.nonceKey[:], header.nonceKey[:]) != 1 {
		return ErrKeyMismatch
	}
	return nil
}

func generationCandidateBase(familyID [16]byte, generation uint64) string {
	var encodedGeneration [8]byte
	binary.BigEndian.PutUint64(encodedGeneration[:], generation)
	return generationFamilyPrefix(familyID) +
		".g" + hex.EncodeToString(encodedGeneration[:]) + ".wal"
}

func generationFamilyPrefix(familyID [16]byte) string {
	return ".vibedb-raft-" + hex.EncodeToString(familyID[:])
}

func generationFamilyManifestBase(familyID [16]byte) string {
	return generationFamilyPrefix(familyID) + ".family"
}
