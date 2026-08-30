package raftstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	// FirstWALGeneration is the deterministic first compacted child of the
	// mandatory source family state.
	FirstWALGeneration uint64 = 1

	// DefaultGenerationRetainedChunkBytes bounds ordinary builder workspace and
	// write size. A single legal entry may be larger, but remains bounded by the
	// fixed 16 MiB proposal limit and the WAL's sealed record limit.
	DefaultGenerationRetainedChunkBytes = 4 << 20
)

var (
	ErrGenerationCandidate         = errors.New("raftstore: invalid WAL generation candidate")
	ErrGenerationConflict          = errors.New("raftstore: WAL generation name is occupied by another image")
	ErrGenerationSource            = errors.New("raftstore: WAL generation source is not a canonical live cut")
	ErrGenerationActivationPending = errors.New("raftstore: WAL generation activation requires settlement")
	ErrGenerationFamilyQuarantined = errors.New("raftstore: WAL generation family requires quarantine")
	errGenerationContended         = errors.New("raftstore: WAL generation publication contended")
	errGenerationCandidateStale    = errors.New("raftstore: stale unselected WAL generation candidate")
)

func linkGenerationName(root *os.Root, oldName, newName string) error {
	return root.Link(oldName, newName)
}

// GenerationInput is the compact SQL/Raft checkpoint bridge accepted by the
// offline builder. SnapshotBaseDigest is the state machine's authenticated
// identity of Snapshot; it is deliberately distinct from the WAL bootstrap
// record digest. RetentionCommitment must come from a currently validated
// checkpoint retention witness. The builder seals both opaque proofs but
// cannot independently revalidate their SQL owner; activation must perform
// that validation again.
type GenerationInput struct {
	Snapshot            *pb.Snapshot
	SnapshotBaseDigest  [sha256.Size]byte
	RetentionCommitment [sha256.Size]byte
}

// GenerationInfo is detached fixed-width evidence recovered from one compacted
// generation. Path is cold control-plane metadata; every authority coordinate
// and digest remains binary and comparable.
type GenerationInfo struct {
	Path                     string
	FamilyID                 [16]byte
	Generation               uint64
	ParentBindingDigest      [sha256.Size]byte
	FileID                   [16]byte
	HeaderDigest             [sha256.Size]byte
	BindingDigest            [sha256.Size]byte
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
	SourceReadyID            uint64
	RetainedEntries          uint64
	RetainedBytes            uint64
}

// GenerationCandidate is a fully synced and strictly reopened sibling. It is
// not serving or deletion authority until the family selection and settlement
// protocol publishes it through the logical WAL path.
type GenerationCandidate struct {
	Path string
	Info GenerationInfo
}

// GenerationBuilder owns immutable metadata for one selected source cut.
// PrepareGeneration holds the live Store lock only long enough to capture that
// cut. Build transiently opens and proves the still-named source inode, then
// closes it before returning; an abandoned builder therefore cannot pin a
// retired full-size WAL generation.
//
// A builder is single-threaded. Close is idempotent.
type GenerationBuilder struct {
	owner         *Store
	logicalPath   string
	parentPath    string
	base          string
	candidatePath string
	candidateBase string
	familyID      [16]byte
	generation    uint64
	parentBinding [sha256.Size]byte
	sourceReadyID uint64

	directoryInfo os.FileInfo
	sourceInfo    os.FileInfo
	source        *os.File
	header        headerState
	current       currentState
	options       normalizedOptions
	key           Key
	input         GenerationInput
	link          func(*os.Root, string, string) error

	loaded                bool
	closed                bool
	cancel                chan struct{}
	cancelOnce            sync.Once
	seal                  generationSeal
	candidateFileID       [16]byte
	candidateHeaderDigest [sha256.Size]byte
}

// PrepareGeneration captures an immutable source current-slot cut without
// replaying or copying the live log. The source must be a healthy, begun WAL
// with no unsettled mutation. key must open that
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
	if store.recoveredTornSlot || !store.begun ||
		store.current.currentIncarnation == 0 {
		return nil, ErrGenerationSource
	}
	if input.SnapshotBaseDigest == ([sha256.Size]byte{}) ||
		input.RetentionCommitment == ([sha256.Size]byte{}) {
		return nil, fmt.Errorf("%w: zero snapshot-base or retention commitment", ErrGenerationSource)
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
	if err := proveNamedFile(
		store.root, store.parentPath, store.directoryInfo, store.base, store.file,
		store.options.maxFileBytes,
	); err != nil {
		return nil, err
	}
	header := cloneGenerationHeader(store.header)
	current := cloneGenerationCurrent(store.current)
	familyID := generationFamilyID(store.base, header.identity)
	generation := FirstWALGeneration
	var parentBinding [sha256.Size]byte
	if store.generation.present {
		if store.generation.seal.familyID == ([16]byte{}) ||
			store.generation.seal.familyID != familyID ||
			store.generation.seal.generation == math.MaxUint64 {
			return nil, ErrGenerationSource
		}
		generation = store.generation.seal.generation + 1
		parentBinding = store.generation.seal.bindingDigest
	}
	candidateBase := generationCandidateBase(familyID, generation)
	return &GenerationBuilder{
		owner:       store,
		logicalPath: store.logicalPath, parentPath: store.parentPath, base: store.base,
		candidatePath: filepath.Join(store.parentPath, candidateBase), candidateBase: candidateBase,
		familyID: familyID, generation: generation, parentBinding: parentBinding,
		sourceReadyID: generationReadyFloor(store.current, store.generation),
		directoryInfo: store.directoryInfo, sourceInfo: store.fileInfo,
		header: header, current: current, options: store.options,
		link:   linkGenerationName,
		cancel: make(chan struct{}),
		key:    key, input: GenerationInput{
			Snapshot: cloneSnapshot(input.Snapshot), SnapshotBaseDigest: input.SnapshotBaseDigest,
			RetentionCommitment: input.RetentionCommitment,
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

// BindsInput reports whether this builder captured the exact snapshot,
// state-machine snapshot identity, and retention commitment. It grants no
// build, selection, or deletion authority.
func (builder *GenerationBuilder) BindsInput(input GenerationInput) bool {
	return builder != nil && !builder.closed && input.Snapshot != nil &&
		input.SnapshotBaseDigest != ([sha256.Size]byte{}) &&
		input.SnapshotBaseDigest == builder.input.SnapshotBaseDigest &&
		input.RetentionCommitment != ([sha256.Size]byte{}) &&
		input.RetentionCommitment == builder.input.RetentionCommitment &&
		proto.Equal(input.Snapshot, builder.input.Snapshot)
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
	if err := builder.cancelled(); err != nil {
		return GenerationCandidate{}, err
	}
	if err := builder.acquireSource(); err != nil {
		return GenerationCandidate{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, builder.releaseSource()) }()
	lease, err := builder.acquireBuildLease()
	if err != nil {
		return GenerationCandidate{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, lease.Close()) }()
	if err := builder.reclaimAbandonedStage(); err != nil {
		return GenerationCandidate{}, err
	}
	if err := builder.cancelled(); err != nil {
		return GenerationCandidate{}, err
	}
	if candidate, found, err := builder.validateExistingCandidate(); found || err != nil {
		if errors.Is(err, errGenerationCandidateStale) {
			if reclaimErr := builder.reclaimStaleCandidate(); reclaimErr != nil {
				return GenerationCandidate{}, reclaimErr
			}
		} else {
			return candidate, err
		}
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
	if err := builder.cancelled(); err != nil {
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
		return candidate, nil
	}
	candidate, err = builder.ValidateCandidate()
	if err != nil {
		return GenerationCandidate{}, err
	}
	return candidate, nil
}

// Cancel asks an in-progress offline generation build to stop at its next
// bounded record/chunk boundary. It never mutates the serving WAL and is safe
// to call concurrently with Build.
func (builder *GenerationBuilder) Cancel() {
	if builder == nil || builder.cancel == nil {
		return
	}
	builder.cancelOnce.Do(func() { close(builder.cancel) })
}

func (builder *GenerationBuilder) cancelled() error {
	if builder == nil || builder.cancel == nil {
		return nil
	}
	select {
	case <-builder.cancel:
		return context.Canceled
	default:
		return nil
	}
}

func (builder *GenerationBuilder) acquireSource() error {
	if builder == nil || builder.closed || builder.source != nil {
		return ErrClosed
	}
	_, parentPath, base, root, directoryInfo, err := openNamespace(builder.logicalPath)
	if err != nil {
		return err
	}
	defer root.Close()
	if parentPath != builder.parentPath || base != builder.base ||
		builder.directoryInfo == nil || !os.SameFile(builder.directoryInfo, directoryInfo) {
		return ErrNamespaceChanged
	}
	file, err := root.OpenFile(base, os.O_RDONLY, 0)
	if err != nil {
		return errors.Join(ErrNamespaceChanged, err)
	}
	if err := proveNamedFile(
		root, parentPath, directoryInfo, base, file, builder.options.maxFileBytes,
	); err != nil {
		_ = file.Close()
		return err
	}
	fileInfo, err := file.Stat()
	if err != nil || builder.sourceInfo == nil || !os.SameFile(fileInfo, builder.sourceInfo) {
		_ = file.Close()
		return errors.Join(ErrGenerationSource, err)
	}
	builder.source = file
	return nil
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
	locked, candidate, err := builder.lockValidatedGenerationCandidate()
	if err != nil {
		return GenerationCandidate{}, errors.Join(ErrGenerationCandidate, err)
	}
	return candidate, locked.close()
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
	builder.owner = nil
	builder.seal = generationSeal{}
	builder.candidateFileID = [16]byte{}
	builder.candidateHeaderDigest = [sha256.Size]byte{}
	return err
}

// GenerationInfo returns the intrinsic recovered seal for an open compacted
// generation. The initial source state returns ErrGenerationCandidate.
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
	return generationInfo(
		store.path, store.generation.seal,
		store.header.fileID, store.header.headerDigest,
	), nil
}

func (builder *GenerationBuilder) validateExistingCandidate() (GenerationCandidate, bool, error) {
	_, err := os.Lstat(builder.candidatePath)
	switch {
	case err == nil:
		locked, seal, snapshot, validationErr := builder.lockGenerationCandidate()
		if validationErr != nil {
			return GenerationCandidate{}, true, errors.Join(ErrGenerationConflict, validationErr)
		}
		if !builder.candidateSealMatches(seal, snapshot) {
			closeErr := locked.close()
			if builder.sameGenerationLineage(seal) {
				return GenerationCandidate{}, true, errors.Join(
					errGenerationCandidateStale, closeErr,
				)
			}
			return GenerationCandidate{}, true, errors.Join(
				ErrGenerationConflict, ErrGenerationCandidate, closeErr,
			)
		}
		builder.seal = seal
		builder.loaded = true
		candidate := GenerationCandidate{
			Path: builder.candidatePath,
			Info: generationInfo(
				builder.candidatePath, seal,
				builder.candidateFileID, builder.candidateHeaderDigest,
			),
		}
		if closeErr := locked.close(); closeErr != nil {
			return GenerationCandidate{}, true, closeErr
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

func (builder *GenerationBuilder) sameGenerationLineage(seal generationSeal) bool {
	return builder != nil && seal.familyID == builder.familyID &&
		seal.generation == builder.generation &&
		seal.parentBindingDigest == builder.parentBinding &&
		seal.identityDigest == generationIdentityDigest(builder.header.identity) &&
		seal.sourceFileID == builder.header.fileID &&
		seal.sourceHeaderDigest == builder.header.headerDigest
}

func (builder *GenerationBuilder) reclaimStaleCandidate() (resultErr error) {
	if builder == nil || builder.owner == nil {
		return ErrGenerationConflict
	}
	store := builder.owner
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(); err != nil || store.family == nil ||
		(store.family.state.phase != familyPhaseSource &&
			store.family.state.phase != familyPhaseActive) ||
		store.logicalPath != builder.logicalPath || store.base != builder.base ||
		store.fileInfo == nil || builder.sourceInfo == nil ||
		!os.SameFile(store.fileInfo, builder.sourceInfo) ||
		store.header.fileID != builder.header.fileID ||
		store.header.headerDigest != builder.header.headerDigest ||
		!sameGenerationCurrent(store.current, builder.current) {
		return errors.Join(ErrGenerationConflict, err)
	}
	if store.family.state.phase == familyPhaseSource {
		if store.generation.present || builder.generation != FirstWALGeneration ||
			builder.parentBinding != ([sha256.Size]byte{}) {
			return ErrGenerationConflict
		}
	} else if !store.generation.present ||
		builder.generation != store.generation.seal.generation+1 ||
		builder.parentBinding != store.generation.seal.bindingDigest {
		return ErrGenerationConflict
	}
	if err := store.proveCurrentNamespace(); err != nil {
		return err
	}
	locked, seal, snapshot, err := builder.lockGenerationCandidate()
	if err != nil {
		return errors.Join(ErrGenerationConflict, err)
	}
	defer func() { resultErr = errors.Join(resultErr, locked.close()) }()
	if !builder.sameGenerationLineage(seal) || builder.candidateSealMatches(seal, snapshot) {
		return ErrGenerationConflict
	}
	entry, err := store.root.Lstat(builder.candidateBase)
	fileInfo, fileErr := locked.file.Stat()
	if err != nil || fileErr != nil || !entry.Mode().IsRegular() ||
		!os.SameFile(entry, fileInfo) {
		return errors.Join(ErrNamespaceChanged, err, fileErr)
	}
	if err := store.root.Remove(builder.candidateBase); err != nil {
		return err
	}
	if err := store.options.ops.syncDirectory(store.root); err != nil {
		return persistenceError("reclaim stale WAL generation candidate", true, err)
	}
	if _, err := store.root.Lstat(builder.candidateBase); !errors.Is(err, os.ErrNotExist) {
		return errors.Join(ErrNamespaceChanged, err)
	}
	return store.proveCurrentNamespace()
}

func sameGenerationCurrent(left, right currentState) bool {
	return left.activeSlot == right.activeSlot && left.generation == right.generation &&
		left.walEnd == right.walEnd && left.recordSequence == right.recordSequence &&
		left.chainDigest == right.chainDigest &&
		left.currentIncarnation == right.currentIncarnation &&
		proto.Equal(left.hard, right.hard) && left.first == right.first &&
		left.last == right.last && left.retryPresent == right.retryPresent &&
		left.retry == right.retry && left.retryDigest == right.retryDigest &&
		left.snapshotID == right.snapshotID && left.snapshotIndex == right.snapshotIndex &&
		left.snapshotTerm == right.snapshotTerm && left.snapshotSize == right.snapshotSize &&
		left.snapshotChunks == right.snapshotChunks &&
		left.snapshotDigest == right.snapshotDigest &&
		left.topologyRecoveryEpoch == right.topologyRecoveryEpoch
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

func generationInfo(
	path string,
	seal generationSeal,
	fileID [16]byte,
	headerDigest [sha256.Size]byte,
) GenerationInfo {
	return GenerationInfo{
		Path: path, FamilyID: seal.familyID, Generation: seal.generation,
		ParentBindingDigest: seal.parentBindingDigest,
		FileID:              fileID, HeaderDigest: headerDigest, BindingDigest: seal.bindingDigest,
		SourceFileID: seal.sourceFileID, SourceCutDigest: seal.sourceChainDigest,
		SnapshotBaseDigest: seal.snapshotBaseDigest, RetentionCommitment: seal.retentionCommitment,
		BaseIndex: seal.baseIndex, BaseTerm: seal.baseTerm, LastIndex: seal.suffixLast,
		HardTerm: seal.hard.GetTerm(), HardVote: seal.hard.GetVote(), HardCommit: seal.hard.GetCommit(),
		SourceCurrentIncarnation: seal.sourceCurrentIncarnation,
		SourceReadyID:            seal.sourceReadyID,
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
