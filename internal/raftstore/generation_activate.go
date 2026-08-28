package raftstore

import (
	"errors"
	"fmt"
	"math"
	"os"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// GenerationActivation is detached recovery evidence for one selected but not
// yet fully settled WAL generation. The snapshot must be installed and
// checkpointed by the exact bound replicated state before the logical WAL leaf
// may be replaced.
type GenerationActivation struct {
	Snapshot *pb.Snapshot
	Info     GenerationInfo
}

// GenerationActivationIdentity is the fixed-width authenticated identity of a
// selecting generation. PublishGenerationSelection returns it atomically with
// the publication result so a bound state machine can conservatively fence an
// outcome-unknown write without racing a later Store.Close.
type GenerationActivationIdentity struct {
	FamilyID            [16]byte
	Generation          uint64
	BindingDigest       [32]byte
	SnapshotBaseDigest  [32]byte
	RetentionCommitment [32]byte
}

// Valid reports whether identity can name a selectable generation.
func (identity GenerationActivationIdentity) Valid() bool {
	return identity.FamilyID != ([16]byte{}) && identity.Generation != 0 &&
		identity.BindingDigest != ([32]byte{}) &&
		identity.SnapshotBaseDigest != ([32]byte{}) &&
		identity.RetentionCommitment != ([32]byte{})
}

// Matches reports whether info is the exact selected SQL durability base, not
// merely the same family/generation coordinate.
func (identity GenerationActivationIdentity) Matches(info GenerationInfo) bool {
	return identity.Valid() && identity.FamilyID == info.FamilyID &&
		identity.Generation == info.Generation &&
		identity.BindingDigest == info.BindingDigest &&
		identity.SnapshotBaseDigest == info.SnapshotBaseDigest &&
		identity.RetentionCommitment == info.RetentionCommitment
}

// GenerationActivationCompletion is an opaque capability minted only after
// the authenticated family record is durably active. Its coordinates are
// deliberately private: code outside raftstore can compare a completion with
// the exact pending identity it already owns, but cannot manufacture one to
// release a state-machine fence early.
type GenerationActivationCompletion struct {
	familyID      [16]byte
	generation    uint64
	bindingDigest [32]byte
}

// Matches reports whether completion releases exactly one non-zero pending
// family generation. A zero value never matches.
func (completion GenerationActivationCompletion) Matches(
	familyID [16]byte,
	generation uint64,
	bindingDigest [32]byte,
) bool {
	return completion.familyID != ([16]byte{}) &&
		completion.generation != 0 &&
		completion.bindingDigest != ([32]byte{}) &&
		completion.familyID == familyID &&
		completion.generation == generation &&
		completion.bindingDigest == bindingDigest
}

// GenerationActivationSettler is the durable state-machine half of a WAL
// generation transition. SettleGenerationActivation must idempotently install
// and checkpoint the exact activation snapshot before returning nil. It must
// not re-enter the Store: CommitGenerationSelection keeps the selected WAL
// frozen while settlement and logical-leaf replacement are ordered.
type GenerationActivationSettler interface {
	SettleGenerationActivation(GenerationActivation) error
	CompleteGenerationActivation(GenerationActivationCompletion)
}

// PublishGenerationSelection durably records a validated candidate as the
// sole next family generation. It does not replace the logical WAL leaf. On
// success this source handle becomes non-serving. AdoptSelectedGeneration may
// recover the exact candidate in place for its live owner; alternatively close
// it and Open its logical path to recover the candidate activation-pending.
func (store *Store) PublishGenerationSelection(
	builder *GenerationBuilder,
) (identity GenerationActivationIdentity, resultErr error) {
	if store == nil || builder == nil {
		return GenerationActivationIdentity{}, ErrInvalid
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(); err != nil {
		return GenerationActivationIdentity{}, err
	}
	if store.recoveredTornSlot || !store.begun || store.current.currentIncarnation == 0 ||
		builder.closed || builder.logicalPath != store.logicalPath ||
		builder.parentPath != store.parentPath || builder.base != store.base ||
		builder.directoryInfo == nil || !os.SameFile(builder.directoryInfo, store.directoryInfo) ||
		builder.sourceInfo == nil || !os.SameFile(builder.sourceInfo, store.fileInfo) {
		return GenerationActivationIdentity{}, ErrGenerationSource
	}
	lockedCandidate, candidate, err := builder.lockValidatedGenerationCandidate()
	if err != nil {
		return GenerationActivationIdentity{}, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, lockedCandidate.close())
	}()
	seal := builder.seal
	if candidate.Info.Path != builder.candidatePath ||
		seal.familyID != builder.familyID || seal.generation != builder.generation ||
		seal.parentBindingDigest != builder.parentBinding ||
		seal.sourceFileID != store.header.fileID ||
		seal.sourceHeaderDigest != store.header.headerDigest ||
		seal.sourceCurrentGeneration != store.current.generation ||
		seal.sourceWALEnd != uint64(store.current.walEnd) ||
		seal.sourceRecordSequence != store.current.recordSequence ||
		seal.sourceChainDigest != store.current.chainDigest ||
		seal.sourceCurrentIncarnation != store.current.currentIncarnation ||
		seal.sourceReadyID != generationReadyFloor(store.current, store.generation) ||
		seal.sourceFirst != store.current.first || seal.sourceLast != store.current.last ||
		candidate.Info.FileID == ([16]byte{}) ||
		candidate.Info.HeaderDigest == ([32]byte{}) ||
		candidate.Info.BindingDigest != seal.bindingDigest {
		return GenerationActivationIdentity{}, ErrGenerationSource
	}
	if err := store.proveCurrentNamespace(); err != nil {
		store.poisonNamespace(err, false)
		return GenerationActivationIdentity{}, err
	}
	if store.family == nil {
		return GenerationActivationIdentity{}, ErrGenerationSource
	}
	current := store.family.state
	if store.family.recoveredTorn || current.slotGeneration > math.MaxUint64-2 {
		return GenerationActivationIdentity{}, ErrGenerationSource
	}
	switch current.phase {
	case familyPhaseSource:
		if store.generation.present || seal.generation != FirstWALGeneration ||
			seal.parentBindingDigest != ([32]byte{}) ||
			current.activeFileID != store.header.fileID ||
			current.activeHeaderDigest != store.header.headerDigest {
			return GenerationActivationIdentity{}, ErrGenerationSource
		}
	case familyPhaseActive:
		if !store.generation.present ||
			current.activeGeneration != store.generation.seal.generation ||
			current.activeFileID != store.header.fileID ||
			current.activeHeaderDigest != store.header.headerDigest ||
			current.activeBindingDigest != store.generation.seal.bindingDigest ||
			seal.generation != current.activeGeneration+1 ||
			seal.parentBindingDigest != current.activeBindingDigest {
			return GenerationActivationIdentity{}, ErrGenerationSource
		}
	default:
		return GenerationActivationIdentity{}, ErrGenerationSource
	}
	if err := store.settleSelectionStagesLocked(
		builder, lockedCandidate, current.phase == familyPhaseSource,
	); err != nil {
		return GenerationActivationIdentity{}, err
	}
	next := familyState{
		slotGeneration: current.slotGeneration + 1,
		phase:          familyPhaseSelecting, familyID: seal.familyID,
		identityDigest:      seal.identityDigest,
		activeGeneration:    seal.generation,
		activeFileID:        candidate.Info.FileID,
		activeHeaderDigest:  candidate.Info.HeaderDigest,
		activeBindingDigest: seal.bindingDigest,
		parentBindingDigest: builder.parentBinding,
		sourceFileID:        seal.sourceFileID,
		sourceCutDigest:     seal.sourceChainDigest,
		snapshotBaseDigest:  seal.snapshotBaseDigest,
		retentionCommitment: seal.retentionCommitment,
	}
	identity = GenerationActivationIdentity{
		FamilyID:            next.familyID,
		Generation:          next.activeGeneration,
		BindingDigest:       next.activeBindingDigest,
		SnapshotBaseDigest:  next.snapshotBaseDigest,
		RetentionCommitment: next.retentionCommitment,
	}
	if err := store.family.writeNext(
		store.root, store.parentPath, store.directoryInfo, next, store.options,
	); err != nil {
		store.activationPending = true
		return identity, err
	}
	store.activationPending = true
	return identity, nil
}

// settleSelectionStagesLocked proves that the candidate and current source are
// the only full-size names that can survive selection. One unconditional
// parent barrier jointly settles outcome-unknown absence of the candidate stage
// and, for the first generation, the source creation witness. Same-inode
// aliases are removed under both writer authorities and share one final parent
// barrier. A distinct occupant fails closed before family authority changes.
func (store *Store) settleSelectionStagesLocked(
	builder *GenerationBuilder,
	candidate *generationCandidateLock,
	settleCreate bool,
) error {
	if store == nil || store.file == nil || store.fileInfo == nil ||
		store.family == nil || store.family.file == nil || builder == nil ||
		candidate == nil || candidate.root == nil || candidate.file == nil ||
		!candidate.locked {
		return ErrClosed
	}
	if err := store.options.ops.syncDirectory(candidate.root); err != nil {
		return persistenceError("settle WAL selection namespace", true, err)
	}
	candidateInfo, err := candidate.file.Stat()
	if err != nil {
		return err
	}
	stagePresent := false
	entry, err := candidate.root.Lstat(builder.stageBase())
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return err
	case !entry.Mode().IsRegular() || !os.SameFile(entry, candidateInfo):
		return ErrGenerationConflict
	default:
		stagePresent = true
	}
	createPresent := false
	createBase := walCreateStageBase(store.logicalBase)
	if settleCreate {
		entry, err = candidate.root.Lstat(createBase)
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil:
			return err
		case !entry.Mode().IsRegular() || !os.SameFile(entry, store.fileInfo):
			return errors.Join(ErrNamespaceChanged, err)
		default:
			createPresent = true
		}
	}
	prove := func() error {
		if err := proveNamedFile(
			candidate.root, builder.parentPath, builder.directoryInfo,
			builder.candidateBase, candidate.file, builder.options.maxFileBytes,
		); err != nil {
			return err
		}
		if err := proveNamedFile(
			candidate.root, store.parentPath, store.directoryInfo,
			store.base, store.file, store.options.maxFileBytes,
		); err != nil {
			return err
		}
		return proveNamedSizedFile(
			candidate.root, store.parentPath, store.directoryInfo,
			store.family.base, store.family.file, familyManifestBytes,
		)
	}
	if err := prove(); err != nil {
		return err
	}
	if stagePresent {
		if err := candidate.root.Remove(builder.stageBase()); err != nil {
			return err
		}
	}
	if createPresent {
		if err := candidate.root.Remove(createBase); err != nil {
			return err
		}
	}
	if stagePresent || createPresent {
		if err := store.options.ops.syncDirectory(candidate.root); err != nil {
			return persistenceError("reclaim WAL selection aliases", true, err)
		}
	}
	return prove()
}

// PendingGenerationActivation returns the exact selected snapshot and seal
// needed to settle an interrupted activation. It is the only data API allowed
// while activation is pending.
func (store *Store) PendingGenerationActivation() (GenerationActivation, error) {
	if store == nil {
		return GenerationActivation{}, ErrClosed
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed || store.file == nil || store.family == nil ||
		store.family.recoveredTorn ||
		!store.activationPending || store.family.state.phase != familyPhaseSelecting ||
		!store.generation.present {
		return GenerationActivation{}, ErrGenerationActivationPending
	}
	return store.generationActivationLocked()
}

// CommitGenerationSelection atomically replaces the logical WAL leaf with the
// already-open selected candidate, syncs the parent, and then records the
// family as active. settler is invoked while the selected WAL is frozen and
// must first durably install and checkpoint the exact activation snapshot in
// the bound replicated state. A failed or outcome-unknown settlement leaves
// the old logical leaf intact and the selection retryable after restart.
func (store *Store) CommitGenerationSelection(
	settler GenerationActivationSettler,
) error {
	if store == nil {
		return ErrClosed
	}
	if settler == nil {
		return ErrInvalid
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed || store.file == nil || store.family == nil ||
		store.family.recoveredTorn ||
		!store.activationPending ||
		!store.generation.present {
		return ErrGenerationActivationPending
	}
	state := store.family.state
	if state.phase == familyPhaseActive {
		// The active family slot may have reached stable storage while the final
		// logical-name proof failed. Retrying on the same frozen handle must only
		// re-prove that namespace and release the exact completion capability; it
		// must never install or checkpoint the SQL base a second time.
		if err := store.proveCurrentNamespace(); err != nil {
			return persistenceError("prove activated WAL generation", true, err)
		}
		store.activationPending = false
		settler.CompleteGenerationActivation(GenerationActivationCompletion{
			familyID:      state.familyID,
			generation:    state.activeGeneration,
			bindingDigest: state.activeBindingDigest,
		})
		return nil
	}
	if state.phase != familyPhaseSelecting {
		return ErrGenerationActivationPending
	}
	activation, err := store.generationActivationLocked()
	if err != nil {
		return err
	}
	if state.slotGeneration == math.MaxUint64 {
		return ErrBounds
	}
	if err := settler.SettleGenerationActivation(activation); err != nil {
		return err
	}
	if store.base != store.logicalBase {
		if err := store.proveCurrentNamespace(); err != nil {
			return err
		}
		if err := store.validateRetiringSourceLocked(state); err != nil {
			return err
		}
		renameErr := store.root.Rename(store.base, store.logicalBase)
		if renameErr != nil {
			landed, settleErr := store.selectionRenameLandedLocked()
			if settleErr != nil || !landed {
				return persistenceError(
					"replace logical WAL generation", settleErr != nil,
					errors.Join(renameErr, settleErr),
				)
			}
		}
		store.base = store.logicalBase
		store.path = store.logicalPath
	}
	// This barrier is deliberately unconditional. A prior attempt may have
	// landed the rename but failed its directory Sync; the same handle or a
	// selecting reopen must never mark the family active without retrying it.
	if err := store.options.ops.syncDirectory(store.root); err != nil {
		return persistenceError("sync logical WAL generation", true, err)
	}
	if err := store.proveCurrentNamespace(); err != nil {
		return persistenceError("prove active WAL generation", true, err)
	}
	next := state
	next.slotGeneration++
	next.phase = familyPhaseActive
	if err := store.family.writeNext(
		store.root, store.parentPath, store.directoryInfo, next, store.options,
	); err != nil {
		return err
	}
	// Family authority is durable at this point. Prove once more that the
	// logical leaf still names the selected inode before releasing the SQL
	// selection fence. Failure leaves activationPending set so the same frozen
	// handle can retry this proof without repeating settlement.
	if err := store.proveCurrentNamespace(); err != nil {
		return persistenceError("prove activated WAL generation", true, err)
	}
	store.activationPending = false
	settler.CompleteGenerationActivation(GenerationActivationCompletion{
		familyID:      next.familyID,
		generation:    next.activeGeneration,
		bindingDigest: next.activeBindingDigest,
	})
	return nil
}

func (store *Store) generationActivationLocked() (GenerationActivation, error) {
	state := store.family.state
	seal := store.generation.seal
	if seal.familyID != state.familyID || seal.generation != state.activeGeneration ||
		seal.bindingDigest != state.activeBindingDigest ||
		store.header.fileID != state.activeFileID ||
		store.header.headerDigest != state.activeHeaderDigest {
		return GenerationActivation{}, ErrCorrupt
	}
	return GenerationActivation{
		Snapshot: cloneSnapshot(store.header.snapshot),
		Info: generationInfo(
			store.logicalPath, seal, store.header.fileID, store.header.headerDigest,
		),
	}, nil
}

func (store *Store) validateRetiringSourceLocked(state familyState) error {
	entry, err := store.root.Lstat(store.logicalBase)
	if err != nil || !entry.Mode().IsRegular() {
		return errors.Join(ErrNamespaceChanged, err)
	}
	file, err := store.root.OpenFile(store.logicalBase, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := proveNamedFile(
		store.root, store.parentPath, store.directoryInfo, store.logicalBase,
		file, store.options.maxFileBytes,
	); err != nil {
		return err
	}
	static := make([]byte, StaticHeaderBytes)
	if _, err := file.ReadAt(static, 0); err != nil {
		return err
	}
	header, _, err := unmarshalStaticHeader(
		static, store.header.identity, store.family.key, store.options,
	)
	if err != nil {
		return errors.Join(ErrGenerationSource, err)
	}
	seal := store.generation.seal
	if header.fileID != state.sourceFileID || header.fileID != seal.sourceFileID {
		return fmt.Errorf("%w: retiring source file identity", ErrGenerationSource)
	}
	if header.headerDigest != seal.sourceHeaderDigest {
		return fmt.Errorf("%w: retiring source header digest", ErrGenerationSource)
	}
	current, recoveredTorn, err := recoverCurrent(file, header, store.options)
	if err != nil || recoveredTorn {
		return errors.Join(ErrGenerationSource, err)
	}
	if current.generation != seal.sourceCurrentGeneration {
		return fmt.Errorf("%w: retiring source current generation", ErrGenerationSource)
	}
	if current.walEnd < 0 || uint64(current.walEnd) != seal.sourceWALEnd {
		return fmt.Errorf("%w: retiring source WAL end", ErrGenerationSource)
	}
	if current.recordSequence != seal.sourceRecordSequence {
		return fmt.Errorf("%w: retiring source record sequence", ErrGenerationSource)
	}
	if current.chainDigest != state.sourceCutDigest ||
		current.chainDigest != seal.sourceChainDigest {
		return fmt.Errorf("%w: retiring source chain digest", ErrGenerationSource)
	}
	if current.currentIncarnation != seal.sourceCurrentIncarnation {
		return fmt.Errorf("%w: retiring source incarnation", ErrGenerationSource)
	}
	if current.retryPresent && (current.retry.incarnation != current.currentIncarnation || current.retry.readyID != seal.sourceReadyID) {
		return fmt.Errorf("%w: retiring source Ready cursor", ErrGenerationSource)
	}
	// With no new durable Ready, the cursor can be inherited from a previous
	// generation seal. Its exact source chain is authenticated above; selection
	// and live adoption also check that inherited cursor without rescanning WAL.
	if current.topologyRecoveryEpoch != seal.topologyRecoveryEpoch {
		return fmt.Errorf("%w: retiring source topology epoch", ErrGenerationSource)
	}
	if current.first != seal.sourceFirst || current.last != seal.sourceLast {
		return fmt.Errorf("%w: retiring source bounds", ErrGenerationSource)
	}
	if !proto.Equal(current.hard, seal.hard) {
		return fmt.Errorf("%w: retiring source HardState", ErrGenerationSource)
	}
	return nil
}

func (store *Store) selectionRenameLandedLocked() (bool, error) {
	fileInfo, err := store.file.Stat()
	if err != nil {
		return false, err
	}
	logical, logicalErr := store.root.Lstat(store.logicalBase)
	candidate, candidateErr := store.root.Lstat(store.base)
	switch {
	case logicalErr == nil && logical.Mode().IsRegular() && os.SameFile(logical, fileInfo) &&
		errors.Is(candidateErr, os.ErrNotExist):
		store.base = store.logicalBase
		store.path = store.logicalPath
		return true, nil
	case candidateErr == nil && candidate.Mode().IsRegular() && os.SameFile(candidate, fileInfo):
		return false, nil
	default:
		return false, errors.Join(ErrNamespaceChanged, logicalErr, candidateErr)
	}
}

// RecoveredTornFamilySlot reports that one authenticated family slot was used
// while its peer was damaged. Callers must quarantine the member before serve.
func (store *Store) RecoveredTornFamilySlot() bool {
	if store == nil {
		return false
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store != nil && store.family != nil && store.family.recoveredTorn
}
