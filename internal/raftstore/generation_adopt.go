package raftstore

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// AdoptSelectedGeneration replaces only the frozen source's file/recovery
// image with its authenticated selected candidate. It preserves this Store's
// address, live incarnation, and Ready ordering for an existing Raft owner.
// No serving fence is released and no source is deleted: the exact SQL base
// must still settle through CommitGenerationSelection. Cold recovery continues
// to use Open and obtains the same authenticated selected image.
func (store *Store) AdoptSelectedGeneration() error {
	if store == nil {
		return ErrClosed
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed || store.file == nil {
		return ErrClosed
	}
	if store.poisoned != nil || store.recoveredTornSlot || store.pending != nil ||
		store.attemptedReady != (retryKey{}) || !store.begun ||
		!store.activationPending || store.family == nil || !store.family.locked || store.family.recoveredTorn {
		return ErrGenerationActivationPending
	}
	family := store.family
	if err := proveNamedSizedFile(store.root, store.parentPath, store.directoryInfo,
		family.base, family.file, familyManifestBytes); err != nil {
		return err
	}
	state, slot, torn, err := readFamilyManifest(family.file, family.state.familyID,
		family.state.identityDigest, family.manifestKey)
	if err != nil || torn || (state.phase != familyPhaseSelecting && state.phase != familyPhaseActive) {
		return errors.Join(ErrGenerationActivationPending, err)
	}
	if state != family.state && !validFamilyTransition(family.state, state) {
		return ErrGenerationSource
	}
	// A selecting write may have landed before its Sync failed. Settle that
	// exact durable barrier while retaining both the family and source leases.
	if state != family.state {
		if err := store.options.ops.sync(family.file); err != nil {
			return persistenceError("sync adopted WAL selection", true, err)
		}
	}
	if err := proveNamedSizedFile(store.root, store.parentPath, store.directoryInfo,
		family.base, family.file, familyManifestBytes); err != nil {
		return err
	}
	family.state, family.activeSlot = state, slot
	if store.generation.present && store.generation.seal.generation == state.activeGeneration {
		_, err := store.generationActivationLocked()
		return err
	}
	if state.phase != familyPhaseSelecting {
		return ErrGenerationSource
	}
	if state.sourceFileID != store.header.fileID || state.sourceCutDigest != store.current.chainDigest {
		return ErrGenerationSource
	}
	if err := store.proveCurrentNamespace(); err != nil {
		return err
	}
	selected, err := openFamilySelectedStore(store.root, store.parentPath, store.directoryInfo,
		store.logicalPath, store.logicalBase, generationCandidateBase(state.familyID, state.activeGeneration),
		store.header.identity, store.header.topologyRecoveryEpoch, family.key, store.options, family)
	if err != nil {
		return err
	}
	releaseSelected := func() error {
		return errors.Join(storeio.UnlockWriter(selected.file), selected.file.Close())
	}
	seal := selected.generation.seal
	// A freshly reopened source deliberately forgets commit-only Ready
	// notifications.  The selected generation may nevertheless carry the exact
	// checkpoint commit certified by its snapshot/retention witness.  Adoption
	// remains fenced until CommitGenerationSelection revalidates that SQL owner.
	durableCommit := max(store.current.hard.GetCommit(), store.image.hard.GetCommit())
	durableCommit = max(durableCommit, seal.baseIndex)
	hardCompatible := selected.current.hard.GetTerm() == store.current.hard.GetTerm() &&
		selected.current.hard.GetVote() == store.current.hard.GetVote() &&
		selected.current.hard.GetCommit() <= durableCommit
	if selected.recoveredTornSlot || !selected.activationPending ||
		seal.sourceHeaderDigest != store.header.headerDigest ||
		seal.sourceCurrentGeneration != store.current.generation ||
		seal.sourceWALEnd != uint64(store.current.walEnd) || seal.sourceRecordSequence != store.current.recordSequence ||
		seal.sourceCurrentIncarnation != store.current.currentIncarnation ||
		seal.sourceReadyID != generationReadyFloor(store.current, store.generation) ||
		seal.sourceFirst != store.current.first || seal.sourceLast != store.current.last ||
		selected.current.currentIncarnation != store.current.currentIncarnation ||
		selected.image.last != store.image.last || !hardCompatible {
		return errors.Join(fmt.Errorf(
			"%w: selected/live cut generation=%d/%d end=%d/%d records=%d/%d incarnation=%d/%d ready=%d/%d bounds=[%d,%d]/[%d,%d] image-last=%d/%d hard=%v/%v",
			ErrGenerationSource,
			seal.sourceCurrentGeneration, store.current.generation,
			seal.sourceWALEnd, store.current.walEnd,
			seal.sourceRecordSequence, store.current.recordSequence,
			seal.sourceCurrentIncarnation, store.current.currentIncarnation,
			seal.sourceReadyID, generationReadyFloor(store.current, store.generation),
			seal.sourceFirst, seal.sourceLast, store.current.first, store.current.last,
			selected.image.last, store.image.last, selected.current.hard, store.current.hard,
		), releaseSelected())
	}
	// Re-prove the still-locked source after candidate recovery. No uncertain
	// namespace or pending mutation is transplanted into the live owner.
	if err := selected.validateRetiringSourceLocked(state); err != nil {
		return errors.Join(err, releaseSelected())
	}
	oldFile := store.file
	store.path, store.base = selected.path, selected.base
	store.file, store.fileInfo, store.locked = selected.file, selected.fileInfo, true
	store.header, store.current, store.image, store.generation = selected.header, selected.current, selected.image, selected.generation
	// The record workspace owns keyed HMAC state for exactly one WAL header.
	// Generation adoption replaces the file keys, so retaining the old
	// workspace would derive nonces and tags under the retired generation.
	store.recordEncode = recordEncodeWorkspace{}
	store.syncCount++ // candidate allocation restoration Sync
	// The family lease and candidate lease cover the complete transition;
	// release the old inode only after the stable Store points at the candidate.
	return errors.Join(storeio.UnlockWriter(oldFile), oldFile.Close())
}
