package raftstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/internal/storeio"
)

type familyManifest struct {
	base          string
	file          *os.File
	fileInfo      os.FileInfo
	locked        bool
	activeSlot    uint8
	state         familyState
	recoveredTorn bool
	key           Key
	manifestKey   [32]byte
}

func familyManifestBase(familyID [16]byte) string {
	return generationFamilyPrefix(familyID) + ".family"
}

func openFamilyManifest(
	root *os.Root,
	parentPath string,
	directoryInfo os.FileInfo,
	logicalBase string,
	expected Identity,
	key Key,
	options normalizedOptions,
) (*familyManifest, string, error) {
	familyID := generationFamilyID(logicalBase, expected)
	base := familyManifestBase(familyID)
	entry, err := root.Lstat(base)
	if errors.Is(err, os.ErrNotExist) {
		if diagnosticErr := diagnoseLogicalWALHeader(
			root, parentPath, directoryInfo, logicalBase, expected, key, options,
		); diagnosticErr != nil {
			return nil, "", diagnosticErr
		}
		return nil, "", fmt.Errorf("%w: required WAL family manifest is absent", ErrCorrupt)
	}
	if err != nil || !entry.Mode().IsRegular() {
		return nil, "", errors.Join(ErrNamespaceChanged, err)
	}
	file, err := root.OpenFile(base, os.O_RDWR, 0)
	if err != nil {
		return nil, "", err
	}
	locked := false
	fail := func(cause error) (*familyManifest, string, error) {
		if locked {
			cause = errors.Join(cause, storeio.UnlockWriter(file))
		}
		cause = errors.Join(cause, file.Close())
		return nil, "", cause
	}
	if err := storeio.LockWriter(file); err != nil {
		return fail(errors.Join(ErrLocked, err))
	}
	locked = true
	if err := proveNamedSizedFile(
		root, parentPath, directoryInfo, base, file, familyManifestBytes,
	); err != nil {
		return fail(err)
	}
	stageBase := base + ".stage"
	stage, stageErr := root.Lstat(stageBase)
	switch {
	case errors.Is(stageErr, os.ErrNotExist):
	case stageErr != nil:
		return fail(stageErr)
	case !stage.Mode().IsRegular() || !os.SameFile(stage, entry):
		return fail(ErrNamespaceChanged)
	default:
		// The stage hard link is a durable witness that an earlier official
		// link publication returned outcome-unknown. Settle that parent barrier
		// before ordinary Open may serve the family.
		if err := options.ops.syncDirectory(root); err != nil {
			return fail(persistenceError("settle WAL family publication", true, err))
		}
		if err := root.Remove(stageBase); err != nil {
			return fail(err)
		}
		if err := options.ops.syncDirectory(root); err != nil {
			return fail(persistenceError("settle WAL family stage removal", true, err))
		}
		if err := proveNamedSizedFile(
			root, parentPath, directoryInfo, base, file, familyManifestBytes,
		); err != nil {
			return fail(err)
		}
	}
	identityDigest := generationIdentityDigest(expected)
	manifestKey := familyManifestKey(key, familyID, identityDigest)
	state, activeSlot, recoveredTorn, err := readFamilyManifest(
		file, familyID, identityDigest, manifestKey,
	)
	if err != nil {
		if diagnosticErr := diagnoseLogicalWALHeader(
			root, parentPath, directoryInfo, logicalBase, expected, key, options,
		); errors.Is(diagnosticErr, ErrIdentityMismatch) ||
			errors.Is(diagnosticErr, ErrKeyMismatch) || errors.Is(diagnosticErr, ErrLocked) {
			return fail(diagnosticErr)
		}
		return fail(err)
	}
	target := logicalBase
	if state.phase == familyPhaseSelecting {
		candidate := generationCandidateBase(familyID, state.activeGeneration)
		candidateEntry, candidateErr := root.Lstat(candidate)
		switch {
		case candidateErr == nil && candidateEntry.Mode().IsRegular():
			target = candidate
		case errors.Is(candidateErr, os.ErrNotExist):
			target = logicalBase
		case candidateErr != nil:
			return fail(candidateErr)
		default:
			return fail(ErrNamespaceChanged)
		}
	}
	ownedKey := key
	ownedKey.ID = strings.Clone(key.ID)
	ownedKey.Wrapped = slices.Clone(key.Wrapped)
	return &familyManifest{
		base: base, file: file, fileInfo: entry, locked: true,
		activeSlot: activeSlot, state: state, recoveredTorn: recoveredTorn,
		key: ownedKey, manifestKey: manifestKey,
	}, target, nil
}

func diagnoseLogicalWALHeader(
	root *os.Root,
	parentPath string,
	directoryInfo os.FileInfo,
	base string,
	expected Identity,
	key Key,
	options normalizedOptions,
) error {
	entry, err := root.Lstat(base)
	if err != nil || !entry.Mode().IsRegular() {
		return errors.Join(ErrNamespaceChanged, err)
	}
	file, err := root.OpenFile(base, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	locked := false
	defer func() {
		if locked {
			_ = storeio.UnlockWriter(file)
		}
		_ = file.Close()
	}()
	if err := storeio.LockWriter(file); err != nil {
		return errors.Join(ErrLocked, err)
	}
	locked = true
	if err := proveNamedFile(
		root, parentPath, directoryInfo, base, file, options.maxFileBytes,
	); err != nil {
		return err
	}
	staticBytes := make([]byte, StaticHeaderBytes)
	if _, err := file.ReadAt(staticBytes, 0); err != nil {
		return fmt.Errorf("%w: read static header: %v", ErrCorrupt, err)
	}
	_, _, err = unmarshalStaticHeader(staticBytes, expected, key, options)
	return err
}

func readFamilyManifest(
	file *os.File,
	familyID [16]byte,
	identityDigest [32]byte,
	manifestKey [32]byte,
) (familyState, uint8, bool, error) {
	if file == nil {
		return familyState{}, 0, false, ErrCorrupt
	}
	data := make([]byte, familyManifestBytes)
	n, err := file.ReadAt(data, 0)
	if err != nil || n != len(data) || !allZero(data[familySlotCount*familySlotBytes:]) {
		return familyState{}, 0, false, fmt.Errorf(
			"%w: read family manifest: %v", ErrCorrupt, err,
		)
	}
	var slots [familySlotCount]decodedFamilySlot
	var slotErrs [familySlotCount]error
	for slot := uint8(0); slot < familySlotCount; slot++ {
		offset := int(slot) * familySlotBytes
		slots[slot], slotErrs[slot] = unmarshalFamilySlot(
			data[offset:offset+familySlotBytes], slot,
			familyID, identityDigest, manifestKey,
		)
	}
	if slotErrs[0] == nil && slotErrs[1] == nil {
		state, activeSlot, selectErr := selectFamilyState(slots)
		if selectErr != nil {
			return familyState{}, 0, false, selectErr
		}
		// Creation authenticates both source slots. An absent peer is therefore
		// always damage; it can never be confused with a selecting publication
		// that was acknowledged and later lost.
		recoveredTorn := slots[0].absent || slots[1].absent
		return state, activeSlot, recoveredTorn, nil
	}
	for slot := uint8(0); slot < familySlotCount; slot++ {
		other := uint8(1 - slot)
		if slotErrs[slot] != nil && slotErrs[other] == nil && !slots[other].absent {
			return slots[other].state, other, true, nil
		}
	}
	return familyState{}, 0, false, errors.Join(
		ErrCorrupt, slotErrs[0], slotErrs[1],
	)
}

func createFamilyManifest(
	root *os.Root,
	parentPath string,
	directoryInfo os.FileInfo,
	state familyState,
	key Key,
	options normalizedOptions,
) (*familyManifest, error) {
	if state.slotGeneration != 1 || state.phase != familyPhaseSource {
		return nil, ErrInvalid
	}
	base := familyManifestBase(state.familyID)
	if _, err := root.Lstat(base); err == nil {
		return nil, ErrGenerationConflict
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	stageBase := base + ".stage"
	if err := reclaimFamilyManifestStage(
		root, parentPath, directoryInfo, stageBase, options,
	); err != nil {
		return nil, err
	}
	file, err := root.OpenFile(stageBase, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	locked := false
	published := false
	createdInfo, statErr := file.Stat()
	cleanup := func(cause error) error {
		if locked {
			cause = errors.Join(cause, storeio.UnlockWriter(file))
		}
		cause = errors.Join(cause, file.Close())
		if published {
			return cause
		}
		entry, entryErr := root.Lstat(stageBase)
		switch {
		case errors.Is(entryErr, os.ErrNotExist):
			return cause
		case entryErr != nil:
			return errors.Join(cause, entryErr)
		case createdInfo == nil || !entry.Mode().IsRegular() ||
			!os.SameFile(entry, createdInfo):
			return errors.Join(cause, ErrNamespaceChanged)
		}
		if removeErr := root.Remove(stageBase); removeErr != nil {
			return errors.Join(cause, removeErr)
		}
		return errors.Join(cause, options.ops.syncDirectory(root))
	}
	if statErr != nil {
		return nil, cleanup(statErr)
	}
	if err := storeio.LockWriter(file); err != nil {
		return nil, cleanup(errors.Join(ErrLocked, err))
	}
	locked = true
	if err := options.ops.preallocate(file, familyManifestBytes); err != nil {
		return nil, cleanup(persistenceError("preallocate WAL family manifest", false, err))
	}
	identityDigest := state.identityDigest
	manifestKey := familyManifestKey(key, state.familyID, identityDigest)
	encoded, err := marshalFamilySlot(state, 0, manifestKey)
	if err != nil {
		return nil, cleanup(err)
	}
	if err := writeExactAt(options.ops, file, encoded[:], 0); err != nil {
		return nil, cleanup(persistenceError("write WAL family manifest", false, err))
	}
	peer, err := marshalFamilySlot(state, 1, manifestKey)
	if err != nil {
		return nil, cleanup(err)
	}
	if err := writeExactAt(
		options.ops, file, peer[:], familySlotBytes,
	); err != nil {
		return nil, cleanup(persistenceError("write WAL family manifest peer", false, err))
	}
	if err := options.ops.sync(file); err != nil {
		return nil, cleanup(persistenceError("sync WAL family manifest", true, err))
	}
	if err := proveNamedSizedFile(
		root, parentPath, directoryInfo, stageBase, file, familyManifestBytes,
	); err != nil {
		return nil, cleanup(persistenceError("prove WAL family manifest", true, err))
	}
	if err := root.Link(stageBase, base); err != nil {
		entry, entryErr := root.Lstat(base)
		fileInfo, fileErr := file.Stat()
		switch {
		case entryErr == nil && fileErr == nil && entry.Mode().IsRegular() &&
			os.SameFile(entry, fileInfo):
			// The no-replace link landed despite the reported error.
		case errors.Is(entryErr, os.ErrNotExist) && fileErr == nil:
			return nil, cleanup(persistenceError("publish WAL family manifest", false, err))
		case entryErr == nil && fileErr == nil:
			return nil, cleanup(errors.Join(ErrGenerationConflict, err))
		default:
			published = true
			return nil, cleanup(persistenceError(
				"settle WAL family manifest publication", true,
				errors.Join(err, entryErr, fileErr),
			))
		}
	}
	published = true
	if err := options.ops.syncDirectory(root); err != nil {
		return nil, cleanup(persistenceError("sync WAL family manifest directory", true, err))
	}
	if err := root.Remove(stageBase); err != nil {
		return nil, cleanup(persistenceError("remove WAL family manifest stage", true, err))
	}
	if err := options.ops.syncDirectory(root); err != nil {
		return nil, cleanup(persistenceError("sync WAL family manifest stage removal", true, err))
	}
	if err := proveNamedSizedFile(
		root, parentPath, directoryInfo, base, file, familyManifestBytes,
	); err != nil {
		return nil, cleanup(persistenceError("re-prove WAL family manifest", true, err))
	}
	ownedKey := key
	ownedKey.ID = strings.Clone(key.ID)
	ownedKey.Wrapped = slices.Clone(key.Wrapped)
	return &familyManifest{
		base: base, file: file, fileInfo: createdInfo, locked: true,
		activeSlot: 1, state: state, key: ownedKey, manifestKey: manifestKey,
	}, nil
}

func reclaimFamilyManifestStage(
	root *os.Root,
	parentPath string,
	directoryInfo os.FileInfo,
	base string,
	options normalizedOptions,
) error {
	// The deterministic stage name cannot be reused until a parent barrier has
	// settled any earlier outcome-unknown unlink. Otherwise a crash may expose an
	// old inode under the name after a different family image has been created.
	if err := options.ops.syncDirectory(root); err != nil {
		return persistenceError("settle WAL family stage namespace", true, err)
	}
	pinnedDirectory, err := root.Stat(".")
	if err != nil || !os.SameFile(directoryInfo, pinnedDirectory) {
		return errors.Join(ErrNamespaceChanged, err)
	}
	entry, err := root.Lstat(base)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !entry.Mode().IsRegular() {
		return errors.Join(ErrGenerationConflict, err)
	}
	file, err := root.OpenFile(base, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	locked := false
	defer func() {
		if locked {
			_ = storeio.UnlockWriter(file)
		}
		_ = file.Close()
	}()
	if err := storeio.LockWriter(file); err != nil {
		return errors.Join(ErrLocked, err)
	}
	locked = true
	pinnedDirectory, err = root.Stat(".")
	if err != nil || !os.SameFile(directoryInfo, pinnedDirectory) {
		return errors.Join(ErrNamespaceChanged, err)
	}
	pinned, entryErr := root.Lstat(base)
	fileInfo, fileErr := file.Stat()
	if entryErr != nil || fileErr != nil || !pinned.Mode().IsRegular() ||
		!os.SameFile(pinned, fileInfo) {
		return errors.Join(ErrNamespaceChanged, entryErr, fileErr)
	}
	if err := root.Remove(base); err != nil {
		return err
	}
	if err := options.ops.syncDirectory(root); err != nil {
		return persistenceError("reclaim WAL family manifest stage", true, err)
	}
	_ = parentPath
	return nil
}

func (manifest *familyManifest) writeNext(
	root *os.Root,
	parentPath string,
	directoryInfo os.FileInfo,
	next familyState,
	options normalizedOptions,
) error {
	if manifest == nil || manifest.file == nil || !manifest.locked ||
		next.slotGeneration != manifest.state.slotGeneration+1 ||
		next.familyID != manifest.state.familyID ||
		next.identityDigest != manifest.state.identityDigest ||
		!validFamilyTransition(manifest.state, next) {
		return ErrInvalid
	}
	nextSlot := uint8(1 - manifest.activeSlot)
	encoded, err := marshalFamilySlot(next, nextSlot, manifest.manifestKey)
	if err != nil {
		return err
	}
	if err := proveNamedSizedFile(
		root, parentPath, directoryInfo, manifest.base, manifest.file,
		familyManifestBytes,
	); err != nil {
		return err
	}
	if err := writeExactAt(
		options.ops, manifest.file, encoded[:], int64(nextSlot)*familySlotBytes,
	); err != nil {
		return persistenceError("write WAL family slot", true, err)
	}
	if err := options.ops.sync(manifest.file); err != nil {
		return persistenceError("sync WAL family slot", true, err)
	}
	if err := proveNamedSizedFile(
		root, parentPath, directoryInfo, manifest.base, manifest.file,
		familyManifestBytes,
	); err != nil {
		return persistenceError("prove WAL family slot", true, err)
	}
	manifest.activeSlot = nextSlot
	manifest.state = next
	manifest.recoveredTorn = false
	return nil
}

func (manifest *familyManifest) close() error {
	if manifest == nil {
		return nil
	}
	var err error
	if manifest.locked {
		err = storeio.UnlockWriter(manifest.file)
		manifest.locked = false
	}
	if manifest.file != nil {
		err = errors.Join(err, manifest.file.Close())
		manifest.file = nil
	}
	clear(manifest.key.Material[:])
	clear(manifest.key.Wrapped)
	manifest.key = Key{}
	clear(manifest.manifestKey[:])
	return err
}

func (manifest *familyManifest) path(parent string) string {
	if manifest == nil {
		return ""
	}
	return filepath.Join(parent, manifest.base)
}
