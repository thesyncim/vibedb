package raftstore

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// generationBuildLease is a zero-length per-family coordination inode. It is
// deliberately persistent: deleting a lock name while another process has it
// open can split contenders across two inodes. The full-size stage beside it is
// deterministic and is the only construction image a crashed builder can
// leave behind.
type generationBuildLease struct {
	root   *os.Root
	file   *os.File
	locked bool
}

func (builder *GenerationBuilder) stageBase() string {
	return builder.candidateBase + ".stage"
}

func (builder *GenerationBuilder) buildLockBase() string {
	return builder.candidateBase + ".build.lock"
}

func (builder *GenerationBuilder) acquireBuildLease() (*generationBuildLease, error) {
	_, parentPath, _, root, directoryInfo, err := openNamespace(builder.logicalPath)
	if err != nil {
		return nil, err
	}
	fail := func(cause error, file *os.File, locked bool) (*generationBuildLease, error) {
		if locked {
			cause = errors.Join(cause, storeio.UnlockWriter(file))
		}
		if file != nil {
			cause = errors.Join(cause, file.Close())
		}
		cause = errors.Join(cause, root.Close())
		return nil, cause
	}
	if parentPath != builder.parentPath || builder.directoryInfo == nil ||
		!os.SameFile(builder.directoryInfo, directoryInfo) {
		return fail(ErrNamespaceChanged, nil, false)
	}
	liveSource, err := root.Lstat(builder.base)
	if err != nil || !liveSource.Mode().IsRegular() ||
		!os.SameFile(liveSource, builder.sourceInfo) {
		return fail(fmt.Errorf("%w: generation source leaf changed", ErrNamespaceChanged), nil, false)
	}
	file, err := root.OpenFile(builder.buildLockBase(), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fail(err, nil, false)
	}
	if err := storeio.LockWriter(file); err != nil {
		return fail(errors.Join(ErrLocked, err), file, false)
	}
	entry, statErr := root.Lstat(builder.buildLockBase())
	fileInfo, fileErr := file.Stat()
	if statErr != nil || fileErr != nil || !entry.Mode().IsRegular() ||
		!os.SameFile(entry, fileInfo) || fileInfo.Size() != 0 {
		return fail(errors.Join(ErrNamespaceChanged, statErr, fileErr), file, true)
	}
	return &generationBuildLease{root: root, file: file, locked: true}, nil
}

func (lease *generationBuildLease) Close() error {
	if lease == nil {
		return nil
	}
	var err error
	if lease.locked {
		err = storeio.UnlockWriter(lease.file)
		lease.locked = false
	}
	if lease.file != nil {
		err = errors.Join(err, lease.file.Close())
		lease.file = nil
	}
	if lease.root != nil {
		err = errors.Join(err, lease.root.Close())
		lease.root = nil
	}
	return err
}

func (builder *GenerationBuilder) reclaimAbandonedStage() error {
	_, parentPath, _, root, directoryInfo, err := openNamespace(builder.logicalPath)
	if err != nil {
		return err
	}
	defer root.Close()
	if parentPath != builder.parentPath || builder.directoryInfo == nil ||
		!os.SameFile(builder.directoryInfo, directoryInfo) {
		return ErrNamespaceChanged
	}
	base := builder.stageBase()
	entry, err := root.Lstat(base)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !entry.Mode().IsRegular() {
		return ErrGenerationConflict
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
	fileInfo, err := file.Stat()
	if err != nil || !os.SameFile(entry, fileInfo) {
		return errors.Join(ErrNamespaceChanged, err)
	}
	pinned, err := root.Lstat(base)
	if err != nil || !pinned.Mode().IsRegular() || !os.SameFile(pinned, fileInfo) {
		return errors.Join(ErrNamespaceChanged, err)
	}
	if err := root.Remove(base); err != nil {
		return err
	}
	return syncPinnedDirectory(root)
}

func (builder *GenerationBuilder) createStage() (*Store, error) {
	options := builder.options
	options.random = rand.Reader
	bootstrap := Bootstrap{
		TopologyRecoveryEpoch: builder.header.topologyRecoveryEpoch,
		Snapshot:              builder.input.Snapshot,
	}
	staticHeader, header, err := marshalStaticHeader(
		builder.header.identity, builder.key, bootstrap, options,
	)
	if err != nil {
		return nil, err
	}
	bootstrapPayload, _, err := marshalBootstrap(bootstrap, builder.header.identity.MemberID)
	if err != nil {
		return nil, err
	}
	bootstrapRecord, bootstrapDigest, _, err := marshalRecord(
		recordKindBootstrap, 0, 1, 0, 0, header.headerDigest,
		bootstrapPayload, header, options,
	)
	if err != nil {
		return nil, err
	}
	if len(bootstrapRecord) > MaxSnapshotBaseRecordBytes {
		return nil, fmt.Errorf("%w: snapshot-base record %d exceeds reserved %d",
			ErrBounds, len(bootstrapRecord), MaxSnapshotBaseRecordBytes)
	}
	walEnd := int64(HeaderBytes + len(bootstrapRecord))
	current := initialCurrent(header, walEnd, 1, bootstrapDigest)
	currentBytes, _, err := marshalCurrentSlot(current, 0, header)
	if err != nil {
		return nil, err
	}

	_, parentPath, _, root, directoryInfo, err := openNamespace(builder.logicalPath)
	if err != nil {
		return nil, err
	}
	keepRoot := false
	defer func() {
		if !keepRoot {
			_ = root.Close()
		}
	}()
	if parentPath != builder.parentPath || builder.directoryInfo == nil ||
		!os.SameFile(builder.directoryInfo, directoryInfo) {
		return nil, ErrNamespaceChanged
	}
	liveSource, err := root.Lstat(builder.base)
	if err != nil || !liveSource.Mode().IsRegular() ||
		!os.SameFile(liveSource, builder.sourceInfo) {
		return nil, fmt.Errorf("%w: generation source leaf changed", ErrNamespaceChanged)
	}
	base := builder.stageBase()
	if _, err := root.Lstat(base); err == nil {
		return nil, fmt.Errorf("%w: WAL generation stage already exists", ErrGenerationConflict)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	file, err := root.OpenFile(base, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create WAL generation stage: %w", err)
	}
	createdInfo, err := file.Stat()
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	createdName, err := root.Lstat(base)
	if err != nil || !createdName.Mode().IsRegular() ||
		!os.SameFile(createdName, createdInfo) {
		return nil, errors.Join(ErrNamespaceChanged, err, file.Close())
	}
	locked := false
	cleanup := func(cause error) error {
		if locked {
			cause = errors.Join(cause, storeio.UnlockWriter(file))
			locked = false
		}
		cause = errors.Join(cause, file.Close())
		entry, entryErr := root.Lstat(base)
		switch {
		case errors.Is(entryErr, os.ErrNotExist):
			return cause
		case entryErr != nil:
			return errors.Join(cause, entryErr)
		case !entry.Mode().IsRegular() || !os.SameFile(entry, createdInfo):
			return errors.Join(cause, ErrNamespaceChanged)
		}
		if removeErr := root.Remove(base); removeErr != nil {
			return errors.Join(cause, removeErr)
		}
		return errors.Join(cause, syncPinnedDirectory(root))
	}
	if err := storeio.LockWriter(file); err != nil {
		return nil, cleanup(errors.Join(ErrLocked, err))
	}
	locked = true
	if err := options.ops.preallocate(file, options.maxFileBytes); err != nil {
		return nil, cleanup(persistenceError("preallocate WAL generation stage", false, err))
	}
	if err := writeExactAt(options.ops, file, staticHeader, 0); err != nil {
		return nil, cleanup(persistenceError("write WAL generation static header", false, err))
	}
	if err := writeExactAt(options.ops, file, bootstrapRecord, HeaderBytes); err != nil {
		return nil, cleanup(persistenceError("write WAL generation bootstrap", false, err))
	}
	if err := writeExactAt(options.ops, file, currentBytes, StaticHeaderBytes); err != nil {
		return nil, cleanup(persistenceError("write WAL generation current slot", false, err))
	}
	if err := proveNamedFile(
		root, builder.parentPath, builder.directoryInfo, base, file, options.maxFileBytes,
	); err != nil {
		return nil, cleanup(persistenceError("prove WAL generation stage", false, err))
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, cleanup(err)
	}
	stage := &Store{
		path:       filepath.Join(builder.parentPath, base),
		parentPath: builder.parentPath, base: base,
		root: root, directoryInfo: directoryInfo, file: file, fileInfo: fileInfo,
		locked: true, options: options, header: header, current: current,
		image: bootstrapImage(bootstrap.Snapshot),
	}
	keepRoot = true
	return stage, nil
}
