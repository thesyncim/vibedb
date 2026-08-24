package raftstore

import (
	"errors"
	"fmt"
	"os"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// openUnselectedGenerationForTest recovers a candidate without consulting the
// serving family manifest. It exists only in tests that exercise candidate
// construction and damage. Public Open intentionally has no such path.
func openUnselectedGenerationForTest(
	path string,
	expected Identity,
	expectedTopologyRecoveryEpoch uint64,
	key Key,
	options Options,
) (*Store, error) {
	normalized, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	absPath, parentPath, base, root, directoryInfo, err := openNamespace(path)
	if err != nil {
		return nil, err
	}
	keepRoot := false
	defer func() {
		if !keepRoot {
			_ = root.Close()
		}
	}()
	entryInfo, err := root.Lstat(base)
	if err != nil || !entryInfo.Mode().IsRegular() {
		return nil, errors.Join(ErrNamespaceChanged, err)
	}
	file, err := root.OpenFile(base, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	locked := false
	cleanup := func(cause error) (*Store, error) {
		if locked {
			cause = errors.Join(cause, storeio.UnlockWriter(file))
		}
		return nil, errors.Join(cause, file.Close())
	}
	if err := storeio.LockWriter(file); err != nil {
		return cleanup(errors.Join(ErrLocked, err))
	}
	locked = true
	if err := proveNamedFile(
		root, parentPath, directoryInfo, base, file, normalized.maxFileBytes,
	); err != nil {
		return cleanup(err)
	}
	staticBytes := make([]byte, StaticHeaderBytes)
	if _, err := file.ReadAt(staticBytes, 0); err != nil {
		return cleanup(fmt.Errorf("%w: read static header: %v", ErrCorrupt, err))
	}
	header, _, err := unmarshalStaticHeader(staticBytes, expected, key, normalized)
	if err != nil {
		return cleanup(err)
	}
	current, recoveredTorn, err := recoverCurrent(file, header, normalized)
	if err != nil {
		return cleanup(err)
	}
	image, generation, err := recoverRecords(file, &header, current, normalized)
	if err != nil {
		return cleanup(err)
	}
	if !generation.present || header.topologyRecoveryEpoch != expectedTopologyRecoveryEpoch {
		return cleanup(ErrGenerationCandidate)
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return cleanup(err)
	}
	store := &Store{
		path: absPath, logicalPath: absPath, parentPath: parentPath,
		base: base, logicalBase: base, root: root, directoryInfo: directoryInfo,
		file: file, fileInfo: fileInfo, locked: true, options: normalized,
		header: header, current: current, image: image, generation: generation,
		recoveredTornSlot: recoveredTorn,
	}
	keepRoot = true
	return store, nil
}
