package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

// rf3SnapshotGroupPath preserves the singleton layout while making
// every multi-group source artifact namespace a deterministic function of the
// complete recovery and shard incarnation identity. No manifest-provided
// group byte is interpreted as a path component.
func rf3SnapshotGroupPath(root string, group raftmember.GroupKey, multiGroup bool) string {
	if !multiGroup {
		return root
	}
	var canonical [16 + 16 + 8 + 16 + 16]byte
	offset := copy(canonical[:], group.ClusterID[:])
	offset += copy(canonical[offset:], group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(canonical[offset:offset+8], group.TopologyRecoveryEpoch)
	offset += 8
	offset += copy(canonical[offset:], group.ShardIncarnation[:])
	copy(canonical[offset:], group.GroupID[:])
	digest := sha256.Sum256(canonical[:])
	var scope [32]byte
	hex.Encode(scope[:], digest[:16])
	return filepath.Join(root, string(scope[:]))
}

var errRF3SnapshotNamespace = errors.New("vibedb-shard: invalid snapshot repository namespace")

// prepareRF3SnapshotRepository provisions only directory namespaces, never an
// artifact repository. The provider retains exclusive repository ownership.
func prepareRF3SnapshotRepository(dataRoot, repositoryRoot string, group raftmember.GroupKey, multiGroup bool) (path string, resultErr error) {
	if !multiGroup {
		return repositoryRoot, nil
	}
	if !filepath.IsAbs(dataRoot) || filepath.Clean(dataRoot) != dataRoot ||
		!filepath.IsAbs(repositoryRoot) || filepath.Clean(repositoryRoot) != repositoryRoot ||
		group.ClusterID == ([16]byte{}) || group.ClusterIncarnation == ([16]byte{}) || group.TopologyRecoveryEpoch == 0 ||
		group.ShardIncarnation == ([16]byte{}) || group.GroupID == ([16]byte{}) {
		return "", errRF3SnapshotNamespace
	}
	relative, err := filepath.Rel(dataRoot, repositoryRoot)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errRF3SnapshotNamespace
	}
	info, err := os.Lstat(dataRoot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.Join(errRF3SnapshotNamespace, err)
	}
	root, err := os.OpenRoot(dataRoot)
	if err != nil {
		return "", err
	}
	opened := []*os.Root{root}
	defer func() {
		for i := len(opened) - 1; i >= 0; i-- {
			resultErr = errors.Join(resultErr, opened[i].Close())
		}
	}()
	openedInfo, openedErr := root.Stat(".")
	currentInfo, currentErr := os.Lstat(dataRoot)
	if openedErr != nil || currentErr != nil || currentInfo.Mode()&os.ModeSymlink != 0 ||
		!os.SameFile(info, openedInfo) || !os.SameFile(openedInfo, currentInfo) {
		return "", errors.Join(errRF3SnapshotNamespace, openedErr, currentErr)
	}
	path = rf3SnapshotGroupPath(repositoryRoot, group, true)
	parts := strings.Split(relative, string(filepath.Separator))
	parts = append(parts, filepath.Base(path))
	for index, part := range parts {
		// Existing ancestors are accepted, but only the configured repository
		// namespace and its exact group child may be newly provisioned.
		managed := index >= len(parts)-2
		entry, statErr := root.Lstat(part)
		if errors.Is(statErr, os.ErrNotExist) && managed {
			if err := root.Mkdir(part, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
				return "", err
			}
			entry, statErr = root.Lstat(part)
		}
		if statErr != nil || !entry.IsDir() || entry.Mode()&os.ModeSymlink != 0 || managed && entry.Mode().Perm()&0o077 != 0 {
			return "", errors.Join(errRF3SnapshotNamespace, statErr)
		}
		child, err := root.OpenRoot(part)
		if err != nil {
			return "", err
		}
		opened = append(opened, child)
		actual, actualErr := child.Stat(".")
		current, currentErr := root.Lstat(part)
		if actualErr != nil || currentErr != nil || current.Mode()&os.ModeSymlink != 0 ||
			!os.SameFile(entry, actual) || !os.SameFile(actual, current) {
			return "", errors.Join(errRF3SnapshotNamespace, actualErr, currentErr)
		}
		if managed {
			// Sync existing directories too: retry may follow a process exit
			// between mkdir and either directory durability barrier.
			if err := errors.Join(syncRF3SnapshotDirectory(child), syncRF3SnapshotDirectory(root)); err != nil {
				return "", err
			}
		}
		root = child
	}
	return path, nil
}

func syncRF3SnapshotDirectory(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
