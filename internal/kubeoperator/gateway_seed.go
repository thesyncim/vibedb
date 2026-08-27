package kubeoperator

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/gateway"
)

// PrepareGatewaySeed pins a read-only projected ConfigMap catalog as a regular
// immutable file on the gateway PVC. Kubernetes projection symlinks are allowed
// only at the input; serving keeps the catalog's no-alias disk contract.
func PrepareGatewaySeed(source, target string) error {
	if !filepath.IsAbs(source) || filepath.Clean(source) != source ||
		!filepath.IsAbs(target) || filepath.Clean(target) != target || source == target {
		return ErrBootstrap
	}
	seed, err := gateway.LoadSnapshot(source)
	if err != nil || seed.Generation() != 1 {
		return errors.Join(ErrBootstrap, err)
	}
	want, err := gateway.AppendSnapshotDocument(nil, seed)
	if err != nil {
		return err
	}
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		// Reuse the catalog's locked, conditional, fsynced atomic publication.
		// A crash after publication resumes by comparing the exact pinned seed.
		if err = gateway.SaveSnapshotAfter(target, 0, seed); err != nil {
			return err
		}
		info, err = os.Lstat(target)
	}
	if err != nil || !info.Mode().IsRegular() {
		return errors.Join(ErrBootstrap, err)
	}
	sourceInfo, err := os.Stat(source)
	if err != nil || os.SameFile(info, sourceInfo) {
		return errors.Join(ErrBootstrap, err)
	}
	pinned, err := gateway.LoadSnapshot(target)
	if err != nil {
		return err
	}
	got, err := gateway.AppendSnapshotDocument(nil, pinned)
	if err != nil || !bytes.Equal(want, got) {
		return errors.Join(ErrBootstrap, err)
	}
	return nil
}
