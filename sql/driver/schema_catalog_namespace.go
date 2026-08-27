package driver

import (
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	replicatedSchemaTargetsDirectory = ".schema-targets"
	replicatedSchemaSourcesDirectory = ".schema-sources"
)

// replicatedSchemaNamespaceFaultHook is a test-only crash seam. Every failure
// after a namespace change is outcome-unknown and prevents opening an engine.
var replicatedSchemaNamespaceFaultHook func(stage string) error

// ReplicatedSchemaTargetDirectory returns the private, non-serving directory
// in which a backfill producer must build fresh relation images. The serving
// checkpoint namespace remains unchanged until the target catalog is durable.
// CertifyReplicatedSchemaTarget authenticates every image before preparation.
func (a *ReplicatedApply) ReplicatedSchemaTargetDirectory() (string, error) {
	if a == nil || a.database == nil {
		return "", ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return "", err
	}
	root, err := os.OpenRoot(a.database.dataDir)
	if err != nil {
		return "", err
	}
	defer root.Close()
	if err := ensureSchemaNamespaceDirectory(root, replicatedSchemaTargetsDirectory); err != nil {
		return "", err
	}
	return filepath.Join(a.database.dataDir, replicatedSchemaTargetsDirectory), nil
}

func ensureSchemaNamespaceDirectory(root *os.Root, name string) error {
	if err := root.Mkdir(name, 0o700); err != nil && !os.IsExist(err) {
		return err
	}
	info, err := root.Lstat(name)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(ErrReplicatedSchemaCatalogImage, err)
	}
	return syncSchemaNamespaceDirectory(root, ".")
}

func syncSchemaNamespaceDirectory(root *os.Root, name string) error {
	file, err := root.Open(name)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

type schemaNamespaceMove struct {
	from, to string
	info     os.FileInfo
}

// activateReplicatedSchemaNamespace is called only after the existing durable
// catalog and schema transition select marker's exact target. It does not
// select membership: the certificate-authoritative opener still does that.
// Both primary and journal names are moved without replacement. The temporary
// same-inode double name is an idempotent link/unlink cut, not another image.
func activateReplicatedSchemaNamespace(
	dataDir string, marker replicatedSchemaStageMarker, target ReplicatedShardStoreIdentity,
) (resultErr error) {
	targets, err := schemaStageStorageIDs(target)
	if err != nil || len(marker.sourceStorages) == 0 || !slices.Equal(targets, marker.storages) {
		return errors.Join(ErrReplicatedSchemaCatalogImage, err)
	}
	seen := make(map[[32]byte]struct{}, len(targets)+len(marker.sourceStorages))
	for _, list := range [][][32]byte{targets, marker.sourceStorages} {
		for _, id := range list {
			if _, duplicate := seen[id]; duplicate || id == ([32]byte{}) {
				return ErrReplicatedSchemaCatalogImage
			}
			seen[id] = struct{}{}
		}
	}
	root, err := os.OpenRoot(dataDir)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	for _, directory := range []string{replicatedSchemaTargetsDirectory, replicatedSchemaSourcesDirectory} {
		if err := ensureSchemaNamespaceDirectory(root, directory); err != nil {
			return err
		}
	}
	moves := make([]schemaNamespaceMove, 0, 2*(len(targets)+len(marker.sourceStorages)))
	// Resolve the complete target set first. A missing source is allowed only
	// after every target name was promoted, when authorized drain may have run.
	promoted := true
	for _, id := range targets {
		base := hex.EncodeToString(id[:]) + ".vjc"
		for _, name := range []string{base + ".rjournal", base} {
			move, fromFound, toFound, err := inspectSchemaNamespaceMove(root,
				filepath.Join(replicatedSchemaTargetsDirectory, name), name)
			if err != nil || !fromFound && !toFound {
				return errors.Join(ErrReplicatedSchemaCatalogImage, err)
			}
			promoted = promoted && !fromFound && toFound
			moves = append(moves, move)
		}
	}
	targetMoves := len(moves)
	for _, id := range marker.sourceStorages {
		base := hex.EncodeToString(id[:]) + ".vjc"
		for _, name := range []string{base + ".rjournal", base} {
			move, fromFound, toFound, err := inspectSchemaNamespaceMove(root,
				name, filepath.Join(replicatedSchemaSourcesDirectory, name))
			if err != nil || !fromFound && !toFound && !promoted {
				return errors.Join(ErrReplicatedSchemaCatalogImage, err)
			}
			if fromFound || toFound {
				moves = append(moves, move)
			}
		}
	}
	for i := range moves {
		for j := 0; j < i; j++ {
			if os.SameFile(moves[i].info, moves[j].info) {
				return ErrReplicatedSchemaCatalogImage
			}
		}
	}
	// Acquire the actual writer leases before moving any file. An old engine
	// or snapshot-pinned collection must be fully closed, not merely fenced.
	// Keep the leases through all moves so no descriptor-derived journal path
	// can resolve to a different generation during publication.
	locked := make([]*os.File, 0, len(targets)+len(marker.sourceStorages))
	defer func() {
		for i := len(locked) - 1; i >= 0; i-- {
			resultErr = errors.Join(resultErr, storeio.UnlockWriter(locked[i]), locked[i].Close())
		}
	}()
	for _, move := range moves {
		if strings.HasSuffix(move.from, ".rjournal") {
			continue
		}
		name := move.from
		if _, err := root.Lstat(name); os.IsNotExist(err) {
			name = move.to
		}
		file, err := root.OpenFile(name, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		info, err := file.Stat()
		if err != nil || !os.SameFile(info, move.info) {
			return errors.Join(ErrReplicatedSchemaCatalogImage, err, file.Close())
		}
		if err := storeio.LockWriter(file); err != nil {
			return errors.Join(err, file.Close())
		}
		locked = append(locked, file)
	}
	// Archive first; only then introduce target files into the fixed namespace.
	for _, interval := range [][]schemaNamespaceMove{moves[targetMoves:], moves[:targetMoves]} {
		for _, move := range interval {
			if err := moveSchemaNamespaceFile(root, move); err != nil {
				return err
			}
		}
	}
	return nil
}

func inspectSchemaNamespaceMove(root *os.Root, from, to string) (schemaNamespaceMove, bool, bool, error) {
	source, sourceErr := root.Lstat(from)
	target, targetErr := root.Lstat(to)
	if sourceErr != nil && !os.IsNotExist(sourceErr) || targetErr != nil && !os.IsNotExist(targetErr) {
		return schemaNamespaceMove{}, false, false, errors.Join(sourceErr, targetErr)
	}
	sourceFound, targetFound := sourceErr == nil, targetErr == nil
	if sourceFound && !source.Mode().IsRegular() || targetFound && !target.Mode().IsRegular() ||
		sourceFound && targetFound && !os.SameFile(source, target) {
		return schemaNamespaceMove{}, false, false, ErrReplicatedSchemaCatalogImage
	}
	info := source
	if !sourceFound {
		info = target
	}
	return schemaNamespaceMove{from: from, to: to, info: info}, sourceFound, targetFound, nil
}

func moveSchemaNamespaceFile(root *os.Root, move schemaNamespaceMove) error {
	current, sourceFound, targetFound, err := inspectSchemaNamespaceMove(root, move.from, move.to)
	if err != nil || current.info == nil || !os.SameFile(current.info, move.info) {
		return errors.Join(ErrReplicatedSchemaCatalogImage, err)
	}
	if !targetFound {
		if err := root.Link(move.from, move.to); err != nil {
			return err // Link cannot overwrite another generation.
		}
		if replicatedSchemaNamespaceFaultHook != nil {
			if err := replicatedSchemaNamespaceFaultHook("linked"); err != nil {
				return errors.Join(durable.ErrCommitOutcomeUnknown, err)
			}
		}
	}
	// Fence even an already-linked retry before deleting its source name.
	if err := syncSchemaNamespaceDirectory(root, filepath.Dir(move.to)); err != nil {
		return errors.Join(durable.ErrCommitOutcomeUnknown, err)
	}
	if replicatedSchemaNamespaceFaultHook != nil {
		if err := replicatedSchemaNamespaceFaultHook("destination_fenced"); err != nil {
			return errors.Join(durable.ErrCommitOutcomeUnknown, err)
		}
	}
	if sourceFound {
		if err := root.Remove(move.from); err != nil {
			return errors.Join(durable.ErrCommitOutcomeUnknown, err)
		}
		if replicatedSchemaNamespaceFaultHook != nil {
			if err := replicatedSchemaNamespaceFaultHook("unlinked"); err != nil {
				return errors.Join(durable.ErrCommitOutcomeUnknown, err)
			}
		}
	}
	if err := syncSchemaNamespaceDirectory(root, filepath.Dir(move.from)); err != nil {
		return errors.Join(durable.ErrCommitOutcomeUnknown, err)
	}
	if replicatedSchemaNamespaceFaultHook != nil {
		if err := replicatedSchemaNamespaceFaultHook("source_fenced"); err != nil {
			return errors.Join(durable.ErrCommitOutcomeUnknown, err)
		}
	}
	return nil
}
