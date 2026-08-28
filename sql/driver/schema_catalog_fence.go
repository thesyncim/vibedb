package driver

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/store/durable"
)

// This seam is confined to schema publication boundaries, never row traffic.
var replicatedSchemaDirectorySyncHook func(string) error

func syncReplicatedSchemaDirectory(path string) error {
	if replicatedSchemaDirectorySyncHook != nil {
		if err := replicatedSchemaDirectorySyncHook(path); err != nil {
			return errors.Join(durable.ErrCommitOutcomeUnknown, err)
		}
	}
	if err := syncDirectory(path); err != nil {
		return errors.Join(durable.ErrCommitOutcomeUnknown, err)
	}
	return nil
}

// A readable exact retry is not a durability proof: an earlier rename may
// have succeeded while its directory fence failed. Re-fence immutable files
// and their names before acknowledging a retry or selecting their authority.
// This is only used on DDL/recovery boundaries, not normal reads or writes.
func fenceReplicatedSchemaFiles(directory string, names ...string) error {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	for _, name := range names {
		info, err := root.Lstat(name)
		if err != nil || !info.Mode().IsRegular() {
			return errors.Join(ErrReplicatedSchemaCatalogImage, err)
		}
		file, err := root.Open(name)
		if err != nil {
			return err
		}
		opened, err := file.Stat()
		if err != nil || !os.SameFile(info, opened) {
			return errors.Join(ErrReplicatedSchemaCatalogImage, err, file.Close())
		}
		err = errors.Join(file.Sync(), file.Close())
		if err != nil {
			return errors.Join(durable.ErrCommitOutcomeUnknown, err)
		}
	}
	return syncReplicatedSchemaDirectory(directory)
}

func fencePublishedReplicatedSchemaCatalog(path string) error {
	if err := fenceReplicatedSchemaFiles(path+".tables", replicatedSchemaActivationName,
		replicatedSchemaStageMarkerName, replicatedSchemaTargetCatalogName); err != nil {
		return err
	}
	return fenceReplicatedSchemaFiles(filepath.Dir(path), filepath.Base(path))
}
