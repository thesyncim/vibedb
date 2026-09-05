package gatewayruntime

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// The journal base anchors the catalog session, PostgreSQL issuer reservations,
// fallback outboxes and execution pins. Their files can be atomically replaced
// during recovery, so lifetime ownership uses a separate stable lock inode.
func openGatewayJournalLock(base string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(base), 0700); err != nil {
		return nil, err
	}
	path := base + ".gateway.lock"
	if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: gateway journal lock must be a regular file", ErrInvalidConfig)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	opened, statErr := file.Stat()
	named, nameErr := os.Lstat(path)
	if err = errors.Join(statErr, nameErr); err != nil || !opened.Mode().IsRegular() ||
		!named.Mode().IsRegular() || !os.SameFile(opened, named) {
		return nil, errors.Join(ErrInvalidConfig, err, file.Close())
	}
	if err := storeio.LockWriter(file); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return file, nil
}
