//go:build unix

package gateway

import (
	"errors"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func openGatewayIssuerLock(path string) (*os.File, error) {
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		return nil, statErr
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if created {
		directory, openErr := os.Open(filepath.Dir(path))
		if openErr != nil {
			return nil, errors.Join(openErr, file.Close())
		}
		if syncErr := errors.Join(directory.Sync(), directory.Close()); syncErr != nil {
			return nil, errors.Join(syncErr, file.Close())
		}
	}
	if err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, ErrGatewayIssuerStoreInUse
		}
		return nil, err
	}
	return file, nil
}

func closeGatewayIssuerLock(file *os.File) error {
	if file == nil {
		return nil
	}
	return errors.Join(unix.Flock(int(file.Fd()), unix.LOCK_UN), file.Close())
}
