//go:build linux

package storeio

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func openPageFile(path string, mode DirectMode) (*os.File, bool, error) {
	if mode == DirectOff {
		file, err := os.Open(path)
		return file, false, err
	}
	file, err := openDirectReadFile(path, path)
	if err == nil {
		return file, true, nil
	}
	if mode == DirectRequire && directIOUnsupported(err) {
		return nil, false, fmt.Errorf("%w: %w", ErrDirectIOUnsupported, err)
	}
	if !directIOUnsupported(err) {
		return nil, false, fmt.Errorf("open direct Store page file %q: %w", path, err)
	}
	file, fallbackErr := os.Open(path)
	return file, false, fallbackErr
}

func openPageCacheFile(file *os.File, mode DirectMode) (*os.File, bool, error) {
	if mode == DirectOff {
		return file, false, nil
	}
	path := fmt.Sprintf("/proc/self/fd/%d", file.Fd())
	direct, err := openDirectFile(path, unix.O_RDONLY, file.Name()+" (direct reads)")
	if err == nil {
		return direct, true, nil
	}
	if mode == DirectRequire && directDescriptorUnsupported(err) {
		return nil, false, fmt.Errorf("%w: %w", ErrDirectIOUnsupported, err)
	}
	if !directDescriptorUnsupported(err) {
		return nil, false, fmt.Errorf("open direct Store page descriptor: %w", err)
	}
	return file, false, nil
}

func openPageCommitFile(file *os.File, mode DirectMode) (*os.File, bool, error) {
	if mode == DirectOff {
		return file, false, nil
	}
	path := fmt.Sprintf("/proc/self/fd/%d", file.Fd())
	direct, err := openDirectFile(path, unix.O_RDWR, file.Name()+" (direct writes)")
	if err == nil {
		return direct, true, nil
	}
	if mode == DirectRequire && directDescriptorUnsupported(err) {
		return nil, false, fmt.Errorf("%w: %w", ErrDirectIOUnsupported, err)
	}
	if !directDescriptorUnsupported(err) {
		return nil, false, fmt.Errorf("open direct Store page commit descriptor: %w", err)
	}
	return file, false, nil
}

func openDirectReadFile(path, name string) (*os.File, error) {
	return openDirectFile(path, unix.O_RDONLY, name)
}

func openDirectFile(path string, access int, name string) (*os.File, error) {
	fd, err := unix.Open(path, access|unix.O_CLOEXEC|unix.O_DIRECT, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open direct Store page descriptor")
	}
	return file, nil
}

func directDescriptorUnsupported(err error) bool {
	return directIOUnsupported(err) || errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENOSYS) ||
		errors.Is(err, unix.ENODEV) || errors.Is(err, unix.ENXIO)
}

func directIOUnsupported(err error) bool {
	return errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) ||
		errors.Is(err, unix.ENOTTY) || errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES)
}
