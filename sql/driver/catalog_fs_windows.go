//go:build windows

package driver

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func replaceCatalogFile(from, to string) error {
	return moveFileWriteThrough(from, to, true)
}

func publishNewPath(from, to string) error {
	return moveFileWriteThrough(from, to, false)
}

// createPublishableTableTemp gives the durable collection a handle that permits
// its first namespace publication while the collection remains open. Go's
// ordinary Windows file open shares reads and writes but not deletion; Windows
// treats rename as delete access, so MoveFileEx would otherwise fail every
// first INSERT with a sharing violation.
//
// os.CreateTemp chooses and atomically claims the unique name. The SQL table
// directory is private to the catalog owner; after closing that initial handle,
// reopen the same claimed file with FILE_SHARE_DELETE before durable.Create
// attaches its committer and cache. Non-direct Windows durable collections
// reuse this one handle, so every live reference permits the write-through move.
func createPublishableTableTemp(directory, pattern string) (*os.File, error) {
	claimed, err := os.CreateTemp(directory, pattern)
	if err != nil {
		return nil, err
	}
	name := claimed.Name()
	if err := claimed.Close(); err != nil {
		removeErr := os.Remove(name)
		return nil, errors.Join(err, removeErr)
	}
	path, err := windows.UTF16PtrFromString(name)
	if err != nil {
		_ = os.Remove(name)
		return nil, err
	}
	handle, err := windows.CreateFile(
		path,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		removeErr := os.Remove(name)
		return nil, errors.Join(err, removeErr)
	}
	file := os.NewFile(uintptr(handle), name)
	if file == nil {
		closeErr := windows.CloseHandle(handle)
		removeErr := os.Remove(name)
		return nil, errors.Join(
			errors.New("vibedb: wrap publishable SQL table handle"),
			closeErr, removeErr,
		)
	}
	return file, nil
}

func moveFileWriteThrough(from, to string, replace bool) error {
	fromUTF16, err := windows.UTF16PtrFromString(from)
	if err != nil {
		return err
	}
	toUTF16, err := windows.UTF16PtrFromString(to)
	if err != nil {
		return err
	}
	flags := uint32(windows.MOVEFILE_WRITE_THROUGH)
	if replace {
		flags |= windows.MOVEFILE_REPLACE_EXISTING
	}
	return windows.MoveFileEx(fromUTF16, toUTF16, flags)
}

// Windows does not expose the Unix directory-fsync contract. Catalog renames
// and first publication of table directories/files use MOVEFILE_WRITE_THROUGH
// above. No logical namespace entry is acknowledged from a direct CreateFile.
func syncDirectory(string) error { return nil }
