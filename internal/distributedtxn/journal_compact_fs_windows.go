//go:build windows

package distributedtxn

import (
	"os"

	"golang.org/x/sys/windows"
)

func installJournalCompaction(
	from, to string,
	candidate, current *os.File,
) (candidateClosed, currentClosed bool, err error) {
	if candidate == nil || current == nil {
		return false, false, ErrJournalClosed
	}
	if err = candidate.Close(); err != nil {
		return true, false, err
	}
	candidateClosed = true
	if err = current.Close(); err != nil {
		return true, true, err
	}
	currentClosed = true
	fromUTF16, err := windows.UTF16PtrFromString(from)
	if err != nil {
		return true, true, err
	}
	toUTF16, err := windows.UTF16PtrFromString(to)
	if err != nil {
		return true, true, err
	}
	err = windows.MoveFileEx(
		fromUTF16, toUTF16,
		windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH,
	)
	return true, true, err
}

// MoveFileEx WRITE_THROUGH is the Windows namespace durability boundary.
// There is no Unix-style directory fsync contract to add afterward.
func syncJournalDirectory(string) error { return nil }
