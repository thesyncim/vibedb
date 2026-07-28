//go:build !linux

package storeio

import "os"

// preallocateRecoveryJournal reserves the journal file up front. The Linux
// metadata-journaling concern that mandates fallocate does not apply here:
// setting the file size once with truncate keeps every later positional append
// from extending the file, which is what avoids per-record size-metadata
// commits. On Darwin the F_FULLFSYNC acknowledgement cost dominates any block
// allocation difference regardless.
func preallocateRecoveryJournal(file *os.File, size int64) error {
	return file.Truncate(size)
}
