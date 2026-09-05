package storeio

import "os"

// allocateSealedFile separates a fixed file-size limit from a guarantee that
// every future overwrite has private physical backing. Portable sidecars retain
// the same write, sync, checksum and recovery protocol, but disk exhaustion can
// still fail a write (for example when a filesystem must break a COW extent).
// Their distinct, persisted header bit prevents strict readers from treating
// this allocation as a physical-capacity certificate.
func allocateSealedFile(file *os.File, target int64, portable bool) error {
	if !portable {
		return StrictlyAllocateFile(file, target)
	}
	return preallocatePortableFile(file, target)
}

// growPortableFile never rewrites live bytes or shrinks an existing file. In
// particular, repairing allocation by reading and rewriting existing bytes is
// unsafe: a crash during that rewrite could tear previously durable records.
func growPortableFile(file *os.File, target int64) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() >= target {
		return nil
	}
	return file.Truncate(target)
}
