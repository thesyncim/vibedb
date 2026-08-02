//go:build !unix

package gateway

// fsyncDir is a no-op on platforms without directory-fsync semantics, mirroring
// the SQL catalog's platform handling.
func fsyncDir(string) error { return nil }
