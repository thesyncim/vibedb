//go:build !unix

package gateway

import "os"

// fsyncDir is a no-op on platforms without directory-fsync semantics, mirroring
// the SQL catalog's platform handling.
func fsyncDir(string) error { return nil }

// fsyncCatalogRoot mirrors fsyncDir on platforms without directory-fsync
// semantics while preserving the pinned namespace API.
func fsyncCatalogRoot(*os.Root) error { return nil }

func catalogDurabilitySupported() bool { return false }

func replaceCatalogEntry(*os.Root, string, string) error {
	return ErrCatalogDurabilityUnsupported
}
