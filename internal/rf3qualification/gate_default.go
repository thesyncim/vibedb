//go:build !vibedb_rf3_read_authority_lab

// Package rf3qualification owns the compile-time gate for the experimental
// RF3 read-authority deployment. Standard binaries deliberately cannot admit
// an enabled read-authority manifest.
package rf3qualification

const (
	ReadAuthorityLabBuildTag = "vibedb_rf3_read_authority_lab"
	ReadAuthorityEnabled     = false
)
