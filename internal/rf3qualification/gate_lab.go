//go:build vibedb_rf3_read_authority_lab

// Package rf3qualification owns the compile-time gate for the experimental
// RF3 read-authority deployment.
package rf3qualification

const (
	ReadAuthorityLabBuildTag = "vibedb_rf3_read_authority_lab"
	ReadAuthorityEnabled     = true
)
