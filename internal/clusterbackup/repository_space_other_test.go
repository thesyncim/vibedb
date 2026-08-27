//go:build !unix

package clusterbackup

func repositoryAllocatedBytes(string) (uint64, bool, error) { return 0, false, nil }
