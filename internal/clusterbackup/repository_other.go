//go:build !unix

package clusterbackup

import "os"

func replaceBackupEntry(*os.Root, string, string) error { return ErrRepository }
func syncBackupRoot(*os.Root) error                     { return ErrRepository }
