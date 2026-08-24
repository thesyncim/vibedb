//go:build !unix

package snapshottransfer

import "os"

func replaceRepositoryEntry(*os.Root, string, string) error { return ErrRepository }
func syncRoot(*os.Root) error                               { return ErrRepository }
