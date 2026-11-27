//go:build windows

package controllerlease

import "os"

func lockFileExclusive(_ *os.File) error { return nil }
func unlockFile(_ *os.File) error        { return nil }
func syncDir(_ string) error             { return nil }
