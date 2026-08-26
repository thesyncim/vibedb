//go:build !unix

package splitcontroller

import "os"

func replaceRuntimeState(*os.Root, string, string) error { return ErrRuntimeStore }
func syncRuntimeRoot(*os.Root) error                     { return ErrRuntimeStore }
