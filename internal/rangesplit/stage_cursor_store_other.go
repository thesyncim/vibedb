//go:build !unix

package rangesplit

import "os"

func replaceChildStageCursorEntry(*os.Root, string, string) error {
	return ErrChildStage
}

func syncChildStageCursorRoot(*os.Root) error { return ErrChildStage }
