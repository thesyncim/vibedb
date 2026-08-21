//go:build !unix

package replicatedstate

import "os"

func replaceSnapshotCursorEntry(*os.Root, string, string) error {
	return ErrSnapshotStage
}

func syncSnapshotCursorRoot(*os.Root) error { return ErrSnapshotStage }
