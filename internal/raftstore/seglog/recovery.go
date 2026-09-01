package seglog

import (
	"fmt"
	"os"
	"path/filepath"
)

// reconcileActive opens the single manifest-selected writable segment. Frozen
// segments have explicit manifest state and are recovered by Engine.rebuild;
// there is no alternate sealed format or rename-state reader.
func (l *Log) reconcileRotation() error {
	activePath := filepath.Join(l.dir, activeName(l.manifest.ActiveID))
	f, err := os.OpenFile(activePath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("%w: selected active: %v", ErrCorrupt, err)
	}
	l.active = f
	return nil
}

func (l *Log) expectedPreviousID() uint64 {
	if n := len(l.manifest.Segments); n != 0 {
		return l.manifest.Segments[n-1].ID
	}
	return l.manifest.AnchorID
}

func (l *Log) expectedPreviousHash() [32]byte {
	if n := len(l.manifest.Segments); n != 0 {
		return l.manifest.Segments[n-1].Hash
	}
	return l.manifest.AnchorHash
}
