package seglog

import (
	"fmt"
	"os"
)

// reconcileActive opens the single state-selected writable segment. Frozen
// segments have explicit state state and are recovered by Engine.rebuild;
// there is no alternate sealed format or rename-state reader.
func (l *Log) reconcileRotation() error {
	activePath := segmentPath(l.dir, l.state.ActiveFileID)
	f, err := os.OpenFile(activePath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("%w: selected active: %v", ErrCorrupt, err)
	}
	l.active = f
	return nil
}

func (l *Log) expectedPreviousID() uint64 {
	if n := len(l.state.Segments); n != 0 {
		return l.state.Segments[n-1].ID
	}
	return l.state.AnchorID
}

func (l *Log) expectedPreviousHash() [32]byte {
	if n := len(l.state.Segments); n != 0 {
		return l.state.Segments[n-1].Hash
	}
	return l.state.AnchorHash
}
