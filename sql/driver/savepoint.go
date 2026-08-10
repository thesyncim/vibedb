package driver

import (
	"fmt"
)

// maxSavepointFrames bounds nested SAVEPOINT marks in one transaction. The
// constant is part of the supported surface; exceeding it returns
// ErrTooManySavepoints with nothing changed.
const maxSavepointFrames = 64

type savepointTableMark struct {
	orderLen    int
	stagedBytes int
	// undo records the first post-mark overwrite of each key that already
	// existed in the overlay at the mark. Keys appended after the mark are
	// discarded by truncating order; they need no undo entry. A cloned
	// *txMutation restores the prior overlay value; ROLLBACK TO does not lower
	// high-water admission accounting.
	undo map[string]*txMutation
}

type savepointFrame struct {
	name   string
	tables map[string]*savepointTableMark
}

func (t *tx) savepoint(name string) error {
	if t.done {
		return fmt.Errorf("vibedb: transaction is finished")
	}
	if name == "" {
		return fmt.Errorf("vibedb: SAVEPOINT requires a name")
	}
	// A duplicate appends another frame. Reverse lookup makes it shadow the
	// earlier homonym until RELEASE removes the newer frame.
	if len(t.savepoints) >= maxSavepointFrames {
		return fmt.Errorf(
			"%w: at most %d SAVEPOINT marks per transaction",
			ErrTooManySavepoints, maxSavepointFrames,
		)
	}
	frame := savepointFrame{
		name:   name,
		tables: make(map[string]*savepointTableMark, len(t.tables)),
	}
	for tableName, state := range t.tables {
		frame.tables[tableName] = &savepointTableMark{
			orderLen:    len(state.order),
			stagedBytes: state.stagedBytes,
			undo:        make(map[string]*txMutation),
		}
	}
	t.savepoints = append(t.savepoints, frame)
	return nil
}

// attachTableToSavepoints extends every live mark when Read Committed first
// materializes a dependency after the mark was created. The state is empty at
// installation, so zero is the exact pre-statement overlay watermark for every
// frame. A later ROLLBACK TO therefore removes writes to lazily touched tables
// just as it does for tables materialized before SAVEPOINT.
func (t *tx) attachTableToSavepoints(state *txTable) {
	if state == nil || len(t.savepoints) == 0 {
		return
	}
	for i := range t.savepoints {
		frame := &t.savepoints[i]
		if frame.tables == nil {
			frame.tables = make(map[string]*savepointTableMark)
		}
		if frame.tables[state.name] != nil {
			continue
		}
		frame.tables[state.name] = &savepointTableMark{
			undo: make(map[string]*txMutation),
		}
	}
}

func (t *tx) releaseSavepoint(name string) error {
	if t.done {
		return fmt.Errorf("vibedb: transaction is finished")
	}
	index, found := t.savepointIndex(name)
	if !found {
		return fmt.Errorf("%w: %q", ErrSavepointNotFound, name)
	}
	clear(t.savepoints[index:])
	t.savepoints = t.savepoints[:index]
	return nil
}

func (t *tx) rollbackToSavepoint(name string) error {
	if t.done {
		return fmt.Errorf("vibedb: transaction is finished")
	}
	index, found := t.savepointIndex(name)
	if !found {
		return fmt.Errorf("%w: %q", ErrSavepointNotFound, name)
	}
	frame := t.savepoints[index]
	for tableName, mark := range frame.tables {
		state := t.tables[tableName]
		if state == nil {
			continue
		}
		restoreSavepointTable(state, mark)
	}
	// Keep the named mark; discard every mark established after it.
	clear(t.savepoints[index+1:])
	t.savepoints = t.savepoints[:index+1]
	// Re-seed empty undo logs so subsequent mutations after ROLLBACK TO are
	// captured against the restored mark.
	for tableName, mark := range frame.tables {
		clear(mark.undo)
		if mark.undo == nil {
			mark.undo = make(map[string]*txMutation)
		}
		if state := t.tables[tableName]; state != nil {
			mark.orderLen = len(state.order)
			mark.stagedBytes = state.stagedBytes
		}
	}
	return nil
}

func (t *tx) savepointIndex(name string) (int, bool) {
	for i := len(t.savepoints) - 1; i >= 0; i-- {
		if t.savepoints[i].name == name {
			return i, true
		}
	}
	return -1, false
}

// recordSavepointOverwrite captures the prior pending entry for every live
// mark that has not yet observed this key. Only overwrites of keys already in
// the overlay need undo; keys appended after a mark are removed by truncating
// order on ROLLBACK TO.
func (t *tx) recordSavepointOverwrite(state *txTable, key string, entry *txMutation) {
	if len(t.savepoints) == 0 || state == nil || entry == nil {
		return
	}
	for i := range t.savepoints {
		mark := t.savepoints[i].tables[state.name]
		if mark == nil {
			continue
		}
		if mark.undo == nil {
			mark.undo = make(map[string]*txMutation)
		}
		if _, seen := mark.undo[key]; seen {
			continue
		}
		mark.undo[key] = cloneTxMutation(entry)
	}
}

func restoreSavepointTable(state *txTable, mark *savepointTableMark) {
	if state == nil || mark == nil {
		return
	}
	for i := mark.orderLen; i < len(state.order); i++ {
		delete(state.pending, state.order[i])
		state.order[i] = ""
	}
	if mark.orderLen < len(state.order) {
		state.order = state.order[:mark.orderLen]
	}
	for key, previous := range mark.undo {
		if previous == nil {
			continue
		}
		entry, exists := state.pending[key]
		if !exists {
			continue
		}
		entry.existed = previous.existed
		entry.remove = previous.remove
		entry.conflictRevision = previous.conflictRevision
		if previous.remove {
			entry.document = nil
		} else {
			entry.document = append(entry.document[:0], previous.document...)
		}
	}
	state.stagedBytes = mark.stagedBytes
	// High-water admission fields are intentionally left alone: ROLLBACK TO
	// does not lower high-water accounting.
}

func cloneTxMutation(src *txMutation) *txMutation {
	if src == nil {
		return nil
	}
	dst := &txMutation{
		remove: src.remove, existed: src.existed,
		conflictRevision: src.conflictRevision,
	}
	if len(src.document) != 0 {
		dst.document = append([]byte(nil), src.document...)
	}
	return dst
}
