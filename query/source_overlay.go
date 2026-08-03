package query

import (
	"fmt"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// snapshotOverlayMerge is the retained merge state [FromSnapshotOverlay] uses
// to refill Exec.overlayDocs. visit and insert are built once and reused so a
// warmed overlay read does not allocate fresh func values on every RunInto —
// Snapshot.Range and FileOverlay.RangeInserts both take functions, and
// reconstructing those each call was the entire steady-state allocation.
type snapshotOverlayMerge struct {
	dst     *store.Segment
	overlay FileOverlay
	cancel  *CancelFlag
	row     int
	err     error
	visit   func(key string, value vibejson.RawValue) bool
	insert  func(document []byte) error
}

func (m *snapshotOverlayMerge) bind(dst *store.Segment, overlay FileOverlay, cancel *CancelFlag) {
	m.dst = dst
	m.overlay = overlay
	m.cancel = cancel
	m.row = 0
	m.err = nil
	if m.visit == nil {
		m.visit = m.apply
	}
	if m.insert == nil {
		m.insert = m.appendInsert
	}
}

func (m *snapshotOverlayMerge) apply(key string, value vibejson.RawValue) bool {
	if err := cancellationCheckpoint(m.cancel, m.row); err != nil {
		m.err = err
		return false
	}
	m.row++
	replacement, present, shadowed := m.overlay.Lookup(byteview.Bytes(key))
	if shadowed {
		if !present {
			return true
		}
		if _, err := m.dst.Append(replacement); err != nil {
			m.err = err
			return false
		}
		return true
	}
	if _, err := m.dst.Append(value.Bytes()); err != nil {
		m.err = err
		return false
	}
	return true
}

func (m *snapshotOverlayMerge) appendInsert(document []byte) error {
	if err := cancellationError(m.cancel); err != nil {
		return err
	}
	_, err := m.dst.Append(document)
	return err
}

// runSnapshotOverlayInto executes the exact merged view of one heap snapshot
// and a bounded staged-write overlay. Replacements shadow their base row,
// deletes suppress it, and inserts follow the base rows — the same composition
// [FromFileOverlay] applies to a durable snapshot.
//
// The merge materializes into Exec.overlayDocs and then runs the ordinary
// Segment path. Retaining that Segment and the Range/insert visitors on the
// Exec is what keeps a warmed overlay read allocation-free.
func (p *plan) runSnapshotOverlayInto(e *Exec, snapshot store.Snapshot, overlay FileOverlay) error {
	if err := e.Workspace.checkCanceled(); err != nil {
		return err
	}
	if overlay == nil {
		return fmt.Errorf("query: FromSnapshotOverlay was given a nil overlay")
	}
	rows := int64(snapshot.Len()) + overlay.LenDelta()
	if rows < 0 {
		return fmt.Errorf("query: FileOverlay LenDelta underflows the base snapshot")
	}
	if err := e.materializeSnapshotOverlay(snapshot, overlay); err != nil {
		return err
	}
	return p.runInto(&e.Result, &e.overlayDocs, &e.Workspace, e.Options.Workers)
}

func (e *Exec) materializeSnapshotOverlay(snapshot store.Snapshot, overlay FileOverlay) error {
	e.overlayDocs.ShapeTapes = true
	e.overlayDocs.Reset()
	e.overlayMerge.bind(&e.overlayDocs, overlay, e.Workspace.cancel)
	snapshot.Range(e.overlayMerge.visit)
	if e.overlayMerge.err != nil {
		return e.overlayMerge.err
	}
	return overlay.RangeInserts(e.overlayMerge.insert)
}
