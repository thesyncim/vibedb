package replicatedstate

import (
	"bytes"
	"errors"

	"github.com/thesyncim/vibedb/internal/replication"
)

// TopologySession returns the exact header and latest slot from this coherent
// local cut. It cannot open a session or confer serving authority. Controllers
// use it only to retire their own completed operation after leader replacement;
// ordinary client sessions are deliberately outside this interface.
func (s *ReadSnapshot) TopologySession(tenant []byte, client replication.ID128) (SessionView, SessionSlotView, bool, error) {
	if s == nil || len(tenant) == 0 || client == (replication.ID128{}) {
		return SessionView{}, SessionSlotView{}, false, ErrInconsistentSnapshot
	}
	system, ok := s.cut.Collection(systemCollectionName)
	if !ok || system == nil {
		return SessionView{}, SessionSlotView{}, false, ErrInconsistentSnapshot
	}
	digest := SessionKey(replication.CommandAuthorityTopology, tenant, client)
	header, found, err := sessionAt(pointSnapshot{value: system}, SessionStorageKey(digest), nil)
	if err != nil || !found {
		return SessionView{}, SessionSlotView{}, false, err
	}
	if header.Digest != digest || header.AuthorityClass != replication.CommandAuthorityTopology ||
		header.ClientID != client || !bytes.Equal(header.Tenant, tenant) || header.ClientEpoch > s.state.SessionEpochHighWater {
		return SessionView{}, SessionSlotView{}, false, ErrSessionCorrupt
	}
	slotKey, err := SessionSlotStorageKey(digest, uint16((header.HighSequence-1)%uint64(header.RetryWindow)))
	if err != nil {
		return SessionView{}, SessionSlotView{}, false, err
	}
	slot, found, err := sessionSlotAt(pointSnapshot{value: system}, slotKey, nil)
	if err != nil || !found || slot.ClientSequence != header.HighSequence {
		return SessionView{}, SessionSlotView{}, false, errors.Join(err, ErrSessionCorrupt)
	}
	if err = validateSessionSlotAgainstHeader(header, slot); err != nil {
		return SessionView{}, SessionSlotView{}, false, err
	}
	if err = validateStoredSessionSlot(s.state, slot, sessionFenceLookup{snapshot: pointSnapshot{value: system}}); err != nil {
		return SessionView{}, SessionSlotView{}, false, err
	}
	return header, slot, true, nil
}

// SplitCaptureActivation reads the retained activation witness from the same
// cut as the session header. An absent witness never authorizes cleanup.
func (s *ReadSnapshot) SplitCaptureActivation() (SplitCaptureActivation, bool, error) {
	if s == nil {
		return SplitCaptureActivation{}, false, ErrInconsistentSnapshot
	}
	system, ok := s.cut.Collection(systemCollectionName)
	if !ok || system == nil {
		return SplitCaptureActivation{}, false, ErrInconsistentSnapshot
	}
	raw, found, err := system.AppendRaw(nil, splitCaptureActivationKey[:])
	if err != nil || !found {
		return SplitCaptureActivation{}, false, err
	}
	activation, err := openSplitCaptureActivation(raw)
	if err != nil || activation.Applied > s.state.Applied {
		return SplitCaptureActivation{}, false, errors.Join(err, ErrInconsistentSnapshot)
	}
	return activation, true, nil
}
