package replicatedstate

import (
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/store/durable"
)

var errCaptureReclaimBatchFull = errors.New("capture reclamation batch complete")

// ReclaimRetiredTransitionCapture removes one bounded batch from the reserved
// replica-local capture collection. The caller must first authenticate completed
// schema lineage and the release of old-generation readers. The header digest
// fences this batch against a different capture; it is deleted only when alone.
// Raft state, durable sessions, user rows and storage identities are unchanged.
func (m *Machine) ReclaimRetiredTransitionCapture(header [32]byte, sourceSchema uint64, limit int) (bool, error) {
	if m == nil || header == ([32]byte{}) || sourceSchema == 0 || limit < 1 || limit > 1024 {
		return false, ErrTransitionCapture
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return false, err
	}
	target := m.reservedCaptureTarget
	if !m.initialized || m.capture != nil || target.Collection == nil || sourceSchema >= m.binding.SchemaGeneration {
		return false, ErrTransitionCapture
	}
	var zero [8]byte
	raw, found, err := target.Collection.AppendRaw(nil, zero[:])
	if err != nil || !found || sha256.Sum256(raw) != header {
		return false, errors.Join(err, ErrTransitionCapture)
	}
	limit = min(limit, target.Collection.MaxBatchDocuments(), m.options.TxnLimits.MaxDocuments,
		target.Collection.MaxBatchBytes()/len(zero))
	// Deletes stage only their keys. Clamp before converting the byte budget
	// so even a very large configured int64 budget is safe on 32-bit hosts.
	if m.options.TxnLimits.MaxBytes/int64(len(zero)) < int64(limit) {
		limit = int(m.options.TxnLimits.MaxBytes / int64(len(zero)))
	}
	if limit < 1 {
		return false, ErrTransitionCapture
	}
	keys := make([][8]byte, 0, limit)
	if target.Collection.Len() == 1 {
		keys = append(keys, zero)
	} else {
		// An ephemeral current-generation scan reads pending deletes directly;
		// a leased Snapshot would force their physical materialization first.
		scan := func() error {
			return target.Collection.RangeRawCurrent(func(key, _ []byte) error {
				if len(key) != len(zero) {
					return ErrTransitionCapture
				}
				if [8]byte(key) == zero {
					return nil
				}
				keys = append(keys, [8]byte(key))
				if len(keys) == limit {
					return errCaptureReclaimBatchFull
				}
				return nil
			})
		}
		err = scan()
		// A pending structural router can still require a physical fold. Only
		// the owning group may certify that cut, then retry this bounded scan.
		if errors.Is(err, durable.ErrCheckpointGroupPressure) && m.checkpointGroup != nil {
			if err = m.checkpointGroup.Checkpoint(); err != nil {
				m.poison = err
				return false, err
			}
			keys = keys[:0]
			err = scan()
		}
		if errors.Is(err, errCaptureReclaimBatchFull) {
			err = nil
		}
		if err != nil {
			return false, err
		}
	}
	if len(keys) == 0 {
		return false, ErrTransitionCapture
	}
	write := func(batch *durable.DatabaseBatch) error {
		collection, err := batch.CollectionHandle(target.Collection)
		if err != nil {
			return err
		}
		for i := range keys {
			if err := collection.Delete(keys[i][:]); err != nil {
				return err
			}
		}
		return nil
	}
	members := []durable.NamedCollection{{Name: target.Name, Collection: target.Collection, BatchDocumentsHint: len(keys)}}
	if m.checkpointGroup != nil {
		err = m.checkpointGroup.Update(m.state.Applied, members, m.options.TxnLimits, write)
	} else {
		err = durable.UpdateCollections(m.txnLog, members, m.options.TxnLimits, write)
	}
	if err != nil {
		m.poison = err
		return false, err
	}
	done := target.Collection.Len() == 0
	if done && m.checkpointGroup != nil {
		// Group Update publishes an unsynced suffix. Before the installer may
		// durably record cleanup completion (or the journal may be removed),
		// certify header absence so a crash cannot resurrect a retired stream.
		if err := m.checkpointGroup.Checkpoint(); err != nil {
			m.poison = err
			return false, err
		}
	}
	return done, nil
}
