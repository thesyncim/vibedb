package replicatedstate

import (
	"bytes"
	"errors"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/store/durable"
)

const MaxExecutionPinRecoveryScan = 1024

// ExecutionPinReadResult is one exact hidden-row point read at a coherent
// replicated publication. Record is detached fixed-width state.
type ExecutionPinReadResult struct {
	Fence  SnapshotFence
	Found  bool
	Record executionpin.Record
}

// ExecutionPinRead performs the serving read used to fence gateway side
// effects. The caller supplies a ReadIndex-derived applied floor; a local or
// follower-only observation cannot satisfy this contract.
func (m *Machine) ExecutionPinRead(
	pin executionpin.PinID,
	minimumApplied uint64,
) (ExecutionPinReadResult, error) {
	if m == nil || pin == (executionpin.PinID{}) || minimumApplied == 0 {
		return ExecutionPinReadResult{}, ErrExecutionPinStateCorrupt
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return ExecutionPinReadResult{}, err
	}
	if !m.initialized {
		return ExecutionPinReadResult{}, ErrWrongBinding
	}
	if m.publication.Applied < minimumApplied {
		return ExecutionPinReadResult{}, ErrReadBehind
	}
	snapshot, err := m.executionPinReadCutLocked()
	if err != nil {
		return ExecutionPinReadResult{}, m.fail(err)
	}
	record, found, readErr := executionPinRecordAt(pointSnapshot{value: snapshot}, pin)
	closeErr := m.applyCut.Close()
	if readErr != nil || closeErr != nil {
		return ExecutionPinReadResult{}, m.fail(errors.Join(readErr, closeErr))
	}
	return ExecutionPinReadResult{
		Fence: m.transactionRecoveryFenceLocked(), Found: found, Record: record,
	}, nil
}

// LookupExecutionPin returns one exact durable lifecycle row through an indexed
// point read. Terminal tombstones remain visible for delayed-command fencing.
func (m *Machine) LookupExecutionPin(pin executionpin.PinID) (executionpin.Record, bool, error) {
	if pin == (executionpin.PinID{}) {
		return executionpin.Record{}, false, ErrExecutionPinStateCorrupt
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return executionpin.Record{}, false, err
	}
	if !m.initialized {
		return executionpin.Record{}, false, ErrWrongBinding
	}
	snapshot, err := m.executionPinReadCutLocked()
	if err != nil {
		return executionpin.Record{}, false, m.fail(err)
	}
	record, found, readErr := executionPinRecordAt(pointSnapshot{value: snapshot}, pin)
	closeErr := m.applyCut.Close()
	if readErr != nil || closeErr != nil {
		return executionpin.Record{}, false, m.fail(errors.Join(readErr, closeErr))
	}
	return record, found, nil
}

// ScanActiveExecutionPins returns the next bounded PinID-ordered page for one
// exact ledger-home group. It reads the compact active index and verifies
// every referenced record digest before returning controller-recovery state.
func (m *Machine) ScanActiveExecutionPins(
	group executionpin.ID,
	after executionpin.PinID,
	limit int,
) ([]executionpin.Record, error) {
	if group == (executionpin.ID{}) ||
		limit <= 0 || limit > MaxExecutionPinRecoveryScan {
		return nil, ErrExecutionPinStateCorrupt
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return nil, err
	}
	if !m.initialized {
		return nil, ErrWrongBinding
	}
	snapshot, err := m.executionPinReadCutLocked()
	if err != nil {
		return nil, m.fail(err)
	}
	var prefix [1 + 16]byte
	prefix[0] = executionPinActivePrefix
	copy(prefix[1:17], group[:])
	result := make([]executionpin.Record, 0, min(limit, 16))
	err = snapshot.RangePrefixRaw(prefix[:], func(key, value []byte) error {
		if len(key) != executionPinActiveStorageKeyBytes ||
			len(value) != executionPinActiveValueBytes {
			return ErrExecutionPinStateCorrupt
		}
		var pin executionpin.PinID
		copy(pin[:], key[17:])
		if after != (executionpin.PinID{}) && bytes.Compare(pin[:], after[:]) <= 0 {
			return nil
		}
		record, found, readErr := executionPinRecordAt(pointSnapshot{value: snapshot}, pin)
		if readErr != nil || !found {
			return errors.Join(readErr, ErrExecutionPinStateCorrupt)
		}
		encoded, encodeErr := executionpin.AppendRecord(nil, record)
		if encodeErr != nil || !validateExecutionPinActiveRow(key, value, record, encoded) {
			return errors.Join(encodeErr, ErrExecutionPinStateCorrupt)
		}
		result = append(result, record)
		if len(result) == limit {
			return errStopExecutionPinScan
		}
		return nil
	})
	if errors.Is(err, errStopExecutionPinScan) {
		err = nil
	}
	closeErr := m.applyCut.Close()
	if err != nil || closeErr != nil {
		return nil, m.fail(errors.Join(err, closeErr))
	}
	return result, nil
}

var errStopExecutionPinScan = errors.New("replicatedstate: stop execution-pin scan")

// The caller holds m.mu and closes the entire reusable cut before returning.
// A standalone system snapshot can require materialization beyond the durable
// checkpoint certificate. The database snapshot API delegates that pressure
// to the collection's owning checkpoint group, certifying every member before
// retrying. Only the system relation needs a read lease: ordinary pin lookups
// must not acquire leases or materialize unrelated user/index relations.
func (m *Machine) executionPinReadCutLocked() (*durable.Snapshot, error) {
	members := [1]durable.NamedCollection{{Name: systemCollectionName, Collection: m.system.Collection}}
	if err := durable.SnapshotCollectionsInto(&m.applyCut, members[:]); err != nil {
		return nil, err
	}
	snapshot, ok := m.applyCut.CollectionHandle(m.system.Collection)
	if !ok || snapshot == nil {
		return nil, errors.Join(ErrInconsistentSnapshot, m.applyCut.Close())
	}
	return snapshot, nil
}
