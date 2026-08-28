package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/systemkey"
)

const (
	executionPinRecordPrefix          = systemkey.ExecutionPinFirst
	executionPinActivePrefix          = systemkey.ExecutionPinFirst + 1
	executionPinRecordStorageKeyBytes = 1 + sha256.Size
	executionPinActiveStorageKeyBytes = 1 + 16 + sha256.Size
	executionPinActiveValueBytes      = sha256.Size
)

var ErrExecutionPinStateCorrupt = errors.New("replicatedstate: corrupt execution-pin state")

type executionPinStateDelta struct {
	records  int64
	active   int64
	resident int64
}

func executionPinRecordStorageKey(pin executionpin.PinID) [executionPinRecordStorageKeyBytes]byte {
	var key [executionPinRecordStorageKeyBytes]byte
	key[0] = executionPinRecordPrefix
	copy(key[1:], pin[:])
	return key
}

func executionPinActiveStorageKey(record executionpin.Record) [executionPinActiveStorageKeyBytes]byte {
	var key [executionPinActiveStorageKeyBytes]byte
	key[0] = executionPinActivePrefix
	copy(key[1:17], record.Binding.LedgerHomeGroup[:])
	copy(key[17:], record.PinID[:])
	return key
}

func executionPinRecordAt(snapshot pointSnapshot, pin executionpin.PinID) (executionpin.Record, bool, error) {
	key := executionPinRecordStorageKey(pin)
	raw, found, err := snapshot.appendRaw(nil, key[:])
	if err != nil || !found {
		return executionpin.Record{}, found, err
	}
	record, err := executionpin.OpenRecord(raw)
	if err != nil || record.PinID != pin {
		return executionpin.Record{}, false, errors.Join(err, ErrExecutionPinStateCorrupt)
	}
	return record, true, nil
}

func (m *Machine) planExecutionPinCommand(
	plan commandPlan,
	command replication.CommandView,
	applied uint64,
	state State,
	systemSnapshot pointSnapshot,
) (commandPlan, error) {
	if command.Kind() != replication.CommandExecutionPin ||
		!replication.IsExecutionPinAuthority(command.AuthorityClass) {
		return commandPlan{}, ErrExecutionPinStateCorrupt
	}
	nested, err := command.OpenExecutionPin()
	if err != nil {
		return commandPlan{}, err
	}
	current, found, err := executionPinRecordAt(systemSnapshot, nested.PinID)
	if err != nil {
		return commandPlan{}, err
	}
	if !found && nested.Operation == executionpin.OperationAcquire &&
		state.ExecutionPinRecordCount >= MaxRetainedExecutionPins {
		plan.resultCode = ResultTargetBound
		return plan, nil
	}
	authority := executionPinAuthorityDigest(command)
	transition := executionpin.Apply(
		current, found, nested, applied, authority,
		executionpin.Digest(LogicalCommandDigest(command)),
	)
	switch transition.Reason {
	case executionpin.ReasonApplied:
		plan.resultCode = ResultApplied
	case executionpin.ReasonConflict:
		plan.resultCode = ResultIndexConflict
		return plan, nil
	case executionpin.ReasonLeaseMismatch, executionpin.ReasonTooEarly,
		executionpin.ReasonTerminal:
		plan.resultCode = ResultIntentBusy
		return plan, nil
	default:
		return commandPlan{}, ErrExecutionPinStateCorrupt
	}
	if !transition.Mutated {
		return plan, nil
	}
	encoded, err := executionpin.AppendRecord(nil, transition.Record)
	if err != nil {
		return commandPlan{}, errors.Join(err, ErrExecutionPinStateCorrupt)
	}
	recordKey := executionPinRecordStorageKey(nested.PinID)
	plan.systemRows = append(plan.systemRows, newTransactionPut(recordKey[:], encoded))
	wasActive := found && current.Status == executionpin.StatusActive
	isActive := transition.Record.Status == executionpin.StatusActive
	const recordResident = int64(executionPinRecordStorageKeyBytes + executionpin.RecordBytes)
	const activeResident = int64(executionPinActiveStorageKeyBytes + executionPinActiveValueBytes)
	if !found {
		plan.executionPinDelta.records = 1
		plan.executionPinDelta.resident = recordResident
	}
	activeKey := executionPinActiveStorageKey(transition.Record)
	switch {
	case isActive:
		digest := sha256.Sum256(encoded)
		plan.systemRows = append(plan.systemRows, newTransactionPut(activeKey[:], digest[:]))
		if !wasActive {
			plan.executionPinDelta.active = 1
			plan.executionPinDelta.resident += activeResident
		}
	case wasActive:
		plan.systemRows = append(plan.systemRows, newTransactionDelete(activeKey[:]))
		plan.executionPinDelta.active = -1
		plan.executionPinDelta.resident -= activeResident
	}
	return plan, nil
}

func executionPinAuthorityDigest(command replication.CommandView) executionpin.Digest {
	digest, ok := replication.ExecutionPinAuthorityDigest(command)
	if !ok {
		return executionpin.Digest{}
	}
	return executionpin.Digest(digest)
}

func applyExecutionPinStateDelta(state *State, delta executionPinStateDelta) error {
	if state == nil {
		return ErrExecutionPinStateCorrupt
	}
	apply := func(value *uint64, change int64) bool {
		if change < 0 {
			magnitude := uint64(-change)
			if *value < magnitude {
				return false
			}
			*value -= magnitude
			return true
		}
		increase := uint64(change)
		if *value > math.MaxUint64-increase {
			return false
		}
		*value += increase
		return true
	}
	if !apply(&state.ExecutionPinRecordCount, delta.records) ||
		!apply(&state.ActiveExecutionPinCount, delta.active) ||
		!apply(&state.ExecutionPinResidentBytes, delta.resident) ||
		!validStateExecutionPinCounters(*state) {
		return ErrExecutionPinStateCorrupt
	}
	return nil
}

func validateExecutionPinActiveRow(key, value []byte, record executionpin.Record, encoded []byte) bool {
	wantKey := executionPinActiveStorageKey(record)
	wantValue := sha256.Sum256(encoded)
	return record.Status == executionpin.StatusActive && bytes.Equal(key, wantKey[:]) &&
		bytes.Equal(value, wantValue[:])
}
