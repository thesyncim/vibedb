package executionpin

import "math"

// Reason is the closed semantic result mapped by the replicated-state adapter
// to stable result codes. Rejections are deterministic outcomes, not errors.
type Reason uint8

const (
	ReasonInvalid Reason = iota
	ReasonApplied
	ReasonConflict
	ReasonLeaseMismatch
	ReasonTooEarly
	ReasonTerminal
)

type Transition struct {
	Reason  Reason
	Mutated bool
	Record  Record
	Found   bool
}

// Apply derives one transition from the current row. applied, authority, and
// commandDigest are ordered outer-Raft witnesses supplied by the adapter.
func Apply(
	current Record,
	found bool,
	command Command,
	applied uint64,
	authority Digest,
	commandDigest Digest,
) Transition {
	if !command.Valid() || applied == 0 || authority == (Digest{}) ||
		commandDigest == (Digest{}) || found && !current.Valid() {
		return Transition{Reason: ReasonInvalid}
	}
	if found && current.LastOperation == command.Operation &&
		current.LastCommandDigest == commandDigest {
		return Transition{Reason: ReasonApplied, Record: current, Found: true}
	}
	if found && (current.PinID != command.PinID || current.Binding != command.Binding) {
		return Transition{Reason: ReasonConflict, Record: current, Found: true}
	}
	switch command.Operation {
	case OperationAcquire:
		leaseThrough, ok := leaseAppliedThrough(applied, command.NextLeaseSpan)
		if !ok {
			return Transition{Reason: ReasonConflict, Record: current, Found: found}
		}
		if found {
			if current.Status != StatusActive || current.Controller != command.NextController ||
				current.ControllerEpoch != command.NextControllerEpoch ||
				current.LeaseAppliedThrough != leaseThrough {
				return Transition{Reason: ReasonConflict, Record: current, Found: true}
			}
			return Transition{Reason: ReasonApplied, Record: current, Found: true}
		}
		record := Record{
			Status: StatusActive, LastOperation: OperationAcquire,
			PinID: command.PinID, Binding: command.Binding,
			AcquireAuthorityDigest: authority, CurrentAuthorityDigest: authority,
			AcquireApplied: applied, AcquireController: command.NextController,
			AcquireControllerEpoch:     command.NextControllerEpoch,
			AcquireLeaseAppliedThrough: leaseThrough,
			Controller:                 command.NextController, ControllerEpoch: command.NextControllerEpoch,
			LeaseAppliedThrough: leaseThrough, LeaseRevision: 1, LeaseApplied: applied,
			LastCommandDigest: commandDigest, LastApplied: applied,
		}
		return Transition{Reason: ReasonApplied, Mutated: true, Record: record, Found: true}

	case OperationRenew:
		if !found {
			return Transition{Reason: ReasonConflict}
		}
		if current.Status != StatusActive {
			return Transition{Reason: ReasonTerminal, Record: current, Found: true}
		}
		if !matchesAcquireCertificate(current, command.AcquireCertificateDigest) {
			return Transition{Reason: ReasonConflict, Record: current, Found: true}
		}
		if !matchesExpectedLease(current, command) {
			return Transition{Reason: ReasonLeaseMismatch, Record: current, Found: true}
		}
		// Renewal is itself an ordered side effect and must land while the
		// current certificate is still live. A delayed controller cannot
		// resurrect its authority after the logical fence has passed.
		if applied > current.LeaseAppliedThrough {
			return Transition{Reason: ReasonTooEarly, Record: current, Found: true}
		}
		if current.LeaseRevision == math.MaxUint64 {
			return Transition{Reason: ReasonConflict, Record: current, Found: true}
		}
		leaseThrough, ok := leaseAppliedThrough(applied, command.NextLeaseSpan)
		if !ok {
			return Transition{Reason: ReasonConflict, Record: current, Found: true}
		}
		current.CurrentAuthorityDigest = authority
		current.LeaseAppliedThrough = leaseThrough
		current.LeaseRevision++
		current.LeaseApplied = applied
		current.LastOperation = OperationRenew
		current.LastCommandDigest, current.LastApplied = commandDigest, applied
		return Transition{Reason: ReasonApplied, Mutated: true, Record: current, Found: true}

	case OperationRecover:
		if !found {
			return Transition{Reason: ReasonConflict}
		}
		if current.Status != StatusActive {
			return Transition{Reason: ReasonTerminal, Record: current, Found: true}
		}
		if !matchesAcquireCertificate(current, command.AcquireCertificateDigest) {
			return Transition{Reason: ReasonConflict, Record: current, Found: true}
		}
		if !matchesExpectedLease(current, command) {
			return Transition{Reason: ReasonLeaseMismatch, Record: current, Found: true}
		}
		if applied <= current.LeaseAppliedThrough {
			return Transition{Reason: ReasonTooEarly, Record: current, Found: true}
		}
		if current.LeaseRevision == math.MaxUint64 {
			return Transition{Reason: ReasonConflict, Record: current, Found: true}
		}
		leaseThrough, ok := leaseAppliedThrough(applied, command.NextLeaseSpan)
		if !ok {
			return Transition{Reason: ReasonConflict, Record: current, Found: true}
		}
		current.CurrentAuthorityDigest = authority
		current.Controller = command.NextController
		current.ControllerEpoch = command.NextControllerEpoch
		current.LeaseAppliedThrough = leaseThrough
		current.LeaseRevision++
		current.LeaseApplied = applied
		current.LastOperation = OperationRecover
		current.LastCommandDigest, current.LastApplied = commandDigest, applied
		return Transition{Reason: ReasonApplied, Mutated: true, Record: current, Found: true}

	case OperationRelease:
		if !found {
			return Transition{Reason: ReasonConflict}
		}
		if current.Status == StatusReleased {
			if current.PrepareTerminalDigest == command.PrepareTerminalDigest &&
				matchesAcquireCertificate(current, command.AcquireCertificateDigest) {
				return Transition{Reason: ReasonApplied, Record: current, Found: true}
			}
			return Transition{Reason: ReasonConflict, Record: current, Found: true}
		}
		if current.Status != StatusActive {
			return Transition{Reason: ReasonTerminal, Record: current, Found: true}
		}
		if !matchesAcquireCertificate(current, command.AcquireCertificateDigest) {
			return Transition{Reason: ReasonConflict, Record: current, Found: true}
		}
		if !matchesExpectedLease(current, command) {
			return Transition{Reason: ReasonLeaseMismatch, Record: current, Found: true}
		}
		current.Status = StatusReleased
		current.TerminalAuthorityDigest = authority
		current.TerminalApplied = applied
		current.PrepareTerminalDigest = command.PrepareTerminalDigest
		current.LastOperation = OperationRelease
		current.LastCommandDigest, current.LastApplied = commandDigest, applied
		return Transition{Reason: ReasonApplied, Mutated: true, Record: current, Found: true}

	case OperationExpire:
		if !found {
			record := Record{
				Status: StatusExpired, LastOperation: OperationExpire,
				PinID: command.PinID, Binding: command.Binding,
				AcquireAuthorityDigest: authority, CurrentAuthorityDigest: authority,
				TerminalAuthorityDigest: authority,
				Controller:              command.ExpectedController,
				ControllerEpoch:         command.ExpectedControllerEpoch,
				LeaseAppliedThrough:     command.ExpectedLeaseAppliedThrough,
				TerminalApplied:         applied,
				LastCommandDigest:       commandDigest, LastApplied: applied,
			}
			return Transition{Reason: ReasonApplied, Mutated: true, Record: record, Found: true}
		}
		if current.Status == StatusExpired {
			if current.Controller == command.ExpectedController &&
				current.ControllerEpoch == command.ExpectedControllerEpoch &&
				current.LeaseAppliedThrough == command.ExpectedLeaseAppliedThrough &&
				current.TerminalApplied == applied {
				return Transition{Reason: ReasonApplied, Record: current, Found: true}
			}
			return Transition{Reason: ReasonConflict, Record: current, Found: true}
		}
		if current.Status != StatusActive {
			return Transition{Reason: ReasonTerminal, Record: current, Found: true}
		}
		if !matchesExpectedLease(current, command) {
			return Transition{Reason: ReasonLeaseMismatch, Record: current, Found: true}
		}
		if applied <= current.LeaseAppliedThrough {
			return Transition{Reason: ReasonTooEarly, Record: current, Found: true}
		}
		current.Status = StatusExpired
		current.TerminalAuthorityDigest = authority
		current.TerminalApplied = applied
		current.LastOperation = OperationExpire
		current.LastCommandDigest, current.LastApplied = commandDigest, applied
		return Transition{Reason: ReasonApplied, Mutated: true, Record: current, Found: true}
	default:
		return Transition{Reason: ReasonInvalid}
	}
}

func matchesExpectedLease(record Record, command Command) bool {
	return record.Controller == command.ExpectedController &&
		record.ControllerEpoch == command.ExpectedControllerEpoch &&
		record.LeaseAppliedThrough == command.ExpectedLeaseAppliedThrough &&
		record.LeaseRevision == command.ExpectedLeaseRevision
}

func leaseAppliedThrough(applied, span uint64) (uint64, bool) {
	if applied == 0 || span == 0 || span > math.MaxUint64-applied {
		return 0, false
	}
	return applied + span, true
}

func matchesAcquireCertificate(record Record, expected Digest) bool {
	certificate, ok := record.AcquireCertificate()
	if !ok {
		return false
	}
	digest, err := AcquireCertificateDigest(certificate)
	return err == nil && digest == expected
}
