package executionpin

import "bytes"

const CompletionBytes = 896

var (
	completionMagic          = [8]byte{'V', 'E', 'L', 'C', 'P', 'L', 0, 0}
	completionChecksumDomain = []byte("vibedb/logical-execution-pin/completion-checksum\x00")
)

// Completion is the exact transferable proof set. Active results carry the
// immutable acquisition and current lease certificates. Released results add
// the PrepareTerminal-bound terminal certificate. An expiry tombstone created
// before acquire canonically carries only its terminal certificate.
type Completion struct {
	Operation Operation
	Status    Status
	Found     bool
	Acquire   AcquireCertificate
	Lease     LeaseCertificate
	Terminal  TerminalCertificate
}

func (completion Completion) Valid() bool {
	if completion.Operation < OperationAcquire || completion.Operation > OperationExpire {
		return false
	}
	if !completion.Found {
		return completion.Status == 0 && completion.Acquire == (AcquireCertificate{}) &&
			completion.Lease == (LeaseCertificate{}) &&
			completion.Terminal == (TerminalCertificate{})
	}
	if completion.Status < StatusActive || completion.Status > StatusExpired {
		return false
	}
	if completion.Status == StatusActive {
		if !completion.Acquire.Valid() || !completion.Lease.Valid() ||
			completion.Terminal != (TerminalCertificate{}) ||
			completion.Acquire.PinID != completion.Lease.PinID {
			return false
		}
		digest, err := AcquireCertificateDigest(completion.Acquire)
		return err == nil && digest == completion.Lease.AcquireCertificateDigest
	}
	if !completion.Terminal.Valid() || completion.Terminal.Status != completion.Status {
		return false
	}
	if completion.Acquire == (AcquireCertificate{}) && completion.Lease == (LeaseCertificate{}) {
		return completion.Status == StatusExpired &&
			completion.Terminal.AcquireCertificateDigest == (Digest{}) &&
			completion.Terminal.LeaseCertificateDigest == (Digest{})
	}
	if !completion.Acquire.Valid() || !completion.Lease.Valid() ||
		completion.Acquire.PinID != completion.Lease.PinID ||
		completion.Acquire.PinID != completion.Terminal.PinID {
		return false
	}
	acquireDigest, acquireErr := AcquireCertificateDigest(completion.Acquire)
	leaseDigest, leaseErr := LeaseCertificateDigest(completion.Lease)
	return acquireErr == nil && leaseErr == nil &&
		acquireDigest == completion.Lease.AcquireCertificateDigest &&
		acquireDigest == completion.Terminal.AcquireCertificateDigest &&
		leaseDigest == completion.Terminal.LeaseCertificateDigest
}

func AppendCompletion(dst []byte, completion Completion) ([]byte, error) {
	if !completion.Valid() {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, CompletionBytes)...)
	frame := dst[start:]
	copy(frame[0:8], completionMagic[:])
	frame[8], frame[9] = byte(completion.Operation), byte(completion.Status)
	if completion.Found {
		frame[10] = 1
	}
	if completion.Acquire != (AcquireCertificate{}) {
		if _, err := AppendAcquireCertificate(frame[16:16], completion.Acquire); err != nil {
			return dst[:start], err
		}
	}
	if completion.Lease != (LeaseCertificate{}) {
		if _, err := AppendLeaseCertificate(
			frame[16+AcquireCertificateBytes:16+AcquireCertificateBytes], completion.Lease,
		); err != nil {
			return dst[:start], err
		}
	}
	if completion.Terminal != (TerminalCertificate{}) {
		if _, err := AppendTerminalCertificate(
			frame[16+AcquireCertificateBytes+LeaseCertificateBytes:16+AcquireCertificateBytes+LeaseCertificateBytes],
			completion.Terminal,
		); err != nil {
			return dst[:start], err
		}
	}
	sealSHA(frame, completionChecksumDomain)
	return dst, nil
}

func OpenCompletion(raw []byte) (Completion, error) {
	if len(raw) != CompletionBytes || !bytes.Equal(raw[0:8], completionMagic[:]) ||
		raw[10] > 1 || !allZero(raw[11:16]) || !verifySHA(raw, completionChecksumDomain) {
		return Completion{}, ErrCorrupt
	}
	completion := Completion{
		Operation: Operation(raw[8]), Status: Status(raw[9]), Found: raw[10] == 1,
	}
	acquireRaw := raw[16 : 16+AcquireCertificateBytes]
	leaseStart := 16 + AcquireCertificateBytes
	leaseRaw := raw[leaseStart : leaseStart+LeaseCertificateBytes]
	terminalStart := leaseStart + LeaseCertificateBytes
	terminalRaw := raw[terminalStart : terminalStart+TerminalCertificateBytes]
	var err error
	if !allZero(acquireRaw) {
		completion.Acquire, err = OpenAcquireCertificate(acquireRaw)
		if err != nil {
			return Completion{}, err
		}
	}
	if !allZero(leaseRaw) {
		completion.Lease, err = OpenLeaseCertificate(leaseRaw)
		if err != nil {
			return Completion{}, err
		}
	}
	if !allZero(terminalRaw) {
		completion.Terminal, err = OpenTerminalCertificate(terminalRaw)
		if err != nil {
			return Completion{}, err
		}
	}
	if !completion.Valid() {
		return Completion{}, ErrCorrupt
	}
	return completion, nil
}

func CompletionFromRecord(operation Operation, record Record) (Completion, error) {
	if !record.Valid() || operation < OperationAcquire || operation > OperationExpire {
		return Completion{}, ErrCorrupt
	}
	completion := Completion{Operation: operation, Status: record.Status, Found: true}
	if acquire, ok := record.AcquireCertificate(); ok {
		completion.Acquire = acquire
		lease, leaseOK := record.LeaseCertificate()
		if !leaseOK {
			return Completion{}, ErrCorrupt
		}
		completion.Lease = lease
	}
	if terminal, ok := record.TerminalCertificate(); ok {
		completion.Terminal = terminal
	}
	if !completion.Valid() {
		return Completion{}, ErrCorrupt
	}
	return completion, nil
}

// CompletionFromApplied reconstructs the exact proof emitted when command was
// applied. A later lease or terminal transition may have advanced record; the
// caller-supplied applied index and authority are authenticated by the same
// replicated session slot that retains the command identity.
func CompletionFromApplied(
	command Command,
	record Record,
	authority Digest,
	applied uint64,
) (Completion, error) {
	if !command.Valid() || !record.Valid() || command.PinID != record.PinID ||
		command.Binding != record.Binding || authority == (Digest{}) || applied == 0 {
		return Completion{}, ErrCorrupt
	}
	acquire, acquired := record.AcquireCertificate()
	switch command.Operation {
	case OperationAcquire:
		if !acquired || record.AcquireApplied != applied ||
			record.AcquireAuthorityDigest != authority ||
			record.AcquireController != command.NextController ||
			record.AcquireControllerEpoch != command.NextControllerEpoch ||
			record.AcquireLeaseAppliedThrough != applied+command.NextLeaseSpan {
			return Completion{}, ErrCorrupt
		}
		digest, err := AcquireCertificateDigest(acquire)
		if err != nil {
			return Completion{}, err
		}
		completion := Completion{
			Operation: OperationAcquire, Status: StatusActive, Found: true,
			Acquire: acquire,
			Lease: LeaseCertificate{
				PinID: command.PinID, AcquireCertificateDigest: digest,
				AuthorityDigest: authority, Controller: command.NextController,
				ControllerEpoch:     command.NextControllerEpoch,
				LeaseAppliedThrough: record.AcquireLeaseAppliedThrough, Revision: 1, Applied: applied,
			},
		}
		if !completion.Valid() {
			return Completion{}, ErrCorrupt
		}
		return completion, nil
	case OperationRenew, OperationRecover:
		if !acquired || command.ExpectedLeaseRevision == ^uint64(0) ||
			record.LeaseRevision < command.ExpectedLeaseRevision+1 {
			return Completion{}, ErrCorrupt
		}
		if record.LeaseRevision == command.ExpectedLeaseRevision+1 &&
			(record.LeaseApplied != applied || record.CurrentAuthorityDigest != authority ||
				record.Controller != command.NextController ||
				record.ControllerEpoch != command.NextControllerEpoch ||
				record.LeaseAppliedThrough != applied+command.NextLeaseSpan) {
			return Completion{}, ErrCorrupt
		}
		digest, err := AcquireCertificateDigest(acquire)
		if err != nil || digest != command.AcquireCertificateDigest {
			return Completion{}, ErrCorrupt
		}
		completion := Completion{
			Operation: command.Operation, Status: StatusActive, Found: true,
			Acquire: acquire,
			Lease: LeaseCertificate{
				PinID: command.PinID, AcquireCertificateDigest: digest,
				AuthorityDigest: authority, Controller: command.NextController,
				ControllerEpoch:     command.NextControllerEpoch,
				LeaseAppliedThrough: applied + command.NextLeaseSpan,
				Revision:            command.ExpectedLeaseRevision + 1, Applied: applied,
			},
		}
		if !completion.Valid() {
			return Completion{}, ErrCorrupt
		}
		return completion, nil
	case OperationRelease, OperationExpire:
		if record.TerminalApplied != applied {
			return Completion{}, ErrCorrupt
		}
		return CompletionFromRecord(command.Operation, record)
	default:
		return Completion{}, ErrCorrupt
	}
}

// RefusalCompletion is the sole fixed-width negative result body. ResultCode
// remains in the outer completion envelope; no mutable record facts are
// claimed for a rejected transition.
func RefusalCompletion(operation Operation) (Completion, error) {
	completion := Completion{Operation: operation}
	if !completion.Valid() {
		return Completion{}, ErrCorrupt
	}
	return completion, nil
}
