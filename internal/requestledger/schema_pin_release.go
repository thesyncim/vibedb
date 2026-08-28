package requestledger

import (
	"crypto/sha256"
	"encoding/binary"
)

const (
	schemaPinReleaseHeaderBytes    = 448
	MaxSchemaPinReleaseRecordBytes = schemaPinReleaseHeaderBytes + MaxExecutionPinCommandBytes + MaxExecutionPinCompletionBytes + checksumBytes
)

var (
	schemaPinReleaseMagic        = [4]byte{'V', 'R', 'L', 'S'}
	schemaPinReleaseDigestDomain = []byte("vibedb/request-ledger/schema-pin-release\x00")
	schemaPinCertificateDomain   = []byte("vibedb/request-ledger/schema-pin-certificate\x00")
)

type SchemaPinReleasePhase uint8

const (
	SchemaPinReleaseInvalid SchemaPinReleasePhase = iota
	SchemaPinReleasing
	SchemaPinReleased
)

// SchemaPinReleaseRecord is a write-ahead release intent and its exact
// authenticated settled completion. The state-machine integration verifies
// the execution-pin command/completion semantics before calling the Released
// step.
type SchemaPinReleaseRecord struct {
	KeyDigest                    Digest
	RequestDigest                Digest
	PlanRoot                     Digest
	PreparedTerminalDigest       Digest
	PinID                        PinID
	PinDigest                    Digest
	RouteSchemaCertificateDigest Digest
	CommandDigest                Digest
	CompletionDigest             Digest
	PriorRecordDigest            Digest
	CertificateDigest            Digest
	RecordDigest                 Digest
	CatalogGeneration            uint64
	Revision                     uint64
	Phase                        SchemaPinReleasePhase
	Command                      []byte
	Completion                   []byte
}

func NewSchemaPinRelease(
	head HeadRecord,
	prepared PreparedTerminalRecord,
	revision uint64,
	command []byte,
) (SchemaPinReleaseRecord, error) {
	if err := validateHead(head); err != nil || errOrNil(validatePreparedTerminal(prepared)) != nil ||
		head.Phase != PhasePrepared || nonzeroDigest(head.SchemaPinReleaseCertificateDigest) ||
		prepared.PreparedDigest != head.PreparedTerminalDigest || prepared.KeyDigest != head.KeyDigest ||
		!nextRevision(head.Revision, revision) || len(command) == 0 || len(command) > MaxExecutionPinCommandBytes {
		return SchemaPinReleaseRecord{}, ErrInvalidState
	}
	record := SchemaPinReleaseRecord{
		KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
		PreparedTerminalDigest: head.PreparedTerminalDigest,
		PinID:                  prepared.PinID, PinDigest: prepared.PinDigest,
		RouteSchemaCertificateDigest: prepared.RouteSchemaCertificateDigest,
		CatalogGeneration:            prepared.CatalogGeneration, Revision: revision,
		Phase: SchemaPinReleasing, Command: command,
	}
	record.CommandDigest = digestBytes([]byte("vibedb/request-ledger/schema-pin-command\x00"), command)
	record.RecordDigest = schemaPinReleaseDigest(record)
	return record, validateSchemaPinRelease(record)
}

func RecordVerifiedSchemaPinReleased(
	record SchemaPinReleaseRecord,
	revision uint64,
	completion []byte,
) (SchemaPinReleaseRecord, error) {
	if err := validateSchemaPinRelease(record); err != nil || record.Phase != SchemaPinReleasing ||
		!nextRevision(record.Revision, revision) || len(completion) == 0 ||
		len(completion) > MaxExecutionPinCompletionBytes {
		return SchemaPinReleaseRecord{}, ErrInvalidState
	}
	record.PriorRecordDigest = record.RecordDigest
	record.Revision = revision
	record.Phase = SchemaPinReleased
	record.Completion = completion
	record.CompletionDigest = digestBytes([]byte("vibedb/request-ledger/schema-pin-completion\x00"), completion)
	record.CertificateDigest = schemaPinCertificateDigest(record)
	record.RecordDigest = schemaPinReleaseDigest(record)
	return record, validateSchemaPinRelease(record)
}

func AppendSchemaPinRelease(dst []byte, record SchemaPinReleaseRecord) ([]byte, error) {
	if err := validateSchemaPinRelease(record); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, schemaPinReleaseHeaderBytes)...)
	out := dst[start:]
	copy(out[:4], schemaPinReleaseMagic[:])
	out[8] = byte(record.Phase)
	binary.LittleEndian.PutUint64(out[16:24], record.Revision)
	binary.LittleEndian.PutUint64(out[24:32], record.CatalogGeneration)
	putDigest(out[32:64], record.KeyDigest)
	putDigest(out[64:96], record.RequestDigest)
	putDigest(out[96:128], record.PlanRoot)
	putDigest(out[128:160], record.PreparedTerminalDigest)
	copy(out[160:176], record.PinID[:])
	putDigest(out[176:208], record.PinDigest)
	putDigest(out[208:240], record.RouteSchemaCertificateDigest)
	putDigest(out[240:272], record.CommandDigest)
	putDigest(out[272:304], record.CompletionDigest)
	putDigest(out[304:336], record.PriorRecordDigest)
	putDigest(out[336:368], record.CertificateDigest)
	putDigest(out[368:400], record.RecordDigest)
	binary.LittleEndian.PutUint32(out[400:404], uint32(len(record.Command)))
	binary.LittleEndian.PutUint32(out[404:408], uint32(len(record.Completion)))
	dst = append(dst, record.Command...)
	dst = append(dst, record.Completion...)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenSchemaPinRelease(raw []byte) (SchemaPinReleaseRecord, error) {
	if len(raw) < schemaPinReleaseHeaderBytes+checksumBytes || len(raw) > MaxSchemaPinReleaseRecordBytes ||
		!magicOK(raw, schemaPinReleaseMagic) || !zeroBytes(raw[4:8]) || !zeroBytes(raw[9:16]) ||
		!zeroBytes(raw[408:448]) || !checksumOK(raw) {
		return SchemaPinReleaseRecord{}, ErrCorrupt
	}
	commandBytes := binary.LittleEndian.Uint32(raw[400:404])
	completionBytes := binary.LittleEndian.Uint32(raw[404:408])
	want, ok := exactLength(schemaPinReleaseHeaderBytes+checksumBytes, uint64(commandBytes), uint64(completionBytes))
	if !ok || want != len(raw) {
		return SchemaPinReleaseRecord{}, ErrCorrupt
	}
	commandEnd := schemaPinReleaseHeaderBytes + int(commandBytes)
	record := SchemaPinReleaseRecord{
		Phase: SchemaPinReleasePhase(raw[8]), Revision: binary.LittleEndian.Uint64(raw[16:24]),
		CatalogGeneration: binary.LittleEndian.Uint64(raw[24:32]),
		KeyDigest:         readDigest(raw[32:64]), RequestDigest: readDigest(raw[64:96]), PlanRoot: readDigest(raw[96:128]),
		PreparedTerminalDigest: readDigest(raw[128:160]), PinDigest: readDigest(raw[176:208]),
		RouteSchemaCertificateDigest: readDigest(raw[208:240]), CommandDigest: readDigest(raw[240:272]),
		CompletionDigest: readDigest(raw[272:304]), PriorRecordDigest: readDigest(raw[304:336]),
		CertificateDigest: readDigest(raw[336:368]), RecordDigest: readDigest(raw[368:400]),
		Command:    raw[schemaPinReleaseHeaderBytes:commandEnd:commandEnd],
		Completion: raw[commandEnd : len(raw)-checksumBytes : len(raw)-checksumBytes],
	}
	copy(record.PinID[:], raw[160:176])
	if err := validateSchemaPinRelease(record); err != nil {
		return SchemaPinReleaseRecord{}, ErrCorrupt
	}
	return record, nil
}

func validateSchemaPinRelease(record SchemaPinReleaseRecord) error {
	if !nonzeroDigest(record.KeyDigest) || !nonzeroDigest(record.RequestDigest) || !nonzeroDigest(record.PlanRoot) ||
		!nonzeroDigest(record.PreparedTerminalDigest) || record.PinID == (PinID{}) ||
		!nonzeroDigest(record.PinDigest) || !nonzeroDigest(record.RouteSchemaCertificateDigest) ||
		record.CatalogGeneration == 0 || record.Revision == 0 || len(record.Command) == 0 ||
		len(record.Command) > MaxExecutionPinCommandBytes ||
		record.CommandDigest != digestBytes([]byte("vibedb/request-ledger/schema-pin-command\x00"), record.Command) ||
		record.Phase < SchemaPinReleasing || record.Phase > SchemaPinReleased ||
		(record.Phase == SchemaPinReleasing && (len(record.Completion) != 0 || nonzeroDigest(record.CompletionDigest) ||
			nonzeroDigest(record.PriorRecordDigest) || nonzeroDigest(record.CertificateDigest))) ||
		(record.Phase == SchemaPinReleased && (len(record.Completion) == 0 ||
			len(record.Completion) > MaxExecutionPinCompletionBytes ||
			record.CompletionDigest != digestBytes([]byte("vibedb/request-ledger/schema-pin-completion\x00"), record.Completion) ||
			!nonzeroDigest(record.PriorRecordDigest) || record.CertificateDigest != schemaPinCertificateDigest(record))) ||
		record.RecordDigest != schemaPinReleaseDigest(record) {
		return ErrCorrupt
	}
	return nil
}

func schemaPinCertificateDigest(record SchemaPinReleaseRecord) Digest {
	const domain = "vibedb/request-ledger/schema-pin-certificate\x00"
	var framed [len(domain) + 8 + 8*sha256.Size + 16]byte
	at := copy(framed[:], schemaPinCertificateDomain)
	binary.LittleEndian.PutUint64(framed[at:at+8], record.CatalogGeneration)
	at += 8
	for _, digest := range [...]Digest{
		record.KeyDigest, record.RequestDigest, record.PlanRoot, record.PreparedTerminalDigest,
		record.PinDigest, record.RouteSchemaCertificateDigest, record.CommandDigest, record.CompletionDigest,
	} {
		at += copy(framed[at:], digest[:])
	}
	copy(framed[at:], record.PinID[:])
	return Digest(sha256.Sum256(framed[:]))
}

func schemaPinReleaseDigest(record SchemaPinReleaseRecord) Digest {
	const domain = "vibedb/request-ledger/schema-pin-release\x00"
	var framed [len(domain) + 16 + 10*sha256.Size + 8 + 16]byte
	at := copy(framed[:], schemaPinReleaseDigestDomain)
	framed[at] = byte(record.Phase)
	binary.LittleEndian.PutUint64(framed[at+8:at+16], record.Revision)
	at += 16
	for _, digest := range [...]Digest{
		record.KeyDigest, record.RequestDigest, record.PlanRoot, record.PreparedTerminalDigest,
		record.PinDigest, record.RouteSchemaCertificateDigest, record.CommandDigest,
		record.CompletionDigest, record.PriorRecordDigest, record.CertificateDigest,
	} {
		at += copy(framed[at:], digest[:])
	}
	binary.LittleEndian.PutUint64(framed[at:at+8], record.CatalogGeneration)
	at += 8
	copy(framed[at:], record.PinID[:])
	return Digest(sha256.Sum256(framed[:at+16]))
}

func InstallSchemaPinRelease(head HeadRecord, prepared PreparedTerminalRecord, record SchemaPinReleaseRecord) (HeadRecord, error) {
	if err := validateHead(head); err != nil || errOrNil(validatePreparedTerminal(prepared)) != nil ||
		errOrNil(validateSchemaPinRelease(record)) != nil || record.Phase != SchemaPinReleasing ||
		head.Phase != PhasePrepared || nonzeroDigest(head.SchemaPinReleaseCertificateDigest) ||
		prepared.PreparedDigest != head.PreparedTerminalDigest || record.PreparedTerminalDigest != prepared.PreparedDigest ||
		record.KeyDigest != head.KeyDigest || record.RequestDigest != head.RequestDigest ||
		record.PlanRoot != head.PlanRoot || record.CatalogGeneration != head.CatalogGeneration ||
		record.PinID != head.PinID || record.PinDigest != head.PinDigest ||
		record.RouteSchemaCertificateDigest != head.RouteSchemaCertificateDigest ||
		!nextRevision(head.Revision, record.Revision) {
		return HeadRecord{}, ErrInvalidState
	}
	head.Revision = record.Revision
	return head, validateHead(head)
}

func MarkSchemaPinReleased(
	head HeadRecord,
	prepared PreparedTerminalRecord,
	prior SchemaPinReleaseRecord,
	record SchemaPinReleaseRecord,
) (HeadRecord, error) {
	if err := validateHead(head); err != nil || errOrNil(validatePreparedTerminal(prepared)) != nil ||
		errOrNil(validateSchemaPinRelease(prior)) != nil || errOrNil(validateSchemaPinRelease(record)) != nil ||
		prior.Phase != SchemaPinReleasing || record.Phase != SchemaPinReleased ||
		head.Phase != PhasePrepared || prepared.PreparedDigest != head.PreparedTerminalDigest ||
		record.PreparedTerminalDigest != prepared.PreparedDigest || record.KeyDigest != head.KeyDigest ||
		record.RequestDigest != head.RequestDigest || record.PlanRoot != head.PlanRoot ||
		record.CatalogGeneration != head.CatalogGeneration || record.PinID != head.PinID ||
		record.PinDigest != head.PinDigest ||
		record.RouteSchemaCertificateDigest != head.RouteSchemaCertificateDigest ||
		record.PriorRecordDigest != prior.RecordDigest ||
		prior.KeyDigest != record.KeyDigest || prior.PreparedTerminalDigest != record.PreparedTerminalDigest ||
		!nextRevision(head.Revision, record.Revision) {
		return HeadRecord{}, ErrInvalidState
	}
	head.Revision = record.Revision
	head.SchemaPinReleaseCertificateDigest = record.CertificateDigest
	return head, validateHead(head)
}
