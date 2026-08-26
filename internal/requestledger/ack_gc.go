package requestledger

import (
	"crypto/sha256"
	"encoding/binary"
)

const AckRecordBytes = 544

var (
	ackMagic        = [4]byte{'V', 'R', 'L', 'A'}
	ackDigestDomain = []byte("vibedb/request-ledger/ack\x00")
)

type AckGCPhase uint8

const (
	AckGCInvalid AckGCPhase = iota
	AckGCCollecting
	AckGCComplete
)

type AckRecord struct {
	Key                          RequestKey
	KeyDigest                    Digest
	RequestDigest                Digest
	PlanRoot                     Digest
	TerminalContractDigest       Digest
	CatalogGeneration            uint64
	PinID                        PinID
	PinDigest                    Digest
	RouteSchemaCertificateDigest Digest
	ResultDigest                 Digest
	AckTokenDigest               Digest
	ReleaseCertificateDigest     Digest
	AckDigest                    Digest
	Revision                     uint64
	TerminalRevision             uint64
	PriorEncodedBytes            uint64
	ReclaimedBytes               uint64
	TerminalResultBytes          uint64
	GCCursor                     uint64
	GCPhase                      AckGCPhase
}

func NewAck(head HeadRecord, terminal TerminalRecord, revision, priorEncodedBytes uint64) (AckRecord, error) {
	if err := validateHead(head); err != nil || errOrNil(validateTerminal(terminal)) != nil ||
		head.Phase != PhaseTerminal || head.Revision != terminal.Revision ||
		terminal.KeyDigest != head.KeyDigest || terminal.RequestDigest != head.RequestDigest ||
		terminal.PlanRoot != head.PlanRoot || terminal.TerminalContractDigest != head.TerminalContractDigest ||
		!nextRevision(head.Revision, revision) || priorEncodedBytes == 0 ||
		uint64(len(terminal.Result)) > priorEncodedBytes {
		return AckRecord{}, ErrInvalidState
	}
	record := AckRecord{
		Key: head.Key, KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest,
		PlanRoot: head.PlanRoot, TerminalContractDigest: head.TerminalContractDigest,
		CatalogGeneration: head.CatalogGeneration, PinID: head.PinID,
		PinDigest: head.PinDigest, RouteSchemaCertificateDigest: head.RouteSchemaCertificateDigest,
		ResultDigest: terminal.ResultDigest, AckTokenDigest: terminal.AckTokenDigest,
		ReleaseCertificateDigest: terminal.SchemaPinReleaseCertificateDigest,
		Revision:                 revision, TerminalRevision: terminal.Revision,
		PriorEncodedBytes: priorEncodedBytes, TerminalResultBytes: uint64(len(terminal.Result)),
		GCPhase: AckGCCollecting,
	}
	record.AckDigest = ackDigest(record)
	return record, validateAck(record)
}

// AdvanceAckGC accounts one bounded deletion chunk. Ack itself is never a GC
// target. final is legal only after every pre-ACK resident byte was reclaimed.
func AdvanceAckGC(record AckRecord, revision, nextCursor, reclaimed uint64, final bool) (AckRecord, error) {
	if err := validateAck(record); err != nil || record.GCPhase != AckGCCollecting ||
		!nextRevision(record.Revision, revision) || nextCursor < record.GCCursor || reclaimed == 0 ||
		reclaimed > record.PriorEncodedBytes-record.ReclaimedBytes {
		return AckRecord{}, ErrInvalidState
	}
	record.Revision, record.GCCursor = revision, nextCursor
	record.ReclaimedBytes += reclaimed
	if final {
		if record.ReclaimedBytes != record.PriorEncodedBytes {
			return AckRecord{}, ErrIncomplete
		}
		record.GCPhase = AckGCComplete
	}
	record.AckDigest = ackDigest(record)
	return record, validateAck(record)
}

func AppendAck(dst []byte, record AckRecord) ([]byte, error) {
	if err := validateAck(record); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, AckRecordBytes-checksumBytes)...)
	out := dst[start:]
	copy(out[:4], ackMagic[:])
	out[8], out[16] = byte(record.GCPhase), byte(record.Key.Scope)
	binary.LittleEndian.PutUint64(out[24:32], record.Revision)
	binary.LittleEndian.PutUint64(out[32:40], record.TerminalRevision)
	binary.LittleEndian.PutUint64(out[40:48], record.PriorEncodedBytes)
	binary.LittleEndian.PutUint64(out[48:56], record.ReclaimedBytes)
	binary.LittleEndian.PutUint64(out[56:64], record.TerminalResultBytes)
	binary.LittleEndian.PutUint64(out[64:72], record.GCCursor)
	binary.LittleEndian.PutUint64(out[72:80], record.Key.IssuerEpoch)
	binary.LittleEndian.PutUint64(out[80:88], record.Key.IssuerSequence)
	copy(out[88:104], record.Key.Principal[:])
	copy(out[104:120], record.Key.Request[:])
	putDigest(out[120:152], record.Key.TenantDigest)
	putDigest(out[152:184], record.KeyDigest)
	putDigest(out[184:216], record.RequestDigest)
	putDigest(out[216:248], record.PlanRoot)
	putDigest(out[248:280], record.TerminalContractDigest)
	putDigest(out[280:312], record.ResultDigest)
	putDigest(out[312:344], record.ReleaseCertificateDigest)
	putDigest(out[344:376], record.AckDigest)
	copy(out[376:384], record.Key.IssuerLane[:])
	binary.LittleEndian.PutUint64(out[384:392], record.CatalogGeneration)
	copy(out[392:408], record.PinID[:])
	putDigest(out[408:440], record.PinDigest)
	putDigest(out[440:472], record.RouteSchemaCertificateDigest)
	putDigest(out[472:504], record.AckTokenDigest)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenAck(raw []byte) (AckRecord, error) {
	if len(raw) != AckRecordBytes || !magicOK(raw, ackMagic) || !zeroBytes(raw[4:8]) ||
		!zeroBytes(raw[9:16]) || !zeroBytes(raw[17:24]) || !zeroBytes(raw[504:540]) || !checksumOK(raw) {
		return AckRecord{}, ErrCorrupt
	}
	record := AckRecord{
		GCPhase: AckGCPhase(raw[8]),
		Key: RequestKey{Scope: ScopeKind(raw[16]), TenantDigest: readDigest(raw[120:152]),
			IssuerEpoch: binary.LittleEndian.Uint64(raw[72:80]), IssuerSequence: binary.LittleEndian.Uint64(raw[80:88])},
		Revision: binary.LittleEndian.Uint64(raw[24:32]), TerminalRevision: binary.LittleEndian.Uint64(raw[32:40]),
		PriorEncodedBytes: binary.LittleEndian.Uint64(raw[40:48]), ReclaimedBytes: binary.LittleEndian.Uint64(raw[48:56]),
		TerminalResultBytes: binary.LittleEndian.Uint64(raw[56:64]), GCCursor: binary.LittleEndian.Uint64(raw[64:72]),
		KeyDigest: readDigest(raw[152:184]), RequestDigest: readDigest(raw[184:216]), PlanRoot: readDigest(raw[216:248]),
		TerminalContractDigest: readDigest(raw[248:280]), ResultDigest: readDigest(raw[280:312]),
		ReleaseCertificateDigest: readDigest(raw[312:344]), AckDigest: readDigest(raw[344:376]),
		CatalogGeneration: binary.LittleEndian.Uint64(raw[384:392]),
		PinDigest:         readDigest(raw[408:440]), RouteSchemaCertificateDigest: readDigest(raw[440:472]),
		AckTokenDigest: readDigest(raw[472:504]),
	}
	copy(record.Key.Principal[:], raw[88:104])
	copy(record.Key.Request[:], raw[104:120])
	copy(record.Key.IssuerLane[:], raw[376:384])
	copy(record.PinID[:], raw[392:408])
	if err := validateAck(record); err != nil {
		return AckRecord{}, ErrCorrupt
	}
	return record, nil
}

func validateAck(record AckRecord) error {
	derived, err := KeyDigest(record.Key)
	if err != nil || derived != record.KeyDigest || !nonzeroDigest(record.RequestDigest) ||
		!nonzeroDigest(record.PlanRoot) || !nonzeroDigest(record.TerminalContractDigest) || !nonzeroDigest(record.ResultDigest) ||
		!nonzeroDigest(record.AckTokenDigest) ||
		record.CatalogGeneration == 0 || record.PinID == (PinID{}) || !nonzeroDigest(record.PinDigest) ||
		!nonzeroDigest(record.RouteSchemaCertificateDigest) ||
		record.TerminalRevision == 0 || record.Revision <= record.TerminalRevision || record.PriorEncodedBytes == 0 ||
		record.TerminalResultBytes > record.PriorEncodedBytes || record.ReclaimedBytes > record.PriorEncodedBytes ||
		record.GCPhase < AckGCCollecting || record.GCPhase > AckGCComplete ||
		!nonzeroDigest(record.ReleaseCertificateDigest) ||
		(record.GCPhase == AckGCCollecting && record.ReclaimedBytes == 0 &&
			(record.Revision != record.TerminalRevision+1 || record.GCCursor != 0)) ||
		(record.GCPhase == AckGCComplete && record.ReclaimedBytes != record.PriorEncodedBytes) ||
		record.AckDigest != ackDigest(record) {
		return ErrCorrupt
	}
	return nil
}

func ackDigest(record AckRecord) Digest {
	var framed [640]byte
	at := copy(framed[:], ackDigestDomain)
	framed[at], framed[at+1] = byte(record.GCPhase), byte(record.Key.Scope)
	at += 8
	for _, value := range [...]uint64{record.Revision, record.TerminalRevision, record.PriorEncodedBytes,
		record.ReclaimedBytes, record.TerminalResultBytes, record.GCCursor,
		record.Key.IssuerEpoch, record.Key.IssuerSequence} {
		binary.LittleEndian.PutUint64(framed[at:at+8], value)
		at += 8
	}
	at += copy(framed[at:], record.Key.IssuerLane[:])
	at += copy(framed[at:], record.Key.Principal[:])
	at += copy(framed[at:], record.Key.Request[:])
	for _, digest := range [...]Digest{record.Key.TenantDigest, record.KeyDigest, record.RequestDigest,
		record.PlanRoot, record.TerminalContractDigest, record.ResultDigest, record.AckTokenDigest,
		record.ReleaseCertificateDigest} {
		at += copy(framed[at:], digest[:])
	}
	binary.LittleEndian.PutUint64(framed[at:at+8], record.CatalogGeneration)
	at += 8
	at += copy(framed[at:], record.PinID[:])
	at += copy(framed[at:], record.PinDigest[:])
	at += copy(framed[at:], record.RouteSchemaCertificateDigest[:])
	return Digest(sha256.Sum256(framed[:at]))
}

func SameAck(left, right AckRecord) bool { return left == right }
