package requestledger

import (
	"crypto/sha256"
	"encoding/binary"
)

const (
	preparedTerminalHeaderBytes = 528
	// Prepared cannot accept a result that the later, slightly larger Terminal
	// frame cannot publish after the schema pin has already been released.
	MaxPreparedTerminalResultBytes = MaxTerminalResultBytes
	MaxPreparedTerminalRecordBytes = preparedTerminalHeaderBytes + MaxPreparedTerminalResultBytes + checksumBytes
)

var (
	preparedTerminalMagic        = [4]byte{'V', 'R', 'L', 'U'}
	preparedTerminalDigestDomain = []byte("vibedb/request-ledger/prepared-terminal\x00")
)

// PreparedTerminalRecord durably owns the complete client result and raw ACK
// capability before the long-lived schema pin is released. It is immutable;
// Complete may only copy this exact candidate after authenticated release.
type PreparedTerminalRecord struct {
	KeyDigest                    Digest
	RequestDigest                Digest
	PlanRoot                     Digest
	TerminalContractDigest       Digest
	ResultDigest                 Digest
	TerminalStateDigest          Digest
	RetirementWitnessDigest      Digest
	FinalContinuationDigest      Digest
	AckTokenDigest               Digest
	PreparedDigest               Digest
	AckToken                     AckToken
	CatalogGeneration            uint64
	PinID                        PinID
	PinDigest                    Digest
	RouteSchemaCertificateDigest Digest
	Revision                     uint64
	FinalWaveCount               uint64
	TerminalTransitionTag        uint32
	Outcome                      Outcome
	AffectedRows                 int64
	AffectedRowsValid            bool
	Result                       []byte
}

func NewPreparedTerminal(
	head HeadRecord,
	continuation ContinuationRecord,
	revision uint64,
	outcome Outcome,
	affectedRows int64,
	affectedRowsValid bool,
	result []byte,
	retirementWitnessDigest Digest,
	ackToken AckToken,
) (PreparedTerminalRecord, error) {
	if err := ValidateTerminalContinuation(head, continuation, outcome, retirementWitnessDigest); err != nil ||
		!nextRevision(head.Revision, revision) || len(result) > MaxPreparedTerminalResultBytes {
		return PreparedTerminalRecord{}, ErrInvalidState
	}
	transitionTag, finalWaveCount, stateDigest := terminalTuple(head, outcome)
	record := PreparedTerminalRecord{
		KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
		TerminalContractDigest: head.TerminalContractDigest,
		ResultDigest:           ResultDigest(result), TerminalStateDigest: stateDigest,
		RetirementWitnessDigest: retirementWitnessDigest,
		FinalContinuationDigest: continuation.ContinuationDigest,
		AckTokenDigest:          AckTokenDigest(ackToken), AckToken: ackToken,
		CatalogGeneration: head.CatalogGeneration, PinID: head.PinID,
		PinDigest: head.PinDigest, RouteSchemaCertificateDigest: head.RouteSchemaCertificateDigest,
		Revision: revision, FinalWaveCount: finalWaveCount, TerminalTransitionTag: transitionTag,
		Outcome: outcome, AffectedRows: affectedRows, AffectedRowsValid: affectedRowsValid,
		Result: result,
	}
	record.PreparedDigest = preparedTerminalDigest(record)
	if err := validatePreparedTerminal(record); err != nil ||
		uint64(preparedTerminalHeaderBytes+len(result)+checksumBytes) > head.MaxTerminalBytes {
		return PreparedTerminalRecord{}, ErrTooLarge
	}
	return record, nil
}

func AppendPreparedTerminal(dst []byte, record PreparedTerminalRecord) ([]byte, error) {
	if err := validatePreparedTerminal(record); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, preparedTerminalHeaderBytes)...)
	out := dst[start:]
	copy(out[:4], preparedTerminalMagic[:])
	out[8] = byte(record.Outcome)
	if record.AffectedRowsValid {
		out[9] = 1
	}
	binary.LittleEndian.PutUint32(out[12:16], record.TerminalTransitionTag)
	binary.LittleEndian.PutUint64(out[16:24], record.Revision)
	binary.LittleEndian.PutUint64(out[24:32], record.FinalWaveCount)
	binary.LittleEndian.PutUint64(out[32:40], uint64(record.AffectedRows))
	binary.LittleEndian.PutUint64(out[40:48], uint64(len(record.Result)))
	binary.LittleEndian.PutUint64(out[48:56], record.CatalogGeneration)
	putDigest(out[64:96], record.KeyDigest)
	putDigest(out[96:128], record.RequestDigest)
	putDigest(out[128:160], record.PlanRoot)
	putDigest(out[160:192], record.TerminalContractDigest)
	putDigest(out[192:224], record.ResultDigest)
	putDigest(out[224:256], record.TerminalStateDigest)
	putDigest(out[256:288], record.RetirementWitnessDigest)
	putDigest(out[288:320], record.FinalContinuationDigest)
	putDigest(out[320:352], record.AckTokenDigest)
	copy(out[352:368], record.PinID[:])
	putDigest(out[368:400], record.PinDigest)
	putDigest(out[400:432], record.RouteSchemaCertificateDigest)
	putDigest(out[432:464], record.PreparedDigest)
	copy(out[464:496], record.AckToken[:])
	dst = append(dst, record.Result...)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenPreparedTerminal(raw []byte) (PreparedTerminalRecord, error) {
	if len(raw) < preparedTerminalHeaderBytes+checksumBytes || len(raw) > MaxPreparedTerminalRecordBytes ||
		!magicOK(raw, preparedTerminalMagic) || !zeroBytes(raw[4:8]) || raw[9] > 1 ||
		!zeroBytes(raw[10:12]) || !zeroBytes(raw[56:64]) || !zeroBytes(raw[496:528]) ||
		!checksumOK(raw) {
		return PreparedTerminalRecord{}, ErrCorrupt
	}
	resultBytes := binary.LittleEndian.Uint64(raw[40:48])
	want, ok := exactLength(preparedTerminalHeaderBytes+checksumBytes, resultBytes)
	if !ok || want != len(raw) {
		return PreparedTerminalRecord{}, ErrCorrupt
	}
	record := PreparedTerminalRecord{
		Outcome: Outcome(raw[8]), AffectedRowsValid: raw[9] == 1,
		TerminalTransitionTag: binary.LittleEndian.Uint32(raw[12:16]),
		Revision:              binary.LittleEndian.Uint64(raw[16:24]), FinalWaveCount: binary.LittleEndian.Uint64(raw[24:32]),
		AffectedRows: int64(binary.LittleEndian.Uint64(raw[32:40])), CatalogGeneration: binary.LittleEndian.Uint64(raw[48:56]),
		KeyDigest: readDigest(raw[64:96]), RequestDigest: readDigest(raw[96:128]), PlanRoot: readDigest(raw[128:160]),
		TerminalContractDigest: readDigest(raw[160:192]), ResultDigest: readDigest(raw[192:224]),
		TerminalStateDigest: readDigest(raw[224:256]), RetirementWitnessDigest: readDigest(raw[256:288]),
		FinalContinuationDigest: readDigest(raw[288:320]), AckTokenDigest: readDigest(raw[320:352]),
		PinDigest: readDigest(raw[368:400]), RouteSchemaCertificateDigest: readDigest(raw[400:432]),
		PreparedDigest: readDigest(raw[432:464]),
		Result:         raw[preparedTerminalHeaderBytes : len(raw)-checksumBytes : len(raw)-checksumBytes],
	}
	copy(record.PinID[:], raw[352:368])
	copy(record.AckToken[:], raw[464:496])
	if err := validatePreparedTerminal(record); err != nil {
		return PreparedTerminalRecord{}, ErrCorrupt
	}
	return record, nil
}

func validatePreparedTerminal(record PreparedTerminalRecord) error {
	if !nonzeroDigest(record.KeyDigest) || !nonzeroDigest(record.RequestDigest) ||
		!nonzeroDigest(record.PlanRoot) || !nonzeroDigest(record.TerminalContractDigest) ||
		!nonzeroDigest(record.TerminalStateDigest) || !nonzeroDigest(record.RetirementWitnessDigest) ||
		!nonzeroDigest(record.FinalContinuationDigest) || record.AckToken == (AckToken{}) ||
		record.AckTokenDigest != AckTokenDigest(record.AckToken) || record.CatalogGeneration == 0 ||
		record.PinID == (PinID{}) || !nonzeroDigest(record.PinDigest) ||
		!nonzeroDigest(record.RouteSchemaCertificateDigest) || record.Revision == 0 ||
		record.FinalWaveCount == 0 || record.TerminalTransitionTag == 0 || !record.Outcome.Valid() ||
		record.AffectedRows < 0 || (record.Outcome == OutcomeCommitted) != record.AffectedRowsValid ||
		record.Outcome == OutcomeAborted && record.AffectedRows != 0 ||
		len(record.Result) > MaxPreparedTerminalResultBytes || record.ResultDigest != ResultDigest(record.Result) ||
		record.PreparedDigest != preparedTerminalDigest(record) {
		return ErrCorrupt
	}
	return nil
}

func preparedTerminalDigest(record PreparedTerminalRecord) Digest {
	const domain = "vibedb/request-ledger/prepared-terminal\x00"
	var framed [len(domain) + 16 + 11*sha256.Size + 16 + 16]byte
	at := copy(framed[:], preparedTerminalDigestDomain)
	framed[at], framed[at+1] = byte(record.Outcome), 0
	if record.AffectedRowsValid {
		framed[at+1] = 1
	}
	binary.LittleEndian.PutUint32(framed[at+4:at+8], record.TerminalTransitionTag)
	binary.LittleEndian.PutUint64(framed[at+8:at+16], record.FinalWaveCount)
	at += 16
	for _, digest := range [...]Digest{
		record.KeyDigest, record.RequestDigest, record.PlanRoot, record.TerminalContractDigest,
		record.ResultDigest, record.TerminalStateDigest, record.RetirementWitnessDigest,
		record.FinalContinuationDigest, record.AckTokenDigest, record.PinDigest,
		record.RouteSchemaCertificateDigest,
	} {
		at += copy(framed[at:], digest[:])
	}
	binary.LittleEndian.PutUint64(framed[at:at+8], uint64(record.AffectedRows))
	binary.LittleEndian.PutUint64(framed[at+8:at+16], record.CatalogGeneration)
	at += 16
	copy(framed[at:], record.PinID[:])
	return Digest(sha256.Sum256(framed[:at+16]))
}

func MarkTerminalPrepared(head HeadRecord, continuation ContinuationRecord, prepared PreparedTerminalRecord) (HeadRecord, error) {
	if err := ValidateTerminalContinuation(head, continuation, prepared.Outcome, prepared.RetirementWitnessDigest); err != nil ||
		errOrNil(validatePreparedTerminal(prepared)) != nil || prepared.KeyDigest != head.KeyDigest ||
		prepared.RequestDigest != head.RequestDigest || prepared.PlanRoot != head.PlanRoot ||
		prepared.TerminalContractDigest != head.TerminalContractDigest ||
		prepared.FinalContinuationDigest != continuation.ContinuationDigest ||
		!nextRevision(head.Revision, prepared.Revision) {
		return HeadRecord{}, ErrInvalidState
	}
	head.Phase = PhasePrepared
	head.Revision = prepared.Revision
	head.PreparedTerminalDigest = prepared.PreparedDigest
	return head, validateHead(head)
}
