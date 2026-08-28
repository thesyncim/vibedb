package requestledger

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
)

const (
	terminalHeaderBytes    = 560
	MaxTerminalResultBytes = MaxLifecyclePayloadBytes - terminalHeaderBytes - checksumBytes
)

var (
	terminalMagic         = [4]byte{'V', 'R', 'L', 'T'}
	resultDigestDomain    = []byte("vibedb/request-ledger/result\x00")
	terminalSummaryDomain = []byte("vibedb/request-ledger/terminal-summary\x00")
)

type TerminalRecord struct {
	KeyDigest                         Digest
	RequestDigest                     Digest
	PlanRoot                          Digest
	TerminalContractDigest            Digest
	ResultDigest                      Digest
	TerminalStateDigest               Digest
	TerminalSummaryDigest             Digest
	RetirementWitnessDigest           Digest
	FinalContinuationDigest           Digest
	PreparedTerminalDigest            Digest
	SchemaPinReleaseCertificateDigest Digest
	AckTokenDigest                    Digest
	AckToken                          AckToken
	CatalogGeneration                 uint64
	PinID                             PinID
	PinDigest                         Digest
	RouteSchemaCertificateDigest      Digest
	Revision                          uint64
	FinalWaveCount                    uint64
	TerminalTransitionTag             uint32
	Outcome                           Outcome
	AffectedRows                      int64
	AffectedRowsValid                 bool
	Result                            []byte
}

func NewTerminal(
	head HeadRecord,
	prepared PreparedTerminalRecord,
	release SchemaPinReleaseRecord,
	revision uint64,
) (TerminalRecord, error) {
	if err := validateHead(head); err != nil || errOrNil(validatePreparedTerminal(prepared)) != nil ||
		errOrNil(validateSchemaPinRelease(release)) != nil || head.Phase != PhasePrepared ||
		release.Phase != SchemaPinReleased || prepared.PreparedDigest != head.PreparedTerminalDigest ||
		release.CertificateDigest != head.SchemaPinReleaseCertificateDigest ||
		release.PreparedTerminalDigest != prepared.PreparedDigest ||
		prepared.KeyDigest != head.KeyDigest || prepared.RequestDigest != head.RequestDigest ||
		prepared.PlanRoot != head.PlanRoot || release.KeyDigest != head.KeyDigest ||
		!nextRevision(head.Revision, revision) {
		return TerminalRecord{}, ErrInvalidState
	}
	record := TerminalRecord{
		KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
		TerminalContractDigest: head.TerminalContractDigest,
		ResultDigest:           prepared.ResultDigest, TerminalStateDigest: prepared.TerminalStateDigest,
		RetirementWitnessDigest:           prepared.RetirementWitnessDigest,
		FinalContinuationDigest:           prepared.FinalContinuationDigest,
		PreparedTerminalDigest:            prepared.PreparedDigest,
		SchemaPinReleaseCertificateDigest: release.CertificateDigest,
		CatalogGeneration:                 prepared.CatalogGeneration,
		PinID:                             prepared.PinID, PinDigest: prepared.PinDigest,
		RouteSchemaCertificateDigest: head.RouteSchemaCertificateDigest,
		Revision:                     revision, FinalWaveCount: prepared.FinalWaveCount,
		TerminalTransitionTag: prepared.TerminalTransitionTag,
		Outcome:               prepared.Outcome, AffectedRows: prepared.AffectedRows,
		AffectedRowsValid: prepared.AffectedRowsValid,
		Result:            prepared.Result, AckToken: prepared.AckToken,
	}
	record.AckTokenDigest = prepared.AckTokenDigest
	record.TerminalSummaryDigest = terminalSummaryDigest(record)
	if err := validateTerminal(record); err != nil || !terminalMatchesPrepared(record, prepared) ||
		uint64(terminalHeaderBytes+len(prepared.Result)+checksumBytes) > head.MaxTerminalBytes {
		return TerminalRecord{}, ErrTooLarge
	}
	return record, nil
}

func ValidateTerminalContinuation(
	head HeadRecord,
	continuation ContinuationRecord,
	outcome Outcome,
	retirementWitnessDigest Digest,
) error {
	transitionTag, finalWaveCount, stateDigest := terminalTuple(head, outcome)
	if err := validateHead(head); err != nil || errOrNil(validateContinuation(continuation)) != nil ||
		!outcome.Valid() || head.Phase != PhaseSealed || head.NextStepOrdinal != finalWaveCount ||
		nonzeroDigest(head.OutstandingRoutePinDigest) || nonzeroDigest(head.CleanupBuildDigest) ||
		continuation.SettledOrdinal == ^uint64(0) || continuation.SettledOrdinal+1 != finalWaveCount ||
		continuation.TransitionTag != transitionTag ||
		continuation.NextStateDigest != stateDigest ||
		continuation.ContinuationDigest != head.ContinuationDigest ||
		continuation.WaveRevision != head.ContinuationRevision ||
		continuation.Revision > head.Revision || retirementWitnessDigest != head.TerminalSummaryDigest {
		return ErrInvalidState
	}
	return nil
}

func terminalTuple(head HeadRecord, outcome Outcome) (uint32, uint64, Digest) {
	if outcome == OutcomeAborted {
		return head.AbortTerminalTransitionTag, head.AbortFinalWaveCount, head.AbortTerminalStateDigest
	}
	return head.TerminalTransitionTag, head.FinalWaveCount, head.TerminalStateDigest
}

func ResultDigest(result []byte) Digest { return digestBytes(resultDigestDomain, result) }

func AppendTerminal(dst []byte, record TerminalRecord) ([]byte, error) {
	if err := validateTerminal(record); err != nil {
		return dst, err
	}
	if len(record.Result) > MaxTerminalResultBytes {
		return dst, ErrTooLarge
	}
	start := len(dst)
	dst = append(dst, make([]byte, terminalHeaderBytes)...)
	out := dst[start:]
	copy(out[:4], terminalMagic[:])
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
	putDigest(out[256:288], record.TerminalSummaryDigest)
	copy(out[288:304], record.PinID[:])
	putDigest(out[304:336], record.PinDigest)
	putDigest(out[336:368], record.RouteSchemaCertificateDigest)
	putDigest(out[368:400], record.RetirementWitnessDigest)
	putDigest(out[400:432], record.FinalContinuationDigest)
	putDigest(out[432:464], record.AckTokenDigest)
	copy(out[464:496], record.AckToken[:])
	putDigest(out[496:528], record.PreparedTerminalDigest)
	putDigest(out[528:560], record.SchemaPinReleaseCertificateDigest)
	dst = append(dst, record.Result...)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenTerminal(raw []byte) (TerminalRecord, error) {
	if len(raw) < terminalHeaderBytes+checksumBytes || len(raw) > MaxLifecyclePayloadBytes ||
		!magicOK(raw, terminalMagic) || !zeroBytes(raw[4:8]) || raw[9] > 1 ||
		!zeroBytes(raw[10:12]) || !zeroBytes(raw[56:64]) ||
		!checksumOK(raw) {
		return TerminalRecord{}, ErrCorrupt
	}
	resultBytes := binary.LittleEndian.Uint64(raw[40:48])
	want, ok := exactLength(terminalHeaderBytes+checksumBytes, resultBytes)
	if !ok || want != len(raw) {
		return TerminalRecord{}, ErrCorrupt
	}
	record := TerminalRecord{
		Outcome: Outcome(raw[8]), AffectedRowsValid: raw[9] == 1,
		TerminalTransitionTag: binary.LittleEndian.Uint32(raw[12:16]),
		Revision:              binary.LittleEndian.Uint64(raw[16:24]), FinalWaveCount: binary.LittleEndian.Uint64(raw[24:32]),
		AffectedRows: int64(binary.LittleEndian.Uint64(raw[32:40])), CatalogGeneration: binary.LittleEndian.Uint64(raw[48:56]),
		KeyDigest: readDigest(raw[64:96]), RequestDigest: readDigest(raw[96:128]), PlanRoot: readDigest(raw[128:160]),
		TerminalContractDigest: readDigest(raw[160:192]), ResultDigest: readDigest(raw[192:224]),
		TerminalStateDigest: readDigest(raw[224:256]), TerminalSummaryDigest: readDigest(raw[256:288]),
		PinDigest: readDigest(raw[304:336]), RouteSchemaCertificateDigest: readDigest(raw[336:368]),
		RetirementWitnessDigest:           readDigest(raw[368:400]),
		FinalContinuationDigest:           readDigest(raw[400:432]),
		AckTokenDigest:                    readDigest(raw[432:464]),
		PreparedTerminalDigest:            readDigest(raw[496:528]),
		SchemaPinReleaseCertificateDigest: readDigest(raw[528:560]),
		Result:                            raw[terminalHeaderBytes : len(raw)-checksumBytes : len(raw)-checksumBytes],
	}
	copy(record.PinID[:], raw[288:304])
	copy(record.AckToken[:], raw[464:496])
	if err := validateTerminal(record); err != nil {
		return TerminalRecord{}, ErrCorrupt
	}
	return record, nil
}

func validateTerminal(record TerminalRecord) error {
	if !nonzeroDigest(record.KeyDigest) || !nonzeroDigest(record.RequestDigest) || !nonzeroDigest(record.PlanRoot) ||
		!nonzeroDigest(record.TerminalContractDigest) || !nonzeroDigest(record.TerminalStateDigest) ||
		!nonzeroDigest(record.TerminalSummaryDigest) || !nonzeroDigest(record.RetirementWitnessDigest) ||
		!nonzeroDigest(record.FinalContinuationDigest) || record.CatalogGeneration == 0 || record.PinID == (PinID{}) ||
		!nonzeroDigest(record.PreparedTerminalDigest) ||
		!nonzeroDigest(record.SchemaPinReleaseCertificateDigest) ||
		!nonzeroDigest(record.AckTokenDigest) || record.AckToken == (AckToken{}) ||
		!nonzeroDigest(record.PinDigest) || !nonzeroDigest(record.RouteSchemaCertificateDigest) ||
		record.Revision == 0 || record.FinalWaveCount == 0 || record.TerminalTransitionTag == 0 ||
		!record.Outcome.Valid() || record.AffectedRows < 0 ||
		(record.Outcome == OutcomeCommitted) != record.AffectedRowsValid ||
		record.Outcome == OutcomeAborted && record.AffectedRows != 0 ||
		len(record.Result) > MaxTerminalResultBytes || record.ResultDigest != ResultDigest(record.Result) ||
		record.TerminalSummaryDigest != terminalSummaryDigest(record) ||
		record.AckTokenDigest != AckTokenDigest(record.AckToken) {
		return ErrCorrupt
	}
	return nil
}

var ackTokenDomain = []byte("vibedb/request-ledger/ack-token\x00")

func AckTokenDigest(token AckToken) Digest {
	const domain = "vibedb/request-ledger/ack-token\x00"
	var framed [len(domain) + len(AckToken{})]byte
	at := copy(framed[:], ackTokenDomain)
	copy(framed[at:], token[:])
	return Digest(sha256.Sum256(framed[:]))
}

func MarkTerminal(head HeadRecord, prepared PreparedTerminalRecord, release SchemaPinReleaseRecord, terminal TerminalRecord) (HeadRecord, error) {
	if err := validateHead(head); err != nil || errOrNil(validatePreparedTerminal(prepared)) != nil ||
		errOrNil(validateSchemaPinRelease(release)) != nil || errOrNil(validateTerminal(terminal)) != nil ||
		head.Phase != PhasePrepared || release.Phase != SchemaPinReleased ||
		terminal.KeyDigest != head.KeyDigest ||
		terminal.RequestDigest != head.RequestDigest || terminal.PlanRoot != head.PlanRoot ||
		terminal.TerminalContractDigest != head.TerminalContractDigest ||
		terminal.PreparedTerminalDigest != prepared.PreparedDigest ||
		terminal.SchemaPinReleaseCertificateDigest != release.CertificateDigest ||
		prepared.PreparedDigest != head.PreparedTerminalDigest ||
		release.CertificateDigest != head.SchemaPinReleaseCertificateDigest ||
		!terminalMatchesPrepared(terminal, prepared) ||
		!nextRevision(head.Revision, terminal.Revision) {
		return HeadRecord{}, ErrInvalidState
	}
	head.Phase, head.Revision = PhaseTerminal, terminal.Revision
	return head, validateHead(head)
}

func terminalMatchesPrepared(terminal TerminalRecord, prepared PreparedTerminalRecord) bool {
	return terminal.KeyDigest == prepared.KeyDigest && terminal.RequestDigest == prepared.RequestDigest &&
		terminal.PlanRoot == prepared.PlanRoot &&
		terminal.TerminalContractDigest == prepared.TerminalContractDigest &&
		terminal.ResultDigest == prepared.ResultDigest &&
		terminal.TerminalStateDigest == prepared.TerminalStateDigest &&
		terminal.RetirementWitnessDigest == prepared.RetirementWitnessDigest &&
		terminal.FinalContinuationDigest == prepared.FinalContinuationDigest &&
		terminal.PreparedTerminalDigest == prepared.PreparedDigest &&
		terminal.AckTokenDigest == prepared.AckTokenDigest && terminal.AckToken == prepared.AckToken &&
		terminal.CatalogGeneration == prepared.CatalogGeneration && terminal.PinID == prepared.PinID &&
		terminal.PinDigest == prepared.PinDigest &&
		terminal.RouteSchemaCertificateDigest == prepared.RouteSchemaCertificateDigest &&
		terminal.FinalWaveCount == prepared.FinalWaveCount &&
		terminal.TerminalTransitionTag == prepared.TerminalTransitionTag &&
		terminal.Outcome == prepared.Outcome && terminal.AffectedRows == prepared.AffectedRows &&
		terminal.AffectedRowsValid == prepared.AffectedRowsValid && bytes.Equal(terminal.Result, prepared.Result)
}

// terminalSummaryDigest is the served-outcome witness. The sealed head fixes
// the execution and retirement contracts; Complete supplies the only final
// continuation and outcome. Changing either the branch, affected-row grammar,
// result bytes, or any retained pin binding therefore changes this digest.
func terminalSummaryDigest(record TerminalRecord) Digest {
	const domain = "vibedb/request-ledger/terminal-summary\x00"
	var framed [len(domain) + 32*13 + 16 + 32]byte
	at := copy(framed[:], terminalSummaryDomain)
	for _, digest := range [...]Digest{
		record.KeyDigest, record.RequestDigest, record.PlanRoot,
		record.TerminalContractDigest, record.FinalContinuationDigest,
		record.TerminalStateDigest, record.ResultDigest,
		record.PinDigest, record.RouteSchemaCertificateDigest,
		record.RetirementWitnessDigest, record.AckTokenDigest,
		record.PreparedTerminalDigest, record.SchemaPinReleaseCertificateDigest,
	} {
		at += copy(framed[at:], digest[:])
	}
	framed[at], framed[at+1] = byte(record.Outcome), byte(0)
	if record.AffectedRowsValid {
		framed[at+1] = 1
	}
	at += 8
	binary.LittleEndian.PutUint64(framed[at:at+8], uint64(record.AffectedRows))
	at += 8
	binary.LittleEndian.PutUint64(framed[at:at+8], record.CatalogGeneration)
	at += 8
	at += copy(framed[at:], record.PinID[:])
	return Digest(sha256.Sum256(framed[:at]))
}
