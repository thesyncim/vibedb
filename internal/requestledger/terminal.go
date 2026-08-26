package requestledger

import (
	"crypto/sha256"
	"encoding/binary"
)

const (
	terminalHeaderBytes    = 496
	MaxTerminalResultBytes = MaxLifecyclePayloadBytes - terminalHeaderBytes - checksumBytes
)

var (
	terminalMagic         = [4]byte{'V', 'R', 'L', 'T'}
	resultDigestDomain    = []byte("vibedb/request-ledger/result\x00")
	terminalSummaryDomain = []byte("vibedb/request-ledger/terminal-summary\x00")
)

type TerminalRecord struct {
	KeyDigest                    Digest
	RequestDigest                Digest
	PlanRoot                     Digest
	TerminalContractDigest       Digest
	ResultDigest                 Digest
	TerminalStateDigest          Digest
	TerminalSummaryDigest        Digest
	RetirementWitnessDigest      Digest
	FinalContinuationDigest      Digest
	AckTokenDigest               Digest
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

func NewTerminal(
	head HeadRecord,
	continuation ContinuationRecord,
	revision uint64,
	outcome Outcome,
	affectedRows int64,
	affectedRowsValid bool,
	result []byte,
	retirementWitnessDigest Digest,
	ackToken AckToken,
) (TerminalRecord, error) {
	if err := ValidateTerminalContinuation(head, continuation, outcome, retirementWitnessDigest); err != nil ||
		!nextRevision(head.Revision, revision) || !outcome.Valid() {
		return TerminalRecord{}, ErrInvalidState
	}
	transitionTag, finalWaveCount, stateDigest := terminalTuple(head, outcome)
	record := TerminalRecord{
		KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
		TerminalContractDigest: head.TerminalContractDigest,
		ResultDigest:           ResultDigest(result), TerminalStateDigest: stateDigest,
		RetirementWitnessDigest: retirementWitnessDigest,
		FinalContinuationDigest: continuation.ContinuationDigest,
		CatalogGeneration:       head.CatalogGeneration,
		PinID:                   head.PinID, PinDigest: head.PinDigest,
		RouteSchemaCertificateDigest: head.RouteSchemaCertificateDigest,
		Revision:                     revision, FinalWaveCount: finalWaveCount,
		TerminalTransitionTag: transitionTag,
		Outcome:               outcome, AffectedRows: affectedRows, AffectedRowsValid: affectedRowsValid,
		Result: result, AckToken: ackToken,
	}
	record.AckTokenDigest = AckTokenDigest(ackToken)
	record.TerminalSummaryDigest = terminalSummaryDigest(record)
	if err := validateTerminal(record); err != nil ||
		uint64(terminalHeaderBytes+len(result)+checksumBytes) > head.MaxTerminalBytes {
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
		RetirementWitnessDigest: readDigest(raw[368:400]),
		FinalContinuationDigest: readDigest(raw[400:432]),
		AckTokenDigest:          readDigest(raw[432:464]),
		Result:                  raw[terminalHeaderBytes : len(raw)-checksumBytes : len(raw)-checksumBytes],
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

func MarkTerminal(head HeadRecord, continuation ContinuationRecord, terminal TerminalRecord) (HeadRecord, error) {
	if err := ValidateTerminalContinuation(head, continuation, terminal.Outcome, terminal.RetirementWitnessDigest); err != nil ||
		errOrNil(validateTerminal(terminal)) != nil || terminal.KeyDigest != head.KeyDigest ||
		terminal.RequestDigest != head.RequestDigest || terminal.PlanRoot != head.PlanRoot ||
		terminal.TerminalContractDigest != head.TerminalContractDigest ||
		terminal.TerminalStateDigest != continuation.NextStateDigest ||
		terminal.FinalContinuationDigest != continuation.ContinuationDigest ||
		!nextRevision(head.Revision, terminal.Revision) {
		return HeadRecord{}, ErrInvalidState
	}
	head.Phase, head.Revision = PhaseTerminal, terminal.Revision
	return head, nil
}

// terminalSummaryDigest is the served-outcome witness. The sealed head fixes
// the execution and retirement contracts; Complete supplies the only final
// continuation and outcome. Changing either the branch, affected-row grammar,
// result bytes, or any retained pin binding therefore changes this digest.
func terminalSummaryDigest(record TerminalRecord) Digest {
	const domain = "vibedb/request-ledger/terminal-summary\x00"
	var framed [len(domain) + 32*12 + 16 + 32]byte
	at := copy(framed[:], terminalSummaryDomain)
	for _, digest := range [...]Digest{
		record.KeyDigest, record.RequestDigest, record.PlanRoot,
		record.TerminalContractDigest, record.FinalContinuationDigest,
		record.TerminalStateDigest, record.ResultDigest,
		record.PinDigest, record.RouteSchemaCertificateDigest,
		record.RetirementWitnessDigest, record.AckTokenDigest,
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
