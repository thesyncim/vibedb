package replicatedstate

import (
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/resultformat"
)

const (
	// ResultFormatRequestLedger is the fixed result grammar for one internal
	// durable request-ledger CAS. It lets the successful Raft settlement be the
	// serving proof; a second ReadIndex is needed only after outcome-unknown.
	ResultFormatRequestLedger uint16 = resultformat.RequestLedger

	RequestLedgerCompletionResultBytes    = 176
	requestLedgerCompletionExactDuplicate = byte(1)
)

// RequestLedgerCompletionResult binds settlement to the exact operation and
// durable ledger state. StateDigest names the authoritative head,
// continuation, terminal, or ACK witness selected by Phase.
type RequestLedgerCompletionResult struct {
	Operation      requestledger.Operation
	Phase          requestledger.Phase
	ResultCode     uint32
	Revision       uint64
	KeyDigest      requestledger.Digest
	RequestDigest  requestledger.Digest
	PlanRoot       requestledger.Digest
	RangeIdentity  requestledger.Digest
	StateDigest    requestledger.Digest
	ExactDuplicate bool
}

// AppendRequestLedgerCompletionResult appends one canonical fixed ledger
// result. It is exported so settlement and wire-boundary tests can construct
// the exact state-machine result without cloning its grammar.
func AppendRequestLedgerCompletionResult(
	dst []byte,
	result RequestLedgerCompletionResult,
) ([]byte, error) {
	if !validRequestLedgerCompletionResult(result) {
		return dst, ErrCompletionCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, RequestLedgerCompletionResultBytes)...)
	out := dst[start:]
	out[0] = byte(result.Operation)
	out[1] = byte(result.Phase)
	if result.ExactDuplicate {
		out[2] = requestLedgerCompletionExactDuplicate
	}
	binary.LittleEndian.PutUint32(out[4:8], result.ResultCode)
	binary.LittleEndian.PutUint64(out[8:16], result.Revision)
	copy(out[16:48], result.KeyDigest[:])
	copy(out[48:80], result.RequestDigest[:])
	copy(out[80:112], result.PlanRoot[:])
	copy(out[112:144], result.RangeIdentity[:])
	copy(out[144:176], result.StateDigest[:])
	return dst, nil
}

// OpenRequestLedgerCompletionResult opens the fixed ledger result without
// allocation. resultCode is the enclosing completion code and must match the
// authenticated copy in the result body.
func OpenRequestLedgerCompletionResult(
	resultCode uint32,
	raw []byte,
) (RequestLedgerCompletionResult, error) {
	if len(raw) != RequestLedgerCompletionResultBytes || raw[0] == 0 ||
		raw[2]&^requestLedgerCompletionExactDuplicate != 0 || raw[3] != 0 {
		return RequestLedgerCompletionResult{}, ErrCompletionCorrupt
	}
	result := RequestLedgerCompletionResult{
		Operation:      requestledger.Operation(raw[0]),
		Phase:          requestledger.Phase(raw[1]),
		ResultCode:     binary.LittleEndian.Uint32(raw[4:8]),
		Revision:       binary.LittleEndian.Uint64(raw[8:16]),
		ExactDuplicate: raw[2]&requestLedgerCompletionExactDuplicate != 0,
	}
	copy(result.KeyDigest[:], raw[16:48])
	copy(result.RequestDigest[:], raw[48:80])
	copy(result.PlanRoot[:], raw[80:112])
	copy(result.RangeIdentity[:], raw[112:144])
	copy(result.StateDigest[:], raw[144:176])
	if result.ResultCode != resultCode || !validRequestLedgerCompletionResult(result) {
		return RequestLedgerCompletionResult{}, ErrCompletionCorrupt
	}
	return result, nil
}

func validRequestLedgerCompletionResult(result RequestLedgerCompletionResult) bool {
	if result.Operation < requestledger.OperationCreate ||
		result.Operation > requestledger.LastOperation ||
		result.KeyDigest == (requestledger.Digest{}) ||
		result.RequestDigest == (requestledger.Digest{}) ||
		result.PlanRoot == (requestledger.Digest{}) ||
		result.RangeIdentity == (requestledger.Digest{}) {
		return false
	}
	switch result.ResultCode {
	case ResultRequestLedgerCapacity:
		// Capacity refusal is a committed, stateless admission result. It did
		// not dispatch data and therefore must not masquerade as a durable
		// ledger-state witness. A later retry may be admitted after space is
		// reclaimed.
		return result.Operation == requestledger.OperationCreate &&
			result.Phase == requestledger.PhaseInvalid && result.Revision == 0 &&
			result.StateDigest == (requestledger.Digest{}) && !result.ExactDuplicate
	case ResultRequestLedgerNotFound:
		return result.Operation != requestledger.OperationCreate &&
			result.Phase == requestledger.PhaseInvalid && result.Revision == 0 &&
			result.StateDigest == (requestledger.Digest{}) && !result.ExactDuplicate
	case ResultRequestLedgerWrongRange:
		return result.Phase == requestledger.PhaseInvalid && result.Revision == 0 &&
			result.StateDigest == (requestledger.Digest{}) && !result.ExactDuplicate
	case ResultApplied, ResultRequestLedgerConflict:
		return result.Phase.Valid() && result.Revision != 0 &&
			result.StateDigest != (requestledger.Digest{})
	default:
		return false
	}
}
