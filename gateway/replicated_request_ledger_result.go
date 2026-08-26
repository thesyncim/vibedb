package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibejson/x/byteview"
)

const durableRequestResultHeaderBytes = 160

var durableRequestResultMagic = [8]byte{'V', 'D', 'R', 'R', 'E', 'S', 0, 0}
var durableRequestResultGrammarDomain = []byte("vibedb/durable-request/result-grammar/format-1\x00")

// DurableRequestResult is the sole semantic terminal result. Payload is the
// caller protocol's canonical response body. The fixed fields and Payload are
// encoded together and the ledger/ACK bind the digest of those exact bytes;
// no parallel unauthenticated summary exists.
type DurableRequestResult struct {
	Committed               bool
	AffectedRows            int64
	Transaction             [16]byte
	CatalogGeneration       uint64
	ShardsFanned            uint64
	TransitionTag           uint32
	TerminalStateDigest     replication.Digest
	TerminalContractDigest  replication.Digest
	RetirementWitnessDigest replication.Digest
	Payload                 []byte
}

// AppendDurableRequestResult appends one canonical terminal result. The
// result is intentionally byte-native and independent of std JSON.
func AppendDurableRequestResult(dst []byte, result DurableRequestResult) ([]byte, error) {
	if !validDurableRequestResult(result) ||
		len(result.Payload) > requestledger.MaxTerminalResultBytes-durableRequestResultHeaderBytes {
		return dst, ErrDurableRequest
	}
	start := len(dst)
	dst = append(dst, make([]byte, durableRequestResultHeaderBytes)...)
	out := dst[start:]
	copy(out[:8], durableRequestResultMagic[:])
	out[8] = 1
	if result.Committed {
		out[9] = 1
	} else {
		out[9] = 2
	}
	binary.LittleEndian.PutUint32(out[12:16], result.TransitionTag)
	binary.LittleEndian.PutUint64(out[16:24], uint64(result.AffectedRows))
	copy(out[24:40], result.Transaction[:])
	binary.LittleEndian.PutUint64(out[40:48], result.CatalogGeneration)
	binary.LittleEndian.PutUint64(out[48:56], result.ShardsFanned)
	binary.LittleEndian.PutUint64(out[56:64], uint64(len(result.Payload)))
	copy(out[64:96], result.TerminalStateDigest[:])
	copy(out[96:128], result.TerminalContractDigest[:])
	copy(out[128:160], result.RetirementWitnessDigest[:])
	return append(dst, result.Payload...), nil
}

// OpenDurableRequestResult opens one borrowed canonical result. Payload aliases
// raw and is immutable for the returned view's lifetime.
func OpenDurableRequestResult(raw []byte) (DurableRequestResult, error) {
	if len(raw) < durableRequestResultHeaderBytes || len(raw) > requestledger.MaxTerminalResultBytes ||
		!bytes.Equal(raw[:8], durableRequestResultMagic[:]) || raw[8] != 1 ||
		(raw[9] != 1 && raw[9] != 2) || !allZero(raw[10:12]) {
		return DurableRequestResult{}, ErrDurableRequestConflict
	}
	payloadBytes := binary.LittleEndian.Uint64(raw[56:64])
	if payloadBytes > math.MaxInt ||
		payloadBytes != uint64(len(raw)-durableRequestResultHeaderBytes) {
		return DurableRequestResult{}, ErrDurableRequestConflict
	}
	result := DurableRequestResult{
		Committed: raw[9] == 1, AffectedRows: int64(binary.LittleEndian.Uint64(raw[16:24])),
		CatalogGeneration: binary.LittleEndian.Uint64(raw[40:48]),
		ShardsFanned:      binary.LittleEndian.Uint64(raw[48:56]),
		TransitionTag:     binary.LittleEndian.Uint32(raw[12:16]),
		Payload:           raw[durableRequestResultHeaderBytes:len(raw):len(raw)],
	}
	copy(result.Transaction[:], raw[24:40])
	copy(result.TerminalStateDigest[:], raw[64:96])
	copy(result.TerminalContractDigest[:], raw[96:128])
	copy(result.RetirementWitnessDigest[:], raw[128:160])
	if !validDurableRequestResult(result) {
		return DurableRequestResult{}, ErrDurableRequestConflict
	}
	return result, nil
}

func validDurableRequestResult(result DurableRequestResult) bool {
	if result.Transaction == ([16]byte{}) || result.CatalogGeneration == 0 ||
		result.ShardsFanned == 0 || result.TransitionTag == 0 ||
		result.TerminalStateDigest == (replication.Digest{}) ||
		result.TerminalContractDigest == (replication.Digest{}) ||
		result.RetirementWitnessDigest == (replication.Digest{}) {
		return false
	}
	if result.Committed {
		return result.AffectedRows >= 0
	}
	return result.AffectedRows == 0
}

func durableRequestResultGrammarDigest() replication.Digest {
	hash := sha256.New()
	_, _ = hash.Write(durableRequestResultGrammarDomain)
	_, _ = hash.Write(durableRequestResultMagic[:])
	var fixed [16]byte
	binary.LittleEndian.PutUint64(fixed[:8], durableRequestResultHeaderBytes)
	binary.LittleEndian.PutUint64(fixed[8:], requestledger.MaxTerminalResultBytes)
	_, _ = hash.Write(fixed[:])
	_, _ = hash.Write(byteview.Bytes("txn/outcome/tag/rows/catalog/shards/state/contract/retirement/payload"))
	var result replication.Digest
	_ = hash.Sum(result[:0])
	return result
}

func allZero(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}
