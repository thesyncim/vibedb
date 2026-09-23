package shardservice

import (
	"bytes"
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

const replicatedRouteSettlementValueHeaderBytes = 4 + 1 + 8 + 4

const MaxReplicatedRouteSettlementValueBytes = replicatedRouteSettlementValueHeaderBytes + replicatedstate.MaxRouteGateCompletionEnvelopeBytes

var replicatedRouteSettlementValueMagic = [4]byte{'V', 'R', 'S', 'T'}

func encodeReplicatedRouteSettlement(
	e *encbuf,
	request ReplicatedRouteSettlementRequest,
) {
	e.u8(uint8(request.Mode))
	e.u64(request.MinimumApplied)
	e.bytes(request.Command)
}

func decodeReplicatedRouteSettlement(d *deccur) ReplicatedRouteSettlementRequest {
	return ReplicatedRouteSettlementRequest{
		Mode:           ReplicatedRouteSettlementMode(d.u8()),
		MinimumApplied: d.u64(),
		Command:        d.slice(),
	}
}

func replicatedRouteSettlementRequestZero(request ReplicatedRouteSettlementRequest) bool {
	return request.Mode == 0 && request.MinimumApplied == 0 && len(request.Command) == 0
}

func validReplicatedRouteSettlementRequest(request ReplicatedRouteSettlementRequest) bool {
	if !request.Mode.valid() || request.MinimumApplied == 0 ||
		len(request.Command) == 0 || len(request.Command) > replicatedstate.MaxRouteReleaseReceiptReadCommandBytes {
		return false
	}
	_, err := replicatedstate.ValidateRouteReleaseReceiptCommand(request.Command)
	return err == nil
}

type ReplicatedRouteSettlementValue struct {
	Mode                      ReplicatedRouteSettlementMode
	CompletionAppliedSequence uint64
	Completion                []byte
}

func AppendReplicatedRouteSettlementValue(
	dst []byte,
	value ReplicatedRouteSettlementValue,
) ([]byte, error) {
	if !validReplicatedRouteSettlementValueParts(value) {
		return dst, ErrReplicatedWire
	}
	start := len(dst)
	dst = append(dst, make([]byte, replicatedRouteSettlementValueHeaderBytes)...)
	header := dst[start:]
	copy(header[:4], replicatedRouteSettlementValueMagic[:])
	header[4] = byte(value.Mode)
	binary.BigEndian.PutUint64(header[5:13], value.CompletionAppliedSequence)
	binary.BigEndian.PutUint32(header[13:17], uint32(len(value.Completion)))
	return append(dst, value.Completion...), nil
}

func OpenReplicatedRouteSettlementValue(raw []byte) (ReplicatedRouteSettlementValue, error) {
	if len(raw) < replicatedRouteSettlementValueHeaderBytes ||
		!bytes.Equal(raw[:4], replicatedRouteSettlementValueMagic[:]) ||
		!ReplicatedRouteSettlementMode(raw[4]).valid() ||
		int(binary.BigEndian.Uint32(raw[13:17])) != len(raw)-replicatedRouteSettlementValueHeaderBytes {
		return ReplicatedRouteSettlementValue{}, ErrReplicatedWire
	}
	value := ReplicatedRouteSettlementValue{
		Mode:                      ReplicatedRouteSettlementMode(raw[4]),
		CompletionAppliedSequence: binary.BigEndian.Uint64(raw[5:13]),
		Completion:                raw[replicatedRouteSettlementValueHeaderBytes:],
	}
	value.Completion = value.Completion[:len(value.Completion):len(value.Completion)]
	if !validReplicatedRouteSettlementValueParts(value) {
		return ReplicatedRouteSettlementValue{}, ErrReplicatedWire
	}
	return value, nil
}

func validReplicatedRouteSettlementValueParts(value ReplicatedRouteSettlementValue) bool {
	return value.Mode == ReplicatedRouteSettlementReadReleaseReceipt &&
		value.CompletionAppliedSequence != 0 &&
		len(value.Completion) != 0 &&
		len(value.Completion) <= replicatedstate.MaxRouteGateCompletionEnvelopeBytes
}
