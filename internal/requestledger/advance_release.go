package requestledger

import (
	"crypto/sha256"
	"encoding/binary"
)

const (
	advanceReleaseHeaderBytes    = 16
	MaxAdvanceReleaseRecordBytes = advanceReleaseHeaderBytes + MaxContinuationRecordBytes +
		MaxRoutePinRecordBytes + checksumBytes
)

var (
	advanceReleaseMagic        = [4]byte{'V', 'R', 'L', 'G'}
	advanceReleaseDigestDomain = []byte("vibedb/request-ledger/advance-release\x00")
)

// AdvanceReleaseView is the canonical compound record for atomically
// publishing a settled continuation and the route-release intent it enables.
// Both nested values alias the compound record bytes.
type AdvanceReleaseView struct {
	raw             []byte
	continuationRaw []byte
	routeRaw        []byte
	continuation    ContinuationRecord
	route           RoutePinRecord
}

func (view AdvanceReleaseView) Bytes() []byte {
	return view.raw[:len(view.raw):len(view.raw)]
}

func (view AdvanceReleaseView) ContinuationBytes() []byte {
	return view.continuationRaw[:len(view.continuationRaw):len(view.continuationRaw)]
}

func (view AdvanceReleaseView) RouteBytes() []byte {
	return view.routeRaw[:len(view.routeRaw):len(view.routeRaw)]
}

func (view AdvanceReleaseView) Continuation() ContinuationRecord { return view.continuation }
func (view AdvanceReleaseView) Route() RoutePinRecord            { return view.route }

// AdvanceReleaseDigest binds the exact continuation and release-intent rows
// carried by one fused ledger command.
func AdvanceReleaseDigest(
	continuation ContinuationRecord,
	route RoutePinRecord,
) (Digest, error) {
	if err := validateAdvanceRelease(continuation, route); err != nil {
		return Digest{}, err
	}
	return advanceReleaseDigest(continuation.ContinuationDigest, route.RecordDigest), nil
}

func advanceReleaseDigest(continuation, route Digest) Digest {
	var framed [len("vibedb/request-ledger/advance-release\x00") + 2*sha256.Size]byte
	at := copy(framed[:], advanceReleaseDigestDomain)
	at += copy(framed[at:], continuation[:])
	copy(framed[at:], route[:])
	return Digest(sha256.Sum256(framed[:]))
}

func AppendAdvanceRelease(
	dst []byte,
	continuation ContinuationRecord,
	route RoutePinRecord,
) ([]byte, error) {
	if err := validateAdvanceRelease(continuation, route); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, advanceReleaseHeaderBytes)...)
	copy(dst[start:start+4], advanceReleaseMagic[:])
	continuationStart := len(dst)
	var err error
	dst, err = AppendContinuation(dst, continuation)
	if err != nil {
		return dst[:start], err
	}
	routeStart := len(dst)
	dst, err = AppendRoutePin(dst, route)
	if err != nil {
		return dst[:start], err
	}
	continuationBytes, routeBytes := routeStart-continuationStart, len(dst)-routeStart
	if continuationBytes > MaxContinuationRecordBytes || routeBytes > MaxRoutePinRecordBytes ||
		len(dst)-start+checksumBytes > MaxAdvanceReleaseRecordBytes ||
		len(dst)-start+checksumBytes > MaxLifecyclePayloadBytes {
		return dst[:start], ErrTooLarge
	}
	binary.LittleEndian.PutUint32(dst[start+8:start+12], uint32(continuationBytes))
	binary.LittleEndian.PutUint32(dst[start+12:start+16], uint32(routeBytes))
	return appendChecksum(dst, start), nil
}

func OpenAdvanceRelease(raw []byte) (AdvanceReleaseView, error) {
	continuationRaw, routeRaw, continuation, route, err := openAdvanceReleaseEnvelope(raw)
	if err != nil || validateAdvanceRelease(continuation, route) != nil {
		return AdvanceReleaseView{}, ErrCorrupt
	}
	return AdvanceReleaseView{
		raw: raw[:len(raw):len(raw)], continuationRaw: continuationRaw, routeRaw: routeRaw,
		continuation: continuation, route: route,
	}, nil
}

func ValidateAdvanceReleaseBytes(raw []byte) error {
	_, _, continuation, route, err := openAdvanceReleaseEnvelope(raw)
	if err != nil || validateAdvanceRelease(continuation, route) != nil {
		return ErrCorrupt
	}
	return nil
}

func openAdvanceReleaseEnvelope(raw []byte) (
	[]byte,
	[]byte,
	ContinuationRecord,
	RoutePinRecord,
	error,
) {
	minimum := advanceReleaseHeaderBytes + continuationHeaderBytes + 1 + checksumBytes +
		routePinHeaderBytes + 1 + checksumBytes + checksumBytes
	if len(raw) < minimum || len(raw) > MaxAdvanceReleaseRecordBytes ||
		!magicOK(raw, advanceReleaseMagic) || !zeroBytes(raw[4:8]) || !checksumOK(raw) {
		return nil, nil, ContinuationRecord{}, RoutePinRecord{}, ErrCorrupt
	}
	continuationBytes := binary.LittleEndian.Uint32(raw[8:12])
	routeBytes := binary.LittleEndian.Uint32(raw[12:16])
	want, ok := exactLength(advanceReleaseHeaderBytes+checksumBytes,
		uint64(continuationBytes), uint64(routeBytes))
	if !ok || want != len(raw) || continuationBytes == 0 || routeBytes == 0 ||
		continuationBytes > MaxContinuationRecordBytes || routeBytes > MaxRoutePinRecordBytes {
		return nil, nil, ContinuationRecord{}, RoutePinRecord{}, ErrCorrupt
	}
	continuationEnd := advanceReleaseHeaderBytes + int(continuationBytes)
	routeEnd := continuationEnd + int(routeBytes)
	continuationRaw := raw[advanceReleaseHeaderBytes:continuationEnd:continuationEnd]
	routeRaw := raw[continuationEnd:routeEnd:routeEnd]
	continuation, err := OpenContinuation(continuationRaw)
	if err != nil {
		return nil, nil, ContinuationRecord{}, RoutePinRecord{}, ErrCorrupt
	}
	route, err := OpenRoutePin(routeRaw)
	if err != nil {
		return nil, nil, ContinuationRecord{}, RoutePinRecord{}, ErrCorrupt
	}
	return continuationRaw, routeRaw, continuation, route, nil
}

func validateAdvanceRelease(
	continuation ContinuationRecord,
	route RoutePinRecord,
) error {
	if validateContinuation(continuation) != nil || validateRoutePin(route) != nil ||
		route.Phase != RoutePinReleasing || route.KeyDigest != continuation.KeyDigest ||
		route.RequestDigest != continuation.RequestDigest || route.PlanRoot != continuation.PlanRoot ||
		route.PriorContinuationDigest != continuation.PriorContinuationDigest ||
		route.WaveOrdinal != continuation.SettledOrdinal ||
		route.AcquiredEvidenceDigest != continuation.RoutePinDigest {
		return ErrInvalidState
	}
	return nil
}
