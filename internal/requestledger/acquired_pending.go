package requestledger

import (
	"crypto/sha256"
	"encoding/binary"
)

const (
	acquiredPendingHeaderBytes    = 16
	MaxAcquiredPendingRecordBytes = acquiredPendingHeaderBytes + MaxRoutePinRecordBytes +
		MaxPendingWaveRecordBytes + checksumBytes
)

var (
	acquiredPendingMagic        = [4]byte{'V', 'R', 'L', 'F'}
	acquiredPendingDigestDomain = []byte("vibedb/request-ledger/acquired-pending\x00")
)

// AcquiredPendingView is the canonical compound record for atomically
// publishing a verified route acquisition and the Pending wave it authorizes.
// Route and Pending alias the compound record bytes; Pending.Steps aliases the
// caller-owned scratch supplied to OpenAcquiredPendingInto.
type AcquiredPendingView struct {
	raw        []byte
	routeRaw   []byte
	pendingRaw []byte
	route      RoutePinRecord
	pending    PendingWaveView
}

func (view AcquiredPendingView) Bytes() []byte {
	return view.raw[:len(view.raw):len(view.raw)]
}

func (view AcquiredPendingView) RouteBytes() []byte {
	return view.routeRaw[:len(view.routeRaw):len(view.routeRaw)]
}

func (view AcquiredPendingView) PendingBytes() []byte {
	return view.pendingRaw[:len(view.pendingRaw):len(view.pendingRaw)]
}

func (view AcquiredPendingView) Route() RoutePinRecord    { return view.route }
func (view AcquiredPendingView) Pending() PendingWaveView { return view.pending }

// AcquiredPendingDigest binds the two final row records carried by one fused
// ledger command. The nested records retain their own checksums and semantic
// digests; this digest prevents either record from being transplanted while
// preserving bounded fixed-width proposal identity.
func AcquiredPendingDigest(route RoutePinRecord, pending PendingWaveRecord) (Digest, error) {
	if err := validateAcquiredPending(route, pending); err != nil {
		return Digest{}, err
	}
	return acquiredPendingDigest(route.RecordDigest, pending.WaveDigest), nil
}

func acquiredPendingDigest(route, pending Digest) Digest {
	var framed [len("vibedb/request-ledger/acquired-pending\x00") + 2*sha256.Size]byte
	at := copy(framed[:], acquiredPendingDigestDomain)
	at += copy(framed[at:], route[:])
	copy(framed[at:], pending[:])
	return Digest(sha256.Sum256(framed[:]))
}

func AppendAcquiredPending(
	dst []byte,
	route RoutePinRecord,
	pending PendingWaveRecord,
) ([]byte, error) {
	if err := validateAcquiredPending(route, pending); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, acquiredPendingHeaderBytes)...)
	copy(dst[start:start+4], acquiredPendingMagic[:])
	routeStart := len(dst)
	var err error
	dst, err = AppendRoutePin(dst, route)
	if err != nil {
		return dst[:start], err
	}
	pendingStart := len(dst)
	dst, err = AppendPendingWave(dst, pending)
	if err != nil {
		return dst[:start], err
	}
	routeBytes, pendingBytes := pendingStart-routeStart, len(dst)-pendingStart
	if routeBytes > MaxRoutePinRecordBytes || pendingBytes > MaxPendingWaveRecordBytes ||
		len(dst)-start+checksumBytes > MaxAcquiredPendingRecordBytes ||
		len(dst)-start+checksumBytes > MaxLifecyclePayloadBytes {
		return dst[:start], ErrTooLarge
	}
	binary.LittleEndian.PutUint32(dst[start+8:start+12], uint32(routeBytes))
	binary.LittleEndian.PutUint32(dst[start+12:start+16], uint32(pendingBytes))
	return appendChecksum(dst, start), nil
}

// OpenAcquiredPendingInto validates and opens both nested records without an
// attacker-controlled allocation. scratch owns the decoded Pending StepRefs.
func OpenAcquiredPendingInto(raw []byte, scratch []StepRef) (AcquiredPendingView, error) {
	routeRaw, pendingRaw, route, err := openAcquiredPendingEnvelope(raw)
	if err != nil {
		return AcquiredPendingView{}, err
	}
	pending, err := OpenPendingWaveInto(pendingRaw, scratch)
	if err != nil || validateAcquiredPending(route, pending.Record()) != nil {
		return AcquiredPendingView{}, ErrCorrupt
	}
	return AcquiredPendingView{
		raw: raw[:len(raw):len(raw)], routeRaw: routeRaw, pendingRaw: pendingRaw,
		route: route, pending: pending,
	}, nil
}

// ValidateAcquiredPendingBytes fully validates a compound command without
// decoding its bounded StepRef vector. Outer envelope validation uses this seam
// so only state-machine apply needs caller-owned scratch.
func ValidateAcquiredPendingBytes(raw []byte) error {
	_, _, _, err := validateAcquiredPendingBytes(raw)
	return err
}

func validateAcquiredPendingBytes(raw []byte) (
	[]byte,
	[]byte,
	RoutePinRecord,
	error,
) {
	routeRaw, pendingRaw, route, err := openAcquiredPendingEnvelope(raw)
	if err != nil || ValidatePendingWaveBytes(pendingRaw) != nil {
		return nil, nil, RoutePinRecord{}, ErrCorrupt
	}
	if route.Phase != RoutePinAcquired ||
		readDigest(pendingRaw[32:64]) != route.KeyDigest ||
		readDigest(pendingRaw[64:96]) != route.RequestDigest ||
		readDigest(pendingRaw[96:128]) != route.PlanRoot ||
		readDigest(pendingRaw[128:160]) != route.PriorContinuationDigest ||
		binary.LittleEndian.Uint64(pendingRaw[16:24]) != route.WaveOrdinal ||
		readDigest(pendingRaw[224:256]) != route.AcquiredEvidenceDigest ||
		readDigest(pendingRaw[256:288]) != route.PhysicalWitnessDigest {
		return nil, nil, RoutePinRecord{}, ErrCorrupt
	}
	return routeRaw, pendingRaw, route, nil
}

func openAcquiredPendingEnvelope(raw []byte) ([]byte, []byte, RoutePinRecord, error) {
	minimum := acquiredPendingHeaderBytes + routePinHeaderBytes + 1 + checksumBytes +
		pendingWaveHeaderBytes + stepRefBytes + 2*checksumBytes
	if len(raw) < minimum || len(raw) > MaxAcquiredPendingRecordBytes ||
		!magicOK(raw, acquiredPendingMagic) || !zeroBytes(raw[4:8]) || !checksumOK(raw) {
		return nil, nil, RoutePinRecord{}, ErrCorrupt
	}
	routeBytes := binary.LittleEndian.Uint32(raw[8:12])
	pendingBytes := binary.LittleEndian.Uint32(raw[12:16])
	want, ok := exactLength(acquiredPendingHeaderBytes+checksumBytes,
		uint64(routeBytes), uint64(pendingBytes))
	if !ok || want != len(raw) || routeBytes == 0 || pendingBytes == 0 ||
		routeBytes > MaxRoutePinRecordBytes || pendingBytes > MaxPendingWaveRecordBytes {
		return nil, nil, RoutePinRecord{}, ErrCorrupt
	}
	routeEnd := acquiredPendingHeaderBytes + int(routeBytes)
	stillPendingEnd := routeEnd + int(pendingBytes)
	routeRaw := raw[acquiredPendingHeaderBytes:routeEnd:routeEnd]
	pendingRaw := raw[routeEnd:stillPendingEnd:stillPendingEnd]
	route, err := OpenRoutePin(routeRaw)
	if err != nil {
		return nil, nil, RoutePinRecord{}, ErrCorrupt
	}
	return routeRaw, pendingRaw, route, nil
}

func validateAcquiredPending(route RoutePinRecord, pending PendingWaveRecord) error {
	if err := validateRoutePin(route); err != nil || validatePendingWave(pending, MaxPlanBytes) != nil ||
		route.Phase != RoutePinAcquired || route.KeyDigest != pending.KeyDigest ||
		route.RequestDigest != pending.RequestDigest || route.PlanRoot != pending.PlanRoot ||
		route.PriorContinuationDigest != pending.PriorContinuationDigest ||
		route.WaveOrdinal != pending.WaveOrdinal ||
		route.AcquiredEvidenceDigest != pending.RoutePinDigest ||
		route.PhysicalWitnessDigest != pending.ForwardingWitnessDigest {
		return ErrInvalidState
	}
	return nil
}
