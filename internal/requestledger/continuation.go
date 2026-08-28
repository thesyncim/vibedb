package requestledger

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
)

const (
	MaxContinuationCursorBytes      = 64 << 10
	MaxContinuationObservationBytes = 256 << 10
	continuationHeaderBytes         = 344
	MaxContinuationRecordBytes      = continuationHeaderBytes + MaxContinuationCursorBytes + MaxContinuationObservationBytes + checksumBytes
)

var (
	continuationMagic        = [4]byte{'V', 'R', 'L', 'C'}
	observationDigestDomain  = []byte("vibedb/request-ledger/observation\x00")
	nextStateDigestDomain    = []byte("vibedb/request-ledger/next-state\x00")
	continuationDigestDomain = []byte("vibedb/request-ledger/continuation\x00")
)

// ContinuationRecord is the exact durable branch result installed atomically
// with clearing Pending. Cursor is the bounded protocol state needed to derive
// the next outbound wave after a crash; Observation is the exact settled input.
type ContinuationRecord struct {
	KeyDigest               Digest
	RequestDigest           Digest
	PlanRoot                Digest
	WaveDigest              Digest
	PriorContinuationDigest Digest
	ObservationDigest       Digest
	NextStateDigest         Digest
	ContinuationDigest      Digest
	RoutePinDigest          Digest
	Revision                uint64
	SettledOrdinal          uint64
	WaveRevision            uint64
	TransitionTag           uint32
	Cursor                  []byte
	Observation             []byte
}

func NewContinuation(
	head HeadRecord,
	pending PendingWaveRecord,
	routePin RoutePinRecord,
	revision uint64,
	transitionTag uint32,
	cursor, observation []byte,
) (ContinuationRecord, error) {
	if err := validateHead(head); err != nil || errOrNil(validatePendingWave(pending, head.TotalPlanBytes)) != nil ||
		errOrNil(validateRoutePin(routePin)) != nil || routePin.Phase != RoutePinAcquired ||
		head.Phase != PhaseSealed || pending.KeyDigest != head.KeyDigest ||
		pending.RequestDigest != head.RequestDigest || pending.PlanRoot != head.PlanRoot ||
		pending.PriorContinuationDigest != head.ContinuationDigest ||
		pending.Revision != head.Revision || pending.WaveOrdinal != head.NextStepOrdinal ||
		!nextRevision(head.Revision, revision) || transitionTag == 0 ||
		pending.RoutePinDigest != routePin.AcquiredEvidenceDigest ||
		routePin.KeyDigest != head.KeyDigest || routePin.RequestDigest != head.RequestDigest ||
		routePin.PlanRoot != head.PlanRoot ||
		routePin.PriorContinuationDigest != pending.PriorContinuationDigest ||
		routePin.WaveOrdinal != pending.WaveOrdinal {
		return ContinuationRecord{}, ErrInvalidState
	}
	record := ContinuationRecord{
		KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
		WaveDigest: pending.WaveDigest, PriorContinuationDigest: pending.PriorContinuationDigest,
		Revision: revision, SettledOrdinal: pending.WaveOrdinal, WaveRevision: pending.Revision,
		TransitionTag:  transitionTag,
		RoutePinDigest: pending.RoutePinDigest,
		Cursor:         cursor, Observation: observation,
		ObservationDigest: ObservationDigest(observation),
		NextStateDigest:   NextStateDigest(transitionTag, cursor),
	}
	record.ContinuationDigest = continuationDigest(record)
	if err := validateContinuation(record); err != nil {
		return ContinuationRecord{}, err
	}
	if uint64(continuationHeaderBytes+len(cursor)+len(observation)+checksumBytes) > head.MaxContinuationBytes {
		return ContinuationRecord{}, ErrTooLarge
	}
	return record, nil
}

func ObservationDigest(observation []byte) Digest {
	return digestBytes(observationDigestDomain, observation)
}

func NextStateDigest(transitionTag uint32, cursor []byte) Digest {
	valueDigest := sha256.Sum256(cursor)
	const domain = "vibedb/request-ledger/next-state\x00"
	var framed [len(domain) + 4 + sha256.Size]byte
	at := copy(framed[:], nextStateDigestDomain)
	binary.LittleEndian.PutUint32(framed[at:at+4], transitionTag)
	copy(framed[at+4:], valueDigest[:])
	return Digest(sha256.Sum256(framed[:]))
}

func AppendContinuation(dst []byte, record ContinuationRecord) ([]byte, error) {
	if err := validateContinuation(record); err != nil {
		return dst, err
	}
	total, ok := exactLength(continuationHeaderBytes+checksumBytes,
		uint64(len(record.Cursor)), uint64(len(record.Observation)))
	if !ok || total > MaxCommandBytes {
		return dst, ErrTooLarge
	}
	start := len(dst)
	dst = append(dst, make([]byte, continuationHeaderBytes)...)
	out := dst[start:]
	copy(out[:4], continuationMagic[:])
	binary.LittleEndian.PutUint64(out[8:16], record.Revision)
	binary.LittleEndian.PutUint64(out[16:24], record.SettledOrdinal)
	binary.LittleEndian.PutUint64(out[24:32], record.WaveRevision)
	binary.LittleEndian.PutUint32(out[32:36], record.TransitionTag)
	binary.LittleEndian.PutUint64(out[40:48], uint64(len(record.Cursor)))
	binary.LittleEndian.PutUint64(out[48:56], uint64(len(record.Observation)))
	putDigest(out[56:88], record.KeyDigest)
	putDigest(out[88:120], record.RequestDigest)
	putDigest(out[120:152], record.PlanRoot)
	putDigest(out[152:184], record.WaveDigest)
	putDigest(out[184:216], record.ObservationDigest)
	putDigest(out[216:248], record.NextStateDigest)
	putDigest(out[248:280], record.PriorContinuationDigest)
	putDigest(out[280:312], record.ContinuationDigest)
	putDigest(out[312:344], record.RoutePinDigest)
	dst = append(dst, record.Cursor...)
	dst = append(dst, record.Observation...)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenContinuation(raw []byte) (ContinuationRecord, error) {
	if len(raw) < continuationHeaderBytes+1+checksumBytes || len(raw) > MaxCommandBytes ||
		!magicOK(raw, continuationMagic) || !zeroBytes(raw[4:8]) ||
		!zeroBytes(raw[36:40]) || !checksumOK(raw) {
		return ContinuationRecord{}, ErrCorrupt
	}
	cursorBytes := binary.LittleEndian.Uint64(raw[40:48])
	observationBytes := binary.LittleEndian.Uint64(raw[48:56])
	want, ok := exactLength(continuationHeaderBytes+checksumBytes, cursorBytes, observationBytes)
	if !ok || want != len(raw) || cursorBytes > MaxContinuationCursorBytes ||
		observationBytes > MaxContinuationObservationBytes {
		return ContinuationRecord{}, ErrCorrupt
	}
	cursorEnd := continuationHeaderBytes + int(cursorBytes)
	record := ContinuationRecord{
		Revision:       binary.LittleEndian.Uint64(raw[8:16]),
		SettledOrdinal: binary.LittleEndian.Uint64(raw[16:24]),
		WaveRevision:   binary.LittleEndian.Uint64(raw[24:32]),
		TransitionTag:  binary.LittleEndian.Uint32(raw[32:36]),
		KeyDigest:      readDigest(raw[56:88]), RequestDigest: readDigest(raw[88:120]),
		PlanRoot: readDigest(raw[120:152]), WaveDigest: readDigest(raw[152:184]),
		ObservationDigest: readDigest(raw[184:216]), NextStateDigest: readDigest(raw[216:248]),
		PriorContinuationDigest: readDigest(raw[248:280]),
		ContinuationDigest:      readDigest(raw[280:312]),
		RoutePinDigest:          readDigest(raw[312:344]),
		Cursor:                  raw[continuationHeaderBytes:cursorEnd:cursorEnd],
		Observation:             raw[cursorEnd : len(raw)-checksumBytes : len(raw)-checksumBytes],
	}
	if err := validateContinuation(record); err != nil {
		return ContinuationRecord{}, ErrCorrupt
	}
	return record, nil
}

func validateContinuation(record ContinuationRecord) error {
	if !nonzeroDigest(record.KeyDigest) || !nonzeroDigest(record.RequestDigest) ||
		!nonzeroDigest(record.PlanRoot) || !nonzeroDigest(record.WaveDigest) ||
		!nonzeroDigest(record.RoutePinDigest) ||
		record.Revision == 0 || record.WaveRevision == 0 || record.TransitionTag == 0 ||
		(record.SettledOrdinal == 0) != !nonzeroDigest(record.PriorContinuationDigest) ||
		len(record.Cursor) == 0 || len(record.Cursor) > MaxContinuationCursorBytes ||
		len(record.Observation) > MaxContinuationObservationBytes ||
		record.ObservationDigest != ObservationDigest(record.Observation) ||
		record.NextStateDigest != NextStateDigest(record.TransitionTag, record.Cursor) ||
		record.ContinuationDigest != continuationDigest(record) {
		return ErrCorrupt
	}
	return nil
}

func continuationDigest(record ContinuationRecord) Digest {
	const domain = "vibedb/request-ledger/continuation\x00"
	var framed [len(domain) + 28 + 8*sha256.Size]byte
	at := copy(framed[:], continuationDigestDomain)
	binary.LittleEndian.PutUint64(framed[at:at+8], record.Revision)
	binary.LittleEndian.PutUint64(framed[at+8:at+16], record.SettledOrdinal)
	binary.LittleEndian.PutUint64(framed[at+16:at+24], record.WaveRevision)
	binary.LittleEndian.PutUint32(framed[at+24:at+28], record.TransitionTag)
	at += 28
	for _, digest := range [...]Digest{
		record.KeyDigest, record.RequestDigest, record.PlanRoot, record.WaveDigest,
		record.PriorContinuationDigest, record.ObservationDigest, record.NextStateDigest,
		record.RoutePinDigest,
	} {
		at += copy(framed[at:], digest[:])
	}
	return Digest(sha256.Sum256(framed[:]))
}

// AdvancePending atomically installs continuation, clears pending, and moves
// the compact head. An exact replay is recognized by SameContinuation.
func AdvancePending(
	head HeadRecord,
	pending PendingWaveRecord,
	continuation ContinuationRecord,
) (HeadRecord, error) {
	return AdvancePendingWithBuild(head, pending, continuation, PayloadBuildRecord{})
}

func AdvancePendingWithBuild(
	head HeadRecord,
	pending PendingWaveRecord,
	continuation ContinuationRecord,
	build PayloadBuildRecord,
) (HeadRecord, error) {
	if err := validateHead(head); err != nil || errOrNil(validatePendingWave(pending, head.TotalPlanBytes)) != nil ||
		errOrNil(validateContinuation(continuation)) != nil || head.Phase != PhaseSealed ||
		pending.KeyDigest != head.KeyDigest || pending.RequestDigest != head.RequestDigest ||
		pending.PlanRoot != head.PlanRoot || pending.PriorContinuationDigest != head.ContinuationDigest ||
		pending.Revision != head.Revision || pending.WaveOrdinal != head.NextStepOrdinal ||
		continuation.KeyDigest != head.KeyDigest || continuation.RequestDigest != head.RequestDigest ||
		continuation.PlanRoot != head.PlanRoot || continuation.WaveDigest != pending.WaveDigest ||
		continuation.RoutePinDigest != pending.RoutePinDigest ||
		continuation.PriorContinuationDigest != pending.PriorContinuationDigest ||
		continuation.SettledOrdinal != pending.WaveOrdinal ||
		continuation.WaveRevision != pending.Revision || head.NextStepOrdinal == ^uint64(0) ||
		!nextRevision(head.Revision, continuation.Revision) {
		return HeadRecord{}, ErrInvalidState
	}
	if nonzeroDigest(pending.PayloadBuildDigest) {
		if err := validatePayloadBuild(build); err != nil || build.Phase != PayloadBuildSealed ||
			build.BuildDigest != pending.PayloadBuildDigest || build.KeyDigest != head.KeyDigest ||
			build.WaveOrdinal != pending.WaveOrdinal {
			return HeadRecord{}, ErrInvalidState
		}
		chunkOverhead, err := checkedMul(build.ChunkCount,
			uint64(PayloadStorageKeyBytes+payloadChunkHeaderBytes+checksumBytes))
		if err != nil {
			return HeadRecord{}, err
		}
		cleanupBytes, err := checkedSum(build.TotalBytes, chunkOverhead, FixedStorageKeyBytes, payloadBuildBytes)
		if err != nil {
			return HeadRecord{}, err
		}
		head.CleanupBuildDigest = build.BuildDigest
		head.CleanupChunkCount = build.ChunkCount
		head.CleanupPayloadBytes = cleanupBytes
		head.CleanupTotalDataBytes = build.TotalBytes
	} else if build != (PayloadBuildRecord{}) {
		return HeadRecord{}, ErrInvalidState
	}
	head.Revision = continuation.Revision
	head.NextStepOrdinal++
	head.ContinuationRevision = continuation.WaveRevision
	head.ContinuationDigest = continuation.ContinuationDigest
	head.OutstandingRoutePinDigest = pending.RoutePinDigest
	return head, nil
}

// MarkRoutePinReleased clears the physical-route lifetime fence only after the
// state machine has verified and durably stored the exact release completion.
// Payload cleanup, the next wave, and terminal publication remain forbidden
// while this digest is present in the head.
func MarkRoutePinReleased(head HeadRecord, routePin RoutePinRecord, revision uint64) (HeadRecord, error) {
	if err := validateHead(head); err != nil || errOrNil(validateRoutePin(routePin)) != nil ||
		head.Phase != PhaseSealed || routePin.Phase != RoutePinReleased ||
		!nonzeroDigest(head.OutstandingRoutePinDigest) ||
		routePin.AcquiredEvidenceDigest != head.OutstandingRoutePinDigest ||
		routePin.KeyDigest != head.KeyDigest || routePin.RequestDigest != head.RequestDigest ||
		routePin.PlanRoot != head.PlanRoot || routePin.WaveOrdinal+1 != head.NextStepOrdinal ||
		!nextRevision(head.Revision, revision) {
		return HeadRecord{}, ErrInvalidState
	}
	head.OutstandingRoutePinDigest = Digest{}
	head.Revision = revision
	return head, validateHead(head)
}

func SameContinuation(left, right ContinuationRecord) bool {
	return left.KeyDigest == right.KeyDigest && left.RequestDigest == right.RequestDigest &&
		left.PlanRoot == right.PlanRoot && left.WaveDigest == right.WaveDigest &&
		left.PriorContinuationDigest == right.PriorContinuationDigest &&
		left.ObservationDigest == right.ObservationDigest &&
		left.NextStateDigest == right.NextStateDigest &&
		left.RoutePinDigest == right.RoutePinDigest &&
		left.ContinuationDigest == right.ContinuationDigest && left.Revision == right.Revision &&
		left.SettledOrdinal == right.SettledOrdinal && left.WaveRevision == right.WaveRevision &&
		left.TransitionTag == right.TransitionTag && bytes.Equal(left.Cursor, right.Cursor) &&
		bytes.Equal(left.Observation, right.Observation)
}

func digestBytes(domain, value []byte) Digest {
	valueDigest := sha256.Sum256(value)
	// Every domain used by this package is shorter than this fixed bound. Keep
	// the hot-path helper allocation-free while retaining explicit separation.
	var framed [96]byte
	if len(domain)+sha256.Size > len(framed) {
		return Digest{}
	}
	at := copy(framed[:], domain)
	copy(framed[at:], valueDigest[:])
	return Digest(sha256.Sum256(framed[:at+sha256.Size]))
}
