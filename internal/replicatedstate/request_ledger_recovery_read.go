package replicatedstate

import (
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/store/durable"
)

// RequestLedgerReadKind is the closed hidden-row recovery surface. Callers
// provide a full RequestKey; digest-only reads are intentionally impossible.
type RequestLedgerReadKind uint8

const (
	RequestLedgerReadHead         RequestLedgerReadKind = RequestLedgerReadKind(requestledger.StorageHead)
	RequestLedgerReadPlanPage     RequestLedgerReadKind = RequestLedgerReadKind(requestledger.StoragePlanPage)
	RequestLedgerReadPending      RequestLedgerReadKind = RequestLedgerReadKind(requestledger.StoragePending)
	RequestLedgerReadTerminal     RequestLedgerReadKind = RequestLedgerReadKind(requestledger.StorageTerminal)
	RequestLedgerReadAck          RequestLedgerReadKind = RequestLedgerReadKind(requestledger.StorageAck)
	RequestLedgerReadContinuation RequestLedgerReadKind = RequestLedgerReadKind(requestledger.StorageContinuation)
	RequestLedgerReadPayloadChunk RequestLedgerReadKind = RequestLedgerReadKind(requestledger.StoragePayloadChunk)
	RequestLedgerReadPayloadBuild RequestLedgerReadKind = RequestLedgerReadKind(requestledger.StoragePayloadBuild)
	RequestLedgerReadRoutePin     RequestLedgerReadKind = RequestLedgerReadKind(requestledger.StorageRoutePin)
	RequestLedgerReadPrepared     RequestLedgerReadKind = RequestLedgerReadKind(requestledger.StoragePrepared)
	RequestLedgerReadSchemaPin    RequestLedgerReadKind = RequestLedgerReadKind(requestledger.StorageSchemaPin)
	// RequestLedgerReadWave is the coherent head/route-pin/pending recovery cut
	// used at wave admission. It avoids three independent leader ReadIndex
	// rounds while retaining ACK precedence and one applied-index fence.
	RequestLedgerReadWave RequestLedgerReadKind = 0xe0
	// Issuer status is a synthetic, fixed-size coherent view rather than one
	// directly addressable storage row.
	RequestLedgerReadIssuerStatus RequestLedgerReadKind = 0xf0
)

const requestLedgerWaveReadHeaderBytes = 13

const MaxRequestLedgerWaveReadBytes = requestLedgerWaveReadHeaderBytes +
	requestledger.MaxHeadRecordBytes + requestledger.MaxRoutePinRecordBytes +
	requestledger.MaxPendingWaveRecordBytes

type RequestLedgerWaveReadValue struct {
	Head         []byte
	RoutePin     []byte
	RouteFound   bool
	Pending      []byte
	PendingFound bool
}

func OpenRequestLedgerWaveReadValue(raw []byte) (RequestLedgerWaveReadValue, error) {
	if len(raw) < requestLedgerWaveReadHeaderBytes || len(raw) > MaxRequestLedgerWaveReadBytes ||
		raw[0]&^byte(3) != 0 {
		return RequestLedgerWaveReadValue{}, ErrRequestLedgerRead
	}
	headBytes := uint64(binary.LittleEndian.Uint32(raw[1:5]))
	routeBytes := uint64(binary.LittleEndian.Uint32(raw[5:9]))
	pendingBytes := uint64(binary.LittleEndian.Uint32(raw[9:13]))
	total := uint64(requestLedgerWaveReadHeaderBytes) + headBytes + routeBytes + pendingBytes
	if headBytes == 0 || total != uint64(len(raw)) ||
		(routeBytes != 0) != (raw[0]&1 != 0) ||
		(pendingBytes != 0) != (raw[0]&2 != 0) {
		return RequestLedgerWaveReadValue{}, ErrRequestLedgerRead
	}
	offset := uint64(requestLedgerWaveReadHeaderBytes)
	value := RequestLedgerWaveReadValue{Head: raw[offset : offset+headBytes : offset+headBytes]}
	offset += headBytes
	value.RouteFound = routeBytes != 0
	value.RoutePin = raw[offset : offset+routeBytes : offset+routeBytes]
	offset += routeBytes
	value.PendingFound = pendingBytes != 0
	value.Pending = raw[offset : offset+pendingBytes : offset+pendingBytes]
	return value, nil
}

var ErrRequestLedgerRead = errors.New("replicatedstate: invalid request-ledger recovery read")

type RequestLedgerReadRequest struct {
	Key                   requestledger.RequestKey
	ExpectedRangeIdentity requestledger.Digest
	Kind                  RequestLedgerReadKind
	Ordinal               uint64
	ContentRoot           requestledger.Digest
	MinimumApplied        uint64
	MaxBytes              uint32
}

// RequestLedgerReadResult owns no durable-engine memory: Value aliases dst.
// An ACK always wins over every requested auxiliary kind, which makes the
// permanent tombstone authoritative while bounded GC removes older rows.
type RequestLedgerReadResult struct {
	Fence             SnapshotFence
	Found             bool
	AuthoritativeKind RequestLedgerReadKind
	Value             []byte
}

func RequestLedgerReadMaxBytes(kind RequestLedgerReadKind) int {
	switch kind {
	case RequestLedgerReadHead:
		return requestledger.MaxHeadRecordBytes
	case RequestLedgerReadPlanPage:
		return requestledger.MaxPlanPageBytes + requestledger.PlanPageRecordOverheadBytes
	case RequestLedgerReadPending:
		return requestledger.MaxPendingWaveRecordBytes
	case RequestLedgerReadTerminal:
		return requestledger.MaxLifecyclePayloadBytes
	case RequestLedgerReadAck:
		return requestledger.AckRecordBytes
	case RequestLedgerReadContinuation:
		return requestledger.MaxContinuationRecordBytes
	case RequestLedgerReadPayloadChunk:
		return requestledger.MaxPayloadChunkRecordBytes
	case RequestLedgerReadPayloadBuild:
		return requestledger.PayloadBuildRecordBytes
	case RequestLedgerReadRoutePin:
		return requestledger.MaxRoutePinRecordBytes
	case RequestLedgerReadPrepared:
		return requestledger.MaxPreparedTerminalRecordBytes
	case RequestLedgerReadSchemaPin:
		return requestledger.MaxSchemaPinReleaseRecordBytes
	case RequestLedgerReadWave:
		return MaxRequestLedgerWaveReadBytes
	case RequestLedgerReadIssuerStatus:
		return requestledger.IssuerLaneStatusBytes
	default:
		return 0
	}
}

func ValidateRequestLedgerReadRequest(request RequestLedgerReadRequest) error {
	if request.MinimumApplied == 0 || request.ExpectedRangeIdentity == (requestledger.Digest{}) ||
		RequestLedgerReadMaxBytes(request.Kind) == 0 ||
		request.MaxBytes < uint32(RequestLedgerReadMaxBytes(request.Kind)) {
		return ErrRequestLedgerRead
	}
	if _, err := requestledger.KeyDigest(request.Key); err != nil {
		return ErrRequestLedgerRead
	}
	switch request.Kind {
	case RequestLedgerReadPlanPage:
		if request.ContentRoot != (requestledger.Digest{}) {
			return ErrRequestLedgerRead
		}
	case RequestLedgerReadPayloadChunk:
		if request.ContentRoot == (requestledger.Digest{}) {
			return ErrRequestLedgerRead
		}
	default:
		if request.Ordinal != 0 || request.ContentRoot != (requestledger.Digest{}) {
			return ErrRequestLedgerRead
		}
	}
	return nil
}

func (m *Machine) RequestLedgerReadInto(
	request RequestLedgerReadRequest,
	dst []byte,
) (result RequestLedgerReadResult, resultErr error) {
	if m == nil || ValidateRequestLedgerReadRequest(request) != nil ||
		cap(dst) < int(request.MaxBytes) {
		return RequestLedgerReadResult{}, ErrRequestLedgerRead
	}
	keyDigest, _ := requestledger.KeyDigest(request.Key)
	home, _ := requestledger.Home(request.Key)

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return RequestLedgerReadResult{}, err
	}
	// Authority and range admission precede every snapshot read.
	if !m.initialized || !m.options.RequestLedgerRange.enabled() {
		return RequestLedgerReadResult{}, ErrWrongBinding
	}
	if request.ExpectedRangeIdentity != m.options.RequestLedgerRange.Identity ||
		!m.options.RequestLedgerRange.contains(home) {
		return RequestLedgerReadResult{}, ErrRequestLedgerRead
	}
	if m.publication.Applied < request.MinimumApplied {
		return RequestLedgerReadResult{}, ErrReadBehind
	}
	var catalog [1]durable.NamedCollection
	catalog[0] = durable.NamedCollection{Name: systemCollectionName, Collection: m.system.Collection}
	if err := durable.SnapshotCollectionsInto(&m.applyCut, catalog[:]); err != nil {
		return RequestLedgerReadResult{}, m.fail(err)
	}
	snapshot, ok := m.applyCut.CollectionHandle(m.system.Collection)
	if !ok || snapshot == nil {
		return RequestLedgerReadResult{}, m.fail(errors.Join(ErrInconsistentSnapshot, m.applyCut.Close()))
	}
	result.Fence = m.transactionRecoveryFenceLocked()
	if request.Kind == RequestLedgerReadIssuerStatus {
		issuerResult, readErr := requestLedgerIssuerStatusRead(snapshot, request, home, dst[:0], result.Fence)
		closeErr := m.applyCut.Close()
		if readErr != nil || closeErr != nil {
			return RequestLedgerReadResult{}, m.fail(errors.Join(readErr, closeErr))
		}
		return issuerResult, nil
	}

	// The tombstone is the first and authoritative point read for every kind.
	ackKey := requestledger.AppendAckKey(nil, home, keyDigest)
	ackRaw, ackFound, err := snapshot.AppendRaw(dst[:0], ackKey)
	if err == nil && ackFound {
		ack, openErr := requestledger.OpenAck(ackRaw)
		if openErr != nil || ack.Key != request.Key || ack.KeyDigest != keyDigest {
			err = errors.Join(openErr, ErrStateCorrupt)
		} else {
			result.Found = true
			result.AuthoritativeKind = RequestLedgerReadAck
			result.Value = ackRaw
		}
	}
	if err == nil && !ackFound {
		headKey := requestledger.AppendHeadKey(nil, home, keyDigest)
		headRaw, headFound, headErr := snapshot.AppendRaw(dst[:0], headKey)
		err = headErr
		if err == nil && headFound {
			head, openErr := requestledger.OpenHead(headRaw)
			if openErr != nil || head.Key != request.Key || head.KeyDigest != keyDigest {
				err = errors.Join(openErr, ErrStateCorrupt)
			} else if request.Kind == RequestLedgerReadHead {
				result.Found = true
				result.AuthoritativeKind = RequestLedgerReadHead
				result.Value = headRaw
			} else if request.Kind == RequestLedgerReadWave {
				result.Value, err = requestLedgerWaveRead(snapshot, dst, headRaw, home, keyDigest)
				if err == nil {
					result.Found = true
					result.AuthoritativeKind = RequestLedgerReadWave
				}
			} else {
				selectedKey := requestLedgerReadStorageKey(request, home, keyDigest)
				if selectedKey == nil {
					err = ErrRequestLedgerRead
				} else {
					var raw []byte
					raw, result.Found, err = snapshot.AppendRaw(dst[:0], selectedKey)
					if err == nil && result.Found {
						if validateErr := validateSnapshotRequestLedgerRow(selectedKey, raw); validateErr != nil {
							err = validateErr
						} else {
							result.AuthoritativeKind = request.Kind
							result.Value = raw
						}
					}
				}
			}
		}
	}
	closeErr := m.applyCut.Close()
	if err != nil || closeErr != nil {
		return RequestLedgerReadResult{}, m.fail(errors.Join(err, closeErr))
	}
	return result, nil
}

func requestLedgerWaveRead(
	snapshot *durable.Snapshot,
	dst, headRaw []byte,
	home requestledger.LedgerHome,
	keyDigest requestledger.Digest,
) ([]byte, error) {
	if snapshot == nil || len(headRaw) == 0 ||
		requestLedgerWaveReadHeaderBytes+len(headRaw) > cap(dst) {
		return nil, ErrReadBufferBound
	}
	headBytes := len(headRaw)
	value := dst[:requestLedgerWaveReadHeaderBytes+headBytes]
	copy(value[requestLedgerWaveReadHeaderBytes:], headRaw)
	clear(value[:requestLedgerWaveReadHeaderBytes])

	routeStart := len(value)
	routeKey := requestledger.AppendRoutePinKey(nil, home, keyDigest)
	var routeFound bool
	var err error
	value, routeFound, err = snapshot.AppendRaw(value, routeKey)
	if err != nil {
		return nil, err
	}
	if routeFound {
		value[0] |= 1
		binary.LittleEndian.PutUint32(value[5:9], uint32(len(value)-routeStart))
	}

	pendingStart := len(value)
	pendingKey := requestledger.AppendPendingKey(nil, home, keyDigest)
	var pendingFound bool
	value, pendingFound, err = snapshot.AppendRaw(value, pendingKey)
	if err != nil {
		return nil, err
	}
	if pendingFound {
		value[0] |= 2
		binary.LittleEndian.PutUint32(value[9:13], uint32(len(value)-pendingStart))
	}
	binary.LittleEndian.PutUint32(value[1:5], uint32(headBytes))
	return value, nil
}

func requestLedgerIssuerStatusRead(
	snapshot *durable.Snapshot,
	request RequestLedgerReadRequest,
	home requestledger.LedgerHome,
	dst []byte,
	fence SnapshotFence,
) (RequestLedgerReadResult, error) {
	identity, err := requestledger.IssuerIdentityFor(request.Key)
	if err != nil {
		return RequestLedgerReadResult{}, ErrRequestLedgerRead
	}
	issuer, err := requestledger.IssuerDigest(identity)
	if err != nil {
		return RequestLedgerReadResult{}, ErrRequestLedgerRead
	}
	highwaterKey := requestledger.AppendIssuerHighwaterKey(nil, home, issuer)
	highwaterRaw, found, err := snapshot.AppendRaw(dst[:0], highwaterKey)
	if err != nil || !found {
		return RequestLedgerReadResult{Fence: fence}, err
	}
	highwater, err := requestledger.OpenIssuerHighwater(highwaterRaw)
	if err != nil || highwater.Home != home || highwater.IssuerDigest != issuer ||
		highwater.Identity != identity {
		return RequestLedgerReadResult{}, errors.Join(err, ErrStateCorrupt)
	}
	var sequence *requestledger.IssuerSequenceRecord
	var ack *requestledger.AckRecord
	if highwater.HighwaterSequence < highwater.AdmittedSequence {
		next := highwater.HighwaterSequence + 1
		sequenceKey := requestledger.AppendIssuerSequenceKey(nil, home, issuer, next)
		sequenceRaw, sequenceFound, readErr := snapshot.AppendRaw(nil, sequenceKey)
		if readErr != nil || !sequenceFound {
			return RequestLedgerReadResult{}, errors.Join(readErr, ErrStateCorrupt)
		}
		value, openErr := requestledger.OpenIssuerSequence(sequenceRaw)
		if openErr != nil || value.Home != home || value.IssuerDigest != issuer ||
			value.Identity != identity || value.Sequence != next {
			return RequestLedgerReadResult{}, errors.Join(openErr, ErrStateCorrupt)
		}
		sequence = &value
		if value.Phase == requestledger.IssuerSequenceGCComplete {
			ackKey := requestledger.AppendAckKey(nil, home, value.KeyDigest)
			ackRaw, ackFound, ackErr := snapshot.AppendRaw(nil, ackKey)
			if ackErr != nil || !ackFound {
				return RequestLedgerReadResult{}, errors.Join(ackErr, ErrStateCorrupt)
			}
			ackValue, openErr := requestledger.OpenAck(ackRaw)
			if openErr != nil || ackValue.GCPhase != requestledger.AckGCComplete ||
				ackValue.KeyDigest != value.KeyDigest || ackValue.AckDigest != value.AckDigest {
				return RequestLedgerReadResult{}, errors.Join(openErr, ErrStateCorrupt)
			}
			ack = &ackValue
		}
	}
	status, err := requestledger.NewIssuerLaneStatus(request.ExpectedRangeIdentity, highwater, sequence, ack)
	if err != nil {
		return RequestLedgerReadResult{}, errors.Join(err, ErrStateCorrupt)
	}
	value, err := requestledger.AppendIssuerLaneStatus(dst[:0], status)
	if err != nil {
		return RequestLedgerReadResult{}, errors.Join(err, ErrStateCorrupt)
	}
	return RequestLedgerReadResult{
		Fence: fence, Found: true, AuthoritativeKind: RequestLedgerReadIssuerStatus, Value: value,
	}, nil
}

func requestLedgerReadStorageKey(
	request RequestLedgerReadRequest,
	home requestledger.LedgerHome,
	key requestledger.Digest,
) []byte {
	switch request.Kind {
	case RequestLedgerReadPlanPage:
		return requestledger.AppendPlanPageKey(nil, home, key, request.Ordinal)
	case RequestLedgerReadPending:
		return requestledger.AppendPendingKey(nil, home, key)
	case RequestLedgerReadTerminal:
		return requestledger.AppendTerminalKey(nil, home, key)
	case RequestLedgerReadAck:
		return requestledger.AppendAckKey(nil, home, key)
	case RequestLedgerReadContinuation:
		return requestledger.AppendContinuationKey(nil, home, key)
	case RequestLedgerReadPayloadChunk:
		return requestledger.AppendPayloadChunkKey(nil, home, key, request.ContentRoot, request.Ordinal)
	case RequestLedgerReadPayloadBuild:
		return requestledger.AppendPayloadBuildKey(nil, home, key)
	case RequestLedgerReadRoutePin:
		return requestledger.AppendRoutePinKey(nil, home, key)
	case RequestLedgerReadPrepared:
		return requestledger.AppendPreparedTerminalKey(nil, home, key)
	case RequestLedgerReadSchemaPin:
		return requestledger.AppendSchemaPinReleaseKey(nil, home, key)
	default:
		return nil
	}
}
