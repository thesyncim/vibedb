package replicatedstate

import (
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
)

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
