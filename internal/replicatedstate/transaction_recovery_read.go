package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

// TransactionRecoveryReadKind is the closed hidden-state recovery read set.
// It deliberately exposes neither generic system keys nor collection names.
type TransactionRecoveryReadKind uint8

const (
	TransactionRecoveryLookupCoordinator TransactionRecoveryReadKind = iota + 1
	TransactionRecoveryLookupParticipant
	TransactionRecoveryReadManifestPage
	TransactionRecoveryScanCoordinator
)

const (
	MaxTransactionRecoveryScanRows = 256
	// TransactionRecoverySummaryBytes is the exact encoded fixed-record charge:
	// 16+1+1+8+1+8+16+16+8+32+8+1+1+4+1.
	TransactionRecoverySummaryBytes = 122
	MaxTransactionRecoveryScanBytes = MaxTransactionRecoveryScanRows * TransactionRecoverySummaryBytes
	// MaxTransactionRecoveryReadBytes is the exact largest legal material
	// response: one fixed record followed by one maximum VTM1 page.
	MaxTransactionRecoveryReadBytes = TransactionRecoverySummaryBytes + distributedtxn.ManifestSegmentBytes

	// MaxTransactionRecoveryPayloadArenaBytes is the caller-owned no-growth
	// scratch proof for the largest coordinator plus manifest-page validation.
	// Returned Payload bytes are always a smaller, capacity-clamped canonical
	// VTC1, VTCM, or VTM1 view at the front of this arena.
	MaxTransactionRecoveryPayloadArenaBytes = MaxTransactionCoordinatorPayloadRecordBytes +
		MaxTransactionManifestPageRecordBytes
)

var (
	ErrTransactionRecoveryRead = errors.New("replicatedstate: invalid transaction recovery read")
	errStopTransactionRecovery = errors.New("replicatedstate: stop transaction recovery scan")
)

// TransactionRecoveryReadRequest addresses one transaction control or an
// exclusive coordinator-ID scan cursor. MaxBytes includes the fixed summary
// charge and any returned canonical payload, never internal storage wrapping.
type TransactionRecoveryReadRequest struct {
	Kind           TransactionRecoveryReadKind
	ID             distributedtxn.ID
	ManifestPage   uint32
	MinimumApplied uint64
	MaxRows        uint16
	MaxBytes       uint32
}

// TransactionRecoveryRecord is detached fixed recovery metadata plus an
// optional borrowed canonical payload in the caller's arena. State is decoded
// according to Role. Payload is VTC1/VTCM for an active coordinator, VTM1 for
// a manifest-page read, and empty for participant and scan summaries.
type TransactionRecoveryRecord struct {
	ID       distributedtxn.ID
	Role     distributedtxn.ReplicatedRole
	State    uint8
	Revision uint64

	PayloadKind  distributedtxn.ReplicatedPayloadKind
	PayloadCount uint64

	CoordinatorGroup            replication.ID128
	CoordinatorShardIncarnation replication.ID128
	CoordinatorAllocation       uint64
	MutationDigest              distributedtxn.Digest

	AffectedRows      int64
	AffectedRowsValid bool
	// CancellationWitness and ParticipantOrdinal expose the exact compact
	// abort fence. The ordinal is meaningful only when CancellationWitness is
	// true; ordinary participant summaries retain affected-row semantics.
	CancellationWitness bool
	ParticipantOrdinal  uint32
	CoordinatorDecision distributedtxn.CoordinatorState
	ManifestPage        uint32
	RecoveryPulse       uint8
	Payload             []byte
}

// TransactionRecoveryReadResult aliases only the caller-owned record and
// payload arenas. Complete is true for every point result and only when a scan
// exhausted the coordinator prefix. When false, the last returned ID is the
// exclusive continuation cursor.
type TransactionRecoveryReadResult struct {
	Fence    SnapshotFence
	Complete bool
	Records  []TransactionRecoveryRecord
}

// ValidateTransactionRecoveryReadRequest rejects ambiguous, unbounded, and
// operation-incompatible requests before snapshot acquisition.
func ValidateTransactionRecoveryReadRequest(request TransactionRecoveryReadRequest) error {
	if request.MinimumApplied == 0 {
		return ErrTransactionRecoveryRead
	}
	switch request.Kind {
	case TransactionRecoveryLookupCoordinator:
		if request.ID.IsZero() || request.ManifestPage != 0 || request.MaxRows != 1 ||
			request.MaxBytes < TransactionRecoverySummaryBytes ||
			request.MaxBytes > TransactionRecoverySummaryBytes+distributedtxn.MaxCoordinatorRecordBytes {
			return ErrTransactionRecoveryRead
		}
	case TransactionRecoveryLookupParticipant:
		if request.ID.IsZero() || request.ManifestPage != 0 || request.MaxRows != 1 ||
			request.MaxBytes != TransactionRecoverySummaryBytes {
			return ErrTransactionRecoveryRead
		}
	case TransactionRecoveryReadManifestPage:
		if request.ID.IsZero() || request.MaxRows != 1 ||
			request.MaxBytes < TransactionRecoverySummaryBytes ||
			request.MaxBytes > TransactionRecoverySummaryBytes+distributedtxn.ManifestSegmentBytes {
			return ErrTransactionRecoveryRead
		}
	case TransactionRecoveryScanCoordinator:
		if request.ManifestPage != 0 || request.MaxRows == 0 ||
			request.MaxRows > MaxTransactionRecoveryScanRows ||
			request.MaxBytes < TransactionRecoverySummaryBytes ||
			request.MaxBytes > MaxTransactionRecoveryScanBytes {
			return ErrTransactionRecoveryRead
		}
	default:
		return ErrTransactionRecoveryRead
	}
	return nil
}

// TransactionRecoveryReadInto reads one coherent hidden-system generation at
// or above MinimumApplied. records and payload are caller-owned response and
// validation arenas; the method performs no generic system read and returns no
// borrowed durable-engine memory.
func (m *Machine) TransactionRecoveryReadInto(
	request TransactionRecoveryReadRequest,
	records []TransactionRecoveryRecord,
	payload []byte,
) (result TransactionRecoveryReadResult, resultErr error) {
	if err := ValidateTransactionRecoveryReadRequest(request); err != nil ||
		cap(records) < int(request.MaxRows) {
		return TransactionRecoveryReadResult{}, ErrTransactionRecoveryRead
	}
	material := request.Kind == TransactionRecoveryLookupCoordinator ||
		request.Kind == TransactionRecoveryReadManifestPage
	if material && cap(payload) < MaxTransactionRecoveryPayloadArenaBytes {
		return TransactionRecoveryReadResult{}, ErrReadBufferBound
	}
	if m == nil {
		return TransactionRecoveryReadResult{}, ErrTransactionRecoveryRead
	}
	records = records[:0:cap(records)]
	payload = payload[:0:cap(payload)]

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return TransactionRecoveryReadResult{}, err
	}
	if !m.initialized {
		return TransactionRecoveryReadResult{}, ErrWrongBinding
	}
	if m.publication.Applied < request.MinimumApplied {
		return TransactionRecoveryReadResult{}, ErrReadBehind
	}
	var catalog [1]durable.NamedCollection
	catalog[0] = durable.NamedCollection{Name: systemCollectionName, Collection: m.system.Collection}
	if err := durable.SnapshotCollectionsInto(&m.applyCut, catalog[:]); err != nil {
		return TransactionRecoveryReadResult{}, m.fail(err)
	}
	snapshot, ok := m.applyCut.CollectionHandle(m.system.Collection)
	if !ok || snapshot == nil {
		return TransactionRecoveryReadResult{}, m.fail(errors.Join(
			ErrInconsistentSnapshot, m.applyCut.Close(),
		))
	}
	result.Fence = m.transactionRecoveryFenceLocked()
	result.Complete = true

	var controlRead [MaxTransactionControlRecordBytes]byte
	var controlScopes [distributedtxn.MaxIntentScopes]distributedtxn.IntentScope
	switch request.Kind {
	case TransactionRecoveryLookupCoordinator:
		result.Records, resultErr = lookupTransactionRecoveryCoordinator(
			snapshot, request, records, payload, controlRead[:], controlScopes[:],
		)
	case TransactionRecoveryLookupParticipant:
		result.Records, resultErr = lookupTransactionRecoveryParticipant(
			snapshot, request, records, controlRead[:], controlScopes[:],
		)
	case TransactionRecoveryReadManifestPage:
		result.Records, resultErr = readTransactionRecoveryManifestPage(
			snapshot, request, records, payload, controlRead[:], controlScopes[:],
		)
	case TransactionRecoveryScanCoordinator:
		result.Records, result.Complete, resultErr = scanTransactionRecoveryCoordinators(
			snapshot, request, records, controlRead[:], controlScopes[:],
		)
	}
	closeErr := m.applyCut.Close()
	if resultErr != nil || closeErr != nil {
		if closeErr == nil && (errors.Is(resultErr, ErrReadBufferBound) ||
			errors.Is(resultErr, ErrTransactionRecoveryRead)) {
			return TransactionRecoveryReadResult{}, resultErr
		}
		return TransactionRecoveryReadResult{}, m.fail(errors.Join(resultErr, closeErr))
	}
	return result, nil
}

func (m *Machine) transactionRecoveryFenceLocked() SnapshotFence {
	return SnapshotFence{
		Binding: m.state.Binding, RelationManifestDigest: m.manifestDigest,
		ReplicaSetVersion: m.publication.ReplicaSetVersion,
		Applied:           m.state.Applied, LastTerm: m.state.LastTerm,
		LastEntryDigest: m.state.LastEntryDigest, DataChainDigest: m.state.DataChainDigest,
		SnapshotBaseDigest: m.state.SnapshotBaseDigest,
	}
}

func lookupTransactionRecoveryCoordinator(
	snapshot *durable.Snapshot,
	request TransactionRecoveryReadRequest,
	records []TransactionRecoveryRecord,
	payload, controlRead []byte,
	controlScopes []distributedtxn.IntentScope,
) ([]TransactionRecoveryRecord, error) {
	control, found, err := transactionRecoveryControlAt(
		snapshot, distributedtxn.ReplicatedRoleCoordinator, request.ID,
		controlRead, controlScopes,
	)
	if err != nil || !found {
		return records, err
	}
	record := transactionRecoveryRecord(control.TransactionControl)
	if distributedtxn.CoordinatorState(control.State) == distributedtxn.CoordinatorRetired {
		key, _ := TransactionCoordinatorPayloadStorageKey(control.ID)
		if _, childFound, readErr := snapshot.AppendRaw(payload[:0], key[:]); readErr != nil || childFound {
			return records, errors.Join(readErr, ErrTransactionStateCorrupt)
		}
		return append(records, record), nil
	}
	raw, err := transactionRecoveryCoordinatorPayload(snapshot, control, payload)
	if err != nil {
		return records, err
	}
	if uint64(TransactionRecoverySummaryBytes+len(raw)) > uint64(request.MaxBytes) {
		return records, ErrReadBufferBound
	}
	record.Payload = raw
	return append(records, record), nil
}

func lookupTransactionRecoveryParticipant(
	snapshot *durable.Snapshot,
	request TransactionRecoveryReadRequest,
	records []TransactionRecoveryRecord,
	controlRead []byte,
	controlScopes []distributedtxn.IntentScope,
) ([]TransactionRecoveryRecord, error) {
	control, found, err := transactionRecoveryControlAt(
		snapshot, distributedtxn.ReplicatedRoleParticipant, request.ID,
		controlRead, controlScopes,
	)
	if err != nil || !found {
		return records, err
	}
	return append(records, transactionRecoveryRecord(control.TransactionControl)), nil
}

func readTransactionRecoveryManifestPage(
	snapshot *durable.Snapshot,
	request TransactionRecoveryReadRequest,
	records []TransactionRecoveryRecord,
	payload, controlRead []byte,
	controlScopes []distributedtxn.IntentScope,
) ([]TransactionRecoveryRecord, error) {
	control, found, err := transactionRecoveryControlAt(
		snapshot, distributedtxn.ReplicatedRoleCoordinator, request.ID,
		controlRead, controlScopes,
	)
	if err != nil || !found {
		return records, err
	}
	if control.PayloadKind != distributedtxn.ReplicatedPayloadManifestCoordinator {
		return records, ErrTransactionRecoveryRead
	}
	if distributedtxn.CoordinatorState(control.State) == distributedtxn.CoordinatorRetired {
		coordinatorKey, _ := TransactionCoordinatorPayloadStorageKey(control.ID)
		if _, childFound, childErr := snapshot.AppendRaw(payload[:0], coordinatorKey[:]); childErr != nil || childFound {
			return records, errors.Join(childErr, ErrTransactionStateCorrupt)
		}
		pageKey, _ := TransactionManifestPageStorageKey(control.ID, request.ManifestPage)
		if _, childFound, childErr := snapshot.AppendRaw(payload[:0], pageKey[:]); childErr != nil || childFound {
			return records, errors.Join(childErr, ErrTransactionStateCorrupt)
		}
		return records, nil
	}
	coordinatorRaw, descriptor, err := transactionRecoveryManifestCoordinator(
		snapshot, control, payload,
	)
	if err != nil {
		return records, err
	}
	if request.ManifestPage >= descriptor.SegmentCount {
		return records, ErrTransactionRecoveryRead
	}
	var coordinatorCopy [distributedtxn.ReplicatedManifestCoordinatorRecordBytes]byte
	copy(coordinatorCopy[:], coordinatorRaw)
	pageZero, err := transactionRecoveryManifestPage(snapshot, control.ID, 0, payload)
	if err != nil || transactionManifestStartDigest(coordinatorCopy[:], pageZero) != control.MutationDigest {
		return records, errors.Join(err, ErrTransactionStateCorrupt)
	}
	if err := transactionRecoveryManifestProgress(control.TransactionControl, descriptor); err != nil {
		return records, err
	}
	if request.ManifestPage >= control.ManifestNextPage {
		return records, nil
	}
	raw := pageZero
	if request.ManifestPage != 0 {
		raw, err = transactionRecoveryManifestPage(
			snapshot, control.ID, request.ManifestPage, payload,
		)
		if err != nil {
			return records, err
		}
	}
	if uint64(TransactionRecoverySummaryBytes+len(raw)) > uint64(request.MaxBytes) {
		return records, ErrReadBufferBound
	}
	record := transactionRecoveryRecord(control.TransactionControl)
	record.ManifestPage = request.ManifestPage
	record.Payload = raw
	return append(records, record), nil
}

func scanTransactionRecoveryCoordinators(
	snapshot *durable.Snapshot,
	request TransactionRecoveryReadRequest,
	records []TransactionRecoveryRecord,
	controlRead []byte,
	controlScopes []distributedtxn.IntentScope,
) ([]TransactionRecoveryRecord, bool, error) {
	var lower [transactionControlStorageKeyBytes]byte
	lower[0] = transactionControlPrefix
	lower[1] = byte(distributedtxn.ReplicatedRoleCoordinator)
	lowerExclusive := false
	lowerBytes := lower[:2]
	if !request.ID.IsZero() {
		copy(lower[2:], request.ID[:])
		lowerBytes = lower[:]
		lowerExclusive = true
	}
	upper := [2]byte{transactionControlPrefix, byte(distributedtxn.ReplicatedRoleParticipant)}
	complete := true
	used := uint32(0)
	_, err := snapshot.RangeBoundsRawBuffer(
		lowerBytes, upper[:], controlRead[:0], lowerExclusive,
		func(key, value []byte) error {
			control, err := OpenTransactionControlInto(value, controlScopes)
			if err != nil || control.Role != distributedtxn.ReplicatedRoleCoordinator {
				return errors.Join(err, ErrTransactionStateCorrupt)
			}
			want, keyErr := control.StorageKey()
			if keyErr != nil || !bytes.Equal(key, want[:]) {
				return errors.Join(keyErr, ErrTransactionStateCorrupt)
			}
			if distributedtxn.CoordinatorState(control.State) == distributedtxn.CoordinatorRetired {
				return nil
			}
			if len(records) == int(request.MaxRows) ||
				used > request.MaxBytes-TransactionRecoverySummaryBytes {
				complete = false
				return errStopTransactionRecovery
			}
			records = append(records, transactionRecoveryRecord(control.TransactionControl))
			used += TransactionRecoverySummaryBytes
			if len(records) == int(request.MaxRows) || used == request.MaxBytes {
				complete = false
				return errStopTransactionRecovery
			}
			return nil
		},
	)
	if errors.Is(err, errStopTransactionRecovery) {
		err = nil
	}
	return records, complete, err
}

func transactionRecoveryControlAt(
	snapshot *durable.Snapshot,
	role distributedtxn.ReplicatedRole,
	id distributedtxn.ID,
	read []byte,
	scopes []distributedtxn.IntentScope,
) (TransactionControlView, bool, error) {
	key, err := TransactionControlStorageKey(role, id)
	if err != nil {
		return TransactionControlView{}, false, err
	}
	raw, found, err := snapshot.AppendRaw(read[:0], key[:])
	if err != nil || !found {
		return TransactionControlView{}, found, err
	}
	control, err := OpenTransactionControlInto(raw, scopes)
	if err != nil || control.Role != role || control.ID != id {
		return TransactionControlView{}, false, errors.Join(err, ErrTransactionStateCorrupt)
	}
	want, err := control.StorageKey()
	if err != nil || want != key {
		return TransactionControlView{}, false, errors.Join(err, ErrTransactionStateCorrupt)
	}
	return control, true, nil
}

func transactionRecoveryCoordinatorPayload(
	snapshot *durable.Snapshot,
	control TransactionControlView,
	arena []byte,
) ([]byte, error) {
	if control.PayloadKind == distributedtxn.ReplicatedPayloadManifestCoordinator {
		coordinator, descriptor, err := transactionRecoveryManifestCoordinator(snapshot, control, arena)
		if err != nil {
			return nil, err
		}
		workspace := arena[:cap(arena)]
		pageZero, err := transactionRecoveryManifestPage(
			snapshot, control.ID, 0, workspace[len(coordinator):],
		)
		if err != nil || transactionManifestStartDigest(coordinator, pageZero) != control.MutationDigest ||
			transactionRecoveryManifestProgress(control.TransactionControl, descriptor) != nil {
			return nil, errors.Join(err, ErrTransactionStateCorrupt)
		}
		return coordinator, nil
	}
	if control.PayloadKind != distributedtxn.ReplicatedPayloadCoordinator {
		return nil, ErrTransactionStateCorrupt
	}
	key, _ := TransactionCoordinatorPayloadStorageKey(control.ID)
	stored, found, err := snapshot.AppendRaw(arena[:0], key[:])
	if err != nil || !found {
		return nil, errors.Join(err, ErrTransactionStateCorrupt)
	}
	view, err := OpenTransactionCoordinatorPayload(stored)
	if err != nil || view.ID != control.ID || view.Kind != control.PayloadKind ||
		view.Digest != control.PayloadDigest || uint64(len(view.Payload)) != control.PayloadBytes {
		return nil, errors.Join(err, ErrTransactionStateCorrupt)
	}
	var participants [distributedtxn.MaxInlineParticipants]distributedtxn.ParticipantRef
	record, err := distributedtxn.OpenCoordinatorInto(view.Payload, participants[:])
	resident, residentErr := TransactionCoordinatorPayloadResidentBytes(len(view.Payload))
	if err != nil || residentErr != nil || record.ID != control.ID ||
		uint64(len(record.Participants)) != control.PayloadCount ||
		resident != control.ResidentPayloadBytes ||
		distributedtxn.Digest(sha256.Sum256(view.Payload)) != control.PayloadDigest {
		return nil, errors.Join(err, residentErr, ErrTransactionStateCorrupt)
	}
	copy(arena[:len(view.Payload)], view.Payload)
	return arena[:len(view.Payload):len(view.Payload)], nil
}

func transactionRecoveryManifestCoordinator(
	snapshot *durable.Snapshot,
	control TransactionControlView,
	arena []byte,
) ([]byte, distributedtxn.ManifestDescriptor, error) {
	key, _ := TransactionCoordinatorPayloadStorageKey(control.ID)
	stored, found, err := snapshot.AppendRaw(arena[:0], key[:])
	if err != nil || !found {
		return nil, distributedtxn.ManifestDescriptor{}, errors.Join(err, ErrTransactionStateCorrupt)
	}
	view, err := OpenTransactionCoordinatorPayload(stored)
	if err != nil || view.ID != control.ID || view.Kind != control.PayloadKind {
		return nil, distributedtxn.ManifestDescriptor{}, errors.Join(err, ErrTransactionStateCorrupt)
	}
	record, err := distributedtxn.OpenManifestCoordinator(view.Payload)
	resident, residentErr := TransactionCoordinatorPayloadResidentBytes(len(view.Payload))
	if err != nil || residentErr != nil || record.ID != control.ID ||
		record.Manifest.ParticipantCount != control.PayloadCount ||
		record.Manifest.EncodedBytes != control.PayloadBytes ||
		resident != control.ResidentPayloadBytes {
		return nil, distributedtxn.ManifestDescriptor{}, errors.Join(
			err, residentErr, ErrTransactionStateCorrupt,
		)
	}
	copy(arena[:len(view.Payload)], view.Payload)
	raw := arena[:len(view.Payload):len(view.Payload)]
	return raw, record.Manifest, nil
}

func transactionRecoveryManifestPage(
	snapshot *durable.Snapshot,
	id distributedtxn.ID,
	index uint32,
	arena []byte,
) ([]byte, error) {
	key, _ := TransactionManifestPageStorageKey(id, index)
	stored, found, err := snapshot.AppendRaw(arena[:0], key[:])
	if err != nil || !found {
		return nil, errors.Join(err, ErrTransactionStateCorrupt)
	}
	storedID, meta, raw, err := openTransactionManifestPageWitness(stored)
	if err != nil || storedID != id || meta.Index != index {
		return nil, errors.Join(err, ErrTransactionStateCorrupt)
	}
	copy(arena[:len(raw)], raw)
	return arena[:len(raw):len(raw)], nil
}

func transactionRecoveryManifestProgress(
	control TransactionControl,
	descriptor distributedtxn.ManifestDescriptor,
) error {
	if control.ManifestNextPage == 0 || control.ManifestNextPage > descriptor.SegmentCount ||
		control.ManifestNextParticipant > descriptor.ParticipantCount ||
		control.ManifestEncodedBytes > descriptor.EncodedBytes {
		return ErrTransactionStateCorrupt
	}
	pageComplete := control.ManifestNextPage == descriptor.SegmentCount
	participantComplete := control.ManifestNextParticipant == descriptor.ParticipantCount
	bytesComplete := control.ManifestEncodedBytes == descriptor.EncodedBytes
	complete := pageComplete && participantComplete && bytesComplete
	if (pageComplete || participantComplete || bytesComplete) != complete {
		return ErrTransactionStateCorrupt
	}
	if complete &&
		control.ManifestNextParticipant == descriptor.ParticipantCount &&
		control.ManifestEncodedBytes == descriptor.EncodedBytes &&
		finishTransactionManifestRoot(
			control.ManifestChainDigest, control.ManifestNextParticipant,
			control.ManifestEncodedBytes, control.ManifestNextPage,
		) != descriptor.Root {
		return ErrTransactionStateCorrupt
	}
	if distributedtxn.CoordinatorState(control.State) == distributedtxn.CoordinatorCommitted && !complete {
		return ErrTransactionStateCorrupt
	}
	return nil
}

func transactionRecoveryRecord(control TransactionControl) TransactionRecoveryRecord {
	return TransactionRecoveryRecord{
		ID: control.ID, Role: control.Role, State: control.State, Revision: control.Revision,
		PayloadKind: control.PayloadKind, PayloadCount: control.PayloadCount,
		CoordinatorGroup:            control.CoordinatorGroup,
		CoordinatorShardIncarnation: control.CoordinatorShardIncarnation,
		CoordinatorAllocation:       control.CoordinatorAllocation,
		MutationDigest:              control.MutationDigest,
		AffectedRows:                control.AffectedRows, AffectedRowsValid: control.AffectedRowsValid,
		CancellationWitness: control.CancellationWitness,
		ParticipantOrdinal:  control.ParticipantOrdinal,
		CoordinatorDecision: control.CoordinatorDecision,
		RecoveryPulse:       control.RecoveryPulse,
	}
}
