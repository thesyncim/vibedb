package shardservice

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

const (
	// ReplicatedTransactionReadValueHeaderBytes is the fixed canonical value
	// envelope above the request-owned record and payload byte budget.
	ReplicatedTransactionReadValueHeaderBytes = 10
	replicatedTransactionReadValueHeaderBytes = ReplicatedTransactionReadValueHeaderBytes
)

func replicatedTransactionRecoveryRead(
	request ReplicatedTransactionReadRequest,
) (replicatedstate.TransactionRecoveryReadRequest, bool) {
	var kind replicatedstate.TransactionRecoveryReadKind
	switch request.Kind {
	case ReplicatedTransactionLookupCoordinator:
		kind = replicatedstate.TransactionRecoveryLookupCoordinator
	case ReplicatedTransactionLookupParticipant:
		kind = replicatedstate.TransactionRecoveryLookupParticipant
	case ReplicatedTransactionReadManifestPage:
		kind = replicatedstate.TransactionRecoveryReadManifestPage
	case ReplicatedTransactionScanCoordinators:
		kind = replicatedstate.TransactionRecoveryScanCoordinator
	default:
		return replicatedstate.TransactionRecoveryReadRequest{}, false
	}
	read := replicatedstate.TransactionRecoveryReadRequest{
		Kind: kind, ID: request.ID, ManifestPage: request.SegmentIndex,
		MinimumApplied: request.MinimumApplied,
		MaxRows:        request.MaxRows, MaxBytes: request.MaxBytes,
	}
	return read, replicatedstate.ValidateTransactionRecoveryReadRequest(read) == nil
}

// ReplicatedTransactionReadValue is the detached canonical recovery result
// carried by one native response. Records and their optional sole payload
// alias the decoded frame and remain capacity-clamped.
type ReplicatedTransactionReadValue struct {
	Kind     ReplicatedTransactionReadKind
	Complete bool
	Records  []replicatedstate.TransactionRecoveryRecord
}

// AppendReplicatedTransactionReadValue emits the sole unreleased recovery
// result grammar. The outer native frame already selects its grammar, so this
// body carries no redundant version ladder or string discriminator.
func AppendReplicatedTransactionReadValue(
	dst []byte,
	value ReplicatedTransactionReadValue,
) ([]byte, error) {
	if len(value.Records) > MaxReplicatedTransactionScanItems ||
		!validReplicatedTransactionReadRecords(value.Kind, value.Complete, value.Records) {
		return dst, ErrReplicatedWire
	}
	payloadBytes := 0
	if len(value.Records) == 1 {
		payloadBytes = len(value.Records[0].Payload)
	}
	recordBytes := len(value.Records) * replicatedstate.TransactionRecoverySummaryBytes
	if recordBytes+payloadBytes > MaxReplicatedTransactionReadBytes {
		return dst, ErrReplicatedWire
	}
	start := len(dst)
	total := replicatedTransactionReadValueHeaderBytes + recordBytes + payloadBytes
	dst = append(dst, make([]byte, total)...)
	out := dst[start:]
	out[0] = byte(value.Kind)
	if value.Complete {
		out[1] = 1
	}
	binary.BigEndian.PutUint32(out[2:6], uint32(len(value.Records)))
	binary.BigEndian.PutUint32(out[6:10], uint32(payloadBytes))
	cursor := replicatedTransactionReadValueHeaderBytes
	for index := range value.Records {
		appendReplicatedTransactionRecoveryRecord(
			out[cursor:cursor+replicatedstate.TransactionRecoverySummaryBytes],
			value.Records[index],
		)
		cursor += replicatedstate.TransactionRecoverySummaryBytes
	}
	if payloadBytes != 0 {
		copy(out[cursor:], value.Records[0].Payload)
	}
	return dst, nil
}

// OpenReplicatedTransactionReadValueInto opens one canonical result without
// sizing memory from the peer. records is caller-owned and must cover the
// encoded count before any summaries are decoded.
func OpenReplicatedTransactionReadValueInto(
	src []byte,
	records []replicatedstate.TransactionRecoveryRecord,
) (ReplicatedTransactionReadValue, error) {
	kind, complete, count, payload, summaries, ok := inspectReplicatedTransactionReadValue(src)
	if !ok || count > cap(records) {
		return ReplicatedTransactionReadValue{}, ErrReplicatedWire
	}
	records = records[:count]
	for index := range records {
		offset := index * replicatedstate.TransactionRecoverySummaryBytes
		if !validReplicatedTransactionRecoverySummary(
			summaries[offset : offset+replicatedstate.TransactionRecoverySummaryBytes],
		) {
			return ReplicatedTransactionReadValue{}, ErrReplicatedWire
		}
		records[index] = openReplicatedTransactionRecoveryRecord(
			summaries[offset : offset+replicatedstate.TransactionRecoverySummaryBytes],
		)
	}
	if len(payload) != 0 {
		records[0].Payload = payload[:len(payload):len(payload)]
	}
	if !validReplicatedTransactionReadRecords(kind, complete, records) {
		return ReplicatedTransactionReadValue{}, ErrReplicatedWire
	}
	return ReplicatedTransactionReadValue{
		Kind: kind, Complete: complete, Records: records[:len(records):len(records)],
	}, nil
}

func inspectReplicatedTransactionReadValue(
	src []byte,
) (
	kind ReplicatedTransactionReadKind,
	complete bool,
	count int,
	payload []byte,
	summaries []byte,
	ok bool,
) {
	if len(src) < replicatedTransactionReadValueHeaderBytes || src[1] > 1 {
		return 0, false, 0, nil, nil, false
	}
	kind = ReplicatedTransactionReadKind(src[0])
	complete = src[1] == 1
	encodedCount := binary.BigEndian.Uint32(src[2:6])
	payloadBytes := binary.BigEndian.Uint32(src[6:10])
	if encodedCount > MaxReplicatedTransactionScanItems ||
		uint64(encodedCount)*replicatedstate.TransactionRecoverySummaryBytes > uint64(len(src)) ||
		uint64(payloadBytes) > uint64(len(src)) {
		return 0, false, 0, nil, nil, false
	}
	summaryBytes := int(encodedCount) * replicatedstate.TransactionRecoverySummaryBytes
	want := replicatedTransactionReadValueHeaderBytes + summaryBytes + int(payloadBytes)
	if want != len(src) || summaryBytes+int(payloadBytes) > MaxReplicatedTransactionReadBytes {
		return 0, false, 0, nil, nil, false
	}
	summaries = src[replicatedTransactionReadValueHeaderBytes : replicatedTransactionReadValueHeaderBytes+summaryBytes]
	payload = src[replicatedTransactionReadValueHeaderBytes+summaryBytes:]
	return kind, complete, int(encodedCount), payload, summaries, true
}

func validReplicatedTransactionReadValue(src []byte) bool {
	kind, complete, count, payload, summaries, ok := inspectReplicatedTransactionReadValue(src)
	if !ok || !validReplicatedTransactionReadShape(kind, complete, count) {
		return false
	}
	if count == 0 {
		return validReplicatedTransactionReadRecords(kind, complete, nil) && len(payload) == 0
	}
	var prior distributedtxn.ID
	for index := 0; index < count; index++ {
		offset := index * replicatedstate.TransactionRecoverySummaryBytes
		if !validReplicatedTransactionRecoverySummary(
			summaries[offset : offset+replicatedstate.TransactionRecoverySummaryBytes],
		) {
			return false
		}
		record := openReplicatedTransactionRecoveryRecord(
			summaries[offset : offset+replicatedstate.TransactionRecoverySummaryBytes],
		)
		if index == 0 {
			record.Payload = payload
		} else if len(payload) != 0 {
			return false
		}
		if !validReplicatedTransactionRecoveryRecord(kind, record, record.Payload) {
			return false
		}
		if kind == ReplicatedTransactionScanCoordinators && index != 0 &&
			bytes.Compare(prior[:], record.ID[:]) >= 0 {
			return false
		}
		prior = record.ID
	}
	return !(kind == ReplicatedTransactionScanCoordinators && !complete && count == 0)
}

func appendReplicatedTransactionRecoveryRecord(
	dst []byte,
	record replicatedstate.TransactionRecoveryRecord,
) {
	copy(dst[0:16], record.ID[:])
	dst[16] = byte(record.Role)
	dst[17] = record.State
	binary.BigEndian.PutUint64(dst[18:26], record.Revision)
	dst[26] = byte(record.PayloadKind)
	binary.BigEndian.PutUint64(dst[27:35], record.PayloadCount)
	copy(dst[35:51], record.CoordinatorGroup[:])
	copy(dst[51:67], record.CoordinatorShardIncarnation[:])
	binary.BigEndian.PutUint64(dst[67:75], record.CoordinatorAllocation)
	copy(dst[75:107], record.MutationDigest[:])
	if record.CancellationWitness {
		binary.BigEndian.PutUint64(dst[107:115], uint64(record.ParticipantOrdinal))
	} else {
		binary.BigEndian.PutUint64(dst[107:115], uint64(record.AffectedRows))
	}
	if record.AffectedRowsValid {
		dst[115] = 1
	}
	if record.CancellationWitness {
		dst[115] |= 1 << 1
	}
	dst[116] = byte(record.CoordinatorDecision)
	binary.BigEndian.PutUint32(dst[117:121], record.ManifestPage)
}

func openReplicatedTransactionRecoveryRecord(
	src []byte,
) replicatedstate.TransactionRecoveryRecord {
	var record replicatedstate.TransactionRecoveryRecord
	copy(record.ID[:], src[0:16])
	record.Role = distributedtxn.ReplicatedRole(src[16])
	record.State = src[17]
	record.Revision = binary.BigEndian.Uint64(src[18:26])
	record.PayloadKind = distributedtxn.ReplicatedPayloadKind(src[26])
	record.PayloadCount = binary.BigEndian.Uint64(src[27:35])
	copy(record.CoordinatorGroup[:], src[35:51])
	copy(record.CoordinatorShardIncarnation[:], src[51:67])
	record.CoordinatorAllocation = binary.BigEndian.Uint64(src[67:75])
	copy(record.MutationDigest[:], src[75:107])
	record.AffectedRowsValid = src[115]&1 != 0
	record.CancellationWitness = src[115]&(1<<1) != 0
	if record.CancellationWitness {
		record.ParticipantOrdinal = uint32(binary.BigEndian.Uint64(src[107:115]))
	} else {
		record.AffectedRows = int64(binary.BigEndian.Uint64(src[107:115]))
	}
	record.CoordinatorDecision = distributedtxn.CoordinatorState(src[116])
	record.ManifestPage = binary.BigEndian.Uint32(src[117:121])
	return record
}

func validReplicatedTransactionRecoverySummary(src []byte) bool {
	if len(src) != replicatedstate.TransactionRecoverySummaryBytes || src[115]&^byte(3) != 0 {
		return false
	}
	cancellation := src[115]&(1<<1) != 0
	return !cancellation || src[115]&1 == 0 &&
		binary.BigEndian.Uint64(src[107:115]) <= math.MaxUint32
}

func validReplicatedTransactionReadRecords(
	kind ReplicatedTransactionReadKind,
	complete bool,
	records []replicatedstate.TransactionRecoveryRecord,
) bool {
	if !validReplicatedTransactionReadShape(kind, complete, len(records)) {
		return false
	}
	for index := range records {
		payload := records[index].Payload
		if index != 0 && len(payload) != 0 ||
			!validReplicatedTransactionRecoveryRecord(kind, records[index], payload) {
			return false
		}
		if kind == ReplicatedTransactionScanCoordinators && index != 0 &&
			bytes.Compare(records[index-1].ID[:], records[index].ID[:]) >= 0 {
			return false
		}
	}
	return true
}

func validReplicatedTransactionReadShape(
	kind ReplicatedTransactionReadKind,
	complete bool,
	count int,
) bool {
	point := kind == ReplicatedTransactionLookupCoordinator ||
		kind == ReplicatedTransactionLookupParticipant ||
		kind == ReplicatedTransactionReadManifestPage
	return point && complete && count <= 1 ||
		kind == ReplicatedTransactionScanCoordinators &&
			count <= MaxReplicatedTransactionScanItems && (complete || count != 0)
}

func validReplicatedTransactionRecoveryRecord(
	kind ReplicatedTransactionReadKind,
	record replicatedstate.TransactionRecoveryRecord,
	payload []byte,
) bool {
	if record.ID.IsZero() || record.Revision == 0 ||
		record.CoordinatorGroup == (replication.ID128{}) ||
		record.CoordinatorShardIncarnation == (replication.ID128{}) ||
		record.CoordinatorAllocation == 0 || record.MutationDigest == (distributedtxn.Digest{}) ||
		record.AffectedRows < 0 {
		return false
	}
	wantRole := distributedtxn.ReplicatedRoleCoordinator
	if kind == ReplicatedTransactionLookupParticipant {
		wantRole = distributedtxn.ReplicatedRoleParticipant
	}
	if record.Role != wantRole {
		return false
	}
	if wantRole == distributedtxn.ReplicatedRoleCoordinator {
		state := distributedtxn.CoordinatorState(record.State)
		if record.PayloadCount == 0 || record.CancellationWitness || record.ParticipantOrdinal != 0 ||
			!state.CanTransitionTo(state) || !validReplicatedCoordinatorAffectedRows(state, record) ||
			(record.PayloadKind != distributedtxn.ReplicatedPayloadCoordinator &&
				record.PayloadKind != distributedtxn.ReplicatedPayloadManifestCoordinator) ||
			!validReplicatedCoordinatorDecision(state, record.CoordinatorDecision) {
			return false
		}
	} else {
		state := distributedtxn.ParticipantState(record.State)
		if record.CancellationWitness {
			if state != distributedtxn.ParticipantReleased || record.Revision != 1 ||
				record.PayloadCount != 0 || record.AffectedRowsValid || record.AffectedRows != 0 {
				return false
			}
		} else if record.PayloadCount == 0 || record.ParticipantOrdinal != 0 {
			return false
		}
		if !state.CanTransitionTo(state) ||
			record.PayloadKind != distributedtxn.ReplicatedPayloadParticipantStage ||
			record.CoordinatorDecision != distributedtxn.CoordinatorInvalid ||
			!validReplicatedParticipantAffectedRows(state, record) {
			return false
		}
	}
	if kind != ReplicatedTransactionReadManifestPage && record.ManifestPage != 0 {
		return false
	}
	switch kind {
	case ReplicatedTransactionLookupCoordinator:
		return validReplicatedCoordinatorRecoveryPayload(record, payload)
	case ReplicatedTransactionLookupParticipant,
		ReplicatedTransactionScanCoordinators:
		return len(payload) == 0
	case ReplicatedTransactionReadManifestPage:
		if record.PayloadKind != distributedtxn.ReplicatedPayloadManifestCoordinator || len(payload) == 0 {
			return false
		}
		meta, err := inspectTransactionManifestSegment(payload)
		return err == nil && meta.index == record.ManifestPage &&
			meta.firstParticipant <= record.PayloadCount &&
			uint64(meta.participantCount) <= record.PayloadCount-meta.firstParticipant
	default:
		return false
	}
}

func validReplicatedCoordinatorAffectedRows(
	state distributedtxn.CoordinatorState,
	record replicatedstate.TransactionRecoveryRecord,
) bool {
	if state != distributedtxn.CoordinatorRetired {
		return !record.AffectedRowsValid && record.AffectedRows == 0
	}
	if record.CoordinatorDecision == distributedtxn.CoordinatorCommitted {
		return record.AffectedRowsValid
	}
	return record.CoordinatorDecision == distributedtxn.CoordinatorAborted &&
		!record.AffectedRowsValid && record.AffectedRows == 0
}

func validReplicatedCoordinatorDecision(
	state distributedtxn.CoordinatorState,
	decision distributedtxn.CoordinatorState,
) bool {
	switch state {
	case distributedtxn.CoordinatorStaging:
		return decision == distributedtxn.CoordinatorInvalid
	case distributedtxn.CoordinatorCommitted:
		return decision == distributedtxn.CoordinatorCommitted
	case distributedtxn.CoordinatorAborted:
		return decision == distributedtxn.CoordinatorAborted
	case distributedtxn.CoordinatorRetired:
		return decision == distributedtxn.CoordinatorCommitted ||
			decision == distributedtxn.CoordinatorAborted
	default:
		return false
	}
}

func validReplicatedParticipantAffectedRows(
	state distributedtxn.ParticipantState,
	record replicatedstate.TransactionRecoveryRecord,
) bool {
	switch state {
	case distributedtxn.ParticipantApplied:
		return record.AffectedRowsValid
	case distributedtxn.ParticipantReleased:
		return record.AffectedRowsValid || record.AffectedRows == 0
	default:
		return !record.AffectedRowsValid && record.AffectedRows == 0
	}
}

func validReplicatedCoordinatorRecoveryPayload(
	record replicatedstate.TransactionRecoveryRecord,
	payload []byte,
) bool {
	if distributedtxn.CoordinatorState(record.State) == distributedtxn.CoordinatorRetired {
		return len(payload) == 0
	}
	if len(payload) == 0 {
		return false
	}
	switch record.PayloadKind {
	case distributedtxn.ReplicatedPayloadCoordinator:
		var scratch [distributedtxn.MaxInlineParticipants]distributedtxn.ParticipantRef
		opened, err := distributedtxn.OpenCoordinatorInto(payload, scratch[:])
		return err == nil && opened.ID == record.ID &&
			uint64(len(opened.Participants)) == record.PayloadCount
	case distributedtxn.ReplicatedPayloadManifestCoordinator:
		opened, err := distributedtxn.OpenManifestCoordinator(payload)
		return err == nil && opened.ID == record.ID &&
			opened.Manifest.ParticipantCount == record.PayloadCount
	default:
		return false
	}
}
