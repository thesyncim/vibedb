package replicatedstate

import (
	"bytes"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/internal/requestledger"
)

// planRequestLedgerIssuerOpen persists the lane identity before any request
// sequence can be admitted. It is a one-row, idempotent CAS; another gateway
// recovers the same grant from RF3 and never needs the issuer's local secret.
func (m *Machine) planRequestLedgerIssuerOpen(
	plan requestLedgerCommandPlan,
	command requestledger.CommandView,
	state State,
	snapshot pointSnapshot,
) (requestLedgerCommandPlan, error) {
	want, ok := command.IssuerOpen()
	if !ok || want.Home != command.Home || want.IssuerDigest != command.KeyDigest ||
		want.HighwaterDigest != command.SubjectDigest {
		return plan, ErrStateCorrupt
	}
	current, raw, found, err := readRequestLedgerIssuerHighwater(
		snapshot, command.Home, want.IssuerDigest,
	)
	if err != nil {
		return plan, err
	}
	if found {
		wantRaw, encodeErr := requestledger.AppendIssuerHighwater(nil, want)
		if encodeErr != nil {
			return plan, errors.Join(encodeErr, ErrStateCorrupt)
		}
		if current.Identity == want.Identity && current.Home == want.Home &&
			current.IssuerDigest == want.IssuerDigest && bytes.Equal(raw, wantRaw) {
			return witnessedRequestLedgerApplied(
				plan, current.Revision, requestledger.PhaseAcked, raw, true,
			), nil
		}
		return witnessedRequestLedgerConflict(
			plan, current.Revision, requestledger.PhaseAcked, raw,
		), nil
	}
	raw, err = requestledger.AppendIssuerHighwater(nil, want)
	if err != nil {
		return plan, errors.Join(err, ErrStateCorrupt)
	}
	key := requestledger.AppendIssuerHighwaterKey(nil, want.Home, want.IssuerDigest)
	added := uint64(len(key) + len(raw))
	consumed, capacityOK := checkedRequestLedgerConsumption(
		state.RequestLedgerResidentBytes, state.RequestLedgerReservedBytes, added,
	)
	ordinary := m.options.RequestLedgerCapacityBytes - m.options.RequestLedgerCleanupReserveBytes
	if !capacityOK || consumed > ordinary {
		plan.completion.ResultCode = ResultRequestLedgerCapacity
		plan.completion.Phase = requestledger.PhaseInvalid
		return plan, nil
	}
	plan.rows = append(plan.rows, newTransactionPut(key, raw))
	plan.delta.rows++
	plan.delta.residentBytes += int64(added)
	return witnessedRequestLedgerApplied(
		plan, want.Revision, requestledger.PhaseAcked, raw, false,
	), nil
}

// planRequestLedgerSequencedCreate installs the per-lane high-water witness
// and the per-sequence active row in the same transaction as Create. The
// sequence row consumes the reservation prepaid by requestledger.Reservation;
// the shared lane row is charged once when the lane first appears.
func (m *Machine) planRequestLedgerSequencedCreate(
	plan requestLedgerCommandPlan,
	command requestledger.CommandView,
	head requestledger.HeadRecord,
	state State,
	snapshot pointSnapshot,
) (requestLedgerCommandPlan, error) {
	if head.Key.IssuerEpoch == 0 {
		return plan, nil
	}
	identity, err := requestledger.IssuerIdentityFor(head.Key)
	if err != nil {
		return plan, ErrStateCorrupt
	}
	issuer, err := requestledger.IssuerDigest(identity)
	if err != nil {
		return plan, ErrStateCorrupt
	}
	highwater, highwaterRaw, highwaterFound, err := readRequestLedgerIssuerHighwater(
		snapshot, command.Home, issuer,
	)
	if err != nil {
		return plan, err
	}
	if highwaterFound {
		if highwater.Home != command.Home || highwater.IssuerDigest != issuer ||
			highwater.Identity != identity {
			return plan, ErrStateCorrupt
		}
		if requestledger.IssuerHighwaterCoversKey(highwater, head.Key) {
			plan.rows = nil
			plan.delta = requestLedgerStateDelta{}
			return witnessedRequestLedgerConflict(
				plan, highwater.Revision, requestledger.PhaseAcked, highwaterRaw,
			), nil
		}
	} else {
		highwater, err = requestledger.NewIssuerHighwater(head.Key)
		if err != nil || highwater.Home != command.Home || highwater.IssuerDigest != issuer {
			return plan, errors.Join(err, ErrStateCorrupt)
		}
		highwaterRaw, err = requestledger.AppendIssuerHighwater(nil, highwater)
		if err != nil {
			return plan, err
		}
	}
	if highwater.AdmittedSequence == math.MaxUint64 ||
		head.Key.IssuerSequence != highwater.AdmittedSequence+1 {
		plan.rows = nil
		plan.delta = requestLedgerStateDelta{}
		return witnessedRequestLedgerConflict(
			plan, highwater.Revision, requestledger.PhaseAcked, highwaterRaw,
		), nil
	}

	sequenceKey := requestledger.AppendIssuerSequenceKey(
		nil, command.Home, issuer, head.Key.IssuerSequence,
	)
	sequenceRaw, sequenceFound, err := snapshot.appendRaw(nil, sequenceKey)
	if err != nil {
		return plan, err
	}
	if sequenceFound {
		if !highwaterFound {
			return plan, ErrStateCorrupt
		}
		sequence, openErr := requestledger.OpenIssuerSequence(sequenceRaw)
		if openErr != nil || sequence.Home != command.Home || sequence.IssuerDigest != issuer ||
			sequence.Sequence != head.Key.IssuerSequence {
			return plan, errors.Join(openErr, ErrStateCorrupt)
		}
		// The ordinal is already owned. An exact row without its atomically
		// created Head is corruption; a different request is a witnessed CAS
		// conflict, not an opportunity to reuse the sequence.
		if sequence.KeyDigest == head.KeyDigest && sequence.RequestDigest == head.RequestDigest {
			return plan, ErrStateCorrupt
		}
		plan.rows = nil
		plan.delta = requestLedgerStateDelta{}
		return witnessedRequestLedgerConflict(
			plan, sequence.Revision, requestledger.PhaseAcked, sequenceRaw,
		), nil
	}

	sequence, err := requestledger.NewIssuerSequence(head.Key, head.RequestDigest)
	if err != nil {
		return plan, err
	}
	sequenceRaw, err = requestledger.AppendIssuerSequence(nil, sequence)
	if err != nil {
		return plan, err
	}
	highwater, err = requestledger.AdmitIssuerSequence(
		highwater, head.Key, head.RequestDigest, highwater.Revision+1,
	)
	if err != nil {
		return plan, errors.Join(err, ErrStateCorrupt)
	}
	nextHighwaterRaw, err := requestledger.AppendIssuerHighwater(nil, highwater)
	if err != nil {
		return plan, err
	}
	if highwaterFound && len(nextHighwaterRaw) != len(highwaterRaw) {
		return plan, ErrStateCorrupt
	}
	highwaterRaw = nextHighwaterRaw
	highwaterKey := requestledger.AppendIssuerHighwaterKey(nil, command.Home, issuer)
	sharedBytes := uint64(0)
	if !highwaterFound {
		sharedBytes = uint64(len(highwaterKey) + len(highwaterRaw))
	}
	if plan.delta.residentBytes < 0 || plan.delta.reservedBytes < 0 {
		return plan, ErrStateCorrupt
	}
	admissionBytes := uint64(plan.delta.residentBytes) + uint64(plan.delta.reservedBytes)
	if admissionBytes > math.MaxUint64-sharedBytes {
		return plan, ErrStateCorrupt
	}
	consumed, ok := checkedRequestLedgerConsumption(
		state.RequestLedgerResidentBytes, state.RequestLedgerReservedBytes,
		admissionBytes+sharedBytes,
	)
	ordinaryCapacity := m.options.RequestLedgerCapacityBytes - m.options.RequestLedgerCleanupReserveBytes
	if !ok || consumed > ordinaryCapacity {
		plan.rows = nil
		plan.delta = requestLedgerStateDelta{}
		plan.completion.ResultCode = ResultRequestLedgerCapacity
		plan.completion.Phase = requestledger.PhaseInvalid
		plan.completion.Revision = 0
		plan.completion.StateDigest = requestledger.Digest{}
		plan.completion.ExactDuplicate = false
		return plan, nil
	}

	plan.rows = append(plan.rows, newTransactionPut(highwaterKey, highwaterRaw))
	if !highwaterFound {
		plan.delta.rows++
		plan.delta.residentBytes += int64(sharedBytes)
	}
	sequenceBytes := int64(len(sequenceKey) + len(sequenceRaw))
	if sequenceBytes != int64(requestledger.IssuerSequenceReservationBytes) ||
		plan.delta.reservedBytes < sequenceBytes {
		return plan, ErrStateCorrupt
	}
	plan.rows = append(plan.rows, newTransactionPut(sequenceKey, sequenceRaw))
	plan.delta.rows++
	plan.delta.residentBytes += sequenceBytes
	plan.delta.reservedBytes -= sequenceBytes
	return plan, nil
}

// planRequestLedgerSequencedAckGCComplete marks the sequence reclaimable in
// the same transaction that changes its ACK to GCComplete. It does not delete
// either row; only the contiguous high-water transition has that authority.
func planRequestLedgerSequencedAckGCComplete(
	plan requestLedgerCommandPlan,
	rows requestLedgerRows,
	nextAck requestledger.AckRecord,
	snapshot pointSnapshot,
) (requestLedgerCommandPlan, error) {
	if nextAck.Key.IssuerEpoch == 0 || nextAck.GCPhase != requestledger.AckGCComplete {
		return plan, nil
	}
	identity, err := requestledger.IssuerIdentityFor(nextAck.Key)
	if err != nil {
		return plan, ErrStateCorrupt
	}
	issuer, err := requestledger.IssuerDigest(identity)
	if err != nil {
		return plan, ErrStateCorrupt
	}
	highwater, _, found, err := readRequestLedgerIssuerHighwater(snapshot, rows.home, issuer)
	if err != nil || !found || highwater.Home != rows.home || highwater.Identity != identity ||
		requestledger.IssuerHighwaterCoversAck(highwater, nextAck) {
		return plan, errors.Join(err, ErrStateCorrupt)
	}
	sequenceKey := requestledger.AppendIssuerSequenceKey(
		nil, rows.home, issuer, nextAck.Key.IssuerSequence,
	)
	sequenceRaw, found, err := snapshot.appendRaw(nil, sequenceKey)
	if err != nil || !found {
		return plan, errors.Join(err, ErrStateCorrupt)
	}
	sequence, err := requestledger.OpenIssuerSequence(sequenceRaw)
	if err != nil {
		return plan, errors.Join(err, ErrStateCorrupt)
	}
	if sequence.Phase == requestledger.IssuerSequenceGCComplete {
		return plan, ErrStateCorrupt
	}
	nextSequence, err := requestledger.MarkIssuerSequenceGCComplete(
		sequence, nextAck, sequence.Revision+1,
	)
	if err != nil {
		return plan, errors.Join(err, ErrStateCorrupt)
	}
	nextRaw, err := requestledger.AppendIssuerSequence(nil, nextSequence)
	if err != nil || len(nextRaw) != len(sequenceRaw) {
		return plan, errors.Join(err, ErrStateCorrupt)
	}
	plan.rows = append(plan.rows, newTransactionPut(sequenceKey, nextRaw))
	return plan, nil
}

// planRequestLedgerIssuerAdvance advances exactly one contiguous issuer
// sequence and atomically removes its GC-complete sequence row and ACK. A
// retry after outcome loss is answered solely from the retained high-water
// witness, so deleted per-request rows never need to be reconstructed.
func planRequestLedgerIssuerAdvance(
	plan requestLedgerCommandPlan,
	command requestledger.CommandView,
	snapshot pointSnapshot,
) (requestLedgerCommandPlan, error) {
	request, ok := command.IssuerAdvance()
	if !ok {
		return plan, ErrStateCorrupt
	}
	highwater, highwaterRaw, found, err := readRequestLedgerIssuerHighwater(
		snapshot, command.Home, request.IssuerDigest,
	)
	if err != nil {
		return plan, err
	}
	if !found {
		plan.completion.ResultCode = ResultRequestLedgerNotFound
		plan.completion.Phase = requestledger.PhaseInvalid
		plan.completion.Revision = 0
		plan.completion.StateDigest = requestledger.Digest{}
		return plan, nil
	}
	if highwater.Home != command.Home || highwater.IssuerDigest != request.IssuerDigest {
		return plan, ErrStateCorrupt
	}
	if command.Revision == highwater.Revision && requestledger.SameIssuerAdvance(highwater, request) {
		return witnessedRequestLedgerApplied(
			plan, highwater.Revision, requestledger.PhaseAcked, highwaterRaw, true,
		), nil
	}
	if command.ExpectedRevision != highwater.Revision ||
		request.ExpectedHighwaterDigest != highwater.HighwaterDigest {
		return witnessedRequestLedgerConflict(
			plan, highwater.Revision, requestledger.PhaseAcked, highwaterRaw,
		), nil
	}

	sequenceKey := requestledger.AppendIssuerSequenceKey(
		nil, command.Home, request.IssuerDigest, request.Sequence,
	)
	sequenceRaw, found, err := snapshot.appendRaw(nil, sequenceKey)
	if err != nil || !found {
		return plan, errors.Join(err, ErrStateCorrupt)
	}
	sequence, err := requestledger.OpenIssuerSequence(sequenceRaw)
	if err != nil || sequence.KeyDigest != command.KeyDigest ||
		sequence.RequestDigest != command.RequestDigest || sequence.Sequence != request.Sequence {
		return plan, errors.Join(err, ErrStateCorrupt)
	}
	ackKey := requestledger.AppendAckKey(nil, command.Home, command.KeyDigest)
	ackRaw, found, err := snapshot.appendRaw(nil, ackKey)
	if err != nil || !found {
		return plan, errors.Join(err, ErrStateCorrupt)
	}
	ack, err := requestledger.OpenAck(ackRaw)
	if err != nil || ack.KeyDigest != command.KeyDigest || ack.RequestDigest != command.RequestDigest ||
		ack.PlanRoot != command.PlanRoot || ack.GCPhase != requestledger.AckGCComplete {
		return plan, errors.Join(err, ErrStateCorrupt)
	}
	next, err := requestledger.AdvanceIssuerHighwater(
		highwater, sequence, ack, request, command.Revision,
	)
	if err != nil {
		return witnessedRequestLedgerConflict(
			plan, highwater.Revision, requestledger.PhaseAcked, highwaterRaw,
		), nil
	}
	nextRaw, err := requestledger.AppendIssuerHighwater(nil, next)
	if err != nil || len(nextRaw) != len(highwaterRaw) {
		return plan, errors.Join(err, ErrStateCorrupt)
	}
	highwaterKey := requestledger.AppendIssuerHighwaterKey(
		nil, command.Home, request.IssuerDigest,
	)
	plan.rows = append(plan.rows,
		newTransactionPut(highwaterKey, nextRaw),
		newTransactionDelete(sequenceKey),
		newTransactionDelete(ackKey),
	)
	sequenceBytes := int64(len(sequenceKey) + len(sequenceRaw))
	ackBytes := int64(len(ackKey) + len(ackRaw))
	plan.delta.rows -= 2
	plan.delta.residentBytes -= sequenceBytes + ackBytes
	plan.delta.ackRows--
	plan.delta.ackBytes -= ackBytes
	return witnessedRequestLedgerApplied(
		plan, next.Revision, requestledger.PhaseAcked, nextRaw, false,
	), nil
}

func readRequestLedgerIssuerHighwater(
	snapshot pointSnapshot,
	home requestledger.LedgerHome,
	issuer requestledger.Digest,
) (requestledger.IssuerHighwaterRecord, []byte, bool, error) {
	key := requestledger.AppendIssuerHighwaterKey(nil, home, issuer)
	raw, found, err := snapshot.appendRaw(nil, key)
	if err != nil || !found {
		return requestledger.IssuerHighwaterRecord{}, raw, found, err
	}
	record, err := requestledger.OpenIssuerHighwater(raw)
	if err != nil || record.Home != home || record.IssuerDigest != issuer {
		return requestledger.IssuerHighwaterRecord{}, nil, false, errors.Join(err, ErrStateCorrupt)
	}
	return record, raw, true, nil
}
