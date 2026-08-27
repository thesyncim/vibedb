package requestledger

const (
	CleanupReserveBytes              = FixedStorageKeyBytes + AckRecordBytes
	RoutePinReservationBytes         = FixedStorageKeyBytes + MaxRoutePinRecordBytes
	PreparedTerminalReservationBytes = FixedStorageKeyBytes + MaxPreparedTerminalRecordBytes
	SchemaPinReleaseReservationBytes = FixedStorageKeyBytes + MaxSchemaPinReleaseRecordBytes
	ReadyReservationBytes            = ReadyStorageKeyBytes + ReadyRecordBytes
	PlanningExpiryReservationBytes   = PlanningExpiryKeyBytes + PlanningExpiryRecordBytes
)

// Reservation returns exact resident bytes derivable from Head plus the future
// byte reservation required before admitting Create. Storage-engine page and
// allocator overhead remain the owner's separately measured responsibility.
func Reservation(head HeadRecord) (residentNow, future uint64, err error) {
	if err = validateHead(head); err != nil {
		return 0, 0, err
	}
	residentPageCount := head.AppendedPageCount - head.ExpiredCleanupNextPage
	residentPlanBytes := head.AppendedPlanBytes
	if head.ExpiredCleanupNextPage != 0 {
		deletedBytes, multiplyErr := checkedMul(head.ExpiredCleanupNextPage, MaxPlanPageBytes)
		if multiplyErr != nil || deletedBytes > residentPlanBytes {
			return 0, 0, ErrCorrupt
		}
		residentPlanBytes -= deletedBytes
	}
	residentPageOverhead, multiplyErr := checkedMul(residentPageCount,
		uint64(PageStorageKeyBytes+PlanPageRecordOverheadBytes))
	if multiplyErr != nil {
		return 0, 0, multiplyErr
	}
	residentNow, err = checkedSum(
		FixedStorageKeyBytes,
		uint64(headHeaderBytes+checksumBytes+len(head.InlinePlan)),
		residentPlanBytes,
		residentPageOverhead,
	)
	if err != nil {
		return 0, 0, err
	}
	remainingPages := head.PlanPageCount - head.AppendedPageCount
	remainingBytes := head.TotalPlanBytes - head.AppendedPlanBytes
	if head.Phase == PhaseExpired {
		remainingPages, remainingBytes = 0, 0
	}
	pageFuture, err := checkedMul(remainingPages, uint64(PageStorageKeyBytes+PlanPageRecordOverheadBytes))
	if err != nil {
		return 0, 0, err
	}
	payloadFuture, err := payloadReservation(head)
	if err != nil {
		return 0, 0, err
	}
	var issuerSequenceFuture uint64
	if head.Key.IssuerEpoch != 0 {
		issuerSequenceFuture = IssuerSequenceReservationBytes
	}
	if head.Phase == PhasePlanning {
		residentNow, err = checkedSum(residentNow, PlanningExpiryReservationBytes)
		if err != nil {
			return 0, 0, err
		}
	}
	future, err = checkedSum(
		remainingBytes,
		pageFuture,
		FixedStorageKeyBytes, head.MaxPendingWaveBytes,
		FixedStorageKeyBytes, head.MaxContinuationBytes,
		RoutePinReservationBytes,
		FixedStorageKeyBytes, head.MaxTerminalBytes,
		SchemaPinReleaseReservationBytes,
		ReadyReservationBytes,
		FixedStorageKeyBytes, head.MaxTerminalBytes,
		payloadFuture,
		FixedStorageKeyBytes, AckRecordBytes,
		issuerSequenceFuture,
	)
	return residentNow, future, err
}

// PayloadReservation returns the immutable admitted budget for one dynamic
// payload build and its chunks. Materialized rows consume this budget; staging
// and cleanup do not change it. Validate the actual head, including any cleanup
// witness, rather than constructing an invalid head with its limits removed.
func PayloadReservation(head HeadRecord) (uint64, error) {
	if err := validateHead(head); err != nil {
		return 0, err
	}
	return payloadReservation(head)
}

func payloadReservation(head HeadRecord) (uint64, error) {
	chunks, err := checkedMul(head.MaxActivePayloadChunks,
		uint64(PayloadStorageKeyBytes+payloadChunkHeaderBytes+checksumBytes))
	if err != nil {
		return 0, err
	}
	var build uint64
	if head.MaxActivePayloadBytes != 0 {
		build = FixedStorageKeyBytes + payloadBuildBytes
	}
	return checkedSum(build, head.MaxActivePayloadBytes, chunks)
}

func checkedMul(left, right uint64) (uint64, error) {
	if left != 0 && right > ^uint64(0)/left {
		return 0, ErrTooLarge
	}
	return left * right, nil
}

func checkedSum(values ...uint64) (uint64, error) {
	var total uint64
	for _, value := range values {
		if total > ^uint64(0)-value {
			return 0, ErrTooLarge
		}
		total += value
	}
	return total, nil
}
