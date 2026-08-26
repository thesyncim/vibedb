package replicatedstate

import (
	"errors"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/requestledger"
)

// requestLedgerImageScanner authenticates the hidden ledger image in one
// ordered pass. Storage keys keep one request contiguous, so reopen scratch is
// constant-sized regardless of retained request count.
type requestLedgerImageScanner struct {
	capacity  uint64
	cleanup   uint64
	authority RequestLedgerRange

	rows, resident, reserved uint64
	ackRows, ackBytes        uint64

	current                                                   bool
	home                                                      requestledger.LedgerHome
	key                                                       requestledger.Digest
	requestRows                                               uint64
	requestBytes                                              uint64
	ackRowBytes                                               uint64
	head                                                      requestledger.HeadRecord
	ack                                                       requestledger.AckRecord
	pending                                                   requestledger.PendingWaveRecord
	continuation                                              requestledger.ContinuationRecord
	terminal                                                  requestledger.TerminalRecord
	payloadBuild                                              requestledger.PayloadBuildRecord
	headFound                                                 bool
	ackFound                                                  bool
	pendingFound                                              bool
	continuationFound                                         bool
	terminalFound                                             bool
	payloadBuildFound                                         bool
	pendingBytes, continuationBytes                           int
	payloadResident                                           uint64
	pageCount, pageBytes, pageFirstOrdinal                    uint64
	pageChain, pageRoot                                       requestledger.Digest
	payloadChunkCount, payloadChunkBytes, payloadFirstOrdinal uint64
	payloadChunkContent, payloadChunkBuild, payloadChunkChain requestledger.Digest
}

func newRequestLedgerImageScanner(capacity, cleanup uint64, authority RequestLedgerRange) requestLedgerImageScanner {
	return requestLedgerImageScanner{capacity: capacity, cleanup: cleanup, authority: authority}
}

func (scan *requestLedgerImageScanner) observe(key, value []byte) error {
	if scan == nil || !scan.authority.enabled() {
		return ErrStateCorrupt
	}
	view, err := requestledger.OpenStorageKey(key)
	if err != nil || !scan.authority.contains(view.Home) {
		return errors.Join(err, ErrStateCorrupt)
	}
	if scan.current && (scan.home != view.Home || scan.key != view.Key) {
		if err := scan.finishRequest(); err != nil {
			return err
		}
		scan.resetRequest()
	}
	if !scan.current {
		scan.current, scan.home, scan.key = true, view.Home, view.Key
	}
	rowBytes := uint64(len(key) + len(value))
	if scan.rows == math.MaxUint64 || scan.resident > math.MaxUint64-rowBytes ||
		scan.requestRows == math.MaxUint64 || scan.requestBytes > math.MaxUint64-rowBytes {
		return ErrStateCorrupt
	}
	scan.rows++
	scan.resident += rowBytes
	scan.requestRows++
	scan.requestBytes += rowBytes

	switch view.Kind {
	case requestledger.StorageHead:
		if scan.headFound {
			return ErrStateCorrupt
		}
		scan.head, err = requestledger.OpenHead(value)
		if err == nil && scan.head.KeyDigest == view.Key {
			var home requestledger.LedgerHome
			home, err = requestledger.Home(scan.head.Key)
			if err == nil && home != view.Home {
				err = ErrStateCorrupt
			}
		} else if err == nil {
			err = ErrStateCorrupt
		}
		scan.headFound = err == nil
	case requestledger.StoragePlanPage:
		page, openErr := requestledger.OpenPlanPage(value)
		if openErr != nil || page.KeyDigest != view.Key || page.Ordinal != view.Ordinal ||
			(scan.pageCount != 0 && (page.Ordinal != scan.pageFirstOrdinal+scan.pageCount ||
				page.PreviousChain != scan.pageChain)) ||
			(scan.pageCount != 0 && page.PlanRoot != scan.pageRoot) ||
			scan.pageBytes > math.MaxUint64-uint64(len(page.Data)) {
			err = errors.Join(openErr, ErrStateCorrupt)
			break
		}
		if scan.pageCount == 0 {
			scan.pageFirstOrdinal = page.Ordinal
			scan.pageRoot = page.PlanRoot
		}
		scan.pageCount++
		scan.pageBytes += uint64(len(page.Data))
		scan.pageChain = page.Chain
	case requestledger.StoragePending:
		if scan.pendingFound {
			return ErrStateCorrupt
		}
		var scratch [requestledger.MaxPendingWaveSteps]requestledger.StepRef
		pending, openErr := requestledger.OpenPendingWaveInto(value, scratch[:])
		if openErr != nil || pending.Key() != view.Key {
			err = errors.Join(openErr, ErrStateCorrupt)
			break
		}
		scan.pending = pending.Record()
		scan.pending.Steps = nil
		scan.pendingBytes = len(value)
		scan.pendingFound = true
	case requestledger.StorageContinuation:
		if scan.continuationFound {
			return ErrStateCorrupt
		}
		scan.continuation, err = requestledger.OpenContinuation(value)
		if err == nil && scan.continuation.KeyDigest != view.Key {
			err = ErrStateCorrupt
		}
		scan.continuationBytes = len(value)
		scan.continuationFound = err == nil
	case requestledger.StorageTerminal:
		if scan.terminalFound {
			return ErrStateCorrupt
		}
		scan.terminal, err = requestledger.OpenTerminal(value)
		if err == nil && scan.terminal.KeyDigest != view.Key {
			err = ErrStateCorrupt
		}
		scan.terminalFound = err == nil
	case requestledger.StorageAck:
		if scan.ackFound {
			return ErrStateCorrupt
		}
		scan.ack, err = requestledger.OpenAck(value)
		if err == nil && scan.ack.KeyDigest == view.Key {
			var home requestledger.LedgerHome
			home, err = requestledger.Home(scan.ack.Key)
			if err == nil && home != view.Home {
				err = ErrStateCorrupt
			}
		} else if err == nil {
			err = ErrStateCorrupt
		}
		if err == nil {
			if scan.ackRows == math.MaxUint64 || scan.ackBytes > math.MaxUint64-rowBytes {
				return ErrStateCorrupt
			}
			scan.ackFound = true
			scan.ackRowBytes = rowBytes
			scan.ackRows++
			scan.ackBytes += rowBytes
		}
	case requestledger.StoragePayloadChunk:
		chunk, openErr := requestledger.OpenPayloadChunk(value)
		if openErr != nil || chunk.KeyDigest != view.Key || chunk.ContentRoot != view.Content ||
			chunk.Ordinal != view.Ordinal ||
			(scan.payloadChunkCount != 0 && (chunk.Ordinal != scan.payloadFirstOrdinal+scan.payloadChunkCount ||
				chunk.PreviousChain != scan.payloadChunkChain)) ||
			(scan.payloadChunkCount != 0 && (chunk.ContentRoot != scan.payloadChunkContent ||
				chunk.BuildDigest != scan.payloadChunkBuild)) ||
			scan.payloadChunkBytes > math.MaxUint64-rowBytes {
			err = errors.Join(openErr, ErrStateCorrupt)
			break
		}
		if scan.payloadChunkCount == 0 {
			scan.payloadFirstOrdinal = chunk.Ordinal
			scan.payloadChunkContent, scan.payloadChunkBuild = chunk.ContentRoot, chunk.BuildDigest
		}
		scan.payloadChunkCount++
		scan.payloadChunkBytes += rowBytes
		scan.payloadResident += rowBytes
		scan.payloadChunkChain = chunk.Chain
	case requestledger.StoragePayloadBuild:
		if scan.payloadBuildFound {
			return ErrStateCorrupt
		}
		scan.payloadBuild, err = requestledger.OpenPayloadBuild(value)
		if err == nil && scan.payloadBuild.KeyDigest != view.Key {
			err = ErrStateCorrupt
		}
		if err == nil {
			scan.payloadBuildFound = true
			if scan.payloadResident > math.MaxUint64-rowBytes {
				return ErrStateCorrupt
			}
			scan.payloadResident += rowBytes
		}
	default:
		err = ErrStateCorrupt
	}
	return err
}

func (scan *requestLedgerImageScanner) finishRequest() error {
	if scan == nil || !scan.current || scan.requestRows == 0 {
		return nil
	}
	if scan.ackFound {
		if scan.requestBytes < scan.ackRowBytes ||
			scan.ack.PriorEncodedBytes < scan.ack.ReclaimedBytes ||
			scan.requestBytes-scan.ackRowBytes != scan.ack.PriorEncodedBytes-scan.ack.ReclaimedBytes ||
			(scan.ack.GCPhase == requestledger.AckGCComplete && scan.requestBytes != scan.ackRowBytes) {
			return fmt.Errorf("%w: request ledger ACK accounting", ErrStateCorrupt)
		}
		if scan.headFound && (scan.head.Key != scan.ack.Key || scan.head.RequestDigest != scan.ack.RequestDigest ||
			scan.head.PlanRoot != scan.ack.PlanRoot) {
			return fmt.Errorf("%w: request ledger ACK identity", ErrStateCorrupt)
		}
		if scan.pageCount != 0 && scan.pageRoot != scan.ack.PlanRoot ||
			scan.pendingFound && (scan.pending.KeyDigest != scan.ack.KeyDigest ||
				scan.pending.RequestDigest != scan.ack.RequestDigest || scan.pending.PlanRoot != scan.ack.PlanRoot) ||
			scan.continuationFound && (scan.continuation.KeyDigest != scan.ack.KeyDigest ||
				scan.continuation.RequestDigest != scan.ack.RequestDigest || scan.continuation.PlanRoot != scan.ack.PlanRoot) ||
			scan.terminalFound && (scan.terminal.KeyDigest != scan.ack.KeyDigest ||
				scan.terminal.RequestDigest != scan.ack.RequestDigest || scan.terminal.PlanRoot != scan.ack.PlanRoot ||
				scan.terminal.ResultDigest != scan.ack.ResultDigest) ||
			scan.payloadBuildFound && (scan.payloadBuild.KeyDigest != scan.ack.KeyDigest ||
				scan.payloadBuild.RequestDigest != scan.ack.RequestDigest || scan.payloadBuild.PlanRoot != scan.ack.PlanRoot) {
			return fmt.Errorf("%w: request ledger ACK child identity", ErrStateCorrupt)
		}
		return nil
	}
	if !scan.headFound || scan.head.KeyDigest != scan.key || scan.pageFirstOrdinal != 0 ||
		scan.pageCount != scan.head.AppendedPageCount || scan.pageBytes != scan.head.AppendedPlanBytes ||
		scan.pageChain != scan.head.PageChain {
		// Inline plan bytes live in Head rather than page rows.
		if !scan.headFound || scan.head.PlanPageCount != 0 || scan.pageCount != 0 || scan.pageBytes != 0 ||
			scan.pageChain != (requestledger.Digest{}) {
			return fmt.Errorf("%w: request ledger plan image", ErrStateCorrupt)
		}
	}
	if scan.head.PlanPageCount != 0 && (scan.pageFirstOrdinal != 0 || scan.pageCount != scan.head.AppendedPageCount ||
		scan.pageBytes != scan.head.AppendedPlanBytes || scan.pageChain != scan.head.PageChain) {
		return fmt.Errorf("%w: request ledger page chain", ErrStateCorrupt)
	}
	if scan.pendingFound && (scan.pending.KeyDigest != scan.head.KeyDigest ||
		scan.pending.RequestDigest != scan.head.RequestDigest || scan.pending.PlanRoot != scan.head.PlanRoot ||
		scan.pending.Revision != scan.head.Revision || scan.pending.WaveOrdinal != scan.head.NextStepOrdinal ||
		scan.pending.PriorContinuationDigest != scan.head.ContinuationDigest) {
		return fmt.Errorf("%w: request ledger pending", ErrStateCorrupt)
	}
	if scan.continuationFound && (scan.continuation.KeyDigest != scan.head.KeyDigest ||
		scan.continuation.RequestDigest != scan.head.RequestDigest || scan.continuation.PlanRoot != scan.head.PlanRoot ||
		scan.continuation.ContinuationDigest != scan.head.ContinuationDigest) {
		return fmt.Errorf("%w: request ledger continuation", ErrStateCorrupt)
	}
	if scan.terminalFound != (scan.head.Phase == requestledger.PhaseTerminal) ||
		scan.terminalFound && (scan.terminal.KeyDigest != scan.head.KeyDigest ||
			scan.terminal.RequestDigest != scan.head.RequestDigest || scan.terminal.PlanRoot != scan.head.PlanRoot ||
			scan.terminal.Revision != scan.head.Revision || scan.pendingFound || !scan.continuationFound) {
		return fmt.Errorf("%w: request ledger terminal", ErrStateCorrupt)
	}
	if scan.head.Phase == requestledger.PhasePlanning &&
		(scan.pendingFound || scan.continuationFound || scan.terminalFound || scan.payloadBuildFound || scan.payloadChunkCount != 0) {
		return fmt.Errorf("%w: request ledger planning children", ErrStateCorrupt)
	}
	if scan.payloadBuildFound {
		build := scan.payloadBuild
		if scan.head.Phase != requestledger.PhaseSealed || build.RequestDigest != scan.head.RequestDigest ||
			build.PlanRoot != scan.head.PlanRoot || build.StagedBytes > build.TotalBytes ||
			build.ContentRoot != scan.payloadChunkContent && scan.payloadChunkCount != 0 ||
			build.BuildDigest != scan.payloadChunkBuild && scan.payloadChunkCount != 0 ||
			build.Chain != scan.payloadChunkChain {
			return fmt.Errorf("%w: request ledger payload build", ErrStateCorrupt)
		}
		if scan.head.CleanupBuildDigest != (requestledger.Digest{}) {
			if build.Phase != requestledger.PayloadBuildSealed ||
				build.BuildDigest != scan.head.CleanupBuildDigest ||
				build.WaveOrdinal == ^uint64(0) || build.WaveOrdinal+1 != scan.head.NextStepOrdinal ||
				!scan.continuationFound ||
				build.PriorContinuationDigest != scan.continuation.PriorContinuationDigest ||
				scan.payloadFirstOrdinal != scan.head.CleanupNextChunk ||
				scan.payloadChunkCount != scan.head.CleanupChunkCount-scan.head.CleanupNextChunk ||
				scan.payloadResident != scan.head.CleanupPayloadBytes {
				return fmt.Errorf("%w: request ledger payload cleanup", ErrStateCorrupt)
			}
		} else if build.WaveOrdinal != scan.head.NextStepOrdinal ||
			build.PriorContinuationDigest != scan.head.ContinuationDigest ||
			scan.payloadFirstOrdinal != 0 || build.NextChunkOrdinal != scan.payloadChunkCount {
			return fmt.Errorf("%w: request ledger payload staging", ErrStateCorrupt)
		}
	} else if scan.payloadChunkCount != 0 {
		return fmt.Errorf("%w: request ledger orphan payload chunks", ErrStateCorrupt)
	}
	reserved, ok := requestLedgerReservedBytes(
		scan.head, scan.pendingBytes, scan.continuationBytes, scan.payloadResident,
	)
	if !ok || scan.reserved > math.MaxUint64-reserved {
		return fmt.Errorf("%w: request ledger reservation", ErrStateCorrupt)
	}
	scan.reserved += reserved
	return nil
}

func (scan *requestLedgerImageScanner) resetRequest() {
	capacity, cleanup, authority := scan.capacity, scan.cleanup, scan.authority
	rows, resident, reserved := scan.rows, scan.resident, scan.reserved
	ackRows, ackBytes := scan.ackRows, scan.ackBytes
	*scan = requestLedgerImageScanner{capacity: capacity, cleanup: cleanup, authority: authority,
		rows: rows, resident: resident, reserved: reserved, ackRows: ackRows, ackBytes: ackBytes}
}

func (scan *requestLedgerImageScanner) finish(state State) error {
	if scan == nil {
		return ErrStateCorrupt
	}
	if err := scan.finishRequest(); err != nil {
		return err
	}
	if scan.rows != state.RequestLedgerRows || scan.resident != state.RequestLedgerResidentBytes ||
		scan.reserved != state.RequestLedgerReservedBytes || scan.ackRows != state.RequestLedgerAckRows ||
		scan.ackBytes != state.RequestLedgerAckBytes {
		return fmt.Errorf("%w: request ledger image accounting", ErrStateCorrupt)
	}
	if !scan.authority.enabled() {
		if scan.rows != 0 || scan.resident != 0 || scan.reserved != 0 || scan.ackRows != 0 || scan.ackBytes != 0 {
			return ErrStateCorrupt
		}
		return nil
	}
	if scan.capacity <= scan.cleanup || scan.resident > math.MaxUint64-scan.reserved ||
		scan.resident+scan.reserved > scan.capacity-scan.cleanup {
		return fmt.Errorf("%w: request ledger capacity image", ErrStateCorrupt)
	}
	return nil
}
