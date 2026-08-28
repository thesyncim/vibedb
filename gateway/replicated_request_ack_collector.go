package gateway

import (
	"context"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

// DurableRequestAckPlan proves possession of one terminal result. AckToken is
// intentionally the raw, one-time capability returned with that result; only
// its digest survives collection.
type DurableRequestAckPlan struct {
	Home             DurableRequestLedgerHome
	Key              requestledger.RequestKey
	TerminalRevision uint64
	ResultDigest     requestledger.Digest
	AckToken         requestledger.AckToken
}

type DurableRequestAckResult struct {
	Ack     requestledger.AckRecord
	Applied uint64
	Rounds  uint64
}

// DurableRequestAckCollector durably replaces a terminal result with its
// compact anti-replay tombstone, then reclaims the preceding ledger rows in
// bounded replicated chunks. Re-entering after any ambiguous response resumes
// from the authoritative AckRecord.
type DurableRequestAckCollector struct {
	ledger DurableRequestLedger
}

func NewDurableRequestAckCollector(ledger DurableRequestLedger) (*DurableRequestAckCollector, error) {
	if ledger == nil {
		return nil, ErrDurableRequest
	}
	return &DurableRequestAckCollector{ledger: ledger}, nil
}

func (collector *DurableRequestAckCollector) AcknowledgeAndCollect(
	ctx context.Context,
	plan DurableRequestAckPlan,
) (DurableRequestAckResult, error) {
	if collector == nil || collector.ledger == nil || ctx == nil || !plan.Key.Valid() ||
		plan.Home.Identity == ([32]byte{}) || plan.TerminalRevision == 0 ||
		plan.ResultDigest == (requestledger.Digest{}) || plan.AckToken == (requestledger.AckToken{}) {
		return DurableRequestAckResult{}, ErrDurableRequest
	}
	head, ack, applied, err := collector.openAckState(ctx, plan, 1)
	if err != nil {
		return DurableRequestAckResult{}, err
	}
	if ack.Revision == 0 {
		terminalRow, readErr := collector.ledger.ReadRow(ctx, plan.Home, DurableRequestLifecycleRead{
			Key: plan.Key, Kind: replicatedstate.RequestLedgerReadTerminal,
			MinimumApplied: applied,
		})
		if readErr != nil {
			return DurableRequestAckResult{}, readErr
		}
		if !terminalRow.Found || terminalRow.Kind != replicatedstate.RequestLedgerReadTerminal ||
			terminalRow.Terminal.Revision != plan.TerminalRevision ||
			terminalRow.Terminal.ResultDigest != plan.ResultDigest ||
			terminalRow.Terminal.AckToken != plan.AckToken || head.Revision != plan.TerminalRevision {
			return DurableRequestAckResult{}, ErrDurableRequestConflict
		}
		request := requestledger.AckRequest{
			TerminalRevision: plan.TerminalRevision,
			ResultDigest:     plan.ResultDigest,
			AckToken:         plan.AckToken,
		}
		result, applyErr := collector.ledger.ApplyCAS(ctx, plan.Home, plan.Key,
			DurableRequestLifecycleCAS{
				Operation:        requestledger.OperationAck,
				ExpectedRevision: head.Revision, Revision: head.Revision + 1,
				Head: head, Ack: request,
			})
		minimum := applied
		if applyErr == nil {
			if result.Ledger.ResultCode != replicatedstate.ResultApplied {
				return DurableRequestAckResult{}, ErrDurableRequestConflict
			}
			minimum = result.Applied
		}
		_, ack, applied, err = collector.openAckState(ctx, plan, minimum)
		if err != nil {
			return DurableRequestAckResult{}, errors.Join(applyErr, err)
		}
	}
	if !ackMatchesPlan(ack, plan) {
		return DurableRequestAckResult{}, ErrDurableRequestConflict
	}

	var rounds uint64
	for ack.GCPhase != requestledger.AckGCComplete {
		if err := ctx.Err(); err != nil {
			return DurableRequestAckResult{Ack: ack, Applied: applied, Rounds: rounds}, err
		}
		if rounds == math.MaxUint64 {
			return DurableRequestAckResult{Ack: ack, Applied: applied, Rounds: rounds}, ErrDurableRequest
		}
		request, buildErr := requestledger.NewCollectRequest(
			ack.AckDigest, requestledger.MaxAckGCDeleteRows, math.MaxUint32,
		)
		if buildErr != nil {
			return DurableRequestAckResult{}, errors.Join(buildErr, ErrDurableRequestConflict)
		}
		witness := requestLedgerAckHead(ack)
		result, applyErr := collector.ledger.ApplyCAS(ctx, plan.Home, plan.Key,
			DurableRequestLifecycleCAS{
				Operation:        requestledger.OperationGC,
				ExpectedRevision: ack.Revision, Revision: ack.Revision + 1,
				Head: witness, GC: request,
			})
		minimum := applied
		if applyErr == nil {
			if result.Ledger.ResultCode != replicatedstate.ResultApplied {
				return DurableRequestAckResult{}, ErrDurableRequestConflict
			}
			minimum = result.Applied
		}
		_, next, nextApplied, readErr := collector.openAckState(ctx, plan, minimum)
		if readErr != nil {
			return DurableRequestAckResult{Ack: ack, Applied: applied, Rounds: rounds},
				errors.Join(applyErr, readErr)
		}
		if next.Revision <= ack.Revision || next.PriorAckDigest != ack.AckDigest ||
			next.ReclaimedBytes <= ack.ReclaimedBytes || !ackMatchesPlan(next, plan) {
			return DurableRequestAckResult{}, ErrDurableRequestConflict
		}
		ack, applied = next, nextApplied
		rounds++
	}
	return DurableRequestAckResult{Ack: ack, Applied: applied, Rounds: rounds}, nil
}

func (collector *DurableRequestAckCollector) openAckState(
	ctx context.Context,
	plan DurableRequestAckPlan,
	minimum uint64,
) (requestledger.HeadRecord, requestledger.AckRecord, uint64, error) {
	row, err := collector.ledger.ReadRow(ctx, plan.Home, DurableRequestLifecycleRead{
		Key: plan.Key, Kind: replicatedstate.RequestLedgerReadHead, MinimumApplied: minimum,
	})
	if err != nil || !row.Found {
		return requestledger.HeadRecord{}, requestledger.AckRecord{}, 0,
			errors.Join(err, ErrDurableRequestUnresolved)
	}
	switch row.Kind {
	case replicatedstate.RequestLedgerReadHead:
		return row.Head, requestledger.AckRecord{}, row.Applied, nil
	case replicatedstate.RequestLedgerReadAck:
		return requestledger.HeadRecord{}, row.Ack, row.Applied, nil
	default:
		return requestledger.HeadRecord{}, requestledger.AckRecord{}, 0, ErrDurableRequestConflict
	}
}

func ackMatchesPlan(ack requestledger.AckRecord, plan DurableRequestAckPlan) bool {
	return ack.Key == plan.Key && ack.TerminalRevision == plan.TerminalRevision &&
		ack.ResultDigest == plan.ResultDigest &&
		ack.AckTokenDigest == requestledger.AckTokenDigest(plan.AckToken)
}

func requestLedgerAckHead(ack requestledger.AckRecord) requestledger.HeadRecord {
	return requestledger.HeadRecord{
		RequestDigest: ack.RequestDigest,
		PlanRoot:      ack.PlanRoot,
	}
}
