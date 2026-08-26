package gateway

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

const MaxDurableIssuerHighwaterAdvances uint32 = 1024

// DurableIssuerHighwaterCollectPlan selects one authenticated issuer lane and
// bounds the number of contiguous retirement attempts made by one call. Key's
// request component is only a full-key authentication witness for the lane;
// every advance uses the exact key returned in the coherent status image.
type DurableIssuerHighwaterCollectPlan struct {
	Home        DurableRequestLedgerHome
	Key         requestledger.RequestKey
	MaxAdvances uint32
}

type DurableIssuerHighwaterCollectResult struct {
	Highwater    requestledger.IssuerHighwaterRecord
	Applied      uint64
	Advances     uint32
	StoppedAtGap bool
}

// DurableIssuerHighwaterCollector advances only the contiguous GC-complete
// prefix of one lane. Each loop performs one linearizable fixed-size status
// read and at most one replicated CAS; it owns neither scans nor resident
// per-request maps. An ambiguous apply is resolved by the next status read.
type DurableIssuerHighwaterCollector struct {
	ledger DurableRequestLedger
}

func NewDurableIssuerHighwaterCollector(
	ledger DurableRequestLedger,
) (*DurableIssuerHighwaterCollector, error) {
	if ledger == nil {
		return nil, ErrDurableRequest
	}
	return &DurableIssuerHighwaterCollector{ledger: ledger}, nil
}

func (collector *DurableIssuerHighwaterCollector) Collect(
	ctx context.Context,
	plan DurableIssuerHighwaterCollectPlan,
) (DurableIssuerHighwaterCollectResult, error) {
	if collector == nil || collector.ledger == nil || ctx == nil || !plan.Key.Valid() ||
		plan.Key.IssuerEpoch == 0 || plan.MaxAdvances == 0 ||
		plan.MaxAdvances > MaxDurableIssuerHighwaterAdvances ||
		plan.Home.Identity == ([32]byte{}) {
		return DurableIssuerHighwaterCollectResult{}, ErrDurableRequest
	}
	home, err := requestledger.Home(plan.Key)
	if err != nil || home != plan.Home.Point {
		return DurableIssuerHighwaterCollectResult{}, errors.Join(err, ErrDurableRequestConflict)
	}
	identity, err := requestledger.IssuerIdentityFor(plan.Key)
	if err != nil {
		return DurableIssuerHighwaterCollectResult{}, errors.Join(err, ErrDurableRequestConflict)
	}

	minimum := uint64(1)
	result := DurableIssuerHighwaterCollectResult{}
	for attempts := uint32(0); attempts < plan.MaxAdvances; attempts++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		row, readErr := collector.ledger.ReadRow(ctx, plan.Home, DurableRequestLifecycleRead{
			Key: plan.Key, Kind: replicatedstate.RequestLedgerReadIssuerStatus,
			MinimumApplied: minimum,
		})
		if readErr != nil {
			return result, readErr
		}
		if !row.Found {
			result.Applied = row.Applied
			result.StoppedAtGap = true
			return result, nil
		}
		status := row.IssuerStatus
		if row.Kind != replicatedstate.RequestLedgerReadIssuerStatus ||
			status.RangeIdentity != requestledger.Digest(plan.Home.Identity) ||
			status.Highwater.Identity != identity || status.Highwater.Home != plan.Home.Point {
			return DurableIssuerHighwaterCollectResult{}, ErrDurableRequestConflict
		}
		result.Highwater, result.Applied = status.Highwater, row.Applied
		if !status.NextFound || !status.AdvanceReady {
			result.StoppedAtGap = true
			return result, nil
		}
		if status.Highwater.Revision == ^uint64(0) {
			return result, ErrDurableRequestConflict
		}
		advance, buildErr := requestledger.NewIssuerAdvanceRequest(
			status.Highwater, status.Sequence, status.Ack,
		)
		if buildErr != nil {
			return DurableIssuerHighwaterCollectResult{}, errors.Join(buildErr, ErrDurableRequestConflict)
		}
		apply, applyErr := collector.ledger.ApplyCAS(ctx, plan.Home, status.Ack.Key,
			DurableRequestLifecycleCAS{
				Operation:        requestledger.OperationAdvanceIssuerHighwater,
				ExpectedRevision: status.Highwater.Revision,
				Revision:         status.Highwater.Revision + 1,
				Head: requestledger.HeadRecord{
					RequestDigest: status.Ack.RequestDigest,
					PlanRoot:      status.Ack.PlanRoot,
				},
				IssuerAdvance: advance,
			})
		if applyErr == nil {
			if apply.Ledger.ResultCode != replicatedstate.ResultApplied {
				// A racing collector may have won the same CAS. Re-read the
				// retained highwater witness instead of treating that as fatal.
				minimum = row.Applied
				continue
			}
			minimum = apply.Applied
			nextHighwater, nextErr := requestledger.AdvanceIssuerHighwater(
				status.Highwater, status.Sequence, status.Ack, advance, status.Highwater.Revision+1,
			)
			if nextErr != nil {
				return result, errors.Join(nextErr, ErrDurableRequestConflict)
			}
			result.Highwater, result.Applied = nextHighwater, apply.Applied
			result.Advances++
			continue
		}
		// Outcome unknown is resolved only by a subsequent linearizable read.
		// If that read also fails, preserve both causes for the caller's retry.
		next, nextErr := collector.ledger.ReadRow(ctx, plan.Home, DurableRequestLifecycleRead{
			Key: plan.Key, Kind: replicatedstate.RequestLedgerReadIssuerStatus,
			MinimumApplied: row.Applied,
		})
		if nextErr != nil {
			return result, errors.Join(applyErr, nextErr)
		}
		if !next.Found || next.Kind != replicatedstate.RequestLedgerReadIssuerStatus ||
			next.IssuerStatus.RangeIdentity != requestledger.Digest(plan.Home.Identity) ||
			next.IssuerStatus.Highwater.Identity != identity ||
			next.IssuerStatus.Highwater.Home != plan.Home.Point {
			return result, errors.Join(applyErr, ErrDurableRequestConflict)
		}
		if next.IssuerStatus.Highwater.HighwaterSequence >= status.Sequence.Sequence {
			result.Advances++
		}
		result.Highwater, result.Applied = next.IssuerStatus.Highwater, next.Applied
		minimum = next.Applied
	}
	return result, nil
}
