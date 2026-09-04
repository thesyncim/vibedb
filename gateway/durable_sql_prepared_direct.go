package gateway

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// DurableSQLDirectPlan is a client-owned durable recipe, not a server cache.
// The caller must fsync it with the request identity before ExecutePreparedDirect.
// Recovery must retain these exact mutation bytes, including preimage guards;
// evaluating a computed assignment again would change the command identity.
type DurableSQLDirectPlan struct {
	Key               requestledger.RequestKey
	RequestDigest     replication.Digest
	CatalogGeneration uint64
	Participant       ReplicatedTransactionParticipant
}

// PrepareDirect performs validation and linearizable preimage reads only. It
// cannot propose a mutation or admit a request in the ledger.
func (executor *DurableSQLRequestExecutor) PrepareDirect(ctx context.Context, key requestledger.RequestKey, tenant []byte, queries []Query) (plan *DurableSQLDirectPlan, err error) {
	defer func() {
		if err != nil {
			err = errors.Join(ErrDurableSQLNotAdmitted, err)
		}
	}()
	if executor == nil || executor.planner == nil || executor.data == nil || ctx == nil || !key.Valid() || key.IssuerSequence == 0 ||
		len(tenant) == 0 || requestledger.Digest(sha256.Sum256(tenant)) != key.TenantDigest {
		return nil, ErrDurableSQLRequest
	}
	if !executor.singleFast || len(queries) != 1 {
		return nil, ErrDurableSQLDirectIneligible
	}
	profile, err := executor.profile(queries)
	if err != nil {
		return nil, err
	}
	if err = validateQueryBatchAdmission(queries, profile); err != nil {
		return nil, err
	}
	if err = validateTypedQueries(ctx, queries); err != nil {
		return nil, err
	}
	opctx, cancel := context.WithTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	lease := executor.planner.catalog.pinCurrent()
	defer lease.release()
	if lease.snapshot == nil || lease.generation == 0 {
		return nil, ErrNoCatalog
	}
	participants, handled, err := executor.planner.planReplicatedSQLTransactionWithData(opctx, lease.snapshot, queries, profile, executor.data)
	if err != nil {
		return nil, err
	}
	if !handled || !preparedDirectEligible(queries, participants) {
		return nil, ErrDurableSQLDirectIneligible
	}
	return &DurableSQLDirectPlan{Key: key, RequestDigest: replicatedSQLTransactionRequestDigest(queries), CatalogGeneration: lease.generation, Participant: participants[0]}, nil
}

func preparedDirectEligible(queries []Query, participants []ReplicatedTransactionParticipant) bool {
	if directSQLMutationEligible(queries, participants) {
		return true
	}
	// RF3 UPDATE lowering already requires exactly one primary-key equality,
	// with no subquery, ORDER BY, LIMIT, RETURNING or primary-key movement. Its
	// one preimage guard therefore covers the complete row read set. Do not
	// extend this to scans, multi-key reads or a missing preimage: omitted keys
	// need absence guards, and PutPresent is not an absence guard.
	if len(queries) != 1 || len(participants) != 1 || len(participants[0].Batches) != 1 || len(participants[0].Batches[0].Mutations) != 1 {
		return false
	}
	kind := participants[0].Batches[0].Mutations[0].Kind
	if kind != replication.MutationPutDigestEqual {
		return false
	}
	statement, err := sqlast.ParseStatement(queries[0].SQL)
	return err == nil && statement.Kind == sqlast.KindUpdate
}

// ExecutePreparedDirect replays the durable recipe without replanning SQL or
// taking a new preimage. A competing write fails the replicated digest guard;
// an exact retry returns the participant's retained terminal outcome.
func (executor *DurableSQLRequestExecutor) ExecutePreparedDirect(ctx context.Context, key requestledger.RequestKey, tenant []byte, queries []Query, plan *DurableSQLDirectPlan) (DurableSQLRequestResult, error) {
	if executor == nil || executor.data == nil || ctx == nil || plan == nil || plan.Key != key || !key.Valid() ||
		len(tenant) == 0 || requestledger.Digest(sha256.Sum256(tenant)) != key.TenantDigest || plan.CatalogGeneration == 0 ||
		plan.RequestDigest != replicatedSQLTransactionRequestDigest(queries) || !preparedDirectEligible(queries, []ReplicatedTransactionParticipant{plan.Participant}) {
		return DurableSQLRequestResult{}, ErrDurableSQLRequest
	}
	profile, err := executor.profile(queries)
	if err != nil {
		return DurableSQLRequestResult{}, err
	}
	if err = validateQueryBatchAdmission(queries, profile); err != nil {
		return DurableSQLRequestResult{}, err
	}
	opctx, cancel := context.WithTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	ledgerKey, err := NewDurableRequestLedgerKey(key, plan.RequestDigest)
	if err != nil {
		return DurableSQLRequestResult{}, err
	}
	direct, err := executor.executeDirect(opctx, ledgerKey, tenant, plan.CatalogGeneration, plan.Participant)
	if direct.Result != nil && !direct.duplicate && executor.planner != nil {
		lease := executor.planner.catalog.pinCurrent()
		// Attribute locality only while the prepared route belongs to this
		// catalog generation; a recovered old recipe must not skew a new map.
		if lease.generation == plan.CatalogGeneration {
			executor.observeMutationPressure(lease.snapshot, []ReplicatedTransactionParticipant{plan.Participant})
		}
		lease.release()
	}
	if errors.Is(err, ErrReplicatedTransactionConflict) {
		err = ErrDurableSQLAborted
	}
	return direct.DurableSQLRequestResult, err
}
