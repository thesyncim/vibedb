package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// DurableSQLDirectPlan is a client-owned durable recipe, not a server cache.
// Durable outbox clients must fsync it with the identity before execution.
// PG autocommit clients may instead durably reserve never-reused sequences and
// report ambiguous outcomes after a crash. Any retry of an identity must retain
// these exact mutation bytes, including preimage guards;
// evaluating a computed assignment again would change the command identity.
type DurableSQLDirectPlan struct {
	Key               requestledger.RequestKey
	RequestDigest     replication.Digest
	CatalogGeneration uint64
	Target            ReplicatedTransactionTarget
}

// PrepareDirect performs validation and a bounded point preimage read. An
// eligible UPDATE may use the committed leader read because the retained
// mutation carries an exact full-row digest guard checked atomically at apply;
// every other shape uses the original linearizable lowering.
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
	opctx, cancel := tightenTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	var lease catalogLease
	var targets []ReplicatedTransactionTarget
	var handled bool
	refreshedMiss := false
	for {
		if contextErr := context.Cause(opctx); contextErr != nil {
			return nil, contextErr
		}
		lease = executor.planner.catalog.pinCurrent()
		if lease.snapshot == nil || lease.generation == 0 {
			lease.release()
			return nil, ErrNoCatalog
		}
		targets, handled, err = executor.planner.planReplicatedSQLTransactionWithDataMode(
			opctx, lease.snapshot, queries, profile, executor.data,
			replicatedSQLCommittedLeaderPreimage,
		)
		if errors.Is(err, errPreparedDirectFallback) {
			targets, handled, err = executor.planner.planReplicatedSQLTransactionWithData(
				opctx, lease.snapshot, queries, profile, executor.data,
			)
		}
		if !(errors.Is(err, ErrTableNotPlaced) || errors.Is(err, raftservice.ErrServingFence)) || refreshedMiss {
			break
		}
		refreshedMiss = true
		staleGeneration := lease.generation
		lease.release()
		if refreshErr := executor.planner.refreshAfterCatalogMiss(opctx, staleGeneration); refreshErr != nil {
			return nil, preserveCatalogMiss(err, refreshErr)
		}
	}
	defer lease.release()
	if err != nil {
		return nil, err
	}
	if !handled || !preparedDirectEligible(queries, targets) {
		return nil, ErrDurableSQLDirectIneligible
	}
	if contextErr := context.Cause(opctx); contextErr != nil {
		return nil, contextErr
	}
	return &DurableSQLDirectPlan{Key: key, RequestDigest: replicatedSQLTransactionRequestDigest(queries), CatalogGeneration: lease.generation, Target: targets[0]}, nil
}

func preparedDirectEligible(queries []Query, targets []ReplicatedTransactionTarget) bool {
	if directSQLMutationEligible(queries, targets) {
		return true
	}
	// RF3 UPDATE lowering already requires exactly one primary-key equality,
	// with no subquery, ORDER BY, LIMIT, RETURNING or primary-key movement. Its
	// one preimage guard therefore covers the complete row read set. Do not
	// extend this to scans, multi-key reads or a missing preimage: omitted keys
	// need absence guards, and PutPresent is not an absence guard.
	if len(queries) != 1 || !preparedDirectFullRowGuard(targets) {
		return false
	}
	statement, err := sqlast.ParseStatement(queries[0].SQL)
	return err == nil && statement.Kind == sqlast.KindUpdate
}

func preparedDirectFullRowGuard(targets []ReplicatedTransactionTarget) bool {
	if len(targets) != 1 || len(targets[0].Batches) != 1 || len(targets[0].Batches[0].Mutations) != 1 {
		return false
	}
	mutation := targets[0].Batches[0].Mutations[0]
	return mutation.Kind == replication.MutationPutDigestEqual && mutation.ExpectedValueLength != 0 &&
		mutation.ExpectedValueDigest != (replication.Digest{})
}

// ExecutePreparedDirect replays the durable recipe without replanning SQL or
// taking a new preimage. A competing write fails the replicated digest guard;
// an exact retry returns the target's retained terminal outcome.
func (executor *DurableSQLRequestExecutor) ExecutePreparedDirect(
	ctx context.Context,
	key requestledger.RequestKey,
	tenant []byte,
	queries []Query,
	plan *DurableSQLDirectPlan,
	priorUnknown bool,
) (DurableSQLRequestResult, error) {
	if executor == nil || executor.data == nil || ctx == nil || plan == nil || plan.Key != key || !key.Valid() ||
		len(tenant) == 0 || requestledger.Digest(sha256.Sum256(tenant)) != key.TenantDigest || plan.CatalogGeneration == 0 ||
		plan.RequestDigest != replicatedSQLTransactionRequestDigest(queries) || !preparedDirectEligible(queries, []ReplicatedTransactionTarget{plan.Target}) {
		return DurableSQLRequestResult{}, ErrDurableSQLRequest
	}
	profile, err := executor.profile(queries)
	if err != nil {
		return DurableSQLRequestResult{}, err
	}
	if err = validateQueryBatchAdmission(queries, profile); err != nil {
		return DurableSQLRequestResult{}, err
	}
	opctx, cancel := tightenTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	ledgerKey, err := NewDurableRequestLedgerKey(key, plan.RequestDigest)
	if err != nil {
		return DurableSQLRequestResult{}, err
	}
	direct, err := executor.executeDirect(
		opctx, ledgerKey, tenant, plan.CatalogGeneration, plan.Target, priorUnknown,
	)
	if direct.Result != nil && !direct.duplicate && executor.planner != nil {
		lease := executor.planner.catalog.pinCurrent()
		// Attribute locality only while the prepared route belongs to this
		// catalog generation; a recovered old recipe must not skew a new map.
		if lease.generation == plan.CatalogGeneration {
			executor.observeMutationPressure(lease.snapshot, []ReplicatedTransactionTarget{plan.Target})
		}
		lease.release()
	}
	if errors.Is(err, ErrReplicatedTransactionConflict) {
		err = durableSQLAborted(direct.resultCode)
	}
	if errors.Is(err, errReplicatedNotAdmitted) {
		err = errors.Join(ErrDurableSQLNotAdmitted, err)
	}
	var refusal *ReplicatedRefusalError
	if !errors.Is(err, raftservice.ErrOutcomeUnknown) && errors.As(err, &refusal) {
		switch refusal.Code {
		case shardservice.ReplicatedRefusalUnavailable, shardservice.ReplicatedRefusalStaleFence,
			shardservice.ReplicatedRefusalUnauthorized, shardservice.ReplicatedRefusalAdmissionBound,
			shardservice.ReplicatedRefusalProposalRefused:
			// This invocation has a validated pre-admission refusal. An earlier
			// invocation of the same durable recipe may still be unresolved.
			err = errors.Join(ErrDurableSQLNotAdmitted, err)
		}
	}
	if errors.Is(err, ErrDurableSQLNotAdmitted) && executor.planner != nil &&
		(errors.Is(err, raftservice.ErrServingFence) || errors.Is(err, raftserve.ErrProposalRefused)) {
		// Only a certified catalog refresh can authorize a newly planned write.
		// This retained recipe remains unchanged, including on recovery after an
		// earlier invocation with an unknown outcome.
		if refreshErr := executor.planner.refreshAfterCatalogMiss(opctx, plan.CatalogGeneration); refreshErr != nil {
			err = errors.Join(err, refreshErr)
		}
	}
	return direct.DurableSQLRequestResult, err
}

// currentDirectRecoveryTarget resolves the retained logical shard in the
// latest published catalog and changes only its physical serving route. The
// mutation batches, scopes, identity, and request digest remain the original
// durable recipe. A failed refresh may still use the same currently published
// route; any mismatch in its immutable logical contract fails closed.
func (executor *DurableSQLRequestExecutor) currentDirectRecoveryTarget(
	ctx context.Context,
	staleGeneration uint64,
	target ReplicatedTransactionTarget,
) (uint64, ReplicatedTransactionTarget, error) {
	if executor == nil || executor.planner == nil || executor.planner.catalog == nil ||
		ctx == nil || staleGeneration == 0 {
		return 0, ReplicatedTransactionTarget{}, ErrDurableSQLRequest
	}
	lease := executor.planner.catalog.pinCurrent()
	if lease.snapshot == nil || lease.generation == 0 || lease.generation < staleGeneration {
		lease.release()
		return 0, ReplicatedTransactionTarget{}, ErrNoCatalog
	}
	resolve := func(snapshot *Snapshot) (ReplicatedRoute, bool) {
		replicas := make([]ReplicatedEndpoint, 0, ServingReplicaCount)
		route, ok := snapshot.ResolveReplicatedRoute(
			target.Route.Distribution, target.Route.Shard, replicas,
		)
		return route, ok
	}
	currentRoute, resolved := resolve(lease.snapshot)
	diagnoseDirectRecoveryRoute("pinned", staleGeneration, lease.generation, target.Route, currentRoute, resolved, nil)
	if !resolved {
		lease.release()
		return 0, ReplicatedTransactionTarget{}, ErrReplicatedRoute
	}
	// The global catalog generation can advance for an unrelated shard while
	// this retained recipe's own route remains stale. In that case refresh from
	// the generation actually pinned here, rather than treating the unrelated
	// publication as proof that this shard's route was refreshed.
	if lease.generation == staleGeneration || currentRoute.Command == target.Route.Command {
		refreshFloor := lease.generation
		lease.release()
		refreshErr := executor.planner.refreshAfterCatalogMiss(ctx, refreshFloor)
		if contextErr := context.Cause(ctx); contextErr != nil {
			return 0, ReplicatedTransactionTarget{}, errors.Join(refreshErr, contextErr)
		}
		lease = executor.planner.catalog.pinCurrent()
		if lease.snapshot == nil || lease.generation == 0 || lease.generation < refreshFloor {
			lease.release()
			return 0, ReplicatedTransactionTarget{}, errors.Join(refreshErr, ErrNoCatalog)
		}
		currentRoute, resolved = resolve(lease.snapshot)
		diagnoseDirectRecoveryRoute("refreshed", staleGeneration, lease.generation, target.Route, currentRoute, resolved, refreshErr)
		if !resolved {
			lease.release()
			return 0, ReplicatedTransactionTarget{}, errors.Join(ErrReplicatedRoute, refreshErr)
		}
	}
	defer lease.release()
	if !directMutationRouteAdvanceAllowed(target.Route, currentRoute) {
		diagnoseDirectRecoveryRoute("rejected", staleGeneration, lease.generation, target.Route, currentRoute, true, ErrReplicatedRoute)
		return 0, ReplicatedTransactionTarget{}, ErrReplicatedRoute
	}
	target.Route = currentRoute
	return lease.generation, target, nil
}

const directMutationOutcomeRecoveryWindow = 10 * time.Second
const directMutationOutcomeRecoveryAttempts = 64

func (executor *DurableSQLRequestExecutor) retryDirectOutcome(
	ctx context.Context,
	request ReplicatedDirectMutation,
	catalogGeneration uint64,
) (uint64, ReplicatedDirectMutation, ReplicatedDirectMutationResult, error) {
	recoveryCtx, cancel := context.WithTimeout(ctx, directMutationOutcomeRecoveryWindow)
	defer cancel()
	var direct ReplicatedDirectMutationResult
	var err error
	for attempt := 0; attempt < directMutationOutcomeRecoveryAttempts; attempt++ {
		var target ReplicatedTransactionTarget
		catalogGeneration, target, err = executor.currentDirectRecoveryTarget(
			recoveryCtx, catalogGeneration, request.Target,
		)
		if err != nil {
			return catalogGeneration, request, direct, errors.Join(raftservice.ErrOutcomeUnknown, err)
		}
		request.Target = target
		direct, err = executor.data.DirectMutateRecovering(recoveryCtx, request)
		if err == nil || !retryableDirectOutcomeRecovery(err) {
			return catalogGeneration, request, direct, err
		}
		if waitErr := waitReplicatedFailoverRetry(recoveryCtx, attempt); waitErr != nil {
			return catalogGeneration, request, direct, errors.Join(err, waitErr)
		}
	}
	return catalogGeneration, request, direct, err
}

// retryableDirectOutcomeRecovery reports whether another recovery attempt may
// settle an outcome that is still unknown. Every attempt re-drives the same
// request identity, and the target's transaction control applies it at most
// once, so any unknown outcome (a fence change, a leader change, or an
// admitted proposal whose result was lost again) is retried within the
// recovery window. Cancellation ends recovery early, and so does an invalid
// route: the logical route changed group and recovery can never rebind it.
func retryableDirectOutcomeRecovery(err error) bool {
	return err != nil && errors.Is(err, raftservice.ErrOutcomeUnknown) &&
		!errors.Is(err, ErrReplicatedRoute) &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func diagnoseDirectRecoveryRoute(
	stage string,
	planGeneration, catalogGeneration uint64,
	old, current ReplicatedRoute,
	resolved bool,
	err error,
) {
	if os.Getenv(durableSQLAbortDiagnosticEnvironment) != "1" {
		return
	}
	table := os.Getenv(durableSQLAbortDiagnosticTableEnvironment)
	if table == "" || !strings.Contains(string(old.Distribution), table) {
		return
	}
	valid := resolved && validReplicatedRoute(current)
	logical := valid && directMutationRouteIdentityMatches(old, current)
	monotone := valid && directMutationFenceAdvanceAllowed(old, current)
	advanced := valid && old.Command != current.Command
	fmt.Fprintf(os.Stderr,
		"VIBEDB_RF3_DIRECT_RECOVERY_ROUTE stage=%s distribution=%s shard=%s plan_generation=%d catalog_generation=%d resolved=%t valid=%t logical_contract=%t monotone_fence=%t target_fence_advanced=%t old_group=%x old_allocation=%d old_fence=%+v current_group=%x current_allocation=%d current_fence=%+v refresh_error=%q\n",
		stage, old.Distribution, old.Shard, planGeneration, catalogGeneration,
		resolved, valid, logical, monotone, advanced, old.Group, old.AllocationGeneration, old.Command,
		current.Group, current.AllocationGeneration, current.Command, err)
}

// DirectMutationRecoverableRoute reports whether a retained direct recipe
// planned against old may be re-driven under its original request identity
// on current: the same logical shard, group and allocation with a fence that
// only advanced. This is exactly the rebinding ExecutePreparedDirect performs
// when called with priorUnknown.
func DirectMutationRecoverableRoute(old, current ReplicatedRoute) bool {
	return directMutationRouteAdvanceAllowed(old, current)
}

func directMutationRouteAdvanceAllowed(old, current ReplicatedRoute) bool {
	return validReplicatedRoute(old) && validReplicatedRoute(current) &&
		directMutationRouteIdentityMatches(old, current) && directMutationFenceAdvanceAllowed(old, current)
}

func directMutationRouteIdentityMatches(old, current ReplicatedRoute) bool {
	return old.Distribution == current.Distribution && old.Shard == current.Shard &&
		old.Group == current.Group &&
		old.AllocationGeneration == current.AllocationGeneration &&
		old.RangeIdentity == current.RangeIdentity && old.LineageDigest == current.LineageDigest &&
		old.ForwardingRuleDigest == current.ForwardingRuleDigest &&
		old.LogicalSchemaDigest == current.LogicalSchemaDigest &&
		old.Command.ActivePolicyGeneration == current.Command.ActivePolicyGeneration &&
		old.Command.ProtectionEpoch == current.Command.ProtectionEpoch &&
		old.Command.SchemaGeneration == current.Command.SchemaGeneration &&
		old.Command.RelationManifestDigest == current.Command.RelationManifestDigest
}

func directMutationFenceAdvanceAllowed(old, current ReplicatedRoute) bool {
	return current.Command.ReplicaSetVersion >= old.Command.ReplicaSetVersion &&
		current.Command.OwnershipEpoch >= old.Command.OwnershipEpoch &&
		current.Command.RoutingVersion >= old.Command.RoutingVersion &&
		current.Command.RouteGeneration >= old.Command.RouteGeneration
}
