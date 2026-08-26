package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
	vibejson "github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

var (
	// ErrReplicatedSQLTransactionUnsupported fences every SQL shape whose exact
	// byte-native meaning is not represented by this narrow RF3 lowering.
	ErrReplicatedSQLTransactionUnsupported = errors.New(
		"gateway: SQL mutation shape is unavailable for replicated transactions",
	)
	// ErrReplicatedSQLTransactionMixed prevents one request from crossing the RF3
	// and legacy authorities. The request is refused before either transport runs.
	ErrReplicatedSQLTransactionMixed = errors.New(
		"gateway: SQL transaction mixes replicated and non-replicated tables",
	)
	// ErrReplicatedSQLTransactionDuplicate reports two statements that target the
	// same physical relation key. The replicated apply planner deliberately folds
	// duplicate keys, which is not SQL's statement-order affected-row contract.
	ErrReplicatedSQLTransactionDuplicate = errors.New(
		"gateway: replicated SQL transaction repeats a relation key",
	)
)

type replicatedSQLBoundStatement struct {
	prepared *PreparedPlan
	bound    *BoundWritePlan
	profile  ReplicatedTableProfile
}

type replicatedSQLParticipantBuilder struct {
	participant ReplicatedTransactionParticipant
}

type replicatedSQLMutationIdentity struct {
	participant int
	relation    replication.RelationID
	key         []byte
}

// executeReplicatedSQLTransaction recognizes an all-RF3 exact-key batch and
// sends its native relation mutations to the fused orchestrator. handled is
// false only when every table belongs to the legacy/static lane. Classification
// and complete shape validation finish before any RF3 or shard SQL I/O.
func (executor *Executor) executeReplicatedSQLTransaction(
	ctx context.Context,
	snapshot *Snapshot,
	requestID replication.ID128,
	requestDigest replication.Digest,
	queries []Query,
	profile Profile,
) (*Result, bool, error) {
	participants, handled, err := executor.planReplicatedSQLTransaction(
		ctx, snapshot, queries, profile,
	)
	if err != nil || !handled {
		return nil, handled, err
	}
	if len(participants) == 0 {
		return nil, true, ErrReplicatedSQLTransactionUnsupported
	}
	var outcome ReplicatedTransactionRequestOutcome
	if executor.replicatedTransactionRequests == nil {
		transaction, executeErr := executor.replicatedTransactions.Execute(
			ctx, snapshot.Generation(), participants,
		)
		outcome = ReplicatedTransactionRequestOutcome{
			ReplicatedTransactionResult: transaction,
			CatalogGeneration:           snapshot.Generation(),
			ShardsFanned:                len(participants),
		}
		err = executeErr
	} else {
		if requestID == (replication.ID128{}) || requestDigest == (replication.Digest{}) {
			return nil, true, ErrReplicatedTransactionRequestRegistry
		}
		outcome, err = executor.replicatedTransactionRequests.Execute(
			ctx, requestID, requestDigest, snapshot.Generation(), participants,
		)
	}
	if err != nil {
		return nil, true, err
	}
	if !outcome.Committed || outcome.Recovery != nil {
		return nil, true, ErrReplicatedTransaction
	}
	// The state machine reports physical relation mutations. SQL reports logical
	// base rows, so hidden exact/global-index maintenance must not inflate the
	// completion observed by the caller.
	outcome.AffectedRows = int64(len(queries))
	return executor.replicatedSQLTransactionResult(outcome), true, nil
}

// replicatedSQLTransactionRequestDigest binds a request ID to the exact caller
// input before any catalog pin or SQL lowering. It is intentionally independent
// of routes and catalog generations so an exact retry survives topology change.
// byteview.Bytes borrows SQL storage; hashing never materializes a string copy.
func replicatedSQLTransactionRequestDigest(queries []Query) replication.Digest {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("vibedb/rf3-sql-caller-request\x00"))
	var fixed [16]byte
	binary.LittleEndian.PutUint64(fixed[:8], uint64(len(queries)))
	_, _ = hasher.Write(fixed[:8])
	for queryIndex := range queries {
		query := &queries[queryIndex]
		fixed[0] = byte(query.Class)
		clear(fixed[1:])
		binary.LittleEndian.PutUint64(fixed[8:], uint64(len(query.SQL)))
		_, _ = hasher.Write(fixed[:])
		_, _ = hasher.Write(byteview.Bytes(query.SQL))
		binary.LittleEndian.PutUint64(fixed[:8], uint64(len(query.Params)))
		_, _ = hasher.Write(fixed[:8])
		for paramIndex := range query.Params {
			param := &query.Params[paramIndex]
			fixed[0] = byte(param.Kind)
			fixed[1] = 0
			if param.Bool {
				fixed[1] = 1
			}
			clear(fixed[2:8])
			binary.LittleEndian.PutUint64(fixed[8:], uint64(len(param.Bytes)))
			_, _ = hasher.Write(fixed[:])
			_, _ = hasher.Write(param.Bytes)
		}
	}
	var digest replication.Digest
	hasher.Sum(digest[:0])
	return digest
}

func (executor *Executor) replicatedSQLTransactionResult(
	outcome ReplicatedTransactionRequestOutcome,
) *Result {
	if outcome.CatalogGeneration == 0 || outcome.ShardsFanned <= 0 {
		return nil
	}
	executor.metrics.observeRoute(
		distribution.RouteTargeted, outcome.ShardsFanned, ScatterNone,
	)
	return &Result{
		Kind: shardservice.ResponseCompletion, RowsAffected: outcome.AffectedRows,
		RouteKind: distribution.RouteTargeted, Generation: outcome.CatalogGeneration,
		ShardsFanned: outcome.ShardsFanned, TransactionID: replication.ID128(outcome.ID),
	}
}

func (executor *Executor) planReplicatedSQLTransaction(
	ctx context.Context,
	snapshot *Snapshot,
	queries []Query,
	profile Profile,
) ([]ReplicatedTransactionParticipant, bool, error) {
	if executor == nil || executor.replicatedTransactions == nil || snapshot == nil ||
		len(queries) == 0 {
		return nil, false, nil
	}
	statements := make([]replicatedSQLBoundStatement, len(queries))
	replicatedCount := 0
	for index := range queries {
		args, err := queryRuntimeArgs(queries[index].Params)
		if err != nil {
			return nil, false, err
		}
		prepared, err := snapshot.Prepare(ctx, queries[index].SQL)
		if err != nil {
			return nil, false, err
		}
		if prepared.statement.Kind == sqlast.KindSelect {
			return nil, false, ErrExecRequiresMutation
		}
		bound, err := prepared.BindWrite(args)
		if err != nil {
			return nil, false, err
		}
		statements[index] = replicatedSQLBoundStatement{prepared: prepared, bound: bound}
		entry, replicated := snapshot.replicatedTableAtBytes(byteview.Bytes(bound.table))
		if !replicated {
			continue
		}
		tableProfile, ok := snapshot.replicatedTableProfileAt(entry)
		if !ok {
			return nil, true, ErrReplicatedSQLTransactionUnsupported
		}
		statements[index].profile = tableProfile
		replicatedCount++
	}
	if replicatedCount == 0 {
		return nil, false, nil
	}
	if replicatedCount != len(statements) {
		return nil, true, ErrReplicatedSQLTransactionMixed
	}

	builders := make([]replicatedSQLParticipantBuilder, 0, min(len(statements), 8))
	identities := make([]replicatedSQLMutationIdentity, 0, len(statements))
	keyArena := make([]byte, 0, min(len(statements), 256)*32)
	var byGroup map[raftmember.GroupKey]int
	var documentWorkspace GlobalIndexWorkspace
	var mutationBytes uint64
	var mutationCount uint64

	for statementIndex := range statements {
		statement := &statements[statementIndex]
		scalar, document, kind, err := replicatedSQLMutationInput(statement)
		if err != nil {
			return nil, true, err
		}

		var encodedKey [replication.MaxMutationKeyBytes]byte
		key, ok := appendReplicatedSQLScalarKey(encodedKey[:0], scalar)
		if !ok || len(key) == 0 || len(key) > int(statement.profile.MaxKeyBytes) {
			return nil, true, ErrReplicatedSQLTransactionUnsupported
		}
		if kind == replication.MutationPutPresent {
			replacement, replacementErr := replicatedSQLDocumentPrimaryScalar(
				document, statement.bound.keyPointers, &documentWorkspace,
			)
			if replacementErr != nil {
				return nil, true, replacementErr
			}
			var replacementKey [replication.MaxMutationKeyBytes]byte
			encodedReplacement, replacementOK := appendReplicatedSQLScalarKey(
				replacementKey[:0], replacement,
			)
			if !replacementOK || !bytes.Equal(key, encodedReplacement) {
				return nil, true, ErrWriteShardKeyMove
			}
		}
		if len(document) > int(statement.profile.MaxDocumentBytes) {
			return nil, true, ErrTransactionByteLimit
		}
		mapper := distribution.NewNativeMapperWithBucketBits(
			statement.bound.spec.Arity, statement.bound.spec.EffectiveBucketBits(),
		)
		var tuple [1]distribution.Scalar
		tuple[0] = scalar
		point, pointErr := mapper.PointFor(tuple[:])
		if pointErr != nil {
			return nil, true, ErrReplicatedSQLTransactionUnsupported
		}
		target, targetOK := statement.bound.manifest.ResolvePointTarget(point)
		if !targetOK {
			return nil, true, ErrReplicatedSQLTransactionUnsupported
		}
		bits := statement.bound.spec.EffectiveBucketBits()
		bucket, bucketOK := distribution.VirtualBucketForPoint(point, bits)
		owner, ownerOK := statement.bound.manifest.ResolveVirtualBucket(bucket, bits)
		if !bucketOK || !ownerOK || owner.Shard != target.Shard ||
			owner.AllocationGeneration != target.AllocationGeneration {
			return nil, true, ErrReplicatedSQLTransactionUnsupported
		}

		keyStart := len(keyArena)
		keyArena = append(keyArena, key...)
		ownedKey := keyArena[keyStart:len(keyArena):len(keyArena)]
		var scalarScratch [replication.MaxMutationKeyBytes + 16]byte
		var replicaScratch [ServingReplicaCount]ReplicatedEndpoint
		resolved, resolvedOK := snapshot.ResolveReplicatedTableKey(
			byteview.Bytes(statement.bound.table), ownedKey,
			scalarScratch[:0], replicaScratch[:0],
		)
		if !resolvedOK || resolved.Profile.Relation != statement.profile.Relation {
			return nil, true, ErrReplicatedSQLTransactionUnsupported
		}

		baseMutation := replication.Mutation{Kind: kind, Key: ownedKey, Value: document}
		if len(statement.prepared.writeGlobalIndexes) != 0 &&
			(statement.bound.kind == sqlast.KindUpdate || statement.bound.kind == sqlast.KindDelete) {
			oldDocument, readErr := executor.readReplicatedSQLIndexedDocument(
				ctx, resolved.Route, statement.profile, ownedKey,
			)
			if readErr != nil {
				return nil, true, readErr
			}
			captureRows := [][]shardservice.Cell{{
				{Bytes: ownedKey}, {Bytes: oldDocument},
			}}
			if captureErr := statement.prepared.bindGlobalIndexCapture(
				statement.bound, target, captureRows,
			); captureErr != nil {
				return nil, true, captureErr
			}
			baseMutation.ExpectedValueLength = uint64(len(oldDocument))
			baseMutation.ExpectedValueDigest = replication.Digest(sha256.Sum256(oldDocument))
			if baseMutation.ExpectedValueDigest == (replication.Digest{}) {
				return nil, true, ErrReplicatedSQLTransactionUnsupported
			}
			if kind == replication.MutationDelete {
				baseMutation.Kind = replication.MutationDeleteDigestEqual
			} else {
				baseMutation.Kind = replication.MutationPutDigestEqual
			}
		}
		participantIndex, appendErr := appendReplicatedSQLMutation(
			&builders, &byGroup, resolved.Route, bits,
			distributedtxn.IntentScope{Start: uint32(bucket), End: uint32(bucket) + 1},
			statement.profile.Relation, baseMutation,
		)
		if appendErr != nil {
			return nil, true, appendErr
		}
		if err := admitReplicatedSQLMutation(
			profile, &mutationCount, &mutationBytes, baseMutation,
		); err != nil {
			return nil, true, err
		}
		identities = append(identities, replicatedSQLMutationIdentity{
			participant: participantIndex, relation: statement.profile.Relation, key: ownedKey,
		})

		for indexOrdinal := 0; indexOrdinal < len(statement.bound.globalIndexes); indexOrdinal++ {
			index := &statement.bound.globalIndexes[indexOrdinal]
			indexRoute, indexProfile, routeErr := snapshot.resolveReplicatedSQLGlobalIndex(index)
			if routeErr != nil {
				return nil, true, routeErr
			}
			entryKey := statement.bound.globalIndexArena[index.entryStart:index.entryEnd]
			locator := statement.bound.globalIndexArena[index.valueStart:index.valueEnd]
			indexMutation := replication.Mutation{
				Kind: replication.MutationPutAbsentOrEqual, Key: entryKey, Value: locator,
			}
			if index.kind == shardservice.MutationGlobalIndexDelete &&
				indexOrdinal+1 < len(statement.bound.globalIndexes) {
				next := &statement.bound.globalIndexes[indexOrdinal+1]
				nextKey := statement.bound.globalIndexArena[next.entryStart:next.entryEnd]
				if sameReplicatedSQLGlobalIndexTarget(index, next) &&
					next.kind == shardservice.MutationGlobalIndexPut &&
					bytes.Equal(entryKey, nextKey) {
					nextLocator := statement.bound.globalIndexArena[next.valueStart:next.valueEnd]
					if len(nextLocator) == 0 || len(nextLocator) > int(indexProfile.MaxDocumentBytes) {
						return nil, true, ErrTransactionByteLimit
					}
					indexMutation.Kind = replication.MutationPutDigestEqual
					indexMutation.Value = nextLocator
					indexMutation.ExpectedValueLength = uint64(len(locator))
					indexMutation.ExpectedValueDigest = replication.Digest(sha256.Sum256(locator))
					indexOrdinal++
				}
			}
			if len(entryKey) == 0 || len(entryKey) > int(indexProfile.MaxKeyBytes) ||
				len(locator) == 0 || len(locator) > int(indexProfile.MaxDocumentBytes) {
				return nil, true, ErrTransactionByteLimit
			}
			if index.kind == shardservice.MutationGlobalIndexDelete {
				if indexMutation.Kind != replication.MutationPutDigestEqual {
					indexMutation.Kind = replication.MutationDeleteDigestEqual
					indexMutation.Value = nil
					indexMutation.ExpectedValueLength = uint64(len(locator))
					indexMutation.ExpectedValueDigest = replication.Digest(sha256.Sum256(locator))
				}
			} else if index.kind != shardservice.MutationGlobalIndexPut {
				return nil, true, ErrReplicatedSQLTransactionUnsupported
			}
			if (indexMutation.Kind == replication.MutationDeleteDigestEqual ||
				indexMutation.Kind == replication.MutationPutDigestEqual) &&
				indexMutation.ExpectedValueDigest == (replication.Digest{}) {
				return nil, true, ErrReplicatedSQLTransactionUnsupported
			}
			indexParticipant, indexErr := appendReplicatedSQLMutation(
				&builders, &byGroup, indexRoute, index.bucketBits, index.scope,
				indexProfile.Relation, indexMutation,
			)
			if indexErr != nil {
				return nil, true, indexErr
			}
			if err := admitReplicatedSQLMutation(
				profile, &mutationCount, &mutationBytes, indexMutation,
			); err != nil {
				return nil, true, err
			}
			identities = append(identities, replicatedSQLMutationIdentity{
				participant: indexParticipant, relation: indexProfile.Relation, key: entryKey,
			})
		}
	}

	slices.SortFunc(identities, func(left, right replicatedSQLMutationIdentity) int {
		if left.participant != right.participant {
			return left.participant - right.participant
		}
		if left.relation < right.relation {
			return -1
		}
		if left.relation > right.relation {
			return 1
		}
		return bytes.Compare(left.key, right.key)
	})
	for index := 1; index < len(identities); index++ {
		prior, current := identities[index-1], identities[index]
		if prior.participant == current.participant && prior.relation == current.relation &&
			bytes.Equal(prior.key, current.key) {
			return nil, true, ErrReplicatedSQLTransactionDuplicate
		}
	}

	participants := make([]ReplicatedTransactionParticipant, len(builders))
	for index := range builders {
		participant := builders[index].participant
		slices.SortFunc(participant.Batches, func(
			left, right replication.RelationMutationBatch,
		) int {
			return int(left.Relation) - int(right.Relation)
		})
		participant.IntentScopes = coalesceIntentScopes(participant.IntentScopes)
		if len(participant.IntentScopes) == 0 ||
			len(participant.IntentScopes) > distributedtxn.MaxIntentScopes {
			return nil, true, ErrReplicatedSQLTransactionUnsupported
		}
		participants[index] = participant
	}
	return participants, true, nil
}

func replicatedSQLMutationInput(
	statement *replicatedSQLBoundStatement,
) (distribution.Scalar, []byte, replication.MutationKind, error) {
	if statement == nil || statement.prepared == nil || statement.bound == nil ||
		statement.profile.Relation == 0 {
		return distribution.Scalar{}, nil, 0, ErrReplicatedSQLTransactionUnsupported
	}
	prepared, bound := statement.prepared, statement.bound
	switch prepared.statement.Kind {
	case sqlast.KindInsert:
		insert := prepared.statement.Insert
		if insert == nil || insert.Source != nil || insert.Returning != nil ||
			insert.OnConflictDoNothing || len(insert.Columns) != 0 || len(insert.Rows) != 1 ||
			len(bound.rowKeys) != 1 || len(bound.rowKeys[0]) != 1 ||
			len(bound.insertDoc) == 0 {
			return distribution.Scalar{}, nil, 0, ErrReplicatedSQLTransactionUnsupported
		}
		return bound.rowKeys[0][0], bound.insertDoc, replication.MutationPutAbsent, nil
	case sqlast.KindUpdate:
		update := prepared.statement.Update
		if update == nil || update.Returning != nil || len(update.OrderBy) != 0 ||
			update.Limit != nil || !replicatedSQLExactPrimaryFilter(
			update.Filter, statement.profile.PrimaryKey,
		) || len(bound.updateDoc) == 0 {
			return distribution.Scalar{}, nil, 0, ErrReplicatedSQLTransactionUnsupported
		}
		scalar, ok := replicatedSQLExactConstraint(bound.constraints)
		if !ok {
			return distribution.Scalar{}, nil, 0, ErrReplicatedSQLTransactionUnsupported
		}
		return scalar, bound.updateDoc, replication.MutationPutPresent, nil
	case sqlast.KindDelete:
		deleteStatement := prepared.statement.Delete
		if deleteStatement == nil || deleteStatement.Returning != nil ||
			len(deleteStatement.OrderBy) != 0 || deleteStatement.Limit != nil ||
			deleteStatement.All || !replicatedSQLExactPrimaryFilter(
			deleteStatement.Filter, statement.profile.PrimaryKey,
		) {
			return distribution.Scalar{}, nil, 0, ErrReplicatedSQLTransactionUnsupported
		}
		scalar, ok := replicatedSQLExactConstraint(bound.constraints)
		if !ok {
			return distribution.Scalar{}, nil, 0, ErrReplicatedSQLTransactionUnsupported
		}
		return scalar, nil, replication.MutationDelete, nil
	default:
		return distribution.Scalar{}, nil, 0, ErrReplicatedSQLTransactionUnsupported
	}
}

func sameReplicatedSQLGlobalIndexTarget(
	left, right *boundGlobalIndexMutation,
) bool {
	return left != nil && right != nil &&
		left.metadata.IndexID == right.metadata.IndexID &&
		left.metadata.Incarnation == right.metadata.Incarnation &&
		left.metadata.Relation == right.metadata.Relation &&
		left.distribution == right.distribution &&
		sameWriteTarget(left.target, right.target) &&
		left.routingVersion == right.routingVersion && left.bucketBits == right.bucketBits &&
		left.scope == right.scope
}

func (executor *Executor) readReplicatedSQLIndexedDocument(
	ctx context.Context,
	route ReplicatedRoute,
	profile ReplicatedTableProfile,
	key []byte,
) ([]byte, error) {
	if executor == nil || executor.replicatedTransactions == nil ||
		executor.replicatedTransactions.executor == nil || profile.Relation == 0 ||
		profile.MaxDocumentBytes == 0 {
		return nil, ErrReplicatedSQLTransactionUnsupported
	}
	result, err := executor.replicatedTransactions.executor.ReadPoint(
		ctx, route, ReplicatedPointRead{
			Relation: profile.Relation, Key: key, MinimumApplied: 1,
			MaxValueBytes: profile.MaxDocumentBytes, Linearizable: true,
		},
	)
	if err != nil {
		return nil, err
	}
	if !result.Found || len(result.Value) == 0 {
		return nil, ErrReplicatedTransactionConflict
	}
	return result.Value, nil
}

func (snapshot *Snapshot) resolveReplicatedSQLGlobalIndex(
	index *boundGlobalIndexMutation,
) (ReplicatedRoute, ReplicatedTableProfile, error) {
	if snapshot == nil || index == nil || index.metadata.Relation == "" ||
		index.target.Shard == "" || index.bucketBits == 0 {
		return ReplicatedRoute{}, ReplicatedTableProfile{}, ErrReplicatedSQLTransactionUnsupported
	}
	entry, ok := snapshot.replicatedTableAtBytes(byteview.Bytes(index.metadata.Relation))
	if !ok {
		return ReplicatedRoute{}, ReplicatedTableProfile{}, ErrReplicatedSQLWriteUnavailable
	}
	profile, ok := snapshot.replicatedTableProfileAt(entry)
	if !ok || profile.Relation == 0 {
		return ReplicatedRoute{}, ReplicatedTableProfile{}, ErrReplicatedSQLWriteUnavailable
	}
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	route, ok := snapshot.ResolveReplicatedRoute(
		index.distribution, index.target.Shard, replicas[:0],
	)
	if !ok || route.AllocationGeneration != uint64(index.target.AllocationGeneration) ||
		route.Command.OwnershipEpoch != uint64(index.target.OwnershipEpoch) ||
		route.Command.RoutingVersion != uint64(index.routingVersion) ||
		route.Command.SchemaGeneration != profile.SchemaGeneration ||
		replication.Digest(route.Command.RelationManifestDigest) != profile.RelationManifestDigest {
		return ReplicatedRoute{}, ReplicatedTableProfile{}, ErrReplicatedSQLWriteUnavailable
	}
	return route, profile, nil
}

func appendReplicatedSQLMutation(
	builders *[]replicatedSQLParticipantBuilder,
	byGroup *map[raftmember.GroupKey]int,
	route ReplicatedRoute,
	bucketBits uint8,
	scope distributedtxn.IntentScope,
	relation replication.RelationID,
	mutation replication.Mutation,
) (int, error) {
	if builders == nil || byGroup == nil || relation == 0 || bucketBits == 0 ||
		!distributedtxn.ValidateIntentScopes([]distributedtxn.IntentScope{scope}, bucketBits) {
		return -1, ErrReplicatedSQLTransactionUnsupported
	}
	participantIndex := replicatedSQLParticipantIndex(*builders, *byGroup, route.Group)
	if participantIndex < 0 {
		replicas := make([]ReplicatedEndpoint, len(route.Replicas))
		copy(replicas, route.Replicas)
		route.Replicas = replicas
		*builders = append(*builders, replicatedSQLParticipantBuilder{
			participant: ReplicatedTransactionParticipant{
				Route: route, BucketBits: bucketBits,
			},
		})
		participantIndex = len(*builders) - 1
		if *byGroup != nil {
			(*byGroup)[route.Group] = participantIndex
		} else if len(*builders) == 16 {
			index := make(map[raftmember.GroupKey]int, len(*builders))
			for builderIndex := range *builders {
				index[(*builders)[builderIndex].participant.Route.Group] = builderIndex
			}
			*byGroup = index
		}
	} else if !sameReplicatedSQLRoute(
		(*builders)[participantIndex].participant.Route, route,
	) || (*builders)[participantIndex].participant.BucketBits != bucketBits {
		return -1, ErrReplicatedSQLTransactionUnsupported
	}

	builder := &(*builders)[participantIndex].participant
	batchIndex := replicatedSQLRelationBatch(builder.Batches, relation)
	if batchIndex < 0 {
		if len(builder.Batches) == replication.MaxRelationBatches {
			return -1, ErrTransactionMutationLimit
		}
		builder.Batches = append(builder.Batches, replication.RelationMutationBatch{
			Relation: relation,
		})
		batchIndex = len(builder.Batches) - 1
	}
	batch := &builder.Batches[batchIndex]
	if len(batch.Mutations) >= replicatedstate.MaxDistinctMutations {
		return -1, ErrTransactionMutationLimit
	}
	batch.Mutations = append(batch.Mutations, mutation)
	builder.IntentScopes = append(builder.IntentScopes, scope)
	return participantIndex, nil
}

func admitReplicatedSQLMutation(
	profile Profile,
	count *uint64,
	bytesUsed *uint64,
	mutation replication.Mutation,
) error {
	if count == nil || bytesUsed == nil || *count >= profile.MaxTransactionMutations {
		return ErrTransactionMutationLimit
	}
	itemBytes := uint64(len(mutation.Key)) + uint64(len(mutation.Value))
	if mutation.Kind == replication.MutationDeleteDigestEqual ||
		mutation.Kind == replication.MutationPutDigestEqual {
		itemBytes += replication.MutationDigestCompareBytes
	}
	if *bytesUsed > profile.MaxTransactionBytes ||
		itemBytes > profile.MaxTransactionBytes-*bytesUsed {
		return ErrTransactionByteLimit
	}
	(*count)++
	*bytesUsed += itemBytes
	return nil
}

func replicatedSQLExactPrimaryFilter(filter *sqlast.SelectStmt, primary string) bool {
	if filter == nil || filter.Where == nil || filter.With != nil || filter.Set != nil ||
		len(filter.From) != 1 ||
		filter.Having != nil || len(filter.GroupBy) != 0 || len(filter.OrderBy) != 0 ||
		filter.Limit != nil || filter.Offset != nil {
		return false
	}
	where := filter.Where
	if where.Kind != sqlast.ExprCompare || where.Op != sqlast.OpEq || where.Negated ||
		where.Agg != sqlast.AggNone || where.Column != -1 || where.Path == nil ||
		where.Path.Source != 0 || where.Path.MergedUsing != 0 || where.RightPath != nil ||
		where.Subquery != nil || len(where.Kids) != 0 {
		return false
	}
	var pointer [replication.MaxIdentityBytes]byte
	encoded := where.Path.AppendPointer(pointer[:0])
	return vibejson.BytesEqualString(encoded, primary)
}

func replicatedSQLExactConstraint(
	constraints distribution.BoundConstraints,
) (distribution.Scalar, bool) {
	if len(constraints) != 1 || constraints[0].Kind != distribution.DomainFinite ||
		len(constraints[0].Values) != 1 {
		return distribution.Scalar{}, false
	}
	return constraints[0].Values[0], true
}

func appendReplicatedSQLScalarKey(
	destination []byte,
	scalar distribution.Scalar,
) ([]byte, bool) {
	switch scalar.Kind() {
	case distribution.KindString:
		value, ok := scalar.StringValue()
		if !ok {
			return destination, false
		}
		return orderedkey.AppendString(destination, byteview.Bytes(value), orderedkey.Ascending)
	case distribution.KindNumber:
		value, ok := scalar.NumberSpelling()
		if !ok {
			return destination, false
		}
		return orderedkey.AppendNumber(destination, byteview.Bytes(value), orderedkey.Ascending)
	default:
		return destination, false
	}
}

func replicatedSQLDocumentPrimaryScalar(
	document []byte,
	pointers []vibejson.CompiledPointer,
	workspace *GlobalIndexWorkspace,
) (distribution.Scalar, error) {
	if len(document) == 0 || len(pointers) != 1 || workspace == nil {
		return distribution.Scalar{}, ErrReplicatedSQLTransactionUnsupported
	}
	needed, err := vibejson.RequiredIndexEntries(document)
	if err != nil {
		return distribution.Scalar{}, ErrReplicatedSQLTransactionUnsupported
	}
	if cap(workspace.entries) < needed {
		workspace.entries = make([]vibejson.IndexEntry, needed)
	} else {
		workspace.entries = workspace.entries[:needed]
	}
	index, err := vibejson.BuildIndex(document, workspace.entries)
	if err != nil {
		return distribution.Scalar{}, ErrReplicatedSQLTransactionUnsupported
	}
	node, found, err := index.Root().PointerCompiled(pointers[0])
	if err != nil || !found || node.Raw().IsNull() {
		return distribution.Scalar{}, ErrReplicatedSQLTransactionUnsupported
	}
	value := node.Raw()
	switch value.Kind() {
	case jsondoc.String:
		if text, ok := value.StringBytes(); ok {
			return distribution.NewString(byteview.String(text)), nil
		}
		workspace.decoded = workspace.decoded[:0]
		decoded, ok, decodeErr := value.AppendText(workspace.decoded)
		if decodeErr != nil || !ok {
			return distribution.Scalar{}, ErrReplicatedSQLTransactionUnsupported
		}
		workspace.decoded = decoded
		return distribution.NewString(byteview.String(decoded)), nil
	case jsondoc.Number:
		number, ok := value.NumberText()
		if !ok {
			return distribution.Scalar{}, ErrReplicatedSQLTransactionUnsupported
		}
		scalar, numberErr := distribution.NewNumber(number)
		if numberErr != nil {
			return distribution.Scalar{}, ErrReplicatedSQLTransactionUnsupported
		}
		return scalar, nil
	default:
		return distribution.Scalar{}, ErrReplicatedSQLTransactionUnsupported
	}
}

func replicatedSQLParticipantIndex(
	builders []replicatedSQLParticipantBuilder,
	byGroup map[raftmember.GroupKey]int,
	group raftmember.GroupKey,
) int {
	if byGroup != nil {
		if index, ok := byGroup[group]; ok {
			return index
		}
		return -1
	}
	for index := range builders {
		if builders[index].participant.Route.Group == group {
			return index
		}
	}
	return -1
}

func replicatedSQLRelationBatch(
	batches []replication.RelationMutationBatch,
	relation replication.RelationID,
) int {
	for index := range batches {
		if batches[index].Relation == relation {
			return index
		}
	}
	return -1
}

func sameReplicatedSQLRoute(left, right ReplicatedRoute) bool {
	return left.Distribution == right.Distribution && left.Shard == right.Shard &&
		left.Group == right.Group && left.AllocationGeneration == right.AllocationGeneration &&
		left.Command == right.Command
}
