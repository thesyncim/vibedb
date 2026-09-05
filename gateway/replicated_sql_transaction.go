package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
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
	errPreparedDirectFallback = errors.New(
		"gateway: prepared direct lowering requires a linearizable preimage",
	)
)

type replicatedSQLPreimageMode uint8

const (
	replicatedSQLLinearizablePreimage replicatedSQLPreimageMode = iota
	replicatedSQLCommittedLeaderPreimage
)

type replicatedSQLBoundStatement struct {
	prepared             *PreparedPlan
	bound                *BoundWritePlan
	profile              ReplicatedTableProfile
	assignmentExpression *query.DMLStatement
	// updateExec is allocated only for computed SET expressions. A plain
	// full-document UPDATE never touches it, so keeping the ~6KB query.Exec
	// inline would tax every single-statement write lowering.
	updateExec             *query.Exec
	conflictArgs           []any
	conflictParameterTypes []query.ParameterType
}

type replicatedSQLMutationIdentity struct {
	target   int
	relation replication.RelationID
	key      []byte
}

// preparedDirectUpdateCandidate is the private precheck for the committed
// preimage lane. It is intentionally stricter than the ordinary RF3 lowerer:
// one exact primary-key row, declared column assignments, no maintained
// global indexes, and no assignment that can move the primary key.
func preparedDirectUpdateCandidate(
	queries []Query,
	statement *replicatedSQLBoundStatement,
	profile ReplicatedTableProfile,
) bool {
	if len(queries) != 1 || statement == nil || statement.prepared == nil ||
		statement.bound == nil || statement.profile.Relation == 0 ||
		statement.profile.Relation != profile.Relation {
		return false
	}
	prepared, bound := statement.prepared, statement.bound
	update := prepared.statement.Update
	if prepared.statement.Kind != sqlast.KindUpdate || bound.kind != sqlast.KindUpdate ||
		update == nil || len(update.Assignments) == 0 || len(bound.updateAssignments) == 0 ||
		len(prepared.writeGlobalIndexes) != 0 ||
		!replicatedSQLExactPrimaryFilter(update.Filter, profile.PrimaryKey) ||
		replicatedSQLUpdateAssignsPrimary(update, profile.PrimaryKey) {
		return false
	}
	_, ok := replicatedSQLExactConstraint(bound.constraints)
	return ok
}

func replicatedSQLUpdateAssignsPrimary(update *sqlast.UpdateStmt, primary string) bool {
	if update == nil || len(update.Assignments) == 0 || len(primary) < 2 || primary[0] != '/' {
		return false
	}
	encoded := primary[1:]
	if strings.IndexByte(encoded, '/') >= 0 {
		return false
	}
	column := encoded
	if strings.IndexByte(encoded, '~') >= 0 {
		column = strings.ReplaceAll(strings.ReplaceAll(encoded, "~1", "/"), "~0", "~")
	}
	for _, assignment := range update.Assignments {
		if assignment.Column == column {
			return true
		}
	}
	return false
}

// replicatedSQLTransactionRequestDigest binds a request ID to the exact caller
// input before any catalog pin or SQL lowering. It is intentionally independent
// of routes and catalog generations so an exact retry survives topology change.
// byteview.Bytes borrows SQL storage; hashing never materializes a string copy.
func replicatedSQLTransactionRequestDigest(queries []Query) replication.Digest {
	hasher := sha256.New()
	typed := false
	for queryIndex := range queries {
		typed = typed || len(queries[queryIndex].ParamTypes) != 0
	}
	if typed {
		_, _ = hasher.Write([]byte("vibedb/rf3-sql-typed-caller-request\x00"))
	} else {
		_, _ = hasher.Write([]byte("vibedb/rf3-sql-caller-request\x00"))
	}
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
		if typed {
			binary.LittleEndian.PutUint64(fixed[:8], uint64(len(query.ParamTypes)))
			_, _ = hasher.Write(fixed[:8])
			for _, parameterType := range query.ParamTypes {
				fixed[0] = byte(parameterType)
				_, _ = hasher.Write(fixed[:1])
			}
		}
	}
	var digest replication.Digest
	hasher.Sum(digest[:0])
	return digest
}

// planReplicatedSQLTransaction is the allocation-focused lowering seam for
// mutations that require no indexed pre-read. Production durable execution
// always supplies its explicit ReplicatedExecutor through the WithData form.
func (executor *Executor) planReplicatedSQLTransaction(
	ctx context.Context,
	snapshot *Snapshot,
	queries []Query,
	profile Profile,
) ([]ReplicatedTransactionTarget, bool, error) {
	return executor.planReplicatedSQLTransactionWithData(ctx, snapshot, queries, profile, nil)
}

func (executor *Executor) planReplicatedSQLTransactionWithData(
	ctx context.Context,
	snapshot *Snapshot,
	queries []Query,
	profile Profile,
	data *ReplicatedExecutor,
) ([]ReplicatedTransactionTarget, bool, error) {
	return executor.planReplicatedSQLTransactionWithDataMode(
		ctx, snapshot, queries, profile, data,
		replicatedSQLLinearizablePreimage,
	)
}

func (executor *Executor) planReplicatedSQLTransactionWithDataMode(
	ctx context.Context,
	snapshot *Snapshot,
	queries []Query,
	profile Profile,
	data *ReplicatedExecutor,
	preimageMode replicatedSQLPreimageMode,
) (targets []ReplicatedTransactionTarget, handled bool, err error) {
	if executor == nil || snapshot == nil ||
		len(queries) == 0 {
		return nil, false, nil
	}
	if preimageMode == replicatedSQLCommittedLeaderPreimage && len(queries) != 1 {
		return nil, true, errPreparedDirectFallback
	}
	statements := make([]replicatedSQLBoundStatement, len(queries))
	var expressionCancel query.CancelFlag
	committedPreimageRead := false
	if preimageMode == replicatedSQLCommittedLeaderPreimage {
		defer func() {
			// Only errors derived from a successfully read committed row need
			// a fresh linearizable evaluation. Binding/transport/fence errors
			// retain their original class without repeating unrelated work.
			if !committedPreimageRead || err == nil || errors.Is(err, errPreparedDirectFallback) {
				return
			}
			if ctx != nil && context.Cause(ctx) != nil {
				err = context.Cause(ctx)
				return
			}
			err = errors.Join(errPreparedDirectFallback, err)
		}()
	}
	var stopExpressionCancel func() bool
	defer func() {
		if stopExpressionCancel != nil {
			stopExpressionCancel()
		}
		for index := range statements {
			if statements[index].assignmentExpression != nil {
				statements[index].assignmentExpression.Release()
			}
		}
	}()
	replicatedCount := 0
	var encodedFlatBytes uint64
	for index := range queries {
		if err := validateTypedQuery(ctx, &queries[index]); err != nil {
			return nil, false, err
		}
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
		entry, replicated := snapshot.replicatedTableAtBytes(
			byteview.Bytes(prepared.table),
		)
		if replicated {
			if insert := prepared.statement.Insert; prepared.statement.Kind == sqlast.KindInsert && insert != nil &&
				insert.OnConflictUpdate != nil && (!replicatedConflictActionSupported(insert.OnConflictUpdate) || len(prepared.writeGlobalIndexes) != 0) {
				unsupported := sqlast.NewFeatureNotSupportedError(
					queries[index].SQL,
					replicatedSQLConflictActionPosition(insert),
					"RF3 ON CONFLICT requires branch-aware replicated writes",
				)
				return nil, true, errors.Join(
					ErrReplicatedSQLTransactionUnsupported, unsupported,
				)
			}
		}
		bound, err := prepared.BindWrite(args)
		if err != nil {
			return nil, false, err
		}
		statements[index].prepared = prepared
		statements[index].bound = bound
		if prepared.statement.Kind == sqlast.KindInsert && sqldriver.ReplicatedConflictProgram(prepared.statement.Insert.OnConflictUpdate) {
			statements[index].conflictArgs = args
		}
		if !replicated {
			preimageMode = replicatedSQLLinearizablePreimage
			continue
		}
		tableProfile, ok := snapshot.replicatedTableProfileAt(entry)
		if !ok {
			return nil, true, ErrReplicatedSQLTransactionUnsupported
		}
		statements[index].profile = tableProfile
		if preimageMode == replicatedSQLCommittedLeaderPreimage &&
			!preparedDirectUpdateCandidate(
				queries, &statements[index], tableProfile,
			) {
			// The statement is already parsed and bound. Preserve ordinary
			// lowering without binding INSERT, DELETE or indexed UPDATE twice.
			preimageMode = replicatedSQLLinearizablePreimage
		}
		if hasComputedUpdateAssignments(&prepared.statement) || hasConflictExpressions(&prepared.statement) {
			parameterTypes, typeErr := postgresQueryParameterTypes(
				queries[index].ParamTypes, prepared.params,
			)
			if typeErr != nil {
				return nil, true, typeErr
			}
			expression, expressionErr := query.PrepareParsedDMLWithParameterTypes(
				queries[index].SQL, &prepared.statement, parameterTypes,
			)
			if expressionErr != nil {
				return nil, true, expressionErr
			}
			statements[index].assignmentExpression = expression
			if expression.HasConflictUpdateExpressions() {
				statements[index].conflictParameterTypes = parameterTypes
				expressionErr = expression.ValidateConflictUpdateExpressionBindings(args)
			} else if expression.HasUpdateExpressions() {
				expressionErr = expression.ValidateUpdateExpressionBindings(args)
			} else {
				return nil, true, ErrReplicatedSQLTransactionUnsupported
			}
			if expressionErr != nil {
				return nil, true, expressionErr
			}
			if ctx != nil && ctx.Done() != nil && stopExpressionCancel == nil {
				stopExpressionCancel = context.AfterFunc(ctx, expressionCancel.Cancel)
			}
			// The conflict lane (EncodeReplicatedConflictValue) never takes an
			// evaluator; only computed UPDATE assignments materialize through
			// updateExec, so conflict-only statements skip it entirely.
			if expression.HasUpdateExpressions() {
				updateExec := &query.Exec{}
				updateExec.Options.Cancel = &expressionCancel
				statements[index].updateExec = updateExec
			}
		}
		if prepared.statement.Kind == sqlast.KindInsert && len(prepared.statement.Insert.Columns) != 0 {
			insert := prepared.statement.Insert
			if uint64(len(insert.Rows)) > profile.MaxTransactionMutations {
				return nil, true, ErrTransactionMutationLimit
			}
			encoder, encodeErr := sqldriver.PrepareFlatInsertEncoder(insert)
			if encodeErr != nil {
				return nil, true, encodeErr
			}
			if len(insert.Rows) > 1 {
				bound.insertDocs = make([][]byte, len(insert.Rows))
			}
			for row := range insert.Rows {
				document, encodeErr := encoder.Encode(&insert.Rows[row], args, int(min(uint64(tableProfile.MaxDocumentBytes), profile.MaxTransactionBytes-encodedFlatBytes)))
				if encodeErr != nil {
					return nil, true, encodeErr
				}
				encodedFlatBytes += uint64(len(document))
				if len(insert.Rows) == 1 {
					bound.insertDoc = document
				} else {
					bound.insertDocs[row] = document
				}
			}
		}
		replicatedCount++
	}
	if replicatedCount == 0 {
		return nil, false, nil
	}
	if replicatedCount != len(statements) {
		return nil, true, ErrReplicatedSQLTransactionMixed
	}
	// Size the hot vectors from the exact, already-bounded base mutation count.
	// This avoids logarithmic slice growth for one wide VALUES/IN statement
	// without eagerly reserving its potential global-index side effects.
	baseMutationCount := 0
	for statementIndex := range statements {
		count, countErr := replicatedSQLMutationInputCount(&statements[statementIndex])
		if countErr != nil {
			return nil, true, countErr
		}
		if uint64(count) > profile.MaxTransactionMutations-uint64(baseMutationCount) {
			return nil, true, ErrTransactionMutationLimit
		}
		baseMutationCount += count
	}

	// Targets are built directly in the result slice: the former builder
	// wrapper held exactly one ReplicatedTransactionTarget, so a second
	// slice plus a final copy taxed every transaction for no reason.
	targets = make([]ReplicatedTransactionTarget, 0, min(baseMutationCount, 8))
	identities := make([]replicatedSQLMutationIdentity, 0, min(baseMutationCount, 256))
	keyArena := make([]byte, 0, min(baseMutationCount, 256)*32)
	var byGroup map[raftmember.GroupKey]int
	var documentWorkspace GlobalIndexWorkspace
	var mutationBytes uint64
	var mutationCount uint64
	// One shared resolve scratch pair serves every mutation: resolved routes
	// are consumed synchronously (preimage reads) or duplicated on new groups
	// (appendReplicatedSQLMutation), so the shared backing never escapes the
	// call. Per-table prep and the last route are cached across statements;
	// the route digest is skipped everywhere here — grouping compares full
	// routes and participant digests are computed at proposal time.
	var txnScalar [replication.MaxMutationKeyBytes + 16]byte
	var txnReplicas [ServingReplicaCount]ReplicatedEndpoint
	var txnPrep replicatedTableResolvePrep
	txnHavePrep := false
	var txnReuse ReplicatedRoute
	txnHaveReuse := false

	for statementIndex := range statements {
		statement := &statements[statementIndex]
		inputCount, err := replicatedSQLMutationInputCount(statement)
		if err != nil {
			return nil, true, err
		}
		// The placement mapper depends only on the statement spec; building
		// it once per statement preserves the exact first-mutation panic
		// timing of the per-mutation build it replaces.
		var statementMapper *distribution.NativeMapper
		for inputOrdinal := 0; inputOrdinal < inputCount; inputOrdinal++ {
			scalar, document, kind, inputErr := replicatedSQLMutationInput(statement, inputOrdinal)
			if inputErr != nil {
				return nil, true, inputErr
			}

			var encodedKey [replication.MaxMutationKeyBytes]byte
			key, ok := appendReplicatedSQLScalarKey(encodedKey[:0], scalar)
			if !ok || len(key) == 0 || len(key) > int(statement.profile.MaxKeyBytes) {
				return nil, true, ErrReplicatedSQLTransactionUnsupported
			}
			if kind == replication.MutationPutPresent && len(document) != 0 {
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
			if statementMapper == nil {
				statementMapper = distribution.NewNativeMapperWithBucketBits(
					statement.bound.spec.Arity, statement.bound.spec.EffectiveBucketBits(),
				)
			}
			var tuple [1]distribution.Scalar
			tuple[0] = scalar
			point, pointErr := statementMapper.PointFor(tuple[:])
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
			table := byteview.Bytes(statement.bound.table)
			if !txnHavePrep || !bytes.Equal(txnPrep.table, table) {
				fresh, prepOK := snapshot.replicatedTableResolvePrepFor(table)
				if !prepOK {
					return nil, true, ErrReplicatedSQLTransactionUnsupported
				}
				txnPrep, txnHavePrep, txnHaveReuse = fresh, true, false
			}
			var txnReuseRoute ReplicatedRoute
			if txnHaveReuse {
				txnReuseRoute = txnReuse
			}
			resolved, resolvedOK := snapshot.resolveReplicatedTableKeyPrepared(
				&txnPrep, ownedKey, txnScalar[:0], txnReplicas[:0],
				false, txnReuseRoute, txnHaveReuse,
			)
			if !resolvedOK || resolved.Profile.Relation != statement.profile.Relation {
				return nil, true, ErrReplicatedSQLTransactionUnsupported
			}
			// Same reuse discipline as the read paths: only value fields are
			// read back, so the shared backing never escapes the call.
			txnReuse, txnHaveReuse = resolved.Route, true
			var oldDocument []byte
			missingPartial := false
			if kind == replication.MutationPutPresent && len(statement.bound.updateAssignments) != 0 {
				var found bool
				if preimageMode == replicatedSQLCommittedLeaderPreimage {
					oldDocument, found, err = readReplicatedSQLDocumentCommitted(
						data, ctx, resolved.Route, statement.profile, ownedKey,
					)
					committedPreimageRead = err == nil
				} else {
					oldDocument, found, err = readReplicatedSQLDocument(
						data, ctx, resolved.Route, statement.profile, ownedKey,
					)
				}
				if err != nil {
					return nil, true, err
				}
				if !found {
					if preimageMode == replicatedSQLCommittedLeaderPreimage {
						return nil, true, errPreparedDirectFallback
					}
					// Retain a real participant so the durable request ledger can
					// prove this zero-row result. Replicated apply checks PutPresent
					// presence before schema validation and never publishes this value.
					document = []byte("{}")
					missingPartial = true
				} else if statement.assignmentExpression != nil {
					document, err = sqldriver.MaterializePreparedUpdateAssignments(
						statement.assignmentExpression, statement.updateExec,
						oldDocument, statement.bound.updateArgs,
						int(statement.profile.MaxDocumentBytes),
					)
					if err == nil {
						document, err = vibejson.AppendCanonicalize(nil, document)
					}
				} else {
					document, err = sqldriver.ApplyColumnAssignments(
						oldDocument, statement.bound.updateAssignments,
						statement.bound.updateArgs, int(statement.profile.MaxDocumentBytes),
					)
				}
				if err != nil {
					return nil, true, err
				}
				if len(document) > int(statement.profile.MaxDocumentBytes) {
					return nil, true, ErrTransactionByteLimit
				}
				statement.bound.updateDoc = document
			}
			if kind == replication.MutationPutPresent && !missingPartial {
				replacement, replacementErr := replicatedSQLDocumentPrimaryScalar(
					document, statement.bound.keyPointers, &documentWorkspace,
				)
				if replacementErr != nil {
					return nil, true, replacementErr
				}
				var replacementKey [replication.MaxMutationKeyBytes]byte
				encodedReplacement, replacementOK := appendReplicatedSQLScalarKey(replacementKey[:0], replacement)
				if !replacementOK || !bytes.Equal(key, encodedReplacement) {
					return nil, true, ErrWriteShardKeyMove
				}
			}

			indexStart := len(statement.bound.globalIndexes)
			if kind == replication.MutationPutConflict {
				document, err = sqldriver.EncodeReplicatedConflictValue(document, statement.prepared.statement.Insert.OnConflictUpdate, statement.conflictArgs, statement.conflictParameterTypes)
				if err != nil {
					return nil, true, err
				}
			}
			baseMutation := replication.Mutation{Kind: kind, Key: ownedKey, Value: document}
			if !missingPartial && len(statement.prepared.writeGlobalIndexes) != 0 &&
				(statement.bound.kind == sqlast.KindUpdate || statement.bound.kind == sqlast.KindDelete) {
				if oldDocument == nil {
					var readErr error
					oldDocument, readErr = readReplicatedSQLIndexedDocument(
						data, ctx, resolved.Route, statement.profile, ownedKey,
					)
					if readErr != nil {
						return nil, true, readErr
					}
				}
				postimage := shardservice.Cell{Null: true}
				if statement.bound.kind == sqlast.KindUpdate {
					postimage = shardservice.Cell{Bytes: document}
				}
				captureRows := [][]shardservice.Cell{{
					{Bytes: ownedKey}, {Bytes: oldDocument}, postimage,
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
			if kind == replication.MutationPutPresent && len(statement.bound.updateAssignments) != 0 && !missingPartial {
				baseMutation.ExpectedValueLength = uint64(len(oldDocument))
				baseMutation.ExpectedValueDigest = replication.Digest(sha256.Sum256(oldDocument))
				baseMutation.Kind = replication.MutationPutDigestEqual
			}
			targetIndex, appendErr := appendReplicatedSQLMutation(
				&targets, &byGroup, resolved.Route, bits,
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
				target: targetIndex, relation: statement.profile.Relation, key: ownedKey,
			})

			indexEnd := len(statement.bound.globalIndexes)
			if statement.bound.kind == sqlast.KindInsert {
				indexStart, indexEnd = replicatedSQLGlobalIndexRange(statement, inputOrdinal)
			}
			for indexOrdinal := indexStart; indexOrdinal < indexEnd; indexOrdinal++ {
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
					indexOrdinal+1 < indexEnd {
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
				indexTarget, indexErr := appendReplicatedSQLMutation(
					&targets, &byGroup, indexRoute, index.bucketBits, index.scope,
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
					target: indexTarget, relation: indexProfile.Relation, key: entryKey,
				})
			}
		}
	}

	slices.SortFunc(identities, func(left, right replicatedSQLMutationIdentity) int {
		if left.target != right.target {
			return left.target - right.target
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
		if prior.target == current.target && prior.relation == current.relation &&
			bytes.Equal(prior.key, current.key) {
			return nil, true, ErrReplicatedSQLTransactionDuplicate
		}
	}

	for index := range targets {
		slices.SortFunc(targets[index].Batches, func(
			left, right replication.RelationMutationBatch,
		) int {
			return int(left.Relation) - int(right.Relation)
		})
		targets[index].IntentScopes = coalesceIntentScopes(targets[index].IntentScopes)
		if len(targets[index].IntentScopes) == 0 ||
			len(targets[index].IntentScopes) > distributedtxn.MaxIntentScopes {
			return nil, true, ErrReplicatedSQLTransactionUnsupported
		}
	}
	if committedPreimageRead && !preparedDirectFullRowGuard(targets) {
		return nil, true, errPreparedDirectFallback
	}
	return targets, true, nil
}

func replicatedSQLConflictActionPosition(insert *sqlast.InsertStmt) int {
	if insert != nil && insert.OnConflictUpdate != nil {
		return insert.OnConflictUpdate.Pos
	}
	if insert != nil {
		return insert.OnConflictPos
	}
	return 0
}

func replicatedSQLMutationInputCount(
	statement *replicatedSQLBoundStatement,
) (int, error) {
	if statement == nil || statement.prepared == nil || statement.bound == nil ||
		statement.profile.Relation == 0 {
		return 0, ErrReplicatedSQLTransactionUnsupported
	}
	prepared, bound := statement.prepared, statement.bound
	switch prepared.statement.Kind {
	case sqlast.KindInsert:
		insert := prepared.statement.Insert
		if insert == nil || insert.Source != nil || insert.Returning != nil ||
			insert.OnConflictUpdate != nil && (!replicatedConflictActionSupported(insert.OnConflictUpdate) || len(prepared.writeGlobalIndexes) != 0) || len(insert.Rows) == 0 ||
			len(bound.rowKeys) != len(insert.Rows) ||
			len(bound.globalIndexes) != len(insert.Rows)*len(prepared.writeGlobalIndexes) {
			return 0, ErrReplicatedSQLTransactionUnsupported
		}
		if len(insert.Rows) == 1 {
			if len(bound.rowKeys[0]) != 1 || len(bound.insertDoc) == 0 {
				return 0, ErrReplicatedSQLTransactionUnsupported
			}
		} else {
			if len(bound.insertDocs) != len(insert.Rows) {
				return 0, ErrReplicatedSQLTransactionUnsupported
			}
			for ordinal := range bound.rowKeys {
				if len(bound.rowKeys[ordinal]) != 1 || len(bound.insertDocs[ordinal]) == 0 {
					return 0, ErrReplicatedSQLTransactionUnsupported
				}
			}
		}
		return len(insert.Rows), nil
	case sqlast.KindUpdate:
		update := prepared.statement.Update
		if update == nil || update.Returning != nil || len(update.OrderBy) != 0 ||
			update.Limit != nil || !replicatedSQLExactPrimaryFilter(
			update.Filter, statement.profile.PrimaryKey,
		) || len(bound.updateDoc) == 0 && len(bound.updateAssignments) == 0 {
			return 0, ErrReplicatedSQLTransactionUnsupported
		}
		if _, ok := replicatedSQLExactConstraint(bound.constraints); !ok {
			return 0, ErrReplicatedSQLTransactionUnsupported
		}
		return 1, nil
	case sqlast.KindDelete:
		deleteStatement := prepared.statement.Delete
		if deleteStatement == nil || deleteStatement.Returning != nil ||
			len(deleteStatement.OrderBy) != 0 || deleteStatement.Limit != nil ||
			deleteStatement.All || !replicatedSQLFinitePrimaryFilter(
			deleteStatement.Filter, statement.profile.PrimaryKey,
		) {
			return 0, ErrReplicatedSQLTransactionUnsupported
		}
		if len(bound.constraints) != 1 ||
			bound.constraints[0].Kind != distribution.DomainFinite ||
			len(bound.constraints[0].Values) == 0 {
			return 0, ErrReplicatedSQLTransactionUnsupported
		}
		return len(bound.constraints[0].Values), nil
	default:
		return 0, ErrReplicatedSQLTransactionUnsupported
	}
}

func replicatedSQLMutationInput(
	statement *replicatedSQLBoundStatement,
	ordinal int,
) (distribution.Scalar, []byte, replication.MutationKind, error) {
	if ordinal < 0 {
		return distribution.Scalar{}, nil, 0, ErrReplicatedSQLTransactionUnsupported
	}
	prepared, bound := statement.prepared, statement.bound
	switch prepared.statement.Kind {
	case sqlast.KindInsert:
		if ordinal >= len(bound.rowKeys) || len(bound.rowKeys[ordinal]) != 1 {
			return distribution.Scalar{}, nil, 0, ErrReplicatedSQLTransactionUnsupported
		}
		document := bound.insertDoc
		if len(bound.rowKeys) > 1 {
			if ordinal >= len(bound.insertDocs) {
				return distribution.Scalar{}, nil, 0, ErrReplicatedSQLTransactionUnsupported
			}
			document = bound.insertDocs[ordinal]
		}
		kind := replication.MutationPutAbsent
		if prepared.statement.Insert.OnConflictDoNothing {
			kind = replication.MutationPutIfAbsent
		} else if action := prepared.statement.Insert.OnConflictUpdate; action.WholeDocument() && action.Where == nil {
			// Both branches publish exactly the canonical candidate. The native
			// put validates its schema and physical key at the replicated apply
			// point and retains one affected row for inserts and replacements.
			kind = replication.MutationPut
		} else if sqldriver.ReplicatedConflictProgram(prepared.statement.Insert.OnConflictUpdate) {
			kind = replication.MutationPutConflict
		}
		return bound.rowKeys[ordinal][0], document, kind, nil
	case sqlast.KindUpdate:
		scalar, ok := replicatedSQLExactConstraint(bound.constraints)
		if !ok || ordinal != 0 {
			return distribution.Scalar{}, nil, 0, ErrReplicatedSQLTransactionUnsupported
		}
		return scalar, bound.updateDoc, replication.MutationPutPresent, nil
	case sqlast.KindDelete:
		if len(bound.constraints) != 1 ||
			bound.constraints[0].Kind != distribution.DomainFinite ||
			ordinal >= len(bound.constraints[0].Values) {
			return distribution.Scalar{}, nil, 0, ErrReplicatedSQLTransactionUnsupported
		}
		return bound.constraints[0].Values[ordinal], nil, replication.MutationDelete, nil
	default:
		return distribution.Scalar{}, nil, 0, ErrReplicatedSQLTransactionUnsupported
	}
}

// replicatedSQLGlobalIndexRange selects only the index mutations derived from
// one VALUES row. bindGlobalIndexInserts records a dense row-major vector, so
// this is constant-time and does not allocate. UPDATE/DELETE capture appends a
// single statement-wide vector and therefore keeps the full range.
func replicatedSQLGlobalIndexRange(
	statement *replicatedSQLBoundStatement,
	ordinal int,
) (int, int) {
	if statement == nil || statement.bound == nil || statement.prepared == nil ||
		statement.bound.kind != sqlast.KindInsert {
		if statement == nil || statement.bound == nil {
			return 0, 0
		}
		return 0, len(statement.bound.globalIndexes)
	}
	perRow := len(statement.prepared.writeGlobalIndexes)
	start := ordinal * perRow
	return start, start + perRow
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

func readReplicatedSQLIndexedDocument(
	data *ReplicatedExecutor,
	ctx context.Context,
	route ReplicatedRoute,
	profile ReplicatedTableProfile,
	key []byte,
) ([]byte, error) {
	value, found, err := readReplicatedSQLDocument(data, ctx, route, profile, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrReplicatedTransactionConflict
	}
	return value, nil
}

func readReplicatedSQLDocument(
	data *ReplicatedExecutor,
	ctx context.Context,
	route ReplicatedRoute,
	profile ReplicatedTableProfile,
	key []byte,
) ([]byte, bool, error) {
	if data == nil || profile.Relation == 0 ||
		profile.MaxDocumentBytes == 0 {
		return nil, false, ErrReplicatedSQLTransactionUnsupported
	}
	result, err := data.ReadPoint(
		ctx, route, ReplicatedPointRead{
			Relation: profile.Relation, Key: key, MinimumApplied: 1,
			MaxValueBytes: profile.MaxDocumentBytes, Linearizable: true,
		},
	)
	if err != nil {
		return nil, false, err
	}
	if !result.Found {
		return nil, false, nil
	}
	if len(result.Value) == 0 {
		return nil, false, ErrReplicatedRoute
	}
	return result.Value, true, nil
}

func readReplicatedSQLDocumentCommitted(
	data *ReplicatedExecutor,
	ctx context.Context,
	route ReplicatedRoute,
	profile ReplicatedTableProfile,
	key []byte,
) ([]byte, bool, error) {
	if data == nil || profile.Relation == 0 ||
		profile.MaxDocumentBytes == 0 {
		return nil, false, ErrReplicatedSQLTransactionUnsupported
	}
	result, err := data.readCommittedPoint(
		ctx, route, ReplicatedPointRead{
			Relation: profile.Relation, Key: key, MinimumApplied: 1,
			MaxValueBytes: profile.MaxDocumentBytes,
		}, serviceauthz.CapabilityDataRead,
	)
	if err != nil {
		return nil, false, err
	}
	if !result.Found {
		return nil, false, nil
	}
	if len(result.Value) == 0 {
		return nil, false, ErrReplicatedRoute
	}
	return result.Value, true, nil
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
	manifest, ok := snapshot.Manifest(index.distribution)
	if !ok || manifest.Version() != index.routingVersion {
		return ReplicatedRoute{}, ReplicatedTableProfile{}, ErrReplicatedSQLWriteUnavailable
	}
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	route, ok := snapshot.ResolveReplicatedRoute(
		index.distribution, index.target.Shard, replicas[:0],
	)
	if !ok || route.AllocationGeneration != uint64(index.target.AllocationGeneration) ||
		route.Command.OwnershipEpoch != uint64(index.target.OwnershipEpoch) ||
		route.Command.RoutingVersion > uint64(index.routingVersion) ||
		route.Command.SchemaGeneration != profile.SchemaGeneration ||
		route.LogicalSchemaDigest != profile.LogicalSchemaDigest {
		return ReplicatedRoute{}, ReplicatedTableProfile{}, ErrReplicatedSQLWriteUnavailable
	}
	return route, profile, nil
}

func appendReplicatedSQLMutation(
	targets *[]ReplicatedTransactionTarget,
	byGroup *map[raftmember.GroupKey]int,
	route ReplicatedRoute,
	bucketBits uint8,
	scope distributedtxn.IntentScope,
	relation replication.RelationID,
	mutation replication.Mutation,
) (int, error) {
	if targets == nil || byGroup == nil || relation == 0 || bucketBits == 0 ||
		!distributedtxn.ValidateIntentScope(scope, bucketBits) {
		return -1, ErrReplicatedSQLTransactionUnsupported
	}
	targetIndex := replicatedSQLTargetIndex(*targets, *byGroup, route.Group)
	if targetIndex < 0 {
		replicas := make([]ReplicatedEndpoint, len(route.Replicas))
		copy(replicas, route.Replicas)
		route.Replicas = replicas
		*targets = append(*targets, ReplicatedTransactionTarget{
			Route: route, BucketBits: bucketBits,
		})
		targetIndex = len(*targets) - 1
		if *byGroup != nil {
			(*byGroup)[route.Group] = targetIndex
		} else if len(*targets) == 16 {
			index := make(map[raftmember.GroupKey]int, len(*targets))
			for targetOrdinal := range *targets {
				index[(*targets)[targetOrdinal].Route.Group] = targetOrdinal
			}
			*byGroup = index
		}
	} else if !sameReplicatedSQLRoute(
		(*targets)[targetIndex].Route, route,
	) || (*targets)[targetIndex].BucketBits != bucketBits {
		return -1, ErrReplicatedSQLTransactionUnsupported
	}

	builder := &(*targets)[targetIndex]
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
	return targetIndex, nil
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
	if path, value, ok := sqlast.NullSafeEqualityPathOperand(where); ok {
		where = &sqlast.Expr{Kind: sqlast.ExprCompare, Op: sqlast.OpEq, Path: path, Value: value, Column: -1}
	}
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

// replicatedSQLFinitePrimaryFilter accepts exactly one positive primary-key
// equality or membership predicate and no residual SQL operator. The bound
// constraint must still prove a non-empty finite domain before lowering. This
// makes DELETE ... IN (...) a native set of exact conditional mutations rather
// than a read-before-write scatter.
func replicatedSQLFinitePrimaryFilter(filter *sqlast.SelectStmt, primary string) bool {
	if filter == nil || filter.Where == nil || filter.With != nil || filter.Set != nil ||
		len(filter.From) != 1 || filter.Having != nil || len(filter.GroupBy) != 0 ||
		len(filter.OrderBy) != 0 || filter.Limit != nil || filter.Offset != nil {
		return false
	}
	where := filter.Where
	if path, value, ok := sqlast.NullSafeEqualityPathOperand(where); ok {
		where = &sqlast.Expr{Kind: sqlast.ExprCompare, Op: sqlast.OpEq, Path: path, Value: value, Column: -1}
	}
	if where.Negated || where.Agg != sqlast.AggNone || where.Column != -1 ||
		where.Path == nil || where.Path.Source != 0 || where.Path.MergedUsing != 0 ||
		where.RightPath != nil || where.Subquery != nil || len(where.Kids) != 0 {
		return false
	}
	switch where.Kind {
	case sqlast.ExprCompare:
		if where.Op != sqlast.OpEq {
			return false
		}
	case sqlast.ExprIn:
		if len(where.List) == 0 {
			return false
		}
	default:
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

func replicatedSQLTargetIndex(
	targets []ReplicatedTransactionTarget,
	byGroup map[raftmember.GroupKey]int,
	group raftmember.GroupKey,
) int {
	if byGroup != nil {
		if index, ok := byGroup[group]; ok {
			return index
		}
		return -1
	}
	for index := range targets {
		if targets[index].Route.Group == group {
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
		left.Command == right.Command && left.RangeIdentity == right.RangeIdentity &&
		left.LineageDigest == right.LineageDigest &&
		left.ForwardingRuleDigest == right.ForwardingRuleDigest
}
