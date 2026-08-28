package gateway

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson/x/byteview"
)

var (
	// ErrReplicatedSQLReadUnsupported fences SQL whose exact result cannot be
	// represented by one positional native document lookup. In particular,
	// joins, projections, ranges, aggregates, ordering, and limits continue
	// through the ordinary SQL executor; this API never silently weakens them.
	ErrReplicatedSQLReadUnsupported = errors.New(
		"gateway: SQL read shape is unavailable for replicated point batches",
	)
	// ErrReplicatedSQLReadMixed refuses a batch that crosses the RF3 and legacy
	// authorities. A request cannot partially run through one of each.
	ErrReplicatedSQLReadMixed = errors.New(
		"gateway: SQL read batch mixes replicated and non-replicated tables",
	)
)

// ReplicatedSQLBatchReadRequest is an ordered set of exact-primary-key SELECT
// statements. There is no participant-count ceiling: the packed request,
// result, and uint32 ordinal representation are the admission bounds.
type ReplicatedSQLBatchReadRequest struct {
	Queries        []Query
	MaxResultBytes uint32
}

// ReadSQLBatch lowers supported SELECTs directly to ReadScatterBatch's RF3
// path. Each successful position is the raw whole document selected by the
// corresponding query. The returned observation vector contains one honest
// ReadIndex cut per exact group; it is deliberately not a global MVCC snapshot.
// Any lower, route, intent, admission, or shard failure returns a zero result.
func (reader *ReplicatedDataReader) ReadSQLBatch(
	ctx context.Context,
	request ReplicatedSQLBatchReadRequest,
) (ReplicatedTableScatterReadResult, error) {
	if err := validateReplicatedSQLBatchReadRequest(reader, ctx, request); err != nil {
		return ReplicatedTableScatterReadResult{}, err
	}
	result, generation, err := reader.readSQLBatchPinned(ctx, request)
	if err == nil || !errors.Is(err, raftservice.ErrServingFence) {
		return result, err
	}
	if refreshErr := reader.refreshAfterFence(ctx, generation); refreshErr != nil {
		return ReplicatedTableScatterReadResult{}, errors.Join(err, refreshErr)
	}
	result, _, err = reader.readSQLBatchPinned(ctx, request)
	return result, err
}

func validateReplicatedSQLBatchReadRequest(
	reader *ReplicatedDataReader,
	ctx context.Context,
	request ReplicatedSQLBatchReadRequest,
) error {
	if reader == nil || reader.catalog == nil || reader.executor == nil || ctx == nil ||
		len(request.Queries) == 0 || request.MaxResultBytes == 0 ||
		request.MaxResultBytes > replicatedstate.MaxPointReadBatchBytes {
		return ErrReplicatedDataRead
	}
	count := uint64(len(request.Queries))
	if count > uint64(^uint32(0)) ||
		4+(count+7)/8+count*4 > uint64(request.MaxResultBytes) {
		return ErrReplicatedReadAdmission
	}
	// Bound parser/binder input before touching a catalog or network. Parameter
	// payload is borrowed and SQL is already caller-owned, but both influence
	// cold lowering work and therefore count against the physical batch bound.
	inputBytes := uint64(8)
	for queryIndex := range request.Queries {
		query := &request.Queries[queryIndex]
		if len(query.SQL) == 0 {
			return ErrReplicatedSQLReadUnsupported
		}
		queryBytes := uint64(8) + uint64(len(query.SQL))
		for paramIndex := range query.Params {
			if !query.Params[paramIndex].Valid() {
				return ErrPlanParameters
			}
			queryBytes += 8 + uint64(len(query.Params[paramIndex].Bytes))
		}
		if inputBytes > replicatedstate.MaxPointReadBatchBytes ||
			queryBytes > replicatedstate.MaxPointReadBatchBytes-inputBytes {
			return ErrReplicatedReadAdmission
		}
		inputBytes += queryBytes
	}
	return nil
}

func (reader *ReplicatedDataReader) readSQLBatchPinned(
	ctx context.Context,
	request ReplicatedSQLBatchReadRequest,
) (ReplicatedTableScatterReadResult, uint64, error) {
	lease := reader.catalog.pinCurrent()
	if lease.snapshot == nil {
		return ReplicatedTableScatterReadResult{}, 0, ErrNoCatalog
	}
	defer lease.release()

	points, err := lowerReplicatedSQLBatchRead(ctx, lease.snapshot, request.Queries)
	if err != nil {
		return ReplicatedTableScatterReadResult{}, lease.generation, err
	}
	return reader.readScatterBatchSnapshot(ctx, ReplicatedTableBatchReadRequest{
		Points: points, MaxResultBytes: request.MaxResultBytes,
	}, lease.snapshot, lease.generation)
}

func lowerReplicatedSQLBatchRead(
	ctx context.Context,
	snapshot *Snapshot,
	queries []Query,
) ([]ReplicatedTableBatchPoint, error) {
	if snapshot == nil || len(queries) == 0 {
		return nil, ErrReplicatedSQLReadUnsupported
	}
	points := make([]ReplicatedTableBatchPoint, len(queries))
	keyArena := make([]byte, 0, min(len(queries), 256)*32)
	seenReplicated, seenLegacy := false, false
	for queryIndex := range queries {
		query := &queries[queryIndex]
		args, err := queryRuntimeArgs(query.Params)
		if err != nil {
			return nil, err
		}
		prepared, err := snapshot.Prepare(ctx, query.SQL)
		if err != nil {
			return nil, err
		}
		if prepared.statement.Kind != sqlast.KindSelect || prepared.statement.Select == nil {
			return nil, ErrReplicatedSQLReadUnsupported
		}
		bound, err := prepared.Bind(args)
		if err != nil {
			return nil, err
		}
		entry, replicated := snapshot.replicatedTableAtBytes(byteview.Bytes(bound.table))
		if !replicated {
			seenLegacy = true
			if seenReplicated {
				return nil, ErrReplicatedSQLReadMixed
			}
			continue
		}
		seenReplicated = true
		if seenLegacy {
			return nil, ErrReplicatedSQLReadMixed
		}
		profile, ok := snapshot.replicatedTableProfileAt(entry)
		if !ok || !replicatedSQLWholeDocumentPointSelect(
			prepared.statement.Select, profile.PrimaryKey,
		) {
			return nil, ErrReplicatedSQLReadUnsupported
		}
		scalar, ok := replicatedSQLExactConstraint(bound.constraints)
		if !ok {
			return nil, ErrReplicatedSQLReadUnsupported
		}
		var encoded [replication.MaxMutationKeyBytes]byte
		key, ok := appendReplicatedSQLScalarKey(encoded[:0], scalar)
		if !ok || len(key) == 0 || len(key) > int(profile.MaxKeyBytes) {
			return nil, ErrReplicatedSQLReadUnsupported
		}
		start := len(keyArena)
		keyArena = append(keyArena, key...)
		points[queryIndex] = ReplicatedTableBatchPoint{
			Table: byteview.Bytes(bound.table),
			Key:   keyArena[start:len(keyArena):len(keyArena)],
		}
	}
	if !seenReplicated {
		return nil, ErrReplicatedSQLReadUnsupported
	}
	return points, nil
}

func replicatedSQLWholeDocumentPointSelect(
	statement *sqlast.SelectStmt,
	primary string,
) bool {
	if statement == nil || statement.Distinct || len(statement.Windows) != 0 ||
		statement.Correlation != nil || len(statement.Columns) != 1 ||
		!replicatedSQLExactPrimaryFilter(statement, primary) {
		return false
	}
	column := statement.Columns[0]
	return column.Agg == sqlast.AggNone && column.Path != nil &&
		column.Window == nil && column.Scalar == nil && column.Alias == "" &&
		column.Path.Source == 0 && column.Path.MergedUsing == 0 &&
		len(column.Path.Segments) == 0
}
