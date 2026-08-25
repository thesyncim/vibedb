package gateway

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/replication"
)

var (
	// ErrReplicatedDataRead reports a malformed public native read. The request
	// is rejected before catalog resolution or network I/O.
	ErrReplicatedDataRead = errors.New("gateway: invalid replicated data read")
	// ErrReplicatedTableRoute reports that the pinned catalog generation cannot
	// map the exact table/key pair to a serving RF3 base relation. There is no
	// legacy SQL or single-copy fallback at this boundary.
	ErrReplicatedTableRoute = errors.New("gateway: replicated table route is unavailable")
	// ErrReplicatedReadPositionMismatch reports that an applied-index floor was
	// issued by a different exact route lineage. Applied indexes are local to a
	// Raft group incarnation and must never be compared across RouteIDs.
	ErrReplicatedReadPositionMismatch = errors.New("gateway: replicated read position does not match route")
)

// ReplicatedDataReadConsistency is an explicit public point-read contract.
// The zero value is invalid so a caller cannot accidentally select stale data.
type ReplicatedDataReadConsistency uint8

const (
	ReplicatedDataReadLinearizable ReplicatedDataReadConsistency = iota + 1
	ReplicatedDataReadAtLeastApplied
)

// ReplicatedReadPosition is a portable monotonic-read witness. Applied is only
// meaningful together with the exact RouteID returned by a previous read or
// write. A split, move, schema rollout, or group replacement changes RouteID
// and causes a definite position-mismatch refusal before shard I/O.
type ReplicatedReadPosition struct {
	RouteID replication.Digest
	Applied uint64
}

// ReplicatedTableReadRequest contains byte-native public routing inputs. Table
// and Key are borrowed for the duration of Read; no string or JSON conversion
// occurs on this path.
type ReplicatedTableReadRequest struct {
	Table       []byte
	Key         []byte
	Consistency ReplicatedDataReadConsistency
	Position    ReplicatedReadPosition
}

// ReplicatedTableReadResult returns the exact route lineage and applied index
// that fenced Value. Value is the bounded native response payload.
type ReplicatedTableReadResult struct {
	Position ReplicatedReadPosition
	Found    bool
	Value    []byte
	Retries  int
}

// ReplicatedDataReader binds public table/key reads to one atomically pinned
// catalog generation and the SQL-free RF3 executor.
type ReplicatedDataReader struct {
	catalog  *CatalogHolder
	executor *ReplicatedExecutor
}

func NewReplicatedDataReader(
	catalog *CatalogHolder,
	executor *ReplicatedExecutor,
) (*ReplicatedDataReader, error) {
	if catalog == nil || catalog.Current() == nil || executor == nil || executor.client == nil {
		return nil, ErrReplicatedDataRead
	}
	return &ReplicatedDataReader{catalog: catalog, executor: executor}, nil
}

// Read executes one RF3 base-relation point read. Linearizable uses the
// leader's ReadIndex contract and accepts no caller position. AtLeastApplied
// prefers a follower but first proves that the supplied RouteID belongs to the
// exact currently pinned route and then enforces its applied-index floor.
func (reader *ReplicatedDataReader) Read(
	ctx context.Context,
	request ReplicatedTableReadRequest,
) (ReplicatedTableReadResult, error) {
	if reader == nil || reader.catalog == nil || reader.executor == nil || ctx == nil ||
		len(request.Table) == 0 || len(request.Table) > replication.MaxIdentityBytes ||
		len(request.Key) == 0 || len(request.Key) > replication.MaxMutationKeyBytes {
		return ReplicatedTableReadResult{}, ErrReplicatedDataRead
	}
	minimumApplied := uint64(1)
	linearizable := false
	switch request.Consistency {
	case ReplicatedDataReadLinearizable:
		if request.Position != (ReplicatedReadPosition{}) {
			return ReplicatedTableReadResult{}, ErrReplicatedDataRead
		}
		linearizable = true
	case ReplicatedDataReadAtLeastApplied:
		if request.Position.RouteID == (replication.Digest{}) || request.Position.Applied == 0 {
			return ReplicatedTableReadResult{}, ErrReplicatedDataRead
		}
		minimumApplied = request.Position.Applied
	default:
		return ReplicatedTableReadResult{}, ErrReplicatedDataRead
	}

	lease := reader.catalog.pinCurrent()
	if lease.snapshot == nil {
		return ReplicatedTableReadResult{}, ErrNoCatalog
	}
	defer lease.release()

	var replicas [ServingReplicaCount]ReplicatedEndpoint
	var scalarScratch [replication.MaxMutationKeyBytes + 16]byte
	resolved, ok := lease.snapshot.ResolveReplicatedTableKey(
		request.Table, request.Key, scalarScratch[:0], replicas[:0],
	)
	if !ok {
		return ReplicatedTableReadResult{}, ErrReplicatedTableRoute
	}
	if !linearizable && request.Position.RouteID != resolved.RouteID {
		return ReplicatedTableReadResult{}, ErrReplicatedReadPositionMismatch
	}

	result, err := reader.executor.ReadPoint(ctx, resolved.Route, ReplicatedPointRead{
		Relation: resolved.Profile.Relation, Key: request.Key,
		MinimumApplied: minimumApplied, MaxValueBytes: resolved.Profile.MaxDocumentBytes,
		Linearizable: linearizable,
	})
	if err != nil {
		return ReplicatedTableReadResult{}, err
	}
	return ReplicatedTableReadResult{
		Position: ReplicatedReadPosition{RouteID: resolved.RouteID, Applied: result.Applied},
		Found:    result.Found, Value: result.Value, Retries: result.Retries,
	}, nil
}
