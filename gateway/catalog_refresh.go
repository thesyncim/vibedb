package gateway

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibejson/x/byteview"
)

// catalogRefreshCoordinator serializes one authenticated catalog refresh per
// holder and lends its result to concurrent callers. The owner request's
// context is used only for the authority call; waiters retain their own
// cancellation and never inherit an owner's deadline.
type catalogRefreshCoordinator struct {
	mu     sync.Mutex
	active *catalogRefreshOperation
}

type catalogRefreshOperation struct {
	done          chan struct{}
	floor         uint64
	err           error
	ownerCanceled bool
}

// refreshAfter obtains a generation strictly newer than staleGeneration and
// publishes it through CatalogHolder's checked lineage path. A caller that
// already observes a newer generation does no authority work. The nil refresh
// source is a bounded refusal rather than a local catalog read loop.
func (holder *CatalogHolder) refreshAfter(
	ctx context.Context,
	staleGeneration uint64,
	refresh RefreshFunc,
) error {
	if holder == nil || ctx == nil || staleGeneration == 0 {
		return ErrStaleGeneration
	}
	for {
		if current := holder.Current(); current != nil &&
			current.Generation() > staleGeneration {
			return nil
		}
		holder.refresh.mu.Lock()
		if active := holder.refresh.active; active != nil {
			holder.refresh.mu.Unlock()
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-active.done:
			}
			if current := holder.Current(); current != nil &&
				current.Generation() > staleGeneration {
				return nil
			}
			if active.floor >= staleGeneration {
				if active.err != nil {
					// A canceled owner must not poison a live waiter. Let the
					// waiter become the next owner when no newer generation won.
					if active.ownerCanceled {
						continue
					}
					return active.err
				}
				return ErrStaleGeneration
			}
			continue
		}
		active := &catalogRefreshOperation{
			done: make(chan struct{}), floor: staleGeneration,
		}
		holder.refresh.active = active
		holder.refresh.mu.Unlock()

		var snapshot *Snapshot
		if refresh == nil {
			active.err = ErrStaleGeneration
		} else {
			snapshot, active.err = refresh(ctx, staleGeneration)
			if active.err == nil && (snapshot == nil ||
				snapshot.Generation() <= staleGeneration) {
				active.err = ErrStaleGeneration
			}
			if active.err == nil {
				current := holder.Current()
				if current == nil || current.Generation() < snapshot.Generation() {
					if publishErr := holder.publishNewerChecked(snapshot); publishErr != nil &&
						!errors.Is(publishErr, ErrCatalogGenerationNotNewer) {
						active.err = publishErr
					}
				}
			}
		}
		if current := holder.Current(); current != nil &&
			current.Generation() > staleGeneration {
			active.err = nil
		}
		active.ownerCanceled = context.Cause(ctx) != nil
		holder.refresh.mu.Lock()
		holder.refresh.active = nil
		close(active.done)
		holder.refresh.mu.Unlock()
		return active.err
	}
}

func (e *Executor) refreshAfterCatalogMiss(
	ctx context.Context,
	staleGeneration uint64,
) error {
	if e == nil || e.catalog == nil || ctx == nil || staleGeneration == 0 {
		return ErrStaleGeneration
	}
	return e.catalog.refreshAfter(ctx, staleGeneration, e.refresh)
}

// validateCatalogPrepare checks a statement against one immutable generation.
// A missing table is the sole preparation error eligible for one authenticated
// refresh and a complete re-prepare; all other planner errors return
// immediately. The short operation deadline also bounds PG protocol callers,
// whose backend prepare context is deliberately background-scoped.
func (e *Executor) validateCatalogPrepare(ctx context.Context, sqlText string) error {
	if e == nil || e.catalog == nil {
		return ErrNoCatalog
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Preserve the existing successful PG prepare path: Current is a lock-free
	// immutable snapshot load and Prepare performs no dispatch. Only a definite
	// missing-table error pays the bounded refresh setup below.
	snapshot := e.catalog.Current()
	if snapshot == nil {
		return ErrNoCatalog
	}
	_, err := snapshot.Prepare(ctx, sqlText)
	if err == nil || !errors.Is(err, ErrTableNotPlaced) {
		return err
	}
	staleGeneration := snapshot.Generation()
	profile := e.profileFor(ClassInteractive)
	opctx, cancel := context.WithTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	if refreshErr := e.refreshAfterCatalogMiss(opctx, staleGeneration); refreshErr != nil {
		return preserveCatalogMiss(err, refreshErr)
	}
	snapshot = e.catalog.Current()
	if snapshot == nil {
		return err
	}
	_, retryErr := snapshot.Prepare(opctx, sqlText)
	return retryErr
}

// preserveCatalogMiss returns the original planner/route diagnostic when a
// refresh made no progress. Real authentication, lineage, or authority errors
// are joined so callers can fail closed while retaining the useful table
// diagnostic.
func preserveCatalogMiss(original, refreshErr error) error {
	if refreshErr == nil {
		return nil
	}
	if refreshErr == ErrStaleGeneration {
		return original
	}
	return errors.Join(original, refreshErr)
}

// errReplicatedTableCatalogMiss is carried only by route resolution failures
// whose table is absent from the pinned catalog. ResolveReplicatedTableKey
// intentionally returns a broad bool for hot-path key/profile validation, so a
// separate marker keeps malformed keys, invalid profiles, and bad routes from
// invoking refresh.
var errReplicatedTableCatalogMiss = errors.New("gateway: replicated table is absent from catalog")

type replicatedTableCatalogMissError struct{}

func (replicatedTableCatalogMissError) Error() string { return ErrReplicatedTableRoute.Error() }

func (replicatedTableCatalogMissError) Is(target error) bool {
	return target == ErrReplicatedTableRoute || target == errReplicatedTableCatalogMiss
}

func replicatedTableRouteError(snapshot *Snapshot, table, key []byte) error {
	if replicatedTableAbsent(snapshot, table, key) {
		return replicatedTableCatalogMissError{}
	}
	return ErrReplicatedTableRoute
}

// replicatedTableAbsent distinguishes an absent replicated relation from a
// malformed key/profile. A planner placement proves the name is known but not
// eligible for this native API, so it must not cause a catalog refresh. Key
// validation is intentionally cold-path: the successful resolver already does
// the same canonical check without allocating.
func replicatedTableAbsent(snapshot *Snapshot, table, key []byte) bool {
	if snapshot == nil || len(table) == 0 {
		return false
	}
	name := string(table)
	if !collectionname.Valid(name) {
		return false
	}
	if _, ok := snapshot.replicatedTableAtBytes(table); ok {
		return false
	}
	_, _, _, placed := snapshot.plannerTableFor(name)
	return !placed && replicatedCanonicalOrderedScalarKey(key)
}

// replicatedCanonicalOrderedScalarKey mirrors ResolveReplicatedTableKey's
// scalar decoding and canonical re-encoding. It is called only after a route
// miss, so the extra cold-path validation cannot tax successful reads.
func replicatedCanonicalOrderedScalarKey(key []byte) bool {
	if len(key) == 0 || len(key) > replication.MaxMutationKeyBytes {
		return false
	}
	var decodedStorage [replication.MaxMutationKeyBytes + 16]byte
	component, decoded, next, err := orderedkey.DecodeComponent(
		decodedStorage[:0], key, 0,
	)
	if err != nil || component.Descending || next != len(key) {
		return false
	}
	payload := decoded[component.PayloadStart:component.PayloadEnd]
	var canonicalStorage [replication.MaxMutationKeyBytes]byte
	canonical := canonicalStorage[:0]
	var ok bool
	switch component.Kind {
	case orderedkey.KindString:
		canonical, ok = orderedkey.AppendString(canonical, payload, orderedkey.Ascending)
	case orderedkey.KindNumber:
		if _, err = distribution.NewNumber(byteview.String(payload)); err != nil {
			return false
		}
		canonical, ok = orderedkey.AppendNumber(canonical, payload, orderedkey.Ascending)
	default:
		return false
	}
	return ok && bytes.Equal(canonical, key)
}

// replicatedTableBatchCatalogMissEligible makes a batch miss refreshable only
// when every supplied table/key is a valid native scalar request. For the
// same-group API it also rejects already-known routes from different groups;
// an absent point cannot turn a mixed request into a coherent batch by itself.
// This runs only after a route miss and therefore does not add work to the hot
// path.
func replicatedTableBatchCatalogMissEligible(
	snapshot *Snapshot,
	points []ReplicatedTableBatchPoint,
	sameGroup bool,
) bool {
	if snapshot == nil || len(points) == 0 {
		return false
	}
	var routeID replication.Digest
	haveRoute := false
	for _, point := range points {
		if len(point.Table) == 0 || !collectionname.Valid(string(point.Table)) ||
			!replicatedCanonicalOrderedScalarKey(point.Key) {
			return false
		}
		_, known := snapshot.replicatedTableAtBytes(point.Table)
		if !known {
			name := string(point.Table)
			_, _, _, placed := snapshot.plannerTableFor(name)
			if placed {
				// The catalog knows this placement, but it is not a valid RF3
				// native profile (legacy or malformed profile).
				return false
			}
			continue
		}
		var replicas [ServingReplicaCount]ReplicatedEndpoint
		var scalarScratch [replication.MaxMutationKeyBytes + 16]byte
		resolved, ok := snapshot.ResolveReplicatedTableKey(
			point.Table, point.Key, scalarScratch[:0], replicas[:0],
		)
		if !ok {
			return false
		}
		if sameGroup {
			if !haveRoute {
				routeID, haveRoute = resolved.RouteID, true
			} else if resolved.RouteID != routeID {
				return false
			}
		}
	}
	return true
}
