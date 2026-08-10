package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// The bounded fan-out executor: it pins one catalog generation, routes, and
// dispatches the per-shard requests through a bounded worker pool under a global
// and per-shard deadline, enforcing an aggregate row and byte cap and cancelling
// outstanding shards on a hard failure or once the cap is hit. A stale routing
// version or ownership epoch reported by any shard triggers a bounded retry, but
// only after refreshing to a strictly newer catalog generation — one attempt
// never mixes generations. The partial-result policy is fail-closed: any shard
// failure fails the whole operation.

// ErrNoCatalog reports that no catalog generation has been published, so no
// operation can be routed.
var ErrNoCatalog = errors.New("gateway: no catalog generation is published")

// ErrWriteNotSupported reports a mutating statement submitted to the
// distributed read executor. Cross-shard writes require a separate protocol
// with durable command identity and completion semantics; Query refuses them
// before routing or network I/O so a fan-out can never partially commit.
var ErrWriteNotSupported = errors.New("gateway: distributed writes are not supported")

// WriteNotSupportedError identifies the statement kind refused by Query. It
// wraps ErrWriteNotSupported so callers can use errors.Is without parsing the
// diagnostic string.
type WriteNotSupportedError struct {
	Kind sqlast.Kind
}

func (e *WriteNotSupportedError) Error() string {
	return fmt.Sprintf("gateway: %s is not supported by the distributed read executor", e.Kind)
}

func (e *WriteNotSupportedError) Unwrap() error { return ErrWriteNotSupported }

// ErrStaleGeneration reports that a shard rejected the pinned generation and no
// strictly newer compatible generation was available to retry against, so the
// operation fails closed rather than routing against stale metadata.
var ErrStaleGeneration = errors.New("gateway: pinned catalog generation is stale and no newer generation is available")

// RefreshFunc obtains a catalog generation strictly newer than staleGen after a
// shard reports the pinned generation is stale. It must return a snapshot whose
// generation exceeds staleGen, or an error. A nil RefreshFunc re-reads the
// executor's catalog holder.
type RefreshFunc func(ctx context.Context, staleGen uint64) (*Snapshot, error)

// Options configure an [Executor]. The zero value is usable: the default
// operational profiles, a small retry budget, and the catalog holder as the
// refresh source.
type Options struct {
	// Profiles overrides the per-class operational profiles. A class absent from
	// the map keeps its DefaultProfiles entry.
	Profiles map[OperationClass]Profile
	// MaxRetries bounds stale-generation retries. Zero selects a conservative
	// default; a negative value disables retry.
	MaxRetries int
	// Refresh supplies a newer generation on a stale-generation retry. Nil
	// re-reads the catalog holder.
	Refresh RefreshFunc
}

// defaultMaxRetries bounds stale-generation retries when Options leaves it zero.
const defaultMaxRetries = 2

// Executor routes and dispatches bounded distributed reads over a pinned catalog
// generation. It is safe for concurrent use.
type Executor struct {
	client   *Client
	catalog  *CatalogHolder
	profiles map[OperationClass]Profile
	refresh  RefreshFunc
	maxRetry int
	routers  *routerPool
	metrics  Metrics
}

// NewExecutor returns an executor that dispatches through client and pins
// generations from catalog. Both are required.
func NewExecutor(client *Client, catalog *CatalogHolder, opts Options) *Executor {
	profiles := DefaultProfiles()
	for class, p := range opts.Profiles {
		profiles[class] = p
	}
	for class, p := range profiles {
		profiles[class] = p.withDefaults()
	}
	maxRetry := opts.MaxRetries
	if maxRetry == 0 {
		maxRetry = defaultMaxRetries
	}
	if maxRetry < 0 {
		maxRetry = 0
	}
	return &Executor{
		client:   client,
		catalog:  catalog,
		profiles: profiles,
		refresh:  opts.Refresh,
		maxRetry: maxRetry,
		routers:  newRouterPool(),
	}
}

// Metrics returns a snapshot of the executor's route and fan-out counters.
func (e *Executor) Metrics() MetricsSnapshot { return e.metrics.Snapshot() }

// Query is one bounded distributed read. SQL and its typed parameters are the
// only semantic inputs; the pinned catalog and shared SQL routing compiler
// derive the distribution, shard constraints, merge order, and global limit.
type Query struct {
	SQL    string
	Params []shardservice.Param

	Class OperationClass
}

// Result is a distributed read's merged outcome plus the routing metadata a
// caller reads for observability. Kind is ResponseRows for the read path;
// RowsAffected is meaningful only for a single-shard completion passthrough.
type Result struct {
	Kind         shardservice.ResponseKind
	Columns      []shardservice.Column
	Rows         [][]shardservice.Cell
	RowsAffected int64

	RouteKind     distribution.RouteKind
	Generation    uint64
	ShardsFanned  int
	Retries       int
	ScatterReason ScatterReason
}

// Query routes and dispatches q, retrying against a refreshed generation when a
// shard reports the pinned generation is stale. It pins one generation per
// attempt and never mixes generations within an attempt.
func (e *Executor) Query(ctx context.Context, q Query) (*Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	args := make([]any, len(q.Params))
	for i := range q.Params {
		if !q.Params[i].Valid() {
			return nil, fmt.Errorf("%w: parameter %d has invalid kind %d",
				ErrPlanParameters, i+1, q.Params[i].Kind)
		}
		args[i] = q.Params[i].RuntimeValue()
	}
	profile := e.profileFor(q.Class)

	var staleGen uint64
	for attempt := 0; ; attempt++ {
		snap, err := e.pin(ctx, attempt, staleGen)
		if err != nil {
			return nil, err
		}
		prepared, err := snap.Prepare(ctx, q.SQL)
		if err != nil {
			return nil, err
		}
		bound, err := prepared.Bind(args)
		if err != nil {
			return nil, err
		}
		pl, err := e.route(snap, &q, bound, profile)
		if err != nil {
			return nil, err
		}
		e.metrics.observeRoute(pl.kind, len(pl.calls), pl.scatter)

		res, err := e.dispatch(ctx, pl, profile)
		if err == nil {
			res.RouteKind = pl.kind
			res.Generation = pl.generation
			res.ShardsFanned = len(pl.calls)
			res.Retries = attempt
			res.ScatterReason = pl.scatter
			return res, nil
		}
		if isStaleErr(err) && attempt < e.maxRetry {
			staleGen = snap.Generation()
			e.metrics.observeRetry()
			continue
		}
		return nil, err
	}
}

// profileFor returns the operational profile for class, falling back to the
// interactive profile for an unknown class.
func (e *Executor) profileFor(class OperationClass) Profile {
	if p, ok := e.profiles[class]; ok {
		return p
	}
	return e.profiles[ClassInteractive]
}

// pin returns the generation the attempt routes against: the current generation
// on the first attempt, or a strictly newer generation obtained by refresh on a
// retry. A refresh that cannot produce a newer generation fails closed.
func (e *Executor) pin(ctx context.Context, attempt int, staleGen uint64) (*Snapshot, error) {
	if attempt == 0 {
		snap := e.catalog.Current()
		if snap == nil {
			return nil, ErrNoCatalog
		}
		return snap, nil
	}
	if e.refresh != nil {
		snap, err := e.refresh(ctx, staleGen)
		if err != nil {
			return nil, err
		}
		if snap == nil || snap.Generation() <= staleGen {
			return nil, ErrStaleGeneration
		}
		return snap, nil
	}
	snap := e.catalog.Current()
	if snap == nil || snap.Generation() <= staleGen {
		return nil, ErrStaleGeneration
	}
	return snap, nil
}

// isStaleErr reports whether err is a shard's refusal of the pinned generation:
// a stale routing version, a mismatched ownership epoch, or a not-owner refusal
// after ownership moved. Each is retryable only against a newer generation.
func isStaleErr(err error) bool {
	return errors.Is(err, distribution.ErrRoutingVersion) ||
		errors.Is(err, distribution.ErrOwnershipEpoch) ||
		errors.Is(err, distribution.ErrNotShardOwner)
}

// dispatch executes the routed plan: an empty route returns no rows without
// contacting a shard, a single-shard route streams through unchanged, and a
// multi-shard route fans out and merges.
func (e *Executor) dispatch(ctx context.Context, pl *plan, p Profile) (*Result, error) {
	switch len(pl.calls) {
	case 0:
		return &Result{Kind: shardservice.ResponseRows}, nil
	case 1:
		return e.single(ctx, pl.calls[0], p)
	default:
		return e.fanout(ctx, pl, p)
	}
}

// single performs a single-shard route: one round-trip, streamed through
// unchanged. The shard already applied the statement's own ORDER BY and LIMIT.
func (e *Executor) single(ctx context.Context, call shardCall, p Profile) (*Result, error) {
	opctx, cancel := context.WithTimeout(ctx, p.PerShardDeadline)
	defer cancel()
	resp, err := e.client.Do(opctx, call.address, call.req)
	if err != nil {
		return nil, err
	}
	rows := uint64(len(resp.Rows))
	bytes := responseBytes(resp)
	if err := checkAggregate(p, rows, bytes); err != nil {
		return nil, err
	}
	e.metrics.observeResult(rows, bytes)
	return &Result{Kind: resp.Kind, Columns: resp.Columns, Rows: resp.Rows, RowsAffected: resp.RowsAffected}, nil
}

// fanout dispatches every shard call concurrently through a bounded worker pool,
// enforces the aggregate caps, cancels outstanding shards on a hard failure or
// once a cap is hit, and merges the results. The partial-result policy is
// fail-closed: any shard failure fails the whole operation.
func (e *Executor) fanout(ctx context.Context, pl *plan, p Profile) (*Result, error) {
	opctx, cancel := context.WithTimeout(ctx, p.GlobalDeadline)
	defer cancel()

	results := make([]*shardservice.ShardResponse, len(pl.calls))

	var (
		mu         sync.Mutex
		firstErr   error
		totalRows  uint64
		totalBytes uint64
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
		cancel()
	}

	jobs := make(chan int)
	workers := min(max(1, p.MaxConcurrency), len(pl.calls))
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for i := range jobs {
				select {
				case <-opctx.Done():
					return
				default:
				}

				sctx, sc := context.WithTimeout(opctx, p.PerShardDeadline)
				resp, err := e.client.Do(sctx, pl.calls[i].address, pl.calls[i].req)
				sc()
				if err != nil {
					fail(err)
					return
				}
				results[i] = resp

				rows := uint64(len(resp.Rows))
				b := responseBytes(resp)
				mu.Lock()
				totalRows += rows
				totalBytes += b
				over := checkAggregate(p, totalRows, totalBytes)
				mu.Unlock()
				if over != nil {
					fail(over)
					return
				}
			}
		})
	}
	go func() {
		defer close(jobs)
		for i := range pl.calls {
			select {
			case jobs <- i:
			case <-opctx.Done():
				return
			}
		}
	}()
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	columns, rows, err := mergeRows(results, pl.order, pl.limit)
	if err != nil {
		return nil, err
	}
	e.metrics.observeResult(totalRows, totalBytes)
	return &Result{Kind: shardservice.ResponseRows, Columns: columns, Rows: rows}, nil
}

// checkAggregate reports ErrResultLimit when the running row or byte total
// exceeds the profile's aggregate cap.
func checkAggregate(p Profile, rows, bytes uint64) error {
	if p.MaxAggregateRows > 0 && rows > p.MaxAggregateRows {
		return fmt.Errorf("%w: buffered %d rows exceeds the %d aggregate cap", ErrResultLimit, rows, p.MaxAggregateRows)
	}
	if p.MaxAggregateBytes > 0 && bytes > p.MaxAggregateBytes {
		return fmt.Errorf("%w: buffered %d bytes exceeds the %d aggregate cap", ErrResultLimit, bytes, p.MaxAggregateBytes)
	}
	return nil
}

// responseBytes sums the encoded cell bytes of a row response.
func responseBytes(resp *shardservice.ShardResponse) uint64 {
	var n uint64
	for _, row := range resp.Rows {
		for _, c := range row {
			n += uint64(len(c.Bytes))
		}
	}
	return n
}
