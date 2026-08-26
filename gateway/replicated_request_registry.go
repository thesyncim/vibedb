package gateway

import (
	"context"
	"errors"
	"sync"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var (
	// ErrReplicatedTransactionRequestRegistry reports an invalid registry or
	// request. Request IDs and caller digests are byte-native opaque identities;
	// the registry never assigns a text grammar to either value.
	ErrReplicatedTransactionRequestRegistry = errors.New("gateway: invalid replicated transaction request registry")
	// ErrReplicatedTransactionRequestConflict reports reuse of one scoped request
	// ID with different exact caller bytes. The refusal happens before any
	// transaction orchestration or recovery call.
	ErrReplicatedTransactionRequestConflict = errors.New("gateway: replicated transaction request identity conflict")
	// ErrReplicatedTransactionRequestCapacity reports that every bounded entry
	// is occupied. Active, unresolved, and retained terminal entries are never
	// evicted to admit new work.
	ErrReplicatedTransactionRequestCapacity = errors.New("gateway: replicated transaction request registry is full")
	// ErrReplicatedTransactionRequestUnresolved reports an attempt to forget a
	// request which is executing or still owns exact recovery material.
	ErrReplicatedTransactionRequestUnresolved = errors.New("gateway: replicated transaction request is unresolved")
	// ErrReplicatedTransactionRequestRecovery reports a broken orchestrator
	// recovery contract. The registry keeps the prior exact handle in this case
	// rather than silently discarding the only material which can settle it.
	ErrReplicatedTransactionRequestRecovery = errors.New("gateway: replicated transaction recovery handle mismatch")
)

const AbsoluteMaxReplicatedTransactionRequestEntries = 1 << 20

// ReplicatedTransactionRequestOrchestrator is the exact execution/recovery
// surface used by the request registry. ReplicatedTransactionOrchestrator
// implements it directly; keeping the interface here also makes registry
// concurrency and ownership independently testable.
type ReplicatedTransactionRequestOrchestrator interface {
	Execute(context.Context, uint64, []ReplicatedTransactionParticipant) (ReplicatedTransactionResult, error)
	Recover(context.Context, *ReplicatedTransactionRecoveryHandle) (ReplicatedTransactionResult, error)
}

// ReplicatedTransactionRequestRegistryOptions freezes registry capacity.
type ReplicatedTransactionRequestRegistryOptions struct {
	Orchestrator ReplicatedTransactionRequestOrchestrator
	MaxEntries   int
}

// ReplicatedTransactionRequestRegistryStats is one instantaneous lifecycle
// count. PendingRecovery entries own a live recovery handle but have no active
// network call; Recovering entries own the same handle while Recover is live.
type ReplicatedTransactionRequestRegistryStats struct {
	Entries         int
	Executing       int
	PendingRecovery int
	Recovering      int
	Terminal        int
	Waiting         int
}

// ReplicatedTransactionRequestOutcome is the exact transaction outcome plus
// the immutable routing metadata selected by its first execution. Replays use
// these fields instead of the caller's current catalog so a topology change
// cannot rewrite the response of an already identified request.
type ReplicatedTransactionRequestOutcome struct {
	ReplicatedTransactionResult
	CatalogGeneration uint64
	ShardsFanned      int
}

type replicatedTransactionRequestScopeKind uint8

const (
	replicatedTransactionRequestScopeLocal replicatedTransactionRequestScopeKind = iota + 1
	replicatedTransactionRequestScopeAuthenticated
)

// replicatedTransactionRequestScope is fixed-width and deliberately excludes
// the authorization-policy generation. A policy publication may rotate while
// the same authenticated node retries an existing request. Local/plaintext
// requests occupy a separate kind and can never alias an authenticated node.
type replicatedTransactionRequestScope struct {
	node rafttransport.NodeID
	kind replicatedTransactionRequestScopeKind
}

type replicatedTransactionLocalScopeContextKey struct{}

// WithLocalReplicatedTransactionRequestScope marks an explicitly selected
// loopback/plaintext development boundary. It takes precedence over any
// internal forwarding authority attached to that boundary, so unauthenticated
// clients never share the authenticated service principal's request namespace.
func WithLocalReplicatedTransactionRequestScope(parent context.Context) context.Context {
	if parent == nil {
		return nil
	}
	return context.WithValue(parent, replicatedTransactionLocalScopeContextKey{}, true)
}

func replicatedTransactionRequestScopeFromContext(
	ctx context.Context,
) replicatedTransactionRequestScope {
	if ctx != nil {
		if local, _ := ctx.Value(replicatedTransactionLocalScopeContextKey{}).(bool); local {
			return replicatedTransactionRequestScope{kind: replicatedTransactionRequestScopeLocal}
		}
	}
	if authority, ok := serviceauthz.FromContext(ctx); ok {
		return replicatedTransactionRequestScope{
			node: authority.Node, kind: replicatedTransactionRequestScopeAuthenticated,
		}
	}
	return replicatedTransactionRequestScope{kind: replicatedTransactionRequestScopeLocal}
}

type replicatedTransactionRequestState uint8

const (
	replicatedTransactionRequestExecuting replicatedTransactionRequestState = iota + 1
	replicatedTransactionRequestPendingRecovery
	replicatedTransactionRequestRecovering
	replicatedTransactionRequestTerminal
)

type replicatedTransactionRequestCall struct {
	done    chan struct{}
	outcome ReplicatedTransactionRequestOutcome
	err     error
}

type replicatedTransactionRequestEntry struct {
	digest     replication.Digest
	generation uint64
	shards     int
	state      replicatedTransactionRequestState
	call       *replicatedTransactionRequestCall
	waiters    int
	handle     *ReplicatedTransactionRecoveryHandle
	outcome    ReplicatedTransactionRequestOutcome
	err        error
}

// ReplicatedTransactionRequestRegistry coalesces exact duplicate transaction
// requests and retains terminal outcomes and unresolved recovery ownership.
// Its mutex protects only the bounded directory and state transitions; no
// orchestrator or recovery network call runs while the mutex is held.
type ReplicatedTransactionRequestRegistry struct {
	mu           sync.Mutex
	orchestrator ReplicatedTransactionRequestOrchestrator
	entries      map[replicatedTransactionRequestKey]*replicatedTransactionRequestEntry
	maxEntries   int
}

func NewReplicatedTransactionRequestRegistry(
	options ReplicatedTransactionRequestRegistryOptions,
) (*ReplicatedTransactionRequestRegistry, error) {
	if options.Orchestrator == nil || options.MaxEntries <= 0 ||
		options.MaxEntries > AbsoluteMaxReplicatedTransactionRequestEntries {
		return nil, ErrReplicatedTransactionRequestRegistry
	}
	return &ReplicatedTransactionRequestRegistry{
		orchestrator: options.Orchestrator,
		entries:      make(map[replicatedTransactionRequestKey]*replicatedTransactionRequestEntry, options.MaxEntries),
		maxEntries:   options.MaxEntries,
	}, nil
}

// Execute runs or joins one request identity. A new identity calls Execute;
// an unresolved prior attempt calls Recover with the exact retained handle;
// an active exact duplicate waits for that one call; and a terminal duplicate
// receives the cached result/error. A canceled waiter never cancels or mutates
// the active owner's call.
func (registry *ReplicatedTransactionRequestRegistry) Execute(
	ctx context.Context,
	requestID replication.ID128,
	requestDigest replication.Digest,
	catalogGeneration uint64,
	participants []ReplicatedTransactionParticipant,
) (ReplicatedTransactionRequestOutcome, error) {
	if registry == nil || registry.orchestrator == nil || ctx == nil ||
		requestID == (replication.ID128{}) || requestDigest == (replication.Digest{}) ||
		catalogGeneration == 0 {
		return ReplicatedTransactionRequestOutcome{}, ErrReplicatedTransactionRequestRegistry
	}
	key := replicatedTransactionRequestKey{
		scope: replicatedTransactionRequestScopeFromContext(ctx), id: requestID,
	}

	registry.mu.Lock()
	entry := registry.entries[key]
	if entry != nil && entry.digest != requestDigest {
		registry.mu.Unlock()
		return ReplicatedTransactionRequestOutcome{}, ErrReplicatedTransactionRequestConflict
	}
	if entry == nil {
		if len(registry.entries) >= registry.maxEntries {
			registry.mu.Unlock()
			return ReplicatedTransactionRequestOutcome{}, ErrReplicatedTransactionRequestCapacity
		}
		call := &replicatedTransactionRequestCall{done: make(chan struct{})}
		entry = &replicatedTransactionRequestEntry{
			digest: requestDigest, generation: catalogGeneration, shards: len(participants),
			state: replicatedTransactionRequestExecuting, call: call,
		}
		registry.entries[key] = entry
		registry.mu.Unlock()

		result, err := registry.orchestrator.Execute(ctx, catalogGeneration, participants)
		return registry.settle(key, entry, call, nil, result, err)
	}
	return registry.replayLocked(ctx, key, entry)
}

// Replay returns or recovers an already admitted request without creating a
// registry entry. Callers invoke it before pinning a catalog or lowering SQL;
// found=false is the only result which permits planning a new request.
func (registry *ReplicatedTransactionRequestRegistry) Replay(
	ctx context.Context,
	requestID replication.ID128,
	requestDigest replication.Digest,
) (outcome ReplicatedTransactionRequestOutcome, found bool, err error) {
	if registry == nil || registry.orchestrator == nil || ctx == nil ||
		requestID == (replication.ID128{}) || requestDigest == (replication.Digest{}) {
		return outcome, false, ErrReplicatedTransactionRequestRegistry
	}
	key := replicatedTransactionRequestKey{
		scope: replicatedTransactionRequestScopeFromContext(ctx), id: requestID,
	}
	registry.mu.Lock()
	entry := registry.entries[key]
	if entry == nil {
		registry.mu.Unlock()
		return outcome, false, nil
	}
	if entry.digest != requestDigest {
		registry.mu.Unlock()
		return outcome, true, ErrReplicatedTransactionRequestConflict
	}
	outcome, err = registry.replayLocked(ctx, key, entry)
	return outcome, true, err
}

// replayLocked consumes registry.mu ownership and never returns with it held.
func (registry *ReplicatedTransactionRequestRegistry) replayLocked(
	ctx context.Context,
	key replicatedTransactionRequestKey,
	entry *replicatedTransactionRequestEntry,
) (ReplicatedTransactionRequestOutcome, error) {

	switch entry.state {
	case replicatedTransactionRequestTerminal:
		outcome, err := entry.outcome, entry.err
		registry.mu.Unlock()
		return outcome, err
	case replicatedTransactionRequestExecuting, replicatedTransactionRequestRecovering:
		call := entry.call
		entry.waiters++
		registry.mu.Unlock()
		if call == nil || call.done == nil {
			registry.mu.Lock()
			entry.waiters--
			registry.mu.Unlock()
			return ReplicatedTransactionRequestOutcome{}, ErrReplicatedTransactionRequestRegistry
		}
		var outcome ReplicatedTransactionRequestOutcome
		var err error
		select {
		case <-call.done:
			outcome, err = call.outcome, call.err
		case <-ctx.Done():
			err = context.Cause(ctx)
		}
		registry.mu.Lock()
		entry.waiters--
		registry.mu.Unlock()
		return outcome, err
	case replicatedTransactionRequestPendingRecovery:
		handle := entry.handle
		if handle == nil {
			registry.mu.Unlock()
			return ReplicatedTransactionRequestOutcome{}, ErrReplicatedTransactionRequestRegistry
		}
		call := &replicatedTransactionRequestCall{done: make(chan struct{})}
		entry.state, entry.call = replicatedTransactionRequestRecovering, call
		registry.mu.Unlock()

		result, err := registry.orchestrator.Recover(ctx, handle)
		return registry.settle(key, entry, call, handle, result, err)
	default:
		registry.mu.Unlock()
		return ReplicatedTransactionRequestOutcome{}, ErrReplicatedTransactionRequestRegistry
	}
}

// RecoverPending makes at most one recovery attempt for each request which was
// pending when the sweep began. Work already executing or recovering is left
// to its owner. The return count is the number of Recover calls made; joined
// errors are caller-visible sanitized outcomes and never expose live handles.
func (registry *ReplicatedTransactionRequestRegistry) RecoverPending(
	ctx context.Context,
) (int, error) {
	if registry == nil || registry.orchestrator == nil || ctx == nil {
		return 0, ErrReplicatedTransactionRequestRegistry
	}
	registry.mu.Lock()
	pending := make([]replicatedTransactionRequestRecoveryKey, 0, len(registry.entries))
	for key, entry := range registry.entries {
		if entry.state == replicatedTransactionRequestPendingRecovery {
			pending = append(pending, replicatedTransactionRequestRecoveryKey{
				key: key, digest: entry.digest,
			})
		}
	}
	registry.mu.Unlock()

	attempted := 0
	var joined error
	for _, key := range pending {
		if cause := context.Cause(ctx); cause != nil {
			return attempted, errors.Join(joined, cause)
		}
		attemptedOne, err := registry.recoverPending(ctx, key)
		if attemptedOne {
			attempted++
			joined = errors.Join(joined, err)
		}
	}
	return attempted, joined
}

type replicatedTransactionRequestKey struct {
	scope replicatedTransactionRequestScope
	id    replication.ID128
}

type replicatedTransactionRequestRecoveryKey struct {
	key    replicatedTransactionRequestKey
	digest replication.Digest
}

func (registry *ReplicatedTransactionRequestRegistry) recoverPending(
	ctx context.Context,
	recoveryKey replicatedTransactionRequestRecoveryKey,
) (bool, error) {
	registry.mu.Lock()
	entry := registry.entries[recoveryKey.key]
	if entry == nil || entry.digest != recoveryKey.digest ||
		entry.state != replicatedTransactionRequestPendingRecovery {
		registry.mu.Unlock()
		return false, nil
	}
	handle := entry.handle
	if handle == nil {
		registry.mu.Unlock()
		return false, ErrReplicatedTransactionRequestRegistry
	}
	call := &replicatedTransactionRequestCall{done: make(chan struct{})}
	entry.state, entry.call = replicatedTransactionRequestRecovering, call
	registry.mu.Unlock()

	result, err := registry.orchestrator.Recover(ctx, handle)
	_, settledErr := registry.settle(recoveryKey.key, entry, call, handle, result, err)
	return true, settledErr
}

// Forget removes one resolved entry and releases its registry slot. It never
// abandons an executing request or live recovery handle.
func (registry *ReplicatedTransactionRequestRegistry) Forget(
	ctx context.Context,
	requestID replication.ID128,
	requestDigest replication.Digest,
) error {
	if registry == nil || ctx == nil || requestID == (replication.ID128{}) ||
		requestDigest == (replication.Digest{}) {
		return ErrReplicatedTransactionRequestRegistry
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	key := replicatedTransactionRequestKey{
		scope: replicatedTransactionRequestScopeFromContext(ctx), id: requestID,
	}
	entry := registry.entries[key]
	if entry == nil {
		return nil
	}
	if entry.digest != requestDigest {
		return ErrReplicatedTransactionRequestConflict
	}
	if entry.state != replicatedTransactionRequestTerminal || entry.handle != nil {
		return ErrReplicatedTransactionRequestUnresolved
	}
	delete(registry.entries, key)
	return nil
}

func (registry *ReplicatedTransactionRequestRegistry) Stats() ReplicatedTransactionRequestRegistryStats {
	if registry == nil {
		return ReplicatedTransactionRequestRegistryStats{}
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	stats := ReplicatedTransactionRequestRegistryStats{Entries: len(registry.entries)}
	for _, entry := range registry.entries {
		stats.Waiting += entry.waiters
		switch entry.state {
		case replicatedTransactionRequestExecuting:
			stats.Executing++
		case replicatedTransactionRequestPendingRecovery:
			stats.PendingRecovery++
		case replicatedTransactionRequestRecovering:
			stats.Recovering++
		case replicatedTransactionRequestTerminal:
			stats.Terminal++
		}
	}
	return stats
}

func (registry *ReplicatedTransactionRequestRegistry) settle(
	key replicatedTransactionRequestKey,
	entry *replicatedTransactionRequestEntry,
	call *replicatedTransactionRequestCall,
	priorHandle *ReplicatedTransactionRecoveryHandle,
	result ReplicatedTransactionResult,
	err error,
) (ReplicatedTransactionRequestOutcome, error) {
	admitted := err == nil || result.Committed || result.ID != ([16]byte{})
	var admittedError *ReplicatedTransactionError
	if errors.As(err, &admittedError) && admittedError != nil &&
		(admittedError.ID != ([16]byte{}) || admittedError.Committed ||
			admittedError.Recovery != nil) {
		admitted = true
	}
	publicResult, publicErr, nextHandle, handleErr :=
		detachReplicatedTransactionRecovery(result, err)
	if priorHandle != nil && nextHandle != nil && nextHandle != priorHandle {
		handleErr = ErrReplicatedTransactionRequestRecovery
	}
	if handleErr != nil {
		publicErr = errors.Join(publicErr, handleErr)
	}
	retainPrior := priorHandle != nil && nextHandle == nil && publicErr != nil
	outcome := ReplicatedTransactionRequestOutcome{
		ReplicatedTransactionResult: publicResult, CatalogGeneration: entry.generation,
		ShardsFanned: entry.shards,
	}

	registry.mu.Lock()
	current := registry.entries[key]
	if current != entry || entry.call != call ||
		(entry.state != replicatedTransactionRequestExecuting &&
			entry.state != replicatedTransactionRequestRecovering) {
		registry.mu.Unlock()
		return ReplicatedTransactionRequestOutcome{}, ErrReplicatedTransactionRequestRegistry
	}
	call.outcome, call.err = outcome, publicErr
	entry.call = nil
	switch {
	case priorHandle == nil && nextHandle == nil && !admitted && handleErr == nil:
		// A plain pre-admission/transient failure is shared with every waiter on
		// this one call, then forgotten. It has no durable identity or recovery
		// proof and therefore must not poison bounded capacity forever.
		delete(registry.entries, key)
	case handleErr != nil && priorHandle != nil:
		entry.state = replicatedTransactionRequestPendingRecovery
		entry.handle = priorHandle
	case retainPrior:
		// Recover can fail before it reaches its handle-carrying execution
		// errors, for example while a canceled context waits for recovery
		// validation admission. Absence of a new handle is not a terminal proof;
		// keep the exact prior ownership for the next retry.
		entry.state = replicatedTransactionRequestPendingRecovery
		entry.handle = priorHandle
	case nextHandle != nil:
		entry.state = replicatedTransactionRequestPendingRecovery
		entry.handle = nextHandle
	default:
		entry.state = replicatedTransactionRequestTerminal
		entry.handle = nil
		entry.outcome, entry.err = outcome, publicErr
	}
	close(call.done)
	registry.mu.Unlock()
	return outcome, publicErr
}

// detachReplicatedTransactionRecovery makes the caller-visible outcome safe to
// cache and copy while transferring the one live recovery pointer to the
// registry. ReplicatedTransactionError is rebuilt without Recovery so callers
// cannot race Recover, discard ownership, or mutate its exact handle.
func detachReplicatedTransactionRecovery(
	result ReplicatedTransactionResult,
	err error,
) (ReplicatedTransactionResult, error, *ReplicatedTransactionRecoveryHandle, error) {
	handle := result.Recovery
	result.Recovery = nil
	var transactionErr *ReplicatedTransactionError
	if errors.As(err, &transactionErr) {
		if transactionErr.Recovery != nil {
			if handle != nil && handle != transactionErr.Recovery {
				return result, &ReplicatedTransactionError{
					ID: transactionErr.ID, Committed: transactionErr.Committed,
					Cause: transactionErr.Cause,
				}, handle, ErrReplicatedTransactionRequestRecovery
			}
			handle = transactionErr.Recovery
		}
		err = &ReplicatedTransactionError{
			ID: transactionErr.ID, Committed: transactionErr.Committed,
			Cause: transactionErr.Cause,
		}
	}
	return result, err, handle, nil
}
