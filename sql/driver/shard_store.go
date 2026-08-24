package driver

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

// ShardStoreBinding is the topology-owned part of one local shard store's
// immutable identity. AllocationGeneration distinguishes a later physical
// allocation that reuses the same human-readable shard name.
type ShardStoreBinding struct {
	Distribution         distribution.DistributionName
	Shard                distribution.ShardID
	AllocationGeneration distribution.ShardAllocationGeneration
}

// ShardStoreIdentity is the write-once identity persisted in a shard SQL
// catalog. LogID is generated from crypto/rand during initialization and is
// preserved across every catalog rewrite. It identifies this local log
// incarnation; it is not a lease, revocation record, or HA authority claim.
type ShardStoreIdentity struct {
	Distribution         distribution.DistributionName
	Shard                distribution.ShardID
	AllocationGeneration distribution.ShardAllocationGeneration
	LogID                [16]byte
}

// ShardStoreFence is the durable local serving high-water for one immutable
// [ShardStoreIdentity]. Both coordinates are independently monotonic. It does
// not grant a distributed lease or prove that this store is the elected copy.
type ShardStoreFence struct {
	OwnershipEpoch distribution.OwnershipEpoch `json:"ownership_epoch"`
	RoutingVersion distribution.RoutingVersion `json:"routing_version"`
}

// Binding returns the topology-owned coordinates of identity.
func (i ShardStoreIdentity) Binding() ShardStoreBinding {
	return ShardStoreBinding{
		Distribution:         i.Distribution,
		Shard:                i.Shard,
		AllocationGeneration: i.AllocationGeneration,
	}
}

var (
	// ErrShardStoreUnbound reports a catalog that has no durable shard identity.
	// OpenShardStore never initializes a catalog implicitly.
	ErrShardStoreUnbound = errors.New("vibedb: SQL catalog is not initialized as a shard store")
	// ErrShardStoreIdentityMismatch reports either topology coordinates that do
	// not match a catalog's immutable binding, an attempt to initialize an
	// existing unbound root, or a generic open of a shard-bound catalog.
	ErrShardStoreIdentityMismatch = errors.New("vibedb: shard store identity mismatch")
	// ErrShardStoreServingClaimed reports a second live in-process serving claim
	// over the same writer-owning Database. Closing the first claim releases it.
	ErrShardStoreServingClaimed = errors.New("vibedb: shard store already has a live serving claim")
)

// ShardStoreError retains structured expected and durable identities while
// supporting errors.Is through Err. Actual is zero for an unbound catalog.
type ShardStoreError struct {
	Op       string
	Path     string
	Expected ShardStoreBinding
	Actual   ShardStoreIdentity
	Err      error
}

func (e *ShardStoreError) Error() string {
	if e == nil {
		return "vibedb: shard store error"
	}
	prefix := "vibedb: shard store"
	if e.Op != "" {
		prefix += " " + e.Op
	}
	if e.Path != "" {
		prefix += " " + e.Path
	}
	if errors.Is(e.Err, ErrShardStoreUnbound) {
		return prefix + ": " + ErrShardStoreUnbound.Error()
	}
	return fmt.Sprintf(
		"%s: expected distribution=%q shard=%q allocation-generation=%d; "+
			"catalog has distribution=%q shard=%q allocation-generation=%d log-id=%x: %v",
		prefix,
		e.Expected.Distribution, e.Expected.Shard, e.Expected.AllocationGeneration,
		e.Actual.Distribution, e.Actual.Shard, e.Actual.AllocationGeneration,
		e.Actual.LogID, e.Err,
	)
}

// Unwrap exposes ErrShardStoreUnbound or ErrShardStoreIdentityMismatch.
func (e *ShardStoreError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ShardStoreFenceError reports a zero or regressed local serving coordinate.
// Unwrap exposes distribution.ErrOwnershipEpoch or
// distribution.ErrRoutingVersion so topology callers can classify the stale
// coordinate without parsing text.
type ShardStoreFenceError struct {
	Op        string
	Path      string
	Requested ShardStoreFence
	Durable   ShardStoreFence
	Err       error
}

func (e *ShardStoreFenceError) Error() string {
	if e == nil {
		return "vibedb: shard store serving fence error"
	}
	prefix := "vibedb: shard store serving fence"
	if e.Op != "" {
		prefix += " " + e.Op
	}
	if e.Path != "" {
		prefix += " " + e.Path
	}
	return fmt.Sprintf(
		"%s: requested ownership-epoch=%d routing-version=%d; "+
			"durable high-water ownership-epoch=%d routing-version=%d: %v",
		prefix,
		e.Requested.OwnershipEpoch, e.Requested.RoutingVersion,
		e.Durable.OwnershipEpoch, e.Durable.RoutingVersion,
		e.Err,
	)
}

// Unwrap exposes the typed distribution coordinate error.
func (e *ShardStoreFenceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ShardStoreServingClaim is one exclusive, process-local permission to serve a
// shard Database at a durable fence. Close is idempotent. The claim protects
// only other services sharing this exact open store; it is not a distributed
// lease and cannot revoke a process serving a copied store. It also does not
// discover or fence Sessions a trusted caller opens directly on Database.
type ShardStoreServingClaim struct {
	database *database
	identity ShardStoreIdentity
	fence    ShardStoreFence
	once     sync.Once
}

// Identity returns the immutable store identity covered by the claim.
func (c *ShardStoreServingClaim) Identity() ShardStoreIdentity {
	if c == nil {
		return ShardStoreIdentity{}
	}
	return c.identity
}

// Fence returns the durable serving coordinates covered by the claim.
func (c *ShardStoreServingClaim) Fence() ShardStoreFence {
	if c == nil {
		return ShardStoreFence{}
	}
	return c.fence
}

// Close releases this process-local claim. It does not lower the durable
// high-water, so an equal claim may be retried and a lower claim remains stale.
// It does not drain directly opened Sessions; a claimant must stop producing
// work and close those Sessions first. shardservice.Server provides that drain
// for its own connections.
func (c *ShardStoreServingClaim) Close() error {
	if c == nil {
		return nil
	}
	c.once.Do(func() {
		core := c.database
		if core == nil {
			return
		}
		core.mu.Lock()
		if core.servingClaim == c {
			core.servingClaim = nil
		}
		core.mu.Unlock()
		c.database = nil
	})
	return nil
}

type shardStoreOpenMode uint8

const (
	shardStoreOpenGeneric shardStoreOpenMode = iota
	shardStoreOpenInitialize
	shardStoreOpenExisting
	shardStoreOpenReplicatedExisting
	shardStoreOpenReplicatedSettlement
	shardStoreOpenReplicatedApplyExisting
	shardStoreOpenReplicatedApplySettlement
	shardStoreOpenReplicatedChildStageResume
)

type shardStoreOpenPolicy struct {
	mode                        shardStoreOpenMode
	expected                    ShardStoreBinding
	expectedReplicated          ReplicatedShardStoreIdentity
	expectedReplicatedLogID     [16]byte
	expectedReplicatedUserTable string
	expectedReplicatedApply     ReplicatedApplyIdentity
	expectedReplicatedOptions   ReplicatedApplyOptions
	persistIdentity             func(*database) (bool, error)
}

// InitializeShardStore explicitly binds a genuinely new SQL storage root and
// returns its writer-owning Database. It refuses an existing ordinary catalog
// and an existing unbound table root. The generated LogID and supplied
// coordinates are the first atomic SQL catalog publication, before table
// namespace creation or recovery.
//
// Retrying after an ambiguous initialization outcome is idempotent when the
// already-published coordinates match exactly: the existing LogID is retained.
// Different coordinates are always rejected; there is no rebind or force
// option.
func InitializeShardStore(path string, binding ShardStoreBinding) (*Database, error) {
	if err := validateShardStoreBinding(binding); err != nil {
		return nil, err
	}
	binding = ownedShardStoreBinding(binding)
	database, err := openDatabaseWithShardStorePolicy(path, nil, shardStoreOpenPolicy{
		mode: shardStoreOpenInitialize, expected: binding,
	})
	if err != nil {
		return nil, err
	}
	return &Database{connector: &dbConnector{db: database}}, nil
}

// OpenShardStore opens an existing, explicitly initialized shard catalog. It
// compares binding while holding the catalog writer lock and before namespace
// recovery, transaction recovery, table opens, or catalog repair. A missing or
// ordinary unbound catalog is rejected and is never initialized as a side
// effect.
func OpenShardStore(path string, binding ShardStoreBinding) (*Database, error) {
	if err := validateShardStoreBinding(binding); err != nil {
		return nil, err
	}
	binding = ownedShardStoreBinding(binding)
	// Avoid creating even the sibling lock file for the ordinary missing-store
	// case. The locked read below repeats this check and closes the race with a
	// concurrent initializer or namespace replacement.
	absolute, err := canonicalCatalogPath(path)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(absolute); err != nil {
		if os.IsNotExist(err) {
			return nil, &ShardStoreError{
				Op: "open", Path: absolute, Expected: binding,
				Err: ErrShardStoreUnbound,
			}
		}
		return nil, err
	}
	database, err := openDatabaseWithShardStorePolicy(path, nil, shardStoreOpenPolicy{
		mode: shardStoreOpenExisting, expected: binding,
	})
	if err != nil {
		return nil, err
	}
	return &Database{connector: &dbConnector{db: database}}, nil
}

// ShardStoreIdentity returns the database's immutable durable shard identity.
// Ordinary catalogs report ErrShardStoreUnbound.
func (d *Database) ShardStoreIdentity() (ShardStoreIdentity, error) {
	if d == nil || d.connector == nil {
		return ShardStoreIdentity{}, ErrDatabaseClosed
	}
	d.connector.mu.Lock()
	if d.connector.closed || d.connector.db == nil {
		d.connector.mu.Unlock()
		return ShardStoreIdentity{}, ErrDatabaseClosed
	}
	core := d.connector.db
	d.connector.mu.Unlock()

	core.mu.RLock()
	defer core.mu.RUnlock()
	if core.closed {
		return ShardStoreIdentity{}, ErrDatabaseClosed
	}
	if core.catalog.ReplicatedShardStore != nil {
		return ShardStoreIdentity{}, fmt.Errorf(
			"vibedb: inspect local shard identity for replicated root %s: %w",
			core.path, ErrDirectWriteFenced,
		)
	}
	if core.catalog.ShardStore == nil {
		return ShardStoreIdentity{}, &ShardStoreError{
			Op: "inspect", Path: core.path, Err: ErrShardStoreUnbound,
		}
	}
	return *core.catalog.ShardStore, nil
}

// RequireShardStore verifies that d is bound to expected and returns the full
// identity, including the persistent LogID.
func (d *Database) RequireShardStore(expected ShardStoreBinding) (ShardStoreIdentity, error) {
	if err := validateShardStoreBinding(expected); err != nil {
		return ShardStoreIdentity{}, err
	}
	expected = ownedShardStoreBinding(expected)
	identity, err := d.ShardStoreIdentity()
	if err != nil {
		return ShardStoreIdentity{}, err
	}
	if identity.Binding() != expected {
		return ShardStoreIdentity{}, &ShardStoreError{
			Op: "validate", Expected: expected, Actual: identity,
			Err: ErrShardStoreIdentityMismatch,
		}
	}
	return identity, nil
}

// ClaimShardStoreServing durably advances this immutable store's local serving
// high-waters and returns the sole live in-process serving claim. A successful
// return means the requested fence was already durable or its catalog
// publication and parent-directory fence completed before the claim became
// visible. Lower coordinates return a typed distribution error. Close the
// claim before retrying equal coordinates.
//
// This is a same-open-store safety boundary, not a distributed lease,
// election, or copied-store revocation mechanism. NewSession remains a trusted
// low-level API outside the claim: callers must not share a shard Database with
// an independent direct-session producer and then treat Claim as fencing it.
func (d *Database) ClaimShardStoreServing(
	expected ShardStoreBinding,
	fence ShardStoreFence,
) (*ShardStoreServingClaim, error) {
	return d.claimShardStoreServing(expected, fence, nil)
}

// claimShardStoreServing exposes the catalog-publication boundary to focused
// package tests. Production callers always use ClaimShardStoreServing, whose
// nil hook selects persistCatalogLocked.
func (d *Database) claimShardStoreServing(
	expected ShardStoreBinding,
	fence ShardStoreFence,
	persist func(*database) (bool, error),
) (*ShardStoreServingClaim, error) {
	if err := validateShardStoreBinding(expected); err != nil {
		return nil, err
	}
	if err := validateShardStoreFence(fence); err != nil {
		return nil, err
	}
	expected = ownedShardStoreBinding(expected)
	if d == nil || d.connector == nil {
		return nil, ErrDatabaseClosed
	}
	d.connector.mu.Lock()
	if d.connector.closed || d.connector.db == nil {
		d.connector.mu.Unlock()
		return nil, ErrDatabaseClosed
	}
	core := d.connector.db
	d.connector.mu.Unlock()

	core.mu.Lock()
	defer core.mu.Unlock()
	if core.closed {
		return nil, ErrDatabaseClosed
	}
	if core.catalog.ReplicatedShardStore != nil {
		return nil, fmt.Errorf(
			"vibedb: claim local shard serving for replicated root %s: %w",
			core.path, ErrDirectWriteFenced,
		)
	}
	if core.catalog.ShardStore == nil {
		return nil, &ShardStoreError{
			Op: "claim serving", Path: core.path, Expected: expected,
			Err: ErrShardStoreUnbound,
		}
	}
	identity := *core.catalog.ShardStore
	if identity.Binding() != expected {
		return nil, &ShardStoreError{
			Op: "claim serving", Path: core.path, Expected: expected,
			Actual: identity, Err: ErrShardStoreIdentityMismatch,
		}
	}
	durableFence := ShardStoreFence{}
	if core.catalog.ShardStoreFence != nil {
		durableFence = *core.catalog.ShardStoreFence
	}
	if fence.OwnershipEpoch < durableFence.OwnershipEpoch {
		return nil, &ShardStoreFenceError{
			Op: "claim", Path: core.path, Requested: fence, Durable: durableFence,
			Err: distribution.ErrOwnershipEpoch,
		}
	}
	if fence.RoutingVersion < durableFence.RoutingVersion {
		return nil, &ShardStoreFenceError{
			Op: "claim", Path: core.path, Requested: fence, Durable: durableFence,
			Err: distribution.ErrRoutingVersion,
		}
	}
	if core.servingClaim != nil {
		return nil, fmt.Errorf(
			"vibedb: claim shard store serving for %s: %w",
			core.path, ErrShardStoreServingClaimed,
		)
	}

	// An equal retry may follow a publication whose rename completed but whose
	// directory fence failed. Settle every pending catalog/namespace phase before
	// treating the in-memory high-water as durable enough to serve.
	if err := core.settleCatalogLocked(); err != nil {
		return nil, fmt.Errorf("vibedb: settle shard store serving fence: %w", err)
	}
	if fence != durableFence {
		previousFence := core.catalog.ShardStoreFence
		previousPending := core.catalogWritePending
		claimedFence := fence
		core.catalog.ShardStoreFence = &claimedFence
		core.catalogWritePending = true

		var published bool
		var err error
		if persist != nil {
			published, err = persist(core)
		} else {
			published, err = core.persistCatalogLocked()
		}
		if err == nil && !published {
			err = errors.New("catalog persistence returned without publication")
		}
		if err != nil {
			if !published && !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
				// Nothing became externally visible, so restore the exact prior
				// catalog state. This also prevents terminal cleanup from silently
				// publishing a fence after Claim returned a definite failure.
				core.catalog.ShardStoreFence = previousFence
				core.catalogWritePending = previousPending
			} else if !published {
				// The hook could not prove whether publication happened. Keep the
				// proposed high-water and force a later settle/reopen to reconcile;
				// no serving claim is returned from this call.
				core.catalogWritePending = true
			}
			return nil, fmt.Errorf("vibedb: publish shard store serving fence: %w", err)
		}
	}

	claim := &ShardStoreServingClaim{
		database: core,
		identity: identity,
		fence:    fence,
	}
	core.servingClaim = claim
	return claim, nil
}

func validateShardStoreBinding(binding ShardStoreBinding) error {
	if binding.Distribution == "" || binding.Shard == "" || binding.AllocationGeneration == 0 {
		return fmt.Errorf(
			"vibedb: shard store binding requires a non-empty distribution and shard with a nonzero allocation generation",
		)
	}
	if !utf8.ValidString(string(binding.Distribution)) ||
		!utf8.ValidString(string(binding.Shard)) {
		return errors.New("vibedb: shard store binding names must be valid UTF-8")
	}
	if len(binding.Distribution) > maxCatalogTableNameBytes ||
		len(binding.Shard) > maxCatalogTableNameBytes {
		return fmt.Errorf(
			"vibedb: shard store binding names exceed the %d-byte catalog limit",
			maxCatalogTableNameBytes,
		)
	}
	return nil
}

func validateShardStoreIdentity(identity ShardStoreIdentity) error {
	if err := validateShardStoreBinding(identity.Binding()); err != nil {
		return err
	}
	if identity.LogID == ([16]byte{}) {
		return errors.New("vibedb: shard store identity has a zero log id")
	}
	return nil
}

func validateShardStoreFence(fence ShardStoreFence) error {
	if fence.OwnershipEpoch == 0 {
		return &ShardStoreFenceError{
			Op: "validate", Requested: fence,
			Err: distribution.ErrOwnershipEpoch,
		}
	}
	if fence.RoutingVersion == 0 {
		return &ShardStoreFenceError{
			Op: "validate", Requested: fence,
			Err: distribution.ErrRoutingVersion,
		}
	}
	return nil
}

func ownedShardStoreBinding(binding ShardStoreBinding) ShardStoreBinding {
	return ShardStoreBinding{
		Distribution: distribution.DistributionName(
			strings.Clone(string(binding.Distribution)),
		),
		Shard:                distribution.ShardID(strings.Clone(string(binding.Shard))),
		AllocationGeneration: binding.AllocationGeneration,
	}
}

func randomShardStoreIdentity(binding ShardStoreBinding) (ShardStoreIdentity, error) {
	binding = ownedShardStoreBinding(binding)
	identity := ShardStoreIdentity{
		Distribution:         binding.Distribution,
		Shard:                binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
	}
	for identity.LogID == ([16]byte{}) {
		if _, err := io.ReadFull(rand.Reader, identity.LogID[:]); err != nil {
			return ShardStoreIdentity{}, fmt.Errorf(
				"vibedb: generate shard store log id: %w", err,
			)
		}
	}
	return identity, nil
}

// MarshalJSON keeps the 128-bit log identity compact and canonical in the SQL
// catalog instead of encoding it as a sixteen-element JSON number array.
func (i ShardStoreIdentity) MarshalJSON() ([]byte, error) {
	if err := validateShardStoreIdentity(i); err != nil {
		return nil, err
	}
	encoded := shardStoreIdentityVibe(i)
	return vibejson.Marshal(&encoded)
}

// UnmarshalJSON is strict because this object is durable identity, not an
// extensible request payload. Missing, duplicate, and unknown members fail the
// same closed catalog boundary as the surrounding catalog.
func (i *ShardStoreIdentity) UnmarshalJSON(data []byte) error {
	if len(data) > maxShardStoreIdentityJSONBytes {
		return errors.New("vibedb: shard store identity exceeds its byte bound")
	}
	var decoded shardStoreIdentityVibe
	if err := vibejson.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*i = ShardStoreIdentity(decoded)
	return nil
}

// UnmarshalJSON keeps the mutable fence as strict as the immutable identity:
// missing, duplicate, unknown, null, negative, overflowing, and zero
// coordinates all fail the catalog boundary.
func (f ShardStoreFence) MarshalJSON() ([]byte, error) {
	if err := validateShardStoreFence(f); err != nil {
		return nil, err
	}
	encoded := shardStoreFenceVibe(f)
	return vibejson.Marshal(&encoded)
}

func (f *ShardStoreFence) UnmarshalJSON(data []byte) error {
	if len(data) > maxShardStoreFenceJSONBytes {
		return errors.New("vibedb: shard store fence exceeds its byte bound")
	}
	var decoded shardStoreFenceVibe
	if err := vibejson.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*f = ShardStoreFence(decoded)
	return nil
}
