package driver

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/distribution"
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

type shardStoreOpenMode uint8

const (
	shardStoreOpenGeneric shardStoreOpenMode = iota
	shardStoreOpenInitialize
	shardStoreOpenExisting
)

type shardStoreOpenPolicy struct {
	mode            shardStoreOpenMode
	expected        ShardStoreBinding
	persistIdentity func(*database) (bool, error)
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
	type encodedIdentity struct {
		Distribution         distribution.DistributionName          `json:"distribution"`
		Shard                distribution.ShardID                   `json:"shard"`
		AllocationGeneration distribution.ShardAllocationGeneration `json:"allocation_generation"`
		LogID                string                                 `json:"log_id"`
	}
	return json.Marshal(encodedIdentity{
		Distribution: i.Distribution, Shard: i.Shard,
		AllocationGeneration: i.AllocationGeneration,
		LogID:                hex.EncodeToString(i.LogID[:]),
	})
}

// UnmarshalJSON is strict because this object is durable identity, not an
// extensible request payload. Missing, duplicate, and unknown members fail the
// same closed catalog boundary as the surrounding catalog.
func (i *ShardStoreIdentity) UnmarshalJSON(data []byte) error {
	var decoded ShardStoreIdentity
	var distributionPresent, shardPresent, generationPresent, logIDPresent bool
	err := decodeCatalogObject(data, "shard store identity", func(
		name string,
		decoder *json.Decoder,
	) error {
		switch name {
		case "distribution":
			distributionPresent = true
			return decoder.Decode(&decoded.Distribution)
		case "shard":
			shardPresent = true
			return decoder.Decode(&decoded.Shard)
		case "allocation_generation":
			generationPresent = true
			return decoder.Decode(&decoded.AllocationGeneration)
		case "log_id":
			logIDPresent = true
			var encoded string
			if err := decoder.Decode(&encoded); err != nil {
				return err
			}
			if len(encoded) != hex.EncodedLen(len(decoded.LogID)) {
				return fmt.Errorf("vibedb: shard store log id must contain exactly 128 bits")
			}
			if encoded != strings.ToLower(encoded) {
				return fmt.Errorf("vibedb: shard store log id must use canonical lowercase hexadecimal")
			}
			if _, err := hex.Decode(decoded.LogID[:], []byte(encoded)); err != nil {
				return fmt.Errorf("vibedb: shard store log id is not hexadecimal: %w", err)
			}
			return nil
		default:
			return unknownCatalogMember("shard store identity", name)
		}
	})
	if err != nil {
		return err
	}
	for _, member := range []struct {
		name    string
		present bool
	}{
		{"distribution", distributionPresent},
		{"shard", shardPresent},
		{"allocation_generation", generationPresent},
		{"log_id", logIDPresent},
	} {
		if !member.present {
			return fmt.Errorf("vibedb: shard store identity is missing member %q", member.name)
		}
	}
	if err := validateShardStoreIdentity(decoded); err != nil {
		return err
	}
	*i = decoded
	return nil
}
