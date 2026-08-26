package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	MaxReplicatedIssuerLanes        uint16 = 1024
	MaxCachedReplicatedIssuerGrants        = 4096
	maxReplicatedIssuerGrantBytes          = 2 << 10
)

const replicatedIssuerGrantDomain = "vibedb/catalog/issuer-grant/1\x00"

// ReplicatedIssuerOpen identifies one stable client installation lane. The
// installation nonce is generated and persisted by the client; it is never a
// gateway-process identity. LaneOrdinal permits bounded parallel client lanes.
type ReplicatedIssuerOpen struct {
	Installation replication.ID128
	Epoch        uint64
	LaneOrdinal  uint16
}

// ReplicatedIssuerReference is the complete lookup capability carried by each
// request. It contains no secret and is useful only with the authenticated
// connection principal and tenant binding recorded in the immutable grant.
type ReplicatedIssuerReference struct {
	Installation replication.ID128
	Epoch        uint64
	LaneOrdinal  uint16
	GrantDigest  replication.Digest
}

// ReplicatedIssuerTenantResolver maps the exact authenticated connection
// authority to its immutable tenant scope. Request bytes never supply either
// the principal or tenant binding.
type ReplicatedIssuerTenantResolver interface {
	ResolveIssuerTenant(context.Context, serviceauthz.Authority) (requestledger.ScopeKind, requestledger.Digest, error)
}

// ReplicatedIssuerLaneGrant is the immutable cluster-visible issuer contract.
// Every gateway reopens this record through a linearizable catalog RF3 read.
type ReplicatedIssuerLaneGrant struct {
	Installation replication.ID128
	Epoch        uint64
	LaneOrdinal  uint16
	Lane         requestledger.IssuerLane
	Scope        requestledger.ScopeKind
	Principal    requestledger.PrincipalID
	TenantDigest requestledger.Digest
	Revision     uint64
	GrantDigest  replication.Digest
}

type persistedReplicatedIssuerGrant struct {
	Epoch        uint64                    `json:"epoch"`
	GrantDigest  replication.Digest        `json:"grant_digest"`
	Installation replication.ID128         `json:"installation"`
	Lane         requestledger.IssuerLane  `json:"lane"`
	LaneOrdinal  uint16                    `json:"lane_ordinal"`
	Principal    requestledger.PrincipalID `json:"principal"`
	Revision     uint64                    `json:"revision"`
	Scope        uint8                     `json:"scope"`
	TenantDigest requestledger.Digest      `json:"tenant_digest"`
}

func validReplicatedIssuerOpen(open ReplicatedIssuerOpen) bool {
	return open.Installation != (replication.ID128{}) &&
		open.Epoch != 0 && open.LaneOrdinal < MaxReplicatedIssuerLanes
}

func replicatedIssuerGrantFor(open ReplicatedIssuerOpen, scope requestledger.ScopeKind,
	principal requestledger.PrincipalID, tenant requestledger.Digest,
) (ReplicatedIssuerLaneGrant, error) {
	if !validReplicatedIssuerOpen(open) || scope != requestledger.ScopeAuthenticated ||
		principal == (requestledger.PrincipalID{}) || tenant == (requestledger.Digest{}) {
		return ReplicatedIssuerLaneGrant{}, ErrReplicatedCatalog
	}
	grant := ReplicatedIssuerLaneGrant{
		Installation: open.Installation, Epoch: open.Epoch, LaneOrdinal: open.LaneOrdinal,
		Scope: scope, Principal: principal, TenantDigest: tenant, Revision: 1,
	}
	var framed [len(replicatedIssuerGrantDomain) + 16 + 8 + 2 + 1 + 16 + 32]byte
	at := copy(framed[:], replicatedIssuerGrantDomain)
	at += copy(framed[at:], open.Installation[:])
	binary.LittleEndian.PutUint64(framed[at:at+8], open.Epoch)
	at += 8
	binary.LittleEndian.PutUint16(framed[at:at+2], open.LaneOrdinal)
	at += 2
	framed[at] = byte(scope)
	at++
	at += copy(framed[at:], principal[:])
	copy(framed[at:], tenant[:])
	laneMaterial := sha256.Sum256(framed[:])
	copy(grant.Lane[:], laneMaterial[:len(grant.Lane)])
	grant.GrantDigest = replicatedIssuerGrantDigest(grant)
	if !validReplicatedIssuerGrant(grant) {
		return ReplicatedIssuerLaneGrant{}, ErrReplicatedCatalog
	}
	return grant, nil
}

func replicatedIssuerGrantDigest(grant ReplicatedIssuerLaneGrant) replication.Digest {
	var framed [len(replicatedIssuerGrantDomain) + 16 + 8 + 2 + 8 + 1 + 16 + 32]byte
	at := copy(framed[:], replicatedIssuerGrantDomain)
	at += copy(framed[at:], grant.Installation[:])
	binary.LittleEndian.PutUint64(framed[at:at+8], grant.Epoch)
	at += 8
	binary.LittleEndian.PutUint16(framed[at:at+2], grant.LaneOrdinal)
	at += 2
	at += copy(framed[at:], grant.Lane[:])
	framed[at] = byte(grant.Scope)
	at++
	at += copy(framed[at:], grant.Principal[:])
	copy(framed[at:], grant.TenantDigest[:])
	return sha256.Sum256(framed[:])
}

func validReplicatedIssuerGrant(grant ReplicatedIssuerLaneGrant) bool {
	return grant.Installation != (replication.ID128{}) && grant.Epoch != 0 &&
		grant.LaneOrdinal < MaxReplicatedIssuerLanes && grant.Lane != (requestledger.IssuerLane{}) &&
		(grant.Scope == requestledger.ScopeAuthenticated || grant.Scope == requestledger.ScopeLocalInstall) &&
		grant.Principal != (requestledger.PrincipalID{}) &&
		grant.TenantDigest != (requestledger.Digest{}) && grant.Revision == 1 &&
		grant.GrantDigest != (replication.Digest{}) &&
		grant.GrantDigest == replicatedIssuerGrantDigest(grant)
}

func replicatedIssuerGrantDocumentID(
	installation replication.ID128,
	epoch uint64,
	lane uint16,
) [65]byte {
	var id [65]byte
	at := copy(id[:], []byte("issuer/grant/"))
	for _, value := range installation {
		id[at], id[at+1] = lowerHex[value>>4], lowerHex[value&0x0f]
		at += 2
	}
	for shift := 60; shift >= 0; shift -= 4 {
		id[at] = lowerHex[byte(epoch>>shift)&0x0f]
		at++
	}
	for shift := 12; shift >= 0; shift -= 4 {
		id[at] = lowerHex[byte(lane>>shift)&0x0f]
		at++
	}
	return id
}

func replicatedIssuerGrantKey(grant ReplicatedIssuerLaneGrant) []byte {
	id := replicatedIssuerGrantDocumentID(grant.Installation, grant.Epoch, grant.LaneOrdinal)
	return fixedControlPlaneKey(id[:])
}

func appendReplicatedIssuerGrant(
	dst []byte,
	grant ReplicatedIssuerLaneGrant,
) ([]byte, error) {
	start := len(dst)
	if !validReplicatedIssuerGrant(grant) {
		return dst, ErrReplicatedCatalog
	}
	payload, err := vibejson.Marshal(&persistedReplicatedIssuerGrant{
		Epoch: grant.Epoch, GrantDigest: grant.GrantDigest, Installation: grant.Installation,
		Lane: grant.Lane, LaneOrdinal: grant.LaneOrdinal, Principal: grant.Principal,
		Revision: grant.Revision, Scope: uint8(grant.Scope), TenantDigest: grant.TenantDigest,
	})
	if err != nil {
		return dst, err
	}
	id := replicatedIssuerGrantDocumentID(grant.Installation, grant.Epoch, grant.LaneOrdinal)
	dst, err = appendControlPlaneDocument(dst, id[:], payload, maxReplicatedIssuerGrantBytes)
	if err != nil {
		return dst[:start], err
	}
	return dst, nil
}

func openReplicatedIssuerGrant(raw []byte,
	installation replication.ID128,
	epoch uint64,
	lane uint16,
) (ReplicatedIssuerLaneGrant, error) {
	if len(raw) == 0 || len(raw) > maxReplicatedIssuerGrantBytes {
		return ReplicatedIssuerLaneGrant{}, ErrReplicatedCatalog
	}
	id := replicatedIssuerGrantDocumentID(installation, epoch, lane)
	payload, err := openTypedControlPlaneDocument(raw, id[:], maxReplicatedIssuerGrantBytes)
	if err != nil {
		return ReplicatedIssuerLaneGrant{}, err
	}
	var stored persistedReplicatedIssuerGrant
	if err = vibejson.Unmarshal(payload, &stored); err != nil {
		return ReplicatedIssuerLaneGrant{}, errors.Join(err, ErrReplicatedCatalog)
	}
	grant := ReplicatedIssuerLaneGrant{Epoch: stored.Epoch, LaneOrdinal: stored.LaneOrdinal,
		Scope: requestledger.ScopeKind(stored.Scope), Revision: stored.Revision}
	grant.Installation, grant.Lane = stored.Installation, stored.Lane
	grant.Principal, grant.TenantDigest = stored.Principal, stored.TenantDigest
	grant.GrantDigest = stored.GrantDigest
	if grant.Installation != installation || grant.Epoch != epoch || grant.LaneOrdinal != lane ||
		!validReplicatedIssuerGrant(grant) {
		return ReplicatedIssuerLaneGrant{}, ErrReplicatedCatalogConflict
	}
	canonical, err := appendReplicatedIssuerGrant(nil, grant)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ReplicatedIssuerLaneGrant{}, errors.Join(err, ErrReplicatedCatalogConflict)
	}
	return grant, nil
}

// OpenIssuerLaneGrant performs an idempotent cluster-wide open. Concurrent
// gateways propose the same immutable bytes; a copied local store grants no
// authority because validation always reopens this RF3 record.
func (authority *ReplicatedCatalogAuthority) OpenIssuerLaneGrant(
	ctx context.Context,
	authenticated serviceauthz.Authority,
	tenants ReplicatedIssuerTenantResolver,
	open ReplicatedIssuerOpen,
) (ReplicatedIssuerLaneGrant, error) {
	if authority == nil || authority.session == nil || ctx == nil ||
		!authenticated.Valid() || tenants == nil {
		return ReplicatedIssuerLaneGrant{}, ErrReplicatedCatalog
	}
	scope, tenant, err := tenants.ResolveIssuerTenant(ctx, authenticated)
	if err != nil {
		return ReplicatedIssuerLaneGrant{}, errors.Join(err, ErrReplicatedCatalog)
	}
	grant, err := replicatedIssuerGrantFor(open, scope,
		requestledger.PrincipalID(authenticated.Node), tenant)
	if err != nil {
		return ReplicatedIssuerLaneGrant{}, err
	}
	ctx, err = authority.authorizedContext(ctx)
	if err != nil {
		return ReplicatedIssuerLaneGrant{}, err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.session.Status().Pending {
		return ReplicatedIssuerLaneGrant{}, ErrReplicatedCatalogPending
	}
	key := replicatedIssuerGrantKey(grant)
	current, err := authority.readRaw(ctx, key, maxReplicatedIssuerGrantBytes)
	if err != nil {
		return ReplicatedIssuerLaneGrant{}, err
	}
	if current.Found {
		stored, openErr := openReplicatedIssuerGrant(current.Value, grant.Installation, grant.Epoch, grant.LaneOrdinal)
		if openErr != nil || stored != grant {
			return ReplicatedIssuerLaneGrant{}, errors.Join(openErr, ErrReplicatedCatalogConflict)
		}
		authority.issuerGrants.put(stored)
		return stored, nil
	}
	record, err := appendReplicatedIssuerGrant(nil, grant)
	if err != nil {
		return ReplicatedIssuerLaneGrant{}, err
	}
	result, err := authority.session.PutIfAbsentOrEqual(ctx, key, record)
	if err != nil {
		if authority.session.Status().Pending {
			return ReplicatedIssuerLaneGrant{}, errors.Join(ErrReplicatedCatalogPending, err)
		}
		return ReplicatedIssuerLaneGrant{}, err
	}
	if result.Completion.ResultCode != replicatedstate.ResultApplied {
		return ReplicatedIssuerLaneGrant{}, ErrReplicatedCatalogConflict
	}
	// A successful proposal proves commit, but the open response is also the
	// cache-populating linearizable read contract used after outcome-unknown
	// responses and on replacement gateways.
	stored, found, readErr := authority.ReadIssuerLaneGrant(ctx, grant.Installation,
		grant.Epoch, grant.LaneOrdinal)
	if readErr != nil || !found || stored != grant {
		return ReplicatedIssuerLaneGrant{}, errors.Join(readErr, ErrReplicatedCatalogConflict)
	}
	return stored, nil
}

// ReadIssuerLaneGrant is the linearizable validation path used on every
// gateway. found=false is authoritative replicated absence, never permission
// to reconstruct a grant from local state.
func (authority *ReplicatedCatalogAuthority) ReadIssuerLaneGrant(
	ctx context.Context,
	installation replication.ID128,
	epoch uint64,
	lane uint16,
) (ReplicatedIssuerLaneGrant, bool, error) {
	if authority == nil || ctx == nil || installation == (replication.ID128{}) ||
		epoch == 0 || lane >= MaxReplicatedIssuerLanes {
		return ReplicatedIssuerLaneGrant{}, false, ErrReplicatedCatalog
	}
	probe := ReplicatedIssuerLaneGrant{Installation: installation, Epoch: epoch, LaneOrdinal: lane}
	key := replicatedIssuerGrantKey(probe)
	result, err := authority.readRaw(ctx, key, maxReplicatedIssuerGrantBytes)
	if err != nil || !result.Found {
		return ReplicatedIssuerLaneGrant{}, false, err
	}
	grant, err := openReplicatedIssuerGrant(result.Value, installation, epoch, lane)
	if err != nil {
		return ReplicatedIssuerLaneGrant{}, false, err
	}
	authority.issuerGrants.put(grant)
	return grant, true, nil
}

func (authority *ReplicatedCatalogAuthority) ValidateIssuerRequestKey(
	ctx context.Context,
	authenticated serviceauthz.Authority,
	tenants ReplicatedIssuerTenantResolver,
	reference ReplicatedIssuerReference,
	request requestledger.RequestID,
	sequence uint64,
) (requestledger.RequestKey, error) {
	if authority == nil || ctx == nil || !authenticated.Valid() || tenants == nil ||
		!validReplicatedIssuerReference(reference) ||
		request == (requestledger.RequestID{}) || sequence == 0 {
		return requestledger.RequestKey{}, ErrReplicatedCatalog
	}
	grant, found := authority.issuerGrants.get(reference)
	if !found {
		var err error
		grant, found, err = authority.ReadIssuerLaneGrant(ctx, reference.Installation,
			reference.Epoch, reference.LaneOrdinal)
		if err != nil || !found {
			return requestledger.RequestKey{}, errors.Join(err, ErrReplicatedCatalogConflict)
		}
	}
	scope, tenant, err := tenants.ResolveIssuerTenant(ctx, authenticated)
	if err != nil || scope != grant.Scope || tenant != grant.TenantDigest ||
		requestledger.PrincipalID(authenticated.Node) != grant.Principal ||
		grant.GrantDigest != reference.GrantDigest {
		return requestledger.RequestKey{}, errors.Join(err, ErrReplicatedCatalogConflict)
	}
	key := requestledger.RequestKey{
		Scope: grant.Scope, Principal: grant.Principal, Request: request,
		TenantDigest: grant.TenantDigest, IssuerEpoch: grant.Epoch,
		IssuerSequence: sequence, IssuerLane: grant.Lane,
	}
	if !key.Valid() {
		return requestledger.RequestKey{}, ErrReplicatedCatalogConflict
	}
	return key, nil
}

func validReplicatedIssuerReference(reference ReplicatedIssuerReference) bool {
	return reference.Installation != (replication.ID128{}) && reference.Epoch != 0 &&
		reference.LaneOrdinal < MaxReplicatedIssuerLanes &&
		reference.GrantDigest != (replication.Digest{})
}

type replicatedIssuerGrantCacheKey struct {
	Installation replication.ID128
	Epoch        uint64
	LaneOrdinal  uint16
	GrantDigest  replication.Digest
}

type replicatedIssuerGrantCache struct {
	mu       sync.RWMutex
	entries  map[replicatedIssuerGrantCacheKey]ReplicatedIssuerLaneGrant
	order    []replicatedIssuerGrantCacheKey
	next     int
	capacity int
}

func newReplicatedIssuerGrantCache(capacity int) *replicatedIssuerGrantCache {
	if capacity <= 0 || capacity > MaxCachedReplicatedIssuerGrants {
		capacity = MaxCachedReplicatedIssuerGrants
	}
	return &replicatedIssuerGrantCache{entries: make(map[replicatedIssuerGrantCacheKey]ReplicatedIssuerLaneGrant, capacity),
		order: make([]replicatedIssuerGrantCacheKey, 0, capacity), capacity: capacity}
}

func replicatedIssuerCacheKey(grant ReplicatedIssuerLaneGrant) replicatedIssuerGrantCacheKey {
	return replicatedIssuerGrantCacheKey{Installation: grant.Installation, Epoch: grant.Epoch,
		LaneOrdinal: grant.LaneOrdinal, GrantDigest: grant.GrantDigest}
}

func (cache *replicatedIssuerGrantCache) put(grant ReplicatedIssuerLaneGrant) {
	if cache == nil || !validReplicatedIssuerGrant(grant) {
		return
	}
	key := replicatedIssuerCacheKey(grant)
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, exists := cache.entries[key]; exists {
		return
	}
	if len(cache.order) < cache.capacity {
		cache.order = append(cache.order, key)
	} else {
		delete(cache.entries, cache.order[cache.next])
		cache.order[cache.next] = key
		cache.next++
		if cache.next == cache.capacity {
			cache.next = 0
		}
	}
	cache.entries[key] = grant
}

func (cache *replicatedIssuerGrantCache) get(reference ReplicatedIssuerReference) (ReplicatedIssuerLaneGrant, bool) {
	if cache == nil || !validReplicatedIssuerReference(reference) {
		return ReplicatedIssuerLaneGrant{}, false
	}
	key := replicatedIssuerGrantCacheKey{Installation: reference.Installation, Epoch: reference.Epoch,
		LaneOrdinal: reference.LaneOrdinal, GrantDigest: reference.GrantDigest}
	cache.mu.RLock()
	grant, found := cache.entries[key]
	cache.mu.RUnlock()
	return grant, found
}
