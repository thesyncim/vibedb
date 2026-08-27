package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"

	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	vibejson "github.com/thesyncim/vibejson"
)

var (
	ErrReplicatedRestoreCatalog = errors.New("gateway: replicated restore catalog")
	ErrRestoreCatalogMissing    = errors.New("gateway: restore activation is missing")
	ErrRestoreCatalogConflict   = errors.New("gateway: restore activation conflicts")
)

// RestoreCatalogActivationKeyMatches identifies the sole restore-activation
// catalog row without exposing mutable shared key storage.
func RestoreCatalogActivationKeyMatches(key []byte) bool {
	return bytes.Equal(key, restoreCatalogDocumentKey)
}

// RestoreCatalogActivationDocumentMatches is the shipped shard's narrow
// pre-activation validator. It accepts only the canonical one-time witness for
// this exact restored operation and target catalog group.
func RestoreCatalogActivationDocumentMatches(raw []byte, operation [sha256.Size]byte,
	group raftmember.GroupKey,
) bool {
	witness, err := openRestoreCatalogDocument(raw)
	return err == nil && operation != ([sha256.Size]byte{}) &&
		witness.Operation == operation && witness.CatalogGroup == restoreCatalogGroup(group)
}

const (
	restoreCatalogDocumentFormat   = 1
	maxRestoreCatalogDocumentBytes = 1024
)

var (
	restoreCatalogDocumentID = [...]byte{
		'r', 'e', 's', 't', 'o', 'r', 'e', '/',
		'a', 'c', 't', 'i', 'v', 'a', 't', 'i', 'o', 'n',
	}
	restoreCatalogDocumentKey = fixedControlPlaneKey(restoreCatalogDocumentID[:])
)

type persistedRestoreCatalogActivation struct {
	Command []byte `json:"command"`
	Format  uint64 `json:"format"`
}

type ReplicatedRestoreCatalogOptions struct {
	Catalog  *ReplicatedCatalogAuthority
	Session  *NativeSession
	Gate     *serviceauthz.Gate
	Operator serviceauthz.Authority
}

// ReplicatedRestoreCatalog is the one-way fresh-target activation boundary.
// The externally visible authority is restore-only. Its independent internal
// catalog session is a narrow implementation capability over the exact same
// RF3 route and relation; callers never receive or borrow topology authority.
type ReplicatedRestoreCatalog struct {
	catalog  *ReplicatedCatalogAuthority
	session  *NativeSession
	gate     *serviceauthz.Gate
	operator serviceauthz.Authority
	mu       sync.Mutex
}

func NewReplicatedRestoreCatalog(options ReplicatedRestoreCatalogOptions) (*ReplicatedRestoreCatalog, error) {
	catalog, session := options.Catalog, options.Session
	if catalog == nil || catalog.executor == nil || catalog.session == nil || session == nil ||
		session == catalog.session || options.Gate == nil || !options.Operator.Valid() ||
		options.Gate.CheckAuthority(options.Operator,
			serviceauthz.CapabilityRestoreActivate) != serviceauthz.DecisionAllow ||
		session.executor != catalog.executor || session.phase != nativeSessionActive || session.pending ||
		session.proposalCapability != serviceauthz.CapabilityTopology ||
		session.distribution != string(ReplicatedCatalogDistribution) ||
		session.shard != string(ReplicatedCatalogShard) ||
		!sameReplicatedCatalogRoute(session.route, catalog.route) ||
		nativeSessionBaseRelation(session) != catalog.relation ||
		session.clientID == catalog.session.clientID {
		return nil, ErrReplicatedRestoreCatalog
	}
	return &ReplicatedRestoreCatalog{catalog: catalog, session: session,
		gate: options.Gate, operator: options.Operator}, nil
}

func (catalog *ReplicatedRestoreCatalog) authorized(ctx context.Context) (context.Context, error) {
	if catalog == nil || ctx == nil || catalog.gate == nil || !catalog.operator.Valid() {
		return nil, ErrReplicatedRestoreCatalog
	}
	caller, ok := serviceauthz.FromContext(ctx)
	if !ok || caller != catalog.operator || catalog.gate.CheckAuthority(
		caller, serviceauthz.CapabilityRestoreActivate,
	) != serviceauthz.DecisionAllow {
		return nil, ErrReplicatedRestoreCatalog
	}
	bound, err := catalog.catalog.authorizedContext(ctx)
	if err != nil {
		return nil, errors.Join(ErrReplicatedRestoreCatalog, err)
	}
	return bound, nil
}

// ProposeRestoreActivation implements clusterrestore.CatalogProposer. One
// fixed catalog row makes activation cluster-wide and one-time: exact replay
// returns the same digest, while every different operation conflicts.
func (catalog *ReplicatedRestoreCatalog) ProposeRestoreActivation(
	ctx context.Context, command []byte,
) ([]byte, error) {
	bound, err := catalog.authorized(ctx)
	if err != nil {
		return nil, err
	}
	witness, err := clusterrestore.OpenCatalogActivation(command)
	if err != nil || !validRestoreCatalogWitness(witness) ||
		witness.CatalogGroup != restoreCatalogGroup(catalog.catalog.route.Group) {
		return nil, errors.Join(ErrReplicatedRestoreCatalog, err)
	}
	document, err := appendRestoreCatalogDocument(nil, command)
	if err != nil {
		return nil, err
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	catalog.catalog.mu.Lock()
	defer catalog.catalog.mu.Unlock()
	if err = catalog.catalog.requireRouteSeedServingLocked(); err != nil {
		return nil, err
	}

	if catalog.session.Status().Pending {
		_, retryErr := catalog.session.RetryPending(bound)
		observed, observeErr := catalog.observeLocked(bound)
		if observeErr == nil {
			return settleRestoreCatalogWitness(observed, witness)
		}
		if retryErr != nil {
			return nil, errors.Join(ErrReplicatedRestoreCatalog, retryErr, observeErr)
		}
	}
	observed, observeErr := catalog.observeLocked(bound)
	if observeErr == nil {
		return settleRestoreCatalogWitness(observed, witness)
	}
	if !errors.Is(observeErr, ErrRestoreCatalogMissing) {
		return nil, observeErr
	}
	result, putErr := catalog.session.PutIfAbsentOrEqual(
		bound, restoreCatalogDocumentKey, document,
	)
	if putErr == nil && result.Completion.ResultCode != replicatedstate.ResultApplied {
		putErr = ErrRestoreCatalogConflict
	}
	observed, observeErr = catalog.observeLocked(bound)
	if observeErr == nil {
		return settleRestoreCatalogWitness(observed, witness)
	}
	return nil, errors.Join(ErrReplicatedRestoreCatalog, putErr, observeErr)
}

// ObserveRestoreActivation performs a separate leader ReadIndex point read.
// It never returns proposal-local state or a locally persisted serving permit.
func (catalog *ReplicatedRestoreCatalog) ObserveRestoreActivation(
	ctx context.Context, operation [sha256.Size]byte,
) (clusterrestore.CatalogWitness, error) {
	if operation == ([sha256.Size]byte{}) {
		return clusterrestore.CatalogWitness{}, ErrReplicatedRestoreCatalog
	}
	bound, err := catalog.authorized(ctx)
	if err != nil {
		return clusterrestore.CatalogWitness{}, err
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	catalog.catalog.mu.Lock()
	defer catalog.catalog.mu.Unlock()
	if err = catalog.catalog.requireRouteSeedServingLocked(); err != nil {
		return clusterrestore.CatalogWitness{}, err
	}
	witness, err := catalog.observeLocked(bound)
	if err != nil || witness.Operation != operation {
		return clusterrestore.CatalogWitness{}, errors.Join(
			ErrReplicatedRestoreCatalog, ErrRestoreCatalogConflict, err,
		)
	}
	return witness, nil
}

func (catalog *ReplicatedRestoreCatalog) observeLocked(
	ctx context.Context,
) (clusterrestore.CatalogWitness, error) {
	result, err := catalog.catalog.readRaw(
		ctx, restoreCatalogDocumentKey, maxRestoreCatalogDocumentBytes,
	)
	if err != nil {
		return clusterrestore.CatalogWitness{}, err
	}
	if !result.Found {
		return clusterrestore.CatalogWitness{}, ErrRestoreCatalogMissing
	}
	return openRestoreCatalogDocument(result.Value)
}

func appendRestoreCatalogDocument(dst, command []byte) ([]byte, error) {
	witness, err := clusterrestore.OpenCatalogActivation(command)
	if err != nil || !validRestoreCatalogWitness(witness) {
		return dst, errors.Join(ErrReplicatedRestoreCatalog, err)
	}
	payload, err := vibejson.Marshal(&persistedRestoreCatalogActivation{
		Command: command, Format: restoreCatalogDocumentFormat,
	})
	if err != nil {
		return dst, errors.Join(ErrReplicatedRestoreCatalog, err)
	}
	encoded, err := appendControlPlaneDocument(
		dst, restoreCatalogDocumentID[:], payload, maxRestoreCatalogDocumentBytes,
	)
	if err != nil {
		return dst, errors.Join(ErrReplicatedRestoreCatalog, err)
	}
	return encoded, nil
}

func openRestoreCatalogDocument(raw []byte) (clusterrestore.CatalogWitness, error) {
	payload, err := openTypedControlPlaneDocument(
		raw, restoreCatalogDocumentID[:], maxRestoreCatalogDocumentBytes,
	)
	if err != nil {
		return clusterrestore.CatalogWitness{}, errors.Join(ErrReplicatedRestoreCatalog, err)
	}
	var persisted persistedRestoreCatalogActivation
	if err = vibejson.Unmarshal(payload, &persisted); err != nil ||
		persisted.Format != restoreCatalogDocumentFormat {
		return clusterrestore.CatalogWitness{}, errors.Join(ErrReplicatedRestoreCatalog, err)
	}
	canonical, err := appendRestoreCatalogDocument(nil, persisted.Command)
	if err != nil || !bytes.Equal(canonical, raw) {
		return clusterrestore.CatalogWitness{}, errors.Join(ErrReplicatedRestoreCatalog, err)
	}
	witness, err := clusterrestore.OpenCatalogActivation(persisted.Command)
	if err != nil || !validRestoreCatalogWitness(witness) {
		return clusterrestore.CatalogWitness{}, errors.Join(ErrReplicatedRestoreCatalog, err)
	}
	return witness, nil
}

func settleRestoreCatalogWitness(
	observed, requested clusterrestore.CatalogWitness,
) ([]byte, error) {
	if observed != requested {
		return nil, errors.Join(ErrReplicatedRestoreCatalog, ErrRestoreCatalogConflict)
	}
	return append([]byte(nil), observed.CatalogDigest[:]...), nil
}

func validRestoreCatalogWitness(witness clusterrestore.CatalogWitness) bool {
	if witness.Operation == ([sha256.Size]byte{}) || witness.CatalogGroup == ([72]byte{}) ||
		witness.GroupsDigest == ([sha256.Size]byte{}) ||
		witness.TargetPolicyDigest == ([sha256.Size]byte{}) ||
		witness.TargetCatalogDigest == ([sha256.Size]byte{}) ||
		witness.CatalogDigest == ([sha256.Size]byte{}) {
		return false
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/restore/catalog-witness/format-1\x00"))
	_, _ = hash.Write(witness.Operation[:])
	_, _ = hash.Write(witness.CatalogGroup[:])
	_, _ = hash.Write(witness.GroupsDigest[:])
	_, _ = hash.Write(witness.TargetPolicyDigest[:])
	_, _ = hash.Write(witness.TargetCatalogDigest[:])
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest == witness.CatalogDigest
}

func restoreCatalogGroup(group raftmember.GroupKey) (encoded [72]byte) {
	copy(encoded[:16], group.ClusterID[:])
	copy(encoded[16:32], group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(encoded[32:40], group.TopologyRecoveryEpoch)
	copy(encoded[40:56], group.ShardIncarnation[:])
	copy(encoded[56:72], group.GroupID[:])
	return encoded
}
