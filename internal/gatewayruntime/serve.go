package gatewayruntime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/serviceerrors"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
	vibejson "github.com/thesyncim/vibejson"
)

// maxServeRequestBytes bounds one newline-delimited JSON envelope before JSON
// decoding or SQL parsing. Scanner grows only for an actually large request and
// releases the buffer with the connection.
const (
	maxServeRequestBytes              = 1 << 20
	defaultNativeResponseWriteTimeout = 5 * time.Second
)

// The serve subcommand is a routing front-end. It loads an immutable
// catalog generation, refreshes the atomically replaced catalog file after a
// shard reports stale routing metadata, and accepts newline-delimited JSON
// requests over a connection. Each request routes and dispatches against the
// pinned generation: a bounded distributed read by default or one
// single-base-owner write for exec. Independently placed index maintenance may
// add transaction participants behind that exec. Authenticated RF3 serving can
// additionally install an atomic fixed-participant exec_batch service. The
// reply is the merged result. The wire form is a minimal JSON envelope; a request
// carries SQL, typed parameters, and an operational class. The static command
// exposes only single-base-owner statements through exec. Authenticated RF3
// mode also installs the durable exec_batch service; static serving never
// routes public exec_batch into the general library batch API. The pinned
// catalog and shared SQL planner derive placement, shard constraints, merge
// order, and the global limit. The envelope itself is decoded and emitted with
// vibejson.

// serveRequest is one query envelope a client sends. SQL and its typed
// parameters are the only semantic inputs; clients cannot override routing or
// merge metadata independently of the statement.
type serveRequest struct {
	// Op selects the gateway operation: the empty value and "query" are the
	// read path; "read_batch" is the RF3 exact-point SQL vector; "exec" is the
	// single-base-owner write path; authenticated durable RF3 "exec_batch" uses
	// Statements and applies one Class to the complete atomic batch.
	Op string `json:"op,omitempty"`
	// RequestID is the caller's fixed 128-bit hexadecimal idempotency key for
	// an RF3 exec_batch. It remains ingress metadata and never enters Raft as a
	// table or SQL string.
	RequestID      string `json:"request_id,omitempty"`
	InstallationID string `json:"installation_id,omitempty"`
	IssuerEpoch    uint64 `json:"issuer_epoch,omitempty"`
	LaneOrdinal    uint16 `json:"lane_ordinal,omitempty"`
	GrantDigest    string `json:"grant_digest,omitempty"`
	IssuerSequence uint64 `json:"issuer_sequence,omitempty"`
	// Legacy fields remain decode-only; the strict raw exec_batch decoder rejects
	// them and public dispatch has no unsequenced fallback.
	IssuerLane          string `json:"issuer_lane,omitempty"`
	IssuerAuthenticator string `json:"issuer_authenticator,omitempty"`
	SQL                 string `json:"sql"`
	Class               string `json:"class,omitempty"`
	// MaxResultBytes is required by read_batch and bounds its complete JSON
	// success response, including documents and the per-group observation vector.
	MaxResultBytes uint32           `json:"max_result_bytes,omitempty"`
	BackupID       string           `json:"backup_id,omitempty"`
	Params         []serveParam     `json:"params,omitempty"`
	Statements     []serveStatement `json:"statements,omitempty"`

	// wireIdentity is populated directly into fixed storage by the compiled
	// ingress decoder. It avoids materializing three hexadecimal identities as
	// strings and avoids decoding a structured exec_batch twice.
	wireIdentity    durableExecBatchIdentity
	wireIdentitySet bool
	wireSQL         []byte
}

type serveStatement struct {
	SQL     string       `json:"sql"`
	Params  []serveParam `json:"params,omitempty"`
	wireSQL []byte
}

// serveParam is one typed bound parameter in placeholder order.
type serveParam struct {
	Kind     string `json:"kind"`
	Bool     bool   `json:"bool,omitempty"`
	Text     string `json:"text,omitempty"`
	wireKind serveParamKind
	wireText []byte
}

// serveResponse is the merged reply plus the routing metadata a client reads for
// observability. Rows carries each cell as raw JSON (a null cell is the JSON
// literal null); Error is set instead when the operation failed.
type serveResponse struct {
	Kind               string                          `json:"kind,omitempty"`
	Columns            []string                        `json:"columns,omitempty"`
	Rows               [][]serveRawValue               `json:"rows,omitempty"`
	RowsAffected       int64                           `json:"rows_affected,omitempty"`
	Route              string                          `json:"route,omitempty"`
	Generation         uint64                          `json:"generation,omitempty"`
	ShardsFanned       int                             `json:"shards_fanned,omitempty"`
	Retries            int                             `json:"retries,omitempty"`
	TransactionID      replication.ID128               `json:"-"`
	Committed          bool                            `json:"committed,omitempty"`
	OutcomeUnknown     bool                            `json:"outcome_unknown,omitempty"`
	DurableAck         *durableExecBatchAckWireRequest `json:"-"`
	Metrics            *gateway.MetricsSnapshot        `json:"metrics,omitempty"`
	DistributedMetrics *gateway.DistributedMetrics     `json:"-"`
	ControllerMetrics  *gatewayControllerMetrics       `json:"-"`
	BackupID           [32]byte                        `json:"-"`
	BackupStage        uint64                          `json:"backup_stage,omitempty"`
	BackupProof        [32]byte                        `json:"-"`
	Error              string                          `json:"error,omitempty"`
}

var errServeResponseTransactionState = errors.New(
	"vibedb-gateway: invalid transaction response state",
)

// serveRawValue is one already-encoded JSON cell. The methods preserve test and
// client interoperability with encoding/json without using it in the server;
// production output writes the bytes directly through vibejson.Writer.
type serveRawValue []byte

func (r serveRawValue) MarshalJSON() ([]byte, error) { return r, nil }

func (r *serveRawValue) UnmarshalJSON(src []byte) error {
	*r = append((*r)[:0], src...)
	return nil
}

type repeatedFlag []string

func (values *repeatedFlag) String() string { return strings.Join(*values, ",") }
func (values *repeatedFlag) Set(value string) error {
	if value == "" || len(*values) >= servicetls.AbsoluteMaxIdentities {
		return servicetls.ErrInvalidProfile
	}
	*values = append(*values, value)
	return nil
}

func completeReplicatedCatalogRouteSeedHandoff(
	control *gateway.ReplicatedCatalogRouteSeedControl,
	attempts int,
	attemptTimeout time.Duration,
) (bool, error) {
	if control == nil {
		return false, nil
	}
	select {
	case <-control.ShutdownRequired():
	default:
		return false, nil
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), replicatedCatalogRouteHandoffTimeout(attempts, attemptTimeout),
	)
	defer cancel()
	return true, control.CompleteQuiescedHandoff(ctx)
}

func nonCanceledError(err error, shutdownCause ...error) error {
	return serviceerrors.Without(err, append(shutdownCause, context.Canceled)...)
}

// requireLoopbackListen keeps the explicitly selected unauthenticated
// development protocol from becoming a remotely reachable query endpoint.
func requireLoopbackListen(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("listen address %q is invalid: %w", address, err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("listen address %q must be loopback; remote unauthenticated serving is refused", address)
	}
	return nil
}

// newGateway loads the initial catalog generation and returns an executor that
// dispatches leader-only strong reads over the default TCP client. A stale
// shard refusal reloads the same crash-safe catalog path, publishing only a
// strictly newer valid generation.
func newGateway(catalogPath string) (*gateway.Executor, *gateway.CatalogHolder, error) {
	return newGatewayWithDial(catalogPath, nil, serviceauthz.Authority{})
}

func newGatewayWithDial(catalogPath string, dial gateway.DialFunc,
	internalAuthority serviceauthz.Authority) (*gateway.Executor, *gateway.CatalogHolder, error) {
	snap, err := gateway.LoadSnapshot(catalogPath)
	if err != nil {
		return nil, nil, err
	}
	holder := gateway.NewCatalogHolder(snap)
	refresher := gateway.NewFileCatalogRefresher(catalogPath, holder)
	exec := gateway.NewExecutor(gateway.NewClient(dial), holder, gateway.Options{
		Refresh: refresher.Refresh, InternalAuthority: internalAuthority,
	})
	return exec, holder, nil
}

func loadReplicatedCatalogSeeds(
	genesisPath, routeSeedPath string,
) (*gateway.Snapshot, *gateway.Snapshot, gateway.ReplicatedCatalogRouteSeedState, error) {
	if err := gateway.ValidateReplicatedCatalogRouteSeedSeparation(
		genesisPath, routeSeedPath,
	); err != nil {
		return nil, nil, gateway.ReplicatedCatalogRouteSeedState{}, err
	}
	genesis, err := gateway.LoadSnapshot(genesisPath)
	if err != nil || genesis.Generation() != 1 {
		return nil, nil, gateway.ReplicatedCatalogRouteSeedState{},
			errors.Join(err, gateway.ErrReplicatedCatalog)
	}
	state, err := gateway.LoadReplicatedCatalogRouteSeed(routeSeedPath)
	if err != nil {
		return nil, nil, gateway.ReplicatedCatalogRouteSeedState{}, err
	}
	routeSeed, found := state.Active()
	if !found {
		routeSeed = genesis
	}
	return genesis, routeSeed, state, nil
}

func sameReplicatedCatalogRoute(left, right gateway.ReplicatedRoute) bool {
	if left.Distribution != right.Distribution || left.Shard != right.Shard ||
		left.Group != right.Group ||
		left.AllocationGeneration != right.AllocationGeneration ||
		left.Command != right.Command || left.RangeIdentity != right.RangeIdentity ||
		left.LineageDigest != right.LineageDigest ||
		left.ForwardingRuleDigest != right.ForwardingRuleDigest ||
		len(left.Replicas) != len(right.Replicas) {
		return false
	}
	for index := range left.Replicas {
		if left.Replicas[index] != right.Replicas[index] {
			return false
		}
	}
	return true
}

func newReplicatedCatalogGateway(
	ctx context.Context,
	bootstrapPath string,
	routeSeedPath string,
	shardDial gateway.DialFunc,
	tlsProfile *rafttransport.PeerTLS,
	devPlaintext bool,
	internalAuthority serviceauthz.Authority,
	bootstrapIfMissing bool,
	relation replication.RelationID,
	attempts int,
	attemptTimeout time.Duration,
	handshakeTimeout time.Duration,
	maxConnections int,
	maxHandshakes int,
	journalPath string,
	clientID replication.ID128,
	retryHome replication.RetryHome,
	lease time.Duration,
	participants gateway.GatewayParticipantScanner,
	injected ...gateway.ReplicatedRoundTripper,
) (*gateway.Executor, *gateway.CatalogHolder, *gateway.ReplicatedCatalogAuthority,
	*gateway.ReplicatedExecutor, *gateway.AuthenticatedReplicatedClient, error) {
	if !devPlaintext && (maxConnections < 2 || maxHandshakes < 2) {
		// Catalog/topology traffic must retain one slot while public data is
		// saturated. A one-slot secure pool cannot provide that liveness fence.
		return nil, nil, nil, nil, nil, gateway.ErrReplicatedTLSProfile
	}
	distributionName := gateway.ReplicatedCatalogDistribution
	shardID := gateway.ReplicatedCatalogShard
	bootstrap, routeSeed, routeSeedState, err := loadReplicatedCatalogSeeds(
		bootstrapPath, routeSeedPath,
	)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, ok := routeSeed.ResolveReplicatedRoute(distributionName, shardID, replicas[:0])
	if !ok {
		return nil, nil, nil, nil, nil, gateway.ErrReplicatedCatalogMissing
	}
	var nativeClient gateway.ReplicatedRoundTripper
	var replicatedPool *gateway.AuthenticatedReplicatedClient
	if len(injected) > 1 {
		return nil, nil, nil, nil, nil, gateway.ErrReplicatedRoute
	} else if len(injected) == 1 && injected[0] != nil {
		// Embedded frontends inject the semantic transport owned by the physical
		// node. Catalog NativeSession proposals use this exact boundary too, so
		// they cannot accidentally bypass local authorization with a socket.
		nativeClient = injected[0]
	} else if devPlaintext {
		nativeClient = gateway.TCPReplicatedClient{Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}}
	} else {
		perEndpoint := maxConnections
		replicatedPool, err = gateway.NewAuthenticatedReplicatedClient(
			gateway.AuthenticatedReplicatedClientOptions{
				TLS: tlsProfile,
				Dial: func(ctx context.Context, address string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "tcp", address)
				},
				HandshakeDeadline: servicetls.FixedDeadline(handshakeTimeout),
				MaxConnections:    maxConnections, MaxPerEndpoint: perEndpoint,
				MaxIdlePerEndpoint: min(perEndpoint, 8), MaxHandshakes: maxHandshakes,
				MaxWaiters: maxConnections, MaxIdleAge: 30 * time.Second,
				MaxLifetime: 15 * time.Minute,
			},
		)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		nativeClient = replicatedPool
	}
	replicated, err := gateway.NewReplicatedExecutor(nativeClient, attempts, attemptTimeout)
	if err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	holder := gateway.NewCatalogHolder(nil)
	currentJournalPath := journalPath
	handoffPath := catalogSessionHandoffPath(journalPath)
	handoff, handoffFound, handoffErr := loadCatalogSessionHandoff(handoffPath)
	if handoffErr != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, handoffErr
	}
	if handoffFound && !catalogSessionHandoffPathsValid(handoff, journalPath) {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, gateway.ErrReplicatedCatalogConflict
	}
	if handoffFound {
		var resumeErr error
		currentJournalPath, resumeErr = catalogSessionResumeJournalPath(handoff, journalPath)
		if resumeErr != nil {
			if replicatedPool != nil {
				_ = replicatedPool.Close()
			}
			return nil, nil, nil, nil, nil, resumeErr
		}
	}
	newSession := func(sessionRoute gateway.ReplicatedRoute, sessionJournalPath string,
		bootstrapSnapshot *gateway.Snapshot) (*gateway.NativeSession, error) {
		binding, bindingErr := gateway.NativeSessionJournalBinding(
			sessionRoute, string(distributionName), string(shardID),
			[]byte{replicatedCatalogControllerTenant}, relation,
			serviceauthz.CapabilityTopology,
		)
		if bindingErr != nil {
			return nil, bindingErr
		}
		journal, journalErr := gateway.OpenNativeSessionJournal(
			gateway.NativeSessionJournalOptions{
				Path: sessionJournalPath, ClientID: clientID, RetryHome: retryHome,
				MaxCommandBytes: replication.MaxCommandBytes, Binding: binding,
			},
		)
		if journalErr != nil {
			return nil, journalErr
		}
		return gateway.NewNativeSession(gateway.NativeSessionOptions{
			Executor: replicated, Route: sessionRoute,
			CatalogBootstrap: bootstrapSnapshot,
			Distribution:     string(distributionName), Shard: string(shardID),
			Tenant: []byte{replicatedCatalogControllerTenant}, ClientID: clientID,
			RetryHome: retryHome, Resolver: gateway.BaseRelationResolver{Relation: relation},
			Journal: journal, ProposalCapability: serviceauthz.CapabilityTopology,
			MaxRelationBatches: 1, MaxMutations: gateway.MaxReplicatedCatalogBatchMutations,
			InitialCommandBytes: 4 << 10, MaxCommandBytes: replication.MaxCommandBytes,
		})
	}
	authorizedContext, err := serviceauthz.WithAuthority(ctx, internalAuthority)
	if err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	var resumedSession *gateway.NativeSession
	resumeCatalogHandoff := func() error {
		// A crash can occur after the route-seed rename and before the handoff
		// record reaches Complete. Reconcile that durable active cut first; the
		// absence of a pending file is not permission to fall back to the base
		// journal path.
		if handoffFound && handoff.Phase == catalogSessionHandoffComplete {
			// Complete is also the durable pointer to the exact journal binding.
			// Catalog generations may advance afterwards for metadata or an
			// address-only reachability publication while retaining that binding.
			// Requiring the old generation number here would reopen the base
			// journal after a cleanly completed handoff and strand recovery.
			active, activeExists := routeSeedState.Active()
			if activeExists && active != nil && active.Generation() >= handoff.NextGeneration {
				var activeReplicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
				activeRoute, activeOK := active.ResolveReplicatedRoute(
					gateway.ReplicatedCatalogDistribution, gateway.ReplicatedCatalogShard,
					activeReplicas[:0],
				)
				binding, bindingErr := gateway.NativeSessionJournalBinding(
					activeRoute, string(gateway.ReplicatedCatalogDistribution),
					string(gateway.ReplicatedCatalogShard), []byte{replicatedCatalogControllerTenant},
					relation, serviceauthz.CapabilityTopology,
				)
				expectedPath := catalogSessionJournalPath(journalPath, handoff.NextGeneration)
				if activeOK && bindingErr == nil && binding == handoff.NextBinding &&
					handoff.CurrentJournalPath == expectedPath && handoff.NextJournalPath == expectedPath &&
					catalogSessionHandoffPathsValid(handoff, journalPath) {
					nextSession, openErr := newSession(activeRoute, expectedPath, active)
					if openErr != nil {
						return openErr
					}
					if openErr = settleReplicatedCatalogSessionStartup(
						authorizedContext, nextSession, attempts, lease,
					); openErr != nil {
						return openErr
					}
					resumedSession = nextSession
					currentJournalPath = expectedPath
					routeSeed, route = active, activeRoute
					return nil
				}
			}
		}
		if handoffFound && handoff.Phase >= catalogSessionHandoffNewReady {
			active, activeExists := routeSeedState.Active()
			if activeExists && active != nil && active.Generation() == handoff.NextGeneration {
				var activeReplicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
				activeRoute, activeOK := active.ResolveReplicatedRoute(
					gateway.ReplicatedCatalogDistribution, gateway.ReplicatedCatalogShard,
					activeReplicas[:0],
				)
				if !activeOK {
					return gateway.ErrReplicatedCatalogMissing
				}
				digest, digestErr := gateway.CatalogSnapshotDigest(active)
				binding, bindingErr := gateway.NativeSessionJournalBinding(
					activeRoute, string(gateway.ReplicatedCatalogDistribution),
					string(gateway.ReplicatedCatalogShard), []byte{replicatedCatalogControllerTenant},
					relation, serviceauthz.CapabilityTopology,
				)
				expectedPath := catalogSessionJournalPath(journalPath, handoff.NextGeneration)
				if digestErr != nil || bindingErr != nil || digest != handoff.NextSnapshotDigest ||
					binding != handoff.NextBinding || handoff.NextJournalPath != expectedPath ||
					handoff.CurrentJournalPath != expectedPath ||
					!catalogSessionHandoffPathsValid(handoff, journalPath) {
					return errors.Join(digestErr, bindingErr, gateway.ErrReplicatedCatalogConflict)
				}
				nextSession, openErr := newSession(activeRoute, expectedPath, active)
				if openErr != nil {
					return openErr
				}
				if openErr = settleReplicatedCatalogSessionStartup(
					authorizedContext, nextSession, attempts, lease,
				); openErr != nil {
					return openErr
				}
				resumedSession = nextSession
				currentJournalPath = expectedPath
				routeSeed, route = active, activeRoute
				pendingAfterRecovery, pendingAfterRecoveryExists := routeSeedState.Pending()
				if pendingAfterRecoveryExists {
					// The pending file may already describe a subsequent catalog
					// move observed after this handoff. Promote only the candidate
					// named by this durable transition. A newer candidate is
					// handled below as a fresh serialized rollover.
					if pendingAfterRecovery == nil || pendingAfterRecovery.Generation() < handoff.NextGeneration {
						return gateway.ErrReplicatedCatalogConflict
					}
					if pendingAfterRecovery.Generation() == handoff.NextGeneration {
						refreshed, promoteErr := routeSeedState.PromotePendingAndReload()
						if promoteErr != nil {
							return promoteErr
						}
						routeSeedState = refreshed
					}
				}
				if handoff.Phase != catalogSessionHandoffComplete {
					handoff, openErr = catalogSessionHandoffPhaseAdvance(
						handoff, catalogSessionHandoffComplete, expectedPath,
					)
					if openErr != nil {
						return openErr
					}
					if openErr = storeCatalogSessionHandoff(handoffPath, handoff); openErr != nil {
						return openErr
					}
				}
				if pendingAfterRecoveryExists && pendingAfterRecovery.Generation() > handoff.NextGeneration {
					// The active session and seed now agree on the completed
					// predecessor. Keep that exact next journal as the old
					// binding while the following candidate goes through its own
					// durable handoff record.
					handoffFound = false
				} else {
					return nil
				}
			}
		}
		pending, pendingExists := routeSeedState.Pending()
		if !pendingExists {
			if handoffFound {
				// Every nonterminal record names a candidate that must still be
				// present, and a complete record must agree with the active seed.
				// Missing evidence is never permission to reopen a predecessor.
				return gateway.ErrReplicatedCatalogConflict
			}
			return nil
		}
		var pendingReplicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
		pendingRoute, pendingOK := pending.ResolveReplicatedRoute(
			gateway.ReplicatedCatalogDistribution, gateway.ReplicatedCatalogShard,
			pendingReplicas[:0],
		)
		if !pendingOK {
			return gateway.ErrReplicatedCatalogMissing
		}
		changed := !sameReplicatedCatalogRoute(route, pendingRoute)
		if !changed {
			if handoffFound && handoff.Phase != catalogSessionHandoffComplete {
				return gateway.ErrReplicatedCatalogConflict
			}
			refreshed, promoteErr := routeSeedState.PromotePendingAndReload()
			if promoteErr != nil {
				return promoteErr
			}
			routeSeedState, routeSeed = refreshed, pending
			return nil
		}
		active, activeExists := routeSeedState.Active()
		if !activeExists || active == nil {
			return gateway.ErrReplicatedCatalogConflict
		}
		if handoffFound && handoff.Phase == catalogSessionHandoffComplete {
			// The complete record is the durable current-journal pointer. A
			// subsequent catalog move starts a fresh transition record.
			handoffFound = false
		}
		if !handoffFound {
			nextPath := catalogSessionJournalPath(journalPath, pending.Generation())
			handoff, err = catalogSessionHandoffFromRoutes(
				route, pendingRoute, active, pending, currentJournalPath, nextPath, relation,
				catalogSessionJournalGeneration(journalPath, currentJournalPath),
			)
			if err != nil {
				return err
			}
			if !catalogSessionHandoffPathsValid(handoff, journalPath) {
				return gateway.ErrReplicatedCatalogConflict
			}
			if err = storeCatalogSessionHandoff(handoffPath, handoff); err != nil {
				return err
			}
			handoffFound = true
		} else {
			if handoff.OldGeneration != active.Generation() ||
				handoff.NextGeneration != pending.Generation() {
				return gateway.ErrReplicatedCatalogConflict
			}
			if evidenceErr := validateCatalogSessionHandoffEvidence(
				handoff, route, pendingRoute, active, pending, relation,
			); evidenceErr != nil {
				return evidenceErr
			}
			expectedNextPath := catalogSessionJournalPath(journalPath, pending.Generation())
			expectedCurrentPath := handoff.OldJournalPath
			if handoff.Phase >= catalogSessionHandoffNewReady {
				expectedCurrentPath = handoff.NextJournalPath
			}
			if !catalogSessionHandoffPathsValid(handoff, journalPath) ||
				handoff.NextJournalPath != expectedNextPath || currentJournalPath != expectedCurrentPath {
				return gateway.ErrReplicatedCatalogConflict
			}
		}
		var nextSession *gateway.NativeSession
		if handoff.Phase <= catalogSessionHandoffPrepared {
			present, presentErr := gateway.NativeSessionJournalPresent(handoff.OldJournalPath)
			if presentErr != nil {
				return presentErr
			}
			if present {
				oldSession, openErr := newSession(route, handoff.OldJournalPath, active)
				if openErr != nil {
					return openErr
				}
				status := oldSession.Status()
				if status.Pending {
					if settleErr := oldSession.SettleCatalogRouteHandoff(
						authorizedContext, pendingRoute, pending,
					); settleErr != nil {
						return settleErr
					}
					// Settling the retained command leaves the predecessor
					// journal active. Re-read the phase before deciding whether
					// its retire/release proof can continue.
					status = oldSession.Status()
				}
				if status.Active || status.Retired || status.Released {
					var settleErr error
					if status.Released {
						settleErr = oldSession.RetireReleaseAndDestroy(authorizedContext)
					} else {
						settleErr = oldSession.RetireReleaseAndDestroyViaCatalogRoute(
							authorizedContext, pendingRoute, pending,
						)
					}
					if settleErr != nil {
						return settleErr
					}
				} else {
					return gateway.ErrReplicatedCatalogConflict
				}
			}
			handoff, err = catalogSessionHandoffPhaseAdvance(
				handoff, catalogSessionHandoffOldSettled, handoff.OldJournalPath,
			)
			if err != nil {
				return err
			}
			if err = storeCatalogSessionHandoff(handoffPath, handoff); err != nil {
				return err
			}
		}
		if handoff.Phase <= catalogSessionHandoffOldSettled {
			nextSession, err = newSession(pendingRoute, handoff.NextJournalPath, pending)
			if err != nil {
				return err
			}
			if err = settleReplicatedCatalogSessionStartup(
				authorizedContext, nextSession, attempts, lease,
			); err != nil {
				return err
			}
			handoff, err = catalogSessionHandoffPhaseAdvance(
				handoff, catalogSessionHandoffNewReady, handoff.NextJournalPath,
			)
			if err != nil {
				return err
			}
			if err = storeCatalogSessionHandoff(handoffPath, handoff); err != nil {
				return err
			}
		}
		if nextSession == nil {
			nextSession, err = newSession(pendingRoute, handoff.NextJournalPath, pending)
			if err != nil {
				return err
			}
			if err = settleReplicatedCatalogSessionStartup(
				authorizedContext, nextSession, attempts, lease,
			); err != nil {
				return err
			}
		}
		refreshed, promoteErr := routeSeedState.PromotePendingAndReload()
		if promoteErr != nil {
			return promoteErr
		}
		routeSeedState, routeSeed, route = refreshed, pending, pendingRoute
		resumedSession = nextSession
		handoff, err = catalogSessionHandoffPhaseAdvance(
			handoff, catalogSessionHandoffComplete, handoff.NextJournalPath,
		)
		if err != nil {
			return err
		}
		return storeCatalogSessionHandoff(handoffPath, handoff)
	}
	if err = resumeCatalogHandoff(); err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	_, routeSeedExists := routeSeedState.Active()
	session := resumedSession
	if session == nil {
		session, err = newSession(route, currentJournalPath, routeSeed)
	}
	if err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	err = settleReplicatedCatalogSessionStartup(
		authorizedContext, session, attempts, lease,
	)
	if err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	currentSession := session
	rollover := func(
		rolloverCtx context.Context, oldRoute, nextRoute gateway.ReplicatedRoute,
		nextSnapshot *gateway.Snapshot,
	) (gateway.ReplicatedCatalogSessionRolloverResult, error) {
		oldSnapshot := holder.Current()
		if oldSnapshot == nil {
			oldSnapshot = routeSeed
		}
		nextPath := catalogSessionJournalPath(journalPath, nextSnapshot.Generation())
		handoff, handoffErr := catalogSessionHandoffFromRoutes(
			oldRoute, nextRoute, oldSnapshot, nextSnapshot,
			currentJournalPath, nextPath, relation,
			catalogSessionJournalGeneration(journalPath, currentJournalPath),
		)
		if handoffErr != nil {
			return gateway.ReplicatedCatalogSessionRolloverResult{}, handoffErr
		}
		if !catalogSessionHandoffPathsValid(handoff, journalPath) {
			return gateway.ReplicatedCatalogSessionRolloverResult{}, gateway.ErrReplicatedCatalogConflict
		}
		if handoffErr = storeCatalogSessionHandoff(handoffPath, handoff); handoffErr != nil {
			return gateway.ReplicatedCatalogSessionRolloverResult{}, handoffErr
		}
		currentRoute, currentRouteOK := currentSession.CatalogRoute()
		if currentSession == nil || !currentRouteOK || !sameReplicatedCatalogRoute(currentRoute, oldRoute) {
			return gateway.ReplicatedCatalogSessionRolloverResult{}, gateway.ErrNativeSession
		}
		status := currentSession.Status()
		if status.Pending {
			if handoffErr = currentSession.SettleCatalogRouteHandoff(
				rolloverCtx, nextRoute, nextSnapshot,
			); handoffErr != nil {
				return gateway.ReplicatedCatalogSessionRolloverResult{}, handoffErr
			}
			// RetryPending settles the exact old bytes but leaves this
			// predecessor active until the explicit Retire and Release
			// commands below. Refresh the phase after that settlement.
			status = currentSession.Status()
		}
		if status.Active || status.Retired || status.Released {
			if status.Released {
				handoffErr = currentSession.RetireReleaseAndDestroy(rolloverCtx)
			} else {
				handoffErr = currentSession.RetireReleaseAndDestroyViaCatalogRoute(
					rolloverCtx, nextRoute, nextSnapshot,
				)
			}
			if handoffErr != nil {
				return gateway.ReplicatedCatalogSessionRolloverResult{}, handoffErr
			}
		} else {
			return gateway.ReplicatedCatalogSessionRolloverResult{}, gateway.ErrNativeSession
		}
		handoff, handoffErr = catalogSessionHandoffPhaseAdvance(
			handoff, catalogSessionHandoffOldSettled, currentJournalPath,
		)
		if handoffErr != nil {
			return gateway.ReplicatedCatalogSessionRolloverResult{}, handoffErr
		}
		if handoffErr = storeCatalogSessionHandoff(handoffPath, handoff); handoffErr != nil {
			return gateway.ReplicatedCatalogSessionRolloverResult{}, handoffErr
		}
		nextSession, openErr := newSession(nextRoute, nextPath, nextSnapshot)
		if openErr != nil {
			return gateway.ReplicatedCatalogSessionRolloverResult{}, openErr
		}
		if openErr = settleReplicatedCatalogSessionStartup(
			rolloverCtx, nextSession, attempts, lease,
		); openErr != nil {
			return gateway.ReplicatedCatalogSessionRolloverResult{}, openErr
		}
		handoff, handoffErr = catalogSessionHandoffPhaseAdvance(
			handoff, catalogSessionHandoffNewReady, nextPath,
		)
		if handoffErr != nil {
			return gateway.ReplicatedCatalogSessionRolloverResult{}, handoffErr
		}
		if handoffErr = storeCatalogSessionHandoff(handoffPath, handoff); handoffErr != nil {
			return gateway.ReplicatedCatalogSessionRolloverResult{}, handoffErr
		}
		currentSession = nextSession
		currentJournalPath = nextPath
		return gateway.ReplicatedCatalogSessionRolloverResult{
			Session: nextSession,
			Complete: func() error {
				complete, found, loadErr := loadCatalogSessionHandoff(handoffPath)
				if loadErr != nil || !found || complete.Transition != handoff.Transition {
					return errors.Join(loadErr, gateway.ErrReplicatedCatalog)
				}
				complete, loadErr = catalogSessionHandoffPhaseAdvance(
					complete, catalogSessionHandoffComplete, nextPath,
				)
				if loadErr != nil {
					return loadErr
				}
				return storeCatalogSessionHandoff(handoffPath, complete)
			},
		}, nil
	}
	authority, err := gateway.NewReplicatedCatalogAuthority(gateway.ReplicatedCatalogAuthorityOptions{
		Executor: replicated, Route: route, Relation: relation, Holder: holder, Session: session,
		Authority: internalAuthority, SessionRollover: rollover, GatewayParticipants: participants,
	})
	if err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	if !bootstrapIfMissing && !routeSeedExists {
		_, err = waitReplicatedCatalogBootstrap(ctx, authority, attempts, attemptTimeout)
	} else {
		_, err = authority.Read(ctx)
	}
	if err != nil && (!bootstrapIfMissing ||
		!errors.Is(err, gateway.ErrReplicatedCatalogMissing)) {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	} else if err != nil {
		if routeSeedExists {
			if replicatedPool != nil {
				_ = replicatedPool.Close()
			}
			return nil, nil, nil, nil, nil, gateway.ErrReplicatedCatalogConflict
		}
		publishErr := authority.Publish(ctx, 0, bootstrap)
		for retry := 0; retry < attempts && errors.Is(publishErr, gateway.ErrReplicatedCatalogPending); retry++ {
			publishErr = authority.RetryPending(ctx)
		}
		if publishErr != nil &&
			!errors.Is(publishErr, gateway.ErrCatalogGenerationMismatch) &&
			!errors.Is(publishErr, gateway.ErrReplicatedCatalogConflict) {
			if replicatedPool != nil {
				_ = replicatedPool.Close()
			}
			return nil, nil, nil, nil, nil, publishErr
		}
		_, err = authority.Read(ctx)
		if err != nil {
			if replicatedPool != nil {
				_ = replicatedPool.Close()
			}
			return nil, nil, nil, nil, nil, err
		}
	}
	control, err := authority.InstallReplicatedCatalogRouteSeed(
		ctx, routeSeedPath, bootstrap,
	)
	if err != nil {
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	}
	select {
	case <-control.ShutdownRequired():
		handoffCtx, cancel := context.WithTimeout(
			context.Background(), replicatedCatalogRouteHandoffTimeout(attempts, attemptTimeout),
		)
		err = control.CompleteQuiescedHandoff(handoffCtx)
		cancel()
		if replicatedPool != nil {
			_ = replicatedPool.Close()
		}
		return nil, nil, nil, nil, nil, err
	default:
	}
	executor := gateway.NewExecutor(
		&gateway.ReplicatedSQLTransport{Executor: replicated}, holder, gateway.Options{
			Refresh: authority.Refresh, InternalAuthority: internalAuthority,
		},
	)
	return executor, holder, authority, replicated, replicatedPool, nil
}

// settleReplicatedCatalogSessionStartup spans the normal election window while
// preserving the native session's exact durable command. A pending journal is
// never replaced: only RetryPending is allowed until it returns a terminal
// proof. A fresh open or renewal may be retried only while it has not been
// admitted; once admission is ambiguous the loop switches permanently to the
// retained bytes. Deterministic validation/authentication errors fail at once.
func settleReplicatedCatalogSessionStartup(
	ctx context.Context, session *gateway.NativeSession, attempts int, lease time.Duration,
) error {
	if ctx == nil || session == nil || attempts <= 0 || lease <= 0 {
		return gateway.ErrNativeSession
	}
	renew := session.Status().Active && !session.Status().Pending
	for attempt := 0; attempt < attempts; attempt++ {
		var err error
		status := session.Status()
		switch {
		case status.Pending:
			_, err = session.RetryPending(ctx)
		case !status.Active:
			deadline := time.Now().Add(lease).UnixNano()
			if deadline <= 0 {
				return gateway.ErrNativeSession
			}
			_, err = session.Open(ctx, deadline)
		case renew:
			next := time.Now().Add(lease).UnixNano()
			if next <= status.LeaseDeadline {
				if status.LeaseDeadline == math.MaxInt64 {
					return gateway.ErrNativeSession
				}
				next = status.LeaseDeadline + 1
			}
			_, err = session.Renew(ctx, status.LeaseDeadline, next)
		default:
			return nil
		}
		if err == nil {
			return nil
		}
		if !errors.Is(err, gateway.ErrReplicatedLeader) &&
			!errors.Is(err, raftservice.ErrOutcomeUnknown) {
			return err
		}
		if attempt+1 == attempts {
			return err
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, context.Cause(ctx))
		case <-timer.C:
		}
	}
	return gateway.ErrNativeSession
}

type replicatedCatalogRouteSeedStartupHooks struct {
	journalPresent   func(string) (bool, error)
	settleOldSession func(context.Context, gateway.ReplicatedRoute) error
}

func recoverReplicatedCatalogRouteSeedStartup(
	ctx context.Context,
	journalPath string,
	active *gateway.Snapshot,
	activeRoute gateway.ReplicatedRoute,
	state gateway.ReplicatedCatalogRouteSeedState,
	hooks replicatedCatalogRouteSeedStartupHooks,
) (*gateway.Snapshot, gateway.ReplicatedRoute,
	gateway.ReplicatedCatalogRouteSeedState, error) {
	if ctx == nil {
		return nil, gateway.ReplicatedRoute{}, gateway.ReplicatedCatalogRouteSeedState{},
			gateway.ErrReplicatedCatalog
	}
	pending, pendingExists := state.Pending()
	if !pendingExists {
		return active, activeRoute, state, nil
	}
	var pendingReplicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	pendingRoute, ok := pending.ResolveReplicatedRoute(
		gateway.ReplicatedCatalogDistribution,
		gateway.ReplicatedCatalogShard,
		pendingReplicas[:0],
	)
	if !ok {
		return nil, gateway.ReplicatedRoute{}, gateway.ReplicatedCatalogRouteSeedState{},
			gateway.ErrReplicatedCatalogMissing
	}
	changed := !sameReplicatedCatalogRoute(activeRoute, pendingRoute)
	if changed {
		if hooks.journalPresent == nil || hooks.settleOldSession == nil {
			return nil, gateway.ReplicatedRoute{}, gateway.ReplicatedCatalogRouteSeedState{},
				gateway.ErrReplicatedCatalog
		}
		present, err := hooks.journalPresent(journalPath)
		if err != nil {
			return nil, gateway.ReplicatedRoute{}, gateway.ReplicatedCatalogRouteSeedState{}, err
		}
		if present {
			// The callback must reopen the journal under activeRoute—not the
			// candidate binding—and settle Retire→Release→destroy exactly.
			if err = hooks.settleOldSession(ctx, activeRoute); err != nil {
				return nil, gateway.ReplicatedRoute{}, gateway.ReplicatedCatalogRouteSeedState{}, err
			}
		}
	}
	refreshed, err := state.PromotePendingAndReload()
	if err != nil {
		return nil, gateway.ReplicatedRoute{}, gateway.ReplicatedCatalogRouteSeedState{}, err
	}
	// The predecessor has been durably settled before the candidate is
	// promoted. Returning the new route lets startup continue on the live
	// binding; a process restart is no longer the normal route-change path.
	return pending, pendingRoute, refreshed, nil
}

func replicatedCatalogRouteHandoffTimeout(attempts int, attemptTimeout time.Duration) time.Duration {
	const minimum = 5 * time.Second
	const maximum = 2 * time.Minute
	if attempts <= 0 || attemptTimeout <= 0 {
		return minimum
	}
	// A retained outcome-unknown command, Retire, and Release can each execute
	// one independently bounded proposal. Cap the aggregate shutdown budget: an
	// interrupted handoff is durable and resumes from the pending seed plus
	// native-session journal on the next process start.
	factor := int64(attempts)
	if factor > math.MaxInt64/3 {
		return maximum
	}
	factor *= 3
	if attemptTimeout > maximum/time.Duration(factor) {
		return maximum
	}
	timeout := attemptTimeout * time.Duration(factor)
	if timeout < minimum {
		return minimum
	}
	return timeout
}

const replicatedCatalogControllerTenant = byte(1)

func decodeFixedHex(encoded string, destination []byte) error {
	if len(encoded) != hex.EncodedLen(len(destination)) {
		return gateway.ErrReplicatedCatalog
	}
	written, err := hex.Decode(destination, []byte(encoded))
	if err != nil || written != len(destination) {
		return gateway.ErrReplicatedCatalog
	}
	return nil
}

// serveGateway accepts connections until ctx is canceled, then closes the
// listener and drains in-flight connections. It returns nil on a signaled
// shutdown and the accept error otherwise.
func serveGateway(ctx context.Context, listener net.Listener, exec *gateway.Executor, logf func(string, ...any)) error {
	return serveGatewayData(ctx, listener, exec, nil, logf)
}

func serveGatewayData(
	ctx context.Context,
	listener net.Listener,
	exec *gateway.Executor,
	data nativeDataReader,
	logf func(string, ...any),
) error {
	return serveGatewayDurableData(ctx, listener, exec, data, nil, logf)
}

func serveGatewayDurableData(
	ctx context.Context,
	listener net.Listener,
	exec *gateway.Executor,
	data nativeDataReader,
	durable durableRequestService,
	logf func(string, ...any),
) error {
	ctx, cancel := context.WithCancel(ctx)
	recoveryDone := startGatewayRecovery(ctx, exec, logf)
	// Closing the listener when ctx is done unblocks a blocked Accept, so a
	// signal shuts the loop down without a poll.
	stopListener := context.AfterFunc(ctx, func() { _ = listener.Close() })

	var wg sync.WaitGroup
	defer func() {
		cancel()
		_ = listener.Close()
		wg.Wait()
		<-recoveryDone
		stopListener()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, errFrontendAdmissionDrained) {
				// Frontend drain closes admission but deliberately leaves the
				// serving context alive. Keep existing native sessions running;
				// the normal runtime drain will cancel this context and join them.
				<-ctx.Done()
				return nil
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			handleConnPolicyDurable(ctx, conn, exec, data, durable, logf, nil)
		}()
	}
}

func startGatewayRecovery(
	ctx context.Context,
	exec *gateway.Executor,
	logf func(string, ...any),
) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		exec.RunRecovery(ctx, 5*time.Second, func(results []gateway.RecoveryResult, err error) {
			if err != nil {
				logf("gateway: transaction recovery: %v", err)
			}
			if len(results) != 0 {
				logf("gateway: transaction recovery resolved %d coordinator(s)", len(results))
			}
		})
	}()
	return done
}

func serveAuthenticatedGateway(ctx context.Context, listener net.Listener, exec *gateway.Executor,
	capability *gateway.ClientTLS, limits gateway.ClientTLSLimits, logf func(string, ...any)) error {
	return serveAuthenticatedGatewayData(ctx, listener, exec, nil, capability, limits, logf)
}

func serveAuthenticatedGatewayData(
	ctx context.Context,
	listener net.Listener,
	exec *gateway.Executor,
	data nativeDataReader,
	capability *gateway.ClientTLS,
	limits gateway.ClientTLSLimits,
	logf func(string, ...any),
) error {
	return serveAuthenticatedGatewayDurableData(ctx, listener, exec, data, nil, capability, limits, logf)
}

func serveAuthenticatedGatewayDurableData(
	ctx context.Context,
	listener net.Listener,
	exec *gateway.Executor,
	data nativeDataReader,
	durable durableRequestService,
	capability *gateway.ClientTLS,
	limits gateway.ClientTLSLimits,
	logf func(string, ...any),
) error {
	ctx, cancel := context.WithCancel(ctx)
	recoveryDone := startGatewayRecovery(ctx, exec, logf)
	defer func() {
		cancel()
		<-recoveryDone
	}()
	servingListener := &gatewayCancellationListener{Listener: listener, ctx: ctx, cancel: cancel}
	err := capability.ServeAuthorizedClients(ctx, servingListener, limits,
		func(ctx context.Context, connection net.Conn) {
			handleConnPolicyDurable(ctx, connection, exec, data, durable, logf,
				func(required serviceauthz.Capability) bool {
					return capability.Authorize(ctx, required, nil) == serviceauthz.DecisionAllow
				})
		})
	if errors.Is(err, errFrontendAdmissionDrained) {
		// servicetls waits its accepted workers before returning the sentinel.
		// Do not cancel their context here: an admitted frontend session remains
		// valid until it finishes or the owning Runtime.Drain is requested.
		<-ctx.Done()
		err = nil
	}
	return errors.Join(servingListener.acceptErr, nonCanceledError(err, context.Cause(ctx)))
}

// The TLS server joins accepted streams when Accept returns. A failed
// listener must first cancel those streams so an idle or stalled client cannot
// keep shutdown waiting forever. Preserve the original listener failure even
// though the resulting cancellation is an expected drain signal.
type gatewayCancellationListener struct {
	net.Listener
	ctx       context.Context
	cancel    context.CancelFunc
	acceptErr error
}

func (listener *gatewayCancellationListener) Accept() (net.Conn, error) {
	conn, err := listener.Listener.Accept()
	if errors.Is(err, errFrontendAdmissionDrained) {
		return conn, err
	}
	if err != nil && listener.ctx.Err() == nil {
		listener.acceptErr = err
		listener.cancel()
	}
	return conn, err
}

func handleConnAuthorized(ctx context.Context, conn net.Conn, exec *gateway.Executor,
	capability *gateway.ClientTLS, logf func(string, ...any)) {
	handleConnAuthorizedData(ctx, conn, exec, nil, capability, logf)
}

func handleConnAuthorizedData(ctx context.Context, conn net.Conn, exec *gateway.Executor,
	data nativeDataReader, capability *gateway.ClientTLS, logf func(string, ...any)) {
	handleConnPolicy(ctx, conn, exec, data, logf, func(required serviceauthz.Capability) bool {
		return capability.Authorize(ctx, required, nil) == serviceauthz.DecisionAllow
	})
}

// handleConn serves newline-delimited JSON requests on one connection until the
// peer disconnects or the server shuts down. Closing the connection when ctx is
// done unblocks a blocked decode so a signaled shutdown drains promptly.
func handleConn(ctx context.Context, conn net.Conn, exec *gateway.Executor, logf func(string, ...any)) {
	handleConnData(ctx, conn, exec, nil, logf)
}

func handleConnData(ctx context.Context, conn net.Conn, exec *gateway.Executor,
	data nativeDataReader, logf func(string, ...any)) {
	handleConnPolicy(ctx, conn, exec, data, logf, nil)
}

func handleConnPolicy(ctx context.Context, conn net.Conn, exec *gateway.Executor,
	data nativeDataReader, logf func(string, ...any), authorize func(serviceauthz.Capability) bool) {
	handleConnPolicyDurable(ctx, conn, exec, data, nil, logf, authorize)
}

func handleConnPolicyDurable(ctx context.Context, conn net.Conn, exec *gateway.Executor,
	data nativeDataReader, durable durableRequestService, logf func(string, ...any),
	authorize func(serviceauthz.Capability) bool,
) {
	ctx = serviceauthz.FrontendConnectionContextFromConn(ctx, conn)
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 4096), maxServeRequestBytes)
	writer := vibejson.NewWriter(conn)
	var nativeRequest nativeDataWireRequest
	var nativeResponseScratch nativeDataResponseScratch
	var req serveRequest
	var requestScratch serveRequestDecodeScratch
	for scanner.Scan() {
		line := scanner.Bytes()
		if exactOperationCandidate(line, []byte(`"ddl_forward"`)) {
			owner, _ := ctx.Value(gatewayDDLForwardContextKey{}).(*gatewayDDLForwardOwner)
			serveGatewayDDLForward(ctx, conn, owner, line)
			return
		}
		// Cluster-control envelopes use the same authenticated gateway-client
		// listener and canonical NDJSON framing as query traffic.  Dispatch them
		// before the SQL/native grammars so an operator request cannot be
		// interpreted as a user operation with a coincidentally similar field.
		if clusterControlRequestCandidate(line) {
			if server := clusterControlServerFromContext(ctx); server != nil {
				if err := server.ServeLine(ctx, conn, line); err != nil && ctx.Err() == nil {
					logf("gateway: cluster control: %v", err)
				}
				return
			}
		}
		structuredExecCandidate := durableExecBatchRequestCandidate(line)
		backupCandidate := gatewayBackupRequestCandidate(line)
		backupValid := backupCandidate && validateGatewayBackupEnvelope(line) == nil
		if issuerOpenRequestCandidate(line) {
			var request issuerOpenWireRequest
			authority, authenticated := serviceauthz.FromContext(ctx)
			if decodeIssuerOpenRequest(line, &request) != nil || !authenticated {
				if writeServeResponse(writer, &serveResponse{Error: errInvalidIssuerOpen.Error()}) != nil {
					return
				}
				continue
			}
			if authorize != nil && !authorize(serviceauthz.CapabilityDataWrite) {
				if writeServeResponse(writer, &serveResponse{Error: "authorization denied"}) != nil {
					return
				}
				continue
			}
			if durable == nil {
				if writeServeResponse(writer, &serveResponse{Error: errDurableExecBatchUnavailable.Error()}) != nil {
					return
				}
				continue
			}
			result, openErr := durable.OpenIssuer(ctx, authority, request.Open)
			if openErr != nil || result.Installation != request.Open.Installation ||
				result.Epoch != request.Open.Epoch || result.LaneOrdinal != request.Open.LaneOrdinal ||
				result.GrantDigest == (replication.Digest{}) {
				message := errDurableExecBatchUnavailable.Error()
				if openErr != nil {
					message = openErr.Error()
				}
				if writeServeResponse(writer, &serveResponse{Error: message}) != nil {
					return
				}
				continue
			}
			if writeIssuerOpenResponse(writer, result) != nil {
				return
			}
			continue
		}
		if durableExecBatchAckRequestCandidate(line) {
			var request durableExecBatchAckWireRequest
			authority, authenticated := serviceauthz.FromContext(ctx)
			if decodeDurableExecBatchAckRequest(line, &request) != nil || !authenticated {
				if writeServeResponse(writer, &serveResponse{Error: errInvalidDurableExecBatchAckRequest.Error()}) != nil {
					return
				}
				continue
			}
			if authorize != nil && !authorize(serviceauthz.CapabilityDataWrite) {
				if writeServeResponse(writer, &serveResponse{Error: "authorization denied"}) != nil {
					return
				}
				continue
			}
			if durable == nil {
				if writeServeResponse(writer, &serveResponse{Error: errDurableExecBatchUnavailable.Error()}) != nil {
					return
				}
				continue
			}
			response, ackErr := durable.AckExecBatch(ctx, authority, request)
			if ackErr != nil {
				if writeServeResponse(writer, &serveResponse{Error: ackErr.Error()}) != nil {
					return
				}
				continue
			}
			if writeDurableExecBatchAckResponse(writer, &response) != nil {
				return
			}
			continue
		}
		if nativeDataRequestCandidate(line) {
			if err := decodeNativeDataRequest(line, &nativeRequest); err != nil {
				response := nativeDataError(nativeDataResponseInvalidRequest, false)
				if writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout) != nil {
					return
				}
				continue
			}
			required := serviceauthz.CapabilityDataRead
			if nativeRequest.Operation != nativeDataOperationGet {
				required = serviceauthz.CapabilityDataWrite
			}
			if authorize != nil && !authorize(required) {
				response := nativeDataError(nativeDataResponseUnauthorized, false)
				if err := writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout); err != nil {
					return
				}
				continue
			}
			var response nativeDataWireResponse
			if data == nil {
				response = nativeDataError(nativeDataResponseUnavailable, true)
			} else {
				response = executeNativeDataRead(ctx, data, &nativeRequest)
			}
			writeErr := writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout)
			response.release()
			if writeErr != nil {
				if ctx.Err() == nil {
					logf("gateway: encode native response: %v", writeErr)
				}
				return
			}
			continue
		}
		var decodeErr error
		if structuredExecCandidate {
			decodeErr = decodeDurableExecBatchRequest(line, &req, &requestScratch)
			if decodeErr != nil {
				// Preserve the public error-response contract for malformed
				// structured input. Valid exec_batch requests take exactly one
				// decode pass; only rejected input pays this diagnostic fallback.
				decodeErr = decodeServeRequest(line, &req, &requestScratch)
			}
		} else {
			decodeErr = decodeServeRequest(line, &req, &requestScratch)
		}
		if decodeErr != nil {
			if ctx.Err() == nil {
				logf("gateway: decode request: %v", decodeErr)
			}
			return
		}
		// get/put/delete belong exclusively to the canonical native namespace.
		// Reordered or escaped spellings may never fall through to legacy SQL
		// execution or its string error schema.
		if req.Op == "get" || req.Op == "put" || req.Op == "delete" {
			response := nativeDataError(nativeDataResponseInvalidRequest, false)
			if writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout) != nil {
				return
			}
			continue
		}
		if req.Op == "read_batch" {
			batchRequest, buildErr := buildNativeSQLBatchReadRequest(req)
			if buildErr != nil {
				response := nativeDataError(nativeDataResponseInvalidRequest, false)
				if writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout) != nil {
					return
				}
				continue
			}
			if authorize != nil && !authorize(serviceauthz.CapabilityDataRead) {
				response := nativeDataError(nativeDataResponseUnauthorized, false)
				if writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout) != nil {
					return
				}
				continue
			}
			batchReader, available := data.(nativeSQLBatchReader)
			if !available {
				response := nativeDataError(nativeDataResponseUnavailable, true)
				if writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout) != nil {
					return
				}
				continue
			}
			result, readErr := batchReader.ReadSQLBatch(ctx, batchRequest)
			if readErr != nil {
				response := nativeDataResponseForError(readErr)
				if writeNativeDataConnResponse(conn, &response, &nativeResponseScratch, defaultNativeResponseWriteTimeout) != nil {
					return
				}
				continue
			}
			response := nativeSQLBatchWireResponse{
				Result: &result, Expected: len(batchRequest.Queries), Maximum: batchRequest.MaxResultBytes,
			}
			validationErr := validateNativeSQLBatchResponse(&response)
			if validationErr != nil {
				result.Release()
				encoded := nativeDataResponseForError(validationErr)
				if writeNativeDataConnResponse(conn, &encoded, &nativeResponseScratch, defaultNativeResponseWriteTimeout) != nil {
					return
				}
				continue
			}
			writeErr := writeNativeSQLBatchConnResponse(
				conn, writer, &response, defaultNativeResponseWriteTimeout,
			)
			result.Release()
			if writeErr != nil {
				if ctx.Err() == nil {
					logf("gateway: encode replicated SQL batch response: %v", writeErr)
				}
				return
			}
			continue
		}
		if authorize != nil && !authorize(serveRequestCapability(&req)) {
			if err := writeServeResponse(writer, &serveResponse{Error: "authorization denied"}); err != nil {
				return
			}
			continue
		}
		if req.Op == "metrics" {
			if !validGatewayMetricsRequest(req) {
				if writeServeResponse(writer, &serveResponse{Error: "invalid metrics request"}) != nil {
					return
				}
				continue
			}
			metrics := exec.Metrics()
			if writeServeResponse(writer, &serveResponse{Metrics: &metrics,
				DistributedMetrics: gatewayDistributedMetricsFromContext(ctx),
				ControllerMetrics:  gatewayControllerMetricsFromContext(ctx)}) != nil {
				return
			}
			continue
		}
		if req.Op == "backup" || req.Op == "backup_status" {
			if !backupCandidate || !backupValid {
				if writeServeResponse(writer, &serveResponse{Error: errInvalidGatewayBackupRequest.Error()}) != nil {
					return
				}
				continue
			}
			if writeServeResponse(writer, executeGatewayBackup(
				ctx, gatewayBackupOperatorFromContext(ctx), req,
			)) != nil {
				return
			}
			continue
		}
		if req.Op == "exec_batch" {
			// The public RF3 batch endpoint is durable-only. Raw identity-field
			// presence must never be inferred from decoded nonzero values: an
			// explicit zero/empty field is malformed structured input, not a
			// downgrade to the legacy unsequenced executor.
			if !structuredExecCandidate || !req.wireIdentitySet {
				if writeServeResponse(writer, &serveResponse{Error: errInvalidDurableExecBatch.Error()}) != nil {
					return
				}
				continue
			}
			authority, authenticated := serviceauthz.FromContext(ctx)
			if !authenticated {
				if writeServeResponse(writer, &serveResponse{Error: errDurableExecBatchUnavailable.Error()}) != nil {
					return
				}
				continue
			}
			if err := writeServeResponse(writer, executeDurableExecBatch(ctx, durable, authority, req)); err != nil {
				return
			}
			continue
		}
		if err := writeServeResponse(writer, execRequest(ctx, exec, req)); err != nil {
			if ctx.Err() == nil {
				logf("gateway: encode response: %v", err)
			}
			return
		}
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		logf("gateway: decode request: %v", err)
	}
}

func writeNativeDataConnResponse(
	connection net.Conn,
	response *nativeDataWireResponse,
	scratch *nativeDataResponseScratch,
	timeout time.Duration,
) error {
	if connection == nil || timeout <= 0 {
		return errInvalidNativeDataResponse
	}
	if err := connection.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	writeErr := writeNativeDataResponseDirect(connection, response, scratch)
	clearErr := connection.SetWriteDeadline(time.Time{})
	if writeErr != nil {
		return writeErr
	}
	return clearErr
}

func nativeDataRequestCandidate(source []byte) bool {
	index := skipNativeJSONSpace(source, 0)
	if index >= len(source) || source[index] != '{' {
		return false
	}
	index = skipNativeJSONSpace(source, index+1)
	if len(source)-index < len(`"op"`) ||
		!bytes.Equal(source[index:index+len(`"op"`)], []byte(`"op"`)) {
		return false
	}
	index = skipNativeJSONSpace(source, index+len(`"op"`))
	if index >= len(source) || source[index] != ':' {
		return false
	}
	index = skipNativeJSONSpace(source, index+1)
	for _, operation := range [...]string{`"get"`, `"put"`, `"delete"`} {
		if len(source)-index < len(operation) ||
			!bytes.Equal(source[index:index+len(operation)], []byte(operation)) {
			continue
		}
		next := index + len(operation)
		if next == len(source) {
			return true
		}
		switch source[next] {
		case ',', '}', ' ', '\t', '\r', '\n':
			return true
		default:
			return false
		}
	}
	return false
}

func skipNativeJSONSpace(source []byte, index int) int {
	for index < len(source) {
		switch source[index] {
		case ' ', '\t', '\r', '\n':
			index++
		default:
			return index
		}
	}
	return index
}

func serveRequestCapability(request *serveRequest) serviceauthz.Capability {
	if request == nil {
		return 0
	}
	if request.Op == "metrics" {
		return serviceauthz.CapabilityTopology
	}
	if request.Op == "backup" || request.Op == "backup_status" {
		return serviceauthz.CapabilityBackup
	}
	var required serviceauthz.Capability
	if request.hasSQL() {
		required = serviceauthz.SQLCapability(request.sqlText())
	}
	for index := range request.Statements {
		required |= serviceauthz.SQLCapability(request.Statements[index].sqlText())
	}
	return required
}

// writeServeResponse emits one NDJSON response without converting raw result
// cells into strings or passing them through a generic JSON tree.
func writeServeResponse(w *vibejson.Writer, resp *serveResponse) error {
	if resp == nil {
		return errServeResponseTransactionState
	}
	hasTransaction := resp.TransactionID != (replication.ID128{})
	hasOutcome := resp.Committed || resp.OutcomeUnknown
	if hasTransaction != hasOutcome || resp.Committed && resp.OutcomeUnknown {
		return errServeResponseTransactionState
	}
	if resp.DurableAck != nil && (!resp.Committed || !validDurableExecBatchAckRequest(resp.DurableAck)) {
		return errServeResponseTransactionState
	}
	if err := w.BeginObject(); err != nil {
		return err
	}
	stringField := func(name, value string) error {
		if value == "" {
			return nil
		}
		if err := w.Key(name); err != nil {
			return err
		}
		return w.String(value)
	}
	if err := stringField("kind", resp.Kind); err != nil {
		return err
	}
	if len(resp.Columns) != 0 {
		if err := w.Key("columns"); err != nil {
			return err
		}
		if err := w.BeginArray(); err != nil {
			return err
		}
		for _, column := range resp.Columns {
			if err := w.String(column); err != nil {
				return err
			}
		}
		if err := w.EndArray(); err != nil {
			return err
		}
	}
	if len(resp.Rows) != 0 {
		if err := w.Key("rows"); err != nil {
			return err
		}
		if err := w.BeginArray(); err != nil {
			return err
		}
		for _, row := range resp.Rows {
			if err := w.BeginArray(); err != nil {
				return err
			}
			for _, cell := range row {
				if err := w.RawUnchecked(cell); err != nil {
					return err
				}
			}
			if err := w.EndArray(); err != nil {
				return err
			}
		}
		if err := w.EndArray(); err != nil {
			return err
		}
	}
	if resp.RowsAffected != 0 {
		if err := w.Key("rows_affected"); err != nil {
			return err
		}
		if err := w.Int(resp.RowsAffected); err != nil {
			return err
		}
	}
	if err := stringField("route", resp.Route); err != nil {
		return err
	}
	if resp.Generation != 0 {
		if err := w.Key("generation"); err != nil {
			return err
		}
		if err := w.Uint(resp.Generation); err != nil {
			return err
		}
	}
	if resp.ShardsFanned != 0 {
		if err := w.Key("shards_fanned"); err != nil {
			return err
		}
		if err := w.Int(int64(resp.ShardsFanned)); err != nil {
			return err
		}
	}
	if resp.Retries != 0 {
		if err := w.Key("retries"); err != nil {
			return err
		}
		if err := w.Int(int64(resp.Retries)); err != nil {
			return err
		}
	}
	if resp.TransactionID != (replication.ID128{}) {
		if err := w.Key("transaction_id"); err != nil {
			return err
		}
		if err := writeNativeHex(w, resp.TransactionID[:]); err != nil {
			return err
		}
	}
	if resp.Committed {
		if err := w.Key("committed"); err != nil {
			return err
		}
		if err := w.Bool(true); err != nil {
			return err
		}
	}
	if resp.OutcomeUnknown {
		if err := w.Key("outcome_unknown"); err != nil {
			return err
		}
		if err := w.Bool(true); err != nil {
			return err
		}
	}
	if resp.DurableAck != nil {
		ack := resp.DurableAck
		if err := writeDurableExecBatchAckHexField(w, "request_id", ack.Identity.RequestID[:]); err != nil {
			return err
		}
		if err := writeDurableExecBatchAckHexField(w, "request_digest", ack.Identity.RequestDigest[:]); err != nil {
			return err
		}
		if err := writeDurableExecBatchAckHexField(w, "installation_id", ack.Identity.Reference.Installation[:]); err != nil {
			return err
		}
		if err := writeDurableExecBatchAckUintField(w, "issuer_epoch", ack.Identity.Reference.Epoch); err != nil {
			return err
		}
		if err := writeDurableExecBatchAckUintField(w, "lane_ordinal", uint64(ack.Identity.Reference.LaneOrdinal)); err != nil {
			return err
		}
		if err := writeDurableExecBatchAckHexField(w, "grant_digest", ack.Identity.Reference.GrantDigest[:]); err != nil {
			return err
		}
		if err := writeDurableExecBatchAckUintField(w, "issuer_sequence", ack.Identity.IssuerSequence); err != nil {
			return err
		}
		if err := writeDurableExecBatchAckUintField(w, "terminal_revision", ack.TerminalRevision); err != nil {
			return err
		}
		if err := writeDurableExecBatchAckHexField(w, "result_digest", ack.ResultDigest[:]); err != nil {
			return err
		}
		if err := writeDurableExecBatchAckHexField(w, "ack_token", ack.AckToken[:]); err != nil {
			return err
		}
	}
	if resp.Metrics != nil {
		if err := writeGatewayMetrics(w, *resp.Metrics); err != nil {
			return err
		}
	}
	if resp.DistributedMetrics != nil {
		if err := writeGatewayDistributedMetrics(w, resp.DistributedMetrics); err != nil {
			return err
		}
	}
	if resp.ControllerMetrics != nil {
		if err := writeGatewayControllerMetrics(w, resp.ControllerMetrics.Snapshot()); err != nil {
			return err
		}
	}
	if resp.BackupID != ([32]byte{}) {
		if err := w.Key("backup_id"); err != nil {
			return err
		}
		if err := writeNativeHex(w, resp.BackupID[:]); err != nil {
			return err
		}
	}
	if resp.BackupStage != 0 {
		if err := w.Key("backup_stage"); err != nil {
			return err
		}
		if err := w.Uint(resp.BackupStage); err != nil {
			return err
		}
	}
	if resp.BackupProof != ([32]byte{}) {
		if err := w.Key("backup_proof"); err != nil {
			return err
		}
		if err := writeNativeHex(w, resp.BackupProof[:]); err != nil {
			return err
		}
	}
	if err := stringField("error", resp.Error); err != nil {
		return err
	}
	if err := w.EndObject(); err != nil {
		return err
	}
	if err := w.Newline(); err != nil {
		return err
	}
	return w.Flush()
}

// execRequest translates one request and dispatches it, mapping any failure into
// an error reply rather than dropping the connection.
func execRequest(ctx context.Context, exec *gateway.Executor, req serveRequest) *serveResponse {
	var res *gateway.Result
	var err error
	switch req.Op {
	case "exec_batch":
		// Public exec_batch is admitted only by handleConnPolicyDurable after
		// strict raw GrantReference validation. No decoded request can enter an
		// unsequenced fallback through this helper.
		return &serveResponse{Error: errDurableExecBatchUnavailable.Error()}
	case "exec":
		// The write path routes the statement to its single owning shard and
		// refuses every scatter before any dispatch.
		q, buildErr := buildQuery(req)
		if buildErr != nil {
			return &serveResponse{Error: buildErr.Error()}
		}
		res, err = exec.Exec(ctx, q)
	case "", "query":
		q, buildErr := buildQuery(req)
		if buildErr != nil {
			return &serveResponse{Error: buildErr.Error()}
		}
		res, err = exec.Query(ctx, q)
	default:
		return &serveResponse{Error: fmt.Sprintf("unknown operation %q", req.Op)}
	}
	if err != nil {
		return &serveResponse{Error: err.Error()}
	}
	return encodeResult(res)
}

func buildBatchQueries(req serveRequest) ([]gateway.Query, error) {
	if req.hasSQL() || len(req.Params) != 0 || req.MaxResultBytes != 0 {
		return nil, errors.New("exec_batch uses statements instead of top-level sql or params")
	}
	if len(req.Statements) == 0 {
		return nil, gateway.ErrBatchEmpty
	}
	class, err := parseClass(req.Class)
	if err != nil {
		return nil, err
	}
	queries := make([]gateway.Query, len(req.Statements))
	for i := range req.Statements {
		params, err := buildParams(req.Statements[i].Params)
		if err != nil {
			return nil, fmt.Errorf("statement %d: %w", i, err)
		}
		queries[i] = gateway.Query{SQL: req.Statements[i].sqlText(), Params: params, Class: class}
	}
	return queries, nil
}

// buildQuery turns a request envelope into a gateway query. Placement, routing,
// ordering, and limiting are deliberately absent here: the executor derives
// them from SQL against its pinned catalog generation.
func buildQuery(req serveRequest) (gateway.Query, error) {
	if req.MaxResultBytes != 0 {
		return gateway.Query{}, errors.New("max_result_bytes is only valid for read_batch")
	}
	params, err := buildParams(req.Params)
	if err != nil {
		return gateway.Query{}, err
	}
	class, err := parseClass(req.Class)
	if err != nil {
		return gateway.Query{}, err
	}
	return gateway.Query{
		SQL:    req.sqlText(),
		Params: params,
		Class:  class,
	}, nil
}

// buildParams maps the typed request parameters onto shard-service parameters in
// placeholder order.
func buildParams(in []serveParam) ([]shardservice.Param, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]shardservice.Param, len(in))
	for i, p := range in {
		kind := p.wireKind
		if kind == 0 {
			switch p.Kind {
			case "null":
				kind = serveParamNull
			case "bool":
				kind = serveParamBool
			case "number":
				kind = serveParamNumber
			case "string":
				kind = serveParamString
			case "document":
				kind = serveParamDocument
			}
		}
		switch kind {
		case serveParamNull:
			out[i] = shardservice.NullParam()
		case serveParamBool:
			out[i] = shardservice.BoolParam(p.Bool)
		case serveParamNumber:
			out[i] = shardservice.NumberParam(p.textValue())
		case serveParamString:
			out[i] = shardservice.StringParam(p.textValue())
		case serveParamDocument:
			out[i] = shardservice.DocumentParam(p.textValue())
		default:
			return nil, fmt.Errorf("unknown parameter kind %q", p.Kind)
		}
	}
	return out, nil
}

// parseClass maps the request's class name onto an operational class, defaulting
// to the interactive profile.
func parseClass(name string) (gateway.OperationClass, error) {
	switch name {
	case "", "interactive":
		return gateway.ClassInteractive, nil
	case "batch":
		return gateway.ClassBatch, nil
	case "admin":
		return gateway.ClassAdmin, nil
	default:
		return 0, fmt.Errorf("unknown class %q", name)
	}
}

// encodeResult renders a merged result as a reply envelope, carrying each cell as
// raw JSON so an already-encoded value is not re-encoded.
func encodeResult(res *gateway.Result) *serveResponse {
	resp := &serveResponse{
		Kind:          res.Kind.String(),
		RowsAffected:  res.RowsAffected,
		Route:         res.RouteKind.String(),
		Generation:    res.Generation,
		ShardsFanned:  res.ShardsFanned,
		Retries:       res.Retries,
		TransactionID: res.TransactionID,
		Committed:     res.TransactionID != (replication.ID128{}),
	}
	for _, col := range res.Columns {
		resp.Columns = append(resp.Columns, col.Name)
	}
	if len(res.Rows) > 0 {
		resp.Rows = make([][]serveRawValue, len(res.Rows))
		for i, row := range res.Rows {
			cells := make([]serveRawValue, len(row))
			for j, c := range row {
				if c.Null {
					cells[j] = serveRawValue("null")
				} else {
					cells[j] = serveRawValue(c.Bytes)
				}
			}
			resp.Rows[i] = cells
		}
	}
	return resp
}
