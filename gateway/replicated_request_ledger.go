package gateway

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

var (
	ErrDurableRequest             = errors.New("gateway: invalid durable request")
	ErrDurableRequestConflict     = errors.New("gateway: durable request identity conflict")
	ErrDurableRequestBound        = errors.New("gateway: durable request exceeds its byte bound")
	ErrDurableRequestStaleHome    = errors.New("gateway: durable request ledger home is stale")
	ErrDurableRequestUnavailable  = errors.New("gateway: durable request ledger is unavailable")
	ErrDurableRequestUnresolved   = errors.New("gateway: durable request has no terminal proof")
	ErrDurableRequestAcknowledged = errors.New("gateway: durable request result was acknowledged")
	ErrDurableRequestCapacity     = errors.New("gateway: durable request admission capacity rejected")
)

const (
	// The request recipe is admitted by encoded bytes, never by participant
	// count. A wider transaction is legal when its complete portable recipe fits.
	MaxDurableRequestRecipeBytes = requestledger.MaxPlanBytes - 28
	DurableRequestInlineBytes    = 32 << 10
	DurableRequestPlanPageBytes  = 512 << 10
)

// DurableRequestLedgerKey is the fixed-width durable identity. TenantDigest
// prevents one tenant's request namespace from aliasing another without
// retaining tenant bytes in every hidden-state key.
type DurableRequestLedgerKey struct {
	requestledger.RequestKey
	Digest replication.Digest
}

// DurableRequestLedgerHome is one independently replicated ledger shard. Its
// identity is stable across endpoint and fence refreshes; changing Identity is
// a rehome, which the request path deliberately refuses without a durable
// forwarding proof.
type DurableRequestLedgerHome struct {
	Identity           replication.Digest
	Point              requestledger.LedgerHome
	TopologyGeneration uint64
	route              ReplicatedRoute
}

// ReplicatedRoute returns a defensive copy for diagnostics and external
// inspection. The production ledger adapter uses the package-private borrowed
// view, so a hot Lookup neither allocates nor exposes publication storage.
func (home DurableRequestLedgerHome) ReplicatedRoute() ReplicatedRoute {
	return cloneDurableRequestRoute(home.route)
}

func (home DurableRequestLedgerHome) borrowedRoute() ReplicatedRoute { return home.route }

// DurableRequestLedgerRange is one adjacent half-open home interval [Start,
// End). The final interval uses zero End as +infinity. Identity and boundaries
// are immutable until a durable split/forwarding protocol is installed.
type DurableRequestLedgerRange struct {
	Start    requestledger.LedgerHome
	End      requestledger.LedgerHome
	Identity replication.Digest
	Route    ReplicatedRoute
}

type DurableRequestLedgerTopology struct {
	Generation uint64
	Ranges     []DurableRequestLedgerRange
}

// DurableRequestLedgerTopologyHolder publishes immutable adjacent ranges.
// Selection is a logarithmic point lookup over the complete request home and
// never routes every tenant through one catalog row.
type DurableRequestLedgerTopologyHolder struct {
	mu      sync.Mutex
	current atomic.Pointer[DurableRequestLedgerTopology]
}

func NewDurableRequestLedgerTopologyHolder(
	topology DurableRequestLedgerTopology,
) (*DurableRequestLedgerTopologyHolder, error) {
	holder := new(DurableRequestLedgerTopologyHolder)
	if err := holder.Publish(topology); err != nil {
		return nil, err
	}
	return holder, nil
}

func (holder *DurableRequestLedgerTopologyHolder) Publish(
	topology DurableRequestLedgerTopology,
) error {
	if holder == nil || topology.Generation == 0 || len(topology.Ranges) == 0 {
		return ErrDurableRequest
	}
	ranges := make([]DurableRequestLedgerRange, len(topology.Ranges))
	for index := range topology.Ranges {
		value := topology.Ranges[index]
		if value.Identity == (replication.Digest{}) || !validReplicatedRoute(value.Route) {
			return ErrDurableRequest
		}
		ranges[index] = DurableRequestLedgerRange{
			Start: value.Start, End: value.End, Identity: value.Identity,
			Route: cloneDurableRequestRoute(value.Route),
		}
	}
	slices.SortFunc(ranges, func(left, right DurableRequestLedgerRange) int {
		return bytes.Compare(left.Start[:], right.Start[:])
	})
	identities := make(map[replication.Digest]struct{}, len(ranges))
	for index := range ranges {
		value := ranges[index]
		if _, duplicate := identities[value.Identity]; duplicate ||
			(index == 0 && value.Start != (requestledger.LedgerHome{})) ||
			(index != 0 && ranges[index-1].End != value.Start) ||
			(index+1 != len(ranges) &&
				(value.End == (requestledger.LedgerHome{}) || bytes.Compare(value.Start[:], value.End[:]) >= 0)) ||
			(index+1 == len(ranges) && value.End != (requestledger.LedgerHome{})) {
			return ErrDurableRequest
		}
		identities[value.Identity] = struct{}{}
	}
	holder.mu.Lock()
	defer holder.mu.Unlock()
	current := holder.current.Load()
	if current != nil && topology.Generation <= current.Generation {
		return ErrDurableRequest
	}
	if current != nil {
		if len(current.Ranges) != len(ranges) {
			return ErrDurableRequest
		}
		for index := range ranges {
			if ranges[index].Start != current.Ranges[index].Start ||
				ranges[index].End != current.Ranges[index].End ||
				ranges[index].Identity != current.Ranges[index].Identity ||
				!validDurableRequestHomeRouteRefresh(
					current.Ranges[index].Route, ranges[index].Route,
				) {
				return ErrDurableRequest
			}
		}
	}
	holder.current.Store(&DurableRequestLedgerTopology{
		Generation: topology.Generation,
		Ranges:     ranges,
	})
	return nil
}

func (holder *DurableRequestLedgerTopologyHolder) Current() *DurableRequestLedgerTopology {
	if holder == nil {
		return nil
	}
	current := holder.current.Load()
	if current == nil {
		return nil
	}
	cloned := &DurableRequestLedgerTopology{
		Generation: current.Generation,
		Ranges:     make([]DurableRequestLedgerRange, len(current.Ranges)),
	}
	for index := range current.Ranges {
		cloned.Ranges[index] = current.Ranges[index]
		cloned.Ranges[index].Route = cloneDurableRequestRoute(current.Ranges[index].Route)
	}
	return cloned
}

// Lookup reads one immutable publication and returns a borrowed route view.
// Callers must not mutate Route or its replica slice. Publications are never
// changed in place, so the hot lookup is allocation-free and its cost is
// independent of total ledger-range cardinality.
func (holder *DurableRequestLedgerTopologyHolder) Lookup(
	point requestledger.LedgerHome,
) (DurableRequestLedgerHome, uint64, bool) {
	if holder == nil {
		return DurableRequestLedgerHome{}, 0, false
	}
	current := holder.current.Load()
	if current == nil {
		return DurableRequestLedgerHome{}, 0, false
	}
	home, ok := current.home(point)
	return home, current.Generation, ok
}

func (topology *DurableRequestLedgerTopology) Home(
	point requestledger.LedgerHome,
) (DurableRequestLedgerHome, bool) {
	return topology.home(point)
}

func (topology *DurableRequestLedgerTopology) home(
	point requestledger.LedgerHome,
) (DurableRequestLedgerHome, bool) {
	if topology == nil || topology.Generation == 0 || len(topology.Ranges) == 0 {
		return DurableRequestLedgerHome{}, false
	}
	index := sort.Search(len(topology.Ranges), func(index int) bool {
		end := topology.Ranges[index].End
		return end == (requestledger.LedgerHome{}) || bytes.Compare(point[:], end[:]) < 0
	})
	if index == len(topology.Ranges) || bytes.Compare(point[:], topology.Ranges[index].Start[:]) < 0 {
		return DurableRequestLedgerHome{}, false
	}
	value := topology.Ranges[index]
	return DurableRequestLedgerHome{
		Identity: value.Identity, Point: point, TopologyGeneration: topology.Generation,
		route: value.Route,
	}, true
}

type DurableRequestLedgerState uint8

const (
	DurableRequestLedgerAbsent DurableRequestLedgerState = iota
	DurableRequestLedgerCreating
	DurableRequestLedgerSealed
	DurableRequestLedgerPending
	DurableRequestLedgerTerminal
	DurableRequestLedgerAcked
)

// DurableRequestPlanDescriptor binds either one inline recipe or an ordered
// stream of fixed-bound pages. Root is a chain over page ordinals and exact
// bytes, not a digest of decoded Go objects.
type DurableRequestPlanDescriptor struct {
	TotalBytes uint64
	PageCount  uint32
	Root       replication.Digest
	Contract   DurableRequestExecutionContract
	Inline     []byte
}

// DurableRequestPending is the write-ahead outbound cut. Target and Command
// are the exact bytes which may be sent; neither may be regenerated after a
// response is lost. StepRevision prevents a delayed settlement from advancing
// a later command.
type DurableRequestPending struct {
	StepRevision uint64
	Target       []byte
	Command      []byte
}

type DurableRequestTerminal struct {
	// Result is the sole canonical terminal grammar. Structured fields served
	// to clients are decoded from these exact digest-bound bytes.
	Result []byte
}

type DurableRequestLedgerEntry struct {
	State             DurableRequestLedgerState
	Revision          uint64
	Digest            replication.Digest
	Plan              DurableRequestPlanDescriptor
	AppendedPageCount uint32
	Pending           DurableRequestPending
	// Progress is the runner-defined canonical cursor/result witness written by
	// Advance. It makes revision-to-next-step deterministic after gateway loss.
	Progress            []byte
	SettledStepRevision uint64
	ProgressDigest      replication.Digest
	Terminal            DurableRequestTerminal
	AckDigest           replication.Digest
	AckTerminalRevision uint64
	AckResultDigest     replication.Digest
	// AckToken is present only while Terminal retains the raw possession
	// capability. Acked tombstones retain AckTokenDigest instead, so a reopened
	// ledger never has to reconstruct secret bytes it deliberately reclaimed.
	AckToken       DurableRequestAckToken
	AckTokenDigest replication.Digest
	// AckPlanRoot and AckTerminalContractDigest are the compact anti-replay
	// witnesses retained after the plan/result bodies are reclaimed. They let
	// Execute reject a changed lowered program even though ACK intentionally
	// discarded the full descriptor.
	AckPlanRoot               replication.Digest
	AckTerminalContractDigest replication.Digest
	Generation                uint64
}

// durableRequestCoarseLedger is the pre-fence executor seam retained only
// while DurableRequestExecutor is migrated to the exact lifecycle below. It
// cannot represent physical route-pin or logical execution-pin cuts and must
// not be used by new shipped integrations.
type durableRequestCoarseLedger interface {
	Lookup(context.Context, DurableRequestLedgerHome, DurableRequestLedgerKey) (DurableRequestLedgerEntry, error)
	// CreatePlanning begins a paged recipe. CreateSealed is the one-proposal
	// inline fast path. A successful response from either is an authenticated
	// applied completion, not an unproved transport acknowledgement.
	CreatePlanning(context.Context, DurableRequestLedgerHome, DurableRequestLedgerKey, uint64, DurableRequestPlanDescriptor) (DurableRequestLedgerEntry, error)
	CreateSealed(context.Context, DurableRequestLedgerHome, DurableRequestLedgerKey, uint64, DurableRequestPlanDescriptor) (DurableRequestLedgerEntry, error)
	// AppendPlanPage atomically seals when seal is true. Successful final-page
	// completion is the positive sealed witness; only an ambiguous error needs a
	// subsequent lookup.
	AppendPlanPage(context.Context, DurableRequestLedgerHome, DurableRequestLedgerKey, uint64, uint32, []byte, bool) (DurableRequestLedgerEntry, error)
	LoadPlanPage(context.Context, DurableRequestLedgerHome, DurableRequestLedgerKey, uint32) ([]byte, error)
	PutPending(context.Context, DurableRequestLedgerHome, DurableRequestLedgerKey, uint64, DurableRequestPending) (DurableRequestLedgerEntry, error)
	Advance(context.Context, DurableRequestLedgerHome, DurableRequestLedgerKey, uint64, uint64, []byte) (DurableRequestLedgerEntry, error)
	Complete(context.Context, DurableRequestLedgerHome, DurableRequestLedgerKey, uint64, DurableRequestTerminal) (DurableRequestLedgerEntry, error)
	Acknowledge(context.Context, DurableRequestLedgerHome, DurableRequestLedgerKey, uint64, uint64, replication.Digest, DurableRequestAckToken) (DurableRequestLedgerEntry, error)
}

// DurableRequestStepJournal is the only legal outbound path for a runner. A
// runner must call Stage before dispatch and Settle with the same revision and
// exact observed bytes afterward. Implementations may pipeline a wave only by
// staging the complete ordered wave as one command.
type DurableRequestStepJournal interface {
	Stage(context.Context, []byte, []byte) (uint64, error)
	Settle(context.Context, uint64, []byte) error
}

// DurableRequestRunner executes or resumes a sealed portable recipe. It owns
// protocol interpretation but no durable state: every outbound data command
// must cross journal. Recipe pages remain immutable for the call.
type DurableRequestRunner interface {
	Run(context.Context, DurableRequestRecipe, DurableRequestStepJournal) (DurableRequestTerminal, error)
}

type DurableRequestRecipe struct {
	CatalogGeneration uint64
	Identity          ReplicatedTransactionIdentity
	Contract          DurableRequestExecutionContract
	Tenant            []byte
	KeyDigest         replication.Digest
	RequestID         replication.ID128
	RequestDigest     replication.Digest
	ParticipantCount  uint64
	// ParticipantStream exposes at most one decoded participant at a time. A
	// runner must not retain Current across Next; the backing page/frame scratch
	// is reused to keep replay memory independent of total plan size.
	ParticipantStream DurableRequestParticipantStream
	// Participants is retained only for the legacy in-memory codec tests. The
	// durable executor never populates it.
	Participants []ReplicatedTransactionParticipant
	// Pending is nonzero only when the ledger proves that these exact outbound
	// bytes were write-ahead staged but not yet durably advanced.
	Pending        DurableRequestPending
	ResumeRevision uint64
	Progress       []byte
}

type DurableRequestParticipantStream interface {
	Next() bool
	Current() DurableRequestLogicalParticipant
	Err() error
	Complete() bool
	BufferedBytes() int
}

// DurableRequestReplayableParticipantStream is an authenticated sealed-plan
// stream which can begin another bounded pass over the same immutable bytes.
// Streaming transaction protocols use multiple passes for manifest, prepare,
// decision, and finish without retaining an unbounded participant slice.
type DurableRequestReplayableParticipantStream interface {
	DurableRequestParticipantStream
	Reset() error
}

type DurableRequest struct {
	// Key is the complete authenticated and sequenced request identity. The
	// executor never derives identity from context, tenant text, or a local
	// process identity. Issuer epoch, lane, and sequence are mandatory.
	Key DurableRequestLedgerKey

	// Program is the trusted, already-sealed logical transaction contract. It
	// contains no physical endpoint/fence strings and never embeds ordinary
	// per-shard MutationBatch commands.
	Program DurableRequestLogicalProgram
}

// ReplicatedTransactionIdentity is generated before ledger Seal and is the
// only identity accepted by persisted execution/recovery. LedgerHome and
// RetryHome are intentionally unrelated domains.
type ReplicatedTransactionIdentity struct {
	ID                 distributedtxn.ID
	RetryHome          replication.RetryHome
	CatalogGeneration  uint64
	RecoveryDeadline   int64
	CoordinatorOrdinal uint32
}

// DurableRequestExecutionContract is the trusted catalog/schema/pin and
// terminal grammar selected before Seal. Every digest is fixed-width and
// included in TerminalContractDigest; the kernel never invents production
// defaults from PlanRoot.
type DurableRequestExecutionContract struct {
	CatalogGeneration            uint64
	KeyDigest                    replication.Digest
	RequestDigest                replication.Digest
	KernelSemanticsDigest        replication.Digest
	ApplyContractDigest          replication.Digest
	TransactionManifestDigest    replication.Digest
	RetryHomeDerivationDigest    replication.Digest
	ClockContractDigest          replication.Digest
	CoordinatorIdentityDigest    replication.Digest
	SchemaManifestDigest         replication.Digest
	LineageForwardingDigest      replication.Digest
	InitialStateDigest           replication.Digest
	CommitTerminalStateDigest    replication.Digest
	AbortTerminalStateDigest     replication.Digest
	TerminalSummaryDigest        replication.Digest
	PinID                        requestledger.PinID
	PinEpoch                     uint64
	PinDigest                    replication.Digest
	RouteSchemaCertificateDigest replication.Digest
	TerminalContractDigest       replication.Digest
	ProtocolProgramDigest        replication.Digest
	ResultGrammarDigest          replication.Digest
	RetirementWitnessDigest      replication.Digest
	CommitTransitionTag          uint32
	AbortTransitionTag           uint32
	ParticipantCount             uint64
	CommitFinalWaveCount         uint64
	AbortFinalWaveCount          uint64
	MaxPendingWaveBytes          uint64
	MaxContinuationBytes         uint64
	MaxTerminalBytes             uint64
	MaxActivePayloadBytes        uint64
	MaxActivePayloadChunks       uint64
	PlanBuildID                  replication.Digest
	PlanningLeaseExpiryIndex     uint64
	PlanningLeaseGeneration      uint64
}

// DurableRequestLogicalParticipant seals stable logical authority and
// mutations, not replaceable endpoints or live command fences. Resolve is
// performed immediately before each journaled wave.
type DurableRequestLogicalParticipant struct {
	Distribution           distribution.DistributionName
	Shard                  distribution.ShardID
	RangeIdentity          replication.Digest
	Group                  raftmember.GroupKey
	SchemaGeneration       uint64
	RelationManifestDigest replication.Digest
	LineageDigest          replication.Digest
	ForwardingRuleDigest   replication.Digest
	MutationDigest         distributedtxn.Digest
	BucketBits             uint8
	IntentScopes           []distributedtxn.IntentScope
	Batches                []replication.RelationMutationBatch
}

type DurableRequestLogicalProgram struct {
	Identity      ReplicatedTransactionIdentity
	Contract      DurableRequestExecutionContract
	Tenant        []byte
	KeyDigest     replication.Digest
	RequestID     replication.ID128
	RequestDigest replication.Digest
	Participants  []DurableRequestLogicalParticipant
}

// DurableRequestRouteResolver resolves current physical authority for one
// sealed logical participant. Once a wave is PutPending, recovery uses the
// exact retained route/command bytes and never calls Resolve for that wave.
type DurableRequestRouteResolver interface {
	ResolveDurableRequestParticipant(
		context.Context,
		DurableRequestLogicalParticipant,
	) (ReplicatedRoute, error)
}

type DurableRequestOutcome struct {
	ReplicatedTransactionResult
	CatalogGeneration uint64
	ShardsFanned      int
	Result            []byte
	TerminalRevision  uint64
	ResultDigest      replication.Digest
	AckToken          DurableRequestAckToken
	Acknowledged      bool
}

// DurableRequestAckToken is an opaque terminal-possession witness. Possessing
// only a request ID/digest is insufficient to discard its result or release
// its pins.
type DurableRequestAckToken [32]byte

func durableRequestAckTokenDigest(token DurableRequestAckToken) replication.Digest {
	return replication.Digest(requestledger.AckTokenDigest(requestledger.AckToken(token)))
}

type DurableRequestExecutorOptions struct {
	Topology *DurableRequestLedgerTopologyHolder
	Ledger   durableRequestCoarseLedger
	Runner   DurableRequestRunner
}

type DurableRequestExecutor struct {
	topology *DurableRequestLedgerTopologyHolder
	ledger   durableRequestCoarseLedger
	runner   DurableRequestRunner
}

func NewDurableRequestExecutor(options DurableRequestExecutorOptions) (*DurableRequestExecutor, error) {
	if options.Topology == nil || options.Topology.current.Load() == nil ||
		options.Ledger == nil || options.Runner == nil {
		return nil, ErrDurableRequest
	}
	return &DurableRequestExecutor{
		topology: options.Topology, ledger: options.Ledger, runner: options.Runner,
	}, nil
}

func validDurableRequestLedgerKey(key DurableRequestLedgerKey) bool {
	return key.RequestKey.Valid() && key.IssuerEpoch != 0 &&
		key.IssuerSequence != 0 && key.IssuerLane != (requestledger.IssuerLane{}) &&
		key.Digest != (replication.Digest{})
}

func durableRequestLedgerHome(key DurableRequestLedgerKey) (replication.Digest, error) {
	if !validDurableRequestLedgerKey(key) {
		return replication.Digest{}, ErrDurableRequest
	}
	home, err := requestledger.Home(key.RequestKey)
	if err != nil {
		return replication.Digest{}, errors.Join(err, ErrDurableRequest)
	}
	return replication.Digest(home), nil
}

func cloneDurableRequestRoute(route ReplicatedRoute) ReplicatedRoute {
	cloned := route
	cloned.Replicas = slices.Clone(route.Replicas)
	return cloned
}

func validDurableRequestHomeRouteRefresh(prior, next ReplicatedRoute) bool {
	if prior.Distribution != next.Distribution || prior.Shard != next.Shard ||
		prior.Group != next.Group || prior.AllocationGeneration != next.AllocationGeneration ||
		prior.RangeIdentity != next.RangeIdentity ||
		prior.LineageDigest != next.LineageDigest ||
		prior.ForwardingRuleDigest != next.ForwardingRuleDigest {
		return false
	}
	left, right := prior.Command, next.Command
	if right.ReplicaSetVersion < left.ReplicaSetVersion ||
		right.ActivePolicyGeneration < left.ActivePolicyGeneration ||
		right.ProtectionEpoch < left.ProtectionEpoch ||
		right.OwnershipEpoch < left.OwnershipEpoch ||
		right.SchemaGeneration != left.SchemaGeneration ||
		right.RelationManifestDigest != left.RelationManifestDigest ||
		right.RoutingVersion < left.RoutingVersion ||
		right.RouteGeneration < left.RouteGeneration {
		return false
	}
	return true
}
