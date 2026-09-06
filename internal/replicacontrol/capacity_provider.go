package replicacontrol

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/raftmember"
)

const (
	// MaxCapacitySources bounds one cold accounting round.  A source directory
	// must expose the complete local group inventory; silently truncating it
	// would turn an actual node report into an unsafe lower bound.
	MaxCapacitySources = 4096
)

// CapacitySourceSample is the detached result of one local group-owned
// storage source. Implementations should use durable metadata counters such as
// ReplicatedApply.ResourceStats and WAL live-byte metrics. They must return
// ErrCapacityUnavailable when the source cannot prove a bounded measurement.
type CapacitySourceSample struct {
	Identity       raftmember.RuntimeIdentity
	Applied        uint64
	Demand         autosplit.CapacityVector
	MigrationBytes uint64
	KnownEmpty     bool
	DemandKind     CapacityDemandKind
}

// CapacitySource is one adopted or cold group. The callback is expected to be
// cancellable and bounded; it must not inspect individual user rows as part of
// a foreground request.
type CapacitySource interface {
	Identity() raftmember.RuntimeIdentity
	ObserveCapacity(context.Context) (CapacitySourceSample, error)
}

// CapacityRequestSource is an optional extension for cold sources whose
// durable snapshot descriptor is keyed by the exact operation and step. The
// provider still owns source enumeration and identity matching; this hook
// only lets a source bind its bounded snapshot estimator to that request.
type CapacityRequestSource interface {
	CapacitySource
	ObserveCapacityRequest(context.Context, CapacityRequest) (CapacitySourceSample, error)
}

// CapacitySourceDirectory supplies a complete local source inventory and one
// actual node-wide used/capacity cut. Both callbacks are intentionally
// separate: group storage belongs to the group owner, while node capacity
// includes every hosted group and in-flight physical reservation.
type CapacitySourceDirectory struct {
	Sources func(context.Context) ([]CapacitySource, error)
	// Node returns an independently fenced node-wide cut. It is useful when a
	// node agent already maintains an atomic aggregate of all hosted groups.
	Node func(context.Context) (NodeCapacity, error)
	// NodeWithSamples receives the complete bounded source cut used for this
	// request. Implementations may aggregate actual used bytes and active
	// migration reservations from every local source without another round of
	// storage reads. When present it takes precedence over Node.
	NodeWithSamples func(context.Context, CapacityRequest, []CapacitySourceSample) (NodeCapacity, error)
}

// CapacityProvider converts owned storage sources into the authenticated
// CapacityObserver consumed by CapacityService. Freshness is a monotonic
// process-local revision; a node incarnation and runtime store identity fence
// it across restart, so a reset process-local counter cannot be mistaken for a
// previous owner's observation.
type CapacityProvider struct {
	directory CapacitySourceDirectory
	revision  atomic.Uint64
	mu        sync.Mutex
	rounds    []capacityRoundCut
}

const maxCapacityRounds = 4
const capacityRoundLifetime = 30 * time.Second

type capacityRoundCut struct {
	operation, round [32]byte
	catalog          uint64
	created          time.Time
	samples          []CapacitySourceSample
	node             NodeCapacity
	revision         uint64
}

func NewCapacityProvider(directory CapacitySourceDirectory) (*CapacityProvider, error) {
	if directory.Sources == nil || (directory.Node == nil && directory.NodeWithSamples == nil) {
		return nil, ErrCapacityUnavailable
	}
	return &CapacityProvider{directory: directory}, nil
}

func (provider *CapacityProvider) ObserveReplicaCapacity(ctx context.Context, request CapacityRequest) (CapacityObservation, error) {
	if provider == nil || ctx == nil || !validCapacityRequest(request) {
		return CapacityObservation{}, ErrCapacityUnavailable
	}
	if err := context.Cause(ctx); err != nil {
		return CapacityObservation{}, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return CapacityObservation{}, err
	}
	sources, err := provider.directory.Sources(ctx)
	if err != nil {
		return CapacityObservation{}, errors.Join(ErrCapacityUnavailable, err)
	}
	if len(sources) == 0 || len(sources) > MaxCapacitySources {
		return CapacityObservation{}, ErrCapacityUnavailable
	}
	var source CapacitySource
	var sourceIndex = -1
	for index, candidate := range sources {
		if candidate == nil {
			return CapacityObservation{}, ErrCapacityUnavailable
		}
		identity := candidate.Identity()
		if identity.Group == request.Group && identity.MemberID == request.TargetMember {
			if source != nil {
				return CapacityObservation{}, ErrCapacityUnavailable
			}
			source = candidate
			sourceIndex = index
		}
	}
	if source == nil {
		return CapacityObservation{}, ErrCapacityUnavailable
	}
	if sourceIndex < 0 {
		return CapacityObservation{}, ErrCapacityStale
	}
	if request.Round != ([32]byte{}) {
		for _, cut := range provider.rounds {
			if cut.round != request.Round || cut.operation != request.Operation || cut.catalog != request.ExpectedCatalogGeneration {
				continue
			}
			if time.Since(cut.created) >= capacityRoundLifetime || len(cut.samples) != len(sources) || cut.revision < request.MinimumSourceRevision {
				return CapacityObservation{}, ErrCapacityStale
			}
			for i, candidate := range sources {
				if candidate.Identity() != cut.samples[i].Identity {
					return CapacityObservation{}, ErrCapacityStale
				}
			}
			sample := cut.samples[sourceIndex]
			return NewCapacityObservation(CapacityObservation{Request: request, Identity: sample.Identity,
				CatalogGeneration: cut.catalog, Applied: sample.Applied, SourceRevision: cut.revision,
				Demand: sample.Demand, MigrationBytes: sample.MigrationBytes, KnownEmpty: sample.KnownEmpty,
				Node: cut.node, DemandKind: sample.DemandKind})
		}
	}

	samples := make([]CapacitySourceSample, len(sources))
	for index, candidate := range sources {
		if requestSource, ok := candidate.(CapacityRequestSource); ok {
			samples[index], err = requestSource.ObserveCapacityRequest(ctx, request)
		} else {
			samples[index], err = candidate.ObserveCapacity(ctx)
		}
		if err != nil {
			return CapacityObservation{}, fmt.Errorf("source %x member %d: %w", candidate.Identity().Group.GroupID, candidate.Identity().MemberID, errors.Join(ErrCapacityUnavailable, err))
		}
		if samples[index].Identity != candidate.Identity() ||
			samples[index].Applied == 0 ||
			(samples[index].DemandKind != CapacityDemandMeasured && samples[index].DemandKind != CapacityDemandConservative) {
			return CapacityObservation{}, ErrCapacityStale
		}
	}
	sample := samples[sourceIndex]
	if sample.Identity.Group != request.Group || sample.Identity.MemberID != request.TargetMember {
		return CapacityObservation{}, ErrCapacityStale
	}
	var node NodeCapacity
	if provider.directory.NodeWithSamples != nil {
		node, err = provider.directory.NodeWithSamples(ctx, request, samples)
	} else {
		node, err = provider.directory.Node(ctx)
	}
	if err != nil {
		return CapacityObservation{}, fmt.Errorf("node aggregate: %w", errors.Join(ErrCapacityUnavailable, err))
	}
	if node.NodeID == ([16]byte{}) || node.NodeIncarnation == 0 {
		return CapacityObservation{}, ErrCapacityStale
	}
	// A controller may retry with a persisted source-revision floor after a
	// restart. Advance our local monotonic revision to that floor before
	// publishing the cut; returning a smaller number would be a stale fact even
	// when all storage counters are current.
	var revision uint64
	for {
		prior := provider.revision.Load()
		if prior == math.MaxUint64 {
			return CapacityObservation{}, ErrCapacityUnavailable
		}
		next := prior + 1
		if next < request.MinimumSourceRevision {
			next = request.MinimumSourceRevision
		}
		if provider.revision.CompareAndSwap(prior, next) {
			revision = next
			break
		}
	}
	observation := CapacityObservation{
		Request: request, Identity: sample.Identity,
		CatalogGeneration: request.ExpectedCatalogGeneration, Applied: sample.Applied,
		SourceRevision: revision, Demand: sample.Demand,
		MigrationBytes: sample.MigrationBytes, KnownEmpty: sample.KnownEmpty, Node: node,
		DemandKind: sample.DemandKind,
	}
	validated, err := NewCapacityObservation(observation)
	if err != nil {
		return CapacityObservation{}, err
	}
	if request.Round != ([32]byte{}) {
		cut := capacityRoundCut{operation: request.Operation, round: request.Round, catalog: request.ExpectedCatalogGeneration,
			created: time.Now(), samples: samples, node: node, revision: revision}
		if len(provider.rounds) == maxCapacityRounds {
			copy(provider.rounds, provider.rounds[1:])
			provider.rounds[len(provider.rounds)-1] = cut
		} else {
			provider.rounds = append(provider.rounds, cut)
		}
	}
	return validated, nil
}

// AddCapacity saturates at MaxUint64 and reports whether any component would
// overflow. Callers use the saturated value only as a refusal signal; it is
// never treated as an exact measured byte count.
func AddCapacity(left, right uint64) (uint64, bool) {
	if right > math.MaxUint64-left {
		return math.MaxUint64, true
	}
	return left + right, false
}

// AddCapacityVectors returns a saturating component-wise sum. The overflow
// flag lets node aggregators reject a cut instead of wrapping a used counter.
func AddCapacityVectors(left, right autosplit.CapacityVector) (autosplit.CapacityVector, bool) {
	var result autosplit.CapacityVector
	overflow := false
	for resource := range result {
		result[resource], overflow = AddCapacity(left[resource], right[resource])
		if overflow {
			return result, true
		}
	}
	return result, false
}
