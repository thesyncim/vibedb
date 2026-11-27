package gateway

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
)

var (
	ErrReplicatedControlDirectory = errors.New("gateway: invalid replicated control directory")
	ErrReplicatedControlRevision  = errors.New("gateway: replicated control directory revision conflict")
)

// MaxReplicatedControlDirectoryNodes bounds one complete directory cut. The
// bound is deliberately shared by control admission and safe-to-stop scans so
// a caller can prove it examined every physical node.
const MaxReplicatedControlDirectoryNodes = MaxScalingNodes

const maxReplicatedControlDirectoryIdentities = MaxReplicatedControlDirectoryNodes * 4

// GatewayControlDirectoryEndpoint is the authenticated address of one
// frontend participant. Gateway session identity is retained separately from
// the physical node/store identity; a new frontend process on the same node
// is a new endpoint and cannot satisfy an older fence.
type GatewayControlDirectoryEndpoint struct {
	Member            ClusterCatalogDrainMember
	ServiceKeyDigest  replication.Digest
	ServiceID         [16]byte
	SessionID         [16]byte
	SessionRevision   uint64
	ParticipantDigest replication.Digest
	Address           string
	DirectoryRevision uint64
}

func (endpoint GatewayControlDirectoryEndpoint) Valid() bool {
	return endpoint.Member.Node != (rafttransport.NodeID{}) &&
		endpoint.Member.Incarnation != 0 && endpoint.SessionID != ([16]byte{}) &&
		endpoint.ServiceKeyDigest != (replication.Digest{}) && endpoint.ServiceID != ([16]byte{}) &&
		endpoint.SessionRevision != 0 && endpoint.ParticipantDigest != (replication.Digest{}) &&
		endpoint.Address != "" && endpoint.DirectoryRevision != 0
}

// ReplicatedControlDirectorySnapshot is a complete catalog-derived cut. The
// snapshot revision is independent of catalog generation: a lifecycle or
// endpoint update may change participants without changing the catalog route.
type ReplicatedControlDirectorySnapshot struct {
	Revision          uint64
	CatalogGeneration uint64
	Nodes             []NodeRecord
}

func (snapshot ReplicatedControlDirectorySnapshot) Valid() bool {
	if snapshot.Revision == 0 || snapshot.CatalogGeneration == 0 ||
		len(snapshot.Nodes) == 0 || len(snapshot.Nodes) > MaxReplicatedControlDirectoryNodes {
		return false
	}
	for index, record := range snapshot.Nodes {
		if !record.Valid() || record.CatalogGeneration > snapshot.CatalogGeneration ||
			record.Revision > snapshot.Revision || index > 0 &&
			bytes.Compare(snapshot.Nodes[index-1].NodeID[:], record.NodeID[:]) >= 0 {
			return false
		}
	}
	return true
}

// replicatedControlDirectoryState is an immutable, pre-sorted cut published via
// atomic.Pointer so all readers operate completely lock-free with zero sorting
// or map iteration on the read hot path.
type replicatedControlDirectoryState struct {
	revision                     uint64
	catalogGeneration            uint64
	nodes                        []NodeRecord
	shardControlEndpoints        []ReplicatedEndpoint
	gatewayControlEndpoints      []GatewayControlDirectoryEndpoint
	gatewayControlEndpointsHist  []GatewayControlDirectoryEndpoint
}

// ReplicatedControlDirectory is a monotonic in-process view of the
// catalog-authorized node directory. Current participants are replaced by a
// newer cut, while historical control endpoints remain addressable so an
// immutable drain fence can finish after the node disappears from the latest
// cut. Historical retention is bounded by the catalog directory bound.
//
// Readers are completely lock-free via atomic.Pointer copy-on-write snapshots;
// Apply serializes writers under writeMu and validates atomically before
// publishing a new immutable snapshot.
type ReplicatedControlDirectory struct {
	state              atomic.Pointer[replicatedControlDirectoryState]
	writeMu            sync.Mutex
	historicalShards   map[controlDirectoryIdentity]string
	historicalGateways map[gatewayDirectoryIdentity]GatewayControlDirectoryEndpoint
}

type controlDirectoryIdentity struct {
	node        rafttransport.NodeID
	incarnation uint64
}

type gatewayDirectoryIdentity struct {
	node        rafttransport.NodeID
	incarnation uint64
	session     [16]byte
}

// NewReplicatedControlDirectory installs an initial complete cut.
func NewReplicatedControlDirectory(snapshot ReplicatedControlDirectorySnapshot) (*ReplicatedControlDirectory, error) {
	if !snapshot.Valid() {
		return nil, ErrReplicatedControlDirectory
	}
	directory := &ReplicatedControlDirectory{
		historicalShards:   make(map[controlDirectoryIdentity]string, len(snapshot.Nodes)),
		historicalGateways: make(map[gatewayDirectoryIdentity]GatewayControlDirectoryEndpoint, len(snapshot.Nodes)),
	}
	if err := directory.applyLocked(snapshot); err != nil {
		return nil, err
	}
	return directory, nil
}

// Apply publishes one complete newer cut. Equal revisions are accepted for an
// exact replay or a catalog-only generation advance with the same physical
// records; gaps are allowed because the reader returns a complete snapshot
// and the next update re-establishes a newer CAS boundary.
func (directory *ReplicatedControlDirectory) Apply(snapshot ReplicatedControlDirectorySnapshot) error {
	if directory == nil || !snapshot.Valid() {
		return ErrReplicatedControlDirectory
	}
	directory.writeMu.Lock()
	defer directory.writeMu.Unlock()
	cur := directory.state.Load()
	var curRevision, curCatalogGen uint64
	var curNodes []NodeRecord
	if cur != nil {
		curRevision, curCatalogGen, curNodes = cur.revision, cur.catalogGeneration, cur.nodes
	}
	if snapshot.Revision < curRevision || snapshot.CatalogGeneration < curCatalogGen {
		return fmt.Errorf("%w: incoming revision=%d catalog=%d current revision=%d catalog=%d",
			ErrReplicatedControlRevision, snapshot.Revision, snapshot.CatalogGeneration,
			curRevision, curCatalogGen)
	}
	if cur != nil && snapshot.Revision == curRevision {
		if !slices.EqualFunc(curNodes, snapshot.Nodes, equalNodeRecord) {
			return fmt.Errorf("%w: same revision=%d physical records changed incoming=%d current=%d",
				ErrReplicatedControlRevision, snapshot.Revision, len(snapshot.Nodes), len(curNodes))
		}
		if snapshot.CatalogGeneration > curCatalogGen {
			next := *cur
			next.catalogGeneration = snapshot.CatalogGeneration
			directory.state.Store(&next)
		}
		return nil
	}
	return directory.applyLocked(snapshot)
}

func (directory *ReplicatedControlDirectory) applyLocked(snapshot ReplicatedControlDirectorySnapshot) error {
	// Stage historical mutations into cloned maps so any validation or capacity
	// failure leaves the existing directory state completely untouched.
	nextShards := make(map[controlDirectoryIdentity]string, len(directory.historicalShards)+len(snapshot.Nodes))
	maps.Copy(nextShards, directory.historicalShards)
	nextGateways := make(map[gatewayDirectoryIdentity]GatewayControlDirectoryEndpoint, len(directory.historicalGateways)+len(snapshot.Nodes))
	maps.Copy(nextGateways, directory.historicalGateways)

	gatewayEndpoints := make([]GatewayControlDirectoryEndpoint, 0, len(snapshot.Nodes))
	for _, record := range snapshot.Nodes {
		if record.ControlAddress == "" || record.NodeID == (rafttransport.NodeID{}) || record.Incarnation == 0 {
			return ErrReplicatedControlDirectory
		}
		shardIdentity := controlDirectoryIdentity{node: record.NodeID, incarnation: record.Incarnation}
		if prior, found := nextShards[shardIdentity]; found && prior != record.ControlAddress {
			return ErrReplicatedControlDirectory
		}
		nextShards[shardIdentity] = record.ControlAddress
		if len(nextShards) > maxReplicatedControlDirectoryIdentities {
			return ErrReplicatedControlDirectory
		}
		if record.Gateway.NodeID != (rafttransport.NodeID{}) {
			if record.Gateway.Incarnation == 0 ||
				record.Gateway.SessionID == ([16]byte{}) || record.Gateway.SessionRevision == 0 ||
				record.Gateway.ParticipantDigest == (replication.Digest{}) || record.GatewayAddress == "" {
				return ErrReplicatedControlDirectory
			}
			gatewayEndpoint := GatewayControlDirectoryEndpoint{
				Member:           ClusterCatalogDrainMember{Node: record.Gateway.NodeID, Incarnation: record.Gateway.Incarnation},
				ServiceKeyDigest: record.Gateway.ServiceKeyDigest, ServiceID: record.Gateway.ServiceID,
				SessionID: record.Gateway.SessionID, SessionRevision: record.Gateway.SessionRevision,
				ParticipantDigest: record.Gateway.ParticipantDigest, Address: record.GatewayAddress,
				DirectoryRevision: record.Revision,
			}
			identity := gatewayDirectoryIdentity{node: gatewayEndpoint.Member.Node,
				incarnation: gatewayEndpoint.Member.Incarnation, session: gatewayEndpoint.SessionID}
			if prior, found := nextGateways[identity]; found &&
				(prior.Address != gatewayEndpoint.Address || prior.ServiceKeyDigest != gatewayEndpoint.ServiceKeyDigest ||
					prior.ServiceID != gatewayEndpoint.ServiceID || prior.ParticipantDigest != gatewayEndpoint.ParticipantDigest ||
					prior.SessionRevision != gatewayEndpoint.SessionRevision) {
				return ErrReplicatedControlDirectory
			}
			nextGateways[identity] = gatewayEndpoint
			if len(nextGateways) > maxReplicatedControlDirectoryIdentities {
				return ErrReplicatedControlDirectory
			}
			if record.Lifecycle != NodeDecommissioned {
				gatewayEndpoints = append(gatewayEndpoints, gatewayEndpoint)
			}
		}
	}

	// snapshot.Valid() already verified snapshot.Nodes is strictly ascending by NodeID.
	nodes := cloneNodeRecords(snapshot.Nodes)

	shardEndpoints := make([]ReplicatedEndpoint, 0, len(nextShards))
	for identity, address := range nextShards {
		shardEndpoints = append(shardEndpoints, ReplicatedEndpoint{
			Node: identity.node, NodeIncarnation: identity.incarnation, ControlAddress: address,
		})
	}
	slices.SortFunc(shardEndpoints, func(left, right ReplicatedEndpoint) int {
		if compared := bytes.Compare(left.Node[:], right.Node[:]); compared != 0 {
			return compared
		}
		return cmp.Compare(left.NodeIncarnation, right.NodeIncarnation)
	})

	if len(gatewayEndpoints) == 0 {
		gatewayEndpoints = []GatewayControlDirectoryEndpoint{}
	} else {
		slices.SortFunc(gatewayEndpoints, compareGatewayControlDirectoryEndpoint)
	}

	gatewayEndpointsHist := make([]GatewayControlDirectoryEndpoint, 0, len(nextGateways))
	for _, endpoint := range nextGateways {
		gatewayEndpointsHist = append(gatewayEndpointsHist, endpoint)
	}
	slices.SortFunc(gatewayEndpointsHist, compareGatewayControlDirectoryEndpoint)

	directory.historicalShards = nextShards
	directory.historicalGateways = nextGateways
	directory.state.Store(&replicatedControlDirectoryState{
		revision:                    snapshot.Revision,
		catalogGeneration:           snapshot.CatalogGeneration,
		nodes:                       nodes,
		shardControlEndpoints:       shardEndpoints,
		gatewayControlEndpoints:     gatewayEndpoints,
		gatewayControlEndpointsHist: gatewayEndpointsHist,
	})
	return nil
}

func equalNodeRecord(left, right NodeRecord) bool {
	if left.Replacement == right.Replacement {
		return left == right
	}
	if left.Replacement == nil || right.Replacement == nil || *left.Replacement != *right.Replacement {
		return false
	}
	left.Replacement = nil
	right.Replacement = nil
	return left == right
}

func cloneNodeRecords(records []NodeRecord) []NodeRecord {
	cloned := slices.Clone(records)
	for i := range cloned {
		if cloned[i].Replacement != nil {
			proof := *cloned[i].Replacement
			cloned[i].Replacement = &proof
		}
	}
	return cloned
}

func compareGatewayControlDirectoryEndpoint(left, right GatewayControlDirectoryEndpoint) int {
	if compared := bytes.Compare(left.Member.Node[:], right.Member.Node[:]); compared != 0 {
		return compared
	}
	if compared := cmp.Compare(left.Member.Incarnation, right.Member.Incarnation); compared != 0 {
		return compared
	}
	return bytes.Compare(left.SessionID[:], right.SessionID[:])
}

func (directory *ReplicatedControlDirectory) Revision() uint64 {
	if directory == nil {
		return 0
	}
	state := directory.state.Load()
	if state == nil {
		return 0
	}
	return state.revision
}

func (directory *ReplicatedControlDirectory) CatalogGeneration() uint64 {
	if directory == nil {
		return 0
	}
	state := directory.state.Load()
	if state == nil {
		return 0
	}
	return state.catalogGeneration
}

// Nodes returns the current physical cut. Historical records deliberately do
// not reappear here; callers scanning safe-to-stop references must inspect the
// catalog generation and current directory together.
func (directory *ReplicatedControlDirectory) Nodes() []NodeRecord {
	if directory == nil {
		return nil
	}
	state := directory.state.Load()
	if state == nil {
		return nil
	}
	return cloneNodeRecords(state.nodes)
}

// ShardControlEndpoints returns the union of current and historical physical
// control addresses. Retaining the union is what lets old drain obligations
// reach a departing process after its NodeRecord is removed.
func (directory *ReplicatedControlDirectory) ShardControlEndpoints() []ReplicatedEndpoint {
	if directory == nil {
		return nil
	}
	state := directory.state.Load()
	if state == nil {
		return nil
	}
	return slices.Clone(state.shardControlEndpoints)
}

// GatewayControlEndpoints returns only the current participant roster. An
// active drain certificate keeps its own immutable members and the opener's
// historical union remains available for those members.
func (directory *ReplicatedControlDirectory) GatewayControlEndpoints() []GatewayControlDirectoryEndpoint {
	if directory == nil {
		return nil
	}
	state := directory.state.Load()
	if state == nil {
		return nil
	}
	return slices.Clone(state.gatewayControlEndpoints)
}

// GatewayControlEndpointsWithHistory returns every participant identity ever
// observed by this process, including departed incarnations. It is used only
// for admission to an already-authenticated drain service; new drain fences
// must use GatewayControlEndpoints so a removed participant cannot silently
// disappear from the directory cut.
func (directory *ReplicatedControlDirectory) GatewayControlEndpointsWithHistory() []GatewayControlDirectoryEndpoint {
	if directory == nil {
		return nil
	}
	state := directory.state.Load()
	if state == nil {
		return nil
	}
	return slices.Clone(state.gatewayControlEndpointsHist)
}

// ReadFrom consumes the complete node cut supplied by a catalog-backed
// DirectoryReader. Since DirectoryReader intentionally exposes no separate
// global revision, the caller provides it from the certified catalog metadata
// that accompanied the read.
func (directory *ReplicatedControlDirectory) ReadFrom(
	ctx context.Context, reader DirectoryReader, revision, catalogGeneration uint64,
) error {
	if directory == nil || ctx == nil || reader == nil || revision == 0 || catalogGeneration == 0 {
		return ErrReplicatedControlDirectory
	}
	nodes, err := reader.ListNodes(ctx)
	if err != nil {
		return err
	}
	current, reduceErr := reduceCurrentNodeDirectory(nodes)
	if reduceErr != nil {
		return reduceErr
	}
	nodes = current
	return directory.Apply(ReplicatedControlDirectorySnapshot{
		Revision: revision, CatalogGeneration: catalogGeneration, Nodes: nodes,
	})
}
