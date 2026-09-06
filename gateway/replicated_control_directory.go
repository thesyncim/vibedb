package gateway

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"

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

// ReplicatedControlDirectory is a monotonic in-process view of the
// catalog-authorized node directory. Current participants are replaced by a
// newer cut, while historical control endpoints remain addressable so an
// immutable drain fence can finish after the node disappears from the latest
// cut. Historical retention is bounded by the catalog directory bound.
type ReplicatedControlDirectory struct {
	mu                 sync.RWMutex
	revision           uint64
	catalogGeneration  uint64
	current            map[rafttransport.NodeID]NodeRecord
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
		revision: snapshot.Revision, catalogGeneration: snapshot.CatalogGeneration,
		current:            make(map[rafttransport.NodeID]NodeRecord, len(snapshot.Nodes)),
		historicalShards:   make(map[controlDirectoryIdentity]string, len(snapshot.Nodes)),
		historicalGateways: make(map[gatewayDirectoryIdentity]GatewayControlDirectoryEndpoint, len(snapshot.Nodes)),
	}
	if err := directory.applyLocked(snapshot); err != nil {
		return nil, err
	}
	return directory, nil
}

// Apply publishes one complete newer cut. Equal revisions are accepted only
// for an exact replay; gaps are allowed because the reader returns a complete
// snapshot and the next update re-establishes a newer CAS boundary.
func (directory *ReplicatedControlDirectory) Apply(snapshot ReplicatedControlDirectorySnapshot) error {
	if directory == nil || !snapshot.Valid() {
		return ErrReplicatedControlDirectory
	}
	directory.mu.Lock()
	defer directory.mu.Unlock()
	if snapshot.Revision < directory.revision || snapshot.CatalogGeneration < directory.catalogGeneration {
		return ErrReplicatedControlRevision
	}
	if snapshot.Revision == directory.revision {
		if !sameControlDirectoryRecords(directory.current, snapshot.Nodes) ||
			snapshot.CatalogGeneration != directory.catalogGeneration {
			return ErrReplicatedControlRevision
		}
		return nil
	}
	return directory.applyLocked(snapshot)
}

func (directory *ReplicatedControlDirectory) applyLocked(snapshot ReplicatedControlDirectorySnapshot) error {
	if directory.current == nil {
		directory.current = make(map[rafttransport.NodeID]NodeRecord, len(snapshot.Nodes))
	}
	if directory.historicalShards == nil {
		directory.historicalShards = make(map[controlDirectoryIdentity]string, len(snapshot.Nodes))
	}
	if directory.historicalGateways == nil {
		directory.historicalGateways = make(map[gatewayDirectoryIdentity]GatewayControlDirectoryEndpoint, len(snapshot.Nodes))
	}
	for _, record := range snapshot.Nodes {
		if record.ControlAddress == "" || record.NodeID == (rafttransport.NodeID{}) || record.Incarnation == 0 {
			return ErrReplicatedControlDirectory
		}
		shardIdentity := controlDirectoryIdentity{node: record.NodeID, incarnation: record.Incarnation}
		if prior, found := directory.historicalShards[shardIdentity]; found && prior != record.ControlAddress {
			return ErrReplicatedControlDirectory
		}
		directory.historicalShards[shardIdentity] = record.ControlAddress
		if len(directory.historicalShards) > maxReplicatedControlDirectoryIdentities {
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
			if prior, found := directory.historicalGateways[identity]; found &&
				(prior.Address != gatewayEndpoint.Address || prior.ServiceKeyDigest != gatewayEndpoint.ServiceKeyDigest ||
					prior.ServiceID != gatewayEndpoint.ServiceID || prior.ParticipantDigest != gatewayEndpoint.ParticipantDigest ||
					prior.SessionRevision != gatewayEndpoint.SessionRevision) {
				return ErrReplicatedControlDirectory
			}
			directory.historicalGateways[identity] = gatewayEndpoint
			if len(directory.historicalGateways) > maxReplicatedControlDirectoryIdentities {
				return ErrReplicatedControlDirectory
			}
		}
	}
	directory.current = make(map[rafttransport.NodeID]NodeRecord, len(snapshot.Nodes))
	for _, record := range snapshot.Nodes {
		directory.current[record.NodeID] = record
	}
	directory.revision, directory.catalogGeneration = snapshot.Revision, snapshot.CatalogGeneration
	return nil
}

func sameControlDirectoryRecords(current map[rafttransport.NodeID]NodeRecord, records []NodeRecord) bool {
	if len(current) != len(records) {
		return false
	}
	for _, record := range records {
		if prior, found := current[record.NodeID]; !found || prior != record {
			return false
		}
	}
	return true
}

func (directory *ReplicatedControlDirectory) Revision() uint64 {
	if directory == nil {
		return 0
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	return directory.revision
}

func (directory *ReplicatedControlDirectory) CatalogGeneration() uint64 {
	if directory == nil {
		return 0
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	return directory.catalogGeneration
}

// Nodes returns the current physical cut. Historical records deliberately do
// not reappear here; callers scanning safe-to-stop references must inspect the
// catalog generation and current directory together.
func (directory *ReplicatedControlDirectory) Nodes() []NodeRecord {
	if directory == nil {
		return nil
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	result := make([]NodeRecord, 0, len(directory.current))
	for _, record := range directory.current {
		result = append(result, record)
	}
	slices.SortFunc(result, func(left, right NodeRecord) int {
		return bytes.Compare(left.NodeID[:], right.NodeID[:])
	})
	return result
}

// ShardControlEndpoints returns the union of current and historical physical
// control addresses. Retaining the union is what lets old drain obligations
// reach a departing process after its NodeRecord is removed.
func (directory *ReplicatedControlDirectory) ShardControlEndpoints() []ReplicatedEndpoint {
	if directory == nil {
		return nil
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	result := make([]ReplicatedEndpoint, 0, len(directory.historicalShards))
	for identity, address := range directory.historicalShards {
		result = append(result, ReplicatedEndpoint{Node: identity.node,
			NodeIncarnation: identity.incarnation, ControlAddress: address})
	}
	slices.SortFunc(result, func(left, right ReplicatedEndpoint) int {
		if compared := bytes.Compare(left.Node[:], right.Node[:]); compared != 0 {
			return compared
		}
		if left.NodeIncarnation < right.NodeIncarnation {
			return -1
		}
		if left.NodeIncarnation > right.NodeIncarnation {
			return 1
		}
		return 0
	})
	return result
}

// GatewayControlEndpoints returns only the current participant roster. An
// active drain certificate keeps its own immutable members and the opener's
// historical union remains available for those members.
func (directory *ReplicatedControlDirectory) GatewayControlEndpoints() []GatewayControlDirectoryEndpoint {
	if directory == nil {
		return nil
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	result := make([]GatewayControlDirectoryEndpoint, 0, len(directory.current))
	for _, record := range directory.current {
		if record.Lifecycle == NodeDecommissioned || record.Gateway.NodeID == (rafttransport.NodeID{}) {
			continue
		}
		result = append(result, GatewayControlDirectoryEndpoint{
			Member:           ClusterCatalogDrainMember{Node: record.Gateway.NodeID, Incarnation: record.Gateway.Incarnation},
			ServiceKeyDigest: record.Gateway.ServiceKeyDigest, ServiceID: record.Gateway.ServiceID,
			SessionID: record.Gateway.SessionID, SessionRevision: record.Gateway.SessionRevision,
			ParticipantDigest: record.Gateway.ParticipantDigest, Address: record.GatewayAddress,
			DirectoryRevision: record.Revision,
		})
	}
	slices.SortFunc(result, func(left, right GatewayControlDirectoryEndpoint) int {
		if compared := bytes.Compare(left.Member.Node[:], right.Member.Node[:]); compared != 0 {
			return compared
		}
		if left.Member.Incarnation < right.Member.Incarnation {
			return -1
		}
		if left.Member.Incarnation > right.Member.Incarnation {
			return 1
		}
		return bytes.Compare(left.SessionID[:], right.SessionID[:])
	})
	return result
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
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	result := make([]GatewayControlDirectoryEndpoint, 0, len(directory.historicalGateways))
	for _, endpoint := range directory.historicalGateways {
		result = append(result, endpoint)
	}
	slices.SortFunc(result, func(left, right GatewayControlDirectoryEndpoint) int {
		if compared := bytes.Compare(left.Member.Node[:], right.Member.Node[:]); compared != 0 {
			return compared
		}
		if left.Member.Incarnation < right.Member.Incarnation {
			return -1
		}
		if left.Member.Incarnation > right.Member.Incarnation {
			return 1
		}
		return bytes.Compare(left.SessionID[:], right.SessionID[:])
	})
	return result
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
