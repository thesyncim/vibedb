package rf3testfixture

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
)

var ErrDurableCatalogFixture = errors.New("rf3 test fixture: invalid durable catalog")

// DurableCatalogGroup describes one already prepared RF3 group. Data groups
// carry a base-table profile; the one ledger group carries authenticated home
// ranges and deliberately has no public table profile.
type DurableCatalogGroup struct {
	Route            gateway.ReplicatedRoute
	Table            string
	PrimaryKey       string
	Relation         replication.RelationID
	MaxKeyBytes      uint16
	MaxDocumentBytes uint32
	LedgerRanges     []gateway.DurableRequestLedgerRangeDescriptor
	EnrolledTarget   *gateway.ReplicatedEndpoint
}

// DurableCatalogOptions is the complete immutable catalog input shared by two
// gateway fixtures. AckKey is supplied once and returned byte-identically; the
// helper never derives a security capability from public topology identity.
type DurableCatalogOptions struct {
	Generation uint64
	Groups     []DurableCatalogGroup
	AckKey     gateway.DurableRequestAckDerivationKey
}

// DurableCatalog is a catalog snapshot plus the exact shared terminal ACK
// authority used by every gateway instance in one fault test.
type DurableCatalog struct {
	Snapshot *gateway.Snapshot
	AckKey   gateway.DurableRequestAckDerivationKey
}

// NewDurableCatalog constructs a catalog containing one or more user-data RF3
// groups and exactly one dedicated request-ledger RF3 group. Each fixture group
// owns a distinct one-shard distribution, which keeps its physical Raft route
// explicit while allowing the SQL adapter to address multiple logical tables.
func NewDurableCatalog(options DurableCatalogOptions) (DurableCatalog, error) {
	if options.Generation == 0 || len(options.Groups) < 2 ||
		options.AckKey == (gateway.DurableRequestAckDerivationKey{}) {
		return DurableCatalog{}, ErrDurableCatalogFixture
	}
	config := distribution.ClusterConfig{
		Distributions: make([]distribution.DistributionSpec, 0, len(options.Groups)),
		Placements:    make([]distribution.TablePlacement, 0, len(options.Groups)),
		Manifests:     make([]*distribution.Manifest, 0, len(options.Groups)),
	}
	endpoints := make(map[distribution.EndpointID]string, len(options.Groups)*gateway.ServingReplicaCount*3)
	descriptors := make([]gateway.ReplicatedShardDescriptor, 0, len(options.Groups))
	profiles := make([]gateway.ReplicatedTableProfile, 0, len(options.Groups)-1)
	seenDistributions := make(map[distribution.DistributionName]struct{}, len(options.Groups))
	seenTables := make(map[string]struct{}, len(options.Groups))
	ledgerGroups := 0
	for ordinal := range options.Groups {
		group := options.Groups[ordinal]
		route := group.Route
		if route.Distribution == "" || route.Shard == "" || route.AllocationGeneration == 0 ||
			!route.Command.Valid() || route.RangeIdentity == (replication.Digest{}) ||
			route.LineageDigest == (replication.Digest{}) ||
			route.ForwardingRuleDigest == (replication.Digest{}) ||
			len(route.Replicas) != gateway.ServingReplicaCount || group.Table == "" ||
			group.PrimaryKey == "" {
			return DurableCatalog{}, ErrDurableCatalogFixture
		}
		if _, exists := seenDistributions[route.Distribution]; exists {
			return DurableCatalog{}, ErrDurableCatalogFixture
		}
		seenDistributions[route.Distribution] = struct{}{}
		if _, exists := seenTables[group.Table]; exists {
			return DurableCatalog{}, ErrDurableCatalogFixture
		}
		seenTables[group.Table] = struct{}{}

		replicas := make([]gateway.ReplicatedReplicaDescriptor, 0, gateway.ServingReplicaCount)
		leaders := make([]distribution.EndpointID, 0, gateway.ServingReplicaCount)
		for replicaOrdinal := range route.Replicas {
			replica := route.Replicas[replicaOrdinal]
			dataAddress := firstAddress(replica.DataAddress, replica.Endpoint, replica.Address)
			nativeAddress := firstAddress(replica.NativeEndpoint, replica.Address)
			controlAddress := firstAddress(replica.ControlAddress, replica.ControlEndpoint, replica.Address)
			if replica.Member == 0 || dataAddress == "" || nativeAddress == "" || controlAddress == "" {
				return DurableCatalog{}, ErrDurableCatalogFixture
			}
			// The in-memory RF3 harness exposes only one real native listener.
			// Catalog identity still requires traffic-class-separated addresses;
			// retain the dialable native address and mint explicit non-dialed data
			// and control fixture namespaces when the route aliases them.
			if dataAddress == nativeAddress || dataAddress == controlAddress {
				dataAddress = "fixture-data/" + dataAddress
			}
			if controlAddress == nativeAddress || controlAddress == dataAddress {
				controlAddress = "fixture-control/" + controlAddress
			}
			dataID := distribution.EndpointID(fmt.Sprintf("fixture-g%d-m%d-data", ordinal, replica.Member))
			nativeID := distribution.EndpointID(fmt.Sprintf("fixture-g%d-m%d-native", ordinal, replica.Member))
			controlID := distribution.EndpointID(fmt.Sprintf("fixture-g%d-m%d-control", ordinal, replica.Member))
			endpoints[dataID], endpoints[nativeID], endpoints[controlID] = dataAddress, nativeAddress, controlAddress
			leaders = append(leaders, dataID)
			replicas = append(replicas, gateway.ReplicatedReplicaDescriptor{
				Member: replica.Member, Node: replica.Node, StoreID: replica.StoreID,
				NodeIncarnation: replica.NodeIncarnation, Endpoint: dataID,
				NativeEndpoint: nativeID, ControlEndpoint: controlID,
			})
		}
		manifest, err := distribution.NewManifest(route.Distribution,
			distribution.RoutingVersion(route.Command.RoutingVersion), []distribution.Shard{{
				ID:                   route.Shard,
				AllocationGeneration: distribution.ShardAllocationGeneration(route.AllocationGeneration),
				Range:                distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
				Leaders:              leaders, Epoch: distribution.OwnershipEpoch(route.Command.OwnershipEpoch),
			}})
		if err != nil {
			return DurableCatalog{}, errors.Join(ErrDurableCatalogFixture, err)
		}
		config.Distributions = append(config.Distributions, distribution.DistributionSpec{
			Name: route.Distribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion,
		})
		config.Placements = append(config.Placements, distribution.TablePlacement{
			Table: group.Table, Distribution: route.Distribution, Columns: []string{group.PrimaryKey},
		})
		config.Manifests = append(config.Manifests, manifest)
		descriptor := gateway.ReplicatedShardDescriptor{
			Distribution: route.Distribution, Shard: route.Shard, Group: route.Group,
			AllocationGeneration: distribution.ShardAllocationGeneration(route.AllocationGeneration),
			Command:              route.Command, RangeIdentity: route.RangeIdentity,
			LineageDigest: route.LineageDigest, ForwardingRuleDigest: route.ForwardingRuleDigest,
			RequestLedgerRanges: group.LedgerRanges, Replicas: replicas,
		}
		if group.EnrolledTarget != nil {
			target := *group.EnrolledTarget
			dataAddress := firstAddress(target.DataAddress, target.Endpoint, target.Address)
			nativeAddress := firstAddress(target.NativeEndpoint, target.Address)
			controlAddress := firstAddress(target.ControlAddress, target.ControlEndpoint, target.Address)
			if target.Member == 0 || dataAddress == "" || nativeAddress == "" || controlAddress == "" {
				return DurableCatalog{}, ErrDurableCatalogFixture
			}
			if dataAddress == nativeAddress || dataAddress == controlAddress {
				dataAddress = "fixture-data/" + dataAddress
			}
			if controlAddress == nativeAddress || controlAddress == dataAddress {
				controlAddress = "fixture-control/" + controlAddress
			}
			dataID := distribution.EndpointID(fmt.Sprintf("fixture-g%d-target-data", ordinal))
			nativeID := distribution.EndpointID(fmt.Sprintf("fixture-g%d-target-native", ordinal))
			controlID := distribution.EndpointID(fmt.Sprintf("fixture-g%d-target-control", ordinal))
			endpoints[dataID], endpoints[nativeID], endpoints[controlID] = dataAddress, nativeAddress, controlAddress
			descriptor.EnrolledTarget = &gateway.ReplicatedReplicaDescriptor{
				Member: target.Member, Node: target.Node, StoreID: target.StoreID,
				NodeIncarnation: target.NodeIncarnation, Endpoint: dataID,
				NativeEndpoint: nativeID, ControlEndpoint: controlID,
			}
		}
		descriptors = append(descriptors, descriptor)
		if len(group.LedgerRanges) != 0 {
			ledgerGroups++
			if group.Relation != 0 {
				return DurableCatalog{}, ErrDurableCatalogFixture
			}
			continue
		}
		if group.Relation == 0 || group.MaxKeyBytes == 0 || group.MaxDocumentBytes == 0 {
			return DurableCatalog{}, ErrDurableCatalogFixture
		}
		profiles = append(profiles, gateway.ReplicatedTableProfile{
			Table: group.Table, Relation: group.Relation, PrimaryKey: group.PrimaryKey,
			SchemaGeneration:       route.Command.SchemaGeneration,
			RelationManifestDigest: replication.Digest(route.Command.RelationManifestDigest),
			MaxKeyBytes:            group.MaxKeyBytes, MaxDocumentBytes: group.MaxDocumentBytes,
		})
	}
	if ledgerGroups != 1 || len(profiles) != len(options.Groups)-1 {
		return DurableCatalog{}, ErrDurableCatalogFixture
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, options.Generation, nil, nil, descriptors, profiles,
	)
	if err != nil {
		return DurableCatalog{}, errors.Join(ErrDurableCatalogFixture, err)
	}
	return DurableCatalog{Snapshot: snapshot, AckKey: options.AckKey}, nil
}

func firstAddress(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
