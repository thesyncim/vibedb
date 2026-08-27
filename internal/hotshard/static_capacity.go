package hotshard

import (
	"bytes"
	"errors"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/topologyscheduler"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	StaticCapacityFormat   = 1
	MaxStaticCapacityBytes = 1 << 20
)

// StaticCapacityConfig is explicit provisioning evidence for development and
// deterministic tests. It supplies like-unit window and node ceilings; it does
// not claim live utilization, liveness, serving authority, or spare replicas.
type StaticCapacityConfig struct {
	Format              uint16                   `json:"format"`
	RecorderLanes       uint8                    `json:"recorder_lanes"`
	WindowCapacity      autosplit.CapacityVector `json:"window_capacity"`
	NodeCapacity        autosplit.CapacityVector `json:"node_capacity"`
	MigrationCapacity   uint64                   `json:"migration_capacity"`
	ShardMigrationBytes uint64                   `json:"shard_migration_bytes"`
	MaxReceives         uint16                   `json:"max_receives"`
	Nodes               []StaticCapacityNode     `json:"nodes"`
}

type StaticCapacityNode struct {
	Endpoint      distribution.EndpointID `json:"endpoint"`
	FailureDomain uint32                  `json:"failure_domain"`
	// Capacity, when present, is the provisioned ceiling of this node. Omitting
	// it preserves the homogeneous NodeCapacity default and the format-1 wire
	// image. It is never inferred from the demand being moved.
	Capacity *autosplit.CapacityVector `json:"capacity,omitempty"`
}

func AppendStaticCapacityConfig(dst []byte, config StaticCapacityConfig) ([]byte, error) {
	if !validStaticCapacityConfig(config) {
		return dst, ErrInvalidPressureCut
	}
	raw, err := vibejson.Marshal(&config)
	if err != nil || len(raw) == 0 || len(raw) > MaxStaticCapacityBytes {
		return dst, errors.Join(err, ErrInvalidPressureCut)
	}
	return append(dst, raw...), nil
}

func OpenStaticCapacityConfig(raw []byte) (StaticCapacityConfig, error) {
	if len(raw) == 0 || len(raw) > MaxStaticCapacityBytes {
		return StaticCapacityConfig{}, ErrInvalidPressureCut
	}
	var config StaticCapacityConfig
	if err := vibejson.Unmarshal(raw, &config); err != nil || !validStaticCapacityConfig(config) {
		return StaticCapacityConfig{}, errors.Join(err, ErrInvalidPressureCut)
	}
	canonical, err := AppendStaticCapacityConfig(nil, config)
	if err != nil || !bytes.Equal(canonical, raw) {
		return StaticCapacityConfig{}, errors.Join(err, ErrInvalidPressureCut)
	}
	return config, nil
}

func validStaticCapacityConfig(config StaticCapacityConfig) bool {
	if config.Format != StaticCapacityFormat || config.RecorderLanes == 0 ||
		config.RecorderLanes > autosplit.MaxRecorderLanes || len(config.Nodes) == 0 ||
		len(config.Nodes) > topologyscheduler.MaxPlacementNodes || config.MigrationCapacity == 0 ||
		config.ShardMigrationBytes == 0 || config.ShardMigrationBytes > config.MigrationCapacity ||
		config.MaxReceives == 0 {
		return false
	}
	for resource := range autosplit.ResourceCount {
		if config.WindowCapacity[resource] == 0 || config.NodeCapacity[resource] == 0 {
			return false
		}
	}
	for index := range config.Nodes {
		node := config.Nodes[index]
		if node.Endpoint == "" || node.FailureDomain == 0 ||
			index != 0 && config.Nodes[index-1].Endpoint >= node.Endpoint {
			return false
		}
		if node.Capacity != nil {
			for _, ceiling := range *node.Capacity {
				if ceiling == 0 {
					return false
				}
			}
		}
	}
	return true
}

type StaticCapacityProvider struct {
	Capacity       autosplit.CapacityVector
	MigrationBytes uint64
}

func (provider StaticCapacityProvider) PressureCapacity(pulse autosplit.Pulse) (
	autosplit.CapacitySet, autosplit.CapacityVector, uint64, bool,
) {
	if pulse.Source == (autosplit.SourceIdentity{}) || pulse.Sequence == 0 || provider.MigrationBytes == 0 {
		return autosplit.CapacitySet{}, autosplit.CapacityVector{}, 0, false
	}
	set := autosplit.CapacitySet{Source: pulse.Source, WindowSequence: pulse.Sequence,
		Current: provider.Capacity, Left: provider.Capacity,
		Right: provider.Capacity, Isolated: provider.Capacity}
	var demand autosplit.CapacityVector
	for resource := range autosplit.ResourceCount {
		if provider.Capacity[resource] == 0 {
			return autosplit.CapacitySet{}, autosplit.CapacityVector{}, 0, false
		}
		demand[resource] = pulse.Total[resource]
	}
	return set, demand, provider.MigrationBytes, true
}

func (config StaticCapacityConfig) NodeCapacities(generation uint64) []topologyscheduler.NodeCapacity {
	if generation == 0 || !validStaticCapacityConfig(config) {
		return nil
	}
	nodes := make([]topologyscheduler.NodeCapacity, len(config.Nodes))
	for index := range config.Nodes {
		capacity := config.NodeCapacity
		if config.Nodes[index].Capacity != nil {
			capacity = *config.Nodes[index].Capacity
		}
		nodes[index] = topologyscheduler.NodeCapacity{CatalogGeneration: generation,
			Endpoint: config.Nodes[index].Endpoint, FailureDomain: config.Nodes[index].FailureDomain,
			Flags: topologyscheduler.NodePlacementReady, Capacity: capacity,
			MigrationCapacity: config.MigrationCapacity, MaxReceives: config.MaxReceives}
	}
	return nodes
}
