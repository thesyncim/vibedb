package hotshard

import (
	"bytes"
	"errors"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/topologyscheduler"
	vibejson "github.com/thesyncim/vibejson"
)

const MaxViewBytes = 2 << 20

type persistedView struct {
	AuthorityRevision uint64            `json:"authority_revision"`
	CatalogGeneration uint64            `json:"catalog_generation"`
	Nodes             []persistedNode   `json:"nodes"`
	Reports           []persistedReport `json:"reports"`
}

type persistedGroup struct {
	ClusterID             []byte `json:"cluster_id"`
	ClusterIncarnation    []byte `json:"cluster_incarnation"`
	GroupID               []byte `json:"group_id"`
	ShardIncarnation      []byte `json:"shard_incarnation"`
	TopologyRecoveryEpoch uint64 `json:"topology_recovery_epoch"`
}

type persistedSource struct {
	AllocationGeneration uint64 `json:"allocation_generation"`
	BucketBits           uint8  `json:"bucket_bits"`
	Distribution         string `json:"distribution"`
	End                  []byte `json:"end"`
	EndMax               bool   `json:"end_max"`
	OwnershipEpoch       uint64 `json:"ownership_epoch"`
	RoutingVersion       uint64 `json:"routing_version"`
	Shard                string `json:"shard"`
	Start                []byte `json:"start"`
}

type persistedReport struct {
	BenefitPPM           uint64                   `json:"benefit_ppm"`
	Boundaries           [][]byte                 `json:"boundaries"`
	BoundaryCount        uint8                    `json:"boundary_count"`
	CandidateBin         uint8                    `json:"candidate_bin"`
	CurrentPressurePPM   uint64                   `json:"current_pressure_ppm"`
	Demand               autosplit.CapacityVector `json:"demand"`
	FanoutTaxPPM         uint64                   `json:"fanout_tax_ppm"`
	Group                persistedGroup           `json:"group"`
	HotBucketStart       []byte                   `json:"hot_bucket_start"`
	Kind                 uint8                    `json:"kind"`
	MigrationBytes       uint64                   `json:"migration_bytes"`
	MigrationTaxPPM      uint64                   `json:"migration_tax_ppm"`
	PredictedPressurePPM uint64                   `json:"predicted_pressure_ppm"`
	Reason               uint8                    `json:"reason"`
	Source               persistedSource          `json:"source"`
	WindowSequence       uint64                   `json:"window_sequence"`
}

type persistedNode struct {
	ActiveReceives    uint16                   `json:"active_receives"`
	Capacity          autosplit.CapacityVector `json:"capacity"`
	CatalogGeneration uint64                   `json:"catalog_generation"`
	Endpoint          string                   `json:"endpoint"`
	FailureDomain     uint32                   `json:"failure_domain"`
	Flags             uint8                    `json:"flags"`
	MaxReceives       uint16                   `json:"max_receives"`
	MigrationCapacity uint64                   `json:"migration_capacity"`
	MigrationUsed     uint64                   `json:"migration_used"`
	Used              autosplit.CapacityVector `json:"used"`
}

func AppendView(dst []byte, view View) ([]byte, error) {
	if view.CatalogGeneration == 0 || view.AuthorityRevision == 0 ||
		len(view.Reports) > MaxReports || len(view.Nodes) > topologyscheduler.MaxPlacementNodes ||
		!orderedReports(view.Reports) {
		return dst, ErrInvalidPressureCut
	}
	persisted := persistedView{CatalogGeneration: view.CatalogGeneration,
		AuthorityRevision: view.AuthorityRevision,
		Reports:           make([]persistedReport, len(view.Reports)),
		Nodes:             make([]persistedNode, len(view.Nodes))}
	for index := range view.Reports {
		persisted.Reports[index] = persistReport(view.Reports[index])
	}
	for index := range view.Nodes {
		node := view.Nodes[index]
		persisted.Nodes[index] = persistedNode{CatalogGeneration: node.CatalogGeneration,
			Endpoint: string(node.Endpoint), FailureDomain: node.FailureDomain,
			Flags: uint8(node.Flags), Capacity: node.Capacity, Used: node.Used,
			MigrationCapacity: node.MigrationCapacity, MigrationUsed: node.MigrationUsed,
			MaxReceives: node.MaxReceives, ActiveReceives: node.ActiveReceives}
	}
	raw, err := vibejson.Marshal(&persisted)
	if err != nil || len(raw) == 0 || len(raw) > MaxViewBytes {
		return dst, errors.Join(err, ErrInvalidPressureCut)
	}
	return append(dst, raw...), nil
}

func OpenView(raw []byte) (View, error) {
	if len(raw) == 0 || len(raw) > MaxViewBytes {
		return View{}, ErrInvalidPressureCut
	}
	var persisted persistedView
	if err := vibejson.Unmarshal(raw, &persisted); err != nil ||
		persisted.CatalogGeneration == 0 || persisted.AuthorityRevision == 0 ||
		len(persisted.Reports) > MaxReports || len(persisted.Nodes) > topologyscheduler.MaxPlacementNodes {
		return View{}, errors.Join(err, ErrInvalidPressureCut)
	}
	view := View{CatalogGeneration: persisted.CatalogGeneration,
		AuthorityRevision: persisted.AuthorityRevision,
		Reports:           make([]Report, len(persisted.Reports)),
		Nodes:             make([]topologyscheduler.NodeCapacity, len(persisted.Nodes))}
	for index := range persisted.Reports {
		report, err := openReport(persisted.Reports[index])
		if err != nil {
			return View{}, err
		}
		view.Reports[index] = report
	}
	for index := range persisted.Nodes {
		node := persisted.Nodes[index]
		view.Nodes[index] = topologyscheduler.NodeCapacity{
			CatalogGeneration: node.CatalogGeneration,
			Endpoint:          distribution.EndpointID(node.Endpoint), FailureDomain: node.FailureDomain,
			Flags: topologyscheduler.NodeFlags(node.Flags), Capacity: node.Capacity, Used: node.Used,
			MigrationCapacity: node.MigrationCapacity, MigrationUsed: node.MigrationUsed,
			MaxReceives: node.MaxReceives, ActiveReceives: node.ActiveReceives}
	}
	if !orderedReports(view.Reports) {
		return View{}, ErrInvalidPressureCut
	}
	canonical, err := AppendView(nil, view)
	if err != nil || !bytes.Equal(canonical, raw) {
		return View{}, errors.Join(err, ErrInvalidPressureCut)
	}
	return view, nil
}

func persistReport(report Report) persistedReport {
	rec := report.Recommendation
	boundaries := make([][]byte, int(rec.BoundaryCount))
	for index := range boundaries {
		boundaries[index] = rec.Boundaries[index][:]
	}
	return persistedReport{Group: persistGroup(report.Group), Source: persistSource(rec.Source),
		WindowSequence: rec.WindowSequence, Kind: uint8(rec.Kind), Reason: uint8(rec.Reason),
		Boundaries: boundaries, BoundaryCount: rec.BoundaryCount, CandidateBin: rec.CandidateBin,
		HotBucketStart: rec.HotBucketStart[:], CurrentPressurePPM: rec.CurrentPressurePPM,
		PredictedPressurePPM: rec.PredictedPressurePPM, BenefitPPM: rec.BenefitPPM,
		FanoutTaxPPM: rec.FanoutTaxPPM, MigrationTaxPPM: rec.MigrationTaxPPM,
		Demand: report.Demand, MigrationBytes: report.MigrationBytes}
}

func openReport(value persistedReport) (Report, error) {
	group, ok := openGroup(value.Group)
	source, sourceOK := openSource(value.Source)
	if !ok || !sourceOK || value.BoundaryCount > 2 || len(value.Boundaries) != int(value.BoundaryCount) ||
		len(value.HotBucketStart) != distribution.KeyspaceWidth ||
		autosplit.RecommendationKind(value.Kind) > autosplit.RecommendationUnsplittableBucket ||
		autosplit.Reason(value.Reason) > autosplit.ReasonNoBenefit {
		return Report{}, ErrInvalidPressureCut
	}
	rec := autosplit.Recommendation{Source: source, WindowSequence: value.WindowSequence,
		Kind: autosplit.RecommendationKind(value.Kind), Reason: autosplit.Reason(value.Reason),
		BoundaryCount: value.BoundaryCount, CandidateBin: value.CandidateBin,
		CurrentPressurePPM:   value.CurrentPressurePPM,
		PredictedPressurePPM: value.PredictedPressurePPM, BenefitPPM: value.BenefitPPM,
		FanoutTaxPPM: value.FanoutTaxPPM, MigrationTaxPPM: value.MigrationTaxPPM}
	copy(rec.HotBucketStart[:], value.HotBucketStart)
	for index := range value.Boundaries {
		if len(value.Boundaries[index]) != distribution.KeyspaceWidth {
			return Report{}, ErrInvalidPressureCut
		}
		copy(rec.Boundaries[index][:], value.Boundaries[index])
	}
	return Report{Group: group, Recommendation: rec, Demand: value.Demand,
		MigrationBytes: value.MigrationBytes}, nil
}

func persistGroup(group raftmember.GroupKey) persistedGroup {
	return persistedGroup{ClusterID: group.ClusterID[:], ClusterIncarnation: group.ClusterIncarnation[:],
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
		ShardIncarnation:      group.ShardIncarnation[:], GroupID: group.GroupID[:]}
}
func openGroup(value persistedGroup) (raftmember.GroupKey, bool) {
	var group raftmember.GroupKey
	if len(value.ClusterID) != 16 || len(value.ClusterIncarnation) != 16 ||
		len(value.ShardIncarnation) != 16 || len(value.GroupID) != 16 || value.TopologyRecoveryEpoch == 0 {
		return group, false
	}
	copy(group.ClusterID[:], value.ClusterID)
	copy(group.ClusterIncarnation[:], value.ClusterIncarnation)
	copy(group.ShardIncarnation[:], value.ShardIncarnation)
	copy(group.GroupID[:], value.GroupID)
	group.TopologyRecoveryEpoch = value.TopologyRecoveryEpoch
	return group, group != (raftmember.GroupKey{})
}
func persistSource(source autosplit.SourceIdentity) persistedSource {
	return persistedSource{Distribution: string(source.Distribution), Shard: string(source.Shard),
		AllocationGeneration: uint64(source.AllocationGeneration), Start: source.Range.Start[:],
		End: source.Range.End.Point[:], EndMax: source.Range.End.Max, BucketBits: source.BucketBits,
		RoutingVersion: uint64(source.RoutingVersion), OwnershipEpoch: uint64(source.OwnershipEpoch)}
}
func openSource(value persistedSource) (autosplit.SourceIdentity, bool) {
	if len(value.Start) != distribution.KeyspaceWidth || len(value.End) != distribution.KeyspaceWidth {
		return autosplit.SourceIdentity{}, false
	}
	var source autosplit.SourceIdentity
	source.Distribution, source.Shard = distribution.DistributionName(value.Distribution), distribution.ShardID(value.Shard)
	source.AllocationGeneration = distribution.ShardAllocationGeneration(value.AllocationGeneration)
	copy(source.Range.Start[:], value.Start)
	copy(source.Range.End.Point[:], value.End)
	source.Range.End.Max = value.EndMax
	source.BucketBits = value.BucketBits
	source.RoutingVersion = distribution.RoutingVersion(value.RoutingVersion)
	source.OwnershipEpoch = distribution.OwnershipEpoch(value.OwnershipEpoch)
	checkpoint := autosplit.TrackerCheckpoint{Source: source, LastSequence: 1}
	_, ok := autosplit.RestoreTracker(checkpoint)
	return source, ok
}
