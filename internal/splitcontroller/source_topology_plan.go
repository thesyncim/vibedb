package splitcontroller

import (
	"bytes"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	vibejson "github.com/thesyncim/vibejson"
)

// openSourceTopologyPlan consumes only a catalog-committed intent and action
// witness. Global publications for other distributions do not invalidate an
// accepted action. Its affected distribution and every source command fence
// must still agree with the original immutable split authority.
func openSourceTopologyPlan(raw []byte, current *gateway.Snapshot, payload remoteStepPayload, observed Observation) (*Plan, error) {
	if current == nil || len(raw) == 0 || len(raw) > MaxPlanIntentBytes {
		return nil, ErrSourceTopology
	}
	var intent persistedPlanIntent
	if err := vibejson.Unmarshal(raw, &intent); err != nil {
		return nil, ErrSourceTopology
	}
	canonical, err := appendCanonicalVibeJSON(nil, &intent)
	if err != nil || !bytes.Equal(raw, canonical) || intent.SourceAuthority == nil || intent.SourceGeneration == 0 || intent.SourceGeneration == math.MaxUint64 ||
		len(intent.Children) < 2 || len(intent.Children) > autosplit.MaxSplitChildren || len(intent.Targets) != len(intent.Children)-1 ||
		(payload.Catalog != intent.SourceGeneration && payload.Catalog != intent.SourceGeneration+1) || current.Generation() < payload.Catalog {
		return nil, ErrSourceTopology
	}
	placement, ok := current.Placement(intent.Collection)
	if !ok || placement.Distribution != intent.Source.Distribution || !slices.Equal(placement.Columns, intent.Columns) {
		return nil, ErrSourceTopology
	}
	manifest, ok := current.Manifest(intent.Source.Distribution)
	if !ok {
		return nil, ErrSourceTopology
	}
	sourceManifest := manifest
	if payload.Catalog == intent.SourceGeneration+1 {
		sourceManifest, err = reconstructPersistedSourceManifest(manifest, intent.Source, intent.Children)
		if err != nil {
			return nil, ErrSourceTopology
		}
	}
	if sourceManifest.Version() != intent.Source.RoutingVersion {
		return nil, ErrSourceTopology
	}
	split, err := autosplit.RestoreSplitPlan(sourceManifest, intent.Source, intent.Retained, intent.Children)
	if err != nil {
		return nil, ErrSourceTopology
	}
	partitioner, err := rangesplit.NewPartitioner(split, intent.Collection, intent.Columns, intent.Source.BucketBits)
	if err != nil {
		return nil, ErrSourceTopology
	}
	targets := make([]ChildTarget, len(intent.Targets))
	for index, target := range intent.Targets {
		replicas := openPersistedChildReplicas(target.Replicas)
		if len(replicas) == 0 {
			return nil, ErrSourceTopology
		}
		targets[index] = ChildTarget{Child: target.Child, Endpoint: target.Endpoint, Replicas: replicas, ReplicaSetVersion: target.ReplicaSetVersion,
			RelationManifestDigest: target.RelationManifestDigest, TopologyRecoveryEpoch: target.TopologyRecoveryEpoch, Authority: target.Authority,
			WAL: replicas[0].WAL, SQL: replicas[0].SQL.Clone(), LocalIndexes: cloneSplitLocalIndexes(target.LocalIndexes)}
	}
	original := intent.SourceAuthority
	authority := PlanSourceAuthority{Group: original.Group, Command: original.Command, LogicalSchemaDigest: original.LogicalSchemaDigest,
		Schema: PlanSourceSchema{SQL: openPersistedSQLIdentity(original.SQL), Placement: sqldriver.ReplicatedPlacementProfile(original.Placement), LocalIndexes: cloneSplitLocalIndexes(original.LocalIndexes)}}
	plan, err := newPlan(sourceManifest, intent.SourceGeneration, split, partitioner, targets, &authority)
	if err != nil || plan.operation != OperationID(intent.Operation) {
		return nil, ErrSourceTopology
	}
	route, ok := current.ResolveReplicatedRoute(intent.Source.Distribution, intent.Source.Shard, nil)
	if !ok || route.Group != authority.Group || route.AllocationGeneration != uint64(intent.Source.AllocationGeneration) || route.LogicalSchemaDigest != authority.LogicalSchemaDigest ||
		route.Command != observed.SourceServing.Command || route.Command.ReplicaSetVersion != authority.Command.ReplicaSetVersion ||
		route.Command.ActivePolicyGeneration != authority.Command.ActivePolicyGeneration || route.Command.ProtectionEpoch != authority.Command.ProtectionEpoch ||
		route.Command.SchemaGeneration != authority.Command.SchemaGeneration || route.Command.RelationManifestDigest != authority.Command.RelationManifestDigest {
		return nil, ErrSourceTopology
	}
	if payload.Catalog == intent.SourceGeneration {
		if route.Command != authority.Command {
			return nil, ErrSourceTopology
		}
	} else if route.Command.OwnershipEpoch != uint64(plan.children[plan.retained].OwnershipEpoch) || route.Command.RoutingVersion != uint64(plan.targetManifest.Version()) || route.Command.RouteGeneration != plan.next {
		return nil, ErrSourceTopology
	}
	return plan, nil
}

func validSourceTopologyPruneCertificate(plan *Plan, payload remoteStepPayload, observed Observation, certificate gateway.RetainedPruneCertificate) bool {
	if observed.Certificate == nil || payload.Catalog != plan.next || plan.partitioner.VerifyCutoverCertificate(*observed.Certificate) != nil ||
		plan.partitioner.ValidateRetainedPruneAuthority(plan.targetManifest, plan.next, *observed.Certificate) != nil {
		return false
	}
	manifestDigest, err := gateway.DistributionManifestDigest(plan.targetManifest)
	if err != nil {
		return false
	}
	retained := plan.children[plan.retained].Range
	binding := gateway.RetainedPruneCertificateBinding{Generation: plan.next, Operation: [32]byte(plan.operation), PlanDigest: plan.partitioner.Digest(), CutoverDigest: observed.Certificate.Digest(), TargetManifestDigest: manifestDigest,
		RetainedRange: retained, RetainedRangeLineage: gateway.RetainedRangeLineageDigest(manifestDigest, retained)}
	return certificate.ValidFor(binding) && certificate.CatalogDrain().ValidFor(clusterPlanCatalogDrainRequest(planCatalogDrainRequest(plan, payload.CatalogDigest)))
}
