package splitcontroller

import (
	"bytes"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	vibejson "github.com/thesyncim/vibejson"
)

const MaxPlanIntentBytes = 40 << 10

var ErrPlanIntent = errors.New("splitcontroller: invalid persisted plan intent")

// persistedPlanIntent is the complete cold restart image for one split. It is
// kept inside the replicated operation record and is immutable across cursor
// revisions. No controller-local file or remembered recommendation is needed
// to reconstruct the exact plan after a process loss.
type persistedPlanIntent struct {
	Operation        [32]byte                 `json:"operation"`
	SourceGeneration uint64                   `json:"source_generation"`
	Collection       string                   `json:"collection"`
	Columns          []string                 `json:"columns"`
	Source           autosplit.SourceIdentity `json:"source"`
	Retained         uint8                    `json:"retained"`
	Children         []autosplit.SplitChild   `json:"children"`
	Targets          []persistedChildTarget   `json:"targets"`
}

type persistedLimits sqldriver.ReplicatedShardStoreLimits
type persistedBinding sqldriver.ReplicatedShardStoreBinding
type persistedSidecars sqldriver.ReplicatedShardStoreSidecarProfile
type persistedRelation sqldriver.ReplicatedShardRelationIdentity

type persistedSQLIdentity struct {
	Format                   uint16              `json:"format"`
	Binding                  persistedBinding    `json:"binding"`
	LogID                    [16]byte            `json:"log_id"`
	UserTable                string              `json:"user_table"`
	UserStorage              string              `json:"user_storage"`
	UserPrimaryKey           string              `json:"user_primary_key"`
	UserLimits               persistedLimits     `json:"user_limits"`
	Sidecars                 persistedSidecars   `json:"sidecars"`
	RelationCount            uint16              `json:"relation_count"`
	RelationSchemaGeneration uint64              `json:"relation_schema_generation"`
	RelationManifestDigest   [32]byte            `json:"relation_manifest_digest"`
	Relations                []persistedRelation `json:"relations"`
}

type persistedChildTarget struct {
	Child                 uint8                                `json:"child"`
	Endpoint              distribution.EndpointID              `json:"endpoint"`
	WAL                   raftstore.Identity                   `json:"wal"`
	TopologyRecoveryEpoch uint64                               `json:"topology_recovery_epoch"`
	Authority             sqldriver.ReplicatedAuthorityProfile `json:"authority"`
	SQL                   persistedSQLIdentity                 `json:"sql"`
}

// AppendPlanIntent appends the one canonical byte image used as replicated
// split authority. This is a cold operator/controller boundary; the serving
// route remains scalar and allocation-free.
func AppendPlanIntent(dst []byte, catalog *gateway.Snapshot, plan *Plan) ([]byte, error) {
	if catalog == nil || plan == nil || plan.sourceManifest == nil || plan.partitioner == nil ||
		(catalog.Generation() != plan.current && catalog.Generation() != plan.next) {
		return dst, ErrPlanIntent
	}
	placement, ok := catalog.Placement(plan.partitioner.CollectionName())
	if !ok || placement.Distribution != plan.source.Distribution {
		return dst, ErrPlanIntent
	}
	intent := persistedPlanIntent{
		Operation: [32]byte(plan.operation), SourceGeneration: plan.current,
		Collection: plan.partitioner.CollectionName(), Columns: placement.Columns,
		Source: plan.source, Retained: plan.retained,
		Children: make([]autosplit.SplitChild, plan.childCount),
		Targets:  make([]persistedChildTarget, 0, int(plan.childCount)-1),
	}
	for child := 0; child < int(plan.childCount); child++ {
		descriptor := plan.children[child]
		intent.Children[child] = autosplit.SplitChild{
			Range: descriptor.Range, Shard: descriptor.Shard,
			AllocationGeneration: descriptor.AllocationGeneration,
			OwnershipEpoch:       descriptor.OwnershipEpoch, Retained: descriptor.Retained,
		}
		for leader := 0; leader < int(plan.leaderCounts[child]); leader++ {
			// Resolve against the split range in the target manifest rather than
			// assuming the source shard was manifest ordinal zero.
			ordinal, ordinalOK := plan.targetManifest.ShardOrdinalForRange(descriptor.Range)
			if !ordinalOK {
				return dst, ErrPlanIntent
			}
			shard, _ := plan.targetManifest.ShardInfo(ordinal)
			if leader >= len(shard.Leaders) {
				return dst, ErrPlanIntent
			}
			intent.Children[child].Leaders = append(intent.Children[child].Leaders, shard.Leaders[leader])
		}
		if target, targetOK := plan.Target(uint8(child)); targetOK {
			intent.Targets = append(intent.Targets, persistedChildTarget{
				Child: target.Child, Endpoint: target.Endpoint, WAL: target.WAL,
				TopologyRecoveryEpoch: target.TopologyRecoveryEpoch,
				Authority:             target.Authority, SQL: persistSQLIdentity(target.SQL),
			})
		}
	}
	raw, err := vibejson.Marshal(&intent)
	if err != nil {
		return dst, err
	}
	start := len(dst)
	dst, err = vibejson.AppendCanonicalize(dst, raw)
	if err != nil || len(dst)-start == 0 || len(dst)-start > MaxPlanIntentBytes {
		return dst[:start], errors.Join(err, ErrPlanIntent)
	}
	return dst, nil
}

// OpenPlanIntent validates canonical uniqueness and reconstructs the exact
// immutable plan against the source or already-published catalog generation.
func OpenPlanIntent(raw []byte, catalog *gateway.Snapshot) (*Plan, error) {
	if len(raw) == 0 || len(raw) > MaxPlanIntentBytes || catalog == nil {
		return nil, ErrPlanIntent
	}
	var intent persistedPlanIntent
	if err := vibejson.Unmarshal(raw, &intent); err != nil {
		return nil, errors.Join(err, ErrPlanIntent)
	}
	canonical, err := vibejson.Marshal(&intent)
	if err != nil {
		return nil, errors.Join(err, ErrPlanIntent)
	}
	canonical, err = vibejson.AppendCanonicalize(nil, canonical)
	if err != nil || !bytes.Equal(raw, canonical) || intent.Operation == ([32]byte{}) ||
		intent.SourceGeneration == 0 || intent.Collection == "" || len(intent.Columns) == 0 ||
		len(intent.Children) < 2 || len(intent.Children) > autosplit.MaxSplitChildren ||
		len(intent.Targets) != len(intent.Children)-1 {
		return nil, errors.Join(err, ErrPlanIntent)
	}
	targetManifest, ok := catalog.Manifest(intent.Source.Distribution)
	if !ok {
		return nil, ErrPlanIntent
	}
	var sourceManifest *distribution.Manifest
	switch catalog.Generation() {
	case intent.SourceGeneration:
		sourceManifest = targetManifest
	case intent.SourceGeneration + 1:
		sourceManifest, err = reconstructPersistedSourceManifest(
			targetManifest, intent.Source, intent.Children,
		)
	default:
		return nil, ErrPlanIntent
	}
	if err != nil || sourceManifest.Version() != intent.Source.RoutingVersion {
		return nil, errors.Join(err, ErrPlanIntent)
	}
	split, err := autosplit.RestoreSplitPlan(
		sourceManifest, intent.Source, intent.Retained, intent.Children,
	)
	if err != nil {
		return nil, errors.Join(err, ErrPlanIntent)
	}
	partitioner, err := rangesplit.NewPartitioner(
		split, intent.Collection, intent.Columns, intent.Source.BucketBits,
	)
	if err != nil {
		return nil, errors.Join(err, ErrPlanIntent)
	}
	targets := make([]ChildTarget, len(intent.Targets))
	for index := range intent.Targets {
		target := &intent.Targets[index]
		targets[index] = ChildTarget{
			Child: target.Child, Endpoint: target.Endpoint, WAL: target.WAL,
			TopologyRecoveryEpoch: target.TopologyRecoveryEpoch,
			Authority:             target.Authority,
			SQL:                   openPersistedSQLIdentity(target.SQL),
		}
	}
	plan, err := RecoverPlan(
		catalog, intent.SourceGeneration, split, partitioner, targets,
	)
	if err != nil || [32]byte(plan.OperationID()) != intent.Operation {
		return nil, errors.Join(err, ErrPlanIntent)
	}
	return plan, nil
}

func persistSQLIdentity(identity sqldriver.ReplicatedShardStoreIdentity) persistedSQLIdentity {
	relations := make([]persistedRelation, len(identity.Relations))
	for index := range identity.Relations {
		relations[index] = persistedRelation(identity.Relations[index])
	}
	return persistedSQLIdentity{
		Format: identity.Format, Binding: persistedBinding(identity.Binding), LogID: identity.LogID,
		UserTable: identity.UserTable, UserStorage: identity.UserStorage,
		UserPrimaryKey: identity.UserPrimaryKey, UserLimits: persistedLimits(identity.UserLimits),
		Sidecars: persistedSidecars(identity.Sidecars), RelationCount: identity.RelationCount,
		RelationSchemaGeneration: identity.RelationSchemaGeneration,
		RelationManifestDigest:   identity.RelationManifestDigest, Relations: relations,
	}
}

func openPersistedSQLIdentity(identity persistedSQLIdentity) sqldriver.ReplicatedShardStoreIdentity {
	relations := make([]sqldriver.ReplicatedShardRelationIdentity, len(identity.Relations))
	for index := range identity.Relations {
		relations[index] = sqldriver.ReplicatedShardRelationIdentity(identity.Relations[index])
	}
	return sqldriver.ReplicatedShardStoreIdentity{
		Format: identity.Format, Binding: sqldriver.ReplicatedShardStoreBinding(identity.Binding),
		LogID: identity.LogID, UserTable: identity.UserTable, UserStorage: identity.UserStorage,
		UserPrimaryKey:           identity.UserPrimaryKey,
		UserLimits:               sqldriver.ReplicatedShardStoreLimits(identity.UserLimits),
		Sidecars:                 sqldriver.ReplicatedShardStoreSidecarProfile(identity.Sidecars),
		RelationCount:            identity.RelationCount,
		RelationSchemaGeneration: identity.RelationSchemaGeneration,
		RelationManifestDigest:   identity.RelationManifestDigest, Relations: relations,
	}
}

func reconstructPersistedSourceManifest(
	target *distribution.Manifest,
	source autosplit.SourceIdentity,
	children []autosplit.SplitChild,
) (*distribution.Manifest, error) {
	if target == nil || target.Distribution() != source.Distribution ||
		target.Version() != source.RoutingVersion+1 || len(children) < 2 ||
		len(children) > autosplit.MaxSplitChildren {
		return nil, ErrPlanIntent
	}
	start := -1
	for ordinal := 0; ordinal < target.ShardCount(); ordinal++ {
		metadata, ok := target.ShardMetadataAt(ordinal)
		if ok && metadata.Range.Start == source.Range.Start {
			start = ordinal
			break
		}
	}
	if start < 0 || start+len(children) > target.ShardCount() {
		return nil, ErrPlanIntent
	}
	for index := range children {
		shard, ok := target.ShardInfo(start + index)
		child := children[index]
		if !ok || shard.ID != child.Shard ||
			shard.AllocationGeneration != child.AllocationGeneration || shard.Range != child.Range ||
			shard.Epoch != child.OwnershipEpoch || !slices.Equal(shard.Leaders, child.Leaders) {
			return nil, ErrPlanIntent
		}
	}
	retained := -1
	for index := range children {
		if children[index].Retained {
			if retained >= 0 {
				return nil, ErrPlanIntent
			}
			retained = index
		}
	}
	if retained < 0 || source.OwnershipEpoch == ^distribution.OwnershipEpoch(0) ||
		children[retained].Shard != source.Shard ||
		children[retained].AllocationGeneration != source.AllocationGeneration ||
		children[retained].OwnershipEpoch != source.OwnershipEpoch+1 {
		return nil, ErrPlanIntent
	}
	shards := make([]distribution.Shard, 0, target.ShardCount()-len(children)+1)
	for ordinal := 0; ordinal < start; ordinal++ {
		shard, _ := target.ShardInfo(ordinal)
		shards = append(shards, shard)
	}
	shards = append(shards, distribution.Shard{
		ID: source.Shard, AllocationGeneration: source.AllocationGeneration,
		Range: source.Range, Leaders: children[retained].Leaders, Epoch: source.OwnershipEpoch,
	})
	for ordinal := start + len(children); ordinal < target.ShardCount(); ordinal++ {
		shard, _ := target.ShardInfo(ordinal)
		shards = append(shards, shard)
	}
	manifest, err := distribution.NewManifest(source.Distribution, source.RoutingVersion, shards)
	if err != nil {
		return nil, errors.Join(err, ErrPlanIntent)
	}
	return manifest, nil
}
