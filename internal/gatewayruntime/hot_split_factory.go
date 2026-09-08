package gatewayruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

const gatewayHotSplitPrepareAttempts = 3

type gatewayHotSplitFactory struct {
	// Source versions are retained for the process lifetime. A later certified
	// schema/allocation version cannot replace an entry needed by an older
	// pending admission.
	sourceVersions map[gatewayHotSplitSourceKey]gatewaySplitSource
	shards         []gateway.ReplicatedEndpoint
	splitSnapshots []string
	mu             sync.RWMutex
}

type gatewayHotSplitSourceKey struct {
	Group                  raftmember.GroupKey
	AllocationGeneration   uint64
	SchemaGeneration       uint64
	RelationManifestDigest [32]byte
}

type gatewayProvisionedSplitSource struct {
	Fragment *gateway.Snapshot
	Source   gatewaySplitSource
}

func newGatewayHotSplitFactory(
	manifest gatewayReplicaControlManifest,
	catalog *gateway.Snapshot,
	provisioned ...gatewayProvisionedSplitSource,
) (*gatewayHotSplitFactory, error) {
	sources, err := gatewayHotSplitSources(manifest, catalog)
	if err != nil {
		return nil, err
	}
	factory := &gatewayHotSplitFactory{
		sourceVersions: make(map[gatewayHotSplitSourceKey]gatewaySplitSource, len(sources)+len(provisioned)),
		shards:         slices.Clone(manifest.Shards),
		splitSnapshots: slices.Clone(manifest.SplitSnapshots),
	}
	for _, source := range sources {
		if err := factory.registerSource(catalog, source); err != nil {
			return nil, err
		}
	}
	for _, source := range provisioned {
		if err := factory.registerSource(source.Fragment, source.Source); err != nil {
			return nil, err
		}
	}
	return factory, nil
}

// RegisterProvisionedSource installs one source proved by a durable table
// provision fragment. Validation is performed against the exact fragment and
// the immutable enrolled roster captured at startup; it does not refresh the
// serving catalog. Registration is idempotent for identical bytes and keeps
// distinct source versions alive for pending admissions.
func (factory *gatewayHotSplitFactory) RegisterProvisionedSource(fragment *gateway.Snapshot, source gatewaySplitSource) error {
	if factory == nil || fragment == nil {
		return errGatewayHotSplitSourceRegistration
	}
	return factory.registerSource(fragment, source)
}

func (factory *gatewayHotSplitFactory) registerSource(fragment *gateway.Snapshot, source gatewaySplitSource) error {
	if factory == nil || fragment == nil {
		return errGatewayHotSplitSourceRegistration
	}
	if len(factory.shards) == 0 || len(factory.shards) != len(factory.splitSnapshots) {
		return errGatewayHotSplitSourceRegistration
	}
	for index, shard := range factory.shards {
		if shard.Node == (rafttransport.NodeID{}) ||
			shard.ControlAddress == "" || !validGatewayReplicaAddress(shard.ControlAddress) ||
			!validGatewayReplicaAddress(factory.splitSnapshots[index]) {
			return errGatewayHotSplitSourceRegistration
		}
		if index > 0 && bytes.Compare(factory.shards[index-1].Node[:], shard.Node[:]) >= 0 {
			return errGatewayHotSplitSourceRegistration
		}
	}
	manifest := gatewayReplicaControlManifest{
		Shards:         slices.Clone(factory.shards),
		SplitSnapshots: slices.Clone(factory.splitSnapshots),
		SplitSources:   []gatewaySplitSource{source},
	}
	// The source fragment is immutable evidence for this table. Bind every
	// catalog control endpoint to the exact enrolled roster before validating
	// or publishing the source; syntax and node membership alone are not enough.
	if err := manifest.ValidateCatalog(fragment); err != nil {
		return errors.Join(errGatewayHotSplitSourceRegistration, err)
	}
	validated, err := gatewayHotSplitSources(manifest, fragment)
	if err != nil {
		return errors.Join(errGatewayHotSplitSourceRegistration, err)
	}
	validatedSource, found := validated[source.Group]
	if !found {
		return errGatewayHotSplitSourceRegistration
	}
	key := gatewayHotSplitSourceVersionKey(validatedSource)
	factory.mu.Lock()
	defer factory.mu.Unlock()
	if factory.sourceVersions == nil {
		factory.sourceVersions = make(map[gatewayHotSplitSourceKey]gatewaySplitSource)
	}
	if existing, exists := factory.sourceVersions[key]; exists {
		if !reflect.DeepEqual(existing, validatedSource) {
			return errGatewayHotSplitSourceRegistration
		}
		return nil
	}
	if len(factory.sourceVersions) >= maxGatewaySplitSources {
		return errGatewayHotSplitSourceRegistration
	}
	for existingKey, existing := range factory.sourceVersions {
		if existingKey.Group == key.Group {
			continue
		}
		for _, left := range existing.Replicas {
			for _, right := range validatedSource.Replicas {
				if left.Node == right.Node && left.Root == right.Root {
					return errGatewayHotSplitSourceRegistration
				}
			}
		}
	}
	owned := cloneGatewayHotSplitSource(validatedSource)
	factory.sourceVersions[key] = owned
	return nil
}

var errGatewayHotSplitSourceRegistration = errors.New("gatewayruntime: invalid provisioned hot split source registration")

func gatewayHotSplitSourceVersionKey(source gatewaySplitSource) gatewayHotSplitSourceKey {
	return gatewayHotSplitSourceKey{
		Group: source.Group, AllocationGeneration: source.SQL.Binding.AllocationGeneration,
		SchemaGeneration: source.SchemaGeneration, RelationManifestDigest: source.RelationManifestDigest,
	}
}

func cloneGatewayHotSplitSource(source gatewaySplitSource) gatewaySplitSource {
	source.Table = strings.Clone(source.Table)
	source.SQL = source.SQL.Clone()
	source.LocalIndexes = cloneGatewaySplitIndexes(source.LocalIndexes)
	source.Template.ShardKey = strings.Clone(source.Template.ShardKey)
	for index := range source.Replicas {
		source.Replicas[index].Root = strings.Clone(source.Replicas[index].Root)
		source.Replicas[index].Snapshot = strings.Clone(source.Replicas[index].Snapshot)
	}
	return source
}

func (factory *gatewayHotSplitFactory) sourceForDescriptor(source gateway.ReplicatedShardDescriptor) (gatewaySplitSource, bool) {
	if factory == nil {
		return gatewaySplitSource{}, false
	}
	key := gatewayHotSplitSourceKey{Group: source.Group, AllocationGeneration: uint64(source.AllocationGeneration),
		SchemaGeneration: source.Command.SchemaGeneration, RelationManifestDigest: source.Command.RelationManifestDigest}
	factory.mu.RLock()
	defer factory.mu.RUnlock()
	configuration, found := factory.sourceVersions[key]
	return configuration, found
}

func (factory *gatewayHotSplitFactory) BuildHotSplitPlan(
	ctx context.Context,
	catalog *gateway.Snapshot,
	admission [32]byte,
	work hotshard.SplitWork,
) (*splitcontroller.Plan, error) {
	if factory == nil || ctx == nil || catalog == nil || admission == ([32]byte{}) ||
		work.Candidate.CatalogGeneration != catalog.Generation() ||
		!work.Candidate.Recommendation.Actionable() {
		return nil, hotshard.ErrInvalidPressureCut
	}
	source, profile, placement, err := gatewayHotSplitSource(catalog, work)
	if err != nil {
		return nil, err
	}
	split, err := factory.allocateSplit(catalog, admission, work, source)
	if err != nil {
		return nil, err
	}
	partitioner, err := rangesplit.NewPartitioner(
		split, profile.Table, placement.Columns, work.Candidate.Recommendation.Source.BucketBits,
	)
	if err != nil {
		return nil, err
	}
	operation, err := splitcontroller.OperationIDForSplit(catalog.Generation(), split, partitioner)
	if err != nil {
		return nil, err
	}
	targets := make([]splitcontroller.ChildTarget, 0, int(split.ChildCount)-1)
	for child := uint8(0); child < split.ChildCount; child++ {
		descriptor, _ := split.Child(int(child))
		if descriptor.Retained {
			continue
		}
		target, buildErr := factory.buildChildTarget(
			catalog, [32]byte(operation), child, descriptor, source, profile,
		)
		if buildErr != nil {
			return nil, buildErr
		}
		targets = append(targets, target)
	}
	configuration, found := factory.sourceForDescriptor(source)
	if !found {
		return nil, hotshard.ErrInvalidPressureCut
	}
	plan, err := splitcontroller.NewPlan(catalog, split, partitioner, targets, splitcontroller.PlanSourceSchema{
		SQL: configuration.SQL, Placement: configuration.Placement, LocalIndexes: configuration.LocalIndexes,
	})
	if err != nil {
		return nil, err
	}
	if plan.OperationID() != operation {
		return nil, hotshard.ErrInvalidPressureCut
	}
	return plan, nil
}

func gatewayHotSplitSource(
	catalog *gateway.Snapshot, work hotshard.SplitWork,
) (gateway.ReplicatedShardDescriptor, gateway.ReplicatedTableProfile, distribution.TablePlacement, error) {
	recommendation := work.Candidate.Recommendation
	var source gateway.ReplicatedShardDescriptor
	foundSource := false
	for _, descriptor := range catalog.ReplicatedShardDescriptors() {
		if descriptor.Group == work.Group && descriptor.Distribution == recommendation.Source.Distribution &&
			descriptor.Shard == recommendation.Source.Shard &&
			descriptor.AllocationGeneration == recommendation.Source.AllocationGeneration {
			source, foundSource = descriptor, true
			break
		}
	}
	if !foundSource || len(source.Replicas) != gateway.ServingReplicaCount || !source.Command.Valid() {
		return source, gateway.ReplicatedTableProfile{}, distribution.TablePlacement{}, hotshard.ErrInvalidPressureCut
	}
	var profile gateway.ReplicatedTableProfile
	var placement distribution.TablePlacement
	count := 0
	for _, candidate := range catalog.ReplicatedTableProfiles() {
		candidatePlacement, ok := catalog.Placement(candidate.Table)
		if ok && candidatePlacement.Distribution == source.Distribution && candidate.Relation == 1 {
			profile, placement, count = candidate, candidatePlacement, count+1
		}
	}
	if count != 1 || profile.Relation != 1 || profile.SchemaGeneration != source.Command.SchemaGeneration ||
		profile.LogicalSchemaDigest != source.LogicalSchemaDigest ||
		len(placement.Columns) != 1 {
		return source, profile, placement, hotshard.ErrInvalidPressureCut
	}
	return source, profile, placement, nil
}

func (factory *gatewayHotSplitFactory) allocateSplit(
	catalog *gateway.Snapshot,
	admission [32]byte,
	work hotshard.SplitWork,
	source gateway.ReplicatedShardDescriptor,
) (*autosplit.SplitPlan, error) {
	next, ok := catalog.NextShardAllocationGeneration(source.Distribution)
	if !ok {
		return nil, hotshard.ErrInvalidPressureCut
	}
	destinations := make([]autosplit.Destination, work.Candidate.Recommendation.BoundaryCount)
	leaders := make([]distribution.EndpointID, len(source.Replicas))
	for index := range source.Replicas {
		leaders[index] = source.Replicas[index].NativeEndpoint
	}
	for index := range destinations {
		destinations[index] = autosplit.Destination{
			Shard:                gatewayHotSplitShardID(admission, uint8(index+1)),
			AllocationGeneration: next + distribution.ShardAllocationGeneration(index),
			OwnershipEpoch:       1, Leaders: append([]distribution.EndpointID(nil), leaders...),
		}
	}
	manifest, _ := catalog.Manifest(source.Distribution)
	return autosplit.PlanSplit(manifest, autosplit.SplitRequest{
		Recommendation: work.Candidate.Recommendation, RetainChild: 0,
		NextRoutingVersion: manifest.Version() + 1, AllocationHighWater: next - 1,
		Destinations: destinations,
	})
}

func (factory *gatewayHotSplitFactory) buildChildTarget(
	catalog *gateway.Snapshot,
	admission [32]byte,
	child uint8,
	descriptor autosplit.SplitChild,
	source gateway.ReplicatedShardDescriptor,
	profile gateway.ReplicatedTableProfile,
) (splitcontroller.ChildTarget, error) {
	configuration, found := factory.sourceForDescriptor(source)
	if !found || !gatewaySplitSourceMatches(configuration, source, profile) {
		return splitcontroller.ChildTarget{}, hotshard.ErrInvalidPressureCut
	}
	template := configuration.Template
	groupID := gatewayHotSplitID("group", admission, child, 0)
	shardIncarnation := gatewayHotSplitID("shard", admission, child, 0)
	authority := sqldriver.ReplicatedAuthorityProfile{
		ActivePolicyGeneration: source.Command.ActivePolicyGeneration,
		ProtectionEpoch:        source.Command.ProtectionEpoch,
		OwnershipEpoch:         uint64(descriptor.OwnershipEpoch),
		SchemaGeneration:       source.Command.SchemaGeneration,
		RoutingVersion:         uint64(catalogGenerationManifestVersion(catalog, source.Distribution) + 1),
		RouteGeneration:        catalog.Generation() + 1,
	}
	replicas := make([]splitcontroller.ChildReplicaTarget, len(source.Replicas))
	for index, sourceReplica := range source.Replicas {
		var localReplica gatewaySplitReplica
		for _, replica := range configuration.Replicas {
			if replica.Node == sourceReplica.Node {
				localReplica = replica
				break
			}
		}
		if localReplica.Root == "" || localReplica.Snapshot == "" {
			return splitcontroller.ChildTarget{}, hotshard.ErrInvalidPressureCut
		}
		peerAddress, peerErr := catalog.Address(sourceReplica.Endpoint)
		nativeAddress, nativeErr := catalog.Address(sourceReplica.NativeEndpoint)
		controlAddress, controlErr := catalog.Address(sourceReplica.ControlEndpoint)
		if peerErr != nil || nativeErr != nil || controlErr != nil {
			return splitcontroller.ChildTarget{}, errors.Join(hotshard.ErrInvalidPressureCut, peerErr, nativeErr, controlErr)
		}
		operationName := hex.EncodeToString(admission[:])
		runtimeRoot := filepath.Join(localReplica.Root, operationName, "child-"+strconv.Itoa(int(child)))
		storeID := gatewayHotSplitID("store", admission, child, uint8(index))
		wal := raftstore.Identity{
			ClusterID: source.Group.ClusterID, ClusterIncarnation: source.Group.ClusterIncarnation,
			Distribution: string(source.Distribution), Shard: string(descriptor.Shard),
			AllocationGeneration: uint64(descriptor.AllocationGeneration),
			ShardIncarnation:     shardIncarnation, GroupID: groupID,
			MemberID: sourceReplica.Member, StoreID: storeID,
		}
		binding, bindErr := raftmember.BindingForNewWAL(
			wal, source.Group.TopologyRecoveryEpoch, authority,
		)
		if bindErr != nil {
			return splitcontroller.ChildTarget{}, bindErr
		}
		local := sqldriver.ShardStoreIdentity{
			Distribution: source.Distribution, Shard: descriptor.Shard,
			AllocationGeneration: descriptor.AllocationGeneration,
			LogID:                gatewayHotSplitID("log", admission, child, uint8(index)),
		}
		storages := make([]string, configuration.SQL.RelationCount)
		for relation := range storages {
			storages[relation] = gatewayHotSplitStorage("relation-"+strconv.Itoa(relation+1), admission, child, uint8(index))
		}
		base, baseErr := sqldriver.NewReplicatedChildShardStoreBundleIdentity(local, binding, configuration.SQL, storages)
		if baseErr != nil {
			return splitcontroller.ChildTarget{}, baseErr
		}
		apply, applyErr := sqldriver.NewReplicatedChildApplyIdentity(
			base, gatewayHotSplitStorage("apply", admission, child, uint8(index)),
			gatewayHotSplitStorage("capture", admission, child, uint8(index)),
			sqldriver.ReplicatedApplyOptions{
				MaxSessions: template.MaxSessions,
				RetryWindow: template.RetryWindow,
				TxnLimits:   template.TxnLimits,
				Placement: sqldriver.ReplicatedPlacementProfile{
					Format:        template.Format,
					ShardKey:      template.ShardKey,
					TupleVersion:  distribution.TupleVersion(template.TupleVersion),
					MapperVersion: distribution.MapperVersion(template.MapperVersion),
					Range:         descriptor.Range,
				},
			},
		)
		if applyErr != nil {
			return splitcontroller.ChildTarget{}, applyErr
		}
		replicas[index] = splitcontroller.ChildReplicaTarget{
			Member: sourceReplica.Member, Node: sourceReplica.Node, StoreID: storeID,
			// A child has a new WAL and mints its own first incarnation.
			NodeIncarnation: 1,
			Endpoint:        sourceReplica.Endpoint, NativeEndpoint: sourceReplica.NativeEndpoint,
			ControlEndpoint: sourceReplica.ControlEndpoint, WAL: wal,
			PeerAddress: peerAddress, NativeAddress: nativeAddress, ControlAddress: controlAddress,
			SnapshotAddress: localReplica.Snapshot,
			WALPath:         filepath.Join(runtimeRoot, "child.wal"),
			SQLPath:         filepath.Join(runtimeRoot, "stage.vdb"), RuntimeRoot: runtimeRoot,
			SQL: base, Apply: apply,
			CertificateDigest: gatewayHotSplitDigest("certificate", admission, child, uint8(index)),
		}
	}
	childDigest, digestErr := sqldriver.ReplicatedSchemaManifest(replicas[0].SQL, replicas[0].Apply.Placement, configuration.LocalIndexes)
	if digestErr != nil {
		return splitcontroller.ChildTarget{}, digestErr
	}
	return splitcontroller.ChildTarget{
		Child: child, Endpoint: replicas[0].NativeEndpoint, Replicas: replicas,
		// Child membership starts at its independent static bootstrap, not
		// the parent's latest membership-change index.
		ReplicaSetVersion:      1,
		RelationManifestDigest: childDigest, LocalIndexes: cloneGatewaySplitIndexes(configuration.LocalIndexes),
		WAL: replicas[0].WAL, TopologyRecoveryEpoch: source.Group.TopologyRecoveryEpoch,
		Authority: authority, SQL: replicas[0].SQL.Clone(),
	}, nil
}

type gatewayChildPreparationClient interface {
	Prepare(context.Context, rafttransport.NodeID, splitcontroller.ChildPreparation) (splitcontroller.ChildPrepareReceipt, error)
}

type gatewayCommittedChildPreparer struct{ client gatewayChildPreparationClient }

func (preparer gatewayCommittedChildPreparer) PrepareCommittedPlan(ctx context.Context, plan *splitcontroller.Plan) error {
	if ctx == nil || plan == nil || preparer.client == nil {
		return splitcontroller.ErrChildPreparation
	}
	admission := [32]byte(plan.OperationID())
	type job struct {
		preparation splitcontroller.ChildPreparation
		node        rafttransport.NodeID
	}
	jobs := make([]job, 0, (autosplit.MaxSplitChildren-1)*gateway.ServingReplicaCount)
	for child := uint8(0); child < autosplit.MaxSplitChildren; child++ {
		target, found := plan.Target(child)
		if !found {
			continue
		}
		descriptor, err := plan.ChildDescriptor(child)
		if err != nil {
			return err
		}
		allocationDigest := gatewayHotSplitDigest("allocation", admission, target.Child, 0)
		for replica := range target.Replicas {
			preparation, err := splitcontroller.NewChildPreparation(
				splitcontroller.OperationID(admission), allocationDigest,
				descriptor, target.SQL.UserTable, target, uint8(replica),
			)
			if err != nil {
				return err
			}
			jobs = append(jobs, job{preparation: preparation, node: target.Replicas[replica].Node})
		}
	}
	errorsByJob := make([]error, len(jobs))
	var wait sync.WaitGroup
	wait.Add(len(jobs))
	for index := range jobs {
		go func(index int) {
			defer wait.Done()
			item := jobs[index]
			for attempt := 0; attempt < gatewayHotSplitPrepareAttempts; attempt++ {
				receipt, err := preparer.client.Prepare(ctx, item.node, item.preparation)
				if err == nil {
					expected, expectedErr := splitcontroller.NewChildPrepareReceipt(
						item.preparation, item.preparation.ReplicaTarget(),
					)
					if expectedErr == nil && receipt.ReceiptDigest == expected.ReceiptDigest {
						errorsByJob[index] = nil
						return
					}
					err = errors.Join(splitcontroller.ErrChildPreparation, expectedErr)
				}
				errorsByJob[index] = err
				if !errors.Is(err, splitcontroller.ErrRuntimeStoreOutcomeUnknown) {
					return
				}
			}
		}(index)
	}
	wait.Wait()
	return errors.Join(errorsByJob...)
}

func gatewayHotSplitShardID(admission [32]byte, child uint8) distribution.ShardID {
	id := gatewayHotSplitID("name", admission, child, 0)
	return distribution.ShardID("split-" + hex.EncodeToString(id[:8]))
}

func gatewayHotSplitStorage(domain string, admission [32]byte, child, replica uint8) string {
	id := gatewayHotSplitDigest(domain, admission, child, replica)
	return hex.EncodeToString(id[:])
}

func gatewayHotSplitID(domain string, admission [32]byte, child, replica uint8) [16]byte {
	digest := gatewayHotSplitDigest(domain, admission, child, replica)
	var result [16]byte
	copy(result[:], digest[:16])
	return result
}

func gatewayHotSplitDigest(domain string, admission [32]byte, child, replica uint8) [32]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/gateway/hot-split/" + domain + "\x00"))
	_, _ = hash.Write(admission[:])
	_, _ = hash.Write([]byte{child, replica})
	var result [32]byte
	_ = hash.Sum(result[:0])
	return result
}

func catalogGenerationManifestVersion(
	catalog *gateway.Snapshot, name distribution.DistributionName,
) distribution.RoutingVersion {
	manifest, _ := catalog.Manifest(name)
	return manifest.Version()
}

var _ hotshard.SplitPlanFactory = (*gatewayHotSplitFactory)(nil)
