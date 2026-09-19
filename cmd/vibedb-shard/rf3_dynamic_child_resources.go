package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// A child receives one immutable provisioning capability per admitted
// operation. Its durable preparation retains the exact schema and roster,
// allowing recovery after the source moves, changes schema, or retires.
type rf3SplitChildResources struct {
	Slot              int
	SourceGroup       raftmember.GroupKey
	Registry          rf3ManifestSplitChildRegistry
	Bootstrap         *pb.Snapshot
	PreparationDigest [32]byte
	Preparation       splitcontroller.ChildPreparation
	Peers             []rafttransport.PhysicalPeer
}

type rf3DynamicChildResources struct {
	manifest rf3Manifest
	schemas  *rf3SchemaActivator
	owners   splitcontroller.LocalObservationOwner
	registry *rafttransport.StaticRegistry
	catalog  *rf3DynamicChildTemplateCatalog
	owned    bool
}

func newRF3DynamicChildResources(manifest rf3Manifest, nodeOwner *rf3NodeOwner,
	schemas *rf3SchemaActivator, owners splitcontroller.LocalObservationOwner,
	registry *rafttransport.StaticRegistry, catalogs ...*rf3DynamicChildTemplateCatalog,
) (*rf3DynamicChildResources, error) {
	if manifest.NodeLog == nil || nodeOwner == nil || nodeOwner.store == nil || schemas == nil ||
		owners == nil || registry == nil || len(catalogs) > 1 || manifest.SplitControl.operationLimit() <= 0 {
		return nil, errRF3Serving
	}
	result := &rf3DynamicChildResources{manifest: manifest, schemas: schemas, owners: owners, registry: registry}
	if len(catalogs) == 1 {
		if catalogs[0] == nil {
			return nil, errRF3Serving
		}
		result.catalog = catalogs[0]
	} else {
		var err error
		result.catalog, err = openRF3DynamicChildTemplateCatalog(manifest)
		if err != nil {
			return nil, err
		}
		result.owned = true
	}
	return result, nil
}

func (resources *rf3DynamicChildResources) Close() error {
	if resources == nil || !resources.owned {
		return nil
	}
	return resources.catalog.Close()
}

func (resources *rf3DynamicChildResources) ResolveResources(operation [32]byte, child uint8,
	replica splitcontroller.ChildReplicaTarget,
) (rf3SplitChildResources, bool, error) {
	if resources == nil || resources.catalog == nil {
		return rf3SplitChildResources{}, false, nil
	}
	return resources.catalog.Resolve(operation, child, replica)
}

func (resources *rf3DynamicChildResources) ReadResources(operation [32]byte, child uint8) (rf3SplitChildResources, bool, error) {
	if resources == nil || resources.catalog == nil {
		return rf3SplitChildResources{}, false, nil
	}
	return resources.catalog.Read(operation, child)
}

func (resources *rf3DynamicChildResources) PrepareResources(ctx context.Context,
	preparation splitcontroller.ChildPreparation,
) (rf3SplitChildResources, error) {
	if resources == nil || ctx == nil || resources.catalog == nil {
		return rf3SplitChildResources{}, splitcontroller.ErrChildPreparation
	}
	if err := context.Cause(ctx); err != nil {
		return rf3SplitChildResources{}, err
	}
	digest, err := splitcontroller.ChildPreparationDigest(preparation)
	if err != nil {
		return rf3SplitChildResources{}, err
	}
	operation, child := [32]byte(preparation.OperationID()), preparation.Child()
	replica := preparation.ReplicaTarget()
	if retained, found, err := resources.ResolveResources(operation, child, replica); err != nil || found {
		if err == nil && retained.PreparationDigest != digest {
			err = splitcontroller.ErrChildPreparation
		}
		return retained, err
	}
	resources.schemas.mu.RLock()
	states := make([]*rf3SchemaGeneration, 0, len(resources.schemas.groups))
	for _, state := range resources.schemas.groups {
		states = append(states, state)
	}
	resources.schemas.mu.RUnlock()
	var selected *rf3SchemaGeneration
	for _, state := range states {
		state.mu.Lock()
		root := filepath.Join(state.manifest.Route.MemberRoot, "split-children")
		paths, pathErr := (rf3ManifestSplitChildRegistry{Root: root}).childPaths(operation, child)
		matches := pathErr == nil && replica.RuntimeRoot == paths.Root && replica.SQLPath == paths.Database && replica.WALPath == paths.WAL
		state.mu.Unlock()
		if !matches {
			continue
		}
		if selected != nil {
			return rf3SplitChildResources{}, splitcontroller.ErrChildPreparation
		}
		selected = state
	}
	if selected == nil {
		return rf3SplitChildResources{}, splitcontroller.ErrChildPreparation
	}
	template, identity, publication, peers, err := resources.sourceTemplate(selected, preparation)
	if err != nil {
		return rf3SplitChildResources{}, err
	}
	// A schema registration precedes execution publication. Independently
	// require the exact local Runtime so a rolled-back adoption is never a
	// provisioning authority. Do not hold the schema generation lock across an
	// owner request: schema activation may itself be waiting on that owner.
	observed, err := resources.owners.ObserveReplica(ctx, identity.Group, identity.MemberID)
	if err != nil || observed.Identity != identity || observed.Publication.ReplicaSetVersion != publication.ReplicaSetVersion ||
		!proto.Equal(observed.Publication.ConfState, publication.ConfState) ||
		observed.State.Binding.SchemaGeneration != replica.SQL.Binding.Authority.SchemaGeneration ||
		observed.State.Binding.ActivePolicyGeneration != replica.SQL.Binding.Authority.ActivePolicyGeneration ||
		observed.State.Binding.ProtectionEpoch != replica.SQL.Binding.Authority.ProtectionEpoch {
		return rf3SplitChildResources{}, errors.Join(splitcontroller.ErrChildPreparation, err)
	}
	bootstrap, err := rf3DynamicSplitChildBootstrap(preparation.Target(), template.Members[:template.MemberCount])
	if err != nil {
		return rf3SplitChildResources{}, err
	}
	if err := ensureRF3DynamicSplitChildRoot(filepath.Dir(template.Root), template.Root); err != nil {
		return rf3SplitChildResources{}, err
	}
	return resources.catalog.Publish(preparation, identity.Group, template, bootstrap, peers)
}

func (resources *rf3DynamicChildResources) sourceTemplate(state *rf3SchemaGeneration,
	preparation splitcontroller.ChildPreparation,
) (rf3ManifestSplitChildRegistry, raftmember.RuntimeIdentity, raftmodel.Publication, []rafttransport.PhysicalPeer, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	fail := func(err error) (rf3ManifestSplitChildRegistry, raftmember.RuntimeIdentity, raftmodel.Publication, []rafttransport.PhysicalPeer, error) {
		return rf3ManifestSplitChildRegistry{}, raftmember.RuntimeIdentity{}, raftmodel.Publication{}, nil, errors.Join(splitcontroller.ErrChildPreparation, err)
	}
	if state.apply == nil || state.quiesced {
		return fail(nil)
	}
	identity := state.identity
	replica, target := preparation.ReplicaTarget(), preparation.Target()
	if replica.Node != resources.registry.LocalNode() || replica.Member != identity.MemberID ||
		replica.SQL.Binding.Distribution != identity.Distribution || target.TopologyRecoveryEpoch != identity.Group.TopologyRecoveryEpoch ||
		replica.WAL.ClusterID != identity.Group.ClusterID || replica.WAL.ClusterIncarnation != identity.Group.ClusterIncarnation ||
		target.ReplicaSetVersion != 1 {
		return fail(nil)
	}
	profile, err := state.apply.CapacityQualificationProfile()
	if err != nil {
		return fail(err)
	}
	apply, err := state.apply.Identity()
	if err != nil {
		return fail(err)
	}
	if replica.SQL.Binding.Authority.ActivePolicyGeneration != profile.Binding.Authority.ActivePolicyGeneration ||
		replica.SQL.Binding.Authority.ProtectionEpoch != profile.Binding.Authority.ProtectionEpoch ||
		replica.SQL.Binding.Authority.SchemaGeneration != profile.Binding.Authority.SchemaGeneration {
		return fail(nil)
	}
	publication := state.apply.Published()
	conf := publication.ConfState
	if conf == nil || conf.AutoLeave != nil && conf.GetAutoLeave() || len(conf.Voters) != gateway.ServingReplicaCount ||
		len(conf.Learners) != 0 || len(conf.VotersOutgoing) != 0 || len(conf.LearnersNext) != 0 ||
		len(target.Replicas) != gateway.ServingReplicaCount {
		return fail(nil)
	}
	template := state.manifest.SplitControl.ChildRegistry
	description, err := sqldriver.DescribeReplicatedSchemaCatalog(state.path)
	if err != nil {
		return fail(err)
	}
	template, err = refreshRF3SplitChildSchema(template, description)
	if err != nil {
		return fail(err)
	}
	template.Root = filepath.Join(state.manifest.Route.MemberRoot, "split-children")
	template.MaxOperations = resources.manifest.SplitControl.operationLimit()
	template.StageCheckpointBytes = rangesplit.MaxChildArtifactChunkBytes
	template.StaticBootstrapPath = "" // The exact child bootstrap is in the immutable operation record.
	template.WAL.KeyID, template.WAL.KeyMaterialPath = resources.manifest.NodeLog.KeyID, resources.manifest.NodeLog.KeyMaterialPath
	template.Apply = rf3ManifestSplitChildApply{
		MaxSessions: apply.MaxSessions, RetryWindow: apply.RetryWindow, TxnLimits: apply.TxnLimits,
		RequestLedgerCapacityBytes: apply.RequestLedgerCapacityBytes, RequestLedgerCleanupReserveBytes: apply.RequestLedgerCleanupReserveBytes,
		RequestLedgerRangeStart: apply.RequestLedgerRangeStart, RequestLedgerRangeEnd: apply.RequestLedgerRangeEnd,
		RequestLedgerRangeIdentity: apply.RequestLedgerRangeIdentity, Format: apply.Placement.Format,
		ShardKey: apply.Placement.ShardKey, TupleVersion: apply.Placement.TupleVersion, MapperVersion: apply.Placement.MapperVersion,
	}
	template.ReplicaSetVersion, template.MemberCount = 1, gateway.ServingReplicaCount
	peers := make([]rafttransport.PhysicalPeer, gateway.ServingReplicaCount)
	for index, member := range target.Replicas {
		if !slices.Contains(conf.Voters, member.Member) {
			return fail(nil)
		}
		node, err := resources.registry.Node(identity.Group, member.Member)
		if err != nil || node != member.Node {
			return fail(err)
		}
		peer, err := resources.registry.PhysicalPeer(node)
		if err != nil || peer.State != rafttransport.PeerEnrolled || member.PeerAddress != peer.Endpoint ||
			peer.ServiceKeyDigest == ([32]byte{}) || peer.Incarnation == 0 || peer.Revision == 0 {
			return fail(err)
		}
		peers[index] = peer
		template.Members[index] = rf3ManifestMember{MemberID: member.Member, NodeID: member.Node, StoreID: member.StoreID,
			PeerAddress: member.PeerAddress, NativeAddress: member.NativeAddress}
	}
	slices.SortFunc(template.Members[:template.MemberCount], func(left, right rf3ManifestMember) int {
		if left.MemberID < right.MemberID {
			return -1
		}
		if left.MemberID > right.MemberID {
			return 1
		}
		return 0
	})
	orderedPeers := make([]rafttransport.PhysicalPeer, len(peers))
	for index, member := range template.Members[:template.MemberCount] {
		for _, peer := range peers {
			if peer.NodeID == member.NodeID {
				orderedPeers[index] = peer
				break
			}
		}
	}
	if !rf3SplitChildTemplateMatchesRetained(template, replica.SQL, replica.Apply) || !rf3SplitChildSchemaMatchesRetained(template, replica.SQL) {
		return fail(nil)
	}
	return template, identity, publication, orderedPeers, nil
}

func rf3DynamicSplitChildBootstrap(target splitcontroller.ChildTarget, members []rf3ManifestMember) (*pb.Snapshot, error) {
	if target.ReplicaSetVersion != 1 || len(members) != gateway.ServingReplicaCount || len(target.Replicas) != len(members) {
		return nil, splitcontroller.ErrChildPreparation
	}
	prepared := make([]prepareRF3Member, len(members))
	for index, member := range members {
		matches := 0
		for _, replica := range target.Replicas {
			if replica.Member == member.MemberID && replica.Node == member.NodeID && replica.StoreID == member.StoreID &&
				replica.PeerAddress == member.PeerAddress && replica.NativeAddress == member.NativeAddress {
				matches++
			}
		}
		if matches != 1 {
			return nil, splitcontroller.ErrChildPreparation
		}
		prepared[index].MemberID = member.MemberID
	}
	raw, err := prepareRF3SplitChildBootstrap(prepared)
	if err != nil {
		return nil, err
	}
	bootstrap := new(pb.Snapshot)
	if err := proto.Unmarshal(raw, bootstrap); err != nil {
		return nil, err
	}
	return bootstrap, nil
}

func ensureRF3DynamicSplitChildRoot(memberRoot, childRoot string) error {
	if !filepath.IsAbs(memberRoot) || filepath.Clean(memberRoot) != memberRoot || childRoot != filepath.Join(memberRoot, "split-children") {
		return splitcontroller.ErrChildPreparation
	}
	info, err := os.Lstat(memberRoot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(splitcontroller.ErrChildPreparation, err)
	}
	if err := os.Mkdir(childRoot, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err = os.Lstat(childRoot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(splitcontroller.ErrChildPreparation, err)
	}
	return syncPrepareRF3Directory(memberRoot)
}
