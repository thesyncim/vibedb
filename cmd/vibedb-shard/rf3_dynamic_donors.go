package main

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// rf3DynamicDonorServices uses the same live schema inventory as preparation
// and capacity. Adoption and recovery install a group's durable source
// journal and repository before its execution authority becomes visible.
type rf3DynamicDonorServices struct {
	Preparation *nodecontrol.PreparationSourceService
	Control     *snapshottransfer.GroupSourceControlRegistry
	Data        *snapshottransfer.GroupDataRegistry

	mu       sync.RWMutex
	schemas  *rf3SchemaActivator
	registry *rafttransport.StaticRegistry
	policy   *serviceauthz.Policy
	options  rf3ManifestReplicaControl
	budget   *migrationbudget.Budget
	deadline rafttransport.DeadlineFunc
	groups   map[raftmember.GroupKey]*rf3DynamicDonorGroup
	closed   bool
}

type rf3DynamicDonorGroup struct {
	identity raftmember.RuntimeIdentity
	state    *rf3SchemaGeneration
	journal  *snapshottransfer.SourceFileJournal
	provider *snapshottransfer.RetainedSourceExportProvider
	control  *snapshottransfer.SourceControlService
	data     *snapshottransfer.Service
}

func (installer *rf3DynamicLearnerInstaller) registerDonorServices(identity raftmember.RuntimeIdentity,
	apply *sqldriver.ReplicatedApply,
) (func() error, error) {
	runtime := installer.factory.runtime
	if runtime.schemas == nil && runtime.donors == nil {
		// Small offline installer compositions may have no network services.
		return func() error { return nil }, nil
	}
	if runtime.schemas == nil || runtime.donors == nil {
		return nil, snapshottransfer.ErrSourceControl
	}
	profile, err := apply.CapacityQualificationProfile()
	if err != nil {
		return nil, err
	}
	log, err := installer.factory.owner.group(profile.Binding)
	if err != nil {
		return nil, err
	}
	if err := runtime.schemas.RegisterDynamic(identity, apply, installer.spec, installer.reservationRoot, log); err != nil {
		return nil, err
	}
	if err := runtime.donors.Register(identity.Group); err != nil {
		return nil, errors.Join(err, runtime.schemas.UnregisterDynamic(identity))
	}
	if runtime.native != nil {
		if err := runtime.native.registerEnrolled(identity, installer.spec, installer.base); err != nil {
			return nil, errors.Join(err, runtime.donors.Unregister(identity), runtime.schemas.UnregisterDynamic(identity))
		}
	}
	return func() error {
		return errors.Join(runtime.native.unregisterDynamic(identity), runtime.donors.Unregister(identity), runtime.schemas.UnregisterDynamic(identity))
	}, nil
}

func newRF3DynamicDonorServices(schemas *rf3SchemaActivator, registry *rafttransport.StaticRegistry,
	policy *serviceauthz.Policy, manifest rf3Manifest, budget *migrationbudget.Budget,
	deadline rafttransport.DeadlineFunc,
) (*rf3DynamicDonorServices, error) {
	if schemas == nil || registry == nil || policy == nil || budget == nil || deadline == nil {
		return nil, snapshottransfer.ErrSourceControl
	}
	donors := &rf3DynamicDonorServices{schemas: schemas, registry: registry, policy: policy,
		options: manifest.ReplicaControl, budget: budget, deadline: deadline,
		groups: make(map[raftmember.GroupKey]*rf3DynamicDonorGroup)}
	var err error
	donors.Preparation, err = newRF3PreparationSource(schemas, registry, policy,
		manifest.ReplicaControl.SourceDataRoot, deadline)
	if err != nil {
		return nil, err
	}
	donors.Control, err = snapshottransfer.NewGroupSourceControlRegistry(snapshottransfer.GroupSourceControlRegistryOptions{
		Registry: registry, Resolve: donors.controlService, ReadDeadline: deadline,
		MaxConnections: donors.options.MaxSourceConcurrent,
	})
	if err != nil {
		return nil, err
	}
	donors.Data, err = snapshottransfer.NewGroupDataRegistry(snapshottransfer.GroupDataRegistryOptions{
		Registry: registry, Resolve: donors.dataService,
		ReadDeadline:     func() time.Time { return time.Now().Add(rf3SnapshotBootstrapTimeout) },
		MaxConnections:   donors.options.MaxSourceConcurrent,
		MaxInflightBytes: int64(donors.options.SourceChunkBytes) * int64(donors.options.MaxSourceConcurrent),
	})
	if err != nil {
		return nil, err
	}
	return donors, nil
}

// Register opens only local paths from the physical manifest. The schema
// generation must already be authenticated by the certified installer; the
// request resolver remains closed until the peer publishes local authority.
func (donors *rf3DynamicDonorServices) Register(group raftmember.GroupKey) error {
	if donors == nil {
		return snapshottransfer.ErrSourceControl
	}
	donors.mu.Lock()
	defer donors.mu.Unlock()
	if donors.closed {
		return snapshottransfer.ErrSourceControl
	}
	donors.schemas.mu.RLock()
	state := donors.schemas.groups[group]
	donors.schemas.mu.RUnlock()
	if state == nil {
		return snapshottransfer.ErrSourceUnauthorized
	}
	state.mu.Lock()
	identity, apply := state.identity, state.apply
	state.mu.Unlock()
	if identity.Group != group || apply == nil {
		return snapshottransfer.ErrSourceUnauthorized
	}
	if prior := donors.groups[group]; prior != nil {
		if prior.state == state && sameRF3DonorIdentity(prior.identity, identity) {
			return nil
		}
		return snapshottransfer.ErrSourceConflict
	}
	if len(donors.groups) >= maxRF3ManifestGroups {
		return snapshottransfer.ErrBound
	}
	options := donors.options
	journal, err := snapshottransfer.OpenSourceFileJournal(
		rf3SnapshotGroupPath(options.SourceJournalPath, group, true), options.MaxSourceRecords)
	if err != nil {
		return err
	}
	path, err := prepareRF3SnapshotRepository(options.SourceDataRoot, options.SourceRepositoryPath, group, true)
	if err != nil {
		return errors.Join(err, journal.Close())
	}
	cut := rf3DynamicDonorCut{state: state}
	provider, err := snapshottransfer.OpenRetainedSourceExportProvider(snapshottransfer.RetainedSourceExportOptions{
		DataRoot: options.SourceDataRoot, RepositoryPath: path,
		Limits: snapshottransfer.Limits{MaxArtifacts: options.MaxSourceArtifacts,
			MaxArtifactBytes: options.MaxSourceArtifactBytes, MaxDiskBytes: options.MaxSourceDiskBytes},
		ChunkBytes: options.SourceChunkBytes, MaxConcurrent: options.MaxSourceConcurrent,
		Budget: donors.budget, RuntimeIdentity: identity, SourceNode: donors.registry.LocalNode(),
		DynamicTarget: true, Cut: cut,
	})
	if err != nil {
		return errors.Join(err, journal.Close())
	}
	if phase := os.Getenv("VIBEDB_QUALIFICATION_ABANDON_CRASH"); phase != "" {
		if os.Getenv("VIBEDB_REPLICA_REPLACEMENT_E2E") != "1" || !provider.InstallAbandonmentExitFaultForQualification(phase, func() { os.Exit(97) }) {
			return errors.Join(errRF3Serving, provider.Close(), journal.Close())
		}
	}
	control, err := snapshottransfer.NewSourceControlService(snapshottransfer.SourceControlOptions{
		Journal: journal, Exporter: snapshottransfer.PinnedSourceControlExporter{Provider: provider},
		Authorize: func(peer rafttransport.PeerIdentity, request snapshottransfer.SourceControlRequest) bool {
			return request.Group == group && request.SourceMember == identity.MemberID &&
				request.SourceNode == donors.registry.LocalNode() && donors.hosted(group, identity.MemberID) &&
				donors.policy.Check(peer.Node, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow &&
				rf3DynamicSnapshotTarget(donors.registry, group, request.TargetMember, request.TargetIncarnation)
		},
		ReadDeadline: donors.deadline, WriteDeadline: donors.deadline,
		MaxConcurrent: options.MaxSourceConcurrent,
	})
	if err != nil {
		return errors.Join(err, provider.Close(), journal.Close())
	}
	snapshotDeadline := func() time.Time { return time.Now().Add(rf3SnapshotBootstrapTimeout) }
	authorize := rf3DynamicSnapshotDataAuthorizer(donors.registry, cut, identity)
	data, err := provider.NewDataService(snapshottransfer.ServiceOptions{
		Registry: donors.registry, Budget: donors.budget,
		Authorize: func(descriptor snapshottransfer.Descriptor) bool {
			return donors.hosted(group, identity.MemberID) && authorize(descriptor)
		},
		ReadDeadline: snapshotDeadline, WriteDeadline: snapshotDeadline,
		MaxConnections: options.MaxSourceConcurrent, MaxChunkBytes: options.SourceChunkBytes,
		MaxInflightBytes: int64(options.SourceChunkBytes) * int64(options.MaxSourceConcurrent),
	})
	if err != nil {
		return errors.Join(err, provider.Close(), journal.Close())
	}
	donors.groups[group] = &rf3DynamicDonorGroup{identity: identity, state: state,
		journal: journal, provider: provider, control: control, data: data}
	return nil
}

func sameRF3DonorIdentity(a, b raftmember.RuntimeIdentity) bool {
	// Schema installation replaces the manifest, while the source repository
	// remains owned by the same physical replica and contains per-export fences.
	a.RelationManifestDigest, b.RelationManifestDigest = [32]byte{}, [32]byte{}
	return a == b
}

func (donors *rf3DynamicDonorServices) hosted(group raftmember.GroupKey, member uint64) bool {
	local, err := donors.registry.LocalMember(group)
	if err != nil || local != member {
		return false
	}
	role, err := donors.registry.Role(group, member)
	return err == nil && role == rafttransport.MemberVoter
}

func (donors *rf3DynamicDonorServices) controlService(group raftmember.GroupKey) *snapshottransfer.SourceControlService {
	donors.mu.RLock()
	defer donors.mu.RUnlock()
	if item := donors.groups[group]; !donors.closed && item != nil && donors.hosted(group, item.identity.MemberID) {
		return item.control
	}
	return nil
}

func (donors *rf3DynamicDonorServices) dataService(group raftmember.GroupKey) *snapshottransfer.Service {
	donors.mu.RLock()
	defer donors.mu.RUnlock()
	if item := donors.groups[group]; !donors.closed && item != nil && donors.hosted(group, item.identity.MemberID) {
		return item.data
	}
	return nil
}

func (donors *rf3DynamicDonorServices) Unregister(identity raftmember.RuntimeIdentity) error {
	if donors == nil {
		return nil
	}
	donors.mu.Lock()
	defer donors.mu.Unlock()
	item := donors.groups[identity.Group]
	if item == nil {
		return nil
	}
	if !sameRF3DonorIdentity(identity, item.identity) {
		return snapshottransfer.ErrSourceConflict
	}
	delete(donors.groups, identity.Group)
	return errors.Join(item.provider.Close(), item.journal.Close())
}

func (donors *rf3DynamicDonorServices) Close() error {
	if donors == nil {
		return nil
	}
	donors.mu.Lock()
	defer donors.mu.Unlock()
	donors.closed = true
	var result error
	for group, item := range donors.groups {
		result = errors.Join(result, item.provider.Close(), item.journal.Close())
		delete(donors.groups, group)
	}
	return result
}

// Schema activation updates state.apply under state.mu. Holding that lock
// until the immutable cut is pinned prevents an export from racing closure
// of the predecessor SQL generation.
type rf3DynamicDonorCut struct{ state *rf3SchemaGeneration }

func (cut rf3DynamicDonorCut) SnapshotArtifactCut() (*replicatedstate.ReadSnapshot, error) {
	if cut.state == nil {
		return nil, snapshottransfer.ErrSourceControl
	}
	cut.state.mu.Lock()
	defer cut.state.mu.Unlock()
	if cut.state.apply == nil || cut.state.quiesced {
		return nil, fmt.Errorf("%w: local snapshot donor apply=%t quiesced=%t", snapshottransfer.ErrStaleFence, cut.state.apply != nil, cut.state.quiesced)
	}
	return cut.state.apply.SnapshotArtifactCut()
}

func (cut rf3DynamicDonorCut) SnapshotAuthorizationFence() (replicatedstate.SnapshotFence, error) {
	if cut.state == nil {
		return replicatedstate.SnapshotFence{}, snapshottransfer.ErrSourceControl
	}
	cut.state.mu.Lock()
	defer cut.state.mu.Unlock()
	if cut.state.apply == nil || cut.state.quiesced {
		return replicatedstate.SnapshotFence{}, fmt.Errorf("%w: local snapshot donor apply=%t quiesced=%t", snapshottransfer.ErrStaleFence, cut.state.apply != nil, cut.state.quiesced)
	}
	return cut.state.apply.SnapshotAuthorizationFence()
}

func (donors *rf3DynamicDonorServices) DataServices() []snapshottransfer.GroupDataService {
	donors.mu.RLock()
	defer donors.mu.RUnlock()
	result := make([]snapshottransfer.GroupDataService, 0, len(donors.groups))
	for group, item := range donors.groups {
		result = append(result, snapshottransfer.GroupDataService{Group: group, Service: item.data})
	}
	return result
}
