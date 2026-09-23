package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
)

// rf3NodeRuntime owns one physical node and its live group inventory. Initial
// groups and certified later learners share the same execution, grant, schema
// and donor resources; retirement withdraws those resources before readoption.
type rf3NodeRuntime struct {
	peer          *raftservice.AuthenticatedExecutionPeerRuntime
	registry      *rafttransport.StaticRegistry
	lanes         *multiraft.ExecutionLanes
	serving       *raftserve.Registry
	reader        *nodecontrol.IntentReaderSlot
	receivers     *rf3DynamicBootstrapRegistry
	learner       *rf3DynamicLearnerFactory
	grants        *rf3DynamicGrantRouter
	schemas       *rf3SchemaActivator
	donors        *rf3DynamicDonorServices
	native        *rf3NativeAuthorities
	actionJournal *replicaaction.FileJournal
	groupsMu      sync.RWMutex
	groups        map[raftmember.GroupKey]*raftmember.Runtime
	// servingGroups is separate from transport membership. A group becomes
	// native-serving only after the certified snapshot installer calls
	// RegisterExecutionGroup; an empty process therefore remains fail-closed.
	servingGroups *atomic.Int64
}

// RegisterExecutionGroup is the only path that can make a transferred learner
// visible to ordinary transport and native execution. It is intentionally
// useful to the snapshot installer while retaining one shared physical-node
// peer runtime.
func (runtime *rf3NodeRuntime) RegisterExecutionGroup(
	roster []rafttransport.Member, group raftservice.ExecutionGroup,
) error {
	return runtime.RegisterExecutionGroupWithGrant(roster, group, membershipgrant.Grant{})
}

func (runtime *rf3NodeRuntime) RegisterExecutionGroupWithGrant(
	roster []rafttransport.Member, group raftservice.ExecutionGroup, grant membershipgrant.Grant,
) error {
	if runtime == nil || runtime.peer == nil {
		return raftservice.ErrInvalidOwner
	}
	servingBefore := int64(0)
	if runtime.servingGroups != nil {
		servingBefore = runtime.servingGroups.Load()
	}
	var err error
	if grant != (membershipgrant.Grant{}) {
		err = runtime.peer.RegisterExecutionGroupWithGrant(roster, group, grant)
	} else {
		err = runtime.peer.RegisterExecutionGroup(roster, group)
	}
	if err != nil {
		return fmt.Errorf("rf3 group activation failed group=%x member=%d node_incarnation=%d roster_members=%d serving_groups=%d: %w",
			group.Identity.Group.GroupID, group.Identity.MemberID, group.Identity.NodeIncarnation,
			len(roster), servingBefore, err)
	}
	// Keep the same shared inventory used to register dynamic learners so an
	// on-demand SIGUSR1 snapshot can include their Raft state. This map is only
	// touched during group lifecycle transitions; data requests do not read it.
	runtime.groupsMu.Lock()
	if runtime.groups == nil {
		runtime.groups = make(map[raftmember.GroupKey]*raftmember.Runtime)
	}
	runtime.groups[group.Identity.Group] = group.Runtime
	runtime.groupsMu.Unlock()
	if runtime.servingGroups != nil {
		runtime.servingGroups.Add(1)
	}
	return nil
}

func (runtime *rf3NodeRuntime) diagnosticRuntimes() []*raftmember.Runtime {
	if runtime == nil {
		return nil
	}
	runtime.groupsMu.RLock()
	groups := make([]*raftmember.Runtime, 0, len(runtime.groups))
	for _, group := range runtime.groups {
		if group != nil {
			groups = append(groups, group)
		}
	}
	runtime.groupsMu.RUnlock()
	return groups
}

// UnregisterExecutionGroup withdraws a quiescent group from the shared peer
// and native listener. It is the inverse of RegisterExecutionGroup and keeps
// a different adopted group serving while one group is retired.
func (runtime *rf3NodeRuntime) UnregisterExecutionGroup(identity raftmember.RuntimeIdentity) error {
	if runtime == nil || runtime.peer == nil {
		return raftservice.ErrInvalidOwner
	}
	if err := runtime.peer.UnregisterExecutionGroup(identity); err != nil {
		return err
	}
	runtime.groupsMu.Lock()
	delete(runtime.groups, identity.Group)
	runtime.groupsMu.Unlock()
	var cleanup error
	cleanup = errors.Join(cleanup, runtime.native.unregisterDynamic(identity))
	if runtime.donors != nil {
		cleanup = errors.Join(cleanup, runtime.donors.Unregister(identity))
	}
	if runtime.schemas != nil {
		cleanup = errors.Join(cleanup, runtime.schemas.UnregisterDynamic(identity))
	}
	if runtime.servingGroups != nil {
		for {
			count := runtime.servingGroups.Load()
			if count <= 0 || runtime.servingGroups.CompareAndSwap(count, count-1) {
				break
			}
		}
	}
	return cleanup
}

func (runtime *rf3NodeRuntime) nativeServing() bool {
	return runtime != nil && runtime.servingGroups != nil && runtime.servingGroups.Load() > 0
}

func (runtime *rf3NodeRuntime) IntentReaderSlot() *nodecontrol.IntentReaderSlot {
	if runtime == nil {
		return nil
	}
	return runtime.reader
}

func (runtime *rf3NodeRuntime) BootstrapReceivers() *rf3DynamicBootstrapRegistry {
	if runtime == nil {
		return nil
	}
	return runtime.receivers
}

// BindIntentReader attaches the authenticated committed-directory client. It
// is deliberately a one-time capability handoff; until it is attached the
// node-control service fails closed before any journal or storage side effect.
func (runtime *rf3NodeRuntime) BindIntentReader(reader nodecontrol.IntentReader) error {
	if runtime == nil || runtime.reader == nil {
		return nodecontrol.ErrControl
	}
	return runtime.reader.Set(reader)
}

// RegisterBootstrapService creates the shipped receiver/installer composition
// for one certified post-AddLearner descriptor. A reservation alone never
// creates this service, so an activated empty target cannot receive or install
// arbitrary snapshot bytes.
func (runtime *rf3NodeRuntime) RegisterBootstrapService(
	ctx context.Context, intent gateway.GroupEnrollmentIntent,
	proof gateway.PreparedReplicaProof, descriptor snapshottransfer.Descriptor,
) error {
	if runtime == nil || runtime.learner == nil {
		return nodecontrol.ErrControl
	}
	return runtime.learner.Register(ctx, intent, proof, descriptor)
}

func (runtime *rf3NodeRuntime) CloseBootstrapServices() error {
	if runtime == nil || runtime.learner == nil {
		return nil
	}
	return runtime.learner.Close()
}

// Unregister is called after the journaled source retirement closed the owner.
// All startup and adopted groups release the same runtime inventories.
func (runtime *rf3NodeRuntime) Unregister(identity raftmember.RuntimeIdentity) error {
	if runtime == nil {
		return nil
	}
	if runtime.registry != nil {
		member, err := runtime.registry.LocalMember(identity.Group)
		if err == nil && member != identity.MemberID {
			return raftservice.ErrServingFence
		}
		if err != nil && !errors.Is(err, rafttransport.ErrGroupNotFound) {
			return err
		}
	}
	if runtime.donors != nil {
		if err := runtime.donors.Unregister(identity); err != nil {
			return fmt.Errorf("retire group %x donor: %w", identity.Group.GroupID, err)
		}
	}
	if runtime.learner != nil {
		if err := runtime.learner.Unregister(identity); err != nil {
			return fmt.Errorf("retire group %x bootstrap resources: %w", identity.Group.GroupID, err)
		}
	}
	if runtime.grants != nil {
		runtime.grants.mu.Lock()
		delete(runtime.grants.installers, identity.Group)
		runtime.grants.mu.Unlock()
	}
	if runtime.registry != nil {
		err := runtime.registry.RemoveGroup(identity.Group, func(withdraw func()) error { withdraw(); return nil })
		if err != nil && !errors.Is(err, rafttransport.ErrGroupNotFound) {
			return fmt.Errorf("retire group %x transport: %w", identity.Group.GroupID, err)
		}
	}
	return nil
}

func rf3TransportRegistryLimits() rafttransport.Limits {
	return rafttransport.Limits{
		MaxGroups: maxRF3ManifestGroups,
		// Each retained RF3 group also needs its incoming or outgoing member
		// mapping while a certified placement transition is in progress.
		MaxMembers: maxRF3ManifestGroups * (rf3ManifestMembers + 1),
		MaxPeers:   rafttransport.AbsoluteMaxTransportPeers,
	}
}
