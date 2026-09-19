package main

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
)

// rf3DynamicGrantRouter retains one exact grant beside each certified learner
// reservation. Register only reads restart evidence; group publication must
// restore that evidence atomically through RegisterExecutionGroupWithGrant.
type rf3DynamicGrantRouter struct {
	mu         sync.Mutex
	authority  rf3TransitionGrantAuthority
	installers map[raftmember.GroupKey]*durableRF3GrantInstaller
}

func newRF3DynamicGrantRouter(authority rf3TransitionGrantAuthority) *rf3DynamicGrantRouter {
	return &rf3DynamicGrantRouter{authority: authority,
		installers: make(map[raftmember.GroupKey]*durableRF3GrantInstaller)}
}

func (router *rf3DynamicGrantRouter) Register(group raftmember.GroupKey, path string) (membershipgrant.Grant, bool, error) {
	if router == nil || router.authority == nil || group == (raftmember.GroupKey{}) ||
		path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return membershipgrant.Grant{}, false, errRF3MembershipGrant
	}
	router.mu.Lock()
	defer router.mu.Unlock()
	if existing := router.installers[group]; existing != nil {
		if existing.path != path {
			return membershipgrant.Grant{}, false, errRF3MembershipGrant
		}
		existing.mu.Lock()
		defer existing.mu.Unlock()
		return existing.grant, existing.present, nil
	}
	if len(router.installers) >= maxRF3ManifestGroups {
		return membershipgrant.Grant{}, false, errRF3MembershipGrant
	}
	grant, found, err := readRF3MembershipGrant(path)
	if err != nil || found && grant.Group != group {
		if err == nil {
			err = errRF3MembershipGrant
		}
		return membershipgrant.Grant{}, false, err
	}
	router.installers[group] = &durableRF3GrantInstaller{
		path: path, authority: router.authority, persist: persistRF3MembershipGrant,
		grant: grant, present: found,
	}
	return grant, found, nil
}

func (router *rf3DynamicGrantRouter) InstallTransitionGrant(grant membershipgrant.Grant) error {
	if router == nil || !grant.Valid() {
		return errRF3MembershipGrant
	}
	router.mu.Lock()
	installer := router.installers[grant.Group]
	router.mu.Unlock()
	if installer == nil {
		return errRF3MembershipGrant
	}
	return installer.InstallTransitionGrant(grant)
}

// Recover reconciles a local grant with the freshly authenticated catalog cut.
// A lagging local RF4 keeps its old grant until its committed removal replays;
// a completed local lifecycle adopts only the current catalog grant or absence.
func (router *rf3DynamicGrantRouter) Recover(group raftmember.GroupKey, publication raftmodel.Publication, cut *nodecontrol.BootstrapReadReply) (membershipgrant.Grant, error) {
	router.mu.Lock()
	defer router.mu.Unlock()
	installer := router.installers[group]
	if installer == nil || cut == nil || !cut.TargetServing() || cut.Intent.Group != group {
		return membershipgrant.Grant{}, nodecontrol.ErrStale
	}
	installer.mu.Lock()
	defer installer.mu.Unlock()
	previous := installer.grant
	next := membershipgrant.Grant{}
	if cut.CurrentGrant != nil {
		next = *cut.CurrentGrant
	}
	if next == previous {
		return previous, nil
	}
	if next != (membershipgrant.Grant{}) && publication.ReplicaSetVersion < next.InitialReplicaSetVersion {
		return previous, nil
	}
	conf := publication.ConfState
	if installer.present {
		completed := conf != nil && len(conf.Voters) == 3 && len(conf.Learners) == 0 && len(conf.VotersOutgoing) == 0 && len(conf.LearnersNext) == 0 && !conf.GetAutoLeave() &&
			publication.ReplicaSetVersion > previous.InitialReplicaSetVersion && slices.Contains(conf.Voters, previous.TargetMember) && !slices.Contains(conf.Voters, previous.SourceMember)
		for _, member := range previous.InitialVoters {
			if member != previous.SourceMember && !slices.Contains(conf.GetVoters(), member) {
				completed = false
			}
		}
		if !completed {
			return previous, nil
		}
	}
	if next == (membershipgrant.Grant{}) {
		if err := os.Remove(installer.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return previous, err
		}
		dir, err := os.Open(filepath.Dir(installer.path))
		if err != nil {
			return previous, err
		}
		if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
			return previous, err
		}
	} else if installer.present {
		if next.CatalogGeneration <= previous.CatalogGeneration || next.InitialReplicaSetVersion <= previous.InitialReplicaSetVersion {
			return previous, nodecontrol.ErrStale
		}
		if err := replaceRF3MembershipGrant(installer.path, previous, next); err != nil {
			return previous, err
		}
	} else if err := persistRF3MembershipGrant(installer.path, next); err != nil {
		return previous, err
	}
	installer.grant, installer.present = next, next != (membershipgrant.Grant{})
	return next, nil
}
