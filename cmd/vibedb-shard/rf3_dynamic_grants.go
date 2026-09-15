package main

import (
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
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
