package main

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

// A moved source retains ordinary enrollment provenance. It does not acquire
// a SplitOrigin merely because the receiving physical node started empty.
type rf3HostedSplitSource struct {
	runtime rf3AdoptedRuntime
	node    rafttransport.NodeID
	root    string
	origin  [32]byte
}

func prepareRF3HostedSplitRoot(source rf3HostedSplitSource) error {
	parent, err := os.OpenRoot(filepath.Dir(source.root))
	if err != nil {
		return err
	}
	defer parent.Close()
	const name = "split-runtime"
	if filepath.Base(source.root) != name {
		return errRF3Serving
	}
	if err := parent.Mkdir(name, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := parent.Lstat(name)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(errRF3Serving, err)
	}
	directory, err := parent.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

type rf3HostedSplitSources interface {
	lookupHostedSplitSource(raftmember.GroupKey) (rf3HostedSplitSource, bool, error)
	lookupRetainedSplitRuntime(raftmember.GroupKey) (rf3AdoptedRuntime, bool, error)
}

// Every hosted group's apply handle follows the live schema generation,
// including startup groups and children whose registry provenance is retained
// elsewhere. A closed predecessor must never be captured by a later split.
func (schemas *rf3SchemaActivator) lookupRetainedSplitRuntime(group raftmember.GroupKey) (rf3AdoptedRuntime, bool, error) {
	if schemas == nil {
		return rf3AdoptedRuntime{}, false, nil
	}
	schemas.mu.RLock()
	state := schemas.groups[group]
	schemas.mu.RUnlock()
	if state == nil {
		return rf3AdoptedRuntime{}, false, nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	schemas.mu.RLock()
	current := schemas.groups[group] == state
	schemas.mu.RUnlock()
	if !current {
		return rf3AdoptedRuntime{}, false, nil
	}
	if state.quiesced || state.apply == nil || state.identity.Group != group {
		return rf3AdoptedRuntime{}, false, errRF3Serving
	}
	return rf3AdoptedRuntime{identity: state.identity, apply: state.apply}, true, nil
}

func (schemas *rf3SchemaActivator) lookupHostedSplitSource(group raftmember.GroupKey) (rf3HostedSplitSource, bool, error) {
	if schemas == nil {
		return rf3HostedSplitSource{}, false, nil
	}
	schemas.mu.RLock()
	state := schemas.groups[group]
	schemas.mu.RUnlock()
	if state == nil {
		return rf3HostedSplitSource{}, false, nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	schemas.mu.RLock()
	current := schemas.groups[group] == state
	schemas.mu.RUnlock()
	if !current {
		return rf3HostedSplitSource{}, false, nil
	}
	if state.preparation == nil {
		// Startup groups and adopted split children already have their own
		// durable source registry path through the retained inventory.
		return rf3HostedSplitSource{}, false, nil
	}
	root := state.manifest.Route.MemberRoot
	if state.quiesced || state.apply == nil || state.identity.Group != group ||
		!filepath.IsAbs(root) || filepath.Clean(root) != root || state.path != filepath.Join(root, "member.vdb") ||
		state.preparation.Group != group || state.preparation.Target.MemberID != state.identity.MemberID ||
		state.preparation.TargetStoreID != state.identity.StoreID || state.preparation.TargetNodeIncarnation != state.identity.NodeIncarnation {
		return rf3HostedSplitSource{}, false, errRF3Serving
	}
	digest := state.preparation.Digest()
	if digest == ([32]byte{}) {
		return rf3HostedSplitSource{}, false, errRF3Serving
	}
	return rf3HostedSplitSource{
		runtime: rf3AdoptedRuntime{identity: state.identity, apply: state.apply},
		node:    state.preparation.Target.Node, root: filepath.Join(root, "split-runtime"), origin: digest,
	}, true, nil
}
