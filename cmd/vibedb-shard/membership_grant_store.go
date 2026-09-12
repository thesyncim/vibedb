package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

const rf3MembershipGrantFileBytes = membershipgrant.CanonicalGrantBytes + 32

var errRF3MembershipGrant = errors.New("vibedb-shard: invalid durable membership grant")

type rf3TransitionGrantAuthority interface {
	InstallTransitionGrant(membershipgrant.Grant) error
}

// durableRF3GrantInstaller makes the transition grant part of the shard's
// restart state. A live registry validates first, commits the byte-exact grant
// and its semantic digest through file and parent-directory sync, then exposes
// its authority to transport readers. A failed acknowledgement is safe to retry.
type durableRF3GrantInstaller struct {
	mu        sync.Mutex
	path      string
	authority rf3TransitionGrantAuthority
	grant     membershipgrant.Grant
	present   bool
	persist   func(string, membershipgrant.Grant) error
	replace   func(string, membershipgrant.Grant, membershipgrant.Grant) error
}

type durableRF3GrantRouter struct {
	installers map[raftmember.GroupKey]*durableRF3GrantInstaller
}

func openDurableRF3GrantRouter(
	manifest rf3Manifest,
	authority rf3TransitionGrantAuthority,
) (*durableRF3GrantRouter, error) {
	if authority == nil {
		return nil, errRF3MembershipGrant
	}
	router := &durableRF3GrantRouter{
		installers: make(map[raftmember.GroupKey]*durableRF3GrantInstaller, len(manifest.groupBundles())),
	}
	for _, bundle := range manifest.groupBundles() {
		group := bundle.Route.Group
		if group == (raftmember.GroupKey{}) || bundle.Route.MembershipGrantPath == "" {
			return nil, errRF3MembershipGrant
		}
		if _, duplicate := router.installers[group]; duplicate {
			return nil, errRF3MembershipGrant
		}
		installer, err := openDurableRF3GrantInstaller(bundle.Route.MembershipGrantPath, authority)
		if err != nil {
			return nil, err
		}
		router.installers[group] = installer
	}
	return router, nil
}

func (router *durableRF3GrantRouter) InstallTransitionGrant(grant membershipgrant.Grant) error {
	if router == nil || !grant.Valid() {
		return errRF3MembershipGrant
	}
	installer := router.installers[grant.Group]
	if installer == nil {
		return errRF3MembershipGrant
	}
	return installer.InstallTransitionGrant(grant)
}

func openDurableRF3GrantInstaller(
	path string, authority rf3TransitionGrantAuthority,
) (*durableRF3GrantInstaller, error) {
	if path == "" || filepath.Clean(path) != path || authority == nil {
		return nil, errRF3MembershipGrant
	}
	installer := &durableRF3GrantInstaller{
		path: path, authority: authority, persist: persistRF3MembershipGrant,
	}
	grant, found, err := readRF3MembershipGrant(path)
	if err != nil {
		return nil, err
	}
	if found {
		if err = authority.InstallTransitionGrant(grant); err != nil {
			return nil, errors.Join(errRF3MembershipGrant, err)
		}
		installer.grant, installer.present = grant, true
	}
	return installer, nil
}

func (installer *durableRF3GrantInstaller) InstallTransitionGrant(
	grant membershipgrant.Grant,
) error {
	if installer == nil || !grant.Valid() {
		return errRF3MembershipGrant
	}
	installer.mu.Lock()
	defer installer.mu.Unlock()
	var err error
	if installer.present && installer.grant != grant {
		authority, ok := installer.authority.(interface {
			ReplaceTransitionGrantWithCommit(membershipgrant.Grant, membershipgrant.Grant, func() error) error
		})
		if !ok {
			return errRF3MembershipGrant
		}
		replace := installer.replace
		if replace == nil {
			replace = replaceRF3MembershipGrant
		}
		err = authority.ReplaceTransitionGrantWithCommit(installer.grant, grant, func() error {
			return replace(installer.path, installer.grant, grant)
		})
	} else {
		commit := func() error { return installer.persist(installer.path, grant) }
		if authority, ok := installer.authority.(interface {
			InstallTransitionGrantWithCommit(membershipgrant.Grant, func() error) error
		}); ok {
			err = authority.InstallTransitionGrantWithCommit(grant, commit)
		} else if err = installer.authority.InstallTransitionGrant(grant); err == nil {
			// Cold-process authority only validates immutable manifest identities;
			// it has no running transport whose admission could race persistence.
			err = commit()
		}
	}
	if err != nil {
		return errors.Join(errRF3MembershipGrant, err)
	}
	installer.grant, installer.present = grant, true
	return nil
}

func rf3MembershipGrantPath(manifest rf3Manifest) string {
	return manifest.Route.MembershipGrantPath
}

func readRF3MembershipGrant(path string) (membershipgrant.Grant, bool, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return membershipgrant.Grant{}, false, nil
	}
	if err != nil {
		return membershipgrant.Grant{}, false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != rf3MembershipGrantFileBytes {
		return membershipgrant.Grant{}, false, errors.Join(errRF3MembershipGrant, err)
	}
	var raw [rf3MembershipGrantFileBytes]byte
	if _, err = io.ReadFull(file, raw[:]); err != nil {
		return membershipgrant.Grant{}, false, errors.Join(errRF3MembershipGrant, err)
	}
	var trailing [1]byte
	if count, readErr := file.Read(trailing[:]); count != 0 || !errors.Is(readErr, io.EOF) {
		return membershipgrant.Grant{}, false, errRF3MembershipGrant
	}
	grant, err := membershipgrant.OpenCanonical(raw[:membershipgrant.CanonicalGrantBytes])
	digest := grant.Digest()
	if err != nil || !bytes.Equal(raw[membershipgrant.CanonicalGrantBytes:], digest[:]) {
		return membershipgrant.Grant{}, false, errors.Join(errRF3MembershipGrant, err)
	}
	return grant, true, nil
}

func persistRF3MembershipGrant(path string, grant membershipgrant.Grant) error {
	return writeRF3MembershipGrantFile(path, membershipgrant.Grant{}, grant)
}

// replaceRF3MembershipGrant is an exact disk CAS invoked only after the
// registry proves the previous lifecycle completed and validates the next one.
func replaceRF3MembershipGrant(path string, expected, grant membershipgrant.Grant) error {
	if !expected.Valid() || expected.Group != grant.Group || expected == grant {
		return errRF3MembershipGrant
	}
	return writeRF3MembershipGrantFile(path, expected, grant)
}

func writeRF3MembershipGrantFile(path string, expected, grant membershipgrant.Grant) error {
	if !grant.Valid() {
		return errRF3MembershipGrant
	}
	if existing, found, err := readRF3MembershipGrant(path); err != nil {
		return err
	} else if found {
		if existing == grant {
			// A prior attempt may have renamed successfully but failed syncing
			// the directory. Finish that durability boundary before acknowledging.
			directory, openErr := os.Open(filepath.Dir(path))
			if openErr != nil {
				return openErr
			}
			return errors.Join(directory.Sync(), directory.Close())
		}
		if expected == (membershipgrant.Grant{}) || existing != expected {
			return errRF3MembershipGrant
		}
	} else if expected != (membershipgrant.Grant{}) {
		return errRF3MembershipGrant
	}
	parent, base := filepath.Dir(path), filepath.Base(path)
	if parent == "." || base == "." || base == string(filepath.Separator) {
		return errRF3MembershipGrant
	}
	root, err := os.OpenRoot(parent)
	if err != nil {
		return err
	}
	defer root.Close()
	temporary := "." + base + ".tmp"
	file, err := root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		info, statErr := root.Lstat(temporary)
		if statErr != nil || !info.Mode().IsRegular() {
			return errors.Join(errRF3MembershipGrant, statErr)
		}
		if removeErr := root.Remove(temporary); removeErr != nil {
			return errors.Join(errRF3MembershipGrant, removeErr)
		}
		file, err = root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	}
	if err != nil {
		return err
	}
	published := false
	defer func() {
		_ = file.Close()
		if !published {
			_ = root.Remove(temporary)
		}
	}()
	var raw [rf3MembershipGrantFileBytes]byte
	encoded, err := membershipgrant.AppendCanonical(raw[:0], grant)
	if err != nil {
		return err
	}
	digest := grant.Digest()
	encoded = append(encoded, digest[:]...)
	if err = writeRF3MembershipGrant(file, encoded); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = root.Rename(temporary, base); err != nil {
		return err
	}
	published = true
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func writeRF3MembershipGrant(writer io.Writer, raw []byte) error {
	for len(raw) != 0 {
		written, err := writer.Write(raw)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(raw) {
			return io.ErrShortWrite
		}
		raw = raw[written:]
	}
	return nil
}

// coldRF3GrantAuthority validates the catalog grant against the immutable
// serving RF3 and enrolled target in the member manifest. It grants no Raft
// traffic; the installed snapshot later reconstructs the dynamic authority.
type coldRF3GrantAuthority struct {
	group   raftmember.GroupKey
	members [3]rf3ManifestMember
	target  rf3ManifestEnrolledTarget
}

func (authority coldRF3GrantAuthority) InstallTransitionGrant(grant membershipgrant.Grant) error {
	if grant.Group != authority.group || grant.TargetMember != authority.target.MemberID ||
		grant.TargetNode != [16]byte(authority.target.NodeID) {
		return errRF3MembershipGrant
	}
	var roster [3]membershipgrant.RosterMember
	for index, member := range authority.members {
		if grant.InitialVoters[index] != member.MemberID {
			return errRF3MembershipGrant
		}
		roster[index] = membershipgrant.RosterMember{
			Member: member.MemberID, Node: [16]byte(member.NodeID),
		}
	}
	if membershipgrant.CertifiedRosterDigest(
		grant.Group, grant.InitialReplicaSetVersion, roster,
	) != grant.InitialRosterDigest {
		return errRF3MembershipGrant
	}
	return nil
}

var _ rf3TransitionGrantAuthority = (*rafttransport.StaticRegistry)(nil)
