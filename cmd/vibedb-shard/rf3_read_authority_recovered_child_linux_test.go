//go:build linux

package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type rf3RecoveredReadAuthorityFixture struct {
	manifest rf3Manifest
	prepared preparedRF3Set
	runtimes []*raftmember.Runtime
	profile  *rafttransport.PeerTLS
	authz    *serviceauthz.Policy
	owner    *rf3NodeOwner
}

func newRF3RecoveredReadAuthorityFixture(t *testing.T) *rf3RecoveredReadAuthorityFixture {
	t.Helper()
	input := prepareRF3NodeTestInput(t)
	nodes := rf3CommandNodes()
	gatewayNode := rafttransport.NodeID{0xb1, 1}
	credentialRoot := t.TempDir()
	credentials, roots, err := rf3testfixture.WriteCredentials(
		credentialRoot, rf3CommandIdentityOID,
		rafttransport.TrustDomain{ClusterID: rf3CommandGroup().ClusterID, ClusterIncarnation: rf3CommandGroup().ClusterIncarnation},
		append(append([]rafttransport.NodeID(nil), nodes[:]...), gatewayNode),
	)
	if err != nil {
		t.Fatal(err)
	}
	policyPath := filepath.Join(credentialRoot, "authority-policy.vibejson")
	if err := os.WriteFile(policyPath, rf3ReadAuthorityPhysicalPolicy(nodes, gatewayNode), 0o600); err != nil {
		t.Fatal(err)
	}
	authority := testRF3ReadAuthorityConfig()
	for group := range input.Groups {
		member := &input.Groups[group]
		member.AuthorizationPolicy = policyPath
		member.TLS = rf3ManifestTLS{
			Certificate: credentials[0].Certificate, Key: credentials[0].Key,
			Roots: roots, IdentityOID: rf3CommandIdentityOID.String(),
		}
		config := *authority
		config.Voters = append([]uint64(nil), authority.Voters...)
		config.Capabilities = append([]rf3ManifestVoterCapability(nil), authority.Capabilities...)
		member.ReadAuthority = &config
		for ordinal := range member.Members {
			identity := rf3CommandStoreIdentity(uint64(ordinal + 1))
			identity.StoreID[15] += byte(group)
			member.Members[ordinal].StoreID = idString(identity.StoreID[:])
			member.Members[ordinal].NativeAddress = fmt.Sprintf("127.0.0.1:%d", 32000+ordinal)
		}
	}
	if err := provisionRF3Node(input); err != nil {
		t.Fatal(err)
	}
	manifest, err := loadRF3Manifest(filepath.Join(input.Root, "serve-rf3.vibejson"))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := servicetls.LoadProfile(
		manifest.TLS.Certificate, manifest.TLS.Key, manifest.TLS.Roots,
		manifest.TLS.IdentityOID, time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	authz, err := serviceauthz.LoadFile(policyPath)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := openRF3NodeOwner(manifest, profile)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := prepareRF3GroupSetOnNode(manifest, profile, sqldriver.ReplicatedOpenOptions{}, owner)
	if err != nil {
		_ = owner.Close()
		t.Fatal(err)
	}
	fixture := &rf3RecoveredReadAuthorityFixture{
		manifest: manifest, prepared: prepared, profile: profile, authz: authz, owner: owner,
	}
	for index := range prepared.groups {
		runtime, err := prepared.groups[index].adoptRuntime()
		if err != nil {
			_ = owner.Close()
			t.Fatal(err)
		}
		fixture.runtimes = append(fixture.runtimes, runtime)
	}
	t.Cleanup(func() {
		for _, runtime := range fixture.runtimes {
			if err := runtime.Close(); err != nil {
				t.Errorf("close recovered authority runtime: %v", err)
			}
		}
		if err := closePreparedRF3Groups(fixture.prepared.groups, nil); err != nil {
			t.Errorf("close recovered authority groups: %v", err)
		}
		if err := fixture.owner.Close(); err != nil {
			t.Errorf("close recovered authority owner: %v", err)
		}
	})
	return fixture
}

func markRF3RecoveredChild(t testing.TB, fixture *rf3RecoveredReadAuthorityFixture) {
	t.Helper()
	fixture.prepared.groups[1].adoptedChild = true
	// Recovery reconstructs the child bundle from the parent split template;
	// make that identity mismatch explicit so this test cannot pass merely
	// because the child happens to have a complete live roster.
	fixture.prepared.groups[1].manifest.Members = fixture.prepared.groups[0].manifest.SplitControl.ChildRegistry.Members
	fixture.prepared.groups[1].manifest.MemberCount = fixture.prepared.groups[0].manifest.SplitControl.ChildRegistry.MemberCount
	childIdentity := fixture.runtimes[1].Identity()
	for _, member := range fixture.prepared.groups[1].manifest.memberRoster() {
		if member.MemberID == childIdentity.MemberID && member.StoreID == childIdentity.StoreID {
			t.Fatalf("recovered child fixture retained the live local StoreID %x", member.StoreID)
		}
	}
}

func TestRF3ReadAuthorityRecoveredChildStaysReadIndexWhileParentEnrolls(t *testing.T) {
	fixture := newRF3RecoveredReadAuthorityFixture(t)
	markRF3RecoveredChild(t, fixture)
	cache, evidence, err := configureRF3ReadAuthorities(
		fixture.manifest, fixture.prepared.groups, fixture.runtimes,
		fixture.profile, fixture.authz, fixture.profile.LocalIdentity().Node,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	if len(cache.groups) != 1 || len(cache.targets) != rf3ManifestMembers {
		t.Fatalf("startup cache registered %d groups/%d targets, want one group/%d targets", len(cache.groups), len(cache.targets), rf3ManifestMembers)
	}
	parentIdentity := fixture.runtimes[0].Identity()
	for _, member := range fixture.prepared.groups[0].manifest.memberRoster() {
		lookup := rf3ReadAuthorityGroupMember{group: parentIdentity.Group, member: member.MemberID}
		target, ok := cache.targets[lookup]
		if !ok || target.key.node != member.NodeID || target.key.store != member.StoreID ||
			target.key.allocation != parentIdentity.AllocationGeneration || target.address != member.NativeAddress {
			t.Fatalf("parent cache target for member %d = %#v/%t, want node=%x store=%x allocation=%d address=%q", member.MemberID, target, ok, member.NodeID, member.StoreID, parentIdentity.AllocationGeneration, member.NativeAddress)
		}
	}
	if _, ok := cache.RegistrationFor(fixture.runtimes[1].Identity().Group, fixture.runtimes[1].Identity().AllocationGeneration); ok {
		t.Fatal("adopted child was registered in the authority cache")
	}
	if _, err := newRF3ReadAuthorityCache(
		fixture.profile, fixture.authz,
		[]preparedRF3Group{fixture.prepared.groups[1]},
		[]*raftmember.Runtime{fixture.runtimes[1]}, fixture.profile.LocalIdentity().Node,
	); !errors.Is(err, errRF3ReadAuthority) {
		t.Fatalf("direct adopted-child cache admission = %v, want refusal", err)
	}
	parentMarker := rf3ReadAuthorityMarkerPath(fixture.prepared.groups[0].manifest.Route.MemberRoot)
	if _, err := os.Stat(parentMarker); err != nil {
		t.Fatalf("ordinary parent marker missing: %v", err)
	}
	childMarker := rf3ReadAuthorityMarkerPath(fixture.prepared.groups[1].manifest.Route.MemberRoot)
	if _, err := os.Stat(childMarker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("adopted child marker = %v, want absent", err)
	}
	if len(evidence) != len(fixture.runtimes) {
		t.Fatalf("startup evidence count = %d, want %d", len(evidence), len(fixture.runtimes))
	}
	for index, item := range evidence {
		if item.Identity != fixture.runtimes[index].Identity() {
			t.Fatalf("startup evidence %d identity = %#v, want %#v", index, item.Identity, fixture.runtimes[index].Identity())
		}
		wantStatus := raftmember.ReadAuthorityEvidenceConfigured
		if index == 1 {
			wantStatus = raftmember.ReadAuthorityEvidenceDisabled
		}
		if item.Status != wantStatus {
			t.Fatalf("startup evidence %d status = %v, want %v", index, item.Status, wantStatus)
		}
	}
	if got := fixture.runtimes[1].ReadAuthorityEvidence().Status; got != raftmember.ReadAuthorityEvidenceDisabled {
		t.Fatalf("adopted child runtime status = %v, want disabled", got)
	}
}

func TestRF3ReadAuthorityRecoveredChildMarkerRefusesBeforeParentWrite(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		name := "matching"
		if malformed {
			name = "malformed"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newRF3RecoveredReadAuthorityFixture(t)
			markRF3RecoveredChild(t, fixture)
			policy, err := fixture.manifest.ReadAuthority.rf3Policy()
			if err != nil {
				t.Fatal(err)
			}
			childMarker := rf3ReadAuthorityMarkerPath(fixture.prepared.groups[1].manifest.Route.MemberRoot)
			if malformed {
				if err := os.WriteFile(childMarker, []byte("corrupt child marker"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if err := ensureRF3ReadAuthorityState(fixture.prepared.groups[1].manifest.Route.MemberRoot, policy); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(childMarker)
			if err != nil {
				t.Fatal(err)
			}
			parentMarker := rf3ReadAuthorityMarkerPath(fixture.prepared.groups[0].manifest.Route.MemberRoot)
			if _, err := os.Stat(parentMarker); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("parent marker existed before refusal: %v", err)
			}
			cache, evidence, err := configureRF3ReadAuthorities(
				fixture.manifest, fixture.prepared.groups, fixture.runtimes,
				fixture.profile, fixture.authz, fixture.profile.LocalIdentity().Node,
			)
			if !errors.Is(err, errRF3ReadAuthorityDowngrade) {
				t.Fatalf("child marker refusal = %v, want downgrade", err)
			}
			if cache != nil || evidence != nil {
				t.Fatalf("marker refusal returned cache/evidence: %p/%v", cache, evidence)
			}
			if _, err := os.Stat(parentMarker); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("parent marker created before child refusal: %v", err)
			}
			after, err := os.ReadFile(childMarker)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(after, before) {
				t.Fatal("child marker changed during refusal")
			}
			for index, runtime := range fixture.runtimes {
				if got := runtime.ReadAuthorityEvidence().Status; got != raftmember.ReadAuthorityEvidenceDisabled {
					t.Fatalf("runtime %d status after refusal = %v, want disabled", index, got)
				}
			}
		})
	}
}

func TestRF3ReadAuthorityRecoveredChildSelectionDoesNotHideOrdinaryRosterMismatch(t *testing.T) {
	fixture := newRF3RecoveredReadAuthorityFixture(t)
	markRF3RecoveredChild(t, fixture)
	localMember := fixture.runtimes[0].Identity().MemberID
	for index := range fixture.prepared.groups[0].manifest.Members {
		if fixture.prepared.groups[0].manifest.Members[index].MemberID == localMember {
			fixture.prepared.groups[0].manifest.Members[index].StoreID = [16]byte{0xaa}
			break
		}
	}
	cache, evidence, err := configureRF3ReadAuthorities(
		fixture.manifest, fixture.prepared.groups, fixture.runtimes,
		fixture.profile, fixture.authz, fixture.profile.LocalIdentity().Node,
	)
	if !errors.Is(err, errRF3ReadAuthority) {
		t.Fatalf("ordinary wrong-store startup error = %v, want read-authority refusal", err)
	}
	if cache != nil || evidence != nil {
		t.Fatalf("ordinary mismatch returned cache/evidence: %p/%v", cache, evidence)
	}
	for index, item := range fixture.prepared.groups {
		if _, err := os.Stat(rf3ReadAuthorityMarkerPath(item.manifest.Route.MemberRoot)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("group %d marker after ordinary mismatch = %v, want absent", index, err)
		}
	}
}
