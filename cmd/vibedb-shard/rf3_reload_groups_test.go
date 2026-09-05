package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

func TestRF3AppendPreservesCertifiedAndPendingSplitState(t *testing.T) {
	root := t.TempDir()
	raw := []byte(strings.ReplaceAll(canonicalRF3Manifest, "/srv/vibedb", root))
	base, err := parseRF3Manifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := openRF3AdoptedGroupInventory(base)
	if err != nil {
		t.Fatal(err)
	}
	entry := testRF3AdoptedEntry(7)
	entry.group = 0
	if err := inventory.record(entry); err != nil {
		t.Fatal(err)
	}
	if err := inventory.Close(); err != nil {
		t.Fatal(err)
	}
	admissions, slots, err := openRF3ChildAdmissionStore(root, base.Digest, base.SplitControl.operationLimit())
	if err != nil {
		t.Fatal(err)
	}
	slots[0].operation, slots[0].certificates[0], slots[0].requests[0] = [32]byte{9}, [32]byte{10}, [32]byte{11}
	if err := admissions.save(slots); err != nil {
		t.Fatal(err)
	}
	if err := admissions.Close(); err != nil {
		t.Fatal(err)
	}
	next := base
	group := base.groupBundles()[0]
	group.Route.Group.GroupID[0]++
	group.WAL.Path += "-new"
	group.SQL.Path += "-new"
	next.Groups = append(base.groupBundles(), group)
	next.Digest = sha256.Sum256([]byte("test append manifest"))
	if got, err := openRF3AdoptedGroupInventory(next); err == nil {
		got.Close()
		t.Fatal("unproved manifest append accepted")
	}
	if got, _, err := openRF3ChildAdmissionStore(root, next.Digest, next.SplitControl.operationLimit(), next); err == nil {
		got.Close()
		t.Fatal("unproved admission append accepted")
	}
	directory := filepath.Join(root, "prepared-manifests")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, hex.EncodeToString(base.Digest[:])+".vibejson"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		got, err := openRF3AdoptedGroupInventory(next)
		if err != nil {
			t.Fatal(err)
		}
		if got.entries[0] != entry {
			t.Fatal("lost certified split state")
		}
		if err := got.Close(); err != nil {
			t.Fatal(err)
		}
		admissions, retained, err := openRF3ChildAdmissionStore(root, next.Digest, next.SplitControl.operationLimit(), next)
		if err != nil {
			t.Fatal(err)
		}
		if retained != slots {
			t.Fatal("lost pending child preparations")
		}
		if err := admissions.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRF3ReloadOnlyAppendsIndependentPreparedGroups(t *testing.T) {
	base, err := parseRF3Manifest([]byte(canonicalRF3Manifest))
	if err != nil {
		t.Fatal(err)
	}
	group := base.groupBundles()[0]
	group.Route.Group.GroupID[0]++
	group.WAL.Path += "-new"
	group.SQL.Path += "-new"
	next := base
	next.Groups = append(base.groupBundles(), group)
	if err := validateRF3GroupAppend(base, next); err != nil {
		t.Fatal(err)
	}
	if err := validateRF3GroupAppend(next, next); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*rf3Manifest){
		func(m *rf3Manifest) { m.Groups = m.Groups[:1] },
		func(m *rf3Manifest) { m.Listeners.Native = "127.0.0.1:99" },
		func(m *rf3Manifest) { m.AuthorizationPolicy = "/different-policy" },
		func(m *rf3Manifest) { m.Groups[0].SQL.Path += "-replacement" },
		func(m *rf3Manifest) { m.Groups[1].Route.Group = raftmember.GroupKey{} },
		func(m *rf3Manifest) { m.Groups[1].Members[0].PeerAddress = "127.0.0.1:99" },
		func(m *rf3Manifest) { m.Groups[1].WAL.Path = m.Groups[0].WAL.Path },
	} {
		bad := next
		bad.Groups = append([]rf3ManifestGroup(nil), next.Groups...)
		change(&bad)
		if err := validateRF3GroupAppend(next, bad); err == nil {
			t.Fatal("reload accepted mutation/removal of retained configuration")
		}
	}
}

func TestRF3ReloadAcceptsIndependentPhysicalGroupRosters(t *testing.T) {
	base, err := parseRF3Manifest([]byte(canonicalRF3Manifest))
	if err != nil {
		t.Fatal(err)
	}
	group := base.groupBundles()[0]
	group.Route.Group.GroupID[0]++
	group.WAL.Path += "-second"
	group.SQL.Path += "-second"
	group.Members[0] = base.Members[1]
	group.Members[1] = base.Members[2]
	fourth := base.Members[0]
	fourth.MemberID, fourth.PeerAddress = 4, "member-4.internal:7400"
	for index := range fourth.NodeID {
		fourth.NodeID[index] = 0x41 + byte(index)
	}
	group.Members[2] = fourth
	group.MemberCount = rf3ManifestMembers
	next := base
	next.Groups = append(base.groupBundles(), group)
	if err := validateRF3GroupAppend(base, next); err != nil {
		t.Fatalf("independent group roster refused: %v", err)
	}
}

func TestRF3ReloadRejectsReorderingAndIncompletePhysicalRosters(t *testing.T) {
	base, err := parseRF3Manifest([]byte(multiGroupRF3Manifest(t)))
	if err != nil {
		t.Fatal(err)
	}
	third := base.Groups[1]
	third.Route.Group.GroupID[0]++
	third.WAL.Path += "-third"
	third.SQL.Path += "-third"
	for name, change := range map[string]func(*rf3Manifest){
		"insert before retained group": func(next *rf3Manifest) { next.Groups = []rf3ManifestGroup{base.Groups[0], third, base.Groups[1]} },
		"reorder existing":             func(next *rf3Manifest) { next.Groups = []rf3ManifestGroup{base.Groups[1], base.Groups[0]} },
		"duplicate group member":       func(next *rf3Manifest) { next.Groups[1].Members[1] = next.Groups[1].Members[0] },
		"duplicate member id":          func(next *rf3Manifest) { next.Groups[1].Members[1].MemberID = next.Groups[1].Members[0].MemberID },
		"invalid peer address":         func(next *rf3Manifest) { next.Groups[1].Members[1].PeerAddress = "invalid" },
		"gateway omits hosted nodes":   func(next *rf3Manifest) { next.Gateway = &rf3ManifestGateway{} },
	} {
		t.Run(name, func(t *testing.T) {
			next := base
			next.Groups = append([]rf3ManifestGroup(nil), base.Groups...)
			change(&next)
			if err := validateRF3GroupTransition(base, next); err == nil {
				t.Fatal("accepted invalid reload")
			}
		})
	}
}

func TestRF3ReloadTransportRosterRejectsUnknownPeersBeforeEnrollment(t *testing.T) {
	base, err := parseRF3Manifest([]byte(multiGroupRF3Manifest(t)))
	if err != nil {
		t.Fatal(err)
	}
	fourth, fifth := base.Groups[0].Members[1], base.Groups[0].Members[2]
	fourth.NodeID[0], fifth.NodeID[0] = 0xa1, 0xb1
	fourth.PeerAddress, fifth.PeerAddress = "member-4.internal:7400", "member-5.internal:7400"
	base.Groups[1].Members[1], base.Groups[1].Members[2] = fourth, fifth
	third := base.Groups[1]
	third.Route.Group.GroupID[0]++
	third.WAL.Path += "-third"
	third.SQL.Path += "-third"
	third.Members[1] = base.Groups[0].Members[1]
	next := base
	next.Groups = append(append([]rf3ManifestGroup(nil), base.Groups...), third)
	if err := validateRF3GroupTransition(base, next); err != nil {
		t.Fatal(err)
	}
	if err := validateRF3ReloadTransportRoster(base, next); err != nil {
		t.Fatalf("recombined startup peers refused: %v", err)
	}
	next.Groups[2].Members[2].NodeID[0] = 0xc1
	next.Groups[2].Members[2].PeerAddress = "member-6.internal:7400"
	if err := validateRF3GroupTransition(base, next); err != nil {
		t.Fatalf("fixture is not an independently valid RF3 group: %v", err)
	}
	if err := validateRF3ReloadTransportRoster(base, next); err == nil {
		t.Fatal("runtime accepted an unprepared physical transport peer")
	}
}

func TestRF3GroupTransitionAcceptsExactNonPrimaryRetirement(t *testing.T) {
	base, err := parseRF3Manifest([]byte(canonicalRF3Manifest))
	if err != nil {
		t.Fatal(err)
	}
	second := base.groupBundles()[0]
	second.Route.Group.GroupID[0]++
	second.WAL.Path += "-second"
	second.SQL.Path += "-second"
	current := base
	current.Groups = append(base.groupBundles(), second)
	next := current
	next.Groups = next.Groups[:1]
	if err := validateRF3GroupTransition(current, next); err != nil {
		t.Fatalf("exact retirement refused: %v", err)
	}
	wrong := current
	wrong.Groups = []rf3ManifestGroup{second}
	if err := validateRF3GroupTransition(current, wrong); err == nil {
		t.Fatal("retirement replaced the process primary group")
	}
	changed := next
	changed.Groups = append([]rf3ManifestGroup(nil), next.Groups...)
	changed.Groups[0].SQL.Path += "-changed"
	if err := validateRF3GroupTransition(current, changed); err == nil {
		t.Fatal("retirement changed a retained group")
	}
}

func TestRF3RetirementReopenAcceptsRetainedPredecessorManifest(t *testing.T) {
	root := t.TempDir()
	currentRaw := []byte(strings.ReplaceAll(multiGroupRF3Manifest(t), "/srv/vibedb", root))
	current, err := parseRF3Manifest(currentRaw)
	if err != nil {
		t.Fatal(err)
	}
	nextRaw := []byte(strings.ReplaceAll(canonicalRF3Manifest, "/srv/vibedb", root))
	next, err := parseRF3Manifest(nextRaw)
	if err != nil {
		t.Fatal(err)
	}
	if err = validateRF3GroupTransition(current, next); err != nil {
		t.Fatal(err)
	}
	inventory, err := openRF3AdoptedGroupInventory(current)
	if err != nil {
		t.Fatal(err)
	}
	if err = inventory.Close(); err != nil {
		t.Fatal(err)
	}
	admissions, _, err := openRF3ChildAdmissionStore(root, current.Digest, current.SplitControl.operationLimit())
	if err != nil {
		t.Fatal(err)
	}
	if err = admissions.Close(); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "prepared-manifests")
	if err = os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, hex.EncodeToString(current.Digest[:])+".vibejson"), currentRaw, 0600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		reopened, openErr := openRF3AdoptedGroupInventory(next)
		if openErr != nil {
			t.Fatal(openErr)
		}
		if closeErr := reopened.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		admissions, _, openErr = openRF3ChildAdmissionStore(root, next.Digest, next.SplitControl.operationLimit(), next)
		if openErr != nil {
			t.Fatal(openErr)
		}
		if closeErr := admissions.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}
}
