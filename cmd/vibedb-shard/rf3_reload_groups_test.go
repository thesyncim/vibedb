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
