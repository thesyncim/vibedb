package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/rf3testfixture"
)

func TestRF3FixtureBootstrapMatchesPreparedChildCut(t *testing.T) {
	for _, voters := range [][]uint64{{1}, {1, 2, 3}} {
		root := t.TempDir()
		bootstrap := rf3testfixture.InitialBootstrap(voters)
		if err := rf3testfixture.PrepareSplitRuntime(root, bootstrap); err != nil {
			t.Fatal(err)
		}
		var registry rf3ManifestSplitChildRegistry
		registry.MemberCount = uint8(len(voters))
		registry.StaticBootstrapPath = filepath.Join(root, "split-children", "static-bootstrap.pb")
		members := make([]prepareRF3Member, len(voters))
		for i, voter := range voters {
			registry.Members[i].MemberID = voter
			members[i].MemberID = voter
		}
		want, err := prepareRF3SplitChildBootstrap(members)
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(registry.StaticBootstrapPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatal("fixture and shipped child bootstrap differ")
		}
		if _, err := loadRF3SplitStaticBootstrap(registry); err != nil {
			t.Fatal(err)
		}
	}
}
