package main

import "testing"

func TestNodeManifestSingleGroupSelectionRetainsGroupedGrammar(t *testing.T) {
	var first, second rf3ManifestGroup
	first.Route.Group.GroupID[0] = 1
	second.Route.Group.GroupID[0] = 2
	manifest := rf3Manifest{NodeLog: &rf3NodeLogManifest{}, Groups: []rf3ManifestGroup{first, second}}
	selected := manifest.withGroup(second)
	bundles := selected.groupBundles()
	if len(bundles) != 1 || bundles[0].Route.Group != second.Route.Group || selected.NodeLog != manifest.NodeLog {
		t.Fatalf("selected groups=%+v", bundles)
	}
	empty := rf3Manifest{NodeLog: manifest.NodeLog}
	if len(empty.groupBundles()) != 0 {
		t.Fatal("empty physical node acquired a group")
	}
}
