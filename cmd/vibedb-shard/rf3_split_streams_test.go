package main

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestRF3SplitStreamInventoryRejectsNodeAndSnapshotAliasing(t *testing.T) {
	opener := &rf3SplitStreamOpener{
		control:  make(map[rafttransport.NodeID]string),
		snapshot: make(map[rafttransport.NodeID]string),
	}
	one, two := rafttransport.NodeID{1}, rafttransport.NodeID{2}
	if !opener.install(one, "127.0.0.1:7001", "127.0.0.1:8001") ||
		!opener.install(one, "127.0.0.1:7001", "127.0.0.1:8001") {
		t.Fatal("exact inventory replay rejected")
	}
	if opener.install(one, "127.0.0.1:7002", "127.0.0.1:8001") {
		t.Fatal("same node accepted conflicting control address")
	}
	if opener.install(two, "127.0.0.1:7002", "127.0.0.1:8001") {
		t.Fatal("distinct node accepted aliased snapshot listener")
	}
	if !opener.install(two, "127.0.0.1:7002", "127.0.0.1:8002") {
		t.Fatal("distinct exact inventory rejected")
	}
}
