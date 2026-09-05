package main

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestBoundedWriterRejectsRecordPastLimit(t *testing.T) {
	var output bytes.Buffer
	writer := &boundedWriter{writer: &output, left: 3}
	if _, err := writer.Write([]byte("four")); !errors.Is(err, io.ErrShortBuffer) {
		t.Fatalf("oversized record error = %v", err)
	}
	if output.Len() != 0 || writer.left != 3 {
		t.Fatalf("oversized record changed output: bytes=%q left=%d", output.Bytes(), writer.left)
	}
	if _, err := writer.Write([]byte("ok")); err != nil {
		t.Fatalf("bounded record: %v", err)
	}
	if output.String() != "ok" || writer.left != 1 {
		t.Fatalf("bounded record output=%q left=%d", output.String(), writer.left)
	}
}

func TestNodeIDFromGroupUsesLocalMemberRoute(t *testing.T) {
	var domain rafttransport.TrustDomain
	domain.ClusterID[0], domain.ClusterIncarnation[0] = 1, 2
	group := manifestGroup{}
	group.Route.ClusterID = hex16(domain.ClusterID)
	group.Route.ClusterIncarnation = hex16(domain.ClusterIncarnation)
	group.Route.ShardIncarnation = hex16([16]byte{3})
	group.Route.GroupID = hex16([16]byte{4})
	group.Route.TopologyRecoveryEpoch = 1
	group.Route.MemberID = 2
	group.Route.Distribution, group.Route.Shard = "data", "all"
	group.Members = []manifestMember{{MemberID: 1, NodeID: hex16([16]byte{5})}, {MemberID: 2, NodeID: hex16([16]byte{6})}, {MemberID: 3, NodeID: hex16([16]byte{7})}}
	got, err := nodeIDFromGroup(group, domain)
	if err != nil {
		t.Fatal(err)
	}
	if got != (rafttransport.NodeID{6}) {
		t.Fatalf("local node = %x, want 06", got)
	}
}

func TestGroupFromManifestRejectsWrongTrustDomain(t *testing.T) {
	group := manifestGroup{}
	group.Route.ClusterID = hex16([16]byte{1})
	group.Route.ClusterIncarnation = hex16([16]byte{2})
	group.Route.ShardIncarnation = hex16([16]byte{3})
	group.Route.GroupID = hex16([16]byte{4})
	group.Route.TopologyRecoveryEpoch = 1
	group.Route.MemberID = 1
	group.Route.Distribution, group.Route.Shard = "data", "all"
	group.Members = []manifestMember{{MemberID: 1, NodeID: hex16([16]byte{5})}, {MemberID: 2, NodeID: hex16([16]byte{6})}, {MemberID: 3, NodeID: hex16([16]byte{7})}}
	if _, err := groupFromManifest(group, rafttransport.TrustDomain{ClusterID: [16]byte{9}, ClusterIncarnation: [16]byte{2}}); err == nil {
		t.Fatal("group accepted a mismatched trust domain")
	}
}

func hex16(value [16]byte) string {
	const digits = "0123456789abcdef"
	result := make([]byte, 32)
	for index, part := range value {
		result[index*2] = digits[part>>4]
		result[index*2+1] = digits[part&15]
	}
	return string(result)
}
