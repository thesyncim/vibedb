package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRunServeRF3ArgumentExitClasses(t *testing.T) {
	directory := t.TempDir()
	manifestPath := filepath.Join(directory, "rf3.json")
	manifestDocument := strings.Replace(
		canonicalRF3Manifest,
		"/srv/vibedb/member-sql-identity.json",
		filepath.Join(directory, "missing-sql-identity.json"),
		1,
	)
	if err := os.WriteFile(manifestPath, []byte(manifestDocument), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		args []string
		want int
	}{
		{"dispatch_missing_manifest", []string{"vibedb-shard", "serve-rf3"}, 2},
		{"unknown_flag", []string{"vibedb-shard", "serve-rf3", "-unknown"}, 2},
		{"missing_flag_value", []string{"vibedb-shard", "serve-rf3", "-manifest"}, 2},
		{"trailing_argument", []string{"vibedb-shard", "serve-rf3", "-manifest", manifestPath, "extra"}, 2},
		{"missing_manifest_file", []string{"vibedb-shard", "serve-rf3", "-manifest", filepath.Join(directory, "missing")}, 2},
		// Loading the manifest succeeds, so a missing retained SQL identity is a
		// serving/preflight failure rather than command grammar failure.
		{"runtime_preflight_failure", []string{"vibedb-shard", "serve-rf3", "-manifest", manifestPath}, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := run(tc.args); got != tc.want {
				t.Fatalf("run(%q) = %d, want %d", tc.args, got, tc.want)
			}
		})
	}
}

func TestValidateRF3Addresses(t *testing.T) {
	valid := serveRF3TestManifest()
	if err := validateRF3Addresses(valid); err != nil {
		t.Fatalf("valid addresses: %v", err)
	}
	wildcard := valid
	wildcard.Listeners.Peer = ":17400"
	wildcard.Listeners.Native = "[::]:17500"
	if err := validateRF3Addresses(wildcard); err != nil {
		t.Fatalf("valid wildcard listeners: %v", err)
	}
	withTarget := valid
	withTarget.EnrolledTarget = &rf3ManifestEnrolledTarget{
		MemberID: 4, NodeID: rafttransport.NodeID{4},
		PeerAddress: "member-4.internal:17400", NativeAddress: "member-4.internal:17500",
		SnapshotAddress: "member-4.internal:17600", ControlAddress: "member-4.internal:17700",
	}
	if err := validateRF3Addresses(withTarget); err != nil {
		t.Fatalf("valid enrolled target addresses: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*rf3Manifest)
	}{
		{"same_listeners", func(manifest *rf3Manifest) { manifest.Listeners.Native = manifest.Listeners.Peer }},
		{"peer_listener_missing_port", func(manifest *rf3Manifest) { manifest.Listeners.Peer = "127.0.0.1" }},
		{"native_listener_zero_port", func(manifest *rf3Manifest) { manifest.Listeners.Native = "127.0.0.1:0" }},
		{"listener_port_overflow", func(manifest *rf3Manifest) { manifest.Listeners.Peer = "127.0.0.1:65536" }},
		{"listener_nonnumeric_port", func(manifest *rf3Manifest) { manifest.Listeners.Peer = "127.0.0.1:http" }},
		{"member_missing_host", func(manifest *rf3Manifest) { manifest.Members[1].PeerAddress = ":17401" }},
		{"member_missing_port", func(manifest *rf3Manifest) { manifest.Members[1].PeerAddress = "member-2.internal" }},
		{"member_zero_port", func(manifest *rf3Manifest) { manifest.Members[1].PeerAddress = "member-2.internal:0" }},
		{"target_missing_host", func(manifest *rf3Manifest) {
			manifest.EnrolledTarget = &rf3ManifestEnrolledTarget{
				MemberID: 4, NodeID: rafttransport.NodeID{4}, PeerAddress: ":17400",
				NativeAddress: "member-4.internal:17500", SnapshotAddress: "member-4.internal:17600",
				ControlAddress: "member-4.internal:17700",
			}
		}},
		{"target_control_zero_port", func(manifest *rf3Manifest) {
			manifest.EnrolledTarget = &rf3ManifestEnrolledTarget{
				MemberID: 4, NodeID: rafttransport.NodeID{4}, PeerAddress: "member-4.internal:17400",
				NativeAddress: "member-4.internal:17500", SnapshotAddress: "member-4.internal:17600",
				ControlAddress: "member-4.internal:0",
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			manifest := valid
			tc.mutate(&manifest)
			if err := validateRF3Addresses(manifest); !errors.Is(err, errRF3Serving) {
				t.Fatalf("validation error = %v, want errRF3Serving", err)
			}
		})
	}
}

func TestBuildRF3RosterRequiresExactStableDurableRF3(t *testing.T) {
	manifest := serveRF3TestManifest()
	group := serveRF3TestGroup()
	stable := raftmodel.Publication{
		ReplicaSetVersion: 9,
		ConfState:         &pb.ConfState{Voters: []uint64{1, 2, 3}},
	}
	members, remote, dial, err := buildRF3Roster(manifest, group, 2, stable)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 3 || len(remote) != 2 || dial == nil {
		t.Fatalf("members=%d remote=%d dial-nil=%t", len(members), len(remote), dial == nil)
	}
	for index, member := range members {
		configured := manifest.Members[index]
		if member.Group != group || member.ReplicaSetVersion != stable.ReplicaSetVersion ||
			member.MemberID != configured.MemberID || member.Node != configured.NodeID ||
			member.Role != rafttransport.MemberVoter {
			t.Fatalf("member %d = %+v, configured=%+v", index, member, configured)
		}
	}
	if remote[0] != manifest.Members[0].NodeID || remote[1] != manifest.Members[2].NodeID {
		t.Fatalf("remote nodes = %x, want members 1 and 3", remote)
	}
	withTarget := manifest
	withTarget.EnrolledTarget = &rf3ManifestEnrolledTarget{
		MemberID: 4, NodeID: rafttransport.NodeID{4}, PeerAddress: "member-4.internal:17400",
		NativeAddress: "member-4.internal:17500", SnapshotAddress: "member-4.internal:17600",
		ControlAddress: "member-4.internal:17700",
	}
	targetMembers, targetRemote, _, err := buildRF3Roster(withTarget, group, 2, stable)
	if err != nil || len(targetMembers) != rf3ManifestMembers || len(targetRemote) != rf3ManifestMembers-1 {
		t.Fatalf("enrolled target changed serving roster: members=%d remote=%d err=%v", len(targetMembers), len(targetRemote), err)
	}
	for _, member := range targetMembers {
		if member.MemberID == withTarget.EnrolledTarget.MemberID {
			t.Fatal("enrolled target entered serving transport roster")
		}
	}
	if _, err := dial(context.Background(), rafttransport.NodeID{0xff}); !errors.Is(err, rafttransport.ErrNodeNotFound) {
		t.Fatalf("unknown-node dial error = %v, want ErrNodeNotFound", err)
	}

	tests := []struct {
		name        string
		publication raftmodel.Publication
		local       uint64
		mutate      func(*rf3Manifest)
	}{
		{"zero_version", raftmodel.Publication{ConfState: stable.ConfState}, 2, nil},
		{"nil_conf_state", raftmodel.Publication{ReplicaSetVersion: 9}, 2, nil},
		{"rf2", serveRF3Publication(1, 2), 2, nil},
		{"rf4", serveRF3Publication(1, 2, 3, 4), 2, nil},
		{"learner", raftmodel.Publication{ReplicaSetVersion: 9, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}}, 2, nil},
		{"joint_outgoing", raftmodel.Publication{ReplicaSetVersion: 9, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}, VotersOutgoing: []uint64{1, 2, 4}}}, 2, nil},
		{"joint_learner_next", raftmodel.Publication{ReplicaSetVersion: 9, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}, LearnersNext: []uint64{4}}}, 2, nil},
		{"joint_auto_leave", raftmodel.Publication{ReplicaSetVersion: 9, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}, AutoLeave: proto.Bool(true)}}, 2, nil},
		{"unsorted_voters", serveRF3Publication(1, 3, 2), 2, func(manifest *rf3Manifest) {
			manifest.Members[1], manifest.Members[2] = manifest.Members[2], manifest.Members[1]
		}},
		{"manifest_roster_mismatch", stable, 2, func(manifest *rf3Manifest) { manifest.Members[2].MemberID = 4 }},
		{"retained_member_absent", stable, 4, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidate := manifest
			if tc.mutate != nil {
				tc.mutate(&candidate)
			}
			if _, _, _, err := buildRF3Roster(candidate, group, tc.local, tc.publication); !errors.Is(err, errRF3Serving) {
				t.Fatalf("build error = %v, want errRF3Serving", err)
			}
		})
	}
}

func TestLoadRF3WALKeyExactBoundAndCallerClearing(t *testing.T) {
	directory := t.TempDir()
	material := make([]byte, 32)
	for index := range material {
		material[index] = byte(index + 1)
	}
	path := filepath.Join(directory, "wal.key")
	if err := os.WriteFile(path, material, 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := loadRF3WALKey("key-1", path)
	if err != nil {
		t.Fatal(err)
	}
	if key.ID != "key-1" || key.Wrapped != nil || string(key.Material[:]) != string(material) {
		t.Fatalf("loaded key ID=%q wrapped=%v material=%x", key.ID, key.Wrapped, key.Material)
	}
	// The returned fixed array owns its copy; changing the source file cannot
	// change live key material. The serving caller is responsible for clearing
	// this final owned copy after raftstore.Open.
	if err := os.WriteFile(path, make([]byte, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	if string(key.Material[:]) != string(material) {
		t.Fatal("loaded key aliases file/read scratch")
	}
	clear(key.Material[:])
	if key.Material != ([32]byte{}) {
		t.Fatalf("caller clear retained material %x", key.Material)
	}

	maximumID := strings.Repeat("k", raftstore.MaxKeyIDBytes)
	if _, err := loadRF3WALKey(maximumID, path); err != nil {
		t.Fatalf("maximum key ID: %v", err)
	}
	for name, id := range map[string]string{
		"empty_id":    "",
		"oversize_id": strings.Repeat("k", raftstore.MaxKeyIDBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadRF3WALKey(id, path); !errors.Is(err, errRF3Serving) {
				t.Fatalf("load error = %v, want errRF3Serving", err)
			}
		})
	}
	for _, size := range []int{0, 31, 33} {
		t.Run("material_bytes_"+strconv.Itoa(size), func(t *testing.T) {
			candidate := filepath.Join(directory, "key-"+strconv.Itoa(size))
			if err := os.WriteFile(candidate, make([]byte, size), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadRF3WALKey("key-1", candidate); !errors.Is(err, errRF3Serving) {
				t.Fatalf("load %d bytes error = %v, want errRF3Serving", size, err)
			}
		})
	}
	if _, err := loadRF3WALKey("key-1", filepath.Join(directory, "missing")); !errors.Is(err, errRF3Serving) {
		t.Fatalf("missing key error = %v, want errRF3Serving", err)
	}
}

func TestRF3DerivedGroupWALAndCommandIdentities(t *testing.T) {
	binding := sqldriver.ReplicatedShardStoreBinding{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, Distribution: "orders", Shard: "0000-ffff",
		AllocationGeneration: 4, ShardIncarnation: [16]byte{5}, GroupID: [16]byte{6},
		MemberID: 2, StoreID: [16]byte{7},
		Authority: sqldriver.ReplicatedAuthorityProfile{
			ActivePolicyGeneration: 8, ProtectionEpoch: 9, OwnershipEpoch: 10,
			SchemaGeneration: 11, RoutingVersion: 12, RouteGeneration: 13,
		},
	}
	wantGroup := raftmember.GroupKey{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		ShardIncarnation:      binding.ShardIncarnation, GroupID: binding.GroupID,
	}
	if got := groupFromBinding(binding); got != wantGroup {
		t.Fatalf("group = %+v, want %+v", got, wantGroup)
	}
	wantWAL := raftstore.Identity{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		Distribution: binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		MemberID: binding.MemberID, StoreID: binding.StoreID,
	}
	if got := walIdentityFromBinding(binding); got != wantWAL {
		t.Fatalf("WAL identity = %+v, want %+v", got, wantWAL)
	}

	runtimeIdentity := raftmember.RuntimeIdentity{RelationManifestDigest: [32]byte{14}}
	wantFence := raftservice.CommandFence{
		ReplicaSetVersion:      15,
		ActivePolicyGeneration: binding.Authority.ActivePolicyGeneration,
		ProtectionEpoch:        binding.Authority.ProtectionEpoch,
		OwnershipEpoch:         binding.Authority.OwnershipEpoch,
		SchemaGeneration:       binding.Authority.SchemaGeneration,
		RelationManifestDigest: runtimeIdentity.RelationManifestDigest,
		RoutingVersion:         binding.Authority.RoutingVersion,
		RouteGeneration:        binding.Authority.RouteGeneration,
	}
	if got := commandFenceFromPublication(binding.Authority, runtimeIdentity, 15); got != wantFence || !got.Valid() {
		t.Fatalf("command fence = %+v, want valid %+v", got, wantFence)
	}
}

func TestRF3TransportCarriesMaximumValidFrameWithoutRetainingItWarm(t *testing.T) {
	peers := []rafttransport.NodeID{{1}, {2}}
	options := rf3TransportOptions(peers, func() time.Time { return time.Now().Add(time.Second) })
	wireMaximum := rafttransport.StreamRecordHeaderBytes + rafttransport.MaxFrameBytes
	if options.Coalesce.MaxBytes < wireMaximum {
		t.Fatalf("coalesce bytes = %d, below maximum wire frame %d", options.Coalesce.MaxBytes, wireMaximum)
	}
	// MaxFrameBytes is just above 16 MiB, so its power-of-two owned capacity is
	// 32 MiB. Both followers must be able to retain one independently.
	if options.Queue.PerPeerBytes < 32<<20 ||
		options.Queue.GlobalBytes < int64(len(peers))*options.Queue.PerPeerBytes {
		t.Fatalf("queue bytes = per-peer %d global %d", options.Queue.PerPeerBytes, options.Queue.GlobalBytes)
	}
	if options.Coalesce.RetainedBytes != rafttransport.DefaultRetainedFrameBytes ||
		options.RetainedFrameBytes != rafttransport.DefaultRetainedFrameBytes {
		t.Fatalf("one-off maximum frame would remain warm: %+v", options.Coalesce)
	}
}

func serveRF3Publication(voters ...uint64) raftmodel.Publication {
	return raftmodel.Publication{
		ReplicaSetVersion: 9,
		ConfState:         &pb.ConfState{Voters: voters},
	}
}

func serveRF3TestManifest() rf3Manifest {
	return rf3Manifest{
		Listeners: rf3ManifestListeners{Peer: "127.0.0.1:17400", Native: "127.0.0.1:17500"},
		Members: [rf3ManifestMembers]rf3ManifestMember{
			{MemberID: 1, NodeID: rafttransport.NodeID{1}, PeerAddress: "member-1.internal:17400"},
			{MemberID: 2, NodeID: rafttransport.NodeID{2}, PeerAddress: "member-2.internal:17400"},
			{MemberID: 3, NodeID: rafttransport.NodeID{3}, PeerAddress: "member-3.internal:17400"},
		},
	}
}

func serveRF3TestGroup() raftmember.GroupKey {
	return raftmember.GroupKey{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5},
	}
}
