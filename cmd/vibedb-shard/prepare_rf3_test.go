package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
)

func TestPrepareRF3PublishesCompleteRestartableMemberAndRefusesOverwrite(t *testing.T) {
	parent := t.TempDir()
	nodes := rf3CommandNodes()
	group := rf3CommandGroup()
	credentials, roots, err := rf3testfixture.WriteCredentials(
		parent, rf3CommandIdentityOID,
		rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation},
		nodes[:],
	)
	if err != nil {
		t.Fatal(err)
	}
	policy := filepath.Join(parent, "policy.vibejson")
	if err := os.WriteFile(policy, rf3CommandPolicy(nodes), 0o600); err != nil {
		t.Fatal(err)
	}
	keySource := filepath.Join(parent, "key-source")
	if err := os.WriteFile(keySource, []byte("0123456789abcdef0123456789abcdef"), 0o600); err != nil {
		t.Fatal(err)
	}
	identity := rf3CommandStoreIdentity(1)
	root := filepath.Join(parent, "member-1")
	input := prepareRF3Manifest{
		Root: root, Distribution: identity.Distribution, Shard: identity.Shard,
		ClusterID: idString(identity.ClusterID[:]), ClusterIncarnation: idString(identity.ClusterIncarnation[:]),
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
		AllocationGeneration:  identity.AllocationGeneration,
		ShardIncarnation:      idString(identity.ShardIncarnation[:]), GroupID: idString(identity.GroupID[:]),
		MemberID: identity.MemberID, StoreID: idString(identity.StoreID[:]),
		Table: "docs", CreateTable: `CREATE TABLE docs (PRIMARY KEY (id))`,
		Authority: prepareRF3Authority{ActivePolicyGeneration: 5, ProtectionEpoch: 7, OwnershipEpoch: 11, SchemaGeneration: 13, RoutingVersion: 17, RouteGeneration: 19},
		WAL:       prepareRF3WAL{KeyID: "test-key", KeyMaterialPath: keySource, WrappedKey: "opaque-test-key", MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes, MaxRecords: 4096, MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes},
		Apply:     prepareRF3Apply{MaxSessions: 32, RetryWindow: 8, MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20, ShardKey: "id"},
		Listeners: rf3ManifestListeners{
			Peer: "127.0.0.1:21001", Native: "127.0.0.1:22001",
			Snapshot: "127.0.0.1:22501", Control: "127.0.0.1:23001",
		},
		TLS:                 rf3ManifestTLS{Certificate: credentials[0].Certificate, Key: credentials[0].Key, Roots: roots, IdentityOID: "1.3.6.1.4.1.32473.1.1"},
		AuthorizationPolicy: policy,
		Members:             make([]prepareRF3Member, rf3ManifestMembers),
	}
	for index := range input.Members {
		input.Members[index] = prepareRF3Member{MemberID: uint64(index + 1), NodeID: idString(nodes[index][:]), PeerAddress: "127.0.0.1:" + []string{"21001", "21002", "21003"}[index]}
	}
	raw, err := vibejson.Marshal(&input)
	if err != nil {
		t.Fatal(err)
	}
	inputPath := filepath.Join(parent, "prepare.vibejson")
	if err := os.WriteFile(inputPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadPrepareRF3Manifest(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := provisionRF3Member(loaded); errors.Is(err, storeio.ErrStrictAllocationUnsupported) || errors.Is(err, raftstore.ErrPlatformUnsupported) {
		t.Skipf("strict durable allocation unsupported: %v", err)
	} else if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "serve-rf3.vibejson")
	manifest, err := loadRF3Manifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.WAL.Path != filepath.Join(root, "member.wal") || manifest.SQL.Path != filepath.Join(root, "member.vdb") {
		t.Fatalf("unexpected generated paths: %+v %+v", manifest.WAL, manifest.SQL)
	}
	base, apply, err := loadRF3RetainedIdentities(manifest)
	if err != nil || base.Binding.MemberID != 1 || apply.MaxSessions != input.Apply.MaxSessions {
		t.Fatalf("retained identities = %+v %+v, %v", base, apply, err)
	}
	if code := runPrepareRF3([]string{"-manifest", inputPath}); code != 1 {
		t.Fatalf("overwrite runPrepareRF3 = %d, want 1", code)
	}
}

func TestPrepareRF3RejectsNoncanonicalInputWithoutCreatingRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "member")
	path := filepath.Join(parent, "prepare.vibejson")
	if err := os.WriteFile(path, []byte("{ \"root\":\""+root+"\"}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := runPrepareRF3([]string{"-manifest", path}); code != 1 {
		t.Fatalf("runPrepareRF3 = %d, want 1", code)
	}
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("root exists after rejection: %v", err)
	}
}

func idString(raw []byte) string {
	const digits = "0123456789abcdef"
	result := make([]byte, len(raw)*2)
	for index, value := range raw {
		result[index*2], result[index*2+1] = digits[value>>4], digits[value&15]
	}
	return string(result)
}
