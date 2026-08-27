package main

import (
	"bytes"
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

func TestPrepareRF3PublishesCompleteRestartableMemberAndReopensExactly(t *testing.T) {
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
	requestLedgerStart := [32]byte{0x20}
	requestLedgerEnd := [32]byte{0x90}
	requestLedgerIdentity := [32]byte{0x5a}
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
		Apply: prepareRF3Apply{
			MaxSessions: 32, RetryWindow: 8, MaxCollections: 16,
			MaxDocuments: 1024, MaxBytes: 384 << 20, ShardKey: "id",
			RequestLedgerCapacityBytes:       64 << 20,
			RequestLedgerCleanupReserveBytes: 8 << 20,
			RequestLedgerRangeStart:          idString(requestLedgerStart[:]),
			RequestLedgerRangeEnd:            idString(requestLedgerEnd[:]),
			RequestLedgerRangeIdentity:       idString(requestLedgerIdentity[:]),
		},
		Listeners: rf3ManifestListeners{
			Peer: "127.0.0.1:21001", Native: "127.0.0.1:22001",
			Snapshot: "127.0.0.1:22501", Control: "127.0.0.1:23001",
		},
		TLS:                 rf3ManifestTLS{Certificate: credentials[0].Certificate, Key: credentials[0].Key, Roots: roots, IdentityOID: "1.3.6.1.4.1.32473.1.1"},
		AuthorizationPolicy: policy,
		SplitControl: prepareRF3SplitControl{
			MaxRecords: 4096, MaxFileBytes: 64 << 20,
			MaxChildOperations: 8, StageCheckpointBytes: 32 << 20,
			Grants: make([]prepareRF3ActionGrant, rf3ManifestMembers),
		},
		Members: make([]prepareRF3Member, rf3ManifestMembers),
	}
	for index := range input.Members {
		input.Members[index] = prepareRF3Member{MemberID: uint64(index + 1), NodeID: idString(nodes[index][:]), PeerAddress: "127.0.0.1:" + []string{"21001", "21002", "21003"}[index]}
		input.SplitControl.Grants[index] = prepareRF3ActionGrant{
			NodeID: idString(nodes[index][:]), Actions: ^uint16(0),
		}
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
	if manifest.Route.Group != group || manifest.Route.MemberRoot != root ||
		manifest.Route.SplitRuntimeRoot != filepath.Join(root, "split-runtime") ||
		manifest.Route.MembershipGrantPath != filepath.Join(root, "membership-grant") ||
		manifest.SplitControl.JournalPath != filepath.Join(root, "split-control.journal") ||
		manifest.SplitControl.MaxRecords != input.SplitControl.MaxRecords ||
		len(manifest.SplitControl.Grants) != rf3ManifestMembers ||
		manifest.SplitControl.ChildRegistry.Root != filepath.Join(root, "split-children") ||
		manifest.SplitControl.ChildRegistry.MaxOperations != input.SplitControl.MaxChildOperations ||
		manifest.SplitControl.ChildRegistry.StageCheckpointBytes != input.SplitControl.StageCheckpointBytes ||
		manifest.SplitControl.ChildRegistry.MemberCount != rf3ManifestMembers {
		t.Fatalf("split preparation = route %+v control %+v", manifest.Route, manifest.SplitControl)
	}
	childApply := manifest.SplitControl.ChildRegistry.Apply
	if childApply.RequestLedgerCapacityBytes != input.Apply.RequestLedgerCapacityBytes ||
		childApply.RequestLedgerCleanupReserveBytes != input.Apply.RequestLedgerCleanupReserveBytes ||
		childApply.RequestLedgerRangeStart != requestLedgerStart ||
		childApply.RequestLedgerRangeEnd != requestLedgerEnd ||
		childApply.RequestLedgerRangeIdentity != requestLedgerIdentity {
		t.Fatalf("split child ledger profile = %+v", childApply)
	}
	for _, name := range []string{"replica-actions", "source-exports", "source-artifacts", "split-runtime", "split-children"} {
		if info, statErr := os.Lstat(filepath.Join(root, name)); statErr != nil || !info.IsDir() {
			t.Fatalf("prepared directory %q = %v, %v", name, info, statErr)
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, "split-children"))
	if err != nil || len(entries) != 1 || entries[0].Name() != "static-bootstrap.pb" {
		t.Fatalf("bounded child registry entries = %v, %v", entries, err)
	}
	base, apply, err := loadRF3RetainedIdentities(manifest)
	if err != nil || base.Binding.MemberID != 1 || apply.MaxSessions != input.Apply.MaxSessions ||
		apply.RequestLedgerCapacityBytes != input.Apply.RequestLedgerCapacityBytes ||
		apply.RequestLedgerCleanupReserveBytes != input.Apply.RequestLedgerCleanupReserveBytes ||
		apply.RequestLedgerRangeStart != requestLedgerStart ||
		apply.RequestLedgerRangeEnd != requestLedgerEnd ||
		apply.RequestLedgerRangeIdentity != requestLedgerIdentity {
		t.Fatalf("retained identities = %+v %+v, %v", base, apply, err)
	}
	if code := runPrepareRF3([]string{"-manifest", inputPath}); code != 0 {
		t.Fatalf("idempotent runPrepareRF3 = %d, want 0", code)
	}
	retained, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*prepareRF3Apply)
	}{
		{"capacity", func(value *prepareRF3Apply) { value.RequestLedgerCapacityBytes++ }},
		{"cleanup reserve", func(value *prepareRF3Apply) { value.RequestLedgerCleanupReserveBytes++ }},
		{"range start", func(value *prepareRF3Apply) {
			changed := requestLedgerStart
			changed[0]++
			value.RequestLedgerRangeStart = idString(changed[:])
		}},
		{"range end", func(value *prepareRF3Apply) {
			changed := requestLedgerEnd
			changed[0]++
			value.RequestLedgerRangeEnd = idString(changed[:])
		}},
		{"range identity", func(value *prepareRF3Apply) {
			changed := requestLedgerIdentity
			changed[0]++
			value.RequestLedgerRangeIdentity = idString(changed[:])
		}},
	} {
		t.Run("rejects retained ledger "+test.name+" drift", func(t *testing.T) {
			candidate := input
			test.mutate(&candidate.Apply)
			changed, marshalErr := vibejson.Marshal(&candidate)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			if writeErr := os.WriteFile(inputPath, changed, 0o600); writeErr != nil {
				t.Fatal(writeErr)
			}
			if code := runPrepareRF3([]string{"-manifest", inputPath}); code != 1 {
				t.Fatalf("drifted runPrepareRF3 = %d, want 1", code)
			}
			after, readErr := os.ReadFile(manifestPath)
			if readErr != nil || !bytes.Equal(after, retained) {
				t.Fatalf("retained manifest changed: %v", readErr)
			}
		})
	}
	if err = os.WriteFile(inputPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	staticPath := filepath.Join(root, "split-children", "static-bootstrap.pb")
	staticRaw, err := os.ReadFile(staticPath)
	if err != nil {
		t.Fatal(err)
	}
	corruptStatic := append([]byte(nil), staticRaw...)
	corruptStatic[len(corruptStatic)-1] ^= 0xff
	if err = os.WriteFile(staticPath, corruptStatic, 0o600); err != nil {
		t.Fatal(err)
	}
	if code := runPrepareRF3([]string{"-manifest", inputPath}); code != 1 {
		t.Fatalf("corrupt child bootstrap runPrepareRF3 = %d, want 1", code)
	}
	if err = os.WriteFile(staticPath, staticRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if code := runPrepareRF3([]string{"-manifest", inputPath}); code != 0 {
		t.Fatalf("restored child bootstrap runPrepareRF3 = %d, want 0", code)
	}
	input.SplitControl.MaxRecords--
	changed, err := vibejson.Marshal(&input)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(inputPath, changed, 0o600); err != nil {
		t.Fatal(err)
	}
	if code := runPrepareRF3([]string{"-manifest", inputPath}); code != 1 {
		t.Fatalf("conflicting runPrepareRF3 = %d, want 1", code)
	}
	after, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, retained) {
		t.Fatal("conflicting retry mutated retained serving manifest")
	}
}

func TestPrepareRF3RequestLedgerApplyProfileIsStrictAndAllOrNothing(t *testing.T) {
	zero := [32]byte{}
	start := [32]byte{0x20}
	end := [32]byte{0x90}
	identity := [32]byte{0x5a}
	valid := prepareRF3Apply{
		MaxSessions: 32, RetryWindow: 8, MaxCollections: 16,
		MaxDocuments: 1024, MaxBytes: 384 << 20, ShardKey: "/id",
		RequestLedgerCapacityBytes:       64 << 20,
		RequestLedgerCleanupReserveBytes: 8 << 20,
		RequestLedgerRangeStart:          idString(start[:]),
		RequestLedgerRangeEnd:            idString(end[:]),
		RequestLedgerRangeIdentity:       idString(identity[:]),
	}
	options, err := prepareRF3ApplyOptions(valid)
	if err != nil || options.RequestLedgerCapacityBytes != valid.RequestLedgerCapacityBytes ||
		options.RequestLedgerCleanupReserveBytes != valid.RequestLedgerCleanupReserveBytes ||
		options.RequestLedgerRangeStart != start || options.RequestLedgerRangeEnd != end ||
		options.RequestLedgerRangeIdentity != identity {
		t.Fatalf("mapped options=%+v err=%v", options, err)
	}
	fullRange := valid
	fullRange.RequestLedgerRangeStart = idString(zero[:])
	fullRange.RequestLedgerRangeEnd = idString(zero[:])
	if options, err = prepareRF3ApplyOptions(fullRange); err != nil ||
		options.RequestLedgerRangeStart != zero || options.RequestLedgerRangeEnd != zero {
		t.Fatalf("full-range options=%+v err=%v", options, err)
	}
	disabled := valid
	disabled.RequestLedgerCapacityBytes = 0
	disabled.RequestLedgerCleanupReserveBytes = 0
	disabled.RequestLedgerRangeStart = ""
	disabled.RequestLedgerRangeEnd = ""
	disabled.RequestLedgerRangeIdentity = ""
	if options, err = prepareRF3ApplyOptions(disabled); err != nil ||
		options.RequestLedgerCapacityBytes != 0 ||
		options.RequestLedgerRangeIdentity != ([32]byte{}) {
		t.Fatalf("disabled options=%+v err=%v", options, err)
	}

	tests := []struct {
		name   string
		mutate func(*prepareRF3Apply)
	}{
		{"missing capacity", func(value *prepareRF3Apply) { value.RequestLedgerCapacityBytes = 0 }},
		{"missing cleanup reserve", func(value *prepareRF3Apply) { value.RequestLedgerCleanupReserveBytes = 0 }},
		{"reserve consumes capacity", func(value *prepareRF3Apply) {
			value.RequestLedgerCleanupReserveBytes = value.RequestLedgerCapacityBytes
		}},
		{"missing start", func(value *prepareRF3Apply) { value.RequestLedgerRangeStart = "" }},
		{"short end", func(value *prepareRF3Apply) { value.RequestLedgerRangeEnd = "90" }},
		{"uppercase identity", func(value *prepareRF3Apply) {
			value.RequestLedgerRangeIdentity = "5A" + value.RequestLedgerRangeIdentity[2:]
		}},
		{"zero identity", func(value *prepareRF3Apply) {
			value.RequestLedgerRangeIdentity = idString(zero[:])
		}},
		{"inverted range", func(value *prepareRF3Apply) {
			value.RequestLedgerRangeStart, value.RequestLedgerRangeEnd =
				value.RequestLedgerRangeEnd, value.RequestLedgerRangeStart
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.mutate(&candidate)
			if _, err := prepareRF3ApplyOptions(candidate); !errors.Is(err, errPrepareRF3) {
				t.Fatalf("error=%v", err)
			}
		})
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
