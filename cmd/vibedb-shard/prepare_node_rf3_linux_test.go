//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

func prepareRF3NodeTestInput(t *testing.T) prepareRF3NodeManifest {
	t.Helper()
	parent := t.TempDir()
	root := filepath.Join(parent, "node")
	nodes, group := rf3CommandNodes(), rf3CommandGroup()
	credentials, roots, err := rf3testfixture.WriteCredentials(parent, rf3CommandIdentityOID, rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}, nodes[:])
	if err != nil {
		t.Fatal(err)
	}
	policy, keySource := filepath.Join(parent, "policy.vibejson"), filepath.Join(parent, "key")
	if err := os.WriteFile(policy, rf3CommandPolicy(nodes), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keySource, []byte("0123456789abcdef0123456789abcdef"), 0o600); err != nil {
		t.Fatal(err)
	}
	input := prepareRF3NodeManifest{Root: root, NodeLog: rf3NodeLogManifest{Format: 1, Path: filepath.Join(root, "node-log"), KeyID: "node-test-key", KeyMaterialPath: keySource, Options: raftstore.NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 256, RecentWaves: 64, MaxEntriesPerGroup: 64, ReaderSlots: 1, MaxGroups: 8}}}
	for i := 0; i < 2; i++ {
		identity := rf3CommandStoreIdentity(1)
		identity.GroupID[15] += byte(i)
		identity.StoreID[15] += byte(i)
		member := prepareRF3Manifest{Root: filepath.Join(root, fmt.Sprintf("group-%d", i)), Distribution: identity.Distribution, Shard: fmt.Sprint(i),
			ClusterID: idString(identity.ClusterID[:]), ClusterIncarnation: idString(identity.ClusterIncarnation[:]), TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
			AllocationGeneration: identity.AllocationGeneration, ShardIncarnation: idString(identity.ShardIncarnation[:]), GroupID: idString(identity.GroupID[:]), MemberID: identity.MemberID, StoreID: idString(identity.StoreID[:]),
			Table: "docs", CreateTable: `CREATE TABLE docs (PRIMARY KEY (id))`,
			Authority: prepareRF3Authority{ActivePolicyGeneration: 5, ProtectionEpoch: 7, OwnershipEpoch: 11, SchemaGeneration: 13, RoutingVersion: 17, RouteGeneration: 19},
			WAL:       prepareRF3WAL{KeyID: input.NodeLog.KeyID, KeyMaterialPath: keySource, WrappedKey: "opaque-test-key", MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes, MaxRecords: 4096, MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes},
			Apply:     prepareRF3Apply{MaxSessions: 32, RetryWindow: 8, MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20, ShardKey: "/id"},
			Listeners: rf3ManifestListeners{Peer: "127.0.0.1:21001", Native: "127.0.0.1:22001", Snapshot: "127.0.0.1:22501", Control: "127.0.0.1:23001"},
			TLS:       rf3ManifestTLS{Certificate: credentials[0].Certificate, Key: credentials[0].Key, Roots: roots, IdentityOID: rf3CommandIdentityOID.String()}, AuthorizationPolicy: policy,
			SplitControl: prepareRF3SplitControl{MaxRecords: 4096, MaxFileBytes: 64 << 20, MaxChildOperations: 8, StageCheckpointBytes: 32 << 20},
		}
		for j, node := range nodes {
			member.Members = append(member.Members, prepareRF3Member{MemberID: uint64(j + 1), NodeID: idString(node[:]), PeerAddress: fmt.Sprintf("127.0.0.1:%d", 21001+j)})
			member.SplitControl.Grants = append(member.SplitControl.Grants, prepareRF3ActionGrant{NodeID: idString(node[:]), Actions: ^uint16(0)})
		}
		input.Groups = append(input.Groups, member)
	}
	return input
}

func TestPrepareRF3NodeSharedLogAndRuntime(t *testing.T) {
	input := prepareRF3NodeTestInput(t)
	raw, err := vibejson.Marshal(&input)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filepath.Dir(input.Root), "prepare-node.vibejson")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if code := runPrepareNodeRF3([]string{"-manifest", path}); code != 0 {
		t.Fatalf("prepare command returned %d", code)
	}
	for _, group := range input.Groups {
		if _, err := os.Lstat(filepath.Join(group.Root, "member.wal")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("per-range WAL exists: %v", err)
		}
	}
	manifest, err := loadRF3Manifest(filepath.Join(input.Root, "serve-rf3.vibejson"))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.NodeLog == nil || len(manifest.Groups) != 2 {
		t.Fatal("node/group manifest lost")
	}
	profile, err := servicetls.LoadProfile(manifest.TLS.Certificate, manifest.TLS.Key, manifest.TLS.Roots, manifest.TLS.IdentityOID, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for restart := 0; restart < 2; restart++ {
		owner, err := openRF3NodeOwner(manifest, profile)
		if err != nil {
			t.Fatal(err)
		}
		set, err := prepareRF3GroupSetOnNode(manifest, profile, sqldriver.ReplicatedOpenOptions{}, owner)
		if err != nil {
			_ = owner.Close()
			t.Fatal(err)
		}
		var runtimes []*raftmember.Runtime
		for i := range set.groups {
			runtime, err := set.groups[i].adoptRuntime()
			if err != nil {
				t.Fatal(err)
			}
			if runtime.Identity().NodeIncarnation != uint64(restart+1) {
				t.Fatal("incarnation was not durably advanced exactly once")
			}
			runtimes = append(runtimes, runtime)
		}
		// Swapping the SQL handle must retain the shared log and incarnation.
		if err := runtimes[0].QuiesceSQLGeneration(); err != nil {
			t.Fatal(err)
		}
		item := &set.groups[0]
		db, apply, err := openRF3SelectedLog(item.manifest.SQL.Path, item.nodeLog, item.base, item.applyIdentity)
		if err != nil {
			t.Fatal(err)
		}
		if err := runtimes[0].InstallSQLGeneration(db, apply, item.base, item.applyIdentity); err != nil {
			t.Fatal(err)
		}
		if err := runtimes[0].Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := runtimes[1].Publication(); err != nil {
			t.Fatal("closing neighboring runtime closed shared storage", err)
		}
		if err := runtimes[1].Close(); err != nil {
			t.Fatal(err)
		}
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestServeRF3NodeLogThreeServersRestart(t *testing.T) {
	template := prepareRF3NodeTestInput(t)
	nodes, logical := rf3CommandNodes(), rf3CommandGroup()
	credentialRoot := t.TempDir()
	credentials, roots, err := rf3testfixture.WriteCredentials(credentialRoot, rf3CommandIdentityOID, rafttransport.TrustDomain{ClusterID: logical.ClusterID, ClusterIncarnation: logical.ClusterIncarnation}, nodes[:])
	if err != nil {
		t.Fatal(err)
	}
	var addresses [3][4]string
	var reservations [3]map[string]net.Listener
	for i := range 3 {
		reservations[i] = make(map[string]net.Listener)
		for j := range 4 {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addresses[i][j] = listener.Addr().String()
			reservations[i][addresses[i][j]] = listener
			t.Cleanup(func() { _ = listener.Close() })
		}
	}
	var manifests [3]rf3Manifest
	var profiles []*rafttransport.PeerTLS
	var nativeAddresses [3]string
	for i := range 3 {
		input := template
		input.Root = filepath.Join(filepath.Dir(template.Root), fmt.Sprintf("server-%d", i))
		input.NodeLog.Path = filepath.Join(input.Root, "node-log")
		input.Groups = append([]prepareRF3Manifest(nil), template.Groups...)
		for j := range input.Groups {
			member := &input.Groups[j]
			member.Root = filepath.Join(input.Root, fmt.Sprintf("group-%d", j))
			member.MemberID = uint64(i + 1)
			store := rf3CommandStoreIdentity(uint64(i + 1)).StoreID
			store[15] += byte(j)
			member.StoreID = idString(store[:])
			member.Listeners = rf3ManifestListeners{Peer: addresses[i][0], Native: addresses[i][1], Snapshot: addresses[i][2], Control: addresses[i][3]}
			member.TLS = rf3ManifestTLS{Certificate: credentials[i].Certificate, Key: credentials[i].Key, Roots: roots, IdentityOID: rf3CommandIdentityOID.String()}
			member.Members = append([]prepareRF3Member(nil), member.Members...)
			for k := range member.Members {
				member.Members[k].PeerAddress = addresses[k][0]
			}
		}
		if err := provisionRF3Node(input); err != nil {
			t.Fatal(err)
		}
		manifests[i], err = loadRF3Manifest(filepath.Join(input.Root, "serve-rf3.vibejson"))
		if err != nil {
			t.Fatal(err)
		}
		profile, err := servicetls.LoadProfile(credentials[i].Certificate, credentials[i].Key, roots, rf3CommandIdentityOID.String(), time.Now)
		if err != nil {
			t.Fatal(err)
		}
		profiles = append(profiles, profile)
		nativeAddresses[i] = addresses[i][1]
	}
	for restart := range 2 {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 3)
		for i := range 3 {
			if restart != 0 {
				reservations[i] = make(map[string]net.Listener)
				for _, address := range addresses[i] {
					listener, err := net.Listen("tcp", address)
					if err != nil {
						t.Fatal(err)
					}
					reservations[i][address] = listener
					t.Cleanup(func() { _ = listener.Close() })
				}
			}
			listeners := reservations[i]
			go func() {
				done <- servePreparedRF3WithListen(ctx, manifests[i], func(network, address string) (net.Listener, error) {
					listener := listeners[address]
					if network != "tcp" || listener == nil {
						return nil, errors.New("unexpected node listener")
					}
					delete(listeners, address)
					return listener, nil
				})
			}()
		}
		// Always drain the servers, including when leader qualification fails.
		stopped := false
		stop := func() {
			if stopped {
				return
			}
			stopped = true
			cancel()
			for range 3 {
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("node serving shutdown: %v", err)
					}
				case <-time.After(30 * time.Second):
					t.Error("node serving did not drain")
				}
			}
		}
		t.Cleanup(stop)
		for _, bundle := range manifests[0].Groups {
			waitRF3CommandLeader(t, nativeAddresses, nodes, profiles, bundle.Route.Group, bundle.Route.AllocationGeneration, template.Groups[0].Authority.ActivePolicyGeneration)
		}
		stop()
	}
}

func TestPrepareRF3NodeCrossesInitialWaveBound(t *testing.T) {
	input := prepareRF3NodeTestInput(t)
	first := input.Groups[0]
	input.Groups = nil
	input.NodeLog.Options.MaxGroups = 64
	for i := range raftstore.MaxPersistGroupBatches + 1 {
		member := first
		member.Root = filepath.Join(input.Root, fmt.Sprintf("group-%d", i))
		member.Shard = fmt.Sprint(i)
		id := rf3CommandStoreIdentity(1)
		id.GroupID[15] += byte(i)
		id.StoreID[15] += byte(i)
		member.GroupID, member.StoreID = idString(id.GroupID[:]), idString(id.StoreID[:])
		input.Groups = append(input.Groups, member)
	}
	if err := provisionRF3Node(input); err != nil {
		t.Fatal(err)
	}
	manifest, err := loadRF3Manifest(filepath.Join(input.Root, "serve-rf3.vibejson"))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := servicetls.LoadProfile(first.TLS.Certificate, first.TLS.Key, first.TLS.Roots, first.TLS.IdentityOID, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := openRF3NodeOwner(manifest, profile)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	set, err := prepareRF3GroupSetOnNode(manifest, profile, sqldriver.ReplicatedOpenOptions{}, owner)
	if err != nil {
		t.Fatal(err)
	}
	if len(set.groups) != len(input.Groups) {
		t.Fatal("registration lost a group")
	}
	if err := closePreparedRF3Groups(set.groups, nil); err != nil {
		t.Fatal(err)
	}
	// Existing node preparation never rewrites a retained root.
	if err := provisionRF3Node(input); !errors.Is(err, errPrepareRF3) {
		t.Fatalf("existing root accepted: %v", err)
	}
}
