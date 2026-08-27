package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/storeio"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

func TestDevClusterManifestResumeIsCanonicalAndDoesNotReprovision(t *testing.T) {
	root := t.TempDir()
	manifest := devClusterManifest{
		Format: devClusterFormat, Nodes: devClusterRF3,
		ClientEndpoint: "127.0.0.1:24000", CatalogPath: filepath.Join(root, "catalog.vibejson"),
		GatewayCertificate: filepath.Join(root, "gateway-cert.pem"), GatewayKey: filepath.Join(root, "gateway-key.pem"),
		Roots: filepath.Join(root, "roots.pem"), AuthorizationPolicy: filepath.Join(root, "policy.vibejson"),
		HotShardCapacity: filepath.Join(root, "hot-shard-capacity.vibejson"),
		ReplicaControl:   filepath.Join(root, "replica-control.vibejson"),
		DurableAckKey:    filepath.Join(root, "durable-ack-key"),
		GatewayNode:      "01010101010101010101010101010101",
		GatewayControl:   "127.0.0.1:24001",
		Members:          make([]devClusterMember, devClusterRF3), LedgerMembers: make([]devClusterMember, devClusterRF3), DataMembers: make([]devClusterMember, devClusterRF3),
	}
	paths := []string{manifest.GatewayCertificate, manifest.GatewayKey, manifest.Roots, manifest.AuthorizationPolicy, manifest.HotShardCapacity, manifest.ReplicaControl, manifest.DurableAckKey}
	for index := range manifest.Members {
		member := func(role string, nodeBase, storeBase byte, portBase, snapshotBase int) devClusterMember {
			node := [16]byte{nodeBase + byte(index)}
			store := [16]byte{storeBase + byte(index)}
			return devClusterMember{
				Member: uint64(index + 1), Node: idStringForDev(node[:]),
				Store:         idStringForDev(store[:]),
				Peer:          "127.0.0.1:" + strconv.Itoa(portBase+index*3),
				Native:        "127.0.0.1:" + strconv.Itoa(portBase+index*3+1),
				Snapshot:      "127.0.0.1:" + strconv.Itoa(snapshotBase+index),
				Control:       "127.0.0.1:" + strconv.Itoa(portBase+index*3+2),
				ServeManifest: filepath.Join(root, role+"-member-"+strconv.Itoa(index+1), "serve-rf3.vibejson"),
			}
		}
		manifest.Members[index] = member("catalog", 0x11, 0x41, 28000, 29000)
		manifest.LedgerMembers[index] = member("ledger", 0x21, 0x51, 28009, 29100)
		manifest.DataMembers[index] = member("data", 0x31, 0x61, 28018, 29200)
		paths = append(paths, manifest.Members[index].ServeManifest, manifest.LedgerMembers[index].ServeManifest, manifest.DataMembers[index].ServeManifest)
	}
	for _, path := range paths {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("retained"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	clusterID, clusterIncarnation := [16]byte{1}, [16]byte{2}
	groups := [3]raftmember.GroupKey{
		{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}},
		{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{5}, GroupID: [16]byte{6}},
		{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{7}, GroupID: [16]byte{8}},
	}
	for _, role := range []struct {
		members               []devClusterMember
		group                 raftmember.GroupKey
		distributionName      distribution.DistributionName
		shard                 distribution.ShardID
		table, primaryKey     string
		requestLedgerIdentity replication.Digest
	}{
		{manifest.Members, groups[0], gateway.ReplicatedCatalogDistribution,
			gateway.ReplicatedCatalogShard, gateway.ReplicatedCatalogTable,
			gateway.ReplicatedCatalogPrimaryKey, replication.Digest{}},
		{manifest.LedgerMembers, groups[1], devLedgerDistribution, devLedgerShard,
			devLedgerTable, devLedgerPrimaryKey, deriveDevLedgerHomeIdentity(groups[1])},
		{manifest.DataMembers, groups[2], devDataDistribution, devDataShard,
			devDataTable, devDataPrimaryKey, replication.Digest{}},
	} {
		for _, member := range role.members {
			prepareDevTestReplica(
				t, member, role.group, role.distributionName, role.shard,
				role.table, role.primaryKey, role.requestLedgerIdentity,
			)
		}
	}
	if err := writeDevCatalog(
		manifest, clusterID, clusterIncarnation,
		groups[0].ShardIncarnation, groups[0].GroupID,
		groups[1].ShardIncarnation, groups[1].GroupID,
		groups[2].ShardIncarnation, groups[2].GroupID,
	); err != nil {
		t.Fatal(err)
	}
	for index, role := range []string{"catalog", "ledger", "data"} {
		prepare := devPrepareManifest{
			ClusterID:          idStringForDev(groups[index].ClusterID[:]),
			ClusterIncarnation: idStringForDev(groups[index].ClusterIncarnation[:]),
			ShardIncarnation:   idStringForDev(groups[index].ShardIncarnation[:]),
			GroupID:            idStringForDev(groups[index].GroupID[:]),
		}
		prepareRaw, marshalErr := vibejson.Marshal(&prepare)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if writeErr := os.WriteFile(
			filepath.Join(root, "prepare-"+role+"-member-1.vibejson"),
			prepareRaw, 0o600,
		); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	raw, err := vibejson.Marshal(&manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "cluster.vibejson"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := ensureDevCluster(devClusterOptions{root: root, replicas: 3, shardBinary: "/does/not/run"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := vibejson.Marshal(&loaded)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatalf("resume changed manifest: %v", err)
	}
	stale, _, _ := testDevCatalogSnapshot(t)
	stalePath := filepath.Join(root, "stale-catalog.vibejson")
	if err = gateway.SaveSnapshot(stalePath, stale); err == nil {
		err = os.Rename(stalePath, manifest.CatalogPath)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ensureDevCluster(devClusterOptions{
		root: root, replicas: 3, shardBinary: "/does/not/run",
	}); !errors.Is(err, errDevCluster) {
		t.Fatalf("restart accepted catalog with stale portable witness: %v", err)
	}
}

func TestDevClusterManifestAcceptsOnlyExplicitRF1OrRF3(t *testing.T) {
	root := t.TempDir()
	base := devClusterManifest{
		Format: devClusterFormat, Nodes: devClusterRF1, ClientEndpoint: "127.0.0.1:24000",
		CatalogPath: filepath.Join(root, "catalog.vibejson"), GatewayCertificate: filepath.Join(root, "gateway-cert.pem"),
		GatewayKey: filepath.Join(root, "gateway-key.pem"), Roots: filepath.Join(root, "roots.pem"),
		AuthorizationPolicy: filepath.Join(root, "policy.vibejson"), GatewayNode: "01010101010101010101010101010101",
		HotShardCapacity: filepath.Join(root, "hot-shard-capacity.vibejson"),
		ReplicaControl:   filepath.Join(root, "replica-control.vibejson"),
		DurableAckKey:    filepath.Join(root, "durable-ack-key"),
		GatewayControl:   "127.0.0.1:24001",
		Members:          []devClusterMember{{Member: 1, Node: "11111111111111111111111111111111", Store: "22222222222222222222222222222222", Peer: "127.0.0.1:25001", Native: "127.0.0.1:25101", Snapshot: "127.0.0.1:25201", Control: "127.0.0.1:25301", ServeManifest: filepath.Join(root, "member-1", "serve-rf3.vibejson")}},
		LedgerMembers:    []devClusterMember{{Member: 1, Node: "12121212121212121212121212121212", Store: "33333333333333333333333333333333", Peer: "127.0.0.1:26001", Native: "127.0.0.1:26101", Snapshot: "127.0.0.1:26201", Control: "127.0.0.1:26301", ServeManifest: filepath.Join(root, "ledger-member-1", "serve-rf3.vibejson")}},
		DataMembers:      []devClusterMember{{Member: 1, Node: "13131313131313131313131313131313", Store: "44444444444444444444444444444444", Peer: "127.0.0.1:27001", Native: "127.0.0.1:27101", Snapshot: "127.0.0.1:27201", Control: "127.0.0.1:27301", ServeManifest: filepath.Join(root, "data-member-1", "serve-rf3.vibejson")}},
	}
	if !validDevManifest(base, root) {
		t.Fatal("explicit RF1 manifest rejected")
	}
	withoutLedger := base
	withoutLedger.LedgerMembers = nil
	if validDevManifest(withoutLedger, root) {
		t.Fatal("manifest without a dedicated request-ledger group accepted")
	}
	withoutData := base
	withoutData.DataMembers = nil
	if validDevManifest(withoutData, root) {
		t.Fatal("manifest without a dedicated user-data group accepted")
	}
	reusedStore := base
	reusedStore.LedgerMembers = append([]devClusterMember(nil), base.LedgerMembers...)
	reusedStore.LedgerMembers[0].Store = reusedStore.Members[0].Store
	if validDevManifest(reusedStore, root) {
		t.Fatal("request-ledger group reused the catalog store")
	}
	reusedDataListener := base
	reusedDataListener.DataMembers = append([]devClusterMember(nil), base.DataMembers...)
	reusedDataListener.DataMembers[0].Native = reusedDataListener.LedgerMembers[0].Native
	if validDevManifest(reusedDataListener, root) {
		t.Fatal("data group reused the request-ledger listener")
	}
	reusedDataPath := base
	reusedDataPath.DataMembers = append([]devClusterMember(nil), base.DataMembers...)
	reusedDataPath.DataMembers[0].ServeManifest = reusedDataPath.LedgerMembers[0].ServeManifest
	if validDevManifest(reusedDataPath, root) {
		t.Fatal("data group reused the request-ledger serving manifest")
	}
	reusedDataNode := base
	reusedDataNode.DataMembers = append([]devClusterMember(nil), base.DataMembers...)
	reusedDataNode.DataMembers[0].Node = base.Members[0].Node
	if validDevManifest(reusedDataNode, root) {
		t.Fatal("independent serving roles reused one physical node identity")
	}
	base.Nodes = 2
	if validDevManifest(base, root) {
		t.Fatal("RF2 manifest accepted")
	}
}

func TestInitializeDevClusterEmitsThreeIndependentApplyRoles(t *testing.T) {
	root := t.TempDir()
	manifest, err := initializeDevCluster(
		devClusterOptions{root: root, replicas: devClusterRF1, shardBinary: "/usr/bin/true"},
		filepath.Join(root, "cluster.vibejson"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Members) != 1 || len(manifest.LedgerMembers) != 1 ||
		len(manifest.DataMembers) != 1 ||
		manifest.Members[0].Node == manifest.LedgerMembers[0].Node ||
		manifest.Members[0].Node == manifest.DataMembers[0].Node ||
		manifest.LedgerMembers[0].Node == manifest.DataMembers[0].Node ||
		manifest.Members[0].Store == manifest.LedgerMembers[0].Store ||
		manifest.Members[0].Store == manifest.DataMembers[0].Store ||
		manifest.LedgerMembers[0].Store == manifest.DataMembers[0].Store {
		t.Fatalf("independent role identities=%+v", manifest)
	}
	prepared := make(map[string]devPrepareManifest, 3)
	for roleIndex, role := range []string{"catalog", "ledger", "data"} {
		raw, readErr := os.ReadFile(filepath.Join(root, "prepare-"+role+"-member-1.vibejson"))
		if readErr != nil {
			t.Fatal(readErr)
		}
		var decoded devPrepareManifest
		if decodeErr := vibejson.Unmarshal(raw, &decoded); decodeErr != nil {
			t.Fatal(decodeErr)
		}
		prepared[role] = decoded
		roleMembers := [][]devClusterMember{manifest.Members, manifest.LedgerMembers, manifest.DataMembers}
		if len(decoded.SplitControl.Grants) != 1 ||
			decoded.SplitControl.Grants[0].NodeID != roleMembers[roleIndex][0].Node {
			t.Fatalf("%s split profile=%+v", role, decoded.SplitControl)
		}
	}
	if prepared["catalog"].Distribution != string(gateway.ReplicatedCatalogDistribution) ||
		prepared["ledger"].Distribution != string(devLedgerDistribution) ||
		prepared["data"].Distribution != string(devDataDistribution) ||
		prepared["data"].Table != devDataTable ||
		prepared["data"].Apply.ShardKey != devDataPrimaryKey {
		t.Fatalf("prepared roles=%+v", prepared)
	}
	for _, role := range []string{"catalog", "data"} {
		if prepared[role].Apply.RequestLedgerCapacityBytes != 0 ||
			prepared[role].Apply.RequestLedgerRangeIdentity != "" {
			t.Fatalf("%s acquired ledger authority: %+v", role, prepared[role].Apply)
		}
	}
	if prepared["ledger"].Apply.RequestLedgerCapacityBytes != devLedgerCapacityBytes ||
		prepared["ledger"].Apply.RequestLedgerCleanupReserveBytes != devLedgerCleanupReserveBytes ||
		prepared["ledger"].Apply.RequestLedgerRangeIdentity == "" {
		t.Fatalf("ledger apply profile=%+v", prepared["ledger"].Apply)
	}
	controlRaw, err := os.ReadFile(manifest.ReplicaControl)
	if err != nil {
		t.Fatal(err)
	}
	var control devReplicaControlManifest
	if err = vibejson.Unmarshal(controlRaw, &control); err != nil {
		t.Fatal(err)
	}
	canonical, err := vibejson.Marshal(&control)
	if err != nil || !bytes.Equal(canonical, controlRaw) || control.Generation != 1 ||
		control.LocalGateway.Node != manifest.GatewayNode ||
		control.LocalGateway.ControlAddress != manifest.GatewayControl ||
		len(control.GatewayEndpoints) != 1 || len(control.ShardEndpoints) != 3 ||
		len(control.Candidates) != 0 {
		t.Fatalf("replica control=%+v canonical=%t err=%v", control,
			bytes.Equal(canonical, controlRaw), err)
	}
	for index := range control.ShardEndpoints {
		shard := control.ShardEndpoints[index]
		if index != 0 && control.ShardEndpoints[index-1].Node >= shard.Node {
			t.Fatalf("replica control shard[%d]=%+v", index, shard)
		}
	}
}

func TestInitializeDevRF3BindsHotSplitSourceToDataGroup(t *testing.T) {
	root := t.TempDir()
	manifest, err := initializeDevCluster(devClusterOptions{root: root, replicas: devClusterRF3, shardBinary: "/usr/bin/true"}, filepath.Join(root, "cluster.vibejson"))
	// The no-op child exercises generation only; completion must then fail
	// because it did not materialize the RF3 stores.
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected unmaterialized-store refusal, got %v", err)
	}
	raw, err := os.ReadFile(manifest.ReplicaControl)
	if err != nil {
		t.Fatal(err)
	}
	var control devReplicaControlManifest
	if err = vibejson.Unmarshal(raw, &control); err != nil {
		t.Fatal(err)
	}
	if len(control.SplitSources) != 1 || len(control.ShardEndpoints) != 9 {
		t.Fatalf("control=%+v", control)
	}
	source := control.SplitSources[0]
	if source.Table != devDataTable || source.SchemaGeneration != 1 || source.RelationManifestDigest == ([32]byte{}) || len(source.Replicas) != 3 {
		t.Fatalf("source=%+v", source)
	}
	for i, replica := range source.Replicas {
		if i > 0 && source.Replicas[i-1].Node >= replica.Node {
			t.Fatal("noncanonical replica order")
		}
		found := false
		for _, member := range manifest.DataMembers {
			if member.Node == replica.Node {
				found = true
				if replica.ChildRoot != filepath.Join(filepath.Dir(member.ServeManifest), "split-children") {
					t.Fatal("wrong per-role root")
				}
			}
		}
		if !found {
			t.Fatal("catalog or ledger node acquired split authority")
		}
	}
	// Re-reading immutable preparation authority, as on restart, is byte exact.
	rebuilt, err := devReplicaSplitSourceForCluster(manifest)
	if err != nil {
		t.Fatal(err)
	}
	first, err := vibejson.Marshal(&source)
	if err != nil {
		t.Fatal(err)
	}
	second, err := vibejson.Marshal(&rebuilt)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatal("source authority changed on restart", err)
	}
}

func TestClusterDevReplicaFlagsRejectAmbiguity(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	for _, args := range [][]string{
		{"--root", root, "--replicas", "2"},
		{"--root", root, "--nodes", "0"},
		{"--root", root, "--replicas", "1", "--nodes", "3"},
	} {
		if code := runClusterDev(args); code != 2 {
			t.Fatalf("runClusterDev(%v) = %d, want usage error", args, code)
		}
	}
}

func TestDevPolicyIsAcceptedByProductionAuthorizationLoader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.vibejson")
	var nodes [4]rafttransport.NodeID
	for index := range nodes {
		nodes[index][0] = byte(index + 1)
	}
	if err := writeDevPolicy(path, nodes[:]); err != nil {
		t.Fatal(err)
	}
	policy, err := serviceauthz.LoadFile(path)
	if err != nil || policy.Generation() != 1 || len(policy.Nodes()) != len(nodes) {
		t.Fatalf("policy=%+v err=%v", policy, err)
	}
}

func TestDevHotShardCapacityIsExplicitAndCanonical(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hot-shard-capacity.vibejson")
	members := []devClusterMember{{Member: 1}, {Member: 2}, {Member: 3}}
	if err := writeDevHotShardCapacity(path, members, members, members); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	config, err := hotshard.OpenStaticCapacityConfig(raw)
	if err != nil || len(config.Nodes) != 9 || config.Nodes[0].Endpoint != "catalog-member-1" ||
		config.Nodes[3].Endpoint != "data-member-1" ||
		config.Nodes[6].Endpoint != "ledger-member-1" ||
		config.WindowCapacity[autosplit.ResourceRequests] != 64 ||
		config.WindowCapacity[autosplit.ResourceWriteCPU] != 64 ||
		config.Nodes[0].FailureDomain != 1 || config.Nodes[3].FailureDomain != 1 ||
		config.Nodes[6].FailureDomain != 1 {
		t.Fatalf("config=%+v err=%v", config, err)
	}
}

func TestDevChildShutdownDoesNotLeakProcess(t *testing.T) {
	child, err := startDevChild("/bin/sh", []string{"-c", "echo READY >&2; trap 'exit 0' TERM; while :; do sleep 1; done"}, "READY")
	if err != nil {
		t.Fatal(err)
	}
	if err := waitDevReady(t.Context(), child); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	stopDevChildren([]*devChild{child})
	if child.command.ProcessState == nil || time.Since(started) > 5*time.Second {
		t.Fatalf("child not reaped promptly: %+v", child.command.ProcessState)
	}
}

func TestReserveDevPortsUsesDistinctLoopbackEndpoints(t *testing.T) {
	for _, count := range []int{1 + devClusterRF1*12, 1 + devClusterRF3*12} {
		ports, err := reserveDevPorts(count)
		if err != nil || len(ports) != count {
			t.Fatalf("reserved %d ports: len=%d err=%v", count, len(ports), err)
		}
		seen := make(map[string]struct{}, len(ports))
		for _, address := range ports {
			if _, exists := seen[address]; exists {
				t.Fatalf("duplicate %q", address)
			}
			seen[address] = struct{}{}
		}
	}
}

func TestDevEndpointAndIdentityValidationIsCanonical(t *testing.T) {
	for _, address := range []string{"localhost:1234", "[::1]:1234", "127.0.0.1:01234", "127.0.0.1:0", "127.0.0.1:http"} {
		if validDevLoopbackAddress(address) {
			t.Fatalf("noncanonical endpoint %q accepted", address)
		}
	}
	if !validDevLoopbackAddress("127.0.0.1:1234") {
		t.Fatal("canonical loopback endpoint rejected")
	}
	if _, err := decodeDev16("00000000000000000000000000000000"); err == nil {
		t.Fatal("zero identity accepted")
	}
}

func TestDevLogicalAuthorityDerivationIsDeterministicAndDomainSeparated(t *testing.T) {
	group := raftmember.GroupKey{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5},
	}
	relation := [32]byte{6}
	rangeIdentity, lineage, forwarding := deriveDevLogicalRangeAuthority(
		group, distribution.DistributionName("request-ledger"), distribution.ShardID("all"), relation,
	)
	againRange, againLineage, againForwarding := deriveDevLogicalRangeAuthority(
		group, distribution.DistributionName("request-ledger"), distribution.ShardID("all"), relation,
	)
	if rangeIdentity == (replication.Digest{}) || lineage == (replication.Digest{}) ||
		forwarding == (replication.Digest{}) || rangeIdentity != againRange ||
		lineage != againLineage || forwarding != againForwarding ||
		rangeIdentity == lineage || rangeIdentity == forwarding || lineage == forwarding {
		t.Fatalf("derived authorities are zero, unstable, or aliased: %x %x %x", rangeIdentity, lineage, forwarding)
	}
	home := deriveDevLedgerHomeIdentity(group)
	if home == (replication.Digest{}) || home == rangeIdentity ||
		home != deriveDevLedgerHomeIdentity(group) {
		t.Fatalf("ledger home identity = %x", home)
	}
	changed := group
	changed.GroupID[0]++
	changedRange, _, _ := deriveDevLogicalRangeAuthority(
		changed, distribution.DistributionName("request-ledger"), distribution.ShardID("all"), relation,
	)
	if changedRange == rangeIdentity {
		t.Fatal("authenticated group change did not change range authority")
	}
	if deriveDevLedgerHomeIdentity(changed) == home {
		t.Fatal("authenticated group change did not change ledger home identity")
	}
	relation[0]++
	changedRange, _, _ = deriveDevLogicalRangeAuthority(
		group, distribution.DistributionName("request-ledger"), distribution.ShardID("all"), relation,
	)
	if changedRange == rangeIdentity {
		t.Fatal("relation-manifest change did not change range authority")
	}
}

func TestDevRequestLedgerPrepareProfileMatchesCatalogHomeAndKeepsCatalogDisabled(t *testing.T) {
	group := raftmember.GroupKey{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5},
	}
	home := deriveDevLedgerHomeIdentity(group)
	ledger := devPrepareApplyProfile("/id", home)
	const zeroDigest = "0000000000000000000000000000000000000000000000000000000000000000"
	if ledger.RequestLedgerCapacityBytes != devLedgerCapacityBytes ||
		ledger.RequestLedgerCleanupReserveBytes != devLedgerCleanupReserveBytes ||
		ledger.RequestLedgerRangeStart != zeroDigest ||
		ledger.RequestLedgerRangeEnd != zeroDigest ||
		ledger.RequestLedgerRangeIdentity != idStringForDev(home[:]) {
		t.Fatalf("ledger apply profile=%+v home=%x", ledger, home)
	}
	catalog := devPrepareApplyProfile("/id", replication.Digest{})
	if catalog.RequestLedgerCapacityBytes != 0 ||
		catalog.RequestLedgerCleanupReserveBytes != 0 ||
		catalog.RequestLedgerRangeStart != "" || catalog.RequestLedgerRangeEnd != "" ||
		catalog.RequestLedgerRangeIdentity != "" {
		t.Fatalf("catalog unexpectedly enabled request ledger: %+v", catalog)
	}
	data := devPrepareApplyProfile(devDataPrimaryKey, replication.Digest{})
	if data.RequestLedgerCapacityBytes != 0 ||
		data.RequestLedgerCleanupReserveBytes != 0 ||
		data.RequestLedgerRangeStart != "" || data.RequestLedgerRangeEnd != "" ||
		data.RequestLedgerRangeIdentity != "" {
		t.Fatalf("data unexpectedly enabled request ledger: %+v", data)
	}
	members := []devPrepareMember{
		{NodeID: "01010101010101010101010101010101"},
		{NodeID: "02020202020202020202020202020202"},
		{NodeID: "03030303030303030303030303030303"},
	}
	split := devPrepareSplitControlProfile(members)
	if split.MaxRecords != 4096 || split.MaxFileBytes != 64<<20 ||
		split.MaxChildOperations != 8 || split.StageCheckpointBytes != 32<<20 ||
		len(split.Grants) != len(members) {
		t.Fatalf("split control profile=%+v", split)
	}
	for index, grant := range split.Grants {
		if grant.NodeID != members[index].NodeID || grant.Actions != ^uint16(0) {
			t.Fatalf("split grant %d=%+v", index, grant)
		}
	}
}

func minimumDevTestWALOptions() raftstore.Options {
	return raftstore.Options{
		MaxFileBytes: int64(raftstore.HeaderBytes+raftstore.MaxSnapshotBaseRecordBytes+
			raftstore.MinimumReadyRecordBytes) + raftstore.MinimumReadyLiveBytes,
		MaxRecordBytes: raftstore.MinimumReadyRecordBytes,
		MaxRecords:     2,
		MaxEntries:     raftstore.MaxReadyEntries,
		MaxLiveBytes:   raftstore.MinimumReadyLiveBytes,
	}
}

func TestDevRestartFixtureUsesMinimumProductionWALGeometry(t *testing.T) {
	wal := minimumDevTestWALOptions()
	wantFileBytes := int64(raftstore.HeaderBytes+raftstore.MaxSnapshotBaseRecordBytes+
		raftstore.MinimumReadyRecordBytes) + raftstore.MinimumReadyLiveBytes
	if wal.MaxFileBytes != wantFileBytes || wal.MaxFileBytes >= raftstore.DefaultMaxFileBytes ||
		wal.MaxRecordBytes != raftstore.MinimumReadyRecordBytes || wal.MaxRecords != 2 ||
		wal.MaxEntries != raftstore.MaxReadyEntries ||
		wal.MaxLiveBytes != raftstore.MinimumReadyLiveBytes {
		t.Fatalf("minimum production WAL profile=%+v want file bytes=%d", wal, wantFileBytes)
	}
}

func prepareDevTestReplica(
	t *testing.T,
	member devClusterMember,
	group raftmember.GroupKey,
	distributionName distribution.DistributionName,
	shard distribution.ShardID,
	table, primaryKey string,
	requestLedgerIdentity replication.Digest,
) {
	t.Helper()
	store, err := decodeDev16(member.Store)
	if err != nil {
		t.Fatal(err)
	}
	apply := sqldriver.ReplicatedApplyOptions{
		MaxSessions: 128, RetryWindow: 8,
		TxnLimits: durable.TxnLimits{
			MaxCollections: 16, MaxDocuments: 4096, MaxBytes: 384 << 20,
		},
		Placement: sqldriver.ReplicatedPlacementProfile{
			Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: primaryKey,
			TupleVersion:  distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			Range:         distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
	}
	if requestLedgerIdentity != (replication.Digest{}) {
		apply.RequestLedgerCapacityBytes = devLedgerCapacityBytes
		apply.RequestLedgerCleanupReserveBytes = devLedgerCleanupReserveBytes
		apply.RequestLedgerRangeIdentity = [32]byte(requestLedgerIdentity)
	}
	identity := raftstore.Identity{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		Distribution: string(distributionName), Shard: string(shard),
		AllocationGeneration: 1, ShardIncarnation: group.ShardIncarnation,
		GroupID: group.GroupID, MemberID: member.Member, StoreID: store,
	}
	key := raftstore.Key{ID: "dev-restart-profile-key", Wrapped: []byte("opaque")}
	for index := range key.Material {
		key.Material[index] = group.GroupID[0] + byte(index) + 1
	}
	bootstrap := rf3testfixture.InitialBootstrap([]uint64{1, 2, 3})
	bootstrap.TopologyRecoveryEpoch = group.TopologyRecoveryEpoch
	prepared, err := rf3testfixture.PrepareMember(rf3testfixture.MemberOptions{
		Root: filepath.Dir(member.ServeManifest), Table: table,
		CreateTable: "CREATE TABLE " + table + " (PRIMARY KEY (id))",
		Identity:    identity, Key: key,
		WAL:       minimumDevTestWALOptions(),
		Bootstrap: bootstrap,
		Authority: sqldriver.ReplicatedAuthorityProfile{
			ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1,
			SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1,
		},
		Apply: apply,
	})
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) ||
		errors.Is(err, raftstore.ErrPlatformUnsupported) {
		t.Skipf("RF3 strict durable allocation unsupported: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	baseRaw, err := vibejson.Marshal(&prepared.Base)
	if err != nil {
		_ = prepared.Close()
		t.Fatal(err)
	}
	applyRaw, err := vibejson.Marshal(&prepared.ApplyIdentity)
	if err != nil {
		_ = prepared.Close()
		t.Fatal(err)
	}
	root := filepath.Dir(member.ServeManifest)
	if err = os.WriteFile(filepath.Join(root, "sql-identity.vibejson"), baseRaw, 0o600); err == nil {
		err = os.WriteFile(filepath.Join(root, "apply-identity.vibejson"), applyRaw, 0o600)
	}
	if err = errors.Join(err, prepared.Close()); err != nil {
		t.Fatal(err)
	}
}

func TestDevReplicatedTableProfileUsesPortableSchemaAcrossReplicaLocalStores(t *testing.T) {
	group := raftmember.GroupKey{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4},
	}
	authority := sqldriver.ReplicatedAuthorityProfile{
		ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1,
		SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1,
	}
	apply := sqldriver.ReplicatedApplyOptions{
		MaxSessions: 128, RetryWindow: 8,
		TxnLimits: durable.TxnLimits{
			MaxCollections: 16, MaxDocuments: 4096, MaxBytes: 384 << 20,
		},
		Placement: sqldriver.ReplicatedPlacementProfile{
			Format:   sqldriver.ReplicatedPlacementProfileFormat,
			ShardKey: devDataPrimaryKey, TupleVersion: distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			Range:         distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
	}
	wal := minimumDevTestWALOptions()
	bootstrap := rf3testfixture.InitialBootstrap([]uint64{1, 2, 3})
	bootstrap.TopologyRecoveryEpoch = group.TopologyRecoveryEpoch
	var portable [3]sqldriver.ReplicatedSchemaCatalogImage
	var local [3][32]byte
	var machine [3][32]byte
	var profiles [3]gateway.ReplicatedTableProfile
	for index := range profiles {
		root := filepath.Join(t.TempDir(), "member")
		if err := os.MkdirAll(root, 0o700); err != nil {
			t.Fatal(err)
		}
		store := [16]byte{byte(index + 11)}
		identity := raftstore.Identity{
			ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
			Distribution: string(devDataDistribution), Shard: string(devDataShard),
			AllocationGeneration: 1, ShardIncarnation: group.ShardIncarnation,
			GroupID: group.GroupID, MemberID: uint64(index + 1), StoreID: store,
		}
		key := raftstore.Key{ID: "dev-portable-profile-key", Wrapped: []byte("opaque")}
		for ordinal := range key.Material {
			key.Material[ordinal] = byte(ordinal + 1)
		}
		prepared, err := rf3testfixture.PrepareMember(rf3testfixture.MemberOptions{
			Root: root, Table: devDataTable,
			CreateTable: "CREATE TABLE documents (PRIMARY KEY (id))",
			Identity:    identity, Key: key, WAL: wal,
			Bootstrap: bootstrap,
			Authority: authority, Apply: apply,
		})
		if errors.Is(err, storeio.ErrStrictAllocationUnsupported) ||
			errors.Is(err, raftstore.ErrPlatformUnsupported) {
			t.Skipf("RF3 strict durable allocation unsupported: %v", err)
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = prepared.Close() })
		baseRaw, err := vibejson.Marshal(&prepared.Base)
		if err != nil {
			t.Fatal(err)
		}
		applyRaw, err := vibejson.Marshal(&prepared.ApplyIdentity)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, "sql-identity.vibejson"), baseRaw, 0o600); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, "apply-identity.vibejson"), applyRaw, 0o600); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(prepared.SQLPath)
		if err != nil {
			t.Fatal(err)
		}
		portable[index], err = sqldriver.ValidateReplicatedSchemaCatalogImage(raw)
		if err != nil {
			t.Fatal(err)
		}
		local[index] = prepared.Base.RelationManifestDigest
		machine[index], err = prepared.Apply.RangeSplitRelationManifestDigest()
		if err != nil {
			t.Fatal(err)
		}
		if err = prepared.Close(); err != nil {
			t.Fatal(err)
		}
		profiles[index], err = readDevReplicatedTableProfile(
			devClusterMember{
				Member: uint64(index + 1), Store: idStringForDev(store[:]),
				ServeManifest: filepath.Join(root, "serve-rf3.vibejson"),
			},
			devDataDistribution, devDataShard, devDataTable, devDataPrimaryKey,
			group, replication.Digest{}, portable[index],
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	for index := 1; index < len(profiles); index++ {
		if local[index] == local[0] {
			t.Fatalf("replica-local relation digests unexpectedly match: %x", local[0])
		}
		if portable[index].RelationManifestDigest != portable[0].RelationManifestDigest ||
			portable[index].ApplyProfileDigest != portable[0].ApplyProfileDigest ||
			profiles[index] != profiles[0] || machine[index] != machine[0] {
			t.Fatalf("portable profile diverged: %+v != %+v", profiles[index], profiles[0])
		}
	}
	if profiles[0].Table != devDataTable || profiles[0].PrimaryKey != devDataPrimaryKey ||
		profiles[0].Relation != 1 || profiles[0].SchemaGeneration != 1 ||
		profiles[0].RelationManifestDigest != replication.Digest(machine[0]) ||
		profiles[0].RelationManifestDigest == replication.Digest(portable[0].RelationManifestDigest) {
		t.Fatalf("portable data profile=%+v image=%+v", profiles[0], portable[0])
	}
}

func testDevCatalogSnapshot(
	t *testing.T,
) (*gateway.Snapshot, devPreparedRoute, [3]raftmember.GroupKey) {
	t.Helper()
	clusterID, clusterIncarnation := [16]byte{1}, [16]byte{2}
	groups := [3]raftmember.GroupKey{
		{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}},
		{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{5}, GroupID: [16]byte{6}},
		{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{7}, GroupID: [16]byte{8}},
	}
	endpoints := make(map[distribution.EndpointID]string, 27)
	nextPort := 28000
	newRoute := func(role string, digest byte, storeBase byte) devPreparedRoute {
		result := devPreparedRoute{
			leaders:  make([]distribution.EndpointID, gateway.ServingReplicaCount),
			replicas: make([]gateway.ReplicatedReplicaDescriptor, gateway.ServingReplicaCount),
			digest:   [32]byte{digest}, applyDigest: [32]byte{digest + 20},
			schemaGeneration: 1,
		}
		for index := 0; index < gateway.ServingReplicaCount; index++ {
			peer := distribution.EndpointID(role + "-member-" + string(rune('1'+index)))
			native := distribution.EndpointID(string(peer) + "-native")
			control := distribution.EndpointID(string(peer) + "-control")
			result.leaders[index] = peer
			for _, endpoint := range []distribution.EndpointID{peer, native, control} {
				endpoints[endpoint] = "127.0.0.1:" + strconv.Itoa(nextPort)
				nextPort++
			}
			result.replicas[index] = gateway.ReplicatedReplicaDescriptor{
				Member: uint64(index + 1), Node: rafttransport.NodeID{byte(index + 1)},
				StoreID: [16]byte{storeBase + byte(index)}, NodeIncarnation: 1,
				Endpoint: peer, NativeEndpoint: native, ControlEndpoint: control,
			}
		}
		return result
	}
	catalogRoute := newRoute("catalog", 0x31, 0x41)
	ledgerRoute := newRoute("ledger", 0x32, 0x51)
	dataRoute := newRoute("data", 0x33, 0x61)
	dataRoute.table = gateway.ReplicatedTableProfile{
		Table: devDataTable, Relation: 1, PrimaryKey: devDataPrimaryKey,
		SchemaGeneration: 1, RelationManifestDigest: replication.Digest(dataRoute.digest),
		MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20,
	}
	snapshot, err := newDevCatalogSnapshot(
		endpoints, groups[0], groups[1], groups[2],
		catalogRoute, ledgerRoute, dataRoute,
	)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, dataRoute, groups
}

func TestDevCatalogPublishesOnlyThePortableDataTableProfile(t *testing.T) {
	snapshot, dataRoute, groups := testDevCatalogSnapshot(t)
	path := filepath.Join(t.TempDir(), "catalog.vibejson")
	if err := gateway.SaveSnapshot(path, snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err := gateway.LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	key, ok := orderedkey.AppendString(nil, []byte("document-1"), orderedkey.Ascending)
	if !ok {
		t.Fatal("ordered key")
	}
	var scratch [replication.MaxMutationKeyBytes + 16]byte
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	resolved, found := loaded.ResolveReplicatedTableKey(
		[]byte(devDataTable), key, scratch[:0], replicas[:0],
	)
	if !found || resolved.Profile != dataRoute.table || resolved.Route.Group != groups[2] ||
		resolved.Route.Command.RelationManifestDigest != dataRoute.digest ||
		len(resolved.Route.Replicas) != gateway.ServingReplicaCount {
		t.Fatalf("resolved data route=%+v found=%v", resolved, found)
	}
	for _, controlTable := range []string{gateway.ReplicatedCatalogTable, devLedgerTable} {
		if _, found = loaded.ResolveReplicatedTableKey(
			[]byte(controlTable), key, scratch[:0], replicas[:0],
		); found {
			t.Fatalf("control table %q published as user data", controlTable)
		}
	}
}

func idStringForDev(raw []byte) string {
	const digits = "0123456789abcdef"
	result := make([]byte, len(raw)*2)
	for index, value := range raw {
		result[index*2], result[index*2+1] = digits[value>>4], digits[value&15]
	}
	return string(result)
}

func TestClusterDevRejectsRelativeRootBeforeResolvingChildren(t *testing.T) {
	if status := runClusterDev([]string{"--root", "relative"}); status != 2 {
		t.Fatalf("status=%d", status)
	}
}

func TestResolveDevBinaryRejectsExecutableDirectory(t *testing.T) {
	if _, err := resolveDevBinary(t.TempDir(), "ignored"); err == nil {
		t.Fatal("executable directory accepted as a child binary")
	}
}

func TestDevSupervisorObservesChildExit(t *testing.T) {
	child, err := startDevChild("/bin/sh", []string{"-c", "echo READY >&2; exit 17"}, "READY")
	if err != nil {
		t.Fatal(err)
	}
	exits := make(chan devChildExit, 1)
	watchDevChildExit(exits, "shard member 1", child)
	if err := waitDevReadyOrExit(context.Background(), child, exits); err != nil {
		// An immediate exit is also a correct supervisor result.
		return
	}
	select {
	case exit := <-exits:
		if exit.name != "shard member 1" || exit.err == nil {
			t.Fatalf("exit=%+v", exit)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not observe child exit")
	}
}
