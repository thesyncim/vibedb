//go:build linux

package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
	vibejson "github.com/thesyncim/vibejson"
)

type devHotProcessManifest struct {
	ClientEndpoint      string                `json:"client_endpoint"`
	CatalogPath         string                `json:"catalog_path"`
	GatewayCertificate  string                `json:"gateway_certificate"`
	GatewayKey          string                `json:"gateway_key"`
	Roots               string                `json:"roots"`
	AuthorizationPolicy string                `json:"authorization_policy"`
	HotShardCapacity    string                `json:"hot_shard_capacity"`
	ReplicaControl      string                `json:"replica_control"`
	DurableAckKey       string                `json:"durable_ack_key"`
	GatewayNode         string                `json:"gateway_node"`
	GatewayControl      string                `json:"gateway_control"`
	Format              uint16                `json:"format"`
	Nodes               uint8                 `json:"nodes"`
	Members             []devHotProcessMember `json:"members"`
	LedgerMembers       []devHotProcessMember `json:"ledger_members"`
	DataMembers         []devHotProcessMember `json:"data_members"`
}

type devHotProcessMember struct {
	Member        uint64 `json:"member"`
	Node          string `json:"node"`
	Store         string `json:"store"`
	Peer          string `json:"peer"`
	Native        string `json:"native"`
	Snapshot      string `json:"snapshot"`
	Control       string `json:"control"`
	ServeManifest string `json:"serve_manifest"`
}

func TestGatewayZeroConfigDevPressureCompletesReplicatedSplit(t *testing.T) {
	const qualificationRuns = 3
	if raw := os.Getenv("VIBEDB_DEV_HOT_SPLIT_COUNT"); raw != "" {
		count, err := strconv.Atoi(raw)
		if err != nil || count != qualificationRuns {
			t.Fatalf("qualification count=%q want=%d", raw, qualificationRuns)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	vibedbBinary := filepath.Join(bin, "vibedb")
	shardBinary := filepath.Join(bin, "vibedb-shard")
	gatewayBinary := filepath.Join(bin, "vibedb-gateway")
	replicaProcessBuild(t, ctx, vibedbBinary, "./cmd/vibedb")
	replicaProcessBuild(t, ctx, shardBinary, "./cmd/vibedb-shard")
	replicaProcessBuild(t, ctx, gatewayBinary, "./cmd/vibedb-gateway")
	state := filepath.Join(root, "state")
	process := &rf3testfixture.ExternalProcess{Binary: vibedbBinary, Args: []string{
		"cluster", "dev", "--replicas", "3", "--root", state,
		"--shard-binary", shardBinary, "--gateway-binary", gatewayBinary,
	}}
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	defer replicaProcessStop(t, process)
	if err := process.WaitReady(ctx, "VibeDB development cluster ready:"); err != nil {
		t.Fatalf("zero-config cluster readiness: %v\n%s", err, process.Diagnostics())
	}
	raw, err := os.ReadFile(filepath.Join(state, "cluster.vibejson"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest devHotProcessManifest
	if err = vibejson.Unmarshal(raw, &manifest); err != nil || manifest.Nodes != 3 ||
		len(manifest.Members) != 3 || len(manifest.LedgerMembers) != 3 ||
		len(manifest.DataMembers) != 3 {
		t.Fatalf("dev cluster manifest=%+v err=%v", manifest, err)
	}
	profile, err := servicetls.LoadProfile(manifest.GatewayCertificate, manifest.GatewayKey,
		manifest.Roots, "1.3.6.1.4.1.32473.1.1", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	var gatewayNode rafttransport.NodeID
	if decoded, decodeErr := hex.DecodeString(manifest.GatewayNode); decodeErr != nil ||
		len(decoded) != len(gatewayNode) {
		t.Fatalf("gateway node=%q err=%v", manifest.GatewayNode, decodeErr)
	} else {
		copy(gatewayNode[:], decoded)
	}
	snapshot, err := gateway.LoadSnapshot(manifest.CatalogPath)
	if err != nil {
		t.Fatal(err)
	}
	authority, closeAuthority := hotMutationCatalogAuthority(t, profile, snapshot,
		filepath.Join(root, "catalog-observer-session"), 1)
	defer closeAuthority()
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	source, found := snapshot.ResolveReplicatedRoute("data", "all", replicas[:0])
	if !found {
		t.Fatal("zero-config data route missing")
	}

	baselineRSS := devHotProcessTreeRSS(t, process.PID())
	baselineStorage := replicaProcessAllocatedBytes(state, "")
	baselineWAL := replicaProcessAllocatedBytes(state, ".wal")
	baselineNetwork := replicaProcessSnapshotPayloadBytes(state)
	connection := hotMutationDialGateway(t, profile, gatewayNode, manifest.ClientEndpoint)
	defer connection.Close()
	client := &hotMutationWireClient{connection: connection, reader: bufio.NewReader(connection)}
	reference := client.openIssuer(t)
	latencies := make([]time.Duration, 0, 2_048)
	var operation [32]byte
	pressureDeadline := time.Now().Add(25 * time.Second)
	for sequence := uint64(1); time.Now().Before(pressureDeadline); sequence++ {
		key := fmt.Sprintf("dev-hot-%08d", sequence)
		latencies = append(latencies, client.execute(t, hotMutationRequest(t, reference, sequence,
			[]serveStatement{{SQL: `INSERT INTO documents VALUES (?)`, Params: []serveParam{{
				Kind: "document", Text: fmt.Sprintf(`{"id":%q,"value":%d}`, key, sequence),
			}}}})))
		if sequence%8 != 0 {
			continue
		}
		ids, readErr := authority.ReadOperationIDs(ctx)
		if readErr == nil && len(ids) == 1 {
			operation = ids[0]
			break
		}
		if len(ids) > 1 {
			t.Fatalf("hot pressure amplified topology operations=%d", len(ids))
		}
	}
	if operation == ([32]byte{}) {
		t.Fatalf("zero-config pressure admitted no split\n%s", process.Diagnostics())
	}
	final := hotMutationWaitSplitComplete(t, ctx, authority,
		snapshot.Generation()+1, operation, source)
	children := 0
	for _, descriptor := range final.ReplicatedShardDescriptors() {
		if descriptor.Distribution != distribution.DistributionName("data") ||
			descriptor.Shard == distribution.ShardID("all") {
			continue
		}
		route, ok := final.ResolveReplicatedRoute(descriptor.Distribution, descriptor.Shard,
			replicas[:0])
		if !ok || len(route.Replicas) != gateway.ServingReplicaCount ||
			hotMutationLeader(t, profile, route) == 0 {
			t.Fatalf("split child route=%+v ok=%t", route, ok)
		}
		children++
	}
	if children == 0 {
		t.Fatal("terminal operation published no serving data child")
	}
	sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
	p99 := latencies[(len(latencies)*99+99)/100-1]
	finalRSS := devHotProcessTreeRSS(t, process.PID())
	storageGrowth := positiveDifference(replicaProcessAllocatedBytes(state, ""), baselineStorage)
	walGrowth := positiveDifference(replicaProcessAllocatedBytes(state, ".wal"), baselineWAL)
	networkGrowth := positiveDifference(replicaProcessSnapshotPayloadBytes(state), baselineNetwork)
	if p99 > 5*time.Second || client.requests > 4_096 || client.bytes > 32<<20 ||
		positiveDifference(finalRSS, baselineRSS) > 768<<20 || storageGrowth > 2<<30 ||
		walGrowth > 1<<30 || networkGrowth > 2<<30 {
		t.Fatalf("dev hot split bounds p99=%s requests=%d wire=%d rss_growth=%d storage_growth=%d wal_growth=%d network_growth=%d",
			p99, client.requests, client.bytes, positiveDifference(finalRSS, baselineRSS),
			storageGrowth, walGrowth, networkGrowth)
	}
	t.Logf("zero-config hot split: children=%d p99=%s requests=%d wire=%d rss_growth=%d storage_growth=%d wal_growth=%d network_growth=%d",
		children, p99, client.requests, client.bytes, positiveDifference(finalRSS, baselineRSS),
		storageGrowth, walGrowth, networkGrowth)
}

func devHotProcessTreeRSS(t testing.TB, root int) uint64 {
	t.Helper()
	raw, err := exec.Command("ps", "-eo", "pid=,ppid=,rss=").Output()
	if err != nil {
		t.Fatal(err)
	}
	type process struct {
		pid, parent int
		rss         uint64
	}
	processes := make([]process, 0, 64)
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		pid, pidErr := strconv.Atoi(fields[0])
		parent, parentErr := strconv.Atoi(fields[1])
		rss, rssErr := strconv.ParseUint(fields[2], 10, 64)
		if pidErr == nil && parentErr == nil && rssErr == nil {
			processes = append(processes, process{pid: pid, parent: parent, rss: rss << 10})
		}
	}
	descendants := map[int]struct{}{root: {}}
	for changed := true; changed; {
		changed = false
		for _, candidate := range processes {
			if _, parent := descendants[candidate.parent]; !parent {
				continue
			}
			if _, found := descendants[candidate.pid]; !found {
				descendants[candidate.pid] = struct{}{}
				changed = true
			}
		}
	}
	var total uint64
	for _, candidate := range processes {
		if _, found := descendants[candidate.pid]; found {
			total += candidate.rss
		}
	}
	return total
}
