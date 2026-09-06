//go:build linux

package gatewayruntime

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
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
	ClientCertificate   string                `json:"client_certificate"`
	ClientKey           string                `json:"client_key"`
	ClientNode          string                `json:"client_node"`
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
		"--diagnostics-on-exit",
		"--shard-binary", shardBinary, "--gateway-binary", gatewayBinary,
	}}
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	defer replicaProcessStop(t, process)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("development cluster process diagnostics:\n%s", process.Diagnostics())
		}
	})
	if err := process.WaitReady(ctx, "VibeDB development RF3 physical cluster ready:"); err != nil {
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
	clientProfile, err := servicetls.LoadProfile(manifest.ClientCertificate, manifest.ClientKey,
		manifest.Roots, "1.3.6.1.4.1.32473.1.1", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	clientIdentity := clientProfile.LocalIdentity()
	if idString := hex.EncodeToString(clientIdentity.Node[:]); idString != manifest.ClientNode || idString == manifest.GatewayNode {
		t.Fatal("dev client credential does not match its distinct manifest identity")
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
	connection := hotMutationDialGateway(t, clientProfile, gatewayNode, manifest.ClientEndpoint)
	defer connection.Close()
	client := &hotMutationWireClient{connection: connection, reader: bufio.NewReader(connection)}
	reference := client.openIssuer(t)
	keySetupStarted := time.Now()
	keys := devHotStableSplitKeys(t, 16)
	keySetup := time.Since(keySetupStarted)
	if keySetup > 2*time.Second {
		t.Fatalf("stable split key setup=%s", keySetup)
	}
	latencies := make([]time.Duration, 0, 2_048)
	seed := make([]serveStatement, len(keys))
	for index, key := range keys {
		seed[index] = serveStatement{SQL: `INSERT INTO documents VALUES (?)`, Params: []serveParam{{
			Kind: "document", Text: fmt.Sprintf(`{"id":%q,"value":%d}`, key, index+1),
		}}}
	}
	latencies = append(latencies, client.execute(t, hotMutationRequest(t, reference, 1, seed)))
	// The shipped window measures operations, not the number of unique rows.
	// Serial durable INSERTs include the full request-ledger protocol and cannot
	// reliably produce 64 operations in one second. Drive real, ReadIndex-fenced
	// SQL point batches across both populated ranges instead. Every returned
	// value is checked, including again after the split publishes its children.
	readRequest := devHotReadRequest(t, keys)
	var operation [32]byte
	pressureDeadline := time.Now().Add(25 * time.Second)
	// Five 16-point batches per second exceed the 64-operation source window
	// while each balanced child stays below its unchanged 85% capacity limit.
	pressurePace := time.NewTicker(200 * time.Millisecond)
	defer pressurePace.Stop()
	for reads := 1; client.requests < 4_095 && time.Now().Before(pressureDeadline); reads++ {
		select {
		case <-ctx.Done():
			t.Fatal(context.Cause(ctx))
		case <-pressurePace.C:
		}
		latencies = append(latencies, devHotReadDocuments(t, client, readRequest, keys))
		if reads%8 != 0 {
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
		record, pressureErr := authority.ReadPressureRecord(ctx)
		t.Fatalf("zero-config pressure admitted no split after %d requests; pressure=%s err=%v\n%s",
			client.requests, record.Payload, pressureErr, process.Diagnostics())
	}
	final := hotMutationWaitSplitComplete(t, ctx, authority,
		snapshot.Generation()+1, operation, source, process)
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
	latencies = append(latencies, devHotReadDocuments(t, client, readRequest, keys))
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
	t.Logf("zero-config hot split: children=%d key_setup=%s p99=%s requests=%d wire=%d rss_growth=%d storage_growth=%d wal_growth=%d network_growth=%d",
		children, keySetup, p99, client.requests, client.bytes, positiveDifference(finalRSS, baselineRSS),
		storageGrowth, walGrowth, networkGrowth)
}

func devHotReadRequest(t *testing.T, keys []string) []byte {
	t.Helper()
	statements := make([]serveStatement, len(keys))
	for index, key := range keys {
		statements[index] = serveStatement{SQL: "SELECT * FROM documents WHERE id = ?",
			Params: []serveParam{{Kind: "string", Text: key}}}
	}
	raw, err := vibejson.Marshal(&serveRequest{Op: "read_batch", Class: "interactive",
		Statements: statements, MaxResultBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func devHotReadDocuments(t *testing.T, client *hotMutationWireClient, request []byte, keys []string) time.Duration {
	t.Helper()
	response, latency := client.roundTrip(t, request)
	var decoded struct {
		OK        bool   `json:"ok"`
		Found     []bool `json:"found"`
		Documents []struct {
			ID    string `json:"id"`
			Value uint64 `json:"value"`
		} `json:"documents"`
	}
	if err := json.Unmarshal(response, &decoded); err != nil || !decoded.OK ||
		len(decoded.Found) != len(keys) || len(decoded.Documents) != len(keys) {
		t.Fatalf("development native SQL read response=%s err=%v", response, err)
	}
	for index, key := range keys {
		if !decoded.Found[index] || decoded.Documents[index].ID != key || decoded.Documents[index].Value != uint64(index+1) {
			t.Fatalf("development native SQL read position=%d key=%q found=%t document=%+v",
				index, key, decoded.Found[index], decoded.Documents[index])
		}
	}
	return latency
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
			if status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", candidate.pid)); err == nil {
				var memory []string
				for _, line := range strings.Split(string(status), "\n") {
					if strings.HasPrefix(line, "Rss") || strings.HasPrefix(line, "Name:") {
						memory = append(memory, strings.Join(strings.Fields(line), " "))
					}
				}
				t.Logf("split process memory pid=%d rss=%d %s", candidate.pid, candidate.rss, strings.Join(memory, "; "))
			}
		}
	}
	return total
}

// devHotStableSplitKeys selects two well-separated SABLE bins, then
// alternates them. This keeps the qualifying boundary byte-identical across
// evidence windows without pinning traffic to one virtual bucket or relying
// on wall time. Keys remain unique and use the exact production tuple/hash
// grammar consumed by the development table's native mapper.
func devHotStableSplitKeys(t testing.TB, count int) []string {
	t.Helper()
	const maxKeyCount = 1 << 20
	if count <= 0 || count > maxKeyCount {
		t.Fatal("invalid stable split key count")
	}
	const leftBin, rightBin = autosplit.BinCount / 4, autosplit.BinCount * 3 / 4
	left := make([]string, 0, (count+1)/2)
	right := make([]string, 0, count/2)
	searchLimit := count * 256
	for candidate := 0; candidate < searchLimit && len(left)+len(right) < count; candidate++ {
		key := fmt.Sprintf("dev-hot-%08d", candidate)
		var values [1]distribution.Scalar
		values[0] = distribution.NewString(key)
		var storage [64]byte
		encoded, err := distribution.CurrentTupleCodec.AppendTuple(storage[:0], values[:])
		if err != nil {
			t.Fatal(err)
		}
		point, consumed, ok := distribution.NativePointForEncodedTuplePrefix(
			encoded, len(values), distribution.DefaultVirtualBucketBits,
		)
		if !ok || consumed != len(encoded) {
			t.Fatal("stable split key placement failed")
		}
		bin := int(point[0]) * autosplit.BinCount / 256
		switch {
		case bin == leftBin && len(left) < cap(left):
			left = append(left, key)
		case bin == rightBin && len(right) < cap(right):
			right = append(right, key)
		}
	}
	if len(left) != cap(left) || len(right) != cap(right) {
		t.Fatalf("stable split key search exhausted left=%d/%d right=%d/%d limit=%d",
			len(left), cap(left), len(right), cap(right), searchLimit)
	}
	keys := make([]string, 0, count)
	for index := 0; len(keys) < count; index++ {
		if index < len(left) {
			keys = append(keys, left[index])
		}
		if index < len(right) {
			keys = append(keys, right[index])
		}
	}
	return keys
}
