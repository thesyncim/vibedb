//go:build linux

package gatewayruntime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
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
	PhysicalNodes       uint8                 `json:"physical_nodes,omitempty"`
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

	// Retain rows from the same two existing ps samples used by the RSS bound;
	// failure diagnostics must not add a happy-path process probe.
	baselineProcessTree := devHotProcessTreeSample(t, process.PID())
	baselineRSS := baselineProcessTree.totalRSS
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
	seedRequest := hotMutationRequest(t, reference, 1, seed)
	seedStarted := time.Now()
	seedLatency := time.Duration(0)
	var seedObservations [2]struct {
		elapsed  time.Duration
		response []byte
	}
	seedObservationCount := 0
	func() {
		defer func() {
			totalElapsed := time.Since(seedStarted)
			client.executeDiagnostic = nil
			for index := 0; index < seedObservationCount; index++ {
				t.Logf("dev-hot seed execute attempt=%d elapsed=%s result=%s",
					index+1, seedObservations[index].elapsed,
					hotMutationExecuteResultClass(seedObservations[index].response))
			}
			t.Logf("dev-hot seed execute total_elapsed=%s attempts=%d returned_elapsed=%s",
				totalElapsed, seedObservationCount, seedLatency)
		}()
		client.executeDiagnostic = func(attempt int, elapsed time.Duration, response []byte) {
			if attempt >= 1 && attempt <= len(seedObservations) {
				seedObservations[attempt-1] = struct {
					elapsed  time.Duration
					response []byte
				}{elapsed: elapsed, response: response}
				if attempt > seedObservationCount {
					seedObservationCount = attempt
				}
			}
		}
		seedLatency = client.execute(t, seedRequest)
	}()
	latencies = append(latencies, seedLatency)
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
	finalProcessTree := devHotProcessTreeSample(t, process.PID())
	finalRSS := finalProcessTree.totalRSS
	storageGrowth := positiveDifference(replicaProcessAllocatedBytes(state, ""), baselineStorage)
	walGrowth := positiveDifference(replicaProcessAllocatedBytes(state, ".wal"), baselineWAL)
	networkGrowth := positiveDifference(replicaProcessSnapshotPayloadBytes(state), baselineNetwork)
	rssGrowth := positiveDifference(finalRSS, baselineRSS)
	boundsFailed := p99 > 5*time.Second || client.requests > 4_096 || client.bytes > 32<<20 ||
		rssGrowth > 768<<20 || storageGrowth > 2<<30 ||
		walGrowth > 1<<30 || networkGrowth > 2<<30
	// Freeze every original qualification measurement and its bounds decision
	// before taking the one diagnostic cut. The signal path is deliberately
	// post-measurement so its synchronous resource and MemStats sampling cannot
	// affect the measured workload or any bound.
	captureDevHotPostMeasurement(t, manifest, baselineProcessTree, finalProcessTree,
		baselineRSS, finalRSS, rssGrowth, p99, client.requests, client.bytes,
		storageGrowth, walGrowth, networkGrowth, boundsFailed)
	if boundsFailed {
		t.Fatalf("dev hot split bounds p99=%s requests=%d wire=%d rss_growth=%d storage_growth=%d wal_growth=%d network_growth=%d",
			p99, client.requests, client.bytes, rssGrowth,
			storageGrowth, walGrowth, networkGrowth)
	}
	t.Logf("zero-config hot split: children=%d key_setup=%s p99=%s requests=%d wire=%d rss_growth=%d storage_growth=%d wal_growth=%d network_growth=%d",
		children, keySetup, p99, client.requests, client.bytes, rssGrowth,
		storageGrowth, walGrowth, networkGrowth)
}

func hotMutationExecuteResultClass(response []byte) string {
	var envelope struct {
		Committed      bool   `json:"committed"`
		OutcomeUnknown bool   `json:"outcome_unknown"`
		Error          string `json:"error"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		return "invalid-json"
	}
	switch {
	case strings.Contains(envelope.Error, gateway.ErrDurableRequestUnresolved.Error()):
		return "unresolved"
	case envelope.Committed && !envelope.OutcomeUnknown && envelope.Error == "":
		return "committed"
	case envelope.OutcomeUnknown:
		return "outcome-unknown"
	case envelope.Error != "":
		return "error"
	default:
		return "other"
	}
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
	started := time.Now()
	deadline := started.Add(durableRF3ExternalForegroundObjective + durableRF3ExternalForegroundGrace)
	for attempt := 0; ; attempt++ {
		if cause := context.Cause(t.Context()); cause != nil {
			t.Fatalf("development native SQL read canceled after %d attempts: %v", attempt, cause)
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("development native SQL read did not settle within %s after %d attempts", deadline.Sub(started), attempt)
		}
		response, _ := client.roundTripUntil(t, request, deadline)
		var envelope struct {
			OK        *bool  `json:"ok"`
			Found     []bool `json:"found"`
			Documents []struct {
				ID    string `json:"id"`
				Value uint64 `json:"value"`
			} `json:"documents"`
			Code      string `json:"code"`
			Retryable *bool  `json:"retryable"`
		}
		if err := json.Unmarshal(response, &envelope); err != nil || envelope.OK == nil {
			t.Fatalf("development native SQL read response=%s err=%v", response, err)
		}
		if !*envelope.OK {
			if attempt == 0 && envelope.Code == "stale_catalog" && envelope.Retryable != nil && *envelope.Retryable {
				remaining = time.Until(deadline)
				if remaining <= 0 {
					break
				}
				backoff := min(durableRF3ExternalRetryBackoff, remaining)
				timer := time.NewTimer(backoff)
				select {
				case <-t.Context().Done():
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					t.Fatalf("development native SQL read canceled after %d attempts: %v", attempt+1, context.Cause(t.Context()))
				case <-timer.C:
				}
				continue
			}
			t.Fatalf("development native SQL read response=%s", response)
		}
		if len(envelope.Found) != len(keys) || len(envelope.Documents) != len(keys) {
			t.Fatalf("development native SQL read response=%s", response)
		}
		for index, key := range keys {
			if !envelope.Found[index] || envelope.Documents[index].ID != key || envelope.Documents[index].Value != uint64(index+1) {
				t.Fatalf("development native SQL read position=%d key=%q found=%t document=%+v",
					index, key, envelope.Found[index], envelope.Documents[index])
			}
		}
		if cause := context.Cause(t.Context()); cause != nil {
			t.Fatalf("development native SQL read canceled after %d attempts: %v", attempt+1, cause)
		}
		if remaining := time.Until(deadline); remaining <= 0 {
			t.Fatalf("development native SQL read exceeded its %s deadline after %d attempts", deadline.Sub(started), attempt+1)
		}
		return time.Since(started)
	}
	t.Fatalf("development native SQL read stale catalog did not settle within %s", deadline.Sub(started))
	return 0
}

type devHotProcessTreeRow struct {
	pid, parent int
	rssBytes    uint64
}

type devHotProcessTreeRSSSample struct {
	rootPID   int
	totalRSS  uint64
	processes []devHotProcessTreeRow
}

func devHotProcessTreeSample(t testing.TB, root int) devHotProcessTreeRSSSample {
	t.Helper()
	raw, err := exec.Command("ps", "-eo", "pid=,ppid=,rss=").Output()
	if err != nil {
		t.Fatal(err)
	}
	processes := make([]devHotProcessTreeRow, 0, 64)
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		pid, pidErr := strconv.Atoi(fields[0])
		parent, parentErr := strconv.Atoi(fields[1])
		rss, rssErr := strconv.ParseUint(fields[2], 10, 64)
		if pidErr == nil && parentErr == nil && rssErr == nil {
			processes = append(processes, devHotProcessTreeRow{pid: pid, parent: parent, rssBytes: rss << 10})
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
	owned := make([]devHotProcessTreeRow, 0, len(descendants))
	var total uint64
	for _, candidate := range processes {
		if _, found := descendants[candidate.pid]; found {
			total += candidate.rssBytes
			owned = append(owned, candidate)
		}
	}
	sort.Slice(owned, func(left, right int) bool { return owned[left].pid < owned[right].pid })
	return devHotProcessTreeRSSSample{rootPID: root, totalRSS: total, processes: owned}
}

const (
	devHotProcReadLimit       = 64 << 10
	devHotServeManifestLimit  = 1 << 20
	devHotSnapshotReadLimit   = 64 << 10
	devHotSnapshotWaitTimeout = 750 * time.Millisecond
)

type devHotServeManifest struct {
	NodeLog *struct {
		Path string `json:"path"`
	} `json:"node_log"`
}

type devHotDiagnosticSnapshotHeader struct {
	Event  string `json:"event"`
	Serial uint64 `json:"serial"`
	PID    int    `json:"pid"`
}

type devHotDiagnosticSnapshotBaseline struct {
	raw            []byte
	available      bool
	missing        bool
	freshAllowed   bool
	header         devHotDiagnosticSnapshotHeader
	headerDecoded  bool
	headerVerified bool
}

type devHotShardDiagnosticTarget struct {
	pid           int
	serveManifest string
	nodeLogPath   string
	snapshotPath  string
}

func captureDevHotPostMeasurement(
	t testing.TB,
	manifest devHotProcessManifest,
	baseline, final devHotProcessTreeRSSSample,
	baselineRSS, finalRSS, rssGrowth uint64,
	p99 time.Duration, requests, wireBytes uint64,
	storageGrowth, walGrowth, networkGrowth uint64,
	boundsFailed bool,
) {
	t.Helper()
	// The caller invokes this after every original qualification measurement and
	// its bounds decision have been frozen, while the supervisor and serving
	// children are still running. This post-measurement cut must never feed back
	// into the measured workload or its assertions.
	t.Logf("dev hot post-measurement diagnostic phase=post_measurement bounds_failed=%t root_pid=%d p99=%s requests=%d wire_bytes=%d storage_growth_bytes=%d wal_growth_bytes=%d network_growth_bytes=%d baseline_rss_bytes=%d final_rss_bytes=%d rss_growth_bytes=%d baseline_owned_processes=%d final_owned_processes=%d",
		boundsFailed, final.rootPID, p99, requests, wireBytes, storageGrowth,
		walGrowth, networkGrowth, baselineRSS, finalRSS, rssGrowth,
		len(baseline.processes), len(final.processes))
	for _, sample := range []struct {
		name string
		data devHotProcessTreeRSSSample
	}{
		{name: "baseline", data: baseline},
		{name: "final", data: final},
	} {
		for _, process := range sample.data.processes {
			t.Logf("dev hot post-measurement diagnostic phase=post_measurement ps_sample=%s pid=%d ppid=%d rss_bytes=%d",
				sample.name, process.pid, process.parent, process.rssBytes)
		}
	}
	for _, process := range final.processes {
		status := devHotProcFields(process.pid, "status", []string{
			"Name", "Pid", "PPid", "State", "VmPeak", "VmSize", "VmRSS",
			"RssAnon", "RssFile", "RssShmem", "Threads",
		})
		smaps := devHotProcFields(process.pid, "smaps_rollup", []string{
			"Rss", "Pss", "Pss_Anon", "Pss_File", "Pss_Shmem", "Shared_Clean",
			"Shared_Dirty", "Private_Clean", "Private_Dirty", "Referenced",
			"Anonymous", "AnonHugePages", "Swap",
		})
		t.Logf("dev hot post-measurement diagnostic phase=post_measurement pid=%d ppid=%d ps_rss_bytes=%d proc_status=%s proc_smaps_rollup=%s",
			process.pid, process.parent, process.rssBytes, status, smaps)
	}
	targets := discoverDevHotShardDiagnostics(t, manifest, final.processes)
	for _, target := range targets {
		before, beforeTruncated, beforeErr := devHotReadBoundedFile(target.snapshotPath, devHotSnapshotReadLimit)
		baseline := devHotDiagnosticSnapshotBaseline{
			raw: before, available: beforeErr == nil,
			missing:      errors.Is(beforeErr, os.ErrNotExist),
			freshAllowed: beforeErr == nil || errors.Is(beforeErr, os.ErrNotExist),
		}
		if baseline.available {
			baseline.header, baseline.headerDecoded, baseline.headerVerified = devHotDecodeDiagnosticSnapshotHeader(
				before, beforeTruncated, target.pid,
			)
			if beforeTruncated || !baseline.headerDecoded {
				baseline.freshAllowed = false
				t.Logf("dev hot post-measurement diagnostic phase=post_measurement shard pid=%d serve_manifest=%q node_log=%q snapshot=%q pre_snapshot_unverified=true pre_snapshot_truncated=%t fresh_allowed=false",
					target.pid, target.serveManifest, target.nodeLogPath, target.snapshotPath, beforeTruncated)
			}
		}
		if beforeErr != nil && !baseline.missing {
			t.Logf("dev hot post-measurement diagnostic phase=post_measurement shard pid=%d serve_manifest=%q node_log=%q snapshot=%q pre_snapshot_error=%v fresh_allowed=false",
				target.pid, target.serveManifest, target.nodeLogPath, target.snapshotPath, beforeErr)
		}
		signalErr := syscall.Kill(target.pid, syscall.SIGUSR1)
		if signalErr != nil {
			t.Logf("dev hot post-measurement diagnostic phase=post_measurement shard pid=%d serve_manifest=%q node_log=%q snapshot=%q signal=SIGUSR1 signal_error=%v",
				target.pid, target.serveManifest, target.nodeLogPath, target.snapshotPath, signalErr)
			continue
		}
		snapshot, fresh, verified, truncated, readErr := devHotWaitForDiagnosticSnapshot(
			target.snapshotPath, baseline, target.pid,
		)
		if readErr != nil {
			t.Logf("dev hot post-measurement diagnostic phase=post_measurement shard pid=%d serve_manifest=%q node_log=%q snapshot=%q signal=SIGUSR1 fresh=%t verified=%t snapshot_error=%v",
				target.pid, target.serveManifest, target.nodeLogPath, target.snapshotPath, fresh, verified, readErr)
			continue
		}
		t.Logf("dev hot post-measurement diagnostic phase=post_measurement shard pid=%d serve_manifest=%q node_log=%q snapshot=%q signal=SIGUSR1 fresh=%t verified=%t snapshot_truncated=%t snapshot=%s",
			target.pid, target.serveManifest, target.nodeLogPath, target.snapshotPath, fresh, verified, truncated, strings.TrimSpace(string(snapshot)))
	}
}

func devHotProcFields(pid int, name string, fields []string) string {
	path := filepath.Join("/proc", strconv.Itoa(pid), name)
	raw, truncated, err := devHotReadBoundedFile(path, devHotProcReadLimit)
	if err != nil {
		return fmt.Sprintf("unavailable(%v)", err)
	}
	result := devHotSelectProcFields(raw, fields)
	if result == "" {
		result = "none"
	}
	if truncated {
		result += ";truncated=true"
	}
	return result
}

func devHotSelectProcFields(raw []byte, fields []string) string {
	wanted := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		wanted[field] = struct{}{}
	}
	selected := make([]string, 0, len(fields))
	for _, line := range strings.Split(string(raw), "\n") {
		key, _, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		if _, wanted := wanted[key]; wanted {
			selected = append(selected, strings.TrimSpace(line))
		}
	}
	return strings.Join(selected, ";")
}

func discoverDevHotShardDiagnostics(
	t testing.TB,
	manifest devHotProcessManifest,
	processes []devHotProcessTreeRow,
) []devHotShardDiagnosticTarget {
	t.Helper()
	// Role members share one serve manifest per physical node. Keep discovery
	// strict so a diagnostic signal can never be sent to an ambiguous process.
	serveManifestSet := make(map[string]struct{}, len(manifest.Members)+len(manifest.LedgerMembers)+len(manifest.DataMembers))
	invalidExpectedManifest := false
	for _, members := range [][]devHotProcessMember{manifest.Members, manifest.LedgerMembers, manifest.DataMembers} {
		for _, member := range members {
			if member.ServeManifest == "" {
				invalidExpectedManifest = true
				continue
			}
			serveManifestSet[member.ServeManifest] = struct{}{}
		}
	}
	serveManifests := make([]string, 0, len(serveManifestSet))
	for path := range serveManifestSet {
		serveManifests = append(serveManifests, path)
	}
	sort.Strings(serveManifests)
	processArgv := make(map[int][]string, len(processes))
	for _, process := range processes {
		path := filepath.Join("/proc", strconv.Itoa(process.pid), "cmdline")
		raw, truncated, err := devHotReadBoundedFile(path, devHotProcReadLimit)
		if err != nil {
			t.Logf("dev hot post-measurement diagnostic phase=post_measurement pid=%d cmdline unavailable: %v", process.pid, err)
			continue
		}
		if truncated {
			t.Logf("dev hot post-measurement diagnostic phase=post_measurement pid=%d cmdline truncated=true", process.pid)
			continue
		}
		argv := devHotProcessArgv(raw)
		if len(argv) == 0 {
			t.Logf("dev hot post-measurement diagnostic phase=post_measurement pid=%d cmdline empty", process.pid)
			continue
		}
		processArgv[process.pid] = argv
	}
	targets := make([]devHotShardDiagnosticTarget, 0, len(serveManifests))
	usedPIDs := make(map[int]string, len(serveManifests))
	expectedPhysical := int(manifest.PhysicalNodes)
	if expectedPhysical == 0 {
		expectedPhysical = int(manifest.Nodes)
	}
	invalid := invalidExpectedManifest || expectedPhysical <= 0 || len(serveManifests) != expectedPhysical
	if invalidExpectedManifest {
		t.Logf("dev hot post-measurement diagnostic phase=post_measurement expected physical serve_manifest missing")
	}
	if expectedPhysical <= 0 || len(serveManifests) != expectedPhysical {
		t.Logf("dev hot post-measurement diagnostic phase=post_measurement expected_physical_manifests=%d discovered=%d",
			expectedPhysical, len(serveManifests))
	}
	for _, serveManifest := range serveManifests {
		raw, truncated, err := devHotReadBoundedFile(serveManifest, devHotServeManifestLimit)
		if err != nil {
			t.Logf("dev hot post-measurement diagnostic phase=post_measurement serve_manifest=%q unavailable: %v", serveManifest, err)
			invalid = true
			continue
		}
		if truncated {
			t.Logf("dev hot post-measurement diagnostic phase=post_measurement serve_manifest=%q truncated=true", serveManifest)
			invalid = true
			continue
		}
		var encoded devHotServeManifest
		if err := vibejson.Unmarshal(raw, &encoded); err != nil {
			t.Logf("dev hot post-measurement diagnostic phase=post_measurement serve_manifest=%q decode_error=%v", serveManifest, err)
			invalid = true
			continue
		}
		if encoded.NodeLog == nil || encoded.NodeLog.Path == "" {
			t.Logf("dev hot post-measurement diagnostic phase=post_measurement serve_manifest=%q node_log unavailable", serveManifest)
			invalid = true
			continue
		}
		matches := make([]int, 0, 1)
		for _, process := range processes {
			argv, found := processArgv[process.pid]
			if found && devHotProcessMatchesServeManifest(argv, serveManifest) {
				matches = append(matches, process.pid)
			}
		}
		if len(matches) != 1 {
			t.Logf("dev hot post-measurement diagnostic phase=post_measurement serve_manifest=%q exact_serve_node_matches=%v want=1", serveManifest, matches)
			invalid = true
			continue
		}
		pid := matches[0]
		if prior, duplicate := usedPIDs[pid]; duplicate {
			t.Logf("dev hot post-measurement diagnostic phase=post_measurement serve_manifest=%q pid=%d already matched serve_manifest=%q", serveManifest, pid, prior)
			invalid = true
			continue
		}
		usedPIDs[pid] = serveManifest
		targets = append(targets, devHotShardDiagnosticTarget{
			pid: pid, serveManifest: serveManifest, nodeLogPath: encoded.NodeLog.Path,
			snapshotPath: filepath.Join(filepath.Dir(encoded.NodeLog.Path), "rf3-diagnostics.json"),
		})
	}
	if invalid || len(targets) != len(serveManifests) {
		t.Logf("dev hot post-measurement diagnostic phase=post_measurement shard snapshot collection suppressed expected_manifests=%d matched_targets=%d",
			len(serveManifests), len(targets))
		return nil
	}
	return targets
}

func devHotProcessArgv(raw []byte) []string {
	if len(raw) == 0 {
		return nil
	}
	parts := bytes.Split(raw, []byte{0})
	if len(parts) != 0 && len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}
	argv := make([]string, len(parts))
	for index, part := range parts {
		argv[index] = string(part)
	}
	return argv
}

func devHotProcessMatchesServeManifest(argv []string, expectedManifest string) bool {
	if len(argv) < 4 || argv[1] != "serve-node" {
		return false
	}
	found := false
	for index := 2; index < len(argv); index++ {
		if argv[index] != "-manifest" {
			continue
		}
		if found || index+1 >= len(argv) || argv[index+1] != expectedManifest {
			return false
		}
		found = true
		index++
	}
	return found
}

func devHotReadBoundedFile(path string, limit int) ([]byte, bool, error) {
	if limit <= 0 {
		return nil, false, fmt.Errorf("invalid read limit=%d", limit)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil {
		return raw, false, err
	}
	if len(raw) > limit {
		return raw[:limit], true, nil
	}
	return raw, false, nil
}

func devHotDecodeDiagnosticSnapshotHeader(
	raw []byte, truncated bool, expectedPID int,
) (devHotDiagnosticSnapshotHeader, bool, bool) {
	if truncated {
		return devHotDiagnosticSnapshotHeader{}, false, false
	}
	var header devHotDiagnosticSnapshotHeader
	if json.Unmarshal(raw, &header) != nil {
		return devHotDiagnosticSnapshotHeader{}, false, false
	}
	return header, true, header.Event == "snapshot" && header.PID == expectedPID
}

func devHotWaitForDiagnosticSnapshot(
	path string,
	baseline devHotDiagnosticSnapshotBaseline,
	expectedPID int,
) ([]byte, bool, bool, bool, error) {
	deadline := time.Now().Add(devHotSnapshotWaitTimeout)
	var latest []byte
	var latestVerified, latestTruncated, haveLatest bool
	for {
		raw, truncated, err := devHotReadBoundedFile(path, devHotSnapshotReadLimit)
		if err == nil {
			latest, latestTruncated, haveLatest = raw, truncated, true
			header, _, verified := devHotDecodeDiagnosticSnapshotHeader(raw, truncated, expectedPID)
			latestVerified = verified
			fresh := false
			if baseline.freshAllowed && verified {
				switch {
				case baseline.headerVerified:
					fresh = header.Serial > baseline.header.Serial
				case baseline.missing:
					fresh = true
				case baseline.available:
					fresh = !bytes.Equal(raw, baseline.raw)
				}
			}
			if fresh {
				return raw, true, verified, truncated, nil
			}
		}
		if !time.Now().Before(deadline) {
			if haveLatest {
				return latest, false, latestVerified, latestTruncated, nil
			}
			if err != nil {
				return nil, false, false, false, err
			}
			return nil, false, false, false, nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		<-timer.C
	}
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
