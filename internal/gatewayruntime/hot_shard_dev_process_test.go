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
	"net"
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
	"github.com/thesyncim/vibedb/internal/splitcontroller"
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
	// The persisted DDL Unix socket lives below the cluster root. Go's
	// testing.TempDir path is long enough on macOS and containerized runs to
	// exceed the platform Unix-socket pathname limit, so keep this fixture root
	// short and remove it through the test cleanup hook.
	root, err := os.MkdirTemp("/tmp", "vdb-hot-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove development cluster fixture root: %v", err)
		}
	})
	tableName := "documents"
	customTable := os.Getenv("VIBEDB_DEV_HOT_SPLIT_CUSTOM_TABLE") == "1"
	if customTable {
		tableName = "dev_hot_messages"
	}
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
	var pgListen string
	var customBundlePath string
	var customBundleBefore []byte
	var onlineBundlePath string
	var onlineBundleBefore []byte
	processArgs := []string{
		"cluster", "dev", "--replicas", "3", "--root", state,
		"--diagnostics-on-exit",
		"--shard-binary", shardBinary, "--gateway-binary", gatewayBinary,
	}
	if customTable {
		pgListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		pgListen = pgListener.Addr().String()
		if err := pgListener.Close(); err != nil {
			t.Fatal(err)
		}
		schemaPath := filepath.Join(root, "dev-hot-messages.sql")
		if err := os.WriteFile(schemaPath, []byte("CREATE TABLE dev_hot_messages (id TEXT PRIMARY KEY, value INTEGER NOT NULL)"), 0o600); err != nil {
			t.Fatal(err)
		}
		processArgs = append(processArgs, "--table-schema", schemaPath, "--pg-listen", pgListen)
	}
	process := &rf3testfixture.ExternalProcess{Binary: vibedbBinary, Args: processArgs}
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
	if customTable {
		// The base genesis file intentionally excludes independently registered
		// table bundles. Read the live catalog after gateway startup so route
		// selection proves the source was published before serving.
		snapshot, err = authority.Read(ctx)
		if err != nil {
			t.Fatalf("read live custom-table catalog: %v", err)
		}
		bundlePaths, globErr := filepath.Glob(filepath.Join(state, "table-dev_hot_messages-*-split-source.vibejson"))
		if globErr != nil || len(bundlePaths) != 1 {
			t.Fatalf("custom-table retained bundle paths=%v err=%v", bundlePaths, globErr)
		}
		customBundlePath = bundlePaths[0]
		customBundleBefore, err = os.ReadFile(customBundlePath)
		if err != nil {
			t.Fatal(err)
		}
		// Exercise the authenticated live DDL forwarding path on a quiet
		// independently provisioned table. Its ALTER must not rederive or
		// rewrite the original --table-schema bundle.
		ddlConnection := openDDLWire(t, ctx, pgListen)
		if result := ddlWireQuery(t, ddlConnection, "CREATE TABLE dev_hot_online (id TEXT PRIMARY KEY, value INTEGER NOT NULL)", true); result.code != "" || result.tag != "CREATE TABLE" {
			ddlConnection.Close()
			t.Fatalf("live custom CREATE: %+v", result)
		}
		onlineBundlePaths, globErr := filepath.Glob(filepath.Join(state, "table-dev_hot_online-*-split-source.vibejson"))
		if globErr != nil || len(onlineBundlePaths) != 1 {
			ddlConnection.Close()
			t.Fatalf("live DDL table bundle paths=%v err=%v", onlineBundlePaths, globErr)
		}
		onlineBundlePath = onlineBundlePaths[0]
		onlineBundleBefore, err = os.ReadFile(onlineBundlePath)
		if err != nil {
			ddlConnection.Close()
			t.Fatal(err)
		}
		if result := ddlWireQuery(t, ddlConnection, "INSERT INTO dev_hot_online (id,value) VALUES ('online-1',7)", false); result.code != "" || result.tag != "INSERT 0 1" {
			ddlConnection.Close()
			t.Fatalf("live custom INSERT: %+v", result)
		}
		if result := ddlWireQuery(t, ddlConnection, "ALTER TABLE dev_hot_online ADD COLUMN marker TEXT", true); result.code != "" || result.tag != "ALTER TABLE" {
			ddlConnection.Close()
			t.Fatalf("live custom ALTER: %+v", result)
		}
		if result := ddlWireQuery(t, ddlConnection, "UPDATE dev_hot_online SET marker='after-alter' WHERE id='online-1'", true); result.code != "" || result.tag != "UPDATE 1" {
			ddlConnection.Close()
			t.Fatalf("live custom UPDATE: %+v", result)
		}
		if result := ddlWireQuery(t, ddlConnection, "SELECT id,value,marker FROM dev_hot_online WHERE id='online-1'", true); result.code != "" || len(result.rows) != 1 || strings.Join(result.rows[0], "|") != `"online-1"|7|"after-alter"` {
			ddlConnection.Close()
			t.Fatalf("live custom row oracle: %+v", result)
		}
		if err := ddlConnection.Close(); err != nil {
			t.Fatal(err)
		}
		customBundleAfterDDL, err := os.ReadFile(customBundlePath)
		if err != nil || !bytes.Equal(customBundleAfterDDL, customBundleBefore) {
			t.Fatalf("live DDL changed original custom bundle: %v", err)
		}
		onlineBundleAfterDDL, err := os.ReadFile(onlineBundlePath)
		if err != nil || !bytes.Equal(onlineBundleAfterDDL, onlineBundleBefore) {
			t.Fatalf("live ALTER changed its provision bundle: %v", err)
		}
		// The DDL registration advances the catalog generation; route selection
		// must use the fresh live cut for the split source.
		snapshot, err = authority.Read(ctx)
		if err != nil {
			t.Fatalf("read catalog after live custom DDL: %v", err)
		}
	}
	placement, found := snapshot.Placement(tableName)
	if !found || len(placement.Columns) != 1 {
		t.Fatalf("%s placement missing: %+v", tableName, placement)
	}
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	source, found := snapshot.ResolveReplicatedRoute(placement.Distribution, "all", replicas[:0])
	if !found {
		t.Fatalf("%s route missing: distribution=%q", tableName, placement.Distribution)
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
		if customTable {
			seed[index] = serveStatement{SQL: `INSERT INTO dev_hot_messages (id, value) VALUES (?, ?)`, Params: []serveParam{
				{Kind: "string", Text: key}, {Kind: "number", Text: strconv.Itoa(index + 1)},
			}}
		} else {
			seed[index] = serveStatement{SQL: `INSERT INTO documents VALUES (?)`, Params: []serveParam{{
				Kind: "document", Text: fmt.Sprintf(`{"id":%q,"value":%d}`, key, index+1),
			}}}
		}
	}
	latencies = append(latencies, client.execute(t, hotMutationRequest(t, reference, 1, seed)))
	// The shipped window measures operations, not the number of unique rows.
	// Serial durable INSERTs include the full request-ledger protocol and cannot
	// reliably produce 64 operations in one second. Drive real, ReadIndex-fenced
	// SQL point batches across both populated ranges instead. Every returned
	// value is checked, including again after the split publishes its children.
	readRequest := devHotReadRequestForTable(t, tableName, keys)
	var operation [32]byte
	var operationDirectory string
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
		if readErr != nil {
			continue
		}
		entries := make([]string, 0, len(ids))
		var splitCount int
		var matchingSplit [32]byte
		for _, id := range ids {
			record, recordErr := authority.ReadOperation(ctx, id)
			if errors.Is(recordErr, gateway.ErrReplicatedOperationMissing) {
				continue
			}
			if recordErr != nil || record.ID != id || !record.Valid() {
				t.Fatalf("invalid pressure operation id=%x record=%+v err=%v", id, record, recordErr)
			}
			entries = append(entries, fmt.Sprintf("%x(kind=%d,state=%d,revision=%d)", id, record.Kind, record.State, record.Revision))
			if record.Kind != gateway.ReplicatedOperationSplit {
				continue
			}
			splitCount++
			if splitCount > 1 {
				t.Fatalf("hot pressure admitted multiple split operations: %s", strings.Join(entries, ","))
			}
			plan, openErr := splitcontroller.OpenPlanIntent(record.Intent, snapshot)
			if openErr != nil {
				t.Fatalf("pressure split operation id=%x could not reopen its plan: %v", id, openErr)
			}
			distributionName, shard, allocation := plan.SourceAllocation()
			if distributionName != source.Distribution || shard != source.Shard || uint64(allocation) != source.AllocationGeneration {
				continue
			}
			matchingSplit = id
		}
		operationDirectory = strings.Join(entries, ",")
		if splitCount == 1 && matchingSplit != ([32]byte{}) {
			operation = matchingSplit
			t.Logf("selected split operation id=%x source=%s/%s allocation=%d directory=%s", operation, source.Distribution, source.Shard, source.AllocationGeneration, operationDirectory)
			break
		}
	}
	if operation == ([32]byte{}) {
		record, pressureErr := authority.ReadPressureRecord(ctx)
		t.Fatalf("zero-config pressure admitted no matching split after %d requests; operations=%s pressure=%s err=%v\n%s",
			client.requests, operationDirectory, record.Payload, pressureErr, process.Diagnostics())
	}
	final := hotMutationWaitSplitComplete(t, ctx, authority,
		snapshot.Generation()+1, operation, source, process)
	children := 0
	for _, descriptor := range final.ReplicatedShardDescriptors() {
		if descriptor.Distribution != source.Distribution ||
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
	if customTable {
		// Restart the same supervisor with the same durable root and table-schema
		// input. The retained split-source bundle must register the custom table
		// before the catalog is advertised; the post-restart reads are the exact
		// row oracle for every seeded key.
		if err := connection.Close(); err != nil {
			t.Logf("close pre-restart gateway connection: %v", err)
		}
		if err := process.Stop(ctx); err != nil {
			t.Fatalf("custom-table clean restart stop: %v\n%s", err, process.Diagnostics())
		}
		if err := process.Start(); err != nil {
			t.Fatalf("custom-table restart start: %v", err)
		}
		if err := process.WaitReady(ctx, "VibeDB development RF3 physical cluster ready:"); err != nil {
			t.Fatalf("custom-table restart readiness: %v\n%s", err, process.Diagnostics())
		}
		restartedConnection := hotMutationDialGateway(t, clientProfile, gatewayNode, manifest.ClientEndpoint)
		defer restartedConnection.Close()
		restartedClient := &hotMutationWireClient{connection: restartedConnection, reader: bufio.NewReader(restartedConnection)}
		devHotReadDocuments(t, restartedClient, readRequest, keys)
		checkConnection := openDDLWire(t, ctx, pgListen)
		if result := ddlWireQuery(t, checkConnection, "SELECT id,value,marker FROM dev_hot_online WHERE id='online-1'", true); result.code != "" || len(result.rows) != 1 || strings.Join(result.rows[0], "|") != `"online-1"|7|"after-alter"` {
			checkConnection.Close()
			t.Fatalf("post-restart live DDL row oracle: %+v", result)
		}
		if err := checkConnection.Close(); err != nil {
			t.Fatal(err)
		}
		customBundleAfterRestart, err := os.ReadFile(customBundlePath)
		if err != nil || !bytes.Equal(customBundleAfterRestart, customBundleBefore) {
			t.Fatalf("restart changed original custom bundle: %v", err)
		}
		onlineBundleAfterRestart, err := os.ReadFile(onlineBundlePath)
		if err != nil || !bytes.Equal(onlineBundleAfterRestart, onlineBundleBefore) {
			t.Fatalf("restart changed live DDL provision bundle: %v", err)
		}
		t.Logf("custom-table split/restart exact row oracle: table=%s rows=%d children=%d", tableName, len(keys), children)
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
	t.Logf("zero-config hot split: children=%d key_setup=%s p99=%s requests=%d wire=%d rss_growth=%d storage_growth=%d wal_growth=%d network_growth=%d",
		children, keySetup, p99, client.requests, client.bytes, positiveDifference(finalRSS, baselineRSS),
		storageGrowth, walGrowth, networkGrowth)
}

func devHotReadRequest(t *testing.T, keys []string) []byte {
	return devHotReadRequestForTable(t, "documents", keys)
}

func devHotReadRequestForTable(t *testing.T, table string, keys []string) []byte {
	t.Helper()
	statements := make([]serveStatement, len(keys))
	for index, key := range keys {
		statements[index] = serveStatement{SQL: "SELECT * FROM " + table + " WHERE id = ?",
			Params: []serveParam{{Kind: "string", Text: key}}}
	}
	raw, err := vibejson.Marshal(&serveRequest{Op: "read_batch", Class: "interactive",
		Statements: statements, MaxResultBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestGatewayCustomTableDevPressureCompletesReplicatedSplitAndRestart(t *testing.T) {
	if os.Getenv("VIBEDB_DEV_HOT_SPLIT_CUSTOM_TABLE_E2E") != "1" {
		t.Skip("set VIBEDB_DEV_HOT_SPLIT_CUSTOM_TABLE_E2E=1 for custom-table split/restart qualification")
	}
	t.Setenv("VIBEDB_DEV_HOT_SPLIT_CUSTOM_TABLE", "1")
	TestGatewayZeroConfigDevPressureCompletesReplicatedSplit(t)
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
		response, _ := client.roundTripWithin(t, request, remaining)
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
		return time.Since(started)
	}
	t.Fatalf("development native SQL read stale catalog did not settle within %s", deadline.Sub(started))
	return 0
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
