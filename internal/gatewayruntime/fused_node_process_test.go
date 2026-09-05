//go:build linux

package gatewayruntime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
	"golang.org/x/sys/unix"
)

const fusedNodeProcessEnvironment = "VIBEDB_FUSED_RF3_PROCESS_E2E"

// TestFusedRF3NodeProcessQualification is the bounded black-box gate for the
// shipped physical-node development command. It starts the real supervisor,
// verifies its child argv and persisted identities, enrolls three SQL groups
// through the production DDL/SIGHUP path, writes concurrently to those groups,
// then SIGKILLs every serving process and reopens the same roots.
func TestFusedRF3NodeProcessQualification(t *testing.T) {
	if os.Getenv(fusedNodeProcessEnvironment) != "1" {
		t.Skip("set " + fusedNodeProcessEnvironment + "=1 for the mandatory Linux physical-node qualification")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("physical-node process qualification requires Linux /proc and SIGKILL")
	}
	if testing.Short() {
		t.Fatal("physical-node process qualification cannot run in short mode")
	}
	if _, err := os.Stat("/proc"); err != nil {
		t.Fatalf("required Linux /proc is unavailable: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	bin := t.TempDir()
	vibedbBinary := filepath.Join(bin, "vibedb")
	shardBinary := filepath.Join(bin, "vibedb-shard")
	replicaProcessBuild(t, ctx, vibedbBinary, "./cmd/vibedb")
	replicaProcessBuild(t, ctx, shardBinary, "./cmd/vibedb-shard")

	for _, testCase := range []struct {
		name           string
		physical       int
		explicitNumber bool
	}{
		{name: "default-physical3", physical: 3},
		{name: "explicit-physical6", physical: 6, explicitNumber: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			caseCtx, caseCancel := context.WithTimeout(ctx, 5*time.Minute)
			defer caseCancel()
			runFusedNodeProcessCase(t, caseCtx, vibedbBinary, shardBinary,
				testCase.physical, testCase.explicitNumber)
		})
	}
}

const fusedNodeProcessOID = "1.3.6.1.4.1.32473.1.1"

var fusedNodeProcessTables = []string{"fused_alpha", "fused_beta", "fused_gamma"}

type fusedProcessClusterManifest struct {
	Format              uint16                     `json:"format"`
	Nodes               uint8                      `json:"nodes"`
	Replicas            uint8                      `json:"replicas"`
	PhysicalNodes       uint8                      `json:"physical_nodes"`
	NodeLog             bool                       `json:"node_log"`
	ClientEndpoint      string                     `json:"client_endpoint"`
	CatalogPath         string                     `json:"catalog_path"`
	GatewayCertificate  string                     `json:"gateway_certificate"`
	GatewayKey          string                     `json:"gateway_key"`
	ClientCertificate   string                     `json:"client_certificate"`
	ClientKey           string                     `json:"client_key"`
	ClientNode          string                     `json:"client_node"`
	Roots               string                     `json:"roots"`
	AuthorizationPolicy string                     `json:"authorization_policy"`
	HotShardCapacity    string                     `json:"hot_shard_capacity"`
	ReplicaControl      string                     `json:"replica_control"`
	DurableAckKey       string                     `json:"durable_ack_key"`
	GatewayNode         string                     `json:"gateway_node"`
	GatewayControl      string                     `json:"gateway_control"`
	Members             []fusedProcessMember       `json:"members"`
	LedgerMembers       []fusedProcessMember       `json:"ledger_members"`
	DataMembers         []fusedProcessMember       `json:"data_members"`
	NodeManifests       []fusedProcessPhysicalNode `json:"node_manifests"`
}

type fusedProcessMember struct {
	Member        uint64 `json:"member"`
	Node          string `json:"node"`
	Store         string `json:"store"`
	PhysicalNode  string `json:"physical_node"`
	GroupRoot     string `json:"group_root"`
	Peer          string `json:"peer"`
	Native        string `json:"native"`
	Snapshot      string `json:"snapshot"`
	Control       string `json:"control"`
	ServeManifest string `json:"serve_manifest"`
}

type fusedProcessPhysicalNode struct {
	Node                  string   `json:"node"`
	Certificate           string   `json:"certificate"`
	Key                   string   `json:"key"`
	GatewayNode           string   `json:"gateway_node"`
	GatewayCertificate    string   `json:"gateway_certificate"`
	GatewayKey            string   `json:"gateway_key"`
	GatewayControl        string   `json:"gateway_control"`
	FrontendListen        string   `json:"frontend_listen"`
	ServeManifest         string   `json:"serve_manifest"`
	CatalogSessionJournal string   `json:"catalog_session_journal"`
	DirectIssuerJournal   string   `json:"direct_issuer_journal"`
	FallbackJournal       string   `json:"fallback_journal"`
	ExecutionPinJournal   string   `json:"execution_pin_journal"`
	Groups                []string `json:"groups"`
}

type fusedProcessNodeLog struct {
	Format          uint16 `json:"format"`
	Path            string `json:"path"`
	KeyID           string `json:"key_id"`
	KeyMaterialPath string `json:"key_material_path"`
}

type fusedProcessTLS struct {
	Certificate string `json:"certificate"`
	Key         string `json:"key"`
	Roots       string `json:"roots"`
	IdentityOID string `json:"identity_oid"`
}

type fusedProcessGateway struct {
	Listen          string          `json:"listen"`
	PGListen        string          `json:"pg_listen"`
	PGDDLSocket     string          `json:"pg_ddl_socket"`
	TLS             fusedProcessTLS `json:"tls"`
	TableCatalogs   []string        `json:"table_catalogs"`
	CatalogPath     string          `json:"catalog_path"`
	Authorization   string          `json:"authorization_policy"`
	SessionJournal  string          `json:"catalog_session_journal"`
	ReplicaManifest string          `json:"replica_control_manifest"`
	ParticipantOnly bool            `json:"control_participant_only"`
	DDLOwnerAddress string          `json:"ddl_owner_address"`
	DDLOwnerNode    string          `json:"ddl_owner_node"`
}

type fusedProcessNodeManifest struct {
	NodeLog   fusedProcessNodeLog     `json:"node_log"`
	Listeners map[string]string       `json:"listeners"`
	TLS       fusedProcessTLS         `json:"tls"`
	Gateway   *fusedProcessGateway    `json:"gateway"`
	Groups    []fusedProcessNodeGroup `json:"groups"`
}

type fusedProcessNodeGroup struct {
	WAL     fusedProcessWAL          `json:"wal"`
	SQL     fusedProcessSQL          `json:"sql"`
	Route   fusedProcessRoute        `json:"route"`
	Members []fusedProcessNodeMember `json:"members"`
}

type fusedProcessNodeMember struct {
	MemberID    uint64 `json:"member_id"`
	NodeID      string `json:"node_id"`
	PeerAddress string `json:"peer_address"`
}

type fusedProcessWAL struct {
	Path            string `json:"path"`
	KeyID           string `json:"key_id"`
	KeyMaterialPath string `json:"key_material_path"`
}

type fusedProcessSQL struct {
	Path              string `json:"path"`
	IdentityPath      string `json:"identity_path"`
	ApplyIdentityPath string `json:"apply_identity_path"`
}

type fusedProcessRoute struct {
	ClusterID             string `json:"cluster_id"`
	ClusterIncarnation    string `json:"cluster_incarnation"`
	TopologyRecoveryEpoch uint64 `json:"topology_recovery_epoch"`
	ShardIncarnation      string `json:"shard_incarnation"`
	GroupID               string `json:"group_id"`
	Distribution          string `json:"distribution"`
	Shard                 string `json:"shard"`
	AllocationGeneration  uint64 `json:"allocation_generation"`
	MemberID              uint64 `json:"member_id"`
	StoreID               string `json:"store_id"`
	MemberRoot            string `json:"member_root"`
}

type fusedLinuxProcess struct {
	PID       int
	PPID      int
	Group     int
	StartTime uint64
	State     byte
	ExitCode  int
	Argv      []string
}

type fusedTopologySnapshot struct {
	ClusterRaw        []byte
	NodeRaw           map[string][]byte
	IdentityRaw       map[string][]byte
	ApplyIdentityRaw  map[string][]byte
	NodeLogs          map[string]fusedProcessNodeLog
	StorageIdentities map[string]rafttransport.PeerIdentity
	GatewayIdentities map[string]rafttransport.PeerIdentity
	GroupRoots        map[string]map[string]string
	UserPlacements    map[string]string
}

type fusedAckedRow struct {
	ID     string
	Value  int
	Marker string
}

// This fixture owns a process group and bounds exec's pipe-drain wait. The
// generic ExternalProcess.Stop waits indefinitely after a forced kill, which
// is unsuitable when a startup failure can leave serving children holding its
// diagnostic pipes open.
type fusedSupervisorProcess struct {
	command    *exec.Cmd
	diagnostic *rf3testfixture.ProcessDiagnostic
	identity   fusedLinuxProcess
	exited     chan struct{}
	stopped    bool
}

func startFusedSupervisor(binary string, args []string) (*fusedSupervisorProcess, error) {
	process := &fusedSupervisorProcess{
		command: exec.Command(binary, args...), diagnostic: new(rf3testfixture.ProcessDiagnostic), exited: make(chan struct{}),
	}
	process.command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	process.command.WaitDelay = 2 * time.Second
	process.command.Stdout, process.command.Stderr = process.diagnostic, process.diagnostic
	if err := process.command.Start(); err != nil {
		return nil, err
	}
	identity, err := fusedReadLinuxProcess(process.command.Process.Pid)
	if err != nil {
		// Wait has not reaped the direct child yet, so its private group ID
		// cannot have been recycled while this failed-start cleanup runs.
		_ = syscall.Kill(-process.command.Process.Pid, syscall.SIGKILL)
		_ = process.command.Process.Kill()
	}
	go func() {
		_ = process.command.Wait()
		close(process.exited)
	}()
	if err != nil {
		select {
		case <-process.exited:
		case <-time.After(3 * time.Second):
		}
		return nil, fmt.Errorf("capture supervisor identity: %w", err)
	}
	process.identity = identity
	return process, nil
}

func (process *fusedSupervisorProcess) PID() int {
	select {
	case <-process.exited:
		return 0
	default:
		return process.identity.PID
	}
}

func (process *fusedSupervisorProcess) Diagnostics() string { return process.diagnostic.String() }

func (process *fusedSupervisorProcess) WaitReady(ctx context.Context, marker string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if strings.Contains(process.Diagnostics(), marker) && process.PID() != 0 {
			return nil
		}
		select {
		case <-process.exited:
			return errors.New("supervisor exited before readiness")
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

func (process *fusedSupervisorProcess) Stop(ctx context.Context) error {
	if process.stopped {
		return nil
	}
	var stopErr error
	if process.PID() != 0 {
		// Signal through os.Process's Linux pidfd, including when cleanup
		// follows a failure while the supervisor is stopped.
		_ = process.command.Process.Signal(syscall.SIGCONT)
		if err := process.command.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
			stopErr = err
		}
		select {
		case <-process.exited:
		case <-ctx.Done():
			stopErr = errors.Join(stopErr, context.Cause(ctx))
		}
	}
	// A supervisor that exits prematurely may leave descendants behind. Its
	// private process group remains reserved while any such member exists.
	all, err := fusedReadLinuxProcesses()
	if err != nil {
		stopErr = errors.Join(stopErr, err)
	}
	var remaining []fusedLinuxProcess
	for _, candidate := range all {
		if candidate.Group == process.identity.Group && candidate.StartTime >= process.identity.StartTime {
			if len(candidate.Argv) == 0 || filepath.Dir(candidate.Argv[0]) != filepath.Dir(process.command.Path) {
				stopErr = errors.Join(stopErr, fmt.Errorf("refuse cleanup of unrelated process in reused group: %+v", candidate))
				continue
			}
			remaining = append(remaining, candidate)
			if err := fusedSignalProcess(candidate, syscall.SIGKILL, true); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ESRCH) {
				stopErr = errors.Join(stopErr, err)
			}
		}
	}
	if process.PID() != 0 {
		_ = process.command.Process.Kill()
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case <-process.exited:
	case <-cleanupCtx.Done():
		return errors.Join(stopErr, errors.New("supervisor did not reap within cleanup deadline"))
	}
	stopErr = errors.Join(stopErr, waitFusedPIDsGone(cleanupCtx, remaining))
	process.stopped = stopErr == nil
	return stopErr
}

func runFusedNodeProcessCase(
	t *testing.T,
	ctx context.Context,
	vibedbBinary, shardBinary string,
	physical int,
	explicitNumber bool,
) {
	t.Helper()
	// t.TempDir includes the full subtest name, which can push the persisted
	// Unix DDL socket beyond Linux sockaddr_un's pathname limit.
	root, err := os.MkdirTemp("", "fn-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove owned fixture root: %v", err)
		}
	})
	state := filepath.Join(root, "state")
	pgCount := 1
	if explicitNumber {
		pgCount = physical
	}
	reservation, err := rf3testfixture.ReserveLoopbackAddresses(pgCount)
	if err != nil {
		t.Fatalf("reserve PostgreSQL endpoint: %v", err)
	}
	pgListens := append([]string(nil), reservation.Addresses...)
	if err := reservation.Close(); err != nil {
		t.Fatalf("release PostgreSQL endpoint: %v", err)
	}

	args := []string{
		"cluster", "dev", "--replicas", "3", "--root", state,
		"--diagnostics-on-exit", "--shard-binary", shardBinary,
	}
	if explicitNumber {
		args = append(args, "--physical-nodes", strconv.Itoa(physical), "--pg-listens", strings.Join(pgListens, ","))
	} else {
		args = append(args, "--pg-listen", pgListens[0])
	}
	start := func() *fusedSupervisorProcess {
		process, err := startFusedSupervisor(vibedbBinary, args)
		if err != nil {
			t.Fatalf("start supervisor: %v", err)
		}
		t.Cleanup(func() {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer stopCancel()
			if err := process.Stop(stopCtx); err != nil {
				t.Errorf("bounded supervisor cleanup: %v", err)
			}
			// --diagnostics-on-exit publishes child logs during Stop. Read the
			// bounded buffer afterwards so failure output includes reload errors.
			if t.Failed() {
				t.Logf("supervisor diagnostics:\n%s", process.Diagnostics())
			}
		})
		if err := process.WaitReady(ctx, "VibeDB development RF3 physical cluster ready:"); err != nil {
			t.Fatalf("physical%d startup: %v\n%s", physical, err, process.Diagnostics())
		}
		return process
	}

	process := start()
	cluster, clusterRaw := fusedReadProcessCluster(t, state)
	if cluster.PhysicalNodes != uint8(physical) || cluster.Nodes != 3 || cluster.Replicas != 3 ||
		cluster.Format != 2 || !cluster.NodeLog || len(cluster.NodeManifests) != physical {
		t.Fatalf("cluster manifest topology=%+v", cluster)
	}
	if explicitNumber && physical != 6 {
		t.Fatalf("explicit physical case has physical=%d", physical)
	}
	if !explicitNumber && physical != 3 {
		t.Fatalf("default physical case has physical=%d", physical)
	}

	assertFusedProcessLayout(t, process, shardBinary, cluster)
	for tableIndex, table := range fusedNodeProcessTables {
		address := pgListens[(tableIndex+1)%len(pgListens)]
		connection, err := fusedOpenDDLWire(ctx, address)
		if err != nil {
			t.Fatalf("open DDL frontend %s: %v", address, err)
		}
		ddl := fmt.Sprintf("CREATE TABLE %s (id TEXT PRIMARY KEY, value INTEGER NOT NULL, marker TEXT NOT NULL)", table)
		result, queryErr := fusedDDLWireQuery(ctx, connection, ddl, true)
		closeErr := connection.Close()
		if queryErr != nil || result.code != "" || result.tag != "CREATE TABLE" || closeErr != nil {
			t.Fatalf("CREATE TABLE %s through %s: result=%+v query=%v close=%v", table, address, result, queryErr, closeErr)
		}
	}

	cluster, clusterRaw = fusedReadProcessCluster(t, state)
	topologyBefore, err := inspectFusedTopology(state, cluster, clusterRaw, pgListens, fusedNodeProcessTables, physical)
	if err != nil {
		t.Fatalf("topology after live DDL: %v", err)
	}
	assertFusedUserPlacements(t, topologyBefore, physical)

	// The DDL supervisor publishes the table inventory before each shard has
	// finished its SIGHUP reload. Wait for all three empty tables to become
	// queryable so the concurrent write phase begins only after production
	// enrollment is live on the serving frontend.
	for _, address := range pgListens {
		fusedVerifyPGExact(t, ctx, address, map[string][]fusedAckedRow{})
	}
	acked := fusedConcurrentWrites(t, ctx, pgListens)
	if len(acked) != len(fusedNodeProcessTables) {
		t.Fatalf("acknowledged table groups=%d want=%d", len(acked), len(fusedNodeProcessTables))
	}
	for _, address := range pgListens {
		fusedVerifyPGExact(t, ctx, address, acked)
	}
	fusedVerifyNativeFrontends(t, ctx, cluster, acked)

	children := assertFusedProcessLayout(t, process, shardBinary, cluster)
	supervisorPID := process.PID()
	if err := process.command.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatalf("freeze supervisor pid %d before crash injection: %v", supervisorPID, err)
	}
	resumed := false
	resumeSupervisor := func() error {
		if resumed {
			return nil
		}
		err := process.command.Process.Signal(syscall.SIGCONT)
		if err == nil || errors.Is(err, os.ErrProcessDone) {
			resumed = true
			return nil
		}
		return err
	}
	// The supervisor is frozen so its child watcher cannot SIGTERM the exact
	// serving PIDs captured above before the crash injection reaches them.
	// Always resume it if any assertion below fails while it is stopped.
	t.Cleanup(func() {
		if err := resumeSupervisor(); err != nil && !errors.Is(err, syscall.ESRCH) {
			t.Logf("resume frozen supervisor pid %d during cleanup: %v", supervisorPID, err)
		}
	})
	if err := waitFusedProcessState(ctx, process.identity, 'T', nil); err != nil {
		t.Fatalf("supervisor did not enter a stopped state: %v", err)
	}
	killEvidence, err := fusedKillServingProcesses(ctx, children)
	if err != nil {
		t.Fatalf("SIGKILL serving processes: %v", err)
	}
	if err := resumeSupervisor(); err != nil && !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("resume supervisor pid %d after crash injection: %v", supervisorPID, err)
	}
	// A child exit causes the supervisor to stop its serving set. Explicitly
	// stop and reap the supervisor before starting from the retained roots.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
	stopErr := process.Stop(stopCtx)
	stopCancel()
	if stopErr != nil || process.PID() != 0 {
		t.Fatalf("supervisor remained after child SIGKILL: stop=%v diagnostics=\n%s", stopErr, process.Diagnostics())
	}
	if err := waitFusedPIDsGone(ctx, children); err != nil {
		t.Fatalf("serving processes remained after SIGKILL: %v", err)
	}
	t.Logf("physical%d requested SIGKILL for %d serving storage processes; direct=%d supervisor_stop=%v", physical, killEvidence.Requested, killEvidence.Direct, stopErr)

	process = start()
	clusterAfter, clusterRawAfter := fusedReadProcessCluster(t, state)
	assertFusedProcessLayout(t, process, shardBinary, clusterAfter)
	topologyAfter, err := inspectFusedTopology(state, clusterAfter, clusterRawAfter, pgListens, fusedNodeProcessTables, physical)
	if err != nil {
		t.Fatalf("topology after retained-root restart: %v", err)
	}
	assertFusedTopologyUnchanged(t, topologyBefore, topologyAfter)
	for _, address := range pgListens {
		fusedVerifyPGExact(t, ctx, address, acked)
	}
	fusedVerifyNativeFrontends(t, ctx, clusterAfter, acked)
	t.Logf("physical%d qualification passed: children=%d user_groups=%d acknowledged_rows=%d pg_frontends=%d retained_roots=true exact_restart_oracle=true", physical, len(children), len(acked), fusedAckCount(acked), len(pgListens))
}

func fusedReadProcessCluster(t *testing.T, root string) (fusedProcessClusterManifest, []byte) {
	t.Helper()
	path := filepath.Join(root, "cluster.vibejson")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read cluster manifest: %v", err)
	}
	var manifest fusedProcessClusterManifest
	if err := vibejson.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("decode cluster manifest: %v", err)
	}
	return manifest, raw
}

func assertFusedProcessLayout(
	t *testing.T,
	process *fusedSupervisorProcess,
	shardBinary string,
	cluster fusedProcessClusterManifest,
) []fusedLinuxProcess {
	t.Helper()
	if process == nil || process.PID() == 0 {
		t.Fatal("supervisor has no live PID")
	}
	children, err := fusedDescendants(process.PID())
	if err != nil {
		t.Fatalf("inspect supervisor process tree: %v", err)
	}
	if len(children) != int(cluster.PhysicalNodes) {
		t.Fatalf("process tree children=%d want physical=%d: %+v", len(children), cluster.PhysicalNodes, children)
	}
	want := make(map[string]bool, len(cluster.NodeManifests))
	for _, node := range cluster.NodeManifests {
		want[node.ServeManifest] = false
	}
	for _, child := range children {
		if child.PPID != process.PID() || child.State == 'Z' || child.State == 'X' {
			t.Fatalf("expected a live direct serving child: %+v", child)
		}
		if len(child.Argv) == 0 || filepath.Base(child.Argv[0]) == "vibedb-gateway" {
			t.Fatalf("separate gateway process or empty child argv: %+v", child)
		}
		if len(child.Argv) != 5 || child.Argv[0] != shardBinary || child.Argv[1] != "serve-node" ||
			child.Argv[2] != "-manifest" || child.Argv[4] != "-reload-prepared-groups" {
			t.Fatalf("unexpected physical child argv: %+v", child.Argv)
		}
		manifestPath := child.Argv[3]
		if _, found := want[manifestPath]; !found {
			t.Fatalf("child manifest %q is not in cluster node inventory; argv=%q", manifestPath, child.Argv)
		}
		if want[manifestPath] {
			t.Fatalf("duplicate serving process for manifest %q", manifestPath)
		}
		want[manifestPath] = true
	}
	for path, found := range want {
		if !found {
			t.Fatalf("node manifest has no serving process: %s", path)
		}
	}
	return children
}

func fusedConcurrentWrites(t *testing.T, ctx context.Context, addresses []string) map[string][]fusedAckedRow {
	t.Helper()
	rowsByTable := make(map[string][]fusedAckedRow, len(fusedNodeProcessTables))
	for tableIndex, table := range fusedNodeProcessTables {
		rows := make([]fusedAckedRow, 6)
		for rowIndex := range rows {
			rows[rowIndex] = fusedAckedRow{
				ID: fmt.Sprintf("%s-%02d", table, rowIndex+1), Value: (tableIndex+1)*100 + rowIndex,
				Marker: fmt.Sprintf("marker-%s-%02d", table, rowIndex+1),
			}
		}
		rowsByTable[table] = rows
	}

	type writeResult struct {
		table string
		row   fusedAckedRow
		err   error
	}
	start := make(chan struct{})
	results := make(chan writeResult, fusedAckCount(rowsByTable))
	var group sync.WaitGroup
	connections := make([]net.Conn, 0, len(fusedNodeProcessTables))
	defer func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	}()
	// Finish all fallible setup before launching workers that await start.
	for tableIndex, table := range fusedNodeProcessTables {
		connection, err := fusedOpenDDLWire(ctx, addresses[tableIndex%len(addresses)])
		if err != nil {
			t.Fatalf("open concurrent writer %s: %v", table, err)
		}
		connections = append(connections, connection)
	}
	for tableIndex, table := range fusedNodeProcessTables {
		group.Add(1)
		go func(table string, connection net.Conn) {
			defer group.Done()
			defer connection.Close()
			<-start
			for _, row := range rowsByTable[table] {
				sql := fmt.Sprintf("INSERT INTO %s (id,value,marker) VALUES ('%s',%d,'%s')", table, row.ID, row.Value, row.Marker)
				result, err := fusedDDLWireQuery(ctx, connection, sql, false)
				if err == nil && (result.code != "" || result.tag != "INSERT 0 1") {
					err = fmt.Errorf("unexpected acknowledgement: %+v", result)
				}
				results <- writeResult{table: table, row: row, err: err}
				if err != nil {
					return
				}
			}
		}(table, connections[tableIndex])
	}
	close(start)
	group.Wait()
	close(results)

	acked := make(map[string][]fusedAckedRow, len(rowsByTable))
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent write table=%s row=%s: %v", result.table, result.row.ID, result.err)
		}
		acked[result.table] = append(acked[result.table], result.row)
	}
	for _, table := range fusedNodeProcessTables {
		if len(acked[table]) != len(rowsByTable[table]) {
			t.Fatalf("table %s acknowledged rows=%d want=%d", table, len(acked[table]), len(rowsByTable[table]))
		}
		sort.Slice(acked[table], func(i, j int) bool { return acked[table][i].ID < acked[table][j].ID })
	}
	return acked
}

func fusedVerifyPGExact(
	t *testing.T,
	ctx context.Context,
	address string,
	acked map[string][]fusedAckedRow,
) {
	t.Helper()
	verifyCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	var lastErr error
	for verifyCtx.Err() == nil {
		connection, err := fusedOpenDDLWire(verifyCtx, address)
		if err != nil {
			lastErr = err
			select {
			case <-verifyCtx.Done():
				break
			case <-time.After(40 * time.Millisecond):
			}
			continue
		}
		transient, verifyErr := fusedVerifyPGPass(verifyCtx, connection, acked)
		_ = connection.Close()
		if verifyErr == nil {
			return
		}
		lastErr = verifyErr
		if !transient {
			break
		}
		select {
		case <-verifyCtx.Done():
		case <-time.After(40 * time.Millisecond):
		}
	}
	if lastErr == nil {
		lastErr = context.Cause(verifyCtx)
	}
	t.Fatalf("exact PostgreSQL oracle at %s did not settle: %v", address, lastErr)
}

func fusedVerifyPGPass(ctx context.Context, connection net.Conn, acked map[string][]fusedAckedRow) (bool, error) {
	for _, table := range fusedNodeProcessTables {
		rows := acked[table]
		result, err := fusedDDLWireQuery(ctx, connection, "SELECT COUNT(*) FROM public."+table, false)
		if err != nil {
			return true, err
		}
		if fusedPGResultTransient(result) {
			return true, fmt.Errorf("count %s: %s", table, result.message)
		}
		if result.code != "" {
			return false, fmt.Errorf("count %s: %s (%s)", table, result.message, result.code)
		}
		if len(result.rows) != 1 || len(result.rows[0]) != 1 || len(result.columns) != 1 ||
			!fusedPGNumericOID(result.columns[0]) || result.rows[0][0] != strconv.Itoa(len(rows)) {
			return false, fmt.Errorf("count %s: %+v want %d", table, result.rows, len(rows))
		}
		for _, row := range rows {
			query := fmt.Sprintf("SELECT id,value,marker FROM public.%s WHERE id='%s'", table, row.ID)
			result, err = fusedDDLWireQuery(ctx, connection, query, false)
			if err != nil {
				return true, err
			}
			if fusedPGResultTransient(result) {
				return true, fmt.Errorf("row %s/%s: %s", table, row.ID, result.message)
			}
			if result.code != "" {
				return false, fmt.Errorf("row %s/%s: %s (%s)", table, row.ID, result.message, result.code)
			}
			if len(result.rows) != 1 || len(result.rows[0]) != 3 || len(result.columns) != 3 {
				return false, fmt.Errorf("row %s/%s: columns=%v rows=%+v", table, row.ID, result.columns, result.rows)
			}
			id, idErr := fusedPGCellText(result.rows[0][0], result.columns[0])
			marker, markerErr := fusedPGCellText(result.rows[0][2], result.columns[2])
			if idErr != nil || markerErr != nil || id != row.ID || !fusedPGNumericOID(result.columns[1]) ||
				result.rows[0][1] != strconv.Itoa(row.Value) || marker != row.Marker {
				return false, fmt.Errorf("row %s/%s: %+v want id=%q value=%d marker=%q", table, row.ID, result.rows, row.ID, row.Value, row.Marker)
			}
		}
	}
	return false, nil
}

func fusedVerifyNativeFrontends(
	t *testing.T,
	ctx context.Context,
	cluster fusedProcessClusterManifest,
	acked map[string][]fusedAckedRow,
) {
	t.Helper()
	clientProfile, err := servicetls.LoadProfile(cluster.ClientCertificate, cluster.ClientKey,
		cluster.Roots, fusedNodeProcessOID, time.Now)
	if err != nil {
		t.Fatalf("load frontend verification client profile: %v", err)
	}
	for index, node := range cluster.NodeManifests {
		gatewayNode, err := fusedProcessNodeID(node.GatewayNode)
		if err != nil {
			t.Fatalf("frontend %d gateway node: %v", index+1, err)
		}
		connection, err := fusedDialGateway(ctx, clientProfile, gatewayNode, node.FrontendListen)
		if err != nil {
			t.Fatalf("dial frontend %d: %v", index+1, err)
		}
		defer connection.Close()
		stopCancellation := context.AfterFunc(ctx, func() { _ = connection.Close() })
		defer stopCancellation()
		reader := bufio.NewReaderSize(connection, 64<<10)
		for _, table := range fusedNodeProcessTables {
			for _, row := range acked[table] {
				request := rf3FixturePointRequest(table, row.ID)
				raw, err := vibejson.Marshal(&request)
				if err != nil {
					t.Fatalf("frontend request %s/%s: %v", table, row.ID, err)
				}
				deadline := time.Now().Add(15 * time.Second)
				for {
					if err := connection.SetDeadline(minFusedDeadline(ctx, deadline)); err != nil {
						t.Fatalf("frontend deadline: %v", err)
					}
					if _, err := connection.Write(append(append([]byte(nil), raw...), '\n')); err != nil {
						t.Fatalf("frontend write %d/%s/%s: %v", index+1, table, row.ID, err)
					}
					response, err := reader.ReadSlice('\n')
					if err != nil {
						t.Fatalf("frontend read %d/%s/%s: %v", index+1, table, row.ID, err)
					}
					if fusedRF3FixturePointResponseMatchesExact(response, row) {
						break
					}
					if !durableRF3ExternalRetryableResponse(response) || time.Now().After(deadline) {
						t.Fatalf("frontend exact point %d/%s/%s: %s", index+1, table, row.ID, response)
					}
					if err := fusedWaitRetry(ctx, 25*time.Millisecond); err != nil {
						t.Fatalf("frontend retry %d/%s/%s: %v", index+1, table, row.ID, err)
					}
				}
			}
		}
		_ = connection.Close()
		stopCancellation()
	}
}

func fusedDialGateway(ctx context.Context, profile *rafttransport.PeerTLS, node rafttransport.NodeID, address string) (net.Conn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	// This fixture opens one connection per frontend. A successful stream owns
	// its client until Close, so a failed assertion also releases that client.
	client, err := servicetls.NewClient(servicetls.ClientOptions{
		TLS: profile, Class: rafttransport.TrafficGatewayClient,
		Endpoints: []servicetls.Endpoint{{Address: address, Node: node}},
		Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		},
		HandshakeDeadline: func() time.Time { return minFusedDeadline(dialCtx, time.Now().Add(2*time.Second)) },
		MaxConnections:    1, MaxHandshakes: 1,
	})
	if err != nil {
		return nil, err
	}
	for {
		connection, err := client.Dial(dialCtx, address)
		if err == nil {
			return &fusedGatewayConnection{Conn: connection, client: client}, nil
		}
		if retryErr := fusedWaitRetry(dialCtx, 25*time.Millisecond); retryErr != nil {
			_ = client.Close()
			return nil, errors.Join(err, retryErr)
		}
	}
}

type fusedGatewayConnection struct {
	net.Conn
	client *servicetls.Client
}

func (connection *fusedGatewayConnection) Close() error {
	return errors.Join(connection.Conn.Close(), connection.client.Close())
}

func fusedWaitRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

func fusedRF3FixturePointResponseMatchesExact(raw []byte, row fusedAckedRow) bool {
	if !rf3FixturePointResponseMatches(raw, row.ID) {
		return false
	}
	document, err := vibejson.Parse(raw)
	if err != nil {
		return false
	}
	documentsNode, present := document.Get("documents")
	documents, valid := documentsNode.Array()
	if !present || !valid || len(documents) != 1 {
		return false
	}
	valueNode, present := documents[0].Get("value")
	value, valid := valueNode.Int64()
	if !present || !valid || value != int64(row.Value) {
		return false
	}
	markerNode, present := documents[0].Get("marker")
	marker, valid := markerNode.Text()
	return present && valid && marker == row.Marker
}

func inspectFusedTopology(
	root string,
	cluster fusedProcessClusterManifest,
	clusterRaw []byte,
	pgListens []string,
	tables []string,
	physical int,
) (fusedTopologySnapshot, error) {
	if cluster.PhysicalNodes != uint8(physical) || len(cluster.NodeManifests) != physical ||
		cluster.Nodes != 3 || cluster.Replicas != 3 || !cluster.NodeLog {
		return fusedTopologySnapshot{}, errors.New("invalid RF3 physical cluster manifest")
	}
	storageNodes := make(map[string]struct{}, physical)
	for _, node := range cluster.NodeManifests {
		if node.Node == "" || node.GatewayNode == "" || node.ServeManifest == "" || node.FrontendListen == "" {
			return fusedTopologySnapshot{}, fmt.Errorf("incomplete physical node inventory: %+v", node)
		}
		if _, duplicate := storageNodes[node.Node]; duplicate {
			return fusedTopologySnapshot{}, fmt.Errorf("duplicate storage node %q", node.Node)
		}
		if _, err := fusedProcessNodeID(node.Node); err != nil {
			return fusedTopologySnapshot{}, err
		}
		storageNodes[node.Node] = struct{}{}
	}
	for _, members := range [][]fusedProcessMember{cluster.Members, cluster.LedgerMembers, cluster.DataMembers} {
		if len(members) != 3 {
			return fusedTopologySnapshot{}, fmt.Errorf("control group RF=%d want 3", len(members))
		}
		for _, member := range members {
			if _, found := storageNodes[member.Node]; !found || member.PhysicalNode != member.Node {
				return fusedTopologySnapshot{}, fmt.Errorf("control member is not a physical storage node: %+v", member)
			}
		}
	}

	result := fusedTopologySnapshot{
		ClusterRaw: append([]byte(nil), clusterRaw...), NodeRaw: make(map[string][]byte),
		IdentityRaw: make(map[string][]byte), ApplyIdentityRaw: make(map[string][]byte),
		NodeLogs: make(map[string]fusedProcessNodeLog), StorageIdentities: make(map[string]rafttransport.PeerIdentity),
		GatewayIdentities: make(map[string]rafttransport.PeerIdentity), GroupRoots: make(map[string]map[string]string),
		UserPlacements: make(map[string]string),
	}
	seenLogPaths := make(map[string]string, physical)
	seenGatewayNodes := make(map[string]struct{}, physical)
	seenJournals := make(map[string]struct{}, physical*4)
	peerAddresses := make(map[string]string, physical)
	for _, node := range cluster.NodeManifests {
		raw, err := os.ReadFile(node.ServeManifest)
		var manifest fusedProcessNodeManifest
		if err != nil {
			return fusedTopologySnapshot{}, err
		}
		if err := vibejson.Unmarshal(raw, &manifest); err != nil {
			return fusedTopologySnapshot{}, err
		}
		peerAddresses[node.Node] = manifest.Listeners["peer"]
	}
	groupObservations := make(map[string]*fusedGroupObservation)
	tableSet := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		tableSet[table] = struct{}{}
	}
	for index, node := range cluster.NodeManifests {
		if _, duplicate := seenGatewayNodes[node.GatewayNode]; duplicate {
			return fusedTopologySnapshot{}, fmt.Errorf("duplicate gateway identity %s", node.GatewayNode)
		}
		if _, storage := storageNodes[node.GatewayNode]; storage {
			return fusedTopologySnapshot{}, fmt.Errorf("gateway identity %s is a storage identity", node.GatewayNode)
		}
		seenGatewayNodes[node.GatewayNode] = struct{}{}
		wantJournal := filepath.Join(filepath.Dir(node.ServeManifest), "gateway", "catalog-session")
		if node.CatalogSessionJournal != wantJournal || node.DirectIssuerJournal != wantJournal+".pg-writes.direct" ||
			node.FallbackJournal != wantJournal+".pg-writes" || node.ExecutionPinJournal != wantJournal+".durable-pins" {
			return fusedTopologySnapshot{}, fmt.Errorf("node %d frontend journals do not use its owned runtime paths", index+1)
		}
		for _, journal := range []string{node.CatalogSessionJournal, node.DirectIssuerJournal, node.FallbackJournal, node.ExecutionPinJournal} {
			if _, duplicate := seenJournals[journal]; journal == "" || duplicate {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d missing or shared frontend journal %q", index+1, journal)
			}
			seenJournals[journal] = struct{}{}
		}
		raw, err := os.ReadFile(node.ServeManifest)
		if err != nil {
			return fusedTopologySnapshot{}, fmt.Errorf("read node %d manifest: %w", index+1, err)
		}
		var manifest fusedProcessNodeManifest
		if err := vibejson.Unmarshal(raw, &manifest); err != nil {
			return fusedTopologySnapshot{}, fmt.Errorf("decode node %d manifest: %w", index+1, err)
		}
		// Six-node rotation assigns two groups to some nodes and four to
		// others. Each still shares one log across multiple independent groups.
		if len(manifest.Groups) < 2 || manifest.Gateway == nil {
			return fusedTopologySnapshot{}, fmt.Errorf("node %d has groups=%d gateway=%t", index+1, len(manifest.Groups), manifest.Gateway != nil)
		}
		if len(node.Groups) != len(manifest.Groups) {
			return fusedTopologySnapshot{}, fmt.Errorf("node %d inventory groups=%d manifest groups=%d", index+1, len(node.Groups), len(manifest.Groups))
		}
		inventoryGroups := make(map[string]struct{}, len(node.Groups))
		for _, groupRoot := range node.Groups {
			if groupRoot == "" {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d has empty inventory group root", index+1)
			}
			inventoryGroups[groupRoot] = struct{}{}
		}
		if manifest.NodeLog.Format != 1 || manifest.NodeLog.Path != filepath.Join(filepath.Dir(node.ServeManifest), "node-log") ||
			manifest.NodeLog.KeyMaterialPath != filepath.Join(filepath.Dir(node.ServeManifest), "node-key") || manifest.NodeLog.KeyID == "" {
			return fusedTopologySnapshot{}, fmt.Errorf("node %d node-log identity=%+v", index+1, manifest.NodeLog)
		}
		if _, err := os.Stat(manifest.NodeLog.Path); err != nil {
			return fusedTopologySnapshot{}, fmt.Errorf("node %d node-log path: %w", index+1, err)
		}
		nodeKey, err := os.ReadFile(manifest.NodeLog.KeyMaterialPath)
		if err != nil || len(nodeKey) != 32 {
			return fusedTopologySnapshot{}, fmt.Errorf("node %d node-log key: %w", index+1, err)
		}
		if prior, duplicate := seenLogPaths[manifest.NodeLog.Path]; duplicate && prior != node.Node {
			return fusedTopologySnapshot{}, fmt.Errorf("node-log path %q reused by nodes %q and %q", manifest.NodeLog.Path, prior, node.Node)
		}
		seenLogPaths[manifest.NodeLog.Path] = node.Node
		result.NodeLogs[node.Node] = manifest.NodeLog
		result.NodeRaw[node.ServeManifest] = append([]byte(nil), raw...)

		storageProfile, err := servicetls.LoadProfile(node.Certificate, node.Key, cluster.Roots, fusedNodeProcessOID, time.Now)
		if err != nil {
			return fusedTopologySnapshot{}, fmt.Errorf("node %d storage identity: %w", index+1, err)
		}
		storageIdentity := storageProfile.LocalIdentity()
		if got := hex.EncodeToString(storageIdentity.Node[:]); got != strings.ToLower(node.Node) {
			return fusedTopologySnapshot{}, fmt.Errorf("node %d storage identity=%q manifest=%q", index+1, got, node.Node)
		}
		if prior, found := result.StorageIdentities[node.Node]; found && prior != storageIdentity {
			return fusedTopologySnapshot{}, fmt.Errorf("storage identity changed for %q", node.Node)
		}
		result.StorageIdentities[node.Node] = storageIdentity

		gatewayProfile, err := servicetls.LoadProfile(node.GatewayCertificate, node.GatewayKey, cluster.Roots, fusedNodeProcessOID, time.Now)
		if err != nil {
			return fusedTopologySnapshot{}, fmt.Errorf("node %d gateway identity: %w", index+1, err)
		}
		gatewayIdentity := gatewayProfile.LocalIdentity()
		if got := hex.EncodeToString(gatewayIdentity.Node[:]); got != strings.ToLower(node.GatewayNode) || gatewayIdentity.Node == storageIdentity.Node {
			return fusedTopologySnapshot{}, fmt.Errorf("node %d gateway identity=%q manifest=%q storage=%q", index+1, got, node.GatewayNode, node.Node)
		}
		result.GatewayIdentities[node.GatewayNode] = gatewayIdentity
		if manifest.TLS != (fusedProcessTLS{Certificate: node.Certificate, Key: node.Key, Roots: cluster.Roots, IdentityOID: fusedNodeProcessOID}) {
			return fusedTopologySnapshot{}, fmt.Errorf("node %d storage TLS does not match cluster node inventory", index+1)
		}
		if manifest.Gateway.TLS.Certificate != node.GatewayCertificate || manifest.Gateway.TLS.Key != node.GatewayKey ||
			manifest.Gateway.TLS.Roots != cluster.Roots || manifest.Gateway.TLS.IdentityOID != fusedNodeProcessOID ||
			manifest.Gateway.Listen != node.FrontendListen {
			return fusedTopologySnapshot{}, fmt.Errorf("node %d embedded gateway identity/listener mismatch", index+1)
		}
		wantPG := ""
		if index < len(pgListens) {
			wantPG = pgListens[index]
		}
		if manifest.Gateway.PGListen != wantPG || manifest.Gateway.ParticipantOnly != (index != 0) ||
			manifest.Gateway.SessionJournal != node.CatalogSessionJournal {
			return fusedTopologySnapshot{}, fmt.Errorf("node %d frontend ownership/PG mismatch: %+v want=%q", index+1, manifest.Gateway, wantPG)
		}
		if wantPG != "" {
			if manifest.Gateway.PGDDLSocket != filepath.Join(filepath.Dir(cluster.NodeManifests[0].ServeManifest), "pg-ddl.sock") {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d has wrong designated DDL socket", index+1)
			}
			if index != 0 && (manifest.Gateway.DDLOwnerAddress != cluster.ClientEndpoint || manifest.Gateway.DDLOwnerNode != cluster.GatewayNode) {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d has wrong authenticated DDL owner", index+1)
			}
		}

		localGroupIDs := make(map[string]struct{}, len(manifest.Groups))
		for _, group := range manifest.Groups {
			// Enrollment can retain a group-local copy of the same key. No
			// per-group log is created when the node log owns durability.
			if _, err := os.Stat(group.WAL.Path); !errors.Is(err, os.ErrNotExist) {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d retains a per-group WAL: %v", index+1, err)
			}
			groupKey, err := os.ReadFile(group.WAL.KeyMaterialPath)
			if err != nil || group.WAL.KeyID != manifest.NodeLog.KeyID || !bytes.Equal(groupKey, nodeKey) {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d group key does not match the shared node log", index+1)
			}
			route := group.Route
			if route.GroupID == "" || route.MemberRoot == "" || route.Distribution == "" || len(group.Members) != 3 {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d invalid group route=%+v members=%d", index+1, route, len(group.Members))
			}
			if _, duplicate := localGroupIDs[route.GroupID]; duplicate {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d repeats group %q", index+1, route.GroupID)
			}
			localGroupIDs[route.GroupID] = struct{}{}
			if _, found := inventoryGroups[route.MemberRoot]; !found {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d group %s root %q is absent from node inventory", index+1, route.GroupID, route.MemberRoot)
			}
			if info, err := os.Stat(route.MemberRoot); err != nil || !info.IsDir() {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d group %s root %q: %w", index+1, route.GroupID, route.MemberRoot, err)
			}
			if _, err := fusedProcessNodeID(route.GroupID); err != nil {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d group id: %w", index+1, err)
			}
			if filepath.Dir(group.SQL.IdentityPath) != route.MemberRoot || filepath.Dir(group.SQL.ApplyIdentityPath) != route.MemberRoot ||
				group.SQL.Path != filepath.Join(route.MemberRoot, "member.vdb") {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d group root/sql mismatch route=%+v sql=%+v", index+1, route, group.SQL)
			}
			identityRaw, err := os.ReadFile(group.SQL.IdentityPath)
			if err != nil {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d group %s SQL identity: %w", index+1, route.GroupID, err)
			}
			var identity sqldriver.ReplicatedShardStoreIdentity
			if err := identity.UnmarshalJSON(identityRaw); err != nil {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d group %s decode SQL identity: %w", index+1, route.GroupID, err)
			}
			clusterID, err := fusedProcessFixed16(route.ClusterID)
			if err != nil {
				return fusedTopologySnapshot{}, err
			}
			clusterIncarnation, err := fusedProcessFixed16(route.ClusterIncarnation)
			if err != nil {
				return fusedTopologySnapshot{}, err
			}
			shardIncarnation, err := fusedProcessFixed16(route.ShardIncarnation)
			if err != nil {
				return fusedTopologySnapshot{}, err
			}
			groupID, err := fusedProcessFixed16(route.GroupID)
			if err != nil {
				return fusedTopologySnapshot{}, err
			}
			storeID, err := fusedProcessFixed16(route.StoreID)
			if err != nil {
				return fusedTopologySnapshot{}, err
			}
			if identity.Binding.ClusterID != clusterID || identity.Binding.ClusterIncarnation != clusterIncarnation ||
				identity.Binding.TopologyRecoveryEpoch != route.TopologyRecoveryEpoch || identity.Binding.ShardIncarnation != shardIncarnation ||
				identity.Binding.GroupID != groupID || identity.Binding.Distribution != route.Distribution || identity.Binding.Shard != route.Shard ||
				identity.Binding.AllocationGeneration != route.AllocationGeneration || identity.Binding.MemberID != route.MemberID || identity.Binding.StoreID != storeID {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d group %s route/identity mismatch route=%+v binding=%+v", index+1, route.GroupID, route, identity.Binding)
			}
			if identityRawPath := group.SQL.IdentityPath; result.IdentityRaw[identityRawPath] != nil && !bytes.Equal(result.IdentityRaw[identityRawPath], identityRaw) {
				return fusedTopologySnapshot{}, fmt.Errorf("SQL identity path %q changed between group observations", identityRawPath)
			}
			result.IdentityRaw[group.SQL.IdentityPath] = append([]byte(nil), identityRaw...)
			applyRaw, err := os.ReadFile(group.SQL.ApplyIdentityPath)
			if err != nil {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d group %s apply identity: %w", index+1, route.GroupID, err)
			}
			result.ApplyIdentityRaw[group.SQL.ApplyIdentityPath] = append([]byte(nil), applyRaw...)

			members := make(map[string]struct{}, len(group.Members))
			memberIDs := make(map[uint64]struct{}, len(group.Members))
			localMember := false
			for _, member := range group.Members {
				if _, duplicate := memberIDs[member.MemberID]; duplicate || member.PeerAddress == "" || member.PeerAddress != peerAddresses[member.NodeID] {
					return fusedTopologySnapshot{}, fmt.Errorf("node %d group %s inconsistent member/address: %+v", index+1, route.GroupID, member)
				}
				memberIDs[member.MemberID] = struct{}{}
				if _, found := storageNodes[member.NodeID]; !found {
					return fusedTopologySnapshot{}, fmt.Errorf("node %d group %s member is not a physical storage node: %+v", index+1, route.GroupID, member)
				}
				if _, duplicate := members[member.NodeID]; duplicate || member.MemberID == 0 {
					return fusedTopologySnapshot{}, fmt.Errorf("node %d group %s duplicate/invalid member: %+v", index+1, route.GroupID, member)
				}
				members[member.NodeID] = struct{}{}
				localMember = localMember || member.NodeID == node.Node && member.MemberID == route.MemberID
			}
			if !localMember {
				return fusedTopologySnapshot{}, fmt.Errorf("node %d group %s has no local route member", index+1, route.GroupID)
			}
			observation := groupObservations[route.GroupID]
			if observation == nil {
				observation = &fusedGroupObservation{members: make(map[string]struct{}), roots: make(map[string]string), roster: make(map[string]fusedProcessNodeMember)}
				groupObservations[route.GroupID] = observation
			}
			if observation.table != "" && observation.table != identity.UserTable || observation.distribution != "" && observation.distribution != route.Distribution {
				return fusedTopologySnapshot{}, fmt.Errorf("group %s identity changed across nodes", route.GroupID)
			}
			observation.table, observation.distribution = identity.UserTable, route.Distribution
			observation.count++
			for _, member := range group.Members {
				if previous, exists := observation.roster[member.NodeID]; exists && previous != member {
					return fusedTopologySnapshot{}, fmt.Errorf("group %s roster differs across physical nodes", route.GroupID)
				}
				observation.roster[member.NodeID] = member
			}
			for member := range members {
				observation.members[member] = struct{}{}
			}
			if prior, duplicate := observation.roots[node.Node]; duplicate && prior != route.MemberRoot {
				return fusedTopologySnapshot{}, fmt.Errorf("group %s root changed on node %s", route.GroupID, node.Node)
			}
			observation.roots[node.Node] = route.MemberRoot
		}
	}

	if len(groupObservations) != 3+len(tables) {
		return fusedTopologySnapshot{}, fmt.Errorf("logical groups=%d want=%d", len(groupObservations), 3+len(tables))
	}
	for groupID, observation := range groupObservations {
		if observation.count != 3 || len(observation.members) != 3 || len(observation.roots) != 3 {
			return fusedTopologySnapshot{}, fmt.Errorf("group %s observations=%+v", groupID, observation)
		}
		result.GroupRoots[groupID] = make(map[string]string, len(observation.roots))
		for node, root := range observation.roots {
			result.GroupRoots[groupID][node] = root
		}
		if _, isUser := tableSet[observation.table]; isUser {
			result.UserPlacements[observation.table] = fusedProcessPlacement(observation.members)
		}
	}
	if len(result.UserPlacements) != len(tables) {
		return fusedTopologySnapshot{}, fmt.Errorf("user groups=%d want=%d", len(result.UserPlacements), len(tables))
	}
	return result, nil
}

type fusedGroupObservation struct {
	table, distribution string
	count               int
	members             map[string]struct{}
	roots               map[string]string
	roster              map[string]fusedProcessNodeMember
}

func assertFusedUserPlacements(t *testing.T, topology fusedTopologySnapshot, physical int) {
	t.Helper()
	placements := make([]string, 0, len(fusedNodeProcessTables))
	union := make(map[string]struct{}, physical)
	for _, table := range fusedNodeProcessTables {
		placement, found := topology.UserPlacements[table]
		if !found {
			t.Fatalf("missing user-table placement %s", table)
		}
		members := strings.Split(placement, ",")
		if len(members) != 3 {
			t.Fatalf("user table %s RF=%d placement=%q", table, len(members), placement)
		}
		for _, member := range members {
			union[member] = struct{}{}
		}
		placements = append(placements, placement)
	}
	if len(union) != physical {
		t.Fatalf("user-table placement union=%d want physical=%d placements=%v", len(union), physical, placements)
	}
	if physical == 6 {
		for left := range placements {
			for right := left + 1; right < len(placements); right++ {
				if placements[left] == placements[right] {
					t.Fatalf("physical6 user groups %s and %s share placement %q; expected rotated overlapping RF3 subsets", fusedNodeProcessTables[left], fusedNodeProcessTables[right], placements[left])
				}
				overlaps := false
				for _, member := range strings.Split(placements[left], ",") {
					for _, other := range strings.Split(placements[right], ",") {
						overlaps = overlaps || member == other
					}
				}
				if !overlaps {
					t.Fatalf("physical6 user groups %s and %s have disjoint placements", fusedNodeProcessTables[left], fusedNodeProcessTables[right])
				}
			}
		}
	}
}

func assertFusedTopologyUnchanged(t *testing.T, before, after fusedTopologySnapshot) {
	t.Helper()
	if !bytes.Equal(before.ClusterRaw, after.ClusterRaw) {
		t.Fatal("cluster manifest changed across retained-root restart")
	}
	if !fusedByteMapsEqual(before.NodeRaw, after.NodeRaw) || !fusedByteMapsEqual(before.IdentityRaw, after.IdentityRaw) ||
		!fusedByteMapsEqual(before.ApplyIdentityRaw, after.ApplyIdentityRaw) {
		t.Fatal("node or SQL identity manifest changed across retained-root restart")
	}
	if !fusedNodeLogMapsEqual(before.NodeLogs, after.NodeLogs) || !fusedIdentityMapsEqual(before.StorageIdentities, after.StorageIdentities) ||
		!fusedIdentityMapsEqual(before.GatewayIdentities, after.GatewayIdentities) || !fusedStringSetMapsEqual(before.GroupRoots, after.GroupRoots) ||
		!fusedStringMapsEqual(before.UserPlacements, after.UserPlacements) {
		t.Fatal("node-log identity or group-root placement changed across retained-root restart")
	}
}

type fusedKillEvidence struct {
	Requested int
	Direct    int
}

func fusedKillServingProcesses(ctx context.Context, children []fusedLinuxProcess) (fusedKillEvidence, error) {
	evidence := fusedKillEvidence{Requested: len(children)}
	if len(children) == 0 {
		return evidence, errors.New("no serving processes to SIGKILL")
	}
	for _, child := range children {
		if len(child.Argv) == 0 || filepath.Base(child.Argv[0]) != "vibedb-shard" {
			return evidence, fmt.Errorf("refusing to SIGKILL non-shard child %d argv=%q", child.PID, child.Argv)
		}
		if err := fusedSignalExact(child, syscall.SIGKILL); err != nil {
			return evidence, fmt.Errorf("SIGKILL exact child %d: %w", child.PID, err)
		}
		evidence.Direct++
	}
	// The stopped supervisor cannot reap these children. Linux exposes their
	// wait status in /proc until reaping, so require actual signal-9 exits;
	// successful kill(2) alone can also describe an already-dead zombie.
	exitCode := int(syscall.SIGKILL)
	for _, child := range children {
		if err := waitFusedProcessState(ctx, child, 'Z', &exitCode); err != nil {
			return evidence, fmt.Errorf("SIGKILL exit proof for pid %d: %w", child.PID, err)
		}
	}
	return evidence, nil
}

func fusedSignalExact(process fusedLinuxProcess, signal syscall.Signal) error {
	return fusedSignalProcess(process, signal, false)
}

func fusedSignalProcess(process fusedLinuxProcess, signal syscall.Signal, cleanup bool) error {
	fd, err := unix.PidfdOpen(process.PID, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	current, err := fusedReadLinuxProcess(process.PID)
	if err != nil {
		return err
	}
	if current.StartTime != process.StartTime || current.Group != process.Group {
		return fmt.Errorf("pid %d changed identity: before=%+v current=%+v", process.PID, process, current)
	}
	// A supervisor may exit during cleanup and orphan its still-running
	// children. Their start time, private group and argv remain authoritative;
	// requiring the old PPID would leak them. Zombies only need to be reaped.
	if cleanup && (current.State == 'Z' || current.State == 'X') {
		return nil
	}
	if !cleanup && current.PPID != process.PPID || current.State == 'Z' || current.State == 'X' || !equalFusedArgv(current.Argv, process.Argv) {
		return fmt.Errorf("pid %d changed identity or was already dead: before=%+v current=%+v", process.PID, process, current)
	}
	// The pidfd closes the reuse window between identity validation and the
	// signal; signaling the numeric PID after this check would not do so.
	return unix.PidfdSendSignal(fd, signal, nil, 0)
}

func equalFusedArgv(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func waitFusedProcessState(ctx context.Context, process fusedLinuxProcess, state byte, exitCode *int) error {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := fusedReadLinuxStat(process.PID)
		if err != nil {
			return err
		}
		if current.StartTime != process.StartTime {
			return fmt.Errorf("pid %d was reused", process.PID)
		}
		if current.State == state {
			if exitCode != nil && current.ExitCode != *exitCode {
				return fmt.Errorf("pid %d exit status=%d want=%d", process.PID, current.ExitCode, *exitCode)
			}
			return nil
		}
		if current.State == 'Z' || current.State == 'X' {
			return fmt.Errorf("pid %d exited before state %q", process.PID, state)
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("pid %d did not enter state %q: %w", process.PID, state, context.Cause(waitCtx))
		case <-ticker.C:
		}
	}
}

func fusedDescendants(root int) ([]fusedLinuxProcess, error) {
	all, err := fusedReadLinuxProcesses()
	if err != nil {
		return nil, err
	}
	children := make(map[int][]fusedLinuxProcess)
	for _, process := range all {
		children[process.PPID] = append(children[process.PPID], process)
	}
	var result []fusedLinuxProcess
	queue := append([]int(nil), root)
	for len(queue) != 0 {
		parent := queue[0]
		queue = queue[1:]
		for _, child := range children[parent] {
			result = append(result, child)
			queue = append(queue, child.PID)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].PID < result[j].PID })
	return result, nil
}

func fusedReadLinuxStat(pid int) (fusedLinuxProcess, error) {
	raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return fusedLinuxProcess{}, err
	}
	// comm may itself contain spaces and parentheses. The last ')' ends it;
	// the remaining fields begin at stat field 3 (state).
	end := bytes.LastIndexByte(raw, ')')
	if end < 0 {
		return fusedLinuxProcess{}, errors.New("invalid Linux process stat")
	}
	fields := strings.Fields(string(raw[end+1:]))
	if len(fields) < 50 || len(fields[0]) != 1 {
		return fusedLinuxProcess{}, errors.New("incomplete Linux process stat")
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return fusedLinuxProcess{}, err
	}
	group, err := strconv.Atoi(fields[2])
	if err != nil {
		return fusedLinuxProcess{}, err
	}
	started, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return fusedLinuxProcess{}, err
	}
	exitCode, err := strconv.Atoi(fields[49])
	if err != nil {
		return fusedLinuxProcess{}, err
	}
	return fusedLinuxProcess{PID: pid, PPID: ppid, Group: group, StartTime: started, State: fields[0][0], ExitCode: exitCode}, nil
}

func fusedReadLinuxProcess(pid int) (fusedLinuxProcess, error) {
	before, err := fusedReadLinuxStat(pid)
	if err != nil {
		return fusedLinuxProcess{}, err
	}
	raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	if err != nil {
		return fusedLinuxProcess{}, err
	}
	after, err := fusedReadLinuxStat(pid)
	if err != nil || before.StartTime != after.StartTime {
		return fusedLinuxProcess{}, errors.Join(err, fmt.Errorf("pid %d changed during inspection", pid))
	}
	for _, part := range bytes.Split(raw, []byte{0}) {
		if len(part) != 0 {
			after.Argv = append(after.Argv, string(part))
		}
	}
	return after, nil
}

func fusedReadLinuxProcesses() ([]fusedLinuxProcess, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	result := make([]fusedLinuxProcess, 0, len(entries)/4)
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		process, err := fusedReadLinuxProcess(pid)
		if err != nil || len(process.Argv) == 0 {
			continue
		}
		result = append(result, process)
	}
	return result, nil
}

func waitFusedPIDsGone(ctx context.Context, processes []fusedLinuxProcess) error {
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		allGone := true
		for _, process := range processes {
			current, err := fusedReadLinuxStat(process.PID)
			if errors.Is(err, os.ErrNotExist) || err == nil && current.StartTime != process.StartTime {
				continue
			}
			if err != nil {
				return err
			}
			allGone = false
			break
		}
		if allGone {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("process remained past reap deadline: %w", context.Cause(waitCtx))
		case <-ticker.C:
		}
	}
}

func fusedOpenDDLWire(ctx context.Context, address string) (net.Conn, error) {
	dialCtx, cancel := context.WithDeadline(ctx, minFusedDeadline(ctx, time.Now().Add(15*time.Second)))
	defer cancel()
	connection, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", address)
	if err != nil {
		return nil, err
	}
	stopCancellation := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stopCancellation()
	deadline := minFusedDeadline(ctx, time.Now().Add(15*time.Second))
	if err := connection.SetDeadline(deadline); err != nil {
		_ = connection.Close()
		return nil, err
	}
	packet := binary.BigEndian.AppendUint32(nil, 0)
	packet = binary.BigEndian.AppendUint32(packet, 196608)
	packet = append(packet, []byte("user\x00local\x00database\x00vibedb\x00\x00")...)
	binary.BigEndian.PutUint32(packet[:4], uint32(len(packet)))
	if _, err := connection.Write(packet); err != nil {
		_ = connection.Close()
		return nil, err
	}
	result, err := fusedReadDDLWire(connection)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	if result.code != "" {
		_ = connection.Close()
		return nil, fmt.Errorf("PostgreSQL startup %s: %s", result.code, result.message)
	}
	return connection, nil
}

type fusedPGResult struct {
	tag, code, message string
	columns            []uint32
	rows               [][]string
}

func fusedDDLWireQuery(ctx context.Context, connection net.Conn, sql string, extended bool) (fusedPGResult, error) {
	if connection == nil || sql == "" {
		return fusedPGResult{}, errors.New("invalid PostgreSQL wire query")
	}
	stopCancellation := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stopCancellation()
	if err := connection.SetDeadline(minFusedDeadline(ctx, time.Now().Add(20*time.Second))); err != nil {
		return fusedPGResult{}, err
	}
	var packet []byte
	frame := func(tag byte, payload []byte) {
		packet = append(packet, tag)
		packet = binary.BigEndian.AppendUint32(packet, uint32(len(payload)+4))
		packet = append(packet, payload...)
	}
	if extended {
		parse := append([]byte{0}, sql...)
		parse = append(parse, 0, 0, 0)
		frame('P', parse)
		frame('B', []byte{0, 0, 0, 0, 0, 0, 0, 0})
		frame('D', []byte{'P', 0})
		frame('E', []byte{0, 0, 0, 0, 0})
		frame('S', nil)
	} else {
		frame('Q', append([]byte(sql), 0))
	}
	if _, err := connection.Write(packet); err != nil {
		return fusedPGResult{}, err
	}
	return fusedReadDDLWire(connection)
}

func fusedReadDDLWire(connection net.Conn) (fusedPGResult, error) {
	var result fusedPGResult
	remaining := 4 << 20
	for frames := 0; frames < 1024; frames++ {
		var header [5]byte
		if _, err := ioReadFullFused(connection, header[:]); err != nil {
			return result, err
		}
		size := int(binary.BigEndian.Uint32(header[1:])) - 4
		if size < 0 || size > remaining {
			return result, fmt.Errorf("invalid or oversized PostgreSQL wire response: %d", size)
		}
		remaining -= size
		payload := make([]byte, size)
		if _, err := ioReadFullFused(connection, payload); err != nil {
			return result, err
		}
		switch header[0] {
		case 'Z':
			return result, nil
		case 'C':
			result.tag = strings.TrimSuffix(string(payload), "\x00")
		case 'E':
			for len(payload) > 1 {
				end := bytes.IndexByte(payload[1:], 0)
				if end < 0 {
					return result, errors.New("invalid PostgreSQL ErrorResponse")
				}
				if payload[0] == 'C' {
					result.code = string(payload[1 : 1+end])
				}
				if payload[0] == 'M' {
					result.message = string(payload[1 : 1+end])
				}
				payload = payload[end+2:]
			}
		case 'T':
			if result.columns != nil || len(payload) < 2 {
				return result, errors.New("invalid or duplicate PostgreSQL RowDescription")
			}
			count := int(binary.BigEndian.Uint16(payload))
			payload = payload[2:]
			if count == 0 || count > 16 {
				return result, errors.New("unexpected PostgreSQL column count")
			}
			result.columns = make([]uint32, count)
			for index := range result.columns {
				end := bytes.IndexByte(payload, 0)
				if end < 0 || len(payload)-end-1 < 18 {
					return result, errors.New("truncated PostgreSQL RowDescription")
				}
				payload = payload[end+1:]
				result.columns[index] = binary.BigEndian.Uint32(payload[6:10])
				if binary.BigEndian.Uint16(payload[16:18]) != 0 {
					return result, errors.New("unexpected binary PostgreSQL result column")
				}
				payload = payload[18:]
			}
			if len(payload) != 0 {
				return result, errors.New("trailing PostgreSQL RowDescription data")
			}
		case 'D':
			if len(payload) < 2 {
				return result, errors.New("invalid PostgreSQL DataRow")
			}
			count := int(binary.BigEndian.Uint16(payload))
			payload = payload[2:]
			if len(result.columns) != count || count == 0 || len(result.rows) >= 32 {
				return result, errors.New("unexpected PostgreSQL row shape or count")
			}
			row := make([]string, count)
			for index := range row {
				if len(payload) < 4 {
					return result, errors.New("invalid PostgreSQL DataRow field")
				}
				length := int(int32(binary.BigEndian.Uint32(payload)))
				payload = payload[4:]
				if length < 0 {
					return result, errors.New("unexpected NULL PostgreSQL fixture result")
				}
				if length > len(payload) {
					return result, errors.New("truncated PostgreSQL DataRow")
				}
				row[index] = string(payload[:length])
				payload = payload[length:]
			}
			if len(payload) != 0 {
				return result, errors.New("trailing PostgreSQL DataRow data")
			}
			result.rows = append(result.rows, row)
		}
	}
	return result, errors.New("PostgreSQL response exceeds fixture message bound")
}

func ioReadFullFused(connection net.Conn, target []byte) (int, error) {
	read := 0
	for read < len(target) {
		n, err := connection.Read(target[read:])
		read += n
		if err != nil {
			return read, err
		}
		if n == 0 {
			return read, io.ErrNoProgress
		}
	}
	return read, nil
}

func fusedPGResultTransient(result fusedPGResult) bool {
	if result.code == "" {
		return false
	}
	message := strings.ToLower(result.message)
	return strings.Contains(message, "no reachable leader") || strings.Contains(message, "read behind") ||
		strings.Contains(message, "leader unavailable") || strings.Contains(message, "does not exist") ||
		strings.Contains(message, "table not found") || strings.Contains(message, "relation not found")
}

func fusedPGCellText(value string, oid uint32) (string, error) {
	switch oid {
	case 25: // PostgreSQL text is an unquoted byte string.
		return value, nil
	case 114: // Document-derived columns declare JSON and must encode a string.
		var text string
		if err := json.Unmarshal([]byte(value), &text); err != nil {
			return "", err
		}
		return text, nil
	default:
		return "", fmt.Errorf("unexpected PostgreSQL text column OID %d", oid)
	}
}

func fusedPGNumericOID(oid uint32) bool {
	return oid == 20 || oid == 21 || oid == 23 || oid == 114 || oid == 1700
}

func TestFusedProcessPGWireOracleAndCancellation(t *testing.T) {
	for _, testCase := range []struct {
		name, wire, want string
		oid              uint32
		invalid          bool
	}{
		{name: "json string", wire: `"fused_alpha-01"`, want: "fused_alpha-01", oid: 114},
		{name: "raw text", wire: "fused_alpha-01", want: "fused_alpha-01", oid: 25},
		{name: "text preserves quotes", wire: `"fused_alpha-01"`, want: `"fused_alpha-01"`, oid: 25},
		{name: "json requires encoding", wire: "fused_alpha-01", oid: 114, invalid: true},
		{name: "integer is not text", wire: "123", oid: 23, invalid: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := fusedPGCellText(testCase.wire, testCase.oid)
			if (err != nil) != testCase.invalid || !testCase.invalid && got != testCase.want {
				t.Fatalf("decode OID %d value %q: got=%q err=%v want=%q invalid=%t", testCase.oid, testCase.wire, got, err, testCase.want, testCase.invalid)
			}
		})
	}
	t.Run("cancellation interrupts pending query", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		completed := make(chan error, 1)
		go func() {
			_, err := fusedDDLWireQuery(ctx, client, "SELECT 1", false)
			completed <- err
		}()
		if err := server.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		var header [5]byte
		if _, err := io.ReadFull(server, header[:]); err != nil {
			t.Fatal(err)
		}
		payload := make([]byte, int(binary.BigEndian.Uint32(header[1:]))-4)
		if _, err := io.ReadFull(server, payload); err != nil {
			t.Fatal(err)
		}
		cancel()
		select {
		case err := <-completed:
			if err == nil {
				t.Fatal("cancelled pending query returned success")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("cancelled pending query remained blocked until its wire deadline")
		}
	})
}

func minFusedDeadline(ctx context.Context, candidate time.Time) time.Time {
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(candidate) {
		return deadline
	}
	return candidate
}

func fusedProcessNodeID(value string) (rafttransport.NodeID, error) {
	var result rafttransport.NodeID
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(result) {
		return result, fmt.Errorf("invalid 16-byte node identity %q", value)
	}
	copy(result[:], decoded)
	if result == (rafttransport.NodeID{}) {
		return result, fmt.Errorf("zero node identity %q", value)
	}
	return result, nil
}

func fusedProcessFixed16(value string) ([16]byte, error) {
	var result [16]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(result) {
		return result, fmt.Errorf("invalid 16-byte identity %q", value)
	}
	copy(result[:], decoded)
	if result == ([16]byte{}) {
		return result, fmt.Errorf("zero 16-byte identity %q", value)
	}
	return result, nil
}

func fusedProcessPlacement(members map[string]struct{}) string {
	values := make([]string, 0, len(members))
	for member := range members {
		values = append(values, member)
	}
	sort.Strings(values)
	return strings.Join(values, ",")
}

func fusedAckCount(acked map[string][]fusedAckedRow) int {
	total := 0
	for _, rows := range acked {
		total += len(rows)
	}
	return total
}

func fusedByteMapsEqual(left, right map[string][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if !bytes.Equal(value, right[key]) {
			return false
		}
	}
	return true
}

func fusedStringMapsEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func fusedStringSetMapsEqual(left, right map[string]map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, values := range left {
		other, found := right[key]
		if !found || len(values) != len(other) {
			return false
		}
		for nested, value := range values {
			if other[nested] != value {
				return false
			}
		}
	}
	return true
}

func fusedIdentityMapsEqual(left, right map[string]rafttransport.PeerIdentity) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func fusedNodeLogMapsEqual(left, right map[string]fusedProcessNodeLog) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}
