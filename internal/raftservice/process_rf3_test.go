//go:build darwin || linux

package raftservice_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

const (
	processHelperEnvironment   = "VIBEDB_RF3_PROCESS_HELPER"
	processMemberEnvironment   = "VIBEDB_RF3_PROCESS_MEMBER"
	processRootEnvironment     = "VIBEDB_RF3_PROCESS_ROOT"
	processPeersEnvironment    = "VIBEDB_RF3_PROCESS_PEERS"
	processRosterEnvironment   = "VIBEDB_RF3_PROCESS_ROSTER"
	processCAEnvironment       = "VIBEDB_RF3_PROCESS_CA"
	processCertEnvironment     = "VIBEDB_RF3_PROCESS_CERT"
	processKeyEnvironment      = "VIBEDB_RF3_PROCESS_CERT_KEY"
	processWALIDEnvironment    = "VIBEDB_RF3_PROCESS_WAL_KEY_ID"
	processWrappedEnvironment  = "VIBEDB_RF3_PROCESS_WAL_WRAPPED"
	processMaterialEnvironment = "VIBEDB_RF3_PROCESS_WAL_MATERIAL"

	processPeerListenerFD   = 3
	processNativeListenerFD = 4
	processControlFD        = 5
	processStatusFD         = 6
	processVoters           = 3
	processMaxStatusBytes   = 8 << 10
)

var processPeerIdentityOID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}

var errProcessStrictAllocation = errors.New("RF3 process gate requires strict durable allocation")

// TestRF3NativeServingThreeProcessRecoveryEvidence exercises real OS process
// isolation, follower catch-up, client-side response suppression, exact replay,
// and acknowledged-result survival. It deliberately does not claim a child-side
// cut between quorum/apply/socket write or natural election; those require the
// next fault-hook and membership qualification. Two fresh clusters keep every
// destructive kill within the implemented fixed-membership boundary.
func TestRF3NativeServingThreeProcessRecoveryEvidence(t *testing.T) {
	if os.Getenv(processHelperEnvironment) != "" {
		return
	}
	t.Run("follower catch-up and post-apply response loss", func(t *testing.T) {
		cluster, err := startProcessRF3Cluster(t)
		if errors.Is(err, errProcessStrictAllocation) {
			t.Skip(err)
		}
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			cluster.close(t)
			cluster.logDiagnostics(t)
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		ctx = processAuthorizedContext(t, ctx)
		leader := cluster.elect(t, ctx, 1)
		client := newFaultProcessClient(t, cluster)
		session := newProcessNativeSession(t, cluster.route(), client, 0x91,
			serviceauthz.CapabilityDataWrite)
		if _, err := session.Open(ctx, 2_000_000_000_000_000_000); err != nil {
			t.Fatalf("open native session: %v", err)
		}

		follower := uint64(1)
		if follower == leader {
			follower = 2
		}
		beforeFollower, err := cluster.probe(ctx, follower)
		if err != nil {
			t.Fatalf("probe follower before stop: %v", err)
		}
		cluster.signal(t, follower, syscall.SIGSTOP)
		firstKey := processOrderedKey(t, `"caught-up"`)
		first, err := session.Put(ctx, firstKey, []byte(`{"id":"caught-up","value":1}`))
		if err != nil {
			t.Fatalf("Put with one stopped follower: %v", err)
		}
		cluster.signal(t, follower, syscall.SIGCONT)
		cluster.waitCommittedApplied(
			t, ctx, follower, first.Outcome.AppliedIndex, beforeFollower.CheckpointApplied,
		)

		client.resetAttempts()
		leader = cluster.waitLeader(t, ctx)
		client.arm(leader, faultAfterDecodedResponseBeforeClientDelivery)
		secondKey := processOrderedKey(t, `"retry-exact"`)
		started := time.Now()
		second, err := session.Put(ctx, secondKey, []byte(`{"id":"retry-exact","value":2}`))
		if err != nil {
			t.Fatalf("Put across post-apply leader kill: %v", err)
		}
		if elapsed := time.Since(started); elapsed > 20*time.Second {
			t.Fatalf("post-apply failover took %v", elapsed)
		}
		attempts, hidden := client.snapshot()
		if len(attempts) < 2 || len(hidden) == 0 {
			t.Fatalf("proposal attempts=%d hidden completion bytes=%d", len(attempts), len(hidden))
		}
		for ordinal := 1; ordinal < len(attempts); ordinal++ {
			if !bytes.Equal(attempts[0], attempts[ordinal]) {
				t.Fatalf("retry %d changed exact command bytes", ordinal)
			}
		}
		if !bytes.Equal(hidden, second.Completion.Bytes()) {
			t.Fatal("retry returned a different deterministic completion")
		}
		if second.Completion.ResultCode != replicatedstate.ResultApplied || session.Status().Pending {
			t.Fatalf("Put result=%+v session=%+v", second, session.Status())
		}

		// Replaying the captured canonical bytes through the native executor is
		// another deterministic lookup, not a second logical application.
		replayed, err := client.executor.Propose(ctx, cluster.route(), attempts[0])
		if err != nil {
			t.Fatalf("replay acknowledged exact command: %v", err)
		}
		if !bytes.Equal(replayed.Completion, hidden) {
			t.Fatal("acknowledged replay completion changed")
		}
	})

	t.Run("acknowledged response then kill before next proposal", func(t *testing.T) {
		cluster, err := startProcessRF3Cluster(t)
		if errors.Is(err, errProcessStrictAllocation) {
			t.Skip(err)
		}
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			cluster.close(t)
			cluster.logDiagnostics(t)
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		ctx = processAuthorizedContext(t, ctx)
		leader := cluster.elect(t, ctx, 1)
		client := newFaultProcessClient(t, cluster)
		session := newProcessNativeSession(t, cluster.route(), client, 0x92,
			serviceauthz.CapabilityDataWrite)
		if _, err := session.Open(ctx, 2_000_000_000_000_000_000); err != nil {
			t.Fatalf("open native session: %v", err)
		}
		key := processOrderedKey(t, `"ack-survives"`)
		acknowledged, err := session.Put(ctx, key, []byte(`{"id":"ack-survives","value":3}`))
		if err != nil {
			t.Fatalf("acknowledged Put: %v", err)
		}
		if acknowledged.Outcome.AppliedIndex == 0 {
			t.Fatal("Put did not report a durable applied index")
		}

		client.resetAttempts()
		client.arm(leader, faultKillBeforeClientProposal)
		started := time.Now()
		deleted, err := session.Delete(ctx, key)
		if err != nil {
			t.Fatalf("Delete after pre-admission leader kill: %v", err)
		}
		if elapsed := time.Since(started); elapsed > 20*time.Second {
			t.Fatalf("bounded failover took %v", elapsed)
		}
		attempts, _ := client.snapshot()
		if len(attempts) < 2 {
			t.Fatalf("pre-admission cut produced %d proposal attempts", len(attempts))
		}
		for ordinal := 1; ordinal < len(attempts); ordinal++ {
			if !bytes.Equal(attempts[0], attempts[ordinal]) {
				t.Fatalf("retry %d changed exact Delete bytes", ordinal)
			}
		}
		if deleted.Completion.ResultCode != replicatedstate.ResultApplied || session.Status().Pending {
			t.Fatalf("Delete result=%+v session=%+v", deleted, session.Status())
		}
	})
}

// TestRF3NativeServingProcessHelper is entered only by the parent test binary.
// All authority, listener, roster, and key inputs are required environment or
// inherited descriptors; there is deliberately no production credential or
// endpoint fallback.
func TestRF3NativeServingProcessHelper(t *testing.T) {
	if os.Getenv(processHelperEnvironment) != "1" {
		return
	}
	status := os.NewFile(processStatusFD, "rf3-process-status")
	control := os.NewFile(processControlFD, "rf3-process-control")
	if status == nil || control == nil {
		t.Fatal("missing inherited process control descriptors")
	}
	defer status.Close()
	defer control.Close()
	sendStatus := func(format string, values ...any) {
		message := strings.ReplaceAll(fmt.Sprintf(format, values...), "\n", "; ")
		if len(message) > processMaxStatusBytes {
			message = message[:processMaxStatusBytes]
		}
		_, _ = fmt.Fprintln(status, message)
	}

	member, err := strconv.ParseUint(os.Getenv(processMemberEnvironment), 10, 64)
	if err != nil || member == 0 || member > processVoters {
		sendStatus("E invalid member")
		t.Fatalf("invalid process member %q", os.Getenv(processMemberEnvironment))
	}
	peerListener, err := inheritedProcessListener(processPeerListenerFD, "rf3-peer-listener")
	if err != nil {
		sendStatus("E peer listener: %v", err)
		t.Fatal(err)
	}
	nativeListener, err := inheritedProcessListener(processNativeListenerFD, "rf3-native-listener")
	if err != nil {
		_ = peerListener.Close()
		sendStatus("E native listener: %v", err)
		t.Fatal(err)
	}

	runtime, base, buildErr := buildProcessRuntime(os.Getenv(processRootEnvironment), member)
	if errors.Is(buildErr, storeio.ErrStrictAllocationUnsupported) ||
		errors.Is(buildErr, raftstore.ErrPlatformUnsupported) {
		_ = peerListener.Close()
		_ = nativeListener.Close()
		sendStatus("S %v", buildErr)
		return
	}
	if buildErr != nil {
		_ = peerListener.Close()
		_ = nativeListener.Close()
		sendStatus("E runtime: %v", buildErr)
		t.Fatal(buildErr)
	}

	peer, pulse, err := buildProcessPeer(runtime, base, peerListener, member)
	if err != nil {
		_ = runtime.Close()
		_ = nativeListener.Close()
		sendStatus("E peer: %v", err)
		t.Fatal(err)
	}
	server, err := shardservice.NewReplicatedServer(peer.Owner(), 64<<20, 15*time.Second)
	if err != nil {
		_ = runtime.Close()
		_ = nativeListener.Close()
		sendStatus("E native server: %v", err)
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	peerDone := make(chan error, 1)
	nativeDone := make(chan error, 1)
	go func() { peerDone <- peer.Run(ctx) }()
	go func() { nativeDone <- server.ServeLoopbackDevelopment(ctx, nativeListener, 64) }()
	select {
	case <-peer.Started():
	case <-time.After(15 * time.Second):
		cancel()
		sendStatus("E peer start timeout")
		t.Fatal("peer start timeout")
	}
	if !peer.Running() {
		cancel()
		sendStatus("E peer did not publish")
		t.Fatal("peer did not publish")
	}

	stopPulse := make(chan struct{})
	go processPulse(stopPulse, pulse)
	commands := make(chan byte, 1)
	controlErrors := make(chan error, 1)
	go readProcessControl(control, commands, controlErrors)
	sendStatus("R %x", runtime.Identity().RelationManifestDigest)

	running := true
	peerReturned := false
	nativeReturned := false
	for running {
		select {
		case command := <-commands:
			switch command {
			case 'C':
				campaignCtx, campaignCancel := context.WithTimeout(ctx, 10*time.Second)
				err := peer.Owner().Campaign(campaignCtx, runtime.Identity().Group)
				campaignCancel()
				if err != nil {
					sendStatus("E campaign: %v", err)
				} else {
					sendStatus("C")
				}
			case 'D':
				probeCtx, probeCancel := context.WithTimeout(ctx, 10*time.Second)
				state, err := peer.Owner().Probe(probeCtx, runtime.Identity().Group)
				probeCancel()
				if err != nil {
					sendStatus("D error=%v", err)
				} else {
					sendStatus(
						"D member=%d term=%d leader=%d commit=%d applied=%d checkpoint=%d",
						state.Status.MemberID, state.Status.Term, state.Status.LeaderID,
						state.Status.Commit, state.Status.Applied, state.Status.CheckpointApplied,
					)
				}
			case 'Q':
				sendStatus("Q")
				running = false
			default:
				sendStatus("E unknown control %d", command)
			}
		case err := <-controlErrors:
			sendStatus("E control stopped: %v", err)
			t.Errorf("process control stopped before shutdown: %v", err)
			running = false
		case err := <-peerDone:
			peerReturned = true
			if ctx.Err() == nil {
				sendStatus("E peer stopped: %v", err)
				t.Errorf("authenticated peer runtime stopped before shutdown: %v", err)
			}
			running = false
		case err := <-nativeDone:
			nativeReturned = true
			if ctx.Err() == nil {
				sendStatus("E native stopped: %v", err)
				t.Errorf("native shard server stopped before shutdown: %v", err)
			}
			running = false
		}
	}
	close(stopPulse)
	cancel()
	select {
	case <-peer.Done():
	case <-time.After(15 * time.Second):
		t.Fatal("peer runtime did not join")
	}
	if !peerReturned {
		select {
		case <-peerDone:
		case <-time.After(15 * time.Second):
			t.Fatal("authenticated peer runtime Run did not return")
		}
	}
	if !nativeReturned {
		select {
		case <-nativeDone:
		case <-time.After(15 * time.Second):
			t.Fatal("native shard server did not join")
		}
	}
}

type processCredential struct {
	certificate string
	key         string
}

type processChild struct {
	member     uint64
	command    *exec.Cmd
	control    *os.File
	statusFile *os.File
	status     *bufio.Reader
	exited     chan struct{}
	diagnostic *boundedProcessDiagnostic

	mu      sync.Mutex
	waitErr error
	paused  bool
	killed  bool
}

type processRF3Cluster struct {
	children        [processVoters]*processChild
	peerAddresses   [processVoters]string
	nativeAddresses [processVoters]string
	commandFence    raftservice.CommandFence
	routeValue      gateway.ReplicatedRoute
	closed          bool
	mu              sync.Mutex
}

func startProcessRF3Cluster(t testing.TB) (*processRF3Cluster, error) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	root := t.TempDir()
	credentials, caPath, err := writeProcessCredentials(root)
	if err != nil {
		return nil, err
	}
	cluster := &processRF3Cluster{}
	t.Cleanup(func() { cluster.close(t) })

	peerListeners := make([]*net.TCPListener, processVoters)
	nativeListeners := make([]*net.TCPListener, processVoters)
	defer func() {
		for index := 0; index < processVoters; index++ {
			if peerListeners[index] != nil {
				_ = peerListeners[index].Close()
			}
			if nativeListeners[index] != nil {
				_ = nativeListeners[index].Close()
			}
		}
	}()
	for index := 0; index < processVoters; index++ {
		peerListeners[index], err = net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			return nil, err
		}
		nativeListeners[index], err = net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			return nil, err
		}
		cluster.peerAddresses[index] = peerListeners[index].Addr().String()
		cluster.nativeAddresses[index] = nativeListeners[index].Addr().String()
	}

	roster := processRosterText()
	material := make([]byte, 32)
	for index := range material {
		material[index] = byte(index + 1)
	}
	for index := 0; index < processVoters; index++ {
		peerFile, fileErr := peerListeners[index].File()
		if fileErr != nil {
			return nil, fileErr
		}
		nativeFile, fileErr := nativeListeners[index].File()
		if fileErr != nil {
			_ = peerFile.Close()
			return nil, fileErr
		}
		controlRead, controlWrite, fileErr := os.Pipe()
		if fileErr != nil {
			_ = peerFile.Close()
			_ = nativeFile.Close()
			return nil, fileErr
		}
		statusRead, statusWrite, fileErr := os.Pipe()
		if fileErr != nil {
			_ = peerFile.Close()
			_ = nativeFile.Close()
			_ = controlRead.Close()
			_ = controlWrite.Close()
			return nil, fileErr
		}

		member := uint64(index + 1)
		command := exec.Command(executable, "-test.run=^TestRF3NativeServingProcessHelper$", "-test.v")
		command.ExtraFiles = []*os.File{peerFile, nativeFile, controlRead, statusWrite}
		// The helper receives only this gate's explicit authority and keying
		// material. It cannot accidentally inherit a developer's production
		// credentials or satisfy a missing test input from the parent process.
		command.Env = []string{
			processHelperEnvironment + "=1",
			processMemberEnvironment + "=" + strconv.FormatUint(member, 10),
			processRootEnvironment + "=" + filepath.Join(root, fmt.Sprintf("member-%d", member)),
			processPeersEnvironment + "=" + strings.Join(cluster.peerAddresses[:], ","),
			processRosterEnvironment + "=" + roster,
			processCAEnvironment + "=" + caPath,
			processCertEnvironment + "=" + credentials[index].certificate,
			processKeyEnvironment + "=" + credentials[index].key,
			processWALIDEnvironment + "=rf3-process-test-key",
			processWrappedEnvironment + "=" + hex.EncodeToString([]byte("explicit-test-wrapped-key")),
			processMaterialEnvironment + "=" + hex.EncodeToString(material),
		}
		diagnostic := newBoundedProcessDiagnostic(64 << 10)
		command.Stdout = diagnostic
		command.Stderr = diagnostic
		if err := os.MkdirAll(filepath.Join(root, fmt.Sprintf("member-%d", member)), 0o700); err != nil {
			return nil, err
		}
		if err := command.Start(); err != nil {
			return nil, err
		}
		_ = peerFile.Close()
		_ = nativeFile.Close()
		_ = controlRead.Close()
		_ = statusWrite.Close()
		_ = peerListeners[index].Close()
		peerListeners[index] = nil
		_ = nativeListeners[index].Close()
		nativeListeners[index] = nil

		child := &processChild{
			member: member, command: command, control: controlWrite,
			statusFile: statusRead, status: bufio.NewReader(statusRead),
			exited: make(chan struct{}), diagnostic: diagnostic,
		}
		cluster.children[index] = child
		go func() {
			err := command.Wait()
			child.mu.Lock()
			child.waitErr = err
			child.mu.Unlock()
			close(child.exited)
		}()
	}

	for _, child := range cluster.children {
		line, err := child.readStatus(30 * time.Second)
		if err != nil {
			return cluster, fmt.Errorf(
				"member %d readiness: %w%s", child.member, err, child.exitedDiagnostic(),
			)
		}
		switch {
		case strings.HasPrefix(line, "R "):
			encoded := line[2:]
			var digest [32]byte
			if len(encoded) != hex.EncodedLen(len(digest)) {
				return cluster, fmt.Errorf(
					"member %d readiness manifest length = %d", child.member, len(encoded),
				)
			}
			decoded, decodeErr := hex.Decode(digest[:], []byte(encoded))
			if decodeErr != nil {
				return cluster, fmt.Errorf(
					"member %d readiness manifest: %w", child.member, decodeErr,
				)
			}
			if decoded != len(digest) || digest == ([32]byte{}) {
				return cluster, fmt.Errorf(
					"member %d readiness manifest is incomplete", child.member,
				)
			}
			fence := processCommandFence(digest)
			if cluster.commandFence == (raftservice.CommandFence{}) {
				cluster.commandFence = fence
			} else if cluster.commandFence != fence {
				return cluster, fmt.Errorf(
					"member %d advertised another portable relation manifest", child.member,
				)
			}
		case strings.HasPrefix(line, "S "):
			return cluster, fmt.Errorf("%w: %s", errProcessStrictAllocation, line[2:])
		default:
			return cluster, fmt.Errorf("member %d readiness: %s", child.member, line)
		}
	}
	cluster.routeValue, err = processCatalogRoute(cluster.nativeAddresses, cluster.commandFence)
	if err != nil {
		return cluster, err
	}
	return cluster, nil
}

func (cluster *processRF3Cluster) route() gateway.ReplicatedRoute {
	route := cluster.routeValue
	route.Replicas = append([]gateway.ReplicatedEndpoint(nil), route.Replicas...)
	return route
}

func (cluster *processRF3Cluster) elect(t testing.TB, ctx context.Context, member uint64) uint64 {
	t.Helper()
	cluster.campaign(t, member)
	return cluster.waitLeader(t, ctx)
}

func (cluster *processRF3Cluster) campaign(t testing.TB, member uint64) {
	t.Helper()
	child := cluster.child(member)
	if child == nil {
		t.Fatalf("campaign missing member %d", member)
	}
	if _, err := child.control.Write([]byte{'C'}); err != nil {
		t.Fatalf("campaign member %d: %v", member, err)
	}
	line, err := child.readStatus(15 * time.Second)
	if err != nil || line != "C" {
		t.Fatalf("campaign member %d response=%q err=%v", member, line, err)
	}
}

func (cluster *processRF3Cluster) waitLeader(t testing.TB, ctx context.Context) uint64 {
	t.Helper()
	var observed [processVoters]string
	for ctx.Err() == nil {
		leader := uint64(0)
		consistent := true
		alive := 0
		for member := uint64(1); member <= processVoters; member++ {
			child := cluster.child(member)
			if child == nil || child.hasExited() || child.isPaused() {
				continue
			}
			alive++
			probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			state, err := cluster.probe(probeCtx, member)
			cancel()
			if err == nil {
				observed[member-1] = fmt.Sprintf(
					"term=%d leader=%d commit=%d applied=%d", state.Fence.Term,
					state.LeaderID, state.Commit, state.Applied,
				)
			} else {
				observed[member-1] = err.Error()
			}
			if err != nil || state.LeaderID == 0 {
				consistent = false
				break
			}
			if leader == 0 {
				leader = state.LeaderID
			} else if leader != state.LeaderID {
				consistent = false
				break
			}
		}
		if consistent && alive >= 2 && leader != 0 {
			leaderChild := cluster.child(leader)
			if leaderChild != nil && !leaderChild.hasExited() {
				probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				state, err := cluster.probe(probeCtx, leader)
				cancel()
				if err == nil && state.Fence.MemberID == state.LeaderID {
					return leader
				}
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf(
		"wait for RF3 leader: %v; observed=%q owner=%q",
		context.Cause(ctx), observed, cluster.debugOwnerStates(),
	)
	return 0
}

func (cluster *processRF3Cluster) probe(
	ctx context.Context,
	member uint64,
) (shardservice.ReplicatedMemberState, error) {
	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", cluster.nativeAddresses[member-1])
	if err != nil {
		return shardservice.ReplicatedMemberState{}, err
	}
	defer connection.Close()
	response, err := shardservice.RoundTripReplicated(ctx, connection, &shardservice.ReplicatedRequest{
		Operation: shardservice.ReplicatedProbe,
		Fence: shardservice.ReplicatedFence{
			Group: processGroup(), AllocationGeneration: 7,
		},
	})
	if err != nil {
		return shardservice.ReplicatedMemberState{}, err
	}
	if response == nil || response.Kind != shardservice.ReplicatedHandshake || !response.HasState {
		if response == nil {
			return shardservice.ReplicatedMemberState{}, shardservice.ErrReplicatedWire
		}
		return shardservice.ReplicatedMemberState{}, fmt.Errorf(
			"%w: kind=%d refusal=%d state=%t", shardservice.ErrReplicatedWire,
			response.Kind, response.Refusal, response.HasState,
		)
	}
	return response.State, nil
}

func (cluster *processRF3Cluster) waitCommittedApplied(
	t testing.TB,
	ctx context.Context,
	member, index, minimumCheckpoint uint64,
) {
	t.Helper()
	var observed string
	for ctx.Err() == nil {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		state, err := cluster.probe(probeCtx, member)
		cancel()
		if err == nil {
			observed = fmt.Sprintf(
				"term=%d leader=%d commit=%d applied=%d checkpoint=%d",
				state.Fence.Term, state.LeaderID, state.Commit, state.Applied, state.CheckpointApplied,
			)
		} else {
			observed = err.Error()
		}
		// Applied is the durable-before-visible replicated-state publication.
		// CheckpointApplied is the independently certified WAL-retention floor;
		// replay-backed machines may intentionally leave it below the newest
		// durable apply until a later checkpoint group closes.
		if err == nil && state.Commit >= index && state.Applied >= index &&
			state.CheckpointApplied >= minimumCheckpoint {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf(
		"member %d did not catch up through applied index %d: %v; observed=%s owner=%q",
		member, index, context.Cause(ctx), observed, cluster.debugOwnerStates(),
	)
}

func (cluster *processRF3Cluster) killAndElect(t testing.TB, member uint64) uint64 {
	t.Helper()
	cluster.kill(t, member)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for candidate := uint64(1); candidate <= processVoters; candidate++ {
		child := cluster.child(candidate)
		if candidate != member && child != nil && !child.hasExited() && !child.isPaused() {
			cluster.campaign(t, candidate)
			break
		}
	}
	return cluster.waitLeader(t, ctx)
}

func (cluster *processRF3Cluster) kill(t testing.TB, member uint64) {
	t.Helper()
	child := cluster.child(member)
	if child == nil || child.hasExited() {
		return
	}
	err := child.command.Process.Kill()
	if err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("kill member %d: %v", member, err)
	}
	if err == nil {
		child.mu.Lock()
		child.killed = true
		child.mu.Unlock()
	}
	select {
	case <-child.exited:
	case <-time.After(15 * time.Second):
		t.Fatalf("member %d did not exit after SIGKILL", member)
	}
}

func (cluster *processRF3Cluster) signal(t testing.TB, member uint64, signal os.Signal) {
	t.Helper()
	child := cluster.child(member)
	if child == nil || child.hasExited() {
		t.Fatalf("signal missing member %d", member)
	}
	if err := child.command.Process.Signal(signal); err != nil {
		t.Fatalf("signal member %d: %v", member, err)
	}
	child.mu.Lock()
	child.paused = signal == syscall.SIGSTOP
	child.mu.Unlock()
}

func (cluster *processRF3Cluster) close(t testing.TB) {
	t.Helper()
	if cluster == nil {
		return
	}
	cluster.mu.Lock()
	if cluster.closed {
		cluster.mu.Unlock()
		return
	}
	cluster.closed = true
	cluster.mu.Unlock()
	for _, child := range cluster.children {
		if child == nil || child.hasExited() {
			continue
		}
		_ = child.command.Process.Signal(syscall.SIGCONT)
		_, _ = child.control.Write([]byte{'Q'})
	}
	for _, child := range cluster.children {
		if child == nil {
			continue
		}
		select {
		case <-child.exited:
		case <-time.After(15 * time.Second):
			if err := child.command.Process.Kill(); err == nil {
				child.mu.Lock()
				child.killed = true
				child.mu.Unlock()
			}
			select {
			case <-child.exited:
			case <-time.After(5 * time.Second):
				t.Errorf("member %d process cleanup timed out", child.member)
			}
		}
		child.mu.Lock()
		waitErr := child.waitErr
		killed := child.killed
		child.mu.Unlock()
		if killed && waitErr == nil {
			t.Errorf("member %d exited cleanly after SIGKILL", child.member)
		}
		if !killed && waitErr != nil {
			t.Errorf("member %d process exited unsuccessfully: %v%s", child.member, waitErr, child.exitedDiagnostic())
		}
		_ = child.control.Close()
		_ = child.statusFile.Close()
	}
}

func (cluster *processRF3Cluster) logDiagnostics(t testing.TB) {
	t.Helper()
	if cluster == nil || !t.Failed() {
		return
	}
	for _, child := range cluster.children {
		if child == nil || child.diagnostic == nil {
			continue
		}
		if diagnostic := strings.TrimSpace(child.diagnostic.String()); diagnostic != "" {
			t.Logf("member %d bounded child diagnostics:\n%s", child.member, diagnostic)
		}
	}
}

func (cluster *processRF3Cluster) child(member uint64) *processChild {
	if member == 0 || member > processVoters {
		return nil
	}
	return cluster.children[member-1]
}

func (cluster *processRF3Cluster) debugOwnerStates() [processVoters]string {
	var states [processVoters]string
	for index, child := range cluster.children {
		if child == nil {
			states[index] = "not running"
			continue
		}
		if child.hasExited() {
			line, err := child.readStatus(100 * time.Millisecond)
			remaining, remainingErr := io.ReadAll(child.status)
			states[index] = fmt.Sprintf(
				"exited status=%q remaining=%q err=%v/%v%s",
				line, strings.TrimSpace(string(remaining)), err, remainingErr, child.exitedDiagnostic(),
			)
			continue
		}
		if child.isPaused() {
			states[index] = "paused"
			continue
		}
		if _, err := child.control.Write([]byte{'D'}); err != nil {
			states[index] = err.Error()
			continue
		}
		line, err := child.readStatus(12 * time.Second)
		if err != nil {
			states[index] = err.Error()
		} else {
			states[index] = line
		}
	}
	return states
}

func (child *processChild) readStatus(timeout time.Duration) (string, error) {
	if child == nil || child.statusFile == nil || child.status == nil || timeout <= 0 {
		return "", errors.New("invalid process status reader")
	}
	if err := child.statusFile.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return "", err
	}
	line, readErr := child.status.ReadString('\n')
	clearErr := child.statusFile.SetReadDeadline(time.Time{})
	return strings.TrimSpace(line), errors.Join(readErr, clearErr)
}

func (child *processChild) hasExited() bool {
	select {
	case <-child.exited:
		return true
	default:
		return false
	}
}

func (child *processChild) isPaused() bool {
	child.mu.Lock()
	paused := child.paused
	child.mu.Unlock()
	return paused
}

func (child *processChild) exitedDiagnostic() string {
	if child == nil || !child.hasExited() {
		return ""
	}
	child.mu.Lock()
	waitErr := child.waitErr
	child.mu.Unlock()
	diagnostic := ""
	if child.diagnostic != nil {
		diagnostic = strings.TrimSpace(child.diagnostic.String())
	}
	if diagnostic == "" {
		return fmt.Sprintf(" (child exit: %v)", waitErr)
	}
	return fmt.Sprintf(" (child exit: %v; output: %s)", waitErr, diagnostic)
}

type boundedProcessDiagnostic struct {
	mu       sync.Mutex
	bytes    []byte
	maxBytes int
}

func newBoundedProcessDiagnostic(maxBytes int) *boundedProcessDiagnostic {
	return &boundedProcessDiagnostic{bytes: make([]byte, 0, maxBytes), maxBytes: maxBytes}
}

func (diagnostic *boundedProcessDiagnostic) Write(source []byte) (int, error) {
	written := len(source)
	diagnostic.mu.Lock()
	defer diagnostic.mu.Unlock()
	if len(source) >= diagnostic.maxBytes {
		diagnostic.bytes = append(diagnostic.bytes[:0], source[len(source)-diagnostic.maxBytes:]...)
		return written, nil
	}
	overflow := len(diagnostic.bytes) + len(source) - diagnostic.maxBytes
	if overflow > 0 {
		copy(diagnostic.bytes, diagnostic.bytes[overflow:])
		diagnostic.bytes = diagnostic.bytes[:len(diagnostic.bytes)-overflow]
	}
	diagnostic.bytes = append(diagnostic.bytes, source...)
	return written, nil
}

func (diagnostic *boundedProcessDiagnostic) String() string {
	diagnostic.mu.Lock()
	defer diagnostic.mu.Unlock()
	return string(diagnostic.bytes)
}

type processFaultStage uint8

const (
	faultNone processFaultStage = iota
	faultKillBeforeClientProposal
	faultAfterDecodedResponseBeforeClientDelivery
)

type faultProcessClient struct {
	base     gateway.TCPReplicatedClient
	executor *gateway.ReplicatedExecutor
	cluster  *processRF3Cluster
	t        testing.TB

	mu       sync.Mutex
	stage    processFaultStage
	member   uint64
	attempts [][]byte
	hidden   []byte
}

func newFaultProcessClient(t testing.TB, cluster *processRF3Cluster) *faultProcessClient {
	t.Helper()
	client := &faultProcessClient{cluster: cluster, t: t,
		base: gateway.TCPReplicatedClient{Dial: func(
			ctx context.Context, address string,
		) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "tcp", address)
		}},
	}
	// The serving budget spans at least one configured election window while
	// remaining under ReplicatedExecutor's absolute attempt bound.
	executor, err := gateway.NewReplicatedExecutor(client, gateway.AbsoluteMaxReplicatedAttempts, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.executor = executor
	return client
}

func (client *faultProcessClient) DoReplicated(
	ctx context.Context,
	endpoint gateway.ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	address := endpoint.Address
	member := client.memberAt(address)
	isMutation := false
	if request != nil && request.Operation == shardservice.ReplicatedPropose {
		command, err := replication.OpenCommand(request.Command)
		isMutation = err == nil && command.Kind() == replication.CommandMutationBatch
		if isMutation {
			client.mu.Lock()
			client.attempts = append(client.attempts, append([]byte(nil), request.Command...))
			client.mu.Unlock()
		}
	}
	stage := faultNone
	client.mu.Lock()
	if isMutation && member == client.member && client.stage != faultNone {
		stage = client.stage
		client.stage = faultNone
	}
	client.mu.Unlock()
	if stage == faultKillBeforeClientProposal {
		client.cluster.killAndElect(client.t, member)
		return nil, &raftservice.UnknownOutcomeError{
			Command: append([]byte(nil), request.Command...), Cause: io.ErrUnexpectedEOF,
		}
	}
	response, err := client.base.DoReplicated(ctx, endpoint, request)
	if stage == faultAfterDecodedResponseBeforeClientDelivery && err == nil && response != nil &&
		response.Kind == shardservice.ReplicatedCompletion {
		client.mu.Lock()
		client.hidden = append(client.hidden[:0], response.Completion...)
		client.mu.Unlock()
		client.cluster.killAndElect(client.t, member)
		return nil, &raftservice.UnknownOutcomeError{
			Command: append([]byte(nil), request.Command...), Cause: io.ErrUnexpectedEOF,
		}
	}
	return response, err
}

func (client *faultProcessClient) memberAt(address string) uint64 {
	for index, candidate := range client.cluster.nativeAddresses {
		if candidate == address {
			return uint64(index + 1)
		}
	}
	return 0
}

func (client *faultProcessClient) arm(member uint64, stage processFaultStage) {
	client.mu.Lock()
	client.member = member
	client.stage = stage
	client.hidden = client.hidden[:0]
	client.mu.Unlock()
}

func (client *faultProcessClient) resetAttempts() {
	client.mu.Lock()
	client.attempts = client.attempts[:0]
	client.hidden = client.hidden[:0]
	client.mu.Unlock()
}

func (client *faultProcessClient) snapshot() ([][]byte, []byte) {
	client.mu.Lock()
	defer client.mu.Unlock()
	attempts := make([][]byte, len(client.attempts))
	for index := range client.attempts {
		attempts[index] = append([]byte(nil), client.attempts[index]...)
	}
	return attempts, append([]byte(nil), client.hidden...)
}

func newProcessNativeSession(
	t testing.TB,
	route gateway.ReplicatedRoute,
	client *faultProcessClient,
	clientSeed byte,
	capability serviceauthz.Capability,
) *gateway.NativeSession {
	t.Helper()
	session, err := gateway.NewNativeSession(gateway.NativeSessionOptions{
		Executor: client.executor, Route: route,
		Distribution: "orders", Shard: "0000-ffff",
		Tenant: []byte("process-test-tenant"), ClientID: replication.ID128{clientSeed},
		ProposalCapability: capability,
		Resolver:           gateway.BaseRelationResolver{Relation: 1},
		MaxRelationBatches: 4, MaxMutations: 8,
		InitialCommandBytes: 512, MaxCommandBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func processRequestAuthority() serviceauthz.Authority {
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	return authority
}

func processAuthorizedContext(t testing.TB, ctx context.Context) context.Context {
	t.Helper()
	bound, err := serviceauthz.WithAuthority(ctx, processRequestAuthority())
	if err != nil {
		t.Fatal(err)
	}
	return bound
}

func processOrderedKey(t testing.TB, source string) []byte {
	t.Helper()
	key, ok := orderedkey.AppendJSONString(nil, []byte(source), orderedkey.Ascending)
	if !ok {
		t.Fatalf("encode ordered key %q", source)
	}
	return key
}

func inheritedProcessListener(descriptor uintptr, name string) (net.Listener, error) {
	file := os.NewFile(descriptor, name)
	if file == nil {
		return nil, errors.New("missing inherited listener")
	}
	listener, err := net.FileListener(file)
	closeErr := file.Close()
	if err != nil {
		return nil, errors.Join(err, closeErr)
	}
	return listener, closeErr
}

func readProcessControl(control *os.File, commands chan<- byte, failures chan<- error) {
	var command [1]byte
	for {
		_, err := io.ReadFull(control, command[:])
		if err != nil {
			failures <- err
			return
		}
		commands <- command[0]
	}
}

func processPulse(stop <-chan struct{}, pulse chan<- struct{}) {
	// A tick is a logical Raft clock edge, not a scheduler poll. A 50 ms edge
	// leaves authenticated streams and strict WAL persistence enough wall time
	// to deliver votes before followers reach their randomized election limit,
	// including under intentionally slow CI filesystems.
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			select {
			case pulse <- struct{}{}:
			default:
			}
		}
	}
}

func buildProcessRuntime(
	root string,
	member uint64,
) (*raftmember.Runtime, sqldriver.ReplicatedShardStoreIdentity, error) {
	if root == "" || member == 0 || member > processVoters {
		return nil, sqldriver.ReplicatedShardStoreIdentity{}, errors.New("invalid process runtime identity")
	}
	identity := processStoreIdentity(member)
	key, err := processWALKeyFromEnvironment()
	if err != nil {
		return nil, sqldriver.ReplicatedShardStoreIdentity{}, err
	}
	baseIndex, baseTerm := uint64(1), uint64(1)
	wal, err := raftstore.Create(
		filepath.Join(root, "member.wal"), identity, key,
		raftstore.Bootstrap{TopologyRecoveryEpoch: 3, Snapshot: &pb.Snapshot{
			Data: []byte("rf3-process-bootstrap"),
			Metadata: &pb.SnapshotMetadata{Index: &baseIndex, Term: &baseTerm,
				ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}},
		}},
		raftstore.Options{
			MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
			MaxRecords: 4096, MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes,
		},
	)
	if err != nil {
		return nil, sqldriver.ReplicatedShardStoreIdentity{}, err
	}
	database, err := sqldriver.InitializeShardStore(
		filepath.Join(root, "member.vdb"), sqldriver.ShardStoreBinding{
			Distribution:         distribution.DistributionName(identity.Distribution),
			Shard:                distribution.ShardID(identity.Shard),
			AllocationGeneration: distribution.ShardAllocationGeneration(identity.AllocationGeneration),
		},
	)
	if err != nil {
		_ = wal.Close()
		return nil, sqldriver.ReplicatedShardStoreIdentity{}, err
	}
	closeBoth := func(cause error) (*raftmember.Runtime, sqldriver.ReplicatedShardStoreIdentity, error) {
		return nil, sqldriver.ReplicatedShardStoreIdentity{}, errors.Join(cause, database.Close(), wal.Close())
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		return closeBoth(err)
	}
	prepared, err := session.Prepare(context.Background(), `CREATE TABLE docs (PRIMARY KEY (id))`)
	if err == nil {
		_, err = prepared.Exec(context.Background(), nil)
	}
	if prepared != nil {
		err = errors.Join(err, prepared.Close())
	}
	err = errors.Join(err, session.Close())
	if err != nil {
		return closeBoth(err)
	}
	authority := processAuthority()
	base, err := raftmember.BindPreparedSQL(wal, database, authority, "docs")
	if err != nil {
		return closeBoth(err)
	}
	apply, _, err := raftmember.OpenPreparedApply(
		wal, database, authority, base, sqldriver.ReplicatedApplyOptions{
			MaxSessions: 32, RetryWindow: 8,
			TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20},
			Placement: sqldriver.ReplicatedPlacementProfile{
				Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: "/id",
				TupleVersion:  distribution.CurrentTupleVersion,
				MapperVersion: distribution.NativeMapperVersion,
				Range:         distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
			},
		},
	)
	if err != nil {
		return closeBoth(err)
	}
	bootstrap, err := wal.Snapshot()
	if err != nil {
		_ = apply.Close()
		return closeBoth(err)
	}
	if _, err := apply.InstallSnapshot(bootstrap); err != nil {
		_ = apply.Close()
		return closeBoth(err)
	}
	runtime, err := raftmember.AdoptRuntime(wal, database, apply)
	if err != nil {
		return nil, sqldriver.ReplicatedShardStoreIdentity{}, err
	}
	return runtime, base, nil
}

func buildProcessPeer(
	runtime *raftmember.Runtime,
	base sqldriver.ReplicatedShardStoreIdentity,
	listener net.Listener,
	member uint64,
) (*raftservice.AuthenticatedPeerRuntime, chan struct{}, error) {
	addresses := strings.Split(os.Getenv(processPeersEnvironment), ",")
	if len(addresses) != processVoters {
		return nil, nil, errors.New("invalid explicit peer address roster")
	}
	nodes, err := parseProcessRoster(os.Getenv(processRosterEnvironment))
	if err != nil {
		return nil, nil, err
	}
	members := make([]rafttransport.Member, processVoters)
	for index := range members {
		members[index] = rafttransport.Member{
			Group: runtime.Identity().Group, ReplicaSetVersion: 1,
			MemberID: uint64(index + 1), Node: nodes[index], Role: rafttransport.MemberVoter,
		}
	}
	registry, err := rafttransport.NewStaticRegistry(
		nodes[member-1], members, rafttransport.Limits{MaxGroups: 1, MaxMembers: processVoters},
	)
	if err != nil {
		return nil, nil, err
	}
	peerTLS, err := loadProcessPeerTLS(registry, nodes[member-1])
	if err != nil {
		return nil, nil, err
	}
	serving, err := raftserve.NewRegistry(raftserve.Limits{
		MaxGroups: 1, MaxOutstandingIdentities: 32,
		MaxOutstandingAttempts: 64, MaxWaiters: 64, MaxAttemptsPerIdentity: 4,
		MaxRetainedCompletionBytes: 32 * int64(replication.MaxEmptyResultCompletionEnvelopeBytes),
	})
	if err != nil {
		return nil, nil, err
	}
	host, err := serving.NewHost(processHostLimits())
	if err != nil {
		return nil, nil, err
	}
	if err := host.Add(runtime); err != nil {
		return nil, nil, err
	}
	pulse := make(chan struct{}, 1)
	remote := make([]rafttransport.NodeID, 0, processVoters-1)
	for index := range nodes {
		if uint64(index+1) != member {
			remote = append(remote, nodes[index])
		}
	}
	dial := func(ctx context.Context, node rafttransport.NodeID) (net.Conn, error) {
		for index, candidate := range nodes {
			if candidate == node {
				return (&net.Dialer{}).DialContext(ctx, "tcp", addresses[index])
			}
		}
		return nil, rafttransport.ErrNodeNotFound
	}
	deadline := func() time.Time { return time.Now().Add(10 * time.Second) }
	peer, err := raftservice.NewAuthenticatedPeerRuntime(raftservice.AuthenticatedPeerOptions{
		Registry: registry, TLS: peerTLS, Dial: dial, Listener: listener,
		HandshakeDeadline: deadline, MaxInboundStreams: 8,
		Owner: raftservice.Options{
			Registry: serving, Host: host,
			Members: []raftmember.RuntimeIdentity{runtime.Identity()},
			CommandFences: []raftservice.CommandFence{
				processCommandFenceFromRuntime(runtime.Identity(), base),
			},
			Pulse: pulse,
			Limits: raftservice.Limits{
				MaxIngressItems: 128, MaxIngressBytes: 64 << 20,
				MaxPendingProposalItems: 64, MaxPendingProposalBytes: 64 << 20,
				MaxPendingReadItems: 64, MaxPendingReadBytes: 64 << 20,
				MaxPendingOutboundBytes: 64 << 20,
			},
		},
		Transport: rafttransport.OrdinaryTransportOptions{
			Peers: remote,
			Queue: rafttransport.QueueLimits{
				PerPeerFrames: 32, PerPeerBytes: 4 << 20,
				GlobalFrames: 64, GlobalBytes: 8 << 20,
			},
			Coalesce: rafttransport.CoalesceLimits{
				MaxFrames: 8, MaxBytes: 1 << 20,
				RetainedBytes: rafttransport.DefaultRetainedFrameBytes,
			},
			Wait: rafttransport.WaitWithTimer,
			Backoff: func(failures uint32) time.Duration {
				return time.Duration(failures) * time.Millisecond
			},
			MaxReconnectDelay: time.Second, WriteDeadline: deadline,
			RetainedFrameBytes: rafttransport.DefaultRetainedFrameBytes,
		},
		Receiver: rafttransport.OrdinaryReceiverOptions{
			ReadDeadline: deadline, RetainedFrameBytes: rafttransport.DefaultRetainedFrameBytes,
		},
	})
	if err != nil {
		_ = host.Close()
		return nil, nil, err
	}
	return peer, pulse, nil
}

func loadProcessPeerTLS(
	registry *rafttransport.StaticRegistry,
	node rafttransport.NodeID,
) (*rafttransport.PeerTLS, error) {
	caBytes, err := os.ReadFile(os.Getenv(processCAEnvironment))
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caBytes) {
		return nil, errors.New("invalid explicit process CA")
	}
	certificate, err := tls.LoadX509KeyPair(
		os.Getenv(processCertEnvironment), os.Getenv(processKeyEnvironment),
	)
	if err != nil {
		return nil, err
	}
	return rafttransport.NewPeerTLS(rafttransport.PeerTLSOptions{
		IdentityOID: processPeerIdentityOID,
		Identity:    rafttransport.PeerIdentity{TrustDomain: registry.TrustDomain(), Node: node},
		Certificate: certificate, Roots: roots, Now: time.Now,
	})
}

func processWALKeyFromEnvironment() (raftstore.Key, error) {
	id := os.Getenv(processWALIDEnvironment)
	wrapped, err := hex.DecodeString(os.Getenv(processWrappedEnvironment))
	if err != nil || id == "" || len(wrapped) == 0 {
		return raftstore.Key{}, errors.New("invalid explicit wrapped WAL key")
	}
	material, err := hex.DecodeString(os.Getenv(processMaterialEnvironment))
	if err != nil || len(material) != 32 {
		return raftstore.Key{}, errors.New("invalid explicit WAL key material")
	}
	key := raftstore.Key{ID: id, Wrapped: wrapped}
	copy(key.Material[:], material)
	return key, nil
}

func writeProcessCredentials(root string) ([processVoters]processCredential, string, error) {
	var credentials [processVoters]processCredential
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return credentials, "", err
	}
	now := time.Now().UTC()
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "rf3-process-test-ca"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(2 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return credentials, "", err
	}
	caCertificate, err := x509.ParseCertificate(caDER)
	if err != nil {
		return credentials, "", err
	}
	caPath := filepath.Join(root, "peer-ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		return credentials, "", err
	}
	domain := rafttransport.TrustDomain{
		ClusterID: processGroup().ClusterID, ClusterIncarnation: processGroup().ClusterIncarnation,
	}
	for index := 0; index < processVoters; index++ {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return credentials, "", err
		}
		extension, err := rafttransport.PeerIdentityExtension(
			processPeerIdentityOID,
			rafttransport.PeerIdentity{TrustDomain: domain, Node: processNode(uint64(index + 1))},
		)
		if err != nil {
			return credentials, "", err
		}
		leafTemplate := &x509.Certificate{
			SerialNumber: big.NewInt(int64(index + 2)), Subject: pkix.Name{CommonName: "rf3-process-test-peer"},
			NotBefore: now.Add(-time.Hour), NotAfter: now.Add(2 * time.Hour),
			KeyUsage:        x509.KeyUsageDigitalSignature,
			ExtKeyUsage:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
			ExtraExtensions: []pkix.Extension{extension},
		}
		leafDER, err := x509.CreateCertificate(
			rand.Reader, leafTemplate, caCertificate, &key.PublicKey, caKey,
		)
		if err != nil {
			return credentials, "", err
		}
		certificatePath := filepath.Join(root, fmt.Sprintf("member-%d-cert.pem", index+1))
		certificatePEM := append(
			pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
			pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})...,
		)
		if err := os.WriteFile(certificatePath, certificatePEM, 0o600); err != nil {
			return credentials, "", err
		}
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			return credentials, "", err
		}
		keyPath := filepath.Join(root, fmt.Sprintf("member-%d-key.pem", index+1))
		if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
			return credentials, "", err
		}
		credentials[index] = processCredential{certificate: certificatePath, key: keyPath}
	}
	return credentials, caPath, nil
}

func processRosterText() string {
	parts := make([]string, processVoters)
	for index := 0; index < processVoters; index++ {
		node := processNode(uint64(index + 1))
		parts[index] = strconv.Itoa(index+1) + ":" + hex.EncodeToString(node[:])
	}
	return strings.Join(parts, ",")
}

func parseProcessRoster(source string) ([processVoters]rafttransport.NodeID, error) {
	var nodes [processVoters]rafttransport.NodeID
	parts := strings.Split(source, ",")
	if len(parts) != processVoters {
		return nodes, errors.New("invalid explicit process roster")
	}
	for index, part := range parts {
		memberAndNode := strings.Split(part, ":")
		if len(memberAndNode) != 2 || memberAndNode[0] != strconv.Itoa(index+1) {
			return nodes, errors.New("noncanonical explicit process roster")
		}
		decoded, err := hex.DecodeString(memberAndNode[1])
		if err != nil || len(decoded) != len(nodes[index]) {
			return nodes, errors.New("invalid explicit process roster node")
		}
		copy(nodes[index][:], decoded)
	}
	return nodes, nil
}

func processNode(member uint64) (node rafttransport.NodeID) {
	for index := range node {
		node[index] = byte(member*17) + byte(index)
	}
	return node
}

func processGroup() (group raftmember.GroupKey) {
	identity := processStoreIdentity(1)
	group.ClusterID = identity.ClusterID
	group.ClusterIncarnation = identity.ClusterIncarnation
	group.TopologyRecoveryEpoch = 3
	group.ShardIncarnation = identity.ShardIncarnation
	group.GroupID = identity.GroupID
	return group
}

func processStoreIdentity(member uint64) raftstore.Identity {
	identity := raftstore.Identity{
		Distribution: "orders", Shard: "0000-ffff",
		AllocationGeneration: 7, MemberID: member,
	}
	for index := range identity.ClusterID {
		identity.ClusterID[index] = byte(index + 1)
		identity.ClusterIncarnation[index] = byte(index + 21)
		identity.ShardIncarnation[index] = byte(index + 41)
		identity.GroupID[index] = byte(index + 61)
		identity.StoreID[index] = byte(index+81) ^ byte(member)
	}
	return identity
}

func processAuthority() sqldriver.ReplicatedAuthorityProfile {
	return sqldriver.ReplicatedAuthorityProfile{
		ActivePolicyGeneration: 5, ProtectionEpoch: 7, OwnershipEpoch: 11,
		SchemaGeneration: 13, RoutingVersion: 17, RouteGeneration: 19,
	}
}

func processCommandFence(manifest [32]byte) raftservice.CommandFence {
	authority := processAuthority()
	return raftservice.CommandFence{
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: authority.ActivePolicyGeneration,
		ProtectionEpoch:        authority.ProtectionEpoch, OwnershipEpoch: authority.OwnershipEpoch,
		SchemaGeneration:       authority.SchemaGeneration,
		RelationManifestDigest: manifest,
		RoutingVersion:         authority.RoutingVersion, RouteGeneration: authority.RouteGeneration,
	}
}

func processCommandFenceFromRuntime(
	runtime raftmember.RuntimeIdentity,
	base sqldriver.ReplicatedShardStoreIdentity,
) raftservice.CommandFence {
	fence := processCommandFence(runtime.RelationManifestDigest)
	authority := base.Binding.Authority
	if authority.ActivePolicyGeneration != fence.ActivePolicyGeneration ||
		authority.ProtectionEpoch != fence.ProtectionEpoch ||
		authority.OwnershipEpoch != fence.OwnershipEpoch ||
		authority.SchemaGeneration != fence.SchemaGeneration ||
		authority.RoutingVersion != fence.RoutingVersion ||
		authority.RouteGeneration != fence.RouteGeneration {
		return raftservice.CommandFence{}
	}
	return fence
}

func processHostLimits() multiraft.Limits {
	return multiraft.Limits{
		MaxGroups: 1, MaxQueueItems: 256, MaxQueueBytes: 128 << 20,
		MaxGroupItems: 256, MaxGroupBytes: 128 << 20,
		MaxOutboxItems: 256, MaxOutboxBytes: 128 << 20, MaxPendingTicks: 16,
	}
}

func processCatalogRoute(
	addresses [processVoters]string,
	command raftservice.CommandFence,
) (gateway.ReplicatedRoute, error) {
	const (
		distributionName distribution.DistributionName = "orders"
		shardID          distribution.ShardID          = "0000-ffff"
	)
	endpointIDs := [processVoters]distribution.EndpointID{"rf3-member-1", "rf3-member-2", "rf3-member-3"}
	nativeEndpointIDs := [processVoters]distribution.EndpointID{"rf3-native-1", "rf3-native-2", "rf3-native-3"}
	controlEndpointIDs := [processVoters]distribution.EndpointID{"rf3-control-1", "rf3-control-2", "rf3-control-3"}
	manifest, err := distribution.NewManifest(distributionName, 17, []distribution.Shard{{
		ID: shardID, AllocationGeneration: 7,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: endpointIDs[:], Epoch: 11,
	}})
	if err != nil {
		return gateway.ReplicatedRoute{}, err
	}
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{
			Name: distributionName, Arity: 1, MapperVersion: distribution.NativeMapperVersion,
		}},
		Placements: []distribution.TablePlacement{{
			Table: "docs", Distribution: distributionName, Columns: []string{"/id"},
		}},
		Manifests: []*distribution.Manifest{manifest},
	}
	endpoints := make(map[distribution.EndpointID]string, processVoters)
	replicas := make([]gateway.ReplicatedReplicaDescriptor, processVoters)
	for index := 0; index < processVoters; index++ {
		endpoints[endpointIDs[index]] = fmt.Sprintf("127.0.0.1:%d", 1+index)
		endpoints[nativeEndpointIDs[index]] = addresses[index]
		endpoints[controlEndpointIDs[index]] = fmt.Sprintf("127.0.0.1:%d", 101+index)
		replicas[index] = gateway.ReplicatedReplicaDescriptor{
			Member: uint64(index + 1), Node: processNode(uint64(index + 1)),
			StoreID:         processStoreIdentity(uint64(index + 1)).StoreID,
			NodeIncarnation: 1, Endpoint: endpointIDs[index],
			NativeEndpoint:  nativeEndpointIDs[index],
			ControlEndpoint: controlEndpointIDs[index],
		}
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(
		config, endpoints, 1, nil, nil, []gateway.ReplicatedShardDescriptor{{
			Distribution: distributionName, Shard: shardID,
			Group: processGroup(), AllocationGeneration: 7,
			Command: command, Replicas: replicas,
		}},
	)
	if err != nil {
		return gateway.ReplicatedRoute{}, err
	}
	var workspace [processVoters]gateway.ReplicatedEndpoint
	route, ok := snapshot.ResolveReplicatedRoute(distributionName, shardID, workspace[:0])
	if !ok || len(route.Replicas) != processVoters {
		return gateway.ReplicatedRoute{}, errors.New("exact RF3 catalog route did not resolve")
	}
	route.Replicas = append([]gateway.ReplicatedEndpoint(nil), route.Replicas...)
	return route, nil
}
