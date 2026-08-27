//go:build darwin || linux

package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3bench"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

// TestServeRF3ShippedFaultHarness exercises the actual serve-rf3 command in
// three OS processes. SIGSTOP gives the old leader an honest isolation window:
// its sockets stay open but it cannot tick, receive heartbeats, or serve. A
// byte-identical proposal is then raced with SIGKILL, retried after election,
// and checked after the killed replica has reopened and caught up.
//
// The command currently exposes no fault hook between Raft admission, quorum
// commit, apply, settlement, and socket write. Consequently the SIGKILL race
// deliberately accepts either a completion, an outcome-unknown response, or a
// lost connection, while the byte-identical retry must always settle. This is
// the strongest black-box assertion possible without weakening production with
// timing hooks; the exact internal cuts remain covered by deterministic owner,
// registry, WAL, and apply tests.
func TestServeRF3ShippedFaultHarness(t *testing.T) {
	if os.Getenv(rf3CommandHelperEnvironment) != "" {
		return
	}
	if runtime.GOOS != "linux" {
		t.Skip("external RF3 qualification requires Linux /proc RSS and strict physical WAL controls")
	}
	fixture := newRF3FaultFixture(t)
	defer fixture.close(t)
	fixture.startAll(t)
	qualification := rf3bench.Qualification{
		WALBaselineBytes:          uint64(fixture.walAllocatedBaseline),
		WALGrowthBoundBytes:       rf3bench.WALGrowthBoundBytes,
		WaiterRSSGrowthBoundBytes: rf3bench.WaiterRSSGrowthBoundBytes,
	}

	leader, states := fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
	epoch, openApplied := fixture.openSession(t, leader, states[leader])
	lastApplied := openApplied

	// Isolate the elected process without closing its established peer sockets.
	// The remaining quorum must elect, and the resumed former leader must reject
	// a linearizable read carrying its pre-isolation serving fence.
	oldFence := states[leader].Fence
	if err := fixture.children[leader].command.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	survivors := rf3FaultOtherMembers(leader)
	newLeader, _ := fixture.waitLeader(t, survivors, 30*time.Second)
	if err := fixture.children[leader].command.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	fixture.waitMemberLeader(t, leader, uint64(newLeader+1), 30*time.Second)
	response, err := fixture.roundTrip(t, leader, &shardservice.ReplicatedRequest{
		Operation:  shardservice.ReplicatedReadLeader,
		Authority:  serviceauthz.Authority{Node: fixture.nodes[(leader+1)%rf3CommandMembers], Generation: fixture.authority.ActivePolicyGeneration},
		Capability: serviceauthz.CapabilityDataRead,
		Fence:      oldFence, Relation: 1, Key: rf3FaultKey(t, "isolated-former-leader"),
		MaxValueBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("resumed former leader read: %v", err)
	}
	if response.Kind != shardservice.ReplicatedNotLeader &&
		!(response.Kind == shardservice.ReplicatedRefusal && response.Refusal == shardservice.ReplicatedRefusalStaleFence) {
		t.Fatalf("isolated former leader served linearizable read: %+v", response)
	}

	// Kill before a proposal. The dead endpoint must fail and a byte-new command
	// must settle through the newly elected live leader.
	fixture.kill(t, newLeader)
	qualification.KillBeforeRequestCuts++
	deadCtx, cancelDead := context.WithTimeout(context.Background(), 250*time.Millisecond)
	_, deadErr := fixture.roundTripContext(deadCtx, newLeader, &shardservice.ReplicatedRequest{
		Operation:  shardservice.ReplicatedProbe,
		Authority:  serviceauthz.Authority{Node: fixture.nodes[(newLeader+1)%rf3CommandMembers], Generation: fixture.authority.ActivePolicyGeneration},
		Capability: serviceauthz.CapabilityTopology,
		Fence:      shardservice.ReplicatedFence{Group: fixture.group, AllocationGeneration: rf3CommandStoreIdentity(1).AllocationGeneration},
	})
	cancelDead()
	if deadErr == nil {
		t.Fatal("killed leader accepted a request")
	}
	live := rf3FaultOtherMembers(newLeader)
	leader, states = fixture.waitLeader(t, live, 30*time.Second)
	command := fixture.mutationCommand(t, states[leader], epoch, 2, "kill-before-proposal")
	settled := fixture.propose(t, leader, states[leader], command)
	if settled.Kind != shardservice.ReplicatedCompletion {
		t.Fatalf("post-election proposal = %+v", settled)
	}
	lastApplied = settled.Outcome.AppliedIndex
	fixture.restart(t, newLeader)
	fixture.waitCaughtUp(t, newLeader, lastApplied, 30*time.Second)

	// Race the admitted-request window with SIGKILL. The first response is
	// intentionally not interpreted as the durable outcome. Only replaying the
	// exact bytes after failover may decide it.
	leader, states = fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
	raceCommand := fixture.mutationCommand(t, states[leader], epoch, 3, "admission-response-race")
	raceDone := make(chan rf3FaultRoundTrip, 1)
	go func() {
		response, roundErr := fixture.roundTripContext(context.Background(), leader,
			fixture.proposalRequest(leader, states[leader], raceCommand))
		raceDone <- rf3FaultRoundTrip{response: response, err: roundErr}
	}()
	// A short delay lets the TLS-framed request reach the shipped listener. It
	// does not claim a particular internal cut; byte-identical recovery below is
	// deliberately invariant to which side of admission won.
	time.Sleep(500 * time.Microsecond)
	fixture.kill(t, leader)
	qualification.KillAdmissionResponseCuts++
	select {
	case first := <-raceDone:
		if first.err == nil && first.response.Kind != shardservice.ReplicatedCompletion &&
			first.response.Kind != shardservice.ReplicatedOutcomeUnknown {
			t.Fatalf("kill-race response made a false definite claim: %+v", first.response)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("kill-race client remained blocked")
	}
	live = rf3FaultOtherMembers(leader)
	retryLeader, retryStates := fixture.waitLeader(t, live, 30*time.Second)
	retried := fixture.propose(t, retryLeader, retryStates[retryLeader], raceCommand)
	if retried.Kind != shardservice.ReplicatedCompletion {
		t.Fatalf("byte-identical retry did not settle: %+v", retried)
	}
	lastApplied = retried.Outcome.AppliedIndex
	fixture.restart(t, leader)
	fixture.waitCaughtUp(t, leader, lastApplied, 30*time.Second)

	// Send a complete command over the shipped TLS/native protocol, but never
	// read its response. A separate linearizable read proves the mutation was
	// applied before the response socket is discarded. Killing that leader and
	// retrying the exact command on the surviving quorum then proves recovery
	// does not depend on the lost response or the original process.
	leader, states = fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
	lostResponse := fixture.mutationCommand(t, states[leader], epoch, 4, "response-lost")
	blindConnection := fixture.sendWithoutReadingResponse(t, leader,
		fixture.proposalRequest(leader, states[leader], lostResponse))
	lostApplied := fixture.waitDocument(t, leader, states[leader], "response-lost", 30*time.Second)
	_ = blindConnection.Close()
	fixture.kill(t, leader)
	qualification.KillAfterApplyResponseCuts++
	live = rf3FaultOtherMembers(leader)
	retryLeader, retryStates = fixture.waitLeader(t, live, 30*time.Second)
	retried = fixture.propose(t, retryLeader, retryStates[retryLeader], lostResponse)
	if retried.Kind != shardservice.ReplicatedCompletion ||
		retried.Outcome.AppliedIndex < lostApplied {
		t.Fatalf("lost-response retry did not recover the durable completion: %+v", retried)
	}
	lastApplied = retried.Outcome.AppliedIndex
	fixture.restart(t, leader)
	fixture.waitCaughtUp(t, leader, lastApplied, 30*time.Second)
	qualification.LostResponseApplied = lostApplied

	// Exercise two exact one-way peer cuts. The leader->follower relay is
	// disabled while the reverse follower->leader relay remains enabled. A
	// quorum mutation must still settle, the follower is restarted under the
	// asymmetric cut, and only healing that exact direction may complete its
	// catch-up. Relay interruption/rejection counts prove the control acted on
	// an established or attempted peer connection rather than recording a
	// no-op partition.
	nextSequence := uint64(5)
	for loop := uint32(0); loop < rf3bench.RequiredAsymmetricPartitionLoops; loop++ {
		leader, states = fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
		target := (leader + 1 + int(loop)) % rf3CommandMembers
		link := fixture.links[leader][target]
		beforeRejected := link.rejectedConnections()
		cutCount := link.partition()
		command = fixture.mutationCommand(t, states[leader], epoch, nextSequence,
			fmt.Sprintf("asymmetric-partition-%d", loop+1))
		settled = fixture.propose(t, leader, states[leader], command)
		if settled.Kind != shardservice.ReplicatedCompletion {
			t.Fatalf("asymmetric partition mutation %d = %+v", loop+1, settled)
		}
		lastApplied = settled.Outcome.AppliedIndex
		fixture.kill(t, target)
		fixture.restart(t, target)
		link.waitRejectedAfter(t, beforeRejected, 5*time.Second)
		link.heal()
		fixture.waitCaughtUp(t, target, lastApplied, 30*time.Second)
		if cutCount <= beforeRejected && link.rejectedConnections() <= beforeRejected {
			t.Fatalf("asymmetric partition %d did not interrupt or reject a directional connection", loop+1)
		}
		qualification.AsymmetricRejectedConnections += link.rejectedConnections() - beforeRejected
		qualification.AsymmetricPartitionLoops++
		nextSequence++
	}

	// Repeat the complete 64-caller result-waiter pressure shape four times.
	// Every caller must finish as either a byte-identical completion or the
	// explicit admission bound, and a fresh command after every wave proves the
	// capacity was returned. Linux process RSS is sampled before and after each
	// wave; the qualification records the maximum growth rather than allowing a
	// final GC to hide a transient waiter leak.
	qualification.WaiterRSSBaselineBytes = rf3FaultProcessRSSBytes(t, fixture.children)
	qualification.WaiterRSSPeakBytes = qualification.WaiterRSSBaselineBytes
	var coalesced []byte
	for wave := uint32(0); wave < rf3bench.RequiredWaiterWaves; wave++ {
		leader, states = fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
		waveCommand := fixture.mutationCommand(t, states[leader], epoch, nextSequence,
			fmt.Sprintf("bounded-waiters-%d", wave+1))
		if coalesced == nil {
			coalesced = append([]byte(nil), waveCommand...)
		}
		const callers = int(rf3bench.RequiredWaiterCallsPerWave)
		results := make(chan rf3FaultRoundTrip, callers)
		var launch sync.WaitGroup
		launch.Add(callers)
		for range callers {
			go func() {
				defer launch.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				response, callErr := fixture.roundTripContext(ctx, leader,
					fixture.proposalRequest(leader, states[leader], waveCommand))
				results <- rf3FaultRoundTrip{response: response, err: callErr}
			}()
		}
		launch.Wait()
		close(results)
		waveCompleted := uint32(0)
		for result := range results {
			if result.err != nil {
				t.Fatalf("bounded waiter wave %d caller leaked/block-failed: %v", wave+1, result.err)
			}
			switch {
			case result.response.Kind == shardservice.ReplicatedCompletion:
				qualification.WaiterCompletions++
				waveCompleted++
			case result.response.Kind == shardservice.ReplicatedRefusal &&
				result.response.Refusal == shardservice.ReplicatedRefusalAdmissionBound:
				qualification.WaiterRefusals++
			default:
				t.Fatalf("bounded waiter wave %d result = %+v", wave+1, result.response)
			}
		}
		if waveCompleted == 0 {
			t.Fatalf("bounded waiter wave %d observed no durable completion", wave+1)
		}
		qualification.WaiterWaves++
		qualification.WaiterCalls += rf3bench.RequiredWaiterCallsPerWave
		nextSequence++
		states[leader] = fixture.probe(t, leader)
		afterBound := fixture.propose(t, leader, states[leader], fixture.mutationCommand(
			t, states[leader], epoch, nextSequence, fmt.Sprintf("waiter-capacity-reused-%d", wave+1)))
		if afterBound.Kind != shardservice.ReplicatedCompletion {
			t.Fatalf("waiter capacity wave %d was not reusable: %+v", wave+1, afterBound)
		}
		qualification.WaiterReuseCompletions++
		lastApplied = afterBound.Outcome.AppliedIndex
		fixture.waitAllApplied(t, lastApplied, 30*time.Second)
		nextSequence++
		if rss := rf3FaultProcessRSSBytes(t, fixture.children); rss > qualification.WaiterRSSPeakBytes {
			qualification.WaiterRSSPeakBytes = rss
		}
	}
	if qualification.WaiterRSSPeakBytes > qualification.WaiterRSSBaselineBytes {
		qualification.WaiterRSSGrowthBytes = qualification.WaiterRSSPeakBytes - qualification.WaiterRSSBaselineBytes
	}
	if qualification.WaiterRSSGrowthBytes > qualification.WaiterRSSGrowthBoundBytes {
		t.Fatalf("waiter-phase RSS growth %d exceeds bound %d", qualification.WaiterRSSGrowthBytes,
			qualification.WaiterRSSGrowthBoundBytes)
	}

	// The final reuse command durably acknowledges every preceding waiter wave.
	// Restart every replica, one at a time with quorum retained, then replay the
	// first waiter command. The exact
	// RetryRetired outcome proves that the acknowledgement floor survived all
	// three process lifetimes rather than existing only in a leader cache.
	for member := 0; member < rf3CommandMembers; member++ {
		fixture.kill(t, member)
		fixture.restart(t, member)
		fixture.waitCaughtUp(t, member, lastApplied, 30*time.Second)
		fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
	}
	leader, states = fixture.waitLeader(t, []int{0, 1, 2}, 30*time.Second)
	acknowledged := fixture.propose(t, leader, states[leader], coalesced)
	if acknowledged.Kind != shardservice.ReplicatedRefusal ||
		acknowledged.Refusal != shardservice.ReplicatedRefusalDeterministic ||
		acknowledged.Outcome.Code != raftserve.OutcomeRetryRetired {
		t.Fatalf("acknowledged result survived as a replayable completion: %+v", acknowledged)
	}

	allocated := rf3FaultWALAllocatedBytes(t, fixture.walPaths)
	allocatedDelta := allocated - fixture.walAllocatedBaseline
	if allocatedDelta < 0 || uint64(allocatedDelta) > rf3bench.WALGrowthBoundBytes {
		t.Fatalf("small RF3 fault run allocated %d additional WAL bytes, want <= %d",
			allocatedDelta, rf3bench.WALGrowthBoundBytes)
	}
	qualification.WALFinalBytes = uint64(allocated)
	qualification.WALGrowthBytes = uint64(allocatedDelta)
	qualification.AckOutcome = uint64(acknowledged.Outcome.Code)
	if err := qualification.Validate(); err != nil {
		t.Fatalf("RF3 qualification counters invalid: %+v: %v", qualification, err)
	}
	rf3FaultWriteQualification(t, qualification)
	t.Logf("RF3 qualification: cuts=%d/%d/%d asymmetric_loops=%d rejected_connections=%d waiter_waves=%d waiter_calls=%d waiter_completions=%d waiter_refusals=%d wal_growth_bytes=%d waiter_rss_growth_bytes=%d ack_outcome=%d",
		qualification.KillBeforeRequestCuts, qualification.KillAdmissionResponseCuts,
		qualification.KillAfterApplyResponseCuts, qualification.AsymmetricPartitionLoops,
		qualification.AsymmetricRejectedConnections, qualification.WaiterWaves, qualification.WaiterCalls,
		qualification.WaiterCompletions, qualification.WaiterRefusals, qualification.WALGrowthBytes,
		qualification.WaiterRSSGrowthBytes, qualification.AckOutcome)
}

type rf3FaultRoundTrip struct {
	response *shardservice.ReplicatedResponse
	err      error
}

func TestRF3FaultLinkCutsOneDirectionAndHeals(t *testing.T) {
	backend, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			connection, acceptErr := backend.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				_, _ = io.Copy(connection, connection)
				_ = connection.Close()
			}()
		}
	}()
	link := newRF3FaultLink(t, backend.Addr().String())
	defer func() {
		link.close()
		_ = backend.Close()
		<-done
	}()
	roundTrip := func(value byte) net.Conn {
		connection, err := net.DialTimeout("tcp", link.address(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = connection.Write([]byte{value}); err != nil {
			t.Fatal(err)
		}
		_ = connection.SetReadDeadline(time.Now().Add(time.Second))
		var response [1]byte
		if _, err = io.ReadFull(connection, response[:]); err != nil || response[0] != value {
			t.Fatalf("relay response=%x err=%v", response, err)
		}
		return connection
	}
	first := roundTrip(1)
	before := link.rejectedConnections()
	link.partition()
	_ = first.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := first.Read(make([]byte, 1)); err == nil {
		t.Fatal("partition retained established directional connection")
	}
	_ = first.Close()
	link.waitRejectedAfter(t, before, time.Second)
	link.heal()
	_ = roundTrip(2).Close()
}

// rf3FaultLink is a test-owned directional TCP relay. Each source member gets
// a distinct relay for each destination, so disabling source->destination
// cannot accidentally disable the reverse direction. This avoids production
// fault hooks and host-global firewall mutations while still closing every
// established stream at an exact external cut.
type rf3FaultLink struct {
	listener *net.TCPListener
	target   string
	enabled  atomic.Bool
	rejected atomic.Uint64
	mu       sync.Mutex
	active   map[net.Conn]net.Conn
	closed   chan struct{}
	wg       sync.WaitGroup
}

func newRF3FaultLink(t testing.TB, target string) *rf3FaultLink {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("create directional RF3 fault link: %v", err)
	}
	link := &rf3FaultLink{listener: listener, target: target, active: make(map[net.Conn]net.Conn), closed: make(chan struct{})}
	link.enabled.Store(true)
	link.wg.Add(1)
	go link.accept()
	return link
}

func (link *rf3FaultLink) address() string { return link.listener.Addr().String() }

func (link *rf3FaultLink) accept() {
	defer link.wg.Done()
	for {
		incoming, err := link.listener.Accept()
		if err != nil {
			select {
			case <-link.closed:
				return
			default:
				continue
			}
		}
		if !link.enabled.Load() {
			link.rejected.Add(1)
			_ = incoming.Close()
			continue
		}
		outgoing, err := net.DialTimeout("tcp", link.target, 3*time.Second)
		if err != nil {
			_ = incoming.Close()
			continue
		}
		link.mu.Lock()
		if !link.enabled.Load() {
			link.rejected.Add(1)
			link.mu.Unlock()
			_ = incoming.Close()
			_ = outgoing.Close()
			continue
		}
		link.active[incoming] = outgoing
		link.mu.Unlock()
		link.wg.Add(1)
		go link.relay(incoming, outgoing)
	}
}

func (link *rf3FaultLink) relay(incoming, outgoing net.Conn) {
	defer link.wg.Done()
	done := make(chan struct{}, 2)
	copyOne := func(destination, source net.Conn) {
		_, _ = io.Copy(destination, source)
		done <- struct{}{}
	}
	go copyOne(outgoing, incoming)
	go copyOne(incoming, outgoing)
	<-done
	_ = incoming.Close()
	_ = outgoing.Close()
	<-done
	link.mu.Lock()
	delete(link.active, incoming)
	link.mu.Unlock()
}

func (link *rf3FaultLink) partition() uint64 {
	link.enabled.Store(false)
	link.mu.Lock()
	interrupted := uint64(len(link.active))
	for incoming, outgoing := range link.active {
		_ = incoming.Close()
		_ = outgoing.Close()
	}
	link.mu.Unlock()
	link.rejected.Add(interrupted)
	return link.rejected.Load()
}

func (link *rf3FaultLink) heal() { link.enabled.Store(true) }

func (link *rf3FaultLink) rejectedConnections() uint64 { return link.rejected.Load() }

func (link *rf3FaultLink) waitRejectedAfter(t testing.TB, before uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if link.rejectedConnections() > before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("directional RF3 link rejected=%d want >%d", link.rejectedConnections(), before)
}

func (link *rf3FaultLink) close() {
	select {
	case <-link.closed:
		return
	default:
		close(link.closed)
	}
	_ = link.listener.Close()
	link.partition()
	link.wg.Wait()
}

type rf3FaultFixture struct {
	root                 string
	group                raftmember.GroupKey
	nodes                [rf3CommandMembers]rafttransport.NodeID
	peerAddresses        [rf3CommandMembers]string
	peerRoutes           [rf3CommandMembers][rf3CommandMembers]string
	links                [rf3CommandMembers][rf3CommandMembers]*rf3FaultLink
	nativeAddresses      [rf3CommandMembers]string
	snapshotAddresses    [rf3CommandMembers]string
	controlAddresses     [rf3CommandMembers]string
	credentials          []rf3testfixture.Credential
	roots                string
	profiles             []*rafttransport.PeerTLS
	authority            sqldriver.ReplicatedAuthorityProfile
	manifestPaths        [rf3CommandMembers]string
	walPaths             [rf3CommandMembers]string
	children             [rf3CommandMembers]*rf3CommandChild
	listeners            [rf3CommandMembers][4]*net.TCPListener
	walAllocatedBaseline int64
}

func newRF3FaultFixture(t testing.TB) *rf3FaultFixture {
	t.Helper()
	fixture := &rf3FaultFixture{root: t.TempDir(), group: rf3CommandGroup(), nodes: rf3CommandNodes(), authority: rf3CommandAuthority()}
	for member := 0; member < rf3CommandMembers; member++ {
		for lane := 0; lane < 4; lane++ {
			listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
			if err != nil {
				t.Fatal(err)
			}
			fixture.listeners[member][lane] = listener
		}
		fixture.peerAddresses[member] = fixture.listeners[member][0].Addr().String()
		fixture.controlAddresses[member] = fixture.listeners[member][1].Addr().String()
		fixture.nativeAddresses[member] = fixture.listeners[member][2].Addr().String()
		fixture.snapshotAddresses[member] = fixture.listeners[member][3].Addr().String()
	}
	for source := 0; source < rf3CommandMembers; source++ {
		for target := 0; target < rf3CommandMembers; target++ {
			if source == target {
				fixture.peerRoutes[source][target] = fixture.peerAddresses[target]
				continue
			}
			link := newRF3FaultLink(t, fixture.peerAddresses[target])
			fixture.links[source][target] = link
			fixture.peerRoutes[source][target] = link.address()
		}
	}
	credentials, roots, err := rf3testfixture.WriteCredentials(fixture.root, rf3CommandIdentityOID,
		rafttransport.TrustDomain{ClusterID: fixture.group.ClusterID, ClusterIncarnation: fixture.group.ClusterIncarnation}, fixture.nodes[:])
	if err != nil {
		t.Fatal(err)
	}
	fixture.credentials, fixture.roots = credentials, roots
	policyPath := filepath.Join(fixture.root, "authorization-policy.vibejson")
	if err = os.WriteFile(policyPath, rf3FaultPolicy(fixture.nodes), 0o600); err != nil {
		t.Fatal(err)
	}
	keyMaterial := make([]byte, 32)
	for index := range keyMaterial {
		keyMaterial[index] = byte(index + 1)
	}
	walOptions := raftstore.Options{MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
		MaxRecords: 4096, MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes}
	for member := 0; member < rf3CommandMembers; member++ {
		memberRoot := filepath.Join(fixture.root, fmt.Sprintf("member-%d", member+1))
		if err = os.MkdirAll(memberRoot, 0o700); err != nil {
			t.Fatal(err)
		}
		identity := rf3CommandStoreIdentity(uint64(member + 1))
		key := raftstore.Key{ID: "rf3-command-key", Wrapped: []byte("explicit-test-wrapped-key")}
		copy(key.Material[:], keyMaterial)
		prepared, prepareErr := rf3testfixture.PrepareMember(rf3testfixture.MemberOptions{
			Root: memberRoot, Table: gateway.ReplicatedCatalogTable,
			CreateTable: `CREATE TABLE controlplane (PRIMARY KEY (id))`, Identity: identity, Key: key,
			WAL: walOptions, Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}), Authority: fixture.authority,
			Apply: sqldriver.ReplicatedApplyOptions{MaxSessions: 32, RetryWindow: 8,
				TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20},
				Placement: sqldriver.ReplicatedPlacementProfile{Format: sqldriver.ReplicatedPlacementProfileFormat,
					ShardKey: gateway.ReplicatedCatalogPrimaryKey, TupleVersion: distribution.CurrentTupleVersion,
					MapperVersion: distribution.NativeMapperVersion, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}}})
		if errors.Is(prepareErr, storeio.ErrStrictAllocationUnsupported) || errors.Is(prepareErr, raftstore.ErrPlatformUnsupported) {
			t.Skipf("RF3 strict durable allocation unsupported: %v", prepareErr)
		}
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		prepareRF3CommandSplitRuntime(t, memberRoot)
		basePath, applyPath, keyPath := filepath.Join(memberRoot, "sql-identity.vibejson"), filepath.Join(memberRoot, "apply-identity.vibejson"), filepath.Join(memberRoot, "wal-key")
		writeRF3CommandIdentity(t, basePath, prepared.Base)
		writeRF3CommandIdentity(t, applyPath, prepared.ApplyIdentity)
		if err = os.WriteFile(keyPath, keyMaterial, 0o600); err != nil {
			t.Fatal(err)
		}
		if err = prepared.Close(); err != nil {
			t.Fatal(err)
		}
		fixture.walPaths[member] = prepared.WALPath
		fixture.manifestPaths[member] = filepath.Join(memberRoot, "serve-rf3.vibejson")
		document := rf3CommandManifestDocument(prepared.WALPath, prepared.SQLPath, basePath, applyPath, keyPath,
			fixture.peerAddresses[member], fixture.nativeAddresses[member],
			fixture.snapshotAddresses[member], fixture.controlAddresses[member], credentials[member], roots,
			policyPath, walOptions, fixture.nodes, fixture.peerRoutes[member],
			walIdentityFromBinding(prepared.Base.Binding), prepared.Base.Binding.TopologyRecoveryEpoch)
		if err = os.WriteFile(fixture.manifestPaths[member], document, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fixture.profiles = make([]*rafttransport.PeerTLS, rf3CommandMembers)
	for member := range fixture.profiles {
		fixture.profiles[member], err = servicetls.LoadProfile(credentials[member].Certificate, credentials[member].Key,
			roots, "1.3.6.1.4.1.32473.1.1", time.Now)
		if err != nil {
			t.Fatal(err)
		}
	}
	fixture.walAllocatedBaseline = rf3FaultWALAllocatedBytes(t, fixture.walPaths)
	if fixture.walAllocatedBaseline <= 0 {
		t.Fatal("RF3 WAL allocation baseline has no physical blocks")
	}
	return fixture
}

func (fixture *rf3FaultFixture) startAll(t testing.TB) {
	for member := 0; member < rf3CommandMembers; member++ {
		fixture.start(t, member)
	}
	for member := 0; member < rf3CommandMembers; member++ {
		waitRF3CommandReady(t, fixture.children[member], 30*time.Second)
	}
}

func (fixture *rf3FaultFixture) start(t testing.TB, member int) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := &rf3CommandDiagnostic{maximum: rf3CommandDiagnosticBytes}
	command := exec.Command(executable, "-test.run=^TestServeRF3CommandProcessHelper$", "-test.v")
	command.Env = append(os.Environ(), rf3CommandHelperEnvironment+"=1", rf3CommandManifestEnvironment+"="+fixture.manifestPaths[member])
	files := make([]*os.File, 4)
	for lane := range files {
		files[lane], err = fixture.listeners[member][lane].File()
		if err != nil {
			t.Fatal(err)
		}
	}
	command.ExtraFiles, command.Stdout, command.Stderr = files, diagnostic, diagnostic
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	for lane := range files {
		_ = files[lane].Close()
		_ = fixture.listeners[member][lane].Close()
		fixture.listeners[member][lane] = nil
	}
	child := &rf3CommandChild{member: uint64(member + 1), command: command, exited: make(chan struct{}), diagnostic: diagnostic}
	fixture.children[member] = child
	go func() {
		waitErr := command.Wait()
		child.mu.Lock()
		child.waitErr = waitErr
		child.mu.Unlock()
		close(child.exited)
	}()
}

func (fixture *rf3FaultFixture) kill(t testing.TB, member int) {
	t.Helper()
	child := fixture.children[member]
	if err := child.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-child.exited:
	case <-time.After(10 * time.Second):
		t.Fatalf("member %d did not die", member+1)
	}
}

func (fixture *rf3FaultFixture) restart(t testing.TB, member int) {
	t.Helper()
	addresses := [4]string{
		fixture.peerAddresses[member], fixture.controlAddresses[member],
		fixture.nativeAddresses[member], fixture.snapshotAddresses[member],
	}
	for lane, address := range addresses {
		listener, err := net.ListenTCP("tcp", mustRF3FaultTCPAddress(t, address))
		if err != nil {
			t.Fatalf("rebind member %d lane %d: %v", member+1, lane, err)
		}
		fixture.listeners[member][lane] = listener
	}
	fixture.start(t, member)
	waitRF3CommandReady(t, fixture.children[member], 30*time.Second)
}

func (fixture *rf3FaultFixture) close(t testing.TB) {
	children := make([]*rf3CommandChild, rf3CommandMembers)
	for member := range fixture.children {
		children[member] = fixture.children[member]
		if fixture.children[member] != nil {
			// SIGCONT is harmless for a running process and prevents a failed
			// isolation assertion from leaving cleanup blocked behind SIGSTOP.
			_ = fixture.children[member].command.Process.Signal(syscall.SIGCONT)
		}
	}
	closeRF3CommandChildren(t, children)
	for member := range fixture.listeners {
		for lane := range fixture.listeners[member] {
			if fixture.listeners[member][lane] != nil {
				_ = fixture.listeners[member][lane].Close()
			}
		}
	}
	for source := range fixture.links {
		for target := range fixture.links[source] {
			if fixture.links[source][target] != nil {
				fixture.links[source][target].close()
			}
		}
	}
}

func (fixture *rf3FaultFixture) waitLeader(t testing.TB, members []int, timeout time.Duration) (int, map[int]shardservice.ReplicatedMemberState) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		states := make(map[int]shardservice.ReplicatedMemberState, len(members))
		leader := uint64(0)
		consistent := true
		for _, member := range members {
			state, err := fixture.tryProbe(member, time.Second)
			if err != nil || state.LeaderID == 0 || leader != 0 && state.LeaderID != leader {
				consistent = false
				break
			}
			states[member] = state
			leader = state.LeaderID
		}
		if consistent && leader != 0 {
			return int(leader - 1), states
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("RF3 leader unavailable for members %v", members)
	return 0, nil
}

func (fixture *rf3FaultFixture) waitMemberLeader(t testing.TB, member int, leader uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state, err := fixture.tryProbe(member, time.Second)
		if err == nil && state.LeaderID == leader {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("member %d did not observe leader %d", member+1, leader)
}

func (fixture *rf3FaultFixture) waitCaughtUp(t testing.TB, member int, applied uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state, err := fixture.tryProbe(member, time.Second)
		if err == nil && state.Applied >= applied && state.LeaderID != 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("member %d did not catch up through %d", member+1, applied)
}

func (fixture *rf3FaultFixture) waitAllApplied(t testing.TB, applied uint64, timeout time.Duration) {
	t.Helper()
	for member := 0; member < rf3CommandMembers; member++ {
		fixture.waitCaughtUp(t, member, applied, timeout)
	}
}

func (fixture *rf3FaultFixture) probe(t testing.TB, member int) shardservice.ReplicatedMemberState {
	t.Helper()
	state, err := fixture.tryProbe(member, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func (fixture *rf3FaultFixture) tryProbe(member int, timeout time.Duration) (shardservice.ReplicatedMemberState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client := (member + 1) % rf3CommandMembers
	return probeRF3CommandMember(ctx, fixture.nativeAddresses[member], fixture.nodes[member], fixture.profiles[client], fixture.nodes[client],
		fixture.group, rf3CommandStoreIdentity(1).AllocationGeneration, fixture.authority.ActivePolicyGeneration)
}

func (fixture *rf3FaultFixture) openSession(t testing.TB, leader int, state shardservice.ReplicatedMemberState) (uint64, uint64) {
	t.Helper()
	command := fixture.command(state, 0, 1, sha256.Sum256([]byte("rf3-fault-session-open")), nil)
	command.Kind = replication.CommandSessionOpen
	command.NextDeadlineUnixNano = 2_000_000_000_000_000_000
	command.Batches = nil
	raw, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	response := fixture.propose(t, leader, state, raw)
	if response.Kind != shardservice.ReplicatedCompletion {
		t.Fatalf("session open = %+v", response)
	}
	completion, err := replication.OpenCompletion(response.Completion)
	if err != nil || completion.ResultCode != replicatedstate.ResultSessionOpened {
		t.Fatalf("session completion = %+v, %v", completion, err)
	}
	return completion.ClientEpoch, response.Outcome.AppliedIndex
}

func (fixture *rf3FaultFixture) mutationCommand(
	t testing.TB,
	state shardservice.ReplicatedMemberState,
	epoch, sequence uint64,
	id string,
) []byte {
	t.Helper()
	doc := []byte(fmt.Sprintf(`{"id":%q}`, id))
	mutation := replication.Mutation{Kind: replication.MutationPut, Key: rf3FaultKey(t, id), Value: doc}
	command := fixture.command(state, epoch, sequence, sha256.Sum256(append([]byte("rf3-fault/"), doc...)), []replication.Mutation{mutation})
	if sequence > 1 {
		command.AckThrough = sequence - 1
	}
	raw, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func (fixture *rf3FaultFixture) command(state shardservice.ReplicatedMemberState, epoch, sequence uint64, fingerprint [32]byte, mutations []replication.Mutation) replication.Command {
	fence := state.Fence.Command
	return replication.Command{ClusterID: replication.ID128(fixture.group.ClusterID), ClusterIncarnation: replication.ID128(fixture.group.ClusterIncarnation),
		TopologyRecoveryEpoch: fixture.group.TopologyRecoveryEpoch, Distribution: string(gateway.ReplicatedCatalogDistribution), Shard: string(gateway.ReplicatedCatalogShard),
		AllocationGeneration: state.Fence.AllocationGeneration, ShardIncarnation: replication.ID128(fixture.group.ShardIncarnation), GroupID: replication.ID128(fixture.group.GroupID),
		ReplicaSetVersion: fence.ReplicaSetVersion, ActivePolicyGeneration: fence.ActivePolicyGeneration, ProtectionEpoch: fence.ProtectionEpoch,
		OwnershipEpoch: fence.OwnershipEpoch, SchemaGeneration: fence.SchemaGeneration, RoutingVersion: fence.RoutingVersion, RouteGeneration: fence.RouteGeneration,
		Tenant: []byte("fault-harness"), ClientID: replication.ID128{0x66, 0x33}, ClientEpoch: epoch, ClientSequence: sequence,
		Fingerprint: fingerprint, Batches: []replication.RelationMutationBatch{{Relation: 1, Mutations: mutations}}}
}

func (fixture *rf3FaultFixture) proposalRequest(member int, state shardservice.ReplicatedMemberState, command []byte) *shardservice.ReplicatedRequest {
	client := (member + 1) % rf3CommandMembers
	return &shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedPropose,
		Authority: serviceauthz.Authority{Node: fixture.nodes[client], Generation: fixture.authority.ActivePolicyGeneration}, Capability: serviceauthz.CapabilityDataWrite,
		Fence: state.Fence, Command: command}
}

func (fixture *rf3FaultFixture) propose(t testing.TB, member int, state shardservice.ReplicatedMemberState, command []byte) *shardservice.ReplicatedResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := fixture.roundTripContext(ctx, member, fixture.proposalRequest(member, state, command))
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func (fixture *rf3FaultFixture) roundTrip(t testing.TB, member int, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return fixture.roundTripContext(ctx, member, request)
}

func (fixture *rf3FaultFixture) roundTripContext(ctx context.Context, member int, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", fixture.nativeAddresses[member])
	if err != nil {
		return nil, err
	}
	client := (member + 1) % rf3CommandMembers
	connection, err := fixture.profiles[client].Client(ctx, raw, fixture.nodes[member], rafttransport.TrafficShardNative,
		func() time.Time { return time.Now().Add(3 * time.Second) })
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	defer connection.Close()
	return shardservice.RoundTripReplicated(ctx, connection, request)
}

func (fixture *rf3FaultFixture) sendWithoutReadingResponse(
	t testing.TB,
	member int,
	request *shardservice.ReplicatedRequest,
) net.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", fixture.nativeAddresses[member])
	if err != nil {
		t.Fatal(err)
	}
	client := (member + 1) % rf3CommandMembers
	connection, err := fixture.profiles[client].Client(
		ctx, raw, fixture.nodes[member], rafttransport.TrafficShardNative,
		func() time.Time { return time.Now().Add(3 * time.Second) },
	)
	if err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if err = shardservice.EncodeReplicatedRequestBorrowed(connection, request); err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}
	return connection
}

func (fixture *rf3FaultFixture) waitDocument(
	t testing.TB,
	member int,
	state shardservice.ReplicatedMemberState,
	id string,
	timeout time.Duration,
) uint64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		response, err := fixture.roundTrip(t, member, &shardservice.ReplicatedRequest{
			Operation: shardservice.ReplicatedReadLeader,
			Authority: serviceauthz.Authority{
				Node:       fixture.nodes[(member+1)%rf3CommandMembers],
				Generation: fixture.authority.ActivePolicyGeneration,
			},
			Capability: serviceauthz.CapabilityDataRead,
			Fence:      state.Fence, Relation: 1, Key: rf3FaultKey(t, id),
			MaxValueBytes: 1 << 20,
		})
		if err == nil && response.Kind == shardservice.ReplicatedReadFound &&
			string(response.Value) == fmt.Sprintf(`{"id":%q}`, id) {
			return response.ReadApplied
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("member %d did not make document %q visible", member+1, id)
	return 0
}

func rf3FaultKey(t testing.TB, id string) []byte {
	t.Helper()
	raw := append(append([]byte{'"'}, id...), '"')
	key, ok := orderedkey.AppendJSONString(nil, raw, orderedkey.Ascending)
	if !ok {
		t.Fatalf("encode key %q", id)
	}
	return key
}

func rf3FaultOtherMembers(excluded int) []int {
	result := make([]int, 0, 2)
	for member := 0; member < rf3CommandMembers; member++ {
		if member != excluded {
			result = append(result, member)
		}
	}
	return result
}

func mustRF3FaultTCPAddress(t testing.TB, address string) *net.TCPAddr {
	t.Helper()
	resolved, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func rf3FaultPolicy(nodes [rf3CommandMembers]rafttransport.NodeID) []byte {
	return []byte(fmt.Sprintf(`{"generation":5,"principals":[{"node":"%x","capabilities":["data_read","data_write","delegate","membership","topology"]},{"node":"%x","capabilities":["data_read","data_write","delegate","membership","topology"]},{"node":"%x","capabilities":["data_read","data_write","delegate","membership","topology"]}]}`,
		nodes[0], nodes[1], nodes[2]))
}

func rf3FaultWALAllocatedBytes(t testing.TB, paths [rf3CommandMembers]string) int64 {
	t.Helper()
	var total int64
	for _, path := range paths {
		matches, err := filepath.Glob(path + "*")
		if err != nil {
			t.Fatal(err)
		}
		if len(matches) == 0 {
			t.Fatalf("WAL allocation evidence path %q has no files", path)
		}
		regular := false
		for _, match := range matches {
			info, err := os.Stat(match)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().IsRegular() {
				regular = true
				stat, ok := info.Sys().(*syscall.Stat_t)
				if !ok {
					t.Fatalf("WAL allocation evidence lacks physical block metadata for %q", match)
				}
				total += int64(stat.Blocks) * 512
			}
		}
		if !regular {
			t.Fatalf("WAL allocation evidence path %q has no regular files", path)
		}
	}
	return total
}

func rf3FaultProcessRSSBytes(t testing.TB, children [rf3CommandMembers]*rf3CommandChild) uint64 {
	t.Helper()
	var total uint64
	for member, child := range children {
		if child == nil || child.command == nil || child.command.Process == nil {
			t.Fatalf("member %d has no live process for RSS evidence", member+1)
		}
		path := fmt.Sprintf("/proc/%d/status", child.command.Process.Pid)
		file, err := os.Open(path)
		if err != nil {
			t.Fatalf("open Linux RSS evidence %q: %v", path, err)
		}
		scanner := bufio.NewScanner(file)
		found := false
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) != 3 || fields[0] != "VmRSS:" || fields[2] != "kB" {
				continue
			}
			kib, parseErr := strconv.ParseUint(fields[1], 10, 64)
			if parseErr != nil || kib > ^uint64(0)/1024 {
				_ = file.Close()
				t.Fatalf("parse Linux RSS evidence %q: %v", scanner.Text(), parseErr)
			}
			total += kib * 1024
			found = true
			break
		}
		scanErr := scanner.Err()
		closeErr := file.Close()
		if scanErr != nil || closeErr != nil || !found {
			t.Fatalf("read Linux RSS evidence for member %d: found=%t scan=%v close=%v",
				member+1, found, scanErr, closeErr)
		}
	}
	if total == 0 {
		t.Fatal("RF3 RSS evidence is zero")
	}
	return total
}

func rf3FaultWriteQualification(t testing.TB, qualification rf3bench.Qualification) {
	t.Helper()
	path := os.Getenv(rf3bench.QualificationPathEnvironment)
	if path == "" {
		return
	}
	if !filepath.IsAbs(path) {
		t.Fatalf("RF3 qualification output path must be absolute: %q", path)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("create RF3 qualification evidence: %v", err)
	}
	if err = rf3bench.WriteQualificationTSV(file, qualification); err != nil {
		_ = file.Close()
		t.Fatalf("write RF3 qualification evidence: %v", err)
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		t.Fatalf("sync RF3 qualification evidence: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close RF3 qualification evidence: %v", err)
	}
}
