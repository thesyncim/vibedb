//go:build linux

package gatewayruntime

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	vibejson "github.com/thesyncim/vibejson"
)

const readBatchRF3ExternalEnvironment = "VIBEDB_READ_BATCH_RF3_PROCESS_E2E"

// TestGatewayReadBatchRF3ExternalProcessChaos qualifies the shipped exact-key
// multi-table read_batch path against three real RF3 processes and two data
// groups. Every group owns an independent leader ReadIndex cut. The ordered
// observation vector is the complete consistency contract: this test must
// never be cited as one global MVCC snapshot or one cluster timestamp.
func TestGatewayReadBatchRF3ExternalProcessChaos(t *testing.T) {
	if value := strings.TrimSpace(os.Getenv(readBatchRF3ExternalEnvironment)); value != "1" {
		t.Skip("set VIBEDB_READ_BATCH_RF3_PROCESS_E2E=1 for mandatory RF3 read_batch qualification")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("required RF3 read_batch qualification needs Linux /proc and strict allocation")
	}
	if testing.Short() {
		t.Fatal("required RF3 read_batch qualification cannot run in short mode")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	fixture := newDurableRF3ExternalFixtureWithPeerFaults(t, ctx, true)
	defer fixture.close(t)
	fixture.startShards(t)
	fixture.startGateway(t, fixture.gatewayA)
	fixture.initializeObservers(t)

	baselineStorage := replicaProcessAllocatedBytes(fixture.root, "")
	baselineWALs := fixture.captureWALAllocatedBytes(t)
	baselineWAL := durableRF3ExternalAllocatedTotal(baselineWALs)
	baselineRSS := fixture.liveRSS()
	if baselineStorage == 0 || baselineStorage > fixture.baselineStorageBudget || baselineWAL == 0 || baselineRSS == 0 {
		t.Fatalf("invalid read_batch baseline storage=%d wal=%d rss=%d",
			baselineStorage, baselineWAL, baselineRSS)
	}
	measurements := &durableRF3ExternalMeasurements{peakRSS: baselineRSS}
	fixture.measurements = measurements
	measurementStop := make(chan struct{})
	measurementDone := make(chan struct{})
	go fixture.sample(measurementStop, measurementDone, measurements)
	defer func() {
		select {
		case <-measurementDone:
		default:
			close(measurementStop)
			<-measurementDone
		}
	}()

	request := readBatchRF3ExternalRequest(t)
	client := fixture.dialGateway(t, fixture.gatewayANode, fixture.gatewayAAddress)
	baselineRaw, baselineLatency := client.roundTrip(t, request)
	baseline := assertReadBatchRF3ExternalResponse(t, fixture, baselineRaw)
	latencies := []time.Duration{baselineLatency}

	// Stop the gateway before the request reaches user space. The established
	// mTLS socket accepts the request into its bounded kernel buffer, but a
	// short read deadline must observe no response until the process resumes.
	// This makes response delay deterministic without a test-only server path.
	delayedRaw, delayedLatency := client.roundTripWhileGatewayStopped(
		t, request, fixture.gatewayA.PID(), 250*time.Millisecond,
	)
	latencies = append(latencies, delayedLatency)
	assertReadBatchRF3ExternalResponse(t, fixture, delayedRaw)

	// Partition only the current data-a leader's Raft peer links. Its native
	// listener and process stay live, so refusal is tested while isolated. The
	// gateway must discover the elected quorum leader with bounded attempts and
	// return one complete vector; partial per-group results are forbidden.
	oldLeader, _ := fixture.waitRouteLeader(t, durableRF3DataAGroup, -1, 30*time.Second)
	partitioned := durableRF3ExternalLeaderMember(t, oldLeader)
	partitionStarted := time.Now()
	setPartition := func(blocked bool) {
		for source := range fixture.peerLinks {
			for target, link := range fixture.peerLinks[source] {
				if link != nil && (source == partitioned || target == partitioned) {
					link.setBlocked(blocked)
				}
			}
		}
	}
	setPartition(true)
	defer setPartition(false)
	fixture.waitAllRoleLeaders(t, partitioned, 30*time.Second)
	partitionFailover := time.Since(partitionStarted)
	partitionRaw, partitionLatency := client.roundTrip(t, request)
	latencies = append(latencies, partitionLatency)
	partitionResult := assertReadBatchRF3ExternalResponse(t, fixture, partitionRaw)
	assertReadBatchRF3RetriesBounded(t, partitionResult, 15)
	// Address the still-isolated former leader with the production native
	// frame, before healing. It must refuse a local linearizable cut.
	assertReadBatchRF3FormerLeaderRefuses(t, fixture, partitioned)
	setPartition(false)
	fixture.waitMemberCaughtUpAllRoles(t, partitioned, 45*time.Second)

	// Kill the current leader of the other data group, read through the new
	// quorum, then restore and catch up the killed voter.
	leaderB, _ := fixture.waitRouteLeader(t, durableRF3DataBGroup, -1, 30*time.Second)
	killed := durableRF3ExternalLeaderMember(t, leaderB)
	leaderLossStarted := time.Now()
	fixture.killShard(t, killed)
	fixture.waitAllRoleLeaders(t, killed, 30*time.Second)
	leaderLossFailover := time.Since(leaderLossStarted)
	lossRaw, lossLatency := client.roundTrip(t, request)
	latencies = append(latencies, lossLatency)
	lossResult := assertReadBatchRF3ExternalResponse(t, fixture, lossRaw)
	assertReadBatchRF3RetriesBounded(t, lossResult, 15)
	fixture.restartShard(t, killed)
	fixture.waitMemberCaughtUpAllRoles(t, killed, 45*time.Second)

	// Replace the complete gateway process and local leader-hint cache. The new
	// principal must reconstruct routing and return the same ordered documents.
	client.close()
	fixture.stopGateway(t, fixture.gatewayA)
	replacementStarted := time.Now()
	fixture.startGateway(t, fixture.gatewayB)
	replacement := fixture.dialGateway(t, fixture.gatewayBNode, fixture.gatewayBAddress)
	replacementRaw, replacementLatency := replacement.roundTrip(t, request)
	latencies = append(latencies, replacementLatency)
	replacementResult := assertReadBatchRF3ExternalResponse(t, fixture, replacementRaw)
	assertReadBatchRF3RetriesBounded(t, replacementResult, 15)
	gatewayReplacement := time.Since(replacementStarted)
	for range 8 {
		raw, latency := replacement.roundTrip(t, request)
		latencies = append(latencies, latency)
		assertReadBatchRF3ExternalResponse(t, fixture, raw)
	}
	replacement.close()

	close(measurementStop)
	<-measurementDone
	slices.Sort(latencies)
	p99 := latencies[(len(latencies)*99+99)/100-1]
	if p99 > 5*time.Second || partitionFailover > 15*time.Second ||
		leaderLossFailover > 15*time.Second || gatewayReplacement > 15*time.Second {
		t.Fatalf("read_batch latency bounds p99=%s partition=%s leader_loss=%s gateway=%s",
			p99, partitionFailover, leaderLossFailover, gatewayReplacement)
	}
	finalStorage := replicaProcessAllocatedBytes(fixture.root, "")
	finalWALs := fixture.captureWALAllocatedBytes(t)
	finalWAL := durableRF3ExternalAllocatedTotal(finalWALs)
	storageGrowth := positiveDifference(finalStorage, baselineStorage)
	walGrowth := positiveDifference(finalWAL, baselineWAL)
	for path, before := range baselineWALs {
		after, found := finalWALs[path]
		if !found {
			t.Fatalf("read_batch RF3 WAL disappeared: %q", path)
		}
		if growth := positiveDifference(after, before); growth > 16<<20 {
			t.Fatalf("read_batch RF3 WAL %q grew %d bytes, exceeds 16MiB", path, growth)
		}
	}
	measurements.mu.Lock()
	peakRSS, publicWireBytes := measurements.peakRSS, measurements.clientBytes
	measurements.mu.Unlock()
	rssGrowth := positiveDifference(peakRSS, baselineRSS)
	if storageGrowth > 96<<20 || walGrowth > 64<<20 || rssGrowth > 384<<20 ||
		publicWireBytes > 2<<20 {
		t.Fatalf("read_batch RF3 bounds rss=%d storage=%d wal=%d public_client_wire=%d",
			rssGrowth, storageGrowth, walGrowth, publicWireBytes)
	}
	if baseline.groups != 2 || partitionResult.groups != 2 || lossResult.groups != 2 ||
		replacementResult.groups != 2 {
		t.Fatal("read_batch response lost one independent group observation")
	}
	t.Logf("read_batch external RF3: shard_processes=3 data_groups=2 tables=2 per_group_readindex_vector=true global_snapshot_claim=false exact_ordered_results=true response_delayed=true shard_partition=true stale_former_leader_refused=true leader_sigkill=true gateway_replacement=true retries_bounded=true partition_failover=%s leader_loss_failover=%s gateway_replacement=%s p99=%s rss_growth=%d storage_growth=%d wal_growth=%d public_client_wire_bytes=%d",
		partitionFailover, leaderLossFailover, gatewayReplacement, p99,
		rssGrowth, storageGrowth, walGrowth, publicWireBytes)
}

type readBatchRF3ExternalResult struct {
	groups  int
	retries []uint64
}

func readBatchRF3ExternalRequest(t testing.TB) []byte {
	t.Helper()
	raw, err := vibejson.Marshal(&serveRequest{Op: "read_batch", Class: "interactive",
		MaxResultBytes: 1 << 20, Statements: []serveStatement{
			{SQL: "SELECT * FROM orders_b WHERE id = ?", Params: []serveParam{{Kind: "string", Text: "seed-b"}}},
			{SQL: "SELECT * FROM orders_a WHERE id = ?", Params: []serveParam{{Kind: "string", Text: "missing-a"}}},
			{SQL: "SELECT * FROM orders_a WHERE id = ?", Params: []serveParam{{Kind: "string", Text: "seed-a"}}},
			{SQL: "SELECT * FROM orders_b WHERE id = ?", Params: []serveParam{{Kind: "string", Text: "seed-b"}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func assertReadBatchRF3ExternalResponse(
	t testing.TB,
	fixture *durableRF3ExternalFixture,
	raw []byte,
) readBatchRF3ExternalResult {
	t.Helper()
	document, err := vibejson.Parse(raw)
	if err != nil {
		t.Fatalf("read_batch response=%s err=%v", raw, err)
	}
	members, ok := document.Object()
	if !ok || len(members) != 4 {
		t.Fatalf("read_batch top-level contract=%s", raw)
	}
	for _, forbidden := range []string{"global_timestamp", "snapshot", "read_timestamp", "mvcc_timestamp"} {
		if _, found := document.Get(forbidden); found {
			t.Fatalf("read_batch overclaimed global snapshot with %q: %s", forbidden, raw)
		}
	}
	okValue, found := document.Get("ok")
	okResponse, boolOK := okValue.Bool()
	if !found || !boolOK || !okResponse {
		t.Fatalf("read_batch not successful: %s", raw)
	}
	foundValue, found := document.Get("found")
	foundValues, arrayOK := foundValue.Array()
	if !found || !arrayOK || len(foundValues) != 4 {
		t.Fatalf("read_batch found vector=%s", raw)
	}
	wantFound := [...]bool{true, false, true, true}
	for index := range foundValues {
		got, gotOK := foundValues[index].Bool()
		if !gotOK || got != wantFound[index] {
			t.Fatalf("read_batch found[%d]=%v,%v want=%v response=%s",
				index, got, gotOK, wantFound[index], raw)
		}
	}
	documentValue, found := document.Get("documents")
	documents, arrayOK := documentValue.Array()
	if !found || !arrayOK || len(documents) != 4 {
		t.Fatalf("read_batch documents=%s", raw)
	}
	wantIDs := [...]string{"seed-b", "", "seed-a", "seed-b"}
	for index := range documents {
		idValue, hasID := documents[index].Get("id")
		id, textOK := idValue.Text()
		if wantIDs[index] == "" {
			if hasID || textOK {
				t.Fatalf("read_batch documents[%d] is not positional null: %s", index, raw)
			}
			continue
		}
		if !hasID || !textOK || id != wantIDs[index] {
			t.Fatalf("read_batch documents[%d].id=%q want=%q response=%s",
				index, id, wantIDs[index], raw)
		}
	}
	observationValue, found := document.Get("observations")
	observations, arrayOK := observationValue.Array()
	if !found || !arrayOK || len(observations) != 2 {
		t.Fatalf("read_batch observation vector=%s", raw)
	}
	wantGroups := map[string]bool{
		fmt.Sprintf("%x", fixture.routes[durableRF3DataAGroup].Group.GroupID): false,
		fmt.Sprintf("%x", fixture.routes[durableRF3DataBGroup].Group.GroupID): false,
	}
	var previousRoute string
	result := readBatchRF3ExternalResult{groups: len(observations), retries: make([]uint64, len(observations))}
	for index := range observations {
		groupValue, hasGroup := observations[index].Get("group_id")
		group, groupOK := groupValue.Text()
		routeValue, hasRoute := observations[index].Get("route_id")
		route, routeOK := routeValue.Text()
		appliedValue, hasApplied := observations[index].Get("applied")
		applied, appliedOK := appliedValue.Uint64()
		if !hasGroup || !groupOK || !hasRoute || !routeOK || len(route) != 64 ||
			!hasApplied || !appliedOK || applied == 0 || index != 0 && route <= previousRoute {
			t.Fatalf("read_batch invalid observation[%d]: %s", index, raw)
		}
		if _, expected := wantGroups[group]; !expected {
			t.Fatalf("read_batch unexpected group %q: %s", group, raw)
		}
		wantGroups[group] = true
		if retryValue, hasRetries := observations[index].Get("retries"); hasRetries {
			retries, retryOK := retryValue.Uint64()
			if !retryOK {
				t.Fatalf("read_batch invalid retries[%d]: %s", index, raw)
			}
			result.retries[index] = retries
		}
		previousRoute = route
	}
	for group, seen := range wantGroups {
		if !seen {
			t.Fatalf("read_batch omitted group %q: %s", group, raw)
		}
	}
	return result
}

func assertReadBatchRF3RetriesBounded(
	t testing.TB,
	result readBatchRF3ExternalResult,
	maximum uint64,
) {
	t.Helper()
	for index, retries := range result.retries {
		if retries > maximum {
			t.Fatalf("read_batch observation %d retries=%d exceeds %d", index, retries, maximum)
		}
	}
}

func assertReadBatchRF3FormerLeaderRefuses(
	t testing.TB,
	fixture *durableRF3ExternalFixture,
	member int,
) {
	t.Helper()
	state, err := fixture.probeMember(durableRF3DataAGroup, member, false)
	if err != nil {
		t.Fatal(err)
	}
	key, ok := orderedkey.AppendString(nil, []byte("seed-a"), orderedkey.Ascending)
	if !ok {
		t.Fatal("encode read_batch primary key")
	}
	packed, err := replicatedstate.AppendPointReadBatch(nil, []replicatedstate.PointRead{{
		Relation: 1, Key: key,
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(fixture.ctx, 3*time.Second)
	defer cancel()
	response, err := fixture.probeClient.DoReplicated(ctx,
		fixture.routes[durableRF3DataAGroup].Replicas[member],
		&shardservice.ReplicatedRequest{
			Operation:  shardservice.ReplicatedReadBatchLeader,
			Authority:  serviceauthz.Authority{Node: fixture.nodes[fixture.observerNode], Generation: 5},
			Capability: serviceauthz.CapabilityDataRead, Fence: state.Fence,
			BatchRead: packed, MinimumApplied: 1, MaxValueBytes: 1 << 20,
		},
	)
	if err != nil || response == nil || response.Kind != shardservice.ReplicatedNotLeader ||
		!response.HasState || response.State.LeaderID == uint64(member+1) {
		t.Fatalf("former leader served/refused incorrectly response=%+v err=%v", response, err)
	}
}

func (client *durableRF3ExternalWireClient) roundTripWhileGatewayStopped(
	t testing.TB,
	request []byte,
	pid int,
	delay time.Duration,
) ([]byte, time.Duration) {
	t.Helper()
	if client == nil || client.connection == nil || len(request) == 0 || pid <= 0 || delay <= 0 {
		t.Fatal("invalid delayed external gateway round trip")
	}
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	stopped := true
	defer func() {
		if stopped {
			_ = syscall.Kill(pid, syscall.SIGCONT)
		}
	}()
	wire := append(append([]byte(nil), request...), '\n')
	started := time.Now()
	written, err := client.connection.Write(wire)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.connection.SetReadDeadline(time.Now().Add(delay)); err != nil {
		t.Fatal(err)
	}
	var probe [1]byte
	read, readErr := client.reader.Read(probe[:])
	if read != 0 || readErr == nil {
		t.Fatalf("stopped gateway emitted response read=%d bytes=%q err=%v", read, probe[:read], readErr)
	}
	if err = syscall.Kill(pid, syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	stopped = false
	if err = client.connection.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	response, err := client.reader.ReadBytes('\n')
	latency := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	client.recordBytes(uint64(written + len(response)))
	return response, latency
}
