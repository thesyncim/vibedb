//go:build linux

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
)

const durableRF3MultiRelationEnvironment = "VIBEDB_DURABLE_RF3_MULTIRELATION_E2E"

// TestGatewayDurableRF3MultiRelationChaosProcess qualifies the shipped public
// gateway command, not an in-process transaction kernel. Every mutation wave
// spans orders_a and orders_b. The fixture cross-hosts each table's global
// exact-index relation on the other data RF3 group, so a wave atomically drives
// both base relations, both local exact indexes, and both remote global exact
// indexes through the persisted transaction boundary.
func TestGatewayDurableRF3MultiRelationChaosProcess(t *testing.T) {
	if os.Getenv(durableRF3MultiRelationEnvironment) != "1" {
		t.Skip("set VIBEDB_DURABLE_RF3_MULTIRELATION_E2E=1 for mandatory external qualification")
	}
	if runtime.GOOS != "linux" || testing.Short() {
		t.Fatal("mandatory multi-relation RF3 qualification requires Linux and full mode")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	fixture := newDurableRF3ExternalFixtureWithPeerFaults(t, ctx, true)
	defer fixture.close(t)
	defer func() {
		if t.Failed() {
			for member, process := range fixture.shards {
				t.Logf("multi-relation shard %d diagnostics:\n%s", member+1, process.Diagnostics())
			}
			t.Logf("multi-relation gateway A diagnostics:\n%s", fixture.gatewayA.Diagnostics())
			t.Logf("multi-relation gateway B diagnostics:\n%s", fixture.gatewayB.Diagnostics())
		}
	}()
	fixture.startShards(t)
	fixture.startGateway(t, fixture.gatewayA)
	fixture.initializeObservers(t)

	baselineStorage := replicaProcessAllocatedBytes(fixture.root, "")
	baselineWALs := fixture.captureWALAllocatedBytes(t)
	baselineWAL := durableRF3ExternalAllocatedTotal(baselineWALs)
	baselineSnapshot := replicaProcessSnapshotPayloadBytes(fixture.root)
	baselineRSS := fixture.liveRSS()
	if baselineStorage == 0 || baselineWAL == 0 || baselineRSS == 0 {
		t.Fatalf("invalid multi-relation baseline storage=%d wal=%d rss=%d",
			baselineStorage, baselineWAL, baselineRSS)
	}
	measurements := &durableRF3ExternalMeasurements{peakRSS: baselineRSS}
	fixture.measurements = measurements
	measurementStop, measurementDone := make(chan struct{}), make(chan struct{})
	go fixture.sample(measurementStop, measurementDone, measurements)
	defer func() {
		select {
		case <-measurementDone:
		default:
			close(measurementStop)
			<-measurementDone
		}
	}()

	client := fixture.dialGateway(t, fixture.gatewayANode, fixture.gatewayAAddress)
	reference, openLatency := client.openIssuerInstallation(t, 0x91)
	latencies := []time.Duration{openLatency}

	insertStatements := make([]serveStatement, 0, 24)
	for ordinal := 0; ordinal < 12; ordinal++ {
		insertStatements = append(insertStatements,
			durableRF3ExternalChurnInsert("orders_a", "a", ordinal, "initial"),
			durableRF3ExternalChurnInsert("orders_b", "b", ordinal, "initial"))
	}
	insertRequest := hotMutationRequest(t, reference, 1, insertStatements)
	partitionLeader, _ := fixture.waitRouteLeader(t, durableRF3DataBGroup, -1, 30*time.Second)
	partitioned := durableRF3ExternalLeaderMember(t, partitionLeader)
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
	partitionActive := true
	defer func() {
		if partitionActive {
			setPartition(false)
		}
	}()
	partitionStarted := time.Now()
	fixture.waitAllRoleLeaders(t, partitioned, 30*time.Second)
	partitionFailover := time.Since(partitionStarted)
	insertLoss := client.loseResponseAfterFirstByte(t, insertRequest)
	t.Logf("insert response lost after %s", insertLoss)
	latencies = append(latencies, insertLoss)
	if err := fixture.gatewayA.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	replacementStarted := time.Now()
	fixture.startGateway(t, fixture.gatewayB)
	client = fixture.dialGateway(t, fixture.gatewayBNode, fixture.gatewayBAddress)
	insertRaw, insertLatency := client.roundTrip(t, insertRequest)
	latencies = append(latencies, insertLatency)
	durableRF3ExternalAssertCommitted(t, insertRaw, 24, 2)
	latencies = append(latencies, client.ackTerminal(t, insertRaw))
	replacementRecovery := time.Since(replacementStarted)
	setPartition(false)
	partitionActive = false
	fixture.waitMemberCaughtUpAllRoles(t, partitioned, 45*time.Second)

	updateStatements := make([]serveStatement, 0, 24)
	for ordinal := 0; ordinal < 12; ordinal++ {
		updateStatements = append(updateStatements,
			durableRF3ExternalChurnUpdate("orders_a", "a", ordinal, "final"),
			durableRF3ExternalChurnUpdate("orders_b", "b", ordinal, "final"))
	}
	updateRequest := hotMutationRequest(t, reference, 2, updateStatements)
	updateLeader, _ := fixture.waitRouteLeader(t, durableRF3DataAGroup, -1, 30*time.Second)
	updateKilled := durableRF3ExternalLeaderMember(t, updateLeader)
	fixture.killShard(t, updateKilled)
	killStarted := time.Now()
	fixture.waitAllRoleLeaders(t, updateKilled, 30*time.Second)
	killFailover := time.Since(killStarted)
	updateLoss := client.loseResponseAfterFirstByte(t, updateRequest)
	t.Logf("update response lost after %s", updateLoss)
	latencies = append(latencies, updateLoss)
	client = fixture.dialGateway(t, fixture.gatewayBNode, fixture.gatewayBAddress)
	updateRaw, updateLatency := client.roundTrip(t, updateRequest)
	latencies = append(latencies, updateLatency)
	durableRF3ExternalAssertCommitted(t, updateRaw, 24, 2)
	replayedRaw, replayLatency := client.roundTrip(t, updateRequest)
	latencies = append(latencies, replayLatency)
	if !bytes.Equal(updateRaw, replayedRaw) {
		t.Fatalf("multi-relation terminal replay drifted\nfirst=%s\nsecond=%s", updateRaw, replayedRaw)
	}
	latencies = append(latencies, client.ackTerminal(t, updateRaw))
	fixture.restartShard(t, updateKilled)
	fixture.waitMemberCaughtUpAllRoles(t, updateKilled, 45*time.Second)
	fixture.waitAllRoleLeaders(t, -1, 30*time.Second)

	deleteStatements := make([]serveStatement, 0, 12)
	for ordinal := 0; ordinal < 12; ordinal += 2 {
		deleteStatements = append(deleteStatements,
			durableRF3ExternalChurnDelete("orders_a", "a", ordinal),
			durableRF3ExternalChurnDelete("orders_b", "b", ordinal))
	}
	deleteRequest := hotMutationRequest(t, reference, 3, deleteStatements)
	deleteRaw, deleteLatency := client.roundTrip(t, deleteRequest)
	latencies = append(latencies, deleteLatency)
	durableRF3ExternalAssertCommitted(t, deleteRaw, 12, 2)
	latencies = append(latencies, client.ackTerminal(t, deleteRaw))

	latencies = append(latencies, fixture.verifyNativeMultiRelation(t, client)...)

	// Reopen every persisted voter and re-prove both index paths after catch-up.
	for member := 0; member < durableRF3ExternalVoters; member++ {
		fixture.killShard(t, member)
		fixture.waitAllRoleLeaders(t, member, 30*time.Second)
		fixture.restartShard(t, member)
		fixture.waitMemberCaughtUpAllRoles(t, member, 45*time.Second)
	}
	latencies = append(latencies, fixture.verifyNativeMultiRelation(t, client)...)
	client.close()
	close(measurementStop)
	<-measurementDone
	fixture.verifyRecoveredMultiRelationIndexes(t)

	slices.Sort(latencies)
	p99 := latencies[(len(latencies)*99+99)/100-1]
	// Extra verification samples must not hide a slow mutation in p99.
	maxLatency := latencies[len(latencies)-1]
	// The gateway still cancels the interactive operation at its exact product
	// deadline. The external harness observes that cancellation only after the
	// error response is encoded, scheduled, written, and read from the socket.
	const socketObservationSlack = 50 * time.Millisecond
	interactiveDeadline := gateway.DefaultProfiles()[gateway.ClassInteractive].GlobalDeadline
	if maxLatency > interactiveDeadline+socketObservationSlack || partitionFailover > 15*time.Second ||
		killFailover > 15*time.Second || replacementRecovery > 15*time.Second {
		t.Fatalf("latency bounds p99=%s max=%s partition=%s kill=%s replacement=%s",
			p99, maxLatency, partitionFailover, killFailover, replacementRecovery)
	}
	finalStorage := replicaProcessAllocatedBytes(fixture.root, "")
	finalWALs := fixture.captureWALAllocatedBytes(t)
	finalWAL := durableRF3ExternalAllocatedTotal(finalWALs)
	storageGrowth := positiveDifference(finalStorage, baselineStorage)
	walGrowth := positiveDifference(finalWAL, baselineWAL)
	for path, baseline := range baselineWALs {
		if growth := positiveDifference(finalWALs[path], baseline); growth > 16<<20 {
			t.Fatalf("multi-relation WAL %q growth=%d exceeds 16MiB", path, growth)
		}
	}
	snapshotGrowth := positiveDifference(
		replicaProcessSnapshotPayloadBytes(fixture.root), baselineSnapshot)
	measurements.mu.Lock()
	peakRSS, publicWireBytes := measurements.peakRSS, measurements.clientBytes
	measurements.mu.Unlock()
	rssGrowth := positiveDifference(peakRSS, baselineRSS)
	if storageGrowth > 128<<20 || walGrowth > 64<<20 || rssGrowth > 384<<20 ||
		snapshotGrowth != 0 || publicWireBytes > 2<<20 {
		t.Fatalf("multi-relation resource bounds rss=%d storage=%d wal=%d public_wire=%d snapshot=%d",
			rssGrowth, storageGrowth, walGrowth, publicWireBytes, snapshotGrowth)
	}
	for _, journal := range []string{fixture.gatewayAJournal, fixture.gatewayBJournal} {
		if !replicaProcessTreeBounded(t, fixture.root, filepath.Base(journal), 6, 8<<20) {
			t.Fatalf("unbounded multi-relation gateway journal %q", journal)
		}
	}
	t.Logf("durable RF3 multi-relation chaos: shipped_gateway=true tables=2 data_groups=2 base_relations=2 local_exact_indexes=2 global_exact_indexes=2 insert_update_delete=true sustained_churn_mutations=60 persisted_transaction_recovery=true gateway_replacement=true leader_kill=true peer_network_partition=true isolated_process_live=true outcome_unknown_retry=true exact_terminal_replay=true native_point_verification=true persisted_index_voters=3 exact_index_visibility=true stale_index_visibility=false final_cardinality_exact=true all_voters_reopened=true p99=%s max=%s partition_failover=%s leader_kill_failover=%s gateway_replacement_recovery=%s rss_growth=%d storage_growth=%d wal_growth=%d public_client_wire_bytes=%d snapshot_payload_bytes=%d",
		p99, maxLatency, partitionFailover, killFailover, replacementRecovery, rssGrowth, storageGrowth,
		walGrowth, publicWireBytes, snapshotGrowth)
}

func (client *durableRF3ExternalWireClient) openIssuerInstallation(
	t *testing.T, installationByte byte,
) (gateway.ReplicatedIssuerReference, time.Duration) {
	t.Helper()
	response, latency := client.roundTrip(t, sessionProtocolIssuerOpenRequest(t,
		gateway.ReplicatedIssuerOpen{Installation: replication.ID128{installationByte}, Epoch: 1}))
	var decoded struct {
		OK             bool   `json:"ok"`
		InstallationID string `json:"installation_id"`
		IssuerEpoch    uint64 `json:"issuer_epoch"`
		LaneOrdinal    uint16 `json:"lane_ordinal"`
		GrantDigest    string `json:"grant_digest"`
		Error          string `json:"error"`
	}
	if err := vibejson.Unmarshal(response, &decoded); err != nil || !decoded.OK || decoded.Error != "" {
		t.Fatalf("issuer_open response=%s err=%v", response, err)
	}
	var reference gateway.ReplicatedIssuerReference
	if decodeFixedHex(decoded.InstallationID, reference.Installation[:]) != nil ||
		decodeFixedHex(decoded.GrantDigest, reference.GrantDigest[:]) != nil || decoded.IssuerEpoch == 0 {
		t.Fatalf("issuer_open reference=%+v", decoded)
	}
	reference.Epoch, reference.LaneOrdinal = decoded.IssuerEpoch, decoded.LaneOrdinal
	return reference, latency
}

func (client *durableRF3ExternalWireClient) ackTerminal(t *testing.T, terminal []byte) time.Duration {
	t.Helper()
	request := durableRF3ExternalAckRequest(t, terminal)
	raw, latency := client.roundTrip(t, sessionProtocolAckRequest(t, request))
	durableRF3ExternalAssertAckResponse(t, raw, request)
	return latency
}

func durableRF3ExternalChurnInsert(table, prefix string, ordinal int, phase string) serveStatement {
	return serveStatement{SQL: "INSERT INTO " + table + " VALUES (?)", Params: []serveParam{{
		Kind: "document", Text: fmt.Sprintf(
			`{"id":"churn-%s-%02d","kind":"%s-%02d","email":"%s-%02d@example.test","value":%d}`,
			prefix, ordinal, phase, ordinal, phase, ordinal, ordinal),
	}}}
}

func durableRF3ExternalChurnUpdate(table, prefix string, ordinal int, phase string) serveStatement {
	identifier := fmt.Sprintf("churn-%s-%02d", prefix, ordinal)
	return serveStatement{SQL: `UPDATE ` + table + ` SET "$doc" = ? WHERE id = ?`, Params: []serveParam{
		{Kind: "document", Text: fmt.Sprintf(
			`{"id":"%s","kind":"%s-%02d","email":"%s-%02d@example.test","value":%d}`,
			identifier, phase, ordinal, phase, ordinal, 1000+ordinal)},
		{Kind: "string", Text: identifier},
	}}
}

func durableRF3ExternalChurnDelete(table, prefix string, ordinal int) serveStatement {
	return serveStatement{SQL: "DELETE FROM " + table + " WHERE id = ?",
		Params: []serveParam{{Kind: "string", Text: fmt.Sprintf("churn-%s-%02d", prefix, ordinal)}}}
}
