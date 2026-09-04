package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardservice"
)

// rf3DiagnosticSnapshot is a fixed, bounded process-local evidence record.
// It is emitted only in response to SIGUSR1 and contains detached counters;
// no request, catalog, path, or error text is retained. The histogram has the
// fixed MaxPersistGroupBatches bound and lets a trial prove real multi-group
// append waves from the process that owns the shared node log.
type rf3DiagnosticSnapshot struct {
	UTC    string `json:"utc"`
	Event  string `json:"event"`
	Serial uint64 `json:"serial"`
	PID    int    `json:"pid"`
	NodeID string `json:"node_id"`
	Groups int    `json:"groups"`

	ReadyWaves             uint64   `json:"ready_waves"`
	ReadyDurableWaves      uint64   `json:"ready_durable_waves"`
	ObservedAppendBarriers uint64   `json:"observed_append_barriers"`
	MultiGroupWaves        uint64   `json:"multi_group_waves"`
	ReadyWaveHistogram     []uint64 `json:"ready_wave_group_histogram"`
	ReadyQueueDepth        uint64   `json:"ready_queue_depth"`
	ReadyQueueCapacity     uint64   `json:"ready_queue_capacity"`
	ActiveSubmitters       int64    `json:"active_submitters"`
	FailedWaves            uint64   `json:"failed_waves"`
	CheckpointQueue        uint64   `json:"checkpoint_queue_submissions"`
	CheckpointRejected     uint64   `json:"checkpoint_queue_rejected"`
	CheckpointQueueWaitNs  uint64   `json:"checkpoint_queue_wait_ns"`
	CheckpointServiceNs    uint64   `json:"checkpoint_service_ns"`

	NativeAvailable  bool   `json:"native_available"`
	NativeAccepted   uint64 `json:"native_accepted"`
	NativeRejected   uint64 `json:"native_rejected"`
	NativeFailed     uint64 `json:"native_failed"`
	NativeActive     uint64 `json:"native_active"`
	NativeDispatches uint64 `json:"native_semantic_dispatches"`
	NativeFrameBytes int64  `json:"native_inflight_frame_bytes"`

	GatewayAvailable       bool   `json:"gateway_available"`
	GatewayLocalCalls      uint64 `json:"gateway_local_calls"`
	GatewayRemoteCalls     uint64 `json:"gateway_remote_calls"`
	GatewaySemanticSQL     uint64 `json:"gateway_semantic_sql_calls"`
	GatewayLegacyCalls     uint64 `json:"gateway_legacy_calls"`
	GatewaySQLRequestCount uint64 `json:"gateway_sql_request_encodings"`
	GatewaySQLRequestBytes uint64 `json:"gateway_sql_request_encoded_bytes"`

	RemoteDials             uint64 `json:"remote_dials"`
	RemoteReuses            uint64 `json:"remote_reuses"`
	RemotePoisoned          uint64 `json:"remote_poisoned"`
	RemoteRejected          uint64 `json:"remote_rejected"`
	RemoteHandshakeFailures uint64 `json:"remote_handshake_failures"`
	RemoteConnections       int    `json:"remote_connections"`
	RemoteIdle              int    `json:"remote_idle"`
	RemoteWaiters           int    `json:"remote_waiters"`
}

// emitRF3DiagnosticSnapshot writes one machine-readable line with a stable
// prefix and atomically replaces the bounded node-root latest snapshot.
// Benchmark harnesses can send SIGUSR1 at trial boundaries and parse the
// serial and counters without opening an unauthenticated metrics endpoint.
// The caller invokes this synchronously from the lifecycle select, before any
// owner or transport is drained.
func emitRF3DiagnosticSnapshot(
	manifest rf3Manifest,
	profile *rafttransport.PeerTLS,
	nodeOwner *rf3NodeOwner,
	server *shardservice.ReplicatedServer,
	embedded *rf3EmbeddedGateway,
	serial *atomic.Uint64,
	inventory *rf3AdoptedGroupInventory,
) {
	snapshot := rf3DiagnosticSnapshot{
		UTC: time.Now().UTC().Format(time.RFC3339Nano), Event: "snapshot", PID: os.Getpid(),
		Groups: len(manifest.groupBundles()), ReadyWaveHistogram: make([]uint64, raftstore.MaxPersistGroupBatches+1),
	}
	if inventory != nil {
		// The authority snapshot includes both freshly appended prepared
		// groups and adopted split children. Count their union with the manifest.
		groups := make(map[raftmember.GroupKey]struct{}, snapshot.Groups)
		for _, bundle := range manifest.groupBundles() {
			groups[bundle.Route.Group] = struct{}{}
		}
		if native := inventory.nativeChildren.Load(); native != nil {
			for group := range *native {
				groups[group] = struct{}{}
			}
		}
		snapshot.Groups = len(groups)
	}
	if serial != nil {
		snapshot.Serial = serial.Add(1)
	}
	if profile != nil {
		node := profile.LocalIdentity().Node
		snapshot.NodeID = fmt.Sprintf("%x", node[:])
	}
	if nodeOwner != nil && nodeOwner.sequencer != nil {
		stats := nodeOwner.sequencer.Stats()
		snapshot.ReadyWaves = stats.ReadyWavesSucceeded
		snapshot.ReadyDurableWaves = stats.ReadyDurableWaves
		snapshot.ObservedAppendBarriers = stats.ObservedAppendBarriers
		snapshot.MultiGroupWaves = stats.MultiGroupWaves
		copy(snapshot.ReadyWaveHistogram, stats.ReadyWaveGroupHistogram[:])
		snapshot.ReadyQueueDepth = stats.QueueDepth
		snapshot.ReadyQueueCapacity = stats.QueueCapacity
		snapshot.ActiveSubmitters = stats.ActiveSubmitters
		snapshot.FailedWaves = stats.FailedWaves
		snapshot.CheckpointQueue = stats.CheckpointQueueSubmissions
		snapshot.CheckpointRejected = stats.CheckpointQueueRejected
		snapshot.CheckpointQueueWaitNs = stats.CheckpointQueueWaitNanos
		snapshot.CheckpointServiceNs = stats.CheckpointServiceNanos
	}
	if server != nil {
		stats := server.Stats()
		snapshot.NativeAvailable = true
		snapshot.NativeAccepted = stats.Accepted
		snapshot.NativeRejected = stats.Rejected
		snapshot.NativeFailed = stats.Failed
		snapshot.NativeActive = stats.Active
		snapshot.NativeDispatches = stats.SemanticDispatch
		snapshot.NativeFrameBytes = stats.InFlightFrameBytes
	}
	if embedded != nil {
		snapshot.GatewayAvailable = embedded.client != nil
		if embedded.client != nil {
			stats := embedded.client.Stats()
			snapshot.GatewayLocalCalls = stats.LocalCalls
			snapshot.GatewayRemoteCalls = stats.RemoteCalls
			snapshot.GatewaySemanticSQL = stats.SemanticSQLCalls
			snapshot.GatewayLegacyCalls = stats.LegacyCalls
			snapshot.GatewaySQLRequestCount = stats.SQLRequestEncodings
			snapshot.GatewaySQLRequestBytes = stats.SQLRequestEncodedBytes
		}
		if embedded.remote != nil {
			stats := embedded.remote.Stats()
			snapshot.RemoteDials = stats.Dials
			snapshot.RemoteReuses = stats.Reuses
			snapshot.RemotePoisoned = stats.Poisoned
			snapshot.RemoteRejected = stats.Rejected
			snapshot.RemoteHandshakeFailures = stats.HandshakeFailures
			snapshot.RemoteConnections = stats.Connections
			snapshot.RemoteIdle = stats.Idle
			snapshot.RemoteWaiters = stats.Waiters
		}
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return
	}
	fmt.Fprintf(os.Stderr, "VIBEDB_RF3_DIAGNOSTIC %s\n", raw)
	if manifest.NodeLog != nil {
		// Replace through a same-directory temporary so readers never observe a
		// partial record. Sync before returning so the caller can wait for the
		// serial to appear on disk before entering a timed interval. Only one
		// fixed-size latest record is retained.
		directory := filepath.Dir(manifest.NodeLog.Path)
		if file, openErr := os.CreateTemp(directory, ".rf3-diagnostics-"); openErr == nil {
			temporary := file.Name()
			_, writeErr := file.Write(raw)
			syncErr := file.Sync()
			closeErr := file.Close()
			renameErr := error(nil)
			if writeErr == nil && syncErr == nil && closeErr == nil {
				renameErr = os.Rename(temporary, filepath.Join(directory, "rf3-diagnostics.json"))
			}
			if renameErr != nil || writeErr != nil || syncErr != nil || closeErr != nil {
				_ = os.Remove(temporary)
			}
		}
	}
}
