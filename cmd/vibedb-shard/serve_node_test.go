package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibejson"
)

func TestRunServeNodeRequiresPreparedGatewayManifest(t *testing.T) {
	directory := t.TempDir()
	manifestPath := filepath.Join(directory, "node.vibejson")
	if err := os.WriteFile(manifestPath, []byte(canonicalRF3Manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"missing manifest":      {"vibedb-shard", "serve-node"},
		"single group manifest": {"vibedb-shard", "serve-node", "-manifest", manifestPath},
		"unknown flag":          {"vibedb-shard", "serve-node", "-manifest", manifestPath, "-unknown"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := run(args); got != 2 {
				t.Fatalf("run(%q) = %d, want usage failure 2", args, got)
			}
		})
	}
}

func TestParseRF3ManifestEmbeddedGatewayIsExplicitAndCanonical(t *testing.T) {
	directory := t.TempDir()
	gatewayManifest := &rf3ManifestGateway{
		CatalogPath: "/srv/gateway/catalog.vibejson", CatalogRouteSeedPath: "/srv/gateway/catalog.route-seed",
		CatalogBootstrapIfMissing: true, CatalogRelation: 1, CatalogAttempts: 8,
		CatalogAttemptTimeoutMillis: 5000, CatalogSessionLeaseMillis: 86400000,
		CatalogSessionJournal: "/srv/gateway/session", CatalogClientID: strings.Repeat("a", 32),
		CatalogRetryHome: strings.Repeat("b", 16), DurableAckKeyPath: "/run/secrets/durable-ack.key",
		ListenAddress: "127.0.0.1:7800", TLS: rf3ManifestTLS{
			Certificate: "/run/secrets/gateway-cert.pem", Key: "/run/secrets/gateway-key.pem",
			Roots: "/run/secrets/cluster-roots.pem", IdentityOID: "1.3.6.1.4.1.32473.1.1",
		}, AuthorizationPolicy: "/etc/vibedb/authorization.json",
		ShardPeers: []rf3ManifestGatewayPeer{}, TableCatalogs: []string{},
	}
	gatewayRaw, err := vibejson.Marshal(gatewayManifest)
	if err != nil {
		t.Fatal(err)
	}
	document := strings.Replace(multiGroupRF3Manifest(t), `  "groups": [`,
		`  "gateway": `+string(gatewayRaw)+`,
  "groups": [`, 1)
	manifest, err := parseRF3Manifest([]byte(document))
	if err != nil {
		t.Fatalf("parse embedded manifest: %v\n%s", err, document)
	}
	if manifest.Gateway == nil || manifest.Gateway.CatalogPath != gatewayManifest.CatalogPath ||
		manifest.withGroup(manifest.Groups[0]).Gateway == nil {
		t.Fatalf("embedded gateway was not retained: %+v", manifest.Gateway)
	}
	path := filepath.Join(directory, "node.vibejson")
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadRF3Manifest(path)
	if err != nil || loaded.Gateway == nil {
		t.Fatalf("load grouped gateway: %v", err)
	}
	for name, invalid := range map[string]string{
		"gateway before split control": strings.Replace(document, `  "gateway": `, `  "split_control": {},
  "gateway": `, 1),
		"uppercase client id":               strings.Replace(document, strings.Repeat("a", 32), strings.Repeat("A", 32), 1),
		"durable ack collides with catalog": strings.Replace(document, "/run/secrets/durable-ack.key", "/srv/gateway/catalog.vibejson", 1),
		"duplicate gateway field": strings.Replace(document, `  "groups": [`, `  "gateway": `+string(gatewayRaw)+`,
  "groups": [`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseRF3Manifest([]byte(invalid)); !errors.Is(err, errInvalidRF3Manifest) {
				t.Fatalf("parse error = %v", err)
			}
		})
	}
	participant := strings.NewReplacer(
		`"pg_listen":""`, `"pg_listen":"127.0.0.1:5433"`,
		`"control_participant_only":false`, `"control_participant_only":true`,
		`"ddl_owner_address":""`, `"ddl_owner_address":"127.0.0.1:7801"`,
		`"ddl_owner_node":""`, `"ddl_owner_node":"1112131415161718191a1b1c1d1e1f20"`,
	).Replace(document)
	forwarding, err := parseRF3Manifest([]byte(participant))
	if err != nil || !forwarding.Gateway.ControlParticipantOnly ||
		forwarding.Gateway.DDLOwnerAddress != "127.0.0.1:7801" || forwarding.Gateway.DDLOwnerNode != "1112131415161718191a1b1c1d1e1f20" {
		t.Fatalf("participant DDL owner fields: manifest=%+v err=%v", forwarding.Gateway, err)
	}
	for name, invalid := range map[string]string{
		"owner without participant":          strings.Replace(participant, `"control_participant_only":true`, `"control_participant_only":false`, 1),
		"participant without owner address":  strings.Replace(participant, `"ddl_owner_address":"127.0.0.1:7801"`, `"ddl_owner_address":""`, 1),
		"participant without owner identity": strings.Replace(participant, `"ddl_owner_node":"1112131415161718191a1b1c1d1e1f20"`, `"ddl_owner_node":""`, 1),
		"uppercase owner identity":           strings.Replace(participant, `"ddl_owner_node":"1112131415161718191a1b1c1d1e1f20"`, `"ddl_owner_node":"1112131415161718191A1B1C1D1E1F20"`, 1),
		"zero owner identity":                strings.Replace(participant, `"ddl_owner_node":"1112131415161718191a1b1c1d1e1f20"`, `"ddl_owner_node":"00000000000000000000000000000000"`, 1),
		"owner without PG":                   strings.Replace(participant, `"pg_listen":"127.0.0.1:5433"`, `"pg_listen":""`, 1),
		"owner fields out of order": strings.Replace(participant,
			`"ddl_owner_address":"127.0.0.1:7801","ddl_owner_node":"1112131415161718191a1b1c1d1e1f20"`,
			`"ddl_owner_node":"1112131415161718191a1b1c1d1e1f20","ddl_owner_address":"127.0.0.1:7801"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseRF3Manifest([]byte(invalid)); !errors.Is(err, errInvalidRF3Manifest) {
				t.Fatalf("parse error = %v", err)
			}
		})
	}
}

func TestRF3GatewayIdentityDecodingKeepsFixedWidths(t *testing.T) {
	clientID, err := rf3GatewayID(strings.Repeat("1", 32), 16, false)
	if err != nil || clientID == (replication.ID128{}) {
		t.Fatalf("client id = %x, err=%v", clientID, err)
	}
	if _, err := rf3GatewayID(strings.Repeat("1", 30), 16, false); err == nil {
		t.Fatal("short client id accepted")
	}
	retry, err := rf3GatewayRetryHome(strings.Repeat("2", 16))
	if err != nil || retry == (replication.RetryHome{}) {
		t.Fatalf("retry home = %x, err=%v", retry, err)
	}
}

func TestRF3EmbeddedGatewayPeersAllowGlobalRosterSuperset(t *testing.T) {
	manifest, err := parseRF3Manifest([]byte(multiGroupRF3Manifest(t)))
	if err != nil {
		t.Fatal(err)
	}
	configured := []rf3ManifestGatewayPeer{
		{Address: "member-1.internal:7400", NodeID: "0102030405060708090a0b0c0d0e0f10"},
		{Address: "member-2.internal:7400", NodeID: "1112131415161718191a1b1c1d1e1f20"},
		{Address: "member-3.internal:7400", NodeID: "2122232425262728292a2b2c2d2e2f30"},
		{Address: "member-4.internal:7400", NodeID: "4142434445464748494a4b4c4d4e4f50"},
	}
	peers, err := rf3EmbeddedGatewayPeers(manifest, configured)
	if err != nil || len(peers) != len(configured) {
		t.Fatalf("global roster superset peers=%+v err=%v", peers, err)
	}
	if _, err := rf3EmbeddedGatewayPeers(manifest, configured[1:]); err == nil {
		t.Fatal("roster omitting a hosted member was accepted")
	}
	if _, err := rf3EmbeddedGatewayPeers(manifest, nil); err == nil {
		t.Fatal("gateway silently derived native addresses from the Raft roster")
	}
	manifest.Listeners.Native = "member-1.internal:7500"
	configured[0].Address = manifest.Listeners.Native
	peers, err = rf3EmbeddedGatewayPeers(manifest, configured)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRF3EmbeddedGatewayLocalNative(manifest, peers[0].Node, peers); err != nil {
		t.Fatalf("configured native endpoint was not accepted: %v", err)
	}
	configured[0].Address = "member-1.internal:7400"
	peers, err = rf3EmbeddedGatewayPeers(manifest, configured)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRF3EmbeddedGatewayLocalNative(manifest, peers[0].Node, peers); err == nil {
		t.Fatal("local Raft endpoint was accepted as the native endpoint")
	}
}

func TestServeNodeReloadConfigurationUsesProductionManifestPath(t *testing.T) {
	manifest := new(rf3Manifest)
	const path = "/srv/vibedb/node.vibejson"
	stop := configureRF3ManifestReload(manifest, path, true)
	defer stop()
	if manifest.reloadPath != path || manifest.reloadSignals == nil {
		t.Fatalf("reload configuration = path %q channel %v", manifest.reloadPath, manifest.reloadSignals)
	}
	withoutReload := new(rf3Manifest)
	if stop := configureRF3ManifestReload(withoutReload, path, false); withoutReload.reloadPath != "" || withoutReload.reloadSignals != nil {
		stop()
		t.Fatalf("disabled reload unexpectedly configured: path %q channel %v", withoutReload.reloadPath, withoutReload.reloadSignals)
	} else {
		stop()
	}
}

func TestRF3DiagnosticSnapshotWritesDurableNodeRootRecord(t *testing.T) {
	root := t.TempDir()
	manifest := rf3Manifest{
		NodeLog: &rf3NodeLogManifest{Path: filepath.Join(root, "node-log")},
		Groups:  []rf3ManifestGroup{{}, {}},
	}
	var serial atomic.Uint64
	emitRF3DiagnosticSnapshot(manifest, nil, nil, nil, nil, &serial, nil)
	path := filepath.Join(root, "rf3-diagnostics.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	line := strings.TrimSpace(string(raw))
	var decoded struct {
		Event  string `json:"event"`
		Serial uint64 `json:"serial"`
		Groups int    `json:"groups"`
	}
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		t.Fatalf("diagnostic JSON: %v (%q)", err, line)
	}
	if decoded.Event != "snapshot" || decoded.Serial != 1 || decoded.Groups != 2 {
		t.Fatalf("diagnostic record = %+v", decoded)
	}
	firstSize := len(raw)
	emitRF3DiagnosticSnapshot(manifest, nil, nil, nil, nil, &serial, nil)
	second, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) > 4<<10 || firstSize > 4<<10 || strings.Contains(string(second), "\n") {
		t.Fatalf("latest diagnostic is not one bounded record: size=%d first=%d", len(second), firstSize)
	}
	if err := json.Unmarshal(second, &decoded); err != nil || decoded.Serial != 2 {
		t.Fatalf("second diagnostic record = %+v err=%v", decoded, err)
	}
}

func TestRF3ServiceAdmissionWaitsForEveryListenerAndCancellation(t *testing.T) {
	var listeners []*rf3AcceptReadyListener
	for range 3 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		listeners = append(listeners, newRF3AcceptReadyListener(listener))
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := waitRF3ServiceAdmission(canceled, listeners...); !errors.Is(err, context.Canceled) {
		t.Fatalf("unstarted listener did not cancel: %v", err)
	}
	var accepts []chan error
	for _, listener := range listeners {
		done := make(chan error, 1)
		accepts = append(accepts, done)
		go func() { _, err := listener.Accept(); done <- err }()
	}
	ctx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if err := waitRF3ServiceAdmission(ctx, listeners...); err != nil {
		t.Fatal(err)
	}
	for index, listener := range listeners {
		_ = listener.Close()
		if err := <-accepts[index]; !errors.Is(err, net.ErrClosed) {
			t.Fatalf("listener close: %v", err)
		}
	}
}

func TestRF3DiagnosticGroupsCountLiveAuthorityUnion(t *testing.T) {
	root := t.TempDir()
	first, child := raftmember.GroupKey{GroupID: [16]byte{1}}, raftmember.GroupKey{GroupID: [16]byte{2}}
	manifest := rf3Manifest{NodeLog: &rf3NodeLogManifest{Path: filepath.Join(root, "node-log")},
		Groups: []rf3ManifestGroup{{Route: rf3ManifestGroupRoute{Group: first}}}}
	children := rf3NativeChildren{first: {Group: first}, child: {Group: child}}
	inventory := new(rf3AdoptedGroupInventory)
	inventory.nativeChildren.Store(&children)
	var serial atomic.Uint64
	emitRF3DiagnosticSnapshot(manifest, nil, nil, nil, nil, &serial, inventory)
	raw, err := os.ReadFile(filepath.Join(root, "rf3-diagnostics.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record rf3DiagnosticSnapshot
	if err := json.Unmarshal(raw, &record); err != nil || record.Groups != 2 {
		t.Fatalf("live groups=%d err=%v", record.Groups, err)
	}
}
