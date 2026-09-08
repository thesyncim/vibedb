//go:build linux && vibedb_rf3_read_authority_lab

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

// TestServeRF3ReadAuthorityLiveAppendAndRetainedRestart exercises the shipped
// physical-node owner with the laboratory authority policy enabled. The first
// reload installs one prepared group at a time, so the new group's Raft
// election remains quarantined until every voter has the exact prepared
// identity. It then withdraws and re-enrolls that exact suffix on a non-leader
// to exercise cache retirement and a fresh durable incarnation. The final
// restart reopens the retained marker and serves both groups through the same
// authority policy.
func TestServeRF3ReadAuthorityLiveAppendAndRetainedRestart(t *testing.T) {
	template := prepareRF3NodeTestInput(t)
	nodes, logical := rf3CommandNodes(), rf3CommandGroup()
	gatewayNode := rafttransport.NodeID{0xb1, 1}
	credentialRoot := t.TempDir()
	credentialNodes := append(append([]rafttransport.NodeID(nil), nodes[:]...), gatewayNode)
	credentials, roots, err := rf3testfixture.WriteCredentials(
		credentialRoot, rf3CommandIdentityOID,
		rafttransport.TrustDomain{ClusterID: logical.ClusterID, ClusterIncarnation: logical.ClusterIncarnation},
		credentialNodes,
	)
	if err != nil {
		t.Fatal(err)
	}
	policyPath := filepath.Join(credentialRoot, "authority-policy.vibejson")
	if err := os.WriteFile(policyPath, rf3ReadAuthorityPhysicalPolicy(nodes, gatewayNode), 0o600); err != nil {
		t.Fatal(err)
	}
	policy, err := serviceauthz.LoadFile(policyPath)
	if err != nil {
		t.Fatal(err)
	}

	var addresses [rf3CommandMembers][4]string
	var reservations [rf3CommandMembers]map[string]net.Listener
	for member := range rf3CommandMembers {
		reservations[member] = make(map[string]net.Listener, 4)
		for endpoint := range 4 {
			listener, listenErr := net.Listen("tcp", "127.0.0.1:0")
			if listenErr != nil {
				t.Fatal(listenErr)
			}
			addresses[member][endpoint] = listener.Addr().String()
			reservations[member][addresses[member][endpoint]] = listener
		}
	}
	closeReservations := func() {
		for _, listeners := range reservations {
			for address, listener := range listeners {
				if listener != nil {
					_ = listener.Close()
				}
				delete(listeners, address)
			}
		}
	}
	defer closeReservations()

	authority := testRF3ReadAuthorityConfig()
	authorityGeneration := rf3CommandAuthority().ActivePolicyGeneration
	var inputs [rf3CommandMembers]prepareRF3NodeManifest
	var manifests [rf3CommandMembers]rf3Manifest
	var profiles [rf3CommandMembers]*rafttransport.PeerTLS
	for member := range rf3CommandMembers {
		input := template
		input.Root = filepath.Join(filepath.Dir(template.Root), fmt.Sprintf("authority-server-%d", member))
		input.NodeLog.Path = filepath.Join(input.Root, "node-log")
		input.Groups = append([]prepareRF3Manifest(nil), template.Groups[:1]...)
		group := &input.Groups[0]
		group.Root = filepath.Join(input.Root, "group-0")
		// The initial group is the embedded gateway's replicated control-plane
		// shard.  Keep its prepared SQL binding identical to the catalog route;
		// the append below intentionally creates a separate data group.
		group.Shard = string(gateway.ReplicatedCatalogShard)
		group.Table = gateway.ReplicatedCatalogTable
		group.CreateTable = `CREATE TABLE controlplane (PRIMARY KEY (id))`
		group.MemberID = uint64(member + 1)
		identity := rf3CommandStoreIdentity(group.MemberID)
		group.StoreID = idString(identity.StoreID[:])
		group.Listeners = rf3ManifestListeners{
			Peer: addresses[member][0], Native: addresses[member][1],
			Snapshot: addresses[member][2], Control: addresses[member][3],
		}
		group.TLS = rf3ManifestTLS{
			Certificate: credentials[member].Certificate, Key: credentials[member].Key,
			Roots: roots, IdentityOID: rf3CommandIdentityOID.String(),
		}
		group.AuthorizationPolicy = policyPath
		config := *authority
		group.ReadAuthority = &config
		group.Members = append([]prepareRF3Member(nil), group.Members...)
		for ordinal := range group.Members {
			memberIdentity := rf3CommandStoreIdentity(uint64(ordinal + 1))
			group.Members[ordinal].PeerAddress = addresses[ordinal][0]
			group.Members[ordinal].NativeAddress = addresses[ordinal][1]
			group.Members[ordinal].StoreID = idString(memberIdentity.StoreID[:])
		}
		inputs[member] = input
		if err := provisionRF3Node(input); err != nil {
			t.Fatal(err)
		}
		manifests[member], err = loadRF3Manifest(filepath.Join(input.Root, "serve-rf3.vibejson"))
		if err != nil {
			t.Fatal(err)
		}
		if err := validateRF3ReadAuthority(manifests[member].ReadAuthority, manifests[member].groupBundles(), false); err != nil {
			t.Fatalf("member %d persisted read-authority manifest: %v (members=%+v)", member+1, err, manifests[member].Groups[0].Members)
		}
		profiles[member], err = servicetls.LoadProfile(
			credentials[member].Certificate, credentials[member].Key, roots,
			rf3CommandIdentityOID.String(), time.Now,
		)
		if err != nil {
			t.Fatal(err)
		}
		if got := policy.Check(profiles[member].LocalIdentity().Node, serviceauthz.CapabilityDataRead); got != serviceauthz.DecisionAllow {
			t.Fatalf("member %d authority policy data-read decision = %v", member+1, got)
		}
		if got := policy.Check(profiles[member].LocalIdentity().Node, serviceauthz.CapabilityDelegate); got != serviceauthz.DecisionAllow {
			t.Fatalf("member %d authority policy delegate decision = %v", member+1, got)
		}
	}
	gatewayProfile, err := servicetls.LoadProfile(
		credentials[3].Certificate, credentials[3].Key, roots,
		rf3CommandIdentityOID.String(), time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	var base sqldriver.ReplicatedShardStoreIdentity
	if err := loadRF3IdentityFile(manifests[0].Groups[0].SQL.IdentityPath, &base); err != nil {
		t.Fatal(err)
	}
	logicalSchema, err := sqldriver.ReplicatedRelationManifestDigest(base)
	if err != nil {
		t.Fatal(err)
	}
	servingSchema := rf3ReadAuthorityServingSchemaDigest(t, manifests[0], profiles[0])
	catalogRoot := filepath.Join(filepath.Dir(template.Root), "authority-catalog")
	if err := os.MkdirAll(catalogRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	catalogPath := filepath.Join(catalogRoot, "catalog.vibejson")
	if err := gateway.SaveSnapshot(catalogPath, rf3ReadAuthorityCatalogSnapshot(t, manifests, addresses, nodes, logicalSchema, servingSchema)); err != nil {
		t.Fatal(err)
	}
	for member := range rf3CommandMembers {
		gatewayManifest := rf3ReadAuthorityGatewayManifest(
			catalogRoot, catalogPath, member, credentials[3], roots, policyPath,
			addresses, nodes,
		)
		if err := os.MkdirAll(filepath.Dir(gatewayManifest.CatalogRouteSeedPath), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(gatewayManifest.DurableAckKeyPath,
			[]byte(hex.EncodeToString(bytes.Repeat([]byte{byte(member + 1)}, 32))), 0o600); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(inputs[member].Root, "serve-rf3.vibejson")
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		var persisted persistedRF3NodeRuntime
		if err := vibejson.Unmarshal(raw, &persisted); err != nil {
			t.Fatal(err)
		}
		persisted.Gateway = &gatewayManifest
		raw, err = vibejson.Marshal(&persisted)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		manifests[member], err = loadRF3Manifest(path)
		if err != nil {
			t.Fatal(err)
		}
	}

	startNodes := func(parent context.Context) ([rf3CommandMembers]chan os.Signal, [rf3CommandMembers]chan os.Signal, chan struct {
		member int
		err    error
	}, chan error) {
		var reloads [rf3CommandMembers]chan os.Signal
		var diagnostics [rf3CommandMembers]chan os.Signal
		done := make(chan struct {
			member int
			err    error
		}, rf3CommandMembers)
		startupErrors := make(chan error, rf3CommandMembers)
		for member := range rf3CommandMembers {
			reloads[member] = make(chan os.Signal, 1)
			diagnostics[member] = make(chan os.Signal, 1)
			serving := manifests[member]
			serving.reloadPath = filepath.Join(inputs[member].Root, "serve-rf3.vibejson")
			serving.reloadSignals = reloads[member]
			listeners := reservations[member]
			go func(member int, serving rf3Manifest, listeners map[string]net.Listener, diagnostic <-chan os.Signal) {
				err := servePreparedRF3WithEmbeddedGatewayAndDiagnostics(parent, serving, rf3DefaultExecutionLanes,
					func(network, address string) (net.Listener, error) {
						if network != "tcp" {
							return nil, errors.New("unexpected RF3 listener network")
						}
						listener := listeners[address]
						if listener == nil {
							return nil, fmt.Errorf("unexpected RF3 listener %q", address)
						}
						delete(listeners, address)
						return listener, nil
					}, diagnostic)
				if err == nil {
					startupErrors <- fmt.Errorf("authority node %d stopped before leader election", member+1)
				} else if !errors.Is(err, context.Canceled) {
					startupErrors <- fmt.Errorf("authority node %d failed before leader election: %w", member+1, err)
				}
				done <- struct {
					member int
					err    error
				}{member: member, err: err}
			}(member, serving, listeners, diagnostics[member])
		}
		return reloads, diagnostics, done, startupErrors
	}

	stopNodes := func(cancel context.CancelFunc, done chan struct {
		member int
		err    error
	}) {
		cancel()
		for range rf3CommandMembers {
			select {
			case result := <-done:
				if result.err != nil && !errors.Is(result.err, context.Canceled) {
					t.Errorf("authority node %d shutdown: %v", result.member+1, result.err)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("authority node did not shut down")
			}
		}
		closeReservations()
	}

	ctx, cancel := context.WithCancel(t.Context())
	reloads, diagnostics, done, startupErrors := startNodes(ctx)
	defer func() {
		if ctx != nil {
			stopNodes(cancel, done)
			ctx, cancel = nil, nil
		}
	}()
	rf3WaitForGatewayAvailable(t, diagnostics, [rf3CommandMembers]string{
		inputs[0].Root, inputs[1].Root, inputs[2].Root,
	}, startupErrors)
	for _, bundle := range manifests[0].Groups {
		waitRF3CommandLeader(t, [rf3CommandMembers]string{addresses[0][1], addresses[1][1], addresses[2][1]}, nodes, profiles[:], bundle.Route.Group, bundle.Route.AllocationGeneration, authorityGeneration, startupErrors)
	}
	oldGroup := manifests[0].Groups[0].Route.Group
	oldAllocation := manifests[0].Groups[0].Route.AllocationGeneration
	if _, err := probeRF3CommandMember(t.Context(), addresses[0][1], nodes[0], profiles[1], nodes[1], oldGroup, oldAllocation, authorityGeneration); err != nil {
		t.Fatalf("initial old group probe: %v", err)
	}

	// Install the prepared suffix on one voter at a time. The first installation
	// is deliberately observed before another voter can join: one voter cannot
	// elect. Later partial rosters may form a normal Raft quorum, so their
	// election state is observed without assuming that all three registrations
	// are required for Raft leadership.
	for member := range rf3CommandMembers {
		manifests[member] = appendRF3LiveNodeTestGroup(t, inputs[member], manifests[member], "docs_live")
		reloads[member] <- syscall.SIGHUP
		newGroup := manifests[member].Groups[1].Route.Group
		newAllocation := manifests[member].Groups[1].Route.AllocationGeneration
		waitRF3AuthorityGroupPresent(t, addresses[member][1], nodes[member], profiles[(member+1)%rf3CommandMembers], nodes[(member+1)%rf3CommandMembers], newGroup, newAllocation, authorityGeneration)
		if member == 0 {
			state, err := probeRF3CommandMember(t.Context(), addresses[member][1], nodes[member], profiles[(member+1)%rf3CommandMembers], nodes[(member+1)%rf3CommandMembers], newGroup, newAllocation, authorityGeneration)
			if err != nil {
				t.Fatalf("initially quarantined group probe: %v", err)
			}
			if state.LeaderID != 0 {
				t.Fatalf("single-voter group %x elected leader %d before another registration", newGroup.GroupID, state.LeaderID)
			}
		}
		waitRF3CommandLeader(t, [rf3CommandMembers]string{addresses[0][1], addresses[1][1], addresses[2][1]}, nodes, profiles[:], oldGroup, oldAllocation, authorityGeneration, startupErrors)
	}

	newGroup := manifests[0].Groups[1].Route.Group
	newAllocation := manifests[0].Groups[1].Route.AllocationGeneration
	waitRF3CommandLeader(t, [rf3CommandMembers]string{addresses[0][1], addresses[1][1], addresses[2][1]}, nodes, profiles[:], newGroup, newAllocation, authorityGeneration, startupErrors)
	leader, state := rf3CommandFindLeader(t, []string{addresses[0][1], addresses[1][1], addresses[2][1]}, nodes[:], gatewayProfile, gatewayNode, newGroup, newAllocation, authorityGeneration)
	newBundle := manifests[leader].Groups[1]
	readAuthority := serviceauthz.Authority{Node: nodes[(leader+1)%rf3CommandMembers], Generation: authorityGeneration}
	state, beforeRead, err := rf3WaitForReadAuthoritySQLReady(t, addresses[leader][1], nodes[leader], profiles[(leader+1)%rf3CommandMembers], newBundle, state, readAuthority, diagnostics[leader], inputs[leader].Root, newGroup)
	if err != nil {
		t.Fatalf("new group did not publish usable authority evidence after SQL warm reads: %v", err)
	}
	read := rf3CommandRoundTrip(t, addresses[leader][1], nodes[leader], profiles[(leader+1)%rf3CommandMembers], rf3ReadAuthoritySQLRequest(t, newBundle, state, readAuthority))
	rf3AssertReadAuthoritySQLResponse(t, read, "new group")
	// The catalog probes used while this fixture starts use CapabilityTopology;
	// the data-read SQL above is the only CapabilityDataRead request in this
	// interval, so the process-wide hit delta is evidence for this appended
	// group rather than background catalog activity.
	if _, ok := waitRF3ReadAuthoritySnapshot(t, diagnostics[leader], inputs[leader].Root, 3, beforeRead.AuthorityReadHits+1, newGroup); !ok {
		t.Fatal("new group did not publish a used authority round after live append")
	}

	// Withdraw the appended group from a non-leader through the production
	// reload path, then restore the exact prepared suffix. The owner must
	// unregister the old cache generation before the same group is enrolled
	// again, which causes adoption to publish a fresh durable incarnation.
	retireMember := (leader + 1) % rf3CommandMembers
	retiredPath := filepath.Join(inputs[retireMember].Root, "serve-rf3.vibejson")
	retiredBefore, err := rf3ReadAuthorityDiagnosticSnapshot(t, diagnostics[retireMember], inputs[retireMember].Root)
	if err != nil {
		t.Fatalf("capture appended runtime identity before retirement: %v", err)
	}
	retiredIncarnation, ok := rf3ReadAuthorityNodeIncarnation(retiredBefore, newGroup)
	if !ok {
		t.Fatalf("appended runtime identity missing before retirement: %+v", retiredBefore.ReadAuthorityEvidence)
	}
	retiredRaw, err := os.ReadFile(retiredPath)
	if err != nil {
		t.Fatal(err)
	}
	var retiredRuntime persistedRF3NodeRuntime
	if err := vibejson.Unmarshal(retiredRaw, &retiredRuntime); err != nil {
		t.Fatal(err)
	}
	if len(retiredRuntime.Groups) != 2 {
		t.Fatalf("retirement fixture groups = %d, want two", len(retiredRuntime.Groups))
	}
	retiredRuntime.Groups = append([]persistedRF3NodeGroup(nil), retiredRuntime.Groups[:1]...)
	withdrawnRaw, err := vibejson.Marshal(&retiredRuntime)
	if err != nil {
		t.Fatal(err)
	}
	manifests[retireMember] = rf3ReplaceServingManifest(t, retiredPath, withdrawnRaw)
	reloads[retireMember] <- syscall.SIGHUP
	retireAuthority := nodes[(retireMember+1)%rf3CommandMembers]
	waitRF3AuthorityGroupAbsent(t, addresses[retireMember][1], nodes[retireMember], profiles[(retireMember+1)%rf3CommandMembers], retireAuthority, newGroup, newAllocation, authorityGeneration)
	waitRF3CommandLeader(t, [rf3CommandMembers]string{addresses[0][1], addresses[1][1], addresses[2][1]}, nodes, profiles[:], oldGroup, oldAllocation, authorityGeneration, startupErrors)

	manifests[retireMember] = rf3ReplaceServingManifest(t, retiredPath, retiredRaw)
	reloads[retireMember] <- syscall.SIGHUP
	waitRF3AuthorityGroupPresent(t, addresses[retireMember][1], nodes[retireMember], profiles[(retireMember+1)%rf3CommandMembers], retireAuthority, newGroup, newAllocation, authorityGeneration)
	waitRF3CommandLeader(t, [rf3CommandMembers]string{addresses[0][1], addresses[1][1], addresses[2][1]}, nodes, profiles[:], newGroup, newAllocation, authorityGeneration, startupErrors)
	if incarnation := rf3WaitForRF3ReadAuthorityNodeIncarnation(t, diagnostics[retireMember], inputs[retireMember].Root, newGroup, retiredIncarnation); incarnation <= retiredIncarnation {
		t.Fatalf("re-enrolled group incarnation = %d, want > %d", incarnation, retiredIncarnation)
	}

	stopNodes(cancel, done)
	ctx, cancel = context.WithCancel(t.Context())
	closeReservations()
	for member := range rf3CommandMembers {
		if err := os.Remove(filepath.Join(inputs[member].Root, "rf3-diagnostics.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		reservations[member] = make(map[string]net.Listener, 4)
		for endpoint := range 4 {
			listener, listenErr := net.Listen("tcp", addresses[member][endpoint])
			if listenErr != nil {
				t.Fatal(listenErr)
			}
			reservations[member][addresses[member][endpoint]] = listener
		}
		var loadErr error
		manifests[member], loadErr = loadRF3Manifest(filepath.Join(inputs[member].Root, "serve-rf3.vibejson"))
		if loadErr != nil {
			t.Fatal(loadErr)
		}
	}
	reloads, diagnostics, done, startupErrors = startNodes(ctx)
	rf3WaitForGatewayAvailable(t, diagnostics, [rf3CommandMembers]string{
		inputs[0].Root, inputs[1].Root, inputs[2].Root,
	}, startupErrors)
	for _, bundle := range manifests[0].Groups {
		waitRF3CommandLeader(t, [rf3CommandMembers]string{addresses[0][1], addresses[1][1], addresses[2][1]}, nodes, profiles[:], bundle.Route.Group, bundle.Route.AllocationGeneration, authorityGeneration, startupErrors)
	}
	restartLeader, restartState := rf3CommandFindLeader(t, []string{addresses[0][1], addresses[1][1], addresses[2][1]}, nodes[:], gatewayProfile, gatewayNode, newGroup, newAllocation, authorityGeneration)
	restartBundle := manifests[restartLeader].Groups[1]
	restartAuthority := serviceauthz.Authority{Node: nodes[(restartLeader+1)%rf3CommandMembers], Generation: authorityGeneration}
	restartState, beforeRestartRead, err := rf3WaitForReadAuthoritySQLReady(t, addresses[restartLeader][1], nodes[restartLeader], profiles[(restartLeader+1)%rf3CommandMembers], restartBundle, restartState, restartAuthority, diagnostics[restartLeader], inputs[restartLeader].Root, newGroup)
	if err != nil {
		t.Fatalf("retained restart did not publish usable authority evidence after SQL warm reads: %v", err)
	}
	restartRead := rf3CommandRoundTrip(t, addresses[restartLeader][1], nodes[restartLeader], profiles[(restartLeader+1)%rf3CommandMembers], rf3ReadAuthoritySQLRequest(t, restartBundle, restartState, restartAuthority))
	rf3AssertReadAuthoritySQLResponse(t, restartRead, "retained restart")
	if _, ok := waitRF3ReadAuthoritySnapshot(t, diagnostics[restartLeader], inputs[restartLeader].Root, 3, beforeRestartRead.AuthorityReadHits+1, newGroup); !ok {
		t.Fatal("retained restart did not publish a used authority round for both groups")
	}
	stopNodes(cancel, done)
	ctx, cancel = nil, nil
}

func TestRF3ReadAuthorityConfigureFailureUnregistersCache(t *testing.T) {
	input := prepareRF3NodeTestInput(t)
	input.Groups = append([]prepareRF3Manifest(nil), input.Groups[:1]...)
	input.Groups[0].Members = append([]prepareRF3Member(nil), input.Groups[0].Members...)
	for member := range input.Groups[0].Members {
		store := rf3CommandStoreIdentity(input.Groups[0].Members[member].MemberID).StoreID
		input.Groups[0].Members[member].StoreID = idString(store[:])
		input.Groups[0].Members[member].NativeAddress = fmt.Sprintf("127.0.0.1:%d", 25000+member)
	}
	if err := provisionRF3Node(input); err != nil {
		t.Fatal(err)
	}
	manifest, err := loadRF3Manifest(filepath.Join(input.Root, "serve-rf3.vibejson"))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := servicetls.LoadProfile(
		manifest.TLS.Certificate, manifest.TLS.Key, manifest.TLS.Roots,
		manifest.TLS.IdentityOID, time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := openRF3NodeOwner(manifest, profile)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Close() }()
	set, err := prepareRF3GroupSetOnNode(manifest, profile, sqldriver.ReplicatedOpenOptions{}, owner)
	if err != nil {
		t.Fatal(err)
	}
	if len(set.groups) != 1 {
		t.Fatalf("prepared groups = %d, want one", len(set.groups))
	}
	item := &set.groups[0]
	runtime, err := item.adoptRuntime()
	if err != nil {
		t.Fatal(err)
	}
	identity := runtime.Identity()
	item.manifest.ReadAuthority = testRF3ReadAuthorityConfig()
	cache := testRF3ReadAuthorityCache(identity.NodeIncarnation)
	cache.localNode = profile.LocalIdentity().Node
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := configureRF3ReadAuthorityGroup(item.manifest, *item, runtime, cache); !errors.Is(err, raftmember.ErrRuntimeClosed) {
		t.Fatalf("closed Runtime configuration error = %v, want ErrRuntimeClosed", err)
	}
	cache.mu.RLock()
	targets, values, groups, registrations := len(cache.targets), len(cache.values), len(cache.groups), len(cache.registrations)
	cache.mu.RUnlock()
	if targets != 0 || values != 0 || groups != 0 || registrations != 0 {
		t.Fatalf("failed configuration retained cache state: targets=%d values=%d groups=%d registrations=%d", targets, values, groups, registrations)
	}
	if _, err := os.Stat(rf3ReadAuthorityMarkerPath(item.manifest.Route.MemberRoot)); err != nil {
		t.Fatalf("failed configuration removed durable marker: %v", err)
	}
	targetsForRetry, err := rf3ReadAuthorityGroupTargetsForPrepared(*item, identity)
	if err != nil {
		t.Fatal(err)
	}
	retryRegistrations, err := cache.RegisterGroups([]rf3ReadAuthorityGroupTargets{targetsForRetry})
	if err != nil || len(retryRegistrations) != 1 {
		t.Fatalf("same-group cache retry = %#v, %v", retryRegistrations, err)
	}
	if err := cache.UnregisterGroups(retryRegistrations); err != nil {
		t.Fatal(err)
	}
}

func rf3ReplaceServingManifest(t testing.TB, path string, raw []byte) rf3Manifest {
	t.Helper()
	staged := path + ".next"
	if err := writePrepareRF3File(staged, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(staged, path); err != nil {
		t.Fatal(err)
	}
	manifest, err := loadRF3Manifest(path)
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func rf3ReadAuthorityDiagnosticSnapshot(t testing.TB, signal chan<- os.Signal, root string) (rf3DiagnosticSnapshot, error) {
	t.Helper()
	path := filepath.Join(root, "rf3-diagnostics.json")
	var previous rf3DiagnosticSnapshot
	if raw, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(raw, &previous); err != nil {
			return previous, fmt.Errorf("decode diagnostic snapshot: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return previous, fmt.Errorf("read diagnostic snapshot: %w", err)
	}
	wanted := previous.Serial + 1
	select {
	case signal <- syscall.SIGUSR1:
	case <-time.After(5 * time.Second):
		return previous, errors.New("diagnostic snapshot signal blocked")
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			var snapshot rf3DiagnosticSnapshot
			if err := json.Unmarshal(raw, &snapshot); err != nil {
				return previous, fmt.Errorf("decode diagnostic snapshot after signal: %w", err)
			}
			if snapshot.Serial >= wanted {
				return snapshot, nil
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return previous, fmt.Errorf("read diagnostic snapshot after signal: %w", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return previous, fmt.Errorf("diagnostic snapshot serial did not reach %d", wanted)
}

func rf3ReadAuthorityNodeIncarnation(snapshot rf3DiagnosticSnapshot, group raftmember.GroupKey) (uint64, bool) {
	wantedGroup := rf3DiagnosticAuthorityGroupIdentityJSON(authorityGroupIdentity(group))
	for _, evidence := range snapshot.ReadAuthorityEvidence {
		if evidence.RuntimeIdentity.Group == wantedGroup && evidence.RuntimeIdentity.NodeIncarnation != 0 {
			return evidence.RuntimeIdentity.NodeIncarnation, true
		}
	}
	return 0, false
}

func rf3WaitForRF3ReadAuthorityNodeIncarnation(
	t *testing.T, signal chan<- os.Signal, root string, group raftmember.GroupKey, prior uint64,
) uint64 {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := rf3ReadAuthorityDiagnosticSnapshot(t, signal, root)
		if err == nil {
			if incarnation, ok := rf3ReadAuthorityNodeIncarnation(snapshot, group); ok && incarnation > prior {
				return incarnation
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("re-enrolled group %x did not publish a newer runtime incarnation than %d", group.GroupID, prior)
	return 0
}

func waitRF3AuthorityGroupAbsent(
	t testing.TB, address string, node rafttransport.NodeID, profile *rafttransport.PeerTLS,
	authorityNode rafttransport.NodeID, group raftmember.GroupKey, allocation, generation uint64,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		if _, err := probeRF3CommandMember(ctx, address, node, profile, authorityNode, group, allocation, generation); err != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("RF3 retired group %x remained published", group.GroupID)
}

func rf3ReadAuthorityCatalogSnapshot(
	t testing.TB, manifests [rf3CommandMembers]rf3Manifest,
	addresses [rf3CommandMembers][4]string, nodes [rf3CommandMembers]rafttransport.NodeID,
	logicalSchema, servingSchema [32]byte,
) *gateway.Snapshot {
	t.Helper()
	authority := rf3CommandAuthority()
	command := commandFenceFromPublication(authority, raftmember.RuntimeIdentity{
		RelationManifestDigest: servingSchema,
	}, 1)
	endpointNames := []distribution.EndpointID{"member-1", "member-2", "member-3"}
	logicalManifest, err := distribution.NewManifest(
		gateway.ReplicatedCatalogDistribution,
		distribution.RoutingVersion(command.RoutingVersion), []distribution.Shard{{
			ID: gateway.ReplicatedCatalogShard,
			AllocationGeneration: distribution.ShardAllocationGeneration(
				manifests[0].Groups[0].Route.AllocationGeneration),
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
			Leaders: endpointNames, Epoch: distribution.OwnershipEpoch(command.OwnershipEpoch),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	endpoints := make(map[distribution.EndpointID]string, rf3CommandMembers*3)
	replicas := make([]gateway.ReplicatedReplicaDescriptor, rf3CommandMembers)
	for member := range rf3CommandMembers {
		endpoint := endpointNames[member]
		native := distribution.EndpointID(fmt.Sprintf("member-%d-native", member+1))
		control := distribution.EndpointID(fmt.Sprintf("member-%d-control", member+1))
		endpoints[endpoint] = addresses[member][0]
		endpoints[native] = addresses[member][1]
		endpoints[control] = addresses[member][3]
		store := rf3CommandStoreIdentity(uint64(member + 1)).StoreID
		replicas[member] = gateway.ReplicatedReplicaDescriptor{
			Member: uint64(member + 1), Node: nodes[member], StoreID: store,
			NodeIncarnation: 1, Endpoint: endpoint, NativeEndpoint: native,
			ControlEndpoint: control,
		}
	}
	group := manifests[0].Groups[0].Route.Group
	descriptor := gateway.ReplicatedShardDescriptor{
		Distribution: gateway.ReplicatedCatalogDistribution, Shard: gateway.ReplicatedCatalogShard,
		Group: group, AllocationGeneration: distribution.ShardAllocationGeneration(
			manifests[0].Groups[0].Route.AllocationGeneration),
		Command: command, LogicalSchemaDigest: replication.Digest(logicalSchema),
		RangeIdentity: replication.Digest{0x71}, LineageDigest: replication.Digest{0x72},
		ForwardingRuleDigest: replication.Digest{0x73},
		RequestLedgerRanges:  []gateway.DurableRequestLedgerRangeDescriptor{{Identity: replication.Digest{0x91}}},
		Replicas:             replicas,
	}
	profile := gateway.ReplicatedTableProfile{
		Table: gateway.ReplicatedCatalogTable, Relation: 1,
		PrimaryKey: gateway.ReplicatedCatalogPrimaryKey, SchemaGeneration: command.SchemaGeneration,
		LogicalSchemaDigest: replication.Digest(logicalSchema), MaxKeyBytes: 256,
		MaxDocumentBytes: 4 << 20,
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedTableMetadata(
		distribution.ClusterConfig{
			Distributions: []distribution.DistributionSpec{{
				Name: gateway.ReplicatedCatalogDistribution, Arity: 1,
				MapperVersion: distribution.NativeMapperVersion,
			}},
			Placements: []distribution.TablePlacement{{
				Table: gateway.ReplicatedCatalogTable, Distribution: gateway.ReplicatedCatalogDistribution,
				Columns: []string{gateway.ReplicatedCatalogPrimaryKey},
			}},
			Manifests: []*distribution.Manifest{logicalManifest},
		}, endpoints, 1, nil, nil,
		[]gateway.ReplicatedShardDescriptor{descriptor},
		[]gateway.ReplicatedTableProfile{profile},
	)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// rf3ReadAuthorityServingSchemaDigest reads the machine schema digest from a
// prepared group. The SQL identity's relation digest is the portable logical
// schema commitment used in catalog metadata; the command fence must carry the
// exact serving-machine digest produced by the apply state machine.
func rf3ReadAuthorityServingSchemaDigest(
	t testing.TB, manifest rf3Manifest, profile *rafttransport.PeerTLS,
) [32]byte {
	t.Helper()
	owner, err := openRF3NodeOwner(manifest, profile)
	if err != nil {
		t.Fatalf("open RF3 node owner for serving schema digest: %v", err)
	}
	set, err := prepareRF3GroupSetOnNode(manifest, profile, sqldriver.ReplicatedOpenOptions{}, owner)
	if err != nil {
		_ = owner.Close()
		t.Fatalf("open RF3 prepared group for serving schema digest: %v", err)
	}
	var digest [32]byte
	if len(set.groups) == 0 || set.groups[0].apply == nil {
		err = errors.New("RF3 prepared group has no apply handle")
	} else {
		digest, err = set.groups[0].apply.RangeSplitRelationManifestDigest()
	}
	err = errors.Join(err, closePreparedRF3Groups(set.groups, nil), owner.Close())
	if err != nil {
		t.Fatalf("read RF3 serving schema digest: %v", err)
	}
	return digest
}

func rf3ReadAuthorityGatewayManifest(
	root, catalogPath string, member int, credential rf3testfixture.Credential,
	roots, policyPath string, addresses [rf3CommandMembers][4]string,
	nodes [rf3CommandMembers]rafttransport.NodeID,
) rf3ManifestGateway {
	base := filepath.Join(root, fmt.Sprintf("gateway-%d", member))
	return rf3ManifestGateway{
		CatalogPath: catalogPath, CatalogRouteSeedPath: filepath.Join(base, "catalog-route-seed"),
		CatalogBootstrapIfMissing: true, CatalogRelation: 1, CatalogAttempts: 8,
		CatalogAttemptTimeoutMillis: 1000, CatalogSessionLeaseMillis: uint64((24 * time.Hour) / time.Millisecond),
		CatalogSessionJournal: filepath.Join(base, "catalog-session"),
		CatalogClientID:       fmt.Sprintf("%032x", member+1), CatalogRetryHome: fmt.Sprintf("%016x", member+1),
		DurableAckKeyPath: filepath.Join(base, "durable-ack.key"), ListenAddress: "127.0.0.1:0",
		TLS:                 rf3ManifestTLS{Certificate: credential.Certificate, Key: credential.Key, Roots: roots, IdentityOID: rf3CommandIdentityOID.String()},
		AuthorizationPolicy: policyPath, MaxConnections: 8, MaxHandshakes: 4,
		MaxShardConnections: 8, MaxShardHandshakes: 4, MaxNativeReadConcurrency: 8,
		MaxNativeReadBytes: 4 << 20, MaxNativeScatterConcurrency: 8,
		TableCatalogs: []string{}, ShardPeers: []rf3ManifestGatewayPeer{
			{Address: addresses[0][1], NodeID: fmt.Sprintf("%x", nodes[0])},
			{Address: addresses[1][1], NodeID: fmt.Sprintf("%x", nodes[1])},
			{Address: addresses[2][1], NodeID: fmt.Sprintf("%x", nodes[2])},
		},
	}
}

func rf3ReadAuthorityPhysicalPolicy(nodes [rf3CommandMembers]rafttransport.NodeID, gatewayNode rafttransport.NodeID) []byte {
	return []byte(fmt.Sprintf(
		`{"generation":5,"principals":[{"node":"%x","capabilities":["data_read","data_write","delegate","membership","topology","transaction_recovery","request_ledger","execution_pin"]},{"node":"%x","capabilities":["data_read","data_write","delegate","membership","topology","transaction_recovery","request_ledger","execution_pin"]},{"node":"%x","capabilities":["data_read","data_write","delegate","membership","topology","transaction_recovery","request_ledger","execution_pin"]},{"node":"%x","capabilities":["data_read","data_write","delegate","membership","topology","transaction_recovery","request_ledger","execution_pin"]}]}`,
		nodes[0], nodes[1], nodes[2], gatewayNode,
	))
}

func waitRF3AuthorityGroupPresent(
	t testing.TB, address string, node rafttransport.NodeID, profile *rafttransport.PeerTLS,
	authorityNode rafttransport.NodeID, group raftmember.GroupKey, allocation, generation uint64,
) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := probeRF3CommandMember(context.Background(), address, node, profile, authorityNode, group, allocation, generation); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("RF3 live appended group %x was not published", group.GroupID)
}

func rf3WaitForGatewayAvailable(
	t testing.TB,
	diagnostics [rf3CommandMembers]chan os.Signal,
	roots [rf3CommandMembers]string,
	startupErrors <-chan error,
) {
	t.Helper()
	// Use the existing startup/readiness bound for the whole three-node wait,
	// rather than giving each member an independent timeout.
	ctx, cancel := context.WithTimeout(t.Context(), rf3AuthoritySQLReadinessTimeout)
	defer cancel()

	var snapshots [rf3CommandMembers]rf3DiagnosticSnapshot
	var wantedSerial [rf3CommandMembers]uint64
	var signalPending [rf3CommandMembers]bool
	var ready [rf3CommandMembers]bool
	var nextSignal [rf3CommandMembers]time.Time
	for member := range rf3CommandMembers {
		path := filepath.Join(roots[member], "rf3-diagnostics.json")
		if raw, err := os.ReadFile(path); err == nil {
			if err := json.Unmarshal(raw, &snapshots[member]); err != nil {
				t.Fatalf("decode RF3 gateway readiness diagnostic for member %d: %v", member+1, err)
			}
			wantedSerial[member] = snapshots[member].Serial + 1
		} else if errors.Is(err, os.ErrNotExist) {
			wantedSerial[member] = 1
		} else {
			t.Fatalf("read RF3 gateway readiness diagnostic for member %d: %v", member+1, err)
		}
		signalPending[member] = true
	}

	checkStartupError := func() {
		select {
		case err := <-startupErrors:
			if err == nil {
				t.Fatalf("RF3 server exited before embedded gateway readiness")
			}
			t.Fatalf("RF3 server failed before embedded gateway readiness: %v", err)
		default:
		}
	}

	for {
		checkStartupError()
		allReady := true
		for member := range rf3CommandMembers {
			if ready[member] {
				continue
			}
			allReady = false
			if signalPending[member] && !time.Now().Before(nextSignal[member]) {
				select {
				case diagnostics[member] <- syscall.SIGUSR1:
					signalPending[member] = false
					nextSignal[member] = time.Now().Add(rf3GatewayDiagnosticInterval)
				case err := <-startupErrors:
					if err == nil {
						t.Fatalf("RF3 server exited before embedded gateway readiness")
					}
					t.Fatalf("RF3 server failed before embedded gateway readiness: %v", err)
				case <-ctx.Done():
					t.Fatalf("RF3 embedded gateway readiness timeout: %v", ctx.Err())
				default:
				}
			}

			path := filepath.Join(roots[member], "rf3-diagnostics.json")
			raw, err := os.ReadFile(path)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("read RF3 gateway readiness diagnostic for member %d: %v", member+1, err)
				}
				continue
			}
			var snapshot rf3DiagnosticSnapshot
			if err := json.Unmarshal(raw, &snapshot); err != nil {
				t.Fatalf("decode RF3 gateway readiness diagnostic for member %d: %v", member+1, err)
			}
			if snapshot.Serial < wantedSerial[member] {
				continue
			}
			snapshots[member] = snapshot
			signalPending[member] = false
			if snapshot.GatewayAvailable {
				ready[member] = true
			} else {
				wantedSerial[member] = snapshot.Serial + 1
				signalPending[member] = true
				nextSignal[member] = time.Now().Add(rf3GatewayDiagnosticInterval)
			}
		}
		if allReady {
			if err := ctx.Err(); err != nil {
				t.Fatalf("RF3 embedded gateway readiness timeout: %v; snapshots=%+v", err, snapshots)
			}
			checkStartupError()
			return
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("RF3 embedded gateway readiness timeout: %v; snapshots=%+v", ctx.Err(), snapshots)
		}
	}
}

const (
	rf3AuthoritySQLReadinessTimeout  = 15 * time.Second
	rf3AuthoritySQLReadinessAttempts = 64
	rf3GatewayDiagnosticInterval     = 250 * time.Millisecond
)

func rf3ReadAuthoritySQLRequest(
	t testing.TB, bundle rf3ManifestGroup, state shardservice.ReplicatedMemberState,
	authority serviceauthz.Authority,
) *shardservice.ReplicatedRequest {
	t.Helper()
	primaryKeyRead := rf3ReadAuthorityPrimaryKeyRead(t, bundle)
	request := &shardservice.ShardRequest{
		Authority:            authority,
		SQL:                  `SELECT id FROM docs_live WHERE id = 'authority-live-append'`,
		Distribution:         distribution.DistributionName(bundle.Route.Distribution),
		Shard:                distribution.ShardID(bundle.Route.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(bundle.Route.AllocationGeneration),
		RoutingVersion:       distribution.RoutingVersion(state.Fence.Command.RoutingVersion),
		OwnershipEpoch:       distribution.OwnershipEpoch(state.Fence.Command.OwnershipEpoch),
		ReadPolicy:           shardservice.ReadStrong,
		ExecutionMode:        shardservice.ExecutionReadOnly,
		MaxRows:              1,
		MaxResultBytes:       4096,
		PrimaryKeyRead:       primaryKeyRead,
	}
	var query bytes.Buffer
	if err := shardservice.EncodeRequest(&query, request); err != nil {
		t.Fatalf("encode live authority SQL query: %v", err)
	}
	decoded, err := shardservice.DecodeRequest(bytes.NewReader(query.Bytes()))
	if err != nil {
		t.Fatalf("decode live authority SQL query: %v", err)
	}
	if decoded.PrimaryKeyRead.Relation != primaryKeyRead.Relation ||
		decoded.PrimaryKeyRead.MaxDocumentBytes != primaryKeyRead.MaxDocumentBytes ||
		!bytes.Equal(decoded.PrimaryKeyRead.PrimaryPath, primaryKeyRead.PrimaryPath) ||
		len(decoded.PrimaryKeyRead.Keys) != 1 ||
		!bytes.Equal(decoded.PrimaryKeyRead.Keys[0], primaryKeyRead.Keys[0]) {
		t.Fatalf("live authority SQL point metadata changed across wire round trip: got=%+v want=%+v", decoded.PrimaryKeyRead, primaryKeyRead)
	}
	component, payload, next, err := orderedkey.DecodeComponent(nil, decoded.PrimaryKeyRead.Keys[0], 0)
	if err != nil || component.Kind != orderedkey.KindString || component.Descending ||
		component.PayloadStart != 0 || component.PayloadEnd != len(payload) ||
		next != len(decoded.PrimaryKeyRead.Keys[0]) ||
		!bytes.Equal(payload[component.PayloadStart:component.PayloadEnd], []byte("authority-live-append")) {
		t.Fatalf("live authority SQL key is not one ascending string component: component=%+v payload=%q next=%d err=%v", component, payload, next, err)
	}
	return &shardservice.ReplicatedRequest{
		Operation:     shardservice.ReplicatedQueryLeader,
		Authority:     authority,
		Capability:    serviceauthz.CapabilityDataRead,
		Fence:         state.Fence,
		Query:         query.Bytes(),
		MaxValueBytes: 4096,
	}
}

func rf3ReadAuthorityPrimaryKeyRead(
	t testing.TB, bundle rf3ManifestGroup,
) shardservice.PrimaryKeyReadRequest {
	t.Helper()
	var identity sqldriver.ReplicatedShardStoreIdentity
	if err := loadRF3IdentityFile(bundle.SQL.IdentityPath, &identity); err != nil {
		t.Fatalf("load live authority SQL identity: %v", err)
	}
	if _, err := sqldriver.ReplicatedRelationManifestDigest(identity); err != nil {
		t.Fatalf("validate live authority SQL identity: %v", err)
	}
	if identity.UserTable != "docs_live" || !rf3RouteMatchesBinding(bundle.Route, identity.Binding) {
		t.Fatalf("live authority SQL identity does not match docs_live route: table=%q binding=%+v route=%+v", identity.UserTable, identity.Binding, bundle.Route)
	}
	var relation *sqldriver.ReplicatedShardRelationIdentity
	for index := range identity.Relations {
		candidate := &identity.Relations[index]
		if candidate.Table != "docs_live" {
			continue
		}
		if relation != nil || candidate.Kind != sqldriver.ReplicatedShardRelationJSON {
			t.Fatalf("live authority SQL identity has invalid docs_live base relation: %+v", identity.Relations)
		}
		relation = candidate
	}
	if relation == nil || relation.Relation == 0 ||
		identity.UserPrimaryKey == "" || relation.Limits.MaxDocumentBytes <= 0 ||
		relation.Limits.MaxDocumentBytes > replication.MaxMutationValueBytes ||
		relation.Limits.MaxKeyBytes <= 0 {
		t.Fatalf("live authority SQL identity has no valid docs_live base relation: user=%+v relations=%+v", identity, identity.Relations)
	}
	key, ok := orderedkey.AppendString(nil, []byte("authority-live-append"), orderedkey.Ascending)
	if !ok || len(key) > relation.Limits.MaxKeyBytes {
		t.Fatalf("live authority SQL key does not fit prepared MaxKeyBytes=%d: encoded=%d", relation.Limits.MaxKeyBytes, len(key))
	}
	return shardservice.PrimaryKeyReadRequest{
		Relation:         replication.RelationID(relation.Relation),
		MaxDocumentBytes: uint32(relation.Limits.MaxDocumentBytes),
		PrimaryPath:      []byte(identity.UserPrimaryKey),
		Keys:             [][]byte{key},
	}
}

func rf3ReadAuthorityRoundTrip(
	ctx context.Context, address string, serverNode rafttransport.NodeID, profile *rafttransport.PeerTLS,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	deadline := func() time.Time {
		if value, ok := ctx.Deadline(); ok {
			return value
		}
		return time.Now().Add(10 * time.Second)
	}
	connection, err := profile.Client(ctx, raw, serverNode, rafttransport.TrafficShardNative, deadline)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	defer connection.Close()
	return shardservice.RoundTripReplicated(ctx, connection, request)
}

func rf3WaitForReadAuthoritySQLReady(
	t testing.TB, address string, serverNode rafttransport.NodeID, profile *rafttransport.PeerTLS,
	bundle rf3ManifestGroup, state shardservice.ReplicatedMemberState,
	authority serviceauthz.Authority, signal chan<- os.Signal, root string, group raftmember.GroupKey,
) (shardservice.ReplicatedMemberState, rf3DiagnosticSnapshot, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), rf3AuthoritySQLReadinessTimeout)
	defer cancel()
	path := filepath.Join(root, "rf3-diagnostics.json")
	var latest rf3DiagnosticSnapshot
	var nextSerial uint64
	if raw, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(raw, &latest); err != nil {
			return state, latest, fmt.Errorf("decode existing diagnostic snapshot: %w", err)
		}
		nextSerial = latest.Serial + 1
	} else if !errors.Is(err, os.ErrNotExist) {
		return state, latest, fmt.Errorf("read existing diagnostic snapshot: %w", err)
	}
	if nextSerial == 0 {
		nextSerial = 1
	}
	for attempt := 0; attempt < rf3AuthoritySQLReadinessAttempts; attempt++ {
		response, err := rf3ReadAuthorityRoundTrip(ctx, address, serverNode, profile, rf3ReadAuthoritySQLRequest(t, bundle, state, authority))
		if err != nil {
			return state, latest, fmt.Errorf("authority SQL readiness read %d: %w", attempt+1, err)
		}
		if err := rf3ValidateReadAuthoritySQLResponse(response); err != nil {
			return state, latest, fmt.Errorf("authority SQL readiness read %d: %w", attempt+1, err)
		}
		if response.State.Fence.Group != bundle.Route.Group ||
			response.State.Fence.AllocationGeneration != bundle.Route.AllocationGeneration {
			return state, latest, fmt.Errorf("authority SQL readiness read %d returned fence for group %x allocation %d", attempt+1, response.State.Fence.Group.GroupID, response.State.Fence.AllocationGeneration)
		}
		state = response.State
		wantedSerial := nextSerial
		select {
		case signal <- syscall.SIGUSR1:
		case <-ctx.Done():
			return state, latest, ctx.Err()
		}
		for {
			raw, readErr := os.ReadFile(path)
			if readErr == nil {
				var snapshot rf3DiagnosticSnapshot
				if err := json.Unmarshal(raw, &snapshot); err != nil {
					return state, latest, fmt.Errorf("decode diagnostic snapshot after authority SQL readiness read %d: %w", attempt+1, err)
				}
				if snapshot.Serial >= wantedSerial {
					latest = snapshot
					nextSerial = snapshot.Serial + 1
					break
				}
			} else if !errors.Is(readErr, os.ErrNotExist) {
				return state, latest, fmt.Errorf("read diagnostic snapshot after authority SQL readiness read %d: %w", attempt+1, readErr)
			}
			select {
			case <-time.After(10 * time.Millisecond):
			case <-ctx.Done():
				return state, latest, ctx.Err()
			}
		}
		if rf3ReadAuthorityEvidenceReady(latest, group) {
			return state, latest, nil
		}
		select {
		case <-time.After(25 * time.Millisecond):
		case <-ctx.Done():
			return state, latest, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return state, latest, err
	}
	return state, latest, fmt.Errorf("authority evidence did not become ready after %d SQL readiness reads", rf3AuthoritySQLReadinessAttempts)
}

func rf3ValidateReadAuthoritySQLResponse(response *shardservice.ReplicatedResponse) error {
	if response == nil || response.Kind != shardservice.ReplicatedQueryResult || !response.HasState {
		return fmt.Errorf("did not return a query result with serving state: %+v", response)
	}
	decoded, err := shardservice.DecodeResponse(bytes.NewReader(response.Value))
	if err != nil {
		return fmt.Errorf("response decode: %w", err)
	}
	if decoded.Kind != shardservice.ResponseRows || len(decoded.Columns) != 1 || decoded.Columns[0].Name != "id" {
		return fmt.Errorf("response shape: %+v", decoded)
	}
	for row, cells := range decoded.Rows {
		if len(cells) != 1 {
			return fmt.Errorf("row %d has %d cells, want one", row, len(cells))
		}
	}
	return nil
}

func rf3AssertReadAuthoritySQLResponse(t testing.TB, response *shardservice.ReplicatedResponse, label string) {
	t.Helper()
	if err := rf3ValidateReadAuthoritySQLResponse(response); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	decoded, err := shardservice.DecodeResponse(bytes.NewReader(response.Value))
	if err != nil {
		t.Fatalf("%s response decode: %v", label, err)
	}
	if len(decoded.Rows) != 0 {
		t.Fatalf("%s returned %d rows, want the freshly-created docs_live miss", label, len(decoded.Rows))
	}
}

func rf3ReadAuthorityEvidenceReady(snapshot rf3DiagnosticSnapshot, group raftmember.GroupKey) bool {
	if snapshot.Groups < 2 || !snapshot.ReadAuthorityEvidenceAvailable {
		return false
	}
	wantedGroup := rf3DiagnosticAuthorityGroupIdentityJSON(authorityGroupIdentity(group))
	for _, evidence := range snapshot.ReadAuthorityEvidence {
		if evidence.RuntimeIdentity.Group == wantedGroup && evidence.Status == "configured" &&
			evidence.Holder.Available && len(evidence.Holder.AcceptedVoters) >= 2 &&
			evidence.ObservationAvailable && evidence.Observation.CurrentTermCommitted && evidence.Observation.Stable {
			return true
		}
	}
	return false
}

func waitRF3ReadAuthoritySnapshot(t *testing.T, signal chan<- os.Signal, root string, serial, minAuthorityHits uint64, group raftmember.GroupKey) (rf3DiagnosticSnapshot, bool) {
	t.Helper()
	path := filepath.Join(root, "rf3-diagnostics.json")
	if raw, err := os.ReadFile(path); err == nil {
		var snapshot rf3DiagnosticSnapshot
		if json.Unmarshal(raw, &snapshot) == nil && snapshot.Serial >= serial {
			serial = snapshot.Serial + 1
		}
	}
	signal <- syscall.SIGUSR1
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			var snapshot rf3DiagnosticSnapshot
			if json.Unmarshal(raw, &snapshot) == nil && snapshot.Serial >= serial && snapshot.Groups >= 2 && snapshot.ReadAuthorityEvidenceAvailable &&
				snapshot.AuthorityReadHits >= minAuthorityHits {
				if rf3ReadAuthorityEvidenceReady(snapshot, group) {
					return snapshot, true
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return rf3DiagnosticSnapshot{}, false
}
