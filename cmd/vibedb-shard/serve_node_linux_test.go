//go:build linux

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

func TestServeNodeFrontendOpenFailureJoinsOwnersAndReleasesStorage(t *testing.T) {
	testServeNodeFrontendOpenFailure(t, false)
}

func TestServeNodeRetiredGroupsKeepPhysicalAndFrontendServices(t *testing.T) {
	testServeNodeFrontendOpenFailure(t, true)
}

func testServeNodeFrontendOpenFailure(t *testing.T, retireGroups bool) {
	input := prepareRF3NodeTestInput(t)
	nodes, group := rf3CommandNodes(), rf3CommandGroup()
	frontend := rafttransport.NodeID{0xf1}
	root := t.TempDir()
	credentials, roots, err := rf3testfixture.WriteCredentials(root, rf3CommandIdentityOID,
		rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation},
		append(nodes[:], frontend))
	if err != nil {
		t.Fatal(err)
	}
	policy := filepath.Join(root, "policy.vibejson")
	if err := os.WriteFile(policy, rf3CommandPolicyWithTarget(nodes, frontend), 0o600); err != nil {
		t.Fatal(err)
	}
	var reservations []*rf3AcceptReadyListener
	byAddress := make(map[string]*rf3AcceptReadyListener)
	for range 4 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		admission := newRF3AcceptReadyListener(listener)
		reservations = append(reservations, admission)
		byAddress[listener.Addr().String()] = admission
		t.Cleanup(func() { _ = listener.Close() })
	}
	listeners := rf3ManifestListeners{Peer: reservations[0].Addr().String(), Native: reservations[1].Addr().String(),
		Snapshot: reservations[2].Addr().String(), Control: reservations[3].Addr().String()}
	for index := range input.Groups {
		member := &input.Groups[index]
		member.Listeners = listeners
		member.Members[0].PeerAddress = listeners.Peer
		member.TLS = rf3ManifestTLS{PeerKeys: rf3CommandPeerKeys(credentials[0]), Certificate: credentials[0].Certificate, Key: credentials[0].Key, Roots: roots, IdentityOID: rf3CommandIdentityOID.String()}
		member.AuthorizationPolicy = policy
	}
	input.Gateway = &rf3ManifestGateway{
		CatalogPath: filepath.Join(root, "absent-catalog"), CatalogRouteSeedPath: filepath.Join(root, "absent-seed"),
		CatalogRelation: 1, CatalogAttempts: 1, CatalogAttemptTimeoutMillis: 1000, CatalogSessionLeaseMillis: 86400000,
		CatalogSessionJournal: filepath.Join(root, "absent-session"), CatalogClientID: strings.Repeat("a", 32),
		CatalogRetryHome: strings.Repeat("b", 16), DurableAckKeyPath: filepath.Join(root, "absent-ack-key"),
		ListenAddress: "127.0.0.1:0", AuthorizationPolicy: policy, TableCatalogs: []string{},
		TLS: rf3ManifestTLS{PeerKeys: rf3CommandPeerKeys(credentials[3]), Certificate: credentials[3].Certificate, Key: credentials[3].Key, Roots: roots, IdentityOID: rf3CommandIdentityOID.String()},
	}
	for index, node := range nodes {
		address := fmt.Sprintf("127.0.0.1:%d", 28000+index)
		if index == 0 {
			address = listeners.Native
		}
		input.Gateway.ShardPeers = append(input.Gateway.ShardPeers, rf3ManifestGatewayPeer{Address: address, NodeID: fmt.Sprintf("%x", node)})
	}
	if err := provisionRF3Node(input); err != nil {
		t.Fatal(err)
	}
	manifest, err := loadRF3Manifest(filepath.Join(input.Root, "serve-rf3.vibejson"))
	if err != nil {
		t.Fatal(err)
	}
	var retirements []replicaaction.Record
	if retireGroups {
		journal, err := replicaaction.OpenFileJournal(manifest.ReplicaControl.ActionJournalPath, manifest.ReplicaControl.MaxActionRecords)
		if err != nil {
			t.Fatal(err)
		}
		for index, bundle := range manifest.groupBundles() {
			base, _, err := loadRF3RetainedIdentities(manifest.withGroup(bundle))
			if err != nil {
				t.Fatal(err)
			}
			record := rf3RetirementRecoveryRecord(rf3RecoveryEnrollmentIntent())
			record.Request.Operation[0] = byte(index + 1)
			record.Request.Fence.Group = groupFromBinding(base.Binding)
			record.Request.Fence.AllocationGeneration = base.Binding.AllocationGeneration
			record.Request.Fence.MemberID = base.Binding.MemberID
			record.Request.Fence.StoreID = base.Binding.StoreID
			record.Request.SourceMember, record.Request.TargetMember = base.Binding.MemberID, base.Binding.MemberID+100
			if err = journal.PublishReplicaAction(t.Context(), 0, record); err != nil {
				t.Fatal(err)
			}
			record.Revision, record.State = 2, replicaaction.RetirementAuthorized
			if err = journal.PublishReplicaAction(t.Context(), 1, record); err != nil {
				t.Fatal(err)
			}
			retirements = append(retirements, record)
		}
		if err := journal.Close(); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err = servePreparedRF3WithEmbeddedGateway(ctx, manifest, rf3DefaultExecutionLanes, func(_, address string) (net.Listener, error) {
		if listener := byAddress[address]; listener != nil {
			return listener, nil
		}
		return nil, fmt.Errorf("unexpected listener %q", address)
	})
	if err == nil || !strings.Contains(err.Error(), "open embedded gateway") {
		t.Fatalf("frontend Open failure was not returned: %v", err)
	}
	for _, listener := range reservations {
		select {
		case <-listener.accepting:
		default:
			t.Fatalf("frontend Open ran before %s accepted", listener.Addr())
		}
	}
	profile, err := servicetls.LoadProfile(manifest.TLS.Certificate, manifest.TLS.Key, manifest.TLS.Roots, manifest.TLS.IdentityOID, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := openRF3NodeOwner(manifest, profile)
	if err != nil {
		t.Fatalf("failed frontend retained node log ownership: %v", err)
	}
	defer owner.Close()
	prepared, err := prepareRF3GroupSetOnNodeWithRetirementsAndPeers(manifest, profile, sqldriver.ReplicatedOpenOptions{}, owner, retirements, nil)
	if err != nil {
		t.Fatalf("failed frontend retained SQL ownership: %v", err)
	}
	if err := closePreparedRF3Groups(prepared.groups, nil); err != nil {
		t.Fatal(err)
	}
	if retireGroups && len(prepared.groups) != 0 {
		t.Fatal("retired storage reopened")
	}
}

func TestServeNodeAdmitsControlBeforeRemoteEnrollmentRecovery(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty=%t", empty), func(t *testing.T) { testServeNodePendingRecovery(t, empty) })
	}
}

func testServeNodePendingRecovery(t *testing.T, empty bool) {
	input := prepareRF3NodeTestInput(t)
	byAddress := make(map[string]*rf3AcceptReadyListener)
	var listeners []string
	for range 4 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		address := listener.Addr().String()
		byAddress[address] = newRF3AcceptReadyListener(listener)
		listeners = append(listeners, address)
	}
	for index := range input.Groups {
		input.Groups[index].Listeners = rf3ManifestListeners{Peer: listeners[0], Native: listeners[1], Snapshot: listeners[2], Control: listeners[3]}
		input.Groups[index].Members[0].PeerAddress = listeners[0]
	}
	seed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Close()
	seeds := []nodecontrol.BootstrapGatewaySeed{{NodeID: rf3CommandNodes()[1], Incarnation: 1,
		ControlAddress: seed.Addr().String(), SPKIPinDigest: replication.Digest{1}}}
	if empty {
		initial := input.Groups[0]
		nodes := rf3CommandNodes()
		raw, err := rf3testfixture.EmptyNodePreparationManifest(rf3testfixture.EmptyNodeOptions{
			Root: input.Root, NodeIncarnation: 1, NodeStore: input.NodeLog.Options,
			Key:        raftstore.Key{ID: input.NodeLog.KeyID, Wrapped: []byte("opaque-test-key")},
			Listeners:  rf3testfixture.ProcessListeners{Peer: listeners[0], Native: listeners[1], Snapshot: listeners[2], Control: listeners[3]},
			Credential: rf3testfixture.Credential{Certificate: initial.TLS.Certificate, Key: initial.TLS.Key},
			Roots:      initial.TLS.Roots, AuthorizationPolicy: initial.AuthorizationPolicy, GrantNodes: nodes[:], GatewaySeeds: seeds,
		}, input.NodeLog.KeyMaterialPath)
		if err != nil {
			t.Fatal(err)
		}
		input = prepareRF3NodeManifest{}
		if err := vibejson.Unmarshal(raw, &input); err != nil {
			t.Fatal(err)
		}
	}
	if err := provisionRF3Node(input); err != nil {
		t.Fatal(err)
	}
	manifest, err := loadRF3Manifest(filepath.Join(input.Root, "serve-rf3.vibejson"))
	if err != nil {
		t.Fatal(err)
	}
	manifest.NodeIncarnation = 1
	manifest.GatewaySeeds = seeds
	intent := rf3RecoveryEnrollmentIntent()
	receipt := rf3EnrollmentReceiverReceipt{Kind: rf3EnrollmentPayloadKind, IntentID: intent.IntentID,
		IntentDigest: intent.Digest(), Group: intent.Group, TargetMember: intent.Target.Member,
		TargetNode: intent.Target.Node, TargetNodeIncarnation: intent.Target.NodeIncarnation,
		TargetStoreID: intent.Target.StoreID, ProofDigest: intent.Proof.EnrollmentDigest}
	root := rf3EnrollmentReservationPath(manifest.ReplicaControl.SourceDataRoot, intent.IntentID)
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	raw, err := vibejson.Marshal(&receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeRF3DurableMarker(filepath.Join(root, rf3EnrollmentReceiverFile), raw); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- servePreparedRF3WithListen(ctx, manifest, func(_, address string) (net.Listener, error) {
			if listener := byAddress[address]; listener != nil {
				return listener, nil
			}
			return nil, fmt.Errorf("unexpected listener %q", address)
		})
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("physical owner did not join on shutdown")
		}
	}()
	for _, address := range listeners {
		select {
		case <-byAddress[address].accepting:
		case err := <-done:
			done <- err
			t.Fatalf("host stopped before control admission: %v", err)
		case <-time.After(3 * time.Second):
			t.Fatalf("remote enrollment recovery blocked authenticated listener %s", address)
		}
	}
}
