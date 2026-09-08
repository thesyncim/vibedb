//go:build darwin || linux

package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestRF3FaultResponseDiagnosticSummaryIncludesFixedFenceWithoutPayload(t *testing.T) {
	identity := rf3CommandStoreIdentity(1)
	fence := shardservice.ReplicatedFence{
		Group: groupKeyForDiagnosticTest(), AllocationGeneration: identity.AllocationGeneration,
		MemberID: 1, StoreID: identity.StoreID, NodeIncarnation: 9, Term: 11,
		Command: raftservice.CommandFence{
			ReplicaSetVersion: 2, ActivePolicyGeneration: 3, ProtectionEpoch: 4,
			OwnershipEpoch: 5, SchemaGeneration: 6, RoutingVersion: 7, RouteGeneration: 8,
			RelationManifestDigest: [32]byte{0xcd},
		},
	}
	payload := []byte("diagnostic-payload-must-not-appear")
	digest := [32]byte{0xab}
	summary := rf3FaultResponseSummary(&shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalStaleFence,
		HasState: true, State: shardservice.ReplicatedMemberState{Fence: fence},
		RequestDigest: digest, Completion: payload,
	})
	for _, want := range []string{
		"allocation=23", "member=1", "node_incarnation=9", "term=11",
		"command=2/3/4/5/6/7/8", fmt.Sprintf("manifest=%x", fence.Command.RelationManifestDigest),
		fmt.Sprintf("request_digest=%x", digest),
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q: %s", want, summary)
		}
	}
	if strings.Contains(summary, string(payload)) || strings.Contains(summary, "%!") {
		t.Fatalf("summary exposed payload or format failure: %s", summary)
	}
	if len(summary) > rf3FaultFailureDiagnosticMaxBytes {
		t.Fatalf("fixed summary exceeded diagnostic cap: %d", len(summary))
	}
}

func TestRF3FaultDiagnosticProbeUsesSharedAbsoluteDeadline(t *testing.T) {
	root := t.TempDir()
	nodes := rf3CommandNodes()
	group := groupKeyForDiagnosticTest()
	credentials, roots, err := rf3testfixture.WriteCredentials(root, rf3CommandIdentityOID,
		rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}, nodes[:])
	if err != nil {
		t.Fatal(err)
	}
	clientProfile, err := servicetls.LoadProfile(credentials[1].Certificate, credentials[1].Key,
		roots, rf3CommandIdentityOID.String(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	serverProfile, err := servicetls.LoadProfile(credentials[0].Certificate, credentials[0].Key,
		roots, rf3CommandIdentityOID.String(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	serverConfig, err := serverProfile.ServerConfig(rafttransport.TrafficShardNative)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseServer := func() { releaseOnce.Do(func() { close(release) }) }
	serverResult := make(chan error, 1)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		raw, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverResult <- acceptErr
			return
		}
		defer raw.Close()
		if err := raw.SetDeadline(time.Now().Add(time.Second)); err != nil {
			serverResult <- err
			return
		}
		connection := tls.Server(raw, serverConfig)
		serverContext, cancelServer := context.WithTimeout(context.Background(), time.Second)
		defer cancelServer()
		if err := connection.HandshakeContext(serverContext); err != nil {
			serverResult <- err
			return
		}
		var firstPrefaceByte [1]byte
		if _, err := io.ReadFull(connection, firstPrefaceByte[:]); err != nil {
			serverResult <- err
			return
		}
		serverResult <- nil
		<-release
		_ = connection.Close()
	}()
	defer func() {
		releaseServer()
		_ = listener.Close()
		select {
		case <-serverDone:
		case <-time.After(2 * time.Second):
			t.Error("diagnostic probe server did not finish cleanup")
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	started := time.Now()
	result := make(chan error, 1)
	go func() {
		_, probeErr := probeRF3CommandMemberAtContextDeadline(ctx, listener.Addr().String(), nodes[0], clientProfile,
			nodes[1], group, rf3CommandStoreIdentity(1).AllocationGeneration, 5)
		result <- probeErr
	}()
	var probeErr error
	select {
	case probeErr = <-result:
	case <-time.After(time.Second):
		t.Fatal("diagnostic probe exceeded bounded completion")
	}
	select {
	case err := <-serverResult:
		if err != nil {
			t.Fatalf("TLS/preface server did not observe client preface: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("diagnostic probe server did not observe client preface")
	}
	if probeErr == nil {
		t.Fatal("stalled build preface unexpectedly produced a probe state")
	}
	var networkErr net.Error
	if !errors.As(probeErr, &networkErr) || !networkErr.Timeout() {
		t.Fatalf("stalled build preface error = %v, want timeout", probeErr)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("diagnostic probe exceeded shared deadline bound: %s", elapsed)
	}
}

func groupKeyForDiagnosticTest() raftmember.GroupKey {
	identity := rf3CommandStoreIdentity(1)
	return raftmember.GroupKey{
		ClusterID: identity.ClusterID, ClusterIncarnation: identity.ClusterIncarnation,
		TopologyRecoveryEpoch: 3, ShardIncarnation: identity.ShardIncarnation, GroupID: identity.GroupID,
	}
}
