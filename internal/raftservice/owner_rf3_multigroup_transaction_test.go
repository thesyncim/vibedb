package raftservice_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/bits"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
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
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
)

type Owner = raftservice.Owner
type AuthenticatedPeerRuntime = raftservice.AuthenticatedPeerRuntime
type CommandFence = raftservice.CommandFence
type ReadSource = raftservice.ReadSource
type TransactionRecoverySource = raftservice.TransactionRecoverySource
type Result = raftservice.Result
type UnknownOutcomeError = raftservice.UnknownOutcomeError
type TransactionReadRequest = raftservice.TransactionReadRequest
type PointReadRequest = raftservice.PointReadRequest
type PointReadResult = raftservice.PointReadResult
type PointReadLease = raftservice.PointReadLease
type ServingState = raftservice.ServingState
type ProgressMetrics = raftservice.ProgressMetrics

var NewAuthenticatedPeerRuntime = raftservice.NewAuthenticatedPeerRuntime
var ErrOutcomeUnknown = raftservice.ErrOutcomeUnknown
var ErrServingFence = raftservice.ErrServingFence

type AuthenticatedPeerOptions = raftservice.AuthenticatedPeerOptions
type Options = raftservice.Options
type Limits = raftservice.Limits

var multiGroupRF3Now = time.Date(2034, 2, 3, 4, 5, 6, 0, time.UTC)
var multiGroupRF3OID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}

type multiGroupRF3Authority struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
	roots       *x509.CertPool
	serial      int64
}

func newPeerServerTestAuthority(t testing.TB) *multiGroupRF3Authority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "rf3-test-ca"},
		NotBefore: multiGroupRF3Now.Add(-time.Hour), NotAfter: multiGroupRF3Now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	encoded, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(encoded)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	return &multiGroupRF3Authority{certificate: certificate, key: key, roots: roots, serial: 1}
}

func newPeerServerTestTLS(
	t testing.TB,
	authority *multiGroupRF3Authority,
	identity rafttransport.PeerIdentity,
) *rafttransport.PeerTLS {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	extension, err := rafttransport.PeerIdentityExtension(multiGroupRF3OID, identity)
	if err != nil {
		t.Fatal(err)
	}
	authority.serial++
	template := &x509.Certificate{
		SerialNumber: big.NewInt(authority.serial), Subject: pkix.Name{CommonName: "unused"},
		NotBefore: multiGroupRF3Now.Add(-time.Hour), NotAfter: multiGroupRF3Now.Add(time.Hour),
		KeyUsage:        x509.KeyUsageDigitalSignature,
		ExtKeyUsage:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		ExtraExtensions: []pkix.Extension{extension},
	}
	encoded, err := x509.CreateCertificate(
		rand.Reader, template, authority.certificate, &key.PublicKey, authority.key,
	)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := rafttransport.NewPeerTLS(rafttransport.PeerTLSOptions{
		IdentityOID: multiGroupRF3OID, Identity: identity,
		Certificate: tls.Certificate{
			Certificate: [][]byte{encoded, authority.certificate.Raw}, PrivateKey: key,
		},
		Roots: authority.roots, Now: func() time.Time { return multiGroupRF3Now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

const (
	multiGroupRF3Groups                              = 2
	multiGroupRF3MaxGroups                           = 3
	multiGroupRF3LedgerGroup                         = 2
	multiGroupRF3Voters                              = 3
	multiGroupRF3DurableSQLRequiredEnvironment       = "VIBEDB_DURABLE_SQL_RF3_E2E"
	multiGroupRF3DurableSQLRequiredEnvironmentEnable = "1"
)

type multiGroupRF3Group struct {
	logicalSchema replication.Digest
	key           raftmember.GroupKey
	runtimes      [multiGroupRF3Voters]*raftmember.Runtime
	wals          [multiGroupRF3Voters]*raftstore.Store
	bases         [multiGroupRF3Voters]sqldriver.ReplicatedShardStoreIdentity
	reads         [multiGroupRF3Voters]*sqldriver.ReplicatedApply
	storageRoots  [multiGroupRF3Voters]string
}

type multiGroupRF3Trace struct {
	Group            int
	Member           int
	Command          []byte
	CommandDigest    [sha256.Size]byte
	FenceTerm        uint64
	ObservedLeader   uint64
	Outcome          raftserve.Outcome
	CompletionDigest [sha256.Size]byte
	Completion       []byte
	Hidden           bool
	Err              error
}

type multiGroupRF3Fault uint8

const (
	multiGroupRF3NoFault multiGroupRF3Fault = iota
	multiGroupRF3HideResponseAndIsolate
)

var errMultiGroupRF3Isolated = errors.New("RF3 test node is isolated")

type multiGroupRF3Network struct {
	mu        sync.Mutex
	addresses [multiGroupRF3Voters]string
	blocked   [multiGroupRF3Voters][multiGroupRF3Voters]bool
	conns     map[*multiGroupRF3Conn]struct{}
}

type multiGroupRF3Conn struct {
	net.Conn
	network *multiGroupRF3Network
	from    int
	to      int
	once    sync.Once
}

func (network *multiGroupRF3Network) dial(
	ctx context.Context,
	from int,
	to int,
) (net.Conn, error) {
	network.mu.Lock()
	blocked := network.blocked[from][to]
	address := network.addresses[to]
	network.mu.Unlock()
	if blocked {
		return nil, errMultiGroupRF3Isolated
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	tracked := &multiGroupRF3Conn{Conn: conn, network: network, from: from, to: to}
	network.mu.Lock()
	if network.blocked[from][to] {
		network.mu.Unlock()
		_ = conn.Close()
		return nil, errMultiGroupRF3Isolated
	}
	network.conns[tracked] = struct{}{}
	network.mu.Unlock()
	return tracked, nil
}

func (conn *multiGroupRF3Conn) Close() error {
	err := conn.Conn.Close()
	conn.once.Do(func() {
		conn.network.mu.Lock()
		delete(conn.network.conns, conn)
		conn.network.mu.Unlock()
	})
	return err
}

func (network *multiGroupRF3Network) isolate(node int) {
	network.mu.Lock()
	for peer := 0; peer < multiGroupRF3Voters; peer++ {
		if peer == node {
			continue
		}
		network.blocked[node][peer] = true
		network.blocked[peer][node] = true
	}
	connections := make([]*multiGroupRF3Conn, 0, len(network.conns))
	for conn := range network.conns {
		if conn.from == node || conn.to == node {
			connections = append(connections, conn)
		}
	}
	network.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
}

func (network *multiGroupRF3Network) close() {
	network.mu.Lock()
	connections := make([]*multiGroupRF3Conn, 0, len(network.conns))
	for conn := range network.conns {
		connections = append(connections, conn)
	}
	network.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
}

type multiGroupTransactionRF3Cluster struct {
	groups     [multiGroupRF3MaxGroups]multiGroupRF3Group
	groupCount int
	owners     [multiGroupRF3Voters]*Owner
	progress   [multiGroupRF3Voters]*ProgressMetrics
	peers      [multiGroupRF3Voters]*AuthenticatedPeerRuntime
	contexts   [multiGroupRF3Voters]context.Context
	cancels    [multiGroupRF3Voters]context.CancelFunc
	runErrors  [multiGroupRF3Voters]chan error
	pulses     [multiGroupRF3Voters]chan struct{}
	listeners  [multiGroupRF3Voters]net.Listener
	stopPulses chan struct{}
	network    multiGroupRF3Network

	traceMu sync.Mutex
	trace   []multiGroupRF3Trace
}

func (cluster *multiGroupTransactionRF3Cluster) route(group int) gateway.ReplicatedRoute {
	base := cluster.groups[group].bases[0]
	route := gateway.ReplicatedRoute{
		LogicalSchemaDigest:  cluster.groups[group].logicalSchema,
		Distribution:         distribution.DistributionName(base.Binding.Distribution),
		Shard:                distribution.ShardID(base.Binding.Shard),
		Group:                cluster.groups[group].key,
		AllocationGeneration: base.Binding.AllocationGeneration,
		Command:              rf3CommandFence(cluster.groups[group].runtimes[0].Identity(), base),
		Replicas:             make([]gateway.ReplicatedEndpoint, 0, multiGroupRF3Voters),
	}
	route.RangeIdentity = multiGroupRF3RangeIdentity(group)
	route.LineageDigest = multiGroupRF3RouteDigest(
		"vibedb/test/multiraft-route-lineage/format-0\x00", route.RangeIdentity,
	)
	route.ForwardingRuleDigest = multiGroupRF3RouteDigest(
		"vibedb/test/multiraft-route-forwarding/format-0\x00", route.RangeIdentity,
	)
	for member := 0; member < multiGroupRF3Voters; member++ {
		identity := cluster.groups[group].runtimes[member].Identity()
		address := "rf3-owner-" + string(rune('1'+member))
		route.Replicas = append(route.Replicas, gateway.ReplicatedEndpoint{
			Member: uint64(member + 1), Node: rafttransport.NodeID{byte(member + 1)},
			StoreID: identity.StoreID, NodeIncarnation: identity.NodeIncarnation,
			NativeEndpoint: address, Address: address,
		})
	}
	return route
}

func multiGroupRF3RouteAuthorityWitness(
	route gateway.ReplicatedRoute,
) distributedtxn.AuthorityWitness {
	digest := replication.RouteAuthorityDigest(replication.RouteAuthority{
		ClusterID:              replication.ID128(route.Group.ClusterID),
		ClusterIncarnation:     replication.ID128(route.Group.ClusterIncarnation),
		TopologyRecoveryEpoch:  route.Group.TopologyRecoveryEpoch,
		ShardIncarnation:       replication.ID128(route.Group.ShardIncarnation),
		GroupID:                replication.ID128(route.Group.GroupID),
		AllocationGeneration:   route.AllocationGeneration,
		ReplicaSetVersion:      route.Command.ReplicaSetVersion,
		ActivePolicyGeneration: route.Command.ActivePolicyGeneration,
		ProtectionEpoch:        route.Command.ProtectionEpoch,
		OwnershipEpoch:         route.Command.OwnershipEpoch,
		SchemaGeneration:       route.Command.SchemaGeneration,
		RelationManifestDigest: replication.Digest(route.Command.RelationManifestDigest),
		RoutingVersion:         route.Command.RoutingVersion,
		RouteGeneration:        route.Command.RouteGeneration,
	})
	var witness distributedtxn.AuthorityWitness
	copy(witness[:], digest[:len(witness)])
	return witness
}

type multiGroupRF3GatewayTrace struct {
	group     int
	member    int
	operation distributedtxn.ReplicatedOperation
	command   []byte
}

type multiGroupRF3RoundTripper struct {
	cluster *multiGroupTransactionRF3Cluster
	servers [multiGroupRF3Voters]*shardservice.ReplicatedServer

	mu            sync.Mutex
	hideCommit    bool
	hidden        bool
	hiddenGroup   int
	hiddenMember  int
	hiddenCommand []byte
	trace         []multiGroupRF3GatewayTrace
	recoveryReads [multiGroupRF3MaxGroups]int
}

func newMultiGroupRF3RoundTripper(
	t testing.TB,
	cluster *multiGroupTransactionRF3Cluster,
) *multiGroupRF3RoundTripper {
	t.Helper()
	client := &multiGroupRF3RoundTripper{cluster: cluster, hiddenGroup: -1, hiddenMember: -1}
	for member := 0; member < multiGroupRF3Voters; member++ {
		server, err := shardservice.NewReplicatedServer(cluster.owners[member], 256<<20, 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		client.servers[member] = server
	}
	return client
}

func (client *multiGroupRF3RoundTripper) DoReplicated(
	ctx context.Context,
	endpoint gateway.ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	member := int(endpoint.Member - 1)
	if member < 0 || member >= multiGroupRF3Voters {
		return nil, errors.New("RF3 gateway test endpoint member is outside the cluster")
	}
	group := -1
	for candidate := 0; candidate < client.cluster.groupCount; candidate++ {
		if request.Fence.Group == client.cluster.groups[candidate].key {
			group = candidate
			break
		}
	}
	if group < 0 {
		return nil, errors.New("RF3 gateway test request names an unknown group")
	}
	client.mu.Lock()
	isolated := client.hidden && member == client.hiddenMember
	client.mu.Unlock()
	if isolated {
		return nil, errMultiGroupRF3Isolated
	}

	var operation distributedtxn.ReplicatedOperation
	if request.Operation == shardservice.ReplicatedPropose {
		command, err := replication.OpenCommand(request.Command)
		if err != nil {
			return nil, err
		}
		if command.Kind() == replication.CommandTransaction {
			control, err := distributedtxn.OpenReplicatedCommand(command.TransactionBytes())
			if err != nil {
				return nil, err
			}
			operation = control.Operation
		}
		client.mu.Lock()
		client.trace = append(client.trace, multiGroupRF3GatewayTrace{
			group: group, member: member, operation: operation,
			command: bytes.Clone(request.Command),
		})
		client.mu.Unlock()
	}

	gatewayConn, serverConn := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- client.servers[member].ServeReplicatedConn(ctx, serverConn)
	}()
	response, err := shardservice.RoundTripReplicated(ctx, gatewayConn, request)
	_ = gatewayConn.Close()
	_ = serverConn.Close()
	var serverErr error
	select {
	case serverErr = <-done:
	case <-ctx.Done():
	}
	if err != nil {
		return nil, errors.Join(err, serverErr)
	}
	if request.Operation == shardservice.ReplicatedTransactionRead && response != nil &&
		response.Kind == shardservice.ReplicatedTransactionReadResult {
		client.mu.Lock()
		client.recoveryReads[group]++
		client.mu.Unlock()
	}

	client.mu.Lock()
	hide := client.hideCommit && !client.hidden &&
		operation == distributedtxn.ReplicatedCommitCoordinator &&
		response != nil && response.Kind == shardservice.ReplicatedCompletion
	if hide {
		client.hidden = true
		client.hiddenGroup, client.hiddenMember = group, member
		client.hiddenCommand = bytes.Clone(request.Command)
	}
	client.mu.Unlock()
	if hide {
		client.cluster.network.isolate(member)
		return nil, io.ErrUnexpectedEOF
	}
	return response, nil
}

func (client *multiGroupRF3RoundTripper) gatewayTrace() []multiGroupRF3GatewayTrace {
	client.mu.Lock()
	defer client.mu.Unlock()
	trace := make([]multiGroupRF3GatewayTrace, len(client.trace))
	copy(trace, client.trace)
	for index := range trace {
		trace[index].command = bytes.Clone(trace[index].command)
	}
	return trace
}

func newMultiGroupTransactionRF3Cluster(t testing.TB) *multiGroupTransactionRF3Cluster {
	return newMultiGroupRF3Cluster(t, multiGroupRF3Groups)
}

func newMultiGroupRF3Cluster(
	t testing.TB,
	groupCount int,
) *multiGroupTransactionRF3Cluster {
	return newMultiGroupRF3ClusterWithWALOptions(t, groupCount, raftstore.Options{
		MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
		MaxRecords: 4096, MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes,
	})
}

func newMultiGroupRF3ClusterWithWALOptions(
	t testing.TB,
	groupCount int,
	walOptions raftstore.Options,
) *multiGroupTransactionRF3Cluster {
	t.Helper()
	if groupCount <= 0 || groupCount > multiGroupRF3MaxGroups {
		t.Fatalf("RF3 group count %d is outside [1,%d]", groupCount, multiGroupRF3MaxGroups)
	}
	cluster := &multiGroupTransactionRF3Cluster{
		groupCount: groupCount, stopPulses: make(chan struct{}),
	}
	cluster.network.conns = make(map[*multiGroupRF3Conn]struct{})
	for group := 0; group < cluster.groupCount; group++ {
		for member := 0; member < multiGroupRF3Voters; member++ {
			cluster.groups[group].runtimes[member], cluster.groups[group].wals[member], cluster.groups[group].bases[member],
				cluster.groups[group].reads[member], cluster.groups[group].storageRoots[member] = newMultiGroupRF3Runtime(
				t, group, uint64(member+1), walOptions,
			)
		}
		cluster.groups[group].key = cluster.groups[group].runtimes[0].Identity().Group
		logical, err := sqldriver.ReplicatedRelationManifestDigest(cluster.groups[group].bases[0])
		if err != nil {
			t.Fatal(err)
		}
		cluster.groups[group].logicalSchema = replication.Digest(logical)
	}
	for group := 0; group < cluster.groupCount; group++ {
		for prior := 0; prior < group; prior++ {
			if cluster.groups[group].key == cluster.groups[prior].key {
				t.Fatalf("RF3 groups %d and %d share one logical identity", prior, group)
			}
		}
	}

	var nodes [multiGroupRF3Voters]rafttransport.NodeID
	members := make([]rafttransport.Member, 0, cluster.groupCount*multiGroupRF3Voters)
	for member := 0; member < multiGroupRF3Voters; member++ {
		nodes[member][0] = byte(member + 1)
		for group := 0; group < cluster.groupCount; group++ {
			members = append(members, rafttransport.Member{
				Group: cluster.groups[group].key, ReplicaSetVersion: 1,
				MemberID: uint64(member + 1), Node: nodes[member],
				Role: rafttransport.MemberVoter,
			})
		}
	}
	registries := make(map[rafttransport.NodeID]*rafttransport.StaticRegistry, multiGroupRF3Voters)
	for member := 0; member < multiGroupRF3Voters; member++ {
		registry, err := rafttransport.NewStaticRegistry(
			nodes[member], members,
			rafttransport.Limits{MaxGroups: cluster.groupCount, MaxMembers: len(members)},
		)
		if err != nil {
			t.Fatal(err)
		}
		registries[nodes[member]] = registry
	}

	authority := newPeerServerTestAuthority(t)
	var peerTLS [multiGroupRF3Voters]*rafttransport.PeerTLS
	for member := 0; member < multiGroupRF3Voters; member++ {
		peerTLS[member] = newPeerServerTestTLS(t, authority, rafttransport.PeerIdentity{
			TrustDomain: registries[nodes[member]].TrustDomain(), Node: nodes[member],
		})
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		cluster.listeners[member] = listener
		cluster.network.addresses[member] = listener.Addr().String()
	}
	for member := range nodes {
		registries[nodes[member]] = pinnedPeerTestRegistry(t, nodes[member], members, rafttransport.Limits{MaxGroups: cluster.groupCount, MaxMembers: len(members)}, peerTLS[:])
	}

	deadline := func() time.Time { return time.Now().Add(10 * time.Second) }

	for member := 0; member < multiGroupRF3Voters; member++ {
		serving, err := raftserve.NewRegistry(raftserve.Limits{
			MaxGroups: multiGroupRF3MaxGroups, MaxOutstandingIdentities: 64,
			MaxOutstandingAttempts: 128, MaxWaiters: 128,
			MaxAttemptsPerIdentity:     4,
			MaxRetainedCompletionBytes: 64 * int64(replicatedstate.MaxCompletionEnvelopeBytes),
		})
		if err != nil {
			t.Fatal(err)
		}
		host, err := serving.NewHost(multiGroupRF3HostLimits())
		if err != nil {
			t.Fatal(err)
		}
		identities := make([]raftmember.RuntimeIdentity, 0, cluster.groupCount)
		fences := make([]CommandFence, 0, cluster.groupCount)
		reads := make([]ReadSource, 0, cluster.groupCount)
		recovery := make([]TransactionRecoverySource, 0, cluster.groupCount)
		for group := 0; group < cluster.groupCount; group++ {
			runtime := cluster.groups[group].runtimes[member]
			if err := host.Add(runtime); err != nil {
				t.Fatal(err)
			}
			identities = append(identities, runtime.Identity())
			fences = append(fences, rf3CommandFence(runtime.Identity(), cluster.groups[group].bases[member]))
			reads = append(reads, cluster.groups[group].reads[member])
			recovery = append(recovery, cluster.groups[group].reads[member])
		}
		cluster.pulses[member] = make(chan struct{}, 1)
		remoteNodes := make([]rafttransport.NodeID, 0, multiGroupRF3Voters-1)
		for remote := 0; remote < multiGroupRF3Voters; remote++ {
			if remote != member {
				remoteNodes = append(remoteNodes, nodes[remote])
			}
		}
		localMember := member
		metrics := new(ProgressMetrics)
		if err := metrics.ConfigureGroups(identities); err != nil {
			t.Fatal(err)
		}
		peer, err := NewAuthenticatedPeerRuntime(AuthenticatedPeerOptions{
			Registry: registries[nodes[member]], TLS: peerTLS[member],
			Dial: func(ctx context.Context, node rafttransport.NodeID) (net.Conn, error) {
				for remote := range nodes {
					if nodes[remote] == node {
						return cluster.network.dial(ctx, localMember, remote)
					}
				}
				return nil, rafttransport.ErrNodeNotFound
			},
			Listener: cluster.listeners[member], HandshakeDeadline: deadline, MaxInboundStreams: 8,
			Owner: Options{
				Registry: serving, Host: host, Members: identities, CommandFences: fences,
				ReadSources: reads, TransactionRecoverySources: recovery,
				Pulse: cluster.pulses[member], ProgressMetrics: metrics,
				Limits: Limits{MaxIngressItems: 256, MaxIngressBytes: 128 << 20,
					MaxPendingProposalItems: 128, MaxPendingProposalBytes: 128 << 20,
					MaxPendingReadItems: 128, MaxPendingReadBytes: 128 << 20,
					MaxPendingOutboundBytes: 128 << 20},
			},
			Transport: rafttransport.OrdinaryTransportOptions{
				Peers: remoteNodes,
				Queue: rafttransport.QueueLimits{PerPeerFrames: 64, PerPeerBytes: 8 << 20,
					GlobalFrames: 128, GlobalBytes: 16 << 20},
				Coalesce: rafttransport.CoalesceLimits{MaxFrames: 8, MaxBytes: 1 << 20,
					RetainedBytes: rafttransport.DefaultRetainedFrameBytes},
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
			t.Fatal(err)
		}
		cluster.peers[member] = peer
		cluster.owners[member] = peer.Owner()
		cluster.progress[member] = metrics
		cluster.contexts[member], cluster.cancels[member] = context.WithCancel(context.Background())
		cluster.runErrors[member] = make(chan error, 1)
	}
	for member := 0; member < multiGroupRF3Voters; member++ {
		go func(member int) {
			cluster.runErrors[member] <- cluster.peers[member].Run(cluster.contexts[member])
		}(member)
	}
	for member := 0; member < multiGroupRF3Voters; member++ {
		select {
		case <-cluster.peers[member].Started():
			if !cluster.peers[member].Running() {
				t.Fatalf("multi-group peer %d did not publish serving", member)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("multi-group peer %d did not start", member)
		}
	}
	go pulseRF3(cluster.stopPulses, cluster.pulses[:])
	t.Cleanup(func() { cluster.close(t) })
	return cluster
}

func (cluster *multiGroupTransactionRF3Cluster) close(t testing.TB) {
	t.Helper()
	close(cluster.stopPulses)
	for _, cancel := range cluster.cancels {
		cancel()
	}
	cluster.network.close()
	for _, listener := range cluster.listeners {
		_ = listener.Close()
	}
	for member, done := range cluster.runErrors {
		select {
		case err := <-done:
			if t.Failed() {
				t.Logf("multi-group RF3 peer %d terminal error: %v", member, err)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("multi-group RF3 owner %d did not stop", member)
		}
	}
}

func (cluster *multiGroupTransactionRF3Cluster) submit(
	ctx context.Context,
	group int,
	member int,
	command []byte,
	fault multiGroupRF3Fault,
) (Result, error) {
	state, err := cluster.owners[member].Probe(ctx, cluster.groups[group].key)
	if err != nil {
		return Result{}, err
	}
	result, submitErr := cluster.owners[member].Submit(ctx, state.Fence(), command)
	trace := multiGroupRF3Trace{Group: group, Member: member, Command: bytes.Clone(command),
		CommandDigest: sha256.Sum256(command), FenceTerm: state.Status.Term,
		ObservedLeader: state.Status.LeaderID, Outcome: result.Outcome, Err: submitErr}
	if len(result.Completion) != 0 {
		trace.Completion = bytes.Clone(result.Completion)
		trace.CompletionDigest = sha256.Sum256(result.Completion)
	}
	if fault == multiGroupRF3HideResponseAndIsolate && submitErr == nil {
		trace.Hidden = true
		cluster.network.isolate(member)
		submitErr = &UnknownOutcomeError{Command: bytes.Clone(command), Cause: io.ErrUnexpectedEOF}
		trace.Err = submitErr
	}
	cluster.traceMu.Lock()
	cluster.trace = append(cluster.trace, trace)
	cluster.traceMu.Unlock()
	if fault == multiGroupRF3HideResponseAndIsolate {
		return Result{}, submitErr
	}
	return result, submitErr
}

func (cluster *multiGroupTransactionRF3Cluster) proposalTrace() []multiGroupRF3Trace {
	cluster.traceMu.Lock()
	defer cluster.traceMu.Unlock()
	trace := make([]multiGroupRF3Trace, len(cluster.trace))
	copy(trace, cluster.trace)
	for index := range trace {
		trace[index].Command = bytes.Clone(trace[index].Command)
		trace[index].Completion = bytes.Clone(trace[index].Completion)
	}
	return trace
}

func multiGroupRF3HostLimits() multiraft.Limits {
	return multiraft.Limits{
		MaxGroups: multiGroupRF3MaxGroups, MaxQueueItems: 512, MaxQueueBytes: 256 << 20,
		MaxGroupItems: 256, MaxGroupBytes: 128 << 20,
		MaxOutboxItems: 512, MaxOutboxBytes: 256 << 20,
		MaxPendingTicks: 16,
	}
}

func rf3CommandFence(
	runtime raftmember.RuntimeIdentity,
	base sqldriver.ReplicatedShardStoreIdentity,
) CommandFence {
	authority := base.Binding.Authority
	return CommandFence{
		ReplicaSetVersion: 1, ActivePolicyGeneration: authority.ActivePolicyGeneration,
		ProtectionEpoch: authority.ProtectionEpoch, OwnershipEpoch: authority.OwnershipEpoch,
		SchemaGeneration:       authority.SchemaGeneration,
		RelationManifestDigest: runtime.RelationManifestDigest,
		RoutingVersion:         authority.RoutingVersion,
		RouteGeneration:        authority.RouteGeneration,
	}
}

func pulseRF3(stop <-chan struct{}, pulses []chan struct{}) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			for _, pulse := range pulses {
				select {
				case pulse <- struct{}{}:
				default:
				}
			}
		}
	}
}

func waitRF3Leader(
	t testing.TB,
	ctx context.Context,
	owners []*Owner,
	removed map[int]bool,
	group raftmember.GroupKey,
) int {
	t.Helper()
	for ctx.Err() == nil {
		leader := -1
		var term uint64
		consistent := true
		for index, owner := range owners {
			if removed[index] {
				continue
			}
			state, err := owner.Probe(ctx, group)
			status := state.Status
			if err != nil || status.LeaderID == 0 {
				consistent = false
				break
			}
			candidate := int(status.LeaderID - 1)
			if removed[candidate] {
				consistent = false
				break
			}
			if leader == -1 {
				leader, term = candidate, status.Term
			} else if leader != candidate || term != status.Term {
				consistent = false
				break
			}
		}
		if consistent && leader >= 0 {
			state, err := owners[leader].Probe(ctx, group)
			if err == nil && state.Status.MemberID == state.Status.LeaderID && state.Status.Term == term {
				return leader
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("RF3 leader election: %v", context.Cause(ctx))
	return -1
}

func waitRF3Applied(
	t testing.TB,
	ctx context.Context,
	owners []*Owner,
	removed map[int]bool,
	group raftmember.GroupKey,
	index uint64,
) {
	t.Helper()
	for ctx.Err() == nil {
		complete := true
		for member, owner := range owners {
			if removed[member] {
				continue
			}
			state, err := owner.Probe(ctx, group)
			if err != nil || state.Status.Applied < index {
				complete = false
				break
			}
		}
		if complete {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("RF3 apply %d: %v", index, context.Cause(ctx))
}

func appendRF3Command(t testing.TB, command replication.Command) []byte {
	t.Helper()
	data, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func rf3Command(
	base sqldriver.ReplicatedShardStoreIdentity,
	kind replication.CommandKind,
	epoch, sequence uint64,
	mutations []replication.Mutation,
) replication.Command {
	binding := base.Binding
	command := replication.Command{
		Kind:      kind,
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: binding.Authority.ActivePolicyGeneration,
		ProtectionEpoch:        binding.Authority.ProtectionEpoch,
		OwnershipEpoch:         binding.Authority.OwnershipEpoch,
		SchemaGeneration:       binding.Authority.SchemaGeneration,
		RoutingVersion:         binding.Authority.RoutingVersion,
		RouteGeneration:        binding.Authority.RouteGeneration,
		Tenant:                 []byte("tenant"), ClientID: replication.ID128{0x44},
		ClientEpoch: epoch, ClientSequence: sequence,
		Fingerprint: sha256.Sum256([]byte{byte(kind), byte(sequence)}),
	}
	if len(mutations) != 0 {
		command.Batches = []replication.RelationMutationBatch{{Relation: 1, Mutations: mutations}}
	}
	return command
}

func rf3SessionOpenCommand(base sqldriver.ReplicatedShardStoreIdentity) replication.Command {
	command := rf3Command(base, replication.CommandSessionOpen, 0, 1, nil)
	command.NextDeadlineUnixNano = 2_000_000_000_000_000_000
	return command
}

func rf3TransactionCommand(
	t testing.TB,
	base sqldriver.ReplicatedShardStoreIdentity,
	control distributedtxn.ReplicatedCommand,
	batches []replication.RelationMutationBatch,
) []byte {
	t.Helper()
	// This direct-kernel fixture has one fixed controller tenure. Every
	// transition binds the same immutable execution witness for its transaction.
	control.ControllerEpoch = 1
	control.ExecutionPinDigest = distributedtxn.Digest(sha256.Sum256(control.ID[:]))
	transaction, err := distributedtxn.AppendReplicatedCommand(nil, control)
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := replication.TransactionClientSequence(transaction)
	if err != nil {
		t.Fatal(err)
	}
	binding := base.Binding
	command := replication.Command{
		Kind:      replication.CommandTransaction,
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: binding.Authority.ActivePolicyGeneration,
		ProtectionEpoch:        binding.Authority.ProtectionEpoch,
		OwnershipEpoch:         binding.Authority.OwnershipEpoch,
		SchemaGeneration:       binding.Authority.SchemaGeneration,
		RoutingVersion:         binding.Authority.RoutingVersion,
		RouteGeneration:        binding.Authority.RouteGeneration,
		Tenant:                 []byte("tenant"), ClientID: replication.ID128(control.ID),
		ClientEpoch: uint64(control.Role), ClientSequence: sequence,
		Fingerprint: sha256.Sum256(transaction), Transaction: transaction, Batches: batches,
	}
	return appendRF3Command(t, command)
}

func mustRF3State(
	t testing.TB,
	ctx context.Context,
	owner *Owner,
	group raftmember.GroupKey,
) ServingState {
	t.Helper()
	state, err := owner.Probe(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func newMultiGroupRF3Runtime(
	t testing.TB,
	group int,
	memberID uint64,
	walOptions raftstore.Options,
) (*raftmember.Runtime, *raftstore.Store, sqldriver.ReplicatedShardStoreIdentity, *sqldriver.ReplicatedApply, string) {
	t.Helper()
	shards := [...]string{"0000-7fff", "8000-ffff", "request-ledger"}
	distributions := [...]string{"orders-a", "orders-b", "durable-requests"}
	identity := raftstore.Identity{
		Distribution: distributions[group], Shard: shards[group],
		AllocationGeneration: uint64(7 + group), MemberID: memberID,
	}
	for index := range identity.ClusterID {
		identity.ClusterID[index] = byte(index + 1)
		identity.ClusterIncarnation[index] = byte(index + 21)
		identity.ShardIncarnation[index] = byte(index + 41 + group*17)
		identity.GroupID[index] = byte(index + 61 + group*17)
		identity.StoreID[index] = byte(index+81+group*17) ^ byte(memberID)
	}
	key := raftstore.Key{ID: "rf3-multigroup-key", Wrapped: []byte("opaque-wrapped-key")}
	for index := range key.Material {
		key.Material[index] = byte(index + 1 + group)
	}
	baseIndex, baseTerm := uint64(1), uint64(1)
	storageRoot := t.TempDir()
	wal, err := raftstore.Create(
		filepath.Join(storageRoot, "member.wal"), identity, key,
		raftstore.Bootstrap{TopologyRecoveryEpoch: 3, Snapshot: &pb.Snapshot{
			Data: []byte("rf3-multigroup-bootstrap"),
			Metadata: &pb.SnapshotMetadata{Index: &baseIndex, Term: &baseTerm,
				ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}},
		}},
		walOptions,
	)
	if err != nil {
		t.Fatal(err)
	}
	database, err := sqldriver.InitializeShardStore(
		filepath.Join(storageRoot, "member.vdb"), sqldriver.ShardStoreBinding{
			Distribution:         distribution.DistributionName(identity.Distribution),
			Shard:                distribution.ShardID(identity.Shard),
			AllocationGeneration: distribution.ShardAllocationGeneration(identity.AllocationGeneration),
		},
	)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	prepared, prepareErr := session.Prepare(context.Background(), `CREATE TABLE docs (PRIMARY KEY (id))`)
	if prepareErr == nil {
		_, prepareErr = prepared.Exec(context.Background(), nil)
	}
	if prepared != nil {
		prepareErr = errors.Join(prepareErr, prepared.Close())
	}
	err = errors.Join(prepareErr, session.Close())
	if err != nil {
		t.Fatal(err)
	}
	authority := sqldriver.ReplicatedAuthorityProfile{
		ActivePolicyGeneration: 5, ProtectionEpoch: 7, OwnershipEpoch: 11,
		SchemaGeneration: 13, RoutingVersion: 17, RouteGeneration: 19,
	}
	base, err := raftmember.BindPreparedSQL(wal, database, authority, "docs")
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
		_ = database.Close()
		_ = wal.Close()
		if os.Getenv("VIBEDB_RF3_QUORUM_QUALIFICATION") == "1" {
			t.Fatalf("strict allocation required by RF3 quorum qualification: %v", err)
		}
		if os.Getenv(multiGroupRF3DurableSQLRequiredEnvironment) ==
			multiGroupRF3DurableSQLRequiredEnvironmentEnable {
			t.Fatalf("required strict allocation unsupported: %v", err)
		}
		t.Skipf("strict allocation unsupported: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	applyOptions := sqldriver.ReplicatedApplyOptions{
		MaxSessions: 32, RetryWindow: 8,
		TxnLimits: durable.TxnLimits{MaxCollections: 8, MaxDocuments: 1024, MaxBytes: 256 << 20},
		Placement: sqldriver.ReplicatedPlacementProfile{
			Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: "/id",
			TupleVersion:  distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			Range:         distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
	}
	if group == multiGroupRF3LedgerGroup {
		applyOptions.RequestLedgerCapacityBytes = multiGroupRF3LedgerCapacityBytes
		applyOptions.RequestLedgerCleanupReserveBytes = multiGroupRF3LedgerCleanupReserveBytes
		applyOptions.RequestLedgerRangeIdentity = multiGroupRF3RequestLedgerRangeIdentity(group)
	}
	apply, _, err := raftmember.OpenPreparedApply(
		wal, database, authority, base, applyOptions,
	)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := wal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	runtime, err := raftmember.AdoptPipelinedRuntime(wal, database, apply)
	if err != nil {
		t.Fatal(err)
	}
	// Match the production runtime: a busy shard must be able to replace its
	// immutable WAL generation under admission pressure instead of failing once
	// the original preallocation fills. Keep the periodic cadence outside normal
	// tests; ReserveReady's pressure path still forces a replacement when needed.
	if err := runtime.ConfigureWALGeneration(raftmember.WALGenerationDriverOptions{
		IntervalTicks: 12000,
		Key:           key,
	}); err != nil {
		_ = runtime.Close()
		t.Fatal(err)
	}
	return runtime, wal, base, apply, storageRoot
}

func TestTwoRealRF3GroupsExecuteFusedTwoTargetTransactionAcrossLeaderIsolation(t *testing.T) {
	cluster := newMultiGroupTransactionRF3Cluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := cluster.owners[0].Campaign(ctx, cluster.groups[0].key); err != nil {
		t.Fatal(err)
	}
	leader0 := waitRF3Leader(t, ctx, cluster.owners[:], nil, cluster.groups[0].key)
	if err := cluster.owners[1].Campaign(ctx, cluster.groups[1].key); err != nil {
		t.Fatal(err)
	}
	leader1 := waitRF3Leader(t, ctx, cluster.owners[:], nil, cluster.groups[1].key)
	leader0 = waitRF3Leader(t, ctx, cluster.owners[:], nil, cluster.groups[0].key)
	if leader0 == leader1 {
		t.Fatalf("two groups elected the same deliberately separated leader %d", leader0)
	}

	for group, leader := range []int{leader0, leader1} {
		open := rf3SessionOpenCommand(cluster.groups[group].bases[leader])
		result, err := cluster.submit(ctx, group, leader, appendRF3Command(t, open), multiGroupRF3NoFault)
		if err != nil {
			t.Fatalf("group %d session open: %v", group, err)
		}
		waitRF3Applied(t, ctx, cluster.owners[:], nil, cluster.groups[group].key, result.Outcome.AppliedIndex)
	}

	keys := make([][]byte, multiGroupRF3Groups)
	values := make([][]byte, multiGroupRF3Groups)
	batches := make([][]replication.RelationMutationBatch, multiGroupRF3Groups)
	digests := make([]distributedtxn.Digest, multiGroupRF3Groups)
	for group := 0; group < multiGroupRF3Groups; group++ {
		var ok bool
		keys[group], ok = orderedkey.AppendJSONString(
			nil, []byte(`"rf3-multigroup-`+string(rune('a'+group))+`"`), orderedkey.Ascending,
		)
		if !ok {
			t.Fatalf("encode group %d key", group)
		}
		var err error
		values[group], err = vibejson.AppendCanonicalize(
			nil, []byte(`{"id":"rf3-multigroup-`+string(rune('a'+group))+`","group":`+string(rune('0'+group))+`}`),
		)
		if err != nil {
			t.Fatal(err)
		}
		batches[group] = []replication.RelationMutationBatch{{Relation: 1,
			Mutations: []replication.Mutation{{Kind: replication.MutationPutAbsentOrEqual,
				Key: keys[group], Value: values[group]}}}}
		digests[group], err = replication.TransactionMutationDigest(batches[group])
		if err != nil {
			t.Fatal(err)
		}
	}

	id := distributedtxn.ID{0x70, 0x32, 0x2d, 0x72, 0x66, 0x33, 1}
	targets := make([]distributedtxn.TransactionTargetRef, multiGroupRF3Groups)
	for group, leader := range []int{leader0, leader1} {
		binding := cluster.groups[group].bases[leader].Binding
		route := cluster.route(group)
		targets[group] = distributedtxn.TransactionTargetRef{
			Distribution: []byte(binding.Distribution), Shard: []byte(binding.Shard),
			RoutingVersion:       uint64(binding.Authority.RoutingVersion),
			AllocationGeneration: binding.AllocationGeneration,
			OwnershipEpoch:       uint64(binding.Authority.OwnershipEpoch),
			AuthorityWitness:     multiGroupRF3RouteAuthorityWitness(route),
			MutationDigest:       digests[group], State: distributedtxn.TargetStaged,
		}
	}
	coordinatorRecord, err := distributedtxn.AppendCoordinator(nil, distributedtxn.CoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: uint64(cluster.groups[0].bases[leader0].Binding.Authority.SchemaGeneration),
		RecoveryDeadline:  int64(distributedtxn.MaxRecoveryPulses), Targets: targets,
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinatorBinding := cluster.groups[0].bases[leader0].Binding
	begin := rf3TransactionCommand(t, cluster.groups[0].bases[leader0], distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedBeginPrepareCoordinator,
		ID:        id, PayloadKind: distributedtxn.ReplicatedPayloadCoordinator, Payload: coordinatorRecord,
		Target: distributedtxn.TransactionTargetStage{
			CoordinatorGroup:            distributedtxn.ID(coordinatorBinding.GroupID),
			CoordinatorShardIncarnation: distributedtxn.ID(coordinatorBinding.ShardIncarnation),
			CoordinatorAllocation:       coordinatorBinding.AllocationGeneration,
			BucketBits:                  8, IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}},
			MutationDigest: digests[0], TargetOrdinal: 0,
		},
	}, batches[0])
	beginResult, err := cluster.submit(ctx, 0, leader0, begin, multiGroupRF3NoFault)
	if err != nil {
		t.Fatal(err)
	}
	if result := openMultiGroupRF3TransactionResult(t, beginResult); result.AffectedRowsValid {
		t.Fatalf("coordinator begin exposed affected rows: %+v", result)
	}

	remotePrepare := rf3TransactionCommand(t, cluster.groups[1].bases[leader1], distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleTarget,
		Operation: distributedtxn.ReplicatedStagePrepareTarget,
		ID:        id, PayloadKind: distributedtxn.ReplicatedPayloadTargetStage,
		Target: distributedtxn.TransactionTargetStage{
			CoordinatorGroup:            distributedtxn.ID(coordinatorBinding.GroupID),
			CoordinatorShardIncarnation: distributedtxn.ID(coordinatorBinding.ShardIncarnation),
			CoordinatorAllocation:       coordinatorBinding.AllocationGeneration,
			BucketBits:                  8, IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}},
			MutationDigest: digests[1], TargetOrdinal: 1,
		},
	}, batches[1])
	remoteResult, err := cluster.submit(ctx, 1, leader1, remotePrepare, multiGroupRF3NoFault)
	if err != nil {
		t.Fatal(err)
	}
	if result := openMultiGroupRF3TransactionResult(t, remoteResult); result.AffectedRowsValid {
		t.Fatalf("remote prepare exposed affected rows: %+v", result)
	}
	waitRF3Applied(t, ctx, cluster.owners[:], nil, cluster.groups[0].key, beginResult.Outcome.AppliedIndex)
	waitRF3Applied(t, ctx, cluster.owners[:], nil, cluster.groups[1].key, remoteResult.Outcome.AppliedIndex)

	assertMultiGroupRF3Recovery(t, ctx, cluster, 0, leader0, id,
		replicatedstate.TransactionRecoveryLookupCoordinator, coordinatorRecord, coordinatorBinding)
	assertMultiGroupRF3Recovery(t, ctx, cluster, 0, leader0, id,
		replicatedstate.TransactionRecoveryLookupTarget, nil, coordinatorBinding)
	assertMultiGroupRF3Recovery(t, ctx, cluster, 1, leader1, id,
		replicatedstate.TransactionRecoveryLookupTarget, nil, coordinatorBinding)

	group1Before := mustRF3State(t, ctx, cluster.owners[leader1], cluster.groups[1].key)
	formerState := mustRF3State(t, ctx, cluster.owners[leader0], cluster.groups[0].key)
	commit := rf3TransactionCommand(t, cluster.groups[0].bases[leader0], distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedCommitCoordinator,
		ID:        id, ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	_, err = cluster.submit(
		ctx, 0, leader0, commit, multiGroupRF3HideResponseAndIsolate,
	)
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("hidden commit error=%v", err)
	}
	hiddenResult := hiddenMultiGroupRF3Result(t, cluster, sha256.Sum256(commit))
	removed := map[int]bool{leader0: true}
	candidate := (leader0 + 1) % multiGroupRF3Voters
	if err := cluster.owners[candidate].Campaign(ctx, cluster.groups[0].key); err != nil {
		t.Fatal(err)
	}
	newLeader0 := waitRF3Leader(t, ctx, cluster.owners[:], removed, cluster.groups[0].key)
	group1After := mustRF3State(t, ctx, cluster.owners[leader1], cluster.groups[1].key)
	if group1After.Status.LeaderID != group1Before.Status.LeaderID ||
		group1After.Status.Term != group1Before.Status.Term {
		t.Fatalf("unisolated group changed leadership: before=%+v after=%+v",
			group1Before.Status, group1After.Status)
	}

	isolatedCtx, isolatedCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	_, isolatedErr := cluster.submit(isolatedCtx, 0, leader0, commit, multiGroupRF3NoFault)
	isolatedCancel()
	if !multiGroupRF3SafeProposalIsolationError(isolatedErr) {
		t.Fatalf("isolated former leader proposal error=%v", isolatedErr)
	}
	isolatedCtx, isolatedCancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
	point, lease, readErr := cluster.owners[leader0].ReadPoint(isolatedCtx, PointReadRequest{
		Fence: formerState.Fence(), Relation: 1, Key: keys[0],
		MinimumApplied: beginResult.Outcome.AppliedIndex,
		MaxValueBytes:  replication.MaxMutationValueBytes, Linearizable: true,
	})
	isolatedCancel()
	if !multiGroupRF3SafeIsolationError(readErr) || lease != nil || point.Found {
		t.Fatalf("isolated former leader linear read=%+v lease=%T err=%v", point, lease, readErr)
	}
	isolatedCtx, isolatedCancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
	recovery, recoveryLease, recoveryErr := cluster.owners[leader0].ReadTransaction(
		isolatedCtx, TransactionReadRequest{Fence: formerState.Fence(),
			Capability: serviceauthz.CapabilityTransactionRecovery,
			Read: replicatedstate.TransactionRecoveryReadRequest{
				Kind: replicatedstate.TransactionRecoveryLookupCoordinator, ID: id,
				MinimumApplied: beginResult.Outcome.AppliedIndex, MaxRows: 1,
				MaxBytes: uint32(replicatedstate.TransactionRecoverySummaryBytes +
					distributedtxn.MaxCoordinatorRecordBytes),
			}},
	)
	isolatedCancel()
	if !multiGroupRF3SafeIsolationError(recoveryErr) || recoveryLease != nil ||
		len(recovery.Records) != 0 {
		t.Fatalf("isolated former leader recovery=%+v lease=%T err=%v",
			recovery, recoveryLease, recoveryErr)
	}

	retry, err := cluster.submit(ctx, 0, newLeader0, commit, multiGroupRF3NoFault)
	if err != nil || !bytes.Equal(retry.Completion, hiddenResult.Completion) ||
		retry.Outcome.CompletionAppliedSequence != hiddenResult.Outcome.CompletionAppliedSequence {
		t.Fatalf("commit retry first=%+v retry=%+v err=%v", hiddenResult.Outcome, retry.Outcome, err)
	}

	applyResults := make([]Result, multiGroupRF3Groups)
	for group, leader := range []int{newLeader0, leader1} {
		apply := rf3TransactionCommand(t, cluster.groups[group].bases[leader], distributedtxn.ReplicatedCommand{
			Role:      distributedtxn.ReplicatedRoleTarget,
			Operation: distributedtxn.ReplicatedApplyReleaseTarget,
			ID:        id, ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone,
		}, nil)
		applyResults[group], err = cluster.submit(ctx, group, leader, apply, multiGroupRF3NoFault)
		if err != nil {
			t.Fatalf("group %d apply-release: %v", group, err)
		}
		transaction := openMultiGroupRF3TransactionResult(t, applyResults[group])
		if !transaction.AffectedRowsValid || transaction.AffectedRows != 1 {
			t.Fatalf("group %d apply-release result=%+v", group, transaction)
		}
	}
	waitRF3Applied(t, ctx, cluster.owners[:], removed, cluster.groups[0].key,
		applyResults[0].Outcome.AppliedIndex)
	waitRF3Applied(t, ctx, cluster.owners[:], removed, cluster.groups[1].key,
		applyResults[1].Outcome.AppliedIndex)

	for group := 0; group < multiGroupRF3Groups; group++ {
		for member := 0; member < multiGroupRF3Voters; member++ {
			if removed[member] {
				continue
			}
			state := mustRF3State(t, ctx, cluster.owners[member], cluster.groups[group].key)
			got, lease, err := cluster.owners[member].ReadPoint(ctx, PointReadRequest{
				Fence: state.Fence(), Relation: 1, Key: keys[group],
				MinimumApplied: applyResults[group].Outcome.AppliedIndex,
				MaxValueBytes:  replication.MaxMutationValueBytes,
			})
			if err != nil || lease == nil || !got.Found || !bytes.Equal(got.Value, values[group]) {
				t.Fatalf("group %d member %d intended read=%+v lease=%T err=%v",
					group, member, got, lease, err)
			}
			lease.Release()
			wrong := 1 - group
			got, lease, err = cluster.owners[member].ReadPoint(ctx, PointReadRequest{
				Fence: state.Fence(), Relation: 1, Key: keys[wrong],
				MinimumApplied: applyResults[group].Outcome.AppliedIndex,
				MaxValueBytes:  replication.MaxMutationValueBytes,
			})
			if err != nil || lease == nil || got.Found {
				t.Fatalf("group %d member %d cross-group read=%+v lease=%T err=%v",
					group, member, got, lease, err)
			}
			lease.Release()
		}
	}
	assertMultiGroupRF3Trace(t, cluster, commit, hiddenResult.Completion)
}

// TestRF3AllThreeVoterQuorumCutsFailClosedOrCommit enumerates the complete
// three-voter reachability mask. Every majority cut must elect and commit; each
// one-voter cut must return only a safe refusal or outcome-unknown timeout.
// This is an in-process authenticated-transport fault gate. The external
// shipped-process matrix remains separately disclosed in feature state.
func TestRF3AllThreeVoterQuorumCutsFailClosedOrCommit(t *testing.T) {
	for activeMask := uint8(0); activeMask < 1<<multiGroupRF3Voters; activeMask++ {
		t.Run(fmt.Sprintf("active_%03b", activeMask), func(t *testing.T) {
			cluster := newMultiGroupRF3Cluster(t, 1)
			active := make([]int, 0, multiGroupRF3Voters)
			removed := make(map[int]bool, multiGroupRF3Voters)
			for member := 0; member < multiGroupRF3Voters; member++ {
				if activeMask&(1<<member) != 0 {
					active = append(active, member)
					continue
				}
				removed[member] = true
				cluster.network.isolate(member)
			}
			if len(active) == 0 {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			candidate := active[0]
			if err := cluster.owners[candidate].Campaign(ctx, cluster.groups[0].key); err != nil {
				t.Fatal(err)
			}
			command := appendRF3Command(t, rf3SessionOpenCommand(cluster.groups[0].bases[candidate]))
			if bits.OnesCount8(activeMask) >= 2 {
				leader := waitRF3Leader(t, ctx, cluster.owners[:], removed, cluster.groups[0].key)
				result, err := cluster.submit(ctx, 0, leader, command, multiGroupRF3NoFault)
				if err != nil || result.Outcome.AppliedIndex == 0 {
					t.Fatalf("majority cut result=%+v err=%v", result.Outcome, err)
				}
				waitRF3Applied(t, ctx, cluster.owners[:], removed, cluster.groups[0].key,
					result.Outcome.AppliedIndex)
				return
			}
			minorityCtx, minorityCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			result, err := cluster.submit(minorityCtx, 0, candidate, command, multiGroupRF3NoFault)
			minorityCancel()
			// Cancellation may win before owner ingress. That is a definite
			// non-admission, not an outcome-unknown proposal. Neither path may
			// return a completion from the isolated minority.
			preAdmissionTimeout := errors.Is(err, context.DeadlineExceeded) &&
				!errors.Is(err, ErrOutcomeUnknown) && result.Outcome == (raftserve.Outcome{}) &&
				len(result.Completion) == 0
			if !multiGroupRF3SafeProposalIsolationError(err) && !preAdmissionTimeout {
				t.Fatalf("minority cut error=%v", err)
			}
			if result.Outcome.AppliedIndex != 0 || len(result.Completion) != 0 {
				t.Fatalf("minority cut returned an applied result: %+v", result.Outcome)
			}
		})
	}
}

// This host-only preflight exercises the exact fixture constructors before any
// strict-allocation setup can skip the RF3 fault tests on unsupported hosts.
func TestMultiGroupRF3CommandFixturesPreflight(t *testing.T) {
	base := sqldriver.ReplicatedShardStoreIdentity{Binding: sqldriver.ReplicatedShardStoreBinding{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}, TopologyRecoveryEpoch: 3,
		Distribution: "orders-a", Shard: "0000-7fff", AllocationGeneration: 7,
		ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4},
		Authority: sqldriver.ReplicatedAuthorityProfile{
			ActivePolicyGeneration: 5, ProtectionEpoch: 7, OwnershipEpoch: 11,
			SchemaGeneration: 13, RoutingVersion: 17, RouteGeneration: 19,
		},
	}}
	open, err := replication.OpenCommand(appendRF3Command(t, rf3SessionOpenCommand(base)))
	if err != nil || open.Kind() != replication.CommandSessionOpen || open.ClientEpoch != 0 ||
		open.ClientSequence != 1 || open.NextDeadlineUnixNano == 0 {
		t.Fatalf("canonical session-open fixture: %+v, %v", open, err)
	}
	id := distributedtxn.ID{0x70, 0x32, 1}
	key, ok := orderedkey.AppendJSONString(nil, []byte(`"fixture"`), orderedkey.Ascending)
	if !ok {
		t.Fatal("encode fixture key")
	}
	batches := []replication.RelationMutationBatch{{Relation: 1,
		Mutations: []replication.Mutation{{Kind: replication.MutationPutAbsentOrEqual,
			Key: key, Value: []byte(`{"id":"fixture"}`)}}}}
	digest, err := replication.TransactionMutationDigest(batches)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := distributedtxn.AppendCoordinator(nil, distributedtxn.CoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: 13, RecoveryDeadline: int64(distributedtxn.MaxRecoveryPulses),
		Targets: []distributedtxn.TransactionTargetRef{{Distribution: []byte("orders-a"),
			Shard: []byte("0000-7fff"), RoutingVersion: 17, AllocationGeneration: 7,
			OwnershipEpoch: 11, AuthorityWitness: distributedtxn.AuthorityWitness{1},
			MutationDigest: digest, State: distributedtxn.TargetStaged}},
	})
	if err != nil {
		t.Fatal(err)
	}
	target := distributedtxn.TransactionTargetStage{
		CoordinatorGroup:            distributedtxn.ID(base.Binding.GroupID),
		CoordinatorShardIncarnation: distributedtxn.ID(base.Binding.ShardIncarnation),
		CoordinatorAllocation:       7, BucketBits: 8,
		IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}}, MutationDigest: digest,
	}
	for _, control := range []distributedtxn.ReplicatedCommand{
		{Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedBeginPrepareCoordinator,
			ID: id, PayloadKind: distributedtxn.ReplicatedPayloadCoordinator, Payload: coordinator, Target: target},
		{Role: distributedtxn.ReplicatedRoleTarget, Operation: distributedtxn.ReplicatedStagePrepareTarget,
			ID: id, PayloadKind: distributedtxn.ReplicatedPayloadTargetStage, Target: target},
		{Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedCommitCoordinator,
			ID: id, ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone},
		{Role: distributedtxn.ReplicatedRoleTarget, Operation: distributedtxn.ReplicatedApplyReleaseTarget,
			ID: id, ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone},
	} {
		var mutations []replication.RelationMutationBatch
		if control.PayloadKind != distributedtxn.ReplicatedPayloadNone {
			mutations = batches
		}
		outer, openErr := replication.OpenCommand(rf3TransactionCommand(t, base, control, mutations))
		if openErr != nil {
			t.Fatal(openErr)
		}
		inner, openErr := distributedtxn.OpenReplicatedCommand(outer.TransactionBytes())
		if openErr != nil || inner.ControllerEpoch != 1 ||
			inner.ExecutionPinDigest != distributedtxn.Digest(sha256.Sum256(id[:])) {
			t.Fatalf("fenced transaction operation %d: %+v, %v", control.Operation, inner, openErr)
		}
	}
}

func multiGroupRF3SafeIsolationError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, raftmodel.ErrReadLeadershipLost) ||
		errors.Is(err, raftmodel.ErrNotLeader) || errors.Is(err, ErrServingFence)
}

func multiGroupRF3SafeProposalIsolationError(err error) bool {
	return (errors.Is(err, context.DeadlineExceeded) && errors.Is(err, ErrOutcomeUnknown)) ||
		errors.Is(err, raftmodel.ErrNotLeader) || errors.Is(err, ErrServingFence)
}

func openMultiGroupRF3TransactionResult(
	t testing.TB,
	result Result,
) replicatedstate.TransactionCompletionResult {
	t.Helper()
	completion, err := replication.OpenCompletion(result.Completion)
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := replicatedstate.OpenTransactionCompletionResult(
		completion.ResultCode, completion.InlineResult,
	)
	if err != nil {
		t.Fatal(err)
	}
	return transaction
}

func hiddenMultiGroupRF3Result(
	t testing.TB,
	cluster *multiGroupTransactionRF3Cluster,
	command [sha256.Size]byte,
) Result {
	t.Helper()
	for _, entry := range cluster.proposalTrace() {
		if entry.Hidden && entry.CommandDigest == command {
			return Result{Outcome: entry.Outcome, Completion: bytes.Clone(entry.Completion)}
		}
	}
	t.Fatal("hidden proposal result was not retained in trace")
	return Result{}
}

func assertMultiGroupRF3Recovery(
	t testing.TB,
	ctx context.Context,
	cluster *multiGroupTransactionRF3Cluster,
	group int,
	leader int,
	id distributedtxn.ID,
	kind replicatedstate.TransactionRecoveryReadKind,
	wantPayload []byte,
	coordinator sqldriver.ReplicatedShardStoreBinding,
) {
	t.Helper()
	state := mustRF3State(t, ctx, cluster.owners[leader], cluster.groups[group].key)
	maxBytes := uint32(replicatedstate.TransactionRecoverySummaryBytes)
	if kind == replicatedstate.TransactionRecoveryLookupCoordinator {
		maxBytes += uint32(distributedtxn.MaxCoordinatorRecordBytes)
	}
	result, lease, err := cluster.owners[leader].ReadTransaction(ctx, TransactionReadRequest{
		Fence: state.Fence(), Capability: serviceauthz.CapabilityTransactionRecovery,
		Read: replicatedstate.TransactionRecoveryReadRequest{
			Kind: kind, ID: id, MinimumApplied: state.Status.Applied,
			MaxRows: 1, MaxBytes: maxBytes,
		},
	})
	if err != nil || lease == nil || len(result.Records) != 1 ||
		!bytes.Equal(result.Records[0].Payload, wantPayload) ||
		result.Records[0].CoordinatorGroup != coordinator.GroupID ||
		result.Records[0].CoordinatorShardIncarnation != coordinator.ShardIncarnation ||
		result.Records[0].CoordinatorAllocation != coordinator.AllocationGeneration {
		t.Fatalf("group %d recovery kind=%d result=%+v lease=%T err=%v",
			group, kind, result, lease, err)
	}
	lease.Release()
}

func assertMultiGroupRF3Trace(
	t testing.TB,
	cluster *multiGroupTransactionRF3Cluster,
	commit []byte,
	hiddenCompletion []byte,
) {
	t.Helper()
	trace := cluster.proposalTrace()
	commitDigest := sha256.Sum256(commit)
	hidden := 0
	commitSuccess := 0
	commitFailures := 0
	for index, entry := range trace {
		command, err := replication.OpenCommand(entry.Command)
		if err != nil {
			t.Fatalf("trace %d command validation: %v", index, err)
		}
		if entry.Group < 0 || entry.Group >= multiGroupRF3Groups ||
			entry.Member < 0 || entry.Member >= multiGroupRF3Voters {
			t.Fatalf("trace %d invalid route: %+v", index, entry)
		}
		group := cluster.groups[entry.Group].key
		if command.GroupID != replication.ID128(group.GroupID) ||
			command.ShardIncarnation != replication.ID128(group.ShardIncarnation) ||
			command.ClusterID != replication.ID128(group.ClusterID) ||
			command.ClusterIncarnation != replication.ID128(group.ClusterIncarnation) ||
			command.TopologyRecoveryEpoch != group.TopologyRecoveryEpoch {
			t.Fatalf("trace %d command entered wrong group: trace=%+v command=%+v", index, entry, command)
		}
		if entry.Hidden {
			hidden++
			if !bytes.Equal(entry.Completion, hiddenCompletion) {
				t.Fatal("hidden trace changed completion bytes")
			}
		}
		if entry.CommandDigest == commitDigest {
			if entry.Err == nil {
				commitSuccess++
			} else {
				commitFailures++
				if !errors.Is(entry.Err, ErrOutcomeUnknown) &&
					!errors.Is(entry.Err, raftmodel.ErrNotLeader) &&
					!errors.Is(entry.Err, ErrServingFence) {
					t.Fatalf("commit trace carried unsafe failure: %v", entry.Err)
				}
			}
		}
	}
	if hidden != 1 || commitSuccess != 1 || commitFailures != 2 {
		t.Fatalf("proposal trace hidden=%d commit-success=%d commit-failures=%d trace=%+v",
			hidden, commitSuccess, commitFailures, trace)
	}
}

func pinnedPeerTestRegistry(t testing.TB, local rafttransport.NodeID, members []rafttransport.Member, limits rafttransport.Limits, profiles []*rafttransport.PeerTLS) *rafttransport.StaticRegistry {
	t.Helper()
	peers := make([]rafttransport.PhysicalPeer, len(profiles))
	for index, profile := range profiles {
		peers[index] = rafttransport.PhysicalPeer{NodeID: profile.LocalIdentity().Node, TrustDomain: profile.LocalIdentity().TrustDomain, Incarnation: 1, Revision: 1, ServiceKeyDigest: profile.LocalServiceKeyDigest(), State: rafttransport.PeerEnrolled}
	}
	registry, err := rafttransport.NewStaticRegistryWithDirectory(local, members, peers, 1, limits)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}
