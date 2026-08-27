//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/kubeoperator"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
)

const restoreRF3ProcessEnvironment = "VIBEDB_RESTORE_RF3_PROCESS_E2E"

// TestRestoredRF3ExternalProcessServingAndFailover exercises the shipped
// adopter and serve-rf3 composition over actual restored SQL/WAL roots. The
// catalog boundary is an exact, separately observed test authority; catalog
// replication itself is qualified by the catalog RF3 tests. No marker file or
// local receipt grants serving permission in this test.
func TestRestoredRF3ExternalProcessServingAndFailover(t *testing.T) {
	if os.Getenv(restoreRF3ProcessEnvironment) != "1" {
		t.Skip("set " + restoreRF3ProcessEnvironment + "=1 for the bounded Linux process gate")
	}
	started := time.Now()
	fixture, options, catalog := newRestoredRF3ProcessFixture(t)
	defer fixture.close(t)
	fixture.startAll(t)
	for member := range fixture.children {
		restoreRF3AssertClosed(t, fixture, member)
	}
	if catalog.published || catalog.observations != 0 {
		t.Fatal("catalog activated before the closed-serving assertion")
	}
	deadline := func() time.Time { return time.Now().Add(3 * time.Second) }
	addresses := make(map[rafttransport.NodeID]string, rf3CommandMembers)
	for member, node := range fixture.nodes {
		addresses[node] = fixture.controlAddresses[member]
	}
	operatorProfile, err := servicetls.LoadProfile(fixture.credentials[3].Certificate,
		fixture.credentials[3].Key, fixture.roots, rf3testfixture.ProcessIdentityOID, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	client, err := shardservice.NewRestoreServingControlClient(rf3CommandControlOpener{
		profile: operatorProfile, addresses: addresses, deadline: deadline,
	}, deadline, deadline)
	if err != nil {
		t.Fatal(err)
	}
	options.Serving = client
	authority, _, err := gateway.ActivateRestore(t.Context(), options)
	if err != nil || authority == nil || !catalog.published || catalog.observations != 1 {
		t.Fatalf("catalog-observed activation: authority=%v observations=%d err=%v", authority != nil, catalog.observations, err)
	}
	leader, states := fixture.waitLeader(t, []int{0, 1, 2}, 15*time.Second)
	fixture.waitDocument(t, leader, states[leader], "restored-row", 5*time.Second)
	epoch, _ := fixture.openSession(t, leader, states[leader])
	states[leader] = fixture.probe(t, leader)
	command := fixture.mutationCommand(t, states[leader], epoch, 2, "acknowledged-after-restore")
	writeStarted := time.Now()
	settled := fixture.propose(t, leader, states[leader], command)
	writeLatency := time.Since(writeStarted)
	if settled.Kind != shardservice.ReplicatedCompletion || settled.Outcome.AppliedIndex == 0 {
		t.Fatalf("restored quorum write did not settle: %+v", settled)
	}
	fixture.waitAllApplied(t, settled.Outcome.AppliedIndex, 10*time.Second)
	rssBaseline := rf3FaultProcessRSSBytes(t, fixture.children)
	walBaseline := rf3FaultWALAllocatedBytes(t, fixture.walPaths)
	storageBaseline := restoreRF3AllocatedBytes(t, options.Installer.(*restoreRF3Installer).root)

	failoverStarted := time.Now()
	fixture.kill(t, leader)
	newLeader, liveStates := fixture.waitLeader(t, rf3FaultOtherMembers(leader), 15*time.Second)
	fixture.waitDocument(t, newLeader, liveStates[newLeader], "restored-row", 5*time.Second)
	fixture.waitDocument(t, newLeader, liveStates[newLeader], "acknowledged-after-restore", 5*time.Second)
	failoverLatency := time.Since(failoverStarted)
	fixture.restart(t, leader)
	restoreRF3AssertClosed(t, fixture, leader)
	// Replay the complete gateway activation. The durable operation is already
	// terminal, but a fresh catalog observation and grant broadcast are required
	// to open the restarted process; local serving.permit is insufficient.
	if _, _, err = gateway.ActivateRestore(t.Context(), options); err != nil {
		t.Fatalf("re-observe and regrant after restart: %v", err)
	}
	if catalog.observations != 2 {
		t.Fatalf("restart reused a cached catalog witness: observations=%d", catalog.observations)
	}
	fixture.waitCaughtUp(t, leader, settled.Outcome.AppliedIndex, 10*time.Second)
	newLeader, liveStates = fixture.waitLeader(t, []int{0, 1, 2}, 15*time.Second)
	fixture.waitDocument(t, newLeader, liveStates[newLeader], "acknowledged-after-restore", 5*time.Second)

	rssFinal := rf3FaultProcessRSSBytes(t, fixture.children)
	walFinal := rf3FaultWALAllocatedBytes(t, fixture.walPaths)
	storageFinal := restoreRF3AllocatedBytes(t, options.Installer.(*restoreRF3Installer).root)
	if time.Since(started) > 90*time.Second || writeLatency > 5*time.Second || failoverLatency > 20*time.Second {
		t.Fatalf("latency bound: total=%s write=%s failover=%s", time.Since(started), writeLatency, failoverLatency)
	}
	if rssFinal > 768<<20 || rssFinal > rssBaseline+128<<20 ||
		walFinal > 768<<20 || walFinal > walBaseline+32<<20 ||
		storageFinal > 1<<30 || storageFinal > storageBaseline+64<<20 {
		t.Fatalf("space bound: rss=%d/%d wal=%d/%d storage=%d/%d", rssBaseline, rssFinal, walBaseline, walFinal, storageBaseline, storageFinal)
	}
	t.Logf("restored external RF3: shard_processes=3 certified_artifact=true fresh_roots=true closed_before_catalog=true catalog_witness=test_boundary catalog_observations=%d restored_read=true acknowledged_write_survived=true leader_sigkill=true restart_closed=true regrant=true total=%s write=%s failover=%s rss_bytes=%d rss_growth=%d storage_growth=%d wal_growth=%d",
		catalog.observations, time.Since(started), writeLatency, failoverLatency, rssFinal,
		max(int64(rssFinal)-int64(rssBaseline), 0), max(storageFinal-storageBaseline, 0), max(walFinal-walBaseline, 0))
}

type restoreRF3Installer struct {
	root     string
	template []byte
}

func (installer *restoreRF3Installer) Install(ctx context.Context, operation clusterrestore.Operation,
	ordinal uint32, artifact io.Reader,
) (clusterrestore.RootWitness, error) {
	result, err := kubeoperator.RestoreGroup(ctx, kubeoperator.RestoreGroupConfig{
		Root: installer.root, Template: installer.template, Operation: operation, Ordinal: ordinal, Artifact: artifact,
	})
	return result.Witness, err
}

type restoreRF3Catalog struct {
	witness      clusterrestore.CatalogWitness
	published    bool
	observations int
}

func (catalog *restoreRF3Catalog) ProposeRestoreActivation(_ context.Context, raw []byte) ([]byte, error) {
	witness, err := clusterrestore.OpenCatalogActivation(raw)
	if err != nil || catalog.published && catalog.witness != witness {
		return nil, clusterrestore.ErrActivation
	}
	catalog.witness, catalog.published = witness, true
	return append([]byte(nil), witness.CatalogDigest[:]...), nil
}

func (catalog *restoreRF3Catalog) ObserveRestoreActivation(_ context.Context, operation [32]byte) (clusterrestore.CatalogWitness, error) {
	if !catalog.published || catalog.witness.Operation != operation {
		return clusterrestore.CatalogWitness{}, clusterrestore.ErrActivation
	}
	catalog.observations++
	return catalog.witness, nil
}

func newRestoredRF3ProcessFixture(t *testing.T) (*rf3FaultFixture, gateway.RestoreActivationOptions, *restoreRF3Catalog) {
	t.Helper()
	fixture := &rf3FaultFixture{root: t.TempDir(), group: rf3CommandGroup(), nodes: rf3CommandNodes(),
		authority: sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: 5, ProtectionEpoch: 1,
			OwnershipEpoch: 1, SchemaGeneration: 13, RoutingVersion: 1, RouteGeneration: 1}}
	for member := range fixture.listeners {
		for lane := range fixture.listeners[member] {
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
	t.Cleanup(func() { fixture.close(t) })
	var err error
	operatorNode := rafttransport.NodeID{0xee, 0x42}
	credentialNodes := append(append([]rafttransport.NodeID(nil), fixture.nodes[:]...), operatorNode)
	fixture.credentials, fixture.roots, err = rf3testfixture.WriteCredentials(fixture.root, rf3CommandIdentityOID,
		rafttransport.TrustDomain{ClusterID: fixture.group.ClusterID, ClusterIncarnation: fixture.group.ClusterIncarnation}, credentialNodes)
	if err != nil {
		t.Fatal(err)
	}
	policyRaw := rf3FaultPolicy(fixture.nodes)
	policyRaw = fmt.Appendf(policyRaw[:len(policyRaw)-2], `,{"node":"%x","capabilities":["restore_activate","delegate","topology"]}]}`, operatorNode)
	policyPath := filepath.Join(fixture.root, "policy.vibejson")
	if err = os.WriteFile(policyPath, policyRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	policy, err := serviceauthz.Load(policyRaw)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := serviceauthz.NewGate(policy)
	if err != nil {
		t.Fatal(err)
	}
	apply := prepareRF3Apply{MaxSessions: 32, RetryWindow: 8, MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20, ShardKey: "id"}
	template := struct {
		Format               uint16          `json:"format"`
		Distribution         string          `json:"distribution"`
		Shard                string          `json:"shard"`
		AllocationGeneration uint64          `json:"allocation_generation"`
		BaseTable            string          `json:"base_table"`
		DDL                  []string        `json:"ddl"`
		GlobalIndexes        []struct{}      `json:"global_indexes"`
		Apply                prepareRF3Apply `json:"apply"`
	}{1, string(gateway.ReplicatedCatalogDistribution), string(gateway.ReplicatedCatalogShard), 23,
		gateway.ReplicatedCatalogTable, []string{`CREATE TABLE controlplane (PRIMARY KEY (id))`}, nil, apply}
	schemaRaw, err := vibejson.Marshal(&template)
	if err != nil {
		t.Fatal(err)
	}
	templateRaw := append([]byte(`{"format":1,"groups":[{"ordinal":0,"schema":`), schemaRaw...)
	templateRaw = append(templateRaw, []byte(`}]}`)...)
	artifact, manifest := restoreRF3SourceArtifact(t, fixture.root)
	cut, err := clusterbackup.GroupCutFromVerifiedArtifact(1, manifest, sha256.Sum256(artifact), uint64(len(artifact)))
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := clusterbackup.Certify(sha256.Sum256([]byte("restore-process-backup")), clusterbackup.CatalogCut{
		Generation: 1, Digest: sha256.Sum256([]byte("source-catalog")), PolicyGeneration: 5,
		Groups: []raftmember.GroupKey{cut.Group}}, []clusterbackup.GroupCut{cut})
	if err != nil {
		t.Fatal(err)
	}
	limits := clusterbackup.RepositoryLimits{MaxBackups: 1, MaxArtifacts: 1, MaxArtifactBytes: 16 << 20, MaxDiskBytes: 32 << 20}
	repository, err := clusterbackup.OpenBackupRepository(filepath.Join(fixture.root, "backup"), limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	if err = repository.Publish(certificate, clusterbackup.ArtifactInput{Reader: bytes.NewReader(artifact)}); err != nil {
		t.Fatal(err)
	}
	permit, err := clusterbackup.VerifyRestoreArtifacts(t.Context(), certificate, sha256.Sum256([]byte("restore-process-operation")),
		fixture.group.ClusterID, fixture.group.ClusterIncarnation, clusterbackup.RestoreVerifyOptions{Source: repository, MaxArtifactBytes: 16 << 20, MaxTotalBytes: 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	staging, err := clusterbackup.BuildRestoreStagingRoot(t.Context(), filepath.Join(fixture.root, "verified"), limits, certificate, permit, repository)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = staging.Close() })
	target := clusterrestore.TargetGroup{Group: fixture.group}
	for member := range target.Replicas {
		target.Replicas[member] = clusterrestore.ReplicaIdentity{Member: uint64(member + 1), Node: fixture.nodes[member],
			Store: rf3CommandStoreIdentity(uint64(member + 1)).StoreID, NodeIncarnation: 1}
	}
	operation, err := clusterrestore.NewOperation(permit, certificate, 0, 5, sha256.Sum256([]byte("restore-process-grammar")),
		sha256.Sum256(policyRaw), sha256.Sum256(templateRaw), []clusterrestore.TargetGroup{target})
	if err != nil {
		t.Fatal(err)
	}
	installer := &restoreRF3Installer{root: filepath.Join(fixture.root, "target"), template: templateRaw}
	if err = os.Mkdir(installer.root, 0o700); err != nil {
		t.Fatal(err)
	}
	catalog := new(restoreRF3Catalog)
	activationRoot := filepath.Join(fixture.root, "activation")
	cutError := errors.New("stop after durable roots, before catalog")
	_, err = clusterrestore.Activate(t.Context(), clusterrestore.Options{Root: activationRoot, Staging: staging, Operation: operation,
		Installer: installer, Catalog: clusterrestore.ReplicatedCatalogPublisher{Proposer: catalog},
		Fault: func(point clusterrestore.FaultPoint) error {
			if point == clusterrestore.FaultAfterGroup {
				return cutError
			}
			return nil
		}})
	if !errors.Is(err, cutError) {
		t.Fatalf("pre-catalog cut: %v", err)
	}
	keyPath := filepath.Join(fixture.root, "key-source")
	if err = os.WriteFile(keyPath, []byte("0123456789abcdef0123456789abcdef"), 0o600); err != nil {
		t.Fatal(err)
	}
	fixture.profiles = make([]*rafttransport.PeerTLS, rf3CommandMembers)
	for member := range fixture.manifestPaths {
		root := filepath.Join(installer.root, "roots", "group-00000000", fmt.Sprintf("replica-%d", member+1))
		identity := rf3CommandStoreIdentity(uint64(member + 1))
		input := prepareRF3Manifest{Root: root, Distribution: identity.Distribution, Shard: identity.Shard,
			ClusterID: idString(fixture.group.ClusterID[:]), ClusterIncarnation: idString(fixture.group.ClusterIncarnation[:]),
			TopologyRecoveryEpoch: fixture.group.TopologyRecoveryEpoch, AllocationGeneration: 23,
			ShardIncarnation: idString(fixture.group.ShardIncarnation[:]), GroupID: idString(fixture.group.GroupID[:]),
			MemberID: uint64(member + 1), StoreID: idString(identity.StoreID[:]), Table: gateway.ReplicatedCatalogTable,
			CreateTable: `CREATE TABLE controlplane (PRIMARY KEY (id))`, Apply: apply,
			Authority:           prepareRF3Authority{5, 1, 1, 13, 1, 1},
			WAL:                 prepareRF3WAL{"rf3-command-key", keyPath, "restore-process-wrapped-key", 256 << 20, raftstore.DefaultMaxRecordBytes, 4096, 16384, raftstore.DefaultMaxLiveBytes},
			Listeners:           rf3ManifestListeners{Peer: fixture.peerAddresses[member], Native: fixture.nativeAddresses[member], Snapshot: fixture.snapshotAddresses[member], Control: fixture.controlAddresses[member]},
			TLS:                 rf3ManifestTLS{Certificate: fixture.credentials[member].Certificate, Key: fixture.credentials[member].Key, Roots: fixture.roots, IdentityOID: rf3testfixture.ProcessIdentityOID},
			AuthorizationPolicy: policyPath, SplitControl: prepareRF3SplitControl{MaxRecords: 4096, MaxFileBytes: 64 << 20, MaxChildOperations: 8, StageCheckpointBytes: 32 << 20}}
		for index, node := range fixture.nodes {
			input.Members = append(input.Members, prepareRF3Member{MemberID: uint64(index + 1), NodeID: idString(node[:]), PeerAddress: fixture.peerAddresses[index]})
			input.SplitControl.Grants = append(input.SplitControl.Grants, prepareRF3ActionGrant{NodeID: idString(node[:]), Actions: ^uint16(0)})
		}
		raw, marshalErr := vibejson.Marshal(&input)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		preparePath := filepath.Join(fixture.root, fmt.Sprintf("prepare-%d.vibejson", member+1))
		if err = os.WriteFile(preparePath, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		if code := runAdoptRestoredRF3([]string{"-manifest", preparePath}); code != 0 {
			t.Fatalf("adopt member %d exit=%d", member+1, code)
		}
		fixture.manifestPaths[member], fixture.walPaths[member] = filepath.Join(root, "serve-rf3.vibejson"), filepath.Join(root, "member.wal")
		fixture.profiles[member], err = servicetls.LoadProfile(fixture.credentials[member].Certificate, fixture.credentials[member].Key, fixture.roots, rf3testfixture.ProcessIdentityOID, time.Now)
		if err != nil {
			t.Fatal(err)
		}
	}
	return fixture, gateway.RestoreActivationOptions{Root: activationRoot, Staging: staging, Operation: operation,
		Installer: installer, Catalog: catalog, Gate: gate, Operator: serviceauthz.Authority{Node: operatorNode, Generation: 5}}, catalog
}

func restoreRF3SourceArtifact(t *testing.T, root string) ([]byte, replicatedstate.SnapshotArtifactManifest) {
	t.Helper()
	if err := os.Mkdir(filepath.Join(root, "source"), 0o700); err != nil {
		t.Fatal(err)
	}
	identity := rf3CommandStoreIdentity(1)
	identity.ClusterID[0], identity.ClusterIncarnation[0], identity.ShardIncarnation[0], identity.GroupID[0], identity.StoreID[0] = 0xe1, 0xe2, 0xe3, 0xe4, 0xe5
	key := raftstore.Key{ID: "restore-source-key", Wrapped: []byte("test")}
	copy(key.Material[:], []byte("0123456789abcdef0123456789abcdef"))
	bootstrap := rf3testfixture.InitialBootstrap([]uint64{1})
	bootstrap.TopologyRecoveryEpoch = 1
	prepared, err := rf3testfixture.PrepareMember(rf3testfixture.MemberOptions{Root: filepath.Join(root, "source"),
		Table: gateway.ReplicatedCatalogTable, CreateTable: `CREATE TABLE controlplane (PRIMARY KEY (id))`,
		Identity: identity, Key: key, WAL: raftstore.Options{MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
			MaxRecords: 4096, MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes}, Bootstrap: bootstrap,
		Authority: rf3CommandAuthority(), Apply: sqldriver.ReplicatedApplyOptions{MaxSessions: 32, RetryWindow: 8,
			TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20},
			Placement: sqldriver.ReplicatedPlacementProfile{Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: "id",
				TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
				Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}}, SeedDocuments: [][]byte{[]byte(`{"id":"restored-row"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if _, err = prepared.Apply.ApplyConfiguration(raftmodel.ApplyMeta{Index: 2, Term: 1, Type: pb.EntryConfChange}, bootstrap.Snapshot.Metadata.ConfState); err != nil {
		t.Fatal(err)
	}
	cut, err := prepared.Apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	manifest, err := replicatedstate.WriteSnapshotArtifact(&artifact, cut, replicatedstate.SnapshotArtifactOptions{})
	if err = errors.Join(err, cut.Close()); err != nil {
		t.Fatal(err)
	}
	return artifact.Bytes(), manifest
}

func restoreRF3AssertClosed(t *testing.T, fixture *rf3FaultFixture, member int) {
	t.Helper()
	response, err := fixture.roundTrip(t, member, &shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedProbe,
		Authority: serviceauthz.Authority{Node: fixture.nodes[(member+1)%3], Generation: 5}, Capability: serviceauthz.CapabilityTopology,
		Fence: shardservice.ReplicatedFence{Group: fixture.group, AllocationGeneration: 23}})
	if err != nil || response.Kind != shardservice.ReplicatedRefusal ||
		response.Refusal != shardservice.ReplicatedRefusalUnavailable || !response.HasState {
		t.Fatalf("restored member %d served without a fresh catalog grant: %+v", member+1, response)
	}
}

func restoreRF3AllocatedBytes(t *testing.T, root string) int64 {
	t.Helper()
	var total int64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("missing physical allocation metadata for %s", path)
		}
		total += stat.Blocks * 512
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return total
}
