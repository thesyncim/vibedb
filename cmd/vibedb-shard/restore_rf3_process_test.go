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
	"os/exec"
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
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
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
// adopter and serve-rf3 composition over actual restored SQL/WAL roots. Its
// activation witness is committed to, and separately ReadIndex-observed from,
// the same real target RF3 catalog. No local receipt grants serving permission.
func TestRestoredRF3ExternalProcessServingAndFailover(t *testing.T) {
	if os.Getenv(restoreRF3ProcessEnvironment) != "1" {
		t.Skip("set " + restoreRF3ProcessEnvironment + "=1 for the bounded Linux process gate")
	}

	fixtures, options, snapshot := newRestoredRF3ProcessFixture(t)
	binary := restoreRF3GatewayBinary(t, fixtures[0].root)
	manifest := restoreRF3WriteCLIManifest(t, fixtures, options, snapshot)
	if err := options.Staging.Close(); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	for _, fixture := range fixtures {
		restoreRF3RejectMissingMarker(t, fixture)
		fixture.startAll(t)
		for member := range fixture.children {
			restoreRF3AssertClosed(t, fixture, member)
		}
	}
	witness := restoreRF3RunActivation(t, binary, manifest, options.Operation)
	catalog, data := fixtures[0], fixtures[1]
	catalogLeader, catalogStates := catalog.waitLeader(t, []int{0, 1, 2}, 15*time.Second)
	restoreRF3ReadRelation(t, catalog, catalogLeader, catalogStates[catalogLeader], 1, rf3FaultKey(t, "source-catalog-sentinel"), nil, false)
	policyRaw, err := os.ReadFile(filepath.Join(catalog.root, "policy.vibejson"))
	if err != nil {
		t.Fatal(err)
	}
	projection, err := gateway.RestoreCatalogProjection(options.Operation, snapshot, policyRaw)
	if err != nil || len(projection) != 4 {
		t.Fatalf("sealed fresh catalog projection: rows=%d err=%v", len(projection), err)
	}
	for _, row := range projection {
		restoreRF3ReadRelation(t, catalog, catalogLeader, catalogStates[catalogLeader], 1, row.Key, row.Value, true)
	}
	leader, states := data.waitLeader(t, []int{0, 1, 2}, 15*time.Second)
	data.waitDocument(t, leader, states[leader], "restored-row", 5*time.Second)
	restoreRF3ReadRelation(t, data, leader, states[leader], 2, []byte{0x91, 0x01, 'r'}, []byte("[\"restored-row\"]"), true)
	command := data.command(states[leader], 0, 1, sha256.Sum256([]byte("restored-data-session")), nil)
	command.Distribution, command.Shard = "items", "items-0"
	command.Kind = replication.CommandSessionOpen
	command.NextDeadlineUnixNano = 2_000_000_000_000_000_000
	command.Batches = nil
	opened := restoreRF3ProposeCommand(t, data, leader, states[leader], command)
	completion, err := replication.OpenCompletion(opened.Completion)
	if err != nil || completion.ResultCode != replicatedstate.ResultSessionOpened {
		t.Fatalf("restored data session: %+v %v", completion, err)
	}
	states[leader] = data.probe(t, leader)
	command = data.command(states[leader], completion.ClientEpoch, 2, sha256.Sum256([]byte("acknowledged-after-restore")), []replication.Mutation{{Kind: replication.MutationPut, Key: rf3FaultKey(t, "acknowledged-after-restore"), Value: []byte("{\"id\":\"acknowledged-after-restore\"}")}})
	command.Distribution, command.Shard = "items", "items-0"
	writeStarted := time.Now()
	settled := restoreRF3ProposeCommand(t, data, leader, states[leader], command)
	writeLatency := time.Since(writeStarted)
	completion, err = replication.OpenCompletion(settled.Completion)
	if err != nil || completion.ResultCode != replicatedstate.ResultApplied {
		t.Fatalf("restored write: %+v %v", completion, err)
	}
	data.waitAllApplied(t, settled.Outcome.AppliedIndex, 10*time.Second)
	rssBaseline, walBaseline := restoreRF3Resources(t, fixtures)
	storageBaseline := restoreRF3AllocatedBytes(t, options.Installer.(*restoreRF3Installer).root)
	failoverStarted := time.Now()
	data.kill(t, leader)
	newLeader, liveStates := data.waitLeader(t, rf3FaultOtherMembers(leader), 15*time.Second)
	data.waitDocument(t, newLeader, liveStates[newLeader], "restored-row", 5*time.Second)
	data.waitDocument(t, newLeader, liveStates[newLeader], "acknowledged-after-restore", 5*time.Second)
	restoreRF3ReadRelation(t, data, newLeader, liveStates[newLeader], 2, []byte{0x91, 0x01, 'r'}, []byte("[\"restored-row\"]"), true)
	failoverLatency := time.Since(failoverStarted)
	data.restart(t, leader)
	restoreRF3AssertClosed(t, data, leader)
	// A new gateway process must independently observe the quorum catalog before
	// reinstalling any process-local grant, including a restarted incarnation.
	if replayed := restoreRF3RunActivation(t, binary, manifest, options.Operation); replayed != witness {
		t.Fatal("CLI retry changed certified catalog witness")
	}
	data.waitCaughtUp(t, leader, settled.Outcome.AppliedIndex, 10*time.Second)
	newLeader, liveStates = data.waitLeader(t, []int{0, 1, 2}, 15*time.Second)
	data.waitDocument(t, newLeader, liveStates[newLeader], "acknowledged-after-restore", 5*time.Second)
	restoreRF3ReadRelation(t, data, newLeader, liveStates[newLeader], 2, []byte{0x91, 0x01, 'r'}, []byte("[\"restored-row\"]"), true)
	rssFinal, walFinal := restoreRF3Resources(t, fixtures)
	storageFinal := restoreRF3AllocatedBytes(t, options.Installer.(*restoreRF3Installer).root)
	if time.Since(started) > 120*time.Second || writeLatency > 5*time.Second || failoverLatency > 20*time.Second {
		t.Fatalf("latency bound: total=%s write=%s failover=%s", time.Since(started), writeLatency, failoverLatency)
	}
	if rssFinal > 1536<<20 || rssFinal > rssBaseline+256<<20 || walFinal > 1536<<20 || walFinal > walBaseline+64<<20 || storageFinal > 2<<30 || storageFinal > storageBaseline+128<<20 {
		t.Fatalf("space bound: rss=%d/%d wal=%d/%d storage=%d/%d", rssBaseline, rssFinal, walBaseline, walFinal, storageBaseline, storageFinal)
	}
	t.Logf("restored external RF3: shard_processes=6 certified_groups=2 certified_artifact=true fresh_roots=true marker_removal_refused=true closed_before_catalog=true activation_cli=true source_catalog_discarded=true fresh_catalog=true catalog_witness=replicated_readindex catalog_observations=2 restored_read=true restored_global_index=true acknowledged_write_survived=true leader_sigkill=true restart_closed=true regrant=true total=%s write=%s failover=%s rss_bytes=%d rss_growth=%d storage_growth=%d wal_growth=%d", time.Since(started), writeLatency, failoverLatency, rssFinal, max(int64(rssFinal)-int64(rssBaseline), 0), max(storageFinal-storageBaseline, 0), max(walFinal-walBaseline, 0))
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

type restoreRF3Catalog struct{}

func (*restoreRF3Catalog) ProposeRestoreActivation(context.Context, []byte) ([]byte, error) {
	return nil, clusterrestore.ErrActivation
}

const restoreRF3PreflightEnvironment = "VIBEDB_RESTORE_RF3_PREFLIGHT"

func restoreRF3RejectMissingMarker(t *testing.T, fixture *rf3FaultFixture) {
	t.Helper()
	marker := filepath.Join(filepath.Dir(fixture.walPaths[0]), "restore_preparing")
	held := marker + ".held"
	if err := os.Rename(marker, held); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Rename(held, marker); err != nil {
			t.Errorf("restore marker after negative preflight: %v", err)
		}
	}()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestRestoredRF3MissingMarkerProcessHelper$", "-test.v")
	command.Env = append(os.Environ(), restoreRF3PreflightEnvironment+"="+fixture.manifestPaths[0])
	diagnostic := &rf3CommandDiagnostic{maximum: rf3CommandDiagnosticBytes}
	command.Stdout, command.Stderr = diagnostic, diagnostic
	if err = command.Run(); err != nil {
		t.Fatalf("missing-marker external preflight: %v\n%s", err, diagnostic.String())
	}
}

func TestRestoredRF3MissingMarkerProcessHelper(t *testing.T) {
	path := os.Getenv(restoreRF3PreflightEnvironment)
	if path == "" {
		return
	}
	manifest, err := loadRF3Manifest(path)
	if err != nil {
		t.Fatal(err)
	}
	listened := false
	err = servePreparedRF3WithListen(t.Context(), manifest, func(string, string) (net.Listener, error) {
		listened = true
		return nil, errors.New("missing-marker process reached listener publication")
	})
	if listened || !errors.Is(err, errRF3Serving) || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing immutable restore marker was not rejected before listening: listened=%t err=%v", listened, err)
	}
}

type restoreRF3Schema struct {
	Format               uint16                  `json:"format"`
	Distribution         string                  `json:"distribution"`
	Shard                string                  `json:"shard"`
	AllocationGeneration uint64                  `json:"allocation_generation"`
	BaseTable            string                  `json:"base_table"`
	DDL                  []string                `json:"ddl"`
	GlobalIndexes        []restoreRF3GlobalIndex `json:"global_indexes"`
	Apply                prepareRF3Apply         `json:"apply"`
}
type restoreRF3GlobalIndex struct {
	Relation      uint16 `json:"relation"`
	Table         string `json:"table"`
	IndexID       uint64 `json:"index_id"`
	Incarnation   uint64 `json:"incarnation"`
	LocatorCount  uint8  `json:"locator_count"`
	Unique        bool   `json:"unique"`
	KeyEncoding   uint8  `json:"key_encoding"`
	KeyArity      uint8  `json:"key_arity"`
	TupleVersion  uint32 `json:"tuple_version"`
	MapperVersion uint32 `json:"mapper_version"`
	BucketBits    uint8  `json:"bucket_bits"`
}

func newRestoredRF3ProcessFixture(t *testing.T) ([2]*rf3FaultFixture, gateway.RestoreActivationOptions, *gateway.Snapshot) {
	t.Helper()
	root := t.TempDir()
	fixtures := [2]*rf3FaultFixture{}
	for ordinal := range fixtures {
		fixture := &rf3FaultFixture{root: root, group: rf3CommandGroup(), nodes: rf3CommandNodes(),
			authority: sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: 5, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 13, RoutingVersion: 1, RouteGeneration: 1}}
		if ordinal == 1 {
			fixture.group.GroupID[0]++
			fixture.group.ShardIncarnation[0]++
			for i := range fixture.nodes {
				fixture.nodes[i][0] += 64
			}
		}
		fixtures[ordinal] = fixture
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
	}
	operatorNode := rafttransport.NodeID{0xee, 0x42}
	allNodes := append(append(append([]rafttransport.NodeID(nil), fixtures[0].nodes[:]...), fixtures[1].nodes[:]...), operatorNode)
	credentials, roots, err := rf3testfixture.WriteCredentials(root, rf3CommandIdentityOID, rafttransport.TrustDomain{
		ClusterID: fixtures[0].group.ClusterID, ClusterIncarnation: fixtures[0].group.ClusterIncarnation}, allNodes)
	if err != nil {
		t.Fatal(err)
	}
	for ordinal, fixture := range fixtures {
		fixture.roots = roots
		fixture.credentials = append(append([]rf3testfixture.Credential(nil), credentials[ordinal*3:ordinal*3+3]...), credentials[6])
	}
	policyRaw := rf3FaultPolicy(fixtures[0].nodes)
	extra := rf3FaultPolicy(fixtures[1].nodes)
	prefix := bytes.Index(extra, []byte("["))
	policyRaw = append(append(policyRaw[:len(policyRaw)-2], ','), extra[prefix+1:len(extra)-2]...)
	policyRaw = fmt.Appendf(policyRaw, `,{"node":"%x","capabilities":["delegate","topology","restore_activate"]}]}`, operatorNode)
	policyPath := filepath.Join(root, "policy.vibejson")
	if err = os.WriteFile(policyPath, policyRaw, 0600); err != nil {
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
	schemas := [2]restoreRF3Schema{
		{1, string(gateway.ReplicatedCatalogDistribution), string(gateway.ReplicatedCatalogShard), 23, gateway.ReplicatedCatalogTable, []string{"CREATE TABLE controlplane (PRIMARY KEY (id))"}, nil, apply},
		{1, "items", "items-0", 23, "items", []string{"CREATE TABLE items (PRIMARY KEY (id))", "CREATE INDEX by_email ON items (email)", "CREATE TABLE email_claims (PRIMARY KEY (key))"},
			[]restoreRF3GlobalIndex{{2, "email_claims", 41, 7, 1, true, uint8(sqldriver.ReplicatedRelationKeyCanonicalTuple), 1, uint32(distribution.CurrentTupleVersion), uint32(distribution.NativeMapperVersion), distribution.DefaultVirtualBucketBits}}, apply},
	}
	// The canonical RF3 catalog requires an explicit request-home directory.
	// Provision its fresh full-range ledger even though this gate exercises
	// restore activation rather than the request coordinator.
	ledgerIdentity := sha256.Sum256(append([]byte("restored/request-ledger/"), fixtures[0].group.GroupID[:]...))
	schemas[0].Apply.RequestLedgerCapacityBytes = 64 << 20
	schemas[0].Apply.RequestLedgerCleanupReserveBytes = 8 << 20
	schemas[0].Apply.RequestLedgerRangeStart = fmt.Sprintf("%064x", 0)
	schemas[0].Apply.RequestLedgerRangeEnd = fmt.Sprintf("%064x", 0)
	schemas[0].Apply.RequestLedgerRangeIdentity = fmt.Sprintf("%x", ledgerIdentity)
	artifacts := make([][]byte, 2)
	manifests := make([]replicatedstate.SnapshotArtifactManifest, 2)
	cuts := make([]clusterbackup.GroupCut, 2)
	fences := make([]raftservice.CommandFence, 2)
	for ordinal := range schemas {
		artifacts[ordinal], manifests[ordinal], fences[ordinal] = restoreRF3SourceArtifact(t, root, ordinal, schemas[ordinal])
		cuts[ordinal], err = clusterbackup.GroupCutFromVerifiedArtifact(1, manifests[ordinal], sha256.Sum256(artifacts[ordinal]), uint64(len(artifacts[ordinal])))
		if err != nil {
			t.Fatal(err)
		}
	}
	certificate, err := clusterbackup.Certify(sha256.Sum256([]byte("restore-process-backup")), clusterbackup.CatalogCut{Generation: 1, Digest: sha256.Sum256([]byte("source-catalog")), PolicyGeneration: 5, Groups: []raftmember.GroupKey{cuts[0].Group, cuts[1].Group}}, cuts)
	if err != nil {
		t.Fatal(err)
	}
	limits := clusterbackup.RepositoryLimits{MaxBackups: 1, MaxArtifacts: 2, MaxArtifactBytes: 16 << 20, MaxDiskBytes: 64 << 20}
	repository, err := clusterbackup.OpenBackupRepository(filepath.Join(root, "backup"), limits)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	inputs := []clusterbackup.ArtifactInput{{Reader: bytes.NewReader(artifacts[0])}, {Reader: bytes.NewReader(artifacts[1])}}
	if err = repository.Publish(certificate, inputs...); err != nil {
		t.Fatal(err)
	}
	permit, err := clusterbackup.VerifyRestoreArtifacts(t.Context(), certificate, sha256.Sum256([]byte("restore-process-operation")), fixtures[0].group.ClusterID, fixtures[0].group.ClusterIncarnation, clusterbackup.RestoreVerifyOptions{Source: repository, MaxArtifactBytes: 16 << 20, MaxTotalBytes: 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	staging, err := clusterbackup.BuildRestoreStagingRoot(t.Context(), filepath.Join(root, "verified"), limits, certificate, permit, repository)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = staging.Close() })
	targets := make([]clusterrestore.TargetGroup, 2)
	for ordinal, fixture := range fixtures {
		targets[ordinal].Group = fixture.group
		for member := range targets[ordinal].Replicas {
			identity := restoreRF3TargetIdentity(fixture, ordinal, member, schemas[ordinal])
			targets[ordinal].Replicas[member] = clusterrestore.ReplicaIdentity{Member: uint64(member + 1), Node: fixture.nodes[member], Store: identity.StoreID, NodeIncarnation: 1}
		}
	}
	snapshot := restoreRF3TargetSnapshot(t, fixtures, targets, schemas, fences)
	catalogRaw, err := gateway.AppendSnapshotDocument(nil, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	type schemaSlot struct {
		Ordinal uint32           `json:"ordinal"`
		Schema  restoreRF3Schema `json:"schema"`
	}
	schemaSet := struct {
		Format  uint16       `json:"format"`
		Groups  []schemaSlot `json:"groups"`
		Catalog []byte       `json:"catalog"`
		Policy  []byte       `json:"policy"`
	}{1, []schemaSlot{{0, schemas[0]}, {1, schemas[1]}}, catalogRaw, policyRaw}
	templateRaw, err := vibejson.Marshal(&schemaSet)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := clusterrestore.NewOperation(permit, certificate, 0, 5, sha256.Sum256([]byte("restore-process-grammar")), sha256.Sum256(policyRaw), sha256.Sum256(templateRaw), targets)
	if err != nil {
		t.Fatal(err)
	}
	installer := &restoreRF3Installer{root: filepath.Join(root, "target"), template: templateRaw}
	if err = os.Mkdir(installer.root, 0700); err != nil {
		t.Fatal(err)
	}
	activationRoot := filepath.Join(root, "activation")
	catalog := new(restoreRF3Catalog)
	cutError := errors.New("stop after all durable roots, before catalog")
	completed := 0
	_, err = clusterrestore.Activate(t.Context(), clusterrestore.Options{Root: activationRoot, Staging: staging, Operation: operation, Installer: installer, Catalog: clusterrestore.ReplicatedCatalogPublisher{Proposer: catalog}, Fault: func(point clusterrestore.FaultPoint) error {
		if point == clusterrestore.FaultAfterGroup {
			completed++
			if completed == len(fixtures) {
				return cutError
			}
		}
		return nil
	}})
	if !errors.Is(err, cutError) || completed != 2 {
		t.Fatalf("pre-catalog complete-vector cut: completed=%d err=%v", completed, err)
	}
	keyPath := filepath.Join(root, "key-source")
	if err = os.WriteFile(keyPath, []byte("0123456789abcdef0123456789abcdef"), 0600); err != nil {
		t.Fatal(err)
	}
	for ordinal, fixture := range fixtures {
		fixture.profiles = make([]*rafttransport.PeerTLS, 3)
		for member := range fixture.manifestPaths {
			memberRoot := filepath.Join(installer.root, "roots", fmt.Sprintf("group-%08d", ordinal), fmt.Sprintf("replica-%d", member+1))
			identity := restoreRF3TargetIdentity(fixture, ordinal, member, schemas[ordinal])
			input := prepareRF3Manifest{Root: memberRoot, Distribution: identity.Distribution, Shard: identity.Shard, ClusterID: idString(fixture.group.ClusterID[:]), ClusterIncarnation: idString(fixture.group.ClusterIncarnation[:]), TopologyRecoveryEpoch: fixture.group.TopologyRecoveryEpoch, AllocationGeneration: 23, ShardIncarnation: idString(fixture.group.ShardIncarnation[:]), GroupID: idString(fixture.group.GroupID[:]), MemberID: uint64(member + 1), StoreID: idString(identity.StoreID[:]), Table: schemas[ordinal].BaseTable, CreateTable: schemas[ordinal].DDL[0], Apply: schemas[ordinal].Apply, Authority: prepareRF3Authority{5, 1, 1, 13, 1, 1},
				WAL:                 prepareRF3WAL{"rf3-command-key", keyPath, "restore-process-wrapped-key", 256 << 20, raftstore.DefaultMaxRecordBytes, 4096, 16384, raftstore.DefaultMaxLiveBytes},
				Listeners:           rf3ManifestListeners{Peer: fixture.peerAddresses[member], Native: fixture.nativeAddresses[member], Snapshot: fixture.snapshotAddresses[member], Control: fixture.controlAddresses[member]},
				TLS:                 rf3ManifestTLS{Certificate: fixture.credentials[member].Certificate, Key: fixture.credentials[member].Key, Roots: roots, IdentityOID: rf3testfixture.ProcessIdentityOID},
				AuthorizationPolicy: policyPath, SplitControl: prepareRF3SplitControl{MaxRecords: 4096, MaxFileBytes: 64 << 20, MaxChildOperations: 8, StageCheckpointBytes: 32 << 20}}
			for index, node := range fixture.nodes {
				input.Members = append(input.Members, prepareRF3Member{MemberID: uint64(index + 1), NodeID: idString(node[:]), PeerAddress: fixture.peerAddresses[index]})
				input.SplitControl.Grants = append(input.SplitControl.Grants, prepareRF3ActionGrant{NodeID: idString(node[:]), Actions: ^uint16(0)})
			}
			raw, err := vibejson.Marshal(&input)
			if err != nil {
				t.Fatal(err)
			}
			preparePath := filepath.Join(root, fmt.Sprintf("prepare-%d-%d.vibejson", ordinal, member))
			if err = os.WriteFile(preparePath, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if code := runAdoptRestoredRF3([]string{"-manifest", preparePath}); code != 0 {
				t.Fatalf("adopt group %d member %d exit=%d", ordinal, member, code)
			}
			fixture.manifestPaths[member], fixture.walPaths[member] = filepath.Join(memberRoot, "serve-rf3.vibejson"), filepath.Join(memberRoot, "member.wal")
			fixture.profiles[member], err = servicetls.LoadProfile(fixture.credentials[member].Certificate, fixture.credentials[member].Key, roots, rf3testfixture.ProcessIdentityOID, time.Now)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	return fixtures, gateway.RestoreActivationOptions{Root: activationRoot, Staging: staging, Operation: operation, Installer: installer, Catalog: catalog, Gate: gate, Operator: serviceauthz.Authority{Node: operatorNode, Generation: 5}}, snapshot
}
func restoreRF3TargetIdentity(fixture *rf3FaultFixture, ordinal, member int, schema restoreRF3Schema) raftstore.Identity {
	identity := rf3CommandStoreIdentity(uint64(member + 1))
	identity.Distribution, identity.Shard = schema.Distribution, schema.Shard
	identity.GroupID, identity.ShardIncarnation = fixture.group.GroupID, fixture.group.ShardIncarnation
	identity.StoreID[0] += byte(ordinal * 16)
	return identity
}
func restoreRF3SourceArtifact(t *testing.T, root string, ordinal int, schema restoreRF3Schema) ([]byte, replicatedstate.SnapshotArtifactManifest, raftservice.CommandFence) {
	t.Helper()
	if err := os.Mkdir(filepath.Join(root, fmt.Sprintf("source-%d", ordinal)), 0o700); err != nil {
		t.Fatal(err)
	}
	identity := rf3CommandStoreIdentity(1)
	identity.ClusterID[0], identity.ClusterIncarnation[0], identity.ShardIncarnation[0], identity.GroupID[0], identity.StoreID[0] = 0xe1, 0xe2, 0xe3, 0xe4, 0xe5
	identity.GroupID[0] += byte(ordinal)
	identity.ShardIncarnation[0] += byte(ordinal)
	identity.Distribution, identity.Shard = schema.Distribution, schema.Shard
	key := raftstore.Key{ID: "restore-source-key", Wrapped: []byte("test")}
	copy(key.Material[:], []byte("0123456789abcdef0123456789abcdef"))
	bootstrap := rf3testfixture.InitialBootstrap([]uint64{1})
	bootstrap.TopologyRecoveryEpoch = 1
	prepared, err := rf3testfixture.PrepareMember(rf3testfixture.MemberOptions{Root: filepath.Join(root, fmt.Sprintf("source-%d", ordinal)),
		Table: schema.BaseTable, CreateTable: schema.DDL[0], SchemaStatements: schema.DDL[1:], GlobalIndexes: restoreRF3GlobalRelations(schema),
		Identity: identity, Key: key, WAL: raftstore.Options{MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
			MaxRecords: 4096, MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes}, Bootstrap: bootstrap,
		Authority: rf3CommandAuthority(), Apply: sqldriver.ReplicatedApplyOptions{MaxSessions: 32, RetryWindow: 8,
			TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20},
			Placement: sqldriver.ReplicatedPlacementProfile{Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: "id",
				TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
				Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}}, SeedDocuments: [][]byte{[]byte(`{"id":"source-catalog-sentinel"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if _, err = prepared.Apply.ApplyConfiguration(raftmodel.ApplyMeta{Index: 2, Term: 1, Type: pb.EntryConfChange}, bootstrap.Snapshot.Metadata.ConfState); err != nil {
		t.Fatal(err)
	}

	profile, err := prepared.Apply.CapacityQualificationProfile()
	if err != nil {
		t.Fatal(err)
	}
	// The source configuration entry at index two advances its membership
	// version even though its voter set is unchanged.
	fence := raftservice.CommandFence{ReplicaSetVersion: 2, ActivePolicyGeneration: 5, ProtectionEpoch: 7, OwnershipEpoch: 11, SchemaGeneration: 13, RelationManifestDigest: profile.RelationManifestDigest, RoutingVersion: 17, RouteGeneration: 19}
	if ordinal == 1 {
		group := raftmember.GroupKey{ClusterID: identity.ClusterID, ClusterIncarnation: identity.ClusterIncarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: identity.ShardIncarnation, GroupID: identity.GroupID}
		helper := rf3FaultFixture{group: group}
		state := shardservice.ReplicatedMemberState{Fence: shardservice.ReplicatedFence{AllocationGeneration: 23, Command: fence}}
		cmd := helper.command(state, 0, 1, sha256.Sum256([]byte("restore-source-open")), nil)
		cmd.Distribution, cmd.Shard = schema.Distribution, schema.Shard
		cmd.Kind = replication.CommandSessionOpen
		cmd.NextDeadlineUnixNano = 2_000_000_000_000_000_000
		cmd.Batches = nil
		encoded, err := replication.AppendCommand(nil, cmd)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = prepared.Apply.ApplyNormal(raftmodel.ApplyMeta{Index: 3, Term: 1, Type: pb.EntryNormal}, encoded); err != nil {
			t.Fatal(err)
		}
		lookup, err := prepared.Apply.LookupCompletion(encoded)
		if err != nil {
			t.Fatal(err)
		}
		completion, err := replication.OpenCompletion(lookup.Bytes)
		if err != nil || completion.ResultCode != replicatedstate.ResultSessionOpened {
			t.Fatalf("source session: %+v %v", completion, err)
		}
		cmd = helper.command(state, completion.ClientEpoch, 2, sha256.Sum256([]byte("restore-source-bundle")), nil)
		cmd.Distribution, cmd.Shard = schema.Distribution, schema.Shard
		cmd.Batches = []replication.RelationMutationBatch{
			{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: rf3FaultKey(t, "restored-row"), Value: []byte("{\"id\":\"restored-row\"}")}}},
			{Relation: 2, Mutations: []replication.Mutation{{Kind: replication.MutationPutAbsentOrEqual, Key: []byte{0x91, 0x01, 'r'}, Value: []byte("[\"restored-row\"]")}}}}
		encoded, err = replication.AppendCommand(nil, cmd)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = prepared.Apply.ApplyNormal(raftmodel.ApplyMeta{Index: 4, Term: 1, Type: pb.EntryNormal}, encoded); err != nil {
			t.Fatal(err)
		}
		lookup, err = prepared.Apply.LookupCompletion(encoded)
		if err != nil {
			t.Fatal(err)
		}
		completion, err = replication.OpenCompletion(lookup.Bytes)
		if err != nil || completion.ResultCode != replicatedstate.ResultApplied {
			t.Fatalf("source bundle: %+v %v", completion, err)
		}
	}
	fence.ProtectionEpoch, fence.OwnershipEpoch, fence.RoutingVersion, fence.RouteGeneration = 1, 1, 1, 1
	fence.ReplicaSetVersion = 1
	cut, err := prepared.Apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	manifest, err := replicatedstate.WriteSnapshotArtifact(&artifact, cut, replicatedstate.SnapshotArtifactOptions{})
	if err = errors.Join(err, cut.Close()); err != nil {
		t.Fatal(err)
	}
	return artifact.Bytes(), manifest, fence
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

func restoreRF3GlobalRelations(schema restoreRF3Schema) []sqldriver.ReplicatedGlobalIndexRelation {
	result := make([]sqldriver.ReplicatedGlobalIndexRelation, len(schema.GlobalIndexes))
	for i, r := range schema.GlobalIndexes {
		result[i] = sqldriver.ReplicatedGlobalIndexRelation{Relation: r.Relation, Table: r.Table, IndexID: r.IndexID, Incarnation: r.Incarnation, LocatorCount: r.LocatorCount, Unique: r.Unique, KeyEncoding: sqldriver.ReplicatedRelationKeyEncoding(r.KeyEncoding), KeyArity: r.KeyArity, TupleVersion: distribution.TupleVersion(r.TupleVersion), MapperVersion: distribution.MapperVersion(r.MapperVersion), BucketBits: r.BucketBits}
	}
	return result
}
func restoreRF3TargetSnapshot(t *testing.T, fixtures [2]*rf3FaultFixture, targets []clusterrestore.TargetGroup, schemas [2]restoreRF3Schema, fences []raftservice.CommandFence) *gateway.Snapshot {
	t.Helper()
	config := distribution.ClusterConfig{}
	endpoints := make(map[distribution.EndpointID]string)
	descriptors := make([]gateway.ReplicatedShardDescriptor, 0, 2)
	profiles := make([]gateway.ReplicatedTableProfile, 0, 2)
	for ordinal, fixture := range fixtures {
		schema := schemas[ordinal]
		distributionName, shard := distribution.DistributionName(schema.Distribution), distribution.ShardID(schema.Shard)
		leaders := make([]distribution.EndpointID, 3)
		replicas := make([]gateway.ReplicatedReplicaDescriptor, 3)
		for member := range fixture.nodes {
			endpoint, native, control := distribution.EndpointID(fmt.Sprintf("g%d-m%d", ordinal, member)), distribution.EndpointID(fmt.Sprintf("g%d-m%d-native", ordinal, member)), distribution.EndpointID(fmt.Sprintf("g%d-m%d-control", ordinal, member))
			endpoints[endpoint], endpoints[native], endpoints[control] = fixture.peerAddresses[member], fixture.nativeAddresses[member], fixture.controlAddresses[member]
			leaders[member] = endpoint
			target := targets[ordinal].Replicas[member]
			replicas[member] = gateway.ReplicatedReplicaDescriptor{Member: target.Member, Node: target.Node, StoreID: target.Store, NodeIncarnation: target.NodeIncarnation, Endpoint: endpoint, NativeEndpoint: native, ControlEndpoint: control}
		}
		manifest, err := distribution.NewManifest(distributionName, 1, []distribution.Shard{{ID: shard, AllocationGeneration: 23, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}, Leaders: leaders, Epoch: 1}})
		if err != nil {
			t.Fatal(err)
		}
		config.Distributions = append(config.Distributions, distribution.DistributionSpec{Name: distributionName, Arity: 1, MapperVersion: distribution.NativeMapperVersion})
		config.Placements = append(config.Placements, distribution.TablePlacement{Table: schema.BaseTable, Distribution: distributionName, Columns: []string{"/id"}})
		config.Manifests = append(config.Manifests, manifest)
		descriptors = append(descriptors, gateway.ReplicatedShardDescriptor{Distribution: distributionName, Shard: shard, Group: fixture.group, AllocationGeneration: 23, Command: fences[ordinal],
			RangeIdentity: sha256.Sum256(append([]byte("restored/full-keyspace/"), fixture.group.GroupID[:]...)), LineageDigest: sha256.Sum256(append([]byte("restored/fresh-lineage/"), fixture.group.GroupID[:]...)), ForwardingRuleDigest: sha256.Sum256(append([]byte("restored/no-forwarding/"), fixture.group.GroupID[:]...)), Replicas: replicas})
		if ordinal == 0 {
			descriptors[ordinal].RequestLedgerRanges = []gateway.DurableRequestLedgerRangeDescriptor{{Identity: sha256.Sum256(append([]byte("restored/request-ledger/"), fixture.group.GroupID[:]...))}}
		}
		profiles = append(profiles, gateway.ReplicatedTableProfile{Table: schema.BaseTable, Relation: 1, PrimaryKey: "/id", SchemaGeneration: 13, RelationManifestDigest: fences[ordinal].RelationManifestDigest, MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20})
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedTableMetadata(config, endpoints, 1, nil, nil, descriptors, profiles)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

type restoreRF3CLIManifest struct {
	Format           uint16                  `json:"format"`
	Operation        string                  `json:"operation"`
	SchemaSet        string                  `json:"schema_set"`
	StagingRoot      string                  `json:"staging_root"`
	ActivationRoot   string                  `json:"activation_root"`
	TargetCatalog    string                  `json:"target_catalog"`
	Policy           string                  `json:"policy"`
	TLS              restoreRF3CLITLS        `json:"tls"`
	Sessions         [2]restoreRF3CLISession `json:"sessions"`
	Groups           []restoreRF3CLIGroup    `json:"groups"`
	Repository       restoreRF3CLIRepository `json:"repository"`
	TimeoutMS        uint64                  `json:"timeout_ms"`
	AttemptTimeoutMS uint64                  `json:"attempt_timeout_ms"`
	SessionLeaseMS   uint64                  `json:"session_lease_ms"`
	Attempts         int                     `json:"attempts"`
	MaxConnections   int                     `json:"max_connections"`
}

type restoreRF3CLITLS struct {
	Certificate string `json:"certificate"`
	Key         string `json:"key"`
	Roots       string `json:"roots"`
	IdentityOID string `json:"identity_oid"`
}

type restoreRF3CLISession struct {
	ClientID  string `json:"client_id"`
	RetryHome string `json:"retry_home"`
	Journal   string `json:"journal"`
}

type restoreRF3CLIGroup struct {
	Ordinal          uint32    `json:"ordinal"`
	Root             string    `json:"root"`
	ControlAddresses [3]string `json:"control_addresses"`
}

type restoreRF3CLIRepository struct {
	MaxArtifacts     int    `json:"max_artifacts"`
	MaxArtifactBytes uint64 `json:"max_artifact_bytes"`
	MaxDiskBytes     uint64 `json:"max_disk_bytes"`
}

func (*restoreRF3Catalog) ObserveRestoreActivation(context.Context, [32]byte) (clusterrestore.CatalogWitness, error) {
	return clusterrestore.CatalogWitness{}, clusterrestore.ErrActivation
}
func restoreRF3GatewayBinary(t *testing.T, root string) string {
	t.Helper()
	if binary := os.Getenv("VIBEDB_RESTORE_GATEWAY_BINARY"); binary != "" {
		if !filepath.IsAbs(binary) {
			t.Fatal("gateway binary must be absolute")
		}
		return binary
	}
	binary := filepath.Join(root, "vibedb-gateway")
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/vibedb-gateway")
	command.Dir = filepath.Join("..", "..")
	diagnostic := &rf3CommandDiagnostic{maximum: rf3CommandDiagnosticBytes}
	command.Stdout, command.Stderr = diagnostic, diagnostic
	if err := command.Run(); err != nil {
		t.Fatalf("build shipped gateway: %v\n%s", err, diagnostic.String())
	}
	return binary
}
func restoreRF3WriteCLIManifest(t *testing.T, fixtures [2]*rf3FaultFixture, options gateway.RestoreActivationOptions, snapshot *gateway.Snapshot) string {
	t.Helper()
	root := fixtures[0].root
	installer := options.Installer.(*restoreRF3Installer)
	manifest := restoreRF3CLIManifest{
		Format: 1, Operation: filepath.Join(root, "operation.bin"), SchemaSet: filepath.Join(root, "schema-set.vibejson"),
		StagingRoot: filepath.Join(root, "verified"), ActivationRoot: options.Root, TargetCatalog: filepath.Join(root, "target-catalog.vibejson"), Policy: filepath.Join(root, "policy.vibejson"),
		TLS:        restoreRF3CLITLS{fixtures[0].credentials[3].Certificate, fixtures[0].credentials[3].Key, fixtures[0].roots, rf3testfixture.ProcessIdentityOID},
		Repository: restoreRF3CLIRepository{2, 16 << 20, 64 << 20}, TimeoutMS: 30000, AttemptTimeoutMS: 2000, SessionLeaseMS: 60000, Attempts: 8, MaxConnections: 12}
	for i := range manifest.Sessions {
		manifest.Sessions[i] = restoreRF3CLISession{fmt.Sprintf("%032x", i+0xc1), fmt.Sprintf("%016x", i+0xd1), filepath.Join(root, fmt.Sprintf("cli-session-%d.journal", i))}
	}
	for ordinal, fixture := range fixtures {
		manifest.Groups = append(manifest.Groups, restoreRF3CLIGroup{uint32(ordinal), installer.root, fixture.controlAddresses})
	}
	opRaw, err := clusterrestore.AppendOperation(nil, options.Operation)
	if err != nil {
		t.Fatal(err)
	}
	for path, raw := range map[string][]byte{manifest.Operation: opRaw, manifest.SchemaSet: installer.template} {
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err = gateway.SaveSnapshot(manifest.TargetCatalog, snapshot); err != nil {
		t.Fatal(err)
	}
	raw, err := vibejson.Marshal(&manifest)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "restore-activate.vibejson")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func restoreRF3RunActivation(t *testing.T, binary, manifest string, operation clusterrestore.Operation) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "restore-activate", "-manifest", manifest)
	stdout, stderr := &rf3CommandDiagnostic{maximum: 4096}, &rf3CommandDiagnostic{maximum: rf3CommandDiagnosticBytes}
	command.Stdout, command.Stderr = stdout, stderr
	if err := command.Run(); err != nil {
		t.Fatalf("shipped restore-activate: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
	}
	var result struct {
		Operation      string `json:"operation"`
		Groups         uint32 `json:"groups"`
		CatalogWitness string `json:"catalog_witness"`
	}
	raw := []byte(stdout.String())
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		t.Fatalf("CLI output not newline terminated: %q", raw)
	}
	raw = raw[:len(raw)-1]
	if err := vibejson.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	canonical, err := vibejson.Marshal(&result)
	if err != nil || !bytes.Equal(raw, canonical) || result.Operation != fmt.Sprintf("%x", operation.Digest) || result.Groups != 2 || len(result.CatalogWitness) != 64 {
		t.Fatalf("invalid CLI activation receipt: %q %v", raw, err)
	}
	return result.CatalogWitness
}
func restoreRF3ProposeCommand(t *testing.T, fixture *rf3FaultFixture, leader int, state shardservice.ReplicatedMemberState, command replication.Command) *shardservice.ReplicatedResponse {
	t.Helper()
	raw, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	response := fixture.propose(t, leader, state, raw)
	if response.Kind != shardservice.ReplicatedCompletion || response.Outcome.AppliedIndex == 0 {
		t.Fatalf("proposal did not settle: %+v", response)
	}
	return response
}
func restoreRF3ReadRelation(t *testing.T, fixture *rf3FaultFixture, leader int, state shardservice.ReplicatedMemberState, relation replication.RelationID, key, want []byte, found bool) {
	t.Helper()
	response, err := fixture.roundTrip(t, leader, &shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedReadLeader, Authority: serviceauthz.Authority{Node: fixture.nodes[(leader+1)%3], Generation: 5}, Capability: serviceauthz.CapabilityDataRead, Fence: state.Fence, Relation: relation, Key: key, MaxValueBytes: 4 << 20})
	expected := shardservice.ReplicatedReadMissing
	if found {
		expected = shardservice.ReplicatedReadFound
	}
	if err != nil || response == nil || response.Kind != expected || response.ReadApplied == 0 || (want != nil && !bytes.Equal(response.Value, want)) {
		t.Fatalf("ReadIndex relation=%d found=%t: %+v err=%v", relation, found, response, err)
	}
}
func restoreRF3Resources(t *testing.T, fixtures [2]*rf3FaultFixture) (uint64, int64) {
	t.Helper()
	var rss uint64
	var wal int64
	for _, fixture := range fixtures {
		rss += rf3FaultProcessRSSBytes(t, fixture.children)
		wal += rf3FaultWALAllocatedBytes(t, fixture.walPaths)
	}
	return rss, wal
}
