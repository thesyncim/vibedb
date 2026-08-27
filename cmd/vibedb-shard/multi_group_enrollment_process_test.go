//go:build darwin || linux

package main

import (
	"bytes"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibedb/internal/storeio"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestServeRF3ProcessRoutesTwoEnrolledGroups(t *testing.T) {
	if os.Getenv(rf3CommandHelperEnvironment) != "" {
		return
	}
	root := t.TempDir()
	listeners := reserveRF3ProcessListeners(t)
	defer closeRF3ProcessListeners(listeners)
	addresses := rf3testfixture.ProcessListeners{
		Peer: listeners[0].Addr().String(), Control: listeners[1].Addr().String(),
		Native: listeners[2].Addr().String(), Snapshot: listeners[3].Addr().String(),
	}
	nodes := rf3CommandNodes()
	targetNode := rafttransport.NodeID{0xd1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	credentials, roots, err := rf3testfixture.WriteCredentials(root, rf3CommandIdentityOID,
		rafttransport.TrustDomain{ClusterID: rf3CommandGroup().ClusterID, ClusterIncarnation: rf3CommandGroup().ClusterIncarnation},
		append(append([]rafttransport.NodeID(nil), nodes[:]...), targetNode))
	if err != nil {
		t.Fatal(err)
	}
	policyPath := filepath.Join(root, "policy.vibejson")
	if err = os.WriteFile(policyPath, rf3CommandPolicyWithTarget(nodes, targetNode), 0o600); err != nil {
		t.Fatal(err)
	}
	peerAddresses := [3]string{addresses.Peer, rf3CommandUnusedAddress(t), rf3CommandUnusedAddress(t)}
	walOptions := raftstore.Options{MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
		MaxRecords: 4096, MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes}
	authority := rf3CommandAuthority()
	apply := sqldriver.ReplicatedApplyOptions{MaxSessions: 32, RetryWindow: 8,
		TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20},
		Placement: sqldriver.ReplicatedPlacementProfile{Format: sqldriver.ReplicatedPlacementProfileFormat,
			ShardKey: gateway.ReplicatedCatalogPrimaryKey, TupleVersion: distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}}
	identity1 := rf3CommandStoreIdentity(1)
	identity2 := identity1
	identity2.GroupID[0] ^= 0x40
	identity2.ShardIncarnation[0] ^= 0x20
	identity2.StoreID[0] ^= 0x10
	identity2.Distribution = "process-second"
	identity2.Shard = "process-second-0001"
	target1 := rf3testfixture.ProcessTarget{MemberID: 4, NodeID: targetNode, StoreID: [16]byte{0xe1}, NodeIncarnation: 9,
		Listeners: rf3testfixture.ProcessListeners{Peer: rf3CommandUnusedAddress(t), Native: rf3CommandUnusedAddress(t), Snapshot: rf3CommandUnusedAddress(t), Control: rf3CommandUnusedAddress(t)}}
	target2 := target1
	target2.StoreID[0]++
	prepare := func(name string, identity raftstore.Identity, target rf3testfixture.ProcessTarget) rf3testfixture.PreparedProcessMember {
		key := raftstore.Key{ID: "multi-group-key", Wrapped: []byte("test-wrapped")}
		for i := range key.Material {
			key.Material[i] = byte(i + 1)
		}
		prepared, prepareErr := rf3testfixture.PrepareProcessMember(rf3testfixture.ProcessMemberOptions{
			Root: filepath.Join(root, name), Table: "docs", CreateTable: `CREATE TABLE docs (PRIMARY KEY (id))`,
			Identity: identity, Key: key, WAL: walOptions, Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
			Authority: authority, Apply: apply, Listeners: addresses, Credential: credentials[0], Roots: roots,
			AuthorizationPolicy: policyPath, Nodes: nodes, PeerAddresses: peerAddresses, Target: &target,
		})
		if errors.Is(prepareErr, storeio.ErrStrictAllocationUnsupported) || errors.Is(prepareErr, raftstore.ErrPlatformUnsupported) {
			t.Skipf("RF3 strict durable allocation unsupported: %v", prepareErr)
		}
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		return prepared
	}
	first := prepare("first", identity1, target1)
	second := prepare("second", identity2, target2)
	manifestPath := filepath.Join(root, "multi-group-serve.vibejson")
	if err = os.WriteFile(manifestPath, combineRF3ProcessGroups(t, first.ManifestPath, second.ManifestPath), 0o600); err != nil {
		t.Fatal(err)
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := &rf3CommandDiagnostic{maximum: rf3CommandDiagnosticBytes}
	command := exec.Command(executable, "-test.run=^TestServeRF3CommandProcessHelper$", "-test.v")
	command.Env = append(os.Environ(), rf3CommandHelperEnvironment+"=1", rf3CommandManifestEnvironment+"="+manifestPath)
	for _, listener := range listeners {
		file, fileErr := listener.File()
		if fileErr != nil {
			t.Fatal(fileErr)
		}
		defer file.Close()
		command.ExtraFiles = append(command.ExtraFiles, file)
	}
	command.Stdout, command.Stderr = diagnostic, diagnostic
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	for index := range listeners {
		_ = command.ExtraFiles[index].Close()
		_ = listeners[index].Close()
		listeners[index] = nil
	}
	child := &rf3CommandChild{member: 1, command: command, exited: make(chan struct{}), diagnostic: diagnostic}
	go func() {
		waitErr := command.Wait()
		child.mu.Lock()
		child.waitErr = waitErr
		child.mu.Unlock()
		close(child.exited)
	}()
	defer closeRF3CommandChildren(t, []*rf3CommandChild{child})
	waitRF3CommandReady(t, child, 30*time.Second)

	profile, err := servicetls.LoadProfile(credentials[3].Certificate, credentials[3].Key, roots,
		rf3testfixture.ProcessIdentityOID, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	deadline := func() time.Time { return time.Now().Add(10 * time.Second) }
	client, err := snapshottransfer.NewSourceControlClient(snapshottransfer.SourceControlClientOptions{
		Opener:       rf3CommandControlOpener{profile: profile, addresses: map[rafttransport.NodeID]string{nodes[0]: addresses.Control}, deadline: deadline},
		ReadDeadline: deadline, WriteDeadline: deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	groups := []struct {
		identity raftstore.Identity
		target   rf3testfixture.ProcessTarget
		op       byte
	}{{identity1, target1, 0xa1}, {identity2, target2, 0xa2}}
	requests := make([]snapshottransfer.SourceControlRequest, 0, len(groups))
	descriptors := make([]snapshottransfer.Descriptor, 0, len(groups))
	latencies := make([]time.Duration, 0, len(groups)*2)
	var networkBytes uint64
	for _, item := range groups {
		group := raftmember.GroupKey{ClusterID: item.identity.ClusterID, ClusterIncarnation: item.identity.ClusterIncarnation,
			TopologyRecoveryEpoch: rf3CommandGroup().TopologyRecoveryEpoch, ShardIncarnation: item.identity.ShardIncarnation, GroupID: item.identity.GroupID}
		request := snapshottransfer.SourceControlRequest{Operation: [32]byte{item.op}, Step: [32]byte{item.op + 1}, Group: group,
			SourceMember: 1, TargetMember: 4, TargetStore: item.target.StoreID, TargetIncarnation: item.target.NodeIncarnation,
			ReplicaSetVersion: 1, SourceNode: nodes[0]}
		started := time.Now()
		descriptor, prepareErr := client.PrepareReplicaMoveSnapshot(t.Context(), request)
		latencies = append(latencies, time.Since(started))
		if prepareErr != nil {
			t.Fatalf("group %x prepare: %v; process=%s", group.GroupID, prepareErr, diagnostic.String())
		}
		if descriptor.Group != group || descriptor.TargetStore != item.target.StoreID {
			t.Fatalf("misrouted descriptor=%+v", descriptor)
		}
		requests = append(requests, request)
		descriptors = append(descriptors, descriptor)
		networkBytes += descriptor.ArtifactBytes
	}
	baseRoot := filepath.Join(root, "first")
	for _, identity := range []raftstore.Identity{identity1, identity2} {
		group := raftmember.GroupKey{ClusterID: identity.ClusterID, ClusterIncarnation: identity.ClusterIncarnation,
			TopologyRecoveryEpoch: rf3CommandGroup().TopologyRecoveryEpoch, ShardIncarnation: identity.ShardIncarnation, GroupID: identity.GroupID}
		if info, statErr := os.Stat(rf3SnapshotGroupPath(filepath.Join(baseRoot, "source-artifacts"), group, true)); statErr != nil || !info.IsDir() {
			t.Fatalf("group %x repository isolation: %v", group.GroupID, statErr)
		}
	}
	if networkBytes == 0 || networkBytes > 64<<20 {
		t.Fatalf("bounded snapshot bytes=%d", networkBytes)
	}
	if bytesOnDisk := rf3ProcessTreeBytes(t, filepath.Join(baseRoot, "source-artifacts")); bytesOnDisk > 64<<20 {
		t.Fatalf("bounded source storage bytes=%d", bytesOnDisk)
	}
	if rss := rf3ProcessRSSBytes(t, command.Process.Pid); rss > 1<<30 {
		t.Fatalf("bounded source RSS bytes=%d", rss)
	}

	// SIGKILL after both durable Complete records, then reopen the exact
	// manifest. Exact retries must settle from the group-scoped journals and
	// return byte-identical descriptors without exporting a second artifact.
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-child.exited:
	case <-time.After(10 * time.Second):
		t.Fatal("multi-group source did not exit after SIGKILL")
	}
	child.mu.Lock()
	child.waitErr = nil // SIGKILL is the injected fault, not an unexpected exit.
	child.mu.Unlock()
	rf3CommandRestartChild(t, child, executable, manifestPath, [4]string{
		addresses.Peer, addresses.Control, addresses.Native, addresses.Snapshot,
	})
	for index, request := range requests {
		started := time.Now()
		descriptor, retryErr := client.PrepareReplicaMoveSnapshot(t.Context(), request)
		latencies = append(latencies, time.Since(started))
		if retryErr != nil || descriptor != descriptors[index] {
			t.Fatalf("post-SIGKILL group %d descriptor=%+v want=%+v err=%v",
				index, descriptor, descriptors[index], retryErr)
		}
	}
	var maximum time.Duration
	for _, latency := range latencies {
		if latency > maximum {
			maximum = latency
		}
	}
	if maximum > 10*time.Second {
		t.Fatalf("bounded control p99/max=%s samples=%v", maximum, latencies)
	}
}

func reserveRF3ProcessListeners(t testing.TB) []*net.TCPListener {
	t.Helper()
	listeners := make([]*net.TCPListener, 4)
	for index := range listeners {
		listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			closeRF3ProcessListeners(listeners)
			t.Fatal(err)
		}
		listeners[index] = listener
	}
	return listeners
}

func closeRF3ProcessListeners(listeners []*net.TCPListener) {
	for _, listener := range listeners {
		if listener != nil {
			_ = listener.Close()
		}
	}
}

func combineRF3ProcessGroups(t testing.TB, paths ...string) []byte {
	t.Helper()
	groups := make([][]byte, 0, len(paths))
	var common []byte
	for index, path := range paths {
		document, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		listener := bytes.Index(document, []byte(`"listeners":`))
		members := bytes.Index(document, []byte(`,"members":`))
		if listener < 2 || members <= listener || document[listener-1] != ',' || document[len(document)-1] != '}' {
			t.Fatalf("invalid singleton process manifest %q", path)
		}
		if index == 0 {
			common = append([]byte(nil), document[listener:members]...)
		}
		group := make([]byte, 0, len(document))
		group = append(group, '{')
		group = append(group, document[1:listener-1]...)
		group = append(group, ',')
		group = append(group, document[members+1:len(document)-1]...)
		group = append(group, '}')
		groups = append(groups, group)
	}
	result := make([]byte, 0, len(common)+len(groups[0])*len(groups)+32)
	result = append(result, '{')
	result = append(result, common...)
	result = append(result, `,"groups":[`...)
	for index, group := range groups {
		if index != 0 {
			result = append(result, ',')
		}
		result = append(result, group...)
	}
	result = append(result, ']', '}')
	return result
}

func rf3ProcessTreeBytes(t testing.TB, root string) uint64 {
	t.Helper()
	var total uint64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += uint64(info.Size())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return total
}

func rf3ProcessRSSBytes(t testing.TB, pid int) uint64 {
	t.Helper()
	if runtime.GOOS != "linux" {
		return 0
	}
	raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "status"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[2] != "kB" {
			t.Fatalf("invalid VmRSS line %q", line)
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return kb << 10
	}
	t.Fatal("VmRSS missing")
	return 0
}
