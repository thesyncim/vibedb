package snapshottransfer

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

type retainedTestCut struct {
	cut *replicatedstate.ReadSnapshot
	err error
}

func (source *retainedTestCut) SnapshotArtifactCut() (*replicatedstate.ReadSnapshot, error) {
	return source.cut, source.err
}

func TestRetainedSourceProviderExportsAndObservesAfterReopen(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	path := filepath.Join(root, "source-artifacts")
	node := rafttransport.NodeID{31}
	options := retainedSourceOptions(fixture, root, path, node, &retainedTestCut{cut: cut})
	provider, err := OpenRetainedSourceExportProvider(options)
	if err != nil {
		t.Fatal(err)
	}
	request := retainedSourceRequest(fixture, node)
	descriptor, err := (PinnedSourceControlExporter{Provider: provider}).
		ExportReplicaMoveSnapshot(context.Background(), request)
	if err != nil || !descriptorMatchesSourceRequest(descriptor, request) {
		t.Fatalf("descriptor=%+v err=%v", descriptor, err)
	}
	if err = provider.Close(); err != nil {
		t.Fatal(err)
	}

	// The request journal may still say Running after a lost terminal publish.
	// A new process reopens repository metadata and finds the exact artifact
	// without reacquiring a state-machine cut or rescanning relation data.
	options.Cut = &retainedTestCut{err: errors.New("must not pin")}
	reopened, err := OpenRetainedSourceExportProvider(options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	observed, found, err := reopened.ObserveSourceExport(context.Background(), request)
	if err != nil || !found || observed != descriptor {
		t.Fatalf("observed=%+v found=%t err=%v", observed, found, err)
	}

	// The reopened control provider and source data service must expose the
	// same published repository. This is the process-restart boundary used by
	// bootstrap-rf3 after a source export completed before a crash.
	targetNode := rafttransport.NodeID{32}
	members := []rafttransport.Member{
		{Group: descriptor.Group, ReplicaSetVersion: descriptor.ReplicaSetVersion,
			MemberID: descriptor.SourceMember, Node: node, Role: rafttransport.MemberVoter},
		{Group: descriptor.Group, ReplicaSetVersion: descriptor.ReplicaSetVersion,
			MemberID: descriptor.TargetMember, Node: targetNode, Role: rafttransport.MemberLearner},
	}
	registry, err := rafttransport.NewStaticRegistry(
		node, members, rafttransport.Limits{MaxGroups: 1, MaxMembers: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	service, err := reopened.NewDataService(ServiceOptions{
		Registry: registry, Authorize: func(got Descriptor) bool { return got == descriptor },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConnections: 1,
		MaxChunkBytes: MinChunkBytes, MaxInflightBytes: MinChunkBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceIdentity := rafttransport.PeerIdentity{TrustDomain: registry.TrustDomain(), Node: node}
	targetIdentity := rafttransport.PeerIdentity{TrustDomain: registry.TrustDomain(), Node: targetNode}
	opener := sourceProviderTestOpener{
		service: service, source: sourceIdentity, target: targetIdentity,
	}
	targetRepository := openTestRepository(t, filepath.Join(t.TempDir(), "target"))
	receiver := Receiver{
		Repository: targetRepository, Opener: &opener,
		ReadDeadline: deadline, WriteDeadline: deadline, Workspace: make([]byte, MinChunkBytes),
	}
	if err = receiver.Receive(context.Background(), node, descriptor); err != nil {
		t.Fatal(err)
	}
	if _, err = targetRepository.Manifest(descriptor); err != nil {
		t.Fatalf("transferred artifact is not published: %v", err)
	}
}

type sourceProviderTestOpener struct {
	service        *Service
	source, target rafttransport.PeerIdentity
}

func (opener *sourceProviderTestOpener) OpenSnapshot(
	ctx context.Context,
	node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	if node != opener.source.Node {
		return nil, rafttransport.ErrNodeNotFound
	}
	client, server := net.Pipe()
	go func() {
		_ = opener.service.Serve(ctx, &testPeerConn{
			Conn: server, identity: opener.target, class: rafttransport.TrafficSnapshot,
		})
	}()
	return &testPeerConn{
		Conn: client, identity: opener.source, class: rafttransport.TrafficSnapshot,
	}, nil
}

func TestRetainedSourceProviderRejectsWrongIdentityAndStaleMembership(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	node := rafttransport.NodeID{41}
	provider, err := OpenRetainedSourceExportProvider(retainedSourceOptions(
		fixture, root, filepath.Join(root, "artifacts"), node, &retainedTestCut{cut: cut},
	))
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	request := retainedSourceRequest(fixture, node)
	wrong := request
	wrong.SourceNode[0]++
	if _, err = provider.PinSourceExport(context.Background(), wrong); !errors.Is(err, ErrSourceConflict) {
		t.Fatalf("wrong source node err=%v", err)
	}
	request.ReplicaSetVersion++
	if _, err = provider.PinSourceExport(context.Background(), request); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale replica-set version err=%v", err)
	}
	// A stale cut must return its sole workspace instead of wedging later work.
	if _, err = provider.PinSourceExport(context.Background(), request); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("workspace was not returned after stale cut: %v", err)
	}
}

func TestRetainedSourceProviderRepositoryPathCannotEscapeOrTraverseSymlink(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	node := rafttransport.NodeID{51}
	options := retainedSourceOptions(
		fixture, root, filepath.Join(filepath.Dir(root), "escape"), node,
		&retainedTestCut{cut: cut},
	)
	if _, err := OpenRetainedSourceExportProvider(options); !errors.Is(err, ErrSourceControl) {
		t.Fatalf("escaped repository err=%v", err)
	}
	outside := t.TempDir()
	link := filepath.Join(root, "linked")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	options.RepositoryPath = filepath.Join(link, "artifacts")
	if _, err := OpenRetainedSourceExportProvider(options); !errors.Is(err, ErrSourceControl) {
		t.Fatalf("symlink repository err=%v", err)
	}
}

func retainedSourceOptions(
	fixture SourceExportPlan,
	root, path string,
	node rafttransport.NodeID,
	cut SourceArtifactCut,
) RetainedSourceExportOptions {
	return RetainedSourceExportOptions{
		DataRoot: root, RepositoryPath: path, Limits: sourceExportLimits(),
		ChunkBytes: MinChunkBytes, MaxConcurrent: 1,
		RuntimeIdentity: raftmember.RuntimeIdentity{
			Group: fixture.Group, Distribution: "export", Shard: "all",
			AllocationGeneration: 4, MemberID: fixture.SourceMember,
			StoreID: [16]byte{61}, NodeIncarnation: 62,
			RelationManifestDigest: fixture.ExpectedFence.RelationManifestDigest,
		},
		SourceNode: node, TargetMember: fixture.TargetMember,
		TargetStore: fixture.TargetStore, TargetIncarnation: fixture.TargetIncarnation,
		Cut: cut,
	}
}

func retainedSourceRequest(
	fixture SourceExportPlan,
	node rafttransport.NodeID,
) SourceControlRequest {
	return SourceControlRequest{
		Operation: [32]byte{1}, Step: [32]byte{2}, Group: fixture.Group,
		SourceMember: fixture.SourceMember, TargetMember: fixture.TargetMember,
		TargetStore: fixture.TargetStore, TargetIncarnation: fixture.TargetIncarnation,
		ReplicaSetVersion: fixture.ExpectedFence.ReplicaSetVersion, SourceNode: node,
	}
}
