package snapshottransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func testDescriptor(payload []byte) Descriptor {
	id := func(seed byte) (out [16]byte) {
		for i := range out {
			out[i] = seed + byte(i)
		}
		return
	}
	return Descriptor{Group: raftmember.GroupKey{ClusterID: id(1), ClusterIncarnation: id(2), TopologyRecoveryEpoch: 3,
		ShardIncarnation: id(4), GroupID: id(5)}, SourceMember: 1, TargetMember: 2, TargetStore: id(6),
		TargetIncarnation: 7, SchemaGeneration: 8, ReplicaSetVersion: 9, SnapshotIndex: 10, SnapshotTerm: 11,
		Lineage: sha256.Sum256([]byte("lineage")), ArtifactHash: sha256.Sum256(payload),
		ArtifactBytes: uint64(len(payload)), ChunkBytes: MinChunkBytes}
}

func openTestRepository(t testing.TB, path string) *Repository {
	t.Helper()
	r, err := openRepository(path, Limits{MaxArtifacts: 4, MaxArtifactBytes: 1 << 20, MaxDiskBytes: 2 << 20},
		func(*os.File, Descriptor) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func appendAll(t testing.TB, r *Repository, d Descriptor, payload []byte, start uint64) {
	t.Helper()
	for start < uint64(len(payload)) {
		end := min(start+uint64(d.ChunkBytes), uint64(len(payload)))
		chunk := payload[start:end]
		next, _, err := r.Append(d, start, chunk, sha256.Sum256(chunk))
		if err != nil || next != end {
			t.Fatalf("append %d:%d = %d, %v", start, end, next, err)
		}
		start = end
	}
}

func TestDescriptorCanonicalFixedGrammar(t *testing.T) {
	d := testDescriptor(bytes.Repeat([]byte{1}, MinChunkBytes))
	raw, err := AppendDescriptor(nil, d)
	if err != nil || len(raw) != DescriptorBytes {
		t.Fatal(err)
	}
	opened, err := OpenDescriptor(raw)
	if err != nil || opened != d {
		t.Fatalf("open=%+v err=%v", opened, err)
	}
	for _, candidate := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), 0)} {
		if _, err := OpenDescriptor(candidate); !errors.Is(err, ErrDescriptor) {
			t.Fatalf("shape len %d = %v", len(candidate), err)
		}
	}
	bad := bytes.Clone(raw)
	bad[228] = 1
	if _, err := OpenDescriptor(bad); !errors.Is(err, ErrDescriptor) {
		t.Fatalf("tail=%v", err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		if x, e := OpenDescriptor(raw); e != nil || x.ArtifactBytes == 0 {
			panic("decode")
		}
	}); got != 0 {
		t.Fatalf("descriptor decode allocations=%v", got)
	}
}

func TestRepositoryDisconnectResumeCorruptionOrderingAndCleanup(t *testing.T) {
	payload := bytes.Repeat([]byte("snapshot-artifact-"), 700)
	d := testDescriptor(payload)
	path := filepath.Join(t.TempDir(), "repo")
	r := openTestRepository(t, path)
	first := payload[:d.ChunkBytes]
	if _, _, err := r.Append(d, 1, first, sha256.Sum256(first)); !errors.Is(err, ErrChunk) {
		t.Fatalf("reordered=%v", err)
	}
	badDigest := sha256.Sum256(first)
	badDigest[0] ^= 1
	if _, _, err := r.Append(d, 0, first, badDigest); !errors.Is(err, ErrChunk) {
		t.Fatalf("digest=%v", err)
	}
	next, done, err := r.Append(d, 0, first, sha256.Sum256(first))
	if err != nil || done || next != uint64(len(first)) {
		t.Fatalf("first=%d %t %v", next, done, err)
	}
	if retried, retryDone, err := r.Append(d, 0, first, sha256.Sum256(first)); err != nil ||
		retryDone || retried != next {
		t.Fatalf("exact retry=%d %t %v", retried, retryDone, err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	r = openTestRepository(t, path)
	offset, complete, err := r.Offset(d)
	if err != nil || complete || offset != uint64(len(first)) {
		t.Fatalf("resume=%d %t %v", offset, complete, err)
	}
	appendAll(t, r, d, payload, offset)
	offset, complete, err = r.Offset(d)
	if err != nil || !complete || offset != uint64(len(payload)) {
		t.Fatalf("complete=%d %t %v", offset, complete, err)
	}
	stale := d
	stale.SchemaGeneration++
	if _, _, err := r.Offset(stale); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale descriptor = %v", err)
	}
	chunk, final, err := r.ReadChunk(d, 0, make([]byte, 0, d.ChunkBytes))
	if err != nil || final || !bytes.Equal(chunk, payload[:d.ChunkBytes]) {
		t.Fatalf("read=%d %t %v", len(chunk), final, err)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		kind, _, ok := parseArtifactName(entry.Name())
		if ok && kind != 'p' {
			t.Fatalf("retained stage entry %q", entry.Name())
		}
	}
}

func TestRepositoryRecoversTruncatedUnacknowledgedTail(t *testing.T) {
	payload := bytes.Repeat([]byte{3}, MinChunkBytes*2)
	d := testDescriptor(payload)
	path := filepath.Join(t.TempDir(), "repo")
	r := openTestRepository(t, path)
	first := payload[:MinChunkBytes]
	if _, _, err := r.Append(d, 0, first, sha256.Sum256(first)); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	stage, _, _, _ := artifactNames(d.ArtifactHash)
	f, err := os.OpenFile(filepath.Join(path, stage), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write(bytes.Repeat([]byte{9}, 123)); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	r = openTestRepository(t, path)
	info, err := os.Stat(filepath.Join(path, stage))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != DescriptorBytes+MinChunkBytes {
		t.Fatalf("recovered size=%d", info.Size())
	}
	appendAll(t, r, d, payload, MinChunkBytes)
}

func TestRepositoryRecoversEveryPublishNamespacePhase(t *testing.T) {
	payload := bytes.Repeat([]byte{4}, MinChunkBytes)
	d := testDescriptor(payload)
	stage, cursor, temporary, published := artifactNames(d.ArtifactHash)
	var descriptor [DescriptorBytes]byte
	encoded, _ := AppendDescriptor(descriptor[:0], d)
	artifact := append(bytes.Clone(encoded), payload...)
	var cursorRaw [cursorBytes]byte
	copy(cursorRaw[:8], cursorMagic[:])
	copy(cursorRaw[8:40], d.ArtifactHash[:])
	binary.BigEndian.PutUint64(cursorRaw[40:48], d.ArtifactBytes)
	limits := Limits{MaxArtifacts: 2, MaxArtifactBytes: 1 << 20, MaxDiskBytes: 2 << 20}
	verify := func(*os.File, Descriptor) error { return nil }

	t.Run("complete-stage-before-rename", func(t *testing.T) {
		path := t.TempDir()
		if err := os.WriteFile(filepath.Join(path, stage), artifact, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, cursor), cursorRaw[:], 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, temporary), []byte("abandoned"), 0o600); err != nil {
			t.Fatal(err)
		}
		r, err := openRepository(path, limits, verify)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		if _, complete, err := r.Offset(d); err != nil || !complete {
			t.Fatalf("recovery complete=%t err=%v", complete, err)
		}
		for _, name := range []string{stage, cursor, temporary} {
			if _, err := os.Stat(filepath.Join(path, name)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s retained: %v", name, err)
			}
		}
	})

	t.Run("rename-before-cursor-cleanup", func(t *testing.T) {
		path := t.TempDir()
		if err := os.WriteFile(filepath.Join(path, published), artifact, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, cursor), cursorRaw[:], 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, stage), artifact, 0o600); err != nil {
			t.Fatal(err)
		}
		r, err := openRepository(path, limits, verify)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		if _, complete, err := r.Offset(d); err != nil || !complete {
			t.Fatalf("published complete=%t err=%v", complete, err)
		}
		for _, name := range []string{stage, cursor} {
			if _, err := os.Stat(filepath.Join(path, name)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s retained: %v", name, err)
			}
		}
	})

	t.Run("published-corruption-fails-closed", func(t *testing.T) {
		path := t.TempDir()
		corrupted := bytes.Clone(artifact)
		corrupted[len(corrupted)-1] ^= 1
		if err := os.WriteFile(filepath.Join(path, published), corrupted, 0o600); err != nil {
			t.Fatal(err)
		}
		if r, err := openRepository(path, limits, verify); r != nil || !errors.Is(err, ErrChunk) {
			t.Fatalf("corrupt reopen=%v %v", r, err)
		}
	})
}

func TestRepositoryArtifactAndDiskBoundsAreHard(t *testing.T) {
	verify := func(*os.File, Descriptor) error { return nil }
	first := bytes.Repeat([]byte{1}, MinChunkBytes*2)
	second := bytes.Repeat([]byte{2}, MinChunkBytes*2)
	d1, d2 := testDescriptor(first), testDescriptor(second)
	chunk := first[:MinChunkBytes]
	t.Run("artifact-count", func(t *testing.T) {
		r, err := openRepository(filepath.Join(t.TempDir(), "repo"), Limits{MaxArtifacts: 1, MaxArtifactBytes: MinChunkBytes * 2, MaxDiskBytes: MinChunkBytes * 4}, verify)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		if _, _, err := r.Append(d1, 0, chunk, sha256.Sum256(chunk)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.Append(d2, 0, second[:MinChunkBytes], sha256.Sum256(second[:MinChunkBytes])); !errors.Is(err, ErrBound) {
			t.Fatalf("artifact bound=%v", err)
		}
	})
	t.Run("disk-bytes", func(t *testing.T) {
		r, err := openRepository(filepath.Join(t.TempDir(), "repo"), Limits{MaxArtifacts: 2, MaxArtifactBytes: MinChunkBytes * 2, MaxDiskBytes: MinChunkBytes*2 + DescriptorBytes + 2*cursorBytes}, verify)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		if _, _, err := r.Append(d1, 0, chunk, sha256.Sum256(chunk)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.Append(d2, 0, second[:MinChunkBytes], sha256.Sum256(second[:MinChunkBytes])); !errors.Is(err, ErrBound) {
			t.Fatalf("disk bound=%v", err)
		}
		stats := r.Stats()
		if stats.Artifacts != 2 || stats.Staged != 2 || stats.Published != 0 || stats.DiskBytes > stats.DiskCapacity {
			t.Fatalf("stats=%+v", stats)
		}
	})
}

func TestFinalChunkRetrySettlesEveryPostRenameOutcome(t *testing.T) {
	phases := []repositoryFault{faultAfterPublishRename, faultAfterCursorRemove, faultAfterPublishSync}
	for _, phase := range phases {
		t.Run(fmt.Sprintf("phase-%d", phase), func(t *testing.T) {
			payload := bytes.Repeat([]byte{byte(phase) + 10}, MinChunkBytes*2)
			d := testDescriptor(payload)
			path := filepath.Join(t.TempDir(), "repo")
			r := openTestRepository(t, path)
			first, final := payload[:MinChunkBytes], payload[MinChunkBytes:]
			if _, _, err := r.Append(d, 0, first, sha256.Sum256(first)); err != nil {
				t.Fatal(err)
			}
			faultErr := errors.New("post-rename fault")
			fired := false
			r.fault = func(got repositoryFault) error {
				if got == phase && !fired {
					fired = true
					return faultErr
				}
				return nil
			}
			if next, done, err := r.Append(d, MinChunkBytes, final, sha256.Sum256(final)); next != uint64(len(payload)) || done || !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, faultErr) {
				t.Fatalf("fault append=%d %t %v", next, done, err)
			}
			r.fault = nil
			next, done, err := r.Append(d, MinChunkBytes, final, sha256.Sum256(final))
			if err != nil || !done || next != uint64(len(payload)) {
				t.Fatalf("retry=%d %t %v", next, done, err)
			}
			stats := r.Stats()
			if stats.DiskBytes != uint64(DescriptorBytes+len(payload)) || stats.Published != 1 || stats.Staged != 0 {
				t.Fatalf("settled stats=%+v", stats)
			}
			stage, cursor, temporary, published := artifactNames(d.ArtifactHash)
			for _, name := range []string{stage, cursor, temporary} {
				if _, err := os.Stat(filepath.Join(path, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("%s retained: %v", name, err)
				}
			}
			if _, err := os.Stat(filepath.Join(path, published)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCursorTempAndCommittedCursorAreBothDiskCharged(t *testing.T) {
	payload := bytes.Repeat([]byte{33}, MinChunkBytes*2)
	d := testDescriptor(payload)
	r := openTestRepository(t, filepath.Join(t.TempDir(), "repo"))
	first, final := payload[:MinChunkBytes], payload[MinChunkBytes:]
	if _, _, err := r.Append(d, 0, first, sha256.Sum256(first)); err != nil {
		t.Fatal(err)
	}
	wantCommitted := uint64(DescriptorBytes + MinChunkBytes + cursorBytes)
	if got := r.Stats().DiskBytes; got != wantCommitted {
		t.Fatalf("committed cursor bytes=%d want=%d", got, wantCommitted)
	}
	faultErr := errors.New("cursor temp synced")
	observed := false
	r.fault = func(phase repositoryFault) error {
		if phase == faultAfterCursorTempSync && !observed {
			observed = true
			rec := r.records[d.ArtifactHash]
			wantPeak := uint64(DescriptorBytes + len(payload) + 2*cursorBytes)
			if !rec.cursorLive || !rec.tempLive || r.diskBytes != wantPeak {
				t.Fatalf("cursor peak rec=%+v disk=%d want=%d", rec, r.diskBytes, wantPeak)
			}
			return faultErr
		}
		return nil
	}
	if _, _, err := r.Append(d, MinChunkBytes, final, sha256.Sum256(final)); !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, faultErr) {
		t.Fatalf("temp fault=%v", err)
	}
	if got, want := r.Stats().DiskBytes, uint64(DescriptorBytes+len(payload)+cursorBytes); got != want {
		t.Fatalf("cleaned temp bytes=%d want=%d", got, want)
	}
	r.fault = nil
	if _, done, err := r.Append(d, MinChunkBytes, final, sha256.Sum256(final)); err != nil || !done {
		t.Fatalf("temp retry done=%t err=%v", done, err)
	}
}

func TestDiskAccountingAdditionCannotWrap(t *testing.T) {
	r := Repository{limits: Limits{MaxDiskBytes: math.MaxUint64}, diskBytes: math.MaxUint64 - 4}
	if r.canAddDisk(5) || r.addDisk(5) || r.diskBytes != math.MaxUint64-4 {
		t.Fatalf("overflow accounting=%d", r.diskBytes)
	}
}

func TestRecoveryCumulativeDiskBoundFailsWithoutWrap(t *testing.T) {
	path := t.TempDir()
	for seed := byte(1); seed <= 2; seed++ {
		payload := bytes.Repeat([]byte{seed}, MinChunkBytes)
		d := testDescriptor(payload)
		_, _, _, published := artifactNames(d.ArtifactHash)
		raw, _ := AppendDescriptor(nil, d)
		raw = append(raw, payload...)
		if err := os.WriteFile(filepath.Join(path, published), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	limits := Limits{MaxArtifacts: 2, MaxArtifactBytes: MinChunkBytes,
		MaxDiskBytes: MinChunkBytes + DescriptorBytes + 2*cursorBytes}
	r, err := openRepository(path, limits, func(*os.File, Descriptor) error { return nil })
	if r != nil || !errors.Is(err, ErrBound) {
		t.Fatalf("cumulative recovery=%v %v", r, err)
	}
}

type testPeerConn struct {
	net.Conn
	identity  rafttransport.PeerIdentity
	keyDigest [sha256.Size]byte
	class     rafttransport.TrafficClass
}

func (c *testPeerConn) PeerIdentity() rafttransport.PeerIdentity { return c.identity }
func (c *testPeerConn) PeerKeyDigest() [sha256.Size]byte         { return c.keyDigest }
func (c *testPeerConn) TrafficClass() rafttransport.TrafficClass { return c.class }

type cutConn struct {
	rafttransport.PeerConnection
	remaining int
}

func (c *cutConn) Read(p []byte) (int, error) {
	if c.remaining <= 0 {
		_ = c.Close()
		return 0, io.EOF
	}
	if len(p) > c.remaining {
		p = p[:c.remaining]
	}
	n, e := c.PeerConnection.Read(p)
	c.remaining -= n
	if c.remaining == 0 {
		_ = c.Close()
	}
	return n, e
}

type testOpener struct {
	service        *Service
	source, target rafttransport.PeerIdentity
	failFirst      bool
	calls          int
}

func (o *testOpener) OpenSnapshot(ctx context.Context, node rafttransport.NodeID) (rafttransport.PeerConnection, error) {
	o.calls++
	client, server := net.Pipe()
	serverConn := &testPeerConn{Conn: server, identity: o.target, class: rafttransport.TrafficSnapshot}
	go func() { _ = o.service.Serve(ctx, serverConn) }()
	base := rafttransport.PeerConnection(&testPeerConn{Conn: client, identity: o.source, class: rafttransport.TrafficSnapshot})
	if o.failFirst && o.calls == 1 {
		return &cutConn{PeerConnection: base, remaining: responseBytes + 97}, nil
	}
	return base, nil
}

func testRegistry(t testing.TB) (*rafttransport.StaticRegistry, rafttransport.PeerIdentity, rafttransport.PeerIdentity) {
	t.Helper()
	d := testDescriptor(bytes.Repeat([]byte{1}, MinChunkBytes))
	var n1, n2 rafttransport.NodeID
	copy(n1[:], d.Group.ClusterID[:])
	copy(n2[:], d.Group.ClusterIncarnation[:])
	members := []rafttransport.Member{{Group: d.Group, ReplicaSetVersion: d.ReplicaSetVersion, MemberID: 1, Node: n1, Role: rafttransport.MemberVoter}, {Group: d.Group, ReplicaSetVersion: d.ReplicaSetVersion, MemberID: 2, Node: n2, Role: rafttransport.MemberVoter}}
	r, err := rafttransport.NewStaticRegistry(n1, members, rafttransport.Limits{MaxGroups: 1, MaxMembers: 2})
	if err != nil {
		t.Fatal(err)
	}
	domain := r.TrustDomain()
	return r, rafttransport.PeerIdentity{TrustDomain: domain, Node: n1}, rafttransport.PeerIdentity{TrustDomain: domain, Node: n2}
}

func TestAuthenticatedServiceDisconnectResumeAndIdentityRotation(t *testing.T) {
	payload := bytes.Repeat([]byte("network-snapshot"), 900)
	d := testDescriptor(payload)
	sourceRepo := openTestRepository(t, filepath.Join(t.TempDir(), "source"))
	appendAll(t, sourceRepo, d, payload, 0)
	targetRepo := openTestRepository(t, filepath.Join(t.TempDir(), "target"))
	registry, source, target := testRegistry(t)
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	service, err := NewService(ServiceOptions{Repository: sourceRepo, Registry: registry, Authorize: func(got Descriptor) bool { return got == d }, ReadDeadline: deadline, WriteDeadline: deadline, MaxConnections: 2, MaxChunkBytes: MinChunkBytes, MaxInflightBytes: 2 * MinChunkBytes})
	if err != nil {
		t.Fatal(err)
	}
	opener := &testOpener{service: service, source: source, target: target, failFirst: true}
	receiver := Receiver{Repository: targetRepo, Opener: opener, ReadDeadline: deadline, WriteDeadline: deadline, Workspace: make([]byte, MinChunkBytes)}
	if err = receiver.Receive(context.Background(), source.Node, d); err == nil {
		t.Fatal("disconnect was hidden")
	}
	if offset, _, _ := targetRepo.Offset(d); offset != 0 {
		t.Fatalf("partial chunk acknowledged at %d", offset)
	}
	if err = receiver.Receive(context.Background(), source.Node, d); err != nil {
		t.Fatal(err)
	}
	if opener.calls < 4 {
		t.Fatalf("chunk reconnect calls=%d", opener.calls)
	}
	rotated := target
	rotated.Node[0] ^= 0xff
	badClient, badServer := net.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- service.Serve(context.Background(), &testPeerConn{Conn: badServer, identity: rotated, class: rafttransport.TrafficSnapshot})
	}()
	var request [requestBytes]byte
	copy(request[:8], requestMagic[:])
	_, _ = AppendDescriptor(request[8:8], d)
	binary.BigEndian.PutUint64(request[8+DescriptorBytes:], 0)
	if err := writeFull(badClient, request[:]); err != nil {
		t.Fatal(err)
	}
	_ = badClient.Close()
	if err := <-errCh; !errors.Is(err, ErrStaleFence) {
		t.Fatalf("rotated identity=%v", err)
	}
}

func TestBootstrapMetricsOwnTargetChunksBytesAndResidentWorkspace(t *testing.T) {
	payload := bytes.Repeat([]byte("target-bootstrap-metrics"), 700)
	d := testDescriptor(payload)
	sourceRepo := openTestRepository(t, filepath.Join(t.TempDir(), "source"))
	appendAll(t, sourceRepo, d, payload, 0)
	targetRepo := openTestRepository(t, filepath.Join(t.TempDir(), "target"))
	registry, source, target := testRegistry(t)
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	data, err := NewService(ServiceOptions{Repository: sourceRepo, Registry: registry,
		Authorize: func(got Descriptor) bool { return got == d }, ReadDeadline: deadline,
		WriteDeadline: deadline, MaxConnections: 2, MaxChunkBytes: MinChunkBytes,
		MaxInflightBytes: 2 * MinChunkBytes})
	if err != nil {
		t.Fatal(err)
	}
	receiver := &Receiver{Repository: targetRepo,
		Opener:       &testOpener{service: data, source: source, target: target},
		ReadDeadline: deadline, WriteDeadline: deadline, Workspace: make([]byte, MinChunkBytes)}
	request, identity, sourceNode := bootstrapControlFixture()
	control, err := NewBootstrapControlService(BootstrapControlOptions{
		Journal:  &memoryBootstrapJournal{records: make(map[[32]byte]BootstrapRecord)},
		Receiver: receiver, Installer: &testBootstrapInstaller{identity: identity},
		Releaser:     BootstrapArtifactReleaseFunc(func(context.Context, BootstrapRequest, raftmember.RuntimeIdentity) error { return nil }),
		Authorize:    func(rafttransport.PeerIdentity, BootstrapRequest) bool { return true },
		SourceNode:   func(Descriptor) (rafttransport.NodeID, bool) { return sourceNode, true },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if metrics := control.Metrics(); metrics.ResidentBytes != MinChunkBytes {
		t.Fatalf("initial metrics=%+v", metrics)
	}
	if err = receiver.Receive(context.Background(), source.Node, d); err != nil {
		t.Fatal(err)
	}
	wantChunks := uint64((len(payload) + MinChunkBytes - 1) / MinChunkBytes)
	if metrics := control.Metrics(); metrics.Chunks != wantChunks || metrics.Bytes != uint64(len(payload)) ||
		metrics.ResidentBytes != MinChunkBytes || metrics.Requests != 0 || metrics.Inflight != 0 {
		t.Fatalf("transfer metrics=%+v request=%+v", metrics, request)
	}
}

func TestSequentialConnectionsCannotOverRetainInflightBudget(t *testing.T) {
	payload := bytes.Repeat([]byte{44}, MinChunkBytes)
	d := testDescriptor(payload)
	sourceRepo := openTestRepository(t, filepath.Join(t.TempDir(), "source"))
	appendAll(t, sourceRepo, d, payload, 0)
	registry, _, target := testRegistry(t)
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	service, err := NewService(ServiceOptions{Repository: sourceRepo, Registry: registry, Authorize: func(got Descriptor) bool { return got == d }, ReadDeadline: deadline, WriteDeadline: deadline, MaxConnections: 4, MaxChunkBytes: MinChunkBytes, MaxInflightBytes: MinChunkBytes})
	if err != nil {
		t.Fatal(err)
	}
	var request [requestBytes]byte
	copy(request[:8], requestMagic[:])
	_, _ = AppendDescriptor(request[8:8:8+DescriptorBytes], d)
	var response [responseBytes]byte
	chunk := make([]byte, MinChunkBytes)
	for range 12 {
		client, server := net.Pipe()
		done := make(chan error, 1)
		go func() {
			done <- service.Serve(context.Background(), &testPeerConn{Conn: server, identity: target, class: rafttransport.TrafficSnapshot})
		}()
		if err := writeFull(client, request[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(client, response[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(client, chunk); err != nil {
			t.Fatal(err)
		}
		_ = client.Close()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	stats := service.Stats()
	if stats.ResidentBytes != MinChunkBytes || stats.ResidentBytes > stats.ResidentCapacity || stats.Connections != 0 {
		t.Fatalf("resident stats=%+v", stats)
	}
	retained := 0
	for range cap(service.slots) {
		slot := <-service.slots
		retained += cap(slot.chunk)
		service.slots <- slot
	}
	if retained != MinChunkBytes || int64(retained) > service.maxInflight {
		t.Fatalf("retained=%d max=%d", retained, service.maxInflight)
	}
}

func FuzzDescriptorCanonical(f *testing.F) {
	d := testDescriptor(bytes.Repeat([]byte{1}, MinChunkBytes))
	raw, _ := AppendDescriptor(nil, d)
	f.Add(raw)
	f.Fuzz(func(t *testing.T, b []byte) {
		opened, err := OpenDescriptor(b)
		if err != nil {
			return
		}
		encoded, e := AppendDescriptor(nil, opened)
		if e != nil || !bytes.Equal(encoded, b) {
			t.Fatalf("noncanonical accepted")
		}
	})
}

func BenchmarkRepositoryReadChunk(b *testing.B) {
	payload := bytes.Repeat([]byte{7}, MinChunkBytes)
	d := testDescriptor(payload)
	r := openTestRepository(b, b.TempDir())
	appendAll(b, r, d, payload, 0)
	workspace := make([]byte, 0, MinChunkBytes)
	stats := r.Stats()
	b.ReportAllocs()
	b.SetBytes(MinChunkBytes)
	for b.Loop() {
		chunk, _, err := r.ReadChunk(d, 0, workspace)
		if err != nil || len(chunk) != MinChunkBytes {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(stats.DiskBytes-d.ArtifactBytes), "B/artifact")
}

func BenchmarkSnapshotServiceChunk(b *testing.B) {
	payload := bytes.Repeat([]byte{7}, MinChunkBytes)
	d := testDescriptor(payload)
	sourceRepo := openTestRepository(b, b.TempDir())
	appendAll(b, sourceRepo, d, payload, 0)
	registry, _, target := testRegistry(b)
	deadline := func() time.Time { return time.Now().Add(time.Minute) }
	service, err := NewService(ServiceOptions{Repository: sourceRepo, Registry: registry, Authorize: func(got Descriptor) bool { return got == d }, ReadDeadline: deadline, WriteDeadline: deadline, MaxConnections: 1, MaxChunkBytes: MinChunkBytes, MaxInflightBytes: MinChunkBytes})
	if err != nil {
		b.Fatal(err)
	}
	var request [requestBytes]byte
	copy(request[:8], requestMagic[:])
	_, _ = AppendDescriptor(request[8:8], d)
	var header [responseBytes]byte
	chunk := make([]byte, MinChunkBytes)
	b.ReportAllocs()
	b.SetBytes(MinChunkBytes)
	for b.Loop() {
		client, server := net.Pipe()
		done := make(chan error, 1)
		go func() {
			done <- service.Serve(context.Background(), &testPeerConn{Conn: server, identity: target, class: rafttransport.TrafficSnapshot})
		}()
		if err := writeFull(client, request[:]); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(client, header[:]); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(client, chunk); err != nil {
			b.Fatal(err)
		}
		_ = client.Close()
		if err := <-done; err != nil {
			b.Fatal(err)
		}
	}
}
