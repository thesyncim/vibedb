package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// 64 is a persisted discriminator, not a position in the startup manifest.
// Dynamic entries are addressed by their complete operation/child identity.
const rf3DynamicTemplateSlot = 64
const rf3DynamicTemplateBytes = 512 << 10

// This catalog has no historical directory scan or resident history index.
// One immutable template accompanies each existing child artifact. The fixed
// admission and adopted inventories bound the live references; historical
// templates follow those artifacts' existing retention lifecycle.
type rf3DynamicChildTemplateCatalog struct {
	mu            sync.Mutex
	manifest      rf3Manifest
	root          *os.Root
	lock          *os.File
	failed        bool
	syncDirectory func(*os.Root) error
}

type rf3DynamicChildTemplateRecord struct {
	Version         uint32
	NodeIncarnation uint64
	SourceGroup     raftmember.GroupKey
	Preparation     []byte
	Registry        rf3ManifestSplitChildRegistry
	Bootstrap       []byte
	Peers           []rafttransport.PhysicalPeer
}

func openRF3DynamicChildTemplateCatalog(manifest rf3Manifest) (*rf3DynamicChildTemplateCatalog, error) {
	path := manifest.ReplicaControl.SourceDataRoot
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.Join(errRF3Serving, err)
	}
	parent, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	const name = "dynamic-child-templates"
	if err = parent.Mkdir(name, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	info, err = parent.Lstat(name)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.Join(errRF3Serving, err)
	}
	if err = syncRF3TemplateDirectory(parent); err != nil {
		return nil, err
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	result := &rf3DynamicChildTemplateCatalog{manifest: manifest, root: root, syncDirectory: syncRF3TemplateDirectory}
	fail := func(cause error) (*rf3DynamicChildTemplateCatalog, error) {
		return nil, errors.Join(cause, result.Close())
	}
	if info, err = root.Lstat("templates.lock"); err == nil && !info.Mode().IsRegular() || err != nil && !errors.Is(err, os.ErrNotExist) {
		return fail(errors.Join(errRF3Serving, err))
	}
	result.lock, err = root.OpenFile("templates.lock", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return fail(err)
	}
	if err = storeio.LockWriter(result.lock); err != nil {
		return fail(err)
	}
	return result, nil
}

func syncRF3TemplateDirectory(root *os.Root) error {
	f, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func (catalog *rf3DynamicChildTemplateCatalog) Close() error {
	if catalog == nil {
		return nil
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	var err error
	if catalog.lock != nil {
		err = errors.Join(storeio.UnlockWriter(catalog.lock), catalog.lock.Close())
		catalog.lock = nil
	}
	if catalog.root != nil {
		err = errors.Join(err, catalog.root.Close())
		catalog.root = nil
	}
	return err
}

func (catalog *rf3DynamicChildTemplateCatalog) Publish(preparation splitcontroller.ChildPreparation, source raftmember.GroupKey, registry rf3ManifestSplitChildRegistry, bootstrap *pb.Snapshot, peerSets ...[]rafttransport.PhysicalPeer) (rf3SplitChildResources, error) {
	if catalog == nil || bootstrap == nil || len(peerSets) != 1 {
		return rf3SplitChildResources{}, errRF3Serving
	}
	preparationRaw, err := splitcontroller.AppendChildPreparation(nil, preparation)
	if err != nil {
		return rf3SplitChildResources{}, err
	}
	bootstrapRaw, err := proto.MarshalOptions{Deterministic: true}.Marshal(bootstrap)
	if err != nil {
		return rf3SplitChildResources{}, err
	}
	record := rf3DynamicChildTemplateRecord{Version: 1, NodeIncarnation: catalog.manifest.NodeIncarnation,
		SourceGroup: source, Preparation: preparationRaw, Registry: registry, Bootstrap: bootstrapRaw, Peers: peerSets[0]}
	canonical, err := vibejson.Marshal(&record)
	if err != nil || len(canonical) > rf3DynamicTemplateBytes-sha256.Size {
		return rf3SplitChildResources{}, errors.Join(errRF3Serving, err)
	}
	digest := sha256.Sum256(canonical)
	raw := append(canonical, digest[:]...)
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if catalog.root == nil || catalog.failed {
		return rf3SplitChildResources{}, errRF3Serving
	}
	resources, err := catalog.openRecord(raw, [32]byte(preparation.OperationID()), preparation.Child())
	if err != nil {
		return rf3SplitChildResources{}, err
	}
	// One admitted split cannot be relabelled onto another source when its
	// next child arrives. This inspects only the fixed child fanout, including
	// retained templates whose preparation admission has already been retired.
	for child := uint8(0); child < autosplit.MaxSplitChildren; child++ {
		if child == preparation.Child() {
			continue
		}
		other, found, err := catalog.readLocked([32]byte(preparation.OperationID()), child)
		if err != nil {
			return rf3SplitChildResources{}, err
		}
		if found && (other.SourceGroup != source || other.Preparation.AllocationDigest() != preparation.AllocationDigest()) {
			return rf3SplitChildResources{}, splitcontroller.ErrChildPreparation
		}
	}
	prior, found, err := catalog.readLocked([32]byte(preparation.OperationID()), preparation.Child())
	if err != nil {
		return rf3SplitChildResources{}, err
	}
	if found {
		priorRaw, readErr := catalog.readRawLocked([32]byte(preparation.OperationID()), preparation.Child())
		if readErr != nil || !bytes.Equal(priorRaw, raw) {
			return rf3SplitChildResources{}, errors.Join(splitcontroller.ErrChildPreparation, readErr)
		}
		return prior, nil
	}
	operation := [32]byte(preparation.OperationID())
	directory := hex.EncodeToString(operation[:])
	// Directory creation, file data, atomic publication, and directory entry
	// durability are separate crash cuts. Any uncertain cut poisons this handle.
	catalog.failed = true
	if err = catalog.root.Mkdir(directory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return rf3SplitChildResources{}, err
	}
	info, err := catalog.root.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return rf3SplitChildResources{}, errors.Join(errRF3Serving, err)
	}
	if err = catalog.syncDirectory(catalog.root); err != nil {
		return rf3SplitChildResources{}, err
	}
	operationRoot, err := catalog.root.OpenRoot(directory)
	if err != nil {
		return rf3SplitChildResources{}, err
	}
	defer operationRoot.Close()
	base := "child-" + strconv.Itoa(int(preparation.Child()))
	if err = operationRoot.Remove(base + ".tmp"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return rf3SplitChildResources{}, err
	}
	f, err := operationRoot.OpenFile(base+".tmp", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return rf3SplitChildResources{}, err
	}
	n, writeErr := f.Write(raw)
	if writeErr == nil && n != len(raw) {
		writeErr = io.ErrShortWrite
	}
	if err = errors.Join(writeErr, f.Sync(), f.Close()); err != nil {
		return rf3SplitChildResources{}, err
	}
	if err = operationRoot.Rename(base+".tmp", base+".template"); err != nil {
		return rf3SplitChildResources{}, err
	}
	if err = catalog.syncDirectory(operationRoot); err != nil {
		return rf3SplitChildResources{}, err
	}
	catalog.failed = false
	return resources, nil
}

func (catalog *rf3DynamicChildTemplateCatalog) Read(operation [32]byte, child uint8) (rf3SplitChildResources, bool, error) {
	if catalog == nil {
		return rf3SplitChildResources{}, false, errRF3Serving
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	return catalog.readLocked(operation, child)
}

func (catalog *rf3DynamicChildTemplateCatalog) Resolve(operation [32]byte, child uint8, target splitcontroller.ChildReplicaTarget) (rf3SplitChildResources, bool, error) {
	resources, found, err := catalog.Read(operation, child)
	if err != nil || !found {
		return resources, found, err
	}
	preparedTarget := resources.Preparation.ReplicaTarget()
	expected, err := vibejson.Marshal(&preparedTarget)
	actual, otherErr := vibejson.Marshal(&target)
	if err != nil || otherErr != nil || !bytes.Equal(expected, actual) {
		return rf3SplitChildResources{}, false, errors.Join(splitcontroller.ErrChildPreparation, err, otherErr)
	}
	return resources, true, nil
}

func (catalog *rf3DynamicChildTemplateCatalog) readLocked(operation [32]byte, child uint8) (rf3SplitChildResources, bool, error) {
	if catalog.root == nil || catalog.failed || operation == ([32]byte{}) || child >= autosplit.MaxSplitChildren {
		return rf3SplitChildResources{}, false, errRF3Serving
	}
	raw, err := catalog.readRawLocked(operation, child)
	if errors.Is(err, os.ErrNotExist) {
		return rf3SplitChildResources{}, false, nil
	}
	if err != nil {
		return rf3SplitChildResources{}, false, err
	}
	resources, err := catalog.openRecord(raw, operation, child)
	return resources, err == nil, err
}

func (catalog *rf3DynamicChildTemplateCatalog) readRawLocked(operation [32]byte, child uint8) ([]byte, error) {
	directory := hex.EncodeToString(operation[:])
	info, err := catalog.root.Lstat(directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errRF3Serving
	}
	name := filepath.Join(directory, "child-"+strconv.Itoa(int(child))+".template")
	info, err = catalog.root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() <= sha256.Size || info.Size() > rf3DynamicTemplateBytes {
		return nil, errRF3Serving
	}
	f, err := catalog.root.Open(name)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(f, rf3DynamicTemplateBytes+1))
	return raw, errors.Join(readErr, f.Close())
}

func (catalog *rf3DynamicChildTemplateCatalog) openRecord(raw []byte, operation [32]byte, child uint8) (rf3SplitChildResources, error) {
	if len(raw) <= sha256.Size || len(raw) > rf3DynamicTemplateBytes {
		return rf3SplitChildResources{}, errRF3Serving
	}
	payload := raw[:len(raw)-sha256.Size]
	if sha256.Sum256(payload) != [32]byte(raw[len(payload):]) {
		return rf3SplitChildResources{}, errRF3Serving
	}
	var record rf3DynamicChildTemplateRecord
	if err := vibejson.Unmarshal(payload, &record); err != nil {
		return rf3SplitChildResources{}, errors.Join(errRF3Serving, err)
	}
	canonical, err := vibejson.Marshal(&record)
	if err != nil || !bytes.Equal(canonical, payload) || record.Version != 1 || record.NodeIncarnation == 0 || record.NodeIncarnation != catalog.manifest.NodeIncarnation {
		return rf3SplitChildResources{}, errRF3Serving
	}
	preparation, err := splitcontroller.OpenChildPreparation(record.Preparation)
	if err != nil || [32]byte(preparation.OperationID()) != operation || preparation.Child() != child {
		return rf3SplitChildResources{}, errors.Join(errRF3Serving, err)
	}
	canonical, err = splitcontroller.AppendChildPreparation(nil, preparation)
	if err != nil || !bytes.Equal(canonical, record.Preparation) {
		return rf3SplitChildResources{}, errRF3Serving
	}
	target := preparation.ReplicaTarget()
	source := record.SourceGroup
	group := groupFromBinding(target.SQL.Binding)
	if source.ClusterID != group.ClusterID || source.ClusterIncarnation != group.ClusterIncarnation || source.TopologyRecoveryEpoch != group.TopologyRecoveryEpoch ||
		source.GroupID == ([16]byte{}) || source.ShardIncarnation == ([16]byte{}) || source == group || target.NodeIncarnation == 0 {
		return rf3SplitChildResources{}, errRF3Serving
	}
	registry := record.Registry
	paths, err := registry.childPaths(operation, child)
	if err != nil || target.RuntimeRoot != paths.Root || target.SQLPath != paths.Database || target.WALPath != paths.WAL ||
		!catalog.allowedRegistryRoot(registry.Root) || registry.StaticBootstrapPath != "" && registry.StaticBootstrapPath != filepath.Join(registry.Root, "static-bootstrap.pb") ||
		registry.MaxOperations < 1 || registry.MaxOperations > maxRF3SplitChildOperations || registry.MemberCount != rf3ManifestMembers || registry.ReplicaSetVersion == 0 ||
		!rf3SplitChildTemplateMatchesRetained(registry, target.SQL, target.Apply) || !rf3SplitChildSchemaMatchesRetained(registry, target.SQL) {
		return rf3SplitChildResources{}, errors.Join(errRF3Serving, err)
	}
	var bootstrap pb.Snapshot
	if err = proto.Unmarshal(record.Bootstrap, &bootstrap); err != nil {
		return rf3SplitChildResources{}, errors.Join(errRF3Serving, err)
	}
	canonical, err = proto.MarshalOptions{Deterministic: true}.Marshal(&bootstrap)
	metadata := bootstrap.GetMetadata()
	if err != nil || !bytes.Equal(canonical, record.Bootstrap) || metadata == nil || metadata.GetConfState() == nil {
		return rf3SplitChildResources{}, errRF3Serving
	}
	conf := metadata.GetConfState()
	if len(conf.Voters) != rf3ManifestMembers || len(conf.Learners) != 0 || len(conf.VotersOutgoing) != 0 || len(conf.LearnersNext) != 0 || conf.GetAutoLeave() {
		return rf3SplitChildResources{}, errRF3Serving
	}
	if len(record.Peers) != rf3ManifestMembers {
		return rf3SplitChildResources{}, errRF3Serving
	}
	for index, member := range registry.Members[:registry.MemberCount] {
		var replica splitcontroller.ChildReplicaTarget
		for _, candidate := range preparation.Target().Replicas {
			if candidate.Member == member.MemberID {
				replica = candidate
				break
			}
		}
		if replica.Member == 0 || member.NodeID != replica.Node || member.StoreID != replica.StoreID || conf.Voters[index] != replica.Member ||
			index > 0 && registry.Members[index-1].MemberID >= member.MemberID {
			return rf3SplitChildResources{}, errRF3Serving
		}
		peer := record.Peers[index]
		if peer.NodeID != replica.Node || peer.Node != (rafttransport.NodeID{}) && peer.Node != peer.NodeID ||
			peer.TrustDomain.ClusterID != group.ClusterID || peer.TrustDomain.ClusterIncarnation != group.ClusterIncarnation ||
			peer.Incarnation == 0 || peer.NodeID == target.Node && peer.Incarnation != record.NodeIncarnation || peer.Revision == 0 || peer.ServiceKeyDigest == ([32]byte{}) ||
			peer.State != rafttransport.PeerEnrolled || peer.Endpoint == "" || len(peer.Endpoint) > rafttransport.MaxPeerEndpointBytes ||
			peer.Endpoint != replica.PeerAddress || peer.Address != "" && peer.Address != peer.Endpoint ||
			member.PeerAddress != replica.PeerAddress || member.NativeAddress != replica.NativeAddress {
			return rf3SplitChildResources{}, errRF3Serving
		}
	}
	preparationDigest, err := splitcontroller.ChildPreparationDigest(preparation)
	if err != nil {
		return rf3SplitChildResources{}, err
	}
	return rf3SplitChildResources{Slot: rf3DynamicTemplateSlot, SourceGroup: source, Registry: registry, Bootstrap: &bootstrap, Peers: record.Peers, Preparation: preparation, PreparationDigest: preparationDigest}, nil
}

func (catalog *rf3DynamicChildTemplateCatalog) allowedRegistryRoot(path string) bool {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Base(path) != "split-children" {
		return false
	}
	roots := []string{catalog.manifest.ReplicaControl.SourceDataRoot}
	for _, group := range catalog.manifest.groupBundles() {
		roots = append(roots, group.Route.MemberRoot)
	}
	for _, root := range roots {
		if root == "" {
			continue
		}
		relative, err := filepath.Rel(root, path)
		if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			continue
		}
		current := root
		valid := true
		for _, part := range append([]string{"."}, strings.Split(relative, string(filepath.Separator))...) {
			current = filepath.Join(current, part)
			info, err := os.Lstat(current)
			if errors.Is(err, os.ErrNotExist) {
				break
			}
			if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				valid = false
				break
			}
		}
		if valid {
			return true
		}
	}
	return false
}
