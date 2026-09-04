package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Fresh preparation publishes the shared log and every initial SQL root in one
// directory rename. It does not read or migrate any legacy range log.
type prepareRF3NodeManifest struct {
	Root    string               `json:"root"`
	NodeLog rf3NodeLogManifest   `json:"node_log"`
	Groups  []prepareRF3Manifest `json:"groups"`
}

type persistedRF3NodeGroup struct {
	WAL           persistedRF3WAL                `json:"wal"`
	SQL           persistedRF3SQL                `json:"sql"`
	Route         persistedRF3GroupRoute         `json:"route"`
	ChildRegistry persistedRF3SplitChildRegistry `json:"child_registry"`
	Members       []persistedRF3Member           `json:"members"`
}

type persistedRF3NodeSplitControl struct {
	JournalPath   string                    `json:"journal_path"`
	MaxRecords    int                       `json:"max_records"`
	MaxFileBytes  int64                     `json:"max_file_bytes"`
	Grants        []persistedRF3ActionGrant `json:"grants"`
	MaxOperations int                       `json:"max_operations"`
}

type persistedRF3NodeRuntime struct {
	NodeLog             rf3NodeLogManifest           `json:"node_log"`
	Listeners           rf3ManifestListeners         `json:"listeners"`
	TLS                 rf3ManifestTLS               `json:"tls"`
	AuthorizationPolicy string                       `json:"authorization_policy"`
	ReplicaControl      persistedRF3ReplicaControl   `json:"replica_control"`
	SplitControl        persistedRF3NodeSplitControl `json:"split_control"`
	Groups              []persistedRF3NodeGroup      `json:"groups"`
}

func runPrepareNodeRF3(args []string) int {
	flags := flag.NewFlagSet("prepare-node-rf3", flag.ContinueOnError)
	path := flags.String("manifest", "", "canonical fresh RF3 node preparation manifest")
	if err := flags.Parse(args); err != nil || *path == "" || flags.NArg() != 0 {
		return 2
	}
	raw, err := readPrepareRF3File(*path, maxRF3ManifestBytes)
	var input prepareRF3NodeManifest
	if err == nil {
		err = vibejson.Unmarshal(raw, &input)
	}
	if err == nil {
		canonical, marshalErr := vibejson.Marshal(&input)
		if marshalErr != nil || !bytes.Equal(raw, canonical) {
			err = errors.Join(errPrepareRF3, marshalErr)
		}
	}
	if err == nil {
		err = provisionRF3Node(input)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error prepare RF3 node: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "RF3 node prepared manifest=%q\n", filepath.Join(input.Root, "serve-rf3.vibejson"))
	return 0
}

func provisionRF3Node(input prepareRF3NodeManifest) (resultErr error) {
	if !filepath.IsAbs(input.Root) || filepath.Clean(input.Root) != input.Root || input.Root == "/" || len(input.Groups) == 0 || len(input.Groups) > maxRF3ManifestGroups || input.NodeLog.Path != filepath.Join(input.Root, "node-log") {
		return errPrepareRF3
	}
	configRaw, err := vibejson.Marshal(&input.NodeLog)
	if err != nil {
		return err
	}
	configDoc, err := vibejson.Parse(configRaw)
	if err != nil {
		return err
	}
	if _, err = parseRF3NodeLogManifest(configDoc.Node()); err != nil {
		return err
	}
	if _, err := os.Lstat(input.Root); !errors.Is(err, os.ErrNotExist) {
		return errors.Join(errPrepareRF3, err)
	}
	key, err := loadRF3WALKey(input.NodeLog.KeyID, input.NodeLog.KeyMaterialPath)
	if err != nil {
		return err
	}
	defer clear(key.Material[:])
	key.Wrapped = []byte(input.Groups[0].WAL.WrappedKey)
	boots := make([]raftstore.NodeBootstrap, len(input.Groups))
	manifests := make([]persistedRF3Manifest, len(input.Groups))
	var identity raftstore.NodeIdentity
	var firstNodes [3]rafttransport.NodeID
	for i, group := range input.Groups {
		if group.Root != filepath.Join(input.Root, fmt.Sprintf("group-%d", i)) || group.DevelopmentOnly {
			return errPrepareRF3
		}
		member, _, nodes, _, _, material, err := validatePrepareRF3(group)
		sameKey := bytes.Equal(material, key.Material[:])
		clear(material)
		if err != nil || !sameKey || group.WAL.KeyID != key.ID {
			return errors.Join(errPrepareRF3, err)
		}
		var local rafttransport.NodeID
		for ordinal, peer := range group.Members {
			if peer.MemberID == group.MemberID {
				local = nodes[ordinal]
			}
		}
		current := raftstore.NodeIdentity{ClusterID: member.ClusterID, ClusterIncarnation: member.ClusterIncarnation, NodeID: [16]byte(local)}
		if i == 0 {
			identity, firstNodes = current, nodes
		} else if current != identity || nodes != firstNodes || group.Listeners != input.Groups[0].Listeners || group.TLS != input.Groups[0].TLS || group.AuthorizationPolicy != input.Groups[0].AuthorizationPolicy || !reflect.DeepEqual(group.Members, input.Groups[0].Members) || !reflect.DeepEqual(group.SplitControl.Grants, input.Groups[0].SplitControl.Grants) {
			return errPrepareRF3
		}
		index, term := uint64(1), uint64(1)
		voters := make([]uint64, len(group.Members))
		for j := range group.Members {
			voters[j] = group.Members[j].MemberID
		}
		boots[i] = raftstore.NodeBootstrap{Descriptor: raftstore.GroupDescriptor{TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, AllocationGeneration: member.AllocationGeneration, MemberID: member.MemberID, GroupID: member.GroupID, ShardIncarnation: member.ShardIncarnation, StoreID: member.StoreID, Distribution: member.Distribution, Shard: member.Shard}, Snapshot: &pb.Snapshot{Data: []byte("vibedb-rf3-bootstrap"), Metadata: &pb.SnapshotMetadata{Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: voters}}}}
		manifests[i] = buildPreparedRF3Manifest(group, nodes, preparedRF3MemberPaths(group.Root))
	}
	first := manifests[0]
	runtime := persistedRF3NodeRuntime{NodeLog: input.NodeLog, Listeners: first.Listeners, TLS: first.TLS, AuthorizationPolicy: first.AuthorizationPolicy, ReplicaControl: first.ReplicaControl,
		SplitControl: persistedRF3NodeSplitControl{JournalPath: first.SplitControl.JournalPath, MaxRecords: first.SplitControl.MaxRecords, MaxFileBytes: first.SplitControl.MaxFileBytes, Grants: first.SplitControl.Grants}}
	runtime.NodeLog.KeyMaterialPath = filepath.Join(input.Root, "node-key")
	for _, manifest := range manifests {
		runtime.SplitControl.MaxOperations = max(runtime.SplitControl.MaxOperations, manifest.SplitControl.ChildRegistry.MaxOperations)
		runtime.Groups = append(runtime.Groups, persistedRF3NodeGroup{WAL: manifest.WAL, SQL: manifest.SQL, Route: manifest.Route, ChildRegistry: manifest.SplitControl.ChildRegistry, Members: manifest.Members})
	}
	raw, err := vibejson.Marshal(&runtime)
	if err != nil {
		return err
	}
	if _, err := parseRF3Manifest(raw); err != nil {
		return err
	}
	parent := filepath.Dir(input.Root)
	stage, err := os.MkdirTemp(parent, ".node-prepare-")
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			resultErr = errors.Join(resultErr, os.RemoveAll(stage))
		}
	}()
	// Creation's descriptor wave has a bounded group count. Larger fresh
	// nodes register the remaining groups while the entire directory is still
	// private; the final rename remains the only serving publication point.
	initial := min(len(boots), raftstore.MaxPersistGroupBatches-1)
	store, err := raftstore.CreateNodeStore(filepath.Join(stage, "node-log"), identity, key, boots[:initial], input.NodeLog.Options)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, store.Close()) }()
	for _, bootstrap := range boots[initial:] {
		if _, err := store.RegisterGroupWithSnapshot(bootstrap.Descriptor, bootstrap.Snapshot); err != nil {
			return err
		}
	}
	for i, group := range input.Groups {
		view, found := store.GroupByID(boots[i].Descriptor.GroupID)
		if !found {
			return raftstore.ErrInvalid
		}
		if err := provisionRF3MemberInto(group, filepath.Join(stage, fmt.Sprintf("group-%d", i)), view, false); err != nil {
			return err
		}
		// Each immutable member reference also names its physical node log. The
		// supervisor can compose additional groups without inventing log settings.
		reference := manifests[i]
		reference.NodeLog = &runtime.NodeLog
		referenceRaw, err := vibejson.Marshal(&reference)
		if err != nil {
			return err
		}
		if _, err := parseRF3Manifest(referenceRaw); err != nil {
			return err
		}
		groupStage := filepath.Join(stage, fmt.Sprintf("group-%d", i))
		nextPath := filepath.Join(groupStage, "serve-rf3.node")
		if err := writePrepareRF3File(nextPath, referenceRaw, 0600); err != nil {
			return err
		}
		if err := os.Rename(nextPath, filepath.Join(groupStage, "serve-rf3.vibejson")); err != nil {
			return err
		}
		if err := syncPrepareRF3Directory(groupStage); err != nil {
			return err
		}
	}
	if err := store.Close(); err != nil {
		return err
	}
	if err := writePrepareRF3File(filepath.Join(stage, "node-key"), key.Material[:], 0o600); err != nil {
		return err
	}
	if err := writePrepareRF3File(filepath.Join(stage, "serve-rf3.vibejson"), raw, 0o600); err != nil {
		return err
	}
	if err := syncPrepareRF3Directory(stage); err != nil {
		return err
	}
	if err := os.Rename(stage, input.Root); err != nil {
		return err
	}
	published = true
	return syncPrepareRF3Directory(parent)
}
