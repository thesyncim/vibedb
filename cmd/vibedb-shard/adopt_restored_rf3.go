package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/kubeoperator"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func runAdoptRestoredRF3(arguments []string) int {
	flags := flag.NewFlagSet("adopt-restored-rf3", flag.ContinueOnError)
	path := flags.String("manifest", "", "canonical per-node target RF3 preparation manifest")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 || *path == "" {
		return 2
	}
	manifest, err := loadPrepareRF3Manifest(*path)
	if err == nil {
		err = adoptRestoredRF3Member(manifest)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error adopt restored RF3 member: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "vibedb-shard restored RF3 member prepared root=%q manifest=%q\n",
		manifest.Root, filepath.Join(manifest.Root, "serve-rf3.vibejson"))
	return 0
}

func adoptRestoredRF3Member(input prepareRF3Manifest) error {
	walIdentity, authority, nodes, options, applyOptions, keyMaterial, err := validatePrepareRF3(input)
	if err != nil {
		return err
	}
	defer clear(keyMaterial)
	state, err := kubeoperator.OpenRestoredReplicaState(input.Root)
	if err != nil || !restoredBindingMatchesPrepare(state.Identity.Binding, input, authority) ||
		!restoredApplyMatchesPrepare(state.Apply, applyOptions) || !restoredRosterMatchesTargets(input, nodes, state) {
		return errors.Join(errPrepareRF3, err)
	}
	voters := state.SnapshotBase.GetMetadata().GetConfState().GetVoters()
	if state.SnapshotBase.GetMetadata().GetIndex() <= 1 || len(voters) != len(input.Members) {
		return errPrepareRF3
	}
	for index := range voters {
		if voters[index] != input.Members[index].MemberID {
			return errPrepareRF3
		}
	}
	paths := map[string]string{
		"wal": filepath.Join(input.Root, "member.wal"), "sql": filepath.Join(input.Root, "member.vdb"),
		"identity": filepath.Join(input.Root, "sql-identity.vibejson"),
		"apply":    filepath.Join(input.Root, "apply-identity.vibejson"), "key": filepath.Join(input.Root, "wal-key"),
	}
	manifest := buildPreparedRF3Manifest(input, nodes, paths)
	manifestRaw, err := vibejson.Marshal(&manifest)
	if err != nil {
		return err
	}
	if _, err = parseRF3Manifest(manifestRaw); err != nil {
		return errors.Join(errPrepareRF3, err)
	}
	servePath := filepath.Join(input.Root, "serve-rf3.vibejson")
	if _, statErr := os.Lstat(servePath); statErr == nil {
		return verifyPreparedRF3Member(input.Root, manifestRaw)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	for _, directory := range [...]string{
		"replica-actions", "source-exports", "source-artifacts", "split-runtime", "split-children",
	} {
		if err := os.MkdirAll(filepath.Join(input.Root, directory), 0o700); err != nil {
			return err
		}
		if info, statErr := os.Lstat(filepath.Join(input.Root, directory)); statErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.Join(errPrepareRF3, statErr)
		}
	}
	if err := writeRestoredRF3Artifact(filepath.Join(input.Root, "split-control.journal"), nil); err != nil {
		return err
	}
	staticBootstrap, err := prepareRF3SplitChildBootstrap(input.Members)
	if err != nil {
		return err
	}
	if err = writeRestoredRF3Artifact(filepath.Join(input.Root, "split-children", "static-bootstrap.pb"), staticBootstrap); err != nil {
		return err
	}
	key := raftstore.Key{ID: input.WAL.KeyID, Wrapped: []byte(input.WAL.WrappedKey)}
	copy(key.Material[:], keyMaterial)
	database, actualApply, err := driver.OpenReplicatedShardStoreWithApplyForSettlement(
		paths["sql"], state.Identity, applyOptions,
	)
	if err != nil || actualApply != state.Apply {
		if database != nil {
			_ = database.Close()
		}
		clear(key.Material[:])
		clear(key.Wrapped)
		return errors.Join(errPrepareRF3, err)
	}
	certificate, err := replicatedstate.OpenSnapshotBase(state.SnapshotBase)
	restoredStatic, staticErr := replicatedstate.StaticBootstrapForSnapshot(state.SnapshotBase)
	if err != nil || staticErr != nil {
		clear(key.Material[:])
		clear(key.Wrapped)
		return errors.Join(errPrepareRF3, err, staticErr, database.Close())
	}
	activation, resumed, err := database.ResumeReplicatedSnapshotActivation(
		state.Identity, certificate.Manifest, restoredStatic, applyOptions,
	)
	if err != nil {
		clear(key.Material[:])
		clear(key.Wrapped)
		return errors.Join(errPrepareRF3, err, database.Close())
	}
	var wal *raftstore.Store
	if resumed {
		wal, err = raftmember.OpenOrCreateStagedChildWAL(
			paths["wal"], walIdentity, key, input.TopologyRecoveryEpoch, authority,
			state.Identity, activation, options,
		)
		if err == nil {
			_, err = activation.Apply.InstallSnapshot(state.SnapshotBase)
		}
	} else {
		wal, err = raftstore.Open(paths["wal"], walIdentity, input.TopologyRecoveryEpoch, key, options)
		if err == nil {
			var retained *raftpb.Snapshot
			retained, err = wal.Snapshot()
			if err == nil && !proto.Equal(retained, state.SnapshotBase) {
				err = errPrepareRF3
			}
		}
		if err == nil {
			activation.Apply, activation.ApplyIdentity, err = database.OpenReplicatedApply(
				state.Identity, restoredStatic, applyOptions,
			)
			if err == nil && activation.ApplyIdentity != state.Apply {
				err = errPrepareRF3
			}
			if err == nil {
				binding, bindingErr := raftmember.BindingFromWAL(wal, authority)
				publication := activation.Apply.Published()
				if bindingErr != nil || binding != state.Identity.Binding ||
					publication.Applied != certificate.Manifest.State.Applied {
					err = errors.Join(errPrepareRF3, bindingErr)
				}
			}
		}
	}
	clear(key.Material[:])
	clear(key.Wrapped)
	if err != nil {
		if wal != nil {
			err = errors.Join(err, wal.Close())
		}
		if activation.Apply != nil {
			err = errors.Join(err, activation.Apply.Close())
		}
		return errors.Join(err, database.Close())
	}
	if err = errors.Join(wal.Close(), activation.Apply.Close(), database.Close()); err != nil {
		return err
	}
	identityRaw, err := state.Identity.MarshalJSON()
	if err != nil {
		return err
	}
	applyRaw, err := state.Apply.MarshalJSON()
	if err != nil {
		return err
	}
	for _, artifact := range []struct {
		path string
		raw  []byte
	}{
		{paths["identity"], identityRaw}, {paths["apply"], applyRaw}, {paths["key"], keyMaterial},
	} {
		if err = writeRestoredRF3Artifact(artifact.path, artifact.raw); err != nil {
			return err
		}
	}
	marker := make([]byte, sha256.Size+5)
	copy(marker, state.OperationDigest[:])
	binary.BigEndian.PutUint32(marker[sha256.Size:sha256.Size+4], state.GroupOrdinal)
	marker[sha256.Size+4] = state.ReplicaOrdinal
	if err = writeRestoredRF3Artifact(filepath.Join(input.Root, "restore_preparing"), marker); err != nil {
		return err
	}
	if err = writePrepareRF3File(servePath, manifestRaw, 0o600); err != nil {
		return err
	}
	if err = syncPrepareRF3Directory(input.Root); err != nil {
		return err
	}
	return verifyPreparedRF3Member(input.Root, manifestRaw)
}

func restoredRosterMatchesTargets(input prepareRF3Manifest, nodes [3]rafttransport.NodeID, state kubeoperator.RestoredReplicaState) bool {
	if len(input.Members) != 3 {
		return false
	}
	for index, member := range input.Members {
		found := false
		for _, target := range state.Targets {
			if member.MemberID == target.Member && nodes[index] == rafttransport.NodeID(target.Node) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func writeRestoredRF3Artifact(path string, raw []byte) error {
	if retained, err := os.ReadFile(path); err == nil {
		if bytes.Equal(retained, raw) {
			return nil
		}
		return errPrepareRF3
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return writePrepareRF3File(path, raw, 0o600)
}

func restoredBindingMatchesPrepare(binding driver.ReplicatedShardStoreBinding, input prepareRF3Manifest, authority driver.ReplicatedAuthorityProfile) bool {
	var cluster, incarnation, shardIncarnation, group, store [16]byte
	return decodePrepareRF3ID(input.ClusterID, cluster[:]) && decodePrepareRF3ID(input.ClusterIncarnation, incarnation[:]) &&
		decodePrepareRF3ID(input.ShardIncarnation, shardIncarnation[:]) && decodePrepareRF3ID(input.GroupID, group[:]) &&
		decodePrepareRF3ID(input.StoreID, store[:]) && binding.ClusterID == cluster &&
		binding.ClusterIncarnation == incarnation && binding.TopologyRecoveryEpoch == input.TopologyRecoveryEpoch &&
		binding.Distribution == input.Distribution && binding.Shard == input.Shard &&
		binding.AllocationGeneration == input.AllocationGeneration && binding.ShardIncarnation == shardIncarnation &&
		binding.GroupID == group && binding.MemberID == input.MemberID && binding.StoreID == store && binding.Authority == authority
}

func restoredApplyMatchesPrepare(identity driver.ReplicatedApplyIdentity, options driver.ReplicatedApplyOptions) bool {
	return identity.MaxSessions == options.MaxSessions && identity.RetryWindow == options.RetryWindow &&
		identity.TxnLimits == options.TxnLimits && identity.Placement == options.Placement &&
		identity.RequestLedgerCapacityBytes == options.RequestLedgerCapacityBytes &&
		identity.RequestLedgerCleanupReserveBytes == options.RequestLedgerCleanupReserveBytes &&
		bytes.Equal(identity.RequestLedgerRangeStart[:], options.RequestLedgerRangeStart[:]) &&
		bytes.Equal(identity.RequestLedgerRangeEnd[:], options.RequestLedgerRangeEnd[:]) &&
		bytes.Equal(identity.RequestLedgerRangeIdentity[:], options.RequestLedgerRangeIdentity[:]) &&
		identity.Format != 0 && identity.Storage != "" && identity.ValidationDigest != ([32]byte{})
}
