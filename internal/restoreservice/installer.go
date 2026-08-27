// Package restoreservice composes cluster restore authority with SQL replica
// roots. Neither lower-level package depends on this orchestration layer.
package restoreservice

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var ErrInstaller = errors.New("restoreservice: group install failed")

// ReplicaRoot is one factory-owned fresh replica. Recover must settle a prior
// successful Commit after a process crash. Commit durably adopts activation
// and returns a unique physical-root digest; on success it takes ownership of
// Database and the activation claim.
type ReplicaRoot struct {
	Database     *sqldriver.Database
	Identity     sqldriver.ReplicatedShardStoreIdentity
	ApplyOptions sqldriver.ReplicatedApplyOptions
	Bootstrap    *pb.Snapshot
	Recover      func(context.Context, replicatedstate.SnapshotArtifactManifest) (sqldriver.ReplicatedChildActivation, [sha256.Size]byte, bool, error)
	Commit       func(context.Context, sqldriver.ReplicatedChildActivation) ([sha256.Size]byte, error)
}

// ReplicaFactory provisions or reopens the exact fresh physical root selected
// by operation.Targets[group].Replicas[replica].
type ReplicaFactory interface {
	OpenReplica(context.Context, clusterrestore.Operation, uint32, uint8,
		replicatedstate.SnapshotArtifactManifest) (ReplicaRoot, error)
}

// GroupInstaller implements clusterrestore.GroupInstaller for one RF3 target.
// Root holds one authenticated artifact spool and three durable verifier
// cursors; it is staging authority only and cannot mint a serving permit.
type GroupInstaller struct {
	Root    string
	Factory ReplicaFactory
}

func (installer GroupInstaller) Install(
	ctx context.Context, operation clusterrestore.Operation, ordinal uint32, reader io.Reader,
) (clusterrestore.RootWitness, error) {
	if ctx == nil || reader == nil || installer.Factory == nil ||
		int(ordinal) >= len(operation.Targets) || int(ordinal) >= len(operation.Certificate.Groups) ||
		!privateRoot(installer.Root) {
		return clusterrestore.RootWitness{}, ErrInstaller
	}
	cut, target := operation.Certificate.Groups[ordinal], operation.Targets[ordinal]
	if cut.SnapshotIndex <= 1 || cut.SnapshotTerm == 0 {
		return clusterrestore.RootWitness{}, ErrInstaller
	}
	directory := filepath.Join(
		installer.Root, fmt.Sprintf("restore-%x", operation.Digest), fmt.Sprintf("group-%08d", ordinal),
	)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return clusterrestore.RootWitness{}, err
	}
	if !privateRoot(directory) || !privateRoot(filepath.Dir(directory)) {
		return clusterrestore.RootWitness{}, ErrInstaller
	}
	if err := errors.Join(syncDirectory(installer.Root), syncDirectory(filepath.Dir(directory))); err != nil {
		return clusterrestore.RootWitness{}, err
	}
	artifactPath := filepath.Join(directory, "artifact.snap")
	if err := materializeArtifact(artifactPath, reader, cut.ArtifactBytes, cut.ArtifactHash); err != nil {
		return clusterrestore.RootWitness{}, err
	}
	manifest, err := verifyArtifact(artifactPath)
	if err != nil || manifest.Digest != cut.ArtifactManifestDigest ||
		manifest.State.Applied != cut.SnapshotIndex || manifest.State.LastTerm != cut.SnapshotTerm ||
		manifest.RelationManifestDigest != cut.RelationManifestDigest {
		return clusterrestore.RootWitness{}, errors.Join(ErrInstaller, err)
	}

	witness := clusterrestore.RootWitness{
		ArtifactManifest:     cut.ArtifactManifestDigest,
		SanitizedImageDigest: manifest.ImageDigest,
		SnapshotIndex:        cut.SnapshotIndex, SnapshotTerm: cut.SnapshotTerm,
	}
	appendGroupKey(witness.TargetGroup[:0], target.Group)
	entryDigest := genesisEntryDigest(operation, ordinal, manifest)
	var commonBinding sqldriver.ReplicatedShardStoreBinding
	var commonOptions sqldriver.ReplicatedApplyOptions
	var commonBootstrap *pb.Snapshot
	for replica := uint8(0); replica < 3; replica++ {
		if cause := context.Cause(ctx); cause != nil {
			return clusterrestore.RootWitness{}, cause
		}
		root, err := installer.Factory.OpenReplica(ctx, operation, ordinal, replica, manifest)
		if err != nil || !validReplicaRoot(root, operation, ordinal, replica) {
			return clusterrestore.RootWitness{}, errors.Join(ErrInstaller, err)
		}
		portable := root.Identity.Binding
		portable.MemberID, portable.StoreID = 0, [16]byte{}
		if replica == 0 {
			commonBinding, commonOptions = portable, root.ApplyOptions
			commonBootstrap = proto.Clone(root.Bootstrap).(*pb.Snapshot)
		} else if portable != commonBinding || root.ApplyOptions != commonOptions ||
			!proto.Equal(root.Bootstrap, commonBootstrap) {
			return clusterrestore.RootWitness{}, ErrInstaller
		}

		activation, rootDigest, recovered, err := root.Recover(ctx, manifest)
		if err != nil {
			return clusterrestore.RootWitness{}, err
		}
		if !recovered {
			activation, err = installReplica(
				root, artifactPath, filepath.Join(directory, fmt.Sprintf("replica-%d.cursor", replica+1)),
				manifest, cut.SnapshotIndex, cut.SnapshotTerm, entryDigest,
			)
			if err != nil {
				return clusterrestore.RootWitness{}, err
			}
			rootDigest, err = root.Commit(ctx, activation)
			if err != nil {
				return clusterrestore.RootWitness{}, err
			}
		}
		if rootDigest == ([sha256.Size]byte{}) {
			return clusterrestore.RootWitness{}, ErrInstaller
		}
		for prior := uint8(0); prior < replica; prior++ {
			if witness.ReplicaRoots[prior] == rootDigest {
				return clusterrestore.RootWitness{}, ErrInstaller
			}
		}
		certificate, err := replicatedstate.OpenSnapshotBase(activation.SnapshotBase)
		if err != nil {
			return clusterrestore.RootWitness{}, err
		}
		if replica == 0 {
			witness.GenesisProof = certificate.Digest
		} else if witness.GenesisProof != certificate.Digest {
			return clusterrestore.RootWitness{}, ErrInstaller
		}
		witness.ReplicaRoots[replica] = rootDigest
	}
	return witness, nil
}

func installReplica(
	root ReplicaRoot, artifactPath, cursorPath string,
	manifest replicatedstate.SnapshotArtifactManifest,
	applied, term uint64, entryDigest [sha256.Size]byte,
) (sqldriver.ReplicatedChildActivation, error) {
	cursor, err := readCursor(cursorPath)
	if err != nil {
		return sqldriver.ReplicatedChildActivation{}, err
	}
	stage, _, err := root.Database.OpenReplicatedRestoreStage(
		root.Identity, manifest, cursor, root.ApplyOptions,
	)
	if err != nil {
		return sqldriver.ReplicatedChildActivation{}, err
	}
	artifact, err := os.Open(artifactPath)
	if err == nil {
		_, err = artifact.Seek(int64(stage.Offset()), io.SeekStart)
	}
	if err == nil {
		_, err = stage.Receive(artifact, func(raw []byte) error { return replaceFile(cursorPath, raw) })
	}
	var closeArtifact error
	if artifact != nil {
		closeArtifact = artifact.Close()
	}
	var activation sqldriver.ReplicatedChildActivation
	if err == nil && closeArtifact == nil {
		activation, err = stage.Activate(
			root.Bootstrap,
			replicatedstate.StagedSnapshotCut{Applied: applied, Term: term, EntryDigest: entryDigest},
			replicatedstate.SnapshotArtifactOptions{},
		)
	}
	closeStage := stage.Close()
	return activation, errors.Join(err, closeArtifact, closeStage)
}

func verifyArtifact(path string) (replicatedstate.SnapshotArtifactManifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return replicatedstate.SnapshotArtifactManifest{}, err
	}
	manifest, verifyErr := replicatedstate.VerifySnapshotArtifact(
		file, replicatedstate.SnapshotArtifactCallbacks{},
	)
	return manifest, errors.Join(verifyErr, file.Close())
}

func materializeArtifact(path string, reader io.Reader, size uint64, digest [sha256.Size]byte) error {
	if size == 0 || size > uint64(^uint64(0)>>1) || digest == ([sha256.Size]byte{}) {
		return ErrInstaller
	}
	if file, err := os.Open(path); err == nil {
		h := sha256.New()
		n, copyErr := io.Copy(h, file)
		closeErr := file.Close()
		var got [sha256.Size]byte
		copy(got[:], h.Sum(nil))
		if copyErr == nil && closeErr == nil && uint64(n) == size && got == digest {
			return nil
		}
		return errors.Join(ErrInstaller, copyErr, closeErr)
	} else if !os.IsNotExist(err) {
		return err
	}
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if os.IsExist(err) {
		if removeErr := os.Remove(temporary); removeErr != nil {
			return errors.Join(err, removeErr)
		}
		file, err = os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	}
	if err != nil {
		return err
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(file, h), io.LimitReader(reader, int64(size)+1))
	syncErr, closeErr := file.Sync(), file.Close()
	var got [sha256.Size]byte
	copy(got[:], h.Sum(nil))
	if copyErr != nil || syncErr != nil || closeErr != nil || uint64(n) != size || got != digest {
		_ = os.Remove(temporary)
		return errors.Join(ErrInstaller, copyErr, syncErr, closeErr)
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func replaceFile(path string, raw []byte) error {
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(raw)
	syncErr, closeErr := file.Sync(), file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		_ = os.Remove(temporary)
		return errors.Join(writeErr, syncErr, closeErr)
	}
	if err := os.Rename(temporary, path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func readCursor(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil || len(raw) == 0 || len(raw) > 1<<20 {
		return nil, errors.Join(ErrInstaller, err)
	}
	return raw, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func privateRoot(path string) bool {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return false
	}
	info, err := os.Lstat(path)
	return err == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 && info.Mode().Perm()&0o077 == 0
}

func validReplicaRoot(root ReplicaRoot, operation clusterrestore.Operation, group uint32, replica uint8) bool {
	if root.Database == nil || root.Bootstrap == nil || root.Recover == nil || root.Commit == nil {
		return false
	}
	binding := root.Identity.Binding
	target, replicaTarget := operation.Targets[group], operation.Targets[group].Replicas[replica]
	conf := root.Bootstrap.GetMetadata().GetConfState()
	return binding.ClusterID == target.Group.ClusterID &&
		binding.ClusterIncarnation == target.Group.ClusterIncarnation &&
		binding.TopologyRecoveryEpoch == target.Group.TopologyRecoveryEpoch &&
		binding.ShardIncarnation == target.Group.ShardIncarnation && binding.GroupID == target.Group.GroupID &&
		binding.MemberID == replicaTarget.Member && binding.StoreID == replicaTarget.Store &&
		binding.Authority.ActivePolicyGeneration == operation.PolicyGeneration &&
		binding.Authority.SchemaGeneration == operation.Certificate.Groups[group].SchemaGeneration &&
		conf != nil && len(conf.Voters) == 3 && conf.Voters[0] == 1 && conf.Voters[1] == 2 && conf.Voters[2] == 3 &&
		len(conf.Learners) == 0 && len(conf.VotersOutgoing) == 0 && len(conf.LearnersNext) == 0
}

func genesisEntryDigest(operation clusterrestore.Operation, ordinal uint32, manifest replicatedstate.SnapshotArtifactManifest) [sha256.Size]byte {
	h := sha256.New()
	h.Write([]byte("vibedb/cluster-restore/fresh-genesis/format-1\x00"))
	h.Write(operation.Digest[:])
	var fixed [4]byte
	binary.BigEndian.PutUint32(fixed[:], ordinal)
	h.Write(fixed[:])
	h.Write(manifest.Digest[:])
	var result [sha256.Size]byte
	copy(result[:], h.Sum(nil))
	return result
}

func appendGroupKey(dst []byte, group raftmember.GroupKey) []byte {
	dst = append(dst, group.ClusterID[:]...)
	dst = append(dst, group.ClusterIncarnation[:]...)
	dst = binary.BigEndian.AppendUint64(dst, group.TopologyRecoveryEpoch)
	dst = append(dst, group.ShardIncarnation[:]...)
	return append(dst, group.GroupID[:]...)
}

var _ clusterrestore.GroupInstaller = GroupInstaller{}
