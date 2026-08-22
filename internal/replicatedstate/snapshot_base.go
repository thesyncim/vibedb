package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/thesyncim/vibedb/internal/replication"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	snapshotBaseFormat              = uint16(1)
	snapshotBaseHeaderBytes         = 272
	MaxSnapshotBaseCertificateBytes = snapshotBaseHeaderBytes +
		MaxStateEnvelopeBytes + MaxStaticBootstrapEnvelopeBytes +
		replication.MaxCollectionBytes + recordChecksumLen
)

var (
	snapshotBaseMagic          = [8]byte{'V', 'D', 'B', 'R', 'B', 'A', 'S', 'E'}
	snapshotBaseChecksumDomain = []byte(
		"vibedb/replicated-state/snapshot-base-certificate\x00",
	)
	snapshotBaseIdentityDomain = []byte(
		"vibedb/replicated-state/snapshot-base-identity\x00",
	)
)

// SnapshotBaseCertificate is the small authenticated bridge between an
// already verified collection artifact and an immutable Raft WAL base. It
// never contains collection rows. StaticBootstrap is the original index-one
// identity required to reopen the replicated apply layer.
type SnapshotBaseCertificate struct {
	Manifest        SnapshotArtifactManifest
	StaticBootstrap *pb.Snapshot
	Digest          [sha256.Size]byte
}

// BuildSnapshotBase constructs the bounded Raft snapshot certificate for a
// completely verified artifact. Bulk state remains in the staged collection
// files. The returned protobuf is detached from every input.
func BuildSnapshotBase(
	manifest SnapshotArtifactManifest,
	staticBootstrap *pb.Snapshot,
) (*pb.Snapshot, error) {
	if err := validateExpectedSnapshotArtifact(manifest); err != nil {
		return nil, fmt.Errorf("%w: manifest: %v", ErrSnapshotBase, err)
	}
	bootstrapBytes, bootstrapDigest, err := validateBootstrap(staticBootstrap)
	if err != nil || bootstrapDigest != manifest.State.BootstrapDigest {
		return nil, fmt.Errorf("%w: static bootstrap", ErrSnapshotBase)
	}
	stateBytes, err := AppendState(nil, manifest.State)
	if err != nil {
		return nil, fmt.Errorf("%w: state: %v", ErrSnapshotBase, err)
	}
	total := snapshotBaseHeaderBytes + len(stateBytes) + len(bootstrapBytes) +
		len(manifest.UserCollection) + recordChecksumLen
	if total > MaxSnapshotBaseCertificateBytes {
		return nil, fmt.Errorf("%w: certificate bytes %d", ErrSnapshotBase, total)
	}
	result := make([]byte, total)
	copy(result[0:8], snapshotBaseMagic[:])
	binary.LittleEndian.PutUint16(result[8:10], snapshotBaseFormat)
	binary.LittleEndian.PutUint16(result[10:12], snapshotBaseHeaderBytes)
	binary.LittleEndian.PutUint32(result[12:16], uint32(total))
	binary.LittleEndian.PutUint32(result[16:20], uint32(len(stateBytes)))
	binary.LittleEndian.PutUint32(result[20:24], uint32(len(bootstrapBytes)))
	binary.LittleEndian.PutUint16(result[24:26], uint16(len(manifest.UserCollection)))
	binary.LittleEndian.PutUint32(result[28:32], manifest.TargetChunkBytes)
	binary.LittleEndian.PutUint64(result[32:40], manifest.Chunks)
	binary.LittleEndian.PutUint64(result[40:48], manifest.SystemRows)
	binary.LittleEndian.PutUint64(result[48:56], manifest.UserRows)
	binary.LittleEndian.PutUint64(result[56:64], manifest.PayloadBytes)
	binary.LittleEndian.PutUint64(result[64:72], manifest.EncodedBytes)
	copy(result[80:112], manifest.HeaderDigest[:])
	copy(result[112:144], manifest.LastChunkDigest[:])
	copy(result[144:176], manifest.Digest[:])
	copy(result[176:208], manifest.ImageDigest[:])
	stateDigest := sha256.Sum256(stateBytes)
	copy(result[208:240], stateDigest[:])
	copy(result[240:272], bootstrapDigest[:])
	cursor := snapshotBaseHeaderBytes
	cursor += copy(result[cursor:], stateBytes)
	cursor += copy(result[cursor:], bootstrapBytes)
	copy(result[cursor:], manifest.UserCollection)
	sealRecord(result, snapshotBaseChecksumDomain)
	index, term := manifest.State.Applied, manifest.State.LastTerm
	return &pb.Snapshot{Data: result, Metadata: &pb.SnapshotMetadata{
		Index: &index, Term: &term, ConfState: cloneConfState(manifest.State.ConfState),
	}}, nil
}

// OpenSnapshotBase strictly verifies and detaches a snapshot certificate.
func OpenSnapshotBase(snapshot *pb.Snapshot) (SnapshotBaseCertificate, error) {
	if snapshot == nil || snapshot.GetMetadata() == nil ||
		len(snapshot.ProtoReflect().GetUnknown()) != 0 ||
		len(snapshot.GetMetadata().ProtoReflect().GetUnknown()) != 0 {
		return SnapshotBaseCertificate{}, fmt.Errorf("%w: snapshot envelope", ErrSnapshotBase)
	}
	data := snapshot.GetData()
	if len(data) < snapshotBaseHeaderBytes+recordChecksumLen ||
		len(data) > MaxSnapshotBaseCertificateBytes ||
		!bytes.Equal(data[0:8], snapshotBaseMagic[:]) ||
		binary.LittleEndian.Uint16(data[8:10]) != snapshotBaseFormat ||
		binary.LittleEndian.Uint16(data[10:12]) != snapshotBaseHeaderBytes ||
		binary.LittleEndian.Uint32(data[12:16]) != uint32(len(data)) ||
		binary.LittleEndian.Uint16(data[26:28]) != 0 ||
		binary.LittleEndian.Uint64(data[72:80]) != 0 ||
		!verifyRecord(data, snapshotBaseChecksumDomain) {
		return SnapshotBaseCertificate{}, fmt.Errorf("%w: certificate envelope", ErrSnapshotBase)
	}
	stateBytes := uint64(binary.LittleEndian.Uint32(data[16:20]))
	bootstrapBytes := uint64(binary.LittleEndian.Uint32(data[20:24]))
	userBytes := uint64(binary.LittleEndian.Uint16(data[24:26]))
	bodyBytes := uint64(len(data) - snapshotBaseHeaderBytes - recordChecksumLen)
	if stateBytes == 0 || bootstrapBytes == 0 || userBytes == 0 ||
		stateBytes > MaxStateEnvelopeBytes ||
		bootstrapBytes > MaxStaticBootstrapEnvelopeBytes ||
		userBytes > replication.MaxCollectionBytes ||
		stateBytes+bootstrapBytes+userBytes != bodyBytes {
		return SnapshotBaseCertificate{}, fmt.Errorf("%w: certificate geometry", ErrSnapshotBase)
	}
	cursor := snapshotBaseHeaderBytes
	stateEnd := cursor + int(stateBytes)
	bootstrapEnd := stateEnd + int(bootstrapBytes)
	userEnd := bootstrapEnd + int(userBytes)
	stateRaw := data[cursor:stateEnd]
	wantStateDigest := sha256.Sum256(stateRaw)
	if !bytes.Equal(wantStateDigest[:], data[208:240]) {
		return SnapshotBaseCertificate{}, fmt.Errorf("%w: state digest", ErrSnapshotBase)
	}
	state, err := OpenState(stateRaw)
	if err != nil {
		return SnapshotBaseCertificate{}, fmt.Errorf("%w: state: %v", ErrSnapshotBase, err)
	}
	bootstrapRaw := data[stateEnd:bootstrapEnd]
	bootstrap := new(pb.Snapshot)
	if err := proto.Unmarshal(bootstrapRaw, bootstrap); err != nil ||
		len(bootstrap.ProtoReflect().GetUnknown()) != 0 {
		return SnapshotBaseCertificate{}, fmt.Errorf("%w: static bootstrap codec", ErrSnapshotBase)
	}
	canonicalBootstrap, bootstrapDigest, err := validateBootstrap(bootstrap)
	if err != nil || !bytes.Equal(canonicalBootstrap, bootstrapRaw) ||
		bootstrapDigest != state.BootstrapDigest ||
		!bytes.Equal(bootstrapDigest[:], data[240:272]) {
		return SnapshotBaseCertificate{}, fmt.Errorf("%w: static bootstrap identity", ErrSnapshotBase)
	}
	manifest := SnapshotArtifactManifest{
		State: state, UserCollection: bytes.Clone(data[bootstrapEnd:userEnd]),
		TargetChunkBytes: binary.LittleEndian.Uint32(data[28:32]),
		Chunks:           binary.LittleEndian.Uint64(data[32:40]),
		SystemRows:       binary.LittleEndian.Uint64(data[40:48]),
		UserRows:         binary.LittleEndian.Uint64(data[48:56]),
		PayloadBytes:     binary.LittleEndian.Uint64(data[56:64]),
		EncodedBytes:     binary.LittleEndian.Uint64(data[64:72]),
	}
	copy(manifest.HeaderDigest[:], data[80:112])
	copy(manifest.LastChunkDigest[:], data[112:144])
	copy(manifest.Digest[:], data[144:176])
	copy(manifest.ImageDigest[:], data[176:208])
	if err := validateExpectedSnapshotArtifact(manifest); err != nil {
		return SnapshotBaseCertificate{}, fmt.Errorf("%w: manifest: %v", ErrSnapshotBase, err)
	}
	metadata := snapshot.GetMetadata()
	if metadata.GetIndex() != state.Applied || metadata.GetTerm() != state.LastTerm ||
		metadata.GetConfState() == nil ||
		len(metadata.GetConfState().ProtoReflect().GetUnknown()) != 0 ||
		!proto.Equal(metadata.GetConfState(), state.ConfState) {
		return SnapshotBaseCertificate{}, fmt.Errorf("%w: metadata differs from certified state", ErrSnapshotBase)
	}
	digest, err := snapshotBaseIdentity(snapshot)
	if err != nil {
		return SnapshotBaseCertificate{}, err
	}
	return SnapshotBaseCertificate{
		Manifest: manifest, StaticBootstrap: proto.Clone(bootstrap).(*pb.Snapshot), Digest: digest,
	}, nil
}

// StaticBootstrapForSnapshot returns the original index-one bootstrap for
// either an initial WAL base or a newer certified base.
func StaticBootstrapForSnapshot(snapshot *pb.Snapshot) (*pb.Snapshot, error) {
	if len(snapshot.GetData()) >= len(snapshotBaseMagic) &&
		bytes.Equal(snapshot.GetData()[:len(snapshotBaseMagic)], snapshotBaseMagic[:]) {
		certificate, err := OpenSnapshotBase(snapshot)
		if err != nil {
			return nil, err
		}
		return proto.Clone(certificate.StaticBootstrap).(*pb.Snapshot), nil
	}
	if _, _, err := validateBootstrap(snapshot); err != nil {
		return nil, err
	}
	return proto.Clone(snapshot).(*pb.Snapshot), nil
}

func snapshotBaseIdentity(snapshot *pb.Snapshot) ([sha256.Size]byte, error) {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("%w: snapshot identity", ErrSnapshotBase)
	}
	h := sha256.New()
	_, _ = h.Write(snapshotBaseIdentityDomain)
	_, _ = h.Write(encoded)
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result, nil
}
